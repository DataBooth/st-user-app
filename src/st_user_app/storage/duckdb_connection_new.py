import hashlib
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Union
from urllib.parse import urlparse

import duckdb
import pandas as pd
import streamlit as st
from cachetools import TTLCache
from loguru import logger
from pydantic import BaseModel, Field, HttpUrl, field_validator
from streamlit.connections import BaseConnection


@dataclass
class ConnectionMetrics:
    connection_time_ms: float = 0
    init_time_ms: float = 0
    query_count: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    avg_cache_size: float = 0
    total_query_time_ms: float = 0
    queries: dict = field(default_factory=dict)

    def record_query(self, query: str, execution_time_ms: float):
        self.query_count += 1
        self.total_query_time_ms += execution_time_ms
        if query not in self.queries:
            self.queries[query] = {"count": 0, "total_time_ms": 0}
        self.queries[query]["count"] += 1
        self.queries[query]["total_time_ms"] += execution_time_ms

    @property
    def avg_query_time_ms(self):
        return (
            self.total_query_time_ms / self.query_count if self.query_count > 0 else 0
        )

class ConnectionConfig(BaseModel):
    data_uri: str = Field(..., pattern=r"^(file|memory|md)://")
    source_url: Optional[HttpUrl] = None
    create_table: Optional[Union[bool, str]] = True
    motherduck_token: Optional[str] = None
    description: Optional[str] = None
    timeout_ms: int = 3000
    default_ttl: int = 3600  # 1 hour default
    cache_max_items: int = 50
    cache_enabled: bool = True

    @field_validator('data_uri')
    @classmethod
    def validate_data_uri(cls, v: str, info):
        scheme = urlparse(v).scheme
        if scheme not in {'file', 'memory', 'md'}:
            raise ValueError(f"Invalid URI scheme: {scheme}")
        if scheme == 'md' and 'motherduck_token' not in info.data:
            raise ValueError("MotherDuck token is required for MotherDuck connections")
        return v

    @field_validator('default_ttl')
    @classmethod
    def validate_ttl(cls, v: int):
        if v < 0:
            raise ValueError("TTL must be ≥ 0 (0 disables caching)")
        return v

    @field_validator('cache_max_items')
    @classmethod
    def validate_cache_max_items(cls, v: int):
        if v < 1:
            raise ValueError("cache_max_items must be > 0")
        return v

    @field_validator('timeout_ms')
    @classmethod
    def validate_timeout(cls, v: int):
        if v < 0:
            raise ValueError("timeout_ms must be ≥ 0")
        return v



class DuckDBConnection(BaseConnection):
    def __init__(self, connection_name: str, **kwargs):
        logger.debug(f"Initializing DuckDBConnection for {connection_name}")
        super().__init__(connection_name, **kwargs)
        self._raw_config = self._secrets.get(self._connection_name, {})
        logger.debug(f"Raw configuration: {self._raw_config}")
        self.config = self._validate_config()
        logger.debug(f"Configuration validated for {connection_name}")
        self.metrics = ConnectionMetrics()
        self._init_cache()
        logger.debug(f"Cache initialized for {connection_name}")
        self._conn = self._connect()
        logger.debug(f"Connection established for {connection_name}")

    def _validate_config(self):
        logger.debug("Validating configuration")
        try:
            config = ConnectionConfig(**self._raw_config)
            logger.debug("Configuration validated successfully")
            return config
        except Exception as e:
            logger.error(f"Configuration validation failed: {str(e)}")
            raise

    def _connect(self):
        logger.debug(f"Connecting to {self.config.data_uri}")

        """Connection logic with timing"""
        start = time.monotonic()
        
        # Ensure data_uri is in the correct format
        if not self.config.data_uri.startswith(('file://', 'memory://', 'md://')):
            # Assume it's a file-based connection if not specified
            self.config.data_uri = f"file://{self.config.data_uri}"
        
        uri = urlparse(self.config.data_uri)
        if uri.scheme == "file":
            db_path = Path(uri.path)
            is_new = not db_path.exists()
            conn = duckdb.connect(str(db_path))
            if is_new and self.config.create_table:
                self._create_tables(conn)
                
        elif uri.scheme == "memory":
            conn = duckdb.connect(":memory:")
            if self.config.create_table:
                self._create_tables(conn)
                
        elif uri.scheme == "md":
            if not self.config.motherduck_token:
                raise ValueError("MotherDuck token is required for MotherDuck connections")
            db_uri = f"{self.config.data_uri}?motherduck_token={self.config.motherduck_token}"
            conn = duckdb.connect(db_uri)
        else:
            raise ValueError(f"Unsupported URI scheme: {uri.scheme}")
        
        self.metrics.connection_time_ms = (time.monotonic() - start) * 1000
        logger.info(f"Connected to {self.config.data_uri} in {self.metrics.connection_time_ms:.2f}ms")
        return conn


    def _create_tables(self, conn):
        """Table creation with source URL"""
        if self.config.source_url:
            table_name = (
                self.config.create_table
                if isinstance(self.config.create_table, str)
                else Path(urlparse(self.config.source_url.path).path).stem
            )
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS {table_name} AS FROM '{self.config.source_url}'"
            )
            logger.info(f"Created table {table_name} from {self.config.source_url}")

    def _cache_key(self, query: str) -> str:
        """Create stable hash-based cache key"""
        return hashlib.sha256(query.encode()).hexdigest()

    def query(self, query: str, ttl: Optional[int] = None) -> pd.DataFrame:
        """Execute query with intelligent caching"""
        cache_key = self._cache_key(query)

        # Return cached result if available
        if cached := self._cache.get(cache_key):
            self.metrics.cache_hits += 1
            logger.debug(f"Cache hit for query: {query[:50]}...")
            return cached.copy()  # Prevent cache mutation

        # Execute and cache
        self.metrics.cache_misses += 1
        start = time.monotonic()
        result = self._conn.execute(query).fetchdf()
        query_time = (time.monotonic() - start) * 1000
        self.metrics.query_count += 1
        logger.info(f"Query executed in {query_time:.2f}ms")

        # Apply TTL override if specified
        original_ttl = self._cache.ttl
        if ttl is not None:
            self._cache.ttl = ttl

        self._cache[cache_key] = result.copy()

        # Reset TTL if overridden
        if ttl is not None:
            self._cache.ttl = original_ttl

        return result

    def get_metrics(self):
        """Return current connection metrics"""
        return self.metrics

    def clear_cache(self):
        """Clear all cached queries"""
        self._cache.clear()
        logger.info("Query cache cleared")

    def disable_cache(self):
        """Temporarily disable caching"""
        self._cache.ttl = 0
        logger.warning("Caching disabled for this connection")

    def enable_cache(self):
        """Re-enable caching with configured TTL"""
        self._cache.ttl = self.config.default_ttl
        logger.info("Caching re-enabled")

    # def close(self):
    #     """Close the database connection"""
    #     if self._conn:
    #         self._conn.close()
    #         logger.info("Database connection closed")



# from duckdb_connection import DuckDBConnection, ConnectionConfig

def display_connection_details(config):
    st.sidebar.subheader("Connection Details")
    details = {
        "data_uri": config.data_uri,
        "description": config.description,
        "cache_enabled": config.cache_enabled,
        "default_ttl": config.default_ttl,
        "cache_max_items": config.cache_max_items,
    }
    st.sidebar.json(details)

def display_connection_metrics(metrics):
    st.sidebar.subheader("Connection Metrics")
    metrics_display = {
        "connection_time_ms": metrics.connection_time_ms,
        "query_count": metrics.query_count,
        "cache_hits": metrics.cache_hits,
        "cache_misses": metrics.cache_misses,
        "avg_query_time_ms": metrics.avg_query_time_ms,
    }
    st.sidebar.json(metrics_display)



def main():
    logger.info("Starting DuckDB Connection Demo")
    st.set_page_config(page_title="DuckDB Connection Demo", layout="wide")
    st.title("DuckDB Connection Example")

    # Get available connections from secrets
    connections = st.secrets.get("connections", {})
    available_connections = list(connections.keys())
    logger.debug(f"Available connections: {available_connections}")

    if not available_connections:
        logger.error("No DuckDB connections found in secrets.toml")
        st.error("No DuckDB connections found in secrets.toml")
        st.stop()

    CONNECTION_NAME = st.sidebar.selectbox(
        "Select Connection",
        available_connections,
        format_func=lambda x: f"{x} - {connections[x].get('description', 'No description')}"
    )
    logger.info(f"Selected connection: {CONNECTION_NAME}")

    try:
        logger.debug(f"Attempting to create DuckDBConnection for {CONNECTION_NAME}")
        conn = DuckDBConnection(CONNECTION_NAME)
        logger.info(f"Successfully connected to {CONNECTION_NAME}")
        st.success(f"Successfully connected to {CONNECTION_NAME}")

        # Display connection details and metrics
        logger.debug("Displaying connection details")
        display_connection_details(conn.config)
        logger.debug("Displaying connection metrics")
        display_connection_metrics(conn.get_metrics())

        # Show tables query
        st.subheader("Database Tables")
        df_tables = conn.query("SHOW TABLES;")
        if not df_tables.empty:
            st.dataframe(df_tables, use_container_width=True)
        else:
            st.warning("No tables found in the database.")


        # Show results of default query if defined
        st.subheader("Default Query Result")
        default_query = connections[CONNECTION_NAME].get('default_query')
        if default_query:
            df_default = conn.query(default_query)
            if not df_default.empty:
                st.dataframe(df_default, use_container_width=True)
            else:
                st.warning("Default query returned no results.")
        else:
            st.info("No default query defined for this connection.")

        # Query from file
        st.subheader("Query from File")
        query_file = Path("sql/duckdb_md_select_default.sql")
        if query_file.exists():
            with st.expander(f"View SQL Query: {query_file.name}", expanded=False):
                st.code(query_file.read_text(), language="sql")
            df_file_query = conn.query(query_file.read_text())
            if not df_file_query.empty:
                st.dataframe(df_file_query, use_container_width=True)
                st.download_button(
                    label="Download CSV",
                    data=df_file_query.to_csv(index=False).encode("utf-8"),
                    file_name=f"{query_file.stem}.csv",
                    mime="text/csv",
                )
            else:
                st.warning("Query returned no results.")
        else:
            st.error(f"Query file not found: {query_file}")

        # Custom SQL query input
        st.subheader("Custom SQL Query")
        custom_query = st.text_area("Enter your SQL query:", height=100)
        custom_ttl = st.number_input("Cache TTL (seconds, 0 to disable)", min_value=0, value=conn.config.default_ttl)
        if st.button("Execute Query"):
            if custom_query:
                df_custom = conn.query(custom_query, ttl=custom_ttl if custom_ttl > 0 else None)
                if not df_custom.empty:
                    st.dataframe(df_custom, use_container_width=True)
                    st.download_button(
                        label="Download Custom Query Result",
                        data=df_custom.to_csv(index=False).encode("utf-8"),
                        file_name="custom_query_result.csv",
                        mime="text/csv",
                    )
                else:
                    st.warning("Custom query returned no results.")
            else:
                st.warning("Please enter a SQL query.")

        # Cache management
        st.sidebar.subheader("Cache Management")
        if st.sidebar.button("Clear Cache"):
            conn.clear_cache()
            st.sidebar.success("Cache cleared")
        
        cache_enabled = st.sidebar.checkbox("Enable Cache", value=conn.config.cache_enabled)
        if cache_enabled != conn.config.cache_enabled:
            if cache_enabled:
                conn.enable_cache()
            else:
                conn.disable_cache()
            st.sidebar.success(f"Cache {'enabled' if cache_enabled else 'disabled'}")

    except Exception as e:
        st.error(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
