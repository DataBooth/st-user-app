from streamlit.connections import BaseConnection
import duckdb
from loguru import logger
from pydantic import BaseModel, HttpUrl, field_validator
from typing import Optional, Union
from urllib.parse import urlparse
from pathlib import Path
import time
import pandas as pd
from cachetools import TTLCache
import streamlit as st
import os


class ConnectionConfig(BaseModel):
    data_uri: str
    source_url: Optional[HttpUrl] = None
    create_table: Optional[Union[bool, str]] = True
    motherduck_token: Optional[str] = None
    description: Optional[str] = None
    timeout_ms: int = 3000
    default_ttl: int = 3600
    cache_max_items: int = 50
    cache_enabled: bool = True
    file_query: Optional[str] = None  # ADDED THIS LINE

    @field_validator("data_uri")
    @classmethod
    def validate_data_uri(cls, v: str):
        if not v.startswith(("file://", "memory://", "md://")):
            v = f"file://{v}"
        scheme = urlparse(v).scheme
        if scheme not in {"file", "memory", "md"}:
            raise ValueError(f"Invalid URI scheme: {scheme}")
        return v


class ConnectionMetrics:
    def __init__(self):
        self.connection_time_sec = 0
        self.query_count = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.avg_query_time_ms = 0.0


class DuckDBConnection(BaseConnection):
    def _connect(self, **kwargs) -> duckdb.DuckDBPyConnection:
        logger.debug("Initializing DuckDBConnection")

        try:
            # Access the specific connection's secrets using the connection name
            config = self._secrets
            logger.debug(f"Raw configuration: {config}")

            self.config = ConnectionConfig(**config)
            logger.debug("Configuration validated")
            self.metrics = ConnectionMetrics()
            self._init_cache()
            logger.debug("Cache initialized")

            logger.debug(f"Connecting to {self.config.data_uri}")
            start = time.monotonic()

            uri = urlparse(self.config.data_uri)
            if uri.scheme == "file":
                db_path = uri.path
                logger.debug(f"Attempting to connect to file: {db_path}")
                is_new = not os.path.exists(db_path)
                conn = duckdb.connect(db_path)

                if is_new and self.config.create_table:
                    self._create_tables(conn)

            elif uri.scheme == "memory":
                conn = duckdb.connect(":memory:")
                if self.config.create_table:
                    self._create_tables(conn)

            elif uri.scheme == "md":
                if not self.config.motherduck_token:
                    raise ValueError(
                        "MotherDuck token is required for MotherDuck connections"
                    )

                # Format the URI correctly for DuckDB's MotherDuck connection
                db_uri = (
                    f"md:{uri.path}?motherduck_token={self.config.motherduck_token}"
                )

                # Obfuscate the token for logging
                obfuscated_token = (
                    self.config.motherduck_token[:4]
                    + "..."
                    + self.config.motherduck_token[-4:]
                )
                logger.debug(f"Connecting to MotherDuck with token: {obfuscated_token}")

                conn = duckdb.connect(db_uri)

            else:
                raise ValueError(f"Unsupported URI scheme: {uri.scheme}")

            self.metrics.connection_time_sec = time.monotonic() - start
            logger.info(
                f"Connected to {self.config.data_uri} in {self.metrics.connection_time_sec:.2f}ms"
            )
            return conn

        except Exception as e:
            logger.exception(f"Connection failed: {e}")
            raise

    def _validate_config(self):
        logger.debug("Validating configuration")
        try:
            config = ConnectionConfig(**self._raw_config)
            logger.debug("Configuration validated successfully")
            return config
        except Exception as e:
            logger.error(f"Configuration validation failed: {str(e)}")
            raise

    def _init_cache(self):
        self._cache = TTLCache(
            maxsize=self.config.cache_max_items, ttl=self.config.default_ttl
        )

    def _create_tables(self, conn):
        if self.config.source_url:
            table_name = (
                self.config.create_table
                if isinstance(self.config.create_table, str)
                else Path(urlparse(self.config.source_url).path).stem
            )
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS {table_name} AS FROM '{self.config.source_url}'"
            )
            logger.info(f"Created table {table_name} from {self.config.source_url}")

    def query(self, query: str, ttl: Optional[int] = None) -> pd.DataFrame:
        start_time = time.monotonic()  # Start timing the query

        @st.cache_data(ttl=ttl)
        def _cached_query(q: str) -> pd.DataFrame:
            logger.debug(f"Executing query: {q[:50]}...")
            try:
                result = self._instance.execute(q).df()
                self.metrics.query_count += 1
                logger.debug("Query executed")
                return result
            except Exception as e:
                logger.error(f"Query execution failed: {e}")
                st.error(f"Query execution failed: {e}")
                raise

        result = _cached_query(query)
        query_time = time.monotonic() - start_time  # End timing the query
        self.metrics.avg_query_time_ms = (
            query_time * 1000
        )  # Store query time in milliseconds
        return result

    def get_metrics(self):
        return self.metrics


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
        "connection_time_sec": metrics.connection_time_sec,
        "query_count": metrics.query_count,
        "cache_hits": metrics.cache_hits,
        "cache_misses": metrics.cache_misses,
        "avg_query_time_ms": metrics.avg_query_time_ms,
    }
    st.sidebar.json(metrics_display)


def main():
    logger.info("Starting DuckDB Connection Demo")
    st.set_page_config(page_title="DuckDB Connection Demo", layout="wide")
    st.title("Streamlit DuckDB Connection Examples")

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
        format_func=lambda x: f"{x} - {connections[x].get('description', 'No description')}",
    )
    logger.info(f"Selected connection: {CONNECTION_NAME}")

    try:
        logger.debug(f"Attempting to create DuckDBConnection for {CONNECTION_NAME}")
        conn = st.connection(CONNECTION_NAME, type=DuckDBConnection)
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
        default_query = connections[CONNECTION_NAME].get("default_query")
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
        query_file_path = connections[CONNECTION_NAME].get("file_query")
        if query_file_path:
            query_file = Path(query_file_path)
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
        custom_ttl = st.sidebar.number_input(
            "Cache TTL (seconds, 0 to disable)",
            min_value=0,
            value=conn.config.default_ttl,
        )
        if st.button("Execute Query"):
            if custom_query:
                df_custom = conn.query(
                    custom_query, ttl=custom_ttl if custom_ttl > 0 else None
                )
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
            # conn.clear_cache() # clear cache will need to be refactored.
            st.sidebar.success("Cache cleared")

        cache_enabled = st.sidebar.checkbox(
            "Enable Cache", value=conn.config.cache_enabled
        )
        # if cache_enabled != conn.config.cache_enabled: # the logic for this will need to be reworked.
        #     if cache_enabled:
        #         conn.enable_cache()
        #     else:
        #         conn.disable_cache()
        st.sidebar.success(f"Cache {'enabled' if cache_enabled else 'disabled'}")

    except Exception as e:
        st.error(f"Error: {str(e)}")


if __name__ == "__main__":
    main()
