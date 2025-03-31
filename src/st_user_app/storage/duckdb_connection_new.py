from streamlit.connections import BaseConnection
import duckdb
import pandas as pd
import streamlit as st
from pathlib import Path
from loguru import logger
from typing import Optional, Union

SECRETS_FILE = Path(".streamlit/secrets.toml")

class DuckDBConnection(BaseConnection[duckdb.DuckDBPyConnection]):
    """Custom DuckDB connection for Streamlit with table creation support for in-memory databases"""
    
    def _connect(self, **kwargs) -> duckdb.DuckDBPyConnection:
        db_path = self._secrets.get("database", kwargs.pop("database", ":memory:"))
        
        # MotherDuck support
        if db_path.startswith("md:"):
            token = self._secrets.get("motherduck_token")
            if not token:
                raise ValueError("MotherDuck token is required in secrets.toml")
            db_path = f"{db_path}?motherduck_token={token}"
        
        st.toast(f"Connecting to: {db_path.split('?')[0]}", icon="✅")
        conn = duckdb.connect(database=db_path, **kwargs)
        
        # Execute CREATE TABLE statement only for in-memory databases
        if db_path == ":memory:":
            create_table_sql = self._secrets.get("create_table")
            if create_table_sql:
                st.toast("Creating Table in Memory", icon="⚙️")
                try:
                    conn.execute(create_table_sql)
                except Exception as e:
                    st.error(f"Failed to create table: {str(e)}")
                    raise
        
        return conn
    
    def query(self, query: str, ttl: int = 3600) -> pd.DataFrame:
        @st.cache_data(ttl=ttl)
        def _query(sql: str) -> pd.DataFrame:
            st.toast("Running Query", icon="🚀")
            return self._instance.execute(sql).df()
        return _query(query)


def connect_duckdb(connection_name: str) -> DuckDBConnection:
    """Establish validated DuckDB connection"""
    if not SECRETS_FILE.exists():
        st.error(f"Secrets file not found: {SECRETS_FILE.resolve()}")
        st.stop()

    if connection_name not in st.secrets.get("connections", {}):
        st.error(f"Missing connection '{connection_name}' in {SECRETS_FILE}")
        st.stop()

    logger.info(f"Establishing connection: {connection_name}")
    return st.connection(connection_name, type=DuckDBConnection)


def duckdb_sql(
    query: Union[str, Path], conn: DuckDBConnection
) -> Optional[pd.DataFrame]:
    """Execute SQL query from string or file"""
    try:
        if isinstance(query, Path):
            query = query.resolve().read_text(encoding="utf-8")
        return conn.query(query)

    except Exception as e:
        logger.error(f"Query execution failed: {str(e)}")
        st.error(f"Database error: {str(e)}")
        st.stop()
        return None


# Example usage

if __name__ == "__main__":
    st.set_page_config(page_title="DuckDB Connection Demo", layout="wide")
    st.title("DuckDB Connection Example")

    # CONNECTION_NAME = "md_dementia"
    CONNECTION_NAME = "duckdb_services_csv"

    try:
        conn = connect_duckdb(CONNECTION_NAME)
        st.success(f"Successfully connected to {CONNECTION_NAME}")
    except Exception as e:
        st.error(f"Failed to connect to {CONNECTION_NAME}: {str(e)}")
        st.stop()

    # Show tables (string) query
    st.subheader("Display Database Tables")
    query = "SHOW TABLES;"
    df_tables = duckdb_sql(query, conn)
    if df_tables is not None and not df_tables.empty:
        st.dataframe(df_tables, use_container_width=True)
    else:
        st.warning("No tables found in the database.")

    # Query from file
    st.subheader("Query from File")
    query_file = Path("sql/duckdb_select_data.sql")
    if query_file.exists():
        with st.expander(f"View SQL Query: {query_file.name}", expanded=False):
            st.code(query_file.read_text(), language="sql")
        df_file_query = duckdb_sql(query_file, conn)
        if df_file_query is not None and not df_file_query.empty:
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
    if st.button("Execute Query"):
        if custom_query:
            df_custom = duckdb_sql(custom_query, conn)
            if df_custom is not None and not df_custom.empty:
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
