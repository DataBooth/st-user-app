from streamlit.connections import BaseConnection
import duckdb
import pandas as pd
import streamlit as st
from pathlib import Path
from loguru import logger

SECRETS_FILE = "./streamlit/secrets.toml"

class DuckDBConnection(BaseConnection[duckdb.DuckDBPyConnection]):
    """Custom DuckDB Streamlit connection supporting local/MotherDuck connections"""

    def _connect(self, **kwargs) -> duckdb.DuckDBPyConnection:
        # Get database path from secrets or kwargs
        db_path = self._secrets.get("database", kwargs.pop("database", ":memory:"))

        # For MotherDuck connections
        if db_path.startswith("md:"):
            motherduck_token = self._secrets.get("motherduck_token")
            if not motherduck_token:
                raise ValueError("MotherDuck token required in secrets.toml")
            db_path = f"{db_path}?motherduck_token={motherduck_token}"

        st.toast(f"Connecting to: {db_path.split('?')[0]}")  # Hide token from display
        return duckdb.connect(database=db_path, **kwargs)

    def query(self, query: str, ttl: int = 3600) -> pd.DataFrame:
        @st.cache_data(ttl=ttl)
        def _query(q: str) -> pd.DataFrame:
            return self._instance.execute(q).df()
        return _query(query)


def connect_duckdb(connection_name: str):
    if connection_name not in st.secrets.get("connections", {}):
        st.error(
            f"Connection '{connection_name}' is not defined in {SECRETS_FILE}."
        )
        return
    logger.info(f"Connection Name: {connection_name}")
    return st.connection(connection_name, type=DuckDBConnection)


def duckdb_sql(query: str | Path, conn: DuckDBConnection = None):
    """Execute a SQL query and display the result in Streamlit."""
    if conn is None:
        st.error("No connection provided.")
        return
    if query:
        if isinstance(query, Path):
            if not query.exists():
                st.error(f"Query file '{query}' not found.")
                return
            try:
                with query.open("r") as file:
                    query = file.read()
            except Exception as e:
                st.error(f"Error reading query file: {e}")
                return
        return conn.query(query)


# Example usage

if __name__ == "__main__":
    st.title("DuckDB Connection Example")

    CONNECTION_NAME = "md_dementia"
    conn = connect_duckdb(CONNECTION_NAME)

    query = "SHOW TABLES;"
    # query_file = Path("/path/to/query.sql")

    df = duckdb_sql(query, conn)
    if df is not None:
        st.write("Query Result:")
        st.dataframe(df)
    else:
        st.error("Failed to execute query.")

    query_file = Path("sql/duckdb_select_data.sql")
    df = duckdb_sql(query_file, conn)
    if df is not None:
        st.write("Query Result from File:")
        st.dataframe(df)
    else:
        st.error("Failed to execute query from file.")
