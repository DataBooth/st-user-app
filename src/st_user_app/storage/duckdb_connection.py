from streamlit.connections import BaseConnection
import duckdb
import pandas as pd
import streamlit as st
from pathlib import Path
from loguru import logger
from typing import Optional, Union

SECRETS_FILE = Path(".streamlit/secrets.toml")

class DuckDBConnection(BaseConnection[duckdb.DuckDBPyConnection]):
    def _connect(self, **kwargs) -> duckdb.DuckDBPyConnection:
        db_path = self._secrets.get("database", kwargs.pop("database", ":memory:"))
        create_table_sql = self._secrets.get("create_table")

        if db_path.startswith("md:"):
            token = self._secrets.get("motherduck_token")
            if not token:
                raise ValueError(f"MotherDuck token required in {SECRETS_FILE}")
            db_path = f"{db_path}?motherduck_token={token}"
            conn = duckdb.connect(db_path, **kwargs)
        else:
            is_new_db = db_path != ":memory:" and not Path(db_path).exists()
            conn = duckdb.connect(database=db_path, **kwargs)

            if (db_path == ":memory:" or is_new_db) and create_table_sql:
                conn.execute(create_table_sql)

        return conn

    def query(self, query: Union[str, Path], ttl: int = 3600) -> pd.DataFrame:
        @st.cache_data(ttl=ttl)
        def _query(sql: str) -> pd.DataFrame:
            try:
                return self._instance.execute(sql).df()
            except Exception as e:
                logger.error(f"Query execution failed: {str(e)}")
                st.error(f"Database error: {str(e)}")
                raise

        try:
            if isinstance(query, Path):
                query = query.resolve().read_text(encoding="utf-8")
            return _query(query)
        except Exception as e:
            st.stop()
            return pd.DataFrame()  # Return an empty DataFrame instead of None


def connect_duckdb(connection_name: str) -> DuckDBConnection:
    """Establish validated DuckDB connection"""
    if not SECRETS_FILE.exists():
        raise FileNotFoundError(f"Secrets file not found: {SECRETS_FILE.resolve()}")

    connections = st.secrets.get("connections", {})
    if connection_name not in connections:
        available = ", ".join(connections.keys())
        raise KeyError(f"Connection '{connection_name}' not found in {SECRETS_FILE}. Available connections: {available}")

    logger.info(f"Establishing connection: {connection_name}")
    return st.connection(connection_name, type=DuckDBConnection)


def display_default_query(conn: DuckDBConnection) -> None:
    """Executes and displays the default query, if defined."""
    default_query = (
        st.secrets.get("connections", {})
        .get(conn._connection_name, {})
        .get("default_query")
    )
    if default_query:
        st.info("Running Default Query...")
        df = conn.query(default_query)
        if not df.empty:
            st.dataframe(df)
        else:
            st.warning("Default query returned no results.")

def main():
    st.set_page_config(page_title="DuckDB Connection Demo", layout="wide")
    st.title("DuckDB Connection Example")

    # Get available connections from secrets
    connections = st.secrets.get("connections", {})
    available_connections = list(connections.keys())

    if not available_connections:
        st.error("No DuckDB connections found in secrets.toml")
        st.stop()

    CONNECTION_NAME = st.sidebar.selectbox(
        "Select Connection",
        available_connections
    )

    try:
        conn = connect_duckdb(CONNECTION_NAME)
        st.success(f"Successfully connected to {CONNECTION_NAME}")

        # Show tables query
        st.subheader("Database Tables")
        df_tables = conn.query("SHOW TABLES;")
        if not df_tables.empty:
            st.dataframe(df_tables, use_container_width=True)
        else:
            st.warning("No tables found in the database.")

        # Show results of default query if defined
        st.subheader("Default Query Result")
        display_default_query(conn)

        # Query from file
        st.subheader("Query from File")
        query_file = Path("sql/duckdb_md_select_default.sql")
        if query_file.exists():
            with st.expander(f"View SQL Query: {query_file.name}", expanded=False):
                st.code(query_file.read_text(), language="sql")
            df_file_query = conn.query(query_file)
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
        if st.button("Execute Query"):
            if custom_query:
                df_custom = conn.query(custom_query)
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

    except Exception as e:
        st.error(f"Error: {str(e)}")


if __name__ == "__main__":
    main()
