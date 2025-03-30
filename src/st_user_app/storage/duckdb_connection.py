from streamlit.connections import BaseConnection
import duckdb
import pandas as pd
import streamlit as st


class DuckDBConnection(BaseConnection[duckdb.DuckDBPyConnection]):
    """Custom DuckDB connection for Streamlit, supporting local and MotherDuck connections"""

    def _connect(self, **kwargs) -> duckdb.DuckDBPyConnection:
        if 'database' in kwargs:
            db_path = kwargs.pop('database')
        else:
            db_path = self._secrets['database']
        db_path = self._secrets.get("database", ":memory:")

        if db_path.startswith("md:"):  # MotherDuck connection
            motherduck_token = self._secrets.get("motherduck_token")
            if not motherduck_token:
                raise ValueError(
                    "MotherDuck token is required for MotherDuck connections"
                )
            db_path = f"{db_path}?motherduck_token={motherduck_token}"

        return duckdb.connect(database=db_path, **kwargs)

    def query(self, query: str, ttl: int = 3600) -> pd.DataFrame:
        @st.cache_data(ttl=ttl)
        def _query(query: str) -> pd.DataFrame:
            return self._instance.execute(query).df()

        return _query(query)


# Example usage
if __name__ == "__main__":
    CONNECTION_NAME = "md_dementia"
    # CONNECTION_NAME = "duckdb_local"

    if CONNECTION_NAME not in st.secrets.get("connections", {}):
        st.error(
            f"Connection '{CONNECTION_NAME}' is not defined in ./streamlit/secrets.toml"
        )
    st.title("DuckDB Connection Example")
    st.subheader(f"Connection Name: {CONNECTION_NAME}")
    st.subheader("DuckDB Tables")
    conn = st.connection(CONNECTION_NAME, type=DuckDBConnection)
    df = conn.query("SHOW TABLES;")
    st.dataframe(df)
