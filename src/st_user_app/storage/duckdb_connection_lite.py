from streamlit.connections import BaseConnection
import duckdb
from loguru import logger
from pathlib import Path
import pandas as pd
import os


class DuckDBConnectionLite(BaseConnection):
    def _connect(self, **kwargs) -> duckdb.DuckDBPyConnection:
        conn_type = kwargs.get("conn_type")  # Get conn_type from kwargs
        token = kwargs.get("md_token")  # Get md_token from kwargs

        try:
            # Handle in-memory database
            if conn_type in [None, "", "memory", ":memory:"]:
                conn = duckdb.connect(":memory:")
                logger.info("Connected to in-memory DuckDB database.")

            # Handle file-based database
            elif conn_type and conn_type.endswith(".duckdb"):
                db_path = Path(conn_type).resolve()
                conn = duckdb.connect(str(db_path))
                logger.info(f"Connected to DuckDB file at {db_path}.")

            # Handle MotherDuck connection
            elif conn_type and conn_type.startswith("md:"):
                if not token:
                    raise ValueError(
                        "MOTHERDUCK_TOKEN environment variable is required for md: connections."
                    )
                print(token)
                conn_str = f"{conn_type}?motherduck_token={obscure_api_token(token)}"
                logger.info(f"Connected to MotherDuck database with target: {conn_str}")
                conn = duckdb.connect(f"{conn_type}?motherduck_token={token}")

            # Handle URLs for dynamic table creation
            elif conn_type and conn_type.startswith(("http://", "https://")):
                conn = duckdb.connect(":memory:")
                table_name = "auto_table"
                conn.execute(
                    f"CREATE TABLE {table_name} AS SELECT * FROM '{conn_type}'"
                )
                logger.info(
                    f"Connected to in-memory DuckDB and created table from URL: {conn_type}"
                )

            else:
                raise ValueError(f"Unsupported connection target format: {conn_type}")

            return conn
        except Exception as e:
            logger.error(
                f"Failed to establish DuckDB connection for target '{conn_type}': {e}"
            )
            raise

    def query(self, sql: str):
        try:
            return self._instance.execute(sql).df()
        except Exception as e:
            logger.error(f"Failed to execute query: {e}")
            raise


def obscure_api_token(api_token: str) -> str:
    """
    Obscures an API token by hashing and encoding it to make it shorter and less readable.

    Args:
        api_token (str): The original API token.

    Returns:
        str: The obscured version of the API token.
    """

    import hashlib
    import base64

    try:
        # Hash the token using SHA-256
        hashed_token = hashlib.sha256(api_token.encode()).digest()

        # Encode the hashed token using base64
        encoded_token = base64.urlsafe_b64encode(hashed_token).decode()

        # Shorten the encoded token (e.g., take first 16 characters)
        shortened_token = encoded_token[:16]

        return shortened_token
    except Exception as e:
        raise RuntimeError(f"Failed to obscure API token: {e}")


if __name__ == "__main__":
    import streamlit as st
    from dotenv import load_dotenv
    import duckdb

    st.sidebar.write(f"DuckDB version: {duckdb.__version__}")

    load_dotenv()  # for MotherDuck token

    # Example usage
    conn = st.connection(
        "md_dementia",
        type=DuckDBConnectionLite,
        conn_type="md:dementia_data",
        md_token=os.getenv("MOTHERDUCK_TOKEN"),
    )
    result = conn.query("SHOW DATABASES;")
    st.write(result)
