import os

import duckdb
from dotenv import load_dotenv
from loguru import logger

from .base_storage import BaseStorage


class DuckDBStorage(BaseStorage):
    def __init__(self, location="local"):
        super().__init__("duckdb")
        self.conn = self._create_connection(location)
        self._execute_sql("init")

    def _create_connection(self, location):
        if location == "remote":
            load_dotenv()

            token = os.getenv("MOTHERDUCK_TOKEN")
            db_name = os.getenv("MOTHERDUCK_DB_NAME")

            if not token:
                raise EnvironmentError(
                    "MOTHERDUCK_TOKEN is not defined in the environment or .env fil - required for remote storage"
                )
            if not db_name:
                raise EnvironmentError(
                    "MOTHERDUCK_DB_NAME is not defined in the environment or .env file - required for remote storage"
                )
            logger.info("Connecting to MotherDuck remote storage")
            return duckdb.connect(f"md:{db_name}?motherduck_token={token}")
        logger.info("Using local DuckDB storage")
        return duckdb.connect("local_data.db")

    def _execute_sql(self, template_name, **params):
        query = self._load_sql(template_name).format(**params)
        return self.conn.execute(query)

    def save_data(self, user_id, data):
        self._execute_sql("insert_data", user_id=user_id, data=data)

    def get_user_data(self, user_id):
        result = self._execute_sql("select_data", user_id=user_id)
        return result.fetchall()

    def close(self):
        self.conn.close()
        logger.info("DuckDB connection closed")
