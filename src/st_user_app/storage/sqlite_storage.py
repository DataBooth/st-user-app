import os

import libsql_experimental as libsql
from loguru import logger

from .base_storage import BaseStorage


class SQLiteStorage(BaseStorage):
    def __init__(self, location="local"):
        super().__init__("sqlite")
        self.conn = self._create_connection(location)
        self._execute_sql("init")

    def _create_connection(self, location):
        if location == "remote":
            url = os.getenv("TURSO_DB_URL")
            auth_token = os.getenv("TURSO_AUTH_TOKEN")
            if not url or not auth_token:
                raise ValueError(
                    "TURSO_DB_URL and TURSO_AUTH_TOKEN environment variables are required for remote storage"
                )
            logger.info("Connecting to Turso remote storage")
            conn = libsql.connect(url=url, auth_token=auth_token)
            conn.sync()
            return conn
        logger.info("Using local SQLite storage")
        return libsql.connect("local_data.db")

    def _execute_sql(self, template_name, **params):
        query = self._load_sql(template_name).format(**params)
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        self.conn.commit()
        return cursor

    def save_data(self, user_id, data):
        self._execute_sql("insert_data", user_id=user_id, data=data)

    def get_user_data(self, user_id):
        cursor = self._execute_sql("select_data", user_id=user_id)
        return cursor.fetchall()

    def close(self):
        self.conn.close()
        logger.info("SQLite connection closed")
