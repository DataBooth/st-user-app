from st_user_app.st_user_app import StreamlitUserApp
import os

if __name__ == "__main__":
    app = StreamlitUserApp(storage_type=os.getenv("STORAGE_TYPE", "duckdb"))
    app.run()
