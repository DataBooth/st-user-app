from storage.duckdb_storage import DuckDBStorage
from storage.sqlite_storage import SQLiteStorage
from storage.supabase_storage import SupabaseStorage


class StorageFactory:
    @staticmethod
    def create(storage_type: str, location: str = "local"):
        match storage_type.lower():
            case "duckdb":
                return DuckDBStorage(location)
            case "sqlite":
                return SQLiteStorage(location)
            case "supabase":
                return SupabaseStorage()
            case _:
                raise ValueError(f"Unsupported storage type: {storage_type}")
