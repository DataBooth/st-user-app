from pathlib import Path
from loguru import logger


class BaseStorage:
    @staticmethod
    def get_project_root() -> Path:
        """Determine the project root directory by searching for pyproject.toml."""
        current_path = Path(__file__).parent
        while current_path != current_path.root:
            if (current_path / "pyproject.toml").exists():
                return current_path
            current_path = current_path.parent
        raise FileNotFoundError("Could not find pyproject.toml in any parent directory")

    PROJECT_ROOT = get_project_root()
    SQL_DIR = PROJECT_ROOT / "sql"

    def __init__(self, storage_type: str):
        self.storage_type = storage_type
        logger.info(f"Initializing {storage_type} storage")

    def _load_sql(self, template_name: str) -> str:
        """Load SQL from template file"""
        template_path = self.SQL_DIR / f"{self.storage_type}_{template_name}.sql"
        try:
            with open(template_path, "r") as f:
                return f.read()
        except FileNotFoundError:
            logger.error(f"SQL template not found: {template_path}")
            raise
