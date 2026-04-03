"""Runtime database adapter for metadata and API dependencies."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict

from src.utils.config import load_config
from src.utils.structured_logging import get_logger

logger = get_logger("database_runtime")


def _load_postgres_driver():
    try:
        import psycopg  # type: ignore

        return "psycopg", psycopg
    except ImportError:
        return None, None


class RuntimeDatabaseAdapter:
    def __init__(self, config: Dict[str, Any] | None = None):
        self.config = config or load_config().get("database", {})
        self.backend = self._resolve_backend()
        self._driver_name, self._driver = _load_postgres_driver()

    def _resolve_backend(self) -> str:
        postgres = self.config.get("postgres", {})
        if postgres.get("enabled"):
            return "postgres"
        return "sqlite"

    @property
    def target(self) -> str:
        if self.backend == "postgres":
            postgres = self.config.get("postgres", {})
            return f"postgresql://{postgres.get('user', 'postgres')}@{postgres.get('host', '127.0.0.1')}:{postgres.get('port', 5432)}/{postgres.get('database', 'jamin_industrial_agent')}#{postgres.get('schema', 'public')}"
        sqlite_config = self.config.get("sqlite", {})
        return str(Path(sqlite_config.get("path", "data/metadata.db")).resolve())

    @contextmanager
    def connect(self):
        if self.backend == "postgres":
            if not self._driver:
                raise RuntimeError("Postgres metadata backend is enabled but psycopg is not installed.")

            postgres = self.config.get("postgres", {})
            conn = self._driver.connect(
                host=postgres.get("host", "127.0.0.1"),
                port=int(postgres.get("port", 5432)),
                dbname=postgres.get("database", "jamin_industrial_agent"),
                user=postgres.get("user", "postgres"),
                password=postgres.get("password", "postgres"),
                sslmode=postgres.get("sslmode", "prefer"),
            )
            try:
                yield conn
            finally:
                conn.close()
            return

        sqlite_path = Path(self.config.get("sqlite", {}).get("path", "data/metadata.db")).resolve()
        sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(sqlite_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()


def build_runtime_database_adapter(config: Dict[str, Any] | None = None) -> RuntimeDatabaseAdapter:
    return RuntimeDatabaseAdapter(config=config)
