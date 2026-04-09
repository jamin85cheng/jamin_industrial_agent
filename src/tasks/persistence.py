"""Persistence backends for long-running diagnosis tasks."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.utils.runtime_migrations import apply_runtime_schema_migrations
from src.utils.structured_logging import get_logger

logger = get_logger("task_persistence")


def _json_dumps(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, default=str)


def _json_loads(payload: str) -> Dict[str, Any]:
    return json.loads(payload)


def _resolve_postgres_driver():
    try:
        import psycopg  # type: ignore

        return "psycopg", psycopg
    except ImportError:
        try:
            import psycopg2  # type: ignore
            from psycopg2.extras import RealDictCursor  # type: ignore

            return "psycopg2", (psycopg2, RealDictCursor)
        except ImportError:
            return None, None


@dataclass
class TaskPersistenceRecord:
    payload_json: str


class TaskPersistenceBackend:
    storage_label = "unknown"

    @property
    def persistent(self) -> bool:
        return True

    @property
    def target(self) -> str:
        return self.storage_label

    def init(self) -> None:
        raise NotImplementedError

    def load_payloads(self) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def persist_payload(self, task_id: str, status: str, created_at: str, payload: Dict[str, Any]) -> None:
        raise NotImplementedError


class SqliteTaskPersistenceBackend(TaskPersistenceBackend):
    storage_label = "sqlite"

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path).resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def target(self) -> str:
        return str(self.db_path)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def init(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS tracked_tasks (
                    task_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                )
                """
            )
            conn.commit()

    def load_payloads(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("SELECT payload_json FROM tracked_tasks ORDER BY created_at DESC").fetchall()
        return [_json_loads(row["payload_json"]) for row in rows]

    def persist_payload(self, task_id: str, status: str, created_at: str, payload: Dict[str, Any]) -> None:
        payload_json = _json_dumps(payload)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO tracked_tasks(task_id, status, created_at, updated_at, payload_json)
                VALUES(?, ?, ?, datetime('now'), ?)
                ON CONFLICT(task_id) DO UPDATE SET
                    status = excluded.status,
                    updated_at = excluded.updated_at,
                    payload_json = excluded.payload_json
                """,
                (task_id, status, created_at, payload_json),
            )
            conn.commit()


class PostgresTaskPersistenceBackend(TaskPersistenceBackend):
    storage_label = "postgres"

    def __init__(
        self,
        *,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        schema: str = "public",
        sslmode: str = "prefer",
    ):
        self.host = host
        self.port = int(port)
        self.database = database
        self.user = user
        self.password = password
        self.schema = schema or "public"
        self.sslmode = sslmode
        self._driver_name, self._driver = _resolve_postgres_driver()
        if not self._driver:
            raise RuntimeError("Postgres driver is not installed. Install psycopg[binary] or psycopg2-binary.")

    @property
    def migration_config(self) -> Dict[str, Any]:
        return {
            "postgres": {
                "enabled": True,
                "host": self.host,
                "port": self.port,
                "database": self.database,
                "user": self.user,
                "password": self.password,
                "schema": self.schema,
                "sslmode": self.sslmode,
            }
        }

    @property
    def target(self) -> str:
        return f"postgresql://{self.user}@{self.host}:{self.port}/{self.database}#{self.schema}"

    def _connect(self):
        if self._driver_name == "psycopg":
            psycopg = self._driver
            return psycopg.connect(
                host=self.host,
                port=self.port,
                dbname=self.database,
                user=self.user,
                password=self.password,
                sslmode=self.sslmode,
            )

        psycopg2, _ = self._driver
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.database,
            user=self.user,
            password=self.password,
            sslmode=self.sslmode,
        )

    def _dict_cursor(self, conn):
        if self._driver_name == "psycopg":
            return conn.cursor()

        _, dict_cursor = self._driver
        return conn.cursor(cursor_factory=dict_cursor)

    def init(self) -> None:
        apply_runtime_schema_migrations(self.migration_config)

    def load_payloads(self) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            with self._dict_cursor(conn) as cursor:
                cursor.execute(
                    f'SELECT payload_json FROM "{self.schema}".tracked_tasks ORDER BY created_at DESC'
                )
                rows = cursor.fetchall()

        payloads: List[Dict[str, Any]] = []
        for row in rows:
            raw_payload = row["payload_json"] if isinstance(row, dict) else row[0]
            if isinstance(raw_payload, str):
                payloads.append(_json_loads(raw_payload))
            else:
                payloads.append(raw_payload)
        return payloads

    def persist_payload(self, task_id: str, status: str, created_at: str, payload: Dict[str, Any]) -> None:
        payload_json = _json_dumps(payload)
        with self._connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    INSERT INTO "{self.schema}".tracked_tasks(task_id, status, created_at, updated_at, payload_json)
                    VALUES(%s, %s, %s::timestamptz, NOW(), %s::jsonb)
                    ON CONFLICT(task_id) DO UPDATE SET
                        status = EXCLUDED.status,
                        updated_at = EXCLUDED.updated_at,
                        payload_json = EXCLUDED.payload_json
                    """,
                    (task_id, status, created_at, payload_json),
                )
            conn.commit()


def build_task_persistence_backend(config: Dict[str, Any], db_path: Optional[Path] = None) -> TaskPersistenceBackend:
    task_tracking = dict(config.get("task_tracking") or {})
    backend_name = str(task_tracking.get("backend") or "sqlite").lower()

    if db_path is not None:
        return SqliteTaskPersistenceBackend(db_path)

    if backend_name == "postgres":
        postgres = dict(config.get("postgres") or {})
        if postgres.get("enabled", True):
            return PostgresTaskPersistenceBackend(
                host=str(postgres.get("host", "127.0.0.1")),
                port=int(postgres.get("port", 5432)),
                database=str(postgres.get("database", "jamin_industrial_agent")),
                user=str(postgres.get("user", "postgres")),
                password=str(postgres.get("password", "postgres")),
                schema=str(postgres.get("schema", "jamin_industrial_agent")),
                sslmode=str(postgres.get("sslmode", "prefer")),
            )

    sqlite_path = Path(task_tracking.get("sqlite_path") or "data/runtime/tasks.sqlite")
    return SqliteTaskPersistenceBackend(sqlite_path)
