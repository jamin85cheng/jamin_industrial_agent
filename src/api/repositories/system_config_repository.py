"""Persistent storage for runtime-editable system configuration."""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from src.utils.config import load_config
from src.utils.database_runtime import build_runtime_database_adapter
from src.utils.runtime_migrations import apply_runtime_schema_migrations


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_datetime(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return value
    return value


def _json_dumps(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False)


def _json_loads(payload: Any) -> Dict[str, Any]:
    if payload is None:
        return {}
    if isinstance(payload, dict):
        return payload
    return json.loads(payload)


class SystemConfigRepository:
    """Stores runtime-editable system configuration in the metadata database."""

    def __init__(self, db_config: Optional[Dict[str, Any]] = None):
        self.db_config = db_config or load_config().get("database", {})
        self.adapter = build_runtime_database_adapter(self.db_config)
        self.backend = self.adapter.backend
        self.schema = str(self.db_config.get("postgres", {}).get("schema", "public"))
        self._schema_ready = False

    @contextmanager
    def _connect(self):
        with self.adapter.connect() as connection:
            yield connection

    def _table(self, table_name: str) -> str:
        if self.backend == "postgres":
            return f'"{self.schema}".{table_name}'
        return table_name

    def _fetch_one(self, cursor) -> Optional[Dict[str, Any]]:
        row = cursor.fetchone()
        if not row:
            return None
        if isinstance(row, dict):
            return row
        if hasattr(row, "keys"):
            return dict(row)
        columns = [column[0] for column in (cursor.description or [])]
        return dict(zip(columns, row))

    def init_schema(self) -> None:
        if self._schema_ready:
            return
        if self.backend == "postgres":
            apply_runtime_schema_migrations(self.db_config)
            self._schema_ready = True
            return
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS system_configs (
                    config_key TEXT PRIMARY KEY,
                    config_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    updated_by TEXT NULL
                )
                """
            )
            connection.commit()
        self._schema_ready = True

    def ensure_ready(self) -> None:
        if not self._schema_ready:
            self.init_schema()

    def get_config(self, *, config_key: str = "platform") -> Optional[Dict[str, Any]]:
        self.ensure_ready()
        placeholder = "%s" if self.backend == "postgres" else "?"
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(
                f"SELECT * FROM {self._table('system_configs')} WHERE config_key = {placeholder}",
                (config_key,),
            )
            row = self._fetch_one(cursor)
        if not row:
            return None
        return {
            "config_key": row["config_key"],
            "config": _json_loads(row.get("config_json")),
            "updated_at": _parse_datetime(row.get("updated_at")),
            "updated_by": row.get("updated_by"),
        }

    def save_config(
        self,
        *,
        payload: Dict[str, Any],
        updated_by: Optional[str],
        config_key: str = "platform",
    ) -> Dict[str, Any]:
        self.ensure_ready()
        updated_at = utc_now()
        serialized_payload = _json_dumps(payload)

        with self._connect() as connection:
            cursor = connection.cursor()
            if self.backend == "postgres":
                cursor.execute(
                    f"""
                    INSERT INTO "{self.schema}".system_configs (
                        config_key, config_json, updated_at, updated_by
                    ) VALUES (%s, %s::jsonb, %s, %s)
                    ON CONFLICT (config_key) DO UPDATE
                    SET config_json = EXCLUDED.config_json,
                        updated_at = EXCLUDED.updated_at,
                        updated_by = EXCLUDED.updated_by
                    """,
                    (config_key, serialized_payload, updated_at, updated_by),
                )
            else:
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO system_configs (
                        config_key, config_json, updated_at, updated_by
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (config_key, serialized_payload, updated_at.isoformat(), updated_by),
                )
            connection.commit()

        return {
            "config_key": config_key,
            "config": payload,
            "updated_at": updated_at,
            "updated_by": updated_by,
        }
