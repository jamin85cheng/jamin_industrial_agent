"""Database-backed repository for exported diagnosis reports."""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

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


class ReportRepository:
    """Persists diagnosis report metadata to the configured metadata database."""

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

    def _placeholder(self) -> str:
        return "%s" if self.backend == "postgres" else "?"

    def _table(self, table_name: str) -> str:
        if self.backend == "postgres":
            return f'"{self.schema}".{table_name}'
        return table_name

    def _fetch_all(self, cursor) -> List[Dict[str, Any]]:
        rows = cursor.fetchall()
        if not rows:
            return []
        first = rows[0]
        if isinstance(first, dict):
            return list(rows)
        if hasattr(first, "keys"):
            return [dict(row) for row in rows]
        columns = [column[0] for column in (cursor.description or [])]
        return [dict(zip(columns, row)) for row in rows]

    def _fetch_one(self, cursor) -> Optional[Dict[str, Any]]:
        rows = self._fetch_all(cursor)
        return rows[0] if rows else None

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
                CREATE TABLE IF NOT EXISTS reports (
                    report_id TEXT PRIMARY KEY,
                    task_id TEXT NOT NULL,
                    diagnosis_id TEXT NOT NULL,
                    alert_id TEXT NULL,
                    tenant_id TEXT NOT NULL,
                    format TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    media_type TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    created_by TEXT NULL,
                    file_size_bytes INTEGER NULL,
                    metadata_json TEXT NOT NULL DEFAULT '{}'
                )
                """
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_reports_task_created_at ON reports(task_id, created_at DESC)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_reports_alert_created_at ON reports(alert_id, created_at DESC)"
            )
            connection.commit()
        self._schema_ready = True

    def ensure_ready(self) -> None:
        if not self._schema_ready:
            self.init_schema()

    def _normalize_report(self, row: Dict[str, Any]) -> Dict[str, Any]:
        normalized = dict(row)
        normalized["created_at"] = _parse_datetime(normalized.get("created_at"))
        normalized["metadata"] = _json_loads(normalized.pop("metadata_json", {}))
        normalized["download_url"] = f"/reports/{normalized['report_id']}/download"
        return normalized

    def create_report(
        self,
        *,
        report_id: str,
        task_id: str,
        diagnosis_id: str,
        alert_id: Optional[str],
        tenant_id: str,
        export_format: str,
        file_path: str,
        filename: str,
        media_type: str,
        created_by: Optional[str],
        metadata: Optional[Dict[str, Any]] = None,
        created_at: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        self.ensure_ready()
        report_created_at = created_at or utc_now()
        file_size_bytes = None
        try:
            file_size_bytes = Path(file_path).stat().st_size
        except OSError:
            file_size_bytes = None

        with self._connect() as connection:
            cursor = connection.cursor()
            if self.backend == "postgres":
                cursor.execute(
                    f"""
                    INSERT INTO "{self.schema}".reports (
                        report_id, task_id, diagnosis_id, alert_id, tenant_id, format,
                        file_path, filename, media_type, created_at, created_by,
                        file_size_bytes, metadata_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                    """,
                    (
                        report_id,
                        task_id,
                        diagnosis_id,
                        alert_id,
                        tenant_id,
                        export_format,
                        file_path,
                        filename,
                        media_type,
                        report_created_at,
                        created_by,
                        file_size_bytes,
                        _json_dumps(metadata or {}),
                    ),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO reports (
                        report_id, task_id, diagnosis_id, alert_id, tenant_id, format,
                        file_path, filename, media_type, created_at, created_by,
                        file_size_bytes, metadata_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        report_id,
                        task_id,
                        diagnosis_id,
                        alert_id,
                        tenant_id,
                        export_format,
                        file_path,
                        filename,
                        media_type,
                        report_created_at.isoformat(),
                        created_by,
                        file_size_bytes,
                        _json_dumps(metadata or {}),
                    ),
                )
            connection.commit()
        return self.get_report(report_id, tenant_id=tenant_id) or {
            "report_id": report_id,
            "task_id": task_id,
            "diagnosis_id": diagnosis_id,
            "alert_id": alert_id,
            "tenant_id": tenant_id,
            "format": export_format,
            "file_path": file_path,
            "filename": filename,
            "media_type": media_type,
            "created_at": report_created_at,
            "created_by": created_by,
            "file_size_bytes": file_size_bytes,
            "metadata": metadata or {},
            "download_url": f"/reports/{report_id}/download",
        }

    def get_report(self, report_id: str, *, tenant_id: str) -> Optional[Dict[str, Any]]:
        self.ensure_ready()
        sql = (
            f"SELECT * FROM {self._table('reports')} "
            f"WHERE report_id = {self._placeholder()} AND tenant_id = {self._placeholder()}"
        )
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(sql, (report_id, tenant_id))
            row = self._fetch_one(cursor)
        return self._normalize_report(row) if row else None

    def list_reports(
        self,
        *,
        tenant_id: str,
        task_id: Optional[str] = None,
        alert_id: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        self.ensure_ready()
        filters = [f"tenant_id = {self._placeholder()}"]
        params: List[Any] = [tenant_id]
        if task_id:
            filters.append(f"task_id = {self._placeholder()}")
            params.append(task_id)
        if alert_id:
            filters.append(f"alert_id = {self._placeholder()}")
            params.append(alert_id)
        sql = (
            f"SELECT * FROM {self._table('reports')} WHERE {' AND '.join(filters)} "
            f"ORDER BY created_at DESC LIMIT {self._placeholder()}"
        )
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(sql, tuple(params + [limit]))
            return [self._normalize_report(row) for row in self._fetch_all(cursor)]
