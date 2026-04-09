"""Persistent telemetry repository for collection and analysis workflows."""

from __future__ import annotations

import json
import math
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence

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


def _json_load_list(payload: Any) -> List[str]:
    if payload is None:
        return []
    if isinstance(payload, list):
        return [str(item) for item in payload]
    decoded = json.loads(payload)
    if isinstance(decoded, list):
        return [str(item) for item in decoded]
    if isinstance(decoded, dict):
        return [str(item) for item in decoded.values()]
    return []


class TelemetryRepository:
    """Stores raw telemetry and collection runtime state in the metadata database."""

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

    def _placeholder(self) -> str:
        return "%s" if self.backend == "postgres" else "?"

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
                CREATE TABLE IF NOT EXISTS telemetry_samples (
                    sample_id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    device_id TEXT NOT NULL,
                    tag TEXT NOT NULL,
                    recorded_at TEXT NOT NULL,
                    value REAL NOT NULL,
                    quality TEXT NOT NULL,
                    unit TEXT NULL,
                    source TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS collection_runtime_state (
                    tenant_id TEXT PRIMARY KEY,
                    is_running INTEGER NOT NULL DEFAULT 0,
                    device_ids_json TEXT NOT NULL DEFAULT '[]',
                    scan_interval INTEGER NOT NULL DEFAULT 10,
                    started_at TEXT NULL,
                    started_by TEXT NULL,
                    stopped_at TEXT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_telemetry_tenant_tag_time ON telemetry_samples(tenant_id, tag, recorded_at DESC)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_telemetry_tenant_device_time ON telemetry_samples(tenant_id, device_id, recorded_at DESC)"
            )
            connection.commit()
        self._schema_ready = True

    def ensure_ready(self) -> None:
        if not self._schema_ready:
            self.init_schema()

    def save_collection_state(
        self,
        *,
        tenant_id: str,
        is_running: bool,
        device_ids: Sequence[str],
        scan_interval: int,
        started_by: Optional[str],
        started_at: Optional[datetime] = None,
        stopped_at: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        self.ensure_ready()
        now = utc_now()
        payload = json.dumps(list(device_ids), ensure_ascii=False)

        with self._connect() as connection:
            cursor = connection.cursor()
            if self.backend == "postgres":
                cursor.execute(
                    f"""
                    INSERT INTO {self._table("collection_runtime_state")} (
                        tenant_id, is_running, device_ids_json, scan_interval,
                        started_at, started_by, stopped_at, updated_at
                    ) VALUES (%s, %s, %s::jsonb, %s, %s, %s, %s, %s)
                    ON CONFLICT (tenant_id) DO UPDATE SET
                        is_running = EXCLUDED.is_running,
                        device_ids_json = EXCLUDED.device_ids_json,
                        scan_interval = EXCLUDED.scan_interval,
                        started_at = EXCLUDED.started_at,
                        started_by = EXCLUDED.started_by,
                        stopped_at = EXCLUDED.stopped_at,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        tenant_id,
                        is_running,
                        payload,
                        int(scan_interval),
                        started_at,
                        started_by,
                        stopped_at,
                        now,
                    ),
                )
            else:
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO collection_runtime_state (
                        tenant_id, is_running, device_ids_json, scan_interval,
                        started_at, started_by, stopped_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        tenant_id,
                        1 if is_running else 0,
                        payload,
                        int(scan_interval),
                        started_at.isoformat() if started_at else None,
                        started_by,
                        stopped_at.isoformat() if stopped_at else None,
                        now.isoformat(),
                    ),
                )
            connection.commit()
        return self.get_collection_state(tenant_id=tenant_id)

    def get_collection_state(self, *, tenant_id: str) -> Dict[str, Any]:
        self.ensure_ready()
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(
                f"SELECT * FROM {self._table('collection_runtime_state')} WHERE tenant_id = {self._placeholder()}",
                (tenant_id,),
            )
            row = self._fetch_one(cursor)
        if not row:
            return {
                "tenant_id": tenant_id,
                "is_running": False,
                "device_ids": [],
                "scan_interval": 10,
                "started_at": None,
                "started_by": None,
                "stopped_at": None,
                "updated_at": None,
            }
        return {
            "tenant_id": row["tenant_id"],
            "is_running": bool(row["is_running"]),
            "device_ids": _json_load_list(row.get("device_ids_json")),
            "scan_interval": int(row.get("scan_interval") or 10),
            "started_at": _parse_datetime(row.get("started_at")),
            "started_by": row.get("started_by"),
            "stopped_at": _parse_datetime(row.get("stopped_at")),
            "updated_at": _parse_datetime(row.get("updated_at")),
        }

    def ingest_points(
        self,
        *,
        tenant_id: str,
        points: Iterable[Dict[str, Any]],
        source: str = "api",
    ) -> Dict[str, Any]:
        self.ensure_ready()
        point_list = list(points)
        if not point_list:
            return {"count": 0, "last_recorded_at": None}

        created_at = utc_now()
        last_recorded_at: Optional[datetime] = None
        with self._connect() as connection:
            cursor = connection.cursor()
            for point in point_list:
                recorded_at = point.get("timestamp") or point.get("recorded_at") or created_at
                if isinstance(recorded_at, str):
                    recorded_at = _parse_datetime(recorded_at)
                last_recorded_at = recorded_at if not last_recorded_at or recorded_at > last_recorded_at else last_recorded_at
                sample_id = point.get("sample_id") or f"SMP_{uuid.uuid4().hex[:16].upper()}"
                params = (
                    sample_id,
                    tenant_id,
                    str(point["device_id"]),
                    str(point["tag"]),
                    recorded_at if self.backend == "postgres" else recorded_at.isoformat(),
                    float(point["value"]),
                    str(point.get("quality", "good")),
                    point.get("unit"),
                    str(point.get("source") or source),
                    created_at if self.backend == "postgres" else created_at.isoformat(),
                )
                if self.backend == "postgres":
                    cursor.execute(
                        f"""
                        INSERT INTO {self._table("telemetry_samples")} (
                            sample_id, tenant_id, device_id, tag, recorded_at, value,
                            quality, unit, source, created_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        params,
                    )
                else:
                    cursor.execute(
                        """
                        INSERT INTO telemetry_samples (
                            sample_id, tenant_id, device_id, tag, recorded_at, value,
                            quality, unit, source, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        params,
                    )
            connection.commit()
        return {"count": len(point_list), "last_recorded_at": last_recorded_at}

    def query_points(
        self,
        *,
        tenant_id: str,
        tags: Sequence[str],
        start_time: datetime,
        end_time: datetime,
        device_ids: Optional[Sequence[str]] = None,
        limit_per_tag: int = 5000,
    ) -> Dict[str, List[Dict[str, Any]]]:
        self.ensure_ready()
        result: Dict[str, List[Dict[str, Any]]] = {tag: [] for tag in tags}
        if not tags:
            return result

        for tag in tags:
            filters = [
                f"tenant_id = {self._placeholder()}",
                f"tag = {self._placeholder()}",
                f"recorded_at >= {self._placeholder()}",
                f"recorded_at <= {self._placeholder()}",
            ]
            params: List[Any] = [
                tenant_id,
                tag,
                start_time.isoformat() if self.backend == "sqlite" else start_time,
                end_time.isoformat() if self.backend == "sqlite" else end_time,
            ]
            if device_ids:
                placeholders = ", ".join(self._placeholder() for _ in device_ids)
                filters.append(f"device_id IN ({placeholders})")
                params.extend(device_ids)
            sql = (
                f"SELECT device_id, tag, recorded_at, value, quality, unit, source "
                f"FROM {self._table('telemetry_samples')} "
                f"WHERE {' AND '.join(filters)} "
                f"ORDER BY recorded_at ASC "
                f"LIMIT {self._placeholder()}"
            )
            params.append(limit_per_tag)
            with self._connect() as connection:
                cursor = connection.cursor()
                cursor.execute(sql, tuple(params))
                rows = self._fetch_all(cursor)
            normalized = []
            for row in rows:
                normalized.append(
                    {
                        "device_id": row["device_id"],
                        "tag": row["tag"],
                        "timestamp": _parse_datetime(row["recorded_at"]),
                        "value": float(row["value"]),
                        "quality": row.get("quality") or "good",
                        "unit": row.get("unit"),
                        "source": row.get("source"),
                    }
                )
            result[tag] = normalized
        return result

    def get_latest_points(self, *, tenant_id: str, tags: Sequence[str]) -> Dict[str, Dict[str, Any]]:
        self.ensure_ready()
        result: Dict[str, Dict[str, Any]] = {}
        for tag in tags:
            with self._connect() as connection:
                cursor = connection.cursor()
                cursor.execute(
                    (
                        f"SELECT device_id, tag, recorded_at, value, quality, unit, source "
                        f"FROM {self._table('telemetry_samples')} "
                        f"WHERE tenant_id = {self._placeholder()} AND tag = {self._placeholder()} "
                        f"ORDER BY recorded_at DESC LIMIT 1"
                    ),
                    (tenant_id, tag),
                )
                row = self._fetch_one(cursor)
            if row:
                result[tag] = {
                    "device_id": row["device_id"],
                    "tag": row["tag"],
                    "timestamp": _parse_datetime(row["recorded_at"]),
                    "value": float(row["value"]),
                    "quality": row.get("quality") or "good",
                    "unit": row.get("unit"),
                    "source": row.get("source"),
                }
        return result

    def get_recent_points(self, *, tenant_id: str, tag: str, limit: int = 100) -> List[Dict[str, Any]]:
        self.ensure_ready()
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(
                (
                    f"SELECT device_id, tag, recorded_at, value, quality, unit, source "
                    f"FROM {self._table('telemetry_samples')} "
                    f"WHERE tenant_id = {self._placeholder()} AND tag = {self._placeholder()} "
                    f"ORDER BY recorded_at DESC LIMIT {self._placeholder()}"
                ),
                (tenant_id, tag, limit),
            )
            rows = self._fetch_all(cursor)
        points = [
            {
                "device_id": row["device_id"],
                "tag": row["tag"],
                "timestamp": _parse_datetime(row["recorded_at"]),
                "value": float(row["value"]),
                "quality": row.get("quality") or "good",
                "unit": row.get("unit"),
                "source": row.get("source"),
            }
            for row in rows
        ]
        points.reverse()
        return points

    def telemetry_summary(self, *, tenant_id: str, recent_window_seconds: int = 300) -> Dict[str, Any]:
        self.ensure_ready()
        latest_timestamp: Optional[datetime] = None
        total_recent_points = 0
        window_start = utc_now().timestamp() - recent_window_seconds
        totals_row: Dict[str, Any] = {"latest_recorded_at": None, "total_points": 0}
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(
                f"SELECT MAX(recorded_at) AS latest_recorded_at, COUNT(*) AS total_points FROM {self._table('telemetry_samples')} WHERE tenant_id = {self._placeholder()}",
                (tenant_id,),
            )
            totals_row = self._fetch_one(cursor) or {"latest_recorded_at": None, "total_points": 0}
            latest_timestamp = _parse_datetime(totals_row.get("latest_recorded_at"))
            cursor.execute(
                f"SELECT recorded_at FROM {self._table('telemetry_samples')} WHERE tenant_id = {self._placeholder()} ORDER BY recorded_at DESC LIMIT 5000",
                (tenant_id,),
            )
            recent_rows = self._fetch_all(cursor)
        for recent_row in recent_rows:
            recorded_at = _parse_datetime(recent_row.get("recorded_at"))
            if isinstance(recorded_at, datetime) and recorded_at.timestamp() >= window_start:
                total_recent_points += 1
        throughput = round(total_recent_points / max(recent_window_seconds, 1), 3)
        return {
            "last_data_time": latest_timestamp,
            "throughput": throughput,
            "total_points": int(totals_row.get("total_points", 0) or 0),
        }

    def compute_series_statistics(self, points: Sequence[Dict[str, Any]]) -> Dict[str, float]:
        values = [float(point["value"]) for point in points]
        if not values:
            raise ValueError("No telemetry points available")
        sorted_values = sorted(values)
        count = len(sorted_values)
        mean = sum(sorted_values) / count
        variance = sum((value - mean) ** 2 for value in sorted_values) / count
        std = math.sqrt(variance)
        middle = count // 2
        median = (sorted_values[middle - 1] + sorted_values[middle]) / 2 if count % 2 == 0 else sorted_values[middle]
        return {
            "count": float(count),
            "mean": mean,
            "std": std,
            "min": min(sorted_values),
            "max": max(sorted_values),
            "median": median,
            "p95": sorted_values[min(max(int(count * 0.95) - 1, 0), count - 1)],
            "p99": sorted_values[min(max(int(count * 0.99) - 1, 0), count - 1)],
        }
