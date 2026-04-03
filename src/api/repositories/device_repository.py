"""Database-backed repository for device metadata."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Sequence

from src.utils.config import load_config
from src.utils.database_runtime import build_runtime_database_adapter


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_datetime(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return value
    return value


class DeviceRepository:
    """Persists device definitions and tags to the configured metadata database."""

    def __init__(self, db_config: Optional[Dict[str, Any]] = None):
        self.db_config = db_config or load_config().get("database", {})
        self.adapter = build_runtime_database_adapter(self.db_config)
        self.backend = self.adapter.backend
        self.schema = str(self.db_config.get("postgres", {}).get("schema", "public"))

    @property
    def target(self) -> str:
        return self.adapter.target

    @contextmanager
    def _connect(self):
        with self.adapter.connect() as connection:
            yield connection

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

    def _normalize_device_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        normalized = dict(row)
        for key in ("created_at", "updated_at", "last_seen"):
            normalized[key] = _parse_datetime(normalized.get(key))
        normalized["tag_count"] = int(normalized.get("tag_count") or 0)
        return normalized

    def _normalize_tag_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "name": row["name"],
            "address": row["address"],
            "data_type": row.get("data_type") or "float",
            "unit": row.get("unit"),
            "description": row.get("description"),
        }

    def _table(self, table_name: str) -> str:
        if self.backend == "postgres":
            return f'"{self.schema}".{table_name}'
        return table_name

    def _placeholder(self) -> str:
        return "%s" if self.backend == "postgres" else "?"

    def init_schema(self) -> None:
        with self._connect() as connection:
            cursor = connection.cursor()
            if self.backend == "postgres":
                cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{self.schema}"')
                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS "{self.schema}".devices (
                        id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        type TEXT NOT NULL,
                        host TEXT NOT NULL,
                        port INTEGER NOT NULL,
                        rack INTEGER,
                        slot INTEGER,
                        scan_interval INTEGER NOT NULL DEFAULT 10,
                        status TEXT NOT NULL DEFAULT 'offline',
                        enabled BOOLEAN NOT NULL DEFAULT TRUE,
                        last_seen TIMESTAMPTZ NULL,
                        created_at TIMESTAMPTZ NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL,
                        tenant_id TEXT NOT NULL,
                        created_by TEXT NULL,
                        updated_by TEXT NULL
                    )
                    """
                )
                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS "{self.schema}".device_tags (
                        device_id TEXT NOT NULL REFERENCES "{self.schema}".devices(id) ON DELETE CASCADE,
                        name TEXT NOT NULL,
                        address TEXT NOT NULL,
                        data_type TEXT NOT NULL DEFAULT 'float',
                        unit TEXT NULL,
                        description TEXT NULL,
                        PRIMARY KEY(device_id, name)
                    )
                    """
                )
            else:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS devices (
                        id TEXT PRIMARY KEY,
                        name TEXT NOT NULL,
                        type TEXT NOT NULL,
                        host TEXT NOT NULL,
                        port INTEGER NOT NULL,
                        rack INTEGER,
                        slot INTEGER,
                        scan_interval INTEGER NOT NULL DEFAULT 10,
                        status TEXT NOT NULL DEFAULT 'offline',
                        enabled INTEGER NOT NULL DEFAULT 1,
                        last_seen TEXT NULL,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        tenant_id TEXT NOT NULL,
                        created_by TEXT NULL,
                        updated_by TEXT NULL
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS device_tags (
                        device_id TEXT NOT NULL,
                        name TEXT NOT NULL,
                        address TEXT NOT NULL,
                        data_type TEXT NOT NULL DEFAULT 'float',
                        unit TEXT NULL,
                        description TEXT NULL,
                        PRIMARY KEY(device_id, name),
                        FOREIGN KEY(device_id) REFERENCES devices(id) ON DELETE CASCADE
                    )
                    """
                )
            connection.commit()

    def seed_demo_devices(self, tenant_id: str = "default") -> None:
        if self.list_devices(tenant_id=tenant_id, skip=0, limit=1)["total"] > 0:
            return

        now = utc_now()
        demo_devices = [
            {
                "id": "DEV_AERATION_01",
                "name": "1#曝气池",
                "type": "s7",
                "host": "192.168.1.100",
                "port": 102,
                "rack": 0,
                "slot": 1,
                "scan_interval": 10,
                "status": "online",
                "enabled": True,
                "last_seen": now,
                "created_at": now,
                "updated_at": now,
                "tenant_id": tenant_id,
                "created_by": "system",
                "updated_by": "system",
            },
            {
                "id": "DEV_AERATION_02",
                "name": "2#曝气池",
                "type": "s7",
                "host": "192.168.1.101",
                "port": 102,
                "rack": 0,
                "slot": 1,
                "scan_interval": 10,
                "status": "online",
                "enabled": True,
                "last_seen": now,
                "created_at": now,
                "updated_at": now,
                "tenant_id": tenant_id,
                "created_by": "system",
                "updated_by": "system",
            },
            {
                "id": "DEV_BLOWER_01",
                "name": "鼓风机",
                "type": "modbus",
                "host": "192.168.1.120",
                "port": 502,
                "rack": 0,
                "slot": 1,
                "scan_interval": 10,
                "status": "error",
                "enabled": True,
                "last_seen": now,
                "created_at": now,
                "updated_at": now,
                "tenant_id": tenant_id,
                "created_by": "system",
                "updated_by": "system",
            },
        ]
        demo_tags = {
            "DEV_AERATION_01": [
                {"name": "DO", "address": "DB1.DBW0", "data_type": "float", "unit": "mg/L"},
                {"name": "pH", "address": "DB1.DBW2", "data_type": "float", "unit": ""},
            ],
            "DEV_AERATION_02": [
                {"name": "DO", "address": "DB1.DBW4", "data_type": "float", "unit": "mg/L"},
                {"name": "pH", "address": "DB1.DBW6", "data_type": "float", "unit": ""},
            ],
            "DEV_BLOWER_01": [
                {"name": "current", "address": "40001", "data_type": "float", "unit": "A"},
                {"name": "vibration", "address": "40002", "data_type": "float", "unit": "mm/s"},
                {"name": "temperature", "address": "40003", "data_type": "float", "unit": "C"},
            ],
        }
        for device in demo_devices:
            self.create_device(device=device, tags=demo_tags.get(device["id"], []))

    def create_device(self, *, device: Dict[str, Any], tags: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
        with self._connect() as connection:
            cursor = connection.cursor()
            if self.backend == "postgres":
                cursor.execute(
                    f"""
                    INSERT INTO "{self.schema}".devices (
                        id, name, type, host, port, rack, slot, scan_interval,
                        status, enabled, last_seen, created_at, updated_at,
                        tenant_id, created_by, updated_by
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        device["id"],
                        device["name"],
                        device["type"],
                        device["host"],
                        int(device["port"]),
                        device.get("rack"),
                        device.get("slot"),
                        int(device.get("scan_interval", 10)),
                        device.get("status", "offline"),
                        bool(device.get("enabled", True)),
                        device.get("last_seen"),
                        device["created_at"],
                        device["updated_at"],
                        device.get("tenant_id", "default"),
                        device.get("created_by"),
                        device.get("updated_by"),
                    ),
                )
                for tag in tags:
                    cursor.execute(
                        f"""
                        INSERT INTO "{self.schema}".device_tags (
                            device_id, name, address, data_type, unit, description
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (
                            device["id"],
                            tag["name"],
                            tag["address"],
                            tag.get("data_type", "float"),
                            tag.get("unit"),
                            tag.get("description"),
                        ),
                    )
            else:
                cursor.execute(
                    """
                    INSERT INTO devices (
                        id, name, type, host, port, rack, slot, scan_interval,
                        status, enabled, last_seen, created_at, updated_at,
                        tenant_id, created_by, updated_by
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        device["id"],
                        device["name"],
                        device["type"],
                        device["host"],
                        int(device["port"]),
                        device.get("rack"),
                        device.get("slot"),
                        int(device.get("scan_interval", 10)),
                        device.get("status", "offline"),
                        1 if device.get("enabled", True) else 0,
                        device.get("last_seen").isoformat() if device.get("last_seen") else None,
                        device["created_at"].isoformat(),
                        device["updated_at"].isoformat(),
                        device.get("tenant_id", "default"),
                        device.get("created_by"),
                        device.get("updated_by"),
                    ),
                )
                for tag in tags:
                    cursor.execute(
                        """
                        INSERT INTO device_tags (
                            device_id, name, address, data_type, unit, description
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            device["id"],
                            tag["name"],
                            tag["address"],
                            tag.get("data_type", "float"),
                            tag.get("unit"),
                            tag.get("description"),
                        ),
                    )
            connection.commit()
        return self.get_device(device["id"], tenant_id=device.get("tenant_id", "default")) or dict(device)

    def list_devices(
        self,
        *,
        tenant_id: str,
        device_type: Optional[str] = None,
        status: Optional[str] = None,
        skip: int = 0,
        limit: int = 100,
    ) -> Dict[str, Any]:
        placeholder = self._placeholder()
        filters = [f"d.tenant_id = {placeholder}"]
        params: List[Any] = [tenant_id]
        if device_type:
            filters.append(f"d.type = {placeholder}")
            params.append(device_type)
        if status:
            filters.append(f"d.status = {placeholder}")
            params.append(status)
        where_clause = " AND ".join(filters)

        count_sql = f"SELECT COUNT(*) AS total FROM {self._table('devices')} d WHERE {where_clause}"
        data_sql = (
            f"SELECT d.id, d.name, d.type, d.host, d.port, d.status, d.last_seen, "
            f"d.created_at, d.updated_at, d.tenant_id, COUNT(t.name) AS tag_count "
            f"FROM {self._table('devices')} d "
            f"LEFT JOIN {self._table('device_tags')} t ON t.device_id = d.id "
            f"WHERE {where_clause} "
            f"GROUP BY d.id, d.name, d.type, d.host, d.port, d.status, d.last_seen, d.created_at, d.updated_at, d.tenant_id "
            f"ORDER BY d.updated_at DESC "
            f"LIMIT {placeholder} OFFSET {placeholder}"
        )

        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(count_sql, tuple(params))
            count_row = self._fetch_one(cursor) or {"total": 0}

            cursor = connection.cursor()
            cursor.execute(data_sql, tuple(params + [limit, skip]))
            devices = [self._normalize_device_row(row) for row in self._fetch_all(cursor)]

        return {"total": int(count_row["total"]), "devices": devices}

    def get_device(self, device_id: str, *, tenant_id: str) -> Optional[Dict[str, Any]]:
        placeholder = self._placeholder()
        sql = (
            f"SELECT d.id, d.name, d.type, d.host, d.port, d.status, d.last_seen, "
            f"d.created_at, d.updated_at, d.tenant_id, COUNT(t.name) AS tag_count "
            f"FROM {self._table('devices')} d "
            f"LEFT JOIN {self._table('device_tags')} t ON t.device_id = d.id "
            f"WHERE d.id = {placeholder} AND d.tenant_id = {placeholder} "
            f"GROUP BY d.id, d.name, d.type, d.host, d.port, d.status, d.last_seen, d.created_at, d.updated_at, d.tenant_id"
        )
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(sql, (device_id, tenant_id))
            row = self._fetch_one(cursor)
        return self._normalize_device_row(row) if row else None

    def list_tags(self, device_id: str, *, tenant_id: str) -> List[Dict[str, Any]]:
        if not self.get_device(device_id, tenant_id=tenant_id):
            return []
        sql = (
            f"SELECT name, address, data_type, unit, description "
            f"FROM {self._table('device_tags')} "
            f"WHERE device_id = {self._placeholder()} ORDER BY name"
        )
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(sql, (device_id,))
            return [self._normalize_tag_row(row) for row in self._fetch_all(cursor)]

    def update_device(
        self,
        device_id: str,
        *,
        tenant_id: str,
        updates: Dict[str, Any],
        updated_by: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        current = self.get_device(device_id, tenant_id=tenant_id)
        if not current:
            return None

        payload = dict(updates)
        payload["updated_at"] = utc_now()
        payload["updated_by"] = updated_by

        assignments: List[str] = []
        values: List[Any] = []
        for key, value in payload.items():
            assignments.append(f"{key} = {self._placeholder()}")
            if self.backend == "sqlite" and isinstance(value, datetime):
                values.append(value.isoformat())
            elif self.backend == "sqlite" and key == "enabled" and value is not None:
                values.append(1 if value else 0)
            else:
                values.append(value)

        values.extend([device_id, tenant_id])
        sql = (
            f"UPDATE {self._table('devices')} SET {', '.join(assignments)} "
            f"WHERE id = {self._placeholder()} AND tenant_id = {self._placeholder()}"
        )
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(sql, tuple(values))
            connection.commit()
        return self.get_device(device_id, tenant_id=tenant_id)

    def delete_device(self, device_id: str, *, tenant_id: str) -> bool:
        if not self.get_device(device_id, tenant_id=tenant_id):
            return False
        with self._connect() as connection:
            cursor = connection.cursor()
            if self.backend == "sqlite":
                cursor.execute("DELETE FROM device_tags WHERE device_id = ?", (device_id,))
                cursor.execute("DELETE FROM devices WHERE id = ? AND tenant_id = ?", (device_id, tenant_id))
            else:
                cursor.execute(
                    f'DELETE FROM "{self.schema}".devices WHERE id = %s AND tenant_id = %s',
                    (device_id, tenant_id),
                )
            connection.commit()
        return True

    def set_connection_state(
        self,
        device_id: str,
        *,
        tenant_id: str,
        status: str,
        updated_by: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        updates: Dict[str, Any] = {"status": status}
        if status == "online":
            updates["last_seen"] = utc_now()
        return self.update_device(device_id, tenant_id=tenant_id, updates=updates, updated_by=updated_by)

