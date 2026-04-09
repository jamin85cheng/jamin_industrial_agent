"""Database-backed repository for alerts and alert rules."""

from __future__ import annotations

import json
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
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


class AlertRepository:
    """Persists alert rules and alert events to the configured metadata database."""

    def __init__(self, db_config: Optional[Dict[str, Any]] = None):
        self.db_config = db_config or load_config().get("database", {})
        self.adapter = build_runtime_database_adapter(self.db_config)
        self.backend = self.adapter.backend
        self.schema = str(self.db_config.get("postgres", {}).get("schema", "public"))

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

    def _sqlite_columns(self, connection, table_name: str) -> List[str]:
        cursor = connection.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        return [str(row[1]) for row in cursor.fetchall()]

    def _ensure_sqlite_column(self, connection, table_name: str, column_name: str, column_definition: str) -> None:
        if column_name in self._sqlite_columns(connection, table_name):
            return
        cursor = connection.cursor()
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_definition}")

    def _ensure_alert_extensions(self, connection) -> None:
        cursor = connection.cursor()
        if self.backend == "postgres":
            cursor.execute(
                f'ALTER TABLE "{self.schema}".alerts ADD COLUMN IF NOT EXISTS diagnosis_task_id TEXT NULL'
            )
            cursor.execute(
                f'ALTER TABLE "{self.schema}".alerts ADD COLUMN IF NOT EXISTS latest_report_id TEXT NULL'
            )
            cursor.execute(
                f'ALTER TABLE "{self.schema}".alerts ADD COLUMN IF NOT EXISTS last_action_by TEXT NULL'
            )
            cursor.execute(
                f'ALTER TABLE "{self.schema}".alerts ADD COLUMN IF NOT EXISTS last_action_at TIMESTAMPTZ NULL'
            )
            cursor.execute(
                f'ALTER TABLE "{self.schema}".alerts ADD COLUMN IF NOT EXISTS resolution_notes TEXT NULL'
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS "{self.schema}".alert_task_links (
                    link_id TEXT PRIMARY KEY,
                    alert_id TEXT NOT NULL,
                    task_id TEXT NOT NULL,
                    report_id TEXT NULL,
                    linked_at TIMESTAMPTZ NOT NULL,
                    linked_by TEXT NULL,
                    entrypoint TEXT NULL,
                    tenant_id TEXT NOT NULL
                )
                """
            )
            cursor.execute(
                f'CREATE INDEX IF NOT EXISTS "{self.schema}_alert_task_links_alert_idx" ON "{self.schema}".alert_task_links(alert_id, linked_at DESC)'
            )
            cursor.execute(
                f'CREATE INDEX IF NOT EXISTS "{self.schema}_alert_task_links_task_idx" ON "{self.schema}".alert_task_links(task_id)'
            )
            return

        self._ensure_sqlite_column(connection, "alerts", "diagnosis_task_id", "diagnosis_task_id TEXT NULL")
        self._ensure_sqlite_column(connection, "alerts", "latest_report_id", "latest_report_id TEXT NULL")
        self._ensure_sqlite_column(connection, "alerts", "last_action_by", "last_action_by TEXT NULL")
        self._ensure_sqlite_column(connection, "alerts", "last_action_at", "last_action_at TEXT NULL")
        self._ensure_sqlite_column(connection, "alerts", "resolution_notes", "resolution_notes TEXT NULL")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS alert_task_links (
                link_id TEXT PRIMARY KEY,
                alert_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                report_id TEXT NULL,
                linked_at TEXT NOT NULL,
                linked_by TEXT NULL,
                entrypoint TEXT NULL,
                tenant_id TEXT NOT NULL
            )
            """
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_alert_task_links_alert ON alert_task_links(alert_id, linked_at DESC)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_alert_task_links_task ON alert_task_links(task_id)"
        )

    def init_schema(self) -> None:
        if self.backend == "postgres":
            apply_runtime_schema_migrations(self.db_config)
            return
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS alert_rules (
                    rule_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    condition_json TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    message TEXT NOT NULL,
                    suppression_window_minutes INTEGER NOT NULL DEFAULT 30,
                    created_at TEXT NULL,
                    tenant_id TEXT NOT NULL
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS alerts (
                    id TEXT PRIMARY KEY,
                    rule_id TEXT NULL,
                    rule_name TEXT NULL,
                    severity TEXT NOT NULL,
                    message TEXT NOT NULL,
                    device_id TEXT NULL,
                    tag TEXT NULL,
                    value REAL NULL,
                    threshold REAL NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    acknowledged_by TEXT NULL,
                    acknowledged_at TEXT NULL,
                    acknowledge_comment TEXT NULL,
                    resolved_at TEXT NULL,
                    resolved_by TEXT NULL,
                    diagnosis_task_id TEXT NULL,
                    latest_report_id TEXT NULL,
                    last_action_by TEXT NULL,
                    last_action_at TEXT NULL,
                    resolution_notes TEXT NULL,
                    tenant_id TEXT NOT NULL
                )
                """
            )
            self._ensure_alert_extensions(connection)
            connection.commit()

    def seed_default_rules(self, tenant_id: str = "default") -> None:
        if self.list_rules(tenant_id=tenant_id):
            return
        now = utc_now()
        defaults = [
            {
                "rule_id": "RULE_001",
                "name": "高温告警",
                "enabled": True,
                "condition": {"type": "threshold", "tag": "temperature", "operator": ">", "value": 100},
                "severity": "critical",
                "message": "温度超过 100°C，需要立即处理。",
                "suppression_window_minutes": 30,
                "created_at": now,
                "tenant_id": tenant_id,
            },
            {
                "rule_id": "RULE_002",
                "name": "压力异常告警",
                "enabled": True,
                "condition": {"type": "threshold", "tag": "pressure", "operator": ">", "value": 10},
                "severity": "warning",
                "message": "压力超过 10 bar，请及时检查。",
                "suppression_window_minutes": 15,
                "created_at": now,
                "tenant_id": tenant_id,
            },
        ]
        for rule in defaults:
            self.create_rule(rule)

    def seed_demo_alerts(self, tenant_id: str = "default") -> None:
        if self.list_alerts(tenant_id=tenant_id, limit=1)["total"] > 0:
            return
        self.seed_default_rules(tenant_id=tenant_id)
        self.create_alert(
            rule_id="RULE_001",
            message="溶解氧浓度低于 2.0 mg/L，建议检查曝气量与风机运行状态。",
            severity="critical",
            device_id="DEV_AERATION_01",
            tag="DO",
            value=1.8,
            threshold=2.0,
            tenant_id=tenant_id,
        )
        self.create_alert(
            rule_id="RULE_002",
            message="鼓风机振动偏高，请关注轴承与对中状态。",
            severity="warning",
            device_id="DEV_BLOWER_01",
            tag="vibration",
            value=11.4,
            threshold=10.0,
            tenant_id=tenant_id,
        )

    def _normalize_rule(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "rule_id": row["rule_id"],
            "name": row["name"],
            "enabled": bool(row["enabled"]),
            "condition": _json_loads(row.get("condition_json")),
            "severity": row["severity"],
            "message": row["message"],
            "suppression_window_minutes": int(row.get("suppression_window_minutes") or 30),
            "created_at": _parse_datetime(row.get("created_at")),
            "tenant_id": row.get("tenant_id"),
        }

    def _normalize_alert(self, row: Dict[str, Any]) -> Dict[str, Any]:
        normalized = dict(row)
        for key in ("created_at", "acknowledged_at", "resolved_at", "last_action_at"):
            normalized[key] = _parse_datetime(normalized.get(key))
        if normalized.get("latest_report_id"):
            normalized["latest_report_download_url"] = f"/reports/{normalized['latest_report_id']}/download"
        return normalized

    def list_rules(self, *, tenant_id: str, enabled_only: bool = False) -> List[Dict[str, Any]]:
        placeholder = self._placeholder()
        sql = f"SELECT * FROM {self._table('alert_rules')} WHERE tenant_id = {placeholder}"
        params: List[Any] = [tenant_id]
        if enabled_only:
            sql += f" AND enabled = {placeholder}"
            params.append(True if self.backend == "postgres" else 1)
        sql += " ORDER BY created_at DESC, rule_id ASC"
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(sql, tuple(params))
            return [self._normalize_rule(row) for row in self._fetch_all(cursor)]

    def get_rule(self, rule_id: str, *, tenant_id: str) -> Optional[Dict[str, Any]]:
        sql = f"SELECT * FROM {self._table('alert_rules')} WHERE rule_id = {self._placeholder()} AND tenant_id = {self._placeholder()}"
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(sql, (rule_id, tenant_id))
            row = self._fetch_one(cursor)
        return self._normalize_rule(row) if row else None

    def create_rule(self, rule: Dict[str, Any]) -> Dict[str, Any]:
        tenant_id = rule.get("tenant_id", "default")
        created_at = rule.get("created_at") or utc_now()
        with self._connect() as connection:
            cursor = connection.cursor()
            if self.backend == "postgres":
                cursor.execute(
                    f"""
                    INSERT INTO "{self.schema}".alert_rules (
                        rule_id, name, enabled, condition_json, severity, message,
                        suppression_window_minutes, created_at, tenant_id
                    ) VALUES (%s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s)
                    """,
                    (
                        rule["rule_id"],
                        rule["name"],
                        bool(rule.get("enabled", True)),
                        _json_dumps(rule.get("condition") or {}),
                        rule["severity"],
                        rule["message"],
                        int(rule.get("suppression_window_minutes", 30)),
                        created_at,
                        tenant_id,
                    ),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO alert_rules (
                        rule_id, name, enabled, condition_json, severity, message,
                        suppression_window_minutes, created_at, tenant_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        rule["rule_id"],
                        rule["name"],
                        1 if rule.get("enabled", True) else 0,
                        _json_dumps(rule.get("condition") or {}),
                        rule["severity"],
                        rule["message"],
                        int(rule.get("suppression_window_minutes", 30)),
                        created_at.isoformat() if isinstance(created_at, datetime) else created_at,
                        tenant_id,
                    ),
                )
            connection.commit()
        return self.get_rule(rule["rule_id"], tenant_id=tenant_id) or dict(rule)

    def update_rule(self, rule_id: str, *, tenant_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        current = self.get_rule(rule_id, tenant_id=tenant_id)
        if not current:
            return None
        payload = dict(updates)
        payload["created_at"] = current.get("created_at") or utc_now()
        assignments: List[str] = []
        values: List[Any] = []
        for key, value in payload.items():
            column = "condition_json" if key == "condition" else key
            assignments.append(f"{column} = {self._placeholder()}")
            if key == "condition":
                values.append(_json_dumps(value or {}))
            elif self.backend == "sqlite" and isinstance(value, datetime):
                values.append(value.isoformat())
            elif self.backend == "sqlite" and key == "enabled":
                values.append(1 if value else 0)
            else:
                values.append(value)
        values.extend([rule_id, tenant_id])
        sql = (
            f"UPDATE {self._table('alert_rules')} SET {', '.join(assignments)} "
            f"WHERE rule_id = {self._placeholder()} AND tenant_id = {self._placeholder()}"
        )
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(sql, tuple(values))
            connection.commit()
        return self.get_rule(rule_id, tenant_id=tenant_id)

    def delete_rule(self, rule_id: str, *, tenant_id: str) -> bool:
        if not self.get_rule(rule_id, tenant_id=tenant_id):
            return False
        sql = f"DELETE FROM {self._table('alert_rules')} WHERE rule_id = {self._placeholder()} AND tenant_id = {self._placeholder()}"
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(sql, (rule_id, tenant_id))
            connection.commit()
        return True

    def create_alert(
        self,
        *,
        rule_id: Optional[str],
        message: str,
        severity: str,
        device_id: Optional[str] = None,
        tag: Optional[str] = None,
        value: Optional[float] = None,
        threshold: Optional[float] = None,
        tenant_id: str = "default",
    ) -> str:
        alert_id = f"ALT_{uuid.uuid4().hex[:12].upper()}"
        rule = self.get_rule(rule_id, tenant_id=tenant_id) if rule_id else None
        created_at = utc_now()
        with self._connect() as connection:
            cursor = connection.cursor()
            if self.backend == "postgres":
                cursor.execute(
                    f"""
                    INSERT INTO "{self.schema}".alerts (
                        id, rule_id, rule_name, severity, message, device_id, tag, value,
                        threshold, status, created_at, tenant_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        alert_id,
                        rule_id,
                        rule["name"] if rule else None,
                        severity,
                        message,
                        device_id,
                        tag,
                        value,
                        threshold,
                        "active",
                        created_at,
                        tenant_id,
                    ),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO alerts (
                        id, rule_id, rule_name, severity, message, device_id, tag, value,
                        threshold, status, created_at, tenant_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        alert_id,
                        rule_id,
                        rule["name"] if rule else None,
                        severity,
                        message,
                        device_id,
                        tag,
                        value,
                        threshold,
                        "active",
                        created_at.isoformat(),
                        tenant_id,
                    ),
                )
            connection.commit()
        return alert_id

    def list_alerts(
        self,
        *,
        tenant_id: str,
        status: Optional[str] = None,
        severity: Optional[str] = None,
        device_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100,
    ) -> Dict[str, Any]:
        filters = [f"tenant_id = {self._placeholder()}"]
        params: List[Any] = [tenant_id]
        if status:
            filters.append(f"status = {self._placeholder()}")
            params.append(status)
        if severity:
            filters.append(f"severity = {self._placeholder()}")
            params.append(severity)
        if device_id:
            filters.append(f"device_id = {self._placeholder()}")
            params.append(device_id)
        if start_time:
            filters.append(f"created_at >= {self._placeholder()}")
            params.append(start_time.isoformat() if self.backend == "sqlite" else start_time)
        if end_time:
            filters.append(f"created_at <= {self._placeholder()}")
            params.append(end_time.isoformat() if self.backend == "sqlite" else end_time)
        where_clause = " AND ".join(filters)
        sql = (
            f"SELECT * FROM {self._table('alerts')} WHERE {where_clause} "
            f"ORDER BY created_at DESC LIMIT {self._placeholder()}"
        )
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(sql, tuple(params + [limit]))
            alerts = [self._normalize_alert(row) for row in self._fetch_all(cursor)]
        return {"total": len(alerts), "alerts": alerts}

    def get_stats(self, *, tenant_id: str) -> Dict[str, int]:
        alerts = self.list_alerts(tenant_id=tenant_id, limit=1000)["alerts"]
        active = [alert for alert in alerts if alert.get("status") == "active"]
        acknowledged_today = 0
        today = utc_now().date()
        for alert in alerts:
            acknowledged_at = alert.get("acknowledged_at")
            if isinstance(acknowledged_at, datetime) and acknowledged_at.date() == today:
                acknowledged_today += 1
        return {
            "total_alerts": len(alerts),
            "active_alerts": len(active),
            "critical_alerts": sum(1 for alert in active if alert.get("severity") == "critical"),
            "warning_alerts": sum(1 for alert in active if alert.get("severity") == "warning"),
            "acknowledged_today": acknowledged_today,
        }

    def get_alert(self, alert_id: str, *, tenant_id: str) -> Optional[Dict[str, Any]]:
        sql = f"SELECT * FROM {self._table('alerts')} WHERE id = {self._placeholder()} AND tenant_id = {self._placeholder()}"
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(sql, (alert_id, tenant_id))
            row = self._fetch_one(cursor)
        return self._normalize_alert(row) if row else None

    def acknowledge_alert(
        self,
        alert_id: str,
        *,
        tenant_id: str,
        user_id: str,
        comment: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        if not self.get_alert(alert_id, tenant_id=tenant_id):
            return None
        acknowledged_at = utc_now()
        sql = (
            f"UPDATE {self._table('alerts')} SET "
            f"status = {self._placeholder()}, "
            f"acknowledged_by = {self._placeholder()}, "
            f"acknowledged_at = {self._placeholder()}, "
            f"acknowledge_comment = {self._placeholder()}, "
            f"last_action_by = {self._placeholder()}, "
            f"last_action_at = {self._placeholder()} "
            f"WHERE id = {self._placeholder()} AND tenant_id = {self._placeholder()}"
        )
        params = (
            "acknowledged",
            user_id,
            acknowledged_at.isoformat() if self.backend == "sqlite" else acknowledged_at,
            comment,
            user_id,
            acknowledged_at.isoformat() if self.backend == "sqlite" else acknowledged_at,
            alert_id,
            tenant_id,
        )
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(sql, params)
            connection.commit()
        return self.get_alert(alert_id, tenant_id=tenant_id)

    def resolve_alert(
        self,
        alert_id: str,
        *,
        tenant_id: str,
        user_id: str,
        resolution_notes: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        if not self.get_alert(alert_id, tenant_id=tenant_id):
            return None
        resolved_at = utc_now()
        sql = (
            f"UPDATE {self._table('alerts')} SET "
            f"status = {self._placeholder()}, "
            f"resolved_by = {self._placeholder()}, "
            f"resolved_at = {self._placeholder()}, "
            f"resolution_notes = {self._placeholder()}, "
            f"last_action_by = {self._placeholder()}, "
            f"last_action_at = {self._placeholder()} "
            f"WHERE id = {self._placeholder()} AND tenant_id = {self._placeholder()}"
        )
        params = (
            "resolved",
            user_id,
            resolved_at.isoformat() if self.backend == "sqlite" else resolved_at,
            resolution_notes,
            user_id,
            resolved_at.isoformat() if self.backend == "sqlite" else resolved_at,
            alert_id,
            tenant_id,
        )
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(sql, params)
            connection.commit()
        return self.get_alert(alert_id, tenant_id=tenant_id)

    def link_diagnosis_task(
        self,
        *,
        alert_id: str,
        task_id: str,
        tenant_id: str,
        user_id: Optional[str],
        entrypoint: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        if not self.get_alert(alert_id, tenant_id=tenant_id):
            return None
        linked_at = utc_now()
        link_id = f"ATL_{uuid.uuid4().hex[:12].upper()}"
        with self._connect() as connection:
            cursor = connection.cursor()
            if self.backend == "postgres":
                cursor.execute(
                    f"""
                    INSERT INTO "{self.schema}".alert_task_links (
                        link_id, alert_id, task_id, report_id, linked_at, linked_by, entrypoint, tenant_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (link_id, alert_id, task_id, None, linked_at, user_id, entrypoint, tenant_id),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO alert_task_links (
                        link_id, alert_id, task_id, report_id, linked_at, linked_by, entrypoint, tenant_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (link_id, alert_id, task_id, None, linked_at.isoformat(), user_id, entrypoint, tenant_id),
                )
            cursor.execute(
                (
                    f"UPDATE {self._table('alerts')} SET "
                    f"diagnosis_task_id = {self._placeholder()}, "
                    f"last_action_by = {self._placeholder()}, "
                    f"last_action_at = {self._placeholder()} "
                    f"WHERE id = {self._placeholder()} AND tenant_id = {self._placeholder()}"
                ),
                (
                    task_id,
                    user_id,
                    linked_at.isoformat() if self.backend == "sqlite" else linked_at,
                    alert_id,
                    tenant_id,
                ),
            )
            connection.commit()
        return self.get_alert(alert_id, tenant_id=tenant_id)

    def attach_report_to_alert(
        self,
        *,
        alert_id: str,
        report_id: str,
        tenant_id: str,
        user_id: Optional[str],
        task_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        if not self.get_alert(alert_id, tenant_id=tenant_id):
            return None
        linked_at = utc_now()
        with self._connect() as connection:
            cursor = connection.cursor()
            if task_id:
                cursor.execute(
                    (
                        f"UPDATE {self._table('alert_task_links')} SET "
                        f"report_id = {self._placeholder()} "
                        f"WHERE alert_id = {self._placeholder()} AND task_id = {self._placeholder()} AND tenant_id = {self._placeholder()}"
                    ),
                    (report_id, alert_id, task_id, tenant_id),
                )
            cursor.execute(
                (
                    f"UPDATE {self._table('alerts')} SET "
                    f"latest_report_id = {self._placeholder()}, "
                    f"last_action_by = {self._placeholder()}, "
                    f"last_action_at = {self._placeholder()} "
                    f"WHERE id = {self._placeholder()} AND tenant_id = {self._placeholder()}"
                ),
                (
                    report_id,
                    user_id,
                    linked_at.isoformat() if self.backend == "sqlite" else linked_at,
                    alert_id,
                    tenant_id,
                ),
            )
            connection.commit()
        return self.get_alert(alert_id, tenant_id=tenant_id)
