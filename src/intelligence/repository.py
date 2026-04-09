"""Persistence for industrial intelligence runtime state."""

from __future__ import annotations

import json
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from src.intelligence.knowledge import rank_knowledge_cases
from src.intelligence.models import serialize_value
from src.utils.config import load_config
from src.utils.database_runtime import build_runtime_database_adapter
from src.utils.runtime_migrations import apply_runtime_schema_migrations
from src.utils.structured_logging import get_logger

logger = get_logger("intelligence.repository")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _json_dumps(payload: Any) -> str:
    return json.dumps(serialize_value(payload), ensure_ascii=False)


def _json_loads(payload: Any, default: Any) -> Any:
    if payload is None:
        return default
    if isinstance(payload, (dict, list)):
        return payload
    return json.loads(payload)


def _parse_datetime(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return value
    return value


class IntelligenceRepository:
    """Stores snapshots, patrol runs, labels, knowledge cases, and candidates."""

    def __init__(self, db_config: Optional[Dict[str, Any]] = None):
        self.db_config = db_config or load_config().get("database", {})
        self.adapter = build_runtime_database_adapter(self.db_config)
        self.backend = self.adapter.backend
        self.schema = str(self.db_config.get("postgres", {}).get("schema", "public"))
        self._schema_ready = False
        sqlite_path = (self.db_config.get("sqlite") or {}).get("path", "data/metadata.db")
        self._sqlite_fallback = build_runtime_database_adapter(
            {
                "sqlite": {"path": sqlite_path},
                "postgres": {"enabled": False},
            }
        )

    @contextmanager
    def _connect(self):
        try:
            with self.adapter.connect() as connection:
                yield connection
                return
        except Exception as exc:
            if self.backend != "postgres":
                raise
            logger.warning(f"Intelligence repository falling back to sqlite metadata store: {exc}")
            self.adapter = self._sqlite_fallback
            self.backend = "sqlite"
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
            try:
                apply_runtime_schema_migrations(self.db_config)
                self._schema_ready = True
                return
            except Exception as exc:
                logger.warning(f"Intelligence repository falling back to sqlite metadata store: {exc}")
                self.adapter = self._sqlite_fallback
                self.backend = "sqlite"
                self.schema = "public"
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS intelligence_snapshots (
                    asset_id TEXT PRIMARY KEY,
                    scene_type TEXT NOT NULL,
                    collected_at TEXT NOT NULL,
                    snapshot_json TEXT NOT NULL
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS intelligence_patrol_runs (
                    run_id TEXT PRIMARY KEY,
                    scene_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    risk_level TEXT NOT NULL,
                    risk_score REAL NOT NULL,
                    created_at TEXT NOT NULL,
                    triggered_by TEXT NOT NULL,
                    schedule_type TEXT NOT NULL,
                    result_json TEXT NOT NULL
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS intelligence_labels (
                    label_id TEXT PRIMARY KEY,
                    run_id TEXT NOT NULL,
                    asset_id TEXT NOT NULL,
                    scene_type TEXT NOT NULL,
                    label_status TEXT NOT NULL,
                    anomaly_type TEXT NULL,
                    root_cause TEXT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    review_json TEXT NOT NULL DEFAULT '{}'
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS intelligence_knowledge_cases (
                    case_id TEXT PRIMARY KEY,
                    asset_id TEXT NULL,
                    scene_type TEXT NOT NULL,
                    title TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    content TEXT NOT NULL,
                    tags_json TEXT NOT NULL,
                    root_cause TEXT NULL,
                    recommended_actions_json TEXT NOT NULL,
                    source_label_id TEXT NULL,
                    source_type TEXT NOT NULL,
                    usage_count INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS intelligence_learning_candidates (
                    candidate_id TEXT PRIMARY KEY,
                    candidate_type TEXT NOT NULL,
                    name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    score REAL NOT NULL,
                    rationale TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS intelligence_knowledge_feedback (
                    feedback_id TEXT PRIMARY KEY,
                    diagnosis_id TEXT NOT NULL,
                    tenant_id TEXT NOT NULL,
                    helpful INTEGER NOT NULL,
                    comment TEXT NULL,
                    reference_case_ids_json TEXT NOT NULL DEFAULT '[]',
                    created_by TEXT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS intelligence_knowledge_activity (
                    event_id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    actor_id TEXT NULL,
                    query TEXT NULL,
                    scene_type TEXT NULL,
                    resource_id TEXT NULL,
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL
                )
                """
            )
            connection.commit()
        self._schema_ready = True

    def ensure_ready(self) -> None:
        if not self._schema_ready:
            self.init_schema()

    def upsert_snapshot(self, snapshot: Dict[str, Any]) -> Dict[str, Any]:
        collected_at = snapshot["collected_at"]
        if isinstance(collected_at, datetime):
            collected_at = collected_at.isoformat()
        payload_json = _json_dumps(snapshot)
        with self._connect() as connection:
            cursor = connection.cursor()
            if self.backend == "postgres":
                cursor.execute(
                    f"""
                    INSERT INTO {self._table("intelligence_snapshots")} (
                        asset_id, scene_type, collected_at, snapshot_json
                    ) VALUES (%s, %s, %s, %s::jsonb)
                    ON CONFLICT (asset_id) DO UPDATE SET
                        scene_type = EXCLUDED.scene_type,
                        collected_at = EXCLUDED.collected_at,
                        snapshot_json = EXCLUDED.snapshot_json
                    """,
                    (snapshot["asset_id"], snapshot["scene_type"], collected_at, payload_json),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO intelligence_snapshots (
                        asset_id, scene_type, collected_at, snapshot_json
                    ) VALUES (?, ?, ?, ?)
                    ON CONFLICT(asset_id) DO UPDATE SET
                        scene_type = excluded.scene_type,
                        collected_at = excluded.collected_at,
                        snapshot_json = excluded.snapshot_json
                    """,
                    (snapshot["asset_id"], snapshot["scene_type"], collected_at, payload_json),
                )
            connection.commit()
        return self.get_snapshot(snapshot["asset_id"]) or snapshot

    def get_snapshot(self, asset_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(
                f"SELECT snapshot_json FROM {self._table('intelligence_snapshots')} WHERE asset_id = {self._placeholder()}",
                (asset_id,),
            )
            row = self._fetch_one(cursor)
        if not row:
            return None
        return _json_loads(row["snapshot_json"], {})

    def list_snapshots(self, asset_ids: Optional[Iterable[str]] = None) -> List[Dict[str, Any]]:
        asset_ids = list(asset_ids or [])
        with self._connect() as connection:
            cursor = connection.cursor()
            if asset_ids:
                placeholders = ", ".join(self._placeholder() for _ in asset_ids)
                cursor.execute(
                    f"""
                    SELECT snapshot_json FROM {self._table('intelligence_snapshots')}
                    WHERE asset_id IN ({placeholders})
                    ORDER BY collected_at DESC
                    """,
                    tuple(asset_ids),
                )
            else:
                cursor.execute(
                    f"SELECT snapshot_json FROM {self._table('intelligence_snapshots')} ORDER BY collected_at DESC"
                )
            rows = self._fetch_all(cursor)
        return [_json_loads(row["snapshot_json"], {}) for row in rows]

    def create_patrol_run(self, run_payload: Dict[str, Any]) -> Dict[str, Any]:
        created_at = run_payload["created_at"]
        if isinstance(created_at, datetime):
            created_at = created_at.isoformat()
        payload_json = _json_dumps(run_payload)
        with self._connect() as connection:
            cursor = connection.cursor()
            if self.backend == "postgres":
                cursor.execute(
                    f"""
                    INSERT INTO {self._table("intelligence_patrol_runs")} (
                        run_id, scene_type, status, risk_level, risk_score, created_at,
                        triggered_by, schedule_type, result_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                    """,
                    (
                        run_payload["run_id"],
                        run_payload["scene_type"],
                        run_payload["status"],
                        run_payload["risk_level"],
                        float(run_payload["risk_score"]),
                        created_at,
                        run_payload["triggered_by"],
                        run_payload["schedule_type"],
                        payload_json,
                    ),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO intelligence_patrol_runs (
                        run_id, scene_type, status, risk_level, risk_score, created_at,
                        triggered_by, schedule_type, result_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_payload["run_id"],
                        run_payload["scene_type"],
                        run_payload["status"],
                        run_payload["risk_level"],
                        float(run_payload["risk_score"]),
                        created_at,
                        run_payload["triggered_by"],
                        run_payload["schedule_type"],
                        payload_json,
                    ),
                )
            connection.commit()
        return self.get_patrol_run(run_payload["run_id"]) or run_payload

    def get_patrol_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(
                f"SELECT result_json FROM {self._table('intelligence_patrol_runs')} WHERE run_id = {self._placeholder()}",
                (run_id,),
            )
            row = self._fetch_one(cursor)
        if not row:
            return None
        return _json_loads(row["result_json"], {})

    def list_patrol_runs(self, limit: int = 20) -> List[Dict[str, Any]]:
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(
                f"""
                SELECT result_json FROM {self._table('intelligence_patrol_runs')}
                ORDER BY created_at DESC
                LIMIT {self._placeholder()}
                """,
                (max(int(limit), 1),),
            )
            rows = self._fetch_all(cursor)
        return [_json_loads(row["result_json"], {}) for row in rows]

    def create_label(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        created_at = payload["created_at"]
        updated_at = payload["updated_at"]
        if isinstance(created_at, datetime):
            created_at = created_at.isoformat()
        if isinstance(updated_at, datetime):
            updated_at = updated_at.isoformat()
        review_json = _json_dumps(payload.get("review", {}))
        with self._connect() as connection:
            cursor = connection.cursor()
            if self.backend == "postgres":
                cursor.execute(
                    f"""
                    INSERT INTO {self._table("intelligence_labels")} (
                        label_id, run_id, asset_id, scene_type, label_status,
                        anomaly_type, root_cause, created_at, updated_at, review_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                    """,
                    (
                        payload["label_id"],
                        payload["run_id"],
                        payload["asset_id"],
                        payload["scene_type"],
                        payload["status"],
                        payload.get("anomaly_type"),
                        payload.get("root_cause"),
                        created_at,
                        updated_at,
                        review_json,
                    ),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO intelligence_labels (
                        label_id, run_id, asset_id, scene_type, label_status,
                        anomaly_type, root_cause, created_at, updated_at, review_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        payload["label_id"],
                        payload["run_id"],
                        payload["asset_id"],
                        payload["scene_type"],
                        payload["status"],
                        payload.get("anomaly_type"),
                        payload.get("root_cause"),
                        created_at,
                        updated_at,
                        review_json,
                    ),
                )
            connection.commit()
        return self.get_label(payload["label_id"]) or payload

    def get_label(self, label_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(
                f"""
                SELECT label_id, run_id, asset_id, scene_type, label_status, anomaly_type,
                       root_cause, created_at, updated_at, review_json
                FROM {self._table('intelligence_labels')}
                WHERE label_id = {self._placeholder()}
                """,
                (label_id,),
            )
            row = self._fetch_one(cursor)
        return self._normalize_label(row)

    def list_labels(self, *, status: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        with self._connect() as connection:
            cursor = connection.cursor()
            query = (
                f"""
                SELECT label_id, run_id, asset_id, scene_type, label_status, anomaly_type,
                       root_cause, created_at, updated_at, review_json
                FROM {self._table('intelligence_labels')}
                """
            )
            params: List[Any] = []
            if status:
                query += f" WHERE label_status = {self._placeholder()}"
                params.append(status)
            query += f" ORDER BY updated_at DESC LIMIT {self._placeholder()}"
            params.append(max(int(limit), 1))
            cursor.execute(query, tuple(params))
            rows = self._fetch_all(cursor)
        return [item for item in (self._normalize_label(row) for row in rows) if item]

    def update_label(self, label_id: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        existing = self.get_label(label_id)
        if not existing:
            return None
        merged = dict(existing)
        merged.update(payload)
        updated_at = merged["updated_at"]
        if isinstance(updated_at, datetime):
            updated_at = updated_at.isoformat()
        review_json = _json_dumps(merged.get("review", {}))
        with self._connect() as connection:
            cursor = connection.cursor()
            if self.backend == "postgres":
                cursor.execute(
                    f"""
                    UPDATE {self._table("intelligence_labels")}
                    SET label_status = %s,
                        anomaly_type = %s,
                        root_cause = %s,
                        updated_at = %s,
                        review_json = %s::jsonb
                    WHERE label_id = %s
                    """,
                    (
                        merged["status"],
                        merged.get("anomaly_type"),
                        merged.get("root_cause"),
                        updated_at,
                        review_json,
                        label_id,
                    ),
                )
            else:
                cursor.execute(
                    """
                    UPDATE intelligence_labels
                    SET label_status = ?,
                        anomaly_type = ?,
                        root_cause = ?,
                        updated_at = ?,
                        review_json = ?
                    WHERE label_id = ?
                    """,
                    (
                        merged["status"],
                        merged.get("anomaly_type"),
                        merged.get("root_cause"),
                        updated_at,
                        review_json,
                        label_id,
                    ),
                )
            connection.commit()
        return self.get_label(label_id)

    def upsert_knowledge_case(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        created_at = payload["created_at"]
        updated_at = payload["updated_at"]
        if isinstance(created_at, datetime):
            created_at = created_at.isoformat()
        if isinstance(updated_at, datetime):
            updated_at = updated_at.isoformat()
        tags_json = _json_dumps(payload.get("tags", []))
        actions_json = _json_dumps(payload.get("recommended_actions", []))
        with self._connect() as connection:
            cursor = connection.cursor()
            if self.backend == "postgres":
                cursor.execute(
                    f"""
                    INSERT INTO {self._table("intelligence_knowledge_cases")} (
                        case_id, asset_id, scene_type, title, summary, content,
                        tags_json, root_cause, recommended_actions_json, source_label_id,
                        source_type, usage_count, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s::jsonb, %s, %s, %s, %s, %s)
                    ON CONFLICT (case_id) DO UPDATE SET
                        asset_id = EXCLUDED.asset_id,
                        scene_type = EXCLUDED.scene_type,
                        title = EXCLUDED.title,
                        summary = EXCLUDED.summary,
                        content = EXCLUDED.content,
                        tags_json = EXCLUDED.tags_json,
                        root_cause = EXCLUDED.root_cause,
                        recommended_actions_json = EXCLUDED.recommended_actions_json,
                        source_label_id = EXCLUDED.source_label_id,
                        source_type = EXCLUDED.source_type,
                        usage_count = EXCLUDED.usage_count,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        payload["case_id"],
                        payload.get("asset_id"),
                        payload["scene_type"],
                        payload["title"],
                        payload["summary"],
                        payload["content"],
                        tags_json,
                        payload.get("root_cause"),
                        actions_json,
                        payload.get("source_label_id"),
                        payload["source_type"],
                        int(payload.get("usage_count", 0) or 0),
                        created_at,
                        updated_at,
                    ),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO intelligence_knowledge_cases (
                        case_id, asset_id, scene_type, title, summary, content,
                        tags_json, root_cause, recommended_actions_json, source_label_id,
                        source_type, usage_count, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(case_id) DO UPDATE SET
                        asset_id = excluded.asset_id,
                        scene_type = excluded.scene_type,
                        title = excluded.title,
                        summary = excluded.summary,
                        content = excluded.content,
                        tags_json = excluded.tags_json,
                        root_cause = excluded.root_cause,
                        recommended_actions_json = excluded.recommended_actions_json,
                        source_label_id = excluded.source_label_id,
                        source_type = excluded.source_type,
                        usage_count = excluded.usage_count,
                        updated_at = excluded.updated_at
                    """,
                    (
                        payload["case_id"],
                        payload.get("asset_id"),
                        payload["scene_type"],
                        payload["title"],
                        payload["summary"],
                        payload["content"],
                        tags_json,
                        payload.get("root_cause"),
                        actions_json,
                        payload.get("source_label_id"),
                        payload["source_type"],
                        int(payload.get("usage_count", 0) or 0),
                        created_at,
                        updated_at,
                    ),
                )
            connection.commit()
        return self.get_knowledge_case(payload["case_id"]) or payload

    def get_knowledge_case(self, case_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(
                f"""
                SELECT case_id, asset_id, scene_type, title, summary, content, tags_json,
                       root_cause, recommended_actions_json, source_label_id, source_type,
                       usage_count, created_at, updated_at
                FROM {self._table('intelligence_knowledge_cases')}
                WHERE case_id = {self._placeholder()}
                """,
                (case_id,),
            )
            row = self._fetch_one(cursor)
        return self._normalize_knowledge_case(row)

    def list_knowledge_cases(self, *, scene_type: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        with self._connect() as connection:
            cursor = connection.cursor()
            query = (
                f"""
                SELECT case_id, asset_id, scene_type, title, summary, content, tags_json,
                       root_cause, recommended_actions_json, source_label_id, source_type,
                       usage_count, created_at, updated_at
                FROM {self._table('intelligence_knowledge_cases')}
                """
            )
            params: List[Any] = []
            if scene_type:
                query += f" WHERE scene_type = {self._placeholder()}"
                params.append(scene_type)
            query += f" ORDER BY updated_at DESC LIMIT {self._placeholder()}"
            params.append(max(int(limit), 1))
            cursor.execute(query, tuple(params))
            rows = self._fetch_all(cursor)
        return [item for item in (self._normalize_knowledge_case(row) for row in rows) if item]

    def search_knowledge_cases(self, query: str, *, scene_type: Optional[str] = None, top_k: int = 4) -> List[Dict[str, Any]]:
        return rank_knowledge_cases(
            self.list_knowledge_cases(scene_type=scene_type, limit=200),
            query,
            scene_type=scene_type,
            top_k=top_k,
        )

    def increment_knowledge_case_usage(self, case_id: str) -> Optional[Dict[str, Any]]:
        case = self.get_knowledge_case(case_id)
        if not case:
            return None
        case["usage_count"] = int(case.get("usage_count", 0) or 0) + 1
        case["updated_at"] = utc_now()
        return self.upsert_knowledge_case(case)

    def record_knowledge_activity(
        self,
        *,
        tenant_id: str,
        event_type: str,
        actor_id: Optional[str] = None,
        query: Optional[str] = None,
        scene_type: Optional[str] = None,
        resource_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        created_at: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        event_created_at = created_at or utc_now()
        metadata_json = _json_dumps(metadata or {})
        event_id = f"KN_EVT_{uuid.uuid4().hex[:12].upper()}"
        with self._connect() as connection:
            cursor = connection.cursor()
            if self.backend == "postgres":
                cursor.execute(
                    f"""
                    INSERT INTO {self._table("intelligence_knowledge_activity")} (
                        event_id, tenant_id, event_type, actor_id, query, scene_type,
                        resource_id, metadata_json, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                    """,
                    (
                        event_id,
                        tenant_id,
                        event_type,
                        actor_id,
                        query,
                        scene_type,
                        resource_id,
                        metadata_json,
                        event_created_at,
                    ),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO intelligence_knowledge_activity (
                        event_id, tenant_id, event_type, actor_id, query, scene_type,
                        resource_id, metadata_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event_id,
                        tenant_id,
                        event_type,
                        actor_id,
                        query,
                        scene_type,
                        resource_id,
                        metadata_json,
                        event_created_at.isoformat(),
                    ),
                )
            connection.commit()
        return {
            "event_id": event_id,
            "tenant_id": tenant_id,
            "event_type": event_type,
            "actor_id": actor_id,
            "query": query,
            "scene_type": scene_type,
            "resource_id": resource_id,
            "metadata": metadata or {},
            "created_at": event_created_at,
        }

    def list_knowledge_activity(
        self,
        *,
        tenant_id: Optional[str] = None,
        event_type: Optional[str] = None,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        with self._connect() as connection:
            cursor = connection.cursor()
            query = (
                f"""
                SELECT event_id, tenant_id, event_type, actor_id, query, scene_type, resource_id,
                       metadata_json, created_at
                FROM {self._table('intelligence_knowledge_activity')}
                """
            )
            params: List[Any] = []
            filters: List[str] = []
            if tenant_id:
                filters.append(f"tenant_id = {self._placeholder()}")
                params.append(tenant_id)
            if event_type:
                filters.append(f"event_type = {self._placeholder()}")
                params.append(event_type)
            if filters:
                query += " WHERE " + " AND ".join(filters)
            query += f" ORDER BY created_at DESC LIMIT {self._placeholder()}"
            params.append(max(int(limit), 1))
            cursor.execute(query, tuple(params))
            rows = self._fetch_all(cursor)
        return [item for item in (self._normalize_knowledge_activity(row) for row in rows) if item]

    def create_knowledge_feedback(
        self,
        *,
        diagnosis_id: str,
        tenant_id: str,
        helpful: bool,
        created_by: Optional[str],
        comment: Optional[str] = None,
        reference_case_ids: Optional[List[str]] = None,
        created_at: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        feedback_created_at = created_at or utc_now()
        feedback_id = f"KN_FB_{uuid.uuid4().hex[:12].upper()}"
        reference_case_ids_json = _json_dumps(reference_case_ids or [])
        with self._connect() as connection:
            cursor = connection.cursor()
            if self.backend == "postgres":
                cursor.execute(
                    f"""
                    INSERT INTO {self._table("intelligence_knowledge_feedback")} (
                        feedback_id, diagnosis_id, tenant_id, helpful, comment,
                        reference_case_ids_json, created_by, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s, %s)
                    """,
                    (
                        feedback_id,
                        diagnosis_id,
                        tenant_id,
                        bool(helpful),
                        comment,
                        reference_case_ids_json,
                        created_by,
                        feedback_created_at,
                    ),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO intelligence_knowledge_feedback (
                        feedback_id, diagnosis_id, tenant_id, helpful, comment,
                        reference_case_ids_json, created_by, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        feedback_id,
                        diagnosis_id,
                        tenant_id,
                        1 if helpful else 0,
                        comment,
                        reference_case_ids_json,
                        created_by,
                        feedback_created_at.isoformat(),
                    ),
                )
            connection.commit()
        return {
            "feedback_id": feedback_id,
            "diagnosis_id": diagnosis_id,
            "tenant_id": tenant_id,
            "helpful": bool(helpful),
            "comment": comment,
            "reference_case_ids": reference_case_ids or [],
            "created_by": created_by,
            "created_at": feedback_created_at,
        }

    def list_knowledge_feedback(
        self,
        *,
        tenant_id: Optional[str] = None,
        diagnosis_id: Optional[str] = None,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        with self._connect() as connection:
            cursor = connection.cursor()
            query = (
                f"""
                SELECT feedback_id, diagnosis_id, tenant_id, helpful, comment, reference_case_ids_json,
                       created_by, created_at
                FROM {self._table('intelligence_knowledge_feedback')}
                """
            )
            params: List[Any] = []
            filters: List[str] = []
            if tenant_id:
                filters.append(f"tenant_id = {self._placeholder()}")
                params.append(tenant_id)
            if diagnosis_id:
                filters.append(f"diagnosis_id = {self._placeholder()}")
                params.append(diagnosis_id)
            if filters:
                query += " WHERE " + " AND ".join(filters)
            query += f" ORDER BY created_at DESC LIMIT {self._placeholder()}"
            params.append(max(int(limit), 1))
            cursor.execute(query, tuple(params))
            rows = self._fetch_all(cursor)
        return [item for item in (self._normalize_knowledge_feedback(row) for row in rows) if item]

    def get_knowledge_statistics(self, *, tenant_id: Optional[str] = None) -> Dict[str, Any]:
        cases = self.list_knowledge_cases(limit=500)
        activities = self.list_knowledge_activity(tenant_id=tenant_id, limit=1000)
        feedback_items = self.list_knowledge_feedback(tenant_id=tenant_id, limit=1000)
        today = utc_now().date()

        search_count_today = sum(
            1
            for item in activities
            if item["event_type"] == "search" and getattr(item.get("created_at"), "date", lambda: None)() == today
        )
        diagnosis_count_today = sum(
            1
            for item in activities
            if item["event_type"] == "diagnose" and getattr(item.get("created_at"), "date", lambda: None)() == today
        )
        feedback_count_today = sum(
            1 for item in feedback_items if getattr(item.get("created_at"), "date", lambda: None)() == today
        )
        helpful_feedback_count = sum(1 for item in feedback_items if item["helpful"])
        total_feedback = len(feedback_items)
        accuracy_rate = round(helpful_feedback_count / total_feedback, 3) if total_feedback else None

        return {
            "total_documents": len(cases),
            "categories": len({str(item.get("scene_type") or "").strip() for item in cases if str(item.get("scene_type") or "").strip()}),
            "total_tags": len(
                {
                    str(tag).strip()
                    for item in cases
                    for tag in (item.get("tags") or [])
                    if str(tag).strip()
                }
            ),
            "search_count_today": search_count_today,
            "diagnosis_count_today": diagnosis_count_today,
            "feedback_count_today": feedback_count_today,
            "helpful_feedback_count": helpful_feedback_count,
            "total_feedback": total_feedback,
            "accuracy_rate": accuracy_rate,
        }

    def create_learning_candidate(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        created_at = payload["created_at"]
        updated_at = payload["updated_at"]
        if isinstance(created_at, datetime):
            created_at = created_at.isoformat()
        if isinstance(updated_at, datetime):
            updated_at = updated_at.isoformat()
        payload_json = _json_dumps(payload.get("payload", {}))
        with self._connect() as connection:
            cursor = connection.cursor()
            if self.backend == "postgres":
                cursor.execute(
                    f"""
                    INSERT INTO {self._table("intelligence_learning_candidates")} (
                        candidate_id, candidate_type, name, status, score, rationale,
                        payload_json, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
                    ON CONFLICT (candidate_id) DO UPDATE SET
                        candidate_type = EXCLUDED.candidate_type,
                        name = EXCLUDED.name,
                        status = EXCLUDED.status,
                        score = EXCLUDED.score,
                        rationale = EXCLUDED.rationale,
                        payload_json = EXCLUDED.payload_json,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        payload["candidate_id"],
                        payload["candidate_type"],
                        payload["name"],
                        payload["status"],
                        float(payload["score"]),
                        payload["rationale"],
                        payload_json,
                        created_at,
                        updated_at,
                    ),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO intelligence_learning_candidates (
                        candidate_id, candidate_type, name, status, score, rationale,
                        payload_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(candidate_id) DO UPDATE SET
                        candidate_type = excluded.candidate_type,
                        name = excluded.name,
                        status = excluded.status,
                        score = excluded.score,
                        rationale = excluded.rationale,
                        payload_json = excluded.payload_json,
                        updated_at = excluded.updated_at
                    """,
                    (
                        payload["candidate_id"],
                        payload["candidate_type"],
                        payload["name"],
                        payload["status"],
                        float(payload["score"]),
                        payload["rationale"],
                        payload_json,
                        created_at,
                        updated_at,
                    ),
                )
            connection.commit()
        return self.get_learning_candidate(payload["candidate_id"]) or payload

    def get_learning_candidate(self, candidate_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as connection:
            cursor = connection.cursor()
            cursor.execute(
                f"""
                SELECT candidate_id, candidate_type, name, status, score, rationale,
                       payload_json, created_at, updated_at
                FROM {self._table('intelligence_learning_candidates')}
                WHERE candidate_id = {self._placeholder()}
                """,
                (candidate_id,),
            )
            row = self._fetch_one(cursor)
        return self._normalize_candidate(row)

    def list_learning_candidates(self, *, candidate_type: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        with self._connect() as connection:
            cursor = connection.cursor()
            query = (
                f"""
                SELECT candidate_id, candidate_type, name, status, score, rationale,
                       payload_json, created_at, updated_at
                FROM {self._table('intelligence_learning_candidates')}
                """
            )
            params: List[Any] = []
            if candidate_type:
                query += f" WHERE candidate_type = {self._placeholder()}"
                params.append(candidate_type)
            query += f" ORDER BY updated_at DESC LIMIT {self._placeholder()}"
            params.append(max(int(limit), 1))
            cursor.execute(query, tuple(params))
            rows = self._fetch_all(cursor)
        return [item for item in (self._normalize_candidate(row) for row in rows) if item]

    def _normalize_label(self, row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not row:
            return None
        return {
            "label_id": row["label_id"],
            "run_id": row["run_id"],
            "asset_id": row["asset_id"],
            "scene_type": row["scene_type"],
            "status": row["label_status"],
            "anomaly_type": row.get("anomaly_type"),
            "root_cause": row.get("root_cause"),
            "created_at": _parse_datetime(row.get("created_at")),
            "updated_at": _parse_datetime(row.get("updated_at")),
            "review": _json_loads(row.get("review_json"), {}),
        }

    def _normalize_knowledge_case(self, row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not row:
            return None
        return {
            "case_id": row["case_id"],
            "asset_id": row.get("asset_id"),
            "scene_type": row["scene_type"],
            "title": row["title"],
            "summary": row["summary"],
            "content": row["content"],
            "tags": _json_loads(row.get("tags_json"), []),
            "root_cause": row.get("root_cause"),
            "recommended_actions": _json_loads(row.get("recommended_actions_json"), []),
            "source_label_id": row.get("source_label_id"),
            "source_type": row["source_type"],
            "usage_count": int(row.get("usage_count") or 0),
            "created_at": _parse_datetime(row.get("created_at")),
            "updated_at": _parse_datetime(row.get("updated_at")),
        }

    def _normalize_candidate(self, row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not row:
            return None
        return {
            "candidate_id": row["candidate_id"],
            "candidate_type": row["candidate_type"],
            "name": row["name"],
            "status": row["status"],
            "score": float(row.get("score") or 0.0),
            "rationale": row["rationale"],
            "payload": _json_loads(row.get("payload_json"), {}),
            "created_at": _parse_datetime(row.get("created_at")),
            "updated_at": _parse_datetime(row.get("updated_at")),
        }

    def _normalize_knowledge_feedback(self, row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not row:
            return None
        return {
            "feedback_id": row["feedback_id"],
            "diagnosis_id": row["diagnosis_id"],
            "tenant_id": row["tenant_id"],
            "helpful": bool(row.get("helpful")),
            "comment": row.get("comment"),
            "reference_case_ids": _json_loads(row.get("reference_case_ids_json"), []),
            "created_by": row.get("created_by"),
            "created_at": _parse_datetime(row.get("created_at")),
        }

    def _normalize_knowledge_activity(self, row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not row:
            return None
        return {
            "event_id": row["event_id"],
            "tenant_id": row["tenant_id"],
            "event_type": row["event_type"],
            "actor_id": row.get("actor_id"),
            "query": row.get("query"),
            "scene_type": row.get("scene_type"),
            "resource_id": row.get("resource_id"),
            "metadata": _json_loads(row.get("metadata_json"), {}),
            "created_at": _parse_datetime(row.get("created_at")),
        }
