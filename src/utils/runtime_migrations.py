"""Runtime schema migrations for metadata-backed repositories.

This module introduces a Postgres-first migration path for the metadata
database while keeping SQLite as a development and test fallback.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Dict, Iterable, List, Optional, Sequence

from src.utils.config import load_config
from src.utils.database_runtime import build_runtime_database_adapter
from src.utils.structured_logging import get_logger

logger = get_logger("runtime_migrations")

POSTGRES_RUNTIME_TABLES: Sequence[str] = (
    "auth_roles",
    "tenants",
    "auth_role_permissions",
    "auth_users",
    "auth_user_roles",
    "auth_refresh_tokens",
    "devices",
    "device_tags",
    "alert_rules",
    "alerts",
    "alert_task_links",
    "reports",
    "system_configs",
    "telemetry_samples",
    "collection_runtime_state",
    "tracked_tasks",
    "intelligence_snapshots",
    "intelligence_patrol_runs",
    "intelligence_labels",
    "intelligence_knowledge_cases",
    "intelligence_learning_candidates",
    "intelligence_knowledge_feedback",
    "intelligence_knowledge_activity",
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class RuntimeMigration:
    version: str
    name: str
    postgres_statements: Sequence[str]
    sqlite_statements: Sequence[str]


RUNTIME_MIGRATIONS: Sequence[RuntimeMigration] = (
    RuntimeMigration(
        version="001",
        name="auth_foundation",
        postgres_statements=(
            """
            CREATE TABLE IF NOT EXISTS auth_roles (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                is_system BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS tenants (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TIMESTAMPTZ NOT NULL,
                settings_json JSONB NOT NULL DEFAULT '{}'::jsonb
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS auth_role_permissions (
                role_id TEXT NOT NULL,
                permission TEXT NOT NULL,
                PRIMARY KEY (role_id, permission),
                FOREIGN KEY (role_id) REFERENCES auth_roles(id) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS auth_users (
                user_id TEXT PRIMARY KEY,
                username TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                roles_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                permissions_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                tenant_id TEXT NOT NULL,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                is_demo BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                last_login_at TIMESTAMPTZ NULL,
                FOREIGN KEY (tenant_id) REFERENCES tenants(id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS auth_user_roles (
                user_id TEXT NOT NULL,
                role_id TEXT NOT NULL,
                PRIMARY KEY (user_id, role_id),
                FOREIGN KEY (user_id) REFERENCES auth_users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (role_id) REFERENCES auth_roles(id) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS auth_refresh_tokens (
                token_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                expires_at TIMESTAMPTZ NOT NULL,
                last_used_at TIMESTAMPTZ NULL,
                revoked_at TIMESTAMPTZ NULL,
                replaced_by_token_id TEXT NULL,
                FOREIGN KEY (user_id) REFERENCES auth_users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (tenant_id) REFERENCES tenants(id)
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_auth_users_tenant ON auth_users(tenant_id, username)",
            "CREATE INDEX IF NOT EXISTS idx_auth_refresh_tokens_user ON auth_refresh_tokens(user_id, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_auth_refresh_tokens_tenant ON auth_refresh_tokens(tenant_id, revoked_at, expires_at)",
        ),
        sqlite_statements=(
            """
            CREATE TABLE IF NOT EXISTS auth_roles (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT '',
                is_system INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS tenants (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT NOT NULL,
                settings_json TEXT NOT NULL DEFAULT '{}'
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS auth_role_permissions (
                role_id TEXT NOT NULL,
                permission TEXT NOT NULL,
                PRIMARY KEY (role_id, permission),
                FOREIGN KEY (role_id) REFERENCES auth_roles(id) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS auth_users (
                user_id TEXT PRIMARY KEY,
                username TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                roles_json TEXT NOT NULL DEFAULT '[]',
                permissions_json TEXT NOT NULL DEFAULT '[]',
                tenant_id TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                is_demo INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_login_at TEXT NULL,
                FOREIGN KEY (tenant_id) REFERENCES tenants(id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS auth_user_roles (
                user_id TEXT NOT NULL,
                role_id TEXT NOT NULL,
                PRIMARY KEY (user_id, role_id),
                FOREIGN KEY (user_id) REFERENCES auth_users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (role_id) REFERENCES auth_roles(id) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS auth_refresh_tokens (
                token_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                last_used_at TEXT NULL,
                revoked_at TEXT NULL,
                replaced_by_token_id TEXT NULL,
                FOREIGN KEY (user_id) REFERENCES auth_users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (tenant_id) REFERENCES tenants(id)
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_auth_users_tenant ON auth_users(tenant_id, username)",
            "CREATE INDEX IF NOT EXISTS idx_auth_refresh_tokens_user ON auth_refresh_tokens(user_id, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_auth_refresh_tokens_tenant ON auth_refresh_tokens(tenant_id, revoked_at, expires_at)",
        ),
    ),
    RuntimeMigration(
        version="002",
        name="devices_catalog",
        postgres_statements=(
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
                enabled BOOLEAN NOT NULL DEFAULT TRUE,
                last_seen TIMESTAMPTZ NULL,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                tenant_id TEXT NOT NULL,
                created_by TEXT NULL,
                updated_by TEXT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS device_tags (
                device_id TEXT NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
                name TEXT NOT NULL,
                address TEXT NOT NULL,
                data_type TEXT NOT NULL DEFAULT 'float',
                unit TEXT NULL,
                description TEXT NULL,
                PRIMARY KEY (device_id, name)
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_devices_tenant_status ON devices(tenant_id, status, updated_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_device_tags_device ON device_tags(device_id, name)",
        ),
        sqlite_statements=(
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
            """,
            """
            CREATE TABLE IF NOT EXISTS device_tags (
                device_id TEXT NOT NULL,
                name TEXT NOT NULL,
                address TEXT NOT NULL,
                data_type TEXT NOT NULL DEFAULT 'float',
                unit TEXT NULL,
                description TEXT NULL,
                PRIMARY KEY (device_id, name),
                FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_devices_tenant_status ON devices(tenant_id, status, updated_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_device_tags_device ON device_tags(device_id, name)",
        ),
    ),
    RuntimeMigration(
        version="003",
        name="alerts_and_links",
        postgres_statements=(
            """
            CREATE TABLE IF NOT EXISTS alert_rules (
                rule_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                enabled BOOLEAN NOT NULL DEFAULT TRUE,
                condition_json JSONB NOT NULL,
                severity TEXT NOT NULL,
                message TEXT NOT NULL,
                suppression_window_minutes INTEGER NOT NULL DEFAULT 30,
                created_at TIMESTAMPTZ NULL,
                tenant_id TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS alerts (
                id TEXT PRIMARY KEY,
                rule_id TEXT NULL,
                rule_name TEXT NULL,
                severity TEXT NOT NULL,
                message TEXT NOT NULL,
                device_id TEXT NULL,
                tag TEXT NULL,
                value DOUBLE PRECISION NULL,
                threshold DOUBLE PRECISION NULL,
                status TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                acknowledged_by TEXT NULL,
                acknowledged_at TIMESTAMPTZ NULL,
                acknowledge_comment TEXT NULL,
                resolved_at TIMESTAMPTZ NULL,
                resolved_by TEXT NULL,
                diagnosis_task_id TEXT NULL,
                latest_report_id TEXT NULL,
                last_action_by TEXT NULL,
                last_action_at TIMESTAMPTZ NULL,
                resolution_notes TEXT NULL,
                tenant_id TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS alert_task_links (
                link_id TEXT PRIMARY KEY,
                alert_id TEXT NOT NULL,
                task_id TEXT NOT NULL,
                report_id TEXT NULL,
                linked_at TIMESTAMPTZ NOT NULL,
                linked_by TEXT NULL,
                entrypoint TEXT NULL,
                tenant_id TEXT NOT NULL
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_alert_rules_tenant ON alert_rules(tenant_id, enabled, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_alerts_tenant_status ON alerts(tenant_id, status, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_alerts_device ON alerts(device_id, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_alert_task_links_alert ON alert_task_links(alert_id, linked_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_alert_task_links_task ON alert_task_links(task_id)",
        ),
        sqlite_statements=(
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
            """,
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
            """,
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
            """,
            "CREATE INDEX IF NOT EXISTS idx_alert_rules_tenant ON alert_rules(tenant_id, enabled, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_alerts_tenant_status ON alerts(tenant_id, status, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_alerts_device ON alerts(device_id, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_alert_task_links_alert ON alert_task_links(alert_id, linked_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_alert_task_links_task ON alert_task_links(task_id)",
        ),
    ),
    RuntimeMigration(
        version="004",
        name="reports_registry",
        postgres_statements=(
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
                created_at TIMESTAMPTZ NOT NULL,
                created_by TEXT NULL,
                file_size_bytes BIGINT NULL,
                metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_reports_task_created_at ON reports(task_id, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_reports_alert_created_at ON reports(alert_id, created_at DESC)",
        ),
        sqlite_statements=(
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
            """,
            "CREATE INDEX IF NOT EXISTS idx_reports_task_created_at ON reports(task_id, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_reports_alert_created_at ON reports(alert_id, created_at DESC)",
        ),
    ),
    RuntimeMigration(
        version="005",
        name="system_configs",
        postgres_statements=(
            """
            CREATE TABLE IF NOT EXISTS system_configs (
                config_key TEXT PRIMARY KEY,
                config_json JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                updated_by TEXT NULL
            )
            """,
        ),
        sqlite_statements=(
            """
            CREATE TABLE IF NOT EXISTS system_configs (
                config_key TEXT PRIMARY KEY,
                config_json TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                updated_by TEXT NULL
            )
            """,
        ),
    ),
    RuntimeMigration(
        version="006",
        name="telemetry_pipeline",
        postgres_statements=(
            """
            CREATE TABLE IF NOT EXISTS telemetry_samples (
                sample_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                device_id TEXT NOT NULL,
                tag TEXT NOT NULL,
                recorded_at TIMESTAMPTZ NOT NULL,
                value DOUBLE PRECISION NOT NULL,
                quality TEXT NOT NULL,
                unit TEXT NULL,
                source TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS collection_runtime_state (
                tenant_id TEXT PRIMARY KEY,
                is_running BOOLEAN NOT NULL DEFAULT FALSE,
                device_ids_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                scan_interval INTEGER NOT NULL DEFAULT 10,
                started_at TIMESTAMPTZ NULL,
                started_by TEXT NULL,
                stopped_at TIMESTAMPTZ NULL,
                updated_at TIMESTAMPTZ NOT NULL
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_telemetry_tenant_tag_time ON telemetry_samples(tenant_id, tag, recorded_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_telemetry_tenant_device_time ON telemetry_samples(tenant_id, device_id, recorded_at DESC)",
        ),
        sqlite_statements=(
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
            """,
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
            """,
            "CREATE INDEX IF NOT EXISTS idx_telemetry_tenant_tag_time ON telemetry_samples(tenant_id, tag, recorded_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_telemetry_tenant_device_time ON telemetry_samples(tenant_id, device_id, recorded_at DESC)",
        ),
    ),
    RuntimeMigration(
        version="007",
        name="device_tag_semantic_fields",
        postgres_statements=(
            "ALTER TABLE device_tags ADD COLUMN IF NOT EXISTS asset_id TEXT NULL",
            "ALTER TABLE device_tags ADD COLUMN IF NOT EXISTS point_key TEXT NULL",
            "ALTER TABLE device_tags ADD COLUMN IF NOT EXISTS deadband DOUBLE PRECISION NULL",
            "ALTER TABLE device_tags ADD COLUMN IF NOT EXISTS debounce_ms INTEGER NOT NULL DEFAULT 0",
        ),
        sqlite_statements=(
            "ALTER TABLE device_tags ADD COLUMN asset_id TEXT NULL",
            "ALTER TABLE device_tags ADD COLUMN point_key TEXT NULL",
            "ALTER TABLE device_tags ADD COLUMN deadband REAL NULL",
            "ALTER TABLE device_tags ADD COLUMN debounce_ms INTEGER NOT NULL DEFAULT 0",
        ),
    ),
    RuntimeMigration(
        version="008",
        name="task_tracking_runtime",
        postgres_statements=(
            """
            CREATE TABLE IF NOT EXISTS tracked_tasks (
                task_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                payload_json JSONB NOT NULL
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_tracked_tasks_status_created_at ON tracked_tasks(status, created_at DESC)",
        ),
        sqlite_statements=(
            """
            CREATE TABLE IF NOT EXISTS tracked_tasks (
                task_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                payload_json TEXT NOT NULL
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_tracked_tasks_status_created_at ON tracked_tasks(status, created_at DESC)",
        ),
    ),
    RuntimeMigration(
        version="009",
        name="industrial_intelligence_runtime",
        postgres_statements=(
            """
            CREATE TABLE IF NOT EXISTS intelligence_snapshots (
                asset_id TEXT PRIMARY KEY,
                scene_type TEXT NOT NULL,
                collected_at TIMESTAMPTZ NOT NULL,
                snapshot_json JSONB NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS intelligence_patrol_runs (
                run_id TEXT PRIMARY KEY,
                scene_type TEXT NOT NULL,
                status TEXT NOT NULL,
                risk_level TEXT NOT NULL,
                risk_score DOUBLE PRECISION NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                triggered_by TEXT NOT NULL,
                schedule_type TEXT NOT NULL,
                result_json JSONB NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS intelligence_labels (
                label_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                asset_id TEXT NOT NULL,
                scene_type TEXT NOT NULL,
                label_status TEXT NOT NULL,
                anomaly_type TEXT NULL,
                root_cause TEXT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL,
                review_json JSONB NOT NULL DEFAULT '{}'::jsonb
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS intelligence_knowledge_cases (
                case_id TEXT PRIMARY KEY,
                asset_id TEXT NULL,
                scene_type TEXT NOT NULL,
                title TEXT NOT NULL,
                summary TEXT NOT NULL,
                content TEXT NOT NULL,
                tags_json JSONB NOT NULL,
                root_cause TEXT NULL,
                recommended_actions_json JSONB NOT NULL,
                source_label_id TEXT NULL,
                source_type TEXT NOT NULL,
                usage_count INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS intelligence_learning_candidates (
                candidate_id TEXT PRIMARY KEY,
                candidate_type TEXT NOT NULL,
                name TEXT NOT NULL,
                status TEXT NOT NULL,
                score DOUBLE PRECISION NOT NULL,
                rationale TEXT NOT NULL,
                payload_json JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_intelligence_patrol_runs_created_at ON intelligence_patrol_runs(created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_intelligence_labels_status_updated_at ON intelligence_labels(label_status, updated_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_intelligence_knowledge_cases_scene_updated_at ON intelligence_knowledge_cases(scene_type, updated_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_intelligence_learning_candidates_type_updated_at ON intelligence_learning_candidates(candidate_type, updated_at DESC)",
        ),
        sqlite_statements=(
            """
            CREATE TABLE IF NOT EXISTS intelligence_snapshots (
                asset_id TEXT PRIMARY KEY,
                scene_type TEXT NOT NULL,
                collected_at TEXT NOT NULL,
                snapshot_json TEXT NOT NULL
            )
            """,
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
            """,
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
            """,
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
            """,
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
            """,
            "CREATE INDEX IF NOT EXISTS idx_intelligence_patrol_runs_created_at ON intelligence_patrol_runs(created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_intelligence_labels_status_updated_at ON intelligence_labels(label_status, updated_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_intelligence_knowledge_cases_scene_updated_at ON intelligence_knowledge_cases(scene_type, updated_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_intelligence_learning_candidates_type_updated_at ON intelligence_learning_candidates(candidate_type, updated_at DESC)",
        ),
    ),
    RuntimeMigration(
        version="010",
        name="knowledge_operations_runtime",
        postgres_statements=(
            """
            CREATE TABLE IF NOT EXISTS intelligence_knowledge_feedback (
                feedback_id TEXT PRIMARY KEY,
                diagnosis_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                helpful BOOLEAN NOT NULL,
                comment TEXT NULL,
                reference_case_ids_json JSONB NOT NULL DEFAULT '[]'::jsonb,
                created_by TEXT NULL,
                created_at TIMESTAMPTZ NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS intelligence_knowledge_activity (
                event_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                actor_id TEXT NULL,
                query TEXT NULL,
                scene_type TEXT NULL,
                resource_id TEXT NULL,
                metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL
            )
            """,
            "CREATE INDEX IF NOT EXISTS idx_knowledge_feedback_tenant_created_at ON intelligence_knowledge_feedback(tenant_id, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_knowledge_feedback_diagnosis ON intelligence_knowledge_feedback(diagnosis_id)",
            "CREATE INDEX IF NOT EXISTS idx_knowledge_activity_tenant_event_created_at ON intelligence_knowledge_activity(tenant_id, event_type, created_at DESC)",
        ),
        sqlite_statements=(
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
            """,
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
            """,
            "CREATE INDEX IF NOT EXISTS idx_knowledge_feedback_tenant_created_at ON intelligence_knowledge_feedback(tenant_id, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_knowledge_feedback_diagnosis ON intelligence_knowledge_feedback(diagnosis_id)",
            "CREATE INDEX IF NOT EXISTS idx_knowledge_activity_tenant_event_created_at ON intelligence_knowledge_activity(tenant_id, event_type, created_at DESC)",
        ),
    ),
)


_MIGRATION_CACHE: set[str] = set()
_CACHE_LOCK = Lock()


class RuntimeSchemaMigrationManager:
    def __init__(self, db_config: Optional[Dict[str, Any]] = None):
        self.db_config = db_config or load_config().get("database", {})
        self.adapter = build_runtime_database_adapter(self.db_config)
        self.backend = self.adapter.backend
        self.schema = str(self.db_config.get("postgres", {}).get("schema", "public"))

    @property
    def cache_key(self) -> str:
        return f"{self.backend}:{self.adapter.target}"

    def _tracking_table(self) -> str:
        if self.backend == "postgres":
            return f'"{self.schema}".runtime_schema_migrations'
        return "runtime_schema_migrations"

    def _record_statement(self) -> str:
        if self.backend == "postgres":
            return (
                f"INSERT INTO {self._tracking_table()} (version, name, applied_at) "
                f"VALUES (%s, %s, %s) ON CONFLICT (version) DO NOTHING"
            )
        return (
            f"INSERT OR IGNORE INTO {self._tracking_table()} (version, name, applied_at) "
            f"VALUES (?, ?, ?)"
        )

    def _tracking_table_statement(self) -> str:
        if self.backend == "postgres":
            return (
                f'CREATE TABLE IF NOT EXISTS "{self.schema}".runtime_schema_migrations ('
                "version TEXT PRIMARY KEY, "
                "name TEXT NOT NULL, "
                "applied_at TIMESTAMPTZ NOT NULL)"
            )
        return (
            "CREATE TABLE IF NOT EXISTS runtime_schema_migrations ("
            "version TEXT PRIMARY KEY, "
            "name TEXT NOT NULL, "
            "applied_at TEXT NOT NULL)"
        )

    def _ensure_tracking_table(self, cursor) -> None:
        if self.backend == "postgres":
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{self.schema}"')
            cursor.execute(f'SET search_path TO "{self.schema}", public')
        cursor.execute(self._tracking_table_statement())

    def _fetch_applied_versions(self, cursor) -> set[str]:
        cursor.execute(f"SELECT version FROM {self._tracking_table()} ORDER BY version ASC")
        rows = cursor.fetchall() or []
        versions: set[str] = set()
        for row in rows:
            if isinstance(row, dict):
                versions.add(str(row["version"]))
            elif hasattr(row, "keys"):
                versions.add(str(dict(row)["version"]))
            else:
                versions.add(str(row[0]))
        return versions

    def _statements_for_backend(self, migration: RuntimeMigration) -> Sequence[str]:
        if self.backend == "postgres":
            return migration.postgres_statements
        return migration.sqlite_statements

    def apply_all(self) -> Dict[str, Any]:
        applied_versions: List[str] = []
        with self.adapter.connect() as connection:
            cursor = connection.cursor()
            self._ensure_tracking_table(cursor)
            existing_versions = self._fetch_applied_versions(cursor)

            for migration in RUNTIME_MIGRATIONS:
                if migration.version in existing_versions:
                    continue
                for statement in self._statements_for_backend(migration):
                    cursor.execute(statement)
                applied_at = utc_now()
                cursor.execute(
                    self._record_statement(),
                    (
                        migration.version,
                        migration.name,
                        applied_at if self.backend == "postgres" else applied_at.isoformat(),
                    ),
                )
                applied_versions.append(migration.version)
            connection.commit()

        return {
            "backend": self.backend,
            "target": self.adapter.target,
            "applied_versions": applied_versions,
            "total_migrations": len(RUNTIME_MIGRATIONS),
        }


def apply_runtime_schema_migrations(db_config: Optional[Dict[str, Any]] = None, *, force: bool = False) -> Dict[str, Any]:
    manager = RuntimeSchemaMigrationManager(db_config=db_config)
    cache_key = manager.cache_key

    with _CACHE_LOCK:
        if not force and cache_key in _MIGRATION_CACHE:
            return {
                "backend": manager.backend,
                "target": manager.adapter.target,
                "applied_versions": [],
                "total_migrations": len(RUNTIME_MIGRATIONS),
                "cached": True,
            }

    result = manager.apply_all()

    with _CACHE_LOCK:
        _MIGRATION_CACHE.add(cache_key)

    if result["applied_versions"]:
        logger.info(
            "Applied runtime schema migrations",
            extra={
                "backend": result["backend"],
                "applied_versions": result["applied_versions"],
            },
        )
    return result
