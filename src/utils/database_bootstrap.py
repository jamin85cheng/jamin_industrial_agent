"""Operational helpers for preparing and initializing the runtime database."""

from __future__ import annotations

import argparse
import json
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.api.repositories.alert_repository import AlertRepository
from src.api.repositories.auth_repository import AuthRepository
from src.api.repositories.device_repository import DeviceRepository
from src.dev.runtime_bootstrap import (
    ensure_alert_demo_data,
    ensure_alert_rule_defaults,
    ensure_device_demo_data,
    ensure_report_storage,
    ensure_system_config_storage,
)
from src.intelligence.repository import IntelligenceRepository
from src.intelligence.service import IndustrialIntelligenceService
from src.tasks.task_tracker import TaskTracker
from src.utils.config import PROJECT_ROOT, load_config
from src.utils.database_runtime import build_runtime_database_adapter
from src.utils.runtime_migrations import (
    POSTGRES_RUNTIME_TABLES,
    RUNTIME_MIGRATIONS,
    apply_runtime_schema_migrations,
)
from src.utils.structured_logging import get_logger

logger = get_logger("database_bootstrap")
_runtime_bootstrap_lock = threading.RLock()
_runtime_bootstrap_cache: Dict[str, Any] = {
    "cache_key": None,
    "payload": None,
}


def _load_postgres_driver():
    try:
        import psycopg  # type: ignore

        return psycopg
    except ImportError as exc:
        raise RuntimeError("Postgres bootstrap requires psycopg[binary]>=3.2.0.") from exc


def _quote_ident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _runtime_db_config(config: Dict[str, Any]) -> Dict[str, Any]:
    return dict(config.get("database") or config)


def _postgres_config(config: Dict[str, Any]) -> Dict[str, Any]:
    db_config = _runtime_db_config(config)
    postgres = dict(db_config.get("postgres") or {})
    if not postgres.get("enabled", False):
        raise RuntimeError("Postgres is not enabled in the active database configuration.")
    return postgres


def _resolve_include_demo_data(config: Dict[str, Any], include_demo_data: Optional[bool]) -> bool:
    if include_demo_data is not None:
        return bool(include_demo_data)

    bootstrap_config = dict(config.get("runtime_bootstrap") or {})
    if "include_demo_data" in bootstrap_config:
        return bool(bootstrap_config.get("include_demo_data"))

    environment = str(config.get("project", {}).get("environment", "development")).lower()
    return environment == "development"


def reset_runtime_bootstrap_state() -> None:
    with _runtime_bootstrap_lock:
        _runtime_bootstrap_cache["cache_key"] = None
        _runtime_bootstrap_cache["payload"] = None


def _connect_postgres(postgres: Dict[str, Any], *, database: Optional[str] = None, autocommit: bool = False):
    psycopg = _load_postgres_driver()
    connection = psycopg.connect(
        host=postgres.get("host", "127.0.0.1"),
        port=int(postgres.get("port", 5432)),
        dbname=database or postgres.get("database", "jamin_industrial_agent"),
        user=postgres.get("user", "postgres"),
        password=postgres.get("password", "postgres"),
        sslmode=postgres.get("sslmode", "prefer"),
    )
    connection.autocommit = autocommit
    return connection


def prepare_postgres_database(config: Dict[str, Any]) -> Dict[str, Any]:
    postgres = _postgres_config(config)
    target_database = str(postgres.get("database", "jamin_industrial_agent"))
    target_schema = str(postgres.get("schema", "jamin_industrial_agent"))
    maintenance_db = str(postgres.get("maintenance_database", "postgres"))

    created_database = False
    with _connect_postgres(postgres, database=maintenance_db, autocommit=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (target_database,))
            created_database = cursor.fetchone() is None
            if created_database:
                cursor.execute(f"CREATE DATABASE {_quote_ident(target_database)}")

    moved_tables: List[str] = []
    with _connect_postgres(postgres, database=target_database, autocommit=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_ident(target_schema)}")
            cursor.execute(
                f"ALTER DATABASE {_quote_ident(target_database)} SET search_path TO {_quote_ident(target_schema)}, public"
            )
            cursor.execute(
                f"ALTER ROLE {_quote_ident(str(postgres.get('user', 'postgres')))} IN DATABASE {_quote_ident(target_database)} "
                f"SET search_path TO {_quote_ident(target_schema)}, public"
            )
            moved_tables = _move_runtime_tables_into_schema(cursor, target_schema)

    result = {
        "backend": "postgres",
        "database": target_database,
        "schema": target_schema,
        "created_database": created_database,
        "moved_tables": moved_tables,
        "prepared": True,
    }
    logger.info(
        "Prepared runtime Postgres database",
        database=target_database,
        schema=target_schema,
        created_database=created_database,
    )
    return result


def _move_runtime_tables_into_schema(cursor, schema: str) -> List[str]:
    moved: List[str] = []
    for table_name in POSTGRES_RUNTIME_TABLES:
        cursor.execute("SELECT to_regclass(%s)", (f"{schema}.{table_name}",))
        schema_relation = cursor.fetchone()
        schema_exists = _regclass_exists(schema_relation)
        if schema_exists:
            continue

        cursor.execute("SELECT to_regclass(%s)", (f"public.{table_name}",))
        public_relation = cursor.fetchone()
        public_exists = _regclass_exists(public_relation)
        if not public_exists:
            continue

        cursor.execute(f"ALTER TABLE public.{_quote_ident(table_name)} SET SCHEMA {_quote_ident(schema)}")
        moved.append(table_name)
    return moved


def _regclass_exists(row: Any) -> bool:
    if row is None:
        return False
    if isinstance(row, dict):
        return bool(next(iter(row.values()), None))
    if hasattr(row, "keys"):
        return bool(next(iter(dict(row).values()), None))
    if isinstance(row, (list, tuple)):
        return bool(row[0] if row else None)
    return bool(row)


def migrate_runtime_database(config: Dict[str, Any], *, force: bool = True) -> Dict[str, Any]:
    result = apply_runtime_schema_migrations(_runtime_db_config(config), force=force)
    logger.info(
        "Runtime database migrations applied",
        backend=result.get("backend"),
        applied_versions=result.get("applied_versions"),
    )
    return result


def seed_runtime_reference_data(
    config: Dict[str, Any],
    *,
    include_demo_data: bool = False,
    bootstrap_tracker_path: Optional[Path] = None,
) -> Dict[str, Any]:
    db_config = _runtime_db_config(config)

    auth_repository = AuthRepository(db_config)
    auth_repository.init_schema()
    auth_repository.ensure_tenant(tenant_id="default", name="Default Tenant")
    auth_repository.ensure_default_roles()

    ensure_alert_rule_defaults(db_config, tenant_id="default")
    ensure_report_storage(db_config)
    ensure_system_config_storage(db_config)

    device_repository = DeviceRepository(db_config)
    device_repository.init_schema()

    alert_repository = AlertRepository(db_config)
    alert_repository.init_schema()

    intelligence_repository = IntelligenceRepository(db_config)
    tracker_path = bootstrap_tracker_path or (PROJECT_ROOT / "data" / "runtime" / "bootstrap_tasks.sqlite")
    tracker = TaskTracker(db_path=tracker_path)
    intelligence_service = IndustrialIntelligenceService(
        config=config,
        repository=intelligence_repository,
        tracker=tracker,
    )

    if include_demo_data:
        ensure_device_demo_data(db_config, tenant_id="default")
        ensure_alert_demo_data(db_config, tenant_id="default")

    seeded = {
        "roles": len(auth_repository.list_roles()),
        "tenants": len(auth_repository.list_tenants()),
        "rules": len(alert_repository.list_rules(tenant_id="default")),
        "devices": device_repository.list_devices(tenant_id="default", skip=0, limit=500)["total"],
        "alerts": alert_repository.list_alerts(tenant_id="default", limit=500)["total"],
        "knowledge_cases": len(intelligence_service.list_knowledge_cases(limit=500)),
        "include_demo_data": include_demo_data,
    }
    logger.info("Seeded runtime reference data", **seeded)
    return seeded


def bootstrap_runtime_dependencies(
    config: Dict[str, Any],
    *,
    include_demo_data: Optional[bool] = None,
    force: bool = False,
) -> Dict[str, Any]:
    from src.api.dependencies import init_default_users
    from src.utils.health_check import init_default_checks

    resolved_include_demo_data = _resolve_include_demo_data(config, include_demo_data)
    cache_key = json.dumps(
        {
            "database": _runtime_db_config(config),
            "include_demo_data": resolved_include_demo_data,
        },
        sort_keys=True,
        ensure_ascii=False,
        default=str,
    )

    with _runtime_bootstrap_lock:
        if not force and _runtime_bootstrap_cache.get("cache_key") == cache_key:
            cached_payload = dict(_runtime_bootstrap_cache.get("payload") or {})
            cached_payload["cached"] = True
            return cached_payload

        migration = migrate_runtime_database(config, force=True)
        seed = seed_runtime_reference_data(
            config,
            include_demo_data=resolved_include_demo_data,
        )
        init_default_checks()
        init_default_users()

        payload = {
            "migrate": migration,
            "seed": seed,
            "include_demo_data": resolved_include_demo_data,
            "cached": False,
        }
        _runtime_bootstrap_cache["cache_key"] = cache_key
        _runtime_bootstrap_cache["payload"] = dict(payload)
        logger.info(
            "Bootstrapped runtime dependencies",
            include_demo_data=resolved_include_demo_data,
            migration_backend=migration.get("backend"),
            applied_versions=migration.get("applied_versions", []),
        )
        return payload


def get_runtime_database_status(config: Dict[str, Any]) -> Dict[str, Any]:
    db_config = _runtime_db_config(config)
    adapter = build_runtime_database_adapter(db_config)
    backend = adapter.backend
    schema = str((db_config.get("postgres") or {}).get("schema", "public"))
    tracking_table = f'"{schema}".runtime_schema_migrations' if backend == "postgres" else "runtime_schema_migrations"
    versions: List[str] = []

    try:
        with adapter.connect() as connection:
            cursor = connection.cursor()
            cursor.execute(f"SELECT version FROM {tracking_table} ORDER BY version ASC")
            rows = cursor.fetchall() or []
    except Exception as exc:
        return {
            "backend": backend,
            "target": adapter.target,
            "schema": schema if backend == "postgres" else None,
            "reachable": False,
            "error": str(exc),
            "applied_versions": [],
            "expected_migrations": len(RUNTIME_MIGRATIONS),
        }

    for row in rows:
        if isinstance(row, dict):
            versions.append(str(row["version"]))
        elif hasattr(row, "keys"):
            versions.append(str(dict(row)["version"]))
        else:
            versions.append(str(row[0]))

    return {
        "backend": backend,
        "target": adapter.target,
        "schema": schema if backend == "postgres" else None,
        "reachable": True,
        "applied_versions": versions,
        "applied_count": len(versions),
        "expected_migrations": len(RUNTIME_MIGRATIONS),
        "pending_versions": [
            migration.version for migration in RUNTIME_MIGRATIONS if migration.version not in set(versions)
        ],
    }


def initialize_runtime_database(
    config: Dict[str, Any],
    *,
    include_demo_data: bool = False,
) -> Dict[str, Any]:
    prepare = prepare_postgres_database(config)
    migrate = migrate_runtime_database(config, force=True)
    seed = seed_runtime_reference_data(config, include_demo_data=include_demo_data)
    status = get_runtime_database_status(config)
    return {
        "prepare": prepare,
        "migrate": migrate,
        "seed": seed,
        "status": status,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare and initialize the Jamin runtime database.")
    parser.add_argument(
        "command",
        choices=["prepare", "migrate", "seed", "init", "status"],
        help="Database operation to run.",
    )
    parser.add_argument(
        "--config",
        default="config/settings.yaml",
        help="Path to the YAML configuration file.",
    )
    parser.add_argument(
        "--include-demo-data",
        action="store_true",
        help="Seed demo devices and alerts in addition to core reference data.",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    config = load_config(args.config)

    if args.command == "prepare":
        payload = prepare_postgres_database(config)
    elif args.command == "migrate":
        payload = migrate_runtime_database(config, force=True)
    elif args.command == "seed":
        payload = seed_runtime_reference_data(config, include_demo_data=args.include_demo_data)
    elif args.command == "init":
        payload = initialize_runtime_database(config, include_demo_data=args.include_demo_data)
    else:
        payload = get_runtime_database_status(config)

    print(json.dumps(payload, indent=2, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
