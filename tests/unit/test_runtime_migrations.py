import sqlite3

from src.utils.runtime_migrations import RuntimeSchemaMigrationManager, apply_runtime_schema_migrations


def _config(tmp_path):
    case_dir = tmp_path / "runtime_migrations"
    case_dir.mkdir(parents=True, exist_ok=True)
    return {
        "sqlite": {"path": str(case_dir / "metadata.db")},
        "postgres": {"enabled": False},
    }


def test_runtime_schema_migrations_create_core_tables(tmp_path):
    config = _config(tmp_path)

    first = apply_runtime_schema_migrations(config, force=True)
    second = apply_runtime_schema_migrations(config)

    assert first["applied_versions"] == ["001", "002", "003", "004", "005", "006", "007", "008", "009", "010"]
    assert second["cached"] is True

    conn = sqlite3.connect(config["sqlite"]["path"])
    try:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        device_tag_columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(device_tags)").fetchall()
        }
    finally:
        conn.close()

    assert "runtime_schema_migrations" in tables
    assert "auth_users" in tables
    assert "devices" in tables
    assert "alerts" in tables
    assert "reports" in tables
    assert "system_configs" in tables
    assert "telemetry_samples" in tables
    assert "tracked_tasks" in tables
    assert "intelligence_patrol_runs" in tables
    assert "intelligence_knowledge_cases" in tables
    assert "intelligence_knowledge_feedback" in tables
    assert "intelligence_knowledge_activity" in tables
    assert {"asset_id", "point_key", "deadband", "debounce_ms"}.issubset(device_tag_columns)


def test_runtime_schema_manager_is_idempotent(tmp_path):
    config = _config(tmp_path)
    manager = RuntimeSchemaMigrationManager(config)

    first = manager.apply_all()
    second = manager.apply_all()

    assert first["applied_versions"] == ["001", "002", "003", "004", "005", "006", "007", "008", "009", "010"]
    assert second["applied_versions"] == []
