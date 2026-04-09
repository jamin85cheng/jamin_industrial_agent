from pathlib import Path

from src.utils import database_bootstrap


class _FakeCursor:
    def __init__(self, fetchone_sequence=None):
        self.executed = []
        self._fetchone_sequence = list(fetchone_sequence or [])

    def execute(self, statement, params=None):
        self.executed.append((str(statement).strip(), params))

    def fetchone(self):
        if self._fetchone_sequence:
            return self._fetchone_sequence.pop(0)
        return None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeConnection:
    def __init__(self, cursor):
        self.cursor_instance = cursor
        self.autocommit = False

    def cursor(self):
        return self.cursor_instance

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_prepare_postgres_database_creates_database_and_schema(monkeypatch):
    admin_cursor = _FakeCursor(fetchone_sequence=[None])
    target_cursor = _FakeCursor()
    admin_connection = _FakeConnection(admin_cursor)
    target_connection = _FakeConnection(target_cursor)

    def _fake_connect(postgres, *, database=None, autocommit=False):
        _ = postgres, autocommit
        return admin_connection if database == "postgres" else target_connection

    monkeypatch.setattr(database_bootstrap, "_connect_postgres", _fake_connect)

    payload = database_bootstrap.prepare_postgres_database(
        {
            "database": {
                "postgres": {
                    "enabled": True,
                    "host": "127.0.0.1",
                    "port": 5432,
                    "database": "jamin_industrial_agent",
                    "user": "postgres",
                    "password": "postgres",
                    "schema": "jamin_industrial_agent",
                }
            }
        }
    )

    assert payload["created_database"] is True
    assert any("SELECT 1 FROM pg_database" in item[0] for item in admin_cursor.executed)
    assert any("CREATE DATABASE" in item[0] for item in admin_cursor.executed)
    assert any("CREATE SCHEMA IF NOT EXISTS" in item[0] for item in target_cursor.executed)


def test_seed_runtime_reference_data_initializes_core_records(tmp_path):
    db_config = {
        "sqlite": {"path": str(tmp_path / "metadata.db")},
        "postgres": {"enabled": False},
        "task_tracking": {"backend": "sqlite", "sqlite_path": str(tmp_path / "tasks.sqlite")},
    }
    config = {
        "database": db_config,
        "intelligence": {
            "llm": {"enabled": False},
            "patrol": {"interval_seconds": 180, "run_on_startup": False},
        },
    }

    payload = database_bootstrap.seed_runtime_reference_data(
        config,
        include_demo_data=False,
        bootstrap_tracker_path=Path(tmp_path / "bootstrap_tasks.sqlite"),
    )

    assert payload["roles"] >= 3
    assert payload["tenants"] >= 1
    assert payload["rules"] >= 1
    assert payload["devices"] == 0
    assert payload["alerts"] == 0
    assert payload["knowledge_cases"] >= 1


def test_get_runtime_database_status_reports_applied_versions(tmp_path):
    config = {
        "database": {
            "sqlite": {"path": str(tmp_path / "metadata.db")},
            "postgres": {"enabled": False},
        }
    }

    database_bootstrap.migrate_runtime_database(config, force=True)
    status = database_bootstrap.get_runtime_database_status(config)

    assert status["reachable"] is True
    assert status["applied_count"] == len(database_bootstrap.RUNTIME_MIGRATIONS)
    assert status["pending_versions"] == []


def test_bootstrap_runtime_dependencies_is_cached(monkeypatch):
    calls = []

    monkeypatch.setattr(
        database_bootstrap,
        "migrate_runtime_database",
        lambda config, force=True: calls.append(("migrate", force)) or {"backend": "sqlite", "applied_versions": []},
    )
    monkeypatch.setattr(
        database_bootstrap,
        "seed_runtime_reference_data",
        lambda config, include_demo_data=False, bootstrap_tracker_path=None: calls.append(
            ("seed", include_demo_data, bootstrap_tracker_path)
        )
        or {"devices": 0, "alerts": 0},
    )

    import src.api.dependencies as auth_dependencies
    import src.utils.health_check as health_check

    monkeypatch.setattr(auth_dependencies, "init_default_users", lambda: calls.append(("users", None)))
    monkeypatch.setattr(health_check, "init_default_checks", lambda: calls.append(("health", None)))

    database_bootstrap.reset_runtime_bootstrap_state()
    config = {
        "project": {"environment": "development"},
        "database": {
            "sqlite": {"path": "data/test.db"},
            "postgres": {"enabled": False},
        },
    }

    first = database_bootstrap.bootstrap_runtime_dependencies(config)
    second = database_bootstrap.bootstrap_runtime_dependencies(config)

    assert first["cached"] is False
    assert second["cached"] is True
    assert calls == [
        ("migrate", True),
        ("seed", True, None),
        ("health", None),
        ("users", None),
    ]
