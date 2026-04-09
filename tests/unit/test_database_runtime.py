from pathlib import Path

from src.utils.database_runtime import RuntimeDatabaseAdapter


def test_runtime_database_adapter_uses_sqlite_when_postgres_disabled(tmp_path):
    case_dir = tmp_path / "database_runtime"
    case_dir.mkdir(parents=True, exist_ok=True)
    adapter = RuntimeDatabaseAdapter(
        {
            "sqlite": {"path": str(case_dir / "metadata.db")},
            "postgres": {"enabled": False},
        }
    )

    assert adapter.backend == "sqlite"
    assert adapter.target.endswith("metadata.db")

    with adapter.connect() as connection:
        connection.execute("CREATE TABLE IF NOT EXISTS healthcheck (id INTEGER PRIMARY KEY)")
        connection.commit()

    assert Path(case_dir / "metadata.db").exists()


def test_runtime_database_adapter_reports_postgres_target_when_enabled():
    adapter = RuntimeDatabaseAdapter(
        {
            "sqlite": {"path": "data/metadata.db"},
            "postgres": {
                "enabled": True,
                "host": "127.0.0.1",
                "port": 5432,
                "database": "jamin_industrial_agent",
                "user": "postgres",
                "password": "postgres",
                "schema": "jamin_industrial_agent",
                "sslmode": "prefer",
            },
        }
    )

    assert adapter.backend == "postgres"
    assert "jamin_industrial_agent" in adapter.target
