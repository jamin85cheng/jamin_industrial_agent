import shutil
import uuid
from pathlib import Path

from src.utils.database_runtime import RuntimeDatabaseAdapter


def _make_case_dir() -> Path:
    case_dir = Path("E:/jamin_industrial_agent/tests/.tmp") / f"database_runtime_{uuid.uuid4().hex[:8]}"
    case_dir.mkdir(parents=True, exist_ok=True)
    return case_dir


def test_runtime_database_adapter_uses_sqlite_when_postgres_disabled():
    case_dir = _make_case_dir()
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
    shutil.rmtree(case_dir, ignore_errors=True)


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
