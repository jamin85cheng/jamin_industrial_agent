from src.intelligence import repository as intelligence_repository_module
from src.intelligence.repository import IntelligenceRepository
from src.tasks import persistence as task_persistence
from src.tasks.persistence import PostgresTaskPersistenceBackend


def test_intelligence_repository_postgres_init_uses_runtime_migrations(tmp_path, monkeypatch):
    calls = []

    monkeypatch.setattr(
        intelligence_repository_module,
        "apply_runtime_schema_migrations",
        lambda config: calls.append(config) or {"applied_versions": []},
    )

    repo = IntelligenceRepository(
        {
            "sqlite": {"path": str(tmp_path / "metadata.db")},
            "postgres": {
                "enabled": True,
                "host": "127.0.0.1",
                "port": 5432,
                "database": "jamin_industrial_agent",
                "user": "postgres",
                "password": "postgres",
                "schema": "jamin_industrial_agent",
            },
        }
    )

    repo.init_schema()

    assert len(calls) == 1
    assert calls[0]["postgres"]["enabled"] is True
    assert calls[0]["postgres"]["schema"] == "jamin_industrial_agent"


def test_postgres_task_persistence_init_uses_runtime_migrations(monkeypatch):
    calls = []
    monkeypatch.setattr(task_persistence, "_resolve_postgres_driver", lambda: ("psycopg", object()))
    monkeypatch.setattr(
        task_persistence,
        "apply_runtime_schema_migrations",
        lambda config: calls.append(config) or {"applied_versions": []},
    )

    backend = PostgresTaskPersistenceBackend(
        host="127.0.0.1",
        port=5432,
        database="jamin_industrial_agent",
        user="postgres",
        password="postgres",
        schema="jamin_industrial_agent",
    )

    backend.init()

    assert len(calls) == 1
    assert calls[0]["postgres"]["enabled"] is True
    assert calls[0]["postgres"]["user"] == "postgres"
