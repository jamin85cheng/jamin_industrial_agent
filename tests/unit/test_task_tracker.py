import asyncio
import importlib
import shutil
import uuid
from pathlib import Path

import pytest

from src.tasks import persistence as task_persistence
from src.tasks.task_tracker import TaskPriority, TaskStatus, TaskTracker


def _make_case_dir() -> Path:
    case_dir = Path("E:/jamin_industrial_agent/tests/.tmp") / f"task_tracker_{uuid.uuid4().hex[:8]}"
    case_dir.mkdir(parents=True, exist_ok=True)
    return case_dir


def test_task_tracker_persists_completed_task():
    case_dir = _make_case_dir()
    tracker = TaskTracker(db_path=case_dir / "tasks.sqlite")
    task = tracker.create_task(
        task_type="multi_agent_diagnosis",
        description="Persist completed diagnosis task",
        priority=TaskPriority.HIGH,
        metadata={"timeout_seconds": 30},
    )

    async def work(task_obj):
        tracker.update_progress(task_obj.task_id, step=1, action="running", percentage=50)
        return {"status": "ok"}

    asyncio.run(tracker.execute(task, work))

    reloaded = TaskTracker(db_path=case_dir / "tasks.sqlite")
    restored = reloaded.get_task(task.task_id)

    assert restored is not None
    assert restored.status == TaskStatus.COMPLETED
    assert restored.result == {"status": "ok"}
    assert restored.progress.current_action == "running"
    shutil.rmtree(case_dir, ignore_errors=True)


def test_task_tracker_marks_interrupted_active_task_after_restart():
    case_dir = _make_case_dir()
    tracker = TaskTracker(db_path=case_dir / "tasks.sqlite")
    task = tracker.create_task(
        task_type="camel_diagnosis",
        description="Restart recovery test",
        priority=TaskPriority.NORMAL,
    )
    task.status = TaskStatus.RUNNING
    tracker.update_progress(task.task_id, step=3, action="debating", percentage=35)

    reloaded = TaskTracker(db_path=case_dir / "tasks.sqlite")
    restored = reloaded.get_task(task.task_id)

    assert restored is not None
    assert restored.status == TaskStatus.FAILED
    assert restored.error == "Task interrupted because the tracker process restarted."
    assert restored.metadata["recovery"]["restored_from_persistence"] is True
    shutil.rmtree(case_dir, ignore_errors=True)


def test_task_tracker_uses_sqlite_storage_metadata_for_file_backend():
    case_dir = _make_case_dir()
    tracker = TaskTracker(db_path=case_dir / "tasks.sqlite")
    task = tracker.create_task(
        task_type="multi_agent_diagnosis",
        description="storage metadata test",
        priority=TaskPriority.NORMAL,
    )

    assert tracker.storage_label == "sqlite"
    assert task.metadata["task_runtime"]["storage"] == "sqlite"
    assert str(case_dir / "tasks.sqlite") in task.metadata["task_runtime"]["target"]
    shutil.rmtree(case_dir, ignore_errors=True)


def test_task_tracker_falls_back_to_sqlite_when_postgres_driver_missing(monkeypatch):
    task_tracker_module = importlib.import_module("src.tasks.task_tracker")
    monkeypatch.setattr(
        task_tracker_module,
        "load_config",
        lambda: {
            "database": {
                "postgres": {
                    "enabled": True,
                    "host": "127.0.0.1",
                    "port": 5432,
                    "database": "jamin_industrial_agent",
                    "user": "postgres",
                    "password": "postgres",
                    "schema": "jamin_industrial_agent",
                },
                "task_tracking": {
                    "backend": "postgres",
                    "sqlite_path": "data/runtime/test-fallback.sqlite",
                },
            }
        },
    )
    monkeypatch.setattr(task_persistence, "_resolve_postgres_driver", lambda: (None, None))

    tracker = TaskTracker()

    assert tracker.storage_label == "sqlite"
    assert tracker.persistence_target.endswith("data\\runtime\\test-fallback.sqlite")


def test_build_task_persistence_backend_raises_when_postgres_driver_missing(monkeypatch):
    monkeypatch.setattr(task_persistence, "_resolve_postgres_driver", lambda: (None, None))

    with pytest.raises(RuntimeError):
        task_persistence.build_task_persistence_backend(
            {
                "postgres": {
                    "enabled": True,
                    "host": "127.0.0.1",
                    "port": 5432,
                    "database": "jamin_industrial_agent",
                    "user": "postgres",
                    "password": "postgres",
                    "schema": "jamin_industrial_agent",
                },
                "task_tracking": {"backend": "postgres"},
            }
        )
