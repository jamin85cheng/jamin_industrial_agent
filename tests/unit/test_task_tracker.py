import asyncio
import importlib

import pytest

from src.tasks import persistence as task_persistence
from src.tasks.task_tracker import TaskPriority, TaskStatus, TaskTracker


def test_task_tracker_persists_completed_task(tmp_path):
    case_dir = tmp_path / "task_tracker"
    case_dir.mkdir(parents=True, exist_ok=True)
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


def test_task_tracker_marks_interrupted_active_task_after_restart(tmp_path):
    case_dir = tmp_path / "task_tracker"
    case_dir.mkdir(parents=True, exist_ok=True)
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


def test_task_tracker_marks_queued_task_interrupted_after_restart(tmp_path):
    case_dir = tmp_path / "task_tracker"
    case_dir.mkdir(parents=True, exist_ok=True)
    tracker = TaskTracker(db_path=case_dir / "tasks.sqlite")
    task = tracker.create_task(
        task_type="multi_agent_diagnosis",
        description="Queued restart recovery test",
        priority=TaskPriority.NORMAL,
    )
    tracker.mark_task_queued(task.task_id, action="queued for worker")

    reloaded = TaskTracker(db_path=case_dir / "tasks.sqlite")
    restored = reloaded.get_task(task.task_id)

    assert restored is not None
    assert restored.status == TaskStatus.QUEUED
    assert restored.metadata["recovery"]["restored_from_persistence"] is True
    assert restored.metadata["recovery"]["resume_required"] is True


def test_task_tracker_restores_pending_recoverable_task_as_queued(tmp_path):
    case_dir = tmp_path / "task_tracker"
    case_dir.mkdir(parents=True, exist_ok=True)
    tracker = TaskTracker(db_path=case_dir / "tasks.sqlite")
    task = tracker.create_task(
        task_type="multi_agent_diagnosis",
        description="Pending recovery test",
        priority=TaskPriority.NORMAL,
    )

    reloaded = TaskTracker(db_path=case_dir / "tasks.sqlite")
    restored = reloaded.get_task(task.task_id)

    assert restored is not None
    assert restored.status == TaskStatus.QUEUED
    assert restored.metadata["recovery"]["restored_from_persistence"] is True
    assert restored.metadata["recovery"]["resume_required"] is True


def test_task_tracker_uses_sqlite_storage_metadata_for_file_backend(tmp_path):
    case_dir = tmp_path / "task_tracker"
    case_dir.mkdir(parents=True, exist_ok=True)
    tracker = TaskTracker(db_path=case_dir / "tasks.sqlite")
    task = tracker.create_task(
        task_type="multi_agent_diagnosis",
        description="storage metadata test",
        priority=TaskPriority.NORMAL,
    )

    assert tracker.storage_label == "sqlite"
    assert task.metadata["task_runtime"]["storage"] == "sqlite"
    assert str(case_dir / "tasks.sqlite") in task.metadata["task_runtime"]["target"]


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


def test_task_tracker_cancel_marks_metadata_and_status(tmp_path):
    case_dir = tmp_path / "task_tracker"
    case_dir.mkdir(parents=True, exist_ok=True)
    tracker = TaskTracker(db_path=case_dir / "tasks.sqlite")
    task = tracker.create_task(
        task_type="multi_agent_diagnosis",
        description="cancel metadata test",
        priority=TaskPriority.NORMAL,
    )

    cancelled = asyncio.run(
        tracker.cancel_task(
            task.task_id,
            cancelled_by="operator",
            reason="Operator cancelled duplicate task",
        )
    )

    assert cancelled is not None
    assert cancelled.status == TaskStatus.CANCELLED
    assert cancelled.metadata["cancelled_by"] == "operator"
    assert cancelled.metadata["cancellation_reason"] == "Operator cancelled duplicate task"
    assert cancelled.metadata["cancel_requested_at"] is not None


def test_task_tracker_does_not_execute_pre_cancelled_task(tmp_path):
    case_dir = tmp_path / "task_tracker"
    case_dir.mkdir(parents=True, exist_ok=True)
    tracker = TaskTracker(db_path=case_dir / "tasks.sqlite")
    task = tracker.create_task(
        task_type="multi_agent_diagnosis",
        description="pre-cancel execute test",
        priority=TaskPriority.NORMAL,
    )
    asyncio.run(tracker.cancel_task(task.task_id, cancelled_by="operator"))

    async def work(task_obj):
        tracker.update_progress(task_obj.task_id, step=1, action="should-not-run", percentage=100)
        return {"status": "unexpected"}

    result = asyncio.run(tracker.execute(task, work))

    assert result is None
    assert task.status == TaskStatus.CANCELLED
    assert task.progress.current_action == "Cancellation requested"


def test_task_tracker_mark_task_queued_updates_status_and_stats(tmp_path):
    case_dir = tmp_path / "task_tracker"
    case_dir.mkdir(parents=True, exist_ok=True)
    tracker = TaskTracker(db_path=case_dir / "tasks.sqlite")
    task = tracker.create_task(
        task_type="multi_agent_diagnosis",
        description="queue state test",
        priority=TaskPriority.NORMAL,
    )

    queued = tracker.mark_task_queued(task.task_id, action="queued for executor")

    assert queued is not None
    assert queued.status == TaskStatus.QUEUED
    assert queued.progress.current_action == "queued for executor"
    assert queued.metadata["workflow"]["status"] == "queued"
    assert tracker.get_stats()["total_queued"] == 1
