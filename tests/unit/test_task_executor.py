import asyncio

from fastapi import BackgroundTasks

from src.tasks.executor import (
    AsyncioQueueDiagnosisExecutor,
    BackgroundTaskDiagnosisExecutor,
    build_diagnosis_task_executor,
)
from src.tasks.task_tracker import TaskPriority, TaskStatus, TaskTracker


def test_background_executor_records_submission_runtime(tmp_path):
    tracker = TaskTracker(db_path=tmp_path / "task_executor.sqlite")
    background_tasks = BackgroundTasks()
    executor = BackgroundTaskDiagnosisExecutor(background_tasks=background_tasks, tracker=tracker)
    task = tracker.create_task(
        task_type="multi_agent_diagnosis",
        description="background executor demo",
        priority=TaskPriority.NORMAL,
        metadata={"tenant_id": "default", "diagnosis_mode": "multi_agent"},
    )

    async def fake_handler(task_obj):
        return task_obj.task_id

    asyncio.run(executor.submit(task, fake_handler))

    refreshed = tracker.get_task(task.task_id)
    assert refreshed is not None
    assert refreshed.status == TaskStatus.QUEUED
    assert refreshed.progress.current_action == "Queued via FastAPI background tasks"
    assert refreshed.metadata["task_runtime"]["executor"]["backend"] == "background_tasks"
    assert len(background_tasks.tasks) == 1


def test_build_executor_falls_back_from_unsupported_backend(tmp_path):
    tracker = TaskTracker(db_path=tmp_path / "task_executor_fallback.sqlite")
    executor = build_diagnosis_task_executor(
        {"diagnosis": {"execution": {"backend": "celery"}}},
        background_tasks=BackgroundTasks(),
        tracker=tracker,
    )

    summary = executor.describe()
    assert summary["backend"] == "background_tasks"
    assert summary["requested_backend"] == "celery"
    assert "Unsupported backend" in summary["resolution_note"]


def test_asyncio_queue_executor_runs_submitted_task(tmp_path):
    tracker = TaskTracker(db_path=tmp_path / "task_executor_asyncio.sqlite")
    executor = AsyncioQueueDiagnosisExecutor(tracker=tracker, max_workers=1)
    task = tracker.create_task(
        task_type="multi_agent_diagnosis",
        description="asyncio executor demo",
        priority=TaskPriority.NORMAL,
        metadata={"tenant_id": "default", "diagnosis_mode": "multi_agent"},
    )

    async def fake_handler(task_obj):
        async def work(inner_task):
            tracker.update_progress(inner_task.task_id, step=1, action="worker-running", percentage=50)
            return {"status": "ok"}

        await tracker.execute(task_obj, work)

    async def run_flow():
        await executor.submit(task, fake_handler)
        for _ in range(60):
            current = tracker.get_task(task.task_id)
            if current and current.status == TaskStatus.COMPLETED:
                await executor.shutdown()
                return current
            await asyncio.sleep(0.05)
        await executor.shutdown()
        return tracker.get_task(task.task_id)

    completed_task = asyncio.run(run_flow())

    assert completed_task is not None
    assert completed_task.status == TaskStatus.COMPLETED
    assert completed_task.result == {"status": "ok"}
    assert completed_task.metadata["task_runtime"]["executor"]["backend"] == "asyncio_queue"
