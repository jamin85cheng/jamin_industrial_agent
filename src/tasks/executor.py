"""Execution backends for diagnosis task submission."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import partial
from typing import Any, Awaitable, Callable, Dict, Optional

from fastapi import BackgroundTasks

from src.tasks.task_tracker import TaskTracker, TrackedTask, task_tracker
from src.utils.structured_logging import get_logger

logger = get_logger("task_executor")

TaskHandler = Callable[..., Awaitable[Any]]
TaskRunner = Callable[[], Awaitable[Any]]

_shared_asyncio_executor: Optional["AsyncioQueueDiagnosisExecutor"] = None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class TaskSubmissionEnvelope:
    task_id: str
    runner: TaskRunner


def get_diagnosis_execution_settings(config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    config = config or {}
    diagnosis = dict(config.get("diagnosis") or {})
    execution = dict(diagnosis.get("execution") or {})
    backend = str(execution.get("backend") or "background_tasks").strip().lower()
    asyncio_workers_raw = execution.get("asyncio_workers", 2)
    try:
        asyncio_workers = max(1, int(asyncio_workers_raw))
    except (TypeError, ValueError):
        asyncio_workers = 2
    auto_resume_raw = execution.get("auto_resume_recovered", False)
    auto_resume_recovered = str(auto_resume_raw).strip().lower() in {"1", "true", "yes", "on"} if isinstance(auto_resume_raw, str) else bool(auto_resume_raw)
    return {
        "backend": backend,
        "asyncio_workers": asyncio_workers,
        "auto_resume_recovered": auto_resume_recovered,
    }


class DiagnosisTaskExecutor:
    backend_name = "unknown"
    durable = False
    process_isolated = False
    requires_worker = False

    def __init__(
        self,
        *,
        tracker: TaskTracker = task_tracker,
        requested_backend: Optional[str] = None,
        resolution_note: Optional[str] = None,
    ):
        self.tracker = tracker
        self.requested_backend = requested_backend or self.backend_name
        self.resolution_note = resolution_note

    def describe(self) -> Dict[str, Any]:
        return {
            "backend": self.backend_name,
            "requested_backend": self.requested_backend,
            "durable": self.durable,
            "process_isolated": self.process_isolated,
            "requires_worker": self.requires_worker,
            "resolution_note": self.resolution_note,
        }

    def _record_submission(self, task: TrackedTask, action: str) -> None:
        runtime = task.metadata.setdefault("task_runtime", {})
        runtime["executor"] = {
            **self.describe(),
            "submitted_at": _utc_now_iso(),
        }
        queued_task = self.tracker.mark_task_queued(task.task_id, action=action) or task
        workflow = queued_task.metadata.setdefault("workflow", {})
        workflow["status"] = "queued"
        workflow["current_stage"] = "queued"
        self.tracker.update_progress(queued_task.task_id, step=0, action=action, percentage=0.0)

    async def submit(self, task: TrackedTask, handler: TaskHandler, *args, **kwargs) -> None:
        raise NotImplementedError


class BackgroundTaskDiagnosisExecutor(DiagnosisTaskExecutor):
    backend_name = "background_tasks"

    def __init__(
        self,
        *,
        background_tasks: Optional[BackgroundTasks] = None,
        tracker: TaskTracker = task_tracker,
        requested_backend: Optional[str] = None,
        resolution_note: Optional[str] = None,
    ):
        super().__init__(
            tracker=tracker,
            requested_backend=requested_backend,
            resolution_note=resolution_note,
        )
        self.background_tasks = background_tasks

    def describe(self) -> Dict[str, Any]:
        summary = super().describe()
        summary.update(
            {
                "uses_fastapi_background_tasks": True,
                "queue_depth": len(getattr(self.background_tasks, "tasks", []) or []),
                "worker_count": 0,
            }
        )
        return summary

    async def submit(self, task: TrackedTask, handler: TaskHandler, *args, **kwargs) -> None:
        if self.background_tasks is None:
            raise RuntimeError("Background task executor requires FastAPI BackgroundTasks.")

        self.background_tasks.add_task(handler, task, *args, **kwargs)
        self._record_submission(task, "Queued via FastAPI background tasks")
        logger.info(f"submitted task {task.task_id} via background_tasks executor")


class AsyncioQueueDiagnosisExecutor(DiagnosisTaskExecutor):
    backend_name = "asyncio_queue"
    requires_worker = True

    def __init__(
        self,
        *,
        tracker: TaskTracker = task_tracker,
        max_workers: int = 2,
        requested_backend: Optional[str] = None,
        resolution_note: Optional[str] = None,
    ):
        super().__init__(
            tracker=tracker,
            requested_backend=requested_backend,
            resolution_note=resolution_note,
        )
        self.max_workers = max(1, int(max_workers))
        self._queue: asyncio.Queue[TaskSubmissionEnvelope] = asyncio.Queue()
        self._workers: list[asyncio.Task[Any]] = []
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def describe(self) -> Dict[str, Any]:
        summary = super().describe()
        summary.update(
            {
                "queue_depth": self._queue.qsize(),
                "worker_count": sum(1 for worker in self._workers if not worker.done()),
                "max_workers": self.max_workers,
            }
        )
        return summary

    async def _ensure_workers(self) -> None:
        loop = asyncio.get_running_loop()
        live_workers = [worker for worker in self._workers if not worker.done()]
        if self._loop is loop and len(live_workers) == self.max_workers:
            self._workers = live_workers
            return

        await self.shutdown()
        self._loop = loop
        self._workers = [
            loop.create_task(self._worker_loop(index), name=f"diagnosis-executor-{index}")
            for index in range(self.max_workers)
        ]
        logger.info(f"started asyncio diagnosis executor with {self.max_workers} workers")

    async def _worker_loop(self, worker_index: int) -> None:
        while True:
            envelope = await self._queue.get()
            try:
                await envelope.runner()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(f"diagnosis executor worker {worker_index} failed task {envelope.task_id}: {exc}")
            finally:
                self._queue.task_done()

    async def submit(self, task: TrackedTask, handler: TaskHandler, *args, **kwargs) -> None:
        await self._ensure_workers()
        await self._queue.put(
            TaskSubmissionEnvelope(
                task_id=task.task_id,
                runner=partial(handler, task, *args, **kwargs),
            )
        )
        self._record_submission(task, "Queued in asyncio diagnosis executor")
        logger.info(f"submitted task {task.task_id} via asyncio queue executor")

    async def shutdown(self) -> None:
        workers = [worker for worker in self._workers if not worker.done()]
        self._workers = []
        self._loop = None
        for worker in workers:
            worker.cancel()
        for worker in workers:
            try:
                await worker
            except asyncio.CancelledError:
                pass


def build_diagnosis_task_executor(
    config: Optional[Dict[str, Any]] = None,
    *,
    background_tasks: Optional[BackgroundTasks] = None,
    tracker: TaskTracker = task_tracker,
) -> DiagnosisTaskExecutor:
    settings = get_diagnosis_execution_settings(config)
    backend = settings["backend"]

    if backend == "asyncio_queue":
        if tracker is task_tracker:
            global _shared_asyncio_executor
            if _shared_asyncio_executor is None or _shared_asyncio_executor.max_workers != settings["asyncio_workers"]:
                _shared_asyncio_executor = AsyncioQueueDiagnosisExecutor(
                    tracker=tracker,
                    max_workers=settings["asyncio_workers"],
                    requested_backend=backend,
                )
            return _shared_asyncio_executor
        return AsyncioQueueDiagnosisExecutor(
            tracker=tracker,
            max_workers=settings["asyncio_workers"],
            requested_backend=backend,
        )

    if backend in {"background_tasks", "fastapi_background", "fastapi"}:
        return BackgroundTaskDiagnosisExecutor(
            background_tasks=background_tasks,
            tracker=tracker,
            requested_backend=backend,
        )

    logger.warning(f"unsupported diagnosis executor backend '{backend}', falling back to background_tasks")
    return BackgroundTaskDiagnosisExecutor(
        background_tasks=background_tasks,
        tracker=tracker,
        requested_backend=backend,
        resolution_note=f"Unsupported backend '{backend}', using background_tasks fallback.",
    )


async def shutdown_diagnosis_task_executor(tracker: TaskTracker = task_tracker) -> None:
    global _shared_asyncio_executor
    if tracker is task_tracker and _shared_asyncio_executor is not None:
        await _shared_asyncio_executor.shutdown()
