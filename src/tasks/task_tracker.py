"""Persistent task tracking for long-running diagnosis workflows."""

from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from src.tasks.persistence import TaskPersistenceBackend, build_task_persistence_backend
from src.utils.config import load_config
from src.utils.structured_logging import get_logger
from src.utils.thread_safe import ThreadSafeDict

logger = get_logger("task_tracker")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    return datetime.fromisoformat(value)


class TaskStatus(Enum):
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


class TaskPriority(Enum):
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3


@dataclass
class TaskProgress:
    current_step: int = 0
    total_steps: int = 100
    current_action: str = ""
    percentage: float = 0.0
    estimated_remaining_seconds: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "current_step": self.current_step,
            "total_steps": self.total_steps,
            "current_action": self.current_action,
            "percentage": round(self.percentage, 1),
            "estimated_remaining_seconds": self.estimated_remaining_seconds,
        }

    @classmethod
    def from_dict(cls, payload: Optional[Dict[str, Any]]) -> "TaskProgress":
        payload = payload or {}
        return cls(
            current_step=int(payload.get("current_step", 0)),
            total_steps=int(payload.get("total_steps", 100)),
            current_action=str(payload.get("current_action", "")),
            percentage=float(payload.get("percentage", 0.0)),
            estimated_remaining_seconds=payload.get("estimated_remaining_seconds"),
        )


@dataclass
class TrackedTask:
    task_id: str
    task_type: str
    description: str
    status: TaskStatus
    priority: TaskPriority
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    progress: TaskProgress = field(default_factory=TaskProgress)
    result: Any = None
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    parent_task_id: Optional[str] = None
    sub_task_ids: List[str] = field(default_factory=list)
    on_progress: Optional[Callable] = None
    on_complete: Optional[Callable] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "task_type": self.task_type,
            "description": self.description,
            "status": self.status.value,
            "priority": self.priority.name,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "progress": self.progress.to_dict(),
            "result": self.result,
            "error": self.error,
            "metadata": self.metadata,
            "parent_task_id": self.parent_task_id,
            "sub_task_ids": self.sub_task_ids,
        }

    @classmethod
    def from_dict(cls, payload: Dict[str, Any]) -> "TrackedTask":
        return cls(
            task_id=str(payload["task_id"]),
            task_type=str(payload["task_type"]),
            description=str(payload["description"]),
            status=TaskStatus(payload["status"]),
            priority=TaskPriority[str(payload["priority"])],
            created_at=_parse_datetime(payload["created_at"]) or utc_now(),
            started_at=_parse_datetime(payload.get("started_at")),
            completed_at=_parse_datetime(payload.get("completed_at")),
            progress=TaskProgress.from_dict(payload.get("progress")),
            result=payload.get("result"),
            error=payload.get("error"),
            metadata=dict(payload.get("metadata") or {}),
            parent_task_id=payload.get("parent_task_id"),
            sub_task_ids=list(payload.get("sub_task_ids") or []),
        )

    def duration_seconds(self) -> Optional[float]:
        if not self.started_at:
            return None
        end = self.completed_at or utc_now()
        return (end - self.started_at).total_seconds()

    def is_active(self) -> bool:
        return self.status in {TaskStatus.PENDING, TaskStatus.QUEUED, TaskStatus.RUNNING, TaskStatus.PAUSED}

    def can_cancel(self) -> bool:
        return self.status in {TaskStatus.PENDING, TaskStatus.QUEUED, TaskStatus.RUNNING, TaskStatus.PAUSED}

    def can_retry(self) -> bool:
        return self.status in {TaskStatus.FAILED, TaskStatus.TIMEOUT, TaskStatus.CANCELLED}

    def can_resume(self) -> bool:
        recovery = dict(self.metadata.get("recovery") or {})
        recoverable_state = bool((self.metadata.get("task_runtime") or {}).get("recoverable_state", False))
        return bool(
            recoverable_state
            and self.status == TaskStatus.QUEUED
            and (recovery.get("restored_from_persistence") or recovery.get("resume_required"))
        )


class TaskTracker:
    """Tracks long-running tasks and persists their state to a pluggable backend."""

    def __init__(
        self,
        max_concurrent: int = 10,
        default_timeout: int = 3600,
        db_path: Optional[Path] = None,
        persistence_backend: Optional[TaskPersistenceBackend] = None,
    ):
        self.max_concurrent = max_concurrent
        self.default_timeout = default_timeout
        self._tasks: ThreadSafeDict = ThreadSafeDict()
        self._running_tasks: Dict[str, asyncio.Task] = {}
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._listeners: Dict[str, List[Callable]] = {"progress": [], "complete": [], "fail": []}
        self._persistence_backend = self._build_backend(db_path=db_path, persistence_backend=persistence_backend)
        self._persistence_backend.init()
        self._load_tasks()
        self._stats = self._build_stats()
        logger.info(f"task tracker initialized with {self.storage_label} store: {self.persistence_target}")

    @property
    def storage_label(self) -> str:
        return self._persistence_backend.storage_label

    @property
    def persistence_target(self) -> str:
        return self._persistence_backend.target

    def _build_backend(
        self,
        *,
        db_path: Optional[Path],
        persistence_backend: Optional[TaskPersistenceBackend],
    ) -> TaskPersistenceBackend:
        if persistence_backend is not None:
            return persistence_backend

        config = load_config().get("database", {})
        try:
            return build_task_persistence_backend(config, db_path=db_path)
        except Exception as exc:
            logger.warning(f"task tracker failed to initialize configured backend, falling back to sqlite: {exc}")
            fallback_config = {
                "task_tracking": {
                    "backend": "sqlite",
                    "sqlite_path": (config.get("task_tracking") or {}).get("sqlite_path", "data/runtime/tasks.sqlite"),
                }
            }
            return build_task_persistence_backend(fallback_config, db_path=db_path)

    def _load_tasks(self) -> None:
        for payload in self._persistence_backend.load_payloads():
            task = TrackedTask.from_dict(payload)
            recovery = task.metadata.setdefault("recovery", {})
            task_runtime = dict(task.metadata.get("task_runtime") or {})
            recoverable_state = bool(task_runtime.get("recoverable_state", False))

            if task.status in {TaskStatus.PENDING, TaskStatus.QUEUED} and recoverable_state:
                task.status = TaskStatus.QUEUED
                recovery["restored_from_persistence"] = True
                recovery["resume_required"] = True
                task.metadata.setdefault("workflow", {})
                task.metadata["workflow"]["status"] = "queued"
                task.metadata["workflow"]["current_stage"] = "queued"
                task.progress.current_action = task.progress.current_action or "Awaiting manual resume"
            elif task.status in {TaskStatus.PENDING, TaskStatus.QUEUED, TaskStatus.RUNNING, TaskStatus.PAUSED}:
                task.status = TaskStatus.FAILED
                task.error = "Task interrupted because the tracker process restarted."
                task.completed_at = utc_now()
                recovery["restored_from_persistence"] = True
            self._tasks.set(task.task_id, task)
            self._persist_task(task)

    def _persist_task(self, task: TrackedTask) -> None:
        self._persistence_backend.persist_payload(
            task.task_id,
            task.status.value,
            task.created_at.isoformat(),
            task.to_dict(),
        )

    def _build_stats(self) -> Dict[str, int]:
        tasks = self._tasks.values()
        return {
            "total_created": len(tasks),
            "total_queued": sum(1 for item in tasks if item.status == TaskStatus.QUEUED),
            "total_completed": sum(1 for item in tasks if item.status == TaskStatus.COMPLETED),
            "total_failed": sum(1 for item in tasks if item.status in {TaskStatus.FAILED, TaskStatus.TIMEOUT}),
            "total_cancelled": sum(1 for item in tasks if item.status == TaskStatus.CANCELLED),
        }

    def create_task(
        self,
        task_type: str,
        description: str,
        priority: TaskPriority = TaskPriority.NORMAL,
        metadata: Optional[Dict[str, Any]] = None,
        parent_task_id: Optional[str] = None,
    ) -> TrackedTask:
        metadata = dict(metadata or {})
        metadata.setdefault("retry_count", int(metadata.get("retry_count", 0) or 0))
        metadata.setdefault("retry_of_task_id", metadata.get("retry_of_task_id"))
        metadata.setdefault("cancel_requested_at", metadata.get("cancel_requested_at"))
        metadata.setdefault("cancelled_by", metadata.get("cancelled_by"))
        metadata.setdefault("cancellation_reason", metadata.get("cancellation_reason"))
        metadata.setdefault(
                "task_runtime",
                {
                    "storage": self.storage_label,
                    "persistent": True,
                    "auto_resume": False,
                    "recoverable_state": True,
                    "default_timeout_seconds": self.default_timeout,
                    "target": self.persistence_target,
                },
            )
        task = TrackedTask(
            task_id=f"TASK_{uuid.uuid4().hex[:12].upper()}",
            task_type=task_type,
            description=description,
            status=TaskStatus.PENDING,
            priority=priority,
            created_at=utc_now(),
            metadata=metadata,
            parent_task_id=parent_task_id,
        )
        self._tasks.set(task.task_id, task)
        self._stats["total_created"] += 1
        self._persist_task(task)
        logger.info(f"created task {task.task_id}: {description}")
        return task

    async def execute(self, task: TrackedTask, coro_func: Callable, *args, **kwargs) -> Any:
        if task.status == TaskStatus.CANCELLED:
            self._persist_task(task)
            logger.info(f"skipped execution for cancelled task {task.task_id}")
            return task.result

        async with self._semaphore:
            if task.status == TaskStatus.CANCELLED:
                self._persist_task(task)
                logger.info(f"skipped execution for cancelled task {task.task_id}")
                return task.result
            task.status = TaskStatus.RUNNING
            task.started_at = utc_now()
            self._persist_task(task)
            logger.info(f"started task {task.task_id}")
            try:
                async_task = asyncio.create_task(self._run_with_timeout(task, coro_func, *args, **kwargs))
                self._running_tasks[task.task_id] = async_task
                result = await async_task
                task.status = TaskStatus.COMPLETED
                task.result = result
                task.completed_at = utc_now()
                self._stats["total_completed"] += 1
                self._persist_task(task)
                if task.on_complete:
                    await task.on_complete(task)
                self._trigger_event("complete", task)
                logger.info(f"completed task {task.task_id}")
                return result
            except asyncio.CancelledError:
                if task.status != TaskStatus.CANCELLED:
                    task.status = TaskStatus.CANCELLED
                    task.error = task.error or "Task cancelled by user."
                    task.completed_at = utc_now()
                    self._stats["total_cancelled"] += 1
                self._persist_task(task)
                logger.info(f"task cancelled {task.task_id}")
                raise
            except asyncio.TimeoutError:
                task.status = TaskStatus.TIMEOUT
                task.error = "Task execution timed out."
                task.completed_at = utc_now()
                self._stats["total_failed"] += 1
                self._persist_task(task)
                self._trigger_event("fail", task)
                logger.warning(f"task timed out {task.task_id}")
                raise
            except Exception as exc:
                task.status = TaskStatus.FAILED
                task.error = str(exc)
                task.completed_at = utc_now()
                self._stats["total_failed"] += 1
                self._persist_task(task)
                self._trigger_event("fail", task)
                logger.error(f"task failed {task.task_id}: {exc}")
                raise
            finally:
                self._running_tasks.pop(task.task_id, None)

    async def _run_with_timeout(self, task: TrackedTask, coro_func: Callable, *args, **kwargs) -> Any:
        timeout_seconds = int(task.metadata.get("timeout_seconds", self.default_timeout))
        return await asyncio.wait_for(coro_func(task, *args, **kwargs), timeout=timeout_seconds)

    def update_progress(
        self,
        task_id: str,
        step: Optional[int] = None,
        action: Optional[str] = None,
        percentage: Optional[float] = None,
    ) -> None:
        task = self._tasks.get(task_id)
        if not task:
            return
        if task.status in {TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.TIMEOUT, TaskStatus.CANCELLED}:
            return
        if step is not None:
            task.progress.current_step = step
        if action is not None:
            task.progress.current_action = action
        if percentage is not None:
            task.progress.percentage = percentage
        self._persist_task(task)
        if task.on_progress:
            asyncio.create_task(task.on_progress(task))
        self._trigger_event("progress", task)

    def get_task(self, task_id: str) -> Optional[TrackedTask]:
        return self._tasks.get(task_id)

    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        task = self._tasks.get(task_id)
        if not task:
            return None
        return {
            "task_id": task.task_id,
            "status": task.status.value,
            "progress": task.progress.to_dict(),
            "duration_seconds": task.duration_seconds(),
        }

    def mark_task_queued(self, task_id: str, *, action: Optional[str] = None) -> Optional[TrackedTask]:
        task = self._tasks.get(task_id)
        if not task:
            return None
        if task.status == TaskStatus.CANCELLED:
            self._persist_task(task)
            return task
        if task.status in {TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.TIMEOUT, TaskStatus.RUNNING, TaskStatus.PAUSED}:
            return task

        task.status = TaskStatus.QUEUED
        workflow = task.metadata.setdefault("workflow", {})
        workflow["status"] = "queued"
        workflow["current_stage"] = "queued"
        if action is not None:
            task.progress.current_action = action
        task.progress.percentage = min(float(task.progress.percentage or 0.0), 1.0)
        self._persist_task(task)
        return task

    def list_tasks(
        self,
        status: Optional[TaskStatus] = None,
        task_type: Optional[str] = None,
        limit: int = 100,
    ) -> List[TrackedTask]:
        tasks = []
        for task in self._tasks.values():
            if status and task.status != status:
                continue
            if task_type and task.task_type != task_type:
                continue
            tasks.append(task)
        tasks.sort(key=lambda item: (item.priority.value, -item.created_at.timestamp()))
        return tasks[:limit]

    async def cancel_task(
        self,
        task_id: str,
        *,
        cancelled_by: Optional[str] = None,
        reason: Optional[str] = None,
    ) -> Optional[TrackedTask]:
        task = self._tasks.get(task_id)
        if not task:
            return None
        if task.status == TaskStatus.CANCELLED:
            return task
        if not task.can_cancel():
            return None

        cancellation_timestamp = utc_now()
        task.metadata["cancel_requested_at"] = cancellation_timestamp.isoformat()
        task.metadata["cancelled_by"] = cancelled_by
        task.metadata["cancellation_reason"] = reason
        task.progress.current_action = "Cancellation requested"
        running_task = self._running_tasks.get(task_id)
        if running_task:
            running_task.cancel()
            try:
                await running_task
            except asyncio.CancelledError:
                pass
        task.status = TaskStatus.CANCELLED
        task.error = reason or "Task cancelled by user."
        task.completed_at = task.completed_at or cancellation_timestamp
        self._stats["total_cancelled"] += 1
        self._persist_task(task)
        logger.info(f"cancelled task {task_id}")
        return task

    def add_listener(self, event_type: str, callback: Callable) -> None:
        if event_type in self._listeners:
            self._listeners[event_type].append(callback)

    def _trigger_event(self, event_type: str, task: TrackedTask) -> None:
        for callback in self._listeners.get(event_type, []):
            try:
                if asyncio.iscoroutinefunction(callback):
                    asyncio.create_task(callback(task))
                else:
                    callback(task)
            except Exception as exc:
                logger.error(f"task event handler failed: {exc}")

    def get_stats(self) -> Dict[str, Any]:
        total_finished = self._stats["total_completed"] + self._stats["total_failed"]
        return {
            "total_created": self._stats["total_created"],
            "total_queued": sum(1 for task in self._tasks.values() if task.status == TaskStatus.QUEUED),
            "total_completed": self._stats["total_completed"],
            "total_failed": self._stats["total_failed"],
            "total_cancelled": self._stats["total_cancelled"],
            "active_tasks": sum(1 for task in self._tasks.values() if task.is_active()),
            "running_tasks": len(self._running_tasks),
            "success_rate": (self._stats["total_completed"] / total_finished) if total_finished else 0.0,
            "persistence_path": self.persistence_target,
            "storage": self.storage_label,
        }

    def create_subtask(
        self,
        parent_task_id: str,
        description: str,
        task_type: str = "subtask",
    ) -> Optional[TrackedTask]:
        parent = self._tasks.get(parent_task_id)
        if not parent:
            return None
        subtask = self.create_task(
            task_type=task_type,
            description=description,
            priority=parent.priority,
            parent_task_id=parent_task_id,
        )
        parent.sub_task_ids.append(subtask.task_id)
        self._persist_task(parent)
        return subtask

    async def execute_with_progress(
        self,
        task: TrackedTask,
        steps: List[Dict[str, Any]],
        step_func: Callable,
    ) -> Any:
        task.progress.total_steps = max(len(steps), 1)
        self._persist_task(task)
        results = []
        for index, step in enumerate(steps, start=1):
            self.update_progress(
                task.task_id,
                step=index,
                action=step.get("name", f"Step {index}"),
                percentage=((index - 1) / max(len(steps), 1)) * 100,
            )
            results.append(await step_func(task, step))
        self.update_progress(task.task_id, step=len(steps), percentage=100)
        return results


task_tracker = TaskTracker()
