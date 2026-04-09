"""Runtime bootstrap and scheduler for industrial intelligence patrol."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from src.intelligence.service import IndustrialIntelligenceService
from src.utils.config import load_config
from src.utils.structured_logging import get_logger

logger = get_logger("intelligence.runtime")

_service: Optional[IndustrialIntelligenceService] = None
_scheduler: Optional["IntelligentPatrolScheduler"] = None
runtime_state: Dict[str, Any] = {
    "bootstrapped_at": None,
    "scheduler_enabled": False,
    "scheduler_running": False,
    "last_run_id": None,
    "last_error": None,
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class IntelligentPatrolScheduler:
    """Lightweight async scheduler for periodic patrol tasks."""

    def __init__(self, service: IndustrialIntelligenceService, *, interval_seconds: int, run_on_startup: bool = False):
        self.service = service
        self.interval_seconds = max(int(interval_seconds), 30)
        self.run_on_startup = bool(run_on_startup)
        self._loop_task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self.last_run_id: Optional[str] = None
        self.last_error: Optional[str] = None

    @property
    def is_running(self) -> bool:
        return self._loop_task is not None and not self._loop_task.done()

    async def start(self) -> None:
        if self.is_running:
            return
        self._stop_event = asyncio.Event()
        self._loop_task = asyncio.create_task(self._run_loop(), name="industrial-intelligence-patrol")

    async def stop(self) -> None:
        self._stop_event.set()
        if self._loop_task:
            await self._loop_task
        self._loop_task = None

    async def run_once(self) -> Dict[str, Any]:
        payload = await self.service.run_patrol(triggered_by="scheduler", schedule_type="scheduled")
        self.last_run_id = payload.get("run_id")
        self.last_error = None
        runtime_state["last_run_id"] = self.last_run_id
        runtime_state["last_error"] = None
        return payload

    async def _run_loop(self) -> None:
        if self.run_on_startup:
            await self._safe_run_once()

        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.interval_seconds)
            except asyncio.TimeoutError:
                await self._safe_run_once()

    async def _safe_run_once(self) -> None:
        try:
            await self.run_once()
        except Exception as exc:
            self.last_error = str(exc)
            runtime_state["last_error"] = str(exc)
            logger.warning(f"Scheduled industrial patrol failed: {exc}")


def get_intelligence_service() -> IndustrialIntelligenceService:
    global _service
    if _service is None:
        _service = IndustrialIntelligenceService(config=load_config())
    return _service


def get_patrol_scheduler() -> Optional[IntelligentPatrolScheduler]:
    return _scheduler


async def bootstrap_intelligence_runtime() -> Dict[str, Any]:
    global _service, _scheduler

    config = load_config()
    _service = _service or IndustrialIntelligenceService(config=config)
    patrol_config = (_service.runtime_config.get("patrol") or {})
    _scheduler = IntelligentPatrolScheduler(
        _service,
        interval_seconds=int(patrol_config.get("interval_seconds", 180)),
        run_on_startup=bool(patrol_config.get("run_on_startup", False)),
    )
    await _scheduler.start()

    runtime_state.update(
        {
            "bootstrapped_at": utc_now().isoformat(),
            "scheduler_enabled": True,
            "scheduler_running": _scheduler.is_running,
            "last_run_id": _scheduler.last_run_id,
            "last_error": _scheduler.last_error,
        }
    )
    return {
        "scheduler": {
            "interval_seconds": _scheduler.interval_seconds,
            "run_on_startup": _scheduler.run_on_startup,
            "running": _scheduler.is_running,
        },
        "service": _service.get_runtime_summary(),
    }


async def shutdown_intelligence_runtime() -> None:
    global _scheduler
    if _scheduler:
        await _scheduler.stop()
        runtime_state["scheduler_running"] = False
