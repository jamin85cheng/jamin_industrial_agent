"""
Health check API routes.
"""

from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict

from fastapi import APIRouter

from src.utils.health_check import HealthStatus, get_health_checker, init_default_checks

try:
    import psutil
except ImportError:
    psutil = None

router = APIRouter(prefix="/health", tags=["health"])
health_checker = get_health_checker()
_health_checks_initialized = False


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _run_checks():
    global _health_checks_initialized
    if not _health_checks_initialized:
        init_default_checks()
        _health_checks_initialized = True
    return await asyncio.to_thread(health_checker.check)


def _overall_status(results: Dict[str, Any]) -> HealthStatus:
    status = HealthStatus.HEALTHY
    for result in results.values():
        if result.status == HealthStatus.UNHEALTHY:
            return HealthStatus.UNHEALTHY
        if result.status == HealthStatus.DEGRADED:
            status = HealthStatus.DEGRADED
    return status


@router.get("", response_model=Dict[str, Any])
async def health_check():
    started = time.time()
    results = await _run_checks()
    overall = _overall_status(results)

    return {
        "status": overall.value,
        "timestamp": utc_now_iso(),
        "version": "v1.0.0-beta2",
        "response_time_ms": round((time.time() - started) * 1000, 2),
        "checks": {
            name: {
                "status": result.status.value,
                "response_time_ms": round(result.response_time_ms, 2),
                "message": result.message,
                "details": result.details,
            }
            for name, result in results.items()
        },
    }


@router.get("/ready")
async def readiness_check():
    results = await _run_checks()
    critical_components = {"database", "influxdb", "redis"}
    for component in critical_components:
        result = results.get(component)
        if result and result.status == HealthStatus.UNHEALTHY:
            return {"status": "not_ready", "reason": f"{component} unhealthy"}
    return {"status": "ready"}


@router.get("/live")
async def liveness_check():
    return {"status": "alive"}


@router.get("/detailed")
async def detailed_health_check():
    started = time.time()
    results = await _run_checks()
    overall = _overall_status(results)

    system_info = {
        "cpu_percent": psutil.cpu_percent(interval=0.1) if psutil else None,
        "memory_percent": psutil.virtual_memory().percent if psutil else None,
        "disk_usage": psutil.disk_usage(os.getcwd()).percent if psutil else None,
        "load_average": os.getloadavg() if hasattr(os, "getloadavg") else None,
        "process_count": len(psutil.pids()) if psutil else None,
    }

    return {
        "status": overall.value,
        "timestamp": utc_now_iso(),
        "response_time_ms": round((time.time() - started) * 1000, 2),
        "system": system_info,
        "components": {
            name: {
                "status": result.status.value,
                "response_time_ms": round(result.response_time_ms, 2),
                "message": result.message,
                "details": result.details,
            }
            for name, result in results.items()
        },
    }
