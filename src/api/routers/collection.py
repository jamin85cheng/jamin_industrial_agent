"""数据采集 API。"""

from __future__ import annotations

import math
import random
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field

from src.api.dependencies import TenantContext, UserContext, get_tenant_context, require_permissions

router = APIRouter(prefix="/collection", tags=["数据采集"])


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class DataPoint(BaseModel):
    """采集点数据。"""

    timestamp: datetime
    value: float
    quality: str = "good"


class DataQueryRequest(BaseModel):
    """历史数据查询请求。"""

    tags: List[str] = Field(..., min_length=1)
    start_time: datetime
    end_time: datetime
    aggregation: Optional[str] = Field("raw", pattern=r"^(raw|mean|sum|min|max)$")
    interval: Optional[str] = None


class DataQueryResponse(BaseModel):
    """历史数据查询响应。"""

    tags: List[str]
    start_time: datetime
    end_time: datetime
    data: Dict[str, List[DataPoint]]


class CollectionStartRequest(BaseModel):
    """启动采集请求。"""

    device_ids: Optional[List[str]] = None
    scan_interval: int = Field(default=10, ge=1, le=3600)


class CollectionStatusResponse(BaseModel):
    """采集状态响应。"""

    is_running: bool
    device_count: int
    last_data_time: Optional[datetime]
    throughput: float


_collection_status = {
    "is_running": False,
    "devices": {},
    "last_data_time": None,
    "start_time": None,
}

_data_store: Dict[str, List[Dict]] = {}


@router.post("/start")
async def start_collection(
    request: CollectionStartRequest,
    user: UserContext = Depends(require_permissions("device:write")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    """启动数据采集。"""
    started_at = utc_now()
    _collection_status["is_running"] = True
    _collection_status["start_time"] = started_at
    _collection_status["devices"] = {device_id: {"tenant_id": tenant.tenant_id} for device_id in (request.device_ids or [])}

    return {
        "success": True,
        "message": "数据采集已启动",
        "data": {
            "is_running": True,
            "scan_interval": request.scan_interval,
            "started_at": started_at.isoformat(),
            "tenant_id": tenant.tenant_id,
        },
    }


@router.post("/stop")
async def stop_collection(
    device_id: Optional[str] = Query(None),
    user: UserContext = Depends(require_permissions("device:write")),
):
    """停止数据采集。"""
    _collection_status["is_running"] = False
    if device_id:
        _collection_status.get("devices", {}).pop(device_id, None)
        message = f"设备 {device_id} 的数据采集已停止"
    else:
        _collection_status["devices"] = {}
        message = "数据采集已停止"

    return {
        "success": True,
        "message": message,
        "data": {"is_running": False},
    }


@router.get("/status", response_model=CollectionStatusResponse)
async def get_collection_status(
    user: UserContext = Depends(require_permissions("device:read")),
):
    """获取采集状态。"""
    throughput = 150.5 if _collection_status["is_running"] else 0.0
    return CollectionStatusResponse(
        is_running=_collection_status["is_running"],
        device_count=len(_collection_status.get("devices", {})),
        last_data_time=_collection_status.get("last_data_time"),
        throughput=throughput,
    )


@router.post("/data/query", response_model=DataQueryResponse)
async def query_data(
    request: DataQueryRequest,
    user: UserContext = Depends(require_permissions("data:read")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    """查询历史数据。"""
    result_data: Dict[str, List[DataPoint]] = {}

    for tag in request.tags:
        points: List[DataPoint] = []
        current_time = request.start_time
        while current_time <= request.end_time:
            base_value = 25.0
            variation = 10.0 * math.sin(current_time.timestamp() / 3600)
            noise = (hash(current_time.isoformat()) % 100) / 100.0 * 2 - 1
            points.append(
                DataPoint(
                    timestamp=current_time,
                    value=base_value + variation + noise,
                    quality="good",
                )
            )
            current_time += timedelta(minutes=1)

        if request.aggregation != "raw" and request.interval:
            points = _aggregate_points(points, request.aggregation, request.interval)
        result_data[tag] = points

    return DataQueryResponse(
        tags=request.tags,
        start_time=request.start_time,
        end_time=request.end_time,
        data=result_data,
    )


def _aggregate_points(points: List[DataPoint], aggregation: str, interval: str) -> List[DataPoint]:
    """按时间窗口聚合数据点。"""
    grouped: Dict[datetime, List[DataPoint]] = {}

    if interval.endswith("m"):
        bucket_minutes = max(int(interval[:-1]), 1)
    elif interval.endswith("h"):
        bucket_minutes = max(int(interval[:-1]) * 60, 60)
    else:
        bucket_minutes = 60

    for point in points:
        bucket = point.timestamp.replace(second=0, microsecond=0)
        minute = (bucket.minute // bucket_minutes) * bucket_minutes
        bucket = bucket.replace(minute=minute)
        grouped.setdefault(bucket, []).append(point)

    aggregated: List[DataPoint] = []
    for bucket, group_points in sorted(grouped.items()):
        values = [item.value for item in group_points]
        if aggregation == "sum":
            value = sum(values)
        elif aggregation == "min":
            value = min(values)
        elif aggregation == "max":
            value = max(values)
        else:
            value = sum(values) / len(values)
        aggregated.append(DataPoint(timestamp=bucket, value=round(value, 2), quality="good"))
    return aggregated


@router.get("/data/latest")
async def get_latest_data(
    tags: Optional[List[str]] = Query(None),
    user: UserContext = Depends(require_permissions("data:read")),
):
    """获取最新数据。"""
    target_tags = tags or ["temperature", "pressure", "flow"]
    now = utc_now()
    result: Dict[str, Dict[str, object]] = {}

    for tag in target_tags:
        result[tag] = {
            "timestamp": now.isoformat(),
            "value": round(random.uniform(20.0, 50.0), 2),
            "quality": "good",
            "unit": "°C" if "temp" in tag.lower() else "bar",
        }
    return result


@router.get("/data/realtime")
async def get_realtime_data(
    tag: str = Query(..., description="数据点位名称"),
    limit: int = Query(100, ge=1, le=1000),
    user: UserContext = Depends(require_permissions("data:read")),
):
    """获取最近一段实时数据。"""
    points = []
    now = utc_now()
    for index in range(limit):
        timestamp = now - timedelta(seconds=limit - index)
        base = 30.0
        trend = index / max(limit, 1) * 5
        noise = random.gauss(0, 1)
        points.append(
            {
                "timestamp": timestamp.isoformat(),
                "value": round(base + trend + noise, 2),
                "quality": "good",
            }
        )

    _collection_status["last_data_time"] = now
    _data_store.setdefault(tag, []).extend(points[-10:])
    _data_store[tag] = _data_store[tag][-500:]

    return {
        "tag": tag,
        "count": len(points),
        "data": points,
    }
