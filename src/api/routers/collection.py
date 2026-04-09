"""Collection APIs backed by persistent telemetry storage."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Sequence

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from src.api.dependencies import TenantContext, UserContext, get_tenant_context, require_permissions
from src.api.repositories.device_repository import DeviceRepository
from src.api.repositories.telemetry_repository import TelemetryRepository

router = APIRouter(prefix="/collection", tags=["collection"])
telemetry_repository = TelemetryRepository()
device_repository = DeviceRepository()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class DataPoint(BaseModel):
    timestamp: datetime
    value: float
    quality: str = "good"
    device_id: Optional[str] = None
    unit: Optional[str] = None
    source: Optional[str] = None


class TelemetryIngestPoint(BaseModel):
    device_id: str
    tag: str
    value: float
    timestamp: Optional[datetime] = None
    quality: str = "good"
    unit: Optional[str] = None
    source: Optional[str] = None


class TelemetryIngestRequest(BaseModel):
    points: List[TelemetryIngestPoint] = Field(..., min_length=1)


class DataQueryRequest(BaseModel):
    tags: List[str] = Field(..., min_length=1)
    start_time: datetime
    end_time: datetime
    device_ids: Optional[List[str]] = None
    aggregation: Optional[str] = Field("raw", pattern=r"^(raw|mean|sum|min|max)$")
    interval: Optional[str] = None
    limit_per_tag: int = Field(default=5000, ge=1, le=20000)


class DataQueryResponse(BaseModel):
    tags: List[str]
    start_time: datetime
    end_time: datetime
    data: Dict[str, List[DataPoint]]


class CollectionStartRequest(BaseModel):
    device_ids: Optional[List[str]] = None
    scan_interval: int = Field(default=10, ge=1, le=3600)


class CollectionStatusResponse(BaseModel):
    is_running: bool
    device_count: int
    device_ids: List[str] = Field(default_factory=list)
    scan_interval: int = 10
    last_data_time: Optional[datetime]
    throughput: float
    started_at: Optional[datetime] = None
    started_by: Optional[str] = None
    stopped_at: Optional[datetime] = None


def _resolve_collection_devices(*, tenant_id: str, requested_device_ids: Optional[Sequence[str]]) -> List[str]:
    if requested_device_ids:
        devices = device_repository.list_runtime_devices(
            tenant_id=tenant_id,
            device_ids=list(dict.fromkeys(requested_device_ids)),
            enabled_only=False,
        )
        found_ids = {device["id"] for device in devices}
        missing = [device_id for device_id in requested_device_ids if device_id not in found_ids]
        if missing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Devices not found in tenant scope: {', '.join(missing)}",
            )
        return [device["id"] for device in devices]

    enabled_devices = device_repository.list_runtime_devices(tenant_id=tenant_id, enabled_only=True)
    if not enabled_devices:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="No enabled devices available for collection",
        )
    return [device["id"] for device in enabled_devices]


def _aggregate_points(points: List[DataPoint], aggregation: str, interval: str) -> List[DataPoint]:
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
        sample = group_points[-1]
        aggregated.append(
            DataPoint(
                timestamp=bucket,
                value=round(value, 4),
                quality=sample.quality,
                device_id=sample.device_id if len({item.device_id for item in group_points}) == 1 else None,
                unit=sample.unit,
                source=sample.source,
            )
        )
    return aggregated


def _validate_ingest_points(*, tenant_id: str, points: Sequence[TelemetryIngestPoint]) -> None:
    unique_device_ids = list(dict.fromkeys(point.device_id for point in points))
    devices = device_repository.list_runtime_devices(
        tenant_id=tenant_id,
        device_ids=unique_device_ids,
        enabled_only=False,
    )
    found_devices = {device["id"]: device for device in devices}
    missing_devices = [device_id for device_id in unique_device_ids if device_id not in found_devices]
    if missing_devices:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Devices not found in tenant scope: {', '.join(missing_devices)}",
        )

    tag_catalog = {
        device_id: {tag["name"] for tag in device_repository.list_tags(device_id, tenant_id=tenant_id)}
        for device_id in unique_device_ids
    }
    invalid_pairs = [
        f"{point.device_id}:{point.tag}"
        for point in points
        if point.tag not in tag_catalog.get(point.device_id, set())
    ]
    if invalid_pairs:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f"Unknown device tags: {', '.join(invalid_pairs)}",
        )


def _default_tags_for_tenant(tenant_id: str) -> List[str]:
    runtime_devices = device_repository.list_runtime_devices(tenant_id=tenant_id, enabled_only=False)
    discovered: List[str] = []
    for device in runtime_devices:
        for tag in device_repository.list_tags(device["id"], tenant_id=tenant_id):
            name = str(tag["name"])
            if name not in discovered:
                discovered.append(name)
    return discovered[:20]


@router.post("/start")
async def start_collection(
    request: CollectionStartRequest,
    user: UserContext = Depends(require_permissions("device:write")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    device_ids = _resolve_collection_devices(
        tenant_id=tenant.tenant_id,
        requested_device_ids=request.device_ids,
    )
    started_at = utc_now()
    state = telemetry_repository.save_collection_state(
        tenant_id=tenant.tenant_id,
        is_running=True,
        device_ids=device_ids,
        scan_interval=request.scan_interval,
        started_by=user.user_id,
        started_at=started_at,
        stopped_at=None,
    )

    return {
        "success": True,
        "message": "Collection runtime started",
        "data": {
            **state,
            "tenant_id": tenant.tenant_id,
        },
    }


@router.post("/stop")
async def stop_collection(
    device_id: Optional[str] = Query(None),
    user: UserContext = Depends(require_permissions("device:write")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    current_state = telemetry_repository.get_collection_state(tenant_id=tenant.tenant_id)
    device_ids = list(current_state.get("device_ids", []))

    if device_id:
        if device_id not in device_ids:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Device {device_id} is not part of the active collection scope",
            )
        device_ids = [item for item in device_ids if item != device_id]
        is_running = len(device_ids) > 0
        message = f"Stopped collection for device {device_id}"
    else:
        device_ids = []
        is_running = False
        message = "Collection runtime stopped"

    state = telemetry_repository.save_collection_state(
        tenant_id=tenant.tenant_id,
        is_running=is_running,
        device_ids=device_ids,
        scan_interval=int(current_state.get("scan_interval") or 10),
        started_by=current_state.get("started_by"),
        started_at=current_state.get("started_at"),
        stopped_at=utc_now(),
    )

    return {
        "success": True,
        "message": message,
        "data": {
            **state,
            "stopped_by": user.user_id,
        },
    }


@router.get("/status", response_model=CollectionStatusResponse)
async def get_collection_status(
    user: UserContext = Depends(require_permissions("device:read")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    state = telemetry_repository.get_collection_state(tenant_id=tenant.tenant_id)
    summary = telemetry_repository.telemetry_summary(tenant_id=tenant.tenant_id)
    return CollectionStatusResponse(
        is_running=bool(state.get("is_running", False)),
        device_count=len(state.get("device_ids", [])),
        device_ids=list(state.get("device_ids", [])),
        scan_interval=int(state.get("scan_interval") or 10),
        last_data_time=summary.get("last_data_time"),
        throughput=float(summary.get("throughput") or 0.0),
        started_at=state.get("started_at"),
        started_by=state.get("started_by"),
        stopped_at=state.get("stopped_at"),
    )


@router.post("/data/ingest")
async def ingest_telemetry(
    request: TelemetryIngestRequest,
    user: UserContext = Depends(require_permissions("device:write")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    _validate_ingest_points(tenant_id=tenant.tenant_id, points=request.points)
    payload = []
    for point in request.points:
        payload.append(
            {
                "device_id": point.device_id,
                "tag": point.tag,
                "value": point.value,
                "timestamp": point.timestamp or utc_now(),
                "quality": point.quality,
                "unit": point.unit,
                "source": point.source or "collection_api",
            }
        )

    ingest_result = telemetry_repository.ingest_points(
        tenant_id=tenant.tenant_id,
        points=payload,
        source="collection_api",
    )

    touched_devices = list(dict.fromkeys(point.device_id for point in request.points))
    for device_id in touched_devices:
        device_repository.update_device(
            device_id,
            tenant_id=tenant.tenant_id,
            updates={"status": "online", "last_seen": utc_now()},
            updated_by=user.user_id,
        )

    return {
        "success": True,
        "message": "Telemetry points ingested",
        "data": {
            "count": ingest_result["count"],
            "last_recorded_at": ingest_result["last_recorded_at"],
            "tenant_id": tenant.tenant_id,
            "device_count": len(touched_devices),
        },
    }


@router.post("/data/query", response_model=DataQueryResponse)
async def query_data(
    request: DataQueryRequest,
    user: UserContext = Depends(require_permissions("data:read")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    if request.end_time <= request.start_time:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="end_time must be greater than start_time",
        )

    raw_result = telemetry_repository.query_points(
        tenant_id=tenant.tenant_id,
        tags=request.tags,
        start_time=request.start_time,
        end_time=request.end_time,
        device_ids=request.device_ids,
        limit_per_tag=request.limit_per_tag,
    )

    response_data: Dict[str, List[DataPoint]] = {}
    for tag, points in raw_result.items():
        normalized = [
            DataPoint(
                timestamp=point["timestamp"],
                value=float(point["value"]),
                quality=point.get("quality", "good"),
                device_id=point.get("device_id"),
                unit=point.get("unit"),
                source=point.get("source"),
            )
            for point in points
        ]
        if request.aggregation and request.aggregation != "raw":
            normalized = _aggregate_points(normalized, request.aggregation, request.interval or "10m")
        response_data[tag] = normalized

    return DataQueryResponse(
        tags=request.tags,
        start_time=request.start_time,
        end_time=request.end_time,
        data=response_data,
    )


@router.get("/data/latest")
async def get_latest_data(
    tags: Optional[List[str]] = Query(None),
    user: UserContext = Depends(require_permissions("data:read")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    target_tags = tags or _default_tags_for_tenant(tenant.tenant_id)
    latest = telemetry_repository.get_latest_points(
        tenant_id=tenant.tenant_id,
        tags=target_tags,
    )
    return {
        tag: {
            "timestamp": payload["timestamp"].isoformat() if isinstance(payload["timestamp"], datetime) else payload["timestamp"],
            "value": payload["value"],
            "quality": payload["quality"],
            "unit": payload.get("unit"),
            "device_id": payload.get("device_id"),
            "source": payload.get("source"),
        }
        for tag, payload in latest.items()
    }


@router.get("/data/realtime")
async def get_realtime_data(
    tag: str = Query(..., description="Tag name"),
    limit: int = Query(100, ge=1, le=1000),
    user: UserContext = Depends(require_permissions("data:read")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    points = telemetry_repository.get_recent_points(
        tenant_id=tenant.tenant_id,
        tag=tag,
        limit=limit,
    )
    return {
        "tag": tag,
        "count": len(points),
        "data": [
            {
                "timestamp": point["timestamp"].isoformat() if isinstance(point["timestamp"], datetime) else point["timestamp"],
                "value": point["value"],
                "quality": point["quality"],
                "device_id": point.get("device_id"),
                "unit": point.get("unit"),
                "source": point.get("source"),
            }
            for point in points
        ],
    }
