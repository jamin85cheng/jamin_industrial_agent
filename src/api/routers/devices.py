"""设备管理 API。"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from src.api.dependencies import TenantContext, UserContext, get_tenant_context, require_permissions
from src.api.repositories.device_repository import DeviceRepository

router = APIRouter(prefix="/devices", tags=["设备管理"])
device_repository = DeviceRepository()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def init_default_devices():
    """初始化设备表结构并写入演示设备。"""
    device_repository.init_schema()
    device_repository.seed_demo_devices()


class DeviceTag(BaseModel):
    """设备点位。"""

    name: str
    address: str
    data_type: str = "float"
    unit: Optional[str] = None
    description: Optional[str] = None


class DeviceCreateRequest(BaseModel):
    """创建设备请求。"""

    name: str = Field(..., min_length=1, max_length=100)
    type: str = Field(..., pattern=r"^(s7|modbus)$")
    host: str = Field(..., pattern=r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$")
    port: int = Field(..., ge=1, le=65535)
    rack: Optional[int] = 0
    slot: Optional[int] = 1
    scan_interval: int = Field(default=10, ge=1, le=3600)
    tags: List[DeviceTag] = Field(default_factory=list)


class DeviceUpdateRequest(BaseModel):
    """更新设备请求。"""

    name: Optional[str] = Field(None, min_length=1, max_length=100)
    host: Optional[str] = None
    port: Optional[int] = Field(None, ge=1, le=65535)
    rack: Optional[int] = None
    slot: Optional[int] = None
    scan_interval: Optional[int] = Field(None, ge=1, le=3600)
    enabled: Optional[bool] = None


class Device(BaseModel):
    """设备模型。"""

    id: str
    name: str
    type: str
    host: str
    port: int
    status: str
    last_seen: Optional[datetime] = None
    tag_count: int = 0
    created_at: datetime
    updated_at: datetime
    tenant_id: Optional[str] = None


class DeviceListResponse(BaseModel):
    """设备列表响应。"""

    total: int
    devices: List[Device]


@router.get("", response_model=DeviceListResponse)
async def list_devices(
    type: Optional[str] = Query(None, pattern=r"^(s7|modbus)$"),
    status: Optional[str] = Query(None, pattern=r"^(online|offline|error)$"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    user: UserContext = Depends(require_permissions("device:read")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    """获取设备列表。"""
    result = device_repository.list_devices(
        tenant_id=tenant.tenant_id,
        device_type=type,
        status=status,
        skip=skip,
        limit=limit,
    )
    return DeviceListResponse(
        total=result["total"],
        devices=[Device(**device) for device in result["devices"]],
    )


@router.post("", response_model=Device, status_code=status.HTTP_201_CREATED)
async def create_device(
    request: DeviceCreateRequest,
    user: UserContext = Depends(require_permissions("device:write")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    """创建设备。"""
    import uuid

    device_id = f"DEV_{uuid.uuid4().hex[:8].upper()}"
    now = utc_now()
    created = device_repository.create_device(
        device={
            "id": device_id,
            "name": request.name,
            "type": request.type,
            "host": request.host,
            "port": request.port,
            "rack": request.rack or 0,
            "slot": request.slot or 1,
            "scan_interval": request.scan_interval,
            "status": "offline",
            "enabled": True,
            "last_seen": None,
            "created_at": now,
            "updated_at": now,
            "tenant_id": tenant.tenant_id,
            "created_by": user.user_id,
            "updated_by": user.user_id,
        },
        tags=[tag.model_dump() for tag in request.tags],
    )
    return Device(**created)


@router.get("/{device_id}", response_model=Device)
async def get_device(
    device_id: str,
    user: UserContext = Depends(require_permissions("device:read")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    """获取设备详情。"""
    device = device_repository.get_device(device_id, tenant_id=tenant.tenant_id)
    if not device:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"设备 {device_id} 不存在")
    return Device(**device)


@router.put("/{device_id}", response_model=Device)
async def update_device(
    device_id: str,
    request: DeviceUpdateRequest,
    user: UserContext = Depends(require_permissions("device:write")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    """更新设备。"""
    updated = device_repository.update_device(
        device_id,
        tenant_id=tenant.tenant_id,
        updates=request.model_dump(exclude_unset=True),
        updated_by=user.user_id,
    )
    if not updated:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"设备 {device_id} 不存在")
    return Device(**updated)


@router.delete("/{device_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_device(
    device_id: str,
    user: UserContext = Depends(require_permissions("device:write")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    """删除设备。"""
    deleted = device_repository.delete_device(device_id, tenant_id=tenant.tenant_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"设备 {device_id} 不存在")
    return None


@router.get("/{device_id}/tags", response_model=List[DeviceTag])
async def get_device_tags(
    device_id: str,
    user: UserContext = Depends(require_permissions("device:read")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    """获取设备点位。"""
    device = device_repository.get_device(device_id, tenant_id=tenant.tenant_id)
    if not device:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"设备 {device_id} 不存在")
    return [DeviceTag(**tag) for tag in device_repository.list_tags(device_id, tenant_id=tenant.tenant_id)]


@router.post("/{device_id}/connect")
async def connect_device(
    device_id: str,
    user: UserContext = Depends(require_permissions("device:write")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    """手动连接设备。"""
    device = device_repository.set_connection_state(
        device_id,
        tenant_id=tenant.tenant_id,
        status="online",
        updated_by=user.user_id,
    )
    if not device:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"设备 {device_id} 不存在")
    return {"status": "connected", "device_id": device_id}


@router.post("/{device_id}/disconnect")
async def disconnect_device(
    device_id: str,
    user: UserContext = Depends(require_permissions("device:write")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    """断开设备连接。"""
    device = device_repository.set_connection_state(
        device_id,
        tenant_id=tenant.tenant_id,
        status="offline",
        updated_by=user.user_id,
    )
    if not device:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"设备 {device_id} 不存在")
    return {"status": "disconnected", "device_id": device_id}
