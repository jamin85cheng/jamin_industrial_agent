"""
设备管理API路由

实现设备CRUD、状态查询等接口
"""

from fastapi import APIRouter, Depends, HTTPException, Query, status
from typing import List, Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field

from src.api.dependencies import (
    get_current_user, require_permissions, get_tenant_context,
    UserContext, TenantContext
)
from src.utils.thread_safe import ThreadSafeDict

router = APIRouter(prefix="/devices", tags=["设备管理"])

# 内存设备存储（生产环境应使用数据库）
_devices = ThreadSafeDict()
_device_tags = ThreadSafeDict()


def init_default_devices():
    """Initialize demo devices for local development and UI validation."""
    if _devices.size() > 0:
        return

    now = datetime.utcnow()
    demo_devices = [
        {
            "id": "DEV_AERATION_01",
            "name": "1#曝气池",
            "type": "s7",
            "host": "192.168.1.100",
            "port": 102,
            "status": "online",
            "last_seen": now,
            "created_at": now,
            "updated_at": now,
            "tenant_id": "default",
        },
        {
            "id": "DEV_AERATION_02",
            "name": "2#曝气池",
            "type": "s7",
            "host": "192.168.1.101",
            "port": 102,
            "status": "online",
            "last_seen": now,
            "created_at": now,
            "updated_at": now,
            "tenant_id": "default",
        },
        {
            "id": "DEV_BLOWER_01",
            "name": "鼓风机",
            "type": "modbus",
            "host": "192.168.1.120",
            "port": 502,
            "status": "error",
            "last_seen": now,
            "created_at": now,
            "updated_at": now,
            "tenant_id": "default",
        },
    ]

    demo_tags = {
        "DEV_AERATION_01": [
            {"name": "DO", "address": "DB1.DBW0", "data_type": "float", "unit": "mg/L"},
            {"name": "pH", "address": "DB1.DBW2", "data_type": "float", "unit": ""},
        ],
        "DEV_AERATION_02": [
            {"name": "DO", "address": "DB1.DBW4", "data_type": "float", "unit": "mg/L"},
            {"name": "pH", "address": "DB1.DBW6", "data_type": "float", "unit": ""},
        ],
        "DEV_BLOWER_01": [
            {"name": "current", "address": "40001", "data_type": "float", "unit": "A"},
            {"name": "vibration", "address": "40002", "data_type": "float", "unit": "mm/s"},
            {"name": "temperature", "address": "40003", "data_type": "float", "unit": "C"},
        ],
    }

    for device in demo_devices:
        device_id = device["id"]
        _devices.set(device_id, device)
        _device_tags.set(device_id, demo_tags.get(device_id, []))


# Pydantic模型
class DeviceTag(BaseModel):
    """设备点位"""
    name: str
    address: str
    data_type: str = "float"
    unit: Optional[str] = None
    description: Optional[str] = None


class DeviceCreateRequest(BaseModel):
    """创建设备请求"""
    name: str = Field(..., min_length=1, max_length=100)
    type: str = Field(..., pattern=r"^(s7|modbus)$")
    host: str = Field(..., pattern=r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$")
    port: int = Field(..., ge=1, le=65535)
    rack: Optional[int] = 0
    slot: Optional[int] = 1
    scan_interval: int = Field(default=10, ge=1, le=3600)
    tags: List[DeviceTag] = Field(default_factory=list)


class DeviceUpdateRequest(BaseModel):
    """更新设备请求"""
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    host: Optional[str] = None
    port: Optional[int] = Field(None, ge=1, le=65535)
    rack: Optional[int] = None
    slot: Optional[int] = None
    scan_interval: Optional[int] = Field(None, ge=1, le=3600)
    enabled: Optional[bool] = None


class Device(BaseModel):
    """设备模型"""
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
    """设备列表响应"""
    total: int
    devices: List[Device]


@router.get("", response_model=DeviceListResponse)
async def list_devices(
    type: Optional[str] = Query(None, pattern=r"^(s7|modbus)$"),
    status: Optional[str] = Query(None, pattern=r"^(online|offline|error)$"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    user: UserContext = Depends(require_permissions("device:read")),
    tenant: TenantContext = Depends(get_tenant_context)
):
    """
    获取设备列表
    
    - type: 按设备类型过滤
    - status: 按状态过滤
    """
    devices = []
    
    for device_id in _devices.keys():
        device = _devices.get(device_id)
        if not device:
            continue
        
        # 租户过滤
        if device.get("tenant_id") != tenant.tenant_id:
            continue
        
        # 类型过滤
        if type and device.get("type") != type:
            continue
        
        # 状态过滤
        if status and device.get("status") != status:
            continue
        
        # 计算点位数量
        tags = _device_tags.get(device_id, [])
        
        devices.append(Device(
            id=device_id,
            name=device["name"],
            type=device["type"],
            host=device["host"],
            port=device["port"],
            status=device.get("status", "offline"),
            last_seen=device.get("last_seen"),
            tag_count=len(tags),
            created_at=device["created_at"],
            updated_at=device["updated_at"],
            tenant_id=device.get("tenant_id")
        ))
    
    # 分页
    total = len(devices)
    devices = devices[skip:skip + limit]
    
    return DeviceListResponse(total=total, devices=devices)


@router.post("", response_model=Device, status_code=status.HTTP_201_CREATED)
async def create_device(
    request: DeviceCreateRequest,
    user: UserContext = Depends(require_permissions("device:write")),
    tenant: TenantContext = Depends(get_tenant_context)
):
    """创建设备"""
    import uuid
    
    device_id = f"DEV_{uuid.uuid4().hex[:8].upper()}"
    now = datetime.utcnow()
    
    device_data = {
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
        "created_at": now,
        "updated_at": now,
        "tenant_id": tenant.tenant_id,
        "created_by": user.user_id
    }
    
    _devices.set(device_id, device_data)
    
    # 保存点位
    if request.tags:
        _device_tags.set(device_id, [tag.dict() for tag in request.tags])
    
    return Device(
        id=device_id,
        name=request.name,
        type=request.type,
        host=request.host,
        port=request.port,
        status="offline",
        tag_count=len(request.tags or []),
        created_at=now,
        updated_at=now,
        tenant_id=tenant.tenant_id
    )


@router.get("/{device_id}", response_model=Device)
async def get_device(
    device_id: str,
    user: UserContext = Depends(require_permissions("device:read")),
    tenant: TenantContext = Depends(get_tenant_context)
):
    """获取设备详情"""
    device = _devices.get(device_id)
    
    if not device:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"设备 {device_id} 不存在"
        )
    
    # 租户权限检查
    if device.get("tenant_id") != tenant.tenant_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="无权限访问此设备"
        )
    
    tags = _device_tags.get(device_id, [])
    
    return Device(
        id=device_id,
        name=device["name"],
        type=device["type"],
        host=device["host"],
        port=device["port"],
        status=device.get("status", "offline"),
        last_seen=device.get("last_seen"),
        tag_count=len(tags),
        created_at=device["created_at"],
        updated_at=device["updated_at"],
        tenant_id=device.get("tenant_id")
    )


@router.put("/{device_id}", response_model=Device)
async def update_device(
    device_id: str,
    request: DeviceUpdateRequest,
    user: UserContext = Depends(require_permissions("device:write")),
    tenant: TenantContext = Depends(get_tenant_context)
):
    """更新设备"""
    device = _devices.get(device_id)
    
    if not device:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"设备 {device_id} 不存在"
        )
    
    # 租户权限检查
    if device.get("tenant_id") != tenant.tenant_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="无权限修改此设备"
        )
    
    # 更新字段
    update_data = request.dict(exclude_unset=True)
    update_data["updated_at"] = datetime.utcnow()
    update_data["updated_by"] = user.user_id
    
    device.update(update_data)
    _devices.set(device_id, device)
    
    tags = _device_tags.get(device_id, [])
    
    return Device(
        id=device_id,
        name=device["name"],
        type=device["type"],
        host=device["host"],
        port=device["port"],
        status=device.get("status", "offline"),
        last_seen=device.get("last_seen"),
        tag_count=len(tags),
        created_at=device["created_at"],
        updated_at=device["updated_at"],
        tenant_id=device.get("tenant_id")
    )


@router.delete("/{device_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_device(
    device_id: str,
    user: UserContext = Depends(require_permissions("device:write")),
    tenant: TenantContext = Depends(get_tenant_context)
):
    """删除设备"""
    device = _devices.get(device_id)
    
    if not device:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"设备 {device_id} 不存在"
        )
    
    # 租户权限检查
    if device.get("tenant_id") != tenant.tenant_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="无权限删除此设备"
        )
    
    _devices.delete(device_id)
    _device_tags.delete(device_id)
    
    return None


@router.get("/{device_id}/tags", response_model=List[DeviceTag])
async def get_device_tags(
    device_id: str,
    user: UserContext = Depends(require_permissions("device:read")),
    tenant: TenantContext = Depends(get_tenant_context)
):
    """获取设备点位"""
    device = _devices.get(device_id)
    
    if not device:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"设备 {device_id} 不存在"
        )
    
    # 租户权限检查
    if device.get("tenant_id") != tenant.tenant_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="无权限访问此设备"
        )
    
    tags = _device_tags.get(device_id, [])
    return [DeviceTag(**tag) for tag in tags]


@router.post("/{device_id}/connect")
async def connect_device(
    device_id: str,
    user: UserContext = Depends(require_permissions("device:write")),
    tenant: TenantContext = Depends(get_tenant_context)
):
    """连接设备（手动触发）"""
    device = _devices.get(device_id)
    
    if not device:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"设备 {device_id} 不存在"
        )
    
    if device.get("tenant_id") != tenant.tenant_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="无权限操作此设备"
        )
    
    # 模拟连接
    device["status"] = "online"
    device["last_seen"] = datetime.utcnow()
    device["updated_at"] = datetime.utcnow()
    _devices.set(device_id, device)
    
    return {"status": "connected", "device_id": device_id}


@router.post("/{device_id}/disconnect")
async def disconnect_device(
    device_id: str,
    user: UserContext = Depends(require_permissions("device:write")),
    tenant: TenantContext = Depends(get_tenant_context)
):
    """断开设备连接"""
    device = _devices.get(device_id)
    
    if not device:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"设备 {device_id} 不存在"
        )
    
    if device.get("tenant_id") != tenant.tenant_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="无权限操作此设备"
        )
    
    # 模拟断开
    device["status"] = "offline"
    device["updated_at"] = datetime.utcnow()
    _devices.set(device_id, device)
    
    return {"status": "disconnected", "device_id": device_id}
