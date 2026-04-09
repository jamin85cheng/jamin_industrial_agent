"""Device management APIs."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import List, Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Response, UploadFile, status
from pydantic import BaseModel, Field

from src.api.dependencies import TenantContext, UserContext, get_tenant_context, require_permissions
from src.api.repositories.device_repository import DeviceRepository
from src.plc.tag_importer import build_device_tag_import_template, parse_device_tag_mapping_content

router = APIRouter(prefix="/devices", tags=["Device Management"])
device_repository = DeviceRepository()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class DeviceTag(BaseModel):
    name: str
    address: str
    data_type: str = "float"
    unit: Optional[str] = None
    description: Optional[str] = None
    asset_id: Optional[str] = None
    point_key: Optional[str] = None
    deadband: Optional[float] = None
    debounce_ms: int = Field(default=0, ge=0, le=600000)


class DeviceCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    type: str = Field(..., pattern=r"^(s7|modbus|simulated)$")
    host: str = Field(
        ...,
        pattern=r"^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$",
    )
    port: int = Field(..., ge=0, le=65535)
    rack: Optional[int] = 0
    slot: Optional[int] = 1
    scan_interval: int = Field(default=10, ge=1, le=3600)
    tags: List[DeviceTag] = Field(default_factory=list)


class DeviceTagUpdateRequest(BaseModel):
    tags: List[DeviceTag] = Field(default_factory=list)


class DeviceUpdateRequest(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    host: Optional[str] = None
    port: Optional[int] = Field(None, ge=0, le=65535)
    rack: Optional[int] = None
    slot: Optional[int] = None
    scan_interval: Optional[int] = Field(None, ge=1, le=3600)
    enabled: Optional[bool] = None


class Device(BaseModel):
    id: str
    name: str
    type: str
    host: str
    port: int
    rack: Optional[int] = 0
    slot: Optional[int] = 1
    scan_interval: int = 10
    enabled: bool = True
    status: str
    last_seen: Optional[datetime] = None
    tag_count: int = 0
    created_at: datetime
    updated_at: datetime
    tenant_id: Optional[str] = None


class DeviceListResponse(BaseModel):
    total: int
    devices: List[Device]


class DeviceTagImportPreviewResponse(BaseModel):
    file_name: str
    file_type: str
    detected_columns: List[str] = Field(default_factory=list)
    matched_columns: dict[str, str] = Field(default_factory=dict)
    field_mapping: dict[str, str] = Field(default_factory=dict)
    unmatched_columns: List[str] = Field(default_factory=list)
    available_fields: List[str] = Field(default_factory=list)
    required_fields: List[str] = Field(default_factory=list)
    total_rows: int
    parsed_rows: int
    skipped_rows: int
    warnings: List[str] = Field(default_factory=list)
    tags: List[DeviceTag] = Field(default_factory=list)
    preview_rows: List["DeviceTagPreviewRow"] = Field(default_factory=list)
    validation_report: "DeviceTagValidationSummary"


class DeviceTagValidationIssue(BaseModel):
    code: str
    field: Optional[str] = None
    message: str
    severity: str = "error"


class DeviceTagRepairSuggestion(BaseModel):
    field: str
    value: str
    confidence: str = "medium"
    reason: str


class DeviceTagPreviewRow(BaseModel):
    row_number: int
    status: str = "ok"
    flagged_fields: List[str] = Field(default_factory=list)
    issues: List[DeviceTagValidationIssue] = Field(default_factory=list)
    suggestions: List[DeviceTagRepairSuggestion] = Field(default_factory=list)
    tag: DeviceTag


class DeviceTagDuplicateCluster(BaseModel):
    cluster_key: str
    label: str
    addresses: List[str] = Field(default_factory=list)
    row_numbers: List[int] = Field(default_factory=list)
    duplicate_count: int = 0
    suggestion: str


class DeviceTagValidationSummary(BaseModel):
    total_rows: int = 0
    clean_rows: int = 0
    rows_with_errors: int = 0
    rows_with_warnings: int = 0
    error_count: int = 0
    warning_count: int = 0
    issue_counts: dict[str, int] = Field(default_factory=dict)
    suggestion_count: int = 0
    duplicate_clusters: List[DeviceTagDuplicateCluster] = Field(default_factory=list)
    has_errors: bool = False


DeviceTagImportPreviewResponse.model_rebuild()


@router.get("", response_model=DeviceListResponse)
async def list_devices(
    type: Optional[str] = Query(None, pattern=r"^(s7|modbus|simulated)$"),
    status: Optional[str] = Query(None, pattern=r"^(online|offline|error)$"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    user: UserContext = Depends(require_permissions("device:read")),
    tenant: TenantContext = Depends(get_tenant_context),
):
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


@router.get("/tags/import-template")
async def download_device_tag_import_template(
    format: str = Query("xlsx", pattern=r"^(csv|xlsx)$"),
    user: UserContext = Depends(require_permissions("device:write")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    del user, tenant

    try:
        content, filename, media_type = build_device_tag_import_template(format)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return Response(
        content=content,
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/tags/import-preview", response_model=DeviceTagImportPreviewResponse)
async def preview_device_tag_import(
    file: UploadFile = File(...),
    field_mapping: Optional[str] = Form(None),
    value_overrides: Optional[str] = Form(None),
    user: UserContext = Depends(require_permissions("device:write")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    del user, tenant

    if not file.filename:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Uploaded file name is empty")

    mapping_payload: dict[str, str] | None = None
    if field_mapping:
        try:
            raw_mapping = json.loads(field_mapping)
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="field_mapping must be valid JSON") from exc
        if not isinstance(raw_mapping, dict):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="field_mapping must be an object")
        mapping_payload = {str(key): str(value) for key, value in raw_mapping.items()}

    overrides_payload: dict[int, dict[str, object]] | None = None
    if value_overrides:
        try:
            raw_overrides = json.loads(value_overrides)
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="value_overrides must be valid JSON") from exc
        if not isinstance(raw_overrides, dict):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="value_overrides must be an object")
        overrides_payload = {}
        for row_key, values in raw_overrides.items():
            try:
                row_number = int(row_key)
            except (TypeError, ValueError) as exc:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="value_overrides keys must be row numbers") from exc
            if not isinstance(values, dict):
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="value_overrides values must be objects")
            overrides_payload[row_number] = dict(values)

    try:
        parsed = parse_device_tag_mapping_content(
            file.filename,
            await file.read(),
            field_mapping=mapping_payload,
            value_overrides=overrides_payload,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return DeviceTagImportPreviewResponse(
        file_name=str(parsed["file_name"]),
        file_type=str(parsed["file_type"]),
        detected_columns=list(parsed["detected_columns"]),
        matched_columns=dict(parsed["matched_columns"]),
        field_mapping=dict(parsed["field_mapping"]),
        unmatched_columns=list(parsed["unmatched_columns"]),
        available_fields=list(parsed["available_fields"]),
        required_fields=list(parsed["required_fields"]),
        total_rows=int(parsed["total_rows"]),
        parsed_rows=int(parsed["parsed_rows"]),
        skipped_rows=int(parsed["skipped_rows"]),
        warnings=list(parsed["warnings"]),
        tags=[DeviceTag(**tag) for tag in parsed["tags"]],
        preview_rows=[DeviceTagPreviewRow(**row) for row in parsed["preview_rows"]],
        validation_report=DeviceTagValidationSummary(**parsed["validation_report"]),
    )


@router.get("/{device_id}", response_model=Device)
async def get_device(
    device_id: str,
    user: UserContext = Depends(require_permissions("device:read")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    device = device_repository.get_device(device_id, tenant_id=tenant.tenant_id)
    if not device:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Device {device_id} not found")
    return Device(**device)


@router.put("/{device_id}", response_model=Device)
async def update_device(
    device_id: str,
    request: DeviceUpdateRequest,
    user: UserContext = Depends(require_permissions("device:write")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    updated = device_repository.update_device(
        device_id,
        tenant_id=tenant.tenant_id,
        updates=request.model_dump(exclude_unset=True),
        updated_by=user.user_id,
    )
    if not updated:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Device {device_id} not found")
    return Device(**updated)


@router.delete("/{device_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_device(
    device_id: str,
    user: UserContext = Depends(require_permissions("device:write")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    deleted = device_repository.delete_device(device_id, tenant_id=tenant.tenant_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Device {device_id} not found")
    return None


@router.get("/{device_id}/tags", response_model=List[DeviceTag])
async def get_device_tags(
    device_id: str,
    user: UserContext = Depends(require_permissions("device:read")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    device = device_repository.get_device(device_id, tenant_id=tenant.tenant_id)
    if not device:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Device {device_id} not found")
    return [DeviceTag(**tag) for tag in device_repository.list_tags(device_id, tenant_id=tenant.tenant_id)]


@router.put("/{device_id}/tags", response_model=List[DeviceTag])
async def replace_device_tags(
    device_id: str,
    request: DeviceTagUpdateRequest,
    user: UserContext = Depends(require_permissions("device:write")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    device = device_repository.get_device(device_id, tenant_id=tenant.tenant_id)
    if not device:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Device {device_id} not found")

    tags = device_repository.replace_tags(
        device_id,
        tenant_id=tenant.tenant_id,
        tags=[tag.model_dump() for tag in request.tags],
    )
    device_repository.update_device(
        device_id,
        tenant_id=tenant.tenant_id,
        updates={},
        updated_by=user.user_id,
    )
    return [DeviceTag(**tag) for tag in tags]


@router.post("/{device_id}/connect")
async def connect_device(
    device_id: str,
    user: UserContext = Depends(require_permissions("device:write")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    device = device_repository.set_connection_state(
        device_id,
        tenant_id=tenant.tenant_id,
        status="online",
        updated_by=user.user_id,
    )
    if not device:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Device {device_id} not found")
    return {"status": "connected", "device_id": device_id}


@router.post("/{device_id}/disconnect")
async def disconnect_device(
    device_id: str,
    user: UserContext = Depends(require_permissions("device:write")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    device = device_repository.set_connection_state(
        device_id,
        tenant_id=tenant.tenant_id,
        status="offline",
        updated_by=user.user_id,
    )
    if not device:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Device {device_id} not found")
    return {"status": "disconnected", "device_id": device_id}
