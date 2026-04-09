"""System configuration management API."""

from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from src.api.dependencies import UserContext, require_roles
from src.api.repositories.system_config_repository import SystemConfigRepository
from src.utils.config import load_config
from src.utils.structured_logging import get_logger

router = APIRouter(prefix="/system/config", tags=["System Config"])
system_config_repository = SystemConfigRepository()
audit_logger = get_logger("system_config_audit")


class BasicSystemConfig(BaseModel):
    system_name: str = Field(..., min_length=2, max_length=120)
    scan_interval: int = Field(..., ge=1, le=3600)
    alert_suppression: int = Field(..., ge=0, le=1440)


class PlcSystemConfig(BaseModel):
    plc_type: str = Field(..., min_length=2, max_length=64)
    ip_address: str = Field(..., min_length=3, max_length=128)
    port: int = Field(..., ge=1, le=65535)


class NotificationSystemConfig(BaseModel):
    feishu_enabled: bool = False
    feishu_webhook: Optional[str] = Field(default=None, max_length=500)
    email_enabled: bool = False
    smtp_server: Optional[str] = Field(default=None, max_length=255)


class SystemConfigPayload(BaseModel):
    basic: BasicSystemConfig
    plc: PlcSystemConfig
    notifications: NotificationSystemConfig


class SystemConfigResponse(BaseModel):
    config: SystemConfigPayload
    updated_at: Optional[str] = None
    updated_by: Optional[str] = None
    source: str


def _default_payload(config: Dict[str, Any]) -> Dict[str, Any]:
    project = config.get("project", {})
    plc = config.get("plc", {})
    notifications = config.get("notifications", {})
    return {
        "basic": {
            "system_name": project.get("name", "Jamin Industrial Agent"),
            "scan_interval": int(plc.get("scan_interval", 10) or 10),
            "alert_suppression": int(notifications.get("alert_suppression", 15) or 15),
        },
        "plc": {
            "plc_type": str(plc.get("type", "s7")),
            "ip_address": str(plc.get("host", "127.0.0.1")),
            "port": int(plc.get("port", 102) or 102),
        },
        "notifications": {
            "feishu_enabled": bool(notifications.get("feishu_enabled", False)),
            "feishu_webhook": notifications.get("feishu_webhook"),
            "email_enabled": bool(notifications.get("email_enabled", False)),
            "smtp_server": notifications.get("smtp_server"),
        },
    }


def _merge_payload(defaults: Dict[str, Any], stored: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not stored:
        return defaults
    merged = {
        "basic": dict(defaults.get("basic") or {}),
        "plc": dict(defaults.get("plc") or {}),
        "notifications": dict(defaults.get("notifications") or {}),
    }
    for section in merged.keys():
        merged[section].update(dict((stored.get(section) or {})))
    return merged


@router.get("", response_model=SystemConfigResponse)
async def get_system_config(user: UserContext = Depends(require_roles("admin"))):
    config = load_config()
    defaults = _default_payload(config)
    stored = system_config_repository.get_config()
    payload = _merge_payload(defaults, (stored or {}).get("config"))
    return SystemConfigResponse(
        config=SystemConfigPayload(**payload),
        updated_at=stored["updated_at"].isoformat() if stored and stored.get("updated_at") else None,
        updated_by=stored.get("updated_by") if stored else None,
        source="database" if stored else "defaults",
    )


@router.put("", response_model=SystemConfigResponse)
async def save_system_config(
    payload: SystemConfigPayload,
    user: UserContext = Depends(require_roles("admin")),
):
    stored = system_config_repository.save_config(
        payload=payload.model_dump(),
        updated_by=user.user_id,
    )
    audit_logger.log_audit("save_system_config", user.user_id, "platform", "success", tenant_id=user.tenant_id)
    return SystemConfigResponse(
        config=payload,
        updated_at=stored["updated_at"].isoformat() if stored.get("updated_at") else None,
        updated_by=stored.get("updated_by"),
        source="database",
    )
