"""告警管理 API。"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel

from src.api.dependencies import TenantContext, UserContext, get_tenant_context, require_permissions
from src.api.repositories.alert_repository import AlertRepository, utc_now

router = APIRouter(prefix="/alerts", tags=["告警"])
alert_repository = AlertRepository()


class AlertSeverity(str, Enum):
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


class AlertStatus(str, Enum):
    ACTIVE = "active"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"


class AlertRule(BaseModel):
    rule_id: str
    name: str
    enabled: bool = True
    condition: Dict[str, Any]
    severity: AlertSeverity
    message: str
    suppression_window_minutes: int = 30
    created_at: Optional[datetime] = None


class Alert(BaseModel):
    id: str
    rule_id: Optional[str] = None
    rule_name: Optional[str] = None
    severity: AlertSeverity
    message: str
    device_id: Optional[str] = None
    tag: Optional[str] = None
    value: Optional[float] = None
    threshold: Optional[float] = None
    status: AlertStatus
    created_at: datetime
    acknowledged_by: Optional[str] = None
    acknowledged_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None
    diagnosis_task_id: Optional[str] = None
    latest_report_id: Optional[str] = None
    latest_report_download_url: Optional[str] = None
    last_action_by: Optional[str] = None
    last_action_at: Optional[datetime] = None
    resolution_notes: Optional[str] = None


class AlertListResponse(BaseModel):
    total: int
    alerts: List[Alert]


class AcknowledgeRequest(BaseModel):
    comment: Optional[str] = None


class ResolveRequest(BaseModel):
    notes: Optional[str] = None


@router.get("", response_model=AlertListResponse)
async def list_alerts(
    status: Optional[AlertStatus] = Query(None),
    severity: Optional[AlertSeverity] = Query(None),
    device_id: Optional[str] = Query(None),
    start_time: Optional[datetime] = Query(None),
    end_time: Optional[datetime] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    user: UserContext = Depends(require_permissions("alert:read")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    """获取告警列表。"""
    result = alert_repository.list_alerts(
        tenant_id=tenant.tenant_id,
        status=status.value if status else None,
        severity=severity.value if severity else None,
        device_id=device_id,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
    )
    return AlertListResponse(total=result["total"], alerts=[Alert(**alert) for alert in result["alerts"]])


@router.get("/stats")
async def get_alert_stats(
    user: UserContext = Depends(require_permissions("alert:read")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    """获取告警统计。"""
    return alert_repository.get_stats(tenant_id=tenant.tenant_id)


@router.post("/{alert_id}/acknowledge")
async def acknowledge_alert(
    alert_id: str,
    request: AcknowledgeRequest,
    user: UserContext = Depends(require_permissions("alert:acknowledge")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    """确认告警。"""
    alert = alert_repository.acknowledge_alert(
        alert_id,
        tenant_id=tenant.tenant_id,
        user_id=user.user_id,
        comment=request.comment,
    )
    if not alert:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"告警 {alert_id} 不存在")
    return {
        "success": True,
        "message": "告警已确认",
        "data": {
            "alert_id": alert_id,
            "acknowledged_by": user.user_id,
            "acknowledged_at": alert["acknowledged_at"].isoformat() if alert.get("acknowledged_at") else None,
        },
    }


@router.post("/{alert_id}/resolve")
async def resolve_alert(
    alert_id: str,
    request: ResolveRequest,
    user: UserContext = Depends(require_permissions("alert:acknowledge")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    """解决告警。"""
    alert = alert_repository.resolve_alert(
        alert_id,
        tenant_id=tenant.tenant_id,
        user_id=user.user_id,
        resolution_notes=request.notes,
    )
    if not alert:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"告警 {alert_id} 不存在")
    return {
        "success": True,
        "message": "告警已解决",
        "data": {
            "alert_id": alert_id,
            "resolved_at": alert["resolved_at"].isoformat() if alert.get("resolved_at") else None,
            "resolution_notes": alert.get("resolution_notes"),
        },
    }


@router.get("/rules", response_model=List[AlertRule])
async def list_alert_rules(
    enabled_only: bool = Query(False),
    user: UserContext = Depends(require_permissions("alert:read")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    """获取告警规则列表。"""
    return [AlertRule(**rule) for rule in alert_repository.list_rules(tenant_id=tenant.tenant_id, enabled_only=enabled_only)]


@router.post("/rules", response_model=AlertRule, status_code=status.HTTP_201_CREATED)
async def create_alert_rule(
    rule: AlertRule,
    user: UserContext = Depends(require_permissions("device:write")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    """创建告警规则。"""
    payload = rule.model_dump()
    if not payload.get("rule_id"):
        payload["rule_id"] = f"RULE_{utc_now().strftime('%Y%m%d%H%M%S')}"
    payload["created_at"] = utc_now()
    payload["tenant_id"] = tenant.tenant_id
    created = alert_repository.create_rule(payload)
    return AlertRule(**created)


@router.put("/rules/{rule_id}", response_model=AlertRule)
async def update_alert_rule(
    rule_id: str,
    rule: AlertRule,
    user: UserContext = Depends(require_permissions("device:write")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    """更新告警规则。"""
    updated = alert_repository.update_rule(
        rule_id,
        tenant_id=tenant.tenant_id,
        updates=rule.model_dump(exclude={"rule_id", "created_at"}),
    )
    if not updated:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"规则 {rule_id} 不存在")
    return AlertRule(**updated)


@router.delete("/rules/{rule_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_alert_rule(
    rule_id: str,
    user: UserContext = Depends(require_permissions("device:write")),
    tenant: TenantContext = Depends(get_tenant_context),
):
    """删除告警规则。"""
    deleted = alert_repository.delete_rule(rule_id, tenant_id=tenant.tenant_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"规则 {rule_id} 不存在")
    return None


def create_alert(
    rule_id: str,
    message: str,
    severity: str,
    device_id: Optional[str] = None,
    tag: Optional[str] = None,
    value: Optional[float] = None,
    threshold: Optional[float] = None,
    tenant_id: str = "default",
) -> str:
    """供内部流程调用的告警创建函数。"""
    return alert_repository.create_alert(
        rule_id=rule_id,
        message=message,
        severity=severity,
        device_id=device_id,
        tag=tag,
        value=value,
        threshold=threshold,
        tenant_id=tenant_id,
    )
