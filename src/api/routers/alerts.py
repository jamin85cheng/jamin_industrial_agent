"""
告警管理API路由

实现告警规则、告警事件等接口
"""

from fastapi import APIRouter, Depends, HTTPException, Query, status
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field

from src.api.dependencies import (
    get_current_user, require_permissions, get_tenant_context,
    UserContext, TenantContext
)
from src.utils.thread_safe import ThreadSafeDict

router = APIRouter(prefix="/alerts", tags=["告警"])


# 枚举定义
class AlertSeverity(str, Enum):
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


class AlertStatus(str, Enum):
    ACTIVE = "active"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"


# Pydantic模型
class AlertRule(BaseModel):
    """告警规则"""
    rule_id: str
    name: str
    enabled: bool = True
    condition: Dict[str, Any]
    severity: AlertSeverity
    message: str
    suppression_window_minutes: int = 30
    created_at: Optional[datetime] = None


class Alert(BaseModel):
    """告警"""
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


class AlertListResponse(BaseModel):
    """告警列表响应"""
    total: int
    alerts: List[Alert]


class AcknowledgeRequest(BaseModel):
    """确认告警请求"""
    comment: Optional[str] = None


# 内存存储
_alerts = ThreadSafeDict()
_alert_rules = ThreadSafeDict()
_alert_counters = {"total": 0, "today": 0, "active": 0}


def init_default_rules():
    if _alert_rules.size() > 0:
        return
    """初始化默认告警规则"""
    default_rules = [
        {
            "rule_id": "RULE_001",
            "name": "温度高温告警",
            "enabled": True,
            "condition": {
                "type": "threshold",
                "tag": "temperature",
                "operator": ">",
                "value": 100
            },
            "severity": "critical",
            "message": "温度超过100°C，需要立即处理",
            "suppression_window_minutes": 30,
            "created_at": datetime.utcnow()
        },
        {
            "rule_id": "RULE_002",
            "name": "压力异常告警",
            "enabled": True,
            "condition": {
                "type": "threshold",
                "tag": "pressure",
                "operator": ">",
                "value": 10
            },
            "severity": "warning",
            "message": "压力超过10bar，请注意",
            "suppression_window_minutes": 15,
            "created_at": datetime.utcnow()
        }
    ]
    
    for rule in default_rules:
        _alert_rules.set(rule["rule_id"], rule)


def init_default_alerts():
    """Initialize demo alerts for local development and UI validation."""
    if _alerts.size() > 0:
        return

    init_default_rules()
    create_alert(
        "RULE_001",
        "溶解氧浓度低于 2.0 mg/L，建议检查曝气量与风机运行状态。",
        "critical",
        "DEV_AERATION_01",
        "DO",
        1.8,
        2.0,
        "default",
    )
    create_alert(
        "RULE_002",
        "鼓风机振动偏高，请关注轴承与对中状态。",
        "warning",
        "DEV_BLOWER_01",
        "vibration",
        11.4,
        10.0,
        "default",
    )


@router.get("", response_model=AlertListResponse)
async def list_alerts(
    status: Optional[AlertStatus] = Query(None),
    severity: Optional[AlertSeverity] = Query(None),
    device_id: Optional[str] = Query(None),
    start_time: Optional[datetime] = Query(None),
    end_time: Optional[datetime] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    user: UserContext = Depends(require_permissions("alert:read")),
    tenant: TenantContext = Depends(get_tenant_context)
):
    """
    获取告警列表
    
    支持按状态、严重等级、设备、时间范围过滤
    """
    alerts = []
    
    for alert_id in _alerts.keys():
        alert = _alerts.get(alert_id)
        if not alert:
            continue
        
        # 租户过滤
        if alert.get("tenant_id") != tenant.tenant_id:
            continue
        
        # 状态过滤
        if status and alert.get("status") != status.value:
            continue
        
        # 严重等级过滤
        if severity and alert.get("severity") != severity.value:
            continue
        
        # 设备过滤
        if device_id and alert.get("device_id") != device_id:
            continue
        
        # 时间范围过滤
        if start_time and alert.get("created_at") < start_time:
            continue
        if end_time and alert.get("created_at") > end_time:
            continue
        
        alerts.append(Alert(**alert))
    
    # 按时间倒序
    alerts.sort(key=lambda x: x.created_at, reverse=True)
    
    total = len(alerts)
    alerts = alerts[:limit]
    
    return AlertListResponse(total=total, alerts=alerts)


@router.get("/stats")
async def get_alert_stats(
    user: UserContext = Depends(require_permissions("alert:read"))
):
    """
    获取告警统计
    
    返回告警数量统计信息
    """
    # 计算各状态告警数量
    active_count = 0
    warning_count = 0
    critical_count = 0
    
    for alert_id in _alerts.keys():
        alert = _alerts.get(alert_id)
        if alert and alert.get("status") == "active":
            active_count += 1
            if alert.get("severity") == "warning":
                warning_count += 1
            elif alert.get("severity") == "critical":
                critical_count += 1
    
    return {
        "total_alerts": len(_alerts.keys()),
        "active_alerts": active_count,
        "critical_alerts": critical_count,
        "warning_alerts": warning_count,
        "acknowledged_today": 0
    }


@router.post("/{alert_id}/acknowledge")
async def acknowledge_alert(
    alert_id: str,
    request: AcknowledgeRequest,
    user: UserContext = Depends(require_permissions("alert:acknowledge"))
):
    """
    确认告警
    
    标记告警为已确认状态
    """
    alert = _alerts.get(alert_id)
    
    if not alert:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"告警 {alert_id} 不存在"
        )
    
    alert["status"] = "acknowledged"
    alert["acknowledged_by"] = user.user_id
    alert["acknowledged_at"] = datetime.utcnow()
    if request.comment:
        alert["acknowledge_comment"] = request.comment
    
    _alerts.set(alert_id, alert)
    
    return {
        "success": True,
        "message": "告警已确认",
        "data": {
            "alert_id": alert_id,
            "acknowledged_by": user.user_id,
            "acknowledged_at": alert["acknowledged_at"].isoformat()
        }
    }


@router.post("/{alert_id}/resolve")
async def resolve_alert(
    alert_id: str,
    user: UserContext = Depends(require_permissions("alert:acknowledge"))
):
    """
    解决告警
    
    标记告警为已解决状态
    """
    alert = _alerts.get(alert_id)
    
    if not alert:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"告警 {alert_id} 不存在"
        )
    
    alert["status"] = "resolved"
    alert["resolved_at"] = datetime.utcnow()
    alert["resolved_by"] = user.user_id
    
    _alerts.set(alert_id, alert)
    
    return {
        "success": True,
        "message": "告警已解决",
        "data": {
            "alert_id": alert_id,
            "resolved_at": alert["resolved_at"].isoformat()
        }
    }


@router.get("/rules", response_model=List[AlertRule])
async def list_alert_rules(
    enabled_only: bool = Query(False),
    user: UserContext = Depends(require_permissions("alert:read"))
):
    """
    获取告警规则列表
    
    返回所有告警规则配置
    """
    rules = []
    
    for rule_id in _alert_rules.keys():
        rule = _alert_rules.get(rule_id)
        if rule:
            if enabled_only and not rule.get("enabled"):
                continue
            rules.append(AlertRule(**rule))
    
    return rules


@router.post("/rules", response_model=AlertRule, status_code=status.HTTP_201_CREATED)
async def create_alert_rule(
    rule: AlertRule,
    user: UserContext = Depends(require_permissions("device:write"))
):
    """
    创建告警规则
    
    创建新的告警规则
    """
    import uuid
    
    if not rule.rule_id:
        rule.rule_id = f"RULE_{uuid.uuid4().hex[:8].upper()}"
    
    rule.created_at = datetime.utcnow()
    
    _alert_rules.set(rule.rule_id, rule.dict())
    
    return rule


@router.put("/rules/{rule_id}", response_model=AlertRule)
async def update_alert_rule(
    rule_id: str,
    rule: AlertRule,
    user: UserContext = Depends(require_permissions("device:write"))
):
    """
    更新告警规则
    
    更新指定告警规则
    """
    existing = _alert_rules.get(rule_id)
    
    if not existing:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"规则 {rule_id} 不存在"
        )
    
    rule.rule_id = rule_id
    rule.created_at = existing.get("created_at", datetime.utcnow())
    
    _alert_rules.set(rule_id, rule.dict())
    
    return rule


@router.delete("/rules/{rule_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_alert_rule(
    rule_id: str,
    user: UserContext = Depends(require_permissions("device:write"))
):
    """
    删除告警规则
    
    删除指定告警规则
    """
    if not _alert_rules.get(rule_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"规则 {rule_id} 不存在"
        )
    
    _alert_rules.delete(rule_id)
    
    return None


# 内部函数：创建告警
def create_alert(rule_id: str, message: str, severity: str, 
                 device_id: str = None, tag: str = None, 
                 value: float = None, threshold: float = None,
                 tenant_id: str = "default") -> str:
    """创建告警（内部使用）"""
    import uuid
    
    alert_id = f"ALT_{uuid.uuid4().hex[:12].upper()}"
    
    rule = _alert_rules.get(rule_id)
    rule_name = rule["name"] if rule else None
    
    alert = {
        "id": alert_id,
        "rule_id": rule_id,
        "rule_name": rule_name,
        "severity": severity,
        "message": message,
        "device_id": device_id,
        "tag": tag,
        "value": value,
        "threshold": threshold,
        "status": "active",
        "created_at": datetime.utcnow(),
        "tenant_id": tenant_id
    }
    
    _alerts.set(alert_id, alert)
    _alert_counters["total"] += 1
    _alert_counters["active"] += 1
    
    return alert_id


# 模拟数据
if __name__ == "__main__":
    init_default_rules()
    
    # 创建一些示例告警
    create_alert(
        "RULE_001",
        "温度超过100°C，需要立即处理",
        "critical",
        "DEV_001",
        "temperature",
        105.5,
        100.0,
        "default"
    )
