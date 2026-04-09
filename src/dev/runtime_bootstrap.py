"""Development and test-only runtime bootstrap helpers.

These helpers intentionally live outside the API router modules so manual demo
seeding and storage initialization do not remain part of the business routing
surface.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from src.api.repositories.alert_repository import AlertRepository
from src.api.repositories.device_repository import DeviceRepository
from src.api.repositories.report_repository import ReportRepository
from src.api.repositories.system_config_repository import SystemConfigRepository


def ensure_device_demo_data(
    db_config: Optional[Dict[str, Any]] = None,
    *,
    tenant_id: str = "default",
) -> Dict[str, Any]:
    repository = DeviceRepository(db_config)
    repository.init_schema()
    repository.seed_demo_devices(tenant_id=tenant_id)
    summary = repository.list_devices(tenant_id=tenant_id, skip=0, limit=500)
    return {
        "tenant_id": tenant_id,
        "device_total": int(summary["total"]),
    }


def ensure_alert_rule_defaults(
    db_config: Optional[Dict[str, Any]] = None,
    *,
    tenant_id: str = "default",
) -> Dict[str, Any]:
    repository = AlertRepository(db_config)
    repository.init_schema()
    repository.seed_default_rules(tenant_id=tenant_id)
    rules = repository.list_rules(tenant_id=tenant_id)
    return {
        "tenant_id": tenant_id,
        "rule_total": len(rules),
    }


def ensure_alert_demo_data(
    db_config: Optional[Dict[str, Any]] = None,
    *,
    tenant_id: str = "default",
) -> Dict[str, Any]:
    repository = AlertRepository(db_config)
    repository.init_schema()
    repository.seed_demo_alerts(tenant_id=tenant_id)
    summary = repository.list_alerts(tenant_id=tenant_id, limit=500)
    return {
        "tenant_id": tenant_id,
        "alert_total": int(summary["total"]),
    }


def ensure_report_storage(db_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    repository = ReportRepository(db_config)
    repository.init_schema()
    return {
        "backend": repository.backend,
        "target": repository.adapter.target,
    }


def ensure_system_config_storage(db_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    repository = SystemConfigRepository(db_config)
    repository.init_schema()
    return {
        "backend": repository.backend,
        "target": repository.adapter.target,
    }


def bootstrap_development_runtime(
    db_config: Optional[Dict[str, Any]] = None,
    *,
    tenant_id: str = "default",
    include_demo_data: bool = True,
) -> Dict[str, Any]:
    payload = {
        "rules": ensure_alert_rule_defaults(db_config, tenant_id=tenant_id),
        "reports": ensure_report_storage(db_config),
        "system_config": ensure_system_config_storage(db_config),
    }
    if include_demo_data:
        payload["devices"] = ensure_device_demo_data(db_config, tenant_id=tenant_id)
        payload["alerts"] = ensure_alert_demo_data(db_config, tenant_id=tenant_id)
    return payload
