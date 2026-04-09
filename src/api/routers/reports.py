"""Report management API."""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from src.api.dependencies import UserContext, require_permissions
from src.api.repositories.report_repository import ReportRepository
from src.utils.structured_logging import get_logger

router = APIRouter(prefix="/reports", tags=["Reports"])
report_repository = ReportRepository()
audit_logger = get_logger("report_audit")


class ReportRecord(BaseModel):
    report_id: str
    task_id: str
    diagnosis_id: str
    alert_id: Optional[str] = None
    tenant_id: str
    format: str
    filename: str
    media_type: str
    created_at: str
    created_by: Optional[str] = None
    file_size_bytes: Optional[int] = None
    metadata: dict = Field(default_factory=dict)
    download_url: str


class ReportListResponse(BaseModel):
    total: int
    reports: List[ReportRecord]


def _serialize_report(record: dict) -> ReportRecord:
    created_at = record.get("created_at")
    created_at_value = created_at.isoformat() if hasattr(created_at, "isoformat") else str(created_at)
    return ReportRecord(
        report_id=str(record["report_id"]),
        task_id=str(record["task_id"]),
        diagnosis_id=str(record["diagnosis_id"]),
        alert_id=record.get("alert_id"),
        tenant_id=str(record["tenant_id"]),
        format=str(record["format"]),
        filename=str(record["filename"]),
        media_type=str(record["media_type"]),
        created_at=created_at_value,
        created_by=record.get("created_by"),
        file_size_bytes=record.get("file_size_bytes"),
        metadata=dict(record.get("metadata") or {}),
        download_url=str(record["download_url"]),
    )


def _get_report_or_404(report_id: str, tenant_id: str) -> dict:
    report = report_repository.get_report(report_id, tenant_id=tenant_id)
    if not report:
        raise HTTPException(status_code=404, detail="Report not found")
    return report


@router.get("", response_model=ReportListResponse)
async def list_reports(
    task_id: Optional[str] = Query(None),
    alert_id: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    user: UserContext = Depends(require_permissions("report:read")),
):
    tenant_id = user.tenant_id or "default"
    reports = report_repository.list_reports(
        tenant_id=tenant_id,
        task_id=task_id,
        alert_id=alert_id,
        limit=limit,
    )
    return ReportListResponse(total=len(reports), reports=[_serialize_report(report) for report in reports])


@router.get("/{report_id}", response_model=ReportRecord)
async def get_report(
    report_id: str,
    user: UserContext = Depends(require_permissions("report:read")),
):
    tenant_id = user.tenant_id or "default"
    report = _get_report_or_404(report_id, tenant_id)
    audit_logger.log_audit("read_report_metadata", user.user_id, report_id, "success", tenant_id=tenant_id)
    return _serialize_report(report)


@router.get("/{report_id}/download")
async def download_report(
    report_id: str,
    user: UserContext = Depends(require_permissions("report:read")),
):
    tenant_id = user.tenant_id or "default"
    report = _get_report_or_404(report_id, tenant_id)
    file_path = Path(str(report["file_path"]))
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="Report file not found")

    audit_logger.log_audit("download_report_file", user.user_id, report_id, "success", tenant_id=tenant_id)
    return FileResponse(
        path=file_path,
        filename=str(report["filename"]),
        media_type=str(report["media_type"]),
    )
