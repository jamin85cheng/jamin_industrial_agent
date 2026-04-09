"""Industrial intelligence router for patrol, review, and learning APIs."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from src.api.dependencies import UserContext, require_permissions
from src.intelligence.runtime import get_intelligence_service, get_patrol_scheduler

router = APIRouter(prefix="/intelligence", tags=["Industrial Intelligence"])


class TelemetryPointInput(BaseModel):
    value: Any
    unit: Optional[str] = None
    quality: str = "good"
    timestamp: Optional[datetime] = None


class TelemetryIngestRequest(BaseModel):
    asset_id: str
    source: str = "plc"
    points: Dict[str, TelemetryPointInput]


class PatrolRunRequest(BaseModel):
    asset_ids: Optional[List[str]] = None
    schedule_type: str = Field(default="manual", pattern=r"^(manual|scheduled|shadow)$")


class LabelReviewRequest(BaseModel):
    anomaly_type: Optional[str] = None
    root_cause: Optional[str] = None
    review_notes: str = ""
    final_action: str = ""
    false_positive: bool = False


class CandidateGenerationRequest(BaseModel):
    candidate_types: Optional[List[str]] = None


class DemoSeedRequest(BaseModel):
    asset_id: str = "ASSET_DUST_COLLECTOR_01"
    profile: str = Field(default="warning", pattern=r"^(normal|warning|critical)$")
    run_patrol: bool = True


@router.get("/assets")
async def list_assets(
    user: UserContext = Depends(require_permissions("data:read")),
):
    return get_intelligence_service().list_assets()


@router.get("/runtime")
async def get_runtime(
    user: UserContext = Depends(require_permissions("data:read")),
):
    service = get_intelligence_service()
    scheduler = get_patrol_scheduler()
    return {
        "service": service.get_runtime_summary(),
        "scheduler": {
            "running": scheduler.is_running if scheduler else False,
            "interval_seconds": scheduler.interval_seconds if scheduler else None,
            "last_run_id": scheduler.last_run_id if scheduler else None,
            "last_error": scheduler.last_error if scheduler else None,
        },
    }


@router.post("/telemetry/ingest")
async def ingest_telemetry(
    request: TelemetryIngestRequest,
    user: UserContext = Depends(require_permissions("device:write")),
):
    service = get_intelligence_service()
    try:
        return service.ingest_snapshot(
            asset_id=request.asset_id,
            source=request.source,
            points={
                key: payload.model_dump()
                for key, payload in request.points.items()
            },
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/snapshots/latest")
async def list_latest_snapshots(
    asset_ids: Optional[List[str]] = None,
    user: UserContext = Depends(require_permissions("data:read")),
):
    return get_intelligence_service().get_latest_snapshots(asset_ids)


@router.post("/demo/seed")
async def seed_demo_snapshot(
    request: DemoSeedRequest,
    user: UserContext = Depends(require_permissions("device:write")),
):
    service = get_intelligence_service()
    try:
        return await service.seed_demo_snapshot(
            asset_id=request.asset_id,
            profile=request.profile,
            run_patrol=request.run_patrol,
            triggered_by=user.username,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/patrol/run")
async def run_patrol(
    request: PatrolRunRequest,
    user: UserContext = Depends(require_permissions("data:read")),
):
    service = get_intelligence_service()
    return await service.run_patrol(
        asset_ids=request.asset_ids,
        triggered_by=user.username,
        schedule_type=request.schedule_type,
    )


@router.get("/patrol/runs")
async def list_patrol_runs(
    limit: int = 20,
    user: UserContext = Depends(require_permissions("data:read")),
):
    return get_intelligence_service().list_patrol_runs(limit=limit)


@router.get("/patrol/runs/{run_id}")
async def get_patrol_run(
    run_id: str,
    user: UserContext = Depends(require_permissions("data:read")),
):
    payload = get_intelligence_service().get_patrol_run(run_id)
    if not payload:
        raise HTTPException(status_code=404, detail=f"Unknown patrol run: {run_id}")
    return payload


@router.get("/risk/latest")
async def get_latest_risk_assessment(
    user: UserContext = Depends(require_permissions("data:read")),
):
    runs = get_intelligence_service().list_patrol_runs(limit=1)
    if not runs:
        return {"asset_results": [], "run_id": None}
    latest = runs[0]
    return {
        "run_id": latest.get("run_id"),
        "risk_level": latest.get("risk_level"),
        "risk_score": latest.get("risk_score"),
        "asset_results": latest.get("asset_results", []),
    }


@router.get("/review-queue")
async def list_review_queue(
    status: str = "pending",
    limit: int = 50,
    user: UserContext = Depends(require_permissions("alert:acknowledge")),
):
    return get_intelligence_service().list_review_queue(status=status, limit=limit)


@router.post("/review-queue/{label_id}/review")
async def review_label(
    label_id: str,
    request: LabelReviewRequest,
    user: UserContext = Depends(require_permissions("alert:acknowledge")),
):
    service = get_intelligence_service()
    try:
        if request.false_positive:
            return await service.reject_label(
                label_id,
                reviewer=user.username,
                review_notes=request.review_notes,
            )
        return await service.confirm_label(
            label_id,
            reviewer=user.username,
            anomaly_type=request.anomaly_type,
            root_cause=request.root_cause,
            review_notes=request.review_notes,
            final_action=request.final_action,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/knowledge/cases")
async def list_knowledge_cases(
    scene_type: Optional[str] = None,
    limit: int = 50,
    user: UserContext = Depends(require_permissions("data:read")),
):
    return get_intelligence_service().list_knowledge_cases(scene_type=scene_type, limit=limit)


@router.post("/learning/candidates/generate")
async def generate_learning_candidates(
    request: CandidateGenerationRequest,
    user: UserContext = Depends(require_permissions("knowledge:write")),
):
    return get_intelligence_service().generate_learning_candidates(
        requested_types=request.candidate_types,
    )


@router.get("/learning/candidates")
async def list_learning_candidates(
    candidate_type: Optional[str] = None,
    limit: int = 50,
    user: UserContext = Depends(require_permissions("data:read")),
):
    return get_intelligence_service().list_learning_candidates(candidate_type=candidate_type, limit=limit)
