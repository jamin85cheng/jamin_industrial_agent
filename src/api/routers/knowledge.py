"""Knowledge operations API backed by persisted intelligence cases."""

from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from src.api.dependencies import UserContext, require_permissions
from src.intelligence.runtime import get_intelligence_service
from src.utils.structured_logging import get_logger

router = APIRouter(prefix="/knowledge", tags=["Knowledge"])
audit_logger = get_logger("knowledge_audit")


class KnowledgeDoc(BaseModel):
    """Knowledge document projected from persisted intelligence cases."""

    id: str
    title: str
    content: str
    category: str
    tags: List[str]
    similarity_score: Optional[float] = None


class SearchRequest(BaseModel):
    """Knowledge search request."""

    query: str = Field(..., min_length=1)
    limit: int = Field(default=5, ge=1, le=20)
    category: Optional[str] = Field(default=None, max_length=64)


class DiagnoseRequest(BaseModel):
    """Knowledge-assisted diagnosis request."""

    symptoms: str = Field(..., min_length=5)
    device_id: Optional[str] = None
    tags: Optional[Dict[str, Any]] = None


class DiagnoseResponse(BaseModel):
    """Knowledge-assisted diagnosis response."""

    diagnosis_id: str
    root_cause: str
    confidence: float
    suggestions: List[str]
    spare_parts: List[Dict[str, Any]]
    references: List[KnowledgeDoc]


def _case_to_doc(case: Dict[str, Any], *, include_full_content: bool = False) -> KnowledgeDoc:
    content = str(case.get("content") or case.get("summary") or "").strip()
    if not include_full_content and len(content) > 280:
        content = f"{content[:277]}..."

    score = case.get("score")
    similarity = None
    if score is not None:
        similarity = round(min(float(score) / 20.0, 1.0), 2)

    return KnowledgeDoc(
        id=str(case.get("case_id")),
        title=str(case.get("title") or "Untitled knowledge case"),
        content=content,
        category=str(case.get("scene_type") or "general"),
        tags=[str(tag) for tag in (case.get("tags") or []) if str(tag).strip()],
        similarity_score=similarity,
    )


def _resolve_scene_type(request: DiagnoseRequest, service: Any) -> Optional[str]:
    if request.tags:
        for key in ("scene_type", "category", "process"):
            value = request.tags.get(key)
            if value:
                return str(value)
    if request.device_id and getattr(service, "assets_by_id", None):
        asset = service.assets_by_id.get(request.device_id)
        if asset:
            return str(asset.scene_type)
    return None


def _dedupe_strings(values: List[str], *, limit: int = 5) -> List[str]:
    ordered: List[str] = []
    seen: set[str] = set()
    for item in values:
        text = str(item).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        ordered.append(text)
        if len(ordered) >= limit:
            break
    return ordered


def _infer_spare_parts(references: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    spare_parts: List[Dict[str, Any]] = []
    keyword_map = {
        "滤袋": {"name": "滤袋", "quantity": 4},
        "电磁阀": {"name": "脉冲电磁阀", "quantity": 1},
        "轴承": {"name": "轴承", "quantity": 2},
        "密封": {"name": "机械密封", "quantity": 1},
        "风机": {"name": "风机滤网", "quantity": 1},
    }

    for reference in references:
        haystack = " ".join(
            [
                str(reference.get("title") or ""),
                str(reference.get("summary") or ""),
                str(reference.get("content") or ""),
                " ".join(str(tag) for tag in (reference.get("tags") or [])),
            ]
        )
        for keyword, part in keyword_map.items():
            if keyword in haystack and part not in spare_parts:
                spare_parts.append(part)
    return spare_parts[:4]


@router.post("/search", response_model=List[KnowledgeDoc])
async def search_knowledge(
    request: SearchRequest,
    user: UserContext = Depends(require_permissions("data:read")),
):
    service = get_intelligence_service()
    hits = service.search_knowledge_cases(
        request.query,
        scene_type=request.category,
        limit=request.limit,
    )
    service.record_knowledge_activity(
        tenant_id=user.tenant_id,
        event_type="search",
        actor_id=user.user_id,
        query=request.query,
        scene_type=request.category,
        metadata={"limit": request.limit, "result_count": len(hits)},
    )
    audit_logger.log_audit(
        "search_knowledge",
        user.user_id,
        "knowledge",
        "success",
        tenant_id=user.tenant_id,
        result_count=len(hits),
    )
    return [_case_to_doc(case) for case in hits]


@router.get("/categories")
async def get_categories(
    user: UserContext = Depends(require_permissions("data:read")),
):
    service = get_intelligence_service()
    categories = sorted(
        {
            str(case.get("scene_type") or "").strip()
            for case in service.list_knowledge_cases(limit=500)
            if str(case.get("scene_type") or "").strip()
        }
    )
    audit_logger.log_audit(
        "list_knowledge_categories",
        user.user_id,
        "knowledge/categories",
        "success",
        tenant_id=user.tenant_id,
        category_count=len(categories),
    )
    return categories


@router.get("/doc/{doc_id}", response_model=KnowledgeDoc)
async def get_document(
    doc_id: str,
    user: UserContext = Depends(require_permissions("data:read")),
):
    service = get_intelligence_service()
    case = service.get_knowledge_case(doc_id)
    if not case:
        raise HTTPException(status_code=404, detail=f"Knowledge document {doc_id} not found")

    service.repository.increment_knowledge_case_usage(doc_id)
    service.record_knowledge_activity(
        tenant_id=user.tenant_id,
        event_type="view",
        actor_id=user.user_id,
        resource_id=doc_id,
        scene_type=str(case.get("scene_type") or ""),
    )
    audit_logger.log_audit(
        "read_knowledge_document",
        user.user_id,
        doc_id,
        "success",
        tenant_id=user.tenant_id,
    )
    return _case_to_doc(case, include_full_content=True)


@router.post("/diagnose", response_model=DiagnoseResponse)
async def diagnose(
    request: DiagnoseRequest,
    user: UserContext = Depends(require_permissions("data:read")),
):
    service = get_intelligence_service()
    scene_type = _resolve_scene_type(request, service)
    hits = service.search_knowledge_cases(
        request.symptoms,
        scene_type=scene_type,
        limit=5,
    )
    service.record_knowledge_activity(
        tenant_id=user.tenant_id,
        event_type="diagnose",
        actor_id=user.user_id,
        query=request.symptoms,
        scene_type=scene_type,
        resource_id=request.device_id,
        metadata={"reference_count": len(hits)},
    )

    if hits:
        top_hit = hits[0]
        for reference in hits[:3]:
            service.repository.increment_knowledge_case_usage(str(reference.get("case_id")))

        suggestions = _dedupe_strings(
            [
                *(str(action) for hit in hits[:3] for action in (hit.get("recommended_actions") or [])),
                "结合最近一次巡检与历史案例核对关键点位趋势。",
                "如存在持续异常，请联动告警处置并安排现场复核。",
            ],
            limit=5,
        )
        response = DiagnoseResponse(
            diagnosis_id=f"DIAG_{uuid.uuid4().hex[:12].upper()}",
            root_cause=str(top_hit.get("root_cause") or top_hit.get("title") or "需要进一步排查"),
            confidence=round(min(0.45 + (float(top_hit.get("score") or 0.0) / 25.0), 0.97), 2),
            suggestions=suggestions,
            spare_parts=_infer_spare_parts(hits[:3]),
            references=[_case_to_doc(hit) for hit in hits[:4]],
        )
    else:
        response = DiagnoseResponse(
            diagnosis_id=f"DIAG_{uuid.uuid4().hex[:12].upper()}",
            root_cause="现有知识案例中未找到足够匹配项，建议结合实时遥测与巡检结果进一步排查。",
            confidence=0.4,
            suggestions=[
                "复核设备关键点位最近 30 分钟的趋势变化。",
                "检查最新告警与巡检复核队列，确认是否存在相关案例。",
                "必要时补充现场症状描述后再次检索知识库。",
            ],
            spare_parts=[],
            references=[],
        )

    audit_logger.log_audit(
        "knowledge_diagnose",
        user.user_id,
        response.diagnosis_id,
        "success",
        tenant_id=user.tenant_id,
        reference_count=len(response.references),
    )
    return response


@router.post("/feedback")
async def submit_feedback(
    diagnosis_id: str,
    helpful: bool,
    comment: Optional[str] = None,
    user: UserContext = Depends(require_permissions("data:read")),
):
    service = get_intelligence_service()
    feedback = service.record_knowledge_feedback(
        diagnosis_id=diagnosis_id,
        tenant_id=user.tenant_id,
        helpful=helpful,
        created_by=user.user_id,
        comment=comment,
    )
    service.record_knowledge_activity(
        tenant_id=user.tenant_id,
        event_type="feedback",
        actor_id=user.user_id,
        resource_id=diagnosis_id,
        metadata={"helpful": helpful},
    )
    audit_logger.log_audit(
        "submit_knowledge_feedback",
        user.user_id,
        diagnosis_id,
        "success",
        tenant_id=user.tenant_id,
        helpful=helpful,
    )
    return {
        "success": True,
        "message": "Thanks for the feedback.",
        "data": {
            "feedback_id": feedback["feedback_id"],
            "diagnosis_id": diagnosis_id,
            "helpful": helpful,
            "recorded_at": feedback["created_at"].isoformat(),
        },
    }


@router.get("/statistics")
async def get_knowledge_statistics(
    user: UserContext = Depends(require_permissions("data:read")),
):
    service = get_intelligence_service()
    stats = service.get_knowledge_statistics(tenant_id=user.tenant_id)
    audit_logger.log_audit(
        "read_knowledge_statistics",
        user.user_id,
        "knowledge/statistics",
        "success",
        tenant_id=user.tenant_id,
    )
    return stats
