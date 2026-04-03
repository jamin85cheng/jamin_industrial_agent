"""Commercial-ready diagnosis V2 router with workflow visibility."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from src.agents.camel_integration import IndustrialDiagnosisSociety
from src.api.dependencies import UserContext, require_permissions
from src.api.repositories.alert_repository import AlertRepository
from src.diagnosis.multi_agent_diagnosis import MultiAgentDiagnosisEngine
from src.knowledge.graph_rag import graph_rag
from src.models.agent_model_router import AgentModelRouter
from src.models.diagnosis_report import DiagnosisReport, ReportGenerator
from src.tasks.task_tracker import TaskPriority, TaskStatus, TrackedTask, task_tracker
from src.utils.config import _resolve_config_path, load_config
from src.utils.database_runtime import build_runtime_database_adapter
from src.utils.structured_logging import get_logger

router = APIRouter(prefix="/v2/diagnosis", tags=["Diagnosis V2"])
logger = get_logger("diagnosis_v2")
audit_logger = get_logger("diagnosis_audit")

diagnosis_engine = MultiAgentDiagnosisEngine()
camel_society = IndustrialDiagnosisSociety()
alert_repository = AlertRepository()

DIAGNOSIS_TIMEOUT_SECONDS = 2 * 60 * 60
MODEL_PROBE_TIMEOUT_SECONDS = 10 * 60
SSE_POLL_INTERVAL_SECONDS = 1.0
SSE_HEARTBEAT_INTERVAL_SECONDS = 10.0


class DiagnosisRequestV2(BaseModel):
    symptoms: str = Field(..., min_length=5, description="Fault symptom description")
    device_id: Optional[str] = Field(None, description="Related device ID")
    sensor_data: Optional[Dict[str, float]] = Field(default_factory=dict, description="Sensor telemetry payload")
    use_multi_agent: bool = Field(True, description="Use multi-agent diagnosis")
    use_graph_rag: bool = Field(True, description="Use GraphRAG enhancement")
    use_camel: bool = Field(False, description="Use CAMEL collaboration")
    debug: bool = Field(False, description="Return optional debug metadata")
    priority: str = Field("normal", pattern=r"^(critical|high|normal|low)$")


class DiagnosisResponseV2(BaseModel):
    diagnosis_id: str
    status: str
    message: str
    result: Optional[Dict[str, Any]] = None
    task_id: Optional[str] = None


class AlertDiagnosisRequestV2(BaseModel):
    use_graph_rag: bool = Field(True, description="Use GraphRAG enhancement")
    use_camel: bool = Field(False, description="Use CAMEL collaboration")
    debug: bool = Field(True, description="Return optional debug metadata")
    priority: str = Field("high", pattern=r"^(critical|high|normal|low)$")
    sensor_data: Optional[Dict[str, float]] = Field(default_factory=dict, description="Optional sensor telemetry overrides")
    symptoms_override: Optional[str] = Field(None, min_length=5, description="Optional custom symptom summary")


def _task_runtime_summary(task: TrackedTask) -> Dict[str, Any]:
    runtime = dict(task.metadata.get("task_runtime") or {})
    return {
        "storage": runtime.get("storage", task_tracker.storage_label),
        "persistent": bool(runtime.get("persistent", True)),
        "auto_resume": bool(runtime.get("auto_resume", False)),
        "recoverable_state": bool(runtime.get("recoverable_state", True)),
        "target": runtime.get("target", task_tracker.persistence_target),
        "timeout_seconds": int(task.metadata.get("timeout_seconds", runtime.get("default_timeout_seconds", task_tracker.default_timeout))),
    }


def _task_recovery_summary(task: TrackedTask) -> Dict[str, Any]:
    recovery = dict(task.metadata.get("recovery") or {})
    restored = bool(recovery.get("restored_from_persistence", False))
    interrupted = restored and task.error == "Task interrupted because the tracker process restarted."
    return {
        "restored_from_persistence": restored,
        "interrupted_by_restart": interrupted,
        "resume_supported": False,
    }


def _task_workflow_summary(task: TrackedTask) -> Dict[str, Any]:
    trace = list(task.metadata.get("execution_trace") or [])
    current_stage = trace[-1]["stage"] if trace else task.progress.current_action or "pending"
    current_round = max((int(item.get("round", 0) or 0) for item in trace), default=0)

    workflow_status = task.status.value
    recovery = _task_recovery_summary(task)
    if recovery["interrupted_by_restart"]:
        workflow_status = "interrupted"
    elif recovery["restored_from_persistence"]:
        workflow_status = "recovered" if task.status == TaskStatus.COMPLETED else task.status.value

    result = task.result if isinstance(task.result, dict) else {}
    collaboration = result.get("collaboration_result") if isinstance(result, dict) else None
    round_summaries = []
    degraded_mode = False
    if isinstance(collaboration, dict):
        round_summaries = collaboration.get("round_summaries") or []
        degraded_mode = bool(collaboration.get("degraded_mode", False))
    else:
        round_summaries = task.metadata.get("workflow", {}).get("round_summaries", [])
        degraded_mode = bool(task.metadata.get("workflow", {}).get("degraded_mode", False))

    return {
        "status": workflow_status,
        "current_stage": current_stage,
        "current_round": current_round,
        "round_summaries": round_summaries,
        "degraded_mode": degraded_mode,
    }


def _build_task_response(task: TrackedTask) -> Dict[str, Any]:
    return {
        "task_id": task.task_id,
        "task_type": task.task_type,
        "status": task.status.value,
        "progress": task.progress.to_dict(),
        "metadata": task.metadata,
        "result": task.result,
        "error": task.error,
        "duration_seconds": task.duration_seconds(),
        "runtime": _task_runtime_summary(task),
        "recovery": _task_recovery_summary(task),
        "workflow": _task_workflow_summary(task),
    }


def _build_task_snapshot(task: TrackedTask) -> Dict[str, Any]:
    payload = _build_task_response(task)
    payload["event_emitted_at"] = datetime.now(timezone.utc).isoformat()
    return payload


def _serialize_sse(event: str, data: Dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False, default=str)}\n\n"


def _engine_has_live_bindings(engine: Optional[MultiAgentDiagnosisEngine]) -> bool:
    if not engine:
        return False
    return any(profile.get("llm_enabled") for profile in engine.get_agent_runtime_profiles().values())


def _society_has_live_bindings(society: Optional[IndustrialDiagnosisSociety]) -> bool:
    if not society:
        return False
    return any(bool(agent.llm_client) or bool(agent.model_name) for agent in society.agents.values())


def _copy_history(source: Optional[MultiAgentDiagnosisEngine], target: MultiAgentDiagnosisEngine) -> None:
    if not source:
        return
    for key in source._diagnosis_history.keys():
        item = source._diagnosis_history.get(key)
        if item:
            target._diagnosis_history.set(key, item)


def _refresh_runtime_services(force: bool = False):
    global diagnosis_engine, camel_society

    fresh_router = AgentModelRouter()
    routing_should_be_enabled = bool(fresh_router.enabled)

    if force or diagnosis_engine is None or (routing_should_be_enabled and not _engine_has_live_bindings(diagnosis_engine)):
        previous_engine = diagnosis_engine
        diagnosis_engine = MultiAgentDiagnosisEngine(model_router=fresh_router)
        _copy_history(previous_engine, diagnosis_engine)

    if force or camel_society is None or (routing_should_be_enabled and not _society_has_live_bindings(camel_society)):
        camel_society = IndustrialDiagnosisSociety(model_router=AgentModelRouter())

    return diagnosis_engine, camel_society


def _record_task_trace(task: TrackedTask, event: Dict[str, Any]) -> None:
    trace = task.metadata.setdefault("execution_trace", [])
    trace.append(event)
    task.metadata.setdefault("workflow", {})
    task.metadata["workflow"]["current_stage"] = event.get("stage")
    task.metadata["workflow"]["current_round"] = max(
        int(task.metadata["workflow"].get("current_round", 0) or 0),
        int(event.get("round", 0) or 0),
    )
    step_map = {
        "diagnosis_started": 1,
        "graph_rag_started": 2,
        "graph_rag_completed": 3,
        "graph_rag_failed": 3,
        "expert_started": 4,
        "expert_completed": 5,
        "coordinator_started": 6,
        "coordinator_completed": 7,
        "debate_started": 2,
        "debate_agent_started": 3,
        "debate_agent_completed": 4,
        "debate_round_started": 5,
        "debate_round_completed": 6,
        "debate_completed": 8,
        "scenarios_generated": 8,
        "diagnosis_completed": 9,
    }
    task_tracker.update_progress(
        task.task_id,
        step=step_map.get(event.get("stage"), task.progress.current_step),
        action=event.get("message"),
        percentage=float(event.get("progress", task.progress.percentage)),
    )


def _require_task_access(task: TrackedTask, user: UserContext) -> None:
    task_tenant_id = task.metadata.get("tenant_id") or "default"
    user_tenant_id = user.tenant_id or "default"
    if task_tenant_id != user_tenant_id and "admin" not in user.roles:
        raise HTTPException(status_code=403, detail="Task belongs to another tenant")


def _summarize_graph_rag(task: TrackedTask) -> Dict[str, Any]:
    result = task.result if isinstance(task.result, dict) else {}
    if isinstance(result.get("debug"), dict):
        graph_debug = result["debug"].get("graph_rag")
        if isinstance(graph_debug, dict):
            return graph_debug
    if isinstance(result.get("graph_rag"), dict):
        return result["graph_rag"]
    return {
        "enabled": bool(task.metadata.get("use_graph_rag")),
        "query": task.metadata.get("graph_rag_query"),
        "summary": task.metadata.get("graph_rag_summary"),
    }


def _build_report_model(task: TrackedTask) -> DiagnosisReport:
    result = task.result if isinstance(task.result, dict) else {}
    collaboration = result.get("collaboration_result") if isinstance(result, dict) else None
    is_camel = isinstance(collaboration, dict)
    diagnosis_id = result.get("diagnosis_id") or task.task_id
    symptoms = str(result.get("symptoms") or task.metadata.get("symptoms") or task.description)
    device_name = str(task.metadata.get("device_id") or "Unspecified device")

    if is_camel:
        final_decision = collaboration.get("final_decision") or {}
        opinions = collaboration.get("opinions") or []
        root_cause = str(final_decision.get("root_cause") or collaboration.get("consensus_summary", {}).get("leading_root_cause") or "No root cause identified")
        confidence = float(final_decision.get("confidence", collaboration.get("consensus_summary", {}).get("confidence", 0.0)))
        possible_causes = [str(item.get("output", {}).get("root_cause")) for item in opinions if item.get("output", {}).get("root_cause")]
        suggested_actions = [str(item) for item in final_decision.get("actions", [])]
        references = [{"title": str(item)} for item in (_summarize_graph_rag(task).get("summary", {}) or {}).get("sources", [])]
        similar_cases = [{"case_id": str(item)} for item in (_summarize_graph_rag(task).get("summary", {}) or {}).get("sources", [])]
        spare_parts = ["Inspection kit", "Sensor calibration consumables"]
    else:
        root_cause = str(result.get("final_conclusion") or "No root cause identified")
        confidence = float(result.get("confidence", 0.0))
        possible_causes = [str(item.get("root_cause")) for item in result.get("expert_opinions", []) if item.get("root_cause")]
        suggested_actions = [str(item.get("action")) for item in result.get("recommended_actions", []) if item.get("action")]
        references = [{"title": str(item)} for item in result.get("related_cases", [])]
        similar_cases = [{"case_id": str(item)} for item in result.get("related_cases", [])]
        spare_parts = [str(item.get("name")) for item in result.get("spare_parts", []) if item.get("name")]

    trend_payload = {
        "execution_trace": task.metadata.get("execution_trace", []),
        "workflow": _task_workflow_summary(task),
        "graph_rag": _summarize_graph_rag(task),
    }
    return DiagnosisReport(
        report_id=f"RPT_{uuid.uuid4().hex[:10].upper()}",
        diagnosis_id=str(diagnosis_id),
        created_at=datetime.now(timezone.utc),
        device_name=device_name,
        symptoms=symptoms,
        root_cause=root_cause,
        confidence=confidence,
        possible_causes=possible_causes[:5],
        suggested_actions=suggested_actions[:6],
        spare_parts=spare_parts[:6],
        references=references,
        similar_cases=similar_cases,
        trend_charts=[trend_payload],
        operator=task.metadata.get("user_id"),
        notes=f"diagnosis_mode={task.metadata.get('diagnosis_mode', 'unknown')}",
    )


def _export_report(report: DiagnosisReport, export_format: str) -> str:
    generator = ReportGenerator()
    if export_format == "json":
        return generator.generate_json(report)
    if export_format == "html":
        return generator.generate_html(report)
    if export_format == "markdown":
        return generator.generate_markdown(report)
    if export_format == "pdf":
        return generator.generate_pdf(report)
    raise HTTPException(status_code=400, detail=f"Unsupported report format: {export_format}")


def _create_diagnosis_task(
    *,
    request: DiagnosisRequestV2,
    user: UserContext,
    task_type: str,
    description: str,
    diagnosis_mode: str,
) -> TrackedTask:
    metadata = {
        "user_id": user.user_id,
        "tenant_id": user.tenant_id or "default",
        "device_id": request.device_id,
        "debug": request.debug,
        "diagnosis_mode": diagnosis_mode,
        "execution_trace": [],
        "timeout_seconds": DIAGNOSIS_TIMEOUT_SECONDS,
        "symptoms": request.symptoms,
        "sensor_data": request.sensor_data or {},
        "use_graph_rag": request.use_graph_rag,
        "task_runtime": {
            "storage": task_tracker.storage_label,
            "persistent": True,
            "auto_resume": False,
            "recoverable_state": True,
            "default_timeout_seconds": DIAGNOSIS_TIMEOUT_SECONDS,
            "target": task_tracker.persistence_target,
        },
        "workflow": {
            "status": "pending",
            "current_stage": "queued",
            "current_round": 0,
            "round_summaries": [],
            "degraded_mode": False,
        },
    }
    return task_tracker.create_task(
        task_type=task_type,
        description=description,
        priority=TaskPriority[request.priority.upper()],
        metadata=metadata,
    )


def _dispatch_diagnosis_request(
    *,
    request: DiagnosisRequestV2,
    background_tasks: BackgroundTasks,
    user: UserContext,
    entrypoint: str = "manual",
    source_alert: Optional[Dict[str, Any]] = None,
) -> DiagnosisResponseV2:
    sensor_data = request.sensor_data or {}
    context = {
        "user_id": user.user_id,
        "tenant_id": user.tenant_id or "default",
        "device_id": request.device_id,
        "use_graph_rag": request.use_graph_rag,
        "debug": request.debug,
        "entrypoint": entrypoint,
    }
    engine, society = _refresh_runtime_services()

    if request.use_camel:
        task = _create_diagnosis_task(
            request=request,
            user=user,
            task_type="camel_diagnosis",
            description=f"CAMEL diagnosis: {request.symptoms[:80]}",
            diagnosis_mode="camel",
        )
        task.metadata["entrypoint"] = entrypoint
        if source_alert:
            task.metadata["source_alert"] = source_alert
        audit_logger.log_audit("create_diagnosis_task", user.user_id, task.task_id, "success", tenant_id=user.tenant_id, diagnosis_mode="camel", entrypoint=entrypoint)
        background_tasks.add_task(_execute_camel_diagnosis, task, society, request.symptoms, sensor_data, request.debug)
        return DiagnosisResponseV2(
            diagnosis_id=task.task_id,
            status="processing",
            message="CAMEL diagnosis started. Query task status to fetch progress and result.",
            task_id=task.task_id,
        )

    if request.use_multi_agent:
        task = _create_diagnosis_task(
            request=request,
            user=user,
            task_type="multi_agent_diagnosis",
            description=f"Multi-agent diagnosis: {request.symptoms[:80]}",
            diagnosis_mode="multi_agent",
        )
        task.metadata["entrypoint"] = entrypoint
        if source_alert:
            task.metadata["source_alert"] = source_alert
        audit_logger.log_audit("create_diagnosis_task", user.user_id, task.task_id, "success", tenant_id=user.tenant_id, diagnosis_mode="multi_agent", entrypoint=entrypoint)
        background_tasks.add_task(_execute_multi_agent_diagnosis, task, engine, request.symptoms, sensor_data, context, request.debug)
        return DiagnosisResponseV2(
            diagnosis_id=task.task_id,
            status="processing",
            message="Multi-agent diagnosis started. Query task status to fetch progress and result.",
            task_id=task.task_id,
        )

    return DiagnosisResponseV2(
        diagnosis_id="SIMPLE_001",
        status="completed",
        message="Simple diagnosis completed",
        result={"symptoms": request.symptoms},
    )


def _build_alert_symptoms(alert: Dict[str, Any]) -> str:
    segments = []
    if alert.get("rule_name"):
        segments.append(f"告警规则：{alert['rule_name']}")
    segments.append(f"告警内容：{alert.get('message') or '告警触发'}")
    if alert.get("device_id"):
        segments.append(f"设备：{alert['device_id']}")
    if alert.get("tag") and alert.get("value") is not None:
        measurement = f"测点 {alert['tag']} 当前值 {alert['value']}"
        if alert.get("threshold") is not None:
            measurement += f"，阈值 {alert['threshold']}"
        segments.append(measurement)
    if alert.get("severity"):
        segments.append(f"严重级别：{alert['severity']}")
    return "；".join(segments)


@router.post("/analyze", response_model=DiagnosisResponseV2)
async def analyze_v2(
    request: DiagnosisRequestV2,
    background_tasks: BackgroundTasks,
    user: UserContext = Depends(require_permissions("data:read")),
):
    return _dispatch_diagnosis_request(
        request=request,
        background_tasks=background_tasks,
        user=user,
        entrypoint="manual",
    )


@router.post("/alerts/{alert_id}/analyze", response_model=DiagnosisResponseV2)
async def analyze_alert_v2(
    alert_id: str,
    request: AlertDiagnosisRequestV2,
    background_tasks: BackgroundTasks,
    user: UserContext = Depends(require_permissions("alert:read", "data:read")),
):
    alert = alert_repository.get_alert(alert_id, tenant_id=user.tenant_id or "default")
    if not alert:
        raise HTTPException(status_code=404, detail=f"Alert {alert_id} not found")

    sensor_data = dict(request.sensor_data or {})
    if alert.get("tag") and alert.get("value") is not None:
        sensor_data.setdefault(str(alert["tag"]).lower(), float(alert["value"]))

    diagnosis_request = DiagnosisRequestV2(
        symptoms=request.symptoms_override or _build_alert_symptoms(alert),
        device_id=alert.get("device_id"),
        sensor_data=sensor_data,
        use_multi_agent=True,
        use_graph_rag=request.use_graph_rag,
        use_camel=request.use_camel,
        debug=request.debug,
        priority=request.priority,
    )
    source_alert = {
        "alert_id": alert.get("id"),
        "rule_id": alert.get("rule_id"),
        "severity": alert.get("severity"),
        "device_id": alert.get("device_id"),
        "tag": alert.get("tag"),
        "status": alert.get("status"),
    }
    return _dispatch_diagnosis_request(
        request=diagnosis_request,
        background_tasks=background_tasks,
        user=user,
        entrypoint="alert",
        source_alert=source_alert,
    )


async def _execute_camel_diagnosis(
    task: TrackedTask,
    society: IndustrialDiagnosisSociety,
    symptoms: str,
    sensor_data: Dict[str, Any],
    debug: bool = False,
):
    async def diagnosis_work(task_obj: TrackedTask):
        _record_task_trace(
            task_obj,
            {
                "stage": "diagnosis_started",
                "message": "CAMEL diagnosis workflow started",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "progress": 5,
            },
        )
        result = await society.diagnose(
            symptoms,
            sensor_data,
            debug=debug,
            trace_callback=lambda event: _record_task_trace(task_obj, event),
        )
        collaboration = result.get("collaboration_result", {})
        task_obj.metadata["final_result_type"] = "camel"
        task_obj.metadata["agent_model_map"] = collaboration.get("agent_model_map", {})
        task_obj.metadata["fallback_summary"] = collaboration.get("fallback_summary", {})
        task_obj.metadata["workflow"]["round_summaries"] = collaboration.get("round_summaries", [])
        task_obj.metadata["workflow"]["degraded_mode"] = bool(collaboration.get("degraded_mode", False))
        _record_task_trace(
            task_obj,
            {
                "stage": "diagnosis_completed",
                "message": "CAMEL diagnosis workflow completed",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "progress": 100,
            },
        )
        task_tracker.update_progress(task_obj.task_id, step=9, action="Diagnosis completed", percentage=100)
        return result

    await task_tracker.execute(task, diagnosis_work)


async def _execute_multi_agent_diagnosis(
    task: TrackedTask,
    engine: MultiAgentDiagnosisEngine,
    symptoms: str,
    sensor_data: Dict[str, Any],
    context: Dict[str, Any],
    debug: bool = False,
):
    async def diagnosis_work(task_obj: TrackedTask):
        task_tracker.update_progress(task_obj.task_id, step=1, action="Preparing multi-agent diagnosis", percentage=3)

        def trace_callback(event: Dict[str, Any]) -> None:
            _record_task_trace(task_obj, event)

        result = await engine.diagnose(symptoms, sensor_data, context, trace_callback=trace_callback)
        task_obj.metadata["final_result_type"] = "multi_agent"
        task_obj.metadata["agent_model_map"] = result.agent_model_map
        task_obj.metadata["fallback_summary"] = result.fallback_summary
        task_obj.metadata["coordinator_metadata"] = result.coordinator_metadata
        task_obj.metadata["workflow"]["degraded_mode"] = bool(
            result.coordinator_metadata.get("used_fallback")
            or any(item.get("used_fallback") for item in result.fallback_summary.get("experts", {}).values())
        )
        if debug:
            task_obj.metadata["debug"] = result.debug_metadata
        task_tracker.update_progress(task_obj.task_id, step=9, action="Diagnosis completed", percentage=100)
        return result.to_dict(include_debug=debug)

    await task_tracker.execute(task, diagnosis_work)


@router.get("/task/{task_id}")
async def get_diagnosis_task(task_id: str, user: UserContext = Depends(require_permissions("data:read"))):
    task = task_tracker.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    _require_task_access(task, user)
    audit_logger.log_audit("read_diagnosis_task", user.user_id, task_id, "success", tenant_id=user.tenant_id)
    return _build_task_response(task)


@router.get("/task/{task_id}/events")
async def stream_diagnosis_task_events(
    task_id: str,
    request: Request,
    user: UserContext = Depends(require_permissions("data:read")),
):
    task = task_tracker.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    _require_task_access(task, user)
    audit_logger.log_audit("stream_diagnosis_task", user.user_id, task_id, "success", tenant_id=user.tenant_id)

    async def event_generator():
        last_snapshot = ""
        last_heartbeat = time.monotonic()

        while True:
            if await request.is_disconnected():
                break

            current_task = task_tracker.get_task(task_id)
            if not current_task:
                yield _serialize_sse(
                    "error",
                    {
                        "task_id": task_id,
                        "message": "Task not found",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    },
                )
                break

            snapshot = _build_task_snapshot(current_task)
            serialized = json.dumps(snapshot, ensure_ascii=False, default=str, sort_keys=True)
            if serialized != last_snapshot:
                yield _serialize_sse("snapshot", snapshot)
                last_snapshot = serialized

            workflow = snapshot.get("workflow") or {}
            if snapshot["status"] in {"completed", "failed", "timeout", "cancelled"} or workflow.get("status") == "interrupted":
                yield _serialize_sse(
                    "complete",
                    {
                        "task_id": task_id,
                        "status": snapshot["status"],
                        "workflow_status": workflow.get("status"),
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    },
                )
                break

            now = time.monotonic()
            if now - last_heartbeat >= SSE_HEARTBEAT_INTERVAL_SECONDS:
                yield _serialize_sse(
                    "heartbeat",
                    {
                        "task_id": task_id,
                        "status": snapshot["status"],
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    },
                )
                last_heartbeat = now

            await asyncio.sleep(SSE_POLL_INTERVAL_SECONDS)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/tasks")
async def list_diagnosis_tasks(
    limit: int = Query(20, ge=1, le=100),
    status: Optional[str] = Query(None),
    diagnosis_mode: Optional[str] = Query(None, pattern=r"^(multi_agent|camel)$"),
    user: UserContext = Depends(require_permissions("data:read")),
):
    filtered_status = None
    if status:
        try:
            filtered_status = TaskStatus(status)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"Unsupported task status: {status}") from exc

    diagnosis_types = {"multi_agent_diagnosis", "camel_diagnosis"}
    user_tenant_id = user.tenant_id or "default"
    raw_tasks = task_tracker.list_tasks(status=filtered_status, limit=max(limit * 3, limit))
    tasks = []
    for task in raw_tasks:
        if task.task_type not in diagnosis_types:
            continue
        if (task.metadata.get("tenant_id") or "default") != user_tenant_id and "admin" not in user.roles:
            continue
        if diagnosis_mode and task.metadata.get("diagnosis_mode") != diagnosis_mode:
            continue
        tasks.append(_build_task_response(task))
        if len(tasks) >= limit:
            break

    audit_logger.log_audit("list_diagnosis_tasks", user.user_id, "diagnosis_tasks", "success", tenant_id=user.tenant_id, total=len(tasks))
    return {"total": len(tasks), "tasks": tasks}


@router.get("/task/{task_id}/report")
async def export_diagnosis_report(
    task_id: str,
    format: str = Query("html", pattern=r"^(html|pdf|markdown|json)$"),
    user: UserContext = Depends(require_permissions("report:read", "report:export")),
):
    task = task_tracker.get_task(task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    _require_task_access(task, user)
    if task.status != TaskStatus.COMPLETED or not task.result:
        raise HTTPException(status_code=409, detail="Task is not completed yet")

    report_model = _build_report_model(task)
    output_path = _export_report(report_model, format)
    audit_logger.log_audit("export_diagnosis_report", user.user_id, task_id, "success", tenant_id=user.tenant_id, export_format=format)
    return {
        "task_id": task_id,
        "format": format,
        "path": output_path,
        "filename": Path(output_path).name,
        "report_id": report_model.report_id,
        "generated_at": report_model.created_at.isoformat(),
    }


@router.post("/knowledge/query")
async def query_knowledge_graph(query: str, user: UserContext = Depends(require_permissions("data:read"))):
    return await graph_rag.query(query)


@router.get("/knowledge/graph")
async def get_knowledge_graph(
    entity_id: Optional[str] = None,
    depth: int = 2,
    user: UserContext = Depends(require_permissions("data:read")),
):
    if entity_id:
        return graph_rag.kg.subgraph_query(entity_id, depth)
    return graph_rag.kg.to_dict()


@router.get("/history")
async def get_diagnosis_history(limit: int = 10, user: UserContext = Depends(require_permissions("data:read"))):
    engine, _ = _refresh_runtime_services()
    history = engine.get_diagnosis_history(limit)
    return {"total": len(history), "history": [item.to_dict() for item in history]}


@router.get("/experts")
async def get_expert_agents(user: UserContext = Depends(require_permissions("data:read"))):
    engine, _ = _refresh_runtime_services()
    return engine.get_agent_catalog()


@router.get("/society/status")
async def get_society_status(user: UserContext = Depends(require_permissions("data:read"))):
    _, society = _refresh_runtime_services()
    return society.get_society_status()


@router.get("/runtime-debug")
async def get_runtime_debug(user: UserContext = Depends(require_permissions("data:read"))):
    engine, society = _refresh_runtime_services(force=True)
    config = load_config()
    config_path = _resolve_config_path("config/settings.yaml")
    metadata_database = build_runtime_database_adapter(config.get("database", {}))
    route_keys = ["default", "mechanical", "electrical", "process", "sensor", "historical", "coordinator", "critic"]
    router_obj = engine.model_router

    configured_profiles = {}
    for route_key in route_keys:
        profile = router_obj.get_profile(route_key) if router_obj else None
        configured_profiles[route_key] = {
            "endpoint": profile.endpoint,
            "model": profile.model,
            "timeout_seconds": profile.timeout_seconds,
        } if profile else None

    return {
        "process": {
            "pid": os.getpid(),
            "cwd": os.getcwd(),
            "python_executable": sys.executable,
            "module_file": __file__,
            "sys_path_head": sys.path[:5],
        },
        "config": {
            "resolved_path": str(config_path),
            "exists": config_path.exists(),
            "llm_provider": config.get("llm", {}).get("provider"),
            "routing_enabled_in_config": config.get("llm", {}).get("agent_routing", {}).get("enabled"),
            "configured_route_keys": sorted(list(config.get("llm", {}).get("agent_routing", {}).get("agents", {}).keys())),
            "endpoint_keys": sorted(list(config.get("llm", {}).get("endpoints", {}).keys())),
            "task_tracking_backend": config.get("database", {}).get("task_tracking", {}).get("backend"),
            "postgres_enabled": config.get("database", {}).get("postgres", {}).get("enabled"),
            "postgres_database": config.get("database", {}).get("postgres", {}).get("database"),
            "postgres_schema": config.get("database", {}).get("postgres", {}).get("schema"),
            "metadata_database_backend": metadata_database.backend,
        },
        "runtime": {
            "engine_enable_model_routing": engine.enable_model_routing,
            "router_enabled": bool(router_obj and router_obj.enabled),
            "agent_profiles": configured_profiles,
            "agent_runtime_profiles": engine.get_agent_runtime_profiles(),
            "society_agents": society.get_society_status().get("agents", []),
            "task_tracker": task_tracker.get_stats(),
            "metadata_database": {
                "backend": metadata_database.backend,
                "target": metadata_database.target,
            },
        },
    }


@router.post("/model-probe")
async def probe_routed_models(user: UserContext = Depends(require_permissions("data:read"))):
    engine, _ = _refresh_runtime_services()
    router_obj = engine.model_router
    route_keys = ["mechanical", "electrical", "process", "sensor", "historical", "coordinator", "critic"]

    async def run_probe(route_key: str):
        profile = router_obj.get_profile(route_key) if router_obj else None
        client = router_obj.get_client(route_key) if router_obj else None
        if not profile or not client:
            return {
                "route_key": route_key,
                "endpoint": getattr(profile, "endpoint", None),
                "model_name": getattr(profile, "model", None),
                "success": False,
                "latency_ms": None,
                "response_excerpt": None,
                "error": "model routing is disabled or route is not configured",
            }
        started = time.perf_counter()
        try:
            response = await asyncio.to_thread(
                client.complete,
                'Return exactly one short JSON object like {"status":"ok"} with no markdown.',
                temperature=0,
                max_tokens=80,
            )
            return {
                "route_key": route_key,
                "endpoint": profile.endpoint,
                "model_name": profile.model,
                "success": True,
                "latency_ms": round((time.perf_counter() - started) * 1000, 2),
                "response_excerpt": str(response).strip()[:240],
                "error": None,
            }
        except Exception as exc:
            return {
                "route_key": route_key,
                "endpoint": profile.endpoint,
                "model_name": profile.model,
                "success": False,
                "latency_ms": round((time.perf_counter() - started) * 1000, 2),
                "response_excerpt": None,
                "error": str(exc),
            }

    task = task_tracker.create_task(
        task_type="model_probe",
        description="Probe routed diagnosis models",
        priority=TaskPriority.NORMAL,
        metadata={
            "user_id": user.user_id,
            "tenant_id": user.tenant_id or "default",
            "timeout_seconds": MODEL_PROBE_TIMEOUT_SECONDS,
        },
    )
    probes = await asyncio.gather(*[run_probe(route_key) for route_key in route_keys])
    task.status = TaskStatus.COMPLETED
    task.result = {"success": all(item["success"] for item in probes), "tested_at": datetime.now(timezone.utc).isoformat(), "probes": probes}
    task.completed_at = datetime.now(timezone.utc)
    audit_logger.log_audit("probe_models", user.user_id, task.task_id, "success", tenant_id=user.tenant_id)
    return task.result
