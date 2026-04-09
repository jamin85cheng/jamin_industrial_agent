import asyncio

from datetime import datetime, timezone
from pathlib import Path
from fastapi import BackgroundTasks

from src.agents.camel_integration import IndustrialDiagnosisSociety
from src.api.dependencies import UserContext
from src.api.repositories.alert_repository import AlertRepository
from src.api.repositories.report_repository import ReportRepository
from src.api.routers import diagnosis_v2
from src.diagnosis.multi_agent_diagnosis import ExpertType, MultiAgentDiagnosisEngine
from src.tasks.task_tracker import TaskPriority, TaskStatus, TaskTracker, TrackedTask, task_tracker


def test_multi_agent_diagnosis_integrates_graph_rag_context():
    engine = MultiAgentDiagnosisEngine(enable_model_routing=False)

    result = asyncio.run(
        engine.diagnose(
            '曝气池溶解氧持续偏低，风机噪声异常',
            {'do': 1.5, 'vibration': 8.5, 'current': 25.3},
            {'use_graph_rag': True},
        )
    )

    historical_opinion = next(op for op in result.expert_opinions if op.expert_type == ExpertType.HISTORICAL)
    debug_payload = result.to_dict(include_debug=True)['debug']

    assert result.consensus_level >= 0.2
    assert historical_opinion.root_cause
    assert any('GraphRAG' in evidence for evidence in historical_opinion.evidence)
    assert 'mechanical' in result.agent_model_map
    assert 'coordinator' in result.agent_model_map
    assert 'experts' in result.fallback_summary
    assert 'coordinator' in result.fallback_summary
    assert 'used_fallback' in result.coordinator_metadata
    assert debug_payload['graph_rag']['enabled'] is True
    assert debug_payload['graph_rag']['query']
    assert debug_payload['experts']['historical']['response_excerpt']


def test_multi_agent_diagnosis_without_graph_rag_keeps_default_related_cases():
    engine = MultiAgentDiagnosisEngine(enable_model_routing=False)

    result = asyncio.run(
        engine.diagnose(
            '曝气池溶解氧持续偏低，风机噪声异常',
            {'do': 1.5, 'vibration': 8.5, 'current': 25.3},
            {'use_graph_rag': False},
        )
    )

    payload = result.to_dict(include_debug=True)

    assert result.related_cases == ['CASE_20230815_001', 'CASE_20231022_003']
    assert payload['debug']['graph_rag']['enabled'] is False
    assert result.coordinator_metadata['used_fallback'] is False


def test_camel_society_debate_mode_runs_without_recursion_and_supports_debug():
    society = IndustrialDiagnosisSociety(enable_model_routing=False)

    result = asyncio.run(
        society.diagnose(
            '曝气池溶解氧持续偏低，风机噪声异常',
            {'do': 1.5, 'vibration': 8.5, 'current': 25.3},
            debug=True,
        )
    )

    collaboration_result = result['collaboration_result']

    assert collaboration_result['mode'] == 'debate'
    assert collaboration_result['rounds'] >= 2
    assert collaboration_result['message_count'] >= 15
    assert 1 <= collaboration_result['consensus_summary']['participants'] <= 5
    assert 'round_outputs' in collaboration_result
    assert 'round_summaries' in collaboration_result
    assert 'final_decision' in collaboration_result
    assert 'degraded_mode' in collaboration_result
    assert 'agent_model_map' in collaboration_result
    assert 'fallback_summary' in collaboration_result
    assert 'debug' in collaboration_result
    assert 'agents' in collaboration_result['debug']


def test_diagnosis_v2_refreshes_stale_runtime_services():
    router_config = {
        'llm': {
            'generation': {'temperature': 0.2, 'max_tokens': 256},
            'endpoints': {
                'local': {'base_url': 'http://127.0.0.1:8588/v1', 'api_key': 'none', 'timeout_seconds': 15},
                'cloud': {'base_url': 'https://example.com/v1', 'api_key': 'secret', 'timeout_seconds': 15},
            },
            'agent_routing': {
                'enabled': True,
                'agents': {
                    'default': {'endpoint': 'local', 'model': 'Qwen3.5-9B'},
                    'historical': {'endpoint': 'cloud', 'model': 'GLM-5'},
                    'critic': {'endpoint': 'cloud', 'model': 'Qwen3-Coder-480B-A35B-Instruct'},
                    'coordinator': {'endpoint': 'cloud', 'model': 'DeepSeek-V3.2'},
                },
            },
        }
    }

    stale_engine = MultiAgentDiagnosisEngine(enable_model_routing=False)
    stale_society = IndustrialDiagnosisSociety(enable_model_routing=False)
    stale_result = asyncio.run(
        stale_engine.diagnose(
            '曝气池溶解氧持续偏低，风机噪声异常',
            {'do': 1.5, 'vibration': 8.5, 'current': 25.3},
            {'use_graph_rag': False},
        )
    )

    original_engine = diagnosis_v2.diagnosis_engine
    original_society = diagnosis_v2.camel_society
    diagnosis_v2.diagnosis_engine = stale_engine
    diagnosis_v2.camel_society = stale_society
    original_router_cls = diagnosis_v2.AgentModelRouter
    try:
        diagnosis_v2.AgentModelRouter = lambda: original_router_cls(config=router_config)
        refreshed_engine, refreshed_society = diagnosis_v2._refresh_runtime_services(force=False)
    finally:
        diagnosis_v2.diagnosis_engine = original_engine
        diagnosis_v2.camel_society = original_society
        diagnosis_v2.AgentModelRouter = original_router_cls

    runtime_profiles = refreshed_engine.get_agent_runtime_profiles()

    assert refreshed_engine is not stale_engine
    assert refreshed_society is not stale_society
    assert runtime_profiles['mechanical']['model_name'] == 'Qwen3.5-9B'
    assert runtime_profiles['historical']['model_name'] == 'GLM-5'
    assert refreshed_engine.get_diagnosis_history(1)[0].diagnosis_id == stale_result.diagnosis_id


def test_diagnosis_v2_task_response_exposes_runtime_and_recovery():
    task = TrackedTask(
        task_id="TASK_DEMO",
        task_type="multi_agent_diagnosis",
        description="demo",
        status=TaskStatus.FAILED,
        priority=TaskPriority.NORMAL,
        created_at=datetime.now(timezone.utc),
        error="Task interrupted because the tracker process restarted.",
        metadata={
            "timeout_seconds": 7200,
            "task_runtime": {
                "storage": "sqlite",
                "persistent": True,
                "auto_resume": False,
                "recoverable_state": True,
                "executor": {"backend": "background_tasks", "requested_backend": "background_tasks"},
            },
            "recovery": {"restored_from_persistence": True},
        },
    )

    payload = diagnosis_v2._build_task_response(task)

    assert payload["runtime"]["storage"] == "sqlite"
    assert payload["runtime"]["persistent"] is True
    assert payload["runtime"]["timeout_seconds"] == 7200
    assert payload["runtime"]["executor"]["backend"] == "background_tasks"
    assert payload["recovery"]["restored_from_persistence"] is True
    assert payload["recovery"]["interrupted_by_restart"] is True
    assert payload["workflow"]["status"] == "interrupted"


def test_diagnosis_v2_task_response_exposes_resume_for_recovered_queue():
    task = TrackedTask(
        task_id="TASK_RESUME",
        task_type="multi_agent_diagnosis",
        description="resume demo",
        status=TaskStatus.QUEUED,
        priority=TaskPriority.NORMAL,
        created_at=datetime.now(timezone.utc),
        metadata={
            "task_runtime": {
                "storage": "sqlite",
                "persistent": True,
                "auto_resume": False,
                "recoverable_state": True,
            },
            "recovery": {
                "restored_from_persistence": True,
                "resume_required": True,
                "resume_count": 1,
            },
        },
    )

    payload = diagnosis_v2._build_task_response(task)

    assert payload["recovery"]["resume_supported"] is True
    assert payload["recovery"]["resume_required"] is True
    assert payload["controls"]["resumable"] is True
    assert payload["controls"]["resume_count"] == 1


def test_diagnosis_v2_task_event_stream_emits_snapshot_and_complete():
    task = task_tracker.create_task(
        task_type="multi_agent_diagnosis",
        description="stream demo",
        priority=TaskPriority.NORMAL,
        metadata={"tenant_id": "default", "diagnosis_mode": "multi_agent", "execution_trace": []},
    )
    task.status = TaskStatus.COMPLETED
    task.result = {"diagnosis_id": "demo", "final_conclusion": "轴承磨损"}
    task.completed_at = datetime.now(timezone.utc)

    class DummyRequest:
        async def is_disconnected(self):
            return False

    user = UserContext(user_id="operator", username="operator", roles=["operator"], tenant_id="default", permissions=["data:read"])

    async def collect_stream():
        response = await diagnosis_v2.stream_diagnosis_task_events(task.task_id, DummyRequest(), user)
        chunks = []
        async for chunk in response.body_iterator:
            chunks.append(chunk.decode() if isinstance(chunk, bytes) else chunk)
        return "".join(chunks)

    body = asyncio.run(collect_stream())

    assert "event: snapshot" in body
    assert "event: complete" in body
    assert task.task_id in body


def test_diagnosis_v2_can_start_task_from_alert():
    class FakeAlertRepository:
        def get_alert(self, alert_id: str, tenant_id: str):
            return {
                "id": alert_id,
                "rule_id": "RULE_001",
                "rule_name": "高温告警",
                "severity": "critical",
                "message": "温度超过上限",
                "device_id": "DEV_AERATION_01",
                "tag": "temperature",
                "value": 105.5,
                "threshold": 100.0,
                "status": "active",
            }

    original_repo = diagnosis_v2.alert_repository
    diagnosis_v2.alert_repository = FakeAlertRepository()
    user = UserContext(user_id="operator", username="operator", roles=["operator"], tenant_id="default", permissions=["alert:read", "data:read"])

    try:
        response = asyncio.run(
            diagnosis_v2.analyze_alert_v2(
                "ALT_DEMO_01",
                diagnosis_v2.AlertDiagnosisRequestV2(),
                BackgroundTasks(),
                user,
            )
        )
    finally:
        diagnosis_v2.alert_repository = original_repo

    assert response.status == "processing"
    assert response.task_id
    task = task_tracker.get_task(response.task_id)
    assert task is not None
    assert task.status == TaskStatus.QUEUED
    assert task.metadata["entrypoint"] == "alert"
    assert task.metadata["source_alert"]["alert_id"] == "ALT_DEMO_01"
    assert task.metadata["device_id"] == "DEV_AERATION_01"
    assert task.metadata["task_runtime"]["executor"]["backend"] == "background_tasks"


def test_diagnosis_v2_task_response_exposes_controls():
    task = TrackedTask(
        task_id="TASK_CONTROL",
        task_type="multi_agent_diagnosis",
        description="control demo",
        status=TaskStatus.CANCELLED,
        priority=TaskPriority.NORMAL,
        created_at=datetime.now(timezone.utc),
        error="Task cancelled by user.",
        metadata={
            "retry_count": 1,
            "retry_of_task_id": "TASK_PREVIOUS",
            "cancel_requested_at": datetime.now(timezone.utc).isoformat(),
            "cancelled_by": "operator",
            "cancellation_reason": "duplicate diagnosis",
        },
    )

    payload = diagnosis_v2._build_task_response(task)

    assert payload["controls"]["cancellable"] is False
    assert payload["controls"]["retryable"] is True
    assert payload["controls"]["retry_count"] == 1
    assert payload["controls"]["retry_of_task_id"] == "TASK_PREVIOUS"
    assert payload["controls"]["cancelled_by"] == "operator"


def test_diagnosis_v2_can_cancel_pending_task():
    task = task_tracker.create_task(
        task_type="multi_agent_diagnosis",
        description="cancel demo",
        priority=TaskPriority.NORMAL,
        metadata={
            "tenant_id": "default",
            "diagnosis_mode": "multi_agent",
            "execution_trace": [],
        },
    )
    user = UserContext(user_id="operator", username="operator", roles=["operator"], tenant_id="default", permissions=["data:read"])

    response = asyncio.run(
        diagnosis_v2.cancel_diagnosis_task(
            task.task_id,
            diagnosis_v2.TaskCancelRequest(reason="cancel for retry"),
            user,
        )
    )

    assert response["status"] == "cancelled"
    assert response["controls"]["retryable"] is True
    assert response["controls"]["cancellation_reason"] == "cancel for retry"


def test_diagnosis_v2_can_retry_cancelled_task():
    task = task_tracker.create_task(
        task_type="multi_agent_diagnosis",
        description="retry demo",
        priority=TaskPriority.NORMAL,
        metadata={
            "tenant_id": "default",
            "diagnosis_mode": "multi_agent",
            "execution_trace": [],
            "symptoms": "曝气池溶解氧偏低，风机噪声异常",
            "sensor_data": {"do": 1.4, "vibration": 8.2},
            "use_graph_rag": True,
            "debug": True,
            "device_id": "DEV_RETRY_01",
        },
    )
    task.status = TaskStatus.CANCELLED
    task.error = "Task cancelled by user."
    task.completed_at = datetime.now(timezone.utc)
    user = UserContext(user_id="operator", username="operator", roles=["operator"], tenant_id="default", permissions=["data:read"])

    response = asyncio.run(
        diagnosis_v2.retry_diagnosis_task(
            task.task_id,
            BackgroundTasks(),
            user,
        )
    )

    assert response.status == "processing"
    assert response.task_id
    retry_task = task_tracker.get_task(response.task_id)
    assert retry_task is not None
    assert retry_task.metadata["retry_of_task_id"] == task.task_id
    assert retry_task.metadata["retry_count"] == 1
    assert retry_task.metadata["entrypoint"] == "retry"
    assert retry_task.metadata["device_id"] == "DEV_RETRY_01"


def test_diagnosis_v2_can_resume_recovered_queued_task():
    task = task_tracker.create_task(
        task_type="multi_agent_diagnosis",
        description="resume demo",
        priority=TaskPriority.NORMAL,
        metadata={
            "tenant_id": "default",
            "user_id": "operator",
            "diagnosis_mode": "multi_agent",
            "execution_trace": [],
            "symptoms": "曝气池溶解氧偏低，风机噪声异常",
            "sensor_data": {"do": 1.3, "vibration": 8.1},
            "use_graph_rag": True,
            "debug": True,
            "device_id": "DEV_RESUME_01",
            "task_runtime": {
                "storage": "sqlite",
                "persistent": True,
                "auto_resume": False,
                "recoverable_state": True,
                "target": "sqlite:///tasks.sqlite",
            },
            "recovery": {
                "restored_from_persistence": True,
                "resume_required": True,
            },
        },
    )
    task.status = TaskStatus.QUEUED
    background_tasks = BackgroundTasks()
    user = UserContext(user_id="operator", username="operator", roles=["operator"], tenant_id="default", permissions=["data:read"])

    response = asyncio.run(
        diagnosis_v2.resume_diagnosis_task(
            task.task_id,
            background_tasks,
            user,
        )
    )

    assert response.status == "processing"
    assert response.task_id == task.task_id
    assert len(background_tasks.tasks) == 1
    resumed_task = task_tracker.get_task(task.task_id)
    assert resumed_task is not None
    assert resumed_task.metadata["entrypoint"] == "resume"
    assert resumed_task.metadata["recovery"]["resume_required"] is False
    assert resumed_task.metadata["recovery"]["last_resumed_by"] == "operator"
    assert resumed_task.metadata["recovery"]["resume_count"] == 1


def test_diagnosis_v2_bootstrap_auto_resumes_recovered_tasks(tmp_path, monkeypatch):
    tracker = TaskTracker(db_path=tmp_path / "bootstrap_tasks.sqlite")
    task = tracker.create_task(
        task_type="multi_agent_diagnosis",
        description="bootstrap resume demo",
        priority=TaskPriority.NORMAL,
        metadata={
            "tenant_id": "default",
            "user_id": "system",
            "diagnosis_mode": "multi_agent",
            "task_runtime": {
                "storage": "sqlite",
                "persistent": True,
                "auto_resume": True,
                "recoverable_state": True,
                "target": str(tmp_path / "bootstrap_tasks.sqlite"),
            },
            "recovery": {
                "restored_from_persistence": True,
                "resume_required": True,
            },
        },
    )
    task.status = TaskStatus.QUEUED
    resumed_task_ids = []

    async def fake_schedule(task_obj, *, background_tasks, entrypoint, resumed_by):
        resumed_task_ids.append(task_obj.task_id)
        task_obj.metadata.setdefault("recovery", {})["resume_required"] = False
        return diagnosis_v2.DiagnosisResponseV2(
            diagnosis_id=task_obj.task_id,
            status="processing",
            message="auto resumed",
            task_id=task_obj.task_id,
        )

    monkeypatch.setattr(diagnosis_v2, "task_tracker", tracker)
    monkeypatch.setattr(
        diagnosis_v2,
        "load_config",
        lambda *args, **kwargs: {
            "diagnosis": {
                "execution": {
                    "backend": "asyncio_queue",
                    "asyncio_workers": 1,
                    "auto_resume_recovered": True,
                }
            }
        },
    )
    monkeypatch.setattr(diagnosis_v2, "_schedule_existing_diagnosis_task", fake_schedule)

    state = asyncio.run(diagnosis_v2.bootstrap_diagnosis_runtime())

    assert resumed_task_ids == [task.task_id]
    assert state["auto_resume_enabled"] is True
    assert state["auto_resumed_task_ids"] == [task.task_id]
    assert state["auto_resume_skipped_reason"] is None


def test_diagnosis_v2_bootstrap_skips_auto_resume_without_asyncio_backend(monkeypatch):
    monkeypatch.setattr(
        diagnosis_v2,
        "load_config",
        lambda *args, **kwargs: {
            "diagnosis": {
                "execution": {
                    "backend": "background_tasks",
                    "asyncio_workers": 1,
                    "auto_resume_recovered": True,
                }
            }
        },
    )

    state = asyncio.run(diagnosis_v2.bootstrap_diagnosis_runtime())

    assert state["auto_resume_enabled"] is True
    assert state["auto_resumed_task_ids"] == []
    assert state["auto_resume_skipped_reason"] == "Auto-resume requires the asyncio_queue executor backend."


def test_diagnosis_v2_export_report_persists_report_metadata_and_alert_link(tmp_path, monkeypatch):
    metadata_path = tmp_path / "metadata.db"
    alert_repo = AlertRepository(
        {
            "sqlite": {"path": str(metadata_path)},
            "postgres": {"enabled": False},
        }
    )
    report_repo = ReportRepository(
        {
            "sqlite": {"path": str(metadata_path)},
            "postgres": {"enabled": False},
        }
    )
    alert_repo.init_schema()
    report_repo.init_schema()

    alert_id = alert_repo.create_alert(
        rule_id=None,
        message="曝气池溶解氧持续偏低",
        severity="critical",
        device_id="DEV_EXPORT_01",
        tag="DO",
        value=1.5,
        threshold=2.0,
        tenant_id="default",
    )

    task = task_tracker.create_task(
        task_type="multi_agent_diagnosis",
        description="report export demo",
        priority=TaskPriority.NORMAL,
        metadata={
            "tenant_id": "default",
            "user_id": "operator",
            "device_id": "DEV_EXPORT_01",
            "diagnosis_mode": "multi_agent",
            "execution_trace": [],
            "entrypoint": "alert",
            "source_alert": {"alert_id": alert_id},
        },
    )
    task.status = TaskStatus.COMPLETED
    task.completed_at = datetime.now(timezone.utc)
    task.result = {
        "diagnosis_id": "DIAG_EXPORT_01",
        "symptoms": "曝气池溶解氧持续偏低",
        "final_conclusion": "怀疑鼓风机效率下降",
        "confidence": 0.87,
        "expert_opinions": [
            {
                "expert_name": "机械专家",
                "root_cause": "鼓风机叶轮磨损",
            }
        ],
        "recommended_actions": [
            {"action": "安排鼓风机巡检"},
        ],
        "related_cases": ["CASE_EXPORT_01"],
        "spare_parts": [{"name": "风机叶轮"}],
    }

    report_file = tmp_path / "diagnosis-report.html"
    report_file.write_text("<html><body>report</body></html>", encoding="utf-8")

    monkeypatch.setattr(diagnosis_v2, "alert_repository", alert_repo)
    monkeypatch.setattr(diagnosis_v2, "report_repository", report_repo)
    monkeypatch.setattr(diagnosis_v2, "_export_report", lambda report, export_format: str(report_file))

    user = UserContext(
        user_id="operator",
        username="operator",
        roles=["operator"],
        tenant_id="default",
        permissions=["report:read", "report:export"],
    )

    response = asyncio.run(
        diagnosis_v2.export_diagnosis_report(
            task.task_id,
            format="html",
            user=user,
        )
    )

    assert "path" not in response
    assert response["report_id"]
    assert response["download_url"] == f"/reports/{response['report_id']}/download"

    persisted = report_repo.get_report(response["report_id"], tenant_id="default")
    assert persisted is not None
    assert persisted["task_id"] == task.task_id
    assert persisted["filename"] == Path(report_file).name

    linked_alert = alert_repo.get_alert(alert_id, tenant_id="default")
    assert linked_alert is not None
    assert linked_alert["latest_report_id"] == response["report_id"]
