import asyncio

from src.intelligence.repository import IntelligenceRepository
from src.intelligence.runtime import IntelligentPatrolScheduler
from src.intelligence.service import IndustrialIntelligenceService
from src.tasks.task_tracker import TaskTracker


def _make_service(tmp_path, *, min_candidate=2, min_model=2):
    db_config = {
        "sqlite": {"path": str(tmp_path / "metadata.db")},
        "postgres": {"enabled": False},
    }
    config = {
        "database": db_config,
        "intelligence": {
            "llm": {"enabled": False},
            "patrol": {"interval_seconds": 180, "run_on_startup": False},
            "learning": {
                "min_confirmed_labels_for_candidate": min_candidate,
                "min_confirmed_labels_for_model_candidate": min_model,
                "min_external_grounding_ratio": 0.3,
            },
        },
    }
    repository = IntelligenceRepository(db_config)
    tracker = TaskTracker(db_path=tmp_path / "tasks.sqlite")
    return IndustrialIntelligenceService(config=config, repository=repository, tracker=tracker)


def _ingest_abnormal_snapshot(service: IndustrialIntelligenceService):
    return service.ingest_snapshot(
        asset_id="ASSET_DUST_COLLECTOR_01",
        points={
            "pressure_diff_kpa": {"value": 2.25},
            "fan_current_a": {"value": 17.2},
            "airflow_m3h": {"value": 7000},
            "dust_concentration_mg_m3": {"value": 28.5},
            "cleaning_frequency_hz": {"value": 1.85},
            "valve_state": {"value": "closed"},
            "temperature_c": {"value": 95},
            "running_state": {"value": True},
        },
    )


def test_patrol_run_flags_dust_risk_and_creates_review_label(tmp_path):
    service = _make_service(tmp_path)
    _ingest_abnormal_snapshot(service)

    result = asyncio.run(service.run_patrol(triggered_by="tester"))

    assert result["run_id"].startswith("PATROL_")
    assert result["labels_created"]
    asset_result = result["asset_results"][0]
    assert asset_result["risk_level"] == "high_risk"
    assert asset_result["requires_review"] is True
    assert "滤袋堵塞" in asset_result["suspected_faults"]
    assert asset_result["review_label_id"].startswith("LABEL_")


def test_confirmed_label_becomes_knowledge_case_and_affects_next_patrol(tmp_path):
    service = _make_service(tmp_path)
    _ingest_abnormal_snapshot(service)
    first_run = asyncio.run(service.run_patrol(triggered_by="tester"))
    label_id = first_run["labels_created"][0]["label_id"]

    review_result = asyncio.run(
        service.confirm_label(
            label_id,
            reviewer="engineer",
            anomaly_type="滤袋堵塞",
            root_cause="滤袋堵塞",
            review_notes="现场确认滤袋积灰严重。",
            final_action="安排停机检修滤袋",
        )
    )

    assert review_result["knowledge_case"]["source_type"] == "confirmed_label"

    _ingest_abnormal_snapshot(service)
    second_run = asyncio.run(service.run_patrol(triggered_by="tester"))
    second_asset = second_run["asset_results"][0]

    assert any(hit["source_type"] == "confirmed_label" for hit in second_asset["knowledge_hits"])
    assert second_asset["knowledge_grounding_ratio"] >= 0.3


def test_generate_learning_candidates_uses_confirmed_labels(tmp_path):
    service = _make_service(tmp_path, min_candidate=2, min_model=2)

    for _ in range(2):
        _ingest_abnormal_snapshot(service)
        run_payload = asyncio.run(service.run_patrol(triggered_by="tester"))
        label_id = run_payload["labels_created"][0]["label_id"]
        asyncio.run(
            service.confirm_label(
                label_id,
                reviewer="engineer",
                anomaly_type="滤袋堵塞",
                root_cause="滤袋堵塞",
                review_notes="人工确认",
            )
        )

    candidates = service.generate_learning_candidates()
    candidate_types = {item["candidate_type"] for item in candidates}

    assert {"workflow", "prompt", "model"}.issubset(candidate_types)
    workflow_candidate = next(item for item in candidates if item["candidate_type"] == "workflow")
    assert workflow_candidate["status"] in {"draft", "ready_for_shadow"}
    model_candidate = next(item for item in candidates if item["candidate_type"] == "model")
    assert model_candidate["payload"]["confirmed_label_count"] >= 2


def test_repeated_pending_issue_reuses_existing_label(tmp_path):
    service = _make_service(tmp_path)
    _ingest_abnormal_snapshot(service)
    first_run = asyncio.run(service.run_patrol(triggered_by="tester"))
    first_label_id = first_run["labels_created"][0]["label_id"]

    _ingest_abnormal_snapshot(service)
    second_run = asyncio.run(service.run_patrol(triggered_by="tester"))

    assert second_run["labels_created"][0]["label_id"] == first_label_id
    assert len(service.list_review_queue()) == 1


def test_seed_demo_snapshot_can_trigger_patrol(tmp_path):
    service = _make_service(tmp_path)

    payload = asyncio.run(
        service.seed_demo_snapshot(
            profile="critical",
            run_patrol=True,
            triggered_by="tester",
        )
    )

    assert payload["snapshot"]["source"] == "demo:critical"
    assert payload["patrol_run"] is not None
    assert payload["patrol_run"]["asset_results"][0]["requires_review"] is True


def test_scheduler_run_once_executes_patrol(tmp_path):
    service = _make_service(tmp_path)
    _ingest_abnormal_snapshot(service)
    scheduler = IntelligentPatrolScheduler(service, interval_seconds=180, run_on_startup=False)

    result = asyncio.run(scheduler.run_once())

    assert result["run_id"].startswith("PATROL_")
    assert scheduler.last_run_id == result["run_id"]
