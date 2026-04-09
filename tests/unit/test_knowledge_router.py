import asyncio

from src.api.dependencies import UserContext
from src.api.routers import knowledge as knowledge_router
from src.intelligence.repository import IntelligenceRepository
from src.intelligence.service import IndustrialIntelligenceService
from src.tasks.task_tracker import TaskTracker


def _make_service(tmp_path):
    db_config = {
        "sqlite": {"path": str(tmp_path / "metadata.db")},
        "postgres": {"enabled": False},
    }
    config = {
        "database": db_config,
        "intelligence": {
            "llm": {"enabled": False},
            "patrol": {"interval_seconds": 180, "run_on_startup": False},
        },
    }
    repository = IntelligenceRepository(db_config)
    tracker = TaskTracker(db_path=tmp_path / "tasks.sqlite")
    return IndustrialIntelligenceService(config=config, repository=repository, tracker=tracker)


def _user():
    return UserContext(
        user_id="operator",
        username="operator",
        roles=["operator"],
        tenant_id="default",
        permissions=["data:read"],
    )


def test_knowledge_router_uses_persisted_cases_for_search_doc_and_feedback(tmp_path, monkeypatch):
    service = _make_service(tmp_path)
    seed_case = service.list_knowledge_cases(limit=1)[0]
    query = str(seed_case.get("root_cause") or (seed_case.get("tags") or ["dust"])[0])
    symptoms = f"{query} 导致设备持续异常，需要结合知识案例排查"
    monkeypatch.setattr(knowledge_router, "get_intelligence_service", lambda: service)

    search_result = asyncio.run(
        knowledge_router.search_knowledge(
            request=knowledge_router.SearchRequest(query=query, limit=5),
            user=_user(),
        )
    )

    assert search_result
    assert search_result[0].id == seed_case["case_id"]

    document = asyncio.run(
        knowledge_router.get_document(
            doc_id=seed_case["case_id"],
            user=_user(),
        )
    )

    updated_case = service.get_knowledge_case(seed_case["case_id"])
    assert document.id == seed_case["case_id"]
    assert updated_case is not None
    assert updated_case["usage_count"] >= 1

    diagnosis = asyncio.run(
        knowledge_router.diagnose(
            request=knowledge_router.DiagnoseRequest(
                symptoms=symptoms,
                tags={"scene_type": seed_case["scene_type"]},
            ),
            user=_user(),
        )
    )

    assert diagnosis.references
    assert diagnosis.references[0].id == seed_case["case_id"]
    assert diagnosis.root_cause

    feedback = asyncio.run(
        knowledge_router.submit_feedback(
            diagnosis_id=diagnosis.diagnosis_id,
            helpful=True,
            comment="Matches the confirmed case.",
            user=_user(),
        )
    )
    stats = asyncio.run(knowledge_router.get_knowledge_statistics(user=_user()))

    assert feedback["success"] is True
    assert stats["total_documents"] >= 1
    assert stats["search_count_today"] >= 1
    assert stats["diagnosis_count_today"] >= 1
    assert stats["feedback_count_today"] >= 1
    assert stats["accuracy_rate"] == 1.0


def test_knowledge_router_categories_are_derived_from_persisted_cases(tmp_path, monkeypatch):
    service = _make_service(tmp_path)
    monkeypatch.setattr(knowledge_router, "get_intelligence_service", lambda: service)

    categories = asyncio.run(knowledge_router.get_categories(user=_user()))

    assert categories
    assert set(categories) == {
        item["scene_type"]
        for item in service.list_knowledge_cases(limit=50)
        if item.get("scene_type")
    }
