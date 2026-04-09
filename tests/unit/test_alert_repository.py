from pathlib import Path

from src.api.repositories.alert_repository import AlertRepository


def _make_repo(case_dir: Path) -> AlertRepository:
    return AlertRepository(
        {
            "sqlite": {"path": str(case_dir / "metadata.db")},
            "postgres": {"enabled": False},
        }
    )


def test_alert_repository_seeds_defaults(tmp_path):
    case_dir = tmp_path / "alert_repository"
    case_dir.mkdir(parents=True, exist_ok=True)
    repo = _make_repo(case_dir)
    repo.init_schema()
    repo.seed_default_rules()
    repo.seed_demo_alerts()

    rules = repo.list_rules(tenant_id="default")
    alerts = repo.list_alerts(tenant_id="default")

    assert len(rules) == 2
    assert alerts["total"] == 2


def test_alert_repository_supports_rule_and_alert_lifecycle(tmp_path):
    case_dir = tmp_path / "alert_repository"
    case_dir.mkdir(parents=True, exist_ok=True)
    repo = _make_repo(case_dir)
    repo.init_schema()

    created_rule = repo.create_rule(
        {
            "rule_id": "RULE_TEST_01",
            "name": "振动告警",
            "enabled": True,
            "condition": {"type": "threshold", "tag": "vibration", "operator": ">", "value": 8},
            "severity": "warning",
            "message": "振动偏高",
            "suppression_window_minutes": 10,
            "tenant_id": "default",
        }
    )
    assert created_rule["rule_id"] == "RULE_TEST_01"

    updated_rule = repo.update_rule(
        "RULE_TEST_01",
        tenant_id="default",
        updates={"message": "振动持续偏高", "enabled": False},
    )
    assert updated_rule["message"] == "振动持续偏高"
    assert updated_rule["enabled"] is False

    alert_id = repo.create_alert(
        rule_id="RULE_TEST_01",
        message="振动达到 9.2 mm/s",
        severity="warning",
        device_id="DEV_BLOWER_01",
        tag="vibration",
        value=9.2,
        threshold=8.0,
        tenant_id="default",
    )
    alert = repo.get_alert(alert_id, tenant_id="default")
    assert alert["status"] == "active"

    acknowledged = repo.acknowledge_alert(alert_id, tenant_id="default", user_id="tester", comment="已安排巡检")
    assert acknowledged["status"] == "acknowledged"
    assert acknowledged["acknowledged_by"] == "tester"

    resolved = repo.resolve_alert(
        alert_id,
        tenant_id="default",
        user_id="tester",
        resolution_notes="宸插畬鎴愬缃紝鐜板満鏁板€煎凡鎭㈠",
    )
    assert resolved["status"] == "resolved"
    assert resolved["resolved_by"] == "tester"
    assert resolved["resolution_notes"] == "宸插畬鎴愬缃紝鐜板満鏁板€煎凡鎭㈠"

    deleted = repo.delete_rule("RULE_TEST_01", tenant_id="default")
    assert deleted is True


def test_alert_repository_tracks_diagnosis_and_report_links(tmp_path):
    case_dir = tmp_path / "alert_repository_links"
    case_dir.mkdir(parents=True, exist_ok=True)
    repo = _make_repo(case_dir)
    repo.init_schema()

    alert_id = repo.create_alert(
        rule_id=None,
        message="婧惰В姘ф祿搴︿綆",
        severity="critical",
        device_id="DEV_AERATION_01",
        tag="DO",
        value=1.6,
        threshold=2.0,
        tenant_id="default",
    )

    linked = repo.link_diagnosis_task(
        alert_id=alert_id,
        task_id="TASK_DIAG_001",
        tenant_id="default",
        user_id="operator",
        entrypoint="alert",
    )
    assert linked["diagnosis_task_id"] == "TASK_DIAG_001"
    assert linked["last_action_by"] == "operator"

    updated = repo.attach_report_to_alert(
        alert_id=alert_id,
        report_id="RPT_001",
        tenant_id="default",
        user_id="operator",
        task_id="TASK_DIAG_001",
    )
    assert updated["latest_report_id"] == "RPT_001"
    assert updated["latest_report_download_url"] == "/reports/RPT_001/download"
