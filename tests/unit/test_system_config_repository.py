from pathlib import Path

from src.api.repositories.system_config_repository import SystemConfigRepository


def _make_repo(case_dir: Path) -> SystemConfigRepository:
    return SystemConfigRepository(
        {
            "sqlite": {"path": str(case_dir / "metadata.db")},
            "postgres": {"enabled": False},
        }
    )


def test_system_config_repository_round_trips_payload(tmp_path):
    case_dir = tmp_path / "system_config_repository"
    case_dir.mkdir(parents=True, exist_ok=True)
    repo = _make_repo(case_dir)
    repo.init_schema()

    saved = repo.save_config(
        payload={
            "basic": {
                "system_name": "Enterprise Runtime",
                "scan_interval": 15,
                "alert_suppression": 20,
            },
            "plc": {
                "plc_type": "s7",
                "ip_address": "192.168.1.10",
                "port": 102,
            },
            "notifications": {
                "feishu_enabled": True,
                "feishu_webhook": "https://example.invalid/webhook",
                "email_enabled": False,
                "smtp_server": None,
            },
        },
        updated_by="admin",
    )

    assert saved["updated_by"] == "admin"

    loaded = repo.get_config()
    assert loaded is not None
    assert loaded["config"]["basic"]["system_name"] == "Enterprise Runtime"
    assert loaded["config"]["plc"]["ip_address"] == "192.168.1.10"
    assert loaded["config"]["notifications"]["feishu_enabled"] is True
