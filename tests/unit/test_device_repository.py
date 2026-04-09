from pathlib import Path

from src.api.repositories.device_repository import DeviceRepository, utc_now


def _make_repo(case_dir: Path) -> DeviceRepository:
    return DeviceRepository(
        {
            "sqlite": {"path": str(case_dir / "metadata.db")},
            "postgres": {"enabled": False},
        }
    )


def test_device_repository_seeds_demo_devices_once(tmp_path):
    case_dir = tmp_path / "device_repository"
    case_dir.mkdir(parents=True, exist_ok=True)
    repo = _make_repo(case_dir)
    repo.init_schema()
    repo.seed_demo_devices()
    repo.seed_demo_devices()

    result = repo.list_devices(tenant_id="default")
    ids = {device["id"] for device in result["devices"]}

    assert result["total"] == 3
    assert {"DEV_AERATION_01", "DEV_AERATION_02", "DEV_BLOWER_01"} == ids


def test_device_repository_supports_device_lifecycle(tmp_path):
    case_dir = tmp_path / "device_repository"
    case_dir.mkdir(parents=True, exist_ok=True)
    repo = _make_repo(case_dir)
    repo.init_schema()

    created = repo.create_device(
        device={
            "id": "DEV_TEST_01",
            "name": "测试风机",
            "type": "modbus",
            "host": "192.168.10.10",
            "port": 502,
            "rack": 0,
            "slot": 1,
            "scan_interval": 15,
            "status": "offline",
            "enabled": True,
            "last_seen": None,
            "created_at": utc_now(),
            "updated_at": utc_now(),
            "tenant_id": "default",
            "created_by": "tester",
            "updated_by": "tester",
        },
        tags=[
            {
                "name": "current",
                "address": "40001",
                "data_type": "float",
                "unit": "A",
                "asset_id": "ASSET_BLOWER_01",
                "point_key": "fan_current_a",
                "deadband": 0.25,
                "debounce_ms": 1500,
            },
            {"name": "temperature", "address": "40002", "data_type": "float", "unit": "C"},
        ],
    )

    assert created["id"] == "DEV_TEST_01"
    assert created["tag_count"] == 2

    updated = repo.update_device(
        "DEV_TEST_01",
        tenant_id="default",
        updates={"name": "测试风机-更新", "host": "192.168.10.11", "enabled": False},
        updated_by="tester-2",
    )
    assert updated["name"] == "测试风机-更新"
    assert updated["host"] == "192.168.10.11"

    online = repo.set_connection_state("DEV_TEST_01", tenant_id="default", status="online", updated_by="tester-2")
    assert online["status"] == "online"
    assert online["last_seen"] is not None

    tags = repo.list_tags("DEV_TEST_01", tenant_id="default")
    assert len(tags) == 2
    current_tag = next(tag for tag in tags if tag["name"] == "current")
    assert current_tag["asset_id"] == "ASSET_BLOWER_01"
    assert current_tag["point_key"] == "fan_current_a"
    assert current_tag["deadband"] == 0.25
    assert current_tag["debounce_ms"] == 1500

    replaced_tags = repo.replace_tags(
        "DEV_TEST_01",
        tenant_id="default",
        tags=[
            {
                "name": "dust",
                "address": "40011",
                "data_type": "float",
                "unit": "mg/m3",
                "asset_id": "ASSET_DUST_COLLECTOR_01",
                "point_key": "dust_concentration_mg_m3",
                "deadband": 0.1,
                "debounce_ms": 500,
            }
        ],
    )
    assert len(replaced_tags) == 1
    assert replaced_tags[0]["name"] == "dust"
    assert replaced_tags[0]["point_key"] == "dust_concentration_mg_m3"

    deleted = repo.delete_device("DEV_TEST_01", tenant_id="default")
    assert deleted is True
    assert repo.get_device("DEV_TEST_01", tenant_id="default") is None
