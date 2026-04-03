import shutil
import uuid
from pathlib import Path

from src.api.repositories.device_repository import DeviceRepository, utc_now


def _make_case_dir() -> Path:
    case_dir = Path("E:/jamin_industrial_agent/tests/.tmp") / f"device_repository_{uuid.uuid4().hex[:8]}"
    case_dir.mkdir(parents=True, exist_ok=True)
    return case_dir


def _make_repo(case_dir: Path) -> DeviceRepository:
    return DeviceRepository(
        {
            "sqlite": {"path": str(case_dir / "metadata.db")},
            "postgres": {"enabled": False},
        }
    )


def test_device_repository_seeds_demo_devices_once():
    case_dir = _make_case_dir()
    repo = _make_repo(case_dir)
    repo.init_schema()
    repo.seed_demo_devices()
    repo.seed_demo_devices()

    result = repo.list_devices(tenant_id="default")
    ids = {device["id"] for device in result["devices"]}

    assert result["total"] == 3
    assert {"DEV_AERATION_01", "DEV_AERATION_02", "DEV_BLOWER_01"} == ids
    shutil.rmtree(case_dir, ignore_errors=True)


def test_device_repository_supports_device_lifecycle():
    case_dir = _make_case_dir()
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
            {"name": "current", "address": "40001", "data_type": "float", "unit": "A"},
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

    deleted = repo.delete_device("DEV_TEST_01", tenant_id="default")
    assert deleted is True
    assert repo.get_device("DEV_TEST_01", tenant_id="default") is None
    shutil.rmtree(case_dir, ignore_errors=True)
