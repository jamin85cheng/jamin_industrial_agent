import asyncio

from src.api.repositories.device_repository import DeviceRepository, utc_now
from src.intelligence.repository import IntelligenceRepository
from src.intelligence.service import IndustrialIntelligenceService
from src.plc.models import PlcWriteCommand
from src.plc.runtime import PlcCollectionService
from src.tasks.task_tracker import TaskTracker


def _make_device_repo(tmp_path):
    repo = DeviceRepository(
        {
            "sqlite": {"path": str(tmp_path / "metadata.db")},
            "postgres": {"enabled": False},
        }
    )
    repo.init_schema()
    return repo


def _make_intelligence_service(tmp_path):
    db_config = {
        "sqlite": {"path": str(tmp_path / "metadata.db")},
        "postgres": {"enabled": False},
    }
    tracker = TaskTracker(db_path=tmp_path / "tasks.sqlite")
    return IndustrialIntelligenceService(
        config={
            "database": db_config,
            "intelligence": {
                "llm": {"enabled": False},
                "patrol": {"interval_seconds": 180, "run_on_startup": False},
                "learning": {
                    "min_confirmed_labels_for_candidate": 2,
                    "min_confirmed_labels_for_model_candidate": 2,
                    "min_external_grounding_ratio": 0.3,
                },
            },
        },
        repository=IntelligenceRepository(db_config),
        tracker=tracker,
    )


def _create_simulated_device(repo: DeviceRepository, *, device_id: str, tags):
    now = utc_now()
    repo.create_device(
        device={
            "id": device_id,
            "name": device_id,
            "type": "simulated",
            "host": "127.0.0.1",
            "port": 0,
            "rack": 0,
            "slot": 1,
            "scan_interval": 3,
            "status": "offline",
            "enabled": True,
            "last_seen": None,
            "created_at": now,
            "updated_at": now,
            "tenant_id": "default",
            "created_by": "tester",
            "updated_by": "tester",
        },
        tags=tags,
    )


def test_runtime_collects_simulated_device_and_tracks_changes(tmp_path):
    repo = _make_device_repo(tmp_path)
    _create_simulated_device(
        repo,
        device_id="DEV_SIM_01",
        tags=[
            {
                "name": "pressure",
                "address": "SIM:1",
                "data_type": "float",
                "unit": "kPa",
                "deadband": 0.5,
                "description": "value=10.0;history_policy=on_change",
            }
        ],
    )
    service = PlcCollectionService(
        config={"database": {"sqlite": {"path": str(tmp_path / "metadata.db")}, "postgres": {"enabled": False}}},
        device_repository=repo,
    )

    first = service.collect_once(device_ids=["DEV_SIM_01"])
    runtime = service.ensure_runtime("DEV_SIM_01")
    assert first["devices"]["DEV_SIM_01"] == 1
    assert len(runtime.get_history_records()) == 1
    latest = service.get_latest_values(tags=["pressure"])
    assert latest["DEV_SIM_01.pressure"]["value"] == 10.0

    second = service.collect_once(device_ids=["DEV_SIM_01"])
    assert second["devices"]["DEV_SIM_01"] == 1
    assert len(runtime.get_history_records()) == 1

    runtime.driver.write_batch(
        [
            PlcWriteCommand(
                tag_key="pressure",
                address="SIM:1",
                data_type="float",
                value=11.3,
            )
        ]
    )
    third = service.collect_once(device_ids=["DEV_SIM_01"])
    assert third["devices"]["DEV_SIM_01"] == 1
    assert len(runtime.get_history_records()) == 2
    realtime = service.get_recent_points("DEV_SIM_01.pressure", limit=10)
    assert realtime[-1]["value"] == 11.3


def test_collection_service_bridges_to_intelligence_patrol(tmp_path):
    repo = _make_device_repo(tmp_path)
    intelligence_service = _make_intelligence_service(tmp_path)
    _create_simulated_device(
        repo,
        device_id="DEV_DUST_SIM_01",
        tags=[
            {
                "name": "pressure_diff",
                "address": "SIM:11",
                "data_type": "float",
                "unit": "kPa",
                "asset_id": "ASSET_DUST_COLLECTOR_01",
                "point_key": "pressure_diff_kpa",
                "description": "value=2.25",
            },
            {
                "name": "fan_current",
                "address": "SIM:12",
                "data_type": "float",
                "unit": "A",
                "asset_id": "ASSET_DUST_COLLECTOR_01",
                "point_key": "fan_current_a",
                "description": "value=17.2",
            },
            {
                "name": "airflow",
                "address": "SIM:13",
                "data_type": "float",
                "unit": "m3/h",
                "asset_id": "ASSET_DUST_COLLECTOR_01",
                "point_key": "airflow_m3h",
                "description": "value=7000",
            },
            {
                "name": "dust",
                "address": "SIM:14",
                "data_type": "float",
                "unit": "mg/m3",
                "asset_id": "ASSET_DUST_COLLECTOR_01",
                "point_key": "dust_concentration_mg_m3",
                "description": "value=28.5",
            },
            {
                "name": "cleaning",
                "address": "SIM:15",
                "data_type": "float",
                "unit": "Hz",
                "asset_id": "ASSET_DUST_COLLECTOR_01",
                "point_key": "cleaning_frequency_hz",
                "description": "value=1.85",
            },
            {
                "name": "valve",
                "address": "SIM:16",
                "data_type": "string",
                "asset_id": "ASSET_DUST_COLLECTOR_01",
                "point_key": "valve_state",
                "description": "value=closed",
            },
            {
                "name": "temperature",
                "address": "SIM:17",
                "data_type": "float",
                "unit": "C",
                "asset_id": "ASSET_DUST_COLLECTOR_01",
                "point_key": "temperature_c",
                "description": "value=95",
            },
            {
                "name": "running",
                "address": "SIM:18",
                "data_type": "bool",
                "asset_id": "ASSET_DUST_COLLECTOR_01",
                "point_key": "running_state",
                "description": "value=true",
            },
        ],
    )
    service = PlcCollectionService(
        config={"database": {"sqlite": {"path": str(tmp_path / "metadata.db")}, "postgres": {"enabled": False}}},
        device_repository=repo,
        intelligence_service=intelligence_service,
    )

    payload = service.collect_once(device_ids=["DEV_DUST_SIM_01"])
    assert payload["devices"]["DEV_DUST_SIM_01"] == 8

    snapshots = intelligence_service.get_latest_snapshots(["ASSET_DUST_COLLECTOR_01"])
    assert len(snapshots) == 1
    snapshot = snapshots[0]
    assert snapshot["points"]["dust_concentration_mg_m3"]["value"] == 28.5

    patrol = asyncio.run(
        intelligence_service.run_patrol(
            asset_ids=["ASSET_DUST_COLLECTOR_01"],
            triggered_by="tester",
        )
    )
    asset_result = patrol["asset_results"][0]
    assert asset_result["risk_level"] == "high_risk"
    assert asset_result["requires_review"] is True
    assert patrol["labels_created"][0]["label_id"].startswith("LABEL_")
