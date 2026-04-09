from datetime import timedelta

from src.api.repositories.telemetry_repository import TelemetryRepository


def _make_repo(tmp_path):
    case_dir = tmp_path / "telemetry_repository"
    case_dir.mkdir(parents=True, exist_ok=True)
    repo = TelemetryRepository(
        {
            "sqlite": {"path": str(case_dir / "metadata.db")},
            "postgres": {"enabled": False},
        }
    )
    repo.init_schema()
    return repo


def test_collection_state_round_trip(tmp_path):
    repo = _make_repo(tmp_path)
    state = repo.save_collection_state(
        tenant_id="default",
        is_running=True,
        device_ids=["DEV_001", "DEV_002"],
        scan_interval=15,
        started_by="operator",
    )

    assert state["is_running"] is True
    assert state["device_ids"] == ["DEV_001", "DEV_002"]
    assert state["scan_interval"] == 15
    assert state["started_by"] == "operator"


def test_ingest_query_latest_and_statistics(tmp_path):
    repo = _make_repo(tmp_path)
    now = repo.get_collection_state(tenant_id="default")["updated_at"] or None
    base_time = now or repo.telemetry_summary(tenant_id="default")["last_data_time"]
    if base_time is None:
        from datetime import datetime, timezone

        base_time = datetime.now(timezone.utc)

    ingest_result = repo.ingest_points(
        tenant_id="default",
        points=[
            {
                "device_id": "DEV_001",
                "tag": "temperature",
                "value": 30.0,
                "timestamp": base_time,
                "quality": "good",
                "unit": "C",
            },
            {
                "device_id": "DEV_001",
                "tag": "temperature",
                "value": 33.0,
                "timestamp": base_time + timedelta(minutes=5),
                "quality": "good",
                "unit": "C",
            },
            {
                "device_id": "DEV_001",
                "tag": "temperature",
                "value": 36.0,
                "timestamp": base_time + timedelta(minutes=10),
                "quality": "good",
                "unit": "C",
            },
        ],
    )

    assert ingest_result["count"] == 3

    queried = repo.query_points(
        tenant_id="default",
        tags=["temperature"],
        start_time=base_time - timedelta(minutes=1),
        end_time=base_time + timedelta(minutes=11),
    )
    assert len(queried["temperature"]) == 3

    latest = repo.get_latest_points(tenant_id="default", tags=["temperature"])
    assert latest["temperature"]["value"] == 36.0

    recent = repo.get_recent_points(tenant_id="default", tag="temperature", limit=2)
    assert [point["value"] for point in recent] == [33.0, 36.0]

    stats = repo.compute_series_statistics(queried["temperature"])
    assert stats["mean"] == 33.0
    assert stats["min"] == 30.0
    assert stats["max"] == 36.0

    summary = repo.telemetry_summary(tenant_id="default", recent_window_seconds=3600)
    assert summary["total_points"] == 3
    assert summary["throughput"] > 0
