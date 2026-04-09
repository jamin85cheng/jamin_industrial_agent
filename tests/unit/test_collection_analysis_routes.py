import asyncio
from datetime import datetime, timedelta, timezone

from src.api.dependencies import TenantContext, UserContext
from src.api.repositories.device_repository import DeviceRepository
from src.api.repositories.telemetry_repository import TelemetryRepository
from src.api.routers import analysis, collection


def _tenant() -> TenantContext:
    return TenantContext("default")


def _writer() -> UserContext:
    return UserContext(
        user_id="operator",
        username="operator",
        roles=["operator"],
        tenant_id="default",
        permissions=["device:write", "device:read", "data:read"],
    )


def _reader() -> UserContext:
    return UserContext(
        user_id="viewer",
        username="viewer",
        roles=["viewer"],
        tenant_id="default",
        permissions=["device:read", "data:read"],
    )


def _build_repositories(tmp_path):
    case_dir = tmp_path / "collection_analysis"
    case_dir.mkdir(parents=True, exist_ok=True)
    config = {
        "sqlite": {"path": str(case_dir / "metadata.db")},
        "postgres": {"enabled": False},
    }
    device_repo = DeviceRepository(config)
    telemetry_repo = TelemetryRepository(config)
    device_repo.init_schema()
    telemetry_repo.init_schema()
    device_repo.seed_demo_devices()
    return device_repo, telemetry_repo


def test_collection_and_analysis_use_persisted_telemetry(tmp_path, monkeypatch):
    device_repo, telemetry_repo = _build_repositories(tmp_path)
    monkeypatch.setattr(collection, "device_repository", device_repo)
    monkeypatch.setattr(collection, "telemetry_repository", telemetry_repo)
    monkeypatch.setattr(analysis, "telemetry_repository", telemetry_repo)

    tenant = _tenant()
    writer = _writer()
    reader = _reader()
    base_time = datetime.now(timezone.utc).replace(second=0, microsecond=0)

    start_result = asyncio.run(
        collection.start_collection(
            request=collection.CollectionStartRequest(device_ids=["DEV_BLOWER_01"], scan_interval=5),
            user=writer,
            tenant=tenant,
        )
    )
    assert start_result["data"]["is_running"] is True
    assert start_result["data"]["device_ids"] == ["DEV_BLOWER_01"]

    ingest_result = asyncio.run(
        collection.ingest_telemetry(
            request=collection.TelemetryIngestRequest(
                points=[
                    collection.TelemetryIngestPoint(
                        device_id="DEV_BLOWER_01",
                        tag="temperature",
                        value=30.0,
                        timestamp=base_time,
                        unit="C",
                    ),
                    collection.TelemetryIngestPoint(
                        device_id="DEV_BLOWER_01",
                        tag="temperature",
                        value=31.0,
                        timestamp=base_time + timedelta(minutes=5),
                        unit="C",
                    ),
                    collection.TelemetryIngestPoint(
                        device_id="DEV_BLOWER_01",
                        tag="temperature",
                        value=32.0,
                        timestamp=base_time + timedelta(minutes=10),
                        unit="C",
                    ),
                    collection.TelemetryIngestPoint(
                        device_id="DEV_BLOWER_01",
                        tag="temperature",
                        value=100.0,
                        timestamp=base_time + timedelta(minutes=15),
                        unit="C",
                    ),
                    collection.TelemetryIngestPoint(
                        device_id="DEV_BLOWER_01",
                        tag="temperature",
                        value=33.0,
                        timestamp=base_time + timedelta(minutes=20),
                        unit="C",
                    ),
                    collection.TelemetryIngestPoint(
                        device_id="DEV_BLOWER_01",
                        tag="vibration",
                        value=5.0,
                        timestamp=base_time,
                        unit="mm/s",
                    ),
                    collection.TelemetryIngestPoint(
                        device_id="DEV_BLOWER_01",
                        tag="vibration",
                        value=5.2,
                        timestamp=base_time + timedelta(minutes=5),
                        unit="mm/s",
                    ),
                    collection.TelemetryIngestPoint(
                        device_id="DEV_BLOWER_01",
                        tag="vibration",
                        value=5.4,
                        timestamp=base_time + timedelta(minutes=10),
                        unit="mm/s",
                    ),
                    collection.TelemetryIngestPoint(
                        device_id="DEV_BLOWER_01",
                        tag="vibration",
                        value=9.2,
                        timestamp=base_time + timedelta(minutes=15),
                        unit="mm/s",
                    ),
                    collection.TelemetryIngestPoint(
                        device_id="DEV_BLOWER_01",
                        tag="vibration",
                        value=5.6,
                        timestamp=base_time + timedelta(minutes=20),
                        unit="mm/s",
                    ),
                ]
            ),
            user=writer,
            tenant=tenant,
        )
    )
    assert ingest_result["data"]["count"] == 10

    status = asyncio.run(collection.get_collection_status(user=reader, tenant=tenant))
    assert status.is_running is True
    assert status.device_count == 1
    assert status.last_data_time is not None

    queried = asyncio.run(
        collection.query_data(
            request=collection.DataQueryRequest(
                tags=["temperature"],
                start_time=base_time - timedelta(minutes=1),
                end_time=base_time + timedelta(minutes=21),
            ),
            user=reader,
            tenant=tenant,
        )
    )
    assert len(queried.data["temperature"]) == 5

    latest = asyncio.run(
        collection.get_latest_data(tags=["temperature"], user=reader, tenant=tenant)
    )
    assert latest["temperature"]["value"] == 33.0

    realtime = asyncio.run(
        collection.get_realtime_data(tag="temperature", limit=3, user=reader, tenant=tenant)
    )
    assert realtime["count"] == 3
    assert realtime["data"][-1]["value"] == 33.0

    statistics = asyncio.run(
        analysis.get_statistics(
            tag="temperature",
            start_time=base_time - timedelta(minutes=1),
            end_time=base_time + timedelta(minutes=21),
            user=reader,
            tenant=tenant,
        )
    )
    assert statistics.count == 5
    assert statistics.max == 100.0

    trend = asyncio.run(
        analysis.analyze_trend(
            tag="temperature",
            start_time=base_time - timedelta(minutes=1),
            end_time=base_time + timedelta(minutes=21),
            user=reader,
            tenant=tenant,
        )
    )
    assert trend.data[-1].value == 33.0
    assert trend.change_percent > 0

    anomaly_response = asyncio.run(
        analysis.analyze_anomalies(
            request=analysis.AnomalyAnalysisRequest(
                tag="temperature",
                start_time=base_time - timedelta(minutes=1),
                end_time=base_time + timedelta(minutes=21),
                sensitivity=0.95,
            ),
            user=reader,
            tenant=tenant,
        )
    )
    assert anomaly_response.anomaly_count >= 1

    forecast_response = asyncio.run(
        analysis.forecast(
            request=analysis.ForecastRequest(tag="temperature", horizon=4),
            user=reader,
            tenant=tenant,
        )
    )
    assert len(forecast_response.forecast) == 4

    correlation = asyncio.run(
        analysis.analyze_correlation(
            tags=["temperature", "vibration"],
            start_time=base_time - timedelta(minutes=1),
            end_time=base_time + timedelta(minutes=21),
            user=reader,
            tenant=tenant,
        )
    )
    assert correlation["strongest_correlation"]["tags"] == ["temperature", "vibration"]
