import asyncio
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

from src.data.storage import InfluxDBStorage, SQLiteStorage, StorageManager, TimeSeriesStorage


class TestTimeSeriesStorage:
    def test_async_write_contract(self):
        storage = MagicMock(spec=TimeSeriesStorage)
        storage.write = AsyncMock(return_value=True)

        result = asyncio.run(
            storage.write(
                measurement="test_metric",
                tags={"device": "D001"},
                fields={"value": 3.14},
                timestamp=datetime.now(),
            )
        )

        assert result is True

    def test_async_write_batch_contract(self):
        storage = MagicMock(spec=TimeSeriesStorage)
        storage.write_batch = AsyncMock(return_value=3)

        result = asyncio.run(
            storage.write_batch(
                [
                    {"measurement": "m1", "fields": {"v": 1}},
                    {"measurement": "m2", "fields": {"v": 2}},
                    {"measurement": "m3", "fields": {"v": 3}},
                ]
            )
        )

        assert result == 3

    def test_async_query_contract(self):
        storage = MagicMock(spec=TimeSeriesStorage)
        storage.query = AsyncMock(
            return_value=[
                {"time": datetime(2024, 1, 1, 0, 0), "fields": {"value": 1}},
                {"time": datetime(2024, 1, 1, 1, 0), "fields": {"value": 2}},
            ]
        )

        result = asyncio.run(
            storage.query(
                measurement="test_metric",
                start_time=datetime(2024, 1, 1),
                end_time=datetime(2024, 1, 2),
            )
        )

        assert len(result) == 2
        assert result[0]["fields"]["value"] == 1


class TestStorageManager:
    def test_initialize_sqlite_backend(self, tmp_path):
        manager = StorageManager(
            {
                "type": "sqlite",
                "config": {"db_path": str(tmp_path / "timeseries.db")},
            }
        )

        initialized = asyncio.run(manager.initialize())

        assert initialized is True
        assert isinstance(manager.storage, SQLiteStorage)
        asyncio.run(manager.shutdown())

    def test_initialize_rejects_unknown_backend(self):
        manager = StorageManager({"type": "unknown", "config": {}})

        initialized = asyncio.run(manager.initialize())

        assert initialized is False
        assert manager.storage is None

    def test_write_and_query_via_manager(self, tmp_path):
        manager = StorageManager(
            {
                "type": "sqlite",
                "config": {"db_path": str(tmp_path / "timeseries.db")},
            }
        )
        asyncio.run(manager.initialize())

        now = datetime.now()
        write_result = asyncio.run(
            manager.write(
                measurement="test_metric",
                tags={"device": "D001"},
                fields={"value": 3.14},
                timestamp=now,
            )
        )
        query_result = asyncio.run(
            manager.query(
                measurement="test_metric",
                start_time=now - timedelta(minutes=1),
                end_time=now + timedelta(minutes=1),
            )
        )

        assert write_result is True
        assert len(query_result) == 1
        assert query_result[0]["fields"]["value"] == 3.14
        asyncio.run(manager.shutdown())


class TestInfluxDBStorage:
    def test_connection_uses_host_port_configuration(self):
        fake_client = MagicMock()
        fake_client.health.return_value = MagicMock(status="pass", message="")
        fake_client.write_api.return_value = MagicMock()
        fake_client.query_api.return_value = MagicMock()

        with patch("influxdb_client.InfluxDBClient", return_value=fake_client) as client_cls:
            storage = InfluxDBStorage(
                host="localhost",
                port=8086,
                username="test-user",
                password="test-password",
                org="test-org",
                bucket="test-bucket",
            )

            connected = asyncio.run(storage.connect())

        assert connected is True
        client_cls.assert_called_once()
        assert client_cls.call_args.kwargs["url"] == "http://localhost:8086"
        assert storage.client is fake_client

    def test_write_point_returns_true_when_write_api_succeeds(self):
        storage = InfluxDBStorage(host="localhost", port=8086)
        storage.write_api = MagicMock()

        result = asyncio.run(
            storage.write(
                measurement="DO",
                tags={"device": "blower-01"},
                fields={"value": 3.5},
                timestamp=datetime.now(),
            )
        )

        assert result is True
        storage.write_api.write.assert_called_once()


class TestSQLiteStorage:
    def test_insert_and_query(self, tmp_path):
        db_path = tmp_path / "test.db"
        storage = SQLiteStorage(str(db_path))

        async def scenario():
            connected = await storage.connect()
            assert connected is True

            now = datetime.now()
            write_ok = await storage.write(
                measurement="test_metric",
                tags={"device": "D001"},
                fields={"value": 3.14},
                timestamp=now,
            )
            result = await storage.query(
                measurement="test_metric",
                start_time=now - timedelta(hours=1),
                end_time=now + timedelta(hours=1),
            )
            latest = await storage.get_latest("test_metric")
            await storage.disconnect()

            return write_ok, result, latest

        write_ok, result, latest = asyncio.run(scenario())

        assert write_ok is True
        assert len(result) == 1
        assert result[0]["fields"]["value"] == 3.14
        assert latest is not None
        assert latest["fields"]["value"] == 3.14
