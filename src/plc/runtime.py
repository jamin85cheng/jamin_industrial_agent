"""Device-level PLC collection runtime and service layer."""

from __future__ import annotations

import asyncio
import math
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional

from src.api.repositories.device_repository import DeviceRepository
from src.plc.drivers import build_driver
from src.plc.models import (
    QUALITY_GOOD,
    PlcDeviceConfig,
    PlcTagConfig,
    apply_scale_offset,
    normalize_payload_value,
)
from src.utils.config import load_config
from src.utils.structured_logging import get_logger

logger = get_logger("plc.runtime")

CollectionCallback = Callable[["DeviceCollectorRuntime", List[Dict[str, Any]], List[Dict[str, Any]]], None]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _timestamp_to_datetime(ts_ms: int) -> datetime:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)


def _is_numeric(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _parse_interval_to_seconds(interval: Optional[str]) -> int:
    if not interval:
        return 60
    text = str(interval).strip().lower()
    if text.endswith("m"):
        return max(int(text[:-1]), 1) * 60
    if text.endswith("h"):
        return max(int(text[:-1]), 1) * 3600
    return max(int(float(text)), 1)


def _bucket_timestamp(timestamp: datetime, bucket_seconds: int) -> datetime:
    normalized = timestamp.replace(second=0, microsecond=0)
    epoch = int(normalized.timestamp())
    bucket_epoch = epoch - (epoch % bucket_seconds)
    return datetime.fromtimestamp(bucket_epoch, tz=timezone.utc)


class DeviceCollectorRuntime:
    """Single-device polling runtime with connection reuse and change detection."""

    def __init__(
        self,
        device: PlcDeviceConfig,
        tags: Iterable[PlcTagConfig],
        *,
        history_limit: int = 500,
        on_cycle: Optional[CollectionCallback] = None,
    ):
        self.device = device
        self.tags = [tag for tag in tags if tag.enabled]
        self.tags_by_key = {tag.tag_key: tag for tag in self.tags}
        self.driver = build_driver(device)
        self.on_cycle = on_cycle

        self.latest: Dict[str, Dict[str, Any]] = {}
        self.history = deque(maxlen=max(history_limit, 100))
        self.last_changed_ms: Dict[str, int] = {}
        self.last_history_ms: Dict[str, int] = {}

        self.last_data_time: Optional[datetime] = None
        self.last_error: Optional[str] = None
        self.error_count = 0
        self.total_samples = 0
        self.total_cycles = 0
        self.started_at: Optional[datetime] = None
        self.status = "idle"

        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    @property
    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        if self.is_running:
            return
        self._stop_event = threading.Event()
        self.started_at = self.started_at or utc_now()
        self.status = "starting"
        self._thread = threading.Thread(
            target=self._loop,
            name=f"plc-collector-{self.device.device_key}",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=max(self.device.scan_interval * 2, 3))
        self._thread = None
        try:
            self.driver.close()
        finally:
            if self.status != "error":
                self.status = "stopped"

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            started = time.time()
            self.collect_once()
            elapsed = time.time() - started
            sleep_seconds = max(self.device.scan_interval - elapsed, 0.1)
            if self._stop_event.wait(timeout=sleep_seconds):
                break

    def collect_once(self) -> List[Dict[str, Any]]:
        with self._lock:
            self.started_at = self.started_at or utc_now()
            try:
                if not self.driver.health():
                    self.driver.connect()
                raw_items = self.driver.read_batch(self.tags)
                self.status = "online"
                self.last_error = None
            except Exception as exc:
                self.error_count += 1
                self.last_error = str(exc)
                self.status = "error"
                logger.warning(f"PLC collect failed for {self.device.device_key}: {exc}")
                try:
                    self.driver.close()
                except Exception:
                    pass
                return []

            normalized_items: List[Dict[str, Any]] = []
            changed_items: List[Dict[str, Any]] = []
            for raw in raw_items:
                tag = self.tags_by_key.get(raw.tag_key)
                if tag is None:
                    continue
                normalized = self._normalize_item(tag, raw.value, raw.ts_ms, raw.normalized_quality())
                previous = self.latest.get(tag.tag_key)
                changed = self._is_changed(tag, previous, normalized)
                history_due = self._should_store_history(tag, previous, normalized, changed)
                normalized["changed"] = changed

                self.latest[tag.tag_key] = normalized
                if changed:
                    self.last_changed_ms[tag.tag_key] = int(normalized["ts_ms"])
                    changed_items.append(dict(normalized))
                if history_due:
                    self.last_history_ms[tag.tag_key] = int(normalized["ts_ms"])
                    self.history.append(dict(normalized))
                normalized_items.append(dict(normalized))

            if normalized_items:
                self.last_data_time = utc_now()
                self.total_cycles += 1
                self.total_samples += len(normalized_items)
                if self.on_cycle:
                    self.on_cycle(self, normalized_items, changed_items)
            return normalized_items

    def _normalize_item(
        self,
        tag: PlcTagConfig,
        value: Any,
        ts_ms: int,
        quality: str,
    ) -> Dict[str, Any]:
        normalized_value = apply_scale_offset(normalize_payload_value(value, tag), tag)
        timestamp = _timestamp_to_datetime(ts_ms)
        return {
            "device_id": self.device.device_key,
            "device_name": self.device.name,
            "tag": tag.tag_key,
            "point_key": tag.semantic_point_key,
            "asset_id": tag.asset_id,
            "address": tag.address,
            "value": normalized_value,
            "raw_value": value,
            "quality": quality.lower(),
            "unit": tag.unit,
            "timestamp": timestamp.isoformat(),
            "ts_ms": ts_ms,
        }

    def _is_changed(
        self,
        tag: PlcTagConfig,
        previous: Optional[Dict[str, Any]],
        current: Dict[str, Any],
    ) -> bool:
        if previous is None:
            return True
        if previous.get("quality") != current.get("quality"):
            return True

        new_value = current.get("value")
        old_value = previous.get("value")
        changed = False
        if _is_numeric(old_value) and _is_numeric(new_value):
            changed = math.fabs(float(new_value) - float(old_value)) >= tag.numeric_deadband
        else:
            changed = old_value != new_value

        if not changed:
            return False

        if tag.debounce_ms > 0:
            last_changed_ms = self.last_changed_ms.get(tag.tag_key)
            if last_changed_ms and int(current["ts_ms"]) - last_changed_ms < tag.debounce_ms:
                return False
        return True

    def _should_store_history(
        self,
        tag: PlcTagConfig,
        previous: Optional[Dict[str, Any]],
        current: Dict[str, Any],
        changed: bool,
    ) -> bool:
        if previous is None:
            return True

        policy = str(tag.history_policy or "on_change").strip().lower()
        ts_ms = int(current["ts_ms"])
        last_history_ms = self.last_history_ms.get(tag.tag_key)
        interval_due = False
        if tag.history_interval_ms > 0:
            interval_due = last_history_ms is None or ts_ms - last_history_ms >= tag.history_interval_ms

        if policy == "always":
            return True
        if policy in {"interval", "sampled"}:
            return interval_due
        if policy == "none":
            return False
        return changed or interval_due

    def get_latest_records(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [dict(item) for item in self.latest.values()]

    def get_history_records(self, *, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        with self._lock:
            records = list(self.history)
        if limit:
            return [dict(item) for item in records[-limit:]]
        return [dict(item) for item in records]

    def summary(self) -> Dict[str, Any]:
        runtime_seconds = max((utc_now() - self.started_at).total_seconds(), 1.0) if self.started_at else 1.0
        return {
            "device_id": self.device.device_key,
            "device_name": self.device.name,
            "protocol": self.device.protocol,
            "running": self.is_running,
            "status": self.status,
            "scan_interval": self.device.scan_interval,
            "tag_count": len(self.tags),
            "last_data_time": self.last_data_time.isoformat() if self.last_data_time else None,
            "last_error": self.last_error,
            "throughput": round(self.total_samples / runtime_seconds, 3),
            "total_cycles": self.total_cycles,
            "total_samples": self.total_samples,
        }


class PlcCollectionService:
    """Loads device metadata and orchestrates per-device collector runtimes."""

    def __init__(
        self,
        *,
        config: Optional[Dict[str, Any]] = None,
        device_repository: Optional[DeviceRepository] = None,
        intelligence_service: Optional[Any] = None,
    ):
        self.config = config or load_config()
        self.device_repository = device_repository or DeviceRepository(self.config.get("database", {}))
        self.intelligence_service = intelligence_service

        plc_config = self.config.get("plc", {})
        runtime_config = plc_config.get("runtime", {})
        self.history_limit = int(runtime_config.get("history_limit", 500))
        self.auto_start = bool(runtime_config.get("auto_start", False))
        self.enable_intelligence_bridge = bool(runtime_config.get("intelligence_bridge_enabled", True))

        self._lock = threading.RLock()
        self._runtimes: Dict[str, DeviceCollectorRuntime] = {}
        self._tenant_by_runtime: Dict[str, str] = {}

    def list_runtime_devices(self, tenant_id: str = "default") -> List[Dict[str, Any]]:
        return self.device_repository.list_runtime_devices(tenant_id=tenant_id)

    def ensure_runtime(
        self,
        device_id: str,
        *,
        tenant_id: str = "default",
        scan_interval_override: Optional[int] = None,
    ) -> DeviceCollectorRuntime:
        with self._lock:
            existing = self._runtimes.get(device_id)
            if existing:
                return existing

            row = self.device_repository.get_runtime_device(device_id, tenant_id=tenant_id)
            if not row:
                raise KeyError(f"Unknown device: {device_id}")
            device = PlcDeviceConfig.from_repository(row)
            if scan_interval_override is not None and scan_interval_override > 0:
                device = PlcDeviceConfig(
                    device_key=device.device_key,
                    name=device.name,
                    protocol=device.protocol,
                    host=device.host,
                    port=device.port,
                    rack=device.rack,
                    slot=device.slot,
                    station=device.station,
                    timeout_ms=device.timeout_ms,
                    scan_interval=int(scan_interval_override),
                    enabled=device.enabled,
                    metadata=device.metadata,
                )

            tags = [
                PlcTagConfig.from_repository(device.device_key, tag)
                for tag in self.device_repository.list_tags(device.device_key, tenant_id=tenant_id)
            ]
            runtime = DeviceCollectorRuntime(
                device,
                tags,
                history_limit=self.history_limit,
                on_cycle=self._handle_cycle,
            )
            self._runtimes[device_id] = runtime
            self._tenant_by_runtime[device_id] = tenant_id
            return runtime

    def start(
        self,
        *,
        device_ids: Optional[List[str]] = None,
        tenant_id: str = "default",
        scan_interval_override: Optional[int] = None,
    ) -> Dict[str, Any]:
        target_ids = device_ids or [item["id"] for item in self.list_runtime_devices(tenant_id=tenant_id)]
        started: List[str] = []
        skipped: List[str] = []
        for device_id in target_ids:
            try:
                runtime = self.ensure_runtime(
                    device_id,
                    tenant_id=tenant_id,
                    scan_interval_override=scan_interval_override,
                )
                runtime.start()
                started.append(device_id)
            except Exception as exc:
                skipped.append(device_id)
                logger.warning(f"Failed to start PLC runtime for {device_id}: {exc}")
        return {
            "started": started,
            "skipped": skipped,
            "status": self.summary(),
        }

    def stop(self, *, device_ids: Optional[List[str]] = None) -> Dict[str, Any]:
        with self._lock:
            target_ids = device_ids or list(self._runtimes.keys())
            stopped: List[str] = []
            for device_id in target_ids:
                runtime = self._runtimes.get(device_id)
                if runtime is None:
                    continue
                runtime.stop()
                stopped.append(device_id)
            return {"stopped": stopped, "status": self.summary()}

    def shutdown(self) -> None:
        self.stop()

    def collect_once(
        self,
        *,
        device_ids: Optional[List[str]] = None,
        tenant_id: str = "default",
    ) -> Dict[str, Any]:
        target_ids = device_ids or [item["id"] for item in self.list_runtime_devices(tenant_id=tenant_id)]
        collected: Dict[str, int] = {}
        for device_id in target_ids:
            runtime = self.ensure_runtime(device_id, tenant_id=tenant_id)
            items = runtime.collect_once()
            collected[device_id] = len(items)
        return {"devices": collected, "status": self.summary()}

    def summary(self) -> Dict[str, Any]:
        with self._lock:
            runtimes = list(self._runtimes.values())
        last_times = [item.last_data_time for item in runtimes if item.last_data_time is not None]
        throughput = sum(item.summary()["throughput"] for item in runtimes)
        return {
            "is_running": any(item.is_running for item in runtimes),
            "device_count": len(runtimes),
            "last_data_time": max(last_times).isoformat() if last_times else None,
            "throughput": round(throughput, 3),
            "devices": [item.summary() for item in runtimes],
        }

    def get_latest_values(
        self,
        *,
        tags: Optional[List[str]] = None,
        device_ids: Optional[List[str]] = None,
    ) -> Dict[str, Dict[str, Any]]:
        queries = [item.strip() for item in (tags or []) if item and str(item).strip()]
        results: Dict[str, Dict[str, Any]] = {}
        for runtime in self._iter_runtimes(device_ids):
            for record in runtime.get_latest_records():
                key = f"{record['device_id']}.{record['tag']}"
                if queries and not any(self._matches_query(query, record) for query in queries):
                    continue
                results[key] = {
                    "timestamp": record["timestamp"],
                    "value": record["value"],
                    "quality": record["quality"],
                    "unit": record.get("unit"),
                    "device_id": record["device_id"],
                    "device_name": record["device_name"],
                    "tag": record["tag"],
                    "point_key": record["point_key"],
                    "asset_id": record.get("asset_id"),
                }
        return results

    def get_recent_points(
        self,
        query: str,
        *,
        limit: int = 100,
        device_ids: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        for runtime in self._iter_runtimes(device_ids):
            for record in runtime.get_history_records():
                if self._matches_query(query, record):
                    records.append(record)
        records.sort(key=lambda item: item["ts_ms"])
        return records[-limit:]

    def query_history(
        self,
        *,
        tags: List[str],
        start_time: datetime,
        end_time: datetime,
        aggregation: str = "raw",
        interval: Optional[str] = None,
        device_ids: Optional[List[str]] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        results: Dict[str, List[Dict[str, Any]]] = {}
        for query in tags:
            points: List[Dict[str, Any]] = []
            for runtime in self._iter_runtimes(device_ids):
                for record in runtime.get_history_records():
                    if not self._matches_query(query, record):
                        continue
                    timestamp = datetime.fromisoformat(record["timestamp"])
                    if timestamp < start_time or timestamp > end_time:
                        continue
                    if not _is_numeric(record["value"]):
                        continue
                    points.append(
                        {
                            "timestamp": timestamp,
                            "value": float(record["value"]),
                            "quality": record["quality"],
                        }
                    )
            points.sort(key=lambda item: item["timestamp"])
            if aggregation != "raw":
                points = self._aggregate_points(points, aggregation=aggregation, interval=interval)
            results[query] = points
        return results

    def _aggregate_points(
        self,
        points: List[Dict[str, Any]],
        *,
        aggregation: str,
        interval: Optional[str],
    ) -> List[Dict[str, Any]]:
        bucket_seconds = _parse_interval_to_seconds(interval)
        grouped: Dict[datetime, List[float]] = {}
        for point in points:
            bucket = _bucket_timestamp(point["timestamp"], bucket_seconds)
            grouped.setdefault(bucket, []).append(point["value"])

        aggregated: List[Dict[str, Any]] = []
        for bucket, values in sorted(grouped.items()):
            if aggregation == "sum":
                value = sum(values)
            elif aggregation == "min":
                value = min(values)
            elif aggregation == "max":
                value = max(values)
            else:
                value = sum(values) / len(values)
            aggregated.append(
                {
                    "timestamp": bucket,
                    "value": round(value, 6),
                    "quality": QUALITY_GOOD.lower(),
                }
            )
        return aggregated

    def _iter_runtimes(self, device_ids: Optional[List[str]] = None) -> Iterable[DeviceCollectorRuntime]:
        with self._lock:
            runtimes = list(self._runtimes.values()) if not device_ids else [
                runtime
                for device_id, runtime in self._runtimes.items()
                if device_id in set(device_ids)
            ]
        return runtimes

    def _matches_query(self, query: str, record: Dict[str, Any]) -> bool:
        normalized = str(query).strip()
        if not normalized:
            return False
        exact_key = f"{record['device_id']}.{record['tag']}"
        if normalized == exact_key:
            return True
        return normalized in {
            record["tag"],
            record.get("point_key"),
            record.get("asset_id"),
        }

    def _handle_cycle(
        self,
        runtime: DeviceCollectorRuntime,
        items: List[Dict[str, Any]],
        changed_items: List[Dict[str, Any]],
    ) -> None:
        if not self.enable_intelligence_bridge:
            return
        service = self._get_intelligence_service()
        if service is None:
            return

        asset_points: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for item in items:
            asset_id = item.get("asset_id")
            point_key = item.get("point_key")
            if not asset_id or not point_key:
                continue
            asset_points.setdefault(asset_id, {})[point_key] = {
                "value": item["value"],
                "unit": item.get("unit"),
                "quality": item.get("quality", QUALITY_GOOD.lower()),
                "timestamp": datetime.fromisoformat(item["timestamp"]),
            }

        for asset_id, points in asset_points.items():
            try:
                service.ingest_snapshot(
                    asset_id=asset_id,
                    source=f"plc:{runtime.device.device_key}",
                    points=points,
                )
            except Exception as exc:
                logger.warning(f"Failed to bridge PLC snapshot to intelligence for {asset_id}: {exc}")

    def _get_intelligence_service(self):
        if self.intelligence_service is not None:
            return self.intelligence_service
        try:
            from src.intelligence.runtime import get_intelligence_service

            self.intelligence_service = get_intelligence_service()
            return self.intelligence_service
        except Exception as exc:
            logger.warning(f"Industrial intelligence service unavailable: {exc}")
            return None


_collection_service: Optional[PlcCollectionService] = None


def get_collection_service() -> PlcCollectionService:
    global _collection_service
    if _collection_service is None:
        _collection_service = PlcCollectionService(config=load_config())
    return _collection_service


async def bootstrap_collection_runtime() -> Dict[str, Any]:
    service = get_collection_service()
    if service.auto_start:
        await asyncio.to_thread(service.start)
    return service.summary()


async def shutdown_collection_runtime() -> None:
    service = get_collection_service()
    await asyncio.to_thread(service.shutdown)
