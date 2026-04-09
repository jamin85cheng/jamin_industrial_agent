"""Backward-compatible PLC collector built on the new PLC driver layer."""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from src.plc.drivers import build_driver
from src.plc.models import PlcDeviceConfig, PlcTagConfig, apply_scale_offset, normalize_payload_value
from src.utils.structured_logging import get_logger
from src.utils.thread_safe import SafeValue

logger = get_logger("data.collector")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class PLCCollector:
    """Legacy-compatible collector facade."""

    def __init__(self, config: Dict[str, Any]):
        self.config = dict(config or {})
        self.plc_type = str(self.config.get("type", "s7")).strip().lower()
        self.scan_interval = int(self.config.get("scan_interval", 10) or 10)
        self.callbacks: List[Callable[[Dict[str, Any]], None]] = []

        self._running = SafeValue(False)
        self._thread: Optional[threading.Thread] = None
        self._reconnect_attempts = SafeValue(0)
        self._max_reconnect_attempts = int(self.config.get("max_reconnect_attempts", 5) or 5)
        self._reconnect_delay = int(self.config.get("reconnect_delay", 5) or 5)

        self.device = PlcDeviceConfig(
            device_key=str(self.config.get("device_key") or self.config.get("id") or f"PLC_{self.plc_type.upper()}"),
            name=str(self.config.get("name") or self.config.get("device_key") or f"PLC {self.plc_type.upper()}"),
            protocol=self.plc_type,
            host=str(self.config.get("host", "127.0.0.1")),
            port=int(self.config.get("port", 102 if self.plc_type == "s7" else 502)),
            rack=int(self.config.get("rack", 0) or 0),
            slot=int(self.config.get("slot", 1) or 1),
            station=int(self.config.get("station", 1) or 1),
            timeout_ms=int(self.config.get("timeout_ms", 5000) or 5000),
            scan_interval=self.scan_interval,
            enabled=True,
            metadata={},
        )
        self.tags: List[PlcTagConfig] = self._normalize_tags(self.config.get("tags", []))
        self.driver = build_driver(self.device)
        logger.info(f"PLCCollector initialized for type={self.plc_type}")

    @property
    def client(self):
        return getattr(self.driver, "_client", self.driver)

    @property
    def is_connected(self) -> bool:
        return self.driver.health()

    @is_connected.setter
    def is_connected(self, value: bool):
        if not value:
            self.disconnect()

    def _normalize_tags(self, tags: Any) -> List[PlcTagConfig]:
        normalized: List[PlcTagConfig] = []
        raw_items: List[Dict[str, Any]] = []

        if isinstance(tags, dict):
            for tag_key, payload in tags.items():
                if isinstance(payload, dict):
                    raw_items.append({"tag_id": tag_key, **payload})
                else:
                    raw_items.append({"tag_id": tag_key, "address": payload})
        elif isinstance(tags, list):
            for item in tags:
                if isinstance(item, dict):
                    raw_items.append(item)
                else:
                    raw_items.append({"tag_id": str(item), "address": str(item)})

        for item in raw_items:
            tag_key = str(item.get("tag_id") or item.get("name") or item.get("address"))
            description_parts = []
            for meta_key in (
                "value",
                "base",
                "amplitude",
                "asset_id",
                "point_key",
                "deadband",
                "debounce_ms",
                "group_key",
            ):
                if item.get(meta_key) not in (None, ""):
                    description_parts.append(f"{meta_key}={item.get(meta_key)}")
            normalized.append(
                PlcTagConfig.from_repository(
                    self.device.device_key,
                    {
                        "name": tag_key,
                        "address": item.get("plc_address") or item.get("address") or tag_key,
                        "data_type": item.get("data_type", "FLOAT"),
                        "unit": item.get("unit"),
                        "description": ";".join(description_parts),
                    },
                )
            )
        return normalized

    def set_tags(self, tags: Any):
        self.tags = self._normalize_tags(tags)

    def register_callback(self, callback: Callable[[Dict[str, Any]], None]):
        if callback not in self.callbacks:
            self.callbacks.append(callback)

    def unregister_callback(self, callback: Callable[[Dict[str, Any]], None]):
        if callback in self.callbacks:
            self.callbacks.remove(callback)

    def connect(self) -> bool:
        try:
            self.driver.connect()
            return True
        except Exception as exc:
            logger.warning(f"Failed to connect PLCCollector: {exc}")
            return False

    def disconnect(self):
        self._running.set(False)
        try:
            self.driver.close()
        except Exception as exc:
            logger.warning(f"Failed to disconnect PLCCollector: {exc}")

    def read_tag(self, tag_config: Dict[str, Any]) -> Optional[Any]:
        tag = self._normalize_tags([tag_config])
        if not tag:
            return None
        try:
            item = self.driver.read_batch(tag)[0]
            return apply_scale_offset(normalize_payload_value(item.value, tag[0]), tag[0])
        except Exception as exc:
            logger.warning(f"Failed to read tag {tag[0].address}: {exc}")
            return None

    def read_all_tags(self) -> Dict[str, Any]:
        try:
            items = self.driver.read_batch(self.tags)
        except Exception as exc:
            logger.warning(f"Failed to read all tags: {exc}")
            items = []

        values: Dict[str, Any] = {}
        for item in items:
            tag = next((candidate for candidate in self.tags if candidate.tag_key == item.tag_key), None)
            normalized_value = item.value if tag is None else apply_scale_offset(normalize_payload_value(item.value, tag), tag)
            values[item.tag_key] = {
                "value": normalized_value,
                "timestamp": datetime.fromtimestamp(item.ts_ms / 1000.0, tz=timezone.utc).isoformat(),
                "quality": item.quality.lower(),
            }
        return {
            "timestamp": utc_now().isoformat(),
            "values": values,
        }

    def start_continuous_collection(self, callback: Callable[[Dict[str, Any]], None]):
        self.register_callback(callback)
        self.start_collection()

    def start_collection(
        self,
        addresses: Optional[Any] = None,
        callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ):
        if addresses is not None:
            self.set_tags(addresses)
        if callback is not None:
            self.register_callback(callback)
        if self._running.get():
            logger.warning("Collection is already running")
            return

        self._running.set(True)
        self._reconnect_attempts.set(0)
        self._thread = threading.Thread(target=self._collection_loop, daemon=True)
        self._thread.start()
        logger.info(f"Collection loop started with interval={self.scan_interval}s")

    def _collection_loop(self):
        while self._running.get():
            try:
                if not self.is_connected and not self._try_reconnect():
                    time.sleep(self._reconnect_delay)
                    continue

                data = self.read_all_tags()
                self._reconnect_attempts.set(0)

                for callback in list(self.callbacks):
                    try:
                        callback(data)
                    except Exception as exc:
                        logger.error(f"Collection callback failed: {exc}")

                time.sleep(self.scan_interval)
            except Exception as exc:
                logger.error(f"Collection loop error: {exc}")
                self.disconnect()
                time.sleep(self._reconnect_delay)

    def _try_reconnect(self) -> bool:
        attempts = self._reconnect_attempts.get()
        if attempts >= self._max_reconnect_attempts:
            logger.error(
                f"Reconnect attempts exceeded maximum ({self._max_reconnect_attempts})"
            )
            self._reconnect_attempts.set(0)
            return False

        delay = min(self._reconnect_delay * (2**attempts), 60)
        self._reconnect_attempts.set(attempts + 1)
        logger.info(
            f"Reconnect attempt {attempts + 1}/{self._max_reconnect_attempts} in {delay}s"
        )
        time.sleep(delay)
        return self.connect()

    def stop_continuous_collection(self):
        self.stop_collection()

    def stop_collection(self):
        self._running.set(False)
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=10)
        self.disconnect()
        logger.info("Collection loop stopped")

    def reconnect(self):
        self.disconnect()
        time.sleep(1)
        return self.connect()
