"""Core PLC configuration and telemetry models."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Optional


DEFAULT_NUMERIC_DEADBAND = 0.05
QUALITY_GOOD = "GOOD"
QUALITY_BAD = "BAD"
QUALITY_TIMEOUT = "TIMEOUT"
QUALITY_DISCONNECTED = "DISCONNECTED"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def now_ms() -> int:
    return int(utc_now().timestamp() * 1000)


def normalize_data_type(value: Any) -> str:
    raw = str(value or "FLOAT").strip().upper()
    aliases = {
        "REAL": "FLOAT",
        "DOUBLE": "DOUBLE",
        "BOOL": "BOOL",
        "BOOLEAN": "BOOL",
        "INT": "INT",
        "INT16": "INT",
        "WORD": "UINT",
        "UINT16": "UINT",
        "DINT": "DINT",
        "INT32": "DINT",
        "DWORD": "UDINT",
        "UDINT": "UDINT",
        "UINT32": "UDINT",
        "FLOAT32": "FLOAT",
        "FLOAT64": "DOUBLE",
        "STRING": "STRING",
    }
    return aliases.get(raw, raw or "FLOAT")


def parse_metadata_text(value: Any) -> Dict[str, str]:
    if not isinstance(value, str) or not value.strip():
        return {}

    metadata: Dict[str, str] = {}
    for chunk in re.split(r"[;\n]+", value):
        segment = chunk.strip()
        if not segment or "=" not in segment:
            continue
        key, raw = segment.split("=", 1)
        metadata[key.strip().lower()] = raw.strip()
    return metadata


def _coerce_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _coerce_int(value: Any, default: int = 0) -> int:
    if value in (None, ""):
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _coerce_float(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _maybe_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_quality(value: Any) -> str:
    quality = str(value or QUALITY_GOOD).strip().upper()
    if quality in {QUALITY_GOOD, QUALITY_BAD, QUALITY_TIMEOUT, QUALITY_DISCONNECTED}:
        return quality
    return QUALITY_BAD


@dataclass(frozen=True)
class PlcDeviceConfig:
    device_key: str
    name: str
    protocol: str
    host: str
    port: int
    rack: int = 0
    slot: int = 1
    station: int = 1
    timeout_ms: int = 5000
    scan_interval: int = 10
    enabled: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_repository(cls, row: Dict[str, Any]) -> "PlcDeviceConfig":
        metadata = parse_metadata_text(row.get("description"))
        protocol = str(row.get("type") or row.get("protocol") or "s7").strip().lower()
        return cls(
            device_key=str(row["id"]),
            name=str(row.get("name") or row["id"]),
            protocol=protocol,
            host=str(row.get("host") or "127.0.0.1"),
            port=_coerce_int(row.get("port"), 102 if protocol == "s7" else 502),
            rack=_coerce_int(row.get("rack"), 0),
            slot=_coerce_int(row.get("slot"), 1),
            station=_coerce_int(row.get("station") or metadata.get("station") or metadata.get("unit_id"), 1),
            timeout_ms=_coerce_int(row.get("timeout_ms") or metadata.get("timeout_ms"), 5000),
            scan_interval=max(_coerce_int(row.get("scan_interval"), 10), 1),
            enabled=bool(row.get("enabled", True)),
            metadata=metadata,
        )


@dataclass(frozen=True)
class PlcTagConfig:
    tag_key: str
    device_key: str
    name: str
    address: str
    data_type: str
    bit: Optional[int] = None
    endian: str = "big"
    scale: float = 1.0
    offset: float = 0.0
    unit: Optional[str] = None
    writable: bool = False
    write_min: Optional[float] = None
    write_max: Optional[float] = None
    ack_readback: bool = False
    group_key: Optional[str] = None
    deadband: Optional[float] = None
    debounce_ms: int = 0
    history_policy: str = "on_change"
    history_interval_ms: int = 0
    enabled: bool = True
    asset_id: Optional[str] = None
    point_key: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def semantic_point_key(self) -> str:
        return self.point_key or self.tag_key

    @property
    def numeric_deadband(self) -> float:
        if self.deadband is not None:
            return self.deadband
        if self.data_type in {"FLOAT", "DOUBLE"}:
            return DEFAULT_NUMERIC_DEADBAND
        return 0.0

    @classmethod
    def from_repository(cls, device_key: str, row: Dict[str, Any]) -> "PlcTagConfig":
        metadata = parse_metadata_text(row.get("description"))
        data_type = normalize_data_type(row.get("data_type"))
        point_key = (
            row.get("point_key")
            or metadata.get("point")
            or metadata.get("point_key")
            or metadata.get("semantic")
            or str(row.get("name") or row.get("tag_key") or row.get("address"))
        )
        return cls(
            tag_key=str(row.get("name") or row.get("tag_key") or row.get("address")),
            device_key=device_key,
            name=str(row.get("name") or row.get("address")),
            address=str(row.get("address") or ""),
            data_type=data_type,
            bit=(
                _coerce_int(row.get("bit"), 0)
                if row.get("bit") not in (None, "")
                else (_coerce_int(metadata["bit"], 0) if "bit" in metadata else None)
            ),
            endian=str(row.get("endian") or metadata.get("endian") or "big").strip().lower(),
            scale=_coerce_float(row.get("scale") or metadata.get("scale"), 1.0),
            offset=_coerce_float(row.get("offset") or metadata.get("offset"), 0.0),
            unit=row.get("unit"),
            writable=_coerce_bool(row.get("writable") or metadata.get("writable"), False),
            write_min=_maybe_float(row.get("write_min") or metadata.get("write_min") or metadata.get("min")),
            write_max=_maybe_float(row.get("write_max") or metadata.get("write_max") or metadata.get("max")),
            ack_readback=_coerce_bool(row.get("ack_readback") or metadata.get("ack_readback"), False),
            group_key=row.get("group_key") or metadata.get("group") or metadata.get("group_key"),
            deadband=_maybe_float(row.get("deadband") or metadata.get("deadband")),
            debounce_ms=_coerce_int(row.get("debounce_ms") or metadata.get("debounce_ms"), 0),
            history_policy=str(row.get("history_policy") or metadata.get("history_policy") or "on_change").strip().lower(),
            history_interval_ms=_coerce_int(
                row.get("history_interval_ms") or metadata.get("history_interval_ms"),
                0,
            ),
            enabled=bool(row.get("enabled", True)),
            asset_id=row.get("asset_id") or metadata.get("asset") or metadata.get("asset_id"),
            point_key=point_key,
            metadata=metadata,
        )


@dataclass(frozen=True)
class PlcReadItem:
    tag_key: str
    value: Any
    ts_ms: int
    quality: str = QUALITY_GOOD

    def normalized_quality(self) -> str:
        return _normalize_quality(self.quality)


@dataclass(frozen=True)
class PlcWriteCommand:
    tag_key: str
    address: str
    data_type: str
    value: Any
    bit: Optional[int] = None


@dataclass(frozen=True)
class PlcWriteResult:
    tag_key: str
    ok: bool
    ts_ms: int
    message: Optional[str] = None
    readback_value: Any = None


def apply_scale_offset(value: Any, tag: PlcTagConfig) -> Any:
    if value is None:
        return None
    if tag.data_type == "BOOL":
        return _coerce_bool(value)
    if isinstance(value, (int, float)):
        scaled = float(value) * tag.scale + tag.offset
        if tag.data_type in {"INT", "UINT", "DINT", "UDINT"}:
            return int(round(scaled))
        return round(scaled, 6)
    return value


def normalize_payload_value(value: Any, tag: PlcTagConfig) -> Any:
    if value is None:
        return None
    if tag.data_type == "BOOL":
        return _coerce_bool(value)
    if tag.data_type in {"INT", "UINT", "DINT", "UDINT"}:
        return _coerce_int(value)
    if tag.data_type in {"FLOAT", "DOUBLE"}:
        return _coerce_float(value)
    return value
