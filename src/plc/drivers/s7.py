"""Siemens S7 PLC driver with grouped batch reads."""

from __future__ import annotations

import re
import struct
from dataclasses import dataclass
from typing import Iterable, Optional

from src.plc.drivers.base import BasePlcDriver
from src.plc.models import PlcReadItem, PlcTagConfig, PlcWriteCommand, PlcWriteResult, now_ms

try:
    import snap7  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    snap7 = None


DB_PATTERN = re.compile(r"^DB(?P<db>\d+)\.DB(?P<kind>[XBWDI])(?P<offset>\d+)(?:\.(?P<bit>\d+))?$", re.IGNORECASE)
AREA_PATTERN = re.compile(r"^(?P<area>[MIQV])(?P<kind>[XBWDI]?)(?P<offset>\d+)(?:\.(?P<bit>\d+))?$", re.IGNORECASE)


@dataclass(frozen=True)
class S7Address:
    area: str
    db_number: Optional[int]
    byte_offset: int
    bit_index: Optional[int]
    width: int
    raw: str


def _area_code(area: str):
    if snap7 is None:  # pragma: no cover - guarded by connect
        raise RuntimeError("python-snap7 is not installed")
    areas = getattr(snap7, "types", None) or getattr(snap7, "type", None)
    if areas is None:
        raise RuntimeError("snap7 Areas enum is unavailable")
    mapping = {
        "M": areas.Areas.MK,
        "I": areas.Areas.PE,
        "Q": areas.Areas.PA,
        "V": areas.Areas.MK,
    }
    return mapping[area]


def _byte_width(data_type: str, token: str = "") -> int:
    normalized = str(data_type).upper()
    if normalized == "BOOL":
        return 1
    if normalized in {"INT", "UINT"} or token == "W":
        return 2
    if normalized in {"DINT", "UDINT", "FLOAT"} or token in {"D", "I"}:
        return 4
    if normalized == "DOUBLE":
        return 8
    return 4


def parse_s7_address(address: str, data_type: str, bit: Optional[int] = None) -> S7Address:
    text = str(address or "").strip().upper()
    db_match = DB_PATTERN.match(text)
    if db_match:
        token = db_match.group("kind")
        return S7Address(
            area="DB",
            db_number=int(db_match.group("db")),
            byte_offset=int(db_match.group("offset")),
            bit_index=bit if bit is not None else (int(db_match.group("bit")) if db_match.group("bit") is not None else None),
            width=_byte_width(data_type, token),
            raw=text,
        )

    area_match = AREA_PATTERN.match(text)
    if area_match:
        token = area_match.group("kind") or ""
        normalized_type = "BOOL" if token == "X" and str(data_type).upper() != "BOOL" else data_type
        return S7Address(
            area=area_match.group("area"),
            db_number=None,
            byte_offset=int(area_match.group("offset")),
            bit_index=bit if bit is not None else (int(area_match.group("bit")) if area_match.group("bit") is not None else None),
            width=_byte_width(normalized_type, token),
            raw=text,
        )
    raise ValueError(f"Unsupported S7 address format: {address}")


def decode_s7_value(buffer: bytes, spec: S7Address, data_type: str):
    normalized = str(data_type).upper()
    if normalized == "BOOL":
        bit_index = spec.bit_index or 0
        return bool((buffer[0] >> bit_index) & 1)
    if normalized == "INT":
        return struct.unpack(">h", buffer[:2])[0]
    if normalized == "UINT":
        return struct.unpack(">H", buffer[:2])[0]
    if normalized == "DINT":
        return struct.unpack(">i", buffer[:4])[0]
    if normalized == "UDINT":
        return struct.unpack(">I", buffer[:4])[0]
    if normalized == "DOUBLE":
        return struct.unpack(">d", buffer[:8])[0]
    return struct.unpack(">f", buffer[:4])[0]


class S7PlcDriver(BasePlcDriver):
    def __init__(self, config):
        super().__init__(config)
        self._client = None

    def connect(self) -> None:
        if self._connected:
            return
        if snap7 is None:
            raise RuntimeError("python-snap7 is not installed")
        client = snap7.client.Client()
        client.connect(self.config.host, self.config.rack, self.config.slot, self.config.port)
        checker = getattr(client, "get_connected", None) or getattr(client, "is_connected", None)
        connected = checker() if callable(checker) else False
        if not connected:
            raise RuntimeError(f"Failed to connect to S7 PLC at {self.config.host}:{self.config.port}")
        self._client = client
        self._connected = True

    def close(self) -> None:
        try:
            if self._client is not None:
                self._client.disconnect()
        finally:
            self._client = None
            super().close()

    def read_batch(self, tags: Iterable[PlcTagConfig]) -> list[PlcReadItem]:
        self.connect()
        timestamp = now_ms()
        parsed: list[tuple[PlcTagConfig, S7Address]] = []
        for tag in tags:
            parsed.append((tag, parse_s7_address(tag.address, tag.data_type, tag.bit)))

        grouped: dict[tuple[str, Optional[int]], list[tuple[PlcTagConfig, S7Address]]] = {}
        for tag, spec in parsed:
            grouped.setdefault((spec.area, spec.db_number), []).append((tag, spec))

        items: list[PlcReadItem] = []
        for (area, db_number), members in grouped.items():
            members.sort(key=lambda pair: pair[1].byte_offset)
            current_group: list[tuple[PlcTagConfig, S7Address]] = []
            current_end = -1
            for member in members:
                spec = member[1]
                spec_end = spec.byte_offset + max(spec.width, 1)
                if current_group and spec.byte_offset - current_end > 4:
                    items.extend(self._read_group(area, db_number, current_group, timestamp))
                    current_group = []
                current_group.append(member)
                current_end = max(current_end, spec_end)
            if current_group:
                items.extend(self._read_group(area, db_number, current_group, timestamp))
        return items

    def _read_group(
        self,
        area: str,
        db_number: Optional[int],
        group: list[tuple[PlcTagConfig, S7Address]],
        timestamp: int,
    ) -> list[PlcReadItem]:
        start = min(spec.byte_offset for _, spec in group)
        end = max(spec.byte_offset + max(spec.width, 1) for _, spec in group)
        size = max(end - start, 1)
        if area == "DB":
            raw = self._client.db_read(db_number, start, size)
        else:
            raw = self._client.read_area(_area_code(area), 0, start, size)

        results: list[PlcReadItem] = []
        for tag, spec in group:
            local_offset = spec.byte_offset - start
            chunk = raw[local_offset : local_offset + max(spec.width, 1)]
            results.append(
                PlcReadItem(
                    tag_key=tag.tag_key,
                    value=decode_s7_value(chunk, spec, tag.data_type),
                    ts_ms=timestamp,
                    quality="GOOD",
                )
            )
        return results

    def write_batch(self, cmds: Iterable[PlcWriteCommand]) -> list[PlcWriteResult]:
        timestamp = now_ms()
        return [
            PlcWriteResult(
                tag_key=command.tag_key,
                ok=False,
                ts_ms=timestamp,
                message="S7 write support is not enabled in this build",
            )
            for command in cmds
        ]
