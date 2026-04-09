"""Simulated PLC driver for local development and tests."""

from __future__ import annotations

import math
from typing import Iterable

from src.plc.drivers.base import BasePlcDriver
from src.plc.models import PlcReadItem, PlcTagConfig, PlcWriteCommand, PlcWriteResult, now_ms


class SimulatedPlcDriver(BasePlcDriver):
    """Deterministic driver that derives values from tag metadata."""

    def __init__(self, config):
        super().__init__(config)
        self._overrides: dict[str, object] = {}

    def read_batch(self, tags: Iterable[PlcTagConfig]) -> list[PlcReadItem]:
        self.connect()
        timestamp = now_ms()
        phase = timestamp / 1000.0
        items: list[PlcReadItem] = []

        for index, tag in enumerate(tags, start=1):
            if tag.tag_key in self._overrides:
                value = self._overrides[tag.tag_key]
            elif "value" in tag.metadata:
                value = tag.metadata["value"]
            elif tag.data_type == "BOOL":
                value = (int(phase) + index) % 2 == 0
            elif tag.data_type in {"INT", "UINT", "DINT", "UDINT"}:
                base = int(tag.metadata.get("base", 10 * index))
                value = base + int(abs(math.sin(phase / max(index, 1))) * 5)
            else:
                base = float(tag.metadata.get("base", 10.0 * index))
                amplitude = float(tag.metadata.get("amplitude", 1.5))
                value = round(base + math.sin(phase / max(index, 1)) * amplitude, 6)

            items.append(
                PlcReadItem(
                    tag_key=tag.tag_key,
                    value=value,
                    ts_ms=timestamp,
                    quality="GOOD",
                )
            )
        return items

    def write_batch(self, cmds: Iterable[PlcWriteCommand]) -> list[PlcWriteResult]:
        timestamp = now_ms()
        results: list[PlcWriteResult] = []
        for command in cmds:
            self._overrides[command.tag_key] = command.value
            results.append(
                PlcWriteResult(
                    tag_key=command.tag_key,
                    ok=True,
                    ts_ms=timestamp,
                    readback_value=command.value,
                )
            )
        return results
