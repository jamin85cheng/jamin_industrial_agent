"""PLC driver abstractions."""

from __future__ import annotations

from typing import Iterable, Protocol

from src.plc.models import PlcDeviceConfig, PlcReadItem, PlcTagConfig, PlcWriteCommand, PlcWriteResult


class PlcDriver(Protocol):
    config: PlcDeviceConfig

    def connect(self) -> None: ...

    def close(self) -> None: ...

    def health(self) -> bool: ...

    def read_batch(self, tags: Iterable[PlcTagConfig]) -> list[PlcReadItem]: ...

    def write_batch(self, cmds: Iterable[PlcWriteCommand]) -> list[PlcWriteResult]: ...


class BasePlcDriver:
    """Minimal base class for concrete PLC drivers."""

    def __init__(self, config: PlcDeviceConfig):
        self.config = config
        self._connected = False

    def health(self) -> bool:
        return self._connected

    def connect(self) -> None:
        self._connected = True

    def close(self) -> None:
        self._connected = False

