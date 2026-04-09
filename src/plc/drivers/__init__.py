"""PLC driver exports and factory helpers."""

from __future__ import annotations

from src.plc.drivers.base import PlcDriver
from src.plc.drivers.modbus_tcp import ModbusTcpPlcDriver
from src.plc.drivers.s7 import S7PlcDriver
from src.plc.drivers.simulated import SimulatedPlcDriver
from src.plc.models import PlcDeviceConfig


def build_driver(config: PlcDeviceConfig) -> PlcDriver:
    protocol = str(config.protocol).strip().lower()
    if protocol == "s7":
        return S7PlcDriver(config)
    if protocol in {"modbus", "modbus_tcp"}:
        return ModbusTcpPlcDriver(config)
    if protocol == "simulated":
        return SimulatedPlcDriver(config)
    raise ValueError(f"Unsupported PLC protocol: {config.protocol}")


__all__ = [
    "PlcDriver",
    "S7PlcDriver",
    "ModbusTcpPlcDriver",
    "SimulatedPlcDriver",
    "build_driver",
]
