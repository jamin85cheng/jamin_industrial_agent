"""Modbus TCP PLC driver with grouped register reads."""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Iterable

from src.plc.drivers.base import BasePlcDriver
from src.plc.models import PlcReadItem, PlcTagConfig, PlcWriteCommand, PlcWriteResult, now_ms

try:
    from pymodbus.client import ModbusTcpClient  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    ModbusTcpClient = None


@dataclass(frozen=True)
class ModbusAddress:
    section: str
    offset: int
    width: int
    raw: str


def _register_width(data_type: str) -> int:
    normalized = str(data_type).upper()
    if normalized == "BOOL":
        return 1
    if normalized in {"INT", "UINT"}:
        return 1
    if normalized in {"FLOAT", "DINT", "UDINT"}:
        return 2
    if normalized == "DOUBLE":
        return 4
    return 2


def parse_modbus_address(address: str, data_type: str) -> ModbusAddress:
    text = str(address or "").strip().lower()
    if ":" in text:
        section, raw_offset = text.split(":", 1)
        return ModbusAddress(section=section, offset=max(int(raw_offset), 0), width=_register_width(data_type), raw=text)

    numeric = int(float(text))
    if 1 <= numeric < 10000:
        return ModbusAddress(section="coil", offset=numeric - 1, width=1, raw=text)
    if 10001 <= numeric < 20000:
        return ModbusAddress(section="discrete", offset=numeric - 10001, width=1, raw=text)
    if 30001 <= numeric < 40000:
        return ModbusAddress(section="input", offset=numeric - 30001, width=_register_width(data_type), raw=text)
    return ModbusAddress(section="holding", offset=max(numeric - 40001, 0), width=_register_width(data_type), raw=text)


def decode_modbus_value(registers, bits, address: ModbusAddress, data_type: str, endian: str):
    normalized = str(data_type).upper()
    if address.section in {"coil", "discrete"} or normalized == "BOOL":
        return bool(bits[0]) if bits else False

    byteorder = ">" if endian != "little" else "<"
    buffer = b"".join(int(register).to_bytes(2, "big", signed=False) for register in registers)
    if normalized == "INT":
        return struct.unpack(f"{byteorder}h", buffer[:2])[0]
    if normalized == "UINT":
        return struct.unpack(f"{byteorder}H", buffer[:2])[0]
    if normalized == "DINT":
        return struct.unpack(f"{byteorder}i", buffer[:4])[0]
    if normalized == "UDINT":
        return struct.unpack(f"{byteorder}I", buffer[:4])[0]
    if normalized == "DOUBLE":
        return struct.unpack(f"{byteorder}d", buffer[:8])[0]
    return struct.unpack(f"{byteorder}f", buffer[:4])[0]


class ModbusTcpPlcDriver(BasePlcDriver):
    def __init__(self, config):
        super().__init__(config)
        self._client = None

    def connect(self) -> None:
        if self._connected:
            return
        if ModbusTcpClient is None:
            raise RuntimeError("pymodbus is not installed")
        client = ModbusTcpClient(host=self.config.host, port=self.config.port, timeout=self.config.timeout_ms / 1000.0)
        if not client.connect():
            raise RuntimeError(f"Failed to connect to Modbus TCP device at {self.config.host}:{self.config.port}")
        self._client = client
        self._connected = True

    def close(self) -> None:
        try:
            if self._client is not None:
                self._client.close()
        finally:
            self._client = None
            super().close()

    def read_batch(self, tags: Iterable[PlcTagConfig]) -> list[PlcReadItem]:
        self.connect()
        timestamp = now_ms()
        parsed = [(tag, parse_modbus_address(tag.address, tag.data_type)) for tag in tags]
        grouped: dict[str, list[tuple[PlcTagConfig, ModbusAddress]]] = {}
        for tag, address in parsed:
            grouped.setdefault(address.section, []).append((tag, address))

        items: list[PlcReadItem] = []
        for section, members in grouped.items():
            members.sort(key=lambda pair: pair[1].offset)
            current_group: list[tuple[PlcTagConfig, ModbusAddress]] = []
            current_end = -1
            for member in members:
                address = member[1]
                address_end = address.offset + max(address.width, 1)
                if current_group and address.offset - current_end > 2:
                    items.extend(self._read_group(section, current_group, timestamp))
                    current_group = []
                current_group.append(member)
                current_end = max(current_end, address_end)
            if current_group:
                items.extend(self._read_group(section, current_group, timestamp))
        return items

    def _read_group(
        self,
        section: str,
        group: list[tuple[PlcTagConfig, ModbusAddress]],
        timestamp: int,
    ) -> list[PlcReadItem]:
        start = min(address.offset for _, address in group)
        count = max(address.offset + max(address.width, 1) for _, address in group) - start
        slave = self.config.station

        if section == "coil":
            response = self._client.read_coils(start, count=count, slave=slave)
            bits = list(getattr(response, "bits", []) or [])
            registers: list[int] = []
        elif section == "discrete":
            response = self._client.read_discrete_inputs(start, count=count, slave=slave)
            bits = list(getattr(response, "bits", []) or [])
            registers = []
        elif section == "input":
            response = self._client.read_input_registers(start, count=count, slave=slave)
            registers = list(getattr(response, "registers", []) or [])
            bits = []
        else:
            response = self._client.read_holding_registers(start, count=count, slave=slave)
            registers = list(getattr(response, "registers", []) or [])
            bits = []

        if getattr(response, "isError", lambda: False)():
            return [
                PlcReadItem(tag_key=tag.tag_key, value=None, ts_ms=timestamp, quality="BAD")
                for tag, _ in group
            ]

        items: list[PlcReadItem] = []
        for tag, address in group:
            local_offset = address.offset - start
            slice_registers = registers[local_offset : local_offset + max(address.width, 1)]
            slice_bits = bits[local_offset : local_offset + max(address.width, 1)]
            items.append(
                PlcReadItem(
                    tag_key=tag.tag_key,
                    value=decode_modbus_value(slice_registers, slice_bits, address, tag.data_type, tag.endian),
                    ts_ms=timestamp,
                    quality="GOOD",
                )
            )
        return items

    def write_batch(self, cmds: Iterable[PlcWriteCommand]) -> list[PlcWriteResult]:
        timestamp = now_ms()
        results: list[PlcWriteResult] = []
        for command in cmds:
            address = parse_modbus_address(command.address, command.data_type)
            try:
                if address.section == "coil" or str(command.data_type).upper() == "BOOL":
                    response = self._client.write_coil(address.offset, bool(command.value), slave=self.config.station)
                else:
                    normalized = str(command.data_type).upper()
                    if normalized in {"INT", "UINT"}:
                        payload = [int(command.value)]
                    else:
                        payload = list(struct.unpack(">2H", struct.pack(">f", float(command.value))))
                    response = self._client.write_registers(address.offset, payload, slave=self.config.station)
                ok = not getattr(response, "isError", lambda: False)()
                results.append(
                    PlcWriteResult(
                        tag_key=command.tag_key,
                        ok=ok,
                        ts_ms=timestamp,
                        message=None if ok else "Modbus write failed",
                        readback_value=command.value if ok else None,
                    )
                )
            except Exception as exc:
                results.append(
                    PlcWriteResult(
                        tag_key=command.tag_key,
                        ok=False,
                        ts_ms=timestamp,
                        message=str(exc),
                    )
                )
        return results
