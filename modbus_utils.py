# -*- coding: utf-8 -*-
"""Modbus helpers and channel definitions."""

import struct
from dataclasses import dataclass
from typing import Dict, List, Tuple

def crc16_modbus(data: bytes) -> int:
    """Return Modbus RTU CRC16 (poly 0xA001), little-endian on the wire."""
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 0x0001:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc & 0xFFFF


def hex_bytes(data: bytes) -> str:
    return " ".join(f"{b:02X}" for b in data)


DTYPE_INFO: Dict[str, Tuple[str, int]] = {
    "int16": ("h", 1),
    "uint16": ("H", 1),
    "int32": ("i", 2),
    "uint32": ("I", 2),
    "float32": ("f", 2),
    "float64": ("d", 4),
}


@dataclass
class ChannelConfig:
    enabled: bool
    name: str
    address: int
    dtype: str
    byte_order: str  # "big" or "little" within 16-bit word
    word_order: str  # "big" or "little" across words
    scale: float


@dataclass
class _ChSpec:
    ch: ChannelConfig
    start: int
    end: int
    reg_count: int


@dataclass
class _Block:
    start: int
    end: int
    channels: List[_ChSpec]

    @property
    def count(self) -> int:
        return self.end - self.start + 1


def regs_to_bytes(registers: List[int], byte_order: str, word_order: str) -> bytes:
    words = list(registers)
    if word_order == "little":
        words.reverse()
    out = bytearray()
    for w in words:
        hi = (w >> 8) & 0xFF
        lo = w & 0xFF
        if byte_order == "big":
            out.extend([hi, lo])
        else:
            out.extend([lo, hi])
    return bytes(out)


def decode_registers(registers: List[int], dtype: str, byte_order: str, word_order: str) -> float:
    if dtype not in DTYPE_INFO:
        raise ValueError(f"Unsupported dtype: {dtype}")
    fmt_char, reg_count = DTYPE_INFO[dtype]
    if len(registers) < reg_count:
        raise ValueError(f"Need {reg_count} regs for {dtype}, got {len(registers)}")
    raw = regs_to_bytes(registers[:reg_count], byte_order, word_order)
    # bytes already arranged: unpack as big-endian
    return float(struct.unpack(">" + fmt_char, raw)[0])


def build_blocks(channels: List[ChannelConfig], address_base_1: bool) -> List[_Block]:
    specs: List[_ChSpec] = []
    for ch in channels:
        if not ch.enabled:
            continue
        _, reg_count = DTYPE_INFO.get(ch.dtype, ("", 1))
        start = int(ch.address) - (1 if address_base_1 else 0)
        if start < 0:
            start = 0
        end = start + reg_count - 1
        specs.append(_ChSpec(ch=ch, start=start, end=end, reg_count=reg_count))

    specs.sort(key=lambda s: (s.start, s.end))
    if not specs:
        return []

    blocks: List[_Block] = []
    cur_start, cur_end = specs[0].start, specs[0].end
    cur_list = [specs[0]]
    for sp in specs[1:]:
        if sp.start <= cur_end + 1:
            cur_end = max(cur_end, sp.end)
            cur_list.append(sp)
        else:
            blocks.append(_Block(start=cur_start, end=cur_end, channels=cur_list))
            cur_start, cur_end, cur_list = sp.start, sp.end, [sp]
    blocks.append(_Block(start=cur_start, end=cur_end, channels=cur_list))
    return blocks


# ---------------- RS485 direction control ----------------

