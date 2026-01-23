# -*- coding: utf-8 -*-
"""RS485 direction control helpers."""

from dataclasses import dataclass
import serial

class Rs485CtrlMode:
    AUTO = "自动(不控制)"
    RTS = "RTS 控制 DE"
    DTR = "DTR 控制 DE"


@dataclass
class Rs485CtrlConfig:
    mode: str = Rs485CtrlMode.RTS
    tx_level_high: bool = True   # True => set pin True during TX
    rx_level_high: bool = False  # True => set pin True during RX
    pre_tx_ms: int = 0
    post_tx_ms: int = 2


def apply_rs485_rx_level(ser: serial.Serial, cfg: Rs485CtrlConfig):
    if cfg.mode == Rs485CtrlMode.RTS:
        ser.rts = bool(cfg.rx_level_high)
    elif cfg.mode == Rs485CtrlMode.DTR:
        ser.dtr = bool(cfg.rx_level_high)


def apply_rs485_tx_level(ser: serial.Serial, cfg: Rs485CtrlConfig):
    if cfg.mode == Rs485CtrlMode.RTS:
        ser.rts = bool(cfg.tx_level_high)
    elif cfg.mode == Rs485CtrlMode.DTR:
        ser.dtr = bool(cfg.tx_level_high)


# ---------------- Worker thread ----------------

