# -*- coding: utf-8 -*-
"""In-app virtual serial ports for simulation."""

import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

class _VirtualEndpoint:
    """A minimal in-memory byte stream endpoint with blocking read."""

    def __init__(self):
        self._buf = bytearray()
        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)

    def feed(self, data: bytes):
        if not data:
            return
        with self._cv:
            self._buf.extend(data)
            self._cv.notify_all()

    def read(self, n: int, timeout: Optional[float]) -> bytes:
        if n <= 0:
            return b""
        t_end = None if timeout is None else (time.time() + float(timeout))
        with self._cv:
            while not self._buf:
                if timeout == 0:
                    return b""
                if t_end is None:
                    self._cv.wait()
                else:
                    remaining = t_end - time.time()
                    if remaining <= 0:
                        return b""
                    self._cv.wait(timeout=remaining)
            out = self._buf[:n]
            del self._buf[:len(out)]
            return bytes(out)

    @property
    def in_waiting(self) -> int:
        with self._lock:
            return len(self._buf)

    def clear(self):
        with self._lock:
            self._buf.clear()


class VirtualSerial:
    """A tiny subset of pyserial.Serial API for in-app simulation ports."""

    def __init__(self, port: str, rx_ep: _VirtualEndpoint, peer_rx_ep: _VirtualEndpoint):
        self.port = port
        self._rx = rx_ep
        self._peer_rx = peer_rx_ep
        self.timeout: Optional[float] = 0.1
        self.write_timeout: Optional[float] = 0.1
        self.rts = False
        self.dtr = False
        self.is_open = True

    def close(self):
        self.is_open = False


    def open(self):
        """Re-open the virtual port after it was closed."""
        self.is_open = True

    def flush(self):
        # in-memory stream: nothing to flush
        return

    def reset_input_buffer(self):
        self._rx.clear()

    @property
    def in_waiting(self) -> int:
        return self._rx.in_waiting

    def write(self, data: bytes) -> int:
        if not self.is_open:
            raise IOError("VirtualSerial is closed")
        if data is None:
            data = b""
        self._peer_rx.feed(bytes(data))
        return len(data)

    def read(self, n: int = 1) -> bytes:
        if not self.is_open:
            return b""
        return self._rx.read(int(n), self.timeout)


@dataclass
class VirtualPortInfo:
    key: str               # e.g. 'SIM:COM10'
    com: str               # e.g. 'COM10'
    baudrate: int
    host: VirtualSerial    # used by main app when selecting the simulated port
    sim: VirtualSerial     # used by simulator UI


class VirtualSerialRegistry:
    def __init__(self):
        self._ports: Dict[str, VirtualPortInfo] = {}
        self._lock = threading.Lock()

    def list_infos(self) -> List[VirtualPortInfo]:
        with self._lock:
            return list(self._ports.values())

    def create(self, com_num: int, baudrate: int = 9600) -> VirtualPortInfo:
        com = f"COM{int(com_num)}"
        key = f"SIM:{com}"
        with self._lock:
            if key in self._ports:
                raise ValueError(f"仿真串口已存在：{com}")
            ep_host = _VirtualEndpoint()
            ep_sim = _VirtualEndpoint()
            host = VirtualSerial(com, rx_ep=ep_host, peer_rx_ep=ep_sim)
            sim = VirtualSerial(com, rx_ep=ep_sim, peer_rx_ep=ep_host)
            info = VirtualPortInfo(key=key, com=com, baudrate=int(baudrate), host=host, sim=sim)
            self._ports[key] = info
            return info

    def remove(self, key: str):
        with self._lock:
            info = self._ports.pop(key, None)
        if info:
            try:
                info.host.close()
            except Exception:
                pass
            try:
                info.sim.close()
            except Exception:
                pass

    def get_host(self, key: str) -> Optional[VirtualSerial]:
        with self._lock:
            info = self._ports.get(key)
            return info.host if info else None

    def get_sim(self, key: str) -> Optional[VirtualSerial]:
        with self._lock:
            info = self._ports.get(key)
            return info.sim if info else None

    def set_baudrate(self, key: str, baudrate: int):
        """Update baudrate metadata for a virtual port (thread-safe).

        Note: Virtual ports are in-memory streams; baudrate is used for UI display and
        for user-facing configuration/estimation only.
        """
        with self._lock:
            info = self._ports.get(key)
            if info:
                info.baudrate = int(baudrate)


SIM_REGISTRY = VirtualSerialRegistry()


# ---------------- Modbus utilities ----------------

