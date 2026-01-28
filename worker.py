# -*- coding: utf-8 -*-
"""Worker thread for Modbus RTU polling and serial output."""

import queue
import threading
import time
from typing import Dict, List, Optional, Tuple

import serial

from qt_compat import QThread, Signal, Slot
from modbus_utils import ChannelConfig, _Block, build_blocks, decode_registers, crc16_modbus, hex_bytes
from rs485 import Rs485CtrlConfig, Rs485CtrlMode, apply_rs485_rx_level, apply_rs485_tx_level
from virtual_serial import SIM_REGISTRY

class ModbusRtuWorker(QThread):
    data_ready = Signal(float, dict)      # ts, row dict
    status = Signal(str)
    connected = Signal(bool)
    acquiring = Signal(bool)
    log_line = Signal(str)                # to comm monitor
    # Structured frames for monitor/custom windows.
    # kind: 'TX'/'RX'/'TX_MANUAL'/'RX_MANUAL'
    # data: raw bytes (may be b'' for timeout/error)
    # tag: e.g. 'rx:COM3'/'tx:COM4'
    # note: extra message for empty frames
    frame = Signal(str, bytes, str, str)

    def __init__(
        self,
        port: str,
        baudrate: int,
        parity: str,
        stopbits: int,
        bytesize: int,
        timeout_s: float,
        rs485_cfg: Rs485CtrlConfig,
        tx_enabled: bool,
        tx_port: str,
        tx_baudrate: int,
        tx_interval_ms: int,
        parent=None
    ):
        super().__init__(parent)
        self.port = port
        self.baudrate = baudrate
        self.parity = parity
        self.stopbits = stopbits
        self.bytesize = bytesize
        self.timeout_s = timeout_s
        self.rs485_cfg = rs485_cfg

        self.tx_enabled = bool(tx_enabled)
        self.tx_port = tx_port
        self.tx_baudrate = int(tx_baudrate)
        self.tx_interval_ms = int(tx_interval_ms)

        self._running = True
        self._acquiring = False

        # Manual/custom send queue (processed in worker thread)
        self._custom_send_q: "queue.Queue[Tuple[str, str, bool]]" = queue.Queue()

        self._lock = threading.Lock()
        self.unit_id = 1
        self.func_code = 3
        self.poll_ms = 20
        self.address_base_1 = False
        self.channels: List[ChannelConfig] = []
        self.blocks: List[_Block] = []

        self._ser: Optional[serial.Serial] = None
        self._tx_ser: Optional[serial.Serial] = None

        # TX port async RX tap enabled (for comm monitor). When disabled, we won't
        # read from the TX serial in the background, avoiding consuming bytes.
        self.tx_tap_enabled = True

        # Output sending is decoupled from Modbus polling: we keep only the latest
        # formatted payload and send it at a fixed interval in a separate writer thread.
        self._latest_out_payload: bytes = b""
        self._latest_lock = threading.Lock()
        self._tx_write_q: "queue.Queue[bytes]" = queue.Queue(maxsize=1)
        self._tx_writer_thread: Optional[threading.Thread] = None
        # Serialize TX/output port access and allow pausing data sending
        # while manual commands are in flight.
        self._tx_ser_lock = threading.Lock()
        self._tx_pause_lock = threading.Lock()
        self._tx_pause_count = 0
        self._tx_pause_event = threading.Event()

    # ---------- custom send (thread-safe enqueue from UI thread) ----------
    def enqueue_custom_send(self, target: str, text: str, add_crlf: bool):
        """Enqueue a manual send task.

        target: "rx" (Modbus serial) or "tx" (output serial)
        text: user input string; supports text or hex bytes (e.g. "01 03 00 00 00 02")
        add_crlf: whether to append \r\n (also applies to hex mode)
        """
        try:
            self._custom_send_q.put_nowait((str(target), str(text), bool(add_crlf)))
        except Exception:
            pass

    @staticmethod
    def _try_parse_hex_bytes(s: str) -> Optional[bytes]:
        """Parse common hex input formats.

        Accepts:
          - "01 03 00 00 00 02"
          - "0x01 0x03 00 00"
          - "010300000002"
        Returns bytes on success, else None.
        """
        t = (s or "").strip()
        if not t:
            return None
        # strip common separators
        t2 = t.replace(",", " ").replace("\t", " ")
        parts = [p for p in t2.split(" ") if p]

        # 1) spaced tokens
        if len(parts) >= 2:
            out = bytearray()
            for p in parts:
                pp = p.lower()
                if pp.startswith("0x"):
                    pp = pp[2:]
                if len(pp) != 2:
                    return None
                try:
                    out.append(int(pp, 16) & 0xFF)
                except Exception:
                    return None
            return bytes(out)

        # 2) single token, maybe continuous hex
        one = parts[0].lower() if parts else ""
        if one.startswith("0x"):
            one = one[2:]
        if len(one) >= 2 and all(ch in "0123456789abcdef" for ch in one) and (len(one) % 2 == 0):
            try:
                return bytes(int(one[i:i+2], 16) & 0xFF for i in range(0, len(one), 2))
            except Exception:
                return None
        return None

    @staticmethod
    def _read_until_idle(
        ser: serial.Serial,
        idle_ms: int = 20,
        max_ms: int = 200,
        max_bytes: int = 4096,
    ) -> bytes:
        """Read bytes until the line stays idle for a short period.

        This is used for "manual send" reply listening. It avoids needing to know
        the expected frame length.
        """
        if ser is None:
            return b""

        t0 = time.time()
        last_rx = t0
        buf = bytearray()
        orig_timeout = getattr(ser, "timeout", None)

        try:
            # non-blocking reads
            try:
                ser.timeout = 0
            except Exception:
                pass

            while True:
                try:
                    chunk = ser.read(1024)
                except Exception:
                    chunk = b""

                if chunk:
                    buf.extend(chunk)
                    last_rx = time.time()
                    if len(buf) >= max_bytes:
                        break
                else:
                    now = time.time()
                    if (now - last_rx) * 1000.0 >= float(idle_ms):
                        break
                    if (now - t0) * 1000.0 >= float(max_ms):
                        break
                    time.sleep(0.002)
        finally:
            try:
                if orig_timeout is not None:
                    ser.timeout = orig_timeout
            except Exception:
                pass

        return bytes(buf)

    def _process_custom_sends(self, max_items: int = 50):
        """Process pending manual sends in the worker thread."""
        processed = 0
        while processed < max_items:
            try:
                target, text, add_crlf = self._custom_send_q.get_nowait()
            except queue.Empty:
                break

            target = (target or "").strip().lower()
            add_crlf = bool(add_crlf)
            text = text or ""

            # choose serial
            ser = None
            tag = "?"
            if target == "rx":
                ser = self._ser
                tag = f"rx:{self.port}"
            elif target == "tx":
                ser = self._tx_ser
                tag = f"tx:{self.tx_port}"

            if ser is None:
                self._emit_frame("TX_MANUAL", b"", tag=tag, note="<target not connected>")
                processed += 1
                continue

            payload = self._try_parse_hex_bytes(text)
            mode = "HEX" if payload is not None else "TEXT"
            if payload is None:
                payload = text.encode("utf-8", errors="replace")

            if add_crlf:
                payload += b"\r\n"

            # optional RS485 direction control (important for many RS485 modules)
            # If enabled, apply RS485 direction control for manual sends.
            # Many RS485 modules need RTS/DTR toggling to actually drive the bus (TX灯才会亮)。
            use_rs485_dir = (self.rs485_cfg.mode != Rs485CtrlMode.AUTO)

            pause_tx = (target == "tx")
            if pause_tx:
                self._pause_tx_output()

            try:
                lock = self._tx_ser_lock if (target == "tx") else None
                if lock is not None:
                    lock.acquire()
                try:
                    if use_rs485_dir:
                        try:
                            apply_rs485_tx_level(ser, self.rs485_cfg)
                            if self.rs485_cfg.pre_tx_ms > 0:
                                time.sleep(self.rs485_cfg.pre_tx_ms / 1000.0)
                        except Exception:
                            pass

                    ser.write(payload)
                    ser.flush()
                    self._emit_frame("TX_MANUAL", payload, tag=tag, note=f"[{mode}]")

                    if use_rs485_dir:
                        try:
                            if self.rs485_cfg.post_tx_ms > 0:
                                time.sleep(self.rs485_cfg.post_tx_ms / 1000.0)
                            apply_rs485_rx_level(ser, self.rs485_cfg)
                        except Exception:
                            pass

                    # listen for reply (best-effort)
                    reply = self._read_until_idle(ser, idle_ms=20, max_ms=200)
                    if reply:
                        self._emit_frame("RX_MANUAL", reply, tag=tag)
                    else:
                        self._emit_frame("RX_MANUAL", b"", tag=tag, note="<no reply>")

                finally:
                    if lock is not None:
                        lock.release()

            except Exception as e:
                self._emit_frame("TX_MANUAL", b"", tag=tag, note=f"<error> {e}")
            finally:
                if pause_tx:
                    self._resume_tx_output()

            processed += 1

    def stop_thread(self):
        self._running = False

    def set_acquiring(self, on: bool):
        self._acquiring = bool(on)
        self.acquiring.emit(self._acquiring)

    def update_runtime(self, unit_id: int, func_code: int, poll_ms: int, address_base_1: bool, channels: List[ChannelConfig], tx_interval_ms: Optional[int] = None):
        with self._lock:
            self.unit_id = int(unit_id)
            self.func_code = int(func_code)
            self.poll_ms = int(poll_ms)
            if tx_interval_ms is not None:
                self.tx_interval_ms = int(tx_interval_ms)
            self.address_base_1 = bool(address_base_1)
            self.channels = list(channels)
            self.blocks = build_blocks(self.channels, self.address_base_1)

    def _log(self, s: str):
        self.log_line.emit(s)

    def _emit_frame(self, kind: str, data: bytes, tag: str = "", note: str = ""):
        """Emit raw frame data for UI display.

        The UI can render as HEX or decode as text (UTF-8/GBK).
        """
        try:
            self.frame.emit(str(kind), bytes(data or b""), str(tag or ""), str(note or ""))
        except Exception:
            pass

    def _pause_tx_output(self):
        # Pause periodic TX/output sending (reference-counted).
        with self._tx_pause_lock:
            self._tx_pause_count += 1
            self._tx_pause_event.set()

    def _resume_tx_output(self):
        # Resume periodic TX/output sending when pause count reaches zero.
        with self._tx_pause_lock:
            if self._tx_pause_count > 0:
                self._tx_pause_count -= 1
            if self._tx_pause_count <= 0:
                self._tx_pause_count = 0
                self._tx_pause_event.clear()

    def _tx_output_paused(self) -> bool:
        return self._tx_pause_event.is_set()

    def set_tx_tap_enabled(self, enabled: bool):
        """Enable/disable async RX tap for TX/output serial.

        仅影响后台窥探式读取（用于通讯监视窗口），不影响业务发送。
        """
        self.tx_tap_enabled = bool(enabled)


    def _pump_async_rx(self, max_bytes: int = 512):
        """Best-effort async RX monitor for the *TX/output* serial.

        用途：让“通讯监视窗口”能同时看到发送串口(tx)收到的 RX 数据。
        不改变业务逻辑，只做窥探式读取（read existing bytes）。
        """
        ser = self._tx_ser
        if ser is None:
            return

        try:
            n = int(getattr(ser, "in_waiting", 0) or 0)
        except Exception:
            n = 0

        if max_bytes is not None and max_bytes>0 and n>max_bytes:
            n = int(max_bytes)
        if n <= 0:
            return

        try:
            n = int(n)
            if max_bytes is not None and int(max_bytes) > 0:
                n = min(n, int(max_bytes))
        except Exception:
            pass

        try:
            # read currently available bytes (non-blocking)
            chunk = ser.read(n)
        except Exception:
            chunk = b""

        if chunk:
            self._emit_frame("RX_TX", chunk, tag=f"tx:{self.tx_port}")

    def _send_recv_readregs(self, start_addr: int, count: int) -> Optional[List[int]]:
        """
        Send Modbus RTU request (03/04), return list of 16-bit registers or None on timeout/error.
        Request: [id][fc][addr_hi][addr_lo][qty_hi][qty_lo][crc_lo][crc_hi]
        Response: [id][fc][byte_count][data...][crc_lo][crc_hi]
        """
        ser = self._ser
        if ser is None:
            return None

        unit = self.unit_id & 0xFF
        fc = self.func_code & 0xFF
        req_wo_crc = bytes([
            unit, fc,
            (start_addr >> 8) & 0xFF, start_addr & 0xFF,
            (count >> 8) & 0xFF, count & 0xFF
        ])
        crc = crc16_modbus(req_wo_crc)
        req = req_wo_crc + bytes([crc & 0xFF, (crc >> 8) & 0xFF])

        # flush stale input to avoid mixing old bytes
        try:
            ser.reset_input_buffer()
        except Exception:
            pass

        # direction control: TX
        if self.rs485_cfg.mode != Rs485CtrlMode.AUTO:
            apply_rs485_tx_level(ser, self.rs485_cfg)
            if self.rs485_cfg.pre_tx_ms > 0:
                time.sleep(self.rs485_cfg.pre_tx_ms / 1000.0)

        # write
        try:
            ser.write(req)
            ser.flush()
            self._emit_frame("TX", req, tag=f"rx:{self.port}")
        except Exception as e:
            self._log(f"TX_ERR: {e}")
            return None

        # direction control: back to RX
        if self.rs485_cfg.mode != Rs485CtrlMode.AUTO:
            if self.rs485_cfg.post_tx_ms > 0:
                time.sleep(self.rs485_cfg.post_tx_ms / 1000.0)
            apply_rs485_rx_level(ser, self.rs485_cfg)

        # read response
        # read first 3 bytes
        hdr = ser.read(3)
        if len(hdr) < 3:
            self._emit_frame("RX", b"", tag=f"rx:{self.port}", note="<timeout/no header>")
            return None

        resp_unit, resp_fc, third = hdr[0], hdr[1], hdr[2]
        if resp_unit != unit:
            # might be noise; read remaining quickly but report
            tail = ser.read(256)
            self._emit_frame("RX", hdr + tail, tag=f"rx:{self.port}", note="<unit mismatch>")
            return None

        # exception response
        if resp_fc == (fc | 0x80):
            exc = ser.read(2)  # exception code + crc?
            more = ser.read(2)
            full = hdr + exc + more
            self._log(f"RX_EXC: {hex_bytes(full)}")
            return None

        if resp_fc != fc:
            tail = ser.read(256)
            self._emit_frame("RX", hdr + tail, tag=f"rx:{self.port}", note="<fc mismatch>")
            return None

        byte_count = third
        expected_data = byte_count + 2  # data + crc
        rest = ser.read(expected_data)
        if len(rest) < expected_data:
            self._emit_frame("RX", hdr + rest, tag=f"rx:{self.port}", note="<timeout/incomplete>")
            return None

        resp = hdr + rest
        self._emit_frame("RX", resp, tag=f"rx:{self.port}")

        # CRC check
        body = resp[:-2]
        crc_rx = resp[-2] | (resp[-1] << 8)
        crc_calc = crc16_modbus(body)
        if crc_rx != crc_calc:
            self._log(f"CRC_ERR: rx={crc_rx:04X} calc={crc_calc:04X}")
            return None

        if byte_count != count * 2:
            # still parse what we got, but warn
            self._log(f"LEN_WARN: byte_count={byte_count}, expected={count*2}")

        data = resp[3:3 + byte_count]
        regs = []
        for i in range(0, len(data), 2):
            if i + 1 >= len(data):
                break
            regs.append((data[i] << 8) | data[i + 1])
        return regs


    def _open_any_serial(self, port_key: str, baudrate: int, parity: str = 'N', stopbits: int = 1, bytesize: int = 8, timeout: float = 0.2):
        """Open a physical COM port or a simulated port.

        Simulated ports are addressed with key like 'SIM:COM10'.
        """
        if (port_key or '').startswith('SIM:'):
            ser = SIM_REGISTRY.get_host(port_key)
            if ser is None:
                raise ValueError(f"仿真串口不存在或未启动：{port_key}")
            try:
                ser.timeout = float(timeout)
            except Exception:
                pass
            # VirtualSerial may be closed by a previous disconnect; reopen it.
            try:
                if hasattr(ser, 'open'):
                    ser.open()
                else:
                    ser.is_open = True
            except Exception:
                pass
            return ser

        return serial.Serial(
            port=port_key,
            baudrate=int(baudrate),
            parity=parity,
            stopbits=int(stopbits),
            bytesize=int(bytesize),
            timeout=float(timeout),
            write_timeout=0.05,
        )

    def _tx_writer_loop(self):
        """Write TX/output serial in a background thread (so it never blocks Modbus polling)."""
        while self._running:
            if self._tx_output_paused():
                time.sleep(0.01)
                continue
            ser = self._tx_ser
            if ser is None:
                time.sleep(0.05)
                continue
            try:
                payload = self._tx_write_q.get(timeout=0.2)
            except queue.Empty:
                continue
            if not payload:
                continue
            if self._tx_output_paused():
                continue
            try:
                with self._tx_ser_lock:
                    if self._tx_output_paused():
                        continue
                    # RS485 direction control (if user enabled)
                    if self.rs485_cfg.mode != Rs485CtrlMode.AUTO:
                        try:
                            apply_rs485_tx_level(ser, self.rs485_cfg)
                            if self.rs485_cfg.pre_tx_ms > 0:
                                time.sleep(self.rs485_cfg.pre_tx_ms / 1000.0)
                        except Exception:
                            pass

                    ser.write(payload)
                    # do not flush aggressively (avoid blocking). virtual serial flush is no-op.
                    try:
                        ser.flush()
                    except Exception:
                        pass

                    if self.rs485_cfg.mode != Rs485CtrlMode.AUTO:
                        try:
                            if self.rs485_cfg.post_tx_ms > 0:
                                time.sleep(self.rs485_cfg.post_tx_ms / 1000.0)
                            apply_rs485_rx_level(ser, self.rs485_cfg)
                        except Exception:
                            pass

                    self._emit_frame('TX_TX', payload, tag=f'tx:{self.tx_port}')
            except Exception as e:
                self._log(f'TX_OUT_ERR: {e}')

    def _queue_tx_payload_latest(self, payload: bytes):
        """Keep only the latest payload in the TX queue."""
        if payload is None:
            return
        try:
            while True:
                self._tx_write_q.get_nowait()
        except Exception:
            pass
        try:
            self._tx_write_q.put_nowait(bytes(payload))
        except Exception:
            pass

    def run(self):
        # open serial
        try:
            ser = self._open_any_serial(
                port_key=self.port,
                baudrate=self.baudrate,
                parity=self.parity,
                stopbits=self.stopbits,
                bytesize=self.bytesize,
                timeout=self.timeout_s,
            )
            self._ser = ser

            # open TX (output) serial if enabled
            if self.tx_enabled:
                if not self.tx_port:
                    raise ValueError('已启用发送串口，但未选择发送串口')
                tx_ser = self._open_any_serial(
                    port_key=self.tx_port,
                    baudrate=self.tx_baudrate,
                    parity='N',
                    stopbits=1,
                    bytesize=8,
                    timeout=0.05,
                )
                self._tx_ser = tx_ser

                # start background writer
                self._tx_writer_thread = threading.Thread(target=self._tx_writer_loop, daemon=True)
                self._tx_writer_thread.start()

        except Exception as e:
            self.status.emit(f'连接失败：{e}')
            self.connected.emit(False)
            return

        # default RS485 RX level to avoid blocking bus
        try:
            if self.rs485_cfg.mode != Rs485CtrlMode.AUTO:
                apply_rs485_rx_level(ser, self.rs485_cfg)
        except Exception:
            pass

        self.connected.emit(True)
        self.status.emit(f'已连接 {self.port} @ {self.baudrate}（未采集）')
        self._log(f'INFO: connected {self.port} @ {self.baudrate}, rs485_mode={self.rs485_cfg.mode}')
        if self.tx_enabled and self._tx_ser is not None:
            self._log(f'INFO: tx_port={self.tx_port} @ {self.tx_baudrate} (fixed interval sender)')

        last_warn_ts = 0.0

        # fixed schedules
        next_poll = time.monotonic()
        next_tx = time.monotonic()

        try:
            while self._running:
                # Always process manual/custom send tasks (even when not acquiring)
                self._process_custom_sends()

                # Async RX tap on TX/output port (if enabled)
                if self.tx_tap_enabled:
                    self._pump_async_rx(max_bytes=512)

                if not self._acquiring:
                    time.sleep(0.03)
                    # keep schedules fresh
                    next_poll = time.monotonic()
                    next_tx = time.monotonic()
                    continue

                with self._lock:
                    poll_ms = int(self.poll_ms)
                    tx_iv_ms = int(self.tx_interval_ms)
                    channels = list(self.channels)
                    blocks = list(self.blocks)

                if not blocks or not channels:
                    time.sleep(0.02)
                    continue

                now = time.monotonic()

                # 1) Modbus polling at fixed interval (independent of TX sending)
                if now >= next_poll:
                    next_poll += max(0.02, poll_ms / 1000.0)

                    ts = time.time()
                    row: Dict[str, Optional[float]] = {ch.name: None for ch in channels if ch.enabled}
                    any_success = False

                    for blk in blocks:
                        if not self._running or not self._acquiring:
                            break
                        # allow low-latency manual sends between blocks
                        self._process_custom_sends()
                        regs = self._send_recv_readregs(blk.start, blk.count)
                        if regs is None or len(regs) < blk.count:
                            continue

                        for sp in blk.channels:
                            try:
                                offset = sp.start - blk.start
                                slice_regs = regs[offset: offset + sp.reg_count]
                                val = decode_registers(slice_regs, sp.ch.dtype, sp.ch.byte_order, sp.ch.word_order)
                                val *= float(sp.ch.scale)
                                row[sp.ch.name] = val
                                any_success = True
                            except Exception:
                                row[sp.ch.name] = None

                    # Always emit a sample so UI can handle gaps/quality flags.
                    self.data_ready.emit(ts, row)

                    if any_success:
                        # build and store latest output line; actual sending is periodic
                        if self.tx_enabled and self._tx_ser is not None:
                            try:
                                parts = ['Data']
                                for ch in channels:
                                    if not ch.enabled:
                                        continue
                                    v = row.get(ch.name, None)
                                    parts.append('nan' if v is None else (f'{v:.6g}'))
                                line = (' '.join(parts) + '\r\n').encode('ascii', errors='replace')
                                with self._latest_lock:
                                    self._latest_out_payload = line
                            except Exception:
                                pass
                    else:
                        tnow = time.time()
                        if tnow - last_warn_ts > 2.0:
                            self.status.emit('采集中：未收到有效响应（请看通讯监视窗口）')
                            last_warn_ts = tnow

                # 2) Output sending at fixed interval (send latest received data)
                if self.tx_enabled and self._tx_ser is not None and tx_iv_ms > 0:
                    now2 = time.monotonic()
                    if now2 >= next_tx:
                        next_tx += max(0.005, tx_iv_ms / 1000.0)
                        with self._latest_lock:
                            payload = bytes(self._latest_out_payload or b'')
                        if payload:
                            self._queue_tx_payload_latest(payload)

                # Sleep until next event (avoid busy loop)
                soon = min(next_poll, next_tx) if (self.tx_enabled and self._tx_ser is not None) else next_poll
                dt = soon - time.monotonic()
                if dt > 0.05:
                    dt = 0.05
                if dt > 0:
                    time.sleep(dt)

        finally:
            try:
                if self._ser:
                    self._ser.close()
            except Exception:
                pass
            self._ser = None

            try:
                if self._tx_ser:
                    self._tx_ser.close()
            except Exception:
                pass
            self._tx_ser = None

            self.connected.emit(False)
            self.acquiring.emit(False)
            self.status.emit('已断开连接')
            self._log('INFO: disconnected')


