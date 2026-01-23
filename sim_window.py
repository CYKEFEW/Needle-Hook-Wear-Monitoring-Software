# -*- coding: utf-8 -*-
"""Serial simulator UI window."""

from typing import List

from qt_compat import (
    QMainWindow, QWidget, QLabel, QComboBox, QPushButton, QLineEdit,
    QSpinBox, QDoubleSpinBox, QCheckBox, QHBoxLayout, QVBoxLayout, QGridLayout,
    QGroupBox, QTabWidget, QPlainTextEdit, QMessageBox, QTimer, QTextCursor, Signal
)

from modbus_utils import crc16_modbus, hex_bytes
from virtual_serial import SIM_REGISTRY, VirtualPortInfo

class SerialSimManagerWindow(QMainWindow):
    """串口仿真器（程序内虚拟串口）。

    说明：
    - 这些“仿真串口”只存在于本程序内部，不会在系统中创建真实 COM 口。
    - 创建后，会出现在主界面的串口下拉框中：例如 “COM10  —  仿真串口”。
    """

    ports_changed = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle('串口仿真')
        self.resize(760, 560)

        cw = QWidget()
        self.setCentralWidget(cw)
        v = QVBoxLayout(cw)

        top = QHBoxLayout()
        top.addWidget(QLabel('COM号'))
        self.com_spin = QSpinBox()
        self.com_spin.setRange(1, 256)
        self.com_spin.setValue(10)
        top.addWidget(self.com_spin)

        top.addWidget(QLabel('波特率'))
        self.baud_combo = QComboBox()
        for b in [9600, 19200, 38400, 57600, 115200, 230400]:
            self.baud_combo.addItem(str(b), b)
        self.baud_combo.setCurrentText('115200')
        top.addWidget(self.baud_combo)

        self.btn_create = QPushButton('创建仿真串口')
        self.btn_remove = QPushButton('移除当前')
        top.addWidget(self.btn_create)
        top.addWidget(self.btn_remove)
        top.addStretch(1)
        v.addLayout(top)

        self.tabs = QTabWidget()
        v.addWidget(self.tabs, 1)

        self.btn_create.clicked.connect(lambda *_: self.create_port())
        self.btn_remove.clicked.connect(lambda *_: self.remove_current())

        self.rebuild_tabs()

    def closeEvent(self, ev):
        # 关闭时隐藏窗口，不销毁（保持仿真逻辑继续运行）
        try:
            ev.ignore()
        except Exception:
            pass
        self.hide()

    def rebuild_tabs(self):
        self.tabs.clear()
        for info in SIM_REGISTRY.list_infos():
            w = SimPortWidget(info, parent=self)
            self.tabs.addTab(w, info.com)

    def create_port(self):
        com_num = int(self.com_spin.value())
        baud = int(self.baud_combo.currentData() or 9600)
        try:
            info = SIM_REGISTRY.create(com_num=com_num, baudrate=baud)
        except Exception as e:
            QMessageBox.warning(self, '创建失败', str(e))
            return
        w = SimPortWidget(info, parent=self)
        self.tabs.addTab(w, info.com)
        self.tabs.setCurrentWidget(w)
        self.ports_changed.emit()

    def remove_current(self):
        w = self.tabs.currentWidget()
        if w is None:
            return
        key = getattr(w, 'port_key', '')
        if not key:
            return
        SIM_REGISTRY.remove(key)
        self.rebuild_tabs()
        self.ports_changed.emit()


class SimPortWidget(QWidget):
    """单个仿真串口的配置页。"""

    def __init__(self, info: VirtualPortInfo, parent=None):
        super().__init__(parent)
        self.info = info
        self.port_key = info.key
        self.ser = info.sim

        self._rx_buf = bytearray()

        # log entries are stored as structured data so the user can switch
        # between HEX / text(UTF-8/GBK) display at any time.
        self._log_entries: List[dict] = []
        self._log_dirty = False
        self._log_timer = QTimer(self)
        self._log_timer.setInterval(50)
        self._log_timer.setSingleShot(True)
        self._log_timer.timeout.connect(self._flush_log_render)

        root = QVBoxLayout(self)
        root.addWidget(QLabel(f"{info.com}  (key: {info.key})"))

        top_row = QHBoxLayout()
        top_row.addWidget(QLabel('波特率'))
        self.port_baud_combo = QComboBox()
        for b in [9600, 19200, 38400, 57600, 115200, 230400, 460800, 921600]:
            self.port_baud_combo.addItem(str(b), b)
        try:
            self.port_baud_combo.setCurrentText(str(int(getattr(info, 'baudrate', 115200))))
        except Exception:
            self.port_baud_combo.setCurrentText('115200')
        self.port_baud_combo.currentIndexChanged.connect(self.on_port_baud_changed)
        top_row.addWidget(self.port_baud_combo)
        top_row.addStretch(1)
        root.addLayout(top_row)

        # 连续发送
        g1 = QGroupBox('连续发送模式')
        l1 = QGridLayout(g1)
        self.cont_enable = QCheckBox('启用')
        self.cont_interval = QSpinBox()
        self.cont_interval.setRange(5, 100000)
        self.cont_interval.setValue(100)
        self.cont_interval.setSuffix(' ms')
        self.cont_source = QComboBox()
        self.cont_source.addItem('随机数', 'random')
        self.cont_source.addItem('固定文本', 'fixed')
        l1.addWidget(self.cont_enable, 0, 0)
        l1.addWidget(QLabel('间隔'), 0, 1)
        l1.addWidget(self.cont_interval, 0, 2)
        l1.addWidget(QLabel('发送内容'), 0, 3)
        l1.addWidget(self.cont_source, 0, 4)
        root.addWidget(g1)

        # 随机数配置
        g2 = QGroupBox('随机数生成')
        l2 = QGridLayout(g2)
        self.rand_channels = QSpinBox()
        self.rand_channels.setRange(1, 200)
        self.rand_channels.setValue(2)
        self.rand_min = QDoubleSpinBox()
        self.rand_min.setRange(-1e9, 1e9)
        self.rand_min.setValue(-1.0)
        self.rand_max = QDoubleSpinBox()
        self.rand_max.setRange(-1e9, 1e9)
        self.rand_max.setValue(1.0)
        self.rand_mode = QComboBox()
        self.rand_mode.addItem('纯文本(逗号分隔)', 'text')
        self.rand_mode.addItem('Modbus模式(03响应帧)', 'modbus')
        self.rand_mode.setCurrentIndex(1)
        self.rand_unit = QSpinBox()
        self.rand_unit.setRange(1, 247)
        self.rand_unit.setValue(1)
        self.rand_crlf = QCheckBox('末尾\r\n(文本)')
        self.rand_crlf.setChecked(True)
        self.enc_combo = QComboBox()
        self.enc_combo.addItem('UTF-8', 'utf-8')
        self.enc_combo.addItem('GBK', 'gbk')
        self.btn_rand_once = QPushButton('随机单次发送')

        l2.addWidget(QLabel('通道数'), 0, 0)
        l2.addWidget(self.rand_channels, 0, 1)
        l2.addWidget(QLabel('下限'), 0, 2)
        l2.addWidget(self.rand_min, 0, 3)
        l2.addWidget(QLabel('上限'), 0, 4)
        l2.addWidget(self.rand_max, 0, 5)

        l2.addWidget(QLabel('输出模式'), 1, 0)
        l2.addWidget(self.rand_mode, 1, 1, 1, 2)
        l2.addWidget(QLabel('Modbus地址'), 1, 3)
        l2.addWidget(self.rand_unit, 1, 4)
        l2.addWidget(self.rand_crlf, 1, 5)

        l2.addWidget(QLabel('文本编码'), 2, 0)
        l2.addWidget(self.enc_combo, 2, 1)
        l2.addWidget(self.btn_rand_once, 2, 5)

        root.addWidget(g2)

        # 固定文本
        g3 = QGroupBox('固定信号')
        l3 = QGridLayout(g3)
        self.fixed_cont = QLineEdit()
        self.fixed_cont.setPlaceholderText('固定信号(连续发送/触发发送时使用)')
        self.fixed_cont_crlf = QCheckBox('末尾\r\n')
        self.fixed_cont_crlf.setChecked(True)

        self.fixed_once = QLineEdit()
        self.fixed_once.setPlaceholderText('固定信号(单次发送)')
        self.fixed_once_crlf = QCheckBox('末尾\r\n')
        self.fixed_once_crlf.setChecked(True)
        self.btn_fixed_once = QPushButton('单次发送')

        l3.addWidget(QLabel('固定(连续/触发)'), 0, 0)
        l3.addWidget(self.fixed_cont, 0, 1, 1, 3)
        l3.addWidget(self.fixed_cont_crlf, 0, 4)

        l3.addWidget(QLabel('固定(单次)'), 1, 0)
        l3.addWidget(self.fixed_once, 1, 1, 1, 3)
        l3.addWidget(self.fixed_once_crlf, 1, 4)
        l3.addWidget(self.btn_fixed_once, 1, 5)

        root.addWidget(g3)

        # 触发发送
        g4 = QGroupBox('触发发送模式')
        l4 = QGridLayout(g4)
        self.tr_enable = QCheckBox('启用触发')
        self.tr_type = QComboBox()
        self.tr_type.addItem('Modbus解析模式(识别03/04读请求)', 'modbus')
        self.tr_type.addItem('指令模式(以\\r\\n或\\n结尾)', 'cmd')
        self.tr_cmd = QLineEdit()
        self.tr_cmd.setPlaceholderText('触发指令(不需要输入\\r\\n)')
        self.tr_source = QComboBox()
        self.tr_source.addItem('随机数', 'random')
        self.tr_source.addItem('固定文本', 'fixed')

        l4.addWidget(self.tr_enable, 0, 0)
        l4.addWidget(QLabel('触发类型'), 0, 1)
        l4.addWidget(self.tr_type, 0, 2, 1, 2)
        l4.addWidget(QLabel('触发后发送'), 0, 4)
        l4.addWidget(self.tr_source, 0, 5)
        l4.addWidget(QLabel('触发指令'), 1, 0)
        l4.addWidget(self.tr_cmd, 1, 1, 1, 5)
        root.addWidget(g4)

        # 日志显示模式
        log_mode_row = QHBoxLayout()
        log_mode_row.addWidget(QLabel('日志显示'))
        self.log_mode_combo = QComboBox()
        self.log_mode_combo.addItem('HEX', 'hex')
        self.log_mode_combo.addItem('文本(UTF-8)', 'utf-8')
        self.log_mode_combo.addItem('文本(GBK)', 'gbk')
        self.log_mode_combo.setCurrentIndex(1)
        self.log_mode_combo.currentIndexChanged.connect(lambda *_: self._schedule_log_render(full=True))
        log_mode_row.addWidget(self.log_mode_combo)
        log_mode_row.addStretch(1)
        root.addLayout(log_mode_row)

        self.log = QPlainTextEdit()
        self.log.setReadOnly(True)
        self.log.setUndoRedoEnabled(False)
        try:
            self.log.setMaximumBlockCount(900)
        except Exception:
            pass
        self.log.setMinimumHeight(140)
        root.addWidget(QLabel('日志(仅本窗口)'))
        root.addWidget(self.log, 1)

        # timers
        self._rx_timer = QTimer(self)
        self._rx_timer.setInterval(10)
        self._rx_timer.timeout.connect(self._on_rx_timer)
        self._rx_timer.start()

        self._cont_timer = QTimer(self)
        self._cont_timer.timeout.connect(self._on_cont_timer)

        # signals
        self.cont_enable.toggled.connect(self._update_cont_timer)
        self.cont_interval.valueChanged.connect(self._update_cont_timer)
        self.btn_fixed_once.clicked.connect(lambda *_: self.send_fixed_once())
        self.btn_rand_once.clicked.connect(lambda *_: self.send_random_once())
        self.tr_type.currentIndexChanged.connect(self._update_tr_ui)
        self._update_tr_ui()
        self._update_cont_timer()
    def _log(self, s: str):
        self._append_log_text(str(s))

    def _append_log_text(self, s: str):
        self._log_entries.append({'t': 'text', 'text': str(s)})
        # cap memory
        if len(self._log_entries) > 2000:
            overflow = len(self._log_entries) - 2000
            del self._log_entries[:overflow]
            try:
                self._log_render_idx = max(0, int(getattr(self, '_log_render_idx', 0)) - overflow)
            except Exception:
                self._log_render_idx = 0
        self._schedule_log_render(full=False)

    def _append_log_bytes(self, prefix: str, data: bytes):
        self._log_entries.append({'t': 'bytes', 'prefix': str(prefix), 'data': bytes(data or b'')})
        if len(self._log_entries) > 2000:
            overflow = len(self._log_entries) - 2000
            del self._log_entries[:overflow]
            try:
                self._log_render_idx = max(0, int(getattr(self, '_log_render_idx', 0)) - overflow)
            except Exception:
                self._log_render_idx = 0
        self._schedule_log_render(full=False)

    def _schedule_log_render(self, full: bool = False):
        if full:
            setattr(self, '_log_force_full', True)
        self._log_dirty = True
        if not self._log_timer.isActive():
            self._log_timer.start()

    def _flush_log_render(self):
        if not self._log_dirty:
            return
        self._log_dirty = False
        force_full = bool(getattr(self, '_log_force_full', False))
        try:
            self.render_log(force_full=force_full)
        finally:
            self._log_force_full = False

    def _decode_for_log(self, data: bytes, mode: str) -> str:
        if mode == 'hex':
            return hex_bytes(data)
        enc = 'utf-8' if mode == 'utf-8' else 'gbk'
        try:
            return data.decode(enc, errors='replace')
        except Exception:
            return data.decode('utf-8', errors='replace')

    def _format_log_entry(self, e: dict, mode: str) -> str:
        if e.get('t') == 'text':
            return str(e.get('text', ''))
        b = e.get('data', b'') or b''
        pfx = str(e.get('prefix', ''))
        show = b if len(b) <= 128 else b[:128]
        payload = self._decode_for_log(show, mode)
        if len(b) > 128:
            payload = payload + ' ...'
        return f"{pfx}({len(b)}): {payload}"

    def render_log(self, force_full: bool = False):
        mode = 'utf-8'
        if hasattr(self, 'log_mode_combo'):
            mode = self.log_mode_combo.currentData() or 'utf-8'

        last_mode = getattr(self, '_log_render_mode', None)
        render_idx = int(getattr(self, '_log_render_idx', 0) or 0)

        if force_full or last_mode != mode or render_idx <= 0:
            max_lines = 900
            entries = self._log_entries[-max_lines:]
            out_lines = [self._format_log_entry(e, mode) for e in entries]
            try:
                self.log.blockSignals(True)
                self.log.setPlainText('\n'.join(out_lines))
                self.log.blockSignals(False)
                self.log.moveCursor(QTextCursor.End)
            except Exception:
                pass
            self._log_render_mode = mode
            self._log_render_idx = len(self._log_entries)
            return

        if render_idx < len(self._log_entries):
            new_entries = self._log_entries[render_idx:]
            out_lines = [self._format_log_entry(e, mode) for e in new_entries]
            if out_lines:
                try:
                    self.log.blockSignals(True)
                    self.log.appendPlainText('\n'.join(out_lines))
                    self.log.blockSignals(False)
                    self.log.moveCursor(QTextCursor.End)
                except Exception:
                    pass
        self._log_render_mode = mode
        self._log_render_idx = len(self._log_entries)

    def on_port_baud_changed(self):
        try:
            baud = int(self.port_baud_combo.currentData() or 9600)
        except Exception:
            baud = 9600
        try:
            SIM_REGISTRY.set_baudrate(self.port_key, baud)
            try:
                self.info.baudrate = baud
            except Exception:
                pass
            self._append_log_text(f"INFO: 波特率已设置为 {baud}")
        except Exception as e:
            self._append_log_text(f"BAUD_ERR: {e}")

    def _update_tr_ui(self):
        self.tr_cmd.setEnabled(self.tr_type.currentData() == 'cmd')

    def _update_cont_timer(self):
        if not self.cont_enable.isChecked():
            self._cont_timer.stop()
            return
        iv = max(5, int(self.cont_interval.value()))
        self._cont_timer.setInterval(iv)
        if not self._cont_timer.isActive():
            self._cont_timer.start()

    def _encode_text(self, s: str, add_crlf: bool) -> bytes:
        if add_crlf:
            s = s + '\r\n'
        enc = str(self.enc_combo.currentData() or 'utf-8')
        try:
            return s.encode(enc, errors='replace')
        except Exception:
            return s.encode('utf-8', errors='replace')

    def _rand_values(self, n: int):
        import random
        lo = float(self.rand_min.value())
        hi = float(self.rand_max.value())
        if hi < lo:
            lo, hi = hi, lo
        return [random.uniform(lo, hi) for _ in range(n)]

    def _build_random_payload(self) -> bytes:
        mode = self.rand_mode.currentData()
        n = int(self.rand_channels.value())
        vals = self._rand_values(n)

        if mode == 'modbus':
            # 生成一个“03响应帧”（可用于连续发送；也可用于指令触发发送）
            unit = int(self.rand_unit.value()) & 0xFF
            fc = 0x03
            regs = []
            for v in vals:
                iv = int(round(v))
                iv = max(-32768, min(32767, iv))
                regs.append(iv & 0xFFFF)
            data = bytearray([unit, fc, len(regs) * 2])
            for r in regs:
                data.append((r >> 8) & 0xFF)
                data.append(r & 0xFF)
            crc = crc16_modbus(bytes(data))
            data.append(crc & 0xFF)
            data.append((crc >> 8) & 0xFF)
            return bytes(data)

        # text
        s = ','.join([f'{v:.6g}' for v in vals])
        return self._encode_text(s, add_crlf=bool(self.rand_crlf.isChecked()))

    def _build_fixed_payload(self, s: str, add_crlf: bool) -> bytes:
        return self._encode_text(s or '', add_crlf=add_crlf)
    def _send_bytes(self, b: bytes, prefix: str = 'TX'):
        if not b:
            return
        try:
            self.ser.write(b)
            self._append_log_bytes(prefix, b)
        except Exception as e:
            self._append_log_text(f'{prefix}_ERR: {e}')

    def send_fixed_once(self):
        b = self._build_fixed_payload(self.fixed_once.text(), bool(self.fixed_once_crlf.isChecked()))
        self._send_bytes(b, prefix='TX_ONCE')

    def send_random_once(self):
        b = self._build_random_payload()
        self._send_bytes(b, prefix='TX_RAND')

    def _on_cont_timer(self):
        if not self.cont_enable.isChecked():
            return
        src = self.cont_source.currentData()
        if src == 'random':
            b = self._build_random_payload()
        else:
            b = self._build_fixed_payload(self.fixed_cont.text(), bool(self.fixed_cont_crlf.isChecked()))
        self._send_bytes(b, prefix='TX_CONT')

    def _consume_modbus_read_req(self):
        """Consume one Modbus RTU read request frame (03/04)."""
        buf = self._rx_buf
        while len(buf) >= 8:
            unit = buf[0]
            fc = buf[1]
            if fc not in (0x03, 0x04):
                del buf[0]
                continue
            frame = bytes(buf[:8])
            body = frame[:6]
            crc_rx = frame[6] | (frame[7] << 8)
            if crc16_modbus(body) != crc_rx:
                del buf[0]
                continue
            start = (frame[2] << 8) | frame[3]
            count = (frame[4] << 8) | frame[5]
            del buf[:8]
            return unit, fc, start, count
        return None

    def _handle_trigger_modbus(self):
        req = self._consume_modbus_read_req()
        if not req:
            return
        unit, fc, start, count = req
        cfg_unit = int(self.rand_unit.value()) & 0xFF
        if unit != cfg_unit:
            return

        # 按请求数量返回随机 16-bit 有符号寄存器（四舍五入）
        try:
            import random
            lo = float(self.rand_min.value())
            hi = float(self.rand_max.value())
            if hi < lo:
                lo, hi = hi, lo
            regs = []
            for _ in range(int(count)):
                iv = int(round(random.uniform(lo, hi)))
                iv = max(-32768, min(32767, iv))
                regs.append(iv & 0xFFFF)

            resp = bytearray([unit, fc, len(regs) * 2])
            for r in regs:
                resp.append((r >> 8) & 0xFF)
                resp.append(r & 0xFF)
            crc = crc16_modbus(bytes(resp))
            resp.append(crc & 0xFF)
            resp.append((crc >> 8) & 0xFF)

            self.ser.write(bytes(resp))
            self._log(f'RX_TRIG(modbus): start={start} count={count} -> sent {len(resp)} bytes')
        except Exception as e:
            self._log(f'TRIG_MODBUS_ERR: {e}')

    def _handle_trigger_cmd(self, line: str):
        cmd = (self.tr_cmd.text() or '').strip()
        if not cmd:
            return
        if line.strip() != cmd:
            return
        src = self.tr_source.currentData()
        if src == 'random':
            b = self._build_random_payload()
        else:
            b = self._build_fixed_payload(self.fixed_cont.text(), bool(self.fixed_cont_crlf.isChecked()))
        self._send_bytes(b, prefix='TX_TRIG')

    def _on_rx_timer(self):
        # read bytes from host->sim direction
        try:
            n = int(getattr(self.ser, 'in_waiting', 0) or 0)
        except Exception:
            n = 0
        if n > 0:
            try:
                chunk = self.ser.read(n)
            except Exception:
                chunk = b''
            if chunk:
                self._rx_buf.extend(chunk)
                self._append_log_bytes('RX', chunk)

        if not self.tr_enable.isChecked():
            return

        t = self.tr_type.currentData()
        if t == 'modbus':
            self._handle_trigger_modbus()
        else:
            # 指令模式：按换行解析
            while True:
                idx = self._rx_buf.find(b'\n')
                if idx < 0:
                    break
                line_bytes = bytes(self._rx_buf[:idx])
                del self._rx_buf[:idx+1]
                if line_bytes.endswith(b'\r'):
                    line_bytes = line_bytes[:-1]
                try:
                    line = line_bytes.decode('utf-8', errors='ignore')
                except Exception:
                    line = ''
                if line:
                    self._handle_trigger_cmd(line)


