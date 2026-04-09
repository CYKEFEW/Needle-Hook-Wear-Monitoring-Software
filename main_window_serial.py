# -*- coding: utf-8 -*-
"""Serial connection and acquisition helpers for MainWindow."""

from typing import List, Optional
from serial.tools import list_ports

from qt_compat import QMessageBox, Slot

from rs485 import Rs485CtrlConfig
from virtual_serial import SIM_REGISTRY
from worker import ModbusRtuWorker
from sim_window import SerialSimManagerWindow


class SerialMixin:
    def open_simulator(self):
        """打开串口仿真界面（可同时仿真多个串口）。"""
        if not hasattr(self, 'sim_manager') or self.sim_manager is None:
            self.sim_manager = SerialSimManagerWindow(self)
        self.sim_manager.show()
        try:
            self.sim_manager.raise_()
            self.sim_manager.activateWindow()
        except Exception:
            pass


    def set_status(self, msg: str):
        self.status_label.setText(f"状态：{msg}")

    # ---------- 端口 ----------
    def refresh_ports(self):
        """刷新物理 COM 端口和应用内模拟端口。"""
        # 保留当前选择，避免 UI 跳动。
        cur_rx = self.port_combo.currentData()
        cur_tx = self.tx_port_combo.currentData()

        self.port_combo.clear()
        self.tx_port_combo.clear()

        items = []

        # 物理端口
        try:
            ports = list(list_ports.comports())
        except Exception:
            ports = []

        for p in ports:
            desc = p.description or ""
            manu = getattr(p, "manufacturer", "") or ""
            prod = getattr(p, "product", "") or ""
            hwid = getattr(p, "hwid", "") or ""
            extra = " | ".join([x for x in [desc, manu, prod] if x and x.lower() != "n/a"])
            label = f"{p.device}"
            if extra:
                label += f"  —  {extra}"
            elif hwid:
                label += f"  —  {hwid}"
            items.append((label, p.device))

        # 模拟端口
        for info in SIM_REGISTRY.list_infos():
            label = f"{info.com}  —  仿真串口"
            items.append((label, info.key))

        if not items:
            self.port_combo.addItem("(未发现串口)", "")
            self.tx_port_combo.addItem("(未发现串口)", "")
        else:
            for label, key in items:
                self.port_combo.addItem(label, key)
                self.tx_port_combo.addItem(label, key)
        # 如可能则恢复选择
        try:
            if cur_rx:
                idx = self.port_combo.findData(cur_rx)
                if idx >= 0:
                    self.port_combo.setCurrentIndex(idx)
            if cur_tx:
                idx2 = self.tx_port_combo.findData(cur_tx)
                if idx2 >= 0:
                    self.tx_port_combo.setCurrentIndex(idx2)
        except Exception:
            pass

        # 保持自定义发送面板端口列表同步
        self.update_custom_send_ports()
    def update_custom_send_ports(self):
        """只显示本程序已打开的端口。"""
        if not hasattr(self, "custom_send_port_combo"):
            return

        combo = self.custom_send_port_combo
        combo.blockSignals(True)
        combo.clear()

        items = []
        if self.is_connected and self.worker is not None:
            # 当 worker 发出 connected(True) 时，rx/modbus 端口总是已连接
            rx_port = getattr(self.worker, "port", "")
            if rx_port:
                items.append((f"接收串口(Modbus)：{rx_port}", "rx"))

            # tx/输出端口可能被启用
            if getattr(self.worker, "tx_enabled", False) and getattr(self.worker, "_tx_ser", None) is not None:
                tx_port = getattr(self.worker, "tx_port", "")
                if tx_port and tx_port != rx_port:
                    items.append((f"发送串口(输出)：{tx_port}", "tx"))
                elif tx_port:
                    items.append((f"发送串口(输出)：{tx_port}", "tx"))

        if not items:
            combo.addItem("(未连接串口)", "")
            if hasattr(self, "custom_send_line"):
                self.custom_send_line.setEnabled(False)
            if hasattr(self, "custom_send_btn"):
                self.custom_send_btn.setEnabled(False)
        else:
            for text, data in items:
                combo.addItem(text, data)
            if hasattr(self, "custom_send_line"):
                self.custom_send_line.setEnabled(True)
            if hasattr(self, "custom_send_btn"):
                self.custom_send_btn.setEnabled(True)

        combo.blockSignals(False)

    def send_custom_serial(self):
        if not self.is_connected or self.worker is None:
            QMessageBox.information(self, "提示", "请先连接串口后再发送。")
            return

        target = self.custom_send_port_combo.currentData()
        if not target:
            QMessageBox.warning(self, "提示", "当前没有可发送的已连接串口。")
            return

        text = (self.custom_send_line.text() if self.custom_send_line else "").strip()
        if not text:
            return

        add_crlf = bool(self.custom_send_crlf_chk.isChecked())
        self.worker.enqueue_custom_send(str(target), text, add_crlf)
        # 不清空输入（方便连续修改/重复发送）
        self.custom_send_line.setFocus()

    # ---------- 电机控制 ----------
    def _set_serial_widgets_enabled(self, enabled: bool):
        for w in [
            self.port_combo, self.refresh_ports_btn, self.baud_combo,
            self.parity_combo, self.stopbits_combo, self.bytesize_combo, self.timeout_spin,
            self.rs485_mode_combo, self.pre_tx_spin, self.post_tx_spin,
            self.tx_port_combo, self.tx_baud_combo, self.enable_tx_chk, self.mon_rx_chk, self.mon_tx_chk
        ]:
            w.setEnabled(enabled)

    # ---------- 通道表格 ----------
    def toggle_connect(self):
        if self.is_connected:
            self.disconnect_serial()
        else:
            self.connect_serial()

    def connect_serial(self):
        port = self.port_combo.currentData()
        if not port:
            QMessageBox.warning(self, "提示", "未选择有效串口。请刷新串口后选择。")
            return

        try:
            baud = int(self.baud_combo.currentText())
        except Exception:
            baud = 9600

        parity = self.parity_combo.currentText()
        stopbits = int(self.stopbits_combo.currentText())
        bytesize = int(self.bytesize_combo.currentText())
        timeout_s = float(self.timeout_spin.value())

        tx_enabled = bool(self.enable_tx_chk.isChecked())
        tx_port = self.tx_port_combo.currentData() if tx_enabled else ""
        try:
            tx_baud = int(self.tx_baud_combo.currentText())
        except Exception:
            tx_baud = 115200

        tx_interval_ms = int(self.tx_interval_spin.value()) if hasattr(self, 'tx_interval_spin') else 20
        if tx_enabled and not tx_port:
            QMessageBox.warning(self, "提示", "已启用发送串口，但未选择发送串口。")
            self._set_serial_widgets_enabled(True)
            self.connect_btn.setEnabled(True)
            return

        rs485_cfg = Rs485CtrlConfig(
            mode=self.rs485_mode_combo.currentText(),
            tx_level_high=True,
            rx_level_high=False,
            pre_tx_ms=int(self.pre_tx_spin.value()),
            post_tx_ms=int(self.post_tx_spin.value()),
        )

        self.worker = ModbusRtuWorker(
            port=port, baudrate=baud, parity=parity, stopbits=stopbits,
            bytesize=bytesize, timeout_s=timeout_s, rs485_cfg=rs485_cfg,
            tx_enabled=tx_enabled, tx_port=tx_port, tx_baudrate=tx_baud, tx_interval_ms=tx_interval_ms
        )
        self.worker.data_ready.connect(self.on_data_ready)
        self.worker.status.connect(self.set_status)
        self.worker.connected.connect(self.on_connected_state)
        self.worker.acquiring.connect(self.on_acquiring_state)
        self.worker.log_line.connect(self.append_monitor)
        self.worker.frame.connect(self.on_frame)
        try:
            self.worker.set_tx_tap_enabled(bool(self.mon_tx_chk.isChecked()))
        except Exception:
            pass
        try:
            rpm = getattr(self, "_motor_target_rpm", None)
            self.worker.set_target_rpm("0" if rpm is None else f"{float(rpm):g}")
        except Exception:
            pass
        self.worker.start()

        self.set_status("正在连接...")
        self._set_serial_widgets_enabled(False)
        self.connect_btn.setEnabled(False)

    def disconnect_serial(self):
        if self.worker:
            try:
                self.worker.set_acquiring(False)
                self.worker.stop_thread()
                self.worker.wait(1500)
            except Exception:
                pass
            self.worker = None
        try:
            self._stop_data_logger()
        except Exception:
            pass

        self.is_connected = False
        self.is_acquiring = False
        self.is_paused = False
        self.connect_btn.setText("连接串口")
        self.connect_btn.setEnabled(True)
        self.acquire_btn.setText("开始采集")
        self.acquire_btn.setEnabled(False)
        self.pause_btn.setText("暂停")
        self.pause_btn.setEnabled(False)
        self._set_serial_widgets_enabled(True)
        self.set_status("已断开连接")
        self.update_custom_send_ports()
        try:
            self._update_plot_timer_running()
        except Exception:
            pass

    @Slot(bool)
    def on_connected_state(self, ok: bool):
        self.is_connected = bool(ok)
        if ok:
            self.connect_btn.setText("断开串口")
            self.connect_btn.setEnabled(True)
            self.acquire_btn.setEnabled(True)
            self.pause_btn.setEnabled(False)
            self.pause_btn.setText("暂停")
            self.is_paused = False
            self.set_status("已连接（未采集）")
            self.update_custom_send_ports()
        else:
            self.disconnect_serial()
            return

        try:
            self._update_plot_timer_running()
        except Exception:
            pass

    def toggle_acquire(self):
        if not self.is_connected or not self.worker:
            return
        if self.is_acquiring:
            self.stop_acquire()
        else:
            self.start_acquire()

    def start_acquire(self):
        if not self.worker:
            return

        unit_id = int(self.unit_spin.value())
        func_code = int(self.func_combo.currentData())
        poll_ms = int(self.poll_spin.value())
        address_base_1 = bool(self.addr_base1_chk.isChecked())

        channels = self.gather_channels()
        enabled_channels = [c for c in channels if c.enabled]
        if not enabled_channels:
            QMessageBox.warning(self, "提示", "至少启用一个通道。")
            return

        self.worker.update_runtime(unit_id, func_code, poll_ms, address_base_1, enabled_channels, tx_interval_ms=int(self.tx_interval_spin.value()))

        # 开始时重置绘图数据
        self.clear_data()
        # 重置时间轴的暂停补偿
        self._mono_pause_accum = 0.0
        self._mono_pause_start = None
        self._quality_gap_pending = []
        self._quality_gap_hold_mode = False
        self._quality_gap_start_mono = None
        self._quality_gap_triggered = False
        self._last_valid_row = None
        self.channel_names = [c.name for c in enabled_channels]
        self._log_units = [self._last_unit_map.get(c.name, "") for c in enabled_channels]
        self.init_curves(self.channel_names)
        # 按当前最大点数分配环形缓冲区
        try:
            size = int(self.max_points_spin.value())
        except Exception:
            size = int(getattr(self, '_buf_size', 100) or 100)
        self._alloc_ring_buffers(size, list(self.channel_names), keep_last=False)
        self._start_data_logger(self.channel_names, self._log_units)

        self.worker.set_acquiring(True)
        self.is_acquiring = True
        self.is_paused = False
        self.pause_btn.setText("暂停")
        self.pause_btn.setEnabled(True)
        self.acquire_btn.setText("停止采集")
        self.set_status("采集中...（请看通讯监视窗口 TX/RX）")
        try:
            self._update_plot_timer_running()
        except Exception:
            pass

    def stop_acquire(self):
        if self.worker:
            self.worker.set_acquiring(False)
        self.is_acquiring = False
        self.is_paused = False
        self._mono_pause_start = None
        self._quality_gap_pending = []
        self._quality_gap_hold_mode = False
        self._quality_gap_start_mono = None
        self._quality_gap_triggered = False
        self._last_valid_row = None
        self.pause_btn.setText("暂停")
        self.pause_btn.setEnabled(False)
        self.acquire_btn.setText("开始采集")
        self.set_status("已连接（未采集）")
        self._stop_data_logger()
        try:
            self._update_plot_timer_running()
        except Exception:
            pass

    def toggle_pause(self):
        """
        暂停/继续采集：
        - 暂停：停止发送查询（保持连接），不清空数据，不绘图追加
        - 继续：恢复发送查询，继续在原数据后追加
        """
        if not self.is_connected or not self.worker:
            return
        # 只有在“已开始采集（acquire_btn 处于停止采集状态）”时才允许暂停
        if self.acquire_btn.text() != "停止采集":
            return

        if not self.is_paused:
            # 暂停
            self.worker.set_acquiring(False)
            self.is_acquiring = False
            self.is_paused = True
            self._mono_pause_start = time.monotonic()
            self.pause_btn.setText("继续")
            self.set_status("已暂停（保持连接）")
        else:
            # 继续
            try:
                if getattr(self, '_mono_pause_start', None) is not None:
                    self._mono_pause_accum = float(getattr(self, '_mono_pause_accum', 0.0) or 0.0) + max(0.0, float(time.monotonic() - float(self._mono_pause_start)))
            except Exception:
                pass
            self._mono_pause_start = None
            self.worker.set_acquiring(True)
            self.is_acquiring = True
            self.is_paused = False
            self.pause_btn.setText("暂停")
            self.set_status("采集中...（请看通讯监视窗口 TX/RX）")

        try:
            self._update_plot_timer_running()
        except Exception:
            pass


    @Slot(bool)
    def on_acquiring_state(self, on: bool):
        self.is_acquiring = bool(on)


    # ---------- 数据记录 ----------
    def _start_data_logger(self, channel_names: List[str], channel_units: Optional[List[str]] = None):
        try:
            if not channel_names:
                return
            log_names = list(channel_names)
            log_units = list(channel_units or [])
            if self._quality_flag_name not in log_names:
                log_names.append(self._quality_flag_name)
                log_units.append("")
            path = self._data_logger.start_session(log_names, log_units)
            self._log_db_path = path
            self._log_channels = list(log_names)
            self._log_units = list(log_units)
        except Exception:
            self._log_db_path = ""
            self._log_channels = []
            self._log_units = []

    def _stop_data_logger(self):
        try:
            if self._data_logger:
                self._data_logger.stop()
        except Exception:
            pass

