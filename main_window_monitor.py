# -*- coding: utf-8 -*-
"""Communication monitor rendering and logs for MainWindow."""

from qt_compat import QFileDialog, QMessageBox, QTextCursor, Slot

from modbus_utils import hex_bytes


class MonitorMixin:
    def schedule_monitor_render(self, full: bool = False):
        """限制通讯监视器 UI 更新频率。

        full=True 强制全量重绘（如显示模式改变/面板显示）。
        """
        if full:
            setattr(self, "_monitor_force_full", True)
        self._monitor_dirty = True
        try:
            if not self._monitor_timer.isActive():
                self._monitor_timer.start()
        except Exception:
            self.render_monitor(force_full=bool(full))

    def schedule_custom_send_render(self, full: bool = False):
        if full:
            setattr(self, "_manual_force_full", True)
        self._manual_dirty = True
        try:
            if not self._manual_timer.isActive():
                self._manual_timer.start()
        except Exception:
            self.render_custom_send_log(force_full=bool(full))


    def schedule_motor_monitor_render(self, full: bool = False):
        if full:
            setattr(self, "_motor_mon_force_full", True)
        self._motor_mon_dirty = True
        try:
            if not self._motor_mon_timer.isActive():
                self._motor_mon_timer.start()
        except Exception:
            self.render_motor_monitor(force_full=bool(full))
    def _flush_monitor_render(self):
        if not getattr(self, "_monitor_dirty", False):
            return
        # 面板隐藏时，延迟到可见时再渲染
        if hasattr(self, "monitor_dock") and not self.monitor_dock.isVisible():
            self._monitor_dirty = False
            self._monitor_force_full = True
            return

        self._monitor_dirty = False
        force_full = bool(getattr(self, "_monitor_force_full", False))
        try:
            self.render_monitor(force_full=force_full)
        finally:
            self._monitor_force_full = False

        if getattr(self, "_monitor_dirty", False):
            try:
                if not self._monitor_timer.isActive():
                    self._monitor_timer.start()
            except Exception:
                pass


    def _flush_motor_monitor_render(self):
        if not getattr(self, "_motor_mon_dirty", False):
            return
        if hasattr(self, "motor_dock") and not self.motor_dock.isVisible():
            self._motor_mon_dirty = False
            self._motor_mon_force_full = True
            return

        self._motor_mon_dirty = False
        force_full = bool(getattr(self, "_motor_mon_force_full", False))
        try:
            self.render_motor_monitor(force_full=force_full)
        finally:
            self._motor_mon_force_full = False

        if getattr(self, "_motor_mon_dirty", False):
            try:
                if not self._motor_mon_timer.isActive():
                    self._motor_mon_timer.start()
            except Exception:
                pass
    def _flush_manual_render(self):
        if not getattr(self, "_manual_dirty", False):
            return
        if hasattr(self, "custom_send_dock") and not self.custom_send_dock.isVisible():
            self._manual_dirty = False
            self._manual_force_full = True
            return

        self._manual_dirty = False
        force_full = bool(getattr(self, "_manual_force_full", False))
        try:
            self.render_custom_send_log(force_full=force_full)
        finally:
            self._manual_force_full = False

        if getattr(self, "_manual_dirty", False):
            try:
                if not self._manual_timer.isActive():
                    self._manual_timer.start()
            except Exception:
                pass

    def append_monitor(self, s: str):
        """追加一行纯文本信息（非帧）。

        注意：为避免高帧率卡顿，UI 渲染会限频。
        """
        self._monitor_entries.append({"kind": "INFO", "data": b"", "tag": "", "note": str(s)})
        self.schedule_monitor_render()

    def _custom_send_current_tag(self) -> str:
        """返回自定义发送面板的当前过滤标签（如 'rx:COM3' / 'tx:COM4'）。"""
        if not self.is_connected or self.worker is None:
            return ""
        if not hasattr(self, "custom_send_port_combo"):
            return ""
        target = self.custom_send_port_combo.currentData()
        if target == "rx":
            p = getattr(self.worker, "port", "") or ""
            return f"rx:{p}" if p else ""
        if target == "tx":
            p = getattr(self.worker, "tx_port", "") or ""
            return f"tx:{p}" if p else ""
        return ""
    def on_tx_monitor_toggled(self, checked: bool):
        """启用/禁用 TX 端口异步 RX 监听钩子。

        仅影响通讯监视窗口对发送串口的 RX 监听（窥探式读取）。
        """
        if self.is_connected and self.worker is not None:
            try:
                self.worker.set_tx_tap_enabled(bool(checked))
            except Exception:
                pass
        self.schedule_monitor_render()


    def clear_monitor(self):
        self._monitor_entries.clear()
        self.monitor_text.clear()
        self._monitor_render_idx = 0
        self._monitor_render_mode = None

    def clear_custom_send_log(self):
        self._manual_entries.clear()
        self.custom_send_log.clear()
        self._manual_render_idx = 0
        self._manual_render_mode = None
        self._manual_render_tag = None


    def clear_motor_monitor(self):
        self._motor_mon_entries.clear()
        if hasattr(self, "motor_mon_text"):
            self.motor_mon_text.clear()
        self._motor_mon_render_idx = 0
        self._motor_mon_render_mode = None
    def _decode_bytes(self, data: bytes, mode: str) -> str:
        if mode == "hex":
            return hex_bytes(data)
        enc = "utf-8" if mode == "utf-8" else "gbk"
        try:
            return data.decode(enc, errors="replace")
        except Exception:
            return data.decode("utf-8", errors="replace")

    def _format_entry(self, e: dict, mode: str) -> str:
        kind = e.get("kind", "")
        tag = e.get("tag", "")
        note = e.get("note", "")
        data = e.get("data", b"") or b""
        prefix = kind
        if tag:
            prefix += f"[{tag}]"
        if data:
            payload = self._decode_bytes(data, mode)
            if note:
                return f"{prefix}: {payload}  {note}"
            return f"{prefix}: {payload}"
        # 无数据
        return f"{prefix}: {note}" if prefix else str(note)

    def render_monitor(self, force_full: bool = False):
        mode = "hex"
        if hasattr(self, "monitor_mode_combo"):
            mode = self.monitor_mode_combo.currentData() or "hex"

        # 记录模式/索引用于增量追加渲染
        last_mode = getattr(self, "_monitor_render_mode", None)
        render_idx = int(getattr(self, "_monitor_render_idx", 0) or 0)

        # 若模式变更或强制刷新，则重建最近 N 行（少见路径）
        if force_full or last_mode != mode or render_idx <= 0:
            max_lines = 2000
            entries = self._monitor_entries[-max_lines:]
            try:
                self.monitor_text.blockSignals(True)
                self.monitor_text.setPlainText("\n".join(self._format_entry(e, mode) for e in entries))
                self.monitor_text.blockSignals(False)
                self.monitor_text.moveCursor(QTextCursor.End)
            except Exception:
                pass
            self._monitor_render_mode = mode
            self._monitor_render_idx = len(self._monitor_entries)
            return

        # 增量追加新行
        if render_idx < len(self._monitor_entries):
            new_entries = self._monitor_entries[render_idx:]
            # 批量追加以减少 UI 更新
            lines = [self._format_entry(e, mode) for e in new_entries]
            if lines:
                try:
                    self.monitor_text.blockSignals(True)
                    self.monitor_text.appendPlainText("\n".join(lines))
                    self.monitor_text.blockSignals(False)
                    self.monitor_text.moveCursor(QTextCursor.End)
                except Exception:
                    pass
        self._monitor_render_mode = mode
        self._monitor_render_idx = len(self._monitor_entries)

    def render_custom_send_log(self, force_full: bool = False):
        if not hasattr(self, "custom_send_mode_combo"):
            return
        mode = self.custom_send_mode_combo.currentData() or "hex"
        tag_filter = self._custom_send_current_tag()

        last_mode = getattr(self, "_manual_render_mode", None)
        last_tag = getattr(self, "_manual_render_tag", None)
        render_idx = int(getattr(self, "_manual_render_idx", 0) or 0)

        if force_full or last_mode != mode or last_tag != tag_filter or render_idx <= 0:
            max_lines = 1200
            entries = [e for e in self._manual_entries if (not tag_filter or e.get("tag") == tag_filter)]
            entries = entries[-max_lines:]
            try:
                self.custom_send_log.blockSignals(True)
                self.custom_send_log.setPlainText("\n".join(self._format_entry(e, mode) for e in entries))
                self.custom_send_log.blockSignals(False)
                self.custom_send_log.moveCursor(QTextCursor.End)
            except Exception:
                pass
            self._manual_render_mode = mode
            self._manual_render_tag = tag_filter
            self._manual_render_idx = len(self._manual_entries)
            return

        if render_idx < len(self._manual_entries):
            new_entries = self._manual_entries[render_idx:]
            lines = []
            for e in new_entries:
                if (not tag_filter) or (e.get("tag") == tag_filter):
                    lines.append(self._format_entry(e, mode))
            if lines:
                try:
                    self.custom_send_log.blockSignals(True)
                    self.custom_send_log.appendPlainText("\n".join(lines))
                    self.custom_send_log.blockSignals(False)
                    self.custom_send_log.moveCursor(QTextCursor.End)
                except Exception:
                    pass
        self._manual_render_mode = mode
        self._manual_render_tag = tag_filter
        self._manual_render_idx = len(self._manual_entries)


    def render_motor_monitor(self, force_full: bool = False):
        mode = "utf-8"
        if hasattr(self, "motor_mon_mode_combo"):
            mode = self.motor_mon_mode_combo.currentData() or "utf-8"
        last_mode = getattr(self, "_motor_mon_render_mode", None)
        render_idx = int(getattr(self, "_motor_mon_render_idx", 0) or 0)
        max_lines = 2000

        if force_full or last_mode != mode or render_idx > len(self._motor_mon_entries):
            entries = self._motor_mon_entries[-max_lines:]
            try:
                self.motor_mon_text.blockSignals(True)
                self.motor_mon_text.setPlainText("\n".join(self._format_entry(e, mode) for e in entries))
                self.motor_mon_text.blockSignals(False)
                self.motor_mon_text.moveCursor(QTextCursor.End)
            except Exception:
                pass
            self._motor_mon_render_mode = mode
            self._motor_mon_render_idx = len(self._motor_mon_entries)
            return

        if render_idx < len(self._motor_mon_entries):
            new_entries = self._motor_mon_entries[render_idx:]
            lines_out = [self._format_entry(e, mode) for e in new_entries]
            if lines_out:
                try:
                    self.motor_mon_text.blockSignals(True)
                    self.motor_mon_text.appendPlainText("\n".join(lines_out))
                    self.motor_mon_text.blockSignals(False)
                    self.motor_mon_text.moveCursor(QTextCursor.End)
                except Exception:
                    pass
        self._motor_mon_render_mode = mode
        self._motor_mon_render_idx = len(self._motor_mon_entries)
    @Slot(str, bytes, str, str)
    def on_frame(self, kind: str, data: bytes, tag: str, note: str):
        e = {"kind": str(kind), "data": bytes(data or b""), "tag": str(tag or ""), "note": str(note or "")}

        # 为自定义发送面板始终保留手动 TX/RX 记录
        is_manual = str(kind).startswith('TX_MANUAL') or str(kind).startswith('RX_MANUAL')
        if is_manual:
            self._manual_entries.append(e)
            if self.custom_send_dock.isVisible():
                self.schedule_custom_send_render()

            # 限制日志内存占用
            if len(self._manual_entries) > 6000:
                overflow = len(self._manual_entries) - 6000
                del self._manual_entries[:overflow]
                try:
                    self._manual_render_idx = max(0, int(getattr(self, '_manual_render_idx', 0)) - overflow)
                except Exception:
                    self._manual_render_idx = 0

        # 通讯监视器按端口过滤（rx/tx 监听复选框）
        t = e.get('tag', '') or ''
        allow = True
        if t.startswith('rx:') and hasattr(self, 'mon_rx_chk'):
            allow = bool(self.mon_rx_chk.isChecked())
        elif t.startswith('tx:') and hasattr(self, 'mon_tx_chk'):
            allow = bool(self.mon_tx_chk.isChecked())

        if allow:
            self._monitor_entries.append(e)
            self.schedule_monitor_render()

            if len(self._monitor_entries) > 12000:
                overflow = len(self._monitor_entries) - 12000
                del self._monitor_entries[:overflow]
                try:
                    self._monitor_render_idx = max(0, int(getattr(self, '_monitor_render_idx', 0)) - overflow)
                except Exception:
                    self._monitor_render_idx = 0



        # 电机 TX 监视：仅显示 tx 端口的 RX 帧
        t2 = e.get("tag", "") or ""
        k2 = str(kind)
        if t2.startswith("tx:") and k2.startswith("RX"):
            self._motor_mon_entries.append(e)
            self.schedule_motor_monitor_render()
            if len(self._motor_mon_entries) > 8000:
                overflow = len(self._motor_mon_entries) - 8000
                del self._motor_mon_entries[:overflow]
                try:
                    self._motor_mon_render_idx = max(0, int(getattr(self, "_motor_mon_render_idx", 0)) - overflow)
                except Exception:
                    self._motor_mon_render_idx = 0
    def save_monitor_log(self):
        path, _ = QFileDialog.getSaveFileName(self, "保存通讯日志", "comm_log.txt", "Text Files (*.txt)")
        if not path:
            return
        if not path.lower().endswith(".txt"):
            path += ".txt"
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write(self.monitor_text.toPlainText())
            self.set_status(f"通讯日志已保存：{path}")
        except Exception as e:
            QMessageBox.critical(self, "保存失败", f"保存通讯日志失败：\n{e}")

    # ---------- 状态 ----------
