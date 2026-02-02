# -*- coding: utf-8 -*-
"""Motor control helpers for MainWindow."""

from typing import Optional

from qt_compat import QLabel, QMessageBox


class MotorMixin:
    def open_motor_control(self):
        if not hasattr(self, 'motor_dock') or self.motor_dock is None:
            return
        self.motor_dock.show()
        try:
            self.motor_dock.raise_()
            self.motor_dock.activateWindow()
        except Exception:
            pass

    def _set_lamp_color(self, label: QLabel, color: str):
        if label is None:
            return
        try:
            label.setStyleSheet(f"background:{color};border-radius:7px;border:1px solid #444;")
        except Exception:
            pass


    def _set_motor_mode_lamps(self, mode: Optional[int]):
        if mode == 0:
            self._set_lamp_color(self.motor_mode_tension_lamp, "#5cb85c")
            self._set_lamp_color(self.motor_mode_speed_lamp, "#777777")
        elif mode == 1:
            self._set_lamp_color(self.motor_mode_tension_lamp, "#777777")
            self._set_lamp_color(self.motor_mode_speed_lamp, "#5cb85c")
        else:
            self._set_lamp_color(self.motor_mode_tension_lamp, "#777777")
            self._set_lamp_color(self.motor_mode_speed_lamp, "#777777")

    def _require_motor_mode(self, required: Optional[int] = None) -> bool:
        if getattr(self, "motor_mode", None) is None:
            QMessageBox.warning(self, "提示", "请先选择控制模式。")
            return False
        if required is not None and int(self.motor_mode) != int(required):
            mode_name = "张力模式" if int(required) == 0 else "速度模式"
            QMessageBox.warning(self, "提示", f"请先切换到{mode_name}。")
            return False
        return True

    def _motor_can_send(self) -> bool:
        if not self.is_connected or self.worker is None:
            QMessageBox.information(self, "提示", "请先连接串口后再操作电机控制。")
            return False
        if not getattr(self.worker, "tx_enabled", False) or getattr(self.worker, "_tx_ser", None) is None:
            QMessageBox.information(self, "提示", "请先启用“发送串口(输出)”并连接。")
            return False
        return True

    def _send_motor_cmd(self, cmd: str) -> bool:
        if not self._motor_can_send():
            return False
        text = (cmd or "").strip()
        if not text:
            return False
        self.worker.enqueue_custom_send("tx", text, True)
        return True

    def _parse_number_text(self, text: str, label: str) -> Optional[str]:
        t = (text or "").strip()
        if not t:
            QMessageBox.warning(self, "提示", f"请输入{label}。")
            return None
        try:
            v = float(t)
        except Exception:
            QMessageBox.warning(self, "提示", f"{label}不是有效数字。")
            return None
        if v != v or v in (float("inf"), float("-inf")):
            QMessageBox.warning(self, "提示", f"{label}不是有效数字。")
            return None
        return f"{v:g}"

    def on_motor_mode_tension(self):
        if self._send_motor_cmd("ConMode 0"):
            self.motor_mode = 0
            self._set_motor_mode_lamps(self.motor_mode)

    def on_motor_mode_speed(self):
        if self._send_motor_cmd("ConMode 1"):
            self.motor_mode = 1
            self._set_motor_mode_lamps(self.motor_mode)

    def on_motor_enable(self):
        if self._send_motor_cmd("Enable"):
            self._set_lamp_color(self.motor_enable_lamp, "#5cb85c")

    def on_motor_disable(self):
        if self._send_motor_cmd("Disable"):
            self._set_lamp_color(self.motor_enable_lamp, "#777777")

    def on_motor_forward(self):
        if self._send_motor_cmd("Forward"):
            self._set_lamp_color(self.motor_dir_lamp, "#5cb85c")

    def on_motor_backward(self):
        if self._send_motor_cmd("Backward"):
            self._set_lamp_color(self.motor_dir_lamp, "#f0ad4e")

    def on_motor_speed(self):
        if not self._require_motor_mode(1):
            return
        val = self._parse_number_text(self.motor_speed_edit.text(), "转速(RPM)")
        if val is None:
            return
        self._send_motor_cmd(f"Con {val}")

    def on_motor_tension(self):
        if not self._require_motor_mode(0):
            return
        val = self._parse_number_text(self.motor_tension_edit.text(), "张力(g)")
        if val is None:
            return
        try:
            self._last_tension_setpoint = float(val)
        except Exception:
            pass
        self._send_motor_cmd(f"F {val}")

    def on_motor_pid(self):
        kp = self._parse_number_text(self.motor_kp_edit.text(), "Kp")
        if kp is None:
            return
        ki = self._parse_number_text(self.motor_ki_edit.text(), "Ki")
        if ki is None:
            return
        kd = self._parse_number_text(self.motor_kd_edit.text(), "Kd")
        if kd is None:
            return
        self._send_motor_cmd(f"pid {kp} {ki} {kd}")

    def on_motor_estop(self):
        if not self._motor_can_send():
            return
        # 顺序：设置模式 -> 张力 -> 设置模式 -> 速度 -> 禁用
        # 在发送 F/Con 命令前确保模式已设置。
        self._send_motor_cmd("ConMode 0")
        self._send_motor_cmd("F 0")
        self._send_motor_cmd("ConMode 1")
        self._send_motor_cmd("Con 0")
        self._send_motor_cmd("Disable")
        self.motor_mode = None
        self._set_motor_mode_lamps(None)
        self._set_lamp_color(self.motor_enable_lamp, "#777777")
        self._set_lamp_color(self.motor_dir_lamp, "#777777")

