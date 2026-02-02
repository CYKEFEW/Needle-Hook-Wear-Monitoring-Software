# -*- coding: utf-8 -*-
"""Window layout and geometry helpers for MainWindow."""

import os
import subprocess

from qt_compat import (
    Qt, QTimer, QApplication, QGuiApplication, QPoint, QMessageBox,
    QSizePolicy, QComboBox,
)


class LayoutMixin:
    def showEvent(self, e):
        super().showEvent(e)
        if getattr(self, "_restored_once", False):
            return
        self._restored_once = True
        QTimer.singleShot(0, self._restore_after_show)
    def _restore_after_show(self):
        self._restore_window_layout()

        # 修复：保存的窗口位置可能部分在屏幕外（负 Y 等）
        self._ensure_frame_on_screen()
        QTimer.singleShot(50, self._ensure_frame_on_screen)
        QTimer.singleShot(180, self._ensure_frame_on_screen)

        # 强制进行几次布局/绘制。
        # 在某些系统（Windows + 高 DPI，或基于 OpenGL 的控件）上，第一帧
        # 在发生 resizeEvent（如用户拖动边框）之前可能不会完全布局/绘制。
        # 我们在不改变可见尺寸的情况下模拟一次。
        self._force_first_layout_pass()
        QTimer.singleShot(0, self._force_first_layout_pass)

    def _force_first_layout_pass(self):
        # 激活中央布局
        try:
            cw = self.centralWidget()
            if cw:
                try:
                    cw.updateGeometry()
                except Exception:
                    pass
                if cw.layout():
                    cw.layout().activate()
        except Exception:
            pass

        # 稳定分割器几何
        try:
            if hasattr(self, "main_splitter") and self.main_splitter is not None:
                try:
                    self.main_splitter.updateGeometry()
                except Exception:
                    pass
                # 重新应用当前尺寸以强制内部重新计算
                self.main_splitter.setSizes(self.main_splitter.sizes())
                # 防止恢复出错的分割器状态（如一侧几乎收缩）。
                try:
                    sizes = self.main_splitter.sizes()
                    if isinstance(sizes, (list, tuple)) and len(sizes) >= 2:
                        total = int(sizes[0]) + int(sizes[1])
                        if total > 0 and (int(sizes[0]) < 180 or int(sizes[1]) < 180):
                            left = max(360, int(total * 0.35))
                            right = max(420, total - left)
                            self.main_splitter.setSizes([left, right])
                except Exception:
                    pass

        except Exception:
            pass

        # 刷新一次待处理事件（仅首次显示）
        try:
            QApplication.processEvents()
        except Exception:
            pass

        # 微调窗口尺寸（触发类似手动拖边的 resizeEvent）
        try:
            is_max = getattr(self, "isMaximized", lambda: False)()
            is_full = getattr(self, "isFullScreen", lambda: False)()
            if not is_max and not is_full:
                w, h = int(self.width()), int(self.height())
                self.resize(w + 1, h + 1)
                self.resize(w, h)
        except Exception:
            pass

        # 让绘图控件稳定下来（pyqtgraph / OpenGL）
        try:
            if hasattr(self, "plot") and self.plot is not None:
                try:
                    self.plot.updateGeometry()
                except Exception:
                    pass
                self.plot.update()

                try:
                    self.plot.repaint()
                except Exception:
                    pass
        except Exception:
            pass

        try:
            self.update()
            self.repaint()
        except Exception:
            pass

        # 确保恢复的窗口框架在当前屏幕完全可见
        # （修复 QSettings 恢复出负 Y 等情况）
        self._ensure_frame_on_screen()

    def _ensure_frame_on_screen(self):
        """确保窗口框架在当前屏幕可用区域内。

        这用于修复 QSettings 恢复的窗口位置出现负 Y（顶部被裁剪）或部分在屏幕外的情况。
        我们使用 *frameGeometry*（含标题栏）而不是客户端区域进行限制。
        """
        try:
            # 不干预最大化/全屏状态
            try:
                if getattr(self, "isMaximized", lambda: False)() or getattr(self, "isFullScreen", lambda: False)():
                    return
            except Exception:
                pass

            screen = None
            try:
                screen = self.screen()
            except Exception:
                screen = None
            if screen is None:
                try:
                    screen = QApplication.primaryScreen()
                except Exception:
                    screen = None
            if screen is None:
                return

            avail = screen.availableGeometry()
            fg = self.frameGeometry()

            x, y = int(fg.x()), int(fg.y())
            w, h = int(fg.width()), int(fg.height())

            # 如果完全在屏幕外，则重新居中
            if (x + w) < (avail.left() + 20) or x > (avail.left() + avail.width() - 20) or (y + h) < (avail.top() + 20) or y > (avail.top() + avail.height() - 20):
                new_x = int(avail.left() + max(0, (avail.width() - w) // 2))
                new_y = int(avail.top() + max(0, (avail.height() - h) // 2))
                self.move(new_x, new_y)
                return

            # 限制到可用区域
            max_x = int(avail.left() + max(0, avail.width() - w))
            max_y = int(avail.top() + max(0, avail.height() - h))
            new_x = min(max(x, int(avail.left())), max_x)
            new_y = min(max(y, int(avail.top())), max_y)

            if new_x != x or new_y != y:
                self.move(new_x, new_y)
        except Exception:
            pass

    def _apply_stable_widget_sizing(self):
        """防止下拉框文本长短变化导致左侧面板宽度抖动。

        这用于在点击连接/刷新等操作时保持当前工作区布局稳定。
        """
        combos = [
            getattr(self, 'port_combo', None),
            getattr(self, 'tx_port_combo', None),
            getattr(self, 'custom_send_port_combo', None),
        ]
        for cb in combos:
            if cb is None:
                continue
            # 优先使用不会随当前文本改变尺寸的策略。
            pol = None
            try:
                pol = QComboBox.SizeAdjustPolicy.AdjustToMinimumContentsLengthWithIcon
            except Exception:
                pol = getattr(QComboBox, 'AdjustToMinimumContentsLengthWithIcon', None)
                if pol is None:
                    pol = getattr(QComboBox, 'AdjustToMinimumContentsLength', None)
            try:
                if pol is not None:
                    cb.setSizeAdjustPolicy(pol)
            except Exception:
                pass
            try:
                cb.setMinimumContentsLength(28)
            except Exception:
                pass
            try:
                cb.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
            except Exception:
                pass

        try:
            self.status_label.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        except Exception:
            pass

    def _apply_safe_geometry(self, x, y, w, h):
        """将几何尺寸限制到当前屏幕可用区域。

        这用于避免 Qt 警告，例如：
        QWindowsWindow::setGeometry: Unable to set geometry ...
        当恢复的窗口尺寸超过当前可用屏幕区域
        （任务栏、DPI 缩放、显示器变化等）时会出现该问题。
        """
        try:
            x = int(x); y = int(y); w = int(w); h = int(h)
        except Exception:
            return

        # 若最大化/全屏，不与窗口管理器对抗。
        try:
            if getattr(self, 'isMaximized', lambda: False)() or getattr(self, 'isFullScreen', lambda: False)():
                return
        except Exception:
            pass

        try:
            cx = x + max(0, w // 2)
            cy = y + max(0, h // 2)
            try:
                center = QPoint(cx, cy)
            except Exception:
                center = None

            screen = None
            try:
                if center is not None:
                    screen = QGuiApplication.screenAt(center)
            except Exception:
                screen = None
            if screen is None:
                try:
                    screen = QGuiApplication.primaryScreen()
                except Exception:
                    screen = None

            if screen is None:
                try:
                    self.setGeometry(x, y, w, h)
                except Exception:
                    pass
                return

            avail = screen.availableGeometry()

            try:
                minw = int(self.minimumWidth()) if int(self.minimumWidth()) > 0 else 640
            except Exception:
                minw = 640
            try:
                minh = int(self.minimumHeight()) if int(self.minimumHeight()) > 0 else 480
            except Exception:
                minh = 480

            w = max(minw, min(w, int(avail.width())))
            h = max(minh, min(h, int(avail.height())))

            x = min(max(x, int(avail.left())), int(avail.right()) - w + 1)
            y = min(max(y, int(avail.top())), int(avail.bottom()) - h + 1)

            self.setGeometry(x, y, w, h)
        except Exception:
            try:
                self.setGeometry(x, y, w, h)
            except Exception:
                pass

    def _ensure_window_on_screen(self):
        """限制当前窗口几何，使其在某个屏幕可见。"""
        try:
            if getattr(self, 'isMaximized', lambda: False)() or getattr(self, 'isFullScreen', lambda: False)():
                return
        except Exception:
            pass
        try:
            g = self.geometry()
            self._apply_safe_geometry(g.x(), g.y(), g.width(), g.height())
        except Exception:
            pass

    def _save_window_layout(self):
        s = getattr(self, '_settings', None)
        if s is None:
            return
        try:
            s.setValue('main/geometry', self.saveGeometry())
            s.setValue('main/state', self.saveState())
            try:
                s.setValue('main/wstate', int(self.windowState()))
            except Exception:
                pass
            try:
                g = self.normalGeometry() if getattr(self, 'isMaximized', lambda: False)() else self.geometry()
                s.setValue('main/rect', [int(g.x()), int(g.y()), int(g.width()), int(g.height())])
            except Exception:
                pass
            if hasattr(self, 'main_splitter'):
                s.setValue('main/splitter', self.main_splitter.saveState())
        except Exception:
            pass

    def _restore_window_layout(self):
        s = getattr(self, '_settings', None)
        if s is None:
            return
        had_split = False
        try:
            had_split = bool(s.value('main/splitter'))
        except Exception:
            had_split = False

        # --- 恢复主窗口矩形（优先） ---
        rect = None
        try:
            rect = s.value('main/rect')
        except Exception:
            rect = None
        if rect and isinstance(rect, (list, tuple)) and len(rect) >= 4:
            try:
                x, y, w, h = rect[:4]
                self._apply_safe_geometry(x, y, w, h)
            except Exception:
                pass
        else:
            # 兼容旧版本：回退到 Qt 的 saveGeometry/restoreGeometry
            try:
                geom = s.value('main/geometry')
                if geom:
                    self.restoreGeometry(geom)
            except Exception:
                pass
            # 限制以防恢复的几何尺寸不适配当前屏幕/DPI
            self._ensure_window_on_screen()

        # --- 恢复停靠面板/工具状态 ---
        try:
            state = s.value('main/state')
            if state:
                self.restoreState(state)
        except Exception:
            pass

        # --- 恢复分割器尺寸 ---
        try:
            sp = s.value('main/splitter')
            if sp and hasattr(self, 'main_splitter'):
                self.main_splitter.restoreState(sp)
        except Exception:
            pass

        # 恢复窗口最大化状态（在几何恢复之后）
        try:
            ws = s.value('main/wstate')
            ws_i = int(ws) if ws is not None else 0
            if ws_i & int(Qt.WindowMaximized):
                self.setWindowState(self.windowState() | Qt.WindowMaximized)
        except Exception:
            pass

        # 在 restoreState 之后再次限制（停靠/最小尺寸可能变化）
        self._ensure_window_on_screen()

        # 首次运行兜底：给左侧面板一个合理宽度。
        if not had_split:
            try:
                if hasattr(self, 'main_splitter'):
                    try:
                        if int(self.main_splitter.count()) >= 2:
                            self.main_splitter.setSizes([420, 1000])
                        else:
                            self.main_splitter.setSizes([1000])
                    except Exception:
                        pass
            except Exception:
                pass


    # ---------- UI 限频辅助 ----------
    def closeEvent(self, event):
        try:
            export_dialog = getattr(self, "_export_queue_dialog", None)
            if export_dialog and export_dialog.is_exporting():
                ret = QMessageBox.question(
                    self,
                    "确认退出",
                    "检测到正在导出的项目。\n确认退出将强制结束所有软件相关进程，是否退出？",
                    QMessageBox.Yes | QMessageBox.No,
                    QMessageBox.No,
                )
                if ret != QMessageBox.Yes:
                    event.ignore()
                    return
                try:
                    export_dialog.force_stop_exports()
                except Exception:
                    pass
            elif export_dialog:
                try:
                    export_dialog.force_stop_exports()
                except Exception:
                    pass
            try:
                import subprocess
                pid = os.getpid()
                subprocess.Popen(["taskkill", "/PID", str(pid), "/T", "/F"])
            except Exception:
                pass
        except Exception:
            pass
        # 持久化工作区布局（停靠位置/分割器尺寸/窗口几何）
        try:
            self._save_window_layout()
        except Exception:
            pass
        # 确保退出前串口线程已停止
        try:
            self.disconnect_serial()
        except Exception:
            pass
        super().closeEvent(event)
