# -*- coding: utf-8 -*-
"""Plotting, buffering, and quality calculations for MainWindow."""

import math
import time

from typing import Dict, List, Optional, Tuple

import pyqtgraph as pg

try:
    import numpy as np
except Exception:  # pragma: no cover
    np = None

from qt_compat import QMessageBox, QWidget, Slot


class PlotMixin:
    def _mark_plot_dirty(self, *args, **kwargs):
        self._plot_dirty = True

    def _set_plot_tab_visible(self, tab_widget: QWidget, title: str, visible: bool):
        if not hasattr(self, "plot_tabs"):
            return
        idx = self.plot_tabs.indexOf(tab_widget)
        if visible and idx < 0:
            self.plot_tabs.addTab(tab_widget, title)
            try:
                self.plot_tabs.setCurrentWidget(tab_widget)
            except Exception:
                pass
            try:
                self.plot_dock.show()
                self.plot_dock.raise_()
                self.plot_dock.activateWindow()
            except Exception:
                pass
        elif (not visible) and idx >= 0:
            try:
                self.plot_tabs.removeTab(idx)
            except Exception:
                pass

    def _refresh_friction_channel_options(self):
        if not hasattr(self, "fric_high_combo") or not hasattr(self, "fric_low_combo"):
            return
        names = []
        try:
            rows = int(self.ch_table.rowCount()) if hasattr(self, "ch_table") else 0
        except Exception:
            rows = 0
        for r in range(rows):
            name = None
            try:
                item = self.ch_table.item(r, 1) if hasattr(self, "ch_table") else None
                name = (item.text() if item else "").strip()
            except Exception:
                name = ""
            if not name:
                name = f"CH{r+1}"
            if name and name not in names:
                names.append(name)

        cur_high = self.fric_high_combo.currentText() if hasattr(self, "fric_high_combo") else ""
        cur_low = self.fric_low_combo.currentText() if hasattr(self, "fric_low_combo") else ""
        cur_high_mu = self.mu_high_combo.currentText() if hasattr(self, "mu_high_combo") else ""
        cur_low_mu = self.mu_low_combo.currentText() if hasattr(self, "mu_low_combo") else ""
        cur_high_avg = self.avg_high_combo.currentText() if hasattr(self, "avg_high_combo") else ""
        cur_low_avg = self.avg_low_combo.currentText() if hasattr(self, "avg_low_combo") else ""

        for combo in [
            self.fric_high_combo, self.fric_low_combo,
            getattr(self, "mu_high_combo", None), getattr(self, "mu_low_combo", None),
            getattr(self, "avg_high_combo", None), getattr(self, "avg_low_combo", None),
        ]:
            if combo is None:
                continue
            try:
                combo.blockSignals(True)
                combo.clear()
                if not names:
                    combo.addItem("(无通道)")
                    combo.setEnabled(False)
                else:
                    for n in names:
                        combo.addItem(n)
                    combo.setEnabled(True)
            except Exception:
                pass
            finally:
                try:
                    combo.blockSignals(False)
                except Exception:
                    pass

        # 如可能则恢复选择
        try:
            if cur_high and cur_high in names:
                self.fric_high_combo.setCurrentText(cur_high)
            if cur_low and cur_low in names:
                self.fric_low_combo.setCurrentText(cur_low)
            if cur_high_mu and cur_high_mu in names and hasattr(self, "mu_high_combo"):
                self.mu_high_combo.setCurrentText(cur_high_mu)
            if cur_low_mu and cur_low_mu in names and hasattr(self, "mu_low_combo"):
                self.mu_low_combo.setCurrentText(cur_low_mu)
            if cur_high_avg and cur_high_avg in names and hasattr(self, "avg_high_combo"):
                self.avg_high_combo.setCurrentText(cur_high_avg)
            if cur_low_avg and cur_low_avg in names and hasattr(self, "avg_low_combo"):
                self.avg_low_combo.setCurrentText(cur_low_avg)
        except Exception:
            pass

        self._on_friction_config_changed()

    def _swap_friction_channels(self):
        try:
            if not self.fric_high_combo.isEnabled() or not self.fric_low_combo.isEnabled():
                return
            hi = self.fric_high_combo.currentIndex()
            lo = self.fric_low_combo.currentIndex()
            if hi < 0 or lo < 0:
                return
            self.fric_high_combo.setCurrentIndex(lo)
            self.fric_low_combo.setCurrentIndex(hi)
        except Exception:
            pass
        self._on_friction_config_changed()

    def _swap_mu_channels(self):
        try:
            if not self.mu_high_combo.isEnabled() or not self.mu_low_combo.isEnabled():
                return
            hi = self.mu_high_combo.currentIndex()
            lo = self.mu_low_combo.currentIndex()
            if hi < 0 or lo < 0:
                return
            self.mu_high_combo.setCurrentIndex(lo)
            self.mu_low_combo.setCurrentIndex(hi)
        except Exception:
            pass
        self._on_mu_config_changed()

    def _swap_avg_channels(self):
        try:
            if not self.avg_high_combo.isEnabled() or not self.avg_low_combo.isEnabled():
                return
            hi = self.avg_high_combo.currentIndex()
            lo = self.avg_low_combo.currentIndex()
            if hi < 0 or lo < 0:
                return
            self.avg_high_combo.setCurrentIndex(lo)
            self.avg_low_combo.setCurrentIndex(hi)
        except Exception:
            pass
        self._on_avg_config_changed()

    def _sync_mu_from_fric(self):
        if not hasattr(self, "mu_high_combo"):
            return
        try:
            self.mu_high_combo.blockSignals(True)
            self.mu_low_combo.blockSignals(True)
            self.mu_wrap_angle_spin.blockSignals(True)
            if self.mu_high_combo.isEnabled():
                self.mu_high_combo.setCurrentText(self.fric_high_combo.currentText())
            if self.mu_low_combo.isEnabled():
                self.mu_low_combo.setCurrentText(self.fric_low_combo.currentText())
            if self.mu_wrap_angle_spin.isEnabled():
                self.mu_wrap_angle_spin.setValue(self.wrap_angle_spin.value())
        except Exception:
            pass
        finally:
            try:
                self.mu_high_combo.blockSignals(False)
                self.mu_low_combo.blockSignals(False)
                self.mu_wrap_angle_spin.blockSignals(False)
            except Exception:
                pass

    def _sync_avg_from_fric(self):
        if not hasattr(self, "avg_high_combo"):
            return
        try:
            self.avg_high_combo.blockSignals(True)
            self.avg_low_combo.blockSignals(True)
            if self.avg_high_combo.isEnabled():
                self.avg_high_combo.setCurrentText(self.fric_high_combo.currentText())
            if self.avg_low_combo.isEnabled():
                self.avg_low_combo.setCurrentText(self.fric_low_combo.currentText())
        except Exception:
            pass
        finally:
            try:
                self.avg_high_combo.blockSignals(False)
                self.avg_low_combo.blockSignals(False)
            except Exception:
                pass

    def _sync_fric_from_mu(self):
        if not hasattr(self, "fric_high_combo"):
            return
        try:
            self.fric_high_combo.blockSignals(True)
            self.fric_low_combo.blockSignals(True)
            self.wrap_angle_spin.blockSignals(True)
            if self.fric_high_combo.isEnabled():
                self.fric_high_combo.setCurrentText(self.mu_high_combo.currentText())
            if self.fric_low_combo.isEnabled():
                self.fric_low_combo.setCurrentText(self.mu_low_combo.currentText())
            if self.wrap_angle_spin.isEnabled():
                self.wrap_angle_spin.setValue(self.mu_wrap_angle_spin.value())
        except Exception:
            pass
        finally:
            try:
                self.fric_high_combo.blockSignals(False)
                self.fric_low_combo.blockSignals(False)
                self.wrap_angle_spin.blockSignals(False)
            except Exception:
                pass

    def _sync_fric_from_avg(self):
        if not hasattr(self, "fric_high_combo"):
            return
        try:
            self.fric_high_combo.blockSignals(True)
            self.fric_low_combo.blockSignals(True)
            if self.fric_high_combo.isEnabled():
                self.fric_high_combo.setCurrentText(self.avg_high_combo.currentText())
            if self.fric_low_combo.isEnabled():
                self.fric_low_combo.setCurrentText(self.avg_low_combo.currentText())
        except Exception:
            pass
        finally:
            try:
                self.fric_high_combo.blockSignals(False)
                self.fric_low_combo.blockSignals(False)
            except Exception:
                pass

    def _on_friction_config_changed(self, *args):
        try:
            self._fric_high_name = (self.fric_high_combo.currentText() or "").strip()
            self._fric_low_name = (self.fric_low_combo.currentText() or "").strip()
        except Exception:
            self._fric_high_name = ""
            self._fric_low_name = ""
        try:
            self._wrap_angle_deg = float(self.wrap_angle_spin.value()) if hasattr(self, "wrap_angle_spin") else 0.0
        except Exception:
            self._wrap_angle_deg = 0.0
        try:
            self._wrap_angle_rad = math.radians(float(self._wrap_angle_deg)) if float(self._wrap_angle_deg) > 0 else 0.0
        except Exception:
            self._wrap_angle_rad = 0.0

        self._sync_quality_from_wrap()

        self._sync_mu_from_fric()
        self._sync_avg_from_fric()
        self._recalc_friction_buffers()
        try:
            self._plot_seq = int(getattr(self, "_plot_seq", 0) or 0) + 1
        except Exception:
            pass
        self._plot_dirty = True
        try:
            self.update_plot()
        except Exception:
            pass

    def _on_mu_config_changed(self, *args):
        self._sync_fric_from_mu()
        self._on_friction_config_changed()

    def _on_avg_config_changed(self, *args):
        self._sync_fric_from_avg()
        self._on_friction_config_changed()

    def _on_quality_rmin_changed(self, *args):
        if getattr(self, '_quality_ui_syncing', False):
            return
        self._sync_quality_ui('main')
        # Rmin only affects quality check; no derived sync needed.
        try:
            self._plot_seq = int(getattr(self, '_plot_seq', 0) or 0) + 1
        except Exception:
            pass
        self._plot_dirty = True

    def _on_quality_rmin_changed_mu(self, *args):
        if getattr(self, '_quality_ui_syncing', False):
            return
        self._sync_quality_ui('mu')
        self._on_quality_rmin_changed()

    def _on_quality_mu_max_changed(self, *args):
        if getattr(self, '_quality_syncing', False) or getattr(self, '_quality_ui_syncing', False):
            return
        self._quality_last_source = 'mu'
        self._sync_quality_from_mu()
        self._sync_quality_ui('main')

    def _on_quality_mu_max_changed_mu(self, *args):
        if getattr(self, '_quality_ui_syncing', False):
            return
        self._sync_quality_ui('mu')
        self._on_quality_mu_max_changed()

    def _on_quality_rmax_changed(self, *args):
        if getattr(self, '_quality_syncing', False) or getattr(self, '_quality_ui_syncing', False):
            return
        self._quality_last_source = 'rmax'
        self._sync_quality_from_rmax()
        self._sync_quality_ui('main')

    def _on_quality_rmax_changed_mu(self, *args):
        if getattr(self, '_quality_ui_syncing', False):
            return
        self._sync_quality_ui('mu')
        self._on_quality_rmax_changed()

    def _on_quality_gap_timeout_changed(self, *args):
        if getattr(self, '_quality_ui_syncing', False):
            return
        try:
            self._quality_gap_timeout_s = float(self.qgap_spin.value()) if hasattr(self, 'qgap_spin') else 1.0
        except Exception:
            self._quality_gap_timeout_s = 1.0
        self._sync_quality_ui('main')

    def _on_quality_gap_timeout_changed_mu(self, *args):
        if getattr(self, '_quality_ui_syncing', False):
            return
        self._sync_quality_ui('mu')
        self._on_quality_gap_timeout_changed()

    def _sync_quality_ui(self, source: str):
        if getattr(self, '_quality_ui_syncing', False):
            return
        self._quality_ui_syncing = True
        try:
            if source == 'main':
                if hasattr(self, 'rmin_spin_mu'):
                    self.rmin_spin_mu.setValue(self.rmin_spin.value())
                if hasattr(self, 'mu_max_spin_mu'):
                    self.mu_max_spin_mu.setValue(self.mu_max_spin.value())
                if hasattr(self, 'rmax_spin_mu'):
                    self.rmax_spin_mu.setValue(self.rmax_spin.value())
                if hasattr(self, 'qgap_spin_mu'):
                    self.qgap_spin_mu.setValue(self.qgap_spin.value())
            elif source == 'mu':
                if hasattr(self, 'rmin_spin_mu'):
                    self.rmin_spin.setValue(self.rmin_spin_mu.value())
                if hasattr(self, 'mu_max_spin_mu'):
                    self.mu_max_spin.setValue(self.mu_max_spin_mu.value())
                if hasattr(self, 'rmax_spin_mu'):
                    self.rmax_spin.setValue(self.rmax_spin_mu.value())
                if hasattr(self, 'qgap_spin_mu'):
                    self.qgap_spin.setValue(self.qgap_spin_mu.value())
        finally:
            self._quality_ui_syncing = False

    def _sync_quality_from_wrap(self):
        if getattr(self, '_quality_syncing', False):
            return
        if getattr(self, '_quality_last_source', 'mu') == 'rmax':
            self._sync_quality_from_rmax()
        else:
            self._sync_quality_from_mu()

    def _sync_quality_from_mu(self):
        if getattr(self, '_quality_syncing', False):
            return
        self._quality_syncing = True
        try:
            mu_max = float(self.mu_max_spin.value()) if hasattr(self, 'mu_max_spin') else 0.0
        except Exception:
            mu_max = 0.0
        try:
            theta = float(getattr(self, '_wrap_angle_rad', 0.0) or 0.0)
        except Exception:
            theta = 0.0
        try:
            rmax = math.exp(mu_max * theta) if theta > 0 else 1.0
        except Exception:
            rmax = 1.0
        try:
            if hasattr(self, 'rmax_spin'):
                self.rmax_spin.setValue(float(rmax))
        finally:
            self._quality_syncing = False
        self._sync_quality_ui('main')

    def _sync_quality_from_rmax(self):
        if getattr(self, '_quality_syncing', False):
            return
        self._quality_syncing = True
        try:
            rmax = float(self.rmax_spin.value()) if hasattr(self, 'rmax_spin') else 1.0
        except Exception:
            rmax = 1.0
        try:
            theta = float(getattr(self, '_wrap_angle_rad', 0.0) or 0.0)
        except Exception:
            theta = 0.0
        if theta > 0 and rmax > 0:
            try:
                mu_max = math.log(rmax) / theta
            except Exception:
                mu_max = 0.0
        else:
            mu_max = 0.0
        try:
            if hasattr(self, 'mu_max_spin'):
                self.mu_max_spin.setValue(float(mu_max))
        finally:
            self._quality_syncing = False
        self._sync_quality_ui('main')

    def _get_quality_gap_timeout(self) -> float:
        try:
            return float(getattr(self, '_quality_gap_timeout_s', 1.0) or 1.0)
        except Exception:
            return 1.0
    def _calc_fric_mu(self, high_v, low_v):
        try:
            if high_v is None or low_v is None:
                return None, None
            high = float(high_v)
            low = float(low_v)
        except Exception:
            return None, None
        try:
            if not math.isfinite(high) or not math.isfinite(low):
                return None, None
        except Exception:
            pass
        fric = high - low
        mu = None
        theta = float(getattr(self, "_wrap_angle_rad", 0.0) or 0.0)
        if theta > 0 and low > 0 and high > 0:
            try:
                ratio = high / low
                if ratio > 0:
                    mu = math.log(ratio) / theta
            except Exception:
                mu = None
        return fric, mu

    def _calc_avg_tension(self, high_v, low_v):
        try:
            if high_v is None or low_v is None:
                return None
            high = float(high_v)
            low = float(low_v)
        except Exception:
            return None
        try:
            if not math.isfinite(high) or not math.isfinite(low):
                return None
        except Exception:
            pass
        return (high + low) / 2.0

    def _update_friction_buffers_at_index(self, idx: int, row: dict):
        if self._fric_buf is None or self._mu_buf is None or self._avg_buf is None:
            return
        high_name = (getattr(self, "_fric_high_name", "") or "").strip()
        low_name = (getattr(self, "_fric_low_name", "") or "").strip()
        if (not high_name) or (not low_name):
            fric, mu = None, None
            avg = None
        else:
            fric, mu = self._calc_fric_mu(row.get(high_name), row.get(low_name))
            avg = self._calc_avg_tension(row.get(high_name), row.get(low_name))
        if np is not None:
            try:
                self._fric_buf[idx] = (np.nan if fric is None else float(fric))
                self._mu_buf[idx] = (np.nan if mu is None else float(mu))
                self._avg_buf[idx] = (np.nan if avg is None else float(avg))
            except Exception:
                pass
        else:
            self._fric_buf[idx] = fric
            self._mu_buf[idx] = mu
            self._avg_buf[idx] = avg

    def _recalc_friction_buffers(self):
        size = int(getattr(self, "_buf_size", 0) or 0)
        if size <= 0 or self._fric_buf is None or self._mu_buf is None or self._avg_buf is None:
            return
        high_name = (getattr(self, "_fric_high_name", "") or "").strip()
        low_name = (getattr(self, "_fric_low_name", "") or "").strip()
        high_buf = self._val_buf_by_channel.get(high_name) if high_name else None
        low_buf = self._val_buf_by_channel.get(low_name) if low_name else None
        for i in range(size):
            if high_buf is None or low_buf is None:
                fric, mu = None, None
                avg = None
            else:
                try:
                    hv = high_buf[i]
                    lv = low_buf[i]
                except Exception:
                    hv = None
                    lv = None
                fric, mu = self._calc_fric_mu(hv, lv)
                avg = self._calc_avg_tension(hv, lv)
            if np is not None:
                try:
                    self._fric_buf[i] = (np.nan if fric is None else float(fric))
                    self._mu_buf[i] = (np.nan if mu is None else float(mu))
                    self._avg_buf[i] = (np.nan if avg is None else float(avg))
                except Exception:
                    pass
            else:
                self._fric_buf[i] = fric
                self._mu_buf[i] = mu
                self._avg_buf[i] = avg

    def _update_friction_plots(self, xs, idx: int, full: bool, count: int, scroll_live: bool, x_left: float, x_right: float):
        if xs is None or self._fric_buf is None or self._mu_buf is None or self._avg_buf is None:
            return
        size = int(getattr(self, "_buf_size", 0) or 0)
        if size <= 0:
            return

        # X 轴范围同步
        if scroll_live:
            try:
                self.friction_plot.setXRange(x_left, x_right, padding=0.0)
            except Exception:
                pass
            try:
                self.mu_plot.setXRange(x_left, x_right, padding=0.0)
            except Exception:
                pass
            try:
                self.avg_plot.setXRange(x_left, x_right, padding=0.0)
            except Exception:
                pass

        if np is not None:
            if full:
                first = size - idx
                if self._fric_plot_y is None or getattr(self._fric_plot_y, "shape", (0,))[0] != size:
                    self._fric_plot_y = np.empty(size, dtype=float)
                if self._mu_plot_y is None or getattr(self._mu_plot_y, "shape", (0,))[0] != size:
                    self._mu_plot_y = np.empty(size, dtype=float)
                if self._avg_plot_y is None or getattr(self._avg_plot_y, "shape", (0,))[0] != size:
                    self._avg_plot_y = np.empty(size, dtype=float)
                self._fric_plot_y[:first] = self._fric_buf[idx:]
                self._fric_plot_y[first:] = self._fric_buf[:idx]
                self._mu_plot_y[:first] = self._mu_buf[idx:]
                self._mu_plot_y[first:] = self._mu_buf[:idx]
                self._avg_plot_y[:first] = self._avg_buf[idx:]
                self._avg_plot_y[first:] = self._avg_buf[:idx]
                ys_fric = self._fric_plot_y
                ys_mu = self._mu_plot_y
                ys_avg = self._avg_plot_y
            else:
                ys_fric = self._fric_buf[:count]
                ys_mu = self._mu_buf[:count]
                ys_avg = self._avg_buf[:count]
            try:
                self.friction_curve.setData(xs, ys_fric, connect="finite", skipFiniteCheck=True)
            except Exception:
                try:
                    self.friction_curve.setData(xs, ys_fric, connect="finite")
                except Exception:
                    pass
            try:
                self.mu_curve.setData(xs, ys_mu, connect="finite", skipFiniteCheck=True)
            except Exception:
                try:
                    self.mu_curve.setData(xs, ys_mu, connect="finite")
                except Exception:
                    pass
            try:
                self.avg_curve.setData(xs, ys_avg, connect="finite", skipFiniteCheck=True)
            except Exception:
                try:
                    self.avg_curve.setData(xs, ys_avg, connect="finite")
                except Exception:
                    pass
        else:
            if full:
                fric_raw = list(self._fric_buf[idx:]) + list(self._fric_buf[:idx])
                mu_raw = list(self._mu_buf[idx:]) + list(self._mu_buf[:idx])
                avg_raw = list(self._avg_buf[idx:]) + list(self._avg_buf[:idx])
            else:
                fric_raw = list(self._fric_buf[:count])
                mu_raw = list(self._mu_buf[:count])
                avg_raw = list(self._avg_buf[:count])
            xs_f, ys_f = [], []
            xs_m, ys_m = [], []
            xs_a, ys_a = [], []
            for t, v in zip(xs, fric_raw):
                if v is None:
                    continue
                xs_f.append(t)
                ys_f.append(v)
            for t, v in zip(xs, mu_raw):
                if v is None:
                    continue
                xs_m.append(t)
                ys_m.append(v)
            for t, v in zip(xs, avg_raw):
                if v is None:
                    continue
                xs_a.append(t)
                ys_a.append(v)
            try:
                self.friction_curve.setData(xs_f, ys_f)
            except Exception:
                pass
            try:
                self.mu_curve.setData(xs_m, ys_m)
            except Exception:
                pass
            try:
                self.avg_curve.setData(xs_a, ys_a)
            except Exception:
                pass

        # 派生曲线的自动缩放处理
        try:
            auto = bool(self.autoscale_chk.isChecked()) if hasattr(self, "autoscale_chk") else True
        except Exception:
            auto = True
        try:
            self.friction_plot.enableAutoRange(axis="y", enable=auto)
            self.mu_plot.enableAutoRange(axis="y", enable=auto)
            self.avg_plot.enableAutoRange(axis="y", enable=auto)
        except Exception:
            pass
    def _flush_plot(self):
        # 绘图刷新由计时器（Hz）驱动，我们始终调用 update_plot()，
        # 它仅在有新采样时才重新上传曲线数据，但
        # 会在指定刷新率下保持 X 平滑滚动。
        self.update_plot()


    def _on_plot_fps_changed(self, *args):
        """将绘图刷新率（Hz）应用到绘图计时器。"""
        try:
            hz = int(self.plot_fps_spin.value()) if hasattr(self, 'plot_fps_spin') else 60
        except Exception:
            hz = 60
        hz = max(1, min(240, hz))
        interval_ms = max(1, int(round(1000.0 / float(hz))))
        try:
            self._plot_timer.setInterval(interval_ms)
        except Exception:
            pass

        # 根据当前状态应用启停
        try:
            self._update_plot_timer_running()
        except Exception:
            pass

    def _update_plot_timer_running(self):
        """根据采集状态启动/停止绘图计时器（节省 CPU，冻结滚动）。"""
        try:
            live = bool(getattr(self, "is_acquiring", False)) and (not bool(getattr(self, "is_paused", False)))
        except Exception:
            live = False

        if live:
            try:
                if not self._plot_timer.isActive():
                    self._plot_timer.start()
            except Exception:
                pass
        else:
            try:
                if self._plot_timer.isActive():
                    self._plot_timer.stop()
            except Exception:
                pass


    def _on_max_points_changed(self, *args):
        """最大点数变化时调整环形缓冲区大小。"""
        try:
            new_size = int(self.max_points_spin.value())
        except Exception:
            return
        self._resize_ring_buffers(new_size)
        self._mark_plot_dirty()

    def _resize_ring_buffers(self, new_size: int):
        new_size = int(max(10, new_size))
        old_size = int(getattr(self, '_buf_size', 0) or 0)
        if new_size <= 0 or new_size == old_size:
            return
        xs, ys_map, xs_wall, qf_vals = self._snapshot_ring(include_wall=True, include_quality=True)
        self._alloc_ring_buffers(
            new_size,
            list(self.channel_names),
            keep_last=True,
            xs=xs,
            ys_map=ys_map,
            xs_wall=xs_wall,
            qf_vals=qf_vals,
        )

    def _alloc_ring_buffers(self, size: int, channel_names: list, keep_last: bool = False, xs=None, ys_map=None, xs_wall=None, qf_vals=None):
        """分配环形缓冲区。

        当 keep_last=True 时，将最后 min(len(xs), size) 个样本复制到新缓冲区。
        """
        size = int(max(10, size))
        self._buf_size = size
        self._buf_count = 0
        self._buf_idx = 0
        self._plot_seq = 0
        self._last_plotted_seq = -1

        if np is not None:
            self._ts_buf = np.full(size, np.nan, dtype=float)
            self._ts_wall_buf = np.full(size, np.nan, dtype=float)
            self._plot_x = np.empty(size, dtype=float)
        else:
            self._ts_buf = [None] * size
            self._ts_wall_buf = [None] * size
            self._plot_x = None

        if np is not None:
            self._fric_buf = np.full(size, np.nan, dtype=float)
            self._mu_buf = np.full(size, np.nan, dtype=float)
            self._avg_buf = np.full(size, np.nan, dtype=float)
            self._fric_plot_y = np.empty(size, dtype=float)
            self._mu_plot_y = np.empty(size, dtype=float)
            self._avg_plot_y = np.empty(size, dtype=float)
        else:
            self._fric_buf = [None] * size
            self._mu_buf = [None] * size
            self._avg_buf = [None] * size
            self._fric_plot_y = None
            self._mu_plot_y = None
            self._avg_plot_y = None
        if np is not None:
            self._qf_buf = np.full(size, np.nan, dtype=float)
        else:
            self._qf_buf = [None] * size

        self._val_buf_by_channel = {}
        self._plot_y_by_channel = {}
        for name in channel_names:
            if np is not None:
                self._val_buf_by_channel[name] = np.full(size, np.nan, dtype=float)
                self._plot_y_by_channel[name] = np.empty(size, dtype=float)
            else:
                self._val_buf_by_channel[name] = [None] * size

        if keep_last and xs:
            try:
                k = min(len(xs), size)
            except Exception:
                k = 0
            if k > 0:
                tail_x = xs[-k:]
                if np is not None:
                    self._ts_buf[:k] = np.asarray(tail_x, dtype=float)
                else:
                    self._ts_buf[:k] = list(tail_x)
                if xs_wall:
                    try:
                        tail_w = xs_wall[-k:]
                    except Exception:
                        tail_w = []
                else:
                    tail_w = []
                if tail_w:
                    if np is not None:
                        self._ts_wall_buf[:k] = np.asarray(tail_w, dtype=float)
                    else:
                        self._ts_wall_buf[:k] = list(tail_w)

                for name in channel_names:
                    ys = (ys_map or {}).get(name, [])
                    tail_y = ys[-k:] if ys else [None] * k
                    if np is not None:
                        arr = np.asarray([(np.nan if v is None else float(v)) for v in tail_y], dtype=float)
                        self._val_buf_by_channel[name][:k] = arr
                    else:
                        self._val_buf_by_channel[name][:k] = list(tail_y)

                if qf_vals:
                    tail_qf = qf_vals[-k:] if len(qf_vals) >= k else list(qf_vals)
                    if len(tail_qf) < k:
                        tail_qf = ([None] * (k - len(tail_qf))) + list(tail_qf)
                    if np is not None:
                        try:
                            arr_qf = np.asarray([(np.nan if v is None else float(v)) for v in tail_qf], dtype=float)
                            self._qf_buf[:k] = arr_qf
                        except Exception:
                            pass
                    else:
                        self._qf_buf[:k] = list(tail_qf)

                # 对保留样本重新计算摩擦相关缓冲
                high_name = (getattr(self, "_fric_high_name", "") or "").strip()
                low_name = (getattr(self, "_fric_low_name", "") or "").strip()
                if high_name and low_name:
                    tail_high = (ys_map or {}).get(high_name, [])
                    tail_low = (ys_map or {}).get(low_name, [])
                    tail_high = tail_high[-k:] if tail_high else [None] * k
                    tail_low = tail_low[-k:] if tail_low else [None] * k
                    for j in range(k):
                        fric, mu = self._calc_fric_mu(tail_high[j], tail_low[j])
                        avg = self._calc_avg_tension(tail_high[j], tail_low[j])
                        if np is not None:
                            try:
                                self._fric_buf[j] = (np.nan if fric is None else float(fric))
                                self._mu_buf[j] = (np.nan if mu is None else float(mu))
                                self._avg_buf[j] = (np.nan if avg is None else float(avg))
                            except Exception:
                                pass
                        else:
                            self._fric_buf[j] = fric
                            self._mu_buf[j] = mu
                            self._avg_buf[j] = avg
                else:
                    if np is not None:
                        try:
                            self._fric_buf[:k] = np.nan
                            self._mu_buf[:k] = np.nan
                            self._avg_buf[:k] = np.nan
                        except Exception:
                            pass
                    else:
                        self._fric_buf[:k] = [None] * k
                        self._mu_buf[:k] = [None] * k
                        self._avg_buf[:k] = [None] * k
                self._buf_count = k
                self._buf_idx = k % size

    def _snapshot_ring(self, include_wall: bool = False, include_quality: bool = False):
        """将环形缓冲区快照为按时间排序的 Python 列表（用于调整大小/导出）。"""
        count = int(getattr(self, '_buf_count', 0) or 0)
        size = int(getattr(self, '_buf_size', 0) or 0)
        if count <= 0 or size <= 0 or self._ts_buf is None:
            if include_wall:
                if include_quality:
                    return [], {}, [], []
                return [], {}, []
            if include_quality:
                return [], {}, []
            return [], {}
        idx = int(getattr(self, '_buf_idx', 0) or 0)

        if count < size:
            # 未环绕
            if np is not None:
                xs = [float(x) for x in self._ts_buf[:count]]
            else:
                xs = list(self._ts_buf[:count])
        else:
            # 已环绕：最旧数据在 idx 处
            if np is not None:
                xs = [float(x) for x in self._ts_buf[idx:]] + [float(x) for x in self._ts_buf[:idx]]
            else:
                xs = list(self._ts_buf[idx:]) + list(self._ts_buf[:idx])

        xs_wall = []
        if include_wall:
            buf = self._ts_wall_buf
            if buf is None:
                xs_wall = []
            else:
                if count < size:
                    if np is not None:
                        arr = buf[:count]
                        xs_wall = [None if (not np.isfinite(v)) else float(v) for v in arr]
                    else:
                        xs_wall = list(buf[:count])
                else:
                    if np is not None:
                        arr = list(buf[idx:]) + list(buf[:idx])
                        xs_wall = [None if (not np.isfinite(v)) else float(v) for v in arr]
                    else:
                        xs_wall = list(buf[idx:]) + list(buf[:idx])

        ys_map = {}
        for name in list(self.channel_names):
            buf = self._val_buf_by_channel.get(name)
            if buf is None:
                continue
            if np is not None:
                if count < size:
                    arr = buf[:count]
                else:
                    arr = np.concatenate((buf[idx:], buf[:idx]))
                ys = [None if (not np.isfinite(v)) else float(v) for v in arr]
            else:
                if count < size:
                    ys = list(buf[:count])
                else:
                    ys = list(buf[idx:]) + list(buf[:idx])
            ys_map[name] = ys
        qf_vals = []
        if include_quality and self._qf_buf is not None:
            buf = self._qf_buf
            if np is not None:
                if count < size:
                    arr = buf[:count]
                else:
                    arr = np.concatenate((buf[idx:], buf[:idx]))
                qf_vals = [None if (not np.isfinite(v)) else float(v) for v in arr]
            else:
                if count < size:
                    qf_vals = list(buf[:count])
                else:
                    qf_vals = list(buf[idx:]) + list(buf[:idx])
        if include_wall:
            if include_quality:
                return xs, ys_map, xs_wall, qf_vals
            return xs, ys_map, xs_wall
        if include_quality:
            return xs, ys_map, qf_vals
        return xs, ys_map

    def _safe_float(self, v):
        try:
            fv = float(v)
        except Exception:
            return None
        try:
            if not math.isfinite(fv):
                return None
        except Exception:
            pass
        return fv

    def _row_data_ok(self, row: dict) -> bool:
        for name in self.channel_names:
            if self._safe_float(row.get(name, None)) is None:
                return False
        return True

    def _sanitize_row(self, row: dict) -> Dict[str, Optional[float]]:
        out: Dict[str, Optional[float]] = {}
        for name in self.channel_names:
            out[name] = self._safe_float(row.get(name, None))
        return out

    def _get_quality_params(self) -> Tuple[float, float]:
        try:
            rmin = float(self.rmin_spin.value()) if hasattr(self, "rmin_spin") else 1.01
        except Exception:
            rmin = 1.01
        try:
            rmax = float(self.rmax_spin.value()) if hasattr(self, "rmax_spin") else 1.0
        except Exception:
            rmax = 1.0
        return rmin, rmax

    def _calc_quality_flag(self, row: dict, data_ok: bool) -> int:
        if not data_ok:
            return 0

        high_name = (getattr(self, "_fric_high_name", "") or "").strip()
        low_name = (getattr(self, "_fric_low_name", "") or "").strip()
        if not high_name or not low_name:
            return 0

        high_v = self._safe_float(row.get(high_name, None))
        low_v = self._safe_float(row.get(low_name, None))
        if high_v is None or low_v is None:
            return 0

        if high_v <= 0 or low_v <= 0:
            return 0

        rmin, rmax = self._get_quality_params()
        try:
            ratio = float(high_v) / float(low_v)
        except Exception:
            return 0
        if ratio < rmin or ratio > rmax:
            return 0

        if getattr(self, "motor_mode", None) == 0:
            last_t = getattr(self, "_last_tension_setpoint", None)
            try:
                last_t = float(last_t) if last_t is not None else None
            except Exception:
                last_t = None
            if last_t is not None and last_t > 0:
                tmin = 0.05 * last_t
                if high_v < tmin or low_v < tmin:
                    return 0

        return 1

    def _commit_sample(self, mono_ts: float, wall_ts: float, row: dict, quality_flag: int):
        # Compute relative time based on the sample's monotonic timestamp.
        if self._t0_mono_ts is None:
            self._t0_mono_ts = float(mono_ts)
        pause_accum = float(getattr(self, '_mono_pause_accum', 0.0) or 0.0)
        rel_ts = float(float(mono_ts) - float(self._t0_mono_ts) - pause_accum)
        if rel_ts < 0.0:
            rel_ts = 0.0

        self._last_sample_rel_ts = rel_ts
        self._last_sample_mono_ts = float(mono_ts)

        size = int(getattr(self, '_buf_size', 0) or 0)
        if size <= 0:
            return
        i = int(getattr(self, '_buf_idx', 0) or 0) % size

        if np is not None:
            try:
                self._ts_buf[i] = float(rel_ts)
            except Exception:
                self._ts_buf[i] = np.nan
            try:
                if self._ts_wall_buf is not None:
                    self._ts_wall_buf[i] = float(wall_ts)
            except Exception:
                try:
                    if self._ts_wall_buf is not None:
                        self._ts_wall_buf[i] = np.nan
                except Exception:
                    pass
            for name in self.channel_names:
                v = row.get(name, None)
                try:
                    self._val_buf_by_channel[name][i] = (np.nan if v is None else float(v))
                except Exception:
                    self._val_buf_by_channel[name][i] = np.nan
            try:
                if self._qf_buf is not None:
                    self._qf_buf[i] = float(quality_flag)
            except Exception:
                pass
        else:
            self._ts_buf[i] = rel_ts
            if self._ts_wall_buf is not None:
                self._ts_wall_buf[i] = wall_ts
            for name in self.channel_names:
                self._val_buf_by_channel[name][i] = row.get(name, None)
            try:
                if self._qf_buf is not None:
                    self._qf_buf[i] = int(quality_flag)
            except Exception:
                pass

        # Update derived buffers (friction/mu) using the committed row values.
        self._update_friction_buffers_at_index(i, row)

        try:
            if getattr(self, "_log_db_path", ""):
                row_for_log = {name: row.get(name, None) for name in self.channel_names}
                row_for_log[self._quality_flag_name] = int(quality_flag)
                self._data_logger.append(wall_ts, row_for_log)
        except Exception:
            pass

        self._buf_idx = (i + 1) % size
        if int(getattr(self, '_buf_count', 0) or 0) < size:
            self._buf_count += 1

        self._plot_seq = int(getattr(self, '_plot_seq', 0) or 0) + 1
        self._plot_dirty = True

    def _trigger_comm_gap_stop(self):
        if getattr(self, "_quality_gap_triggered", False):
            return
        self._quality_gap_triggered = True
        try:
            self.on_motor_estop()
        except Exception:
            pass
        try:
            self.stop_acquire()
        except Exception:
            pass
        try:
            QMessageBox.warning(self, "通信异常", "连续通信丢包或解析失败超过 1 秒，已急停并停止采集。")
        except Exception:
            pass

    def _process_quality_sample(self, mono_ts: float, wall_ts: float, row: dict):
        data_ok = self._row_data_ok(row)

        if not data_ok:
            if self._quality_gap_start_mono is None:
                self._quality_gap_start_mono = float(mono_ts)
                self._quality_gap_triggered = False

            if (float(mono_ts) - float(self._quality_gap_start_mono)) >= self._get_quality_gap_timeout():
                self._trigger_comm_gap_stop()
                return

            pending = self._quality_gap_pending
            pending.append({"mono": mono_ts, "wall": wall_ts, "row": row})

            if self._quality_gap_hold_mode:
                hold_row = self._last_valid_row or self._sanitize_row(row)
                self._commit_sample(mono_ts, wall_ts, hold_row, 0)
                return

            if len(pending) > 3:
                hold_row = self._last_valid_row
                if hold_row is None:
                    for s in pending:
                        self._commit_sample(s["mono"], s["wall"], self._sanitize_row(s["row"]), 0)
                else:
                    for s in pending:
                        self._commit_sample(s["mono"], s["wall"], hold_row, 0)
                pending.clear()
                self._quality_gap_hold_mode = True
            return

        # data ok: flush pending gaps if any
        if self._quality_gap_start_mono is not None:
            self._quality_gap_start_mono = None
            self._quality_gap_triggered = False
        self._quality_gap_hold_mode = False

        pending = self._quality_gap_pending
        if pending:
            m = len(pending)
            if self._last_valid_row is not None and m <= 3:
                last_row = self._last_valid_row
                cur_row = self._sanitize_row(row)
                for idx, s in enumerate(pending, start=1):
                    frac = float(idx) / float(m + 1)
                    interp_row: Dict[str, Optional[float]] = {}
                    for name in self.channel_names:
                        v0 = last_row.get(name, None)
                        v1 = cur_row.get(name, None)
                        if v0 is None or v1 is None:
                            v = v0 if v0 is not None else v1
                        else:
                            v = float(v0) + (float(v1) - float(v0)) * frac
                        interp_row[name] = v
                    self._commit_sample(s["mono"], s["wall"], interp_row, 0)
            else:
                hold_row = self._last_valid_row
                for s in pending:
                    if hold_row is None:
                        self._commit_sample(s["mono"], s["wall"], self._sanitize_row(s["row"]), 0)
                    else:
                        self._commit_sample(s["mono"], s["wall"], hold_row, 0)
            pending.clear()
            self._quality_gap_hold_mode = False

        clean_row = self._sanitize_row(row)
        qf = self._calc_quality_flag(clean_row, True)
        self._commit_sample(mono_ts, wall_ts, clean_row, qf)
        self._last_valid_row = clean_row

    # ---------- 监视 ----------
    def clear_data(self):
        # 重置环形缓冲区（大小跟随当前最大点数）
        try:
            size = int(self.max_points_spin.value())
        except Exception:
            size = int(getattr(self, '_buf_size', 100) or 100)

        self.channel_names.clear()
        self._alloc_ring_buffers(size, [], keep_last=False)

        # 重置绘图时间基准（相对秒）
        self._t0_mono_ts = None
        self._last_sample_rel_ts = None
        self._last_sample_mono_ts = None

        # 重置暂停补偿
        self._mono_pause_accum = 0.0
        self._mono_pause_start = None
        self._quality_gap_pending = []
        self._quality_gap_hold_mode = False
        self._quality_gap_start_mono = None
        self._quality_gap_triggered = False
        self._last_valid_row = None

        self.plot.clear()
        self.plot.addLegend()
        self.curves.clear()
        try:
            if hasattr(self, "friction_curve") and self.friction_curve is not None:
                self.friction_curve.setData([], [])
            if hasattr(self, "mu_curve") and self.mu_curve is not None:
                self.mu_curve.setData([], [])
            if hasattr(self, "avg_curve") and self.avg_curve is not None:
                self.avg_curve.setData([], [])
        except Exception:
            pass
        self.set_status("已清空数据")

    def init_curves(self, channel_names: List[str]):
        self.plot.clear()
        self.plot.addLegend()
        self.curves.clear()

        # 1-7 通道配色：红 橙 黄 绿 青 蓝 紫（更高区分度）
        palette = [
            (220, 0, 0),      # 红
            (255, 140, 0),    # 橙
            (255, 200, 0),    # 黄（白底下略深）
            (0, 170, 0),      # 绿
            (0, 170, 170),    # 青
            (0, 0, 220),      # 蓝
            (140, 0, 200),    # 紫
        ]
        width = 2  # 线宽稍微粗一点

        for i, name in enumerate(channel_names):
            color = palette[i % len(palette)]
            pen = pg.mkPen(color=color, width=width)
            self.curves[name] = self.plot.plot([], [], name=name, pen=pen)
            item = self.curves.get(name)
            if item is not None:
                # 每条曲线的性能提示（跨版本安全）
                try:
                    item.setClipToView(True)
                except Exception:
                    pass
                try:
                    item.setDownsampling(auto=True, mode='peak')
                except Exception:
                    try:
                        item.setDownsampling(auto=True, method='peak')
                    except Exception:
                        pass
                # 部分版本支持跳过有限性检查以提速
                try:
                    item.setSkipFiniteCheck(True)
                except Exception:
                    pass



    @Slot(float, dict)
    def on_data_ready(self, ts: float, row: dict):
        mono_now = time.monotonic()
        try:
            wall_ts = float(ts) if ts is not None else time.time()
        except Exception:
            wall_ts = time.time()

        # 首帧懒初始化曲线与缓冲区（兼容未点击开始采集时的数据）
        if not self.channel_names:
            self.channel_names = list(row.keys())
            self.init_curves(self.channel_names)
            try:
                size = int(self.max_points_spin.value())
            except Exception:
                size = int(getattr(self, '_buf_size', 100) or 100)
            self._alloc_ring_buffers(size, list(self.channel_names), keep_last=False)

        # 保持缓冲区大小与 UI 同步
        try:
            want = int(self.max_points_spin.value())
        except Exception:
            want = int(getattr(self, '_buf_size', 0) or 0)
        if want and want != int(getattr(self, '_buf_size', 0) or 0):
            self._resize_ring_buffers(want)

        self._process_quality_sample(mono_now, wall_ts, row)


    def update_plot(self):
        """更新绘图曲线（缓冲 + 刷新率驱动）。"""
        count = int(getattr(self, "_buf_count", 0) or 0)
        if count <= 0 or not self.channel_names:
            return

        last_seq = int(getattr(self, "_last_plotted_seq", -1))
        cur_seq = int(getattr(self, "_plot_seq", 0) or 0)
        new_data = (last_seq != cur_seq)

        size = int(getattr(self, "_buf_size", 0) or 0)
        if size <= 0 or self._ts_buf is None:
            return
        idx = int(getattr(self, "_buf_idx", 0) or 0) % size
        full = (count >= size)
        # 平滑 X 滚动（实时）：用单调时间驱动右边界。
        # 当未采集（停止/暂停）时，冻结滚动且不要
        # 强制更新 XRange（便于用户平移/缩放最后一帧）。
        scroll_live = bool(getattr(self, 'is_acquiring', False)) and (not bool(getattr(self, 'is_paused', False)))

        try:
            if scroll_live and self._t0_mono_ts is not None:
                pause_accum = float(getattr(self, '_mono_pause_accum', 0.0) or 0.0)
                now_rel = float(time.monotonic() - float(self._t0_mono_ts) - pause_accum)
            else:
                now_rel = float(self._last_sample_rel_ts) if self._last_sample_rel_ts is not None else 0.0
        except Exception:
            now_rel = float(self._last_sample_rel_ts) if self._last_sample_rel_ts is not None else 0.0

        # 可见窗口宽度（秒）：max_points * poll_interval
        try:
            poll_ms = int(self.poll_spin.value()) if hasattr(self, 'poll_spin') else 20
        except Exception:
            poll_ms = 20
        poll_s = max(0.001, float(poll_ms) / 1000.0)
        npts = int(min(count, size))
        span = max(0.02, max(1, npts - 1) * poll_s)
        x_left = now_rel - span
        x_right = now_rel

        # 快速路径：没有新样本时避免重新上传曲线数据。
        # 只保持 X 平滑滚动，同时减少 CPU/GPU 开销。
        if not new_data:
            if scroll_live:
                try:
                    self.plot.setXRange(x_left, x_right, padding=0.0)
                except Exception:
                    pass
                try:
                    self.friction_plot.setXRange(x_left, x_right, padding=0.0)
                except Exception:
                    pass
                try:
                    self.mu_plot.setXRange(x_left, x_right, padding=0.0)
                except Exception:
                    pass
                try:
                    self.avg_plot.setXRange(x_left, x_right, padding=0.0)
                except Exception:
                    pass
            return

        # 仅在上传新曲线数据时准备有序 X 视图。
        xs = None
        if new_data:
            if np is not None:
                if not full:
                    xs = self._ts_buf[:count]
                else:
                    first = size - idx
                    self._plot_x[:first] = self._ts_buf[idx:]
                    self._plot_x[first:] = self._ts_buf[:idx]
                    xs = self._plot_x
            else:
                if not full:
                    xs = list(self._ts_buf[:count])
                else:
                    xs = list(self._ts_buf[idx:]) + list(self._ts_buf[:idx])
                if not xs:
                    xs = None

        # 防止一次更新中多次重绘（对 Windows 有帮助）。
        self.plot.setUpdatesEnabled(False)
        try:
            global_y_min = None
            global_y_max = None

            if new_data and xs is not None and np is not None:
                for name in self.channel_names:
                    buf = self._val_buf_by_channel.get(name)
                    if buf is None:
                        continue

                    if not full:
                        ys = buf[:count]
                    else:
                        ytmp = self._plot_y_by_channel.get(name)
                        if ytmp is None or getattr(ytmp, 'shape', (0,))[0] != size:
                            ytmp = np.empty(size, dtype=float)
                            self._plot_y_by_channel[name] = ytmp
                        first = size - idx
                        ytmp[:first] = buf[idx:]
                        ytmp[first:] = buf[:idx]
                        ys = ytmp
                    ys_use = ys

                    curve = self.curves.get(name)
                    if curve is not None:
                        # 缓冲区很大时限制发送到渲染器的点数。
                        xs_use, ys_use = xs, ys
                        try:
                            max_disp = int(getattr(self, '_max_display_points', 0) or 0)
                        except Exception:
                            max_disp = 0
                        if max_disp:
                            try:
                                n = int(len(xs_use))
                            except Exception:
                                n = 0
                            if n > max_disp:
                                step = max(1, int(n // max_disp))
                                if step > 1:
                                    try:
                                        xs_use = xs_use[::step]
                                        ys_use = ys_use[::step]
                                    except Exception:
                                        # 兜底：不降采样
                                        xs_use, ys_use = xs, ys

                        # 支持时优先跳过有限性检查
                        try:
                            curve.setData(xs_use, ys_use, connect='finite', skipFiniteCheck=True)
                        except Exception:
                            curve.setData(xs_use, ys_use, connect='finite')

                    if self.autoscale_chk.isChecked():
                        finite = np.isfinite(ys_use)
                        if finite.any():
                            y_min = float(np.nanmin(ys_use))
                            y_max = float(np.nanmax(ys_use))
                            global_y_min = y_min if global_y_min is None else min(global_y_min, y_min)
                            global_y_max = y_max if global_y_max is None else max(global_y_max, y_max)
            elif new_data and xs is not None:
                # 兜底（无 numpy）
                for name in self.channel_names:
                    buf = self._val_buf_by_channel.get(name, [])
                    if not full:
                        ys_raw = list(buf[:count])
                    else:
                        ys_raw = list(buf[idx:]) + list(buf[:idx])

                    xs2, ys2 = [], []
                    for t, v in zip(xs, ys_raw):
                        if v is None:
                            continue
                        xs2.append(t)
                        ys2.append(v)

                    curve = self.curves.get(name)
                    if curve is not None:
                        curve.setData(xs2, ys2)

                    if self.autoscale_chk.isChecked() and ys2:
                        y_min, y_max = min(ys2), max(ys2)
                        global_y_min = y_min if global_y_min is None else min(global_y_min, y_min)
                        global_y_max = y_max if global_y_max is None else max(global_y_max, y_max)

            now = time.monotonic()
            # 平滑滚动：保持以“现在”为右边界的固定可见窗口。
            # 仅在采集中执行；停止/暂停后冻结并让
            # 用户检查/平移，不被计时器覆盖。
            if scroll_live:
                try:
                    self.plot.setXRange(x_left, x_right, padding=0.0)
                except Exception:
                    pass

            # Y 轴范围带滞回更新以减少抖动/闪烁
            if self.autoscale_chk.isChecked() and global_y_min is not None and global_y_max is not None:
                if (now - float(getattr(self, "_last_yrange_update", 0.0))) >= 1.0:
                    if global_y_min == global_y_max:
                        pad = 1.0 if global_y_min == 0 else abs(global_y_min) * 0.05
                        new_min = global_y_min - pad
                        new_max = global_y_max + pad
                    else:
                        span = global_y_max - global_y_min
                        pad = span * 0.08
                        new_min = global_y_min - pad
                        new_max = global_y_max + pad

                    apply = True
                    try:
                        cur_min, cur_max = self.plot.viewRange()[1]
                        cur_span = cur_max - cur_min
                        if cur_span > 0:
                            margin = cur_span * 0.05
                            # 如果新范围大多落在当前范围内，则跳过更新。
                            if (new_min >= (cur_min + margin)) and (new_max <= (cur_max - margin)):
                                apply = False
                    except Exception:
                        pass

                    if apply:
                        self.plot.setYRange(new_min, new_max, padding=0.0)
                        self._last_yrange_update = now
        finally:
            self.plot.setUpdatesEnabled(True)
            # 更新所有曲线后只请求一次重绘。
            self.plot.update()

        try:
            self._update_friction_plots(xs, idx, full, count, scroll_live, x_left, x_right)
        except Exception:
            pass

        self._last_plotted_seq = int(getattr(self, "_plot_seq", 0) or 0)


    # ---------- 连接/采集 ----------
