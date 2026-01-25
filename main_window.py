# -*- coding: utf-8 -*-
"""Main UI window for Modbus assistant."""

import math
import os
import sqlite3
import time
import unicodedata
from typing import Dict, List, Optional

import pyqtgraph as pg
from pyqtgraph.graphicsItems.DateAxisItem import DateAxisItem

import serial
from serial.tools import list_ports

from openpyxl import Workbook, load_workbook
from openpyxl.utils import get_column_letter

# Optional: numpy speeds up plotting at high rates
try:
    import numpy as np
except Exception:  # pragma: no cover
    np = None

from qt_compat import (
    Qt, QMainWindow, QWidget, QLabel, QComboBox, QPushButton, QLineEdit,
    QSpinBox, QDoubleSpinBox, QCheckBox, QHBoxLayout, QVBoxLayout, QGridLayout,
    QGroupBox, QTableWidget, QTableWidgetItem, QMessageBox, QFileDialog,
    QHeaderView, QDockWidget, QTabWidget, QTextEdit, QPlainTextEdit, QSplitter,
    QSizePolicy, QTimer, QSettings, QPoint, QTextCursor, QGuiApplication, QApplication,
    Slot,
)

from modbus_utils import ChannelConfig, DTYPE_INFO, hex_bytes
from rs485 import Rs485CtrlConfig, Rs485CtrlMode
from virtual_serial import SIM_REGISTRY
from worker import ModbusRtuWorker
from sim_window import SerialSimManagerWindow
from data_logger import DataLogger

class MainWindow(QMainWindow):

    def __init__(self):
        super().__init__()
        try:
            self.setObjectName('MainWindow')
        except Exception:
            pass
        self.setWindowTitle("Modbus RTU 上位机助手（浅色主题 / 通讯监视）")
        self.resize(1260, 820)

        self.worker: Optional[ModbusRtuWorker] = None
        self.is_connected = False
        self.is_acquiring = False
        self.is_paused = False

        self.channel_names: List[str] = []
        self.curves: Dict[str, pg.PlotDataItem] = {}

        # ---- Plot ring buffer (size = 当前窗口最大点数) ----
        self._buf_size = 100
        self._buf_count = 0
        self._buf_idx = 0  # next write index
        self._ts_buf = None  # numpy array or list
        self._ts_wall_buf = None  # wall-clock seconds (epoch)
        self._val_buf_by_channel: Dict[str, object] = {}  # name -> np.ndarray or list
        self._plot_x = None  # contiguous x for plotting (numpy)
        self._plot_y_by_channel: Dict[str, object] = {}  # name -> np.ndarray or list
        self._fric_buf = None
        self._mu_buf = None
        self._fric_plot_y = None
        self._mu_plot_y = None
        self._plot_seq = 0

        self._last_plotted_seq = -1
        # Smooth scrolling time base (relative seconds).
        self._t0_mono_ts = None           # time.monotonic() at first sample
        self._last_sample_rel_ts = None   # last sample relative seconds
        self._last_sample_mono_ts = None  # time.monotonic() at last sample

        # Pause-compensated monotonic timeline (so resume does not jump).
        self._mono_pause_accum = 0.0
        self._mono_pause_start = None
        self._settings = QSettings("ModbusAssistant", "ModbusAssistant")
        self._fric_high_name = ""
        self._fric_low_name = ""
        self._wrap_angle_deg = 0.0
        self._wrap_angle_rad = 0.0
        self._data_logger = DataLogger(base_dir=os.path.join(os.getcwd(), "data_logs"))
        self._log_db_path = ""
        self._log_channels: List[str] = []
        self._log_units: List[str] = []
        self._last_unit_map: Dict[str, str] = {}

        root = QWidget()
        self.setCentralWidget(root)
        layout = QHBoxLayout(root)
        layout.setContentsMargins(0, 0, 0, 0)

        # Use a splitter so left/right sizes stay stable even when widgets change enabled/text states.
        self.main_splitter = QSplitter(Qt.Horizontal)
        layout.addWidget(self.main_splitter, 1)

        left_widget = QWidget()
        left = QVBoxLayout(left_widget)
        left.setContentsMargins(8, 8, 8, 8)


        self.main_splitter.addWidget(left_widget)
        self.main_splitter.setStretchFactor(0, 1)
        try:
            self.main_splitter.setCollapsible(0, False)
        except Exception:
            pass

        # ---- Serial group ----
        serial_box = QGroupBox("串口配置")
        left.addWidget(serial_box)
        sg = QGridLayout(serial_box)

        self.port_combo = QComboBox()
        self.refresh_ports_btn = QPushButton("刷新串口")
        self.refresh_ports_btn.clicked.connect(lambda *_: self.refresh_ports())

        self.baud_combo = QComboBox()
        for b in [9600, 19200, 38400, 57600, 115200, 230400, 460800, 921600]:
            self.baud_combo.addItem(str(b), b)
        self.baud_combo.setCurrentText("38400")  # 默认 9600

        self.parity_combo = QComboBox()
        self.parity_combo.addItems(["N", "E", "O"])

        self.stopbits_combo = QComboBox()
        self.stopbits_combo.addItems(["1", "2"])

        self.bytesize_combo = QComboBox()
        self.bytesize_combo.addItems(["7", "8"])
        self.bytesize_combo.setCurrentText("8")

        self.timeout_spin = QDoubleSpinBox()
        self.timeout_spin.setDecimals(2)
        self.timeout_spin.setRange(0.05, 10.0)
        self.timeout_spin.setSingleStep(0.05)
        self.timeout_spin.setValue(0.5)

        # RS485 direction control
        self.rs485_mode_combo = QComboBox()
        self.rs485_mode_combo.addItems([Rs485CtrlMode.RTS, Rs485CtrlMode.DTR, Rs485CtrlMode.AUTO])
        self.rs485_mode_combo.setCurrentText(Rs485CtrlMode.RTS)  # 默认尝试用RTS解决TX不亮

        self.pre_tx_spin = QSpinBox()
        self.pre_tx_spin.setRange(0, 200)
        self.pre_tx_spin.setValue(0)
        self.pre_tx_spin.setSuffix(" ms")

        self.post_tx_spin = QSpinBox()
        self.post_tx_spin.setRange(0, 200)
        self.post_tx_spin.setValue(2)
        self.post_tx_spin.setSuffix(" ms")

        # Monitor enable for comm monitor (rx/tx)
        self.mon_rx_chk = QCheckBox("监听")
        self.mon_rx_chk.setChecked(False)
        self.mon_tx_chk = QCheckBox("监听")
        self.mon_tx_chk.setChecked(False)

        sg.addWidget(QLabel("接收串口(Modbus)"), 0, 0)
        sg.addWidget(self.port_combo, 0, 1)
        sg.addWidget(self.refresh_ports_btn, 0, 2)

        sg.addWidget(self.mon_rx_chk, 0, 3)

        self.tx_port_combo = QComboBox()
        self.tx_baud_combo = QComboBox()
        for b in [9600, 19200, 38400, 57600, 115200, 230400, 460800, 921600]:
            self.tx_baud_combo.addItem(str(b), b)
        self.tx_baud_combo.setCurrentText("115200")

        self.tx_interval_spin = QSpinBox()
        self.tx_interval_spin.setRange(5, 100000)
        self.tx_interval_spin.setValue(20)  # 默认发送间隔：20ms（115200下更稳妥，且与轮询周期匹配）
        self.tx_interval_spin.setSuffix(" ms")
        self.enable_tx_chk = QCheckBox("启用数据发送串口")
        self.enable_tx_chk.setChecked(True)
        sg.addWidget(QLabel("发送串口(输出)"), 1, 0)
        sg.addWidget(self.tx_port_combo, 1, 1)
        sg.addWidget(self.enable_tx_chk, 1, 2)

        sg.addWidget(self.mon_tx_chk, 1, 3)
        sg.addWidget(QLabel("发送波特率"), 2, 0)
        sg.addWidget(self.tx_baud_combo, 2, 1)
        sg.addWidget(QLabel("发送间隔"), 2, 2)
        sg.addWidget(self.tx_interval_spin, 2, 3)

        sg.addWidget(QLabel("接收波特率"), 3, 0)
        sg.addWidget(self.baud_combo, 3, 1)

        sg.addWidget(QLabel("接收校验"), 4, 0)
        sg.addWidget(self.parity_combo, 4, 1)

        sg.addWidget(QLabel("接收停止位"), 5, 0)
        sg.addWidget(self.stopbits_combo, 5, 1)

        sg.addWidget(QLabel("接收数据位"), 6, 0)
        sg.addWidget(self.bytesize_combo, 6, 1)

        sg.addWidget(QLabel("接收超时(s)"), 7, 0)
        sg.addWidget(self.timeout_spin, 7, 1)

        sg.addWidget(QLabel("RS485方向控制"), 8, 0)
        sg.addWidget(self.rs485_mode_combo, 8, 1)
        sg.addWidget(QLabel("TX前延时"), 9, 0)
        sg.addWidget(self.pre_tx_spin, 9, 1)
        sg.addWidget(QLabel("TX后延时"), 10, 0)
        sg.addWidget(self.post_tx_spin, 10, 1)

        # Connect / Acquire buttons
        btns = QHBoxLayout()
        self.connect_btn = QPushButton("连接串口")
        self.connect_btn.clicked.connect(lambda *_: self.toggle_connect())

        self.acquire_btn = QPushButton("开始采集")
        self.acquire_btn.clicked.connect(lambda *_: self.toggle_acquire())
        self.acquire_btn.setEnabled(False)

        self.pause_btn = QPushButton("暂停")
        self.pause_btn.clicked.connect(lambda *_: self.toggle_pause())
        self.pause_btn.setEnabled(False)

        btns.addWidget(self.connect_btn)
        btns.addWidget(self.acquire_btn)
        btns.addWidget(self.pause_btn)
        left.addLayout(btns)

        # ---- Modbus group ----
        modbus_box = QGroupBox("Modbus 配置")
        left.addWidget(modbus_box)
        mg = QGridLayout(modbus_box)

        self.unit_spin = QSpinBox()
        self.unit_spin.setRange(1, 247)
        self.unit_spin.setValue(1)

        self.func_combo = QComboBox()
        self.func_combo.addItem("03 读保持寄存器", 3)
        self.func_combo.addItem("04 读输入寄存器", 4)

        self.poll_spin = QSpinBox()
        self.poll_spin.setRange(20, 100000)
        self.poll_spin.setValue(20)
        self.poll_spin.setSuffix(" ms")

        self.addr_base1_chk = QCheckBox("地址从 1 开始（自动 -1）")
        self.addr_base1_chk.setChecked(False)  # 你的示例帧 start=0x0000

        mg.addWidget(QLabel("从站地址(站号)"), 0, 0)
        mg.addWidget(self.unit_spin, 0, 1)
        mg.addWidget(QLabel("功能码"), 1, 0)
        mg.addWidget(self.func_combo, 1, 1)
        mg.addWidget(QLabel("轮询周期"), 2, 0)
        mg.addWidget(self.poll_spin, 2, 1)
        mg.addWidget(self.addr_base1_chk, 3, 0, 1, 2)

        # ---- Plot group ----
        plot_box = QGroupBox("绘图设置")
        left.addWidget(plot_box)
        pgd = QGridLayout(plot_box)

        self.max_points_spin = QSpinBox()
        self.max_points_spin.setRange(10, 200000)
        self.max_points_spin.setValue(100)

        self.autoscale_chk = QCheckBox("Y轴自动缩放")
        self.autoscale_chk.setChecked(True)

        # plot update is throttled; mark dirty when settings change
        self.max_points_spin.valueChanged.connect(self._on_max_points_changed)
        self.autoscale_chk.toggled.connect(self._mark_plot_dirty)

        self.plot_fps_spin = QSpinBox()
        self.plot_fps_spin.setRange(1, 240)
        self.plot_fps_spin.setValue(60)
        self.plot_fps_spin.setSuffix(" Hz")
        self.plot_fps_spin.valueChanged.connect(self._on_plot_fps_changed)

        self.clear_btn = QPushButton("清空数据")
        self.clear_btn.clicked.connect(lambda *_: self.clear_data())

        self.save_btn = QPushButton("保存为 XLSX")
        self.save_btn.clicked.connect(lambda *_: self.save_xlsx())

        pgd.addWidget(QLabel("当前窗口最大点数"), 0, 0)
        pgd.addWidget(self.max_points_spin, 0, 1)
        pgd.addWidget(QLabel("绘图刷新率"), 1, 0)
        pgd.addWidget(self.plot_fps_spin, 1, 1)
        pgd.addWidget(self.autoscale_chk, 2, 0, 1, 2)
        pgd.addWidget(self.clear_btn, 3, 0)
        pgd.addWidget(self.save_btn, 3, 1)

        # ---- Channel group ----
        ch_box = QGroupBox("通道配置（多通道）")
        left.addWidget(ch_box, 1)
        cl = QVBoxLayout(ch_box)

        self.ch_table = QTableWidget(0, 8)
        self.ch_table.setHorizontalHeaderLabels([
            "启用", "名称", "地址", "数据类型", "字节序(Word内)", "字顺序(Word间)", "缩放系数", "单位"
        ])
        self.ch_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self.ch_table.horizontalHeader().setStretchLastSection(True)
        self.ch_table.setAlternatingRowColors(True)
        self.ch_table.itemChanged.connect(lambda *_: self._refresh_friction_channel_options())
        cl.addWidget(self.ch_table)

        btn_row = QHBoxLayout()
        self.add_ch_btn = QPushButton("添加通道")
        self.del_ch_btn = QPushButton("删除选中")
        self.add_ch_btn.clicked.connect(lambda *_: self.add_channel_row())
        self.del_ch_btn.clicked.connect(lambda *_: self.delete_selected_rows())
        btn_row.addWidget(self.add_ch_btn)
        btn_row.addWidget(self.del_ch_btn)
        btn_row.addStretch(1)
        cl.addLayout(btn_row)

        # ---- Plot area ----
        # Use a numeric time axis (seconds) for smooth scrolling.
        # DateAxisItem snaps ticks to "nice" boundaries (often 1s), which can
        # look like the x-axis only moves once per second.
        self.plot = pg.PlotWidget()
        # ---- Plot performance options ----
        # 1) Clip drawing to view to avoid rendering off-screen segments
        # 2) Enable auto-downsampling (peak mode) when data is dense
        # 3) Keep a renderer-side point budget as a safe fallback
        try:
            pi = self.plot.getPlotItem()
            try:
                pi.setClipToView(True)
            except Exception:
                pass
            try:
                # Different pyqtgraph versions use "mode" or "method"
                pi.setDownsampling(auto=True, mode="peak")
            except Exception:
                try:
                    pi.setDownsampling(auto=True, method="peak")
                except Exception:
                    pass
        except Exception:
            pass
        # Fallback point budget sent to renderer per curve (0=disable)
        self._max_display_points = 6000

        self.plot.setLabel("bottom", "时间", units="s")
        self.plot.setLabel("left", "张力")
        self.plot.addLegend()
        self.plot.showGrid(x=True, y=True, alpha=0.25)

        # ---- Friction force plot ----
        self.friction_plot = pg.PlotWidget()
        try:
            pi = self.friction_plot.getPlotItem()
            try:
                pi.setClipToView(True)
            except Exception:
                pass
            try:
                pi.setDownsampling(auto=True, mode="peak")
            except Exception:
                try:
                    pi.setDownsampling(auto=True, method="peak")
                except Exception:
                    pass
        except Exception:
            pass
        self.friction_plot.setLabel("bottom", "时间", units="s")
        self.friction_plot.setLabel("left", "摩擦力")
        self.friction_plot.addLegend()
        self.friction_plot.showGrid(x=True, y=True, alpha=0.25)
        self.friction_curve = self.friction_plot.plot([], [], name="摩擦力", pen=pg.mkPen(color=(255, 140, 0), width=2))

        # ---- Friction coefficient plot ----
        self.mu_plot = pg.PlotWidget()
        try:
            pi = self.mu_plot.getPlotItem()
            try:
                pi.setClipToView(True)
            except Exception:
                pass
            try:
                pi.setDownsampling(auto=True, mode="peak")
            except Exception:
                try:
                    pi.setDownsampling(auto=True, method="peak")
                except Exception:
                    pass
        except Exception:
            pass
        self.mu_plot.setLabel("bottom", "时间", units="s")
        self.mu_plot.setLabel("left", "摩擦系数")
        self.mu_plot.addLegend()
        self.mu_plot.showGrid(x=True, y=True, alpha=0.25)
        self.mu_curve = self.mu_plot.plot([], [], name="摩擦系数", pen=pg.mkPen(color=(0, 120, 220), width=2))

        # ---- Plot window (dock) ----
        self.plot_tabs = QTabWidget()
        tension_tab = QWidget()
        t_layout = QVBoxLayout(tension_tab)
        t_layout.setContentsMargins(0, 0, 0, 0)
        t_layout.addWidget(self.plot, 1)
        self.status_label = QLabel("状态：未连接")
        t_layout.addWidget(self.status_label)
        self.plot_tabs.addTab(tension_tab, "张力")

        self.friction_tab = QWidget()
        f_layout = QVBoxLayout(self.friction_tab)
        f_layout.setContentsMargins(0, 0, 0, 0)
        cfg = QGridLayout()
        self.fric_high_combo = QComboBox()
        self.fric_low_combo = QComboBox()
        self.fric_swap_btn = QPushButton("互换")
        self.wrap_angle_spin = QDoubleSpinBox()
        self.wrap_angle_spin.setDecimals(2)
        self.wrap_angle_spin.setRange(0.0, 360.0)
        self.wrap_angle_spin.setSingleStep(1.0)
        self.wrap_angle_spin.setValue(0.0)
        self.wrap_angle_spin.setSuffix(" °")
        cfg.addWidget(QLabel("高张力侧"), 0, 0)
        cfg.addWidget(self.fric_high_combo, 0, 1)
        cfg.addWidget(QLabel("低张力侧"), 0, 2)
        cfg.addWidget(self.fric_low_combo, 0, 3)
        cfg.addWidget(self.fric_swap_btn, 0, 4)
        cfg.addWidget(QLabel("包角"), 1, 0)
        cfg.addWidget(self.wrap_angle_spin, 1, 1)
        cfg.setColumnStretch(5, 1)
        f_layout.addLayout(cfg)
        f_layout.addWidget(self.friction_plot, 1)

        self.mu_tab = QWidget()
        mu_layout = QVBoxLayout(self.mu_tab)
        mu_layout.setContentsMargins(0, 0, 0, 0)
        mu_cfg = QGridLayout()
        self.mu_high_combo = QComboBox()
        self.mu_low_combo = QComboBox()
        self.mu_swap_btn = QPushButton("互换")
        self.mu_wrap_angle_spin = QDoubleSpinBox()
        self.mu_wrap_angle_spin.setDecimals(2)
        self.mu_wrap_angle_spin.setRange(0.0, 360.0)
        self.mu_wrap_angle_spin.setSingleStep(1.0)
        self.mu_wrap_angle_spin.setValue(0.0)
        self.mu_wrap_angle_spin.setSuffix(" °")
        mu_cfg.addWidget(QLabel("高张力侧"), 0, 0)
        mu_cfg.addWidget(self.mu_high_combo, 0, 1)
        mu_cfg.addWidget(QLabel("低张力侧"), 0, 2)
        mu_cfg.addWidget(self.mu_low_combo, 0, 3)
        mu_cfg.addWidget(self.mu_swap_btn, 0, 4)
        mu_cfg.addWidget(QLabel("包角"), 1, 0)
        mu_cfg.addWidget(self.mu_wrap_angle_spin, 1, 1)
        mu_cfg.setColumnStretch(5, 1)
        mu_layout.addLayout(mu_cfg)
        mu_layout.addWidget(self.mu_plot, 1)

        self.plot_dock = QDockWidget("绘图窗口", self)
        try:
            self.plot_dock.setObjectName("dock_plot")
        except Exception:
            pass
        self.plot_dock.setAllowedAreas(Qt.BottomDockWidgetArea | Qt.TopDockWidgetArea | Qt.LeftDockWidgetArea | Qt.RightDockWidgetArea)
        plot_container = QWidget()
        plot_layout = QVBoxLayout(plot_container)
        plot_layout.setContentsMargins(6, 6, 6, 6)
        plot_layout.addWidget(self.plot_tabs, 1)
        self.plot_dock.setWidget(plot_container)
        self.addDockWidget(Qt.RightDockWidgetArea, self.plot_dock)

        self.fric_high_combo.currentIndexChanged.connect(self._on_friction_config_changed)
        self.fric_low_combo.currentIndexChanged.connect(self._on_friction_config_changed)
        self.wrap_angle_spin.valueChanged.connect(self._on_friction_config_changed)
        self.fric_swap_btn.clicked.connect(self._swap_friction_channels)
        self.mu_high_combo.currentIndexChanged.connect(self._on_mu_config_changed)
        self.mu_low_combo.currentIndexChanged.connect(self._on_mu_config_changed)
        self.mu_wrap_angle_spin.valueChanged.connect(self._on_mu_config_changed)
        self.mu_swap_btn.clicked.connect(self._swap_mu_channels)

        # ---- Comm Monitor Dock ----
        self.monitor_dock = QDockWidget("通讯监视窗口", self)
        try:
            self.monitor_dock.setObjectName('dock_monitor')
        except Exception:
            pass
        self.monitor_dock.setAllowedAreas(Qt.BottomDockWidgetArea | Qt.TopDockWidgetArea)
        self.monitor_text = QPlainTextEdit()
        self.monitor_text.setReadOnly(True)
        self.monitor_text.setUndoRedoEnabled(False)
        try:
            self.monitor_text.setMaximumBlockCount(2000)
        except Exception:
            pass

        mon_container = QWidget()
        mon_layout = QVBoxLayout(mon_container)
        mon_layout.setContentsMargins(6, 6, 6, 6)
        # display mode
        mon_mode_row = QHBoxLayout()
        mon_mode_row.addWidget(QLabel("显示模式"))
        self.monitor_mode_combo = QComboBox()
        self.monitor_mode_combo.addItem("HEX", "hex")
        self.monitor_mode_combo.addItem("文本(UTF-8)", "utf-8")
        self.monitor_mode_combo.addItem("文本(GBK)", "gbk")
        self.monitor_mode_combo.setCurrentIndex(1)
        self.monitor_mode_combo.currentIndexChanged.connect(lambda *_: self.schedule_monitor_render(full=True))
        mon_mode_row.addWidget(self.monitor_mode_combo)
        mon_mode_row.addStretch(1)
        mon_layout.addLayout(mon_mode_row)

        mon_layout.addWidget(self.monitor_text, 1)

        mon_btns = QHBoxLayout()
        self.mon_clear_btn = QPushButton("清空监视")
        self.mon_clear_btn.clicked.connect(lambda *_: self.clear_monitor())
        self.mon_save_btn = QPushButton("保存日志")
        self.mon_save_btn.clicked.connect(lambda *_: self.save_monitor_log())
        mon_btns.addWidget(self.mon_clear_btn)
        mon_btns.addWidget(self.mon_save_btn)
        mon_btns.addStretch(1)
        mon_layout.addLayout(mon_btns)

        self.monitor_dock.setWidget(mon_container)
        self.addDockWidget(Qt.BottomDockWidgetArea, self.monitor_dock)
        self.monitor_dock.visibilityChanged.connect(lambda vis: vis and (self.schedule_monitor_render(full=True) or True))

        # structured log storage for re-rendering in HEX/TEXT modes
        self._monitor_entries: List[dict] = []
        self._manual_entries: List[dict] = []
        self._motor_mon_entries: List[dict] = []

        # ---- UI throttle timers (avoid stutter at high frame rates) ----
        self._monitor_dirty = False
        self._manual_dirty = False
        self._monitor_timer = QTimer(self)
        self._monitor_timer.setInterval(50)
        self._monitor_timer.setSingleShot(True)
        self._monitor_timer.timeout.connect(self._flush_monitor_render)

        self._manual_timer = QTimer(self)
        self._manual_timer.setInterval(50)
        self._manual_timer.setSingleShot(True)
        self._manual_timer.timeout.connect(self._flush_manual_render)

        self._motor_mon_dirty = False
        self._motor_mon_timer = QTimer(self)
        self._motor_mon_timer.setInterval(50)
        self._motor_mon_timer.setSingleShot(True)
        self._motor_mon_timer.timeout.connect(self._flush_motor_monitor_render)

        # ---- Plot throttle (refresh-rate driven) ----
        self._plot_dirty = False
        self._plot_timer = QTimer(self)
        self._plot_timer.setInterval(16)  # will be updated by _on_plot_fps_changed()
        self._plot_timer.timeout.connect(self._flush_plot)
        # NOTE: plot timer starts only while acquiring (see _update_plot_timer_running)
        self._on_plot_fps_changed()

        self._last_xrange_update = 0.0
        self._last_yrange_update = 0.0

        # ---- Custom Send Dock (default hidden, show on right) ----
        self.custom_send_dock = QDockWidget("自定义串口发送", self)
        try:
            self.custom_send_dock.setObjectName('dock_custom_send')
        except Exception:
            pass
        self.custom_send_dock.setAllowedAreas(Qt.RightDockWidgetArea | Qt.LeftDockWidgetArea)

        send_container = QWidget()
        send_layout = QVBoxLayout(send_container)
        send_layout.setContentsMargins(8, 8, 8, 8)

        send_grid = QGridLayout()
        self.custom_send_port_combo = QComboBox()
        self.custom_send_line = QLineEdit()
        self.custom_send_line.setPlaceholderText("输入要发送的内容（文本或HEX：01 03 00 00 00 02）")
        self.custom_send_crlf_chk = QCheckBox("末尾添加 \\r\\n")
        self.custom_send_crlf_chk.setChecked(True)
        self.custom_send_btn = QPushButton("发送")

        send_grid.addWidget(QLabel("发送串口"), 0, 0)
        send_grid.addWidget(self.custom_send_port_combo, 0, 1)
        send_grid.addWidget(QLabel("发送内容"), 1, 0)
        send_grid.addWidget(self.custom_send_line, 1, 1)
        send_layout.addLayout(send_grid)

        # display mode
        mode_row = QHBoxLayout()
        mode_row.addWidget(QLabel("日志显示"))
        self.custom_send_mode_combo = QComboBox()
        self.custom_send_mode_combo.addItem("HEX", "hex")
        self.custom_send_mode_combo.addItem("文本(UTF-8)", "utf-8")
        self.custom_send_mode_combo.addItem("文本(GBK)", "gbk")
        self.custom_send_mode_combo.setCurrentIndex(1)
        self.custom_send_mode_combo.currentIndexChanged.connect(lambda *_: self.schedule_custom_send_render(full=True))
        mode_row.addWidget(self.custom_send_mode_combo)
        mode_row.addStretch(1)
        send_layout.addLayout(mode_row)

        send_row = QHBoxLayout()
        send_row.addWidget(self.custom_send_crlf_chk)
        send_row.addStretch(1)
        send_row.addWidget(self.custom_send_btn)
        send_layout.addLayout(send_row)

        tip = QLabel("提示：Enter 直接发送；可输入文本，或输入十六进制字节（如：01 03 00 00 00 02 / 010300000002）。")
        tip.setWordWrap(True)
        send_layout.addWidget(tip)

        # reply/listen area (filtered manual TX/RX for the selected opened port)
        send_layout.addWidget(QLabel("回复 / 日志"))
        self.custom_send_log = QPlainTextEdit()
        self.custom_send_log.setReadOnly(True)
        self.custom_send_log.setUndoRedoEnabled(False)
        try:
            self.custom_send_log.setMaximumBlockCount(1200)
        except Exception:
            pass
        self.custom_send_log.setPlaceholderText("这里会显示自定义发送对应的 TX/RX。")
        send_layout.addWidget(self.custom_send_log, 1)

        log_btns = QHBoxLayout()
        self.custom_send_clear_btn = QPushButton("清空")
        self.custom_send_clear_btn.clicked.connect(lambda *_: self.clear_custom_send_log())
        log_btns.addWidget(self.custom_send_clear_btn)
        log_btns.addStretch(1)
        send_layout.addLayout(log_btns)

        send_layout.addStretch(1)

        self.custom_send_dock.setWidget(send_container)
        self.addDockWidget(Qt.RightDockWidgetArea, self.custom_send_dock)
        self.custom_send_dock.hide()  # default closed

        # ---- Motor Control Dock (default hidden) ----
        self.motor_dock = QDockWidget("电机控制", self)
        try:
            self.motor_dock.setObjectName('dock_motor')
        except Exception:
            pass
        self.motor_dock.setAllowedAreas(Qt.RightDockWidgetArea | Qt.LeftDockWidgetArea)

        motor_container = QWidget()
        motor_layout = QVBoxLayout(motor_container)
        motor_layout.setContentsMargins(8, 8, 8, 8)

        # Enable/Disable
        en_row = QHBoxLayout()
        self.motor_enable_btn = QPushButton("使能")
        self.motor_disable_btn = QPushButton("去使能")
        self.motor_enable_lamp = QLabel()
        self.motor_enable_lamp.setFixedSize(14, 14)
        self._set_lamp_color(self.motor_enable_lamp, "#777777")
        en_row.addWidget(self.motor_enable_btn)
        en_row.addWidget(self.motor_disable_btn)
        en_row.addWidget(QLabel("使能灯"))
        en_row.addWidget(self.motor_enable_lamp)
        en_row.addStretch(1)
        motor_layout.addLayout(en_row)

        # Direction
        dir_row = QHBoxLayout()
        self.motor_forward_btn = QPushButton("正转")
        self.motor_backward_btn = QPushButton("反转")
        self.motor_dir_lamp = QLabel()
        self.motor_dir_lamp.setFixedSize(14, 14)
        self._set_lamp_color(self.motor_dir_lamp, "#777777")
        dir_row.addWidget(self.motor_forward_btn)
        dir_row.addWidget(self.motor_backward_btn)
        dir_row.addWidget(QLabel("转向灯"))
        dir_row.addWidget(self.motor_dir_lamp)
        dir_row.addStretch(1)
        motor_layout.addLayout(dir_row)
        # Mode selection
        mode_row = QHBoxLayout()
        self.motor_mode_tension_btn = QPushButton("张力模式")
        self.motor_mode_speed_btn = QPushButton("速度模式")
        self.motor_mode_tension_lamp = QLabel()
        self.motor_mode_tension_lamp.setFixedSize(14, 14)
        self._set_lamp_color(self.motor_mode_tension_lamp, "#777777")
        self.motor_mode_speed_lamp = QLabel()
        self.motor_mode_speed_lamp.setFixedSize(14, 14)
        self._set_lamp_color(self.motor_mode_speed_lamp, "#777777")
        mode_row.addWidget(self.motor_mode_tension_btn)
        mode_row.addWidget(self.motor_mode_speed_btn)
        mode_row.addWidget(QLabel("张力模式灯"))
        mode_row.addWidget(self.motor_mode_tension_lamp)
        mode_row.addWidget(QLabel("速度模式灯"))
        mode_row.addWidget(self.motor_mode_speed_lamp)
        mode_row.addStretch(1)
        motor_layout.addLayout(mode_row)



        # Speed control
        speed_row = QHBoxLayout()
        speed_row.addWidget(QLabel("转速(RPM)"))
        self.motor_speed_edit = QLineEdit()
        self.motor_speed_edit.setPlaceholderText("例如 100")
        self.motor_speed_btn = QPushButton("转速控制")
        speed_row.addWidget(self.motor_speed_edit, 1)
        speed_row.addWidget(self.motor_speed_btn)
        motor_layout.addLayout(speed_row)

        # Tension control
        tension_row = QHBoxLayout()
        tension_row.addWidget(QLabel("张力(g)"))
        self.motor_tension_edit = QLineEdit()
        self.motor_tension_edit.setPlaceholderText("例如 1")
        self.motor_tension_btn = QPushButton("张力控制")
        tension_row.addWidget(self.motor_tension_edit, 1)
        tension_row.addWidget(self.motor_tension_btn)
        motor_layout.addLayout(tension_row)

        # PID control
        pid_row = QHBoxLayout()
        pid_row.addWidget(QLabel("Kp"))
        self.motor_kp_edit = QLineEdit()
        self.motor_kp_edit.setPlaceholderText("Kp:例如0.1")
        pid_row.addWidget(self.motor_kp_edit)
        pid_row.addWidget(QLabel("Ki"))
        self.motor_ki_edit = QLineEdit()
        self.motor_ki_edit.setPlaceholderText("Ki:例如0.02")
        pid_row.addWidget(self.motor_ki_edit)
        pid_row.addWidget(QLabel("Kd"))
        self.motor_kd_edit = QLineEdit()
        self.motor_kd_edit.setPlaceholderText("Kd:例如0.005")
        pid_row.addWidget(self.motor_kd_edit)
        self.motor_pid_btn = QPushButton("PID设置")
        pid_row.addWidget(self.motor_pid_btn)
        motor_layout.addLayout(pid_row)

        # Emergency stop
        self.motor_estop_btn = QPushButton("急停")
        try:
            self.motor_estop_btn.setStyleSheet("background:#d9534f;color:white;font-weight:bold;")
        except Exception:
            pass
        try:
            self.motor_estop_btn.setFixedSize(160, 120)
        except Exception:
            pass
        estop_row = QHBoxLayout()
        estop_row.addStretch(1)
        estop_row.addWidget(self.motor_estop_btn)
        estop_row.addStretch(1)
        motor_layout.addLayout(estop_row)

        # TX monitor for motor control
        motor_mon_group = QGroupBox("发送串口监视")
        motor_mon_layout = QVBoxLayout(motor_mon_group)
        motor_mon_layout.setContentsMargins(6, 6, 6, 6)
        motor_mon_mode_row = QHBoxLayout()
        motor_mon_mode_row.addWidget(QLabel("显示方式"))
        self.motor_mon_mode_combo = QComboBox()
        self.motor_mon_mode_combo.addItem("文本(UTF-8)", "utf-8")
        self.motor_mon_mode_combo.addItem("HEX", "hex")
        self.motor_mon_mode_combo.addItem("文本(GBK)", "gbk")
        self.motor_mon_mode_combo.setCurrentIndex(0)
        self.motor_mon_mode_combo.currentIndexChanged.connect(lambda *_: self.schedule_motor_monitor_render(full=True))
        motor_mon_mode_row.addWidget(self.motor_mon_mode_combo)
        motor_mon_mode_row.addStretch(1)
        self.motor_mon_clear_btn = QPushButton("清空")
        self.motor_mon_clear_btn.clicked.connect(self.clear_motor_monitor)
        motor_mon_mode_row.addWidget(self.motor_mon_clear_btn)
        motor_mon_layout.addLayout(motor_mon_mode_row)
        self.motor_mon_text = QPlainTextEdit()
        self.motor_mon_text.setReadOnly(True)
        self.motor_mon_text.setUndoRedoEnabled(False)
        try:
            self.motor_mon_text.setMaximumBlockCount(2000)
        except Exception:
            pass
        self.motor_mon_text.setPlaceholderText("这里显示发送串口返回的数据。")
        motor_mon_layout.addWidget(self.motor_mon_text, 1)
        motor_layout.addWidget(motor_mon_group, 1)


        self.motor_dock.setWidget(motor_container)
        self.addDockWidget(Qt.RightDockWidgetArea, self.motor_dock)
        self.motor_dock.hide()  # default hidden
        self.motor_dock.visibilityChanged.connect(lambda vis: vis and (self.schedule_motor_monitor_render(full=True) or True))

        self.motor_enable_btn.clicked.connect(lambda *_: self.on_motor_enable())
        self.motor_disable_btn.clicked.connect(lambda *_: self.on_motor_disable())
        self.motor_forward_btn.clicked.connect(lambda *_: self.on_motor_forward())
        self.motor_backward_btn.clicked.connect(lambda *_: self.on_motor_backward())
        self.motor_speed_btn.clicked.connect(lambda *_: self.on_motor_speed())
        self.motor_tension_btn.clicked.connect(lambda *_: self.on_motor_tension())
        self.motor_pid_btn.clicked.connect(lambda *_: self.on_motor_pid())
        self.motor_estop_btn.clicked.connect(lambda *_: self.on_motor_estop())
        self.motor_mode_tension_btn.clicked.connect(lambda *_: self.on_motor_mode_tension())
        self.motor_mode_speed_btn.clicked.connect(lambda *_: self.on_motor_mode_speed())
        self.motor_mode = None

        self.custom_send_btn.clicked.connect(lambda *_: self.send_custom_serial())
        self.custom_send_line.returnPressed.connect(self.send_custom_serial)
        self.custom_send_dock.visibilityChanged.connect(lambda vis: vis and (self.update_custom_send_ports() or True) and (self.schedule_custom_send_render(full=True) or True))
        self.custom_send_port_combo.currentIndexChanged.connect(lambda *_: self.schedule_custom_send_render(full=True))

        # ---- Workspace menu (show/hide panels) ----
        ws_menu = self.menuBar().addMenu("工作区")

        act_serial = ws_menu.addAction("串口配置")
        act_serial.setCheckable(True)
        act_serial.setChecked(True)
        act_serial.toggled.connect(serial_box.setVisible)

        act_modbus = ws_menu.addAction("Modbus 配置")
        act_modbus.setCheckable(True)
        act_modbus.setChecked(True)
        act_modbus.toggled.connect(modbus_box.setVisible)

        act_plotset = ws_menu.addAction("绘图设置")
        act_plotset.setCheckable(True)
        act_plotset.setChecked(True)
        act_plotset.toggled.connect(plot_box.setVisible)

        act_channels = ws_menu.addAction("通道配置")
        act_channels.setCheckable(True)
        act_channels.setChecked(True)
        act_channels.toggled.connect(ch_box.setVisible)

        ws_menu.addSeparator()
        # Dock 自带的切换 Action
        ws_menu.addAction(self.monitor_dock.toggleViewAction())
        ws_menu.addAction(self.custom_send_dock.toggleViewAction())

        # Monitor enable switches
        self.mon_rx_chk.toggled.connect(self.schedule_monitor_render)
        self.mon_tx_chk.toggled.connect(self.on_tx_monitor_toggled)

        # ---- Control menu ----
        ctrl_menu = self.menuBar().addMenu("控制")
        act_motor = ctrl_menu.addAction("电机控制")
        act_motor.triggered.connect(self.open_motor_control)


        # ---- Plot window menu ----
        plot_menu = self.menuBar().addMenu("绘图窗口")
        self.act_plot_window = plot_menu.addAction("显示绘图窗口")
        self.act_plot_window.setCheckable(True)
        self.act_plot_window.setChecked(True)
        self.act_plot_window.toggled.connect(lambda on: self.plot_dock.setVisible(on))
        self.plot_dock.visibilityChanged.connect(lambda vis: self.act_plot_window.setChecked(vis))

        self.act_friction_plot = plot_menu.addAction("摩擦力绘图窗口")
        self.act_friction_plot.setCheckable(True)
        self.act_friction_plot.setChecked(False)
        self.act_friction_plot.toggled.connect(lambda on: self._set_plot_tab_visible(self.friction_tab, "摩擦力", on))

        self.act_mu_plot = plot_menu.addAction("摩擦系数绘图窗口")
        self.act_mu_plot.setCheckable(True)
        self.act_mu_plot.setChecked(False)
        self.act_mu_plot.toggled.connect(lambda on: self._set_plot_tab_visible(self.mu_tab, "摩擦系数", on))

        # ---- Serial simulator menu ----
        sim_menu = self.menuBar().addMenu('串口仿真')
        act_sim = sim_menu.addAction('打开仿真串口界面')
        self.sim_manager = SerialSimManagerWindow(self)
        self.sim_manager.hide()
        act_sim.triggered.connect(self.open_simulator)
        self.sim_manager.ports_changed.connect(self.refresh_ports)


        # Init
        self.refresh_ports()
        # 默认两通道：两个 int16（01 03 00 00 00 02 ...）
        self.add_channel_row(default_name="CH1", default_addr=0, default_dtype="int16")
        self.add_channel_row(default_name="CH2", default_addr=1, default_dtype="int16")
        self._refresh_friction_channel_options()

        # Keep layout stable (no left-panel width jitter) and restore last workspace state.
        self._apply_stable_widget_sizing()

        # Restore last workspace state only after the window is actually shown.
        # Doing it too early (during __init__) may lead to incomplete first layout on some systems.
        self._restored_once = False

    def showEvent(self, e):
        super().showEvent(e)
        if getattr(self, "_restored_once", False):
            return
        self._restored_once = True
        QTimer.singleShot(0, self._restore_after_show)
    def _restore_after_show(self):
        self._restore_window_layout()

        # Fix: saved window position may be partially off-screen (negative Y, etc.)
        self._ensure_frame_on_screen()
        QTimer.singleShot(50, self._ensure_frame_on_screen)
        QTimer.singleShot(180, self._ensure_frame_on_screen)

        # Force a couple of layout/paint passes.
        # On some systems (Windows + high DPI, and/or OpenGL-backed widgets), the very first
        # frame may not fully lay out/paint until a resizeEvent happens (e.g. user drags border).
        # We emulate that once, without changing the visible size.
        self._force_first_layout_pass()
        QTimer.singleShot(0, self._force_first_layout_pass)

    def _force_first_layout_pass(self):
        # Activate central layout
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

        # Stabilize splitter geometry
        try:
            if hasattr(self, "main_splitter") and self.main_splitter is not None:
                try:
                    self.main_splitter.updateGeometry()
                except Exception:
                    pass
                # Re-apply current sizes to force internal recompute
                self.main_splitter.setSizes(self.main_splitter.sizes())
                # Guard against a bad restored splitter state (e.g. one side nearly collapsed).
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

        # Flush pending events once (first-show only)
        try:
            QApplication.processEvents()
        except Exception:
            pass

        # Nudge window size (triggers resizeEvent like a manual border drag)
        try:
            is_max = getattr(self, "isMaximized", lambda: False)()
            is_full = getattr(self, "isFullScreen", lambda: False)()
            if not is_max and not is_full:
                w, h = int(self.width()), int(self.height())
                self.resize(w + 1, h + 1)
                self.resize(w, h)
        except Exception:
            pass

        # Help plotting widget settle (pyqtgraph / OpenGL)
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

        # Ensure the restored window frame is fully visible on the current screen
        # (fixes cases where QSettings restored a negative Y, etc.)
        self._ensure_frame_on_screen()

    def _ensure_frame_on_screen(self):
        """Keep the window frame inside the current screen's available area.

        This fixes cases where a previously saved QSettings window position is restored with
        a negative Y (top clipped) or otherwise partially off-screen. We clamp using the
        *frameGeometry* (including title bar) rather than the client geometry.
        """
        try:
            # Don't interfere with maximized/fullscreen states
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

            # If it's totally outside, re-center
            if (x + w) < (avail.left() + 20) or x > (avail.left() + avail.width() - 20) or (y + h) < (avail.top() + 20) or y > (avail.top() + avail.height() - 20):
                new_x = int(avail.left() + max(0, (avail.width() - w) // 2))
                new_y = int(avail.top() + max(0, (avail.height() - h) // 2))
                self.move(new_x, new_y)
                return

            # Clamp into available rect
            max_x = int(avail.left() + max(0, avail.width() - w))
            max_y = int(avail.top() + max(0, avail.height() - h))
            new_x = min(max(x, int(avail.left())), max_x)
            new_y = min(max(y, int(avail.top())), max_y)

            if new_x != x or new_y != y:
                self.move(new_x, new_y)
        except Exception:
            pass

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


    def open_motor_control(self):
        if not hasattr(self, 'motor_dock') or self.motor_dock is None:
            return
        self.motor_dock.show()
        try:
            self.motor_dock.raise_()
            self.motor_dock.activateWindow()
        except Exception:
            pass

    def _apply_stable_widget_sizing(self):
        """Prevent left-panel width jitter caused by long/short combo texts.

        This keeps the current workspace layout stable when clicking connect/refresh/etc.
        """
        combos = [
            getattr(self, 'port_combo', None),
            getattr(self, 'tx_port_combo', None),
            getattr(self, 'custom_send_port_combo', None),
        ]
        for cb in combos:
            if cb is None:
                continue
            # Prefer policy that does NOT resize to current text.
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
        """Clamp geometry to current screen's available area.

        This avoids Qt warnings like:
        QWindowsWindow::setGeometry: Unable to set geometry ...
        when restoring a window size larger than the current available screen
        area (taskbar, DPI scaling, monitor change, etc.).
        """
        try:
            x = int(x); y = int(y); w = int(w); h = int(h)
        except Exception:
            return

        # If maximized/fullscreen, don't fight window manager.
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
        """Clamp current window geometry to be visible on some screen."""
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

        # --- Restore main window rect (preferred) ---
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
            # Backward-compat: fall back to Qt's saveGeometry/restoreGeometry
            try:
                geom = s.value('main/geometry')
                if geom:
                    self.restoreGeometry(geom)
            except Exception:
                pass
            # Clamp in case the restored geometry doesn't fit current screen/DPI
            self._ensure_window_on_screen()

        # --- Restore docks/tool state ---
        try:
            state = s.value('main/state')
            if state:
                self.restoreState(state)
        except Exception:
            pass

        # --- Restore splitter sizes ---
        try:
            sp = s.value('main/splitter')
            if sp and hasattr(self, 'main_splitter'):
                self.main_splitter.restoreState(sp)
        except Exception:
            pass

        # Restore window maximized state (after geometry)
        try:
            ws = s.value('main/wstate')
            ws_i = int(ws) if ws is not None else 0
            if ws_i & int(Qt.WindowMaximized):
                self.setWindowState(self.windowState() | Qt.WindowMaximized)
        except Exception:
            pass

        # Clamp again after restoreState (dock/min-size may change)
        self._ensure_window_on_screen()

        # First run fallback: give the left panel a reasonable width.
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


    # ---------- UI throttling helpers ----------
    def schedule_monitor_render(self, full: bool = False):
        """Throttle comm-monitor UI updates.

        full=True forces a full re-render (e.g. display mode changed / panel shown).
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
        # if panel hidden, defer render to when it becomes visible
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

        for combo in [self.fric_high_combo, self.fric_low_combo, getattr(self, "mu_high_combo", None), getattr(self, "mu_low_combo", None)]:
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

        # Restore selection if possible
        try:
            if cur_high and cur_high in names:
                self.fric_high_combo.setCurrentText(cur_high)
            if cur_low and cur_low in names:
                self.fric_low_combo.setCurrentText(cur_low)
            if cur_high_mu and cur_high_mu in names and hasattr(self, "mu_high_combo"):
                self.mu_high_combo.setCurrentText(cur_high_mu)
            if cur_low_mu and cur_low_mu in names and hasattr(self, "mu_low_combo"):
                self.mu_low_combo.setCurrentText(cur_low_mu)
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

        self._sync_mu_from_fric()
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

    def _update_friction_buffers_at_index(self, idx: int, row: dict):
        if self._fric_buf is None or self._mu_buf is None:
            return
        high_name = (getattr(self, "_fric_high_name", "") or "").strip()
        low_name = (getattr(self, "_fric_low_name", "") or "").strip()
        if (not high_name) or (not low_name):
            fric, mu = None, None
        else:
            fric, mu = self._calc_fric_mu(row.get(high_name), row.get(low_name))
        if np is not None:
            try:
                self._fric_buf[idx] = (np.nan if fric is None else float(fric))
                self._mu_buf[idx] = (np.nan if mu is None else float(mu))
            except Exception:
                pass
        else:
            self._fric_buf[idx] = fric
            self._mu_buf[idx] = mu

    def _recalc_friction_buffers(self):
        size = int(getattr(self, "_buf_size", 0) or 0)
        if size <= 0 or self._fric_buf is None or self._mu_buf is None:
            return
        high_name = (getattr(self, "_fric_high_name", "") or "").strip()
        low_name = (getattr(self, "_fric_low_name", "") or "").strip()
        high_buf = self._val_buf_by_channel.get(high_name) if high_name else None
        low_buf = self._val_buf_by_channel.get(low_name) if low_name else None
        for i in range(size):
            if high_buf is None or low_buf is None:
                fric, mu = None, None
            else:
                try:
                    hv = high_buf[i]
                    lv = low_buf[i]
                except Exception:
                    hv = None
                    lv = None
                fric, mu = self._calc_fric_mu(hv, lv)
            if np is not None:
                try:
                    self._fric_buf[i] = (np.nan if fric is None else float(fric))
                    self._mu_buf[i] = (np.nan if mu is None else float(mu))
                except Exception:
                    pass
            else:
                self._fric_buf[i] = fric
                self._mu_buf[i] = mu

    def _update_friction_plots(self, xs, idx: int, full: bool, count: int, scroll_live: bool, x_left: float, x_right: float):
        if xs is None or self._fric_buf is None or self._mu_buf is None:
            return
        size = int(getattr(self, "_buf_size", 0) or 0)
        if size <= 0:
            return

        # X range sync
        if scroll_live:
            try:
                self.friction_plot.setXRange(x_left, x_right, padding=0.0)
            except Exception:
                pass
            try:
                self.mu_plot.setXRange(x_left, x_right, padding=0.0)
            except Exception:
                pass

        if np is not None:
            if full:
                first = size - idx
                if self._fric_plot_y is None or getattr(self._fric_plot_y, "shape", (0,))[0] != size:
                    self._fric_plot_y = np.empty(size, dtype=float)
                if self._mu_plot_y is None or getattr(self._mu_plot_y, "shape", (0,))[0] != size:
                    self._mu_plot_y = np.empty(size, dtype=float)
                self._fric_plot_y[:first] = self._fric_buf[idx:]
                self._fric_plot_y[first:] = self._fric_buf[:idx]
                self._mu_plot_y[:first] = self._mu_buf[idx:]
                self._mu_plot_y[first:] = self._mu_buf[:idx]
                ys_fric = self._fric_plot_y
                ys_mu = self._mu_plot_y
            else:
                ys_fric = self._fric_buf[:count]
                ys_mu = self._mu_buf[:count]
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
        else:
            if full:
                fric_raw = list(self._fric_buf[idx:]) + list(self._fric_buf[:idx])
                mu_raw = list(self._mu_buf[idx:]) + list(self._mu_buf[:idx])
            else:
                fric_raw = list(self._fric_buf[:count])
                mu_raw = list(self._mu_buf[:count])
            xs_f, ys_f = [], []
            xs_m, ys_m = [], []
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
            try:
                self.friction_curve.setData(xs_f, ys_f)
            except Exception:
                pass
            try:
                self.mu_curve.setData(xs_m, ys_m)
            except Exception:
                pass

        # Autoscale handling for derived plots
        try:
            auto = bool(self.autoscale_chk.isChecked()) if hasattr(self, "autoscale_chk") else True
        except Exception:
            auto = True
        try:
            self.friction_plot.enableAutoRange(axis="y", enable=auto)
            self.mu_plot.enableAutoRange(axis="y", enable=auto)
        except Exception:
            pass
    def _flush_plot(self):
        # Plot refresh is driven by timer (Hz). We always call update_plot(),
        # which will only re-upload curve data when new samples arrive, but
        # will keep X scrolling smoothly at the requested refresh rate.
        self.update_plot()


    def _on_plot_fps_changed(self, *args):
        """Apply plot refresh rate (Hz) to the plot timer."""
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

        # apply start/stop based on current state
        try:
            self._update_plot_timer_running()
        except Exception:
            pass

    def _update_plot_timer_running(self):
        """Start/stop plot timer based on acquisition state (save CPU, freeze scrolling)."""
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
        """Resize ring buffers when max points changes."""
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
        xs, ys_map, xs_wall = self._snapshot_ring(include_wall=True)
        self._alloc_ring_buffers(new_size, list(self.channel_names), keep_last=True, xs=xs, ys_map=ys_map, xs_wall=xs_wall)

    def _alloc_ring_buffers(self, size: int, channel_names: list, keep_last: bool = False, xs=None, ys_map=None, xs_wall=None):
        """Allocate ring buffers.

        When keep_last=True, copies the last min(len(xs), size) samples into the new buffer.
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
            self._fric_plot_y = np.empty(size, dtype=float)
            self._mu_plot_y = np.empty(size, dtype=float)
        else:
            self._fric_buf = [None] * size
            self._mu_buf = [None] * size
            self._fric_plot_y = None
            self._mu_plot_y = None

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

                # recompute friction buffers for preserved samples
                high_name = (getattr(self, "_fric_high_name", "") or "").strip()
                low_name = (getattr(self, "_fric_low_name", "") or "").strip()
                if high_name and low_name:
                    tail_high = (ys_map or {}).get(high_name, [])
                    tail_low = (ys_map or {}).get(low_name, [])
                    tail_high = tail_high[-k:] if tail_high else [None] * k
                    tail_low = tail_low[-k:] if tail_low else [None] * k
                    for j in range(k):
                        fric, mu = self._calc_fric_mu(tail_high[j], tail_low[j])
                        if np is not None:
                            try:
                                self._fric_buf[j] = (np.nan if fric is None else float(fric))
                                self._mu_buf[j] = (np.nan if mu is None else float(mu))
                            except Exception:
                                pass
                        else:
                            self._fric_buf[j] = fric
                            self._mu_buf[j] = mu
                else:
                    if np is not None:
                        try:
                            self._fric_buf[:k] = np.nan
                            self._mu_buf[:k] = np.nan
                        except Exception:
                            pass
                    else:
                        self._fric_buf[:k] = [None] * k
                        self._mu_buf[:k] = [None] * k
                self._buf_count = k
                self._buf_idx = k % size

    def _snapshot_ring(self, include_wall: bool = False):
        """Snapshot ring buffer into time-ordered python lists (for resize/export)."""
        count = int(getattr(self, '_buf_count', 0) or 0)
        size = int(getattr(self, '_buf_size', 0) or 0)
        if count <= 0 or size <= 0 or self._ts_buf is None:
            if include_wall:
                return [], {}, []
            return [], {}
        idx = int(getattr(self, '_buf_idx', 0) or 0)

        if count < size:
            # not wrapped
            if np is not None:
                xs = [float(x) for x in self._ts_buf[:count]]
            else:
                xs = list(self._ts_buf[:count])
        else:
            # wrapped: oldest at idx
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
        if include_wall:
            return xs, ys_map, xs_wall
        return xs, ys_map

    # ---------- monitor ----------
    def append_monitor(self, s: str):
        """Append a plain text info line (non-frame).

        NOTE: UI rendering is throttled to avoid stutter at high frame rates.
        """
        self._monitor_entries.append({"kind": "INFO", "data": b"", "tag": "", "note": str(s)})
        self.schedule_monitor_render()

    def _custom_send_current_tag(self) -> str:
        """Return the current filter tag (e.g. 'rx:COM3' / 'tx:COM4') for custom-send dock."""
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
        """Enable/disable TX port async RX monitoring tap.

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
        # no data
        return f"{prefix}: {note}" if prefix else str(note)

    def render_monitor(self, force_full: bool = False):
        mode = "hex"
        if hasattr(self, "monitor_mode_combo"):
            mode = self.monitor_mode_combo.currentData() or "hex"

        # Track mode/index for delta-append rendering
        last_mode = getattr(self, "_monitor_render_mode", None)
        render_idx = int(getattr(self, "_monitor_render_idx", 0) or 0)

        # If mode changed or forced, rebuild last N lines (rare path)
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

        # Delta append new lines
        if render_idx < len(self._monitor_entries):
            new_entries = self._monitor_entries[render_idx:]
            # Batch append to reduce UI updates
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

        # Always keep manual TX/RX entries for the custom-send dock
        is_manual = str(kind).startswith('TX_MANUAL') or str(kind).startswith('RX_MANUAL')
        if is_manual:
            self._manual_entries.append(e)
            if self.custom_send_dock.isVisible():
                self.schedule_custom_send_render()

            # cap memory for logs
            if len(self._manual_entries) > 6000:
                overflow = len(self._manual_entries) - 6000
                del self._manual_entries[:overflow]
                try:
                    self._manual_render_idx = max(0, int(getattr(self, '_manual_render_idx', 0)) - overflow)
                except Exception:
                    self._manual_render_idx = 0

        # Comm monitor filtering per-port (rx/tx listen checkboxes)
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



        # Motor TX monitor: only RX frames from tx port
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

    # ---------- status ----------
    def set_status(self, msg: str):
        self.status_label.setText(f"状态：{msg}")

    # ---------- ports ----------
    def refresh_ports(self):
        """Refresh physical COM ports + in-app simulated ports."""
        # Preserve current selections to avoid UI jump.
        cur_rx = self.port_combo.currentData()
        cur_tx = self.tx_port_combo.currentData()

        self.port_combo.clear()
        self.tx_port_combo.clear()

        items = []

        # Physical ports
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

        # Simulated ports
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
        # Restore selections if possible
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

        # keep custom-send dock port list in sync
        self.update_custom_send_ports()
    def update_custom_send_ports(self):
        """Only show ports that are already opened by this program."""
        if not hasattr(self, "custom_send_port_combo"):
            return

        combo = self.custom_send_port_combo
        combo.blockSignals(True)
        combo.clear()

        items = []
        if self.is_connected and self.worker is not None:
            # rx/modbus port is always connected when worker emits connected(True)
            rx_port = getattr(self.worker, "port", "")
            if rx_port:
                items.append((f"接收串口(Modbus)：{rx_port}", "rx"))

            # tx/output port may be enabled
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
        # don't clear input (方便连续修改/重复发送)
        self.custom_send_line.setFocus()

    # ---------- motor control ----------
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
        # Order: set mode -> tension -> set mode -> speed -> disable
        # Ensure mode is set before issuing F/Con commands.
        self._send_motor_cmd("ConMode 0")
        self._send_motor_cmd("F 0")
        self._send_motor_cmd("ConMode 1")
        self._send_motor_cmd("Con 0")
        self._send_motor_cmd("Disable")
        self.motor_mode = None
        self._set_motor_mode_lamps(None)
        self._set_lamp_color(self.motor_enable_lamp, "#777777")
        self._set_lamp_color(self.motor_dir_lamp, "#777777")

    def _set_serial_widgets_enabled(self, enabled: bool):
        for w in [
            self.port_combo, self.refresh_ports_btn, self.baud_combo,
            self.parity_combo, self.stopbits_combo, self.bytesize_combo, self.timeout_spin,
            self.rs485_mode_combo, self.pre_tx_spin, self.post_tx_spin,
            self.tx_port_combo, self.tx_baud_combo, self.enable_tx_chk, self.mon_rx_chk, self.mon_tx_chk
        ]:
            w.setEnabled(enabled)

    # ---------- channel table ----------
    def add_channel_row(self, default_name: str = "", default_addr: int = 0, default_dtype: str = "float32"):
        row = self.ch_table.rowCount()
        self.ch_table.insertRow(row)

        enabled_chk = QCheckBox()
        enabled_chk.setChecked(True)
        enabled_chk.setStyleSheet("margin-left:12px;")
        self.ch_table.setCellWidget(row, 0, enabled_chk)

        self.ch_table.setItem(row, 1, QTableWidgetItem(default_name or f"CH{row+1}"))
        self.ch_table.setItem(row, 2, QTableWidgetItem(str(default_addr)))

        dtype_combo = QComboBox()
        dtype_combo.addItems(list(DTYPE_INFO.keys()))
        dtype_combo.setCurrentText(default_dtype if default_dtype in DTYPE_INFO else "float32")
        self.ch_table.setCellWidget(row, 3, dtype_combo)

        byte_combo = QComboBox()
        byte_combo.addItems(["big", "little"])
        byte_combo.setCurrentText("big")
        self.ch_table.setCellWidget(row, 4, byte_combo)

        word_combo = QComboBox()
        word_combo.addItems(["big", "little"])
        word_combo.setCurrentText("big")
        self.ch_table.setCellWidget(row, 5, word_combo)

        self.ch_table.setItem(row, 6, QTableWidgetItem("-0.01"))
        self.ch_table.setItem(row, 7, QTableWidgetItem("N"))

        self._refresh_friction_channel_options()
    def delete_selected_rows(self):
        rows = sorted({idx.row() for idx in self.ch_table.selectedIndexes()}, reverse=True)
        for r in rows:
            self.ch_table.removeRow(r)

        self._refresh_friction_channel_options()
    def gather_channels(self) -> List[ChannelConfig]:
        channels: List[ChannelConfig] = []
        seen_names = set()
        unit_map: Dict[str, str] = {}
        for r in range(self.ch_table.rowCount()):
            enabled_widget = self.ch_table.cellWidget(r, 0)
            enabled = bool(enabled_widget.isChecked()) if enabled_widget else True
            name = (self.ch_table.item(r, 1).text() if self.ch_table.item(r, 1) else "").strip() or f"CH{r+1}"
            if name in seen_names:
                i = 2
                base = name
                while f"{base}_{i}" in seen_names:
                    i += 1
                name = f"{base}_{i}"
            seen_names.add(name)

            try:
                address = int((self.ch_table.item(r, 2).text() if self.ch_table.item(r, 2) else "0").strip())
            except Exception:
                address = 0

            dtype_combo = self.ch_table.cellWidget(r, 3)
            dtype = dtype_combo.currentText() if dtype_combo else "float32"

            byte_combo = self.ch_table.cellWidget(r, 4)
            byte_order = byte_combo.currentText() if byte_combo else "big"

            word_combo = self.ch_table.cellWidget(r, 5)
            word_order = word_combo.currentText() if word_combo else "big"

            try:
                scale = float((self.ch_table.item(r, 6).text() if self.ch_table.item(r, 6) else "1.0").strip())
            except Exception:
                scale = 1.0
            try:
                unit = (self.ch_table.item(r, 7).text() if self.ch_table.item(r, 7) else "").strip()
            except Exception:
                unit = ""
            unit_map[name] = unit

            channels.append(ChannelConfig(enabled=enabled, name=name, address=address, dtype=dtype,
                                         byte_order=byte_order, word_order=word_order, scale=scale))
        self._last_unit_map = unit_map
        return channels


    # ---------- plot/data ----------
    def clear_data(self):
        # reset ring buffers (size follows current max-points)
        try:
            size = int(self.max_points_spin.value())
        except Exception:
            size = int(getattr(self, '_buf_size', 100) or 100)

        self.channel_names.clear()
        self._alloc_ring_buffers(size, [], keep_last=False)

        # reset plot time base (relative seconds)
        self._t0_mono_ts = None
        self._last_sample_rel_ts = None
        self._last_sample_mono_ts = None

        # reset pause-compensation
        self._mono_pause_accum = 0.0
        self._mono_pause_start = None

        self.plot.clear()
        self.plot.addLegend()
        self.curves.clear()
        try:
            if hasattr(self, "friction_curve") and self.friction_curve is not None:
                self.friction_curve.setData([], [])
            if hasattr(self, "mu_curve") and self.mu_curve is not None:
                self.mu_curve.setData([], [])
        except Exception:
            pass
        self.set_status("已清空数据")

    def init_curves(self, channel_names: List[str]):
        self.plot.clear()
        self.plot.addLegend()
        self.curves.clear()

        # 1-7 通道配色：红 橙 黄 绿 青 蓝 紫（更高区分度）
        palette = [
            (220, 0, 0),      # red
            (255, 140, 0),    # orange
            (255, 200, 0),    # yellow (slightly darker on white)
            (0, 170, 0),      # green
            (0, 170, 170),    # cyan
            (0, 0, 220),      # blue
            (140, 0, 200),    # purple
        ]
        width = 2  # 线宽稍微粗一点

        for i, name in enumerate(channel_names):
            color = palette[i % len(palette)]
            pen = pg.mkPen(color=color, width=width)
            self.curves[name] = self.plot.plot([], [], name=name, pen=pen)
            item = self.curves.get(name)
            if item is not None:
                # Per-curve performance hints (safe across versions)
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
                # Some versions support skipping finite checks for speed
                try:
                    item.setSkipFiniteCheck(True)
                except Exception:
                    pass



    @Slot(float, dict)
    def on_data_ready(self, ts: float, row: dict):
        # Convert incoming timestamp to a smooth, relative monotonic time base.
        mono_now = time.monotonic()
        if self._t0_mono_ts is None:
            self._t0_mono_ts = float(mono_now)
        pause_accum = float(getattr(self, '_mono_pause_accum', 0.0) or 0.0)
        rel_ts = float(mono_now - float(self._t0_mono_ts) - pause_accum)
        if rel_ts < 0.0:
            rel_ts = 0.0
        self._last_sample_rel_ts = rel_ts
        self._last_sample_mono_ts = float(mono_now)
        try:
            wall_ts = float(ts) if ts is not None else time.time()
        except Exception:
            wall_ts = time.time()

        # Lazily init curves + buffers on first frame (兼容未点击开始采集时的数据)
        if not self.channel_names:
            self.channel_names = list(row.keys())
            self.init_curves(self.channel_names)
            try:
                size = int(self.max_points_spin.value())
            except Exception:
                size = int(getattr(self, '_buf_size', 100) or 100)
            self._alloc_ring_buffers(size, list(self.channel_names), keep_last=False)

        # Keep buffer size synced with UI
        try:
            want = int(self.max_points_spin.value())
        except Exception:
            want = int(getattr(self, '_buf_size', 0) or 0)
        if want and want != int(getattr(self, '_buf_size', 0) or 0):
            self._resize_ring_buffers(want)

        size = int(getattr(self, '_buf_size', 0) or 0)
        if size <= 0:
            return
        i = int(getattr(self, '_buf_idx', 0) or 0) % size

        # Append to ring buffer (array size == 当前窗口最大点数)
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
        else:
            self._ts_buf[i] = rel_ts
            if self._ts_wall_buf is not None:
                self._ts_wall_buf[i] = wall_ts
            for name in self.channel_names:
                self._val_buf_by_channel[name][i] = row.get(name, None)

        # update derived friction buffers
        self._update_friction_buffers_at_index(i, row)
        try:
            if getattr(self, "_log_db_path", ""):
                self._data_logger.append(wall_ts, row)
        except Exception:
            pass

        self._buf_idx = (i + 1) % size
        if int(getattr(self, '_buf_count', 0) or 0) < size:
            self._buf_count += 1

        self._plot_seq = int(getattr(self, '_plot_seq', 0) or 0) + 1
        self._plot_dirty = True


    def update_plot(self):
        """Update plot curves (buffered + 刷新率驱动)."""
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
        # Smooth X scrolling (live): drive the right edge by monotonic time.
        # When NOT acquiring (stopped/paused), freeze scrolling and do NOT
        # keep forcing XRange updates (so users can pan/zoom the last frame).
        scroll_live = bool(getattr(self, 'is_acquiring', False)) and (not bool(getattr(self, 'is_paused', False)))

        try:
            if scroll_live and self._t0_mono_ts is not None:
                pause_accum = float(getattr(self, '_mono_pause_accum', 0.0) or 0.0)
                now_rel = float(time.monotonic() - float(self._t0_mono_ts) - pause_accum)
            else:
                now_rel = float(self._last_sample_rel_ts) if self._last_sample_rel_ts is not None else 0.0
        except Exception:
            now_rel = float(self._last_sample_rel_ts) if self._last_sample_rel_ts is not None else 0.0

        # Visible window width in seconds: max_points * poll_interval
        try:
            poll_ms = int(self.poll_spin.value()) if hasattr(self, 'poll_spin') else 20
        except Exception:
            poll_ms = 20
        poll_s = max(0.001, float(poll_ms) / 1000.0)
        npts = int(min(count, size))
        span = max(0.02, max(1, npts - 1) * poll_s)
        x_left = now_rel - span
        x_right = now_rel

        # Fast path: if no new samples arrived, avoid re-uploading curve data.
        # Keep only X scrolling (smooth) while reducing CPU/GPU overhead.
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
            return

        # Prepare ordered X view only when uploading new curve data.
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

        # Prevent multiple repaints during one update (helps on Windows).
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
                        # Limit points sent to renderer when buffer is huge.
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
                                        # fallback: no decimation
                                        xs_use, ys_use = xs, ys

                        # Prefer skipping finite checks when supported
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
                # Fallback (no numpy)
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
            # Smooth scrolling: keep a fixed visible window ending at "now".
            # Only do this while acquiring; after stop/pause we freeze and let
            # the user inspect/pan without being overridden by the timer.
            if scroll_live:
                try:
                    self.plot.setXRange(x_left, x_right, padding=0.0)
                except Exception:
                    pass

            # Y range update with hysteresis to reduce jitter/flicker
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
                            # If new range is mostly inside current range, skip update.
                            if (new_min >= (cur_min + margin)) and (new_max <= (cur_max - margin)):
                                apply = False
                    except Exception:
                        pass

                    if apply:
                        self.plot.setYRange(new_min, new_max, padding=0.0)
                        self._last_yrange_update = now
        finally:
            self.plot.setUpdatesEnabled(True)
            # Request a single repaint after updating all curves.
            self.plot.update()

        try:
            self._update_friction_plots(xs, idx, full, count, scroll_live, x_left, x_right)
        except Exception:
            pass

        self._last_plotted_seq = int(getattr(self, "_plot_seq", 0) or 0)


    # ---------- connect/acquire ----------
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

        # reset plot data at start
        self.clear_data()
        # reset pause-compensation for timeline
        self._mono_pause_accum = 0.0
        self._mono_pause_start = None
        self.channel_names = [c.name for c in enabled_channels]
        self._log_units = [self._last_unit_map.get(c.name, "") for c in enabled_channels]
        self.init_curves(self.channel_names)
        # allocate ring buffer with current max points
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
            # pause
            self.worker.set_acquiring(False)
            self.is_acquiring = False
            self.is_paused = True
            self._mono_pause_start = time.monotonic()
            self.pause_btn.setText("继续")
            self.set_status("已暂停（保持连接）")
        else:
            # resume
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


    # ---------- data logging ----------
    def _start_data_logger(self, channel_names: List[str], channel_units: Optional[List[str]] = None):
        try:
            if not channel_names:
                return
            path = self._data_logger.start_session(channel_names, channel_units or [])
            self._log_db_path = path
            self._log_channels = list(channel_names)
            self._log_units = list(channel_units or [])
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

    def _db_has_data(self, path: str) -> bool:
        if not path:
            return False
        if not os.path.isfile(path):
            return False
        try:
            conn = sqlite3.connect(path)
            try:
                cur = conn.execute("SELECT 1 FROM data LIMIT 1")
                return cur.fetchone() is not None
            finally:
                conn.close()
        except Exception:
            return False

    def _format_export_time(self, wall_ts, rel_ts) -> str:
        try:
            if wall_ts is not None and math.isfinite(float(wall_ts)):
                return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(wall_ts)))
        except Exception:
            pass
        try:
            return f"{float(rel_ts):.3f}s"
        except Exception:
            return ""

    def _unit_label(self, unit: str) -> str:
        u = (unit or "").strip()
        if not u:
            return ""
        ul = u.lower()
        if ul == "g":
            return "g【克】"
        if ul == "n":
            return "N【牛】"
        if u in ("无量纲", "-"):
            return "无量纲"
        return f"{u}【单位】"

    def _export_xlsx_from_ring(self, path: str, xs, ys_map, xs_wall):
        wb = Workbook()
        ws_all = wb.active
        ws_all.title = "All"

        headers = ["Time"]
        units = list(getattr(self, "_log_units", []))
        for idx, name in enumerate(self.channel_names):
            unit = units[idx] if idx < len(units) else ""
            unit_label = self._unit_label(unit)
            headers.append(f"{name}({unit_label})" if unit_label else name)
        headers += ["摩擦力(N【牛】)", "摩擦系数"]
        for i, h in enumerate(headers, start=1):
            ws_all.cell(row=1, column=i, value=h)

        nrows = len(xs)
        hi_name = (getattr(self, "_fric_high_name", "") or "").strip()
        lo_name = (getattr(self, "_fric_low_name", "") or "").strip()

        for r, rel_ts in enumerate(xs, start=2):
            wall_ts = xs_wall[r - 2] if xs_wall and (r - 2) < len(xs_wall) else None
            t_str = self._format_export_time(wall_ts, rel_ts)
            ws_all.cell(row=r, column=1, value=t_str)

            row_vals = []
            col_idx = 2
            for c, name in enumerate(self.channel_names, start=2):
                col = ys_map.get(name, [])
                v = col[r - 2] if (r - 2) < len(col) else None
                row_vals.append(v)
                ws_all.cell(row=r, column=col_idx, value=v)
                col_idx += 1

            # friction / mu
            high_v = None
            low_v = None
            if hi_name and hi_name in self.channel_names:
                try:
                    high_v = row_vals[self.channel_names.index(hi_name)]
                except Exception:
                    high_v = None
            if lo_name and lo_name in self.channel_names:
                try:
                    low_v = row_vals[self.channel_names.index(lo_name)]
                except Exception:
                    low_v = None
            fric_n, mu = self._calc_fric_mu(high_v, low_v)
            ws_all.cell(row=r, column=col_idx, value=fric_n)
            ws_all.cell(row=r, column=col_idx + 1, value=mu)

        # per-channel sheets (small data only)
        for i, name in enumerate(self.channel_names):
            ws = wb.create_sheet(title=self._safe_sheet_name(name))
            ws.cell(row=1, column=1, value="Time")
            unit = units[i] if i < len(units) else ""
            unit_label = self._unit_label(unit)
            ws.cell(row=1, column=2, value=f"{name}({unit_label})" if unit_label else name)
            vals = ys_map.get(name, [])
            for r in range(nrows):
                wall_ts = xs_wall[r] if xs_wall and r < len(xs_wall) else None
                t_str = self._format_export_time(wall_ts, xs[r])
                ws.cell(row=r + 2, column=1, value=t_str)
                v = vals[r] if r < len(vals) else None
                ws.cell(row=r + 2, column=2, value=v)

        # friction sheet
        ws_f = wb.create_sheet(title="摩擦力")
        ws_f.cell(row=1, column=1, value="Time")
        ws_f.cell(row=1, column=2, value="摩擦力(N【牛】)")
        ws_mu = wb.create_sheet(title="摩擦系数")
        ws_mu.cell(row=1, column=1, value="Time")
        ws_mu.cell(row=1, column=2, value="摩擦系数")
        for r, rel_ts in enumerate(xs, start=2):
            wall_ts = xs_wall[r - 2] if xs_wall and (r - 2) < len(xs_wall) else None
            t_str = self._format_export_time(wall_ts, rel_ts)
            # recompute with current config
            fric_n = None
            mu = None
            if hi_name and lo_name and hi_name in ys_map and lo_name in ys_map:
                try:
                    hv = ys_map.get(hi_name, [])[r - 2]
                    lv = ys_map.get(lo_name, [])[r - 2]
                except Exception:
                    hv = None
                    lv = None
                fric_n, mu = self._calc_fric_mu(hv, lv)
            ws_f.cell(row=r, column=1, value=t_str)
            ws_f.cell(row=r, column=2, value=fric_n)
            ws_mu.cell(row=r, column=1, value=t_str)
            ws_mu.cell(row=r, column=2, value=mu)

        for ws in wb.worksheets:
            self._autosize_sheet(ws)

        wb.save(path)

    def _export_xlsx_from_db(self, db_path: str, path: str):
        # flush queued rows (best-effort)
        try:
            if self._data_logger:
                self._data_logger.flush(wait=True, timeout=3.0)
        except Exception:
            pass

        conn = sqlite3.connect(db_path)
        try:
            try:
                cur = conn.execute("SELECT idx, name, unit FROM channels ORDER BY idx")
                channel_rows = cur.fetchall()
                channel_names = [row[1] for row in channel_rows]
                channel_units = [row[2] if len(row) > 2 else "" for row in channel_rows]
            except Exception:
                cur = conn.execute("SELECT idx, name FROM channels ORDER BY idx")
                channel_rows = cur.fetchall()
                channel_names = [row[1] for row in channel_rows]
                channel_units = ["" for _ in channel_rows]
            n_ch = len(channel_names)
            col_names = [f"ch{i}" for i in range(n_ch)]
            cols_sql = ", ".join(["ts"] + col_names) if col_names else "ts"
            query = f"SELECT {cols_sql} FROM data ORDER BY id"

            wb = Workbook(write_only=True)
            ws_all = wb.create_sheet("All")
            headers = ["Time"]
            for idx, name in enumerate(channel_names):
                unit = channel_units[idx] if idx < len(channel_units) else ""
                unit_label = self._unit_label(unit)
                headers.append(f"{name}({unit_label})" if unit_label else name)
            headers += ["摩擦力(N【牛】)", "摩擦系数"]
            ws_all.append(headers)
            ws_f = wb.create_sheet("摩擦力")
            ws_f.append(["Time", "摩擦力(N【牛】)"])
            ws_mu = wb.create_sheet("摩擦系数")
            ws_mu.append(["Time", "摩擦系数"])

            hi_name = (getattr(self, "_fric_high_name", "") or "").strip()
            lo_name = (getattr(self, "_fric_low_name", "") or "").strip()
            name_to_idx = {name: i for i, name in enumerate(channel_names)}
            hi_idx = name_to_idx.get(hi_name, None)
            lo_idx = name_to_idx.get(lo_name, None)

            cur = conn.execute(query)
            while True:
                rows = cur.fetchmany(1000)
                if not rows:
                    break
                for row in rows:
                    ts_val = row[0]
                    vals = list(row[1:]) if n_ch > 0 else []
                    t_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(ts_val)))
                    high_v = vals[hi_idx] if (hi_idx is not None and hi_idx < len(vals)) else None
                    low_v = vals[lo_idx] if (lo_idx is not None and lo_idx < len(vals)) else None
                    row_out = [t_str]
                    for idx, name in enumerate(channel_names):
                        v = vals[idx] if idx < len(vals) else None
                        row_out.append(v)

                    fric_n, mu = self._calc_fric_mu(high_v, low_v)
                    row_out += [fric_n, mu]
                    ws_all.append(row_out)
                    ws_f.append([t_str, fric_n])
                    ws_mu.append([t_str, mu])

            wb.save(path)
        finally:
            conn.close()
        try:
            self._autosize_workbook(path)
        except Exception:
            pass

    # ---------- export ----------
    def save_xlsx(self):
        db_path = self._log_db_path if getattr(self, "_log_db_path", "") else ""
        use_db = self._db_has_data(db_path)

        xs = []
        ys_map = {}
        xs_wall = []
        if not use_db:
            xs, ys_map, xs_wall = self._snapshot_ring(include_wall=True)
            if not xs or not self.channel_names:
                QMessageBox.information(self, "提示", "当前没有可保存的数据。请先开始采集。")
                return

        path, _ = QFileDialog.getSaveFileName(self, "保存为 XLSX", "modbus_data.xlsx", "Excel Files (*.xlsx)")
        if not path:
            return
        if not path.lower().endswith(".xlsx"):
            path += ".xlsx"

        try:
            if use_db:
                self._export_xlsx_from_db(db_path, path)
            else:
                self._export_xlsx_from_ring(path, xs, ys_map, xs_wall)
            self.set_status(f"已保存：{path}")
        except Exception as e:
            QMessageBox.critical(self, "\u4fdd\u5b58\u5931\u8d25", f"\u4fdd\u5b58 xlsx \u5931\u8d25\uff1a\\n{e}")

    @staticmethod
    def _safe_sheet_name(name: str) -> str:
        bad = ['\\', '/', '*', '[', ']', ':', '?']
        s = "".join("_" if ch in bad else ch for ch in name).strip() or "Sheet"
        return s[:31]

    @staticmethod
    def _autosize_sheet(ws):
        max_rows = min(ws.max_row, 2)
        for col in range(1, ws.max_column + 1):
            max_len = 0
            for row in range(1, max_rows + 1):
                v = ws.cell(row=row, column=col).value
                if v is None:
                    continue
                s = str(v)
                # Count CJK wide chars as width=2 for better Excel column sizing
                width = 0
                for ch in s:
                    width += 2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1
                max_len = max(max_len, width)
            ws.column_dimensions[get_column_letter(col)].width = min(max(10, max_len + 2), 50)

    def _autosize_workbook(self, path: str):
        wb = load_workbook(path)
        try:
            for ws in wb.worksheets:
                self._autosize_sheet(ws)
            wb.save(path)
        finally:
            try:
                wb.close()
            except Exception:
                pass

    def closeEvent(self, event):
        # Persist workspace layout (dock positions / splitter sizes / window geometry)
        try:
            self._save_window_layout()
        except Exception:
            pass
        # Ensure serial threads are stopped before exit
        try:
            self.disconnect_serial()
        except Exception:
            pass
        super().closeEvent(event)


