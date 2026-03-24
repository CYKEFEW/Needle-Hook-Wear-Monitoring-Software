# -*- coding: utf-8 -*-
"""Modbus 助手主界面窗口。"""

import os
from typing import Dict, List, Optional

import pyqtgraph as pg

from qt_compat import (
    Qt, QMainWindow, QWidget, QLabel, QComboBox, QPushButton, QLineEdit,
    QSpinBox, QDoubleSpinBox, QCheckBox, QHBoxLayout, QVBoxLayout, QGridLayout,
    QGroupBox, QTableWidget, QHeaderView, QDockWidget, QTabWidget, QPlainTextEdit,
    QSplitter, QTimer, QSettings,
)

from rs485 import Rs485CtrlMode
from worker import ModbusRtuWorker
from sim_window import SerialSimManagerWindow
from data_logger import DataLogger
from main_window_export import ExportMixin
from main_window_layout import LayoutMixin
from main_window_monitor import MonitorMixin
from main_window_plot import PlotMixin
from main_window_channel import ChannelMixin
from main_window_serial import SerialMixin
from main_window_motor import MotorMixin

class MainWindow(ExportMixin, LayoutMixin, MonitorMixin, PlotMixin, ChannelMixin, SerialMixin, MotorMixin, QMainWindow):

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

        # ---- 绘图环形缓冲区（大小 = 当前窗口最大点数） ----
        self._buf_size = 100
        self._buf_count = 0
        self._buf_idx = 0  # 下一次写入索引
        self._ts_buf = None  # numpy 数组或列表
        self._ts_wall_buf = None  # 墙钟时间秒（epoch）
        self._val_buf_by_channel: Dict[str, object] = {}  # 名称 -> np.ndarray 或列表
        self._plot_x = None  # 用于绘图的连续 x（numpy）
        self._plot_y_by_channel: Dict[str, object] = {}  # 名称 -> np.ndarray 或列表
        self._fric_buf = None
        self._mu_buf = None
        self._avg_buf = None
        self._fric_plot_y = None
        self._mu_plot_y = None
        self._avg_plot_y = None
        self._qf_buf = None
        self._plot_seq = 0

        self._last_plotted_seq = -1
        # 平滑滚动的时间基准（相对秒）。
        self._t0_mono_ts = None           # 首次采样时的 time.monotonic()
        self._last_sample_rel_ts = None   # 上次采样的相对秒
        self._last_sample_mono_ts = None  # 上次采样时的 time.monotonic()

        # 带暂停补偿的单调时间轴（恢复时不跳变）。
        self._mono_pause_accum = 0.0
        self._mono_pause_start = None
        self._settings = QSettings("ModbusAssistant", "ModbusAssistant")
        self._fric_high_name = ""
        self._fric_low_name = ""
        self._wrap_angle_deg = 0.0
        self._wrap_angle_rad = 0.0
        self._quality_flag_name = "质量标志"
        self._quality_flag_label = "质量标志（quality flag）【0为无效，1为有效】"
        self._quality_gap_pending: List[dict] = []
        self._quality_gap_hold_mode = False
        self._quality_gap_start_mono = None
        self._quality_gap_triggered = False
        self._last_valid_row: Optional[Dict[str, float]] = None
        self._last_tension_setpoint: Optional[float] = None
        self._quality_last_source = "mu"
        self._quality_syncing = False
        self._quality_ui_syncing = False
        self._quality_gap_timeout_s = 1.0
        self._data_logger = DataLogger(base_dir=os.path.join(os.getcwd(), "data_logs"))
        self._log_db_path = ""
        self._log_channels: List[str] = []
        self._log_units: List[str] = []
        self._last_unit_map: Dict[str, str] = {}

        root = QWidget()
        self.setCentralWidget(root)
        layout = QHBoxLayout(root)
        layout.setContentsMargins(0, 0, 0, 0)

        # 使用分割器，使左/右大小在控件启用状态或文本变化时保持稳定。
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

        # ---- 串口分组 ----
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

        # RS485 方向控制
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

        # 通讯监视器启用（rx/tx）
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

        # 连接/采集按钮
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

        # ---- Modbus 分组 ----
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

        # ---- 绘图分组 ----
        plot_box = QGroupBox("绘图设置")
        left.addWidget(plot_box)
        pgd = QGridLayout(plot_box)

        self.max_points_spin = QSpinBox()
        self.max_points_spin.setRange(10, 200000)
        self.max_points_spin.setValue(100)

        self.autoscale_chk = QCheckBox("Y轴自动缩放")
        self.autoscale_chk.setChecked(True)

        # 绘图更新做限频；设置变化时标记为脏
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
        self.save_db_btn = QPushButton("保存为标准 DB")
        self.save_db_btn.clicked.connect(lambda *_: self.save_standard_db())

        pgd.addWidget(QLabel("当前窗口最大点数"), 0, 0)
        pgd.addWidget(self.max_points_spin, 0, 1)
        pgd.addWidget(QLabel("绘图刷新率"), 1, 0)
        pgd.addWidget(self.plot_fps_spin, 1, 1)
        pgd.addWidget(self.autoscale_chk, 2, 0, 1, 2)
        pgd.addWidget(self.clear_btn, 3, 0)
        pgd.addWidget(self.save_btn, 3, 1)
        pgd.addWidget(self.save_db_btn, 4, 0, 1, 2)

        # ---- 通道分组 ----
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

        # ---- 绘图区域 ----
        # 使用数值时间轴（秒）以实现平滑滚动。
        # DateAxisItem 会将刻度对齐到“整洁”边界（常为 1s），这会
        # 看起来像是 x 轴每秒只移动一次。
        self.plot = pg.PlotWidget()
        # ---- 绘图性能选项 ----
        # 1) 将绘制裁剪到视窗，避免渲染屏外线段
        # 2) 数据密集时启用自动降采样（峰值模式）
        # 3) 保留渲染端点数上限作为安全兜底
        try:
            pi = self.plot.getPlotItem()
            try:
                pi.setClipToView(True)
            except Exception:
                pass
            try:
                # 不同 pyqtgraph 版本使用的参数名是 “mode” 或 “method”
                pi.setDownsampling(auto=True, mode="peak")
            except Exception:
                try:
                    pi.setDownsampling(auto=True, method="peak")
                except Exception:
                    pass
        except Exception:
            pass
        # 每条曲线发送到渲染器的点数上限（0=禁用）
        self._max_display_points = 6000

        self.plot.setLabel("bottom", "时间", units="s")
        self.plot.setLabel("left", "张力")
        self.plot.addLegend()
        self.plot.showGrid(x=True, y=True, alpha=0.25)

        # ---- 摩擦力曲线 ----
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

        # ---- 摩擦系数曲线 ----
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

        # ---- 平均张力曲线 ----
        self.avg_plot = pg.PlotWidget()
        try:
            pi = self.avg_plot.getPlotItem()
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
        self.avg_plot.setLabel("bottom", "时间", units="s")
        self.avg_plot.setLabel("left", "平均张力")
        self.avg_plot.addLegend()
        self.avg_plot.showGrid(x=True, y=True, alpha=0.25)
        self.avg_curve = self.avg_plot.plot([], [], name="平均张力", pen=pg.mkPen(color=(150, 70, 200), width=2))

        # ---- 绘图窗口（停靠面板） ----
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
        self.wrap_angle_spin.setValue(10.0)
        self.wrap_angle_spin.setSuffix(" °")
        self.rmin_spin = QDoubleSpinBox()
        self.rmin_spin.setDecimals(4)
        self.rmin_spin.setRange(0.0, 1000.0)
        self.rmin_spin.setSingleStep(0.01)
        self.rmin_spin.setValue(1.01)
        self.mu_max_spin = QDoubleSpinBox()
        self.mu_max_spin.setDecimals(4)
        self.mu_max_spin.setRange(0.0, 10.0)
        self.mu_max_spin.setSingleStep(0.01)
        self.mu_max_spin.setValue(0.5)
        self.rmax_spin = QDoubleSpinBox()
        self.rmax_spin.setDecimals(6)
        self.rmax_spin.setRange(0.0, 1e6)
        self.rmax_spin.setSingleStep(0.01)
        self.rmax_spin.setValue(1.0)
        self.rmax_formula_label = QLabel("Rmax=exp(μmax·θ)")
        self.qgap_spin = QDoubleSpinBox()
        self.qgap_spin.setDecimals(2)
        self.qgap_spin.setRange(0.05, 10.0)
        self.qgap_spin.setSingleStep(0.05)
        self.qgap_spin.setValue(1.0)
        self.qgap_spin.setSuffix(" s")
        cfg.addWidget(QLabel("高张力侧"), 0, 0)
        cfg.addWidget(self.fric_high_combo, 0, 1)
        cfg.addWidget(QLabel("低张力侧"), 0, 2)
        cfg.addWidget(self.fric_low_combo, 0, 3)
        cfg.addWidget(self.fric_swap_btn, 0, 4)
        cfg.addWidget(QLabel("包角"), 1, 0)
        cfg.addWidget(self.wrap_angle_spin, 1, 1)
        cfg.addWidget(QLabel("Rmin"), 2, 0)
        cfg.addWidget(self.rmin_spin, 2, 1)
        cfg.addWidget(QLabel("μmax"), 2, 2)
        cfg.addWidget(self.mu_max_spin, 2, 3)
        cfg.addWidget(QLabel("Rmax"), 3, 0)
        cfg.addWidget(self.rmax_spin, 3, 1)
        cfg.addWidget(self.rmax_formula_label, 3, 2, 1, 3)
        cfg.addWidget(QLabel("丢包/解析失败超时(s)"), 4, 0)
        cfg.addWidget(self.qgap_spin, 4, 1)
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
        self.mu_wrap_angle_spin.setValue(10.0)
        self.mu_wrap_angle_spin.setSuffix(" °")
        self.rmin_spin_mu = QDoubleSpinBox()
        self.rmin_spin_mu.setDecimals(4)
        self.rmin_spin_mu.setRange(0.0, 1000.0)
        self.rmin_spin_mu.setSingleStep(0.01)
        self.rmin_spin_mu.setValue(1.01)
        self.mu_max_spin_mu = QDoubleSpinBox()
        self.mu_max_spin_mu.setDecimals(4)
        self.mu_max_spin_mu.setRange(0.0, 10.0)
        self.mu_max_spin_mu.setSingleStep(0.01)
        self.mu_max_spin_mu.setValue(0.5)
        self.rmax_spin_mu = QDoubleSpinBox()
        self.rmax_spin_mu.setDecimals(6)
        self.rmax_spin_mu.setRange(0.0, 1e6)
        self.rmax_spin_mu.setSingleStep(0.01)
        self.rmax_spin_mu.setValue(1.0)
        self.rmax_formula_label_mu = QLabel("Rmax=exp(μmax·θ)")
        self.qgap_spin_mu = QDoubleSpinBox()
        self.qgap_spin_mu.setDecimals(2)
        self.qgap_spin_mu.setRange(0.05, 10.0)
        self.qgap_spin_mu.setSingleStep(0.05)
        self.qgap_spin_mu.setValue(1.0)
        self.qgap_spin_mu.setSuffix(" s")
        mu_cfg.addWidget(QLabel("高张力侧"), 0, 0)
        mu_cfg.addWidget(self.mu_high_combo, 0, 1)
        mu_cfg.addWidget(QLabel("低张力侧"), 0, 2)
        mu_cfg.addWidget(self.mu_low_combo, 0, 3)
        mu_cfg.addWidget(self.mu_swap_btn, 0, 4)
        mu_cfg.addWidget(QLabel("包角"), 1, 0)
        mu_cfg.addWidget(self.mu_wrap_angle_spin, 1, 1)
        mu_cfg.addWidget(QLabel("Rmin"), 2, 0)
        mu_cfg.addWidget(self.rmin_spin_mu, 2, 1)
        mu_cfg.addWidget(QLabel("μmax"), 2, 2)
        mu_cfg.addWidget(self.mu_max_spin_mu, 2, 3)
        mu_cfg.addWidget(QLabel("Rmax"), 3, 0)
        mu_cfg.addWidget(self.rmax_spin_mu, 3, 1)
        mu_cfg.addWidget(self.rmax_formula_label_mu, 3, 2, 1, 3)
        mu_cfg.addWidget(QLabel("丢包/解析失败超时(s)"), 4, 0)
        mu_cfg.addWidget(self.qgap_spin_mu, 4, 1)
        mu_cfg.setColumnStretch(5, 1)
        mu_layout.addLayout(mu_cfg)
        mu_layout.addWidget(self.mu_plot, 1)

        self.avg_tab = QWidget()
        avg_layout = QVBoxLayout(self.avg_tab)
        avg_layout.setContentsMargins(0, 0, 0, 0)
        avg_cfg = QGridLayout()
        self.avg_high_combo = QComboBox()
        self.avg_low_combo = QComboBox()
        self.avg_swap_btn = QPushButton("互换")
        avg_cfg.addWidget(QLabel("高张力侧"), 0, 0)
        avg_cfg.addWidget(self.avg_high_combo, 0, 1)
        avg_cfg.addWidget(QLabel("低张力侧"), 0, 2)
        avg_cfg.addWidget(self.avg_low_combo, 0, 3)
        avg_cfg.addWidget(self.avg_swap_btn, 0, 4)
        avg_cfg.setColumnStretch(5, 1)
        avg_layout.addLayout(avg_cfg)
        avg_layout.addWidget(self.avg_plot, 1)

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
        self.rmin_spin.valueChanged.connect(self._on_quality_rmin_changed)
        self.mu_max_spin.valueChanged.connect(self._on_quality_mu_max_changed)
        self.rmax_spin.valueChanged.connect(self._on_quality_rmax_changed)
        self.qgap_spin.valueChanged.connect(self._on_quality_gap_timeout_changed)
        self.fric_swap_btn.clicked.connect(self._swap_friction_channels)
        self.mu_high_combo.currentIndexChanged.connect(self._on_mu_config_changed)
        self.mu_low_combo.currentIndexChanged.connect(self._on_mu_config_changed)
        self.mu_wrap_angle_spin.valueChanged.connect(self._on_mu_config_changed)
        self.mu_swap_btn.clicked.connect(self._swap_mu_channels)
        self.rmin_spin_mu.valueChanged.connect(self._on_quality_rmin_changed_mu)
        self.mu_max_spin_mu.valueChanged.connect(self._on_quality_mu_max_changed_mu)
        self.rmax_spin_mu.valueChanged.connect(self._on_quality_rmax_changed_mu)
        self.qgap_spin_mu.valueChanged.connect(self._on_quality_gap_timeout_changed_mu)
        self.avg_high_combo.currentIndexChanged.connect(self._on_avg_config_changed)
        self.avg_low_combo.currentIndexChanged.connect(self._on_avg_config_changed)
        self.avg_swap_btn.clicked.connect(self._swap_avg_channels)

        # ---- 通讯监视器停靠面板 ----
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
        # 显示模式
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

        # 结构化日志缓存，用于 HEX/TEXT 模式重绘
        self._monitor_entries: List[dict] = []
        self._manual_entries: List[dict] = []
        self._motor_mon_entries: List[dict] = []

        # ---- UI 限频计时器（高帧率下避免卡顿） ----
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

        # ---- 绘图限频（由刷新率驱动） ----
        self._plot_dirty = False
        self._plot_timer = QTimer(self)
        self._plot_timer.setInterval(16)  # 将由 _on_plot_fps_changed() 更新
        self._plot_timer.timeout.connect(self._flush_plot)
        # 注意：绘图计时器仅在采集中启动（见 _update_plot_timer_running）
        self._on_plot_fps_changed()

        self._last_xrange_update = 0.0
        self._last_yrange_update = 0.0

        # ---- 自定义发送停靠面板（默认隐藏，显示在右侧） ----
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

        # 显示模式
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

        # 回显/监听区域（过滤所选已打开端口的手动 TX/RX）
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
        self.custom_send_dock.hide()  # 默认关闭

        # ---- 电机控制停靠面板（默认隐藏） ----
        self.motor_dock = QDockWidget("电机控制", self)
        try:
            self.motor_dock.setObjectName('dock_motor')
        except Exception:
            pass
        self.motor_dock.setAllowedAreas(Qt.RightDockWidgetArea | Qt.LeftDockWidgetArea)

        motor_container = QWidget()
        motor_layout = QVBoxLayout(motor_container)
        motor_layout.setContentsMargins(8, 8, 8, 8)

        # 启用/禁用
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

        # 方向
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
        # 模式选择
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

        # 速度控制
        speed_row = QHBoxLayout()
        speed_row.addWidget(QLabel("转速(RPM)"))
        self.motor_speed_edit = QLineEdit()
        self.motor_speed_edit.setPlaceholderText("例如 100")
        self.motor_speed_btn = QPushButton("转速控制")
        speed_row.addWidget(self.motor_speed_edit, 1)
        speed_row.addWidget(self.motor_speed_btn)
        motor_layout.addLayout(speed_row)

        # 张力控制
        tension_row = QHBoxLayout()
        tension_row.addWidget(QLabel("张力(N)"))
        self.motor_tension_edit = QLineEdit()
        self.motor_tension_edit.setPlaceholderText("例如 1")
        self.motor_tension_btn = QPushButton("张力控制")
        tension_row.addWidget(self.motor_tension_edit, 1)
        tension_row.addWidget(self.motor_tension_btn)
        motor_layout.addLayout(tension_row)

        # PID 控制
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

        # 急停
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

        # 电机控制的 TX 监视
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
        self.motor_dock.hide()  # 默认隐藏
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

        # ---- 工作区菜单（显示/隐藏面板） ----
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
        # 停靠面板自带的切换动作
        ws_menu.addAction(self.monitor_dock.toggleViewAction())
        ws_menu.addAction(self.custom_send_dock.toggleViewAction())

        # 监视器启用开关
        self.mon_rx_chk.toggled.connect(self.schedule_monitor_render)
        self.mon_tx_chk.toggled.connect(self.on_tx_monitor_toggled)

        # ---- 控制菜单 ----
        ctrl_menu = self.menuBar().addMenu("控制")
        act_motor = ctrl_menu.addAction("电机控制")
        act_motor.triggered.connect(self.open_motor_control)


        # ---- 绘图窗口菜单 ----
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

        self.act_avg_plot = plot_menu.addAction("平均张力绘图窗口")
        self.act_avg_plot.setCheckable(True)
        self.act_avg_plot.setChecked(False)
        self.act_avg_plot.toggled.connect(lambda on: self._set_plot_tab_visible(self.avg_tab, "平均张力", on))

        # ---- 历史数据菜单 ----
        self.hist_menu = self.menuBar().addMenu("历史数据")
        self._build_history_menu()

        # ---- 串口模拟器菜单 ----
        sim_menu = self.menuBar().addMenu('串口仿真')
        act_sim = sim_menu.addAction('打开仿真串口界面')
        self.sim_manager = SerialSimManagerWindow(self)
        self.sim_manager.hide()
        act_sim.triggered.connect(self.open_simulator)
        self.sim_manager.ports_changed.connect(self.refresh_ports)


        # 初始化
        self.refresh_ports()
        # 默认两通道：两个 int16（01 03 00 00 00 02 ...）
        self.add_channel_row(default_name="CH1", default_addr=0, default_dtype="int16")
        self.add_channel_row(default_name="CH2", default_addr=1, default_dtype="int16")
        self._refresh_friction_channel_options()

        # 保持布局稳定（左侧面板宽度不抖动）并恢复上次工作区状态。
        self._apply_stable_widget_sizing()

        # 仅在窗口真正显示后恢复上次工作区状态。
        # 过早执行（在 __init__ 期间）可能导致部分系统首帧布局不完整。
        self._restored_once = False

