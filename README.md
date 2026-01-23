# Needle Hook Wear Monitoring Software

针钩磨损上位机软件（Modbus RTU 串口）。支持多通道采集、实时绘图、通讯监视、串口仿真、电机控制等。

## 运行环境

- Python 3.8+
- 依赖：PySide6 / PyQt5、pyqtgraph、pyserial、openpyxl

安装依赖示例：

```bash
pip install PySide6 pyqtgraph pyserial openpyxl
```

## 运行

```bash
python main.py
```

入口逻辑在 `app.py`，`main.py` 仅做启动包装。

## 目录结构（按功能拆分）

- `main.py`：程序入口（调用 `app.main()`）
- `app.py`：应用启动与主题设置
- `main_window.py`：主界面与主要业务逻辑
- `worker.py`：Modbus RTU 采集/发送线程
- `modbus_utils.py`：Modbus 工具函数与通道配置结构
- `rs485.py`：RS485 方向控制逻辑
- `virtual_serial.py`：程序内虚拟串口（仿真）
- `sim_window.py`：串口仿真管理窗口与端口界面
- `qt_compat.py`：Qt 兼容层（PySide6 优先，PyQt5 兜底）

## 常见说明

- 绘图窗口为 Dock 形式，可在菜单中显示/隐藏不同绘图页。
- 串口仿真只在本程序内部生效，不会创建系统真实 COM 口。

---

如需补充更详细的使用说明、操作截图或协议文档，请告诉我。
