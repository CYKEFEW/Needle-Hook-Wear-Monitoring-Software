# -*- coding: utf-8 -*-
"""Qt compatibility layer (PySide6 preferred, PyQt5 fallback)."""

import os
import sys
from pathlib import Path


def _is_valid_dir(path_value: str) -> bool:
    return bool(path_value) and Path(path_value).is_dir()


def _candidate_qt_roots():
    root_dir = Path(__file__).resolve().parent
    prefixes = []
    for prefix in (Path(sys.prefix), Path(getattr(sys, "base_prefix", sys.prefix)), root_dir / ".venv"):
        if prefix not in prefixes:
            prefixes.append(prefix)

    for prefix in prefixes:
        site_packages = prefix / "Lib" / "site-packages"
        yield site_packages / "PySide6"
        yield site_packages / "PyQt5" / "Qt5"

    frozen_base = getattr(sys, "_MEIPASS", None)
    if frozen_base:
        frozen_base = Path(frozen_base)
        yield frozen_base / "PySide6"
        yield frozen_base / "PyQt5" / "Qt5"


def _prepare_qt_runtime():
    if sys.platform != "win32":
        return

    for qt_root in _candidate_qt_roots():
        plugins_dir = qt_root / "plugins"
        platforms_dir = plugins_dir / "platforms"
        if not (platforms_dir / "qwindows.dll").is_file():
            continue

        if not _is_valid_dir(os.environ.get("QT_PLUGIN_PATH", "")):
            os.environ["QT_PLUGIN_PATH"] = str(plugins_dir)
        if not _is_valid_dir(os.environ.get("QT_QPA_PLATFORM_PLUGIN_PATH", "")):
            os.environ["QT_QPA_PLATFORM_PLUGIN_PATH"] = str(platforms_dir)

        bin_dir = qt_root / "bin"
        if hasattr(os, "add_dll_directory") and bin_dir.is_dir():
            try:
                os.add_dll_directory(str(bin_dir))
            except OSError:
                pass
        return


_prepare_qt_runtime()

QT_LIB = "PySide6"
try:
    from PySide6.QtCore import Qt, QThread, Signal, Slot, QTimer, QSettings, QPoint
    from PySide6.QtGui import QTextCursor, QGuiApplication, QIcon
    from PySide6.QtWidgets import (
        QApplication, QMainWindow, QWidget, QLabel, QComboBox, QPushButton,
        QLineEdit,
        QSpinBox, QDoubleSpinBox, QCheckBox, QHBoxLayout, QVBoxLayout,
        QGridLayout, QGroupBox, QTableWidget, QTableWidgetItem, QMessageBox,
        QFileDialog, QHeaderView, QDockWidget, QTabWidget, QTextEdit, QPlainTextEdit, QSplitter, QSizePolicy,
        QDialog, QListWidget, QListWidgetItem, QAbstractItemView, QDialogButtonBox, QProgressBar
    )
except Exception:
    QT_LIB = "PyQt5"
    from PyQt5.QtCore import Qt, QThread, QTimer, QSettings, QPoint, pyqtSignal as Signal, pyqtSlot as Slot
    from PyQt5.QtGui import QTextCursor, QGuiApplication, QIcon
    from PyQt5.QtWidgets import (
        QApplication, QMainWindow, QWidget, QLabel, QComboBox, QPushButton,
        QLineEdit,
        QSpinBox, QDoubleSpinBox, QCheckBox, QHBoxLayout, QVBoxLayout,
        QGridLayout, QGroupBox, QTableWidget, QTableWidgetItem, QMessageBox,
        QFileDialog, QHeaderView, QDockWidget, QTabWidget, QTextEdit, QPlainTextEdit, QSplitter, QSizePolicy,
        QDialog, QListWidget, QListWidgetItem, QAbstractItemView, QDialogButtonBox, QProgressBar
    )

__all__ = [
    "QT_LIB",
    "Qt", "QThread", "Signal", "Slot", "QTimer", "QSettings", "QPoint",
    "QTextCursor", "QGuiApplication", "QIcon",
    "QApplication", "QMainWindow", "QWidget", "QLabel", "QComboBox", "QPushButton",
    "QLineEdit", "QSpinBox", "QDoubleSpinBox", "QCheckBox", "QHBoxLayout", "QVBoxLayout",
    "QGridLayout", "QGroupBox", "QTableWidget", "QTableWidgetItem", "QMessageBox",
    "QFileDialog", "QHeaderView", "QDockWidget", "QTabWidget", "QTextEdit", "QPlainTextEdit",
    "QSplitter", "QSizePolicy",
    "QDialog", "QListWidget", "QListWidgetItem", "QAbstractItemView", "QDialogButtonBox", "QProgressBar",
]
