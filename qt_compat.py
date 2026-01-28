# -*- coding: utf-8 -*-
"""Qt compatibility layer (PySide6 preferred, PyQt5 fallback)."""

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
        QDialog, QListWidget, QListWidgetItem, QAbstractItemView, QDialogButtonBox
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
        QDialog, QListWidget, QListWidgetItem, QAbstractItemView, QDialogButtonBox
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
    "QDialog", "QListWidget", "QListWidgetItem", "QAbstractItemView", "QDialogButtonBox",
]
