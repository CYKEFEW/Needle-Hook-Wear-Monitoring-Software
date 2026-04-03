# -*- mode: python ; coding: utf-8 -*-

import os
import struct
from pathlib import Path

from PyInstaller.utils.hooks.qt import pyqt5_library_info


APP_NAME = "needle_hook_wear_monitor"


if struct.calcsize("P") * 8 != 32:
    raise SystemExit(
        "main_32.spec must be executed with 32-bit Python and 32-bit "
        "PyInstaller. The current interpreter is not 32-bit."
    )


def _fix_pyqt5_qt_paths():
    package_location = Path(pyqt5_library_info.package_location)
    qt_root = package_location / "Qt5"
    if not qt_root.is_dir():
        return

    path_overrides = {
        "ArchDataPath": qt_root,
        "BinariesPath": qt_root / "bin",
        "DataPath": qt_root,
        "DocumentationPath": qt_root / "doc",
        "ExamplesPath": qt_root / "examples",
        "HeadersPath": qt_root / "include",
        "ImportsPath": qt_root / "imports",
        "LibrariesPath": qt_root / "lib",
        "LibraryExecutablesPath": qt_root / "bin",
        "PluginsPath": qt_root / "plugins",
        "PrefixPath": qt_root,
        "Qml2ImportsPath": qt_root / "qml",
        "TestsPath": qt_root / "tests",
        "TranslationsPath": qt_root / "translations",
    }

    for key, value in path_overrides.items():
        pyqt5_library_info.location[key] = os.fspath(value)

    pyqt5_library_info.qt_lib_dir = Path(pyqt5_library_info.location["BinariesPath"]).resolve()


_fix_pyqt5_qt_paths()


a = Analysis(
    ["app.py"],
    pathex=["."],
    binaries=[],
    datas=[("app.ico", ".")],
    hiddenimports=["PyQt5.QtOpenGL"],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=["PySide6"],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name=f"{APP_NAME}_x86",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=["app.ico"],
    version_info={
        "version": "0.0.2",
    },
)
