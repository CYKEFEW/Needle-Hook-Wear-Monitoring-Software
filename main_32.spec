# -*- mode: python ; coding: utf-8 -*-

import struct


APP_NAME = "needle_hook_wear_monitor"


if struct.calcsize("P") * 8 != 32:
    raise SystemExit(
        "main_32.spec must be executed with 32-bit Python and 32-bit "
        "PyInstaller. The current interpreter is not 32-bit."
    )


a = Analysis(
    ["app.py"],
    pathex=["."],
    binaries=[],
    datas=[("app.ico", ".")],
    hiddenimports=["PySide6.QtOpenGL", "PySide6.QtOpenGLWidgets"],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=["PyQt5"],
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
