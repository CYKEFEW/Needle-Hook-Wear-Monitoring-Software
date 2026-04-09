# -*- coding: utf-8 -*-
"""App entry point."""

import os
import sys

from qt_compat import QApplication, QIcon
import pyqtgraph as pg
from main_window import MainWindow


def _resource_path(rel_path: str) -> str:
    base = getattr(sys, "_MEIPASS", None)
    if base:
        return os.path.join(base, rel_path)
    return os.path.join(os.path.abspath(os.path.dirname(__file__)), rel_path)


def main():
    # Light theme
    pg.setConfigOption("background", "w")
    pg.setConfigOption("foreground", "k")
    # Speed-focused config (Windows: big win for real-time plotting)
    # - antialias=False: avoids costly QPainter AA
    # - useOpenGL=True: lets pyqtgraph render curves via OpenGL when available
    try:
        pg.setConfigOptions(antialias=False, useOpenGL=True)
    except Exception:
        try:
            pg.setConfigOptions(antialias=False)
            pg.setConfigOption("useOpenGL", True)
        except Exception:
            pass

    app = QApplication(sys.argv)
    try:
        icon_path = _resource_path("app.ico")
        if os.path.isfile(icon_path):
            app.setWindowIcon(QIcon(icon_path))
    except Exception:
        pass
    w = MainWindow()
    try:
        icon_path = _resource_path("app.ico")
        if os.path.isfile(icon_path):
            w.setWindowIcon(QIcon(icon_path))
    except Exception:
        pass
    w.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
