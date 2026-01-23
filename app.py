# -*- coding: utf-8 -*-
"""App entry point."""

import sys
import pyqtgraph as pg

from qt_compat import QApplication
from main_window import MainWindow


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
    w = MainWindow()
    w.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
