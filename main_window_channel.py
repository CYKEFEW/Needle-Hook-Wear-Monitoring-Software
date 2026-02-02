# -*- coding: utf-8 -*-
"""Channel table helpers for MainWindow."""

from typing import Dict, List

from qt_compat import QCheckBox, QComboBox, QTableWidgetItem

from modbus_utils import ChannelConfig, DTYPE_INFO


class ChannelMixin:
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


    # ---------- 绘图/数据 ----------
