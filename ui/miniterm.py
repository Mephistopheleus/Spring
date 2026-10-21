"""Мини-экран NeirOS-M: свечи 5м + цена + здоровье модулей.

Живёт в СВОЁМ потоке Qt (утверждено): только читает мост и рисует.
Все объекты Qt создаются внутри потока Qt.
"""
import matplotlib
matplotlib.use("Qt5Agg")

import numpy as np
from matplotlib.backends.backend_qt5agg import FigureCanvas
from matplotlib.figure import Figure
from PyQt5.QtCore import QTimer
from PyQt5.QtWidgets import QLabel, QVBoxLayout, QWidget

UP = "#0ecb81"
DOWN = "#f6465d"
BG = "#131722"
PANEL = "#1e222d"
GRID = "#363c4e"
TEXT = "#d1d4dc"
MUTED = "#9ba3b5"


class MiniTerm(QWidget):
    def __init__(self, bridge, symbol):
        super().__init__()
        self.bridge = bridge
        self.symbol = symbol
        self.setWindowTitle("NeirOS-M — рынок")
        self.setMinimumSize(900, 560)
        self.setStyleSheet(f"background-color: {BG}; color: {TEXT};")

        layout = QVBoxLayout(self)
        layout.setContentsMargins(6, 6, 6, 6)
        self.status = QLabel("Ждём данные...", self)
        self.status.setStyleSheet("font-size: 13px; font-weight: bold; padding: 4px;")
        layout.addWidget(self.status)

        self.fig = Figure(facecolor=BG, figsize=(9, 5))
        self.ax = self.fig.add_subplot(111)
        self.canvas = FigureCanvas(self.fig)
        layout.addWidget(self.canvas)

        self._timer = QTimer(self)
        self._timer.timeout.connect(self._redraw)
        self._timer.start(700)
        # Пользователь закрыл окно → сигнал ядру (bridge.closing)
        self.destroyed.connect(self._check_close)

    def _redraw(self):
        snap = self.bridge.snapshot()
        self.status.setText(snap.get("info") or "Ждём данные...")
        rows = snap.get("candles") or []
        if len(rows) < 2:
            return
        data = rows[-140:]
        o = np.array([r[1] for r in data])
        h = np.array([r[2] for r in data])
        l = np.array([r[3] for r in data])
        c = np.array([r[4] for r in data])
        x = np.arange(len(data))
        up = c >= o
        down = ~up

        self.ax.clear()
        self.ax.set_facecolor(PANEL)
        self.ax.grid(True, color=GRID, alpha=0.6, linewidth=0.6)
        self.ax.tick_params(colors=MUTED, labelsize=8)
        for spine in self.ax.spines.values():
            spine.set_color(GRID)
        self.ax.yaxis.tick_right()
        self.ax.vlines(x[up], l[up], h[up], color=UP, linewidth=1)
        self.ax.vlines(x[down], l[down], h[down], color=DOWN, linewidth=1)
        self.ax.bar(x[up], (c - o)[up], bottom=o[up], width=0.62, color=UP)
        self.ax.bar(x[down], (o - c)[down], bottom=c[down], width=0.62, color=DOWN)

        # Зоны-магниты из картины: зелёные тянут вверх, красные — вниз
        picture = snap.get("picture") or {}
        for zone in picture.get("zones", []):
            color = UP if zone.get("pull") == "up" else DOWN
            self.ax.axhspan(zone["low"], zone["high"], color=color, alpha=0.12)

        # Сценарий: вход, стоп, цели
        scenario = snap.get("scenario") or {}
        if scenario.get("entry"):
            self.ax.axhline(scenario["entry"], color="#e0e0e0",
                            linestyle=":", linewidth=1.0)
            self.ax.axhline(scenario["stop"], color=DOWN,
                            linestyle="--", linewidth=1.0)
            for t in scenario.get("targets", []):
                self.ax.axhline(t, color=UP, linestyle="--", linewidth=1.0)

        price = snap.get("price")
        if price:
            self.ax.axhline(price, color="#f0b90b", linestyle="--", linewidth=0.9)
        self.ax.set_xlim(-1, len(data) + 1)
        self.fig.subplots_adjust(left=0.03, right=0.94, top=0.97, bottom=0.06)
        self.canvas.draw_idle()

    def _check_close(self):
        """Пользователь закрыл окно — сигнал ядру: экран можно гасить."""
        self.bridge.closing = True
        self._timer.stop()
