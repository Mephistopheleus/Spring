"""Экран NeirOS-M — окно в СВОЁМ потоке Qt (утверждено: экран только смотрит).

Модуль живёт в «главном зале», читает шину и копирует данные в мост;
поток Qt лишь рисует. Команды пользователя позже пойдут личными
конвертами через шину.
"""
import asyncio
import collections
import sys
import threading
import time

from core.module import Manifest, NeirModule
from modules._labels import CANDLES, CANDLES_WARMUP, CARD, PRICE


class Screen(NeirModule):
    manifest = Manifest(name="screen", title="Экран (мини-терминал)",
                        requires=("candles",),
                        priority=90, init_timeout=60.0, stop_timeout=10.0)

    async def on_start(self):
        self._thread = None
        if not self.kernel.cfg.ui_enabled:
            self.log.info("Экран выключен настройкой [ui] enabled=false — модуль спит")
            return
        from ui.bridge import UiBridge
        from ui.miniterm import MiniTerm        # тяжёлый импорт — не тормозим старт

        self.bridge = UiBridge()
        self._rows = collections.deque(maxlen=900)   # 900 минутных = 15 часов
        self._last_price = 0.0
        self._last_price_ts = 0.0

        self.subscribe(CANDLES, self._on_candles)
        # ПРОГРЕВ (06.10): история публикуется в ФОНЕ Сборщика — батч
        # может прийти после нашего старта, один get_last не спасает.
        self.subscribe(f"{CANDLES_WARMUP}:{self.kernel.cfg.base_tf_sec}",
                       self._on_candles)
        self.subscribe(PRICE, self._on_price)
        self.subscribe(CARD, self._on_card_kind)   # картина и сценарий — на экран
        # Прогрев: история 5м с доски значений (начали позже Сборщика)
        warm = self.get_last(f"{CANDLES_WARMUP}:{self.kernel.cfg.base_tf_sec}")
        if warm is not None:
            await self._on_candles(warm)
        self.every(1.0, self._pull_health)

        self._thread = threading.Thread(target=self._qt_worker,
                                        args=(MiniTerm,), daemon=True)
        self._thread.start()
        self.log.info("Экран запускается в отдельном потоке Qt")

    def _qt_worker(self, window_cls):
        """Поток Qt. Следит за сигналом закрытия от ядра: как только
        bridge.closing = True — выходим из цикла Qt (иначе app.exec_()
        крутится вечно, а поток daemon — окно остаётся висеть поверх
        консоли после остановки ядра)."""
        try:
            from PyQt5.QtCore import QTimer
            from PyQt5.QtWidgets import QApplication
            app = QApplication.instance() or QApplication([sys.argv[0] if sys.argv else "neiros"])
            window = window_cls(self.bridge, self.kernel.cfg.symbol.upper())
            watcher = QTimer(window)
            watcher.timeout.connect(self._closing_pulse)
            watcher.start(200)
            window.show()
            app.exec_()
        except Exception:
            self.log.exception("Поток экрана упал")

    def _closing_pulse(self):
        """Пульс остановки: сигнал моста → выход из цикла Qt."""
        if not self.bridge.closing:
            return
        try:
            from PyQt5.QtWidgets import QApplication
            app = QApplication.instance()
            if app is not None:
                app.quit()
        except Exception:
            pass

    # ---------- приём данных (главный зал) ----------

    async def _on_candles(self, env):
        data = env.payload
        try:
            if int(data["tf_sec"]) != self.kernel.cfg.base_tf_sec:
                return
            rows = data["candles"]
        except (KeyError, TypeError):
            return
        if data.get("warmup"):
            self._rows.clear()
            for r in rows:
                self._rows.append(tuple(r))
        else:
            for r in rows:
                r = tuple(r)
                if self._rows and self._rows[-1][0] == r[0]:
                    self._rows[-1] = r
                elif not self._rows or r[0] > self._rows[-1][0]:
                    self._rows.append(r)
        self.bridge.update(candles=list(self._rows))

    async def _on_price(self, env):
        self._last_price = float(env.payload.get("price", 0.0))
        self._last_price_ts = time.monotonic()
        self.bridge.update(price=self._last_price)

    async def _on_card_kind(self, env):
        kind = env.payload.get("kind")
        if kind == "picture":
            self.bridge.update(picture=env.payload.get("fields") or {})
        elif kind == "scenario":
            self.bridge.update(scenario=env.payload.get("fields") or {})

    async def _pull_health(self):
        snap = self.kernel.health_snapshot()
        mods = "  ".join(f"{m['name']}={m['state'][:4]}(ош.{m['errors']})"
                         for m in snap["modules"])
        age = time.monotonic() - self._last_price_ts if self._last_price_ts else -1
        freshness = "цена живая" if 0 <= age < 5 else f"цена молчит {age:.0f}с"
        info = (f"{self.kernel.cfg.symbol.upper()}   цена: {self._last_price:.6f}   "
                f"({freshness})   |   {mods}")
        self.bridge.update(info=info)

    async def on_stop(self):
        if self._thread is None:                # экран был выключен настройкой
            return
        self.bridge.closing = True
        await asyncio.to_thread(self._thread.join, 8)
        self.log.info("Поток экрана завершён")
