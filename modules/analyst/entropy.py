"""Энтропия тиков (Волна 5, Этап 30, Срез №1) — датчик хаоса среды.

Хаос Шеннона по распределению направлений тиков в живом потоке
(утверждённый Срез №1 — единственный новый измеримый параметр):
- S → 1 — пила/хаос: ложные пробои, уверенность сценариев тухнет;
- S → 0 при зажатом диапазоне — кристаллизация (порядок перед ходом).

Карточка kind="entropy-state" — факт для Сценариста. Фактор с ручкой
(никаких «умножения на ноль»): уверенность × (1 − penalty × перебор).
Направление не угадываем — только хаос настоящего.
"""
import math
import time
from collections import deque

from core.module import Manifest, NeirModule
from modules._labels import PRICE, DRAFT


class TickEntropy(NeirModule):
    manifest = Manifest(name="entropy", title="Энтропия тиков",
                        priority=52, tick_timeout=10.0)

    async def on_start(self):
        c = self.kernel.cfg
        self.window_ticks = max(20, c.get_int("entropy", "window_ticks", 300))
        self.publish_every = c.get_float("entropy", "publish_sec", 15.0)
        self._ticks = deque(maxlen=self.window_ticks + 1)  # цены тиков
        self._last_tick_ms = 0        # метка последнего тика — для lineage
        self._last_publish = 0.0
        self.subscribe(PRICE, self._on_tick)
        self.every(self.publish_every, self._publish)
        self.log.info("Энтропия тиков включена: окно %d тиков, пульс "
                      "раз в %.0fс", self.window_ticks, self.publish_every)

    async def _on_tick(self, env):
        price = float(env.payload.get("price", 0.0))
        if price > 0:
            self._ticks.append(price)
            self._last_tick_ms = int(time.time() * 1000)

    def _entropy(self):
        """Шеннон по направлению тиков, нормировано на фактическое число
        классов → 0..1 (два класса чередуются = полный хаос = 1.0).
        None — тиков мало."""
        prices = list(self._ticks)
        if len(prices) < 20:
            return None
        ups = downs = flat = 0
        for i in range(1, len(prices)):
            d = prices[i] - prices[i - 1]
            if d > 0:
                ups += 1
            elif d < 0:
                downs += 1
            else:
                flat += 1
        total = ups + downs + flat
        s, classes = 0.0, 0
        for p in (ups / total, downs / total, flat / total):
            if p > 0:
                s -= p * math.log2(p)
                classes += 1
        return round(s / math.log2(classes), 3) if classes > 1 else 0.0

    async def _publish(self):
        now = time.monotonic()
        s = self._entropy()
        if s is None:
            return
        self.publish(DRAFT, {
            "kind": "entropy-state",
            "author": "entropy",
            "confidence": 0.9,
            "fields": {
                "entropy": s,
                "ticks": len(self._ticks) - 1,
                "state": ("хаос" if s > 0.7
                          else "порядок" if s < 0.3 else "смешанно"),
                "ts_ms": int(time.time() * 1000),
            },
            # НИТЬ (протокол lineage, Узел 5): окно тиков — по последнему тику
            "lineage": [f"ticks:{self._last_tick_ms}"],
        })