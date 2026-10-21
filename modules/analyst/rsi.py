"""«RSI-датчик» (Этап 23) — перекупленность/перепроданность по пирамиде ТФ.

Архитектура (утверждено): индикатор считает АНАЛИЗАТОР, не Сценарист.
Модуль ведёт полотна закрытий по уровням ТФ (1м тактика + 4ч
стратегия — те же уровни, что у Регрессии), считает RSI(period) и
публикует карточку СУХИХ ФАКТОВ kind: "rsi-state":
- rsi_base / rsi_strat — RSI обеих нитей (0..100);
- pressure — давление −1..1: перепроданность (RSI ≤ lower) толкает
  цену вверх (+1), перекупленность (RSI ≥ upper) — вниз (−1);
- state — перекуплен / перепродан / нейтрально.

Сценарист учитывает факт с весом (config: [rsi] weight) и сроком
свежести (stale_sec). Модуль не голосует — только измеряет
(как вся Волна 2). RSI по простой средней (Cutler) — детерминирован.
"""
import time

from core.module import Manifest, NeirModule
from modules._labels import CANDLES, DRAFT


def rsi_value(closes, period):
    """RSI по простой средней (Cutler). Мало точек — None."""
    if len(closes) < period + 1:
        return None
    gains = losses = 0.0
    for i in range(-period, 0):
        d = closes[i] - closes[i - 1]
        gains += max(d, 0.0)
        losses += max(-d, 0.0)
    if losses <= 0:
        return 100.0
    if gains <= 0:
        return 0.0
    rs = (gains / period) / (losses / period)
    return 100.0 - 100.0 / (1.0 + rs)


class RsiState(NeirModule):
    manifest = Manifest(name="rsi", title="RSI-датчик (исчерпание хода)",
                        requires=("candles",), priority=44, tick_timeout=5.0)

    async def on_start(self):
        c = self.kernel.cfg
        self.period = c.rsi_period
        self.upper = c.rsi_upper
        self.lower = c.rsi_lower
        self.base = c.base_tf_sec
        self.strat = c.strategy_tf_sec
        self.closes = {tf: [] for tf in dict.fromkeys((self.base, self.strat))}
        self.last_ts = {tf: 0 for tf in self.closes}
        self._last_pub = None
        self.issued = 0
        self.subscribe(CANDLES, self._on_candles)
        self.log.info("RSI-датчик включён: период %d, уровни %d/%d, "
                      "ТФ %s", self.period, self.upper, self.lower,
                      list(self.closes))

    async def on_stop(self):
        self.log.info("Итог: карточек RSI %d", self.issued)

    async def _on_candles(self, env):
        data = env.payload
        try:
            tf = int(data.get("tf_sec", 0))
        except (TypeError, ValueError):
            return
        if tf not in self.closes or not data.get("closed"):
            return
        arr = self.closes[tf]
        changed = False
        for r in data.get("candles") or []:
            try:
                ts, px = int(r[0]), float(r[4])
            except (IndexError, TypeError, ValueError):
                continue
            if ts <= self.last_ts[tf]:
                continue
            arr.append(px)
            self.last_ts[tf] = ts
            changed = True
        del arr[:max(0, len(arr) - (self.period + 40))]
        if not changed:
            return
        self._emit(tf)

    def _emit(self, tf):
        """Карточка сухих фактов: RSI обеих нитей + давление −1..1."""
        r_base = rsi_value(self.closes[self.base], self.period)
        r_strat = rsi_value(self.closes[self.strat], self.period)
        if r_base is None:
            return                    # мало закрытых свечей
        # давление: перепроданность → вверх (+), перекупленность → вниз (−)
        if r_base >= self.upper:
            pressure = (self.upper - r_base) / 30.0 - 1.0
        elif r_base <= self.lower:
            pressure = (self.lower - r_base) / 30.0 + 1.0
        else:
            pressure = (50.0 - r_base) / 30.0
        pressure = max(-1.0, min(1.0, pressure))
        state = ("overbought" if r_base >= self.upper
                 else "oversold" if r_base <= self.lower else "neutral")
        fields = {
            "rsi_base": round(r_base, 1),
            "rsi_strat": (round(r_strat, 1) if r_strat is not None else None),
            "pressure": round(pressure, 3),
            "state": state,
            "ts_ms": int(time.time() * 1000),
        }
        # ДЕДУП ПО ФИЗИКЕ, А НЕ ПО СЛОВАРЮ (аудит 27.10): в fields лежит
        # ts_ms, а он меняется КАЖДЫЙ вызов — сравнение всего словаря не
        # срабатывало НИКОГДА, и комментарий «без изменений — шину не
        # грузим» обещал пустоту. Сверяем только сами измерения.
        sig = (r_base, r_strat, pressure, state)
        if self._last_pub == sig:
            return                    # показания те же — поток не грузим
        self._last_pub = sig
        self.publish(DRAFT, {
            "kind": "rsi-state",
            "author": "rsi",
            "confidence": 0.6,
            "fields": fields,
            "lineage": [f"candle:{tf}"],
        })
        self.issued += 1
        if self.issued == 1 or self.issued % 50 == 0:
            self.log.info("RSI: база %.1f, стратегия %s — %s (давление %+.2f)",
                          r_base, (f"{r_strat:.1f}" if r_strat is not None
                                   else "мало данных"), state, pressure)

