"""Пружина (Этап 32) — растяжение цены от VWAP-якоря + затухание OI.

Физика (утверждено в бэклоге): цена улетела от якоря + OI гаснет =
толпа без бензина → возврат к руслу. Измеритель, не решатель: публикует
карточку сухих фактов kind="spring-state":
- vwap — якорь (VWAP окна минуток: типичная цена × объём);
- stretch_atr — растяжение закрытия от якоря в ATR (±);
- oi_slope_pct — наклон OI за окно наблюдения (%);
- force — давление возврата −1..1 (против направления растяжения),
  включается ТОЛЬКО когда якорь растянут и топливо гаснет.

Сценарист добавляет факт в bias (config: [spring] weight). Расписка:
Лаборатория оценит факт по исходу сделки, Тюнер двигает доверие.
"""
import collections
import time

from core.module import Manifest, NeirModule
from modules._labels import CANDLES, CANDLES_WARMUP, DRAFT, OI_SNAPSHOT

ATR_BARS = 14            # техническое окно ATR (простые диапазоны)


def vwap_value(rows):
    """VWAP окна: типичная цена × объём. Нулевые объёмы — средняя close."""
    denom = sum(v for *_, v in rows)
    if denom <= 0:
        return sum(c for _, _, _, c, _ in rows) / len(rows)
    return sum(((h + l + c) / 3.0) * v for _, h, l, c, v in rows) / denom


def oi_slope_pct(oi_deque, window_ms, now_ms):
    """Наклон OI за окно (%): к самому свежему от опорного в прошлом."""
    if not oi_deque:
        return None
    target = now_ms - window_ms
    old = oi_deque[0][1]
    for t, o in oi_deque:
        if t <= target:
            old = o
        else:
            break
    if old <= 0:
        return None
    return (oi_deque[-1][1] - old) / old * 100.0


class Spring(NeirModule):
    manifest = Manifest(name="spring", title="Пружина (VWAP-якорь + OI)",
                        requires=("candles",), priority=43, tick_timeout=5.0)

    async def on_start(self):
        c = self.kernel.cfg
        self.base_tf = c.base_tf_sec
        self.window = c.spring_window_bars
        self.stretch_min = c.spring_stretch_atr
        self.oi_fall = c.spring_oi_fall_pct
        self.oi_window_ms = c.spring_oi_window_sec * 1000.0
        self.publish_sec = c.spring_publish_sec
        self._rows = collections.deque(maxlen=self.window + 10)
        self._last_ts = 0
        self._oi = collections.deque(maxlen=60)     # (ts_ms, oi)
        self._last_pub = None
        self.issued = 0
        self.subscribe(CANDLES, self._on_candles)
        self.subscribe(f"{CANDLES_WARMUP}:{self.base_tf}", self._on_candles)
        # ПРОГРЕВ ИЗ БАЗЫ (аудит 27.09, урок Фракталов): якорь VWAP
        # нельзя ждать час живых баров — история уже на доске значений
        warm = self.get_last(f"{CANDLES_WARMUP}:{self.base_tf}")
        if warm is not None:
            await self._on_candles(warm)
        self.subscribe(OI_SNAPSHOT, self._on_oi)
        self.every(self.publish_sec, self._publish)
        self.log.info("Пружина включена: якорь VWAP %d баров, натянута от "
                      "%.1f ATR при затухании OI ≥ %.2f%% за %.0f c",
                      self.window, self.stretch_min, self.oi_fall,
                      self.oi_window_ms / 1000.0)

    async def on_stop(self):
        self.log.info("Итог: карточек пружины %d", self.issued)

    # ---------- приём ----------

    async def _on_candles(self, env):
        data = env.payload
        try:
            tf = int(data.get("tf_sec", 0))
        except (TypeError, ValueError):
            return
        if tf != self.base_tf:
            return
        if not data.get("closed") and not data.get("warmup"):
            return
        for r in data.get("candles") or []:
            try:
                ts, h, l, cl, v = (int(r[0]), float(r[2]), float(r[3]),
                                   float(r[4]), float(r[5]))
            except (IndexError, TypeError, ValueError):
                continue
            if ts <= self._last_ts:
                continue
            self._rows.append((ts, h, l, cl, v))
            self._last_ts = ts

    async def _on_oi(self, env):
        try:
            ts = int(env.payload.get("ts_ms", 0)) or int(time.time() * 1000)
            oi = float(env.payload.get("oi", 0.0))
        except (TypeError, ValueError):
            return
        if oi <= 0:
            return
        self._oi.append((ts, oi))

    # ---------- пульс ----------

    async def _publish(self):
        if len(self._rows) < self.window or not self._oi:
            return
        rows = list(self._rows)[-self.window:]
        vwap = vwap_value(rows)
        close = rows[-1][3]
        atr = (sum(h - l for _, h, l, _, _ in rows[-ATR_BARS:])
               / min(ATR_BARS, len(rows)))
        stretch = (close - vwap) / atr if atr > 0 else 0.0
        now_ms = int(time.time() * 1000)
        slope = oi_slope_pct(self._oi, self.oi_window_ms, now_ms)
        if slope is None:
            return
        # Растяжение 0..1 (порог = полная натяжка) + топливо 0..1
        # (глубина затухания OI относительно ручки oi_fall)
        pull = max(-1.0, min(1.0, abs(stretch) / self.stretch_min))
        gas = max(0.0, min(1.0, -slope / max(0.01, self.oi_fall * 5.0)))
        force = 0.0
        if pull >= 1.0 and gas > 0.0:
            force = (-1.0 if stretch > 0 else 1.0) * gas
        state = ("натянута вверх" if stretch > self.stretch_min else
                 "натянута вниз" if stretch < -self.stretch_min
                 else "спокойна")
        fields = {
            "vwap": round(vwap, 8),
            "price": round(close, 8),
            "stretch_atr": round(stretch, 3),
            "stretch_pct": (round((close - vwap) / vwap * 100.0, 4)
                            if vwap > 0 else 0.0),
            "oi_slope_pct": round(slope, 3),
            "force": round(force, 3),
            "state": state,
            "ts_ms": now_ms,
        }
        # ДЕДУП ПО ФИЗИКЕ, А НЕ ПО СЛОВАРЮ (аудит 27.10): ts_ms меняется
        # каждый пульс, поэтому fields != fields всегда и дедуп не работал
        # ни разу. Сверяем только измерения пружины.
        sig = (round(vwap, 8), round(close, 8), round(stretch, 4),
               round(slope, 3), round(force, 4), state)
        if self._last_pub == sig:
            return                    # якорь/натяжка те же — не грузим
        self._last_pub = sig
        self.publish(DRAFT, {
            "kind": "spring-state",
            "author": "spring",
            "confidence": 0.6,
            "fields": fields,
            "lineage": [f"candle:{self.base_tf}"],
        })
        self.issued += 1
        if self.issued == 1 or self.issued % 50 == 0:
            self.log.info("Пружина: VWAP %.5f, растяжение %+.2f ATR, "
                          "OI %+.2f%% — %s (возврат %+.2f)",
                          vwap, stretch, slope, state, force)