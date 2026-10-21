"""«Поводырь» — вожак рынка BTCUSDT (Этап 20, утверждено).

Никаких сторонних «готовых корреляций»: сырьё — тот же поток сделок,
но по вожаку (Сборщик кладёт его в чистый топик btc_price). Поводырь
сам ведёт мини-полотно вожака и нашей нити и выдаёт Сценаристу карточку
СУХИХ ФАКТОВ (kind: "leader-correlation"):
- angle_btc_pct / angle_alt_pct — углы наклона регрессии (лог-цены,
  % в минуту) обеих нитей на одном окне;
- delta_btc / delta_alt — кумулятивная дельта (покупатели − продавцы),
  нормированная −1..1 по объёму окна;
- divergence — маркер расхождения: нити тянут в разные стороны (углы
  противоположных знаков при заметной силе) ИЛИ перекос дельты против
  хода цены вожака.

Сценарист сам решает, что с этими фактами делать (вес и штраф за
расхождение — config.ini [leader], тюнер откалибрует). Поводырь не
голосует и не усиливает — только измеряет (как и вся Волна 2).
"""
import collections
import time

import numpy as np

from core.module import Manifest, NeirModule
from modules._labels import BTC_PRICE, DRAFT, PRICE


class Leader(NeirModule):
    manifest = Manifest(name="leader", title="Поводырь (вожак BTC)",
                        priority=48, tick_timeout=5.0)

    async def on_start(self):
        c = self.kernel.cfg
        self.window_sec = c.leader_window_sec
        self.publish_every = c.leader_publish_interval
        self.diverge_gap = c.leader_diverge_gap
        self._btc = collections.deque(maxlen=20000)   # (ts_ms, px, dq)
        self._alt = collections.deque(maxlen=20000)   # (ts_ms, px, dq)
        self.issued = 0

        self.subscribe(BTC_PRICE, self._on_btc)
        self.subscribe(PRICE, self._on_alt)
        self.every(self.publish_every, self._tick)
        self.log.info("Поводырь включён: окно %.0fс, выпуск раз в %.0fс",
                      self.window_sec, self.publish_every)

    async def on_stop(self):
        self.log.info("Итог: карточек поводыря %d (вожак %s)",
                      self.issued,
                      "жив" if self._btc else "молчал — топик btc_price пуст")

    # ---------- приём сделок ----------

    async def _on_btc(self, env):
        p = env.payload
        try:
            px = float(p["price"])
            qty = float(p.get("qty", 0.0))
            ts = int(p.get("ts_ms", 0))
        except (KeyError, TypeError, ValueError):
            return
        if px > 0 and ts > 0:
            self._trim(self._btc, ts)
            self._btc.append((ts, px, qty if not p.get("is_sell") else -qty))

    async def _on_alt(self, env):
        p = env.payload
        sym = str(p.get("symbol", "")).upper()
        if sym and sym != self.kernel.cfg.symbol.upper():
            return                    # чужая нить не нужна
        try:
            px = float(p["price"])
            qty = float(p.get("qty", 0.0))
            ts = int(p.get("ts_ms", 0))
        except (KeyError, TypeError, ValueError):
            return
        if px > 0 and ts > 0:
            self._trim(self._alt, ts)
            self._alt.append((ts, px, qty if not p.get("is_sell") else -qty))

    def _trim(self, dq, now_ts):
        # ОКНО ЧЕСТНОЕ (аудит 27.10): было window_sec * 2000 — это 10 минут
        # при объявленных в карточке 5. Множитель 2000 дважды пересчитывал
        # секунды в миллисекунды; углы и дельты считались по вдвое большему
        # буферу, а fields["window_sec"] врал. Теперь ровно window_sec.
        edge = now_ts - self.window_sec * 1000
        while dq and dq[0][0] < edge:
            dq.popleft()

    # ---------- измерения (сухие факты) ----------

    def _angle_pct_min(self, dq):
        """Угол регрессии лог-цены на окне: % в минуту. Мало точек — None."""
        if len(dq) < 30:
            return None
        t = np.array([(ts - dq[-1][0]) / 1000.0 for ts, _, _ in dq])
        y = np.log(np.array([px for _, px, _ in dq]))
        if t.std() < 1e-9 or y.std() < 1e-12:
            return None
        slope, _ = np.polyfit(t, y, 1)          # доля/сек
        return float(slope * 60.0 * 100.0)      # % в минуту

    def _delta(self, dq):
        """Кумулятивная дельта окна, нормированная объёмом: −1..1."""
        if len(dq) < 30:
            return None
        total = sum(abs(q) for _, _, q in dq)
        if total <= 0:
            return None
        return max(-1.0, min(1.0, sum(q for _, _, q in dq) / total))

    async def _tick(self):
        now_ms = int(time.time() * 1000)
        self._trim(self._btc, now_ms)
        self._trim(self._alt, now_ms)
        angle_btc = self._angle_pct_min(self._btc)
        angle_alt = self._angle_pct_min(self._alt)
        delta_btc = self._delta(self._btc)
        delta_alt = self._delta(self._alt)
        if angle_btc is None:
            return                    # вожак молчит — карточки нет

        divergence = False
        if angle_alt is not None:
            strength = (abs(angle_btc) + abs(angle_alt)) / 2.0
            if strength >= self.diverge_gap and angle_btc * angle_alt < 0:
                divergence = True
            if delta_btc is not None and delta_alt is not None:
                if angle_btc > 0 and delta_btc > 0.3 and delta_alt < -0.3:
                    divergence = True   # вожака покупают, нашу нить сливают

        self.publish(DRAFT, {
            "kind": "leader-correlation",
            "author": "leader",
            "confidence": 0.5,
            "fields": {
                "angle_btc_pct": round(angle_btc, 5),
                "angle_alt_pct": (round(angle_alt, 5)
                                  if angle_alt is not None else None),
                "delta_btc": (round(delta_btc, 3)
                              if delta_btc is not None else None),
                "delta_alt": (round(delta_alt, 3)
                              if delta_alt is not None else None),
                "divergence": divergence,
                "window_sec": self.window_sec,
                "ts_ms": now_ms,
            },
            "lineage": ["btc:ticks"],
        })
        self.issued += 1
        if self.issued == 1 or self.issued % 20 == 0:
            self.log.info("Вожак: BTC %+.3f%%/мин, наша нить %s, "
                          "расхождение %s",
                          angle_btc,
                          (f"{angle_alt:+.3f}" if angle_alt is not None
                           else "молчит"),
                          "ДА" if divergence else "нет")
