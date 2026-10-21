"""SR-анализатор — зоны-преграды по пирамиде ТФ (мультимасштаб, ДеМарк).

Этап 4, переписан 13.10 под стандарт Грааля (аудит SR):
1. МУЛЬТИМАСШТАБ: пивоты ищутся по пирамиде ТФ (по умолчанию 1м, 5м, 15м,
   1ч, 4ч), заданной в конфиге [sr] scales. Каждый масштаб — свой буфер
   свечей (прогрев с шины CANDLES_WARMUP + живые закрытые с CANDLES).
   Свечи НЕ копим внутри себя второй раз — читаем с шины, хозяин истории
   (полотно/склад) остаётся один.
2. ДЕМАРК: пивот = свеча, чей High/Low — экстремум среди соседей на k
   в обе стороны. Близкие пивоты склеиваем в зону (ширина = доля ATR).
3. РЕЗОНАНС НИСОНА: зоны разных ТФ, совпавшие по цене (в пределах tol),
   сливаются в одну преграду. resonance_count = сколько РАЗНЫХ ТФ
   подтвердили преграду (1..len(scales)). Сила — сумма по масштабам.
   Тяжёлая плита видна сразу по высокому резонансу.
4. ЧИСТАЯ ГЕОМЕТРИЯ: карточка несёт ТОЛЬКО числа — zone_low, zone_high,
   touches, total_strength, resonance_count. Никаких слов «support/
   resistance» — сторона относительно цены дело Сценариста.
5. РУЧКИ В КОНФИГЕ: 6 настроек (k, ширина, топ, устаревание, минимум,
   глубина) читаются из [sr], а не зашиты в код. ATR — честный True Range
   (математика заимствована у Зоны+).
"""
import collections
import time

import numpy as np

from core.module import Manifest, NeirModule
from modules._labels import CANDLES, CANDLES_WARMUP, DRAFT


def _true_atr(rows):
    """Честный ATR (True Range) по последним 14 свечам — как в Зоны+.

    None, если свечей маловато для осмысленного значения.
    """
    if len(rows) < 2:
        return None
    h = np.array([r[2] for r in rows], dtype=float)
    l = np.array([r[3] for r in rows], dtype=float)
    c = np.array([r[4] for r in rows], dtype=float)
    prev = np.roll(c, 1)
    prev[0] = c[0]
    tr = np.maximum(h - l, np.maximum(np.abs(h - prev), np.abs(l - prev)))
    val = float(np.mean(tr[-14:]))
    return val if val > 0 else None


class SrAnalyzer(NeirModule):
    manifest = Manifest(name="sr-analyst", title="SR-анализатор (зоны-преграды)",
                        requires=("candles",), priority=40, tick_timeout=10.0)

    async def on_start(self):
        self.symbol = self.kernel.cfg.symbol.upper()
        c = self.kernel.cfg
        self.scales = tuple(int(x) for x in c.sr_scales) or (60,)
        self.base_tf = c.base_tf_sec
        self.pivot_k = c.sr_pivot_k
        self.cluster_atr = c.sr_cluster_atr
        self.top_zones = c.sr_top_zones
        self.refresh_sec = c.sr_refresh_sec
        self.min_candles = c.sr_min_candles
        keep = c.sr_keep_candles
        self._rows = {tf: collections.deque(maxlen=keep) for tf in self.scales}
        self._signature = None
        self._last_stamp = 0.0
        self.stamps = 0
        self.subscribe(CANDLES, self._on_candles)
        # ПРОГРЕВ ИЗ ИСТОРИИ по КАЖДОМУ масштабу (с шины, как в Зоны+):
        # пивоты нельзя ждать час живых баров — история уже на доске.
        for tf in self.scales:
            self.subscribe(f"{CANDLES_WARMUP}:{tf}", self._on_candles)
            warm = self.get_last(f"{CANDLES_WARMUP}:{tf}")
            if warm is not None:
                await self._on_candles(warm)
        self.log.info("SR-анализатор: масштабы %s мин, пивот k=%d, "
                      "ширина %.2f ATR, топ %d",
                      [t // 60 for t in self.scales], self.pivot_k,
                      self.cluster_atr, self.top_zones)

    async def on_stop(self):
        self.log.info("Итог: обновлений картины преград %d", self.stamps)

    # ---------- приём свечей ----------

    async def _on_candles(self, env):
        data = env.payload
        try:
            tf = int(data["tf_sec"])
            rows = data["candles"]
        except (KeyError, TypeError):
            return
        buf = self._rows.get(tf)
        if buf is None:
            return
        if data.get("warmup"):
            buf.clear()
            for r in rows:
                buf.append(tuple(r))
        else:
            if not data.get("closed"):
                return                     # незакрытые свечи зоны не размывают
            for r in rows:
                r = tuple(r)
                if buf and buf[-1][0] == r[0]:
                    buf[-1] = r
                elif not buf or r[0] > buf[-1][0]:
                    buf.append(r)
        self._maybe_stamp(force=bool(data.get("warmup")))

    # ---------- штампы по показателям ----------

    def _maybe_stamp(self, force=False):
        if not any(len(self._rows[tf]) >= self.min_candles for tf in self.scales):
            return
        now = time.monotonic()
        barriers = self._find_barriers()
        signature = tuple(sorted((round(b["low"], 6), round(b["high"], 6),
                                  b["touches"], b["resonance"])
                                 for b in barriers))
        changed = signature != self._signature
        stale = (now - self._last_stamp) >= self.refresh_sec
        if not (force or changed or stale):
            return
        base_rows = self._rows.get(self.base_tf)
        last_ts = base_rows[-1][0] if base_rows else int(now * 1000)
        for b in barriers:
            self.publish(DRAFT, {
                "kind": "sr-zone",
                "author": "sr-analyst",
                "confidence": round(b["strength"], 3),
                "fields": {
                    "zone_low": round(b["low"], 8),
                    "zone_high": round(b["high"], 8),
                    "touches": b["touches"],
                    "total_strength": round(b["strength"], 3),
                    "resonance_count": b["resonance"],
                },
                "lineage": [f"candle:{self.symbol}:{self.base_tf}:{last_ts}"],
            })
        self._signature = signature
        self._last_stamp = now
        self.stamps += 1
        best = max((b["resonance"] for b in barriers), default=0)
        self.log.info("Картина преград обновлена: %d шт, макс. резонанс %d ТФ",
                      len(barriers), best)

    # ---------- поиск преград по пирамиде ----------

    def _zones_for_scale(self, tf):
        """Зоны ОДНОГО масштаба: список {low, high, touches, strength, tf}.

        ДеМарк: пивот — экстремум High/Low среди соседей на k в обе
        стороны; близкие пивоты склеиваются в зону шириной cluster_atr*ATR.
        """
        rows = list(self._rows[tf])
        if len(rows) < self.min_candles:
            return []
        atr = _true_atr(rows)
        if atr is None:
            atr = float(rows[-1][4]) * 0.002
        tol = self.cluster_atr * atr
        h = np.array([r[2] for r in rows], dtype=float)
        l = np.array([r[3] for r in rows], dtype=float)
        k = self.pivot_k
        pivots = []                          # (цена, индекс, максимум?)
        for i in range(k, len(rows) - k):
            if h[i] == h[i - k:i + k + 1].max():
                pivots.append((float(h[i]), i, True))
            if l[i] == l[i - k:i + k + 1].min():
                pivots.append((float(l[i]), i, False))
        if not pivots:
            return []
        pivots.sort(key=lambda p: p[0])
        clusters = [[pivots[0]]]
        for p in pivots[1:]:
            if p[0] - clusters[-1][-1][0] <= tol:
                clusters[-1].append(p)
            else:
                clusters.append([p])
        n = len(rows)
        zones = []
        for cluster in clusters:
            prices = [p[0] for p in cluster]
            last_idx = max(p[1] for p in cluster)
            mid = float(np.mean(prices))
            recency = last_idx / max(1, n - 1)
            touches = len(cluster)
            strength = 0.6 * min(1.0, touches / 5.0) + 0.4 * recency
            zones.append({
                "low": mid - tol / 2.0, "high": mid + tol / 2.0,
                "touches": touches, "strength": min(1.0, strength), "tf": tf,
            })
        return zones

    def _find_barriers(self):
        """Слияние зон всех масштабов в преграды с резонансом Нисона.

        Зоны разных ТФ, совпавшие по цене (зазор ≤ tol_res), сливаются в
        одну преграду; resonance = число РАЗНЫХ ТФ, её подтвердивших.
        """
        base_rows = list(self._rows.get(self.base_tf) or [])
        atr_base = _true_atr(base_rows)
        if atr_base is None:
            atr_base = (float(base_rows[-1][4]) * 0.002) if base_rows else 1.0
        tol_res = self.cluster_atr * atr_base

        all_zones = []
        for tf in self.scales:
            all_zones.extend(self._zones_for_scale(tf))
        if not all_zones:
            return []
        all_zones.sort(key=lambda z: z["low"])

        barriers = []
        cur = dict(all_zones[0])
        cur["tfs"] = {all_zones[0]["tf"]}
        for z in all_zones[1:]:
            if z["low"] <= cur["high"] + tol_res:
                cur["low"] = min(cur["low"], z["low"])
                cur["high"] = max(cur["high"], z["high"])
                cur["touches"] += z["touches"]
                cur["strength"] += z["strength"]
                cur["tfs"].add(z["tf"])
            else:
                barriers.append(cur)
                cur = dict(z)
                cur["tfs"] = {z["tf"]}
        barriers.append(cur)

        out = [{"low": b["low"], "high": b["high"], "touches": b["touches"],
                "strength": min(1.0, b["strength"]), "resonance": len(b["tfs"])}
               for b in barriers]
        out.sort(key=lambda b: b["strength"], reverse=True)
        return out[:self.top_zones]