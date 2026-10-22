"""«Зоны+» — карта несправедливых цен и стопов, 13 этажей (20.10.2026).

Два вида зон, другая физика, чем у SR:
- FVG (имбаланс): свеча улетела так, что между диапазонами соседних
  свечей осталась «дыра» — рынок обычно возвращается её заполнить.
  Истощение ПРОПОРЦИОНАЛЬНО заполнению (утверждено): зашли на 50%
  тела — масса зоны упала на 50%.
- Стоп-кластер: совпадающие пивоты на одном уровне = скопление стопов.
  Сторона относительно цены — дело Сценариста, мы её не пишем.

СТРОГОЕ РАЗДЕЛЕНИЕ ПОЛЕЙ (20.10, хозяин) — среда не мешается к прогнозу:
- СРЕДА — сухая геометрия холста, сырые факты: zone_low, zone_high,
  tf_sec, width_atr, mass_left, filled_pct, pivots_count, age_min,
  resonance_count.
- ПРОГНОЗ — вектор ожидания, одно плоское число zone_power:
  resonance_count × (mass_left | pivots_count) × ln(tf_sec).
  Дилер и Сценарист отличают геометрический факт наложения уровней
  от расчётной силы преграды; ось zone_power@ТФ есть в Матрице.

13 СТУПЕНЕЙ (20.10, Гугл): уровни = EXCHANGE_SPECTRUM ∪ {base_tf_sec}.
Раньше зажимало 2 ТФ (base+strategy) — резонанс Нисона не видел 11 из
13 этажей. Резонанс — ТОЧНОЕ ПЕРЕСЕЧЕНИЕ интервалов любых своих зон
(FVG и кластеры) разных ТФ: resonance_count ∈ 1..13 (ATR-радиуса в коде
не было и нет — старый докстринг врал).

ЖИЗНЬ ЗОНЫ (TTL-консолидация): автор считает срок сам и кладёт его в
КОРЕНЬ DRAFT (fvg_ttl_min/cluster_ttl_min → ttl_sec) — Поле исполняет
авторскую жизнь, не свои таблички. Сердцебиение heartbeat_sec: тихие
живые зоны перепубликуются не реже heartbeat_sec — ячейки Матрицы
(TTL 30 мин) и полки Поля не умирают на тихом рынке.

Публикация: значимые изменения (новая зона; заполнение перевалило шаг
mass_bucket) + heartbeat. Имена zone_low/zone_high общие с SR — Поле
вберёт зоны. Окна — из config [zones_plus].
"""
import collections
import math
import time

import numpy as np

from core.module import Manifest, NeirModule
from modules._labels import CANDLES, CANDLES_WARMUP, DRAFT
from modules.market.collector import EXCHANGE_SPECTRUM


def _atr(rows, period):
    if len(rows) < period + 1:
        return None
    h = np.array([r[2] for r in rows])
    l = np.array([r[3] for r in rows])
    c = np.array([r[4] for r in rows])
    prev = np.roll(c, 1)
    prev[0] = c[0]
    tr = np.maximum(h - l, np.maximum(np.abs(h - prev), np.abs(l - prev)))
    return float(np.mean(tr[-period:]))


class ZonesPlus(NeirModule):
    manifest = Manifest(name="zones-plus", title="Зоны+ (FVG, стоп-кластеры)",
                        requires=("candles",), priority=46, tick_timeout=10.0)

    async def on_start(self):
        self.symbol = self.kernel.cfg.symbol.upper()
        c = self.kernel.cfg
        self.fvg_min_atr = c.zp_fvg_min_atr
        self.pivot_k = c.zp_pivot_k
        self.cluster_radius = c.zp_cluster_radius_atr
        self.cluster_min = c.zp_cluster_min_pivots
        # технические окна (вынесены из кода в конфиг, 13.10)
        self.atr_period = c.zp_atr_period
        self.rows_keep = c.zp_rows_keep
        self.mass_bucket = c.zp_mass_bucket
        self.cluster_pad = c.zp_cluster_pad
        self.dead_mass = c.zp_dead_mass

        # 13 СТУПЕНЕЙ (20.10, Гугл): раньше зажимало 2 ТФ (base+strategy) —
        # резонанс Нисона не видел 11 из 13 этажей. Источник правды —
        # EXCHANGE_SPECTRUM, как у Регрессии (прецедент узла 6).
        levels = sorted(set(EXCHANGE_SPECTRUM) | {int(c.base_tf_sec)})
        self.levels = {tf: collections.deque(maxlen=self.rows_keep)
                       for tf in levels}
        # РУЧКИ ЖИЗНИ ЗОНЫ (TTL-консолидация): автор считает срок сам
        # и кладёт его в КОРЕНЬ DRAFT (ttl_sec) — Поле исполняет авторскую.
        self.fvg_ttl_sec = int(c.zp_fvg_ttl_min) * 60
        self.cluster_ttl_sec = int(c.zp_cluster_ttl_min) * 60
        # СЕРДЦЕБИЕНИЕ (20.10): тихие живые зоны перепубликуются не реже
        # heartbeat_sec — ячейки Матрицы (TTL 30 мин) и полки не умирают.
        self.hb_sec = int(c.zp_heartbeat_sec)
        # ключ FVG: (tf, price_key) — зоны разных ТФ не перезатирают друг друга
        self._fvgs = {}
        self._fvg_hb = {}         # ключ -> hb-тик последней публикации FVG
        # ключ кластера: (tf, price_key) -> геометрия+sig (для резонанса)
        self._clusters = {}

        self.subscribe(CANDLES, self._on_candles)
        # ПРОГРЕВ ИЗ БАЗЫ (аудит 27.09, урок Фракталов): FVG и кластеры
        # пивотов нельзя ждать час живых баров — история уже на доске
        for tf in self.levels:
            self.subscribe(f"{CANDLES_WARMUP}:{tf}", self._on_candles)
            warm = self.get_last(f"{CANDLES_WARMUP}:{tf}")
            if warm is not None:
                await self._on_candles(warm)
        self.every(self.hb_sec, self._heartbeat)
        self.log.info("Зоны+ включены: %d ступеней, FVG от %.2f ATR, "
                      "кластер от %d пивотов, heartbeat %ds, TTL %dс/%dс",
                      len(self.levels), self.fvg_min_atr, self.cluster_min,
                      self.hb_sec, self.fvg_ttl_sec, self.cluster_ttl_sec)

    async def on_stop(self):
        self.log.info("Итог: FVG %d (активных), кластеров %d",
                      sum(1 for f in self._fvgs.values() if f["pct"] < 1.0),
                      len(self._clusters))

    def _heartbeat(self):
        """Сердцебиение Зон+ (20.10): тихие живые зоны перепубликуются
        не реже heartbeat_sec — hb-тик в подписи дёргает дедуп, карточка
        получает свежий created_ms, ячейки Матрицы и полки Поля живут,
        пока зона физически стоит в памяти модуля."""
        hb = int(time.time() // self.hb_sec)
        for key, f in self._fvgs.items():
            if f["pct"] >= 1.0 or self._fvg_hb.get(key) == hb:
                continue
            self._fvg_hb[key] = hb
            self._publish_fvg(key, f)
        for (tf, pkey), rec in list(self._clusters.items()):
            if rec["sig"][3] == hb:
                continue
            rec["sig"] = (rec["sig"][0], rec["sig"][1], rec["sig"][2], hb)
            self._publish_cluster(tf, pkey, rec, hb)

    def _resonance(self, tf, low, high):
        """Резонанс Нисона: сколько РАЗНЫХ ТФ подтвердили преграду
        своими зонами — FVG И стоп-кластеры. ТОЧНОЕ ПЕРЕСЕЧЕНИЕ
        интервалов, сухая геометрия (ATR-радиуса в коде не было и нет —
        старый докстринг врал). Свой ТФ всегда 1; при 13 ступенях
        resonance_count ∈ 1..13."""
        tfs = {tf}
        for f in self._fvgs.values():
            if f["tf"] != tf and not (f["high"] < low or f["low"] > high):
                tfs.add(f["tf"])
        for (ctf, _), rec in self._clusters.items():
            if ctf != tf and not (rec["hi"] < low or rec["lo"] > high):
                tfs.add(ctf)
        return len(tfs)

    async def _on_candles(self, env):
        data = env.payload
        try:
            tf = int(data["tf_sec"])
            rows = data["candles"]
        except (KeyError, TypeError, ValueError):
            return
        if not rows or tf not in self.levels:
            return
        # незакрытую (текущую) свечу не гоняем — как у коллег по шине
        if not data.get("closed") and not data.get("warmup"):
            return
        rows = [r for r in rows if r[1] > 0 and r[4] > 0] or list(rows)
        self.levels[tf].extend(rows)
        atr = _atr(self.levels[tf], self.atr_period)
        if atr is None or atr <= 0:
            return
        # --- FVG: дыры имбаланса, масса тает с заполнением ---
        for key, f in list(self._fvgs.items()):
            if f["tf"] != tf or f["pct"] >= 1.0:
                continue
            width = f["high"] - f["low"]
            if width <= 0:
                f["pct"] = 1.0
                self._publish_fvg(key, f)
                continue
            for r in rows:
                if r[0] < f["born_ts"]:
                    continue          # свеча старше дыры — не трогаем
                lo, hi = min(r[3], r[4]), max(r[2], r[4])
                overlap_hi = min(hi, f["high"])
                overlap_lo = max(lo, f["low"])
                if overlap_hi <= overlap_lo:
                    continue
                f["fill_lo"] = min(f["fill_lo"], overlap_lo)
                f["fill_hi"] = max(f["fill_hi"], overlap_hi)
            pct = min(1.0, (f["fill_hi"] - f["fill_lo"]) / width)
            if pct - f["pct"] >= self.mass_bucket or pct >= 1.0:
                f["pct"] = pct
                self._publish_fvg(key, f)

        rows = list(self.levels[tf])
        for i in range(2, len(rows)):
            prev2 = rows[i - 2]
            cur = rows[i]
            born_ts = rows[i - 1][0]
            if prev2[2] < cur[3]:                    # бычья дыра (внизу)
                low, high = float(prev2[2]), float(cur[3])
            elif prev2[3] > cur[2]:                  # медвежья дыра (вверху)
                low, high = float(cur[2]), float(prev2[3])
            else:
                continue
            width = high - low
            if width < self.fvg_min_atr * atr:
                continue
            pkey = round((low + high) / 2.0, 6)
            key = (tf, pkey)
            f = self._fvgs.get(key)
            if f is None:
                # заполнение из уже накопившейся истории после дыры
                fill_lo, fill_hi = high, low
                for r in rows[i:]:
                    fill_lo = min(fill_lo, max(r[3], low))
                    fill_hi = max(fill_hi, min(r[2], high))
                pct = min(1.0, (fill_hi - fill_lo) / width) if width > 0 else 1.0
                self._fvgs[key] = {"low": low, "high": high, "tf": tf,
                                   "fill_lo": fill_lo, "fill_hi": fill_hi,
                                   "pct": pct, "width_atr": width / atr,
                                   "born_ts": born_ts}
                if pct < 1.0 - self.dead_mass:
                    self._publish_fvg(key, self._fvgs[key])

        # --- стоп-кластеры: совпадающие пивоты ---
        k = self.pivot_k
        pivots = []
        for i in range(k, len(rows) - k):
            window = rows[i - k:i + k + 1]
            if rows[i][2] == max(r[2] for r in window):
                pivots.append(("high", float(rows[i][2]), int(rows[i][0])))
            elif rows[i][3] == min(r[3] for r in window):
                pivots.append(("low", float(rows[i][3]), int(rows[i][0])))
        hb = int(time.time() // self.hb_sec)
        # пометка текущего прогона: группы без пометки — пивоты вытеснены,
        # зона ушла из памяти (heartbeat её не воскрешает)
        for gkey in list(self._clusters):
            if gkey[0] == tf:
                self._clusters[gkey]["seen"] = False
        used = [False] * len(pivots)
        for a in range(len(pivots)):
            if used[a]:
                continue
            group = [pivots[a]]
            used[a] = True
            for b in range(a + 1, len(pivots)):
                if used[b]:
                    continue
                if abs(pivots[b][1] - pivots[a][1]) <= self.cluster_radius * atr:
                    group.append(pivots[b])
                    used[b] = True
            if len(group) < self.cluster_min:
                continue
            prices = [g[1] for g in group]
            pad = self.cluster_pad * self.cluster_radius * atr
            lo, hi = min(prices) - pad, max(prices) + pad
            pkey = round((lo + hi) / 2.0, 6)
            key = (tf, pkey)
            # подпись на ВРЕМЕННОЙ сетке (20.10): hb-квант дёргает дедуп
            # не реже heartbeat_sec — тихая группа не умирает в журнале/матрице
            sig = (round(lo, 7), round(hi, 7), len(group), hb)
            rec = self._clusters.get(key)
            if rec is not None and rec["sig"] == sig:
                rec["seen"] = True
                continue
            if rec is None:
                rec = {}
                self._clusters[key] = rec
            rec.update({"lo": round(lo, 8), "hi": round(hi, 8),
                        "count": len(group),
                        "min_ts": min(g[2] for g in group),
                        "first_ts": group[0][2], "sig": sig,
                        "seen": True})
            self._publish_cluster(tf, pkey, rec, hb)
            self.log.info("СТОП-КЛАСТЕР: %.5f–%.5f, пивотов %d (ТФ %dм), резонанс %d",
                          lo, hi, len(group), tf // 60,
                          self._resonance(tf, lo, hi))
        # исчезнувшие группы (пивоты вытеснены из deque) — зона ушла
        for gkey in list(self._clusters):
            if gkey[0] == tf and not self._clusters[gkey].get("seen"):
                self._clusters.pop(gkey, None)

    def _zone_power(self, res, mass_or_pivots, tf):
        """ПРОГНОЗ (20.10, разделение среда/прогноз): расчётная сила
        преграды — резонанс масштабов × сырьё (масса дыры или число
        пивотов) × ln(tf_sec): старшие этажи тяжелее по построению.
        Среда рядом остаётся сырым фактом — прогноз её не подменяет."""
        try:
            return round(float(res) * float(mass_or_pivots)
                         * math.log(max(int(tf), 2)), 3)
        except (TypeError, ValueError):
            return 0.0

    def _publish_cluster(self, tf, pkey, rec, hb):
        """СТОП-КЛАСТЕР: поле по договору — сначала СРЕДА (сырая
        геометрия), потом ПРОГНОЗ (zone_power). В корне DRAFT — авторский
        срок жизни ttl_sec (TTL-консолидация: жизнь решает породивший
        аналитик)."""
        age_min = round((time.time() * 1000 - rec["min_ts"]) / 60000, 1)
        res = self._resonance(tf, rec["lo"], rec["hi"])
        power = self._zone_power(res, rec["count"], tf)
        self.publish(DRAFT, {
            "kind": "stop-cluster",
            "author": "zones-plus",
            "confidence": round(min(0.9, 0.4 + 0.15 * rec["count"]), 3),
            "ttl_sec": self.cluster_ttl_sec,   # КОРЕНЬ: срок жизни от автора
            "fields": {
                # ——— СРЕДА: сухая геометрия холста ———
                "zone_id": f"cl{pkey}",        # адрес в решётке Матрицы (дыра 1)
                "zone_low": rec["lo"],
                "zone_high": rec["hi"],
                "tf_sec": tf,
                "pivots_count": rec["count"],   # было "pivots" (паспорт 20.10)
                "age_min": age_min,
                "resonance_count": res,
                # ——— ПРОГНОЗ: вектор ожидания ———
                "zone_power": power,
            },
            "lineage": [f"candle:{self.symbol}:{tf}:{rec['first_ts']}"],
        })
        self.log.info("СТОП-КЛАСТЕР: %.5f–%.5f, пивотов %d, резонанс %d, "
                      "power %.3f (hb %d)", rec["lo"], rec["hi"],
                      rec["count"], res, power, hb)

    def _publish_fvg(self, key, f):
        """FVG: поле по договору — сначала СРЕДА (сырая геометрия и
        заполнение), потом ПРОГНОЗ (zone_power). В корне DRAFT — авторский
        срок жизни ttl_sec (TTL-консолидация)."""
        mass = round(max(0.0, 1.0 - f["pct"]), 3)
        res = self._resonance(f["tf"], f["low"], f["high"])
        power = self._zone_power(res, mass, f["tf"])
        self.publish(DRAFT, {
            "kind": "fvg-zone",
            "author": "zones-plus",
            "confidence": round(0.5 + 0.4 * mass, 3),
            "ttl_sec": self.fvg_ttl_sec,       # КОРЕНЬ: срок жизни от автора
            "fields": {
                # ——— СРЕДА: сухая геометрия холста ———
                "zone_id": f"fvg{round(f['born_ts'])}",  # решётка Матрицы (дыра 1)
                "zone_low": round(f["low"], 8),
                "zone_high": round(f["high"], 8),
                "tf_sec": f["tf"],
                "width_atr": round(f["width_atr"], 3),
                "mass_left": mass,
                "filled_pct": round(f["pct"], 3),
                "resonance_count": res,
                # ——— ПРОГНОЗ: вектор ожидания ———
                "zone_power": power,
            },
            "lineage": [f"candle:{self.symbol}:{f['tf']}:{f['born_ts']}"],
        })