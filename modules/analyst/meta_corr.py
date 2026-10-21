"""Метакоррелятор (Волна 3) — ПАНОРАМНЫЙ РАДАР НЕЛИНЕЙНОГО БУДУЩЕГО.

ТОТАЛЬНЫЙ ОТКАЗ ОТ ПЛОСКИХ СУРРОГАТОВ (27.10, ТЗ «3D-радар лагов»):
раньше модуль честно считал 5 метрик на пару, публиковал их в карточке —
и тут же СЖИМАЛ их в ОДИН `package_bias`. 80% измерений до потребителя
не доходили: в вектор момента уходили два скаляра вместо 5×N осей.
Теперь в `fields` лежат ВСЕ оси, и Сценарист кладёт их в вектор
отдельными полями с весом 0.0 (раскачает Тюнер по факту исхода).

Что считается для КАЖДОЙ монеты пакета (BTC, ETH, SOL, PEPE):
- meta_spearman_{sym} — ранговая линейная связь (устойчива к пампам)
- meta_kendall_{sym}  — согласованность порядков (шпильки не портят)
- meta_dcor_{sym}     — ДИСТАНЦИОННАЯ корреляция: единственная метрика,
                       которая видит НЕЛИНЕЙНЫЕ связи (y = x² и т.п.)
- meta_lag_{sym}      — временной зазор лидера, мин
- meta_lagcorr_{sym}  — сила связи на этом зазоре

ЛАГ-ДИНАМИКА (скорость схлопывания поводка):
- meta_lag_vel_{sym} — первая производная лага: поводок натягивается
                       (лаг растёт) или альт догоняет (лаг падает)
- meta_lag_acc_{sym} — вторая производная: ускорение схлопывания

ЦЕЛОСТНЫЕ ИЗМЕРИТЕЛИ ПАКЕТА:
- meta_dcor_divergence — DCor минус |Спирмен|: нелинейная связь, которой
  нет в линейной. Рост = скрытое стягивание пакета перед штормом.
- meta_packet_spring   — отклонение нашей нити от взвешенного индекса
  пакета, нормированное на ATR (единицы: «сколько ATR отстали»).

Никаких `except: pass`: любая авария матриц NumPy, деление на ноль при
вырожденном буфере лагов или пропуски данных уходит в self.log.error
с полной трассировкой.
"""
import time
from collections import deque

import numpy as np

from core.module import Manifest, NeirModule
from modules._labels import (ALT_PRICE, BTC_LIQ, BTC_PRICE, DRAFT,
                             META_DERIVS, META_METRICS, META_POOL_DEFAULT,
                             PRICE)


def _ranks(arr):
    """Ранги значений (средние при совпадениях) — для Спирмена."""
    order = np.argsort(arr)
    ranks = np.empty_like(order, dtype=float)
    ranks[order] = np.arange(len(arr), dtype=float)
    for v in np.unique(arr):
        mask = arr == v
        if mask.sum() > 1:
            ranks[mask] = ranks[mask].mean()
    return ranks


def _pearson(a, b):
    a, b = a - a.mean(), b - b.mean()
    d = float(np.sqrt((a * a).sum()) * np.sqrt((b * b).sum()))
    return float((a * b).sum() / d) if d > 0 else 0.0


def _spearman(a, b):
    a, b = np.asarray(a, dtype=float), np.asarray(b, dtype=float)
    return _pearson(_ranks(a), _ranks(b))


def _kendall(a, b):
    """Тау-А: (согласованные пары − несогласованные) / всего пар."""
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    n, conc, disc = len(a), 0, 0
    for i in range(n - 1):
        prod = (a[i + 1:] - a[i]) * (b[i + 1:] - b[i])
        conc += int((prod > 0).sum())
        disc += int((prod < 0).sum())
    total = n * (n - 1) / 2.0
    return (conc - disc) / total if total > 0 else 0.0


def _dcor(a, b):
    """Дистанционная корреляция: видит НЕЛИНЕЙНЫЕ связи, которые
    Пирсон/Спирмен пропускают (y = x² на симметричном x). 0..1."""
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    n = len(a)
    if n < 10 or n != len(b):
        return 0.0
    da = np.abs(a[:, None] - a[None, :])
    db = np.abs(b[:, None] - b[None, :])
    for m in (da, db):                   # двойная центровка расстояний
        row = m.mean(axis=1, keepdims=True)
        col = m.mean(axis=0, keepdims=True)
        m -= row + col - m.mean()
    cov = float((da * db).mean())
    va, vb = float((da * da).mean()), float((db * db).mean())
    if va <= 0 or vb <= 0:
        return 0.0
    value = float(cov / np.sqrt(va * vb))
    return round(max(0.0, min(1.0, value)), 3)


class MetaCorr(NeirModule):
    manifest = Manifest(name="meta-corr", title="Метакоррелятор (пакет)",
                        priority=50, tick_timeout=10.0)

    async def on_start(self):
        c = self.kernel.cfg
        self.window = int(c.get_float("meta", "window_min", 60))
        self.lag_max = int(c.get_float("meta", "lag_max_min", 5))
        self.publish_every = c.get_float("meta", "publish_sec", 30.0)
        # Пул конфигом (Этап 33): расширение без кода, наша нить вне его
        self.pairs = [s.strip().upper()
                      for s in c.meta_pool.split(",") if s.strip()]
        self._threads = {}      # symbol -> {minute_win: {close, buy, sell}}
        self._panic = deque(maxlen=200)   # времена ликвидаций вожака
        self._lag_prev = {}      # sym -> прошлый лаг
        self._lagvel_prev = {}   # sym -> прошлый lag_vel (для 2-й производной)
        self.errors = 0
        self.stamps = 0
        self.subscribe(PRICE, self._on_price)
        self.subscribe(BTC_PRICE, self._on_btc)
        self.subscribe(ALT_PRICE, self._on_alt)
        self.subscribe(BTC_LIQ, self._on_btc_liq)
        self.every(self.publish_every, self._publish_card)
        self.log.info("Метакоррелятор включён: 3D-РАДАР по пулу %s — "
                      "%d осей на монету (%s + %s) без сжатия в агрегат, "
                      "окно %d мин, лаги ±%d мин, пульс раз в %.0fс",
                      "+".join(self.pairs),
                      len(META_METRICS) + len(META_DERIVS),
                      "/".join(META_METRICS), "/".join(META_DERIVS),
                      self.window, self.lag_max, self.publish_every)

    async def on_stop(self):
        self.log.info("Итог: карточек пакета %d, ошибок расчёта %d",
                      self.stamps, self.errors)

    def _fail(self, what, exc):
        """Авария расчёта — в журнал с трассировкой, не в молчание."""
        self.errors += 1
        self.log.error("Метакоррелятор: %s: %s: %s", what,
                       type(exc).__name__, exc, exc_info=True)

    # ---------- приём сырых фактов ----------

    def _feed_tick(self, symbol, price, qty, is_sell, ts_ms):
        """Тик нити в минутный квант: close минуты + дельта тейкеров."""
        if price <= 0 or ts_ms <= 0:
            return
        win = ts_ms - ts_ms % 60_000
        th = self._threads.setdefault(symbol, {})
        cur = th.get(win)
        if cur is None:
            cur = th[win] = {"close": price, "buy": 0.0, "sell": 0.0}
        if is_sell:
            cur["sell"] += qty
        else:
            cur["buy"] += qty
        cur["close"] = price

    async def _on_price(self, env):
        p = env.payload
        self._feed_tick(self.kernel.cfg.symbol.upper(),
                        float(p.get("price", 0.0)),
                        float(p.get("qty", 0.0)),
                        bool(p.get("is_sell", False)),
                        int(p.get("ts_ms", 0)))

    async def _on_btc(self, env):
        p = env.payload
        self._feed_tick("BTC", float(p.get("price", 0.0)),
                        float(p.get("qty", 0.0)),
                        bool(p.get("is_sell", False)),
                        int(p.get("ts_ms", 0)))

    async def _on_alt(self, env):
        p = env.payload
        self._feed_tick(str(p.get("symbol", "")).replace("USDT", ""),
                        float(p.get("price", 0.0)),
                        float(p.get("qty", 0.0)),
                        bool(p.get("is_sell", False)),
                        int(p.get("ts_ms", 0)))

    async def _on_btc_liq(self, env):
        if float(env.payload.get("qty", 0.0)) > 0:
            self._panic.append(time.time())

    # ---------- расчёт ----------

    def _series(self, symbol):
        """Доходности нити за окно: % по закрытым минутам (python floats —
        карточки сериализуются в базу, Этап 35, урок живого прогона)."""
        th = self._threads.get(symbol) or {}
        now_ms = time.time() * 1000
        wins = sorted(w for w in th if w <= now_ms
                      and now_ms - w <= (self.window + 2) * 60_000)
        closes = [th[w]["close"] for w in wins[-(self.window + 1):]]
        if len(closes) < 3:
            return []
        a = np.array(closes, dtype=float)
        return [float(x) for x in (a[1:] - a[:-1]) / a[:-1] * 100.0]

    def _lag_dynamics(self, sym, lag):
        """(lag_vel, lag_acc) — производные лага. None — данных нет.

        Скорость схлопывания поводка: lag растёт → альт отстаёт сильнее
        (поводок натягивается); lag падает → догоняет. Вторая производная
        показывает, УСКОРЯЕТСЯ ли схлопывание, — ранний признак того,
        что альт вот-вот примет эталонный вынос.
        """
        vel = acc = None
        if lag is not None:
            prev = self._lag_prev.get(sym)
            if prev is not None:
                vel = round(lag - prev, 3)
            self._lag_prev[sym] = lag
            if vel is not None:
                pv = self._lagvel_prev.get(sym)
                if pv is not None:
                    acc = round(vel - pv, 3)
                self._lagvel_prev[sym] = vel
        return vel, acc

    def _dcor_divergence(self, stats):
        """Нелинейная связь минус линейная: DCor − |Спирмен|.

        Рост = скрытое стягивание пакета, которого линейные метрики
        не видят. Обычное среднее по пакетам.
        """
        vals = []
        for sym, st in (stats or {}).items():
            d, sp = st.get("dcor"), st.get("spearman")
            if d is None or sp is None:
                continue
            vals.append(float(d) - abs(float(sp)))
        if not vals:
            return None
        return round(sum(vals) / len(vals), 4)

    def _packet_spring(self, stats, ours):
        """Пружина пакета: наша нить минус взвешенный индекс, в ATR.

        Индекс пакета — средневзвешенная доходность монет с весами |ρ|.
        Обе величины в процентах, нормируем на ATR (средний минутный
        ход) → ответ в ATR: «на сколько ATR нити отстала от пакета».
        """
        if not ours or len(ours) < 3:
            return None
        drifts, weights = [], []
        for sym, st in (stats or {}).items():
            sp = st.get("spearman")
            ser = self._series(sym)
            if sp is None or not ser:
                continue
            tail = ser[-5:]
            if tail:
                drifts.append(sum(tail))
                weights.append(abs(float(sp)))
        if not drifts:
            return None
        try:
            idx = float(np.average(drifts, weights=weights))   # % пакета
            ours_move = sum(ours[-5:])                          # % нашей нити
            abs_moves = [abs(x) for x in ours[-20:] if x == x]
            if not abs_moves:
                return None
            atr_pct = sum(abs_moves) / len(abs_moves)
            if atr_pct <= 0:
                return None                                   # вырожденный ряд
            return round((ours_move - idx) / atr_pct, 3)
        except (TypeError, ValueError, ZeroDivisionError,
                FloatingPointError) as exc:
            self._fail("пружина пакета", exc)
            return None

    def _lag_corr(self, ours, other, lag):
        """Спирмен при смещении other на lag минут (lag>0 = other
        лидирует: его прошлое предсказывает наше настоящее)."""
        if lag > 0:
            a, b = ours[lag:], other[:-lag]
        elif lag < 0:
            a, b = ours[:lag], other[-lag:]
        else:
            a, b = ours, other
        n = min(len(a), len(b))
        if n < 10:
            return 0.0
        return _spearman(np.array(a[-n:]), np.array(b[-n:]))

    def _pair_stats(self, pair):
        ours = self._series(self.kernel.cfg.symbol.upper())
        other = self._series(pair)
        n = min(len(ours), len(other))
        if n < 10:
            return None
        try:
            oa, ob = np.array(ours[-n:]), np.array(other[-n:])
            best_lag, best_c = 0, 0.0
            for lag in range(-self.lag_max, self.lag_max + 1):
                lc = self._lag_corr(ours, other, lag)
                if abs(lc) >= abs(best_c):
                    best_c, best_lag = lc, lag
            return {"spearman": round(_spearman(oa, ob), 3),
                    "kendall": round(_kendall(oa, ob), 3),
                    "dcor": _dcor(oa, ob),      # нелинейные связи
                    "lag": best_lag,
                    "lagcorr": round(best_c, 3)}
        except (TypeError, ValueError, IndexError, ZeroDivisionError,
                FloatingPointError) as exc:
            self._fail("статистика пары %s" % pair, exc)
            return None

    def _package_bias(self, stats):
        """Куда дрейфует пакет: средневзвешенный импульс последних 5
        минут нитей со значимой связью (|rho|>0.3), −1..1."""
        drifts, weights = [], []
        for pair in self.pairs:
            st = stats.get(pair)
            ser = self._series(pair)
            if st is None or not ser or abs(st["spearman"]) < 0.3:
                continue
            tail = ser[-5:]
            if tail:
                drifts.append(sum(tail))
                weights.append(abs(st["spearman"]))
        if not drifts:
            return 0.0
        return float(np.average(drifts, weights=weights))

    async def _publish_card(self):
        stats = {}
        for pair in self.pairs:
            try:
                st = self._pair_stats(pair)
            except (TypeError, ValueError, IndexError, ZeroDivisionError,
                    FloatingPointError) as exc:
                self._fail("пара %s целиком" % pair, exc)
                st = None
            if st is not None:
                stats[pair] = st
        try:
            package = round(max(-1.0, min(1.0,
                                          self._package_bias(stats))), 3)
        except (TypeError, ValueError, ZeroDivisionError) as exc:
            self._fail("смещение пакета", exc)
            package = 0.0
        ours = self._series(self.kernel.cfg.symbol.upper())
        our_mom = sum(ours[-5:]) if ours else 0.0
        divergence = 0.0
        if package != 0 and our_mom != 0 and (package > 0) != (our_mom > 0):
            divergence = round(min(1.0, abs(our_mom) / 0.3), 3)
        cut = time.time() - 300.0
        panic = min(1.0, sum(1 for ts in self._panic if ts > cut) / 5.0)
        # ЦЕЛОСТНЫЕ ИЗМЕРИТЕЛИ ПАКЕТА
        dcor_div = self._dcor_divergence(stats)
        spring = self._packet_spring(stats, ours)
        self.stamps += 1
        fields = {
            "pairs": [{"pair": p, **s} for p, s in stats.items()],
            "package_bias": package,
            "divergence": divergence,
            "btc_panic": round(panic, 3),
            "our_momentum": round(our_mom, 3),
            "meta_dcor_divergence": dcor_div,
            "meta_packet_spring": spring,
            "errors": self.errors,
            "ts_ms": int(time.time() * 1000),
        }
        # ПОЛНАЯ РАЗВЁРТКА: 7 осей на монету, БЕЗ сжатия в агрегат
        for sym in self.pairs:
            st = stats.get(sym) or {}
            for m in META_METRICS:
                fields[f"meta_{m}_{sym}"] = st.get(m)
            vel, acc = self._lag_dynamics(sym, st.get("lag"))
            fields[f"meta_lag_vel_{sym}"] = vel
            fields[f"meta_lag_acc_{sym}"] = acc
        self.publish(DRAFT, {
            "kind": "meta-corr",
            "author": "meta-corr",
            "confidence": 0.9,
            "fields": fields,
            # НИТЬ (протокол lineage, Узел 5): тики вожака и монет пула —
            # источники радара связности
            "lineage": (["btc:ticks"]
                        + [f"alt-ticks:{sym}" for sym in self.pairs[:4]]),
        })
        self.log.info("Радар пакета: %d монет × %d осей развёрнуто, "
                      "DCor-дивергенция %s, пружина пакета %s ATR, ошибок %d",
                      len(stats), len(META_METRICS) + len(META_DERIVS),
                      dcor_div, spring, self.errors)