"""«Регрессия + G-Trend» — геометрия движения по всему полотну (модуль 2).

Третий вид знания в картине: полки говорят «где магниты», Поток —
«кто давит», Регрессия — «куда и с какой силой катится сама ткань».
Не сигнал: измерения и числовые прогнозы.

ЧТО СЧИТАЕТСЯ (простыми словами):
- ЛИНЕЙКА (канал линейной регрессии лог-цены на окне свечей): угол
  (%/свеча), скорость изменения угла и ускорение (ТОЛЬКО по закрытым
  свечам — пульс не врёт окнами), затухание наклона от пикового,
  длительность состояния lifetime_min (сколько КАЛЕНДАРНЫХ МИНУТ знак
  наклона не менялся — по времени, не «окнами подряд»), качество R²
  (насколько линейка прямая, 0..1).
- G-CHANNEL (настоящий G-Channel АлексГровера вместо SMA5−SMA34,
  как было до 20.10): рекурсивные границы a (верх) и b (низ) с шагом
  α = 1/длина и тренд g_trend ±1, меняющийся только при пробое
  ПРЕДЫДУЩИХ границ: закрепился вверх — держится до пробоя вниз.
  Состояние есть всегда, «плоское молчание» невозможно.
- ЛУЧ ДЕМАРКА: линия по двум опорным пивотам в будущее — вторая,
  независимая от линейки геометрия.
- ПРОГНОЗ (отдельная карточка trend-forecast, решение хозяина 20.10:
  измерение и прогноз стареют по-разному): ожидаемое движение за
  горизонт СОБСТВЕННОГО бара ступени (для 1м — через минуту, для 4ч —
  через 4 часа) — угол, продлённый на горизонт; рядом луч (если есть
  пивоты) и РАЗБРОС двух геометрий: сходятся — надёжнее, разошлись —
  шире неопределённость. Числа, не вердикты.
- КРОСС-ТФ ИЗМЕРЕНИЯ (взаимовлияние ступеней лестницы, БЕЗ голосовалок
  и «общих вердиктов», решение хозяина 20.10):
  atr_ratio = ATR_младшего / (ATR_старшего × √доли времени) — больше 1
  ⇒ младший бьётся сверх своей доли, локальный взрыв импульса;
  align = произведение нормированных ходов старшего и младшего
  (−1..1: знак × сила согласия масштабов);
  dist_mid = где цена от средней G-Channel старшей ступени (в её
  полуширинах: прижата к средней — сжатие у уровня).
  Компоненты публикуются РАЗДЕЛЬНО — склеивать в уверенность решает
  Тюнер/Лаба, здесь нет готовых множителей.

УРОВНИ (полотно целиком, решение хозяина 20.10): вся биржевая палитра
1м…3д (EXCHANGE_SPECTRUM), ручка [regression] levels_sec (пусто =
палитра целиком). Каждый ТФ живёт в СВОИХ ячейках Матрицы «поле@ТФ»
(matrix._absorb) — ступени не затирают друг друга.

КАРТОЧКИ (срок жизни назначает Поле, field.py TTL_BY_KIND):
- trend-vector — измерение: {tf_sec, score, angle_pct, velocity,
  acceleration, lifetime_min, decay_rate, r2, k_impulse,
  atr_ratio?, align?, dist_mid?, prognosis_beam}. Фильтры сняты (27.10):
  зрелость — это поля, а не запрет; вес задаёт Тюнер.
  Канал ±0.77% (channel_low/high) УБРАН: полоса была нарисована
  формулой, а не измерением; настоящий канал — границы G-Channel.
- g-trend — G-Channel: {tf_sec, g_trend, g_upper, g_lower, g_mid};
  старые value/sma5/sma34 убраны, Поле читает g_trend напрямую.
- trend-forecast — ПРОГНОЗ: {tf_sec, horizon_min, expect_pct,
  beam_pct?, spread_pct?, r2}; доверие = своё r2 здесь, кросс-числа —
  в trend-vector (компоненты раздельно).

Ручки — config.ini [regression]: window, atr_period, k_impulse_max,
g_length, heartbeat_sec, levels_sec, beam_*; дефолты —
core.config.DEFAULTS["regression"]. Сердцебиение heartbeat_sec
побуждает публиковать значения, чтобы полки Матрицы не умерли по TTL
(30 мин) на тихом рынке; между сердцебиениями дедуп по подписи
пропускает неизменившиеся числа (журнал не расточается).
"""
import collections
import math
import time

import numpy as np

from core.module import Manifest, NeirModule
from modules._labels import (CANDLES, CANDLES_WARMUP, CONDUCTIVITY, DRAFT,
                             PRICE, SURFACE)
from modules.market.collector import EXCHANGE_SPECTRUM

def _fit(rows):
    """Линейная регрессия лог-цены на окне → (slope/бар, r2)."""
    n = len(rows)
    y = np.log([max(r[4], 1e-12) for r in rows])
    x = np.arange(n, dtype=float)
    xm, ym = x.mean(), y.mean()
    sxx = ((x - xm) ** 2).sum()
    if sxx <= 0:
        return 0.0, 0.0
    slope = ((x - xm) * (y - ym)).sum() / sxx
    pred = ym + slope * (x - xm)
    ss_res = ((y - pred) ** 2).sum()
    ss_tot = ((y - ym) ** 2).sum()
    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
    return float(slope), float(max(0.0, min(1.0, r2)))


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


def _pivot_idx(vals, k, kind):
    """Индексы подтверждённых пивотов (k соседей с каждой стороны).

    Пивот-хай: vals[i] строго выше k баров слева и справа; пивот-лоу —
    строго ниже. NumPy-развёртка скользящего окна, без циклов по ТФ.
    Возвращает индексы в исходном массиве, по возрастанию."""
    m = len(vals)
    if m < 2 * k + 1:
        return np.empty(0, dtype=int)
    sw = np.lib.stride_tricks.sliding_window_view(vals, 2 * k + 1)
    center = sw[:, k]
    left = sw[:, :k]
    right = sw[:, k + 1:]
    if kind == "high":
        mask = (center > left.max(axis=1)) & (center > right.max(axis=1))
    else:
        mask = (center < left.min(axis=1)) & (center < right.min(axis=1))
    return np.flatnonzero(mask) + k

def _prognosis_beam(rows, tf, slope, strat_tf, pivot_k, fresh_mult, weight_min):
    """Луч ДеМарка в будущее по двум последним подтверждённым пивотам.

    Направление берём у тренда (slope): рост — по двум опорным ЛОУ (линия
    поддержки), падение — по двум ХАЙ (линия сопротивления). Возвращает
    [origin_price, slope_pct_min, confidence] — чистые числа, либо None,
    если подтверждённых пивотов меньше двух (холодный старт — без фантомов).
    confidence = вес_масштаба × свежесть: крупный ТФ устойчивее (вес к 1.0),
    а старый пивот теряет силу; возраст нормируется на масштаб ТФ, поэтому
    масштабы сравнимы (пивот 4ч «свежий» дольше, чем пивот 1м)."""
    n = len(rows)
    if n < 2 * pivot_k + 2 or slope == 0:
        return None
    highs = np.array([r[2] for r in rows], dtype=float)
    lows = np.array([r[3] for r in rows], dtype=float)
    if slope > 0:
        piv = _pivot_idx(lows, pivot_k, "low")
        vals = lows
    else:
        piv = _pivot_idx(highs, pivot_k, "high")
        vals = highs
    if len(piv) < 2:
        return None
    i2, i1 = int(piv[-1]), int(piv[-2])
    dt_min = (rows[i2][0] - rows[i1][0]) / 60000.0
    price1, price2 = float(vals[i1]), float(vals[i2])
    if dt_min <= 0 or price1 <= 0:
        return None
    slope_pct_min = ((price2 - price1) / price1) / dt_min * 100.0
    origin = float(rows[-1][4])
    # вес масштаба: стратег (strat_tf) → 1.0, база → weight_min
    if strat_tf and strat_tf > 0:
        scale_w = weight_min + (1.0 - weight_min) * min(1.0, tf / strat_tf)
    else:
        scale_w = 1.0
    # свежесть: возраст нового пивота в «окнах ТФ» (× fresh_mult)
    age_sec = max(0.0, (rows[-1][0] - rows[i2][0]) / 1000.0)
    fresh_scale = fresh_mult * tf
    freshness = max(0.0, 1.0 - age_sec / fresh_scale) if fresh_scale > 0 else 0.0
    confidence = max(0.0, min(1.0, scale_w * freshness))
    return [round(origin, 8), round(slope_pct_min, 5), round(confidence, 3)]


def _gchannel(closes, length):
    """G-Channel АлексГровера — эталон 20.10 (никаких SMA-замен).

    Рекурсия границ:
        a := max(src, a_prev) − (a_prev − b_prev) / length
        b := min(src, b_prev) + (a_prev − b_prev) / length
        mid = (a + b) / 2
    Тренд по пробою ПРЕДЫДУЩИХ границ (до их обновления):
        close > a_prev → +1; close < b_prev → −1; иначе состояние прежнее
        (держится до пробоя — «плоское молчание» невозможно).

    Старт a = b = первая цена (в Pine это nz(0)-вырождение; после length
    баров разницы нет). Stateless: считает по всему переданному списку,
    прогоняется заново на каждом пересчёте — поэтому побитово сверяется
    с эталонной транскрипцией в smoke_regression.
    Возвращает (a, b, trend)."""
    if not closes:
        return None
    a = b = float(closes[0])
    trend = 0
    for i in range(1, len(closes)):
        c = float(closes[i])
        spread = a - b                 # ширина ПРЕДЫДУЩЕГО канала
        if c > a:                      # пробой верхней границы
            trend = 1
        elif c < b:                    # пробой нижней границы
            trend = -1
        a = max(c, a) - spread / length
        b = min(c, b) + spread / length
    return a, b, trend

class Regression(NeirModule):
    manifest = Manifest(name="regression", title="Регрессия + G-Trend",
                        requires=("candles",), priority=46, tick_timeout=10.0)

    @staticmethod
    def _new_state():
        """Состояние ступени. sign_wall — момент (wall-clock) последней
        смены знака: lifetime_min = «сколько минут состояние живёт», а не
        «сколько окон подряд» (то врало при разном темпе пересчётов)."""
        return {"slope_prev": None, "vel_prev": None, "vel_last": None,
                "acc_last": None, "sign": 0, "sign_wall": time.time(),
                "slopes": [], "last_pub": 0.0}

    async def on_start(self):
        self.symbol = self.kernel.cfg.symbol.upper()
        c = self.kernel.cfg
        self.window = c.regression_window
        self.atr_period = c.regression_atr_period
        self.k_impulse_max = c.regression_k_impulse_max
        self.g_length = c.regression_g_length
        self.heartbeat_sec = c.regression_heartbeat_sec
        self.beam_pivot_k = c.regression_beam_pivot_k
        self.beam_fresh_mult = c.regression_beam_fresh_mult
        self.beam_weight_min = c.regression_beam_weight_min
        self.strat_tf = c.strategy_tf_sec
        # ПОЛОТНО ЦЕЛИКОМ (хозяин 20.10): levels_sec пуст → вся биржевая
        # палитра 1м…3д + база, если её нет в палитре.
        levels = list(c.regression_levels_sec)
        if not levels:
            levels = sorted(set(EXCHANGE_SPECTRUM)
                            | {int(c.base_tf_sec)})
        self.levels = {tf: collections.deque(maxlen=400)
                       for tf in dict.fromkeys(levels)}
        ordered = sorted(self.levels)
        # Лестница взаимовлияния: у каждой ступени один СТАРШИЙ СОСЕД —
        # пары «все со всеми» не нужны, знаки и так живут на ступенях.
        self._senior = {tf: (ordered[i + 1] if i + 1 < len(ordered) else None)
                        for i, tf in enumerate(ordered)}
        self._state = {}          # tf -> состояние ступени
        self._sig = {}            # (вид, tf) -> последняя подпись
        self.scans = 0
        self.pub_count = 0
        self._beats = 0
        self._px = 0.0
        self.subscribe(PRICE, self._on_price)
        self.subscribe(CANDLES, self._on_candles)
        self.subscribe(SURFACE, self._on_surface)
        # ПРОГРЕВ ИЗ БАЗЫ (урок Фракталов): порог 30-60 баров нельзя ждать
        # час живых данных — история уже лежит на доске значений.
        for tf in self.levels:
            self.subscribe(f"{CANDLES_WARMUP}:{tf}", self._on_candles)
            warm = self.get_last(f"{CANDLES_WARMUP}:{tf}")
            if warm is not None:
                await self._on_candles(warm)
        self.log.info("Регрессия: ступени %d (%sм…%s), окно %d, "
                      "G-Channel длина %d, сердцебиение %d с",
                      len(ordered), ordered[0] // 60, ordered[-1] // 60,
                      self.window, self.g_length, int(self.heartbeat_sec))

    async def on_stop(self):
        self.log.info("Итог: сканов %d, карточек %d", self.scans,
                      self.pub_count)

    # ==== приём данных ====

    async def _on_candles(self, env):
        data = env.payload
        try:
            tf = int(data["tf_sec"])
            rows = data["candles"]
        except (KeyError, TypeError):
            return
        if tf not in self.levels:
            return
        feed = self.levels[tf]
        if data.get("warmup"):
            feed.clear()
        elif not data.get("closed"):
            return
        for r in rows:
            feed.append(tuple(r))
        st = self._state.setdefault(tf, self._new_state())
        self._scan(tf, st, force=True, closed=True)

    async def _on_price(self, env):
        try:
            px = float(env.payload.get("price") or 0.0)
        except (TypeError, ValueError):
            return
        if px > 0:
            self._px = px

    @staticmethod
    def _with_live(rows, px):
        """Окно с подставленной живой ценой в последнюю свечу.

        Без этого секундный пересчёт бессмыслен: закрытые бары не меняются,
        наклон тот же. Живая цена делает геометрию непрерывной (грань
        последней свечи продолжается)."""
        out = list(rows)
        last = list(out[-1])
        prev_c = float(last[4])
        last[2] = max(float(last[2]), prev_c, px)     # high
        last[3] = min(float(last[3]), prev_c, px)     # low
        last[4] = px                                   # close
        out[-1] = tuple(last)
        return out

    async def _on_surface(self, env):
        """Пульс Полотна (~1/сек): уточнение геометрии живой ценой.

        Кинетику (vel/acc/decay) пульс НЕ двигает — она считается только на
        закрытых свечах. Публикация — по изменению подписи чисел либо по
        сердцебиению heartbeat_sec (ячейки Матрицы не умирают по TTL)."""
        self._beats += 1
        now = time.time()
        for tf, st in self._state.items():
            if len(self.levels[tf]) < 30:
                continue
            force = (now - st["last_pub"]) >= self.heartbeat_sec
            self._scan(tf, st, force=force, live_px=self._px)

    def _k_impulse(self):
        """Громкость тренда: матрица селективной громкости (фазы шторма,
        config [efficiency]) — тренд может глохнуть (вынос 0.1) или реветь
        (пробой 1.8). Нет записи/фазы — legacy k_impulse (1.0..2.0)."""
        env = self.get_last(CONDUCTIVITY)
        if env is None:
            return 1.0
        eff = env.payload.get("efficiency") or {}
        if "trend-vector" in eff:
            try:
                return max(0.1, min(2.0, float(eff["trend-vector"])))
            except (TypeError, ValueError):
                return 1.0
        try:
            return max(1.0, min(self.k_impulse_max,
                                float(env.payload.get("k_impulse", 1.0))))
        except (TypeError, ValueError):
            return 1.0

    # ==== скан ступени ====

    def _scan(self, tf, st, force=False, live_px=None, closed=False):
        self.scans += 1
        rows = list(self.levels[tf])
        if len(rows) < 30:
            return
        if live_px and live_px > 0:
            rows = self._with_live(rows, live_px)
        window = rows[-self.window:]
        atr = _atr(rows, self.atr_period)
        if not atr or atr <= 0:
            return
        now = time.time()
        fresh = force or (now - st["last_pub"]) >= self.heartbeat_sec
        slope, r2 = _fit(window)

        # КИНЕТИКА ОКНА — ТОЛЬКО по закрытым свечам: новый бар — новое
        # окно; пульс между барами скорость/ускорение не выдумывает.
        if closed:
            vel = None if st["slope_prev"] is None \
                else slope - st["slope_prev"]
            acc = None if (vel is None or st["vel_prev"] is None) \
                else vel - st["vel_prev"]
            st["slope_prev"] = slope
            if vel is not None:
                st["vel_prev"] = vel
                st["vel_last"] = vel
            if acc is not None:
                st["acc_last"] = acc
            st["slopes"].append(slope)
            if len(st["slopes"]) > 6:
                st["slopes"].pop(0)
        else:
            vel, acc = st["vel_last"], st["acc_last"]

        # ЗНАК И ДЛИТЕЛЬНОСТЬ СОСТОЯНИЯ — по календарному времени
        # (не зависит от того, раз в секунду мы пересчитываем или в минуту).
        sign = 1 if slope > 0 else (-1 if slope < 0 else 0)
        if sign and sign != st["sign"]:
            st["sign"] = sign
            st["sign_wall"] = now
        lifetime_min = round((now - st["sign_wall"]) / 60.0, 1)

        angle_pct = slope * 100.0                          # %/свеча
        score = max(-1.0, min(1.0, (slope * self.window) / (2.0 * atr)))
        peak = max(abs(s) for s in st["slopes"]) if st["slopes"] else 0.0
        decay = round(max(0.0, min(1.0, 1.0 - abs(slope) / peak)), 3) \
            if peak > 0 else 0.0
        k_imp = self._k_impulse()
        beam = _prognosis_beam(rows, tf, slope, self.strat_tf,
                               self.beam_pivot_k, self.beam_fresh_mult,
                               self.beam_weight_min)
        # G-Channel по живому окну (границы адаптивны к последней цене)
        g_a, g_b, g_trend = _gchannel([r[4] for r in rows], self.g_length)
        g_mid = 0.5 * (g_a + g_b)
        px = float(rows[-1][4])
        g_pos = 0.0
        if abs(g_a - g_b) > 1e-12:
            g_pos = max(-1.0, min(1.0, (px - g_mid) / (0.5 * (g_a - g_b))))
        cross = self._cross(tf, score, atr, px)

        last_ts = rows[-1][0]
        published = self._publish_tv(tf, last_ts, slope, r2, vel, acc, score,
                                     angle_pct, decay, lifetime_min, k_imp,
                                     beam, cross, fresh)
        published |= self._publish_gt(tf, last_ts, g_trend, g_a, g_b, g_mid,
                                      g_pos, atr, fresh)
        published |= self._publish_fc(tf, last_ts, angle_pct, r2, beam, fresh)
        if published:
            st["last_pub"] = now

    def _cross(self, tf, score, atr, px):
        """КРОСС-ТФ ИЗМЕРЕНИЯ взаимовлияния ступеней (не вердикт!).

        Лестница считает отношения СОСЕДЕЙ: младшая ступень против
        старшей. Возвращается набор чисел — компоненты для уверенности
        потребителя; здесь ничего не склеивается и не усредняется.
        Старшей ступени или её данных нет — полей нет вовсе
        (не измерено ≠ ноль)."""
        out = {"fields": {}, "sig": None}
        senior = self._senior.get(tf)
        if senior is None:
            return out
        srows = list(self.levels.get(senior) or ())
        if len(srows) < 30:
            return out
        s_atr = _atr(srows, self.atr_period)
        if not s_atr or s_atr <= 0:
            return out
        sslope, _ = _fit(srows[-self.window:])
        sscore = max(-1.0, min(1.0, (sslope * self.window) / (2.0 * s_atr)))
        # волатильность младшего против доли старшего (√время: так
        # волатильность масштабируется по закону квадратного корня)
        atr_ratio = atr / (s_atr * math.sqrt(tf / float(senior)))
        align = score * sscore                      # знак × сила, −1..1
        fields = {"atr_ratio": round(atr_ratio, 3),
                  "align": round(align, 3)}
        g = _gchannel([r[4] for r in srows], self.g_length)
        if g is not None:
            half = 0.5 * (g[0] - g[1])
            if abs(half) > 1e-12:
                dist = (px - 0.5 * (g[0] + g[1])) / half
                fields["dist_mid"] = round(max(-5.0, min(5.0, dist)), 3)
        out["fields"] = fields
        out["sig"] = tuple(fields.get(k) for k in
                           ("atr_ratio", "align", "dist_mid"))
        return out

    # ==== карточки ====

    def _publish_tv(self, tf, last_ts, slope, r2, vel, acc, score, angle_pct,
                    decay, lifetime_min, k_imp, beam, cross, fresh):
        """Карточка ИЗМЕРЕНИЯ trend-vector: подпись — все значимые числа
        (без lifetime: его обновляет сердцебиение)."""
        sig = (round(score, 2), round(r2, 2),
               1 if slope > 0 else (-1 if slope < 0 else 0),
               round(decay, 2),
               None if vel is None else round(vel, 6),
               None if acc is None else round(acc, 8),
               round(k_imp, 2), cross["sig"])
        changed = self._sig.get(("tv", tf)) != sig
        if not (fresh or changed):
            return False
        self._sig[("tv", tf)] = sig
        self.pub_count += 1
        fields = {
            "tf_sec": tf,
            "score": round(score, 3),
            "angle_pct": round(angle_pct, 5),
            "velocity": round(vel, 6) if vel is not None else None,
            "acceleration": round(acc, 8) if acc is not None else None,
            "lifetime_min": lifetime_min,
            "decay_rate": decay,
            "r2": round(r2, 3),
            "k_impulse": k_imp,
            "prognosis_beam": beam,
        }
        fields.update(cross["fields"])       # кросс-ТФ — числами, раздельно
        self.publish(DRAFT, {
            "kind": "trend-vector",
            "author": "regression",
            "confidence": round(min(0.9, r2 * (0.6 + 0.4
                                               * min(1.0, abs(score) * 2))), 3),
            "fields": fields,
            "lineage": [f"candle:{self.symbol}:{tf}:{last_ts}"],
        })
        if changed:
            self.log.info("ТРЕНД %dм: %s %.4f%%/свеча, состояние %.1f мин, "
                          "затухание %.2f, R² %.2f", tf // 60,
                          "вверх" if slope > 0 else "вниз", angle_pct,
                          lifetime_min, decay, r2)
        else:
            self.log.debug("ТРЕНД %dм: сердцебиение, состояние %.1f мин",
                           tf // 60, lifetime_min)
        return True

    def _publish_gt(self, tf, last_ts, g_trend, g_a, g_b, g_mid, g_pos,
                    atr, fresh):
        """Карточка G-Channel g-trend: состояние всегда есть ( тренд держится
        до пробоя). Подпись — в единицах ATR: границы шевельнулись на
        сотую долю типового диапазона — только такой сдвиг стоит новой
        карточки."""
        sig = (g_trend, round(g_a / atr, 2), round(g_b / atr, 2))
        changed = self._sig.get(("gt", tf)) != sig
        if not (fresh or changed):
            return False
        self._sig[("gt", tf)] = sig
        self.pub_count += 1
        self.publish(DRAFT, {
            "kind": "g-trend",
            "author": "regression",
            "confidence": round(0.4 + 0.3 * min(1.0, abs(g_pos)), 3),
            "fields": {"tf_sec": tf, "g_trend": int(g_trend),
                       "g_upper": round(g_a, 8), "g_lower": round(g_b, 8),
                       "g_mid": round(g_mid, 8)},
            "lineage": [f"candle:{self.symbol}:{tf}:{last_ts}"],
        })
        if changed:
            self.log.info("G-КАНАЛ %dм: тренд %+d, границы %.6g…%.6g",
                          tf // 60, g_trend, g_b, g_a)
        return True

    def _publish_fc(self, tf, last_ts, angle_pct, r2, beam, fresh):
        """Карточка ПРОГНОЗА trend-forecast — отдельно от измерений
        (хозяин 20.10: измерение вчерашнее ещё верно, прогноз вчерашний —
        мусор, у них разные сроки жизни и разные полки в Матрице).

        Горизонт = собственный бар ступени (профиль по всему полотну).
        expect — угол, продлённый на горизонт; beam — вторая геометрия
        (луч, если есть пивоты); spread — разброс геометрий (где сходятся,
        надёжнее). Доверие компонентами: r2 здесь, кросс-числа (atr_ratio,
        align) — в trend-vector; множителей нет — их задаёт Тюнер."""
        horizon_min = tf / 60.0
        expect_pct = angle_pct                     # угол уже «% за бар»
        beam_pct = spread_pct = None
        if beam is not None:
            beam_pct = beam[1] * horizon_min       # %/мин × минуты
            spread_pct = abs(expect_pct - beam_pct)
        sig = (round(expect_pct, 4),
               None if beam_pct is None else round(beam_pct, 4),
               None if spread_pct is None else round(spread_pct, 4),
               round(r2, 2))
        changed = self._sig.get(("fc", tf)) != sig
        if not (fresh or changed):
            return False
        self._sig[("fc", tf)] = sig
        self.pub_count += 1
        fields = {"tf_sec": tf, "horizon_min": round(horizon_min, 1),
                  "expect_pct": round(expect_pct, 5), "r2": round(r2, 3)}
        if beam_pct is not None:
            fields["beam_pct"] = round(beam_pct, 5)
            fields["spread_pct"] = round(spread_pct, 5)
        self.publish(DRAFT, {
            "kind": "trend-forecast",
            "author": "regression",
            "confidence": round(max(0.0, min(1.0, r2)), 3),
            "fields": fields,
            "lineage": [f"candle:{self.symbol}:{tf}:{last_ts}"],
        })
        if changed:
            self.log.info("ПРОГНОЗ %dм: ждём %+.4f%% за %.0f мин (R² %.2f%s)",
                          tf // 60, expect_pct, horizon_min, r2,
                          "" if beam_pct is None else
                          ", луч %+.4f%%, разброс %.4f%%"
                          % (beam_pct, spread_pct))
        return True
