"""«Проводимость Среды» — Функция Влияния Среды для Сценариста (утверждено).

Сливает сырые бухгалтерии Волны 1 в ОДНУ непрерывную строчку:
  R(t) ∈ 0..1 — интенсивность среды (0 = штиль, 1 = шторм)
и публикует две кривые её применения:
  k_shelf  = 1 − 0.9·R → 1.0..0.1: старые лимитные полки ТАЮТ в шторм
  k_impulse — кинетически верифицированный импульс (см. _k_impulse).

Компоненты R — только ИЗМЕРЕНИЯ, ноль голосования (утверждено):
- паника P: частота ликвидаций (!forceOrder) против нормы;
- упрямство S: ΔOI против хода цены, С фрактальным контекстом полотна;
- свежесть импульса I: наклон CVD (карточки Потока) + наклон линзы.

УЗЕЛ 8 (ТЗ утверждено хозяином, 20.10.2026) — полная зачистка хардкода:
- окно ликвидаций = [oi] liq_window_sec — единый источник правды с
  OI-Каскадом (константа LIQ_WINDOW убрана);
- история OI/цены, свежесть Потока, норма лавины, минимальный размах
  меток и сердцебиение — ручки [conductivity]: PRICE_HIST/FLOW_TTL/за-
  шитое «×2» и «2с — полигон» вырезаны из тел методов;
- окно линзы = price_hist_sec (зашитое surface_read(payload, 60) убрано);
- DRAFT идёт в Матрицу плоскими числами: 9 числовых осей, без вложенных
  словарей, ts_ms, строк и efficiency (время карточки Матрица берёт из
  created_ms); шинная строка CONDUCTIVITY каждые publish_interval НЕ
  дедупима — её читают пятеро потребителей через короткий TTL;
- дедуп DRAFT по квантованной подписи до 0.001 + heartbeat_sec: тихий
  рынок молчит, ячейки Матрицы держит сердцебиение.

СВЯЗЬ С ПОЛОТНОМ 13 ЭТАЖЕЙ («погода ↔ полотно», утверждено):
- ФРАКТАЛЬНЫЙ КОНТЕКСТ УПРЯМСТВА: k_ctx = 1 − ctx_strength·(d·G), где
  d — знак локального хода цены, G — ветер старших этажей. Согласован-
  ность вместо голосовалки (канон 27.10, по образцу cross_tf): каждый
  этаж голосует ТРОИЧНЫМ знаком g_trend (−1/0/+1, мёртвая зона
  g_dead_zone — «этаж не видит тренда» — воздерживается); G = сумма
  знаков / число ГОЛОСОВАВШИХ этажей. Совпали все → |G|=1 (полный
  шторм контекста), разошлись → |G|<1 (частичное согласие слабее),
  согласных нет → G=0 (контекст молчит, а не врёт нулём среднего).
  Против ветра (d·G=−1) — ×1.5: толпа ловит против тайфуна — другая
  физика; по ветру — ×0.5: трендовый гул; согласия нет — ×1 (прежнее
  поведение, обратная совместимость).
- КИНЕТИЧЕСКАЯ ВЕРИФИКАЦИЯ ИМПУЛЬСА: v = b × Â — перекос ленты × знак-
  консенсус ускорений старших этажей (кинетика Регрессии ТОЛЬКО по
  закрытым барам). Â — та же механика согласованности по знакам уско-
  рений (±accel_dead_zone). k_impulse = 1 + r·(1 + impulse_confirm·v):
  подтверждено старшими — до 1.5·r (у потолка 2.0), вразрез — 0.5·r:
  ложный вынос гасится к нейтрали, но не ниже 1.0 (потребители читают
  через max(1.0, …) — иначе гашение вырезалось бы ими молча). Числа
  старших живут senior_fresh_sec; данных нет — оба множителя равны 1.

R сглаживается экспоненциально — «плавно меняет веса». Полки гаснут
с силой, обратной их возрасту (глубокие полки шторм не выметает) —
масштабный коэффициент применяет Поле. Границы нерушимы: K не отменяет
издержки, стоп и вердикт риск-менеджера.
"""
import collections
import math
import time

import numpy as np

from core.module import Manifest, NeirModule
from modules._labels import (CARD, CONDUCTIVITY, DRAFT, LIQUIDATIONS,
                             OI_SNAPSHOT, PRICE, SURFACE)
from modules.market.collector import EXCHANGE_SPECTRUM
from modules.market.lenses import surface_read


class Conductivity(NeirModule):
    manifest = Manifest(name="conductivity", title="Проводимость Среды (3D-срез)",
                        requires=("price", "card"), priority=49, tick_timeout=5.0)

    async def on_start(self):
        c = self.kernel.cfg
        self.w_panic = c.cond_panic_weight
        self.w_stubborn = c.cond_stubborn_weight
        self.w_impulse = c.cond_impulse_weight
        self.stubborn_oi = c.cond_stubborn_oi_pct
        self.smooth_half = c.cond_smooth_half_sec
        self.publish_every = c.cond_publish_interval
        # ОКНА (узел 8): ликвидации — та же ручка, что у OI-Каскада;
        # остальное — свои ручки [conductivity]. Никаких констант в теле.
        self.liq_window = c.oi_liq_window_sec
        self.price_hist = c.cond_price_hist_sec
        self.flow_ttl = c.cond_flow_ttl_sec
        self.min_span = c.cond_min_span_sec
        self.heartbeat = c.cond_heartbeat_sec
        self.liq_burst_ref = max(1.0, c.oi_liq_burst_min
                                 * c.cond_liq_burst_multiplier)
        # СВЯЗЬ С ПОЛОТНОМ (утверждено)
        self.ctx_strength = c.cond_ctx_strength
        self.impulse_confirm = c.cond_impulse_confirm
        self.senior_fresh = c.cond_senior_fresh_sec
        # СОГЛАСОВАННОСТЬ (канон 27.10): мёртвые зоны знака — «этаж не
        # видит движения» воздерживается, а не размывает консенсус нулём.
        self.g_dead_zone = c.cond_g_dead_zone
        self.accel_dead_zone = c.cond_accel_dead_zone
        self.trend_z_scale = c.cond_trend_z_scale
        self.impulse_min = c.cond_impulse_min
        self.base_tf = min(EXCHANGE_SPECTRUM)   # базовый этаж полотна (1м)
        self.storm_liq_min = c.cond_storm_liq_min
        self.storm_oi_min = c.cond_storm_oi_min
        self.errors = 0                          # сбойные карточки — в счётчик

        self._liqs = collections.deque()          # (ts, qty)
        self._oi_marks = collections.deque()      # (ts, oi) — снимки Каскада
        self._price_marks = collections.deque()   # (ts, price)
        self._flow = None                         # последний flow-state (факт)
        self._senior_gt = {}                      # tf → (g_trend, created_ms)
        self._senior_acc = {}                     # tf → (acceleration, created_ms)
        self._r_smooth = 0.0
        self._r_have = False
        self._last_pub = 0.0
        self._draft_sig = None
        self._last_draft = 0.0
        self.snapshots = 0

        self.subscribe(LIQUIDATIONS, self._on_liq)
        self.subscribe(OI_SNAPSHOT, self._on_oi)
        self.subscribe(PRICE, self._on_price)
        self.subscribe(CARD, self._on_card)
        self.every(1.0, self._tick)
        self.log.info("Проводимость включена: веса %.2f/%.2f/%.2f "
                      "(паника/упрямство/импульс), окна %dс ликвидаций / "
                      "%dс истории, хартбит %ds, контекст %.2f, верификация %.2f, "
                      "базовый этаж %dс",
                      self.w_panic, self.w_stubborn, self.w_impulse,
                      self.liq_window, self.price_hist, self.heartbeat,
                      self.ctx_strength, self.impulse_confirm, self.base_tf)

    async def on_stop(self):
        v = self._kinetic_v(self._component_speed_bias())
        self.log.info("Итог: показаний в журнал %d, R на остановке %.2f "
                      "(k_shelf %.2f, k_impulse %.2f)",
                      self.snapshots, self._r_smooth,
                      1 - 0.9 * self._r_smooth,
                      self._k_impulse(min(1.0, self._r_smooth), v))

    def _fail(self, what, exc):
        """Сбой приёма факта — в журнал с трассировкой, не в молчание
        (паттерн meta_corr/fractal, аудит 27.10)."""
        self.errors += 1
        self.log.error("Проводимость: %s: %s: %s", what,
                       type(exc).__name__, exc, exc_info=True)

    # ---------- приём фактов ----------

    async def _on_liq(self, env):
        p = env.payload
        now = time.time()
        try:
            qty = float(p.get("qty", 0.0))
        except (TypeError, ValueError) as exc:
            self._fail("ликвидация без честного qty", exc)
            return
        self._liqs.append((now, qty))
        edge = now - self.liq_window
        while self._liqs and self._liqs[0][0] < edge:
            self._liqs.popleft()

    async def _on_oi(self, env):
        p = env.payload
        try:
            oi = float(p["oi"])
            ts = int(p["ts_ms"]) / 1000.0
        except (KeyError, TypeError, ValueError) as exc:
            self._fail("снимок OI бракованный", exc)
            return
        if oi <= 0:
            return
        self._oi_marks.append((ts, oi))
        edge = ts - self.price_hist
        while self._oi_marks and self._oi_marks[0][0] < edge:
            self._oi_marks.popleft()

    async def _on_price(self, env):
        p = env.payload
        try:
            price = float(p.get("price", 0.0))
        except (TypeError, ValueError) as exc:
            self._fail("тик цены бракованный", exc)
            return
        if price <= 0:
            return
        now = time.time()
        self._price_marks.append((now, price))
        edge = now - self.price_hist
        while self._price_marks and self._price_marks[0][0] < edge:
            self._price_marks.popleft()

    async def _on_card(self, env):
        card = env.payload
        kind = card.get("kind")
        f = card.get("fields") or {}
        try:
            created = float(card.get("created_ms", 0) or 0)
        except (TypeError, ValueError) as exc:
            self._fail("карточка %s без честного created_ms" % kind, exc)
            return
        if kind == "g-trend":
            # ВЕТЕР СТАРШИХ ЭТАЖЕЙ (связь с полотном): G-Канал Регрессии
            try:
                tf = int(f.get("tf_sec", 0))
                gt = float(f.get("g_trend", 0))
            except (TypeError, ValueError) as exc:
                self._fail("карточка g-trend бракованная", exc)
                return
            if tf > self.base_tf:
                self._senior_gt[tf] = (gt, created)
            return
        if kind == "trend-vector":
            # КИНЕТИКА СТАРШИХ ЭТАЖЕЙ: ускорение (только закрытые бары)
            try:
                tf = int(f.get("tf_sec", 0))
            except (TypeError, ValueError) as exc:
                self._fail("карточка trend-vector бракованная", exc)
                return
            if tf > self.base_tf:
                acc = f.get("acceleration")
                try:
                    acc = None if acc is None else float(acc)
                except (TypeError, ValueError) as exc:
                    self._fail("trend-vector tf=%s: acceleration не число"
                               " — берём None (этаж воздержится)" % tf, exc)
                    acc = None
                self._senior_acc[tf] = (acc, created)
            return
        if kind != "flow-state":
            return
        try:
            self._flow = {
                "cvd_slope": float(f.get("cvd_slope", 0.0)),
                "direction_bias": float(f.get("direction_bias", 0.5)),
                "ts_ms": int(card.get("created_ms", 0)),
                "seq": card.get("seq"),      # указатель в журнал — для lineage
            }
        except (TypeError, ValueError) as exc:
            self._fail("карточка flow-state бракованная", exc)
            return

    # ---------- связь с полотном (утверждено, узел 8) ----------

    def _concordance(self, values, dead_zone):
        """Механика СОГЛАСОВАННОСТИ (канон 27.10, по образцу кросс-ТФ):
        каждый этаж даёт ТРОИЧНЫЙ знак (−1/0/+1); |x| ≤ dead_zone —
        воздержание («этаж не видит»), а не ноль-голос, размазывающий
        консенсус. Выход — (C ∈ −1..1, n_votes): сумма знаков / число
        ГОЛОСОВАВШИХ. Все согласны → |C|=1; разошлись → |C|<1 (частичное
        согласие слабее полного — это измерение согласия, а не усредне-
        ние величин); голосов нет → (0.0, 0) — контекст молчит честно."""
        votes = [1.0 if v > dead_zone else -1.0 for v in values
                 if abs(v) > dead_zone]
        if not votes:
            return 0.0, 0
        return sum(votes) / len(votes), len(votes)

    def _wind_full(self):
        """Ветер старших этажей: (G, n_senior, n_votes) по свежим карточ-
        кам g_trend (свежесть senior_fresh_sec). Согласованность знаков
        вместо среднего величин: послойная информация не сплющивается."""
        now_ms = time.time() * 1000.0
        fresh = [gt for gt, ts in self._senior_gt.values()
                 if now_ms - ts <= self.senior_fresh * 1000.0]
        g, n_votes = self._concordance(fresh, self.g_dead_zone)
        return g, len(fresh), n_votes

    def _wind(self):
        """Ветер G ∈ −1..1 (скалярное чтение для k_ctx)."""
        return self._wind_full()[0]

    def _accel_wind_full(self):
        """Кинетика старших Â: (Â, n_senior, n_votes) по свежим trend-
        vector (регрессия считает кинетику только по закрытым барам).
        У этажа нет кинетики (None) — не участвует; знак — та же согла-
        сованность с мёртвой зоной accel_dead_zone."""
        now_ms = time.time() * 1000.0
        accs = [acc for acc, ts in self._senior_acc.values()
                if acc is not None and now_ms - ts <= self.senior_fresh * 1000.0]
        return self._concordance(accs, self.accel_dead_zone) + (len(accs),)

    def _accel_wind(self):
        """Кинетика Â ∈ −1..1 (скалярное чтение для v)."""
        return self._accel_wind_full()[0]

    def _kinetic_v(self, speed_bias):
        """Кинетическая верификация импульса: v = b × Â ∈ −1..1 —
        подтверждение старшими этажами (+) или вразрез (−)."""
        return max(-1.0, min(1.0, speed_bias * self._accel_wind()))

    def _k_impulse(self, r, v):
        """k = 1 + r·(1 + impulse_confirm·v), кламп 1.0..2.0.
        Подтверждено (v=+1) → 1+1.5r (у 2.0 упирается), вразрез (v=−1)
        → 1+0.5r — ложный вынос гаснет к нейтрали, НО не ниже единицы
        (потребители читают через max(1.0, …) — иначе гашение вырезалось
        бы ими молча); нет данных (v=0) → 1+r — прежняя формула."""
        k = 1.0 + r * (1.0 + self.impulse_confirm * v)
        return round(max(1.0, min(2.0, k)), 3)

    # ---------- компоненты R (только измерения) ----------

    def _component_panic(self):
        """Частота ликвидаций против нормы: 0 = штиль, 1 = полноценная лавина."""
        rate = len(self._liqs)                  # ликвидаций за окно
        return min(1.0, rate / self.liq_burst_ref)

    def _component_stubborn(self):
        """Упрямство толпы (loss aversion): ΔOI против хода цены.
        ФРАКТАЛЬНЫЙ КОНТЕКСТ (утверждено): k_ctx = 1 − ctx·(d·G) —
        против ветра старших (d·G=−1) ×1.5, по ветру ×0.5, ветра нет — ×1
        (при ctx_strength=0.5 формула даёт ровно эти веса)."""
        if len(self._oi_marks) < 2 or len(self._price_marks) < 2:
            return 0.0
        ts0, oi0 = self._oi_marks[0]
        ts1, oi1 = self._oi_marks[-1]
        if ts1 - ts0 < self.min_span or oi0 <= 0:
            return 0.0
        d_oi = (oi1 - oi0) / oi0 * 100.0
        p0 = self._price_marks[0][1]
        p1 = self._price_marks[-1][1]
        d_price = (p1 - p0) / p0 * 100.0
        if abs(d_oi) < 1e-9 or abs(d_price) < 1e-9:
            return 0.0
        # OI растёт ПРОТИВ хода цены — толпа усредняется (упрямство)
        if d_oi * d_price < 0:
            raw = min(1.0, abs(d_oi) / self.stubborn_oi)
            d = 1.0 if d_price > 0 else -1.0
            k_ctx = 1.0 - self.ctx_strength * (d * self._wind())
            return raw * k_ctx
        return 0.0

    def _component_impulse(self):
        """Свежесть импульса: наклон CVD (факт Потока) + наклон линзы
        в масштабе price_hist_sec (зашитая «60» убрана, узел 8).

        СОГЛАСОВАННОСТЬ вместо среднего величин (канон 27.10): источник
        даёт знак |наклона| (сильный импульс есть / нет), а не величину
        на двоих. Один источник — полное согласие (|C|=1); два совпали —
          1; разошлись — 0 («импульс видят, но куда — неизвестно», это
        честное измерение несогласия, а не половинка среднего); один из
        двух молчит — 0.5. Величины наклонов живут отдельными осями
        (cvd_slope/trend_z у источников), здесь — только согласие."""
        signs = []
        fresh = (self._flow is not None
                 and time.time() * 1000 - self._flow["ts_ms"]
                 < self.flow_ttl * 1000)
        if fresh:
            signs.append(1.0 if abs(self._flow["cvd_slope"]) >= self.impulse_min
                         else 0.0)
        env = self.get_last(SURFACE)
        if env is not None:
            lens = surface_read(env.payload, self.price_hist)
            if lens:
                trend_norm = abs(lens.get("trend_z", 0.0)) / self.trend_z_scale
                signs.append(1.0 if trend_norm >= self.impulse_min else 0.0)
        if not signs:
            return 0.0
        return min(1.0, sum(signs) / len(signs))

    def _component_oi_trend(self):
        """Направление ΔOI: −1..1 (растёт/падает), в нормах stubborn_oi.
        ИЗМЕРЕНИЕ без интерпретации — фазу (пробой/вынос) выводит матрица."""
        if len(self._oi_marks) < 2:
            return 0.0
        ts0, oi0 = self._oi_marks[0]
        ts1, oi1 = self._oi_marks[-1]
        if ts1 - ts0 < self.min_span or oi0 <= 0:
            return 0.0
        d_oi = (oi1 - oi0) / oi0 * 100.0
        return max(-1.0, min(1.0, d_oi / max(1e-9, self.stubborn_oi)))

    def _component_speed_bias_full(self):
        """Перекос ленты: (B ∈ −1..1, n_src, n_votes) — СОГЛАСОВАННОСТЬ
        вместо среднего величин (канон 27.10): каждый свежий источник
        (Поток CVD / линза trend_z) даёт знак перекоса; |значение| в
        мёртвой зоне g_dead_zone — «источник не видит давления» — воз-
        держивается. Два совпали → ±1; разошлись → 0 (честное измерение
        несогласия); один молчит → ±1 с меткой 1/1. Величины наклонов
        живут отдельными осями у источников, здесь — только согласие."""
        vals = []
        fresh = (self._flow is not None
                 and time.time() * 1000 - self._flow["ts_ms"]
                 < self.flow_ttl * 1000)
        if fresh:
            vals.append(max(-1.0, min(1.0, self._flow["cvd_slope"])))
        env = self.get_last(SURFACE)
        if env is not None:
            lens = surface_read(env.payload, self.price_hist)
            if lens:
                vals.append(max(-1.0, min(1.0,
                            lens.get("trend_z", 0.0) / self.trend_z_scale)))
        b, n_votes = self._concordance(vals, self.g_dead_zone)
        return b, len(vals), n_votes

    def _component_speed_bias(self):
        """Перекос ленты B ∈ −1..1 (скалярное чтение для v/k_impulse)."""
        return self._component_speed_bias_full()[0]

    def _phase(self, liq_state, oi_trend):
        """Фаза среды: шторм-пробой / шторм-вынос / штиль (пороги в config)."""
        if liq_state < self.storm_liq_min:
            return "calm"
        if oi_trend >= self.storm_oi_min:
            return "storm-breakout"
        if oi_trend <= -self.storm_oi_min:
            return "storm-washout"
        return "calm"

    # ---------- публикация среды ----------

    def _lineage(self):
        """Нить к реальным источникам такта: последние ликвидации, снимки
        OI, тики цены и свежая карточка Потока — как они участвовали в R."""
        lineage = []
        if self._liqs:
            lineage.append(f"liq:{int(self._liqs[-1][0] * 1000)}")
        if self._oi_marks:
            lineage.append(f"oi:{int(self._oi_marks[-1][0] * 1000)}")
        if self._price_marks:
            lineage.append(f"price:{int(self._price_marks[-1][0] * 1000)}")
        seq = (self._flow or {}).get("seq")
        if seq:
            lineage.append(f"card:{seq}")
        return lineage

    def _draft_gate(self, sig, now):
        """Квантованный дедуп + heartbeat (утверждено): показывать Матрице
        только изменившиеся числа (подпись до 0.001) либо раз в
        heartbeat_sec — тихий рынок держит ячейки сердцебиением.
        Шинная строка CONDUCTIVITY этим гейтом НЕ проходит (её читают
        пятеро потребителей через короткий TTL)."""
        if sig != self._draft_sig or now - self._last_draft >= self.heartbeat:
            self._draft_sig = sig
            self._last_draft = now
            return True
        return False

    async def _tick(self):
        now = time.time()
        p = self._component_panic()
        s = self._component_stubborn()
        i = self._component_impulse()
        r_raw = self.w_panic * p + self.w_stubborn * s + self.w_impulse * i
        # Плавность (утверждено): экспоненциальное сглаживание R
        if self._r_have:
            w = 0.5 ** ((now - self._last_tick) / self.smooth_half)
            r = w * self._r_smooth + (1.0 - w) * r_raw
        else:
            r = r_raw
            self._r_have = True
        self._r_smooth = r
        self._last_tick = now
        # текущее R раз в минуту — между сменами фаз тоже видно движение
        if now - getattr(self, "_last_r_log", 0.0) >= 60.0:
            self._last_r_log = now
            self.log.info("Среда: R %.2f (полки %.2f, импульс %.2f)",
                          r, 1.0 - 0.9 * r, 1.0 + r)

        if now - self._last_pub >= self.publish_every:
            self._last_pub = now
            r = max(0.0, min(1.0, r))
            k_shelf = round(1.0 - 0.9 * r, 3)          # 1.0 → 0.1
            # Вектор живых условий (ИЗМЕРЕНИЯ, ноль голосования)
            liq_state = round(p, 3)
            oi_trend = round(self._component_oi_trend(), 3)
            # МЕТКИ СОГЛАСОВАННОСТИ (n_senior/n_votes) — данные не сплющи-
            # ваются: видно, сколькими этажами/источниками подтверждён знак.
            wind_g, wind_n, wind_votes = self._wind_full()
            acc_a, acc_n, acc_votes = self._accel_wind_full()
            bias_b, bias_n, bias_votes = self._component_speed_bias_full()
            speed_bias = round(bias_b, 3)
            # КИНЕТИЧЕСКАЯ ВЕРИФИКАЦИЯ (утверждено): импульс проверяется
            # ускорением старших этажей — вразрез гасится к нейтрали.
            k_impulse = self._k_impulse(r, max(-1.0, min(1.0,
                                             speed_bias * acc_a)))
            phase = self._phase(liq_state, oi_trend)
            # Матрица селективной громкости: {kind: множитель} из config.ini
            efficiency = (self.kernel.cfg.efficiency_matrix(phase)
                          if phase != "calm" else {})
            # ШИННАЯ СТРОКА — каждые publish_interval, НЕ дедупим (её читают
            # пятеро потребителей через короткий TTL — молчание ломает их).
            self.publish(CONDUCTIVITY, {
                "ts_ms": int(now * 1000),
                "r": round(r, 3),
                "k_shelf": k_shelf,
                "k_impulse": k_impulse,
                "components": {"panic": round(p, 3), "stubborn": round(s, 3),
                               "impulse": round(i, 3)},
                "conditions": {"liq_state": liq_state, "oi_trend": oi_trend,
                               "speed_bias": speed_bias},
                # Согласованность (не усреднение): знак + «сколькими
                # голосующими подтверждён» — consumer видит полноту.
                "concordance": {"g": round(wind_g, 3),
                                "g_n_senior": wind_n, "g_n_votes": wind_votes,
                                "accel": round(acc_a, 3),
                                "accel_n_senior": acc_n,
                                "accel_n_votes": acc_votes,
                                "bias": round(bias_b, 3),
                                "bias_n_src": bias_n,
                                "bias_n_votes": bias_votes},
                "phase": phase,
                "efficiency": efficiency,
            }, ttl=max(10.0, self.publish_every * 4))
            # В ЖУРНАЛ И МАТРИЦУ (Узел 8): квантованный дедуп + heartbeat —
            # рынок спит, журнал молчит; ячейки держит сердцебиение.
            sig = (np.round(r, 3), np.round(p, 3), np.round(s, 3),
                   np.round(i, 3), phase)
            if self._draft_gate(sig, now):
                self.publish(DRAFT, {
                    "kind": "conductivity",
                    "author": "conductivity",
                    "confidence": 0.9,
                    # ПЛОСКИЕ ЧИСЛА (утверждено): 9 числовых осей — вложенные
                    # словари, ts_ms и строки в тензор Матрицы не пишутся
                    # (время карточки Матрица берёт из created_ms).
                    "fields": {
                        "r": round(r, 3),
                        "k_shelf": k_shelf,
                        "k_impulse": k_impulse,
                        "panic": round(p, 3),
                        "stubborn": round(s, 3),
                        "impulse": round(i, 3),
                        "liq_state": liq_state,
                        "oi_trend": oi_trend,
                        "speed_bias": speed_bias,
                        # оси согласованности (знак, этажей, голосов)
                        "g_wind": round(wind_g, 3),
                        "g_senior_n": wind_n, "g_votes_n": wind_votes,
                        "a_wind": round(acc_a, 3),
                        "a_senior_n": acc_n, "a_votes_n": acc_votes,
                        "b_votes_n": bias_votes, "b_src_n": bias_n,
                    },
                    "lineage": self._lineage(),
                })
                self.snapshots += 1
                if phase != "calm":
                    self.log.info("Фаза среды: %s (liq %.2f, ΔOI %+.2f) — "
                                  "громкость: %s",
                                  phase, liq_state, oi_trend,
                                  efficiency or "{}")
