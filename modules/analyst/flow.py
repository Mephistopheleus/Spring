"""«Поток» — анализатор топлива: CVD, лента, поглощения, дивергенции.

Сырьё — поток сделок (нить полотна), ничего нового с биржи не качаем.
НЕ сигналка (утверждено): карточки содержат только факты с координатами
и окном жизни, никаких buy/sell. Решение — у Сценариста.

Карточки:
- absorption-barrier — «ЛОКАЛЬНЫЙ ОБЪЁМНЫЙ БАРЬЕР» (ЗОНА): в цену били
  рыночным объёмом в N раз больше обычного, а цена почти не сдвинулась —
  кто-то лимитками ест напор. Уровень битвы + сторона, которая ела.
  Поле примет её как зону-магнит наравне с SR.
- delta-divergence — цена рисует экстремум, а дельта — нет («топливо
  кончается раньше цены»), с силой расхождения.
- flow-state — вектор топлива: наклон CVD, скорость ленты, всплеск,
  Direction_Bias — доля агрессивных покупок в потоке (измерение, не мнение).

Все прогнозы имеют окно жизни — тюнер сверит обещание с фактом.
Ручки калибровки — config.ini, секция [flow].
"""
import collections
import math
import time

from core.module import Manifest, NeirModule
from modules._labels import DRAFT, PRICE, SURFACE
from modules.market.lenses import surface_read

# Все ручки калибровки — config.ini [flow]/[core] и свойства core/config.py:
# flow_eval_every_sec, flow_state_publish_interval, flow_typical_half_life_sec,
# flow_speed_half_life_sec, flow_level_tol, max_trades. Модульных констант
# под них не держим — единое место ручек (аудит Google, п.6).


class FlowAnalyst(NeirModule):
    manifest = Manifest(name="flow-analyst", title="Поток (CVD, лента, барьеры)",
                        requires=("price",), priority=45, tick_timeout=5.0)

    async def on_start(self):
        self.symbol = self.kernel.cfg.symbol.upper()
        c = self.kernel.cfg
        self.win_sec = c.flow_absorption_window_sec
        self.abs_mult = c.flow_absorption_multiple
        self.cvd_window = c.flow_cvd_window_min * 60
        self.div_force_min = c.flow_divergence_force_min
        self.burst_mult = c.flow_burst_multiple
        self.barrier_ttl = c.flow_barrier_ttl_min * 60
        self.state_ttl = c.flow_state_ttl_min * 60
        self.div_ttl = c.flow_divergence_ttl_min * 60
        self.eval_every = c.flow_eval_every_sec
        self.state_interval = c.flow_state_publish_interval
        self.level_tol = c.flow_level_tol
        self.typical_half_life = c.flow_typical_half_life_sec
        self.speed_half_life = c.flow_speed_half_life_sec

        self._trades = collections.deque(maxlen=c.max_trades)  # (ts, px, signed, abs)
        self._cvd = 0.0             # CVD по окну cvd_window
        self._cvd_marks = collections.deque()                # (ts, cvd, px)
        self._typical_vol = 0.0     # обычный объём за окно (EWMA)
        self._speed = 0.0           # обычная скорость ленты (EWMA)
        self._last_barrier = None   # (level, ts)
        self._last_div = 0.0
        self._last_state = 0.0
        self._last_eval = 0.0
        self._visc_hist = collections.deque(maxlen=30)   # история вязкости
        self._visc_diag = collections.deque(maxlen=20)  # (сделок, замеров)
        self._visc_last = None           # последняя измеренная вязкость
        self._visc_raw = None            # её сырое число (до нормировки, в карточку)
        self.ticks = 0
        self.barriers = 0
        self.divergences = 0
        self.states = 0

        self.subscribe(PRICE, self._on_tick)
        self.every(self.kernel.cfg.flow_eval_every_sec, self._evaluate)
        self.log.info("Поток включён: барьер от %.0fx объёма, CVD %d мин, "
                      "дивергенция от %.2f",
                      self.abs_mult, c.flow_cvd_window_min, self.div_force_min)

    async def on_stop(self):
        self.log.info("Итог: сделок %d, барьеров %d, дивергенций %d, "
                      "срезов состояния %d",
                      self.ticks, self.barriers, self.divergences, self.states)

    # ---------- приём нити ----------

    async def _on_tick(self, env):
        p = env.payload
        price = float(p.get("price", 0.0))
        qty = float(p.get("qty", 0.0))
        if price <= 0 or qty <= 0:
            return
        ts = (int(p.get("ts_ms", 0)) or int(time.time() * 1000)) / 1000.0
        signed = -qty if p.get("is_sell") else qty
        self._trades.append((ts, price, signed, qty))
        # CVD по окну: подрезаем хвост
        edge = ts - self.cvd_window
        cvd = self._cvd + signed
        while self._trades and self._trades[0][0] < edge:
            cvd -= self._trades[0][2]
            self._trades.popleft()
        self._cvd = cvd
        self.ticks += 1

    # ---------- осмотр потока ----------

    def _window(self, ts, seconds):
        """Сделки за последние seconds секунд."""
        edge = ts - seconds
        return [t for t in self._trades if t[0] >= edge]

    def _vol_expect_60(self):
        """Ожидаемый ход цены за минуту (от линз, масштаб 1м)."""
        env = self.get_last(SURFACE)
        if env is None:
            return None
        lens = surface_read(env.payload, 60)
        if not lens:
            return None
        return lens["vol_min_pct"] / 100.0       # доля цены за минуту

    async def _evaluate(self):
        now = time.time()
        if now - self._last_eval < self.eval_every * 0.9:
            return
        self._last_eval = now
        if len(self._trades) < 10:
            return

        trades = self._window(now, self.win_sec)
        if len(trades) < 5:
            return
        total_vol = sum(t[3] for t in trades)
        buy_vol = sum(t[3] for t in trades if t[2] > 0)
        sell_vol = total_vol - buy_vol
        p_min = min(t[1] for t in trades)
        p_max = max(t[1] for t in trades)
        p_ref = (p_min + p_max) / 2.0
        speed = len(trades) / self.win_sec

        w = 0.5 ** (self.win_sec / self.typical_half_life)
        self._typical_vol = (w * self._typical_vol
                             + (1.0 - w) * total_vol) if self._typical_vol else total_vol
        # ВЯЗКОСТЬ МЕРЯЕМ ЗДЕСЬ, раз в 2 с (утверждено 02.10): норма
        # вязкости — скользящая, ей нужно МНОГО замеров. Раньше измерение
        # шло только при публикации (раз в минуту), норма копилась минутами,
        # и вязкость молчала первые 5+ минут после старта — Сценарист всё это
        # время ждал затвор и не писал ни одного сценария.
        self._visc_last = self._viscosity(
            now, trades, (p_min + p_max) / 2.0)
        ws = 0.5 ** (self.win_sec / self.speed_half_life)
        self._speed = (ws * self._speed + (1.0 - ws) * speed) if self._speed else speed

        self._cvd_marks.append((now, self._cvd, trades[-1][1]))
        edge = now - self.cvd_window
        while self._cvd_marks and self._cvd_marks[0][0] < edge:
            self._cvd_marks.popleft()

        await self._check_absorption(now, total_vol, buy_vol, sell_vol,
                                     p_min, p_max, p_ref)
        await self._check_divergence(now, p_ref)
        if now - self._last_state >= self.state_interval:
            self._last_state = now
            self._publish_state(now, buy_vol, sell_vol, speed)

    # ---------- барьер: объём ударил, цена устояла ----------

    async def _check_absorption(self, now, total_vol, buy_vol, sell_vol,
                                p_min, p_max, p_ref):
        if self._typical_vol <= 0:
            return
        multiple = total_vol / self._typical_vol
        if multiple < self.abs_mult:
            return
        p_range = (p_max - p_min) / p_ref if p_ref else 0.0
        expect = self._vol_expect_60()
        range_limit = (0.5 * expect * math.sqrt(self.win_sec / 60.0)
                       if expect else 0.0015)
        if p_range > range_limit:
            return                       # объём был, но цену двигало — не барьер
        level = p_min if sell_vol > buy_vol else p_max   # цена, которую держали
        # Сторона, которая ЕЛА напор: продавцы били вниз — удержали низ
        # (ели покупки); покупатели били вверх — удержали верх (ели продажи).
        # Числовая координата +1/-1 — факт геометрии барьера, не «сигнал».
        absorber_side = 1 if sell_vol > buy_vol else -1
        # антидубль: тот же уровень в пределах срока жизни барьера
        if (self._last_barrier is not None
                and abs(level - self._last_barrier[0]) / level < self.level_tol
                and now - self._last_barrier[1] < self.barrier_ttl):
            return
        self._last_barrier = (level, now)
        conf = round(min(0.9, 0.4 + 0.08 * math.log2(max(1.0, multiple))), 3)
        self.publish(DRAFT, {
            "kind": "absorption-barrier",
            "author": "flow-analyst",
            "confidence": conf,
            "fields": {
                "zone_id": f"abs{round(level, 6)}",   # решётка Матрицы (дыра 1)
                "zone_low": round(level * (1.0 - self.level_tol), 8),
                "zone_high": round(level * (1.0 + self.level_tol), 8),
                "absorber_side": absorber_side,
                "volume": round(total_vol, 2),
                "multiple": round(multiple, 1),
                "duration_sec": self.win_sec,
                "level": round(level, 8),
            },
            "lineage": self._lineage(),
        })
        self.barriers += 1
        self.log.info("БАРЬЕР: %.5f — напор %s, ел %s; объём ×%.1f обычного, "
                      "уверенность %.2f",
                      level,
                      "продажами" if absorber_side > 0 else "покупками",
                      "покупки" if absorber_side > 0 else "продажи",
                      multiple, conf)

    # ---------- дивергенция: цена пошла, топливо — нет ----------

    async def _check_divergence(self, now, p_ref):
        if len(self._cvd_marks) < 2 or self._typical_vol <= 0:
            return
        old_ts, old_cvd, old_px = self._cvd_marks[0]
        minutes = (now - old_ts) / 60.0
        if minutes < 5:
            return
        d_cvd = self._cvd - old_cvd
        d_px = (p_ref - old_px) / old_px if old_px else 0.0
        expect = self._vol_expect_60() or 0.001
        move_expect = expect * math.sqrt(minutes)   # ожидаемый ход за окно
        d_cvd_norm = abs(d_cvd) / (self._typical_vol * math.sqrt(minutes) + 1e-9)
        if abs(d_px) < 0.6 * move_expect:
            return                       # цена и не ходила — дивергенции нет
        # Числовые координаты (аудит Google, п.5): price_dir +1/-1 — куда
        # пошла цена; cvd_sign -1 — дельта ПРОТИВ хода (топливо кончается
        # раньше цены), 0 — дельта в норме (топливо стоит), +1 — вместе
        # с ценой (такая карточка дивергенцией не является и отсекается).
        price_dir = 1 if d_px > 0 else -1
        if d_cvd * price_dir < 0 and d_cvd_norm > 0.3:
            cvd_sign = -1
            force = min(1.0, d_cvd_norm)
        elif d_cvd_norm < 0.2:
            cvd_sign = 0
            force = min(1.0, 0.5 * abs(d_px) / (move_expect + 1e-9))
        else:
            return                       # топливо с ценой — расхождения нет
        if force < self.div_force_min:
            return
        if now - self._last_div < 300:   # не чаще раза в 5 минут
            return
        self._last_div = now
        self.publish(DRAFT, {
            "kind": "delta-divergence",
            "author": "flow-analyst",
            "confidence": round(min(0.9, 0.3 + 0.6 * force), 3),
            "fields": {
                "price_dir": price_dir,
                "cvd_sign": cvd_sign,
                "force": round(force, 3),
                "window_min": round(minutes, 1),
                "price_change_pct": round(d_px * 100, 3),
            },
            "lineage": self._lineage(),
        })
        self.divergences += 1
        self.log.info("ДИВЕРГЕНЦИЯ: цена %s %.2f%%, дельта %s, сила %.2f",
                      "вверх" if price_dir > 0 else "вниз", d_px * 100,
                      "против хода" if cvd_sign < 0 else "топливо стоит", force)

    # ---------- состояние потока (вектор топлива) ----------

    def _lineage(self):
        """Нить — к РЕАЛЬНЫМ сделкам наблюдаемого окна (его концы), не к
        wall-clock времени публикации: по нити путь от карточки ведёт
        в полотно сделок, а не в «когда штампнули»."""
        if not self._trades:
            return []
        return [f"trade:{self.symbol}:{int(self._trades[0][0] * 1000)}",
                f"trade:{self.symbol}:{int(self._trades[-1][0] * 1000)}"]

    def _viscosity(self, now, win_trades, price_now):
        """Вязкость стакана (Волна 5, Этап 30, утверждено): отношение
        «ход цены / съеденный объём» к обычному соотношению. norm > 1 =
        среда «жидкая» (цена течёт от слабых ударов), norm < 1 =
        «каменеет» (удары поглощаются — бетон)."""
        if len(win_trades) < 5 or price_now <= 0:
            return None
        p_start = float(win_trades[0][1])
        move = abs(price_now - p_start) / price_now * 100.0
        vol = sum(float(t[3]) for t in win_trades)
        typ = self._typical_vol if self._typical_vol > 0 else 0.0
        if typ <= 0:
            return None
        raw = move / (vol / typ + 1e-9)          # ход на «типичный объём»
        self._visc_raw = round(raw, 6)           # сырое число — в карточку/матрицу
        # НОРМА ВЯЗКОСТИ (починка 02.10): сырые замеры берутся на КАЖДОЙ
        # оценке потока (раз в 2 с), а не раз в минуту при публикации. Иначе
        # норма копилась бы минутками и вязкость молчала по 5 минут подряд
        # (в логе это было: 410–758 сделок в окне, а замеров нормы — 3).
        if raw > 0:
            self._visc_hist.append(raw)
            while len(self._visc_hist) > 30:
                self._visc_hist.popleft()
        elif not self._visc_hist:
            self._visc_hist.append(1e-9)         # ненулевой якорь нормы
        if len(self._visc_hist) < 3:
            return None                          # норма ещё копится
        avg = sum(self._visc_hist) / len(self._visc_hist)
        if avg <= 0:
            return None
        # «Якорь держит»: ход нулевой при нормальном объёме — это вязкость
        # по нижней границе шкалы, а не «нет данных».
        value = 0.1 if raw <= 1e-9 else raw / avg
        return round(max(0.1, min(3.0, value)), 3)

    def _publish_state(self, now, buy_vol, sell_vol, speed):
        total = buy_vol + sell_vol
        direction_bias = buy_vol / total if total > 0 else 0.5
        cvd_norm = self._typical_vol * math.sqrt(self.cvd_window / 60.0) + 1e-9
        cvd_slope = max(-1.0, min(1.0, self._cvd / cvd_norm))
        # Всплеск ленты (аудит Google, п.6): было «сама обычная скорость»
        # (EWMA) вместо отношения — честное «во сколько раз текущая
        # быстрее обычной»: >1 всплеск, <1 затишье. Флаг burst (bool)
        # держит порог burst_multiple — его читают снимки и диагностика.
        burst_ratio = round(speed / self._speed, 3) if self._speed > 0 else 1.0
        burst = bool(self._speed > 0 and speed >= self.burst_mult * self._speed)
        # Вязкость стакана (Волна 5, утверждено): «ход / съеденный объём».
        # Значение посчитано в _evaluate раз в 2 с (норма копится часто),
        # здесь только читаем накопленное — публикуем раз в минуту.
        price_now = float(self._trades[-1][1]) if self._trades else 0.0
        win_trades = self._window(now, self.win_sec)
        viscosity = self._visc_last
        # ДИАГНОСТИКА (хозяин, 02.10): почему вязкость может быть None —
        # пишем сырые счётчики в карточку, чтобы судить по фактам из БД, а
        # не гадать. Сделки в окне < 5 — данных физически мало.
        self._visc_diag.append((len(win_trades), len(self._visc_hist)))
        # deque нельзя резать срезом — у него свой maxlen, держим хвост сам
        while len(self._visc_diag) > 20:
            self._visc_diag.popleft()
        last = self._visc_diag[-1] if self._visc_diag else (0, 0)
        self.publish(DRAFT, {
            "kind": "flow-state",
            "author": "flow-analyst",
            "confidence": 0.5,
            "fields": {
                "cvd_slope": round(cvd_slope, 3),
                "tape_speed": round(speed, 2),
                "burst_ratio": burst_ratio,     # >1 — всплеск, <1 — затишье
                "burst": burst,                 # порог burst_multiple взят
                "direction_bias": round(direction_bias, 3),
                "viscosity": viscosity,         # >1 жидкая, <1 каменеет
                # СЫРОЕ число вязкости (ход % на «типичный объём») — в
                # матрицу: Тюнер калибрует ручки по сытым фактам, а не
                # по обрезанной шкале 0.1..3.0.
                "viscosity_raw": self._visc_raw,
                "window_sec": self.win_sec,
                # диагностика вязкости: сделок в окне / замеров нормы
                "visc_trades": last[0], "visc_norm": last[1],
            },
            "lineage": self._lineage(),
        })
        self.states += 1



