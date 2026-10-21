"""Фракталы ТФ — топология деформаций полотна (Модуль 3, модернизация 20.10).

ЧТО ДЕЛАЕТ ПРОСТЫМИ СЛОВАМИ: смотрит на рынок сразу с 13 этажей
биржевой палитры (1m…3d) и отвечает тремя сухими числами — без
вердиктов и без голосов:
- ХЕРСТ ПОСЛОЙНО И АСИММЕТРИЧНО: H по каждому кольцу RingCanvas,
  РАЗДЕЛЬНО для максимумов (H_Up — жадность) и минимумов (H_Down —
  страх). H>0.5 — ткань помнит направление (инерция), H≈0.5 — пила
  (хаос), H<0.5 — антиперсистентная пила. momentum — скорость, с
  которой H ушёл от прошлого замера (рассыпается ли этаж в пилу).
- ТРЕНДОВЫЕ НАКЛОНЫ (числа вместо бывшего «голоса»): trend_slope_{tf} —
  нормированный наклон closes кольца, % цены за бар своего ТФ, на всех
  13 этажах. Старый fractal_bias (два голоса 16/64 мин, усреднение в
  −1..1) ВЫРЕЗАН 20.10: голосовалка из числового модуля убрана.
- САМОПОДОБИЕ — ФОРМА, НЕ УРОВЕНЬ: форма последнего часа сравнивается
  с формой последних баров каждого старшего этажа по ЛОГ-ДОХОДНОСТЯМ
  (np.diff(log(closes))): одна фигура при другой цене/амплитуде даёт
  corr≈1, два растущих ряда без родства форм — околонуль. Три оси
  (corr/dir/scale) отдельными ключами на этаж — в вектор Сценариста
  весом 0.0 (Тюнер раскачает по факту исхода).
- МАТРИЦА ИЗЛОМА: точки разворота цены внутри тяжёлых зон (стоп-
  кластеры и фьги от Полевой), срез [-5:] снят; каждый узел несёт цену,
  таймфрейм рождения и физический возраст в секундах.
- ДЕДУП ПО ПОДПИСИ + СЕРДЦЕБИЕНИЕ: без изменений чисел карточка не
  идёт в журнал до heartbeat_sec (ячейки Матрицы живут по своим TTL).

Карточка kind="topological-resonance" — сухие факты, сухие числа.
"""
import time

import numpy as np

from core.module import Manifest, NeirModule
from modules._labels import CANDLES, CANDLES_WARMUP, CARD, DRAFT, FRACT_TFS

# 13 этажей биржевой палитры — как в modules/_labels (FRACT_TFS).
# БИРЖЕВАЯ ШКАЛА (27.10, ТЗ хозяина: полотно построено на биржевом
# стандарте — читаем те же кольца, что приходят с биржи). Раньше здесь
# была log-шкала 1m/2m/4m…512m, но 2m/4m/8m/32m/512m биржа не отдаёт:
# это либо удалённые ступени, либо достраиваются по запросу. Херст
# читает то, что реально живёт в кольцах: биржевой спектр.
PYRAMID_TFS = FRACT_TFS   # единый источник правды — _labels.FRACT_TFS


def _tf_label_to_sec(label):
    """Метка ТФ в СЕКУНДЫ: 15m -> 900, 1h -> 3600, 1d -> 86400.

    Секрета в двух знаках нет, ошибка честная: неизвестная метка падает
    loudly, а не превращается в мусорное кольцо."""
    text = str(label).strip()
    if text.endswith("m"):
        return int(text[:-1]) * 60
    if text.endswith("h"):
        return int(text[:-1]) * 3600
    if text.endswith("d"):
        return int(text[:-1]) * 86400
    if text.endswith("w"):
        return int(text[:-1]) * 604800
    raise ValueError("непонятная метка ТФ: %r" % label)
HURST_MIN_BARS = 60        # минимум колец для осмысленного R/S
HURST_RING = 200           # сколько последних колец берём с полотна


def _rs_hurst(series):
    """R/S-оценка Херста по ряду. None — данных мало или ряд вырожден.

    Никаких молчаливых проглатываний: любая арифметическая авария
    (nan, inf, деление на ноль) поднимается вверх и ловится вызывающим
    кодом через `self._fail` (запись в журнал); здесь — только честный
    None на непригодных данных.
    """
    a = np.asarray(series, dtype=float)
    if a.size < HURST_MIN_BARS:
        return None
    r = np.diff(a)
    n = r.size
    if n < 40:
        return None
    sizes = [s for s in (10, 20, 40, 80, 160) if s <= n // 2]
    if len(sizes) < 2:
        return None
    xs, ys = [], []
    for s in sizes:
        chunks = n // s
        rs = []
        for ch in range(chunks):
            seg = r[ch * s:(ch + 1) * s]
            dev = np.cumsum(seg - seg.mean())
            rng = float(dev.max() - dev.min())
            sd = float(seg.std())
            if sd > 0:
                rs.append(rng / sd)
        if rs:
            xs.append(float(np.log(s)))
            ys.append(float(np.log(np.mean(rs))))
    if len(xs) < 2:
        return None
    h = float(np.polyfit(xs, ys, 1)[0])
    if not np.isfinite(h):
        return None
    return round(h, 3)


def selfsim_corr(junior, senior):
    """Корреляция ФОРМ через лог-доходности (форма, а не уровень цены).

    Младшее окно пересемплируется в длину старшего, затем оба ряда
    переводятся в лог-диффы: общий уровень и чистый масштаб цены (×N)
    больше не дают ложного «похоже». Одинаковая фигура → corr≈1; два
    растущих ряда без родства форм → околонуль. None — данных мало,
    есть не-positive цены или ряд без вариаций.
    """
    if len(junior) < 5 or len(senior) < 5:
        return None
    x_old = np.linspace(0.0, 1.0, num=len(junior))
    x_new = np.linspace(0.0, 1.0, num=len(senior))
    jk = np.interp(x_new, x_old, np.asarray(junior, dtype=float))
    s = np.asarray(senior, dtype=float)
    if jk.min() <= 0 or s.min() <= 0:
        return None                # лог определён только на положительных
    rj = np.diff(np.log(jk))
    rs = np.diff(np.log(s))
    if rj.std() <= 0 or rs.std() <= 0:
        return None                # ряд без вариаций — формы не сравнить
    c = float(np.corrcoef(rj, rs)[0, 1])
    return round(c, 3) if np.isfinite(c) else None


class Fractals(NeirModule):
    manifest = Manifest(name="fractals", title="Фракталы ТФ (топология)",
                        priority=58, tick_timeout=10.0)

    async def on_start(self):
        c = self.kernel.cfg
        self.symbol = c.symbol.upper()
        self.window_bars = c.fractal_window_bars
        self.node_mass = c.fractal_node_mass
        self.publish_every = c.fractal_publish_sec
        # ДЕДУП + СЕРДЦЕБИЕНИЕ (20.10, по образцу Регрессии): такт
        # publish_every — просто проверка; без изменений чисел карточка
        # не идёт в журнал до heartbeat_sec (матричные ячейки по TTL)
        self.heartbeat_every = c.fractal_heartbeat_sec
        # Фракталы 2.0 (Этап 32): самоподобие слоёв
        self.selfsim_window = c.fractal_selfsim_window
        self.selfsim_pts = c.fractal_selfsim_senior_pts
        self._bars = []          # (ts, high, low, close) минутки
        self._picture = None
        self.stamps = 0
        # КОЛЬЦА СБОРЩИКА (RingCanvas): минутное `canvas` + старшие `senior`.
        # Херст больше не берётся из локального минутного буфера — по кольцам.
        self.collector = self.kernel.modules.get("collector")
        self._warned_col = False
        self._h_prev = {}         # tf -> прошлый H (для Fractal Momentum)
        self._h_ts = 0.0
        self._sig = None          # подпись последней опубликованной карточки
        self._last_pub = 0.0      # wall-clock последней публикации
        self.errors = 0
        self.subscribe(CANDLES, self._on_candles)
        # ПРОГРЕВ ИЗ БАЗЫ (Этап 35, урок репетиции): порог 60 баров
        # нельзя ждать час живых данных — история приходит от Кладовщика
        self.subscribe(f"{CANDLES_WARMUP}:{self.kernel.cfg.base_tf_sec}",
                       self._on_candles)
        warm = self.get_last(f"{CANDLES_WARMUP}:{self.kernel.cfg.base_tf_sec}")
        if warm is not None:
            await self._on_candles(warm)
        self.subscribe(CARD, self._on_card)
        self.every(self.publish_every, self._publish)
        self.log.info("Фракталы включены: Херст послойно и АСИММЕТРИЧНО "
                      "(Up/Down) по %d кольцам RingCanvas, наклоны "
                      "trend_slope по тем же этажам, самоподобие по "
                      "лог-доходностям (форма, не уровень); узлы от массы "
                      "зоны > %.2f, дедуп с сердцебиением %dс",
                      len(PYRAMID_TFS), self.node_mass,
                      int(self.heartbeat_every))

    # ---------- чтение колец (RingCanvas) ----------

    def _ring(self, tf):
        """Кольцо Сборщика по метке масштаба. None — кольца нет/сырое."""
        col = self.collector
        if col is None:
            # ЛЕНИВОЕ РАЗРЕШЕНИЕ: на момент on_start Сборщик мог ещё не
            # встать в реестр (приоритет 20 против наших 58, а порядок
            # регистрации — не гарантия). Ищем на каждом обращении.
            col = self.kernel.modules.get("collector")
            self.collector = col
        if col is None:
            if not self._warned_col:
                self._warned_col = True
                self.log.error("Фракталы: Сборщик не найден в реестре модулей — "
                               "кольца RingCanvas недоступны, послойный Херст "
                               "не считается. Проверь порядок регистрации.")
            return None
        self._warned_col = False
        try:
            if tf == "1m":
                canvas = col.canvas
                if canvas is None or canvas.head < HURST_MIN_BARS:
                    return None
                return canvas.get_last(HURST_RING)
            # Метка биржевого ТФ: 15m / 30m / 1h / 4h / 1d. Раньше парсер
            # умел только минуты (`tf.rstrip("m")`), и на новой шкале 1h/2h/4h
            # ронял ValueError на каждом кольце (27.10).
            minutes = _tf_label_to_sec(tf)
            # ЯВНЫЙ ЗАПРОС (27.10) вместо тихого col.senior.get(ms): кольца,
            # которого ещё нет, Сборщик достроит синтезом вверх из младшего.
            # Раньше отсутствие кольца было молчаливым None — та же болезнь,
            # что и с мёртвым kline: «работает, ошибок нет, данных нет».
            ring = col.get_ring(minutes)
            if ring is None or ring.head < HURST_MIN_BARS:
                return None
            return ring.get_last(HURST_RING)
        except (AttributeError, KeyError, TypeError, ValueError,
                ZeroDivisionError) as exc:
            self._fail("кольцо %s не прочитано" % tf, exc)
            return None

    def _fail(self, what, exc):
        """Ошибка расчёта — в журнал с трассировкой, не в молчание."""
        self.errors += 1
        self.log.error("Фракталы: %s: %s: %s", what, type(exc).__name__, exc,
                       exc_info=True)

    # ---------- послойный асимметричный Херст ----------

    def _layer_hurst(self, tf):
        """(H_up, H_down, h_sym) по одному кольцу.

        Анизотропия: H считается по логам МАКСИМУМОВ и МИНИМУМОВ
        раздельно. Страх (Down) и Жадность (Up) — разная эластичность.
        """
        ring = self._ring(tf)
        if ring is None:
            return (None, None, None)
        try:
            hi = np.asarray(ring["h"], dtype=float)
            lo = np.asarray(ring["l"], dtype=float)
            cl = np.asarray(ring["c"], dtype=float)
            # только положительные цены — лог физически определён
            hp = hi[hi > 0]
            lp = lo[lo > 0]
            cp = cl[cl > 0]
            h_up = _rs_hurst(np.log(hp)) if hp.size >= HURST_MIN_BARS else None
            h_dn = _rs_hurst(np.log(lp)) if lp.size >= HURST_MIN_BARS else None
            h_sym = _rs_hurst(np.log(cp)) if cp.size >= HURST_MIN_BARS else None
            return (h_up, h_dn, h_sym)
        except (TypeError, ValueError, ZeroDivisionError, FloatingPointError) as exc:
            self._fail("Херст по кольцу %s" % tf, exc)
            return (None, None, None)

    def _pyramid_hurst(self):
        """Все этажи полотна: {tf: (H_up, H_down, h_sym, momentum)}."""
        out = {}
        for tf in PYRAMID_TFS:
            h_up, h_dn, h_sym = self._layer_hurst(tf)
            mom = None
            if h_sym is not None:
                prev = self._h_prev.get(tf)
                if prev is not None:
                    # FRACTAL MOMENTUM: насколько H ушёл от прошлого за
                    # цикл. Отрицательный — этаж рассыпается в пилу.
                    mom = round(h_sym - prev, 4)
                self._h_prev[tf] = h_sym
            out[tf] = (h_up, h_dn, h_sym, mom)
        return out


    async def on_stop(self):
        self.log.info("Итог: карточек топологии %d", self.stamps)

    # ---------- приём ----------

    async def _on_candles(self, env):
        data = env.payload
        if int(data.get("tf_sec", 0)) != self.kernel.cfg.base_tf_sec:
            return
        if not data.get("closed") and not data.get("warmup"):
            return
        for r in data.get("candles") or []:
            self._bars.append((int(r[0]), float(r[2]), float(r[3]),
                               float(r[4])))
        self._bars = self._bars[-self.window_bars:]

    async def _on_card(self, env):
        if env.payload.get("kind") == "picture":
            self._picture = env.payload.get("fields") or {}

    # ---------- расчёты ----------

    def _layer_slope(self, tf):
        """trend_slope_{tf}: нормированный наклон closes кольца.

        % цены за БАР СВОЕГО ТФ — растущее кольцо даёт плюс, падающее —
        минус, и тем больше, чем круче тренд относительно уровня цены.
        Никакого усреднения двух голосов (бывший fractal_bias 16/64 мин
        вырезан 20.10): у каждого из 13 этажей — своё число. None —
        кольца нет или истории мало.
        """
        ring = self._ring(tf)
        if ring is None:
            return None
        try:
            cl = np.asarray(ring["c"], dtype=float)
            cl = cl[cl > 0]
            if cl.size < 30:
                return None
            x = np.arange(cl.size, dtype=float)
            slope = float(np.polyfit(x, cl, 1)[0])
            value = slope * 100.0 / float(cl.mean())
            return round(value, 6) if np.isfinite(value) else None
        except (TypeError, ValueError, ZeroDivisionError,
                FloatingPointError) as exc:
            self._fail("наклон кольца %s" % tf, exc)
            return None

    def _resonance_nodes(self):
        """МАТРИЦА ИЗЛОМА: полный список узлов (срез [-5:] снят).

        Каждый узел несёт: ценовую координату, таймфрейм, на котором он
        родился, и ФИЗИЧЕСКИЙ ВОЗРАСТ в секундах. Раньше возвращались
        голые цены последних пяти — и возраст, и масштаб терялись.
        """
        if self._picture is None or len(self._bars) < 10:
            return []
        now_ms = int(time.time() * 1000)
        try:
            base_ms = int(self.kernel.cfg.base_tf_sec)
        except (TypeError, ValueError) as exc:
            self._fail("таймфрейм узлов", exc)
            base_ms = 0
        try:
            heavy = []
            for z in self._picture.get("zones") or []:
                kinds = set(z.get("kinds") or ())
                if not kinds & {"stop-cluster", "fvg-zone"}:
                    continue
                if float(z.get("strength", 0.0)) < self.node_mass:
                    continue
                heavy.append((float(z["low"]), float(z["high"])))
        except (TypeError, ValueError, KeyError) as exc:
            self._fail("разбор зон для узлов", exc)
            return []
        if not heavy:
            return []
        bars = self._bars[-60:]
        nodes = []
        try:
            for i in range(3, len(bars) - 3):
                c = bars[i][3]
                win = [bars[j][3] for j in
                       (i - 3, i - 2, i - 1, i + 1, i + 2, i + 3)]
                is_min = all(c < w for w in win)
                is_max = all(c > w for w in win)
                if not (is_min or is_max):
                    continue
                for lo, hi in heavy:
                    if lo <= c <= hi:
                        ts = int(bars[i][0])
                        nodes.append({
                            "price": round(c, 8),
                            "tf": base_ms,
                            "age_sec": round(max(0.0, (now_ms - ts) / 1000.0), 1),
                            "kind": "low" if is_min else "high",
                        })
                        break
        except (TypeError, ValueError, IndexError, ZeroDivisionError) as exc:
            self._fail("поиск узлов излома", exc)
            return []
        # порядок — от свежего к старому; БЕЗ обрезания по количеству
        nodes.sort(key=lambda n: n["age_sec"])
        return nodes

    def _self_similarity(self):
        """ТОТАЛЬНАЯ РАЗВЁРТКА САМОПОДОБИЯ: все 3 оси по КАЖДОМУ кольцу.

        Возвращает {tf: (corr, dir, scale)} вместо одной «лучшей» тройки.
        Раньше выбирался единственный максимум по |corr| — 29 измерений
        терялись, и Тюнер не видел, какой именно этаж похож на младший.
        """
        n = self.selfsim_window
        out = {}
        if len(self._bars) < n + 5:
            return out
        try:
            junior = [b[3] for b in self._bars[-n:]]
        except (TypeError, ValueError, IndexError) as exc:
            self._fail("окно самоподобия", exc)
            return out
        for tf in PYRAMID_TFS:
            if tf == "1m":
                continue
            ring = self._ring(tf)
            if ring is None:
                continue
            try:
                cl = np.asarray(ring["c"], dtype=float)
                cl = cl[cl > 0]
            except (TypeError, ValueError, KeyError) as exc:
                self._fail("кольцо самоподобия %s" % tf, exc)
                continue
            if cl.size < self.selfsim_pts:
                continue
            senior = list(cl[-self.selfsim_pts:])
            corr = selfsim_corr(junior, senior)
            if corr is None:
                continue
            # ОСЬ НАПРАВЛЕНИЯ: старший слой идёт вверх (+1) или вниз (−1)
            sdir = 1.0 if senior[-1] >= senior[0] else -1.0
            # ОСЯ МАСШТАБА: та же метка кольца — сколько секунд старше младший
            scale = _tf_label_to_sec(tf)
            out[tf] = (corr, sdir, scale)
        return out

    # ---------- пульс ----------

    async def _publish(self):
        if len(self._bars) < 60:
            return
        now_ms = int(time.time() * 1000)
        # ХЕРСТ ПОСЛОЙНЫЙ И АСИММЕТРИЧНЫЙ (было: одно число по минуткам)
        layers = self._pyramid_hurst()
        nodes = self._resonance_nodes()
        sims = self._self_similarity()
        # НАКЛОНЫ ПО КОЛЬЦАМ: числа вместо бывшего голоса fractal_bias
        slopes = {tf: self._layer_slope(tf) for tf in PYRAMID_TFS}
        try:
            price = self._bars[-1][3]
        except (IndexError, TypeError, ValueError) as exc:
            self._fail("текущая цена", exc)
            return
        nearest = min((abs(price - n["price"]) / price * 100.0
                       for n in nodes), default=None) if price > 0 else None
        # плоские поля совместимости (старые потребители)
        sym = [(v[2]) for v in layers.values() if v[2] is not None]
        h_avg = round(sum(sym) / len(sym), 3) if sym else None
        # ЛУЧШИЙ ЭТАЖ ПО |corr| — источник плоских осей самоподобия:
        # dir/scale больше не врут постоянным None (были заглушки)
        if sims:
            b_corr, b_dir, b_scale = sims[
                max(sims, key=lambda k: abs(sims[k][0]))]
        else:
            b_corr, b_dir, b_scale = None, None, None
        # РАЗВЁРНУТЫЕ ПОЛЯ: по каждому масштабу — свои ключи
        fields = {
            "hurst_exponent": h_avg,
            "resonance_nodes": nodes,
            "nearest_node_pct": (round(nearest, 4)
                                 if nearest is not None else None),
            "selfsim_corr": b_corr,
            "selfsim_dir": b_dir,
            "selfsim_scale": b_scale,
            "bars": len(self._bars),
            "ts_ms": now_ms,
            "errors": self.errors,
        }
        for tf in PYRAMID_TFS:
            h_up, h_dn, h_sym, mom = layers.get(tf, (None, None, None, None))
            fields[f"hurst_up_{tf}"] = h_up
            fields[f"hurst_down_{tf}"] = h_dn
            fields[f"hurst_{tf}"] = h_sym
            fields[f"fractal_momentum_{tf}"] = mom
            fields[f"trend_slope_{tf}"] = slopes.get(tf)
            corr, sdir, scale = sims.get(tf, (None, None, None))
            # ТРИ ОСИ САМОПОДОБИЯ ОТДЕЛЬНЫМИ ПОЛЯМИ (в вектор, вес 0.0)
            fields[f"fractal_corr_{tf}"] = corr
            fields[f"fractal_dir_{tf}"] = sdir
            fields[f"fractal_scale_{tf}"] = scale
        # ДЕДУП ПО ПОДПИСИ + СЕРДЦЕБИЕНИЕ (20.10, по образцу Регрессии):
        # подпись по значимым числам. Возраст узлов в подписи НЕ входит —
        # он меняется каждую секунду и держал бы карточку «всегда новой»;
        # его освежает сердцебиение heartbeat_every
        sig = (h_avg, fields["nearest_node_pct"], b_corr, b_dir, b_scale,
               tuple((n["price"], n["kind"]) for n in nodes),
               tuple((tf, layers.get(tf), slopes.get(tf), sims.get(tf))
                     for tf in PYRAMID_TFS))
        now = time.time()
        force = (now - self._last_pub) >= self.heartbeat_every
        if sig == self._sig and not force:
            return                  # числа не менялись — молчим до heartbeat
        self._sig = sig
        self._last_pub = now
        self.stamps += 1
        self._h_ts = now_ms / 1000.0
        self.publish(DRAFT, {
            "kind": "topological-resonance",
            "author": "fractals",
            "confidence": 0.9,
            "fields": fields,
            # НИТЬ (протокол lineage, Узел 5): минутное полотно — источник
            # и матрицы излома, и послойного Херста по кольцам
            "lineage": [f"candle:{self.symbol}:1m:{self._bars[-1][0]}"],
        })
        live = sum(1 for v in layers.values() if v[2] is not None)
        aniso = sum(1 for v in layers.values()
                    if v[0] is not None and v[1] is not None
                    and abs(v[0] - v[1]) > 0.01)
        ups = sum(1 for v in slopes.values() if v is not None)
        self.log.info("Топология: Херст послойно в %d/%d кольцах "
                      "(средний %s), асимметрия Up/Down в %d, наклоны "
                      "trend_slope в %d этажах, узлов %d, самоподобие "
                      "в %d, ошибок %d",
                      live, len(PYRAMID_TFS), h_avg, aniso, ups,
                      len(nodes), len(sims), self.errors)
