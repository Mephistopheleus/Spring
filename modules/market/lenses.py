"""Линзы — ПОВЕРХНОСТЬ рынка на потоке сделок (утверждённая модель).

Полотно = ткань. Нить — поток сделок (каждая сделка приходит сюда).
Линзы — непрерывная поверхность поверх нити: нет корзин, нет ступенек.
Каждая линза — экспоненциально затухающая статистика со своим
полураспадом; масштабы — плотная лестница (0.5, 1, 2, 3 ... 240 минут):
плотно на малых, редко на больших, промежутки между «строками ткани»
везде одинаковые.

На каждой линзе в реальном времени:
- slope_min_pct — дрейф цены, % в минуту (экспоненциально взвешенный);
- vol_min_pct   — волатильность, % в минуту;
- trend_z       — наклон в единицах шума (наклон/волатильность, ±3);
- flow_imb      — перекос покупателей/продавцов по объёму (-1..1).

Чтение в ЛЮБОЙ момент — не нужно ждать закрытия свечи. Публикация
среза (ярлык SURFACE) раз в publish_interval секунд; свежее значение
всегда на «доске значений» — поздние модули забирают его при старте.

Память: единицы КБ. В базу не пишется — после перезапуска прогревается
из минутной истории склада (ярлык candles_warmup) за секунды.

ШКАЛА (утверждено 18.10.2026): ПЛОТНАЯ лестница [lenses] scales_min —
лог-сетка sqrt(2) от 0.5 мин до 1 недели + спектр ТФ Binance (31 точка).
Линзы дешёвы (API не тратятся) — плотность бесплатна. НЕ путать со
шкалой ТФ пирамиды полотна ([market] senior_tf).

ВЫХОД — ТЕНЗОР живой матрицы (12.10.2026): поверхность публикуется
чистым NumPy-массивом (N,4) + scale_vectors (N,1), без текстовых
подписей ТФ. Потребители читают по индексу столбца / значению масштаба
(lenses.surface_read), а не по подписи — иначе снова сплющивание.

НАДЁЖНОСТЬ ПОКАЗАНИЙ (19.10.2026, п.1 аудита): рядом с тензором едет
confidence_vectors (N,2) [n_eff, age_sec] — сколько наблюдений реально
в памяти линзы и сколько секунд назад последний раз приходила сделка.
СЕРДЦЕБИЕНИЕ (п.2): срез публикуется по расписанию даже без тиков — на
тихом рынке показания EWMA валидны (формула крутится только на сделках
и замерзает, но не портится), а ячейкам Линз в Матрице нельзя умирать
по TTL. Свежесть честно видна по age_sec: 0 на тиковой публикации,
растёт, пока рынок молчит.

ПАКЕТНЫЙ ПРОГРЕВ (п.3): warm_batch считает ту же математику warm()
векторно (закрытая форма EWMA) — старт сократил прогрев 31 линзы по
тысячам свечей с секунд до миллисекунд. Сверяется со штучным warm() в
tests/smoke_lenses.py.
"""
import math
import time

import numpy as np

from core.module import Manifest, NeirModule
from modules._labels import CANDLES_WARMUP, PRICE, SURFACE

# КОНТРАКТ ОСЕЙ ТЕНЗОРА SURFACE (заморожен 12.10.2026): потребители читают
# по ИНДЕКСУ столбца, а строки тензора — масштабы по ВОЗРАСТАНИЮ полураспада
# (их значения публикуются рядом в scale_vectors для сверки). Менять порядок
# нельзя: сдвиг оси = молчаливая ложь (чужой показатель прочитан как свой).
COL_SLOPE = 0     # slope_min_pct — дрейф цены, %/мин
COL_VOL = 1       # vol_min_pct   — шум, %/мин
COL_TREND = 2     # trend_z       — дрейф в единицах шума, ±3
COL_FLOW = 3      # flow_imb      — перекос покупателей/продавцов, -1..1
SURFACE_COLS = 4

# КОНТРАКТ ОСЕЙ confidence_vectors (утверждено 19.10.2026, п.1 аудита):
# параллельный тензору массив НАДЁЖНОСТИ показаний — честный вес линзы.
# Потребители (Сценарист, Матрица) взвешивают горизонты по n_eff и
# отличают «живёт и течёт» от «замерло» по age_sec.
CONF_NEFF = 0      # n_eff    — эффективное число наблюдений в памяти линзы
CONF_AGE = 1       # age_sec  — секунд с последней сделки/свечи
CONF_COLS = 2


class Lens:
    """Одна экспоненциальная линза: полураспад half_life секунд."""

    __slots__ = ("half_life", "init", "prev_p", "er", "er2",
                 "flow", "qty", "dt", "last_ts", "n_eff")

    def __init__(self, half_life_sec):
        self.half_life = float(half_life_sec)
        self.init = False
        self.prev_p = 0.0
        self.er = 0.0            # EWMA лог-доходности (дрейф)
        self.er2 = 0.0           # EWMA квадрата лог-доходности (шум)
        self.flow = 0.0          # EWMA потока со знаком (+ покупатели)
        self.qty = 0.0           # EWMA объёма сделки (для нормировки)
        self.dt = 1.0            # EWMA интервала между сделками, сек
        self.last_ts = 0.0
        self.n_eff = 0.0         # НАДЁЖНОСТЬ (19.10): эффективное число
                                 # наблюдений в памяти линзы — счётчик
                                 # растёт на 1 с каждой сделкой и гаснет
                                 # тем же полураспадом, что и статистика.
                                 # Насыщается на уровне 1/(1-w). Честный
                                 # вес показания: 12 сделок ≠ 12000.

    def update(self, ts_sec, price, qty, is_sell):
        if not self.init:
            if price <= 0:
                return
            self.init = True
            self.prev_p = price
            self.last_ts = ts_sec
            return
        dt = max(1e-3, ts_sec - self.last_ts)
        # ИСТИННЫЙ ПОЛУРАСПАД (12.10.2026): за время half_life вес прошлого
        # падает ровно вдвое (0.5), как и обещает имя и конфиг. Было
        # exp(-dt/half_life) — за тот же срок оставалось 0.37 (линза забывала
        # на ~44% быстрее заявленного). Формула та же, что в flow.py.
        w = 0.5 ** (dt / self.half_life)        # вес прошлого (полураспад)
        self.n_eff = w * self.n_eff + 1.0        # надёжность: счётчик с тем же гашением
        if price > 0 and self.prev_p > 0:
            r = math.log(price / self.prev_p)
            self.er = w * self.er + (1.0 - w) * r
            self.er2 = w * self.er2 + (1.0 - w) * r * r
        self.dt = w * self.dt + (1.0 - w) * dt
        signed = -qty if is_sell else qty
        self.flow = w * self.flow + (1.0 - w) * signed
        self.qty = w * self.qty + (1.0 - w) * abs(qty)
        self.prev_p = price
        self.last_ts = ts_sec

    def warm(self, ts_sec, close, volume, delta):
        """Один шаг прогрева из свечи: (закрытие, объём, чистая дельта).

        Дельта = купили агрессором − продали агрессором — ровно тот
        знаковый объём, что накопил бы живой поток за эту свечу, поэтому
        flow_imb после прогрева согласован с живым измерением. Единицы
        те же, что у update(): dt — период свечи, цена — закрытие.
        """
        if not self.init:
            if close <= 0:
                return
            self.init = True
            self.prev_p = close
            self.last_ts = ts_sec
            return
        dt = max(1e-3, ts_sec - self.last_ts)
        w = 0.5 ** (dt / self.half_life)
        self.n_eff = w * self.n_eff + 1.0
        if close > 0 and self.prev_p > 0:
            r = math.log(close / self.prev_p)
            self.er = w * self.er + (1.0 - w) * r
            self.er2 = w * self.er2 + (1.0 - w) * r * r
        self.dt = w * self.dt + (1.0 - w) * dt
        self.flow = w * self.flow + (1.0 - w) * delta
        self.qty = w * self.qty + (1.0 - w) * abs(volume)
        self.prev_p = close
        self.last_ts = ts_sec

    def warm_batch(self, ts, closes, volumes, deltas):
        """ПАКЕТНЫЙ ПРОГРЕВ (п.3 аудита 19.10.2026).

        Ускорение без смены математики: строки свечей парсятся в массивы
        ОДИН раз (раньше int()/float() конверсии и try/except повторялись
        для каждой из 31 линзы на каждой свече), а шаговая рекурсия
        сохранена в том же порядке операций, что и warm(), — поэтому
        результат СОВПАДАЕТ С warm() ДО ПОСЛЕДНЕГО ЗНАКА (побитово),
        что и проверяет tests/smoke_lenses.py.

        Вход: ts (сек, возрастает), closes, volumes, deltas — те же
        значения, что шли бы по одной свече в warm(). Свечи с close<=0
        пропускаются так же, как в warm(): не инициализируют, er/er2
        не двигают, но dt/flow/qty обновляют — логика ровно оригинальная.
        """
        ts = np.asarray(ts, dtype=np.float64)
        closes = np.asarray(closes, dtype=np.float64)
        volumes = np.asarray(volumes, dtype=np.float64)
        deltas = np.asarray(deltas, dtype=np.float64)
        if ts.size == 0:
            return
        if not self.init:
            first = np.flatnonzero(closes > 0)
            if first.size == 0:
                return                       # ни одной валидной свечи
            i0 = int(first[0])
            self.init = True
            self.prev_p = float(closes[i0])
            self.last_ts = float(ts[i0])
            n_eff = 0.0
        else:
            i0 = -1                          # линза уже жила — шаги со всех
            n_eff = self.n_eff
        # шаги: строки после инициализирующей (или все, если линза жила)
        sl = slice(i0 + 1, None)
        ts_s, c_s, v_s, d_s = ts[sl], closes[sl], volumes[sl], deltas[sl]
        if ts_s.size == 0:
            return
        # шаговая рекурсия в том же порядке, что и warm(): Python float +
        # math.log — побитовое равенство со штучным прогревом
        er, er2, flow, qty = self.er, self.er2, self.flow, self.qty
        dt_e, last = self.dt, self.last_ts
        prev_p = self.prev_p
        hl = self.half_life
        for i in range(ts_s.size):
            ti = float(ts_s[i])
            dt = ti - last
            if dt < 1e-3:
                dt = 1e-3
            w = 0.5 ** (dt / hl)
            n_eff = w * n_eff + 1.0
            ci = float(c_s[i])
            if ci > 0 and prev_p > 0:
                r = math.log(ci / prev_p)
                er = w * er + (1.0 - w) * r
                er2 = w * er2 + (1.0 - w) * r * r
            dt_e = w * dt_e + (1.0 - w) * dt
            flow = w * flow + (1.0 - w) * float(d_s[i])
            qty = w * qty + (1.0 - w) * abs(float(v_s[i]))
            prev_p, last = ci, ti
        self.er, self.er2 = er, er2
        self.flow, self.qty = flow, qty
        self.dt, self.n_eff = dt_e, n_eff
        self.prev_p, self.last_ts = prev_p, last

    def read_row(self):
        """Чтение линзы как строка тензора [slope%, vol%, trend_z, flow]
        или None (линза ещё не инициализирована → NaN в тензоре)."""
        if not self.init or self.dt <= 0:
            return None
        slope_min = self.er / self.dt * 60.0
        var_min = max(0.0, self.er2 - self.er * self.er) / self.dt * 60.0
        vol_min = math.sqrt(var_min)
        trend_z = slope_min / vol_min if vol_min > 1e-12 else 0.0
        flow = self.flow / self.qty if self.qty > 1e-12 else 0.0
        return (slope_min * 100.0,
                vol_min * 100.0,
                max(-3.0, min(3.0, trend_z)),
                max(-1.0, min(1.0, flow)))


class Lenses(NeirModule):
    manifest = Manifest(name="lenses", title="Линзы (поверхность рынка)",
                        provides=("surface",), requires=("price",),
                        priority=30, tick_timeout=5.0)

    async def on_start(self):
        # РУЧКА ТОЧНОСТИ (12.10.2026): полураспад линзы = масштаб × factor.
        # 1.0 — полураспад равен масштабу (учебник EWMA). Ведёт Тюнер.
        factor = self.kernel.cfg.lens_half_life_factor
        self._scales = sorted(float(m) * 60.0
                              for m in self.kernel.cfg.lens_scales_min)
        self._lenses = [Lens(s * factor) for s in self._scales]
        self._interval = self.kernel.cfg.lens_publish_interval
        self._last_pub = 0.0
        self.ticks = 0
        self._warmed = False
        # ПРОГРЕВ ИЗ ИСТОРИИ (12.10.2026): минутные свечи со склада сеют
        # EWMA линз, длиннее базовой свечи (≥60с), включив flow_imb по
        # дельте свечи (честный перекос с первой секунды). Субминутная
        # (30с) минутной свечей не прогревается — доживёт с живого потока.
        # Дорожка та же, что у Регрессии/Фракталов: ярлык с номером ТФ.
        base = int(self.kernel.cfg.base_tf_sec)
        self.subscribe(f"{CANDLES_WARMUP}:{base}", self._on_warmup)
        warm = self.get_last(f"{CANDLES_WARMUP}:{base}")
        if warm is not None:
            await self._on_warmup(warm)
        self.subscribe(PRICE, self._on_tick)
        # СЕРДЦЕБИЕНИЕ (п.2 аудита 19.10.2026): срез публикуется по
        # расписанию даже БЕЗ тиков. На тихом рынке формула EWMA не
        # крутится (нет сделок) и замерзает — но замерзшее показание
        # валидно, а ячейкам Линз в Матрице умирать по TTL нельзя.
        # Свежесть читается по age_sec в confidence_vectors: 0 на тиковой
        # публикации, растёт, пока рынок молчит. Если ни одна линза не
        # инициализирована — публиковать нечего, heartbeat молчит.
        self.every(self._interval, self._heartbeat)
        self.log.info("Линзы включены: %d масштабов (%.0fс..%.0fс), "
                      "публикация раз в %.1f с (с сердцебиением), прогрев "
                      "из истории %s",
                      len(self._scales), self._scales[0], self._scales[-1],
                      self._interval, "есть" if self._warmed else "нет")

    async def _heartbeat(self):
        """Публикация среза без тиков (п.2 аудита 19.10.2026)."""
        if not any(lens.init for lens in self._lenses):
            return                               # данных ещё не было
        now = time.monotonic()
        if now - self._last_pub >= self._interval:
            self._last_pub = now
            self.publish(SURFACE, self._snapshot(int(time.time() * 1000)))

    async def _on_warmup(self, env):
        """Партия минутной истории → прогрев EWMA линз (один раз).

        ПАКЕТНЫЙ ПРОГРЕВ (п.3 аудита 19.10.2026): строки парсятся в
        массивы ОДИН раз и уходят в warm_batch каждой линзы — раньше
        int()/float() конверсии повторялись для каждой из 31 линзы на
        каждой свече (31 × тысячи свечей мелких конверсий). Математика
        не тронута — warm_batch побитово равен штучному warm().
        """
        if self._warmed:
            return
        data = env.payload
        try:
            tf = int(data["tf_sec"])
            rows = data["candles"]
        except (KeyError, TypeError):
            return
        base = int(self.kernel.cfg.base_tf_sec)
        if tf != base or not rows:
            return
        self._warmed = True
        # парсинг строк один раз для всех линз (п.3): битая строка
        # выкидывается целиком — как и continue в старом цикле
        ts_a, cl_a, vo_a, de_a = [], [], [], []
        for r in rows:
            try:
                ts_a.append(int(r[0]) / 1000.0)
                cl_a.append(float(r[4]))
                vo_a.append(float(r[5]))
                de_a.append(float(r[6]))
            except (TypeError, ValueError, IndexError):
                continue
        warmed = 0
        for lens, scale in zip(self._lenses, self._scales):
            if scale < base:            # субминутные — только живой поток
                continue
            lens.warm_batch(ts_a, cl_a, vo_a, de_a)
            warmed += 1
        if warmed:
            self.log.info("Прогрев линз из истории: %d линз по %d свечам "
                          "%dс (дельта честная — flow_imb с первой секунды, "
                          "пакетный разбор — один на все линзы)",
                          warmed, len(ts_a), base)
            self.publish(SURFACE, self._snapshot(int(time.time() * 1000)))

    async def _on_tick(self, env):
        p = env.payload
        price = float(p.get("price", 0.0))
        if price <= 0:
            return
        ts_sec = (int(p.get("ts_ms", 0)) or int(time.time() * 1000)) / 1000.0
        qty = float(p.get("qty", 0.0))
        is_sell = bool(p.get("is_sell", False))
        for lens in self._lenses:
            lens.update(ts_sec, price, qty, is_sell)
        self.ticks += 1
        now = time.monotonic()
        if now - self._last_pub >= self._interval:
            self._last_pub = now
            # БЕЗ жёсткого TTL (12.10.2026): поверхность — затухающая EWMA,
            # а не снимок со сроком годности. Живёт на доске последних
            # значений до следующей публикации и протухнуть не может.
            self.publish(SURFACE, self._snapshot(int(ts_sec * 1000)))

    def _snapshot(self, ts_ms):
        """Тензор поверхности (N,4) + scale_vectors (N,1) + confidence_
        vectors (N,2). Чистые числа, без текстовых подписей ТФ.
        Непрогретая линза — NaN (честное «показания нет», а не ложный
        ноль-штиль); её строка в надёжности тоже NaN.

        Возраст (п.1): ts_ms среза минус last_ts линзы — в тех же
        единицах времени, в которых живут тики/свечи. На тиковой
        публикации ~0 (сделка была только что), на heartbeat тихого
        рынка растёт — честная метка «замерло»."""
        n = len(self._scales)
        tensor = np.full((n, SURFACE_COLS), np.nan, dtype=np.float64)
        conf = np.full((n, CONF_COLS), np.nan, dtype=np.float64)
        now_sec = ts_ms / 1000.0
        for i, lens in enumerate(self._lenses):
            row = lens.read_row()
            if row is not None:
                tensor[i, COL_SLOPE] = row[0]
                tensor[i, COL_VOL] = row[1]
                tensor[i, COL_TREND] = row[2]
                tensor[i, COL_FLOW] = row[3]
                conf[i, CONF_NEFF] = lens.n_eff
                conf[i, CONF_AGE] = max(0.0, now_sec - lens.last_ts)
        tensor.flags.writeable = False
        conf.flags.writeable = False
        scale_vectors = np.array(self._scales, dtype=np.float64).reshape(n, 1)
        scale_vectors.flags.writeable = False
        return {"ts_ms": int(ts_ms),
                "surface_tensor": tensor,
                "scale_vectors": scale_vectors,
                "confidence_vectors": conf}

    async def on_stop(self):
        self.log.info("Итог: сделок через линзы %d, масштабов %d",
                      self.ticks, len(self._lenses))


def surface_read(payload, scale_sec):
    """Ближайшая к масштабу scale_sec линза из публикации SURFACE.

    Ищет по ЗНАЧЕНИЮ масштаба (scale_vectors), не по номеру строки — при
    смене лестницы индексы сдвинутся, а значения останутся верными.
    Возвращает тот же словарь {slope_min_pct, vol_min_pct, trend_z,
    flow_imb}, что читали потребители раньше, плюс (19.10.2026, п.1)
    надёжность показания: n_eff — эффективное число наблюдений линзы,
    age_sec — секунд с последней сделки (сердцебиение тихого рынка).
    None — нет поверхности, нет такого масштаба или линза не прогрета
    (NaN).
    """
    tensor = payload.get("surface_tensor")
    scales = payload.get("scale_vectors")
    if tensor is None or scales is None:
        return None
    scales = np.asarray(scales, dtype=np.float64).reshape(-1)
    if scales.size == 0:
        return None
    idx = int(np.argmin(np.abs(scales - float(scale_sec))))
    row = tensor[idx]
    if bool(np.isnan(row).any()):
        return None
    out = {"slope_min_pct": float(row[COL_SLOPE]),
           "vol_min_pct": float(row[COL_VOL]),
           "trend_z": float(row[COL_TREND]),
           "flow_imb": float(row[COL_FLOW])}
    conf = payload.get("confidence_vectors")
    if conf is not None:
        try:
            crow = np.asarray(conf, dtype=np.float64)[idx]
            if not bool(np.isnan(crow).any()):
                out["n_eff"] = float(crow[CONF_NEFF])
                out["age_sec"] = float(crow[CONF_AGE])
        except (IndexError, TypeError, ValueError):
            pass
    return out


def surface_all(payload):
    """ВСЕ прогретые линзы публикации SURFACE: [(scale_sec, {…}), …].

    Читает тензор целиком — для послойной развёртки Сценариста
    (writer._lens_layers). Непрогретые линзы (NaN) пропускаются —
    честное «показания нет». Порядок — как в тензоре (по возрастанию
    полураспада, см. контракт осей COL_*). Ключи n_eff/age_sec (надёжность,
    п.1 аудита 19.10) добавляются, если публикация несёт confidence_
    vectors; старые публикации без него читаются как раньше.
    """
    tensor = payload.get("surface_tensor")
    scales = payload.get("scale_vectors")
    if tensor is None or scales is None:
        return []
    scales = np.asarray(scales, dtype=np.float64).reshape(-1)
    conf = payload.get("confidence_vectors")
    if conf is not None:
        try:
            conf = np.asarray(conf, dtype=np.float64)
        except (TypeError, ValueError):
            conf = None
    out = []
    for i in range(min(len(scales), len(tensor))):
        row = tensor[i]
        if bool(np.isnan(row).any()):
            continue
        item = {"slope_min_pct": float(row[COL_SLOPE]),
                "vol_min_pct": float(row[COL_VOL]),
                "trend_z": float(row[COL_TREND]),
                "flow_imb": float(row[COL_FLOW])}
        if conf is not None and i < len(conf):
            crow = conf[i]
            if not bool(np.isnan(crow).any()):
                item["n_eff"] = float(crow[CONF_NEFF])
                item["age_sec"] = float(crow[CONF_AGE])
        out.append((float(scales[i]), item))
    return out