"""Интуит (Волна 3, Этап 27) — охотник за аналогиями.

Сравнивает ТЕКУЩУЮ геометрию/динамику рынка (вектор 12 компонент,
фиксированный порядок — утверждено) с библиотекой слепков идеала
Лаборатории (lab-snapshot) через косинусную близость. Итог — ФАКТ
для Сценариста: «на топ-5 похожих ситуациях исход был X».

Предохранители (утверждено):
- ПОЛ НОРМЫ 0.15: пустой рынок (вектор около нуля) похож на всё —
  молчим;
- МИНИМУМ БИБЛИОТЕКИ 20 валидных слепков — молчим (статистики нет);
- топ-5, итог взвешен близостью × качеством (ideal_confidence);
- стартовое доверие 0.2 — новичок зарабатывает (Тюнер двигает);
- проверяемая расписка: hint публикуется, Лаборатория верифицирует,
  Тюнер калибрует.
"""
import time

import numpy as np

from core.module import Manifest, NeirModule
from modules._labels import (CANDLES, CARD, CONDUCTIVITY, DRAFT, FACT_ALIASES,
                             FACT_AXES, FACT_AXIS_NAMES, PRICE)


def tf_label(tf_sec):
    """Метка ТФ: 60 -> '1m', 14400 -> '4h' (та же, что у Сценариста)."""
    secs = int(tf_sec or 0)
    if secs <= 0:
        return "0m"
    if secs % 3600 == 0:
        return f"{secs // 3600}h"
    return f"{secs // 60}m"


def _signed_rsi(value, upper, lower):
    """RSI 0..100 -> знак давления ±1 (перекупленность жмёт вниз).

    Та же формула, что у датчика (rsi.py) и Сценариста: иначе вектор
    аналогии и карточка сценария говорили бы разное об одном факте.
    """
    try:
        raw = float(value)
    except (TypeError, ValueError):
        return 0.0
    if raw >= upper:
        signed = (upper - raw) / 30.0 - 1.0
    elif raw <= lower:
        signed = (lower - raw) / 30.0 + 1.0
    else:
        signed = (50.0 - raw) / 30.0
    return max(-1.0, min(1.0, signed))


NORM_FLOOR = 0.15       # пол нормы (утверждено)
MIN_LIBRARY = 20        # минимум валидных слепков (утверждено)
TOP_N = 5               # топ-совпадений (утверждено)


def _norm(vec):
    return float(np.linalg.norm(vec))


def _cosine(a, b):
    na, nb = _norm(a), _norm(b)
    if na <= 0 or nb <= 0:
        return 0.0
    return float(np.dot(a, b) / (na * nb))


def _resolve_fv(fv):
    """Приводит ключи вектора к каноническим осям FACT_AXES (п.5 аудита).

    Слепки Лаборатории наследуют имена видов Сценариста (leader/oi/package);
    канонические имена (btc_dir/oi_trend/package_bias) — как пишет живой
    путь. Канонический ключ главнее алиаса; живому пути резолв не мешает.
    """
    if not fv:
        return {}
    for canon, alts in FACT_ALIASES.items():
        if canon not in fv:
            for alt in alts:
                if alt in fv:
                    fv[canon] = fv[alt]
                    break
    return fv


def _axis_value(name, src, norm, geom=None, dyn=None, fv=None,
                upper=70.0, lower=30.0):
    """Значение одной оси вектора аналогии по ЕДИНОЙ нормировке.

    Один и тот же закон для слепка из базы (Лаборатория) и для живого
    вектора (Интуит) — иначе они сравнивают разные величины.
    """
    geom = geom or {}
    dyn = dyn or {}
    fv = _resolve_fv(fv)
    if norm == "atr":
        return float(geom.get(name, 1.0))
    if norm == "angle":
        return float(dyn.get("trend_angle", 0.0)) / 0.5
    if norm == "decay":
        return float(dyn.get("trend_decay", 0.0))
    if norm == "signed":
        return _signed_rsi(fv.get(name, 50.0), upper, lower)
    if norm == "acc":
        # ускорение Регрессии по минуткам — доли процента, нормируем ×1000
        return max(-1.0, min(1.0, float(fv.get(name, 0.0)) * 1000.0))
    if norm == "score4h":
        return max(-1.0, min(1.0, float(fv.get(name, 0.0))))
    if norm == "lifetime":
        return max(-1.0, min(1.0, float(fv.get(name, 0.0)) / 30.0))
    if norm == "btc":
        # ЗНАКОВАЯ ШКАЛА ВОЖАКА (п.5 аудита): оба источника — живой путь
        # (_current_vector) и слепок Лабы — несут уже нормированный
        # clamp(угол/0.10) в −1..1. Прежнее повторное деление на 0.10
        # сажало ось на сатурацию: |угол| ≥ 0.005% → всегда ±1.
        try:
            return max(-1.0, min(1.0, float(fv.get(name, 0.0))))
        except (TypeError, ValueError):
            return 0.0
    return float(fv.get(name, 0.0))


class Intuit(NeirModule):
    manifest = Manifest(name="intuit", title="Интуит (аналогии)",
                        priority=55, tick_timeout=10.0)

    async def on_start(self):
        c = self.kernel.cfg
        self.top_n = max(1, c.get_int("intuit", "top_n", TOP_N))
        self.norm_floor = c.get_float("intuit", "norm_floor", NORM_FLOOR)
        self.min_library = max(1, c.get_int("intuit", "min_library",
                                            MIN_LIBRARY))
        self.publish_every = c.get_float("intuit", "publish_sec", 20.0)
        self._atr = None
        self._picture = None
        self._meta = None
        self._leader = None
        self._rsi = None
        # РЕГРЕССИЯ ПО УРОВНЯМ (утверждено, вариант б): Интуит читает
        # измерители напрямую, по метке ТФ — '1m' (тактика) и '4h'
        # (стратегия). Не «самая громкая карточка», а обе.
        self._reg_by_tf = {}
        self._upper = self.kernel.cfg.rsi_upper
        self._lower = self.kernel.cfg.rsi_lower
        self._closes = []
        self._hints = 0
        self.price = 0.0
        self.library_size = 0          # размер библиотеки аналогий (последний)
        self.keeper = self.kernel.modules.get("keeper")
        self.subscribe(CARD, self._on_card)
        self.subscribe(CANDLES, self._on_candles)
        self.subscribe(PRICE, self._on_price)
        self.every(self.publish_every, self._pulse)
        self.log.info("Интуит включён: вектор 12 компонент, пол нормы "
                      "%.2f, минимум библиотеки %d, топ-%d",
                      self.norm_floor, self.min_library, self.top_n)

    async def on_stop(self):
        self.log.info("Итог: карточек-подсказок %d", self._hints)

    # ---------- приём ----------

    async def _on_candles(self, env):
        if int(env.payload.get("tf_sec", 0)) != self.kernel.cfg.base_tf_sec:
            return
        if not env.payload.get("closed"):
            return
        for r in env.payload.get("candles") or []:
            self._closes.append((int(r[0]), float(r[2]), float(r[3]),
                                 float(r[4])))
        self._closes = self._closes[-100:]
        self._atr = self._calc_atr()

    def _calc_atr(self):
        if len(self._closes) < 15:
            return None
        trs = []
        for i in range(1, 15):
            _, h, l, _ = self._closes[-i]
            ph = self._closes[-i - 1][3]
            trs.append(max(h - l, abs(h - ph), abs(l - ph)))
        return sum(trs) / len(trs)

    async def _on_price(self, env):
        p = float(env.payload.get("price", 0.0))
        if p > 0:
            self.price = p

    async def _on_card(self, env):
        card = env.payload
        kind = card.get("kind")
        if kind == "picture":
            self._picture = card.get("fields") or {}
        elif kind == "meta-corr":
            self._meta = card.get("fields") or {}
        elif kind == "leader-correlation":
            self._leader = card.get("fields") or {}
        elif kind == "rsi-state":
            self._rsi = card.get("fields") or {}
        elif kind == "trend-vector":
            # Регрессия напрямую, по уровню ТФ (вариант б)
            f = card.get("fields") or {}
            try:
                tf = int(f.get("tf_sec", 0))
            except (TypeError, ValueError):
                return
            self._reg_by_tf[tf_label(tf)] = f

    def _cond_values(self):
        """(speed_bias, oi_trend) из Проводимости — среда без посредников."""
        env = self.get_last(CONDUCTIVITY)
        if env is None:
            return 0.0, 0.0
        cond = env.payload.get("conditions") or {}
        try:
            return (float(cond.get("speed_bias", 0.0)),
                    max(-1.0, min(1.0, float(cond.get("oi_trend", 0.0)))))
        except (TypeError, ValueError):
            return 0.0, 0.0

    # ---------- сборка текущего вектора ----------

    def _current_vector(self):
        """Вектор 16 компонент из ПРЯМЫХ измерителей (утверждено).

        ВАРИАНТ (б): Интуит — независимый охотник за аналогиями, он читает
        измерители напрямую (Регрессию и RSI по обоим уровням) и НЕ
        подглядывает в шпаргалку Сценариста. Раньше он брал единственную
        «самую громкую» карточку Поля — то самое плоское усреднение, которое
        мы вырезали из пирамиды линз.

        Порядок осей ЖЁСТКО синхронизирован с `lab._facts_vec`: Лаборатория
        собирает такой же вектор по истории. Расхождение порядка — и
        библиотека аналогий перестаёт сопоставлять векторы, поэтому
        одинаковый набор имён держим в обоих модулях (см. FACT_AXES).
        """
        if self._picture is None or self._atr in (None, 0.0) \
                or self.price <= 0:
            return None
        pf = self._picture
        ups = [float(z.get("low", 0)) for z in pf.get("zones") or []
               if float(z.get("low", 0)) > self.price]
        dns = [float(z.get("high", 0)) for z in pf.get("zones") or []
               if float(z.get("high", 0)) < self.price]
        dist_up = min(1.0, (min(ups) - self.price) / self._atr) \
            if ups else 1.0
        dist_dn = min(1.0, (self.price - max(dns)) / self._atr) \
            if dns else 1.0
        tr = pf.get("trend") or {}
        fuel = pf.get("fuel") or {}
        meta = self._meta or {}
        leader = self._leader or {}
        try:
            btc_dir = max(-1.0, min(1.0, float(
                leader.get("angle_btc_pct", 0.0)) / 0.10))
        except (TypeError, ValueError):
            btc_dir = 0.0
        tape_speed, oi_trend = self._cond_values()
        # РЕГРЕССИЯ ПОСЛОЙНА: тактика (1м) и стратегия (4ч) — раздельно.
        reg = self._reg_by_tf
        r1 = reg.get("1m") or {}
        r4h = reg.get("4h") or {}

        def _num(src, key, default=0.0):
            try:
                return float(src.get(key, default))
            except (TypeError, ValueError):
                return default
        # Ускорение нормируем к своему масштабу (иначе всегда ~0), затухание
        # оставляем знаковым «тренд ещё жив / угас».
        reg_acc_1m = max(-1.0, min(1.0, _num(r1, "acceleration") * 1000.0))
        reg_score_4h = max(-1.0, min(1.0, _num(r4h, "score")))
        # Сборка по ЕДИНЫМ осям FACT_AXES — тот же закон, что у слепков из
        # базы. Живые источники подставляются в поля facts_vec, дальше всё
        # считается общим кодом: разойтись порядок не может физически.
        fv = dict(self._rsi or {})
        fv["reg_acc_1m"] = reg_acc_1m
        fv["reg_score_4h"] = reg_score_4h
        fv["btc_dir"] = btc_dir
        fv["reg_lifetime_4h"] = _num(r4h, "lifetime_min")
        fv["oi_trend"] = oi_trend
        fv["tape_speed"] = tape_speed
        fv["package_bias"] = float(meta.get("package_bias", 0.0))
        fv["divergence"] = float(meta.get("divergence", 0.0))
        geom_live = {"dist_up_atr": dist_up, "dist_dn_atr": dist_dn}
        dyn_live = {"trend_score": float(tr.get("score", 0.0)),
                    "trend_angle": float(tr.get("angle_pct", 0.0)),
                    "trend_decay": float(tr.get("decay_rate", 0.0)),
                    "flow_score": float(fuel.get("score", 0.0))}
        vec = [_axis_value(name, _src, norm, geom=geom_live, dyn=dyn_live,
                           fv=fv, upper=self._upper, lower=self._lower)
               for name, _src, norm in FACT_AXES]
        return np.array(vec, dtype=float)

    # ---------- библиотека слепков ----------

    @staticmethod
    def _validate_snap(f):
        """ПРОВЕРКА СЛЕПКА (утверждено): автономный снимок Лаборатории
        обязан нести всё, на чём держится аналогия. Нет исхода или вектора
        фактов — слепок в библиотеку не идёт, и это видно в журнале, а не
        молча. Отсутствие геометрии/динамики допускается: их значения
        нейтральны и подставляются по умолчанию."""
        if not isinstance(f, dict):
            return None, "снимок не словарь"
        if f.get("outcome_dir") not in ("long", "short"):
            return None, "исход неизвестен"
        fv = f.get("facts_vec")
        if not isinstance(fv, dict) or not fv:
            return None, "нет вектора фактов (facts_vec)"
        if not isinstance(f.get("geometry"), dict):
            f["geometry"] = {}          # нейтральные значения по умолчанию
        if not isinstance(f.get("dynamics"), dict):
            f["dynamics"] = {}
        try:
            quality = max(0.0, min(1.0, float(f.get("ideal_confidence", 0.5))))
        except (TypeError, ValueError):
            quality = 0.5
        f["ideal_confidence"] = quality
        return f, None

    async def _library(self):
        """Валидные слепки: (вектор numpy, исход ±1, качество 0..1)."""
        if self.keeper is None:
            return []
        lib = []
        skipped = 0
        # Слепки Лабы — свой вид карточек (аудит 27.09): раньше брались
        # «последние 400 вообще» — поток картины/энтропии/вожака вытеснял
        # редкие снимки (в окне оказывалось 2 при минимуме библиотеки 20),
        # и Интуит молчал, хотя слепков в базе сотни
        for c in await self.keeper.get_last_cards(400,
                                                  kind="lab-snapshot"):
            if c.get("kind") != "lab-snapshot":
                continue
            f, why = self._validate_snap(c.get("fields") or {})
            if f is None:
                skipped += 1
                continue
            geom, dyn = f.get("geometry") or {}, f.get("dynamics") or {}
            fv = f.get("facts_vec") or {}
            # СБОРКА ПО ЕДИНЫМ ОСЯМ (утверждено): набор и порядок берём из
            # FACT_AXES — того же списка, что и в _current_vector(). Раньше
            # здесь было 12 осей иного порядка, чем у живого вектора:
            # косинусная близость сравнивала разные величины.
            try:
                vec = np.array(
                    [_axis_value(name, src, norm, geom=geom, dyn=dyn, fv=fv,
                                  upper=self._upper, lower=self._lower)
                     for name, src, norm in FACT_AXES], dtype=float)
            except (TypeError, ValueError):
                skipped += 1
                continue
            outcome = 1.0 if f.get("outcome_dir") == "long" else -1.0
            lib.append((vec, outcome, float(f["ideal_confidence"]),
                        c.get("seq")))
        if skipped:
            self.log.info("Библиотека: %d слепков отброшено как "
                          "непригодные для аналогии", skipped)
        self.library_size = len(lib)
        return lib

    # ---------- пульс ----------

    async def _pulse(self):
        vec_list = self._current_vector()
        if vec_list is None:
            return
        a = np.array(vec_list, dtype=float)
        if _norm(a) < self.norm_floor:
            return                     # ПОЛ НОРМЫ (утверждено): рынок пуст
        lib = await self._library()
        if len(lib) < self.min_library:
            return                     # МИНИМУМ БИБЛИОТЕКИ (утверждено)
        scored = sorted(((_cosine(a, v), outcome, quality, seq)
                         for v, outcome, quality, seq in lib),
                        key=lambda t: -t[0])
        top = [t for t in scored[:self.top_n] if t[0] > 0]
        if not top:
            return
        num = sum(t[1] * t[0] * t[2] for t in top)
        den = sum(t[0] * t[2] for t in top)
        bias = round(max(-1.0, min(1.0, num / den)), 3) if den > 0 else 0.0
        self._hints += 1
        self.publish(DRAFT, {
            "kind": "intuit-hint",
            "author": "intuit",
            "confidence": 0.9,
            "fields": {
                "bias": bias,
                "matches": len(top),
                "top_similarity": round(top[0][0], 3),
                "library": len(lib),
                "ts_ms": int(time.time() * 1000),
            },
            # НИТЬ К СНЕПКАМ (протокол lineage, Узел 5): подсказка построена
            # на топ-N ближайших слепках Лаборатории — карточки-родители
            # стоят в ссылках: hint → card(snap) → scenario → …
            "lineage": [f"card:{t[3]}" for t in top[:5]],
        })
        self.log.info("Подсказка Интуита: bias %+.2f (топ-%d, близость "
                      "%.2f, библиотека %d)", bias, len(top),
                      top[0][0], len(lib))