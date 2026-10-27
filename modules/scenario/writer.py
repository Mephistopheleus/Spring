"""Сценарист — превращает картину рынка в сценарий сделки.

Оценивает ВСЕ доступные данные (утверждено): картину (магниты +
настроение), живую цену, волатильность (ATR своих свечей).

Утверждённые правила:
- направление: куда тянут магниты (70%) + настроение новостей (30%);
  неясно — сценария нет (мусорных сценариев не бывает);
- вход: живая цена, зона допуска ±0.1 ATR;
- цель (ступень 1): считаются ВСЕ зоны-мишени, выбирается лучшая по
  чистым деньгам за время маршрута (score = чистые/√T); дрожь и время
  маршрута — со старшего масштаба Линз, а не с минутного шума;
- стоп: структурный (за встречной зоной), в коридоре «дрожь ожидания …
  доля пути до цели» — выживает шум маршрута, RR не хуже 1/stop_frac;
- ЦЕЛЬ СЦЕНАРИСТА — не магнит, а ЧИСТАЯ ПРИБЫЛЬ: комиссия, спред и
  проскальзывание вычитаются из хода к цели; сценарий пишется только
  если чистая прибыль выше порога (config: [scenario]);
- уверенность = ВЕРОЯТНОСТЬ прибыльного закрытия: уверенность картины ×
  сила направления × доля чистой прибыли в ходе × среда (шторм давит:
  × (1 − storm_conf_factor×R)); калибруется тюнером;
- ВЕТО среды (утверждено): шторм-вынос (ликвидации летят, ΔOI падает —
  позиции выносит) + сценарий ПО каскаду = ловушка продолжения,
  сценарий не штампуется; ПРОТИВ каскада (разворот от выгоревшей
  полки) — разрешён. Границы нерушимы: издержки, стоп и вердикт
  риск-менеджера вето не отменяет;
- громкость тренда/топлива — из матрицы Проводимости (шов со Средой),
  нет записи — legacy k_impulse;
- плечо: стартует с минимального (1x), растёт по риску и уверенности,
  но ЖЕЛЕЗНОЕ ограничение — ликвидация должна быть дальше стопа;
- сопровождение: начальный СЛ + трейлинг; активация и шаг — доли пути
  до цели; шторм/тишина/преграды — Дилер; безубыток-костыль убран;
- НЕПРЕРЫВНОСТЬ: сценарий пересчитывается при каждом обновлении цены
  (не чаще раза в секунду); запись на склад — по значимости
  (сменился расчёт) или устареванию, с паузой между штампами.
- ВЕКТОР МОМЕНТА (утверждено): при штампе в карточку едет ПОЛНЫЙ
  слепок данных сделки — все виды данных как есть: сырое значение,
  знак, сила (фактический вклад в bias), свежесть/возраст, состояние
  (fresh/stale/missing). Плюс настройки момента: веса, доверия и
  ручки, которыми жил контур. Решения формула не меняет — только
  запись; Лаборатория копирует вектор в снимок, Тюнер просчитывает.
"""
import collections
import math
import time

import numpy as np

from core.module import Manifest, NeirModule
from modules._labels import (FRACT_PARAMS, FRACT_TFS,
                             CANDLES, CANDLES_WARMUP, CARD, CONDUCTIVITY,
                             DRAFT, FACT_AUTHORS, LENS_PARAMS, PRICE,
                             REG_PARAMS, REG_TFS, SURFACE, lens_field,
                             psy_field, reg_field, register_fractal_fields,
                             register_hurst_fields, register_lens_fields,
                             register_meta_fields, META_METRICS, META_DERIVS,
                             register_psy_fields, register_reg_fields,
                             register_rsi_fields, register_slope_fields)
from modules._knobs import (ALL_KNOBS, PSY_SECTIONS, REG_LIFETIME_NORM_MIN,
                            TUNER_KNOBS, WEIGHT_KNOBS, register_lens_knobs,
                            register_psy_knobs, register_reg_knobs)
from modules.market.lenses import surface_all

MIN_CANDLES = 30        # меньше свечей — ATR не считаем
ATR_PERIOD = 14
STAMP_REFRESH_SEC = 90  # устаревание сценария
RECHECK_SEC = 1.0       # пересчёт при движении цены — не чаще раза в секунду
KEEP_CANDLES = 150
# Окно жизни карточки Регрессии для затвора готовности и для послойных
# полей reg_* (секунд). Регрессия публикует редко (только при прямом
# канале и устоявшемся знаке), поэтому окно generous: 900 с = 15 минут.
REG_MAX_AGE_SEC = 900.0
# ПРЕДОХРАНИТЕЛЬ ЗАТВОРА (утверждено 02.10): ждать первого измерения
# бесконечно нельзя — Регрессия публикует карточку trend-vector РЕДКО
# (только при прямом канале, r2 >= 0.35 и устоявшемся знаке), и на шумном
# рынке она может молчать дольше. Через этот срок Сценарий пишет сценарий,
# помечая несобранные оси как stale: лучше карточка с честной пометкой,
# чем робот, который молча стоит и никому не говорит почему.
GATE_WAIT_MAX_SEC = 420.0


def tf_label(tf_sec):
    """Метка ТФ для имён полей: 60 -> '1m', 14400 -> '4h'.

    Единая точка правды для всех послойных имён (reg_*, lens_*): чтобы
    масштаб в имени и масштаб в карточке не разъехались.
    """
    secs = int(tf_sec or 0)
    if secs <= 0:
        return "0m"
    if secs % 3600 == 0:
        return f"{secs // 3600}h"
    return f"{secs // 60}m"


def topup_leverage(leverage, lev_cap, min_notional, margin_usdt):
    """Добор плеча до исполнимости (Этап 35): объём (маржа × плечо)
    обязан накрыть минимум биржи (округление ВВЕРХ, чтобы не остаться
    в копейке ниже); потолок — lev_cap (max_lev, ликвидация). Не
    добрали — вернём как есть: Риск уведёт в тень честно (below_min)."""
    if min_notional <= 0 or margin_usdt <= 0:
        return leverage
    need = min_notional / margin_usdt
    if need <= leverage:
        return leverage
    if need > lev_cap:
        return leverage            # потолок не позволяет — честная тень
    lev = math.ceil(need * 10) / 10.0
    return round(max(leverage, lev), 1)


def _vec_entry(*, kind, value, weight, push, age_sec, state,
               conf=None, trust=None, extra=None):
    """Запись вектора момента (утверждено): данные КАК ЕСТЬ.

    Без порогов и отсева: сырое значение, знак, сила (|вклад в bias|),
    фактический вклад (push, со знаком), свежесть/возраст и состояние
    (fresh/stale/missing). Нет данных — value=None и state="missing",
    запись всё равно остаётся: «не слышал» тоже факт момента.
    """
    try:
        v = float(value)
    except (TypeError, ValueError):
        v = None
    p = 0.0
    try:
        p = float(push or 0.0)
    except (TypeError, ValueError):
        p = 0.0
    entry = {
        "kind": kind,
        "value": (round(v, 6) if v is not None else None),
        "sign": (1 if v > 0 else -1 if v < 0 else 0) if v is not None else 0,
        "strength": round(abs(p), 6),
        "push": round(p, 6),
        "weight": round(float(weight or 0.0), 4),
        "age_sec": (round(float(age_sec), 1) if age_sec is not None else None),
        "state": state,
    }
    if conf is not None:
        try:
            entry["conf"] = round(float(conf), 4)
        except (TypeError, ValueError):
            pass
    if trust is not None:
        try:
            entry["trust"] = round(float(trust), 4)
        except (TypeError, ValueError):
            pass
    if extra:
        entry["extra"] = extra
    return entry


class ScenarioWriter(NeirModule):
    manifest = Manifest(name="scenario-writer", title="Сценарист",
                        requires=("card",), priority=70, tick_timeout=10.0)

    async def on_start(self):
        self.symbol = self.kernel.cfg.symbol.upper()
        self.base_tf_sec = self.kernel.cfg.base_tf_sec
        self._rows = collections.deque(maxlen=KEEP_CANDLES)
        self._picture = None            # {seq, created_ms, confidence, fields}
        self.price = 0.0
        self._last_stamp_ms = 0
        self._signature = None
        self._published = {}    # живые сценарии: ключ → {direction, magnet}
        self._picture_lookups = 20      # добор картины из базы (поздний старт)
        self.issued = 0
        self.skipped = 0
        # Издержки внутри сценария (утверждено)
        self.fee_pct = self.kernel.cfg.fee_pct / 100.0
        self.slip_pct = self.kernel.cfg.scenario_slippage_pct / 100.0
        self.round_cost = 2.0 * (self.fee_pct + self.slip_pct)
        # ФИЛЬТР ХОДА (утверждено): минимальный чистый ход = издержки ×
        # запас (ручка net_margin), но не ниже пола min_net_move_pct.
        # 0.35% резало 98% путей, окупавших издержки — теперь по деньгам.
        self.net_margin = self.kernel.cfg.scenario_net_margin
        self.min_net_move_pct = self.kernel.cfg.scenario_min_net_move_pct
        self.min_net_move = max(self.min_net_move_pct / 100.0,
                                self.round_cost * self.net_margin)
        self.risk_pct = self.kernel.cfg.scenario_risk_pct / 100.0
        self.max_lev = self.kernel.cfg.scenario_max_lev
        self.bias_min = self.kernel.cfg.scenario_bias_threshold
        self.liq_buffer = self.kernel.cfg.risk_liq_buffer
        self.stamp_min_interval = self.kernel.cfg.scenario_stamp_min_interval
        self.stop_atr_floor = self.kernel.cfg.scenario_stop_atr_floor
        self.storm_factor = self.kernel.cfg.scenario_storm_conf_factor
        self.storm_veto = self.kernel.cfg.scenario_storm_veto
        # Поводырь (вожак BTC, утверждено): вес факта, штраф, срок свежести
        self.leader_w = self.kernel.cfg.leader_weight
        self.leader_penalty = self.kernel.cfg.leader_penalty
        self.leader_stale = self.kernel.cfg.leader_stale_sec
        self._leader = None
        # RSI-датчик и прямой OI-фактор (утверждено): исчерпание хода
        # и топливо открытого интереса — в формуле направления
        self.rsi_w = self.kernel.cfg.rsi_weight
        self.rsi_stale = self.kernel.cfg.rsi_stale_sec
        self.oi_w = self.kernel.cfg.scenario_oi_weight
        self._rsi_state = None
        # TTE-календарь (утверждено): факт времени до события + вес
        self.tte_compress_start = self.kernel.cfg.tte_compress_start_min
        self.tte_max_pressure = self.kernel.cfg.tte_max_pressure
        self._tte = None
        self._tte_weight = None
        # Энтропия тиков (утверждено): хаос/кристаллизация потока
        self.entropy_threshold = self.kernel.cfg.entropy_high_threshold
        self.entropy_penalty = self.kernel.cfg.entropy_penalty
        self._entropy = None
        self._entropy_weight = None
        # ПСИХОЗ СРЕДЫ (Шаг 1 ТЗ): три измерителя плотности/хаоса/жадности.
        # viscosity — из Потока (flow-state); None = среда ещё не измерена,
        # до первых 5 сделок в окне. Далее все три адресны, без переборов.
        self._viscosity = None
        # Метакоррелятор (утверждено): пакет альтов + паника вожака
        self.meta_weight = self.kernel.cfg.meta_weight
        self.meta_diverge_penalty = self.kernel.cfg.meta_diverge_penalty
        self.meta_stale = self.kernel.cfg.meta_stale_sec
        self._meta = None
        self._meta_weight = None
        # Интуит (утверждено): аналогии, стартовое доверие 0.2
        self.intuit_w = self.kernel.cfg.intuit_weight
        self._intuit = None
        self._intuit_weight = None
        # Фракталы (утверждено): Херст + резонансные узлы
        self.fractal_bias_w = self.kernel.cfg.fractal_bias_weight
        self.fractal_hurst_penalty = self.kernel.cfg.fractal_hurst_penalty
        self.fractal_node_prox = self.kernel.cfg.fractal_node_prox_pct
        self.fractal_node_ttl = self.kernel.cfg.fractal_node_ttl_factor
        self._topo = None
        self._topo_weight = None
        # Пружина (Этап 32): растяжение от VWAP-якоря + затухание OI
        self.spring_w = self.kernel.cfg.spring_weight
        # СВЕЖЕСТЬ фактов (канон вместо зашитых TTL 120_000/60_000 мс):
        # просроченный факт — вклад «stale», свежий — «fresh» (аудит 27.10).
        self.spring_fresh_ms = self.kernel.cfg.spring_fact_fresh_sec * 1000.0
        self.intuit_fresh_ms = self.kernel.cfg.intuit_fact_fresh_sec * 1000.0
        self.fractal_fresh_ms = self.kernel.cfg.fractal_fact_fresh_sec * 1000.0
        self._spring = None
        # Фракталы 2.0 (Этап 32): полное самоподобие слоёв
        self.selfsim_w = self.kernel.cfg.fractal_selfsim_weight
        self.selfsim_min = self.kernel.cfg.fractal_selfsim_min_corr
        self._last_check = 0.0
        # МАРШРУТ (ступень 1, утверждено): цель выбирается из ВСЕХ зон по
        # чистым деньгам за время пути; стоп и сопровождение — в масштабе
        # маршрута (дрожь из Линз), а не минутного шума
        self.route_vol_scale_min = self.kernel.cfg.scenario_route_vol_scale_min
        self.route_stop_frac = self.kernel.cfg.scenario_route_stop_frac
        self.route_stop_noise_mult = (
            self.kernel.cfg.scenario_route_stop_noise_mult)
        self.route_wait_min = self.kernel.cfg.scenario_route_wait_min
        self.route_speed = (
            self.kernel.cfg.scenario_route_speed_pct_min / 100.0)
        self.route_trail_act_frac = (
            self.kernel.cfg.scenario_route_trail_activate_frac)
        self.route_trail_dist_frac = (
            self.kernel.cfg.scenario_route_trail_distance_frac)
        self.route_max_dist = (
            self.kernel.cfg.scenario_route_max_dist_pct / 100.0)
        # ВЕСА НАПРАВЛЕНИЯ (аудит 27.09): вынесены из кода в конфиг,
        # стартовые значения — по замерам истории (884 сценария)
        self.magnet_w = self.kernel.cfg.scenario_magnet_weight
        self.news_w = self.kernel.cfg.scenario_news_weight
        self.fuel_w = self.kernel.cfg.scenario_fuel_weight
        self.trend_w = self.kernel.cfg.scenario_trend_weight
        self.tape_speed_w = self.kernel.cfg.scenario_tape_speed_weight
        self.magnets_dist_k = self.kernel.cfg.scenario_magnets_dist_k
        self._surface = None
        # ПОСЛОЙНЫЕ ВЕСА ПИРАМИДЫ (утверждено): свой вес на каждый масштаб
        # и параметр. Справочники FACT_AUTHORS/WEIGHT_KNOBS расширяются
        # под реальные масштабы из конфига, чтобы Тюнер узнал все 3×N полей.
        self.lens_scales = self.kernel.cfg.lens_scales_min
        register_lens_fields(self.lens_scales)
        register_meta_fields(self.kernel.cfg.meta_pool)
        register_lens_knobs(self.lens_scales)
        # Послойная РЕГРЕССИЯ и два RSI (утверждено)
        register_reg_fields()
        register_reg_knobs()
        register_rsi_fields()
        register_psy_fields()
        register_psy_knobs()
        # ФРАКТАЛЫ И ХЕРСТ в справочнике (п.4 аудита 18.10): раньше
        # register_fractal_fields/register_hurst_fields были определены,
        # но НИКЕМ не вызывались (register_all_fields тоже) — оси
        # fractal_corr_4h/hurst_up_1h отсутствовали в FACT_AUTHORS, и
        # Лаба/Тюнер не знали автора этих полей.
        register_fractal_fields()
        register_hurst_fields()
        # НАКЛОНЫ ПО КОЛЬЦАМ (20.10, Модуль 3): trend_slope_{tf} —
        # числа вместо вырезанного fractal_bias
        register_slope_fields()
        # Пороги Психоза Среды: окно жизни показаний и порог «свежести»
        self.psy_stale_sec = self.kernel.cfg.psy_stale_sec
        self.entropy_window = self.kernel.cfg.entropy_window_ticks
        self.anomaly_min_mass = self.kernel.cfg.psy_anomaly_min_mass
        self.cloud_atr_mult = self.kernel.cfg.psy_cloud_atr_mult
        self.cascade_strat_min = self.kernel.cfg.psy_cascade_strat_min
        self.cascade_bend_min = self.kernel.cfg.psy_cascade_bend_min
        self.cascade_step_atr = self.kernel.cfg.psy_cascade_step_atr
        # Зоны Поля для координат входа — заполняется в _maybe_stamp()
        self._zones = []
        # Плотность стакана по Потоку (trust карточки flow-state)
        self._flow_trust = 0.5
        self.strategy_tf = self.kernel.cfg.strategy_tf_sec
        # Пороги RSI — те же, что у датчика (rsi.py): нормировка послойных
        # полей rsi_base/rsi_strat обязана совпадать с его логикой, иначе
        # вектор момента и карточка будут говорить разное.
        self.rsi_upper = self.kernel.cfg.rsi_upper
        self.rsi_lower = self.kernel.cfg.rsi_lower
        self.rsi_stale = self.kernel.cfg.rsi_stale_sec
        # Регрессия по уровням: tf_label -> {fields, ts, trust}
        self._trend_by_tf = {}
        # Затвор готовности: сколько раз ждали первого измерения
        self._gate_waits = 0
        self._gate_since = 0      # когда начали ждать (мс)
        self._gate_stale = False   # предохранитель сработал, ось помечена stale
        # КАНАЛ ТРЕНДА (утверждено): границы движения от Регрессии —
        # «прогноз пути» для цели; раньше поле никто не читал
        self._trend_card = None
        self._trend_ts = 0

        self.subscribe(CARD, self._on_card)
        self.subscribe(CANDLES, self._on_candles)
        # ПРОГРЕВ (06.10): история публикуется в ФОНЕ Сборщика — батч
        # может прийти после нашего старта, один get_last не спасает.
        self.subscribe(f"{CANDLES_WARMUP}:{self.base_tf_sec}",
                       self._on_candles)
        self.subscribe(PRICE, self._on_price)
        self.subscribe(SURFACE, self._on_surface)

        # Прогрев: свечи 5м с доски значений + последняя картина из базы
        warm = self.get_last(f"{CANDLES_WARMUP}:{self.base_tf_sec}")
        if warm is not None:
            await self._on_candles(warm)
        await self._lookup_picture()
        self.every(30, self._maybe_stamp)   # сцена не должна стареть молча
        self.log.info("Сценарист готов: картина №%s, свечей %d",
                      self._picture["seq"] if self._picture else "нет",
                      len(self._rows))

    async def on_stop(self):
        self.log.info("Итог: сценариев %d, пропущено %d (нет направления "
                      "или издержки съедают прибыль)",
                      self.issued, self.skipped)

    # ---------- приём данных ----------

    async def _lookup_picture(self):
        """Последняя картина из базы (на случай, если событие пропустили)."""
        keeper = self.kernel.modules.get("keeper")
        if keeper is None:
            return
        for card in await keeper.get_last_cards(50):
            if card.get("kind") == "picture":
                self._picture = {
                    "seq": int(card["seq"]),
                    "created_ms": int(card.get("created_ms", 0)),
                    "confidence": float(card.get("confidence", 0.0)),
                    "trust": float(card.get("trust", 0.5) or 0.5),
                    "fields": card.get("fields") or {},
                }
                return

    async def _on_card(self, env):
        card = env.payload
        if card.get("kind") == "trend-vector":
            # КАНАЛ ТРЕНДА (утверждено): границы движения + наклон —
            # прогноз пути; карточка живёт, пока свежая (проверка ниже)
            fields = card.get("fields") or {}
            self._trend_card = fields
            # ПОСЛОЙНАЯ РЕГРЕССИЯ (утверждено): Регрессия считает карточку
            # для КАЖДОГО уровня (1м тактика, 4ч стратегия). Держим их
            # раздельно по метке ТФ — раньше приходила одна, и поля
            # velocity/acceleration/lifetime_min/decay_rate выбрасывались.
            try:
                tf_sec = int(fields.get("tf_sec", 0))
            except (TypeError, ValueError):
                tf_sec = 0
            self._trend_by_tf[tf_label(tf_sec)] = {
                "fields": fields,
                "ts": int(card.get("created_ms", 0) or 0),
                "trust": float(card.get("trust", 0.5) or 0.5),
            }
            # карточка стратегического уровня ведёт канал (совпадает с прежним
            # поведением: поле двигалось по старшему сигналу)
            if tf_sec == self.strategy_tf:
                try:
                    self._trend_ts = int(card.get("created_ms", 0))
                except (TypeError, ValueError):
                    self._trend_ts = 0
                self._trend_trust = float(card.get("trust", 0.5) or 0.5)
            return
        if card.get("kind") == "leader-correlation":
            self._leader = card.get("fields") or {}   # сухие факты вожака
            self._leader_trust = float(card.get("trust", 0.5) or 0.5)
            return
        if card.get("kind") == "rsi-state":
            self._rsi_state = card.get("fields") or {}  # исчерпание хода
            self._rsi_trust = float(card.get("trust", 0.5) or 0.5)
            return
        if card.get("kind") == "flow-state":
            # ВЯЗКОСТЬ СТАКАНА (Шаг 1 ТЗ): плотность среды. Публикует
            # Поток (flow.py: _viscosity), а не Поле — Поле её не знает.
            # >1 жидкая (цена течёт), <1 каменеет (удары поглощаются).
            try:
                v = (card.get("fields") or {}).get("viscosity")
                self._viscosity = (float(v) if v is not None else self._viscosity)
            except (TypeError, ValueError):
                pass
            return
        if card.get("kind") == "picture":
            # Поле держит собственную вязкость зон — резервный источник,
            # если Поток ещё не соизмерил (минимум 5 сделок в окне).
            # ВАЖНО (баг 02.10): здесь НЕ было return — ветка перехватывала
            # карточку picture и НЕ доходила до общего обработчика ниже, где
            # обновляется self._picture. Картина в Сценаристе застывала на
            # одной устаревшей копии, а через пару минут истекала
            # (valid_until_ms) → Сценарист молчал в BLOCK_1. Резервная вязкость
            # перенесена в общий обработчик картины (ниже).
            f = card.get("fields") or {}
            if self._viscosity is None and f.get("viscosity") is not None:
                try:
                    self._viscosity = float(f["viscosity"])
                except (TypeError, ValueError):
                    pass
            # дальше — к общему обработчику картины (обновление self._picture)
        if card.get("kind") == "spring-state":
            # Пружина (Этап 32): растяжение от якоря + затухание OI
            self._spring = card.get("fields") or {}
            self._spring_trust = float(card.get("trust", 0.5) or 0.5)
            return
        if card.get("kind") == "entropy-state":
            # Энтропия тиков (Волна 5, Этап 30, утверждено): хаос потока
            self._entropy = card.get("fields") or {}
            try:
                self._entropy_weight = float(card.get("weight", 1.0))
            except (TypeError, ValueError):
                self._entropy_weight = 1.0
            self._entropy_trust = float(card.get("trust", 0.5) or 0.5)
            return
        if card.get("kind") == "tte-state":
            # TTE (утверждено): факт времени до макро-события + ВЕС
            # карточки (доверие календарю от Регистратора) — сила
            # давления масштабируется тем, насколько календарь заслужил
            self._tte = card.get("fields") or {}
            try:
                self._tte_weight = float(card.get("weight", 1.0))
            except (TypeError, ValueError):
                self._tte_weight = 1.0
            self._tte_trust = float(card.get("trust", 0.5) or 0.5)
            return
        if card.get("kind") == "meta-corr":
            # Метакоррелятор (утверждено): пакет альтов + лаг-лидер +
            # паника вожака. Вес карточки = доверие (как у TTE)
            self._meta = card.get("fields") or {}
            try:
                self._meta_weight = float(card.get("weight", 1.0))
            except (TypeError, ValueError):
                self._tte_weight = self._meta_weight = 1.0
            self._meta_trust = float(card.get("trust", 0.5) or 0.5)
            return
        if card.get("kind") == "intuit-hint":
            # Интуит (утверждено): аналогии — факт со стартовым
            # доверием 0.2; вес карточки масштабирует вклад
            self._intuit = card.get("fields") or {}
            try:
                self._intuit_weight = float(card.get("weight", 0.36))
            except (TypeError, ValueError):
                self._intuit_weight = 0.36
            self._intuit_trust = float(card.get("trust", 0.2) or 0.2)
            return
        if card.get("kind") == "topological-resonance":
            # Фракталы (утверждено): инерция старших слоёв + Херст +
            # резонансные узлы. Вес карточки = доверие
            self._topo = card.get("fields") or {}
            try:
                self._topo_weight = float(card.get("weight", 1.0))
            except (TypeError, ValueError):
                self._topo_weight = 1.0
            self._topo_trust = float(card.get("trust", 0.5) or 0.5)
            return
        if card.get("kind") != "picture":
            return
        try:
            self._picture = {
                "seq": int(card["seq"]),
                "created_ms": int(card.get("created_ms", 0)),
                "confidence": float(card.get("confidence", 0.0)),
                "trust": float(card.get("trust", 0.5) or 0.5),
                "fields": card.get("fields") or {},
            }
            # Резервная вязкость из Поля (перенесено 02.10): ветка выше
            # больше не перехватывает карточку, поэтому источник берём здесь.
            pf = self._picture["fields"]
            if (self._viscosity is None
                    and pf.get("viscosity") is not None):
                try:
                    self._viscosity = float(pf["viscosity"])
                except (TypeError, ValueError):
                    pass
        except (KeyError, TypeError, ValueError):
            return
        await self._maybe_stamp()

    async def _on_candles(self, env):
        data = env.payload
        try:
            if int(data["tf_sec"]) != self.base_tf_sec:
                return
            rows = data["candles"]
        except (KeyError, TypeError):
            return
        if data.get("warmup"):
            self._rows.clear()
            for r in rows:
                self._rows.append(tuple(r))
            return
        if not data.get("closed"):
            return                       # незакрытые волатильность не задают
        for r in rows:
            r = tuple(r)
            if self._rows and self._rows[-1][0] == r[0]:
                self._rows[-1] = r
            elif not self._rows or r[0] > self._rows[-1][0]:
                self._rows.append(r)

    async def _on_price(self, env):
        self.price = float(env.payload.get("price", 0.0))
        # Непрерывность (утверждено): пересчёт при каждом движении цены,
        # разреженный до раза в секунду; штамп — по значимости
        now = time.monotonic()
        if now - self._last_check >= RECHECK_SEC:
            self._last_check = now
            await self._maybe_stamp()

    def _on_surface(self, env):
        """Поверхность Линз: волатильность по масштабам (дрожь маршрута)."""
        self._surface = env.payload

    def _vol_route(self):
        """Дрожь маршрута: волатильность (доля/мин) с ближайшего к
        настроенному масштабу Линз. (None, None) — поверхности нет,
        работаем по минутному ATR (фолбэк).

        Чтение ТЕНЗОРА (п.2 аудита 18.10): раньше здесь искался ключ
        `scales` — такого ключа в публикации SURFACE больше нет (там
        surface_tensor/scale_vectors), и дрожь маршрута МОЛЧА жила на
        фолбэке, пока никто не смотрел журнал.
        """
        if not self._surface:
            return None, None
        want = self.route_vol_scale_min * 60.0
        best = best_d = None
        for sec, val in surface_all(self._surface):
            vol = float(val.get("vol_min_pct", 0.0))
            if vol <= 0:
                continue
            d = abs(sec - want)
            if best_d is None or d < best_d:
                best, best_d = (sec, vol), d
        if best is None:
            return None, None
        return best[1] / 100.0, best[0]

    def _heavy_anomaly(self, direction, entry, atr):
        """Ближайшая ТЯЖЁЛАЯ аномалия по стрелке Регрессии (Шаг 3 ТЗ).

        Возвращает цену плиты ликвидности (координату Области Профита) или
        None. Тяжёлой аномалией считаем зону Зон+ с настоящей массой:
        стоп-кластер (много стопов = много ликвидности) или FVG-разрыв
        (дыра, которую рынок возвращается заполнять).

        Строка — по направлению вектора Регрессии. Если оба уровня молчат,
        идём по самому полю маршрута: отсутствие стрелки не отменяет
        того, что прямо ahead стоит тяжёлая плита.
        """
        if atr <= 0 or not self._zones:
            return None
        # Стрелка: сумма знаковых оценок Регрессии по обоим уровням.
        # Старший весомее — он задаёт направление, младший уточняет.
        arrow = 0.0
        for tf, w in (("4h", 1.0), ("1m", 0.6)):
            rec = self._trend_by_tf.get(tf)
            if not rec:
                continue
            try:
                arrow += w * float((rec["fields"] or {}).get("score", 0.0))
            except (TypeError, ValueError):
                continue
        if arrow > 0.05:
            direction = "long"
        elif arrow < -0.05:
            direction = "short"
        best = None
        for z in self._zones:
            if not z.get("kinds"):
                continue          # не аномалия Зон+, а полка SR
            kinds = z.get("kinds") or ()
            if not ({"stop-cluster", "fvg-zone"} & set(kinds)):
                continue
            low, high = float(z["low"]), float(z["high"])
            # Плита должна лежать ВПЕРЕДИ по вектору Регрессии
            if direction == "long":
                if high <= entry:
                    continue
                dist = high - entry
            else:
                if low >= entry:
                    continue
                dist = entry - low
            # Тяжесть = масса зоны; берём ближайшую ТЯЖЁЛУЮ, а не ближайшую
            mass = float(z.get("strength", 0.0))
            if mass < self.anomaly_min_mass:
                continue
            if best is None or dist < best[0]:
                best = (dist, (low + high) / 2.0)
        return best[1] if best else None

    def _instruments_ready(self, now_ms):
        """ЗАТВОР ГОТОВНОСТИ ИЗМЕРИТЕЛЕЙ (утверждено 02.10).

        Сценарий НЕ пишется, пока ключевые измерители молчат:
          - вязкость стакана (`psy_viscosity`) — от Потока, карточка
            flow-state;
          - ускорение Регрессии (`reg_acc_1m`) — от Регрессии, карточка
            trend-vector уровня 1м.

        Почему затвор: без него сценарий штамповался раньше первого
        измерения, и ключевые поля уходили на Склад как `missing`.
        Снимок без плотности стакана и ускорения тренда — неполный вектор,
        а Тюнер учится именно по нему. Молчащий return: пока данных нет,
        решения нет.

        Проверяем ТОЛЬКО наличие живого измерения (не вес и не знак):
        нулевое значение — тоже измерение, None — отсутствие данных.

        Возврат: (готов, причина_неготовности) — причина для диагностики.
        """
        # --- вязкость стакана ---
        if self._viscosity is None:
            return False, "нет вязкости (flow-state ещё не измерил)"
        try:
            float(self._viscosity)
        except (TypeError, ValueError):
            return False, "вязкость не число"
        # --- ускорение Регрессии 1м ---
        rec = self._trend_by_tf.get("1m")
        if not rec:
            return False, "нет карточки Регрессии 1м"
        age = (now_ms - int(rec.get("ts", 0))) / 1000.0
        if age < 0 or age > REG_MAX_AGE_SEC:
            return False, f"карточка Регрессии 1м протухла ({age:.0f}с)"
        if (rec.get("fields") or {}).get("acceleration") is None:
            # acceleration бывает None у Регрессии (не хватило окон на
            # производную) — это «не измерено», затвор держит
            return False, "Регрессия 1м не измерила ускорение"
        return True, ""

    def _psy_state(self, now_ms, cond=None):
        """ПСИХОЗ СРЕДЫ: три измерителя из прямых источников.

        Возврат: {параметр: (значение|None, возраст_сек, доверие, свежесть)}.
        Сложность O(1) на измеритель: каждый берётся по прямому ключу из
        своей карточки, без переборов и без списков.

        viscosity — из Потока (flow-state, он же её единственный издатель);
        chaos     — из Энтропии (entropy-state, энтропия Шеннона по тикам);
        greed     — из Проводимости (CONDUCTIVITY: упрямство толпы ΔOI против
                     хода цены). Ничего не суммируется: это независимые
                     плотности, а не голоса.
        """
        out = {}
        # --- плотность стакана ---
        v_age, v = None, None
        try:
            v = float(self._viscosity) if self._viscosity is not None else None
            v_age = 0.0
        except (TypeError, ValueError):
            v = None
        out["viscosity"] = (v, v_age,
                            float(getattr(self, "_flow_trust", 0.5) or 0.5),
                            v is not None)
        # --- хаос Шеннона ---
        ent = self._entropy or {}
        try:
            chaos = float(ent.get("entropy"))
            age = (now_ms - int(ent.get("ts_ms", 0))) / 1000.0
            fresh = 0.0 <= age <= max(1.0, self.entropy_window)
        except (TypeError, ValueError):
            chaos, age, fresh = None, 0.0, False
        out["chaos"] = (chaos, age,
                        float(getattr(self, "_entropy_trust", 0.5) or 0.5),
                        fresh)
        # --- жадность: упрямство толпы ---
        greed, g_age, g_fresh = None, 0.0, False
        if cond:
            try:
                comps = cond.get("components") or {}
                greed = abs(float(comps.get("stubborn", 0.0)))
                g_age = (now_ms - int(cond.get("ts_ms", 0))) / 1000.0
                g_fresh = 0.0 <= g_age <= max(1.0, self.psy_stale_sec)
            except (TypeError, ValueError):
                greed = None
        out["greed"] = (greed, g_age, 0.5, g_fresh)
        return out

    def _ballistic_entry(self, direction, price, atr):
        """КООРДИНАТА ВХОДА по столкновению с полкой SR (Шаг 2 ТЗ).

        Вход перестаёт быть тиком: это цена, на которой ВЕКТОР ХОДА упирается
        в ближайшую полку SR, с поправкой на трение стакана.

        - psy_viscosity < 0.8 (бетон, стакан каменеет): лимитка на ТОЧНУЮ
          внешнюю границу полки — упругий чистый отскок, заходить рано;
        - psy_viscosity > 1.2 (стакан жидкий): через массу зоны закладываем
          инерционный занос шпильки и уходим ВГЛУБЬ диапазона на +0.05 ATR
          (вход по лучшей цене после ложного пробоя);
        - в промежутке — граница полки без смещения.

        Возврат (entry, zone_low, zone_high) либо (tick, None, None), если
        полки в стороне хода нет — тогда координаты нет и торговать нечем.
        """
        vis = self._viscosity
        # Поиск ближайшей встречной полки SR — единственный проход по зонам
        if direction == "long":
            below = [z for z in self._zones
                     if float(z["high"]) < price]
            if not below:
                return price, None, None
            zone = max(below, key=lambda z: float(z["high"]))
            edge = float(zone["high"])
        else:
            above = [z for z in self._zones
                     if float(z["low"]) > price]
            if not above:
                return price, None, None
            zone = min(above, key=lambda z: float(z["low"]))
            edge = float(zone["low"])
        if vis is None or atr <= 0:
            return edge, float(zone["low"]), float(zone["high"])
        if vis < 0.8:
            # БЕТОН: жёсткая лимитка на внешней границе — отскок упругий
            return edge, float(zone["low"]), float(zone["high"])
        if vis > 1.2:
            # ЖИДКИЙ СТАКАН: занос шпильки за ложный пробой — вход глубже
            # внутрь диапазона на 0.05 ATR
            deep = 0.05 * atr
            inside = (edge - deep if direction == "long" else edge + deep)
            return inside, float(zone["low"]), float(zone["high"])
        return edge, float(zone["low"]), float(zone["high"])

    def _leverage_psy(self, cloud_atr, lev_cap):
        """ПЛЕЧО через КОРИДОР ДИСПЕРСИИ (Шаг 4 ТЗ).

        Облако шума вокруг вектора маршрута имеет ширину `cloud_atr` (ATR).
        Плечо ОБРАТНО пропорционально ширине: широкое облако (пила, хаос) —
        плечо падает к 1х; узкое (лазерный луч в тишине) — безопасно
        растёт к потолку Риска. Согласованность сил в расчёте не участвует.
        """
        if cloud_atr <= 0:
            return 1.0
        # облако в 4 ATR и уже — «кто-то очень тихо, можно чуть больше»
        ratio = max(1.0, 4.0 / cloud_atr)
        return round(max(1.0, min(float(lev_cap), ratio)), 1)

    def _reg_layers(self, now_ms, max_age_sec=900.0):
        """Послойная РЕГРЕССИЯ: пять полей на КАЖДЫЙ уровень (утверждено).

        Возврат: [(метка, параметр, значение, age_sec, trust), …]
        Регрессия публикует velocity/acceleration/lifetime_min/decay_rate
        с самого начала — их просто никто не читал. Здесь они становятся
        самостоятельными измерителями: ускорение тренда и его затухание —
        разные сигналы, и сводить их к score было бы тем же усреднением,
        что мы вырезали из пирамиды линз.
        """
        out = []
        for tf in REG_TFS:
            rec = self._trend_by_tf.get(tf)
            if not rec:
                continue
            f = rec["fields"] or {}
            age = (now_ms - int(rec.get("ts", 0))) / 1000.0
            if age < 0 or age > max_age_sec:
                continue
            trust = rec.get("trust", 0.5)
            for param, raw in (("score", f.get("score")),
                               ("vel", f.get("velocity")),
                               ("acc", f.get("acceleration")),
                               ("decay", f.get("decay_rate")),
                               ("lifetime", f.get("lifetime_min"))):
                if raw is None:
                    continue           # не измерено — поля не существует
                try:
                    out.append((reg_field(param, tf), param,
                                float(raw), age, trust))
                except (TypeError, ValueError):
                    continue
        return out

    @staticmethod
    def _reg_signed(param, value):
        """Знаковая нормировка послойного поля Регрессии к шкале ±1."""
        try:
            v = float(value)
        except (TypeError, ValueError):
            return 0.0
        if param == "decay":
            # decay_rate 0..1: 0 — наклон у пика (сильнее), 1 — угас.
            # Положительный знак = тренд ещё живой.
            return max(-1.0, min(1.0, (1.0 - v) * 2.0 - 1.0))
        if param == "lifetime":
            # lifetime_min → +1 на 30+ минутах устойчивого знака
            return max(-1.0, min(1.0, v / REG_LIFETIME_NORM_MIN))
        return max(-1.0, min(1.0, v))       # score/vel/acc — уже знаковые

    def _lens_layers(self):
        """ТОТАЛЬНАЯ РАЗВЁРТКА ПИРАМИДЫ (утверждено): читаем ВСЕ масштабы
        тензора SURFACE и отдаём три независимых числа на КАЖДЫЙ слой.

        Раньше здесь стоял костыль `_lens_trend()`: он складывал trend_z всех
        масштабов и делил на их количество. Среднее по шкале — величина,
        которой в рынке не бывает: «2 минуты вверх, 512 минут вниз»
        (мощнейший признак разворота) усреднялся ровно в ноль. Многомерность
        пирамиды, ради которой она и строится с лог-шагом ×2, исчезала
        в одно число.

        Чтение ТЕНЗОРА (п.2 аудита 18.10): раньше искался мёртвый ключ
        `scales` (в публикации SURFACE его нет) — развёртка молча отдавала
        пустой список, и все послойные поля вектора были «missing».

        Возврат: [(метка, параметр, значение), …] — по три записи на слой,
        готовые к записи в вектор момента без усреднения. Метка масштаба
        дробная (0.5м → 'lens_trend_0.5m', см. _labels.lens_field).
        """
        if not self._surface:
            return []
        out = []
        for sec, val in surface_all(self._surface):
            mins = sec / 60.0
            for param, raw in (("trend", val.get("trend_z", 0.0)),
                               ("flow", val.get("flow_imb", 0.0)),
                               ("slope", val.get("slope_min_pct", 0.0))):
                try:
                    out.append((lens_field(param, mins), param,
                                float(raw or 0.0)))
                except (TypeError, ValueError):
                    continue
        return out

    def _forecast_targets(self, direction, entry):
        """ПРОГНОЗ ПУТИ (утверждено): цели из движения, когда магнита нет.
        Слои: канал тренда (граница движения), скорость созревания ×
        время, дрожь маршрута × √время — всё уже измерено системой."""
        long = direction == "long"
        out = []
        # ГРАНИЦЫ «КАНАЛА ±0.77%» (channel_low/high) УБРАНЫ 20.10 из
        # Регрессии: полоса рисовалась формулой от цены, а не измерением.
        # Настоящий канал — границы G-Channel (g-trend); здесь же появится
        # кривая прогноза из trend-forecast (Узел 6).
        # 1) СКОРОСТЬ СОЗРЕВАНИЯ × ВРЕМЯ МАРШРУТА
        if self.route_speed > 0 and self.route_wait_min > 0:
            move = self.route_speed * self.route_wait_min
            out.append(entry * (1.0 + move) if long
                       else entry * (1.0 - move))
        # 2) ДРОЖЬ МАРШРУТА × √ВРЕМЯ (движение, что рынок реально даёт)
        vol_route, _ = self._vol_route()
        if vol_route and vol_route > 0:
            move = vol_route * math.sqrt(self.route_wait_min)
            out.append(entry * (1.0 + move) if long
                       else entry * (1.0 - move))
        return out

    # ---------- расчёт ----------

    def _atr(self):
        rows = list(self._rows)
        h = np.array([r[2] for r in rows])
        l = np.array([r[3] for r in rows])
        c = np.array([r[4] for r in rows])
        prev_c = np.roll(c, 1)
        prev_c[0] = c[0]
        tr = np.maximum(h - l, np.maximum(np.abs(h - prev_c), np.abs(l - prev_c)))
        return float(np.mean(tr[-ATR_PERIOD:]))

    def _trend_loudness(self, p):
        """Громкость тренда из матрицы селективной громкости (шов с
        Проводимостью): шторм-пробой 1.8, шторм-вынос 0.1. Нет записи —
        legacy k_impulse (1.0..2.0). Нет среды — 1.0."""
        if p is None:
            return 1.0
        eff = p.get("efficiency") or {}
        if "trend-vector" in eff:
            try:
                return max(0.1, min(2.0, float(eff["trend-vector"])))
            except (TypeError, ValueError):
                return 1.0
        try:
            return max(1.0, min(2.0, float(p.get("k_impulse", 1.0))))
        except (TypeError, ValueError):
            return 1.0

    def _fuel_loudness(self, p):
        """Громкость топлива: legacy k_impulse (1.0..2.0); flow-state в
        матрицу не входит — у потока своей фазы нет."""
        if p is None:
            return 1.0
        try:
            return max(1.0, min(2.0, float(p.get("k_impulse", 1.0))))
        except (TypeError, ValueError):
            return 1.0

    # ---------- штамп сценария ----------

    def _tte_factor(self):
        """Множитель уверенности по TTE (утверждено): сжатие времени у
        макро-события. Сила давления = max_pressure × громкость × близость,
        масштабировано доверием календарю (вес Регистратора; 0.9 =
        нейтраль ×1.0). Возвращает (фактор, ближайшее событие)."""
        if not self._tte:
            return 1.0, None
        factor, event = 1.0, None
        for ev in self._tte.get("events") or []:
            try:
                m = float(ev.get("minutes_to_event", 1e9))
                w = float(ev.get("weight", 0.0))
            except (TypeError, ValueError):
                continue
            if 0 <= m < self.tte_compress_start:
                prox = 1.0 - m / self.tte_compress_start
                f = 1.0 - self.tte_max_pressure * w * prox
                if f < factor:
                    factor = f
                    event = {"event": ev.get("event"), "minutes_to_event": m}
        if factor < 1.0 and self._tte_weight is not None:
            factor = 1.0 - (1.0 - factor) * max(
                0.1, min(1.0, self._tte_weight / 0.9))
        return factor, event


    @staticmethod
    def _magnet_alive(magnet, zones, tol):
        """Магнит жив: какая-то область поля накрывает его уровень
        (с запасом на дрожь перерисовки)."""
        for z in zones:
            lo, hi = float(z.get("low", 0)), float(z.get("high", 0))
            if hi < lo:
                lo, hi = hi, lo
            if lo - tol <= magnet <= hi + tol:
                return True
        return False

    async def _maybe_stamp(self):
        # ДИАГНОСТИКА БЛОКИРОВОК (02.10): каждый ранний выход помечен
        # [BLOCK_N] — по журналу видно, где именно Сценарист остановился.
        if not self._picture:
            if self._picture_lookups > 0:    # картина ещё не доехала — добор
                self._picture_lookups -= 1
                await self._lookup_picture()
            if not self._picture:
                self.log.info("[BLOCK_0] Выход: картина Поля не доехала")
                return

        f = self._picture["fields"]
        now_ms = int(time.time() * 1000)
        if int(f.get("valid_until_ms", 0)) < now_ms:
            self.log.info("[BLOCK_1] Выход: картина протухла "
                          "(valid_until %d < сейчас %d)",
                          int(f.get("valid_until_ms", 0)), now_ms)
            return                           # картина протухла — сценария нет
        zones = f.get("zones") or []

        # РУЧКИ ВЕСОВ — из живого конфига каждый расчёт (утверждено):
        # Тюнер ведёт веса данных по полезности — Сценарист должен
        # слышать правку сразу, а не после перезапуска системы
        c = self.kernel.cfg
        self.magnet_w = c.scenario_magnet_weight
        self.news_w = c.scenario_news_weight
        self.fuel_w = c.scenario_fuel_weight
        self.trend_w = c.scenario_trend_weight
        self.tape_speed_w = c.scenario_tape_speed_weight
        self.magnets_dist_k = c.scenario_magnets_dist_k
        self.leader_w = c.leader_weight
        self.rsi_w = c.rsi_weight
        self.oi_w = c.scenario_oi_weight
        self.meta_weight = c.meta_weight
        self.fractal_bias_w = c.fractal_bias_weight
        self.intuit_w = c.intuit_weight
        self.spring_w = c.spring_weight
        # фильтр хода — из живого конфига (ручка net_margin)
        self.net_margin = c.scenario_net_margin
        self.min_net_move = max(self.min_net_move_pct / 100.0,
                                self.round_cost * self.net_margin)
        # сопровождение — из живого конфига (ручки ведёт Тюнер)
        self.route_trail_act_frac = c.scenario_route_trail_activate_frac
        self.route_trail_dist_frac = c.scenario_route_trail_distance_frac
        # стоп-каркас — тоже из живого конфига (ручки калибровки)
        self.stop_atr_floor = c.scenario_stop_atr_floor
        self.route_stop_noise_mult = c.scenario_route_stop_noise_mult
        # Каждый слой в vector идёт со СВОИМ весом из справочника
        # WEIGHT_KNOBS (ниже, в цикле по _lens_layers()). Здесь ничего не
        # усредняем — это и есть тотальная развёртка пирамиды.

        # ОТЗЫВ СЛОМАННОЙ ИДЕИ (решение хозяина): сценарий живёт, пока его
        # мишень стоит в картине. Магнит исчез — прогноз отозван автором.
        if self._published:
            tol = self._atr() * 0.5
            for key in list(self._published):
                ps = self._published[key]
                if self._magnet_alive(ps["magnet"], zones, tol):
                    continue
                del self._published[key]
                self.send("dealer", {"command": "cancel-pending",
                                     "direction": ps["direction"],
                                     "magnet": ps["magnet"]})
                self.log.info("Отзыв сценария (идея сломана): %s, магнит "
                              "%.5f исчез из картины",
                              ps["direction"], ps["magnet"])
        if not zones or self.price <= 0 or len(self._rows) < MIN_CANDLES:
            self.log.info("[BLOCK_2] Выход: зон=%d цена=%.5f свечей=%d "
                          "(нужно свечей ≥%d)",
                          len(zones), self.price, len(self._rows),
                          MIN_CANDLES)
            return

        # --- ЗАТВОР ГОТОВНОСТИ ИЗМЕРИТЕЛЕЙ (утверждено 02.10) ---
        # Стоит ПЕРЕД любым расчётом геометрии входа и траекторий: пока
        # ключевые измерители молчат, сценарий не пишется вообще. Раньше
        # штамп уходил раньше первого измерения, и psy_viscosity /
        # reg_acc_1m ложились на Склад как missing.
        ready, why = self._instruments_ready(now_ms)
        if not ready:
            # Счётчик ждет первого измерения — видно в журнале, чтобы затвор
            # не выглядел «робот встал».
            if self._gate_since == 0:
                self._gate_since = now_ms
            self._gate_waits += 1
            waited = (now_ms - self._gate_since) / 1000.0
            if self._gate_waits == 1 or self._gate_waits % 10 == 0:
                self.log.info("Затвор измерителей ждёт: %s "
                              "(ожиданий %d)", why, self._gate_waits)
            if waited < GATE_WAIT_MAX_SEC:
                return                  # молчащий затвор: данных нет — решения нет
            # ПРЕДОХРАНИТЕЛЬ: ждём слишком долго — пишем с честной пометкой
            # «измеритель молчит», чтобы затвор не блокировал систему вечно.
            self._gate_stale = True
            self.log.warning("Затвор ПРОПУЩЕН после %.0f с: %s. Сценарий "
                             "пишется с пометкой stale по этой оси",
                             waited, why)
        else:
            self._gate_stale = False
        # Прохождение затвора видно в журнале (раз в 30 ожиданий) —
        # иначе переход «молчал → заработал» и последующий тихий выход
        # невозможно разделить.
        if self._gate_waits and (self._gate_waits % 30 == 0 or ready):
            self.log.info("Затвор измерителей: ПРОЙДЕН (ожиданий %d) — "
                          "вязкость %.3f, ускорение 1м %s",
                          self._gate_waits, self._viscosity,
                          ((self._trend_by_tf.get("1m") or {})
                           .get("fields") or {}).get("acceleration"))
        if self._gate_waits and ready:
            self._gate_waits = 0
            self._gate_since = 0

        # --- направление: магниты + настроение (+ тренд/топливо ниже) ---
        # МАГНИТЫ ПО РАССТОЯНИЮ (аудит 27.09): голос зоны гаснет с
        # удалением (в ATR). Раньше дальние зоны давали тот же голос,
        # что ближние: при падении нижние выгорают/истекают — и баланс
        # механически уходил вверх (лонги против падения, окно 22:27).
        atr_dir = self._atr()
        pull = 0.0
        wsum = 0.0
        for z in zones:
            mid = (float(z["low"]) + float(z["high"])) / 2.0
            s = float(z.get("strength", 0.0))
            dist = abs(mid - self.price) / (atr_dir if atr_dir > 0
                                            else self.price)
            w = s / (1.0 + dist / self.magnets_dist_k)
            if mid > self.price:
                pull += w                    # магнит выше — тянет вверх
            elif mid < self.price:
                pull -= w                    # ниже — вниз
            wsum += w
        pull_norm = pull / wsum if wsum else 0.0
        mood = float(f.get("mood", {}).get("score", 0.0))
        # Вектор топлива (утверждено) + шов с Проводимостью: k_impulse
        # усиливает импульсное слагаемое в шторм (1.0..2.0). Границы
        # нерушимы: издержки/стоп/риск-менеджер коэффициент не отменяет.
        fuel = f.get("fuel") or {}
        trend = f.get("trend") or {}
        env_p = self.get_last(CONDUCTIVITY)
        env_payload = env_p.payload if env_p is not None else None
        trend_mult = self._trend_loudness(env_payload)   # матрица громкости
        fuel_mult = self._fuel_loudness(env_payload)     # legacy k_impulse
        has_fuel = float(fuel.get("voices", 0.0)) > 0
        has_trend = float(trend.get("voices", 0.0)) > 0
        trend_score = float(trend.get("score", 0.0)) if has_trend else 0.0
        # ВЕКТОР ФАКТОВ (утверждено): каждый факт со своим знаком и силой —
        # Лаборатория по исходу сделки оценит, кто был прав
        facts = {"magnets": round(pull_norm, 3), "news": round(mood, 3)}
        # ВЕКТОР МОМЕНТА (утверждено): слепок КАК ЕСТЬ — все виды данных
        # со знаком, силой, свежестью и состоянием. Формулу не меняет.
        vec = {}
        pic_age = (now_ms - int(self._picture.get("created_ms", 0))) / 1000.0
        pic_trust = self._picture.get("trust")
        pic_conf = self._picture.get("confidence")

        def _n(name, **kwargs):
            vec[name] = _vec_entry(**kwargs)

        _n("magnets", kind="picture", value=pull_norm,
           weight=self.magnet_w, push=self.magnet_w * pull_norm,
           age_sec=pic_age, state="fresh", conf=pic_conf, trust=pic_trust)
        _n("news", kind="news-sentiment", value=mood,
           weight=self.news_w, push=self.news_w * mood,
           age_sec=pic_age, state="fresh", conf=pic_conf, trust=pic_trust,
           extra={"src": "picture.mood"})
        if has_trend:
            # Живой тренд (утверждено): тренд усилен громкостью матрицы
            # (шов со Средой); веса — ручки конфига (аудит 27.09)
            trend_term = min(0.4, self.trend_w * trend_score * trend_mult)
            bias = (self.magnet_w * pull_norm + trend_term
                    + self.fuel_w * float(fuel.get("score", 0.0))
                    + self.news_w * mood)
            facts["trend"] = round(trend_score, 3)
            facts["fuel"] = round(float(fuel.get("score", 0.0)), 3)
            f_score = float(fuel.get("score", 0.0))
            _n("trend", kind="trend-vector", value=trend_score,
               weight=self.trend_w, push=trend_term, age_sec=pic_age,
               state="fresh", trust=pic_trust,
               extra={"mult": round(trend_mult, 3),
                      "voices": float(trend.get("voices", 0.0))})
            _n("fuel", kind="flow-state", value=f_score,
               weight=self.fuel_w,
               push=self.fuel_w * f_score, age_sec=pic_age,
               state="fresh",
               extra={"mult": round(fuel_mult, 3),
                      "voices": float(fuel.get("voices", 0.0))})
        elif has_fuel:
            fuel_term = min(0.4, self.fuel_w
                            * float(fuel.get("score", 0.0)) * fuel_mult)
            bias = (self.magnet_w * pull_norm + self.news_w * mood
                    + fuel_term)
            facts["fuel"] = round(float(fuel.get("score", 0.0)), 3)
            _n("fuel", kind="flow-state", value=float(fuel.get("score", 0.0)),
               weight=self.fuel_w, push=fuel_term, age_sec=pic_age,
               state="fresh",
               extra={"mult": round(fuel_mult, 3),
                      "voices": float(fuel.get("voices", 0.0))})
        else:
            bias = self.magnet_w * pull_norm + self.news_w * mood
        # Вожак (Поводырь, утверждено): вожак с нами — плюс к направлению,
        # против — минус. Свежий факт; устаревший не учитывается.
        leader_f = None
        if self._leader and (now_ms - int(self._leader.get("ts_ms", 0))
                             < self.leader_stale * 1000):
            leader_f = self._leader
            try:
                btc_dir = max(-1.0, min(
                    1.0, float(leader_f.get("angle_btc_pct", 0.0)) / 0.10))
                bias += self.leader_w * btc_dir
                facts["leader"] = round(btc_dir, 3)
                _n("leader", kind="leader-correlation", value=btc_dir,
                   weight=self.leader_w, push=self.leader_w * btc_dir,
                   age_sec=(now_ms - int(leader_f.get("ts_ms", 0))) / 1000.0,
                   state="fresh",
                   trust=float(getattr(self, "_leader_trust", 0.5) or 0.5),
                   extra={"angle_btc_pct": float(
                       leader_f.get("angle_btc_pct", 0.0) or 0.0)})
            except (TypeError, ValueError):
                leader_f = None
        elif self._leader:
            # Карточка есть, но устарела — в вектор КАК ЕСТЬ (push 0)
            age = (now_ms - int(self._leader.get("ts_ms", 0))) / 1000.0
            raw = float(self._leader.get("angle_btc_pct", 0.0) or 0.0) / 0.10
            _n("leader", kind="leader-correlation",
               value=max(-1.0, min(1.0, raw)), weight=self.leader_w, push=0.0,
               age_sec=age, state="stale",
               trust=float(getattr(self, "_leader_trust", 0.5) or 0.5))
        # RSI (утверждено): исчерпание хода — перекупленность против
        # продолжения, перепроданность — за разворот
        if (self._rsi_state
                and now_ms - int(self._rsi_state.get("ts_ms", 0))
                < self.rsi_stale * 1000):
            rsi_p = float(self._rsi_state.get("pressure", 0.0))
            bias += self.rsi_w * rsi_p
            facts["rsi"] = round(rsi_p, 3)
            _n("rsi", kind="rsi-state", value=rsi_p, weight=self.rsi_w,
               push=self.rsi_w * rsi_p,
               age_sec=(now_ms - int(self._rsi_state.get("ts_ms", 0))) / 1000.0,
               state="fresh",
               trust=float(getattr(self, "_rsi_trust", 0.5) or 0.5))
        elif self._rsi_state:
            _n("rsi", kind="rsi-state",
               value=float(self._rsi_state.get("pressure", 0.0) or 0.0),
               weight=self.rsi_w, push=0.0,
               age_sec=(now_ms - int(self._rsi_state.get("ts_ms", 0))) / 1000.0,
               state="stale",
               trust=float(getattr(self, "_rsi_trust", 0.5) or 0.5))
        # Пружина (Этап 32): цена улетела от VWAP-якоря, OI гаснет —
        # толпа без бензина, возврат к руслу. Свежий факт.
        if (self._spring
                and now_ms - int(self._spring.get("ts_ms", 0)) < self.spring_fresh_ms):
            try:
                sp = float(self._spring.get("force", 0.0))
                if sp:
                    bias += self.spring_w * sp
                    facts["spring"] = round(sp, 3)
                _n("spring", kind="spring-state", value=sp,
                   weight=self.spring_w, push=self.spring_w * sp,
                   age_sec=(now_ms
                            - int(self._spring.get("ts_ms", 0))) / 1000.0,
                   state="fresh",
                   trust=float(getattr(self, "_spring_trust", 0.5) or 0.5))
            except (TypeError, ValueError):
                pass
        elif self._spring:
            _n("spring", kind="spring-state",
               value=float(self._spring.get("force", 0.0) or 0.0),
               weight=self.spring_w, push=0.0,
               age_sec=(now_ms - int(self._spring.get("ts_ms", 0))) / 1000.0,
               state="stale",
               trust=float(getattr(self, "_spring_trust", 0.5) or 0.5))
        # Прямой OI-фактор (утверждено): ΔOI по ходу — топливо настоящего
        # набора, ΔOI против хода — топливо кончается. Плюс скорость ленты
        # (для слепка Интуита, утверждено) — тот же конверт Проводимости
        if env_payload is not None:
            env_age = (now_ms - int(env_payload.get("ts_ms", 0))) / 1000.0
            try:
                oi_t = float((env_payload.get("conditions") or {})
                             .get("oi_trend", 0.0))
                bias += self.oi_w * max(-1.0, min(1.0, oi_t))
                facts["oi"] = round(max(-1.0, min(1.0, oi_t)), 3)
                _n("oi", kind="level-verdict",
                   value=max(-1.0, min(1.0, oi_t)), weight=self.oi_w,
                   push=self.oi_w * max(-1.0, min(1.0, oi_t)),
                   age_sec=env_age, state="fresh")
            except (TypeError, ValueError):
                pass
            try:
                # СКОРОСТЬ ЛЕНТЫ (аудит 27.09): факт + вклад по ручке.
                # На горизонте маршрута (1–2 ч) данные говорят «знак
                # обратный» (44% прямой точности) — старт −0.05, крутится
                # в конфиге; в моменте (5 мин) лента — сигнал входа.
                ts = max(-1.0, min(1.0, float(
                    (env_payload.get("conditions") or {})
                    .get("speed_bias", 0.0))))
                facts["tape_speed"] = round(ts, 3)
                bias += self.tape_speed_w * ts
                _n("tape_speed", kind="conductivity", value=ts,
                   weight=self.tape_speed_w, push=self.tape_speed_w * ts,
                   age_sec=env_age, state="fresh",
                   extra={"phase": env_payload.get("phase"),
                          "r": env_payload.get("r")})
            except (TypeError, ValueError):
                pass
        # ТОТАЛЬНАЯ РАЗВЁРТКА ПИРАМИДЫ (утверждено): вместо одного плоского
        # `lens_trend` в вектор момента идут 3×N послойных полей — по
        # одному на каждый масштаб карточки SURFACE. Ничего не усредняется.
        surf_age = None
        if self._surface is not None:
            surf_age = (now_ms
                        - int(self._surface.get("ts_ms", 0))) / 1000.0
        lens_state = "fresh" if self._surface is not None else "missing"
        push_terms = []
        for label, param, value in self._lens_layers():
            # Нормируем к знаковой шкале, но КАЖДЫЙ слой остаётся своим
            # полем: trend_z (±3) → /3, flow_imb (−1..1) как есть,
            # slope_min_pct — % в минуту, прижимаем к ±1.
            if param == "trend":
                signed = max(-1.0, min(1.0, value / 3.0))
            elif param == "flow":
                signed = max(-1.0, min(1.0, value))
            else:
                signed = max(-1.0, min(1.0, value / 0.05))
            facts[label] = round(signed, 4)
            # Вектор момента: собственная ручка КАЖДОГО поля (из
            # WEIGHT_KNOBS, её калибрует Тюнер) — вклад в общую формулу
            # направления при этом суммируется по всем слоям.
            spec = WEIGHT_KNOBS.get(label)
            w = c.get_float(spec[0], spec[1], spec[2]) if spec else 0.0
            push_terms.append(w * signed)
            _n(label, kind=f"lens-{param}", value=value, weight=w,
               push=w * signed, age_sec=surf_age, state=lens_state,
               extra={"mins": float(label.rsplit("_", 1)[-1].rstrip("m"))})
        if push_terms:
            # Плоского `lens_trend` больше нет: вклад пирамиды в bias —
            # сумма собственных вкладов всех слоёв (без усреднения по шкале).
            bias += sum(push_terms)

        # ПОСЛОЙНАЯ РЕГРЕССИЯ (утверждено): 5 полей × 2 уровня (1м/4ч).
        # Ускорение (acc) и затухание (decay) Регрессия считала всегда —
        # в векторе момента их не было, и Тюнер не мог на них учиться.
        for label, param, raw, age, trust in self._reg_layers(now_ms):
            signed = self._reg_signed(param, raw)
            facts[label] = round(signed, 4)
            spec = WEIGHT_KNOBS.get(label)
            w = c.get_float(spec[0], spec[1], spec[2]) if spec else 0.0
            bias += w * signed
            _n(label, kind=f"reg-{param}", value=raw, weight=w,
               push=w * signed, age_sec=age, state="fresh", trust=trust,
               extra={"tf": label.rsplit("_", 1)[-1]})

        # ДВА RSI — ДВА САМОСТОТАЯТЕЛЬНЫХ ИЗМЕРИТЕЛЯ (утверждено).
        # Раньше в вектор шёл один `pressure`, посчитанный по 1м: стратегический
        # RSI 64м не читался никем. Теперь обе нити — отдельные поля.
        if (self._rsi_state
                and now_ms - int(self._rsi_state.get("ts_ms", 0))
                <= self.rsi_stale * 1000.0):
            rsi_age = (now_ms - int(self._rsi_state.get("ts_ms", 0))) / 1000.0
            for key, tf in (("rsi_base", "1m"), ("rsi_strat", "4h")):
                try:
                    raw = float(self._rsi_state.get(key))
                except (TypeError, ValueError):
                    continue           # стратегической нити может не быть
                # Нормировка тем же кодом, что у датчика: перекупленность
                # жмёт вниз, перепроданность вверх; вне зон — ноль.
                if raw >= self.rsi_upper:
                    signed = max(-1.0, min(1.0,
                                            (self.rsi_upper - raw) / 30.0 - 1.0))
                elif raw <= self.rsi_lower:
                    signed = max(-1.0, min(1.0,
                                            (self.rsi_lower - raw) / 30.0 + 1.0))
                else:
                    signed = max(-1.0, min(1.0, (50.0 - raw) / 30.0))
                facts[key] = round(signed, 4)
                spec = WEIGHT_KNOBS.get(key)
                w = c.get_float(spec[0], spec[1], spec[2]) if spec else 0.0
                bias += w * signed
                _n(key, kind=f"rsi-{tf}", value=raw, weight=w,
                   push=w * signed, age_sec=rsi_age, state="fresh",
                   trust=float(getattr(self, "_rsi_trust", 0.5) or 0.5),
                   extra={"tf": tf})

        # ПСИХОЗ СРЕДЫ (утверждено, Шаг 1 ТЗ): три измерителя плотности,
        # хаоса и жадности. Всё адресно — прямые ключи, без переборов.
        # Это ФАКТЫ среды, а не голоса за направление: их роль — в координатах
        # входа и ширине облака шума (см. _ballistic_entry / _leverage_psy).
        psy = self._psy_state(now_ms, env_payload)
        for param in ("viscosity", "chaos", "greed"):
            raw, age, trust, fresh = psy[param]
            if raw is None:
                # среда ещё не измерена — поля нет (ноль означал бы «измерено:
                # вязкости нет», а это разные вещи)
                continue
            signed = max(-1.0, min(1.0, raw))
            key = psy_field(param)
            facts[key] = round(signed, 4)
            spec = WEIGHT_KNOBS.get(key)
            w = c.get_float(spec[0], spec[1], spec[2]) if spec else 0.0
            bias += w * signed
            _n(key, kind=f"psy-{param}", value=raw, weight=w, push=w * signed,
               age_sec=age, state="fresh" if fresh else "stale", trust=trust)
        # Пакет альтов (Метакоррелятор, утверждено): пакет дрейфует с нами
        # — микро-плюс к направлению; свежий факт, вес по доверию
        meta_f = None
        if (self._meta and (now_ms - int(self._meta.get("ts_ms", 0))
                            < self.meta_stale * 1000)):
            meta_f = self._meta
            try:
                pkg = max(-1.0, min(1.0, float(meta_f.get("package_bias", 0.0))))
                bias += self.meta_weight * pkg
                facts["package"] = round(pkg, 3)
                _n("package", kind="meta-corr", value=pkg,
                   weight=self.meta_weight, push=self.meta_weight * pkg,
                   age_sec=(now_ms
                            - int(meta_f.get("ts_ms", 0))) / 1000.0,
                   state="fresh",
                   trust=float(getattr(self, "_meta_trust", 0.5) or 0.5))
            except (TypeError, ValueError):
                meta_f = None
        elif self._meta:
            _n("package", kind="meta-corr",
               value=max(-1.0, min(1.0, float(
                   self._meta.get("package_bias", 0.0) or 0.0))),
               weight=self.meta_weight, push=0.0,
               age_sec=(now_ms - int(self._meta.get("ts_ms", 0))) / 1000.0,
               state="stale",
               trust=float(getattr(self, "_meta_trust", 0.5) or 0.5))
        # Интуит (утверждено): аналогии — факт, вклад по весу карточки
        # (стартовое доверие 0.2 → вес ~0.36 → вклад тихий, зарабатывает)
        if (self._intuit
                and now_ms - int(self._intuit.get("ts_ms", 0)) < self.intuit_fresh_ms):
            try:
                hb = max(-1.0, min(1.0, float(self._intuit.get("bias", 0.0))))
                scale = max(0.1, min(1.0, self._intuit_weight / 0.9))
                bias += self.intuit_w * hb * scale
                facts["intuit"] = round(hb, 3)
                _n("intuit", kind="intuit-hint", value=hb, weight=self.intuit_w,
                   push=self.intuit_w * hb * scale,
                   age_sec=(now_ms
                            - int(self._intuit.get("ts_ms", 0))) / 1000.0,
                   state="fresh",
                   trust=float(getattr(self, "_intuit_trust", 0.2) or 0.2),
                   extra={"scale": round(scale, 4),
                          "card_weight": round(
                              float(getattr(self, "_intuit_weight", 0.36)
                                    or 0.36), 4)})
            except (TypeError, ValueError):
                pass
        elif self._intuit:
            _n("intuit", kind="intuit-hint",
               value=max(-1.0, min(1.0, float(
                   self._intuit.get("bias", 0.0) or 0.0))),
               weight=self.intuit_w, push=0.0,
               age_sec=(now_ms - int(self._intuit.get("ts_ms", 0))) / 1000.0,
               state="stale",
               trust=float(getattr(self, "_intuit_trust", 0.2) or 0.2))
        # Фракталы (утверждено): инерция старших слоёв — факт направления
        topo_f = None
        if (self._topo and (now_ms - int(self._topo.get("ts_ms", 0))
                            < self.fractal_fresh_ms)):
            topo_f = self._topo
            topo_age = (now_ms - int(topo_f.get("ts_ms", 0))) / 1000.0
            try:
                fb = float(topo_f.get("fractal_bias", 0.0) or 0.0)
                if fb is not None and fb != 0.0:
                    bias += self.fractal_bias_w * fb
                    facts["fractal"] = round(fb, 3)
                _n("fractal", kind="topological-resonance", value=fb,
                   weight=self.fractal_bias_w,
                   push=self.fractal_bias_w * fb if fb else 0.0,
                   age_sec=topo_age, state="fresh",
                   trust=float(getattr(self, "_topo_trust", 0.5) or 0.5))
            except (TypeError, ValueError):
                topo_f = None
            # Фракталы 2.0 (Этап 32): полное самоподобие — младший слой
            # повторяет форму старшего, ход старшего продолжается в младшем
            try:
                sc = float(topo_f.get("selfsim_corr", 0.0) or 0.0)
                sdir = float(topo_f.get("selfsim_dir", 0.0) or 0.0)
                value = (sc * (1.0 if sdir > 0 else -1.0)
                         if sdir != 0.0 else 0.0)
                gate = abs(sc) >= self.selfsim_min and sdir != 0.0
                if gate:
                    bias += self.selfsim_w * value
                    facts["selfsim"] = round(value, 3)
                _n("selfsim", kind="topological-resonance", value=value,
                   weight=self.selfsim_w,
                   push=self.selfsim_w * value if gate else 0.0,
                   age_sec=topo_age, state="fresh",
                   extra={"corr": round(sc, 4), "dir": sdir,
                          "gate": self.selfsim_min})
            except (TypeError, ValueError):
                pass
        bias = max(-1.0, min(1.0, bias))
        if abs(bias) < self.bias_min:
            self.skipped += 1
            self.log.info("[BLOCK_3] Выход: Пропуск: направления нет (bias %+.2f < %.2f; "
                          "магниты %+.2f, RSI %s, OI %s)",
                          bias, self.bias_min, pull_norm,
                          ((self._rsi_state or {}).get("rsi_base", "—")),
                          ((env_payload.get("conditions") or {})
                           .get("oi_trend", "—") if env_payload else "—"))
            return                           # мусорных сценариев не бывает
        direction = "long" if bias > 0 else "short"

        # Мы одни против пакета (Метакоррелятор, утверждено): сильное
        # расхождение — уверенность режется (наш ход против общего дрейфа
        # чаще бывает догоняющим, чем уникальным; проверку делает Лаба)
        if (meta_f is not None
                and float(meta_f.get("divergence", 0.0)) >= 0.5):
            scenario_conf_mult = 1.0 - self.meta_diverge_penalty
            self.log.info("Мы против пакета (расхождение %.2f, пакет %+.2f)"
                          " — уверенность −%.0f%%",
                          float(meta_f.get("divergence", 0.0)),
                          float(meta_f.get("package_bias", 0.0)),
                          self.meta_diverge_penalty * 100)
        else:
            scenario_conf_mult = 1.0
        if meta_f is not None:
            # величина расхождения — в факты (слепок Интуита, утверждено)
            facts["divergence"] = round(
                float(meta_f.get("divergence", 0.0)), 3)
            _n("divergence", kind="meta-corr",
               value=float(meta_f.get("divergence", 0.0) or 0.0),
               weight=self.meta_weight, push=0.0,
               age_sec=(now_ms - int(meta_f.get("ts_ms", 0))) / 1000.0,
               state="fresh", extra={"directionless": True})
        # Херст (утверждено): пила (H ≈ 0.5) — пробои ловушки, штраф
        if topo_f is not None:
            try:
                h = float(topo_f.get("hurst_exponent") or 0.0)
                if h > 0 and abs(h - 0.5) <= self.kernel.cfg.fractal_hurst_band:
                    scenario_conf_mult *= (1.0 - self.fractal_hurst_penalty)
                    facts["hurst"] = round(h, 3)
                _n("hurst", kind="topological-resonance", value=h,
                   weight=0.0, push=0.0,
                   age_sec=(now_ms - int(topo_f.get("ts_ms", 0))) / 1000.0,
                   state="fresh", extra={"directionless": True})
            except (TypeError, ValueError) as exc:
                # НЕ ГЛОТАЕМ: ошибка разбора факта — в журнал с трассировкой
                self.log.error("Сценарист: разбор поля hurst не удался: %s: %s",
                               type(exc).__name__, exc, exc_info=True)
            # ПОСЛОЙНОЙ АСИММЕТРИЧНЫЙ ХЕРСТ + САМОПОДОБИЕ (27.10, ТЗ).
            # 84 независимых поля (7 × 12 волокон): H_Up/H_Down/ H_сим / Fractal Momentum
            # и три оси самоподобия (corr/dir/scale) по каждому из 12
            # волокон биржевой палитры. Стартовый вес 0.0 — данные идут в вектор «как есть»,
            # в bias не вмешиваются; раскачает их Тюнер по факту исхода.
            # Штраф за пилу выше остался на плоском среднем (обратная
            # совместимость), здесь — только измерения.
            for _tf in FRACT_TFS:
                for _p in FRACT_PARAMS:
                    _key = f"fractal_{_p}_{_tf}"
                    _raw = topo_f.get(_key)
                    try:
                        _val = float(_raw) if _raw is not None else None
                    except (TypeError, ValueError):
                        _val = None
                    _n(_key, kind="topological-resonance", value=_val,
                       weight=0.0, push=0.0,
                       age_sec=(now_ms - int(topo_f.get("ts_ms", 0))) / 1000.0,
                       state=("fresh" if _val is not None else "missing"),
                       extra={"tf": _tf, "axis": _p})
                for _hkey in (f"hurst_up_{_tf}", f"hurst_down_{_tf}",
                              f"hurst_{_tf}", f"fractal_momentum_{_tf}",
                              f"trend_slope_{_tf}"):
                    _raw = topo_f.get(_hkey)
                    try:
                        _val = float(_raw) if _raw is not None else None
                    except (TypeError, ValueError):
                        _val = None
                    _n(_hkey, kind="topological-resonance", value=_val,
                       weight=0.0, push=0.0,
                       age_sec=(now_ms - int(topo_f.get("ts_ms", 0))) / 1000.0,
                       state=("fresh" if _val is not None else "missing"),
                       extra={"tf": _tf, "layer": True})
        # ПАНОРАМНЫЙ РАДАР БЕЗ СЖАТИЯ (27.10, ТЗ). 5 метрик связности
        # + 2 производные лага на каждую монету пула — все в вектор,
        # стартовый вес 0.0. Сплющивать в один package_bias больше нельзя:
        # так терялись лаг, DCor и весь лаг-радар.
        for _sym in (s.strip().upper()
                     for s in self.kernel.cfg.meta_pool.split(",")
                     if s.strip()):
            for _m in META_METRICS:
                _key = f"meta_{_m}_{_sym}"
                _raw = meta_f.get(_key) if meta_f is not None else None
                try:
                    _val = float(_raw) if _raw is not None else None
                except (TypeError, ValueError) as exc:
                    self.log.error("Сценарист: ось %s не разобрана: %s: %s",
                                   _key, type(exc).__name__, exc,
                                   exc_info=True)
                    _val = None
                _n(_key, kind="meta-corr", value=_val, weight=0.0, push=0.0,
                   age_sec=(now_ms - int(meta_f.get("ts_ms", 0))) / 1000.0
                   if meta_f is not None else None,
                   state=("fresh" if _val is not None else "missing"),
                   extra={"sym": _sym, "axis": _m, "radar": True})
            for _d in META_DERIVS:
                _key = f"meta_{_d}_{_sym}"
                _raw = meta_f.get(_key) if meta_f is not None else None
                try:
                    _val = float(_raw) if _raw is not None else None
                except (TypeError, ValueError) as exc:
                    self.log.error("Сценарист: производная %s не разобрана: "
                                   "%s: %s", _key, type(exc).__name__, exc,
                                   exc_info=True)
                    _val = None
                _n(_key, kind="meta-corr", value=_val, weight=0.0, push=0.0,
                   age_sec=(now_ms - int(meta_f.get("ts_ms", 0))) / 1000.0
                   if meta_f is not None else None,
                   state=("fresh" if _val is not None else "missing"),
                   extra={"sym": _sym, "axis": _d, "radar": True})
        # Энтропия тиков (Волна 5, утверждено): хаос потока давит
        # уверенность (ложные пробои); сила — по доверию календарю-стилю
        if self._entropy:
            try:
                s = max(0.0, min(1.0, float(
                    self._entropy.get("entropy", 0.0))))
                if s > self.entropy_threshold:
                    over = (s - self.entropy_threshold) \
                        / (1.0 - self.entropy_threshold)
                    trust_mult = max(0.1, min(1.0,
                        (self._entropy_weight or 0.9) / 0.9))
                    scenario_conf_mult *= (
                        1.0 - self.entropy_penalty * over * trust_mult)
                    facts["entropy"] = round(s, 3)
                _n("entropy", kind="entropy-state", value=s,
                   weight=0.0, push=0.0,
                   age_sec=(now_ms
                            - int(self._entropy.get("ts_ms", 0))) / 1000.0,
                   state="fresh",
                   trust=float(getattr(self, "_entropy_trust", 0.5) or 0.5),
                   extra={"directionless": True,
                          "threshold": self.entropy_threshold})
            except (TypeError, ValueError):
                pass
        # Резонансный узел рядом (утверждено): точка излома среды —
        # ФАКТ поля для разбора; сроком сценария не командует
        node_ttl_factor = 1.0
        if topo_f is not None:
            nearest = topo_f.get("nearest_node_pct")
            try:
                node_ttl_factor = (self.fractal_node_ttl
                                   if (nearest is not None
                                       and float(nearest)
                                       <= self.fractal_node_prox)
                                   else 1.0)
                if nearest is not None \
                        and float(nearest) <= self.fractal_node_prox:
                    node_ttl_factor = self.fractal_node_ttl
                    facts["resonance_near"] = round(float(nearest), 3)
                _n("resonance_near", kind="topological-resonance",
                   value=(float(nearest) if nearest is not None else None),
                   weight=0.0, push=0.0,
                   age_sec=(now_ms - int(topo_f.get("ts_ms", 0))) / 1000.0,
                   state="fresh",
                   extra={"directionless": True,
                          "prox": self.fractal_node_prox,
                          "ttl_factor": round(node_ttl_factor, 3)})
            except (TypeError, ValueError):
                pass

        # --- ВЕТО среды (утверждено, «умножение на ноль»): шторм-вынос
        #     (позиции выносит) + сценарий ПО каскаду = ловушка продолжения.
        #     Сценарий ПРОТИВ каскада (разворот от выгоревшей полки) — можно.
        if self.storm_veto and env_payload is not None:
            phase = str(env_payload.get("phase", "calm"))
            if phase == "storm-washout":
                try:
                    speed = float((env_payload.get("conditions") or {})
                                  .get("speed_bias", 0.0))
                except (TypeError, ValueError):
                    speed = 0.0
                if bias * speed > 0:
                    self.skipped += 1
                    self.log.info("[BLOCK_4] Выход: ВЕТО среды: шторм-вынос, сценарий по "
                                  "каскаду (%s) — ловушка, не пишу", direction)
                    return

        atr = self._atr()
        atr_pct = atr / self.price
        # Зоны Поля — рабочий массив для баллистических координат (Шаг 2/3 ТЗ)
        self._zones = zones
        # КООРДИНАТА ВХОДА (утверждено, Шаг 2): не тик, а столкновение вектора
        # хода с полкой SR с поправкой на трение стакана (psy_viscosity).
        entry, z_lo, z_hi = self._ballistic_entry(direction, self.price, atr)
        entry_zone = (z_lo, z_hi)
        # ОБЛАКО ШУМА вокруг вектора маршрута (утверждено, Шаг 4): ширина
        # прямо пропорциональна хаосу (psy_chaos) и дрожи Линз. Стоп живёт
        # ЗА внешней границей облака — пережить шум, а не умирать в нём.
        vol_route_early, vol_scale_early = self._vol_route()
        chaos_early = psy["chaos"][0] if psy["chaos"][0] is not None else 0.0
        cloud_noise = ((vol_route_early or 0.0) * self.route_stop_noise_mult
                       * (1.0 + self.cloud_atr_mult * chaos_early))
        cloud_pct = cloud_noise if cloud_noise > 1e-9 else 0.0

        # --- стоп-каркас: ЗА границей облака шума (утверждено, Шаг 4) ---
        # Структурная полка по-прежнему даёт ориентир, но ОКОНЧАТЕЛЬНЫЙ стоп
        # — за облаком: иначе шум маршрута выбивает сделку раньше времени.
        cloud_atr = cloud_pct * entry
        if direction == "long":
            below = [z for z in zones if float(z["high"]) < entry]
            if below:
                nearest = max(below, key=lambda z: float(z["high"]))
                stop_raw = float(nearest["low"]) - 0.3 * atr
            else:
                stop_raw = entry - 1.5 * atr
            ahead = sorted([z for z in zones if float(z["low"]) > entry],
                           key=lambda z: float(z["low"]))
            cloud_edge = entry - cloud_atr
        else:
            above = [z for z in zones if float(z["low"]) > entry]
            if above:
                nearest = min(above, key=lambda z: float(z["low"]))
                stop_raw = float(nearest["high"]) + 0.3 * atr
            else:
                stop_raw = entry + 1.5 * atr
            ahead = sorted([z for z in zones if float(z["high"]) < entry],
                           key=lambda z: -float(z["high"]))
            cloud_edge = entry + cloud_atr
        # За облаком — и не ближе структурной полки
        if direction == "long":
            stop_raw = min(stop_raw, cloud_edge)
        else:
            stop_raw = max(stop_raw, cloud_edge)

        # Время пути — «скорость созревания» из фактов прошлых сделок
        # (0.45% ≈ 2 часа), а не диффузия: диффузия с длинных Линз давала
        # фантастические тысячи минут. Приоритет выбора — сила зоны.
        vol_route, vol_scale = self._vol_route()
        cand = []
        # ПОРОГ ШУМА (утверждено): цель обязана ВМЕЩАТЬ честный стоп —
        # иначе дрожь выбьет сделку (стопы 0.1-0.2% гибли за 1-4 мин).
        # Отбор целей идёт по этому порогу, а не постфактум.
        noise_pct = (self.route_stop_noise_mult * vol_route
                     * math.sqrt(self.route_wait_min)
                     if vol_route and vol_route > 1e-9 else 0.0)
        floor_pct = max(noise_pct, self.stop_atr_floor * atr_pct)
        min_dist = floor_pct / self.route_stop_frac
        # ЦЕЛЬ = КОНКРЕТНАЯ АНОМАЛИЯ (утверждено, Шаг 3 ТЗ): смотрим по стрелке
        # вектора Регрессии (1м и 4ч) и берём ближайшую ТЯЖЁЛУЮ аномалию
        # из Зон+ — стоп-кластер или FVG-разрыв. Координата этой плиты
        # ликвидности становится жёсткой точкой Области Профита. Никаких
        # абстрактных «целей в пустоту».
        heavy = self._heavy_anomaly(direction, entry, atr)
        targets_pool = self._forecast_targets(direction, entry)
        if heavy is not None:
            # Плита приоритетнее прогнозного пути: это КОНКРЕТНАЯ цена
            targets_pool.insert(0, heavy)
        targets_pool += [((float(z["low"]) + float(z["high"])) / 2.0)
                         for z in ahead]
        for tgt in targets_pool:
            dist = abs(tgt - entry) / entry
            if dist <= 0 or dist < min_dist:
                continue
            net = dist - self.round_cost
            t_est = (dist / self.route_speed
                     if self.route_speed > 0 else None)
            st = 1.0
            score = net
            cand.append({"price": round(tgt, 8), "dist": dist, "net": net,
                         "t_est": t_est, "strength": st, "score": score})
        viable = [c for c in cand if c["net"] > self.min_net_move]
        # ЦЕЛЬ — БЛИЖАЙШАЯ ОКУПАЕМАЯ (утверждено): деньги берём на том,
        # что близко и окупается, а не на «жирной мечте» вдали
        chosen = (sorted(viable, key=lambda c: (c["dist"], -c["score"]))[0]
                  if viable else None)
        if chosen is not None:
            targets = [chosen["price"]]
            dist_target, net_move = chosen["dist"], chosen["net"]
            t_est = chosen["t_est"]
        else:
            # ни одна мишень не окупает издержки — сценария нет:
            # торгуем только к магнитам, «цель в пустоту» убрана
            self.skipped += 1
            self.log.info("[BLOCK_5] Выход: нет окупаемой цели (зон впереди %d, "
                          "издержки %.2f%%, запас x%.1f)",
                          len(ahead), self.round_cost * 100,
                          self.net_margin)
            return

        # --- ЧИСТАЯ ПРИБЫЛЬ: издержки вычитаются из хода к цели ---
        best_gross = dist_target
        if net_move <= self.min_net_move:
            self.skipped += 1
            self.log.info("[BLOCK_6] Выход: издержки съедают ход (грязный %.2f%%, "
                          "чистый %.2f%% ≤ порога %.2f%%)",
                          best_gross * 100, net_move * 100,
                          self.min_net_move * 100)
            return          # издержки съедают прибыль — сценарий не пишется

        # --- СТОП (ступень 1, утверждено): структурный, но в коридоре
        #     «дрожь ожидания…доля пути»: шум минутного масштаба убивал
        #     сделки раньше маршрута (у 34% выбитых цель потом была
        #     достигнута). Потолок — доля пути (RR не хуже 1/frac).
        struct_pct = abs(entry - stop_raw) / entry
        noise_pct = (self.route_stop_noise_mult * vol_route
                     * math.sqrt(self.route_wait_min)
                     if vol_route and vol_route > 1e-9 else 0.0)
        floor_pct = max(noise_pct, self.stop_atr_floor * atr_pct)
        # ПОТОЛОК ЖЁСТКИЙ: стоп не шире доли пути до цели (RR ≥ 1/frac).
        # Иначе при высокой дрожи пол поднимал стоп ВЫШЕ цели (окно
        # 26.09: стопы 0.72–0.74% при цели 0.6% — риск больше награды).
        cap_pct = self.route_stop_frac * dist_target
        if floor_pct > cap_pct:
            # ШУМ ШИРЕ ЧЕСТНОГО СТОПА (утверждено, аудит 28.09): цель мала
            # для RR — сделка была бы лотереей дрожи (стопы 0.1-0.2%
            # выбивало за 1-4 минуты, Лаба мерила идеал по нулям).
            # Не торгуем: ждём цель крупнее — стоп обязан прятаться за шум.
            self.skipped += 1
            self.log.info("[BLOCK_7] Выход: дрожь %.2f%% шире допустимого стопа "
                          "%.2f%% (цель %.2f%%) — цель мала",
                          floor_pct * 100, cap_pct * 100, dist_target * 100)
            return
        stop_pct_use = min(max(struct_pct, floor_pct), cap_pct)
        if stop_pct_use <= 0:
            self.skipped += 1
            self.log.info("[BLOCK_8] Выход: стоп схлопнулся в ноль (вход == стоп)")
            return
        stop = (entry * (1.0 - stop_pct_use) if direction == "long"
                else entry * (1.0 + stop_pct_use))

        # --- уверенность = вероятность прибыльного закрытия ---
        picture_conf = float(self._picture.get("confidence", 0.0))
        calib = self.kernel.cfg.scenario_conf_calibration
        net_ratio = max(0.0, min(1.0, net_move / best_gross))
        scenario_conf = round(max(0.0, min(0.99, picture_conf
                              * (0.6 + 0.4 * min(1.0, abs(bias)))
                              * (0.7 + 0.3 * net_ratio) * calib
                              * scenario_conf_mult)), 3)
        # --- Шторм давит уверенность (утверждено): в шторм исполнение хуже
        #     (проскальзывание, выносы стопов) — сценарий честно тише.
        if env_payload is not None and self.storm_factor > 0:
            try:
                r = max(0.0, min(1.0, float(env_payload.get("r", 0.0))))
            except (TypeError, ValueError):
                r = 0.0
            scenario_conf = round(
                scenario_conf * (1.0 - self.storm_factor * r), 3)
        # Вожак против направления при расхождении — уверенность режется
        if (leader_f and leader_f.get("divergence")):
            try:
                against = (float(leader_f.get("angle_btc_pct", 0.0))
                           * (1.0 if direction == "long" else -1.0)) < 0
            except (TypeError, ValueError):
                against = False
            if against:
                scenario_conf = round(
                    scenario_conf * (1.0 - self.leader_penalty), 3)
                self.log.info("Вожак против (%s) — уверенность −%.0f%%",
                              "вверх" if (1 if direction == "long" else -1) * -1
                              else "вниз", self.leader_penalty * 100)
        # --- TTE (утверждено): календарь СООБЩАЕТ о макро-событии
        #     (факт + доверие), а решение — Сценариста: как учесть,
        #     решает он сам своей ручкой tte_max_pressure. Направление
        #     не трогаем, срок сценария TTE не назначает. У события
        #     исполнение хуже гарантированно (шторм релиза) — сценарий
        #     честно тише; порог реала сам уводит в тень — статистика
        #     копится, вреда нет.
        tte_factor, tte_event = self._tte_factor()
        if tte_factor < 1.0:
            scenario_conf = round(max(0.0, scenario_conf * tte_factor), 3)
        if self._tte:
            _n("tte", kind="tte-state", value=tte_factor, weight=0.0, push=0.0,
               age_sec=None, state="fresh", extra={"directionless": True,
                                                   "event": tte_event})

        # --- плечо: минимальное → по риску и уверенности, но ликвидация
        #     обязана быть дальше стопа (расстояние ликвидации ≈ 1/плечо) ---
        stop_pct = abs(entry - stop) / entry
        if stop_pct <= 0:
            self.log.info("[BLOCK_9] Выход: вход == стоп (нет расстояния риска)")
            return
        max_lev = self.kernel.cfg.scenario_max_lev   # живая ручка (Этап 35)
        # Риск-огранитель: даже при узком облаке плечо не может превысить
        # то, что выдерживает доля риска против расстояния до стопа.
        base_lev = min(max_lev, max(1.0, self.risk_pct / stop_pct))
        liq_cap = 1.0 / (stop_pct * self.liq_buffer)
        lev_cap = max(1.0, min(max_lev, liq_cap, base_lev))
        # ПЛЕЧО ЧЕРЕЗ КОРИДОР ДИСПЕРСИИ (утверждено, Шаг 4): обратно
        # пропорционально ширине облака шума. Широкое облако (пила, хаос) —
        # плечо падает к 1х; узкое (лазерный луч в тишине) — безопасно
        # растёт к потолку Риска. «Согласованность сил» в расчёте не
        # участвует: плечо — это про ширину коридора, а не про веру в знак.
        lev_psy = self._leverage_psy(cloud_atr / atr if atr > 0 else 0.0,
                                     lev_cap)
        leverage = round(min(lev_psy, lev_cap), 1)
        # ДОБОР ДО МИНИМУМА (Этап 35): сделка обязана быть исполнимой —
        # объём (маржа × плечо) накрывает минимум биржи, если потолки
        # позволяют; не добрали — Риск уведёт в тень честно (below_min)
        min_not = float(getattr(
            self.kernel.modules.get("dealer"), "min_notional", 0.0) or 0.0)
        balance = float(getattr(
            self.kernel.modules.get("risk-manager"), "balance", 0.0) or 0.0)
        margin_usdt = balance * float(
            self.kernel.cfg.scenario_position_pct) / 100.0
        leverage = topup_leverage(leverage, lev_cap, min_not, margin_usdt)

        # --- КАСКАД ТРАЕКТОРИИ (утверждено, Шаг 5) ---
        # Многоразовость включается только вокруг ТЯЖЁЛОЙ СТРАТЕГИЧЕСКОЙ
        # плиты (регрессия 4ч говорит) и достаточно искривлённого поля:
        # одна плохая компания тогда не уничтожает репутацию вектора.
        rec4h = self._trend_by_tf.get("4h") or {}
        try:
            strat_strength = abs(float((rec4h.get("fields") or {})
                                      .get("score", 0.0)))
        except (TypeError, ValueError):
            strat_strength = 0.0
        curve_bend = min(1.0, cloud_atr / atr if atr > 0 else 0.0)
        multi_entry = (strat_strength >= self.cascade_strat_min
                       and curve_bend >= self.cascade_bend_min)
        cascade_levels = []
        if multi_entry and entry_zone[0] is not None and atr > 0:
            # Три лимитных уровня ВДОЛЬ вектора хода: вход, добой после
            # первой неудачи и ещё один глубже — эскалация ставки на ту же
            # траекторию, а не новая идея.
            zl, zh = entry_zone
            step = atr * self.cascade_step_atr
            if direction == "long":
                for i in range(3):
                    lvl = entry - i * step
                    if lvl <= zl:
                        break
                    cascade_levels.append(round(lvl, 8))
            else:
                for i in range(3):
                    lvl = entry + i * step
                    if lvl >= zh:
                        break
                    cascade_levels.append(round(lvl, 8))
        # --- ФЛАГ СВОБОДНОГО ТРЕЙЛИНГА (утверждено, Шаг 5) ---
        # Цена Области Профита (плита ликвидности) — точка, где жёсткие
        # лимиты Дайлера больше не нужны: остаток движения выжимает
        # адаптивное сопровождение.
        free_trail = False
        free_trail_price = None
        if targets:
            try:
                free_trail_price = float(targets[0])
                reached = (free_trail_price >= entry) if direction == "long" \
                    else (free_trail_price <= entry)
                free_trail = bool(reached)
            except (TypeError, ValueError):
                free_trail = False
                free_trail_price = None

        # ПОЛНОТА ВЕКТОРА (утверждено): «не слышал» — тоже факт момента;
        # каждый вид данных из справочника фактов обязан иметь запись в
        # векторе. Порогов нет: слабые и нулевые значения остаются как есть.
        for name in FACT_AUTHORS:
            if name in vec:
                continue
            knob = WEIGHT_KNOBS.get(name)
            w = (float(c.get_float(knob[0], knob[1], knob[2]))
                 if knob is not None else 0.0)
            vec[name] = _vec_entry(kind=FACT_AUTHORS[name][1], value=None,
                                   weight=w, push=0.0, age_sec=None,
                                   state="missing")
        # НАСТРОЙКИ МОМЕНТА (утверждено): веса, доверия и ручки, которыми
        # жил контур в момент штампа — как есть. Сценарист перечитывает их
        # каждым расчётом; правку Тюнера сценарий слышит сразу, а снимок
        # Лаборатории получает ровно те значения, при которых сделка жила.
        settings = {
            "weights": {name: round(float(c.get_float(sec, key, start)), 6)
                        for name, (sec, key, start, _lo, _hi)
                        in WEIGHT_KNOBS.items()},
            "knobs": {name: round(float(c.get_float(sec, key, start)), 6)
                      for name, (sec, key, start, _lo, _hi)
                      in TUNER_KNOBS.items()},
            "trusts": {kind: round(float(val), 4) for kind, val
                       in (c.trust_directory() or {}).items()},
            "at_ms": int(now_ms),
        }

        fields = {
            "direction": direction,
            "entry": round(entry, 8),
            "entry_zone": [round(entry - 0.1 * atr, 8), round(entry + 0.1 * atr, 8)],
            "stop": round(stop, 8),
            "stop_pct": round(stop_pct, 5),
            "targets": targets,
            # ФЛАГ СВОБОДНОГО ТРЕЙЛИНГА (утверждено, Шаг 5): цена плиты
            # ликвидности достигнута — Дилер снимает жёсткие лимиты и
            # отдаёт сделку адаптивному сопровождению для выжимания остатка.
            "free_trail": free_trail,
            "free_trail_price": (round(free_trail_price, 8)
                                 if free_trail_price else None),
            # КАСКАД ТРАЕКТОРИИ (утверждено, Шаг 5): вокруг тяжёлой
            # стратегической плиты (4ч) даём до трёх лимитных уровней —
            # одна плохая компания не должна убивать репутацию вектора.
            "multi_entry": multi_entry,
            "cascade_levels": cascade_levels,
            # МАРШРУТ И КАНДИДАТЫ (ступень 1, утверждено): все зоны-мишени
            # с чистыми деньгами и временем пути — Тюнер/Лаба видят
            # отвергнутые варианты и калибруют правило выбора цели
            "route": {
                "vol_scale_sec": (int(vol_scale) if vol_scale else None),
                "vol_pct_min": (round(vol_route * 100, 5)
                                if vol_route else None),
                "wait_min": self.route_wait_min,
                "t_est_min": (round(t_est, 1) if t_est else None),
                "stop_frac": round(stop_pct_use / dist_target, 4),
            },
            "candidates": [
                {"price": c["price"], "dist_pct": round(c["dist"] * 100, 4),
                 "net_pct": round(c["net"] * 100, 4),
                 "strength": round(c["strength"], 3),
                 "t_est_min": (round(c["t_est"], 1) if c["t_est"] else None)}
                for c in cand[:6]],
            "leverage": leverage,
            "leverage_max": max_lev,
            # ОБЪЁМ — ПАРАМЕТР СДЕЛКИ (утверждено): доля баланса в марже
            # задаёт Сценарист; Риск только ОЦЕНИВАЕТ уровень риска
            "position_pct": self.kernel.cfg.scenario_position_pct,
            # ВЕКТОР ФАКТОВ (утверждено): для формулы направления — как
            # было; полный слепок момента — рядом, ключ «vector»
            "facts": facts,
            # ВЕКТОР МОМЕНТА (утверждено): все виды данных КАК ЕСТЬ —
            # значение, знак, сила, вклад, свежесть, состояние; плюс
            # настройки момента. Читатели: Лаборатория (снимок), Тюнер
            "vector": vec,
            "settings": settings,
            # РАСЧЁТНЫЙ ИТОГ (утверждено): сколько хотим заработать и что
            # теряем, если стоп — в % БАЛАНСА с этим плечом
            "expected_pnl_pct": round(leverage * net_move * 100, 3),
            "expected_loss_pct": round(stop_pct * leverage * 100, 3),
            "atr": round(atr, 8),
            "atr_pct": round(atr_pct, 5),
            "costs": {
                "fee_pct": round(self.fee_pct * 100, 3),
                "slippage_pct": round(self.slip_pct * 100, 3),
                "round_trip_pct": round(self.round_cost * 100, 3),
            },
            "gross_move_pct": round(best_gross * 100, 4),
            "net_move_pct": round(net_move * 100, 4),
            "confidence_meaning": "вероятность прибыльного закрытия",
            # СОПРОВОЖДЕНИЕ — в масштабе МАРШРУТА (ступень 1, утверждено):
            # активация и шаг — доли пути до цели; начальный СЛ + трейлинг,
            # безубыток-костыль убран (закрытие у входа убивало прибыль)
            "trailing": {
                "activation_pct": round(
                    self.route_trail_act_frac * dist_target, 5),
                "distance_pct": round(
                    self.route_trail_dist_frac * dist_target, 5),
            },
            "bias": round(bias, 3),
            "rsi_pressure": (round(float(self._rsi_state.get("pressure", 0.0)), 3)
                             if self._rsi_state else None),
            "oi_trend": (round(float((env_payload.get("conditions") or {})
                                .get("oi_trend", 0.0)), 3)
                         if env_payload is not None else None),
            "picture_seq": self._picture["seq"],
            # ПРИШИТАЯ КАРТИНА: зоны, на которых построен сценарий —
            # Лаборатории второй поиск не нужен (слепок Интуита)
            "picture_zones": ((self._picture.get("fields") or {})
                              .get("zones") or []),
            "tte_factor": round(tte_factor, 3),
            "node_ttl_factor": round(node_ttl_factor, 3),
            "tte_event": tte_event,
            # ГИГИЕНИЧЕСКИЙ ПОТОЛОК (24 ч) — уборка шины; фактическая
            # жизнь сценария = до мишени / отзыва автора / стопа.
            # TTE и узел — факты для разбора (см. выше), сроком не командуют.
            "valid_until_ms": int(now_ms + 3600.0 * 1000.0
                                  * self.kernel.cfg.scenario_horizon_hours),
        }

        # ПОДПИСЬ ИДЕИ: направление + магнит (с точностью до ATR) + цель.
        # МАГНИТ И ЖИЗНЕННЫЙ ЦИКЛ (исправление 02.10): магнитом остаётся
        # ПОЛКА SR — именно её проверяет `_magnet_alive` при отзыве. Раньше
        # сюда клалась targets[0], а это после перехода на баллистику —
        # СЕРЕДИНА ПЛИТЫ Зон+ (fvg/stop-cluster). Такой координаты среди
        # полок Поля нет, и сценарий отзывался автором ЧЕРЕЗ 20 секунд после
        # публикации: 6 штампов подряд и 6 отзывов, робот стоял.
        # Цель-плита продолжает жить в `targets` (это профит, не магнит).
        magnet = (entry_zone[0] if entry_zone[0] is not None
                  else (targets[0] if targets else entry))
        signature = (direction,
                     round(entry / max(atr, 1e-9)),
                     round(magnet / max(atr, 1e-9)),
                     round(psy["viscosity"][0] or 0.0, 1),
                     round(psy["chaos"][0] or 0.0, 1))
        too_soon = (now_ms - self._last_stamp_ms) < self.stamp_min_interval * 1000
        stale = (now_ms - self._last_stamp_ms) >= STAMP_REFRESH_SEC * 1000
        if signature == self._signature and not stale:
            self.log.info("[BLOCK_10] Выход: дедуп — та же идея, не устарела "
                          "(подпись %s)", signature)
            return
        if too_soon and signature != self._signature:
            return          # цена дёргается — не засоряем склад

        self.publish(DRAFT, {
            "kind": "scenario",
            "author": "scenario-writer",
            "confidence": scenario_conf,
            "fields": fields,
            "lineage": [f"card:{self._picture['seq']}",
                        f"candle:{self.symbol}:{self.base_tf_sec}:{self._rows[-1][0]}"],
        })
        self._signature = signature
        self._last_stamp_ms = now_ms
        self.issued += 1
        self._published[(direction, round(magnet, 8))] = {
            "direction": direction,
            # Магнит — координата полки SR: её и проверяет _magnet_alive
            # при отзыве (см. комментарий выше). Кладём сюда тот же magnet,
            # что в подписи, иначе сценарий отзывается сам собой.
            "magnet": magnet,
        }
        self.log.info("Сценарий №%d: %s | вход %.5f, стоп %.5f (%.2f%%), "
                      "цель %s, плечо %.1f, уверенность %.2f | "
                      "расчёт: +%.2f%% / −%.2f%% баланса",
                      self.issued, direction, entry, stop, stop_pct * 100,
                      targets[:1], leverage, scenario_conf,
                      leverage * net_move * 100, stop_pct * leverage * 100)
