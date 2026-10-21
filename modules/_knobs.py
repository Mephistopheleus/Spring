"""РЕЕСТР РУЧЕК NeirOS-M — единый справочник настроек, которые ведёт Тюнер.

Файл начинается с «_» — ядро не считает его модулем (как _labels.py).
Формат записи:  имя: (секция, ключ, старт, lo, hi)
    старт — утверждённое значение (эталон полного сброса:
            config/config.default.ini, собран _dump_defaults.py);
    lo/hi — вилка, за которую Тюнер ручку не уводит.

WEIGHT_KNOBS — веса ДАННЫХ в формуле Сценариста: имя факта сценария →
ручка веса. Старты — аудит 27.09 (замеры по 884 сценариям).
TUNER_KNOBS  — остальные узлы контура: маршрут (стоп/цель/сопровождение),
порог реала, калибровка уверенности, сито спуфинга, потолок плеча.
"""
from modules._labels import lens_field, lens_scales

WEIGHT_KNOBS = {
    "magnets": ("scenario", "magnet_weight", 0.50, 0.05, 0.60),
    "news": ("scenario", "news_weight", 0.30, 0.05, 0.45),
    "fuel": ("scenario", "fuel_weight", 0.10, 0.02, 0.25),
    "trend": ("scenario", "trend_weight", 0.20, 0.05, 0.35),
    # Плоский `lens_trend` УДАЛЁН (утверждено): он был одним весом на всё
    # среднее trend_z по шкале и гасил дивергенцию масштабов. Вместо него —
    # 30 послойных весов ниже, регистрируются register_lens_knobs().
    "tape_speed": ("scenario", "tape_speed_weight", 0.0, -0.20, 0.20),
    "leader": ("leader", "weight", 0.03, 0.01, 0.15),
    "rsi": ("rsi", "weight", 0.071, 0.03, 0.20),
    "oi": ("scenario", "oi_weight", 0.02, 0.01, 0.10),
    "package": ("meta", "weight", 0.15, 0.05, 0.30),
    "fractal": ("fractal", "bias_weight", 0.0, -0.15, 0.15),
    "intuit": ("intuit", "weight", 0.10, 0.05, 0.25),
    "spring": ("spring", "weight", 0.08, 0.02, 0.20),
}

# ---------- ПОСЛОЙНЫЕ ВЕСА ПИРАМИДЫ (утверждено) ----------
# ТОТАЛЬНАЯ РАЗВЁРТКА: вместо одного веса `lens_trend_weight` на весь
# плоский костыль — свой вес на КАЖДЫЙ масштаб и параметр. Ручки живут в
# секции `lenses` и пишутся в config.ini как `lens_<параметр>_<N>m_weight`
# (Тюнер калибрует их по полезности — см. tuner._tune_fact_weights).
#
# Старт 0.0: послойные поля в формулу Сценариста пока не входят (см.
# writer._lens_layers) — их измеряет Лаборатория и оценивает Тюнер. Ненулевой
# старт означал бы, что 30 полей сразу давят на направление вслепую.
#
# ШКАЛА — единая с Линзами (п.3 аудита 18.10): раньше здесь стояла своя
# двоичная лестница (1..512), а Линзы считали по конфигной — 30 ручек
# указывали на масштабы, которых не существует, и Тюнер двигал их впустую.

LENS_SCALES = lens_scales()
LENS_PARAMS = ("trend", "flow", "slope")


def lens_knob(param, minutes):
    """Ключ послойного веса в конфиге: ('trend', 2) -> 'lens_trend_2m_weight'.

    Формат %g — тот же, что у меток полей (_labels.lens_field): дробные
    масштабы пишутся без потери точности ('trend', 0.5) ->
    'lens_trend_0.5m_weight' (баг int(0.5)=0 — п.3 аудита 18.10).
    """
    return f"lens_{param}_{minutes:g}m_weight"


def register_lens_knobs(scales=None, target=None):
    """Занести 3×len(scales) весов в WEIGHT_KNOBS (или в target).

    Вилка широкая и двусторонняя: знак веса выбирает сам Тюнер по полезности
    поля (данные, уводившие путь, получают отрицательный вес и работают
    НАОБОРОТ — та же логика, что у остальных фактов).
    """
    reg = WEIGHT_KNOBS if target is None else target
    for m in (scales or LENS_SCALES):
        for p in LENS_PARAMS:
            reg.setdefault(lens_field(p, m),
                           ("lenses", lens_knob(p, m), 0.0, -0.25, 0.25))
    return len(reg)


# ---------- ВЕСА РЕГРЕССИИ ПОСЛОЙНОЙ И ДВУХ RSI (утверждено) ----------
# Ускорение (acceleration) и затухание (decay) Регрессия считает всегда,
# но в векторе их не было: Сценарист брал только score. Теперь у каждого
# уровня (1м/4ч) своя ручка на каждый из пяти параметров — Тюнер обучается
# на ускорении и затухании тренда.
# Вилка УЗКАЯ на acc: вторая производная зашумлена, ей нельзя давить сильно.
#
# Старт 0.0 — те же соображения безопасного входа, что и у полей пирамиды:
# поля измеряются и учатся, но в формулу направления вслепую не давят.

REG_PARAMS = ("score", "vel", "acc", "decay", "lifetime")
# ЕДИНЫЙ ИСТОЧНИК ПРАВДЫ: метки берём из modules._labels.REG_TFS, а не
# дублируем здесь. Раньше список был продублирован, и после перехода на
# биржевую шкалу (_labels: 1m/4h) здесь осталось "64m" — Тюнер регистрировал
# веса для несуществующего уровня, а Сценарист искал reg_score_4h.
from modules._labels import REG_TFS          # noqa: E402
REG_RANGE = {"score": (-0.30, 0.30), "vel": (-0.20, 0.20),
             "acc": (-0.10, 0.10), "decay": (-0.20, 0.20),
             "lifetime": (-0.20, 0.20)}
REG_LIFETIME_NORM_MIN = 30.0   # минут, при которых lifetime = +1


def reg_knob(param, tf_label):
    """Ключ ручки регрессии: ('vel', '1m') -> 'reg_vel_1m_weight'."""
    return f"reg_{param}_{tf_label}_weight"


def rsi_knob(key):
    """Ключ ручки RSI: 'rsi_base' -> 'rsi_base_weight'."""
    return f"{key}_weight"


def register_reg_knobs(tfs=None, target=None):
    """Занести ручки Регрессии и двух RSI в WEIGHT_KNOBS (или в target)."""
    reg = WEIGHT_KNOBS if target is None else target
    for tf in (tfs or REG_TFS):
        for p in REG_PARAMS:
            lo, hi = REG_RANGE[p]
            reg.setdefault(f"reg_{p}_{tf}",
                           ("regression", reg_knob(p, tf), 0.0, lo, hi))
    for key in ("rsi_base", "rsi_strat"):
        reg.setdefault(key, ("rsi", rsi_knob(key), 0.0, -0.20, 0.20))
    return len(reg)


register_lens_knobs()
register_reg_knobs()

# ---------- ВЕСА ПСИХОЗА СРЕДЫ (утверждено, Шаг 1 ТЗ) ----------
# Три поля плотности/хаоса/жадности рынка. Старт 0.0 — Тюнер выводит их
# из нуля по фактам чистой пользы, вслепую они не давят на формулу.
# Вилки узкие: это параметры СРЕДЫ, а не направления — их роль в выборе
# стороны ограничена, зато велика роль в координатах входа и плече.
#
#   psy_viscosity — вязкость стакана (из Потока)
#   psy_chaos     — энтропия Шеннона (хаос/страх)
#   psy_greed     — упрямство толпы ΔOI против хода (жадность)

PSY_PARAMS = ("viscosity", "chaos", "greed")
# Все три ручки живут в одной секции [psy] — Психоз это одна сущность,
# и разбрасывать её ручки по чужим секциям (flow/entropy/conductivity)
# означало бы прятать её настройки по частям.
PSY_SECTION = "psy"
PSY_SECTIONS = {p: (PSY_SECTION, f"psy_{p}_weight") for p in PSY_PARAMS}
PSY_RANGE = (-0.15, 0.15)


def psy_knob(param):
    """Ключ ручки Психоза: 'chaos' -> 'psy_chaos_weight'."""
    return f"psy_{param}_weight"


def register_psy_knobs(target=None):
    """Занести три ручки Психоза Среды в WEIGHT_KNOBS (или в target)."""
    reg = WEIGHT_KNOBS if target is None else target
    lo, hi = PSY_RANGE
    for p in PSY_PARAMS:
        section, key = PSY_SECTIONS[p]
        reg.setdefault(f"psy_{p}", (section, key, 0.0, lo, hi))
    return len(reg)


register_psy_knobs()

TUNER_KNOBS = {
    "route_stop": ("scenario", "route_stop_frac", 0.60, 0.30, 0.80),
    "stop_noise": ("scenario", "route_stop_noise_mult", 1.30, 1.00, 2.00),
    "stop_atr_floor": ("scenario", "stop_atr_floor", 1.80, 1.00, 3.00),
    "net_margin": ("scenario", "net_margin", 1.40, 1.10, 3.00),
    "trail_activate": ("scenario", "route_trail_activate_frac", 0.70, 0.20, 0.90),
    "trail_distance": ("scenario", "route_trail_distance_frac", 0.20, 0.05, 0.60),
    "conf_calibration": ("scenario", "conf_calibration", 1.00, 0.50, 1.50),
    "max_lev": ("scenario", "max_lev", 1.00, 1.00, 10.0),
    "open_conf": ("risk", "open_conf_threshold", 0.75, 0.50, 0.95),
    "spoof_ticks": ("spoof", "vanish_ticks_max", 1.0, 1.0, 3.0),
    # ТРИ ПОТОКА (утверждено): вес исхода КАЖДОГО потока в обучении Тюнера
    # (реал, тень, лаборатория). Потоки анализируются одинаково, ручки —
    # только доля влияния (хозяин: лаба влияет тише). Лабораторные
    # снимки автономны, поэтому у лаба ОДИН вес, а не два.
    "weight_real": ("tuner", "weight_real", 1.00, 0.10, 1.00),
    "weight_shadow": ("tuner", "weight_shadow", 1.00, 0.10, 1.00),
    "weight_lab": ("tuner", "weight_lab", 0.50, 0.10, 1.00),
    # РУЧКА ТОЧНОСТИ ЛИНЗ (12.10.2026): общий множитель полураспада всех
    # линз. Соседние масштабы пирамиды отличаются ровно в 2 раза, поэтому
    # одна ручка на всю лестницу, а не таблица коэффициентов на каждый ТФ.
    # Вилка 0.5..2.0: не даёт увести память ни в истерику, ни в неподвижность.
    "lens_halflife_factor": ("lenses", "half_life_factor", 1.00, 0.50, 2.00),
}

ALL_KNOBS = dict(WEIGHT_KNOBS)
ALL_KNOBS.update(TUNER_KNOBS)
