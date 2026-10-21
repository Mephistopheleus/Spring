"""Единый словарь ярлыков шины NeirOS-M.

Единственное место, где живут названия ярлыков. Модули импортируют
отсюда — так названия никогда не разойдутся (урок аудита SWARM:
несовпадение ключей «matrix»/«vector» молча теряло данные).

Файлы, начинающиеся с «_», ядро не считает модулями — это справочник.

Формат черновика (ярлык DRAFT), словарь:
    {"kind": str,            -- тип карточки, например "sr-zone"
     "author": str,          -- автор (анализатор/индикатор)
     "confidence": 0..1,     -- уверенность автора в этом прогнозе
     "fields": {...},        -- содержимое: что конкретно прогнозируется
     "lineage": [...]}       -- родословная: откуда данные (необязательно)

Формат готовой карточки (ярлык CARD) добавляет Регистратор:
    seq, created_ms, trust, weight  (вес = произведение:
    уверенность × доверие×2 — «никаких средних голубцов»)
"""

# «Доска объявлений»
CANDLES = "candles"       # свечи: {"symbol", "tf_sec", "candles": [[ts_ms,o,h,l,c,v,delta],...]}
CANDLES_WARMUP = "candles_warmup"
                          # партия прогрева, ярлык с номером ТФ:
                          # f"candles_warmup:{tf_sec}" — например candles_warmup:300.
                          # Хранится на «доске значений» — поздние модули забирают
                          # свою ТФ через get_last при своём запуске
ORDERBOOK = "orderbook"   # стакан: {"bids": [[цена, кол-во]...], "asks": [...]}
PRICE = "price"           # поток сделок (нить полотна): {"symbol","price",
                          # "qty","is_sell","ts_ms"} — каждая сделка
BTC_PRICE = "btc_price"   # то же, но для вожака (BTCUSDT): сырьё Поводыря.
                          # Отдельный чистый топик — не мешает основному
                          # полотну (утверждено: никаких чужих готовых
                          # корреляций, считаем сами из сырых сделок)
ALT_PRICE = "alt_price"   # сделки альтов пакета (ETH/SOL/PEPE): сырьё
                          # Метакоррелятора (Волна 3). Тот же формат, что
                          # PRICE/BTC_PRICE + поле "symbol"
BTC_LIQ = "btc_liq"       # ликвидации вожака (forceOrder, BTCUSDT):
                          # паника вожака — шторм-маркер Метакоррелятора.
                          # Из общего потока !forceOrder (наш символ
                          # фильтруется в LIQUIDATIONS отдельно)
SURFACE = "surface"       # срез поверхности от Линз — ТЕНЗОР живой матрицы:
                          # {"ts_ms", "surface_tensor": np.ndarray (N,4) со
                          # столбцами [slope_min_pct, vol_min_pct, trend_z,
                          # flow_imb], "scale_vectors": np.ndarray (N,1) —
                          # масштабы (сек) по возрастанию, "confidence_
                          # vectors": np.ndarray (N,2) [n_eff, age_sec] —
                          # НАДЁЖНОСТЬ показания (19.10.2026): эффективное
                          # число наблюдений линзы + секунд с последней
                          # сделки (сердцебиение тихого рынка)}. Непрогретая
                          # линза — NaN. Читать по индексу/значению масштаба
                          # (lenses.surface_read), НЕ по подписи ТФ.
LIQUIDATIONS = "liquidations"
                          # принудительные закрытия (поток !forceOrder):
                          # {"symbol","side","qty","price","ts_ms"} —
                          # мгновенный датчик лавины для «OI + Каскад»
OI_SNAPSHOT = "oi_snapshot"
                          # снимок открытого интереса (Каскад, раз в 30с):
                          # {"oi","ts_ms"} — сырьё Проводимости
CONDUCTIVITY = "conductivity"

# ДОВЕРИЕ ПО ФАКТАМ: вердикт факта → автор (модуль, вид карточки).
# Единая карта для Лаборатории (подпись фактов в снимках) и Тюнера
FACT_AUTHORS = {
    "magnets": ("field-analyst", "picture"),
    "trend": ("regression", "trend-vector"),
    "fuel": ("flow-analyst", "flow-state"),
    "news": ("news-aggregator", "news-sentiment"),
    "leader": ("leader", "leader-correlation"),
    "rsi": ("rsi", "rsi-state"),
    "oi": ("oi-cascade", "level-verdict"),
    "spring": ("spring", "spring-state"),
    "selfsim": ("fractals", "topological-resonance"),
    "tape_speed": ("conductivity", "conductivity"),
    # Плоский `lens_trend` УДАЛЁН (утверждено) — вместо него 30 послойных
    # полей lens_{trend,flow,slope}_{N}m, регистрируются
    # register_lens_fields() при старте Сценариста.
    "package": ("meta-corr", "meta-corr"),
    "divergence": ("meta-corr", "meta-corr"),
    "intuit": ("intuit", "intuit-hint"),
    "hurst": ("fractals", "topological-resonance"),
    "entropy": ("entropy", "entropy-state"),
}

# ---------- ПОСЛОЙНЫЕ ПОЛЯ ПИРАМИДЫ МАСШТАБОВ ----------
# ТОТАЛЬНАЯ РАЗВЁРТКА (утверждено): плоский костыль `lens_trend` (одно
# среднее по масштабам) УДАЛЁН. Вместо него Сценарист пишет для КАЖДОГО
# масштаба три независимых числа с уникальным именем:
#     lens_trend_{N}m  — наклон/шум этого масштаба (±3)
#     lens_flow_{N}m   — перекос покупателей/продавцов (−1..1)
#     lens_slope_{N}m  — дрейф цены, % в минуту
# Имена строятся программно, чтобы масштабы в конфиге и справочник
# оставались согласованными без ручного списка на 30 строк.

LENS_PARAMS = ("trend", "flow", "slope")


def lens_field(param, minutes):
    """Имя послойного поля: ('trend', 2) -> 'lens_trend_2m'.

    Формат ОБЩИЙ для меток полей и ключей весов (см. _knobs.lens_knob):
    %g пишет дробные масштабы без потери точности ('trend', 0.5) ->
    'lens_trend_0.5m'. Было int(minutes): 0.5 и 0.71 обе превращались
    в 0 — коллизия полей (п.3 аудита 18.10).
    """
    return f"lens_{param}_{minutes:g}m"


# ЭТАЛОННАЯ ШКАЛА ЛИНЗ (утверждено хозяином 18.10.2026): ПЛОТНАЯ лестница,
# объединение лог-сетки шага sqrt(2) (половина октавы) от 0.5 мин до 1 недели
# СО СПЕКТРОМ ТФ BINANCE. Линзы дешёвы (считаются из потока сделок, API не
# тратятся) — чем плотнее, тем лучше. Все биржевые ТФ (1,3,5,15,30,60,120,
# 240,360,480,720,1440,4320 мин) присутствуют обязательно; при расхождении
# лог-точки и ТФ <10% побеждает биржевой. Верх 10080 мин (1w) — прогрев
# полотна (history_limit=10080 минут) ровно на один полураспад.
# НЕ путать с шкалой ТФ пирамиды полотна ([market] pyramid/senior) — это
# полураспады EWMA-линз поверхности.
# Держать синхронно с config.ini/config.py [lenses] scales_min.
LENS_SCALES_MIN = "0.5,0.71,1,1.41,2,3,4,5,5.66,8,11.3,15,22.6,30,45.3,60,90.5,120,181,240,360,480,720,1024,1440,2048,2896,4320,5793,8192,10080"


def lens_scales(default=None):
    """Масштабы линз в минутах (эталон LENS_SCALES_MIN). Дробные допустимы."""
    raw = LENS_SCALES_MIN if default is None else ",".join(
        str(x) for x in default)
    try:
        return tuple(float(x) for x in raw.split(",") if x.strip())
    except ValueError:
        return (0.5, 1, 2, 3, 5, 8, 13, 21, 34, 55, 90, 144, 240)


def register_lens_fields(scales=None):
    """Занести послойные поля в FACT_AUTHORS.

    Вызывается Сценаристом на старте с реальными масштабами из конфига
    (и тестами). Все поля принадлежат автору Линзы, но Тюнер ведёт доверие
    по ТИПУ данных — поэтому у каждого масштаба свой тип: один факт на
    масштаб, иначе 30 полей слиплись бы в одну усреднённую оценку
    (ровно та ошибка, которую разбирали).
    """
    for m in (scales or lens_scales()):
        for p in LENS_PARAMS:
            FACT_AUTHORS.setdefault(lens_field(p, m),
                                    ("lenses", f"lens-{p}"))
    return len(FACT_AUTHORS)


# ---------- РЕГРЕССИЯ ПОСЛОЙНА (утверждено) ----------
# velocity, acceleration, lifetime_min и decay_rate Регрессия СЧИТАЕТ и
# публикует (regression.py), но в векторе момента их не было: Сценарист
# брал только score. Теперь у каждого уровня регрессии (тактика 1м и
# стратегия 4ч) — пять независимых полей, включая УСКОРЕНИЕ и ЗАТУХАНИЕ.

REG_PARAMS = ("score", "vel", "acc", "decay", "lifetime")
# Метки уровней Регрессии: тактика 1м и стратегия 4ч. Раньше стратегией
# стояло "64m" — интервала, которого биржа не знает: кольцо копилось
# месяцами синтезом вместо настоящих свеч. Метка обязана соответствовать
# strategy_tf_sec (core/config.py), иначе Регрессия читает то, чего нет.
REG_TFS = ("1m", "4h")


def reg_field(param, tf_label):
    """Имя послойного поля Регрессии: ('vel', '1m') -> 'reg_vel_1m'."""
    return f"reg_{param}_{tf_label}"


def register_reg_fields(tfs=None):
    """Занести послойные поля Регрессии в FACT_AUTHORS."""
    for tf in (tfs or REG_TFS):
        for p in REG_PARAMS:
            FACT_AUTHORS.setdefault(reg_field(p, tf),
                                    ("regression", f"reg-{p}"))
    return len(FACT_AUTHORS)


def register_rsi_fields():
    """RSI по двум уровням — два самостоятельных измерителя.

    Раньше в вектор шёл один `pressure`, посчитанный по МЛАДШЕМУ (1м)
    уровню: стратегический 64м RSI не читался никем, хотя Регрессия и
    RSI его считают.
    """
    for key in ("rsi_base", "rsi_strat"):
        FACT_AUTHORS.setdefault(key, ("rsi", f"rsi-{key[4:]}"))
    return len(FACT_AUTHORS)


# Шкала фрактальных полей — БИРЖЕВАЯ палитра (единый источник правды):
# раньше была своя лог-шкала 1m/2m/4m…512m — читатели разъезжались с
# тем, что публикует Фракталов, и поля молча читались как пустые.
FRACT_TFS = ("1m", "3m", "5m", "15m", "30m", "1h",
             "2h", "4h", "6h", "8h", "12h", "1d", "3d")
FRACT_PARAMS = ("corr", "dir", "scale")


def fract_field(param, tf_label):
    """Имя послойного поля самоподобия: ('corr','4h') -> 'fractal_corr_4h'."""
    return f"fractal_{param}_{tf_label}"


def register_fractal_fields(tfs=None):
    """Занести ОСИ САМОПОДОБИЯ по всем масштабам в FACT_AUTHORS.

    Три оси (corr/dir/scale) на каждый масштаб — 39 независимых полей (13 этажей × 3 оси).
    Раньше в вектор шёл один `selfsim` — лучшее по модулю совпадение, и
    Тюнер не видел, КАКОЙ именно этаж похож на младший. Теперь видит.
    """
    for tf in (tfs or FRACT_TFS):
        for p in FRACT_PARAMS:
            FACT_AUTHORS.setdefault(fract_field(p, tf),
                                    ("fractals", f"fractal-{p}"))
    return len(FACT_AUTHORS)


def register_hurst_fields(tfs=None):
    """Послойной асимметричный Херст: Up/Down/симметричный + импульс."""
    for tf in (tfs or FRACT_TFS):
        FACT_AUTHORS.setdefault(f"hurst_up_{tf}", ("fractals", "hurst-up"))
        FACT_AUTHORS.setdefault(f"hurst_down_{tf}", ("fractals", "hurst-down"))
        FACT_AUTHORS.setdefault(f"hurst_{tf}", ("fractals", "hurst-sym"))
        FACT_AUTHORS.setdefault(f"fractal_momentum_{tf}",
                                ("fractals", "fractal-momentum"))
    return len(FACT_AUTHORS)


def register_slope_fields(tfs=None):
    """Нормированные наклоны колец: trend_slope_{tf} — число вместо вердикта.

    Бывший fractal_bias (два голоса 16/64 мин, усреднение в −1..1) вырезан
    20.10: голосовалка из числового модуля убрана. Вместо неё — наклон
    closes каждого из 13 этажей полотна, в % цены за бар своего ТФ.
    """
    for tf in (tfs or FRACT_TFS):
        FACT_AUTHORS.setdefault(f"trend_slope_{tf}",
                                ("fractals", "trend-slope"))
    return len(FACT_AUTHORS)


# ---------- ПАНОРАМНЫЙ РАДАР ПАКЕТА (27.10) ----------
# 5 метрик связности + 2 производные лага на каждую монету пула.
# Имена и набор живут здесь, Метакоррелятор и Сценарист читают их отсюда —
# иначе набор разъедется, как разъехался порядок осей FACT_AXES.

META_METRICS = ("spearman", "kendall", "dcor", "lag", "lagcorr")
META_DERIVS = ("lag_vel", "lag_acc")
META_POOL_DEFAULT = "BTC,ETH,SOL,PEPE"


def meta_field(metric, symbol):
    """Имя оси радара: ('dcor','ETH') -> 'meta_dcor_ETH'."""
    return f"meta_{metric}_{str(symbol).strip().upper()}"


def register_meta_fields(pool=None, metrics=None, derivs=None):
    """Занести оси ПАНОРАМНОГО РАДАРА в FACT_AUTHORS.

    5 метрик связности + 2 производные лага на каждую монету пула.
    Раньше в вектор шли два плоских скаляра (`package` и `divergence`),
    а 5×N измерений оставались в карточке мертвым грузом — Тюнер их
    не видел, сеть их не получала.
    """
    syms = [s.strip().upper() for s in (pool or META_POOL_DEFAULT).split(",")
            if s.strip()]
    for sym in syms:
        for m in (metrics or META_METRICS):
            FACT_AUTHORS.setdefault(meta_field(m, sym),
                                    ("meta-corr", f"meta-{m}"))
        for d in (derivs or META_DERIVS):
            FACT_AUTHORS.setdefault(meta_field(d, sym),
                                    ("meta-corr", f"meta-{d}"))
    return len(FACT_AUTHORS)


# Полная развёртка: пирамида + регрессия + RSI + фракталы + радар. Сценарист.
def register_all_fields(lens_scales_list=None, meta_pool=None):
    return (register_lens_fields(lens_scales_list) and register_reg_fields()
            and register_rsi_fields() and register_fractal_fields()
            and register_hurst_fields() and register_slope_fields()
            and register_meta_fields(meta_pool))


# ---------- ПСИХОЗ СРЕДЫ (утверждено, Шаг 1 ТЗ) ----------
# Три самостоятельных послойных поля — измерители плотности/хаоса/жадности
# рынка. Не голоса за направление, а ФАКТЫ среды: их учит Тюнер наравне с
# остальными, а Сценарист читает их для координат входа и плеча.
#
#   psy_viscosity — вязкость стакана (>1 жидкая, <1 каменеет)
#   psy_chaos     — энтропия Шеннона по тикам (страх и пила)
#   psy_greed     — упрямство толпы: ΔOI против хода цены (жадность)

PSY_PARAMS = ("viscosity", "chaos", "greed")
PSY_AUTHORS = {"viscosity": ("flow-analyst", "flow-state"),
               "chaos": ("entropy", "entropy-state"),
               "greed": ("conductivity", "conductivity")}


def psy_field(param):
    """Имя поля Психоза Среды: 'chaos' -> 'psy_chaos'."""
    return f"psy_{param}"


def register_psy_fields():
    """Занести три поля Психоза Среды в FACT_AUTHORS.

    У каждого измерителя СВОЙ вид карточки: Тюнер ведёт доверие по типу
    данных, и смешивать плотность стакана с хаосом в один тип означало бы
    ровно то усреднение, которое мы вырезали из пирамиды линз.
    """
    for p in PSY_PARAMS:
        author, kind = PSY_AUTHORS[p]
        FACT_AUTHORS.setdefault(psy_field(p), (author, f"psy-{p}"))
    return len(FACT_AUTHORS)


# ---------- ОСИ ВЕКТОРА АНАЛОГИЙ (утверждено) ----------
# Интуит строит вектор из ЖИВЫХ измерителей, Лаборатория — такой же по
# ИСТОРИИ. Если порядок или набор осей разойдутся, косинусная близость
# станет сравнением разных величин, и библиотека аналогий молча осыплется.
# Поэтому набор и порядок объявлены ОДИН раз здесь, а оба модуля читают
# его отсюда. Каждая ось: (ключ в facts_vec, источник, нормировка).

FACT_AXES = (
    # геометрия полотна
    ("dist_up_atr", "geometry", "atr"),
    ("dist_dn_atr", "geometry", "atr"),
    # ПЛОТНОСТЬ ДАННЫХ: ускорение и стратегический тренд — то, что раньше
    # терялось в плоском score
    ("reg_acc_1m", "reg", "acc"),
    ("reg_score_4h", "reg", "score4h"),
    # динамика полотна
    ("trend_score", "dynamics", "raw"),
    ("trend_angle", "dynamics", "angle"),
    # RSI по двум уровням — самостоятельные измерители
    ("rsi_base", "rsi", "signed"),
    ("rsi_strat", "rsi", "signed"),
    ("flow_score", "dynamics", "raw"),
    ("package_bias", "meta", "raw"),
    ("divergence", "meta", "raw"),
    ("btc_dir", "leader", "btc"),
    ("oi_trend", "conductivity", "raw"),
    ("tape_speed", "conductivity", "raw"),
    # стратегия: срок жизни и затухание тренда
    ("reg_lifetime_4h", "reg", "lifetime"),
    ("trend_decay", "dynamics", "decay"),
)

FACT_AXIS_NAMES = tuple(name for name, _src, _norm in FACT_AXES)

# АЛИАСЫ ОСЕЙ (п.5 аудита, Узел 5): слепки Лаборатории собирают facts_vec
# из вектора Сценариста, где те же измерения лежат под ИМЕНАМИ ВИДОВ
# (leader/oi/package). Живой путь Интуита пишет канонические имена осей —
# без приведения старых имён слепки молча давали нулевые оси и библиотека
# аналогий сравнивала картину с пустотой. Старые снимки журнала читаются
# так же — алиасы действуют и на них.
FACT_ALIASES = {
    "btc_dir": ("leader",),
    "oi_trend": ("oi",),
    "package_bias": ("package",),
}
                          # Функция Влияния Среды (Проводимость): шинная
                          # строка {"ts_ms","r","k_shelf","k_impulse",
                          # "components","conditions","phase","efficiency"}
                          # обновляется раз в publish_interval и НЕ дедупится
                          # (её читают пятеро потребителей). DRAFT-карточка
                          # (узел 8) — плоские 9 чисел без ts_ms/строк/словарей,
                          # дедуп по подписи до 0.001 + heartbeat 300с.
                          # k_shelf 0.1..1.0 — глушит старые полки в шторм;
                          # k_impulse 1.0..2.0 — кинетически верифицированный
                          # импульс (v = лента × ускорение старших этажей)
MATRIX = "matrix"
                          # СРЕЗ СКЛАДА-МАТРИЦЫ (18.10.2026) — НАСТОЯЩЕЕ
                          # системы в ОЗУ: {"ts_ms", "cells": {(kind, axis):
                          # {"value", "ts_ms", "seq"}}}. Ячейка перезаписывается
                          # новым измерением (семантика «сейчас»); история —
                          # только в журнале cards (Хранитель), нить — lineage
                          # и seq-указатели ячеек. Читать методом
                          # matrix.slice(), не сплющивая.
DRAFT = "draft"           # черновик карточки → Регистратору
CARD = "card"             # готовая карточка с паспортом → Складу и читателям
PICTURE = "picture"       # картина (зоны-магниты) от Анализатора Поля
TRUST = "trust"           # справочник доверий от автотюнера
                          # {"kind","trust"} или список таких словарей —
                          # доверие ТИПАМ ДАННЫХ (не анализаторам)
