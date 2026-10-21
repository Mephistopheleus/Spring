"""Загрузка настроек: config/config.ini (настройки) и config/keys.ini (ключи).

Все значения имеют запасные значения по умолчанию — программа стартует
даже если файл настроек отсутствует или неполон.
"""
import configparser
import logging
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

DEFAULTS = {
    "core": {
        "box_size": "200",
        "health_interval": "30",
        "init_timeout": "30",
        "stop_timeout": "10",
        "tick_timeout": "10",
        "envelope_alarm_sec": "15",
        # ЛИЧНЫЕ КОНВЕРТЫ, ЖДУЩИЕ ЯЩИК (P0): адресат может не открыть
        # ящик при старте — команда (например «закрыть») ждёт, а не теряется.
        "envelope_defer_max": "200",
        "envelope_defer_ttl_sec": "300",
        # лимит сделок в истории Потока (deque, [core]) — ручка из config.ini
        "max_trades": "20000",
    },
    "market": {
        "symbol": "DOGEUSDT",
        "base_tf_sec": "60",
        # Пусто = только биржевая палитра (см. EXCHANGE_SPECTRUM): 12 ТФ
        # создаются и наполняются автоматически. Свои не-биржевые ступени
        # (синтез из минуток) сюда не пишем — любую достроит лениво
        # Collector.get_ring по запросу анализатора.
        "senior_tf_sec": "",
        "pyramid_depth": "300",
        "archive_tf_sec": "900,3600,14400",
        "history_limit": "10080",
        "rest_limit": "1500",
        "rest_pages": "8",
        "senior_rest_limit": "300",
        "min_senior_history": "200",
        "anchor_bars": "60",
    },
    "lenses": {
        # ПЛОТНАЯ шкала линз (утверждено 18.10.2026): лог-сетка шага
        # sqrt(2) от 0.5 мин до 1 недели + ОБЯЗАТЕЛЬНЫЙ спектр ТФ Binance
        # (1,3,5,15,30,60,120,240,360,480,720,1440,4320 мин). Линзы дешёвы
        # (API не тратятся) — плотность бесплатна; верх 10080 мин (1w) =
        # прогрев полотна ровно на один полураспад. НЕ путать со шкалой ТФ
        # пирамиды полотна ([market] senior_tf). Эталон: modules/_labels.py
        # LENS_SCALES_MIN (держать синхронно!).
        "scales_min": "0.5,0.71,1,1.41,2,3,4,5,5.66,8,11.3,15,22.6,30,45.3,60,90.5,120,181,240,360,480,720,1024,1440,2048,2896,4320,5793,8192,10080",
        # РУЧКА ТОЧНОСТИ (12.10.2026): множитель полураспада каждой линзы.
        # Полураспад линзы = масштаб (мин × 60 сек) × half_life_factor.
        # 1.0 — по умолчанию (полураспад = масштабу, как в учебнике EWMA);
        # <1 — резче и шумнее, >1 — плавнее, но с отставанием.
        # Ведёт Тюнер (см. _knobs.TUNER_KNOBS, вилка 0.5..2.0).
        "half_life_factor": "1.0",
        # СЕКУНДНЫЙ ПУЛЬС ПОЛОТНА (27.10): Регрессия и Нейро-Прогнозист
        # синхронизированы с этим вдохом. Было 2 — измерители расходились
        # на две секунды, и «полностью обновлённые 63 поля» не получались.
        "publish_interval": "1",
    },
    "flow": {
        "absorption_window_sec": "60",
        "absorption_multiple": "4",
        "cvd_window_min": "30",
        "divergence_force_min": "0.5",
        "burst_multiple": "5",
        "barrier_ttl_min": "15",
        "state_ttl_min": "5",
        "divergence_ttl_min": "10",
        # КАЛИБРОВКА ЧАСТОТЫ (19.10): как часто осматриваем поток и как
        # часто штампуется карточка «состояние потока». Измеряем часто
        # (2 с — норма вязкости требует многих замеров), снимок публикуем
        # раз в минуту (матрица и потребители получают своё; архив
        # журнала рутиной не растёт — см. [storage] skip_card_kinds).
        "eval_every_sec": "2",
        "state_publish_interval": "60",
        # ПОЛУРАСПАД ОБЫЧНЫХ ЗНАЧЕНИЙ (сек): «обычный объём окна» и
        # обычная скорость ленты (скользящие EWMA).
        "typical_half_life_sec": "900",
        "speed_half_life_sec": "300",
        # ТОЛЕРАНЦИЯ УРОВНЯ: тот же барьер ±0.15% — не дублируем
        "level_tol": "0.0015",
    },
    "field": {
        "stamp_interval_sec": "30",
        "zone_ttl_min": "15",
        "touch_cooldown_sec": "15",
        "exhaust_step": "0.2",
        "exhaust_floor": "0.4",
        "refill_frac": "0.5",
        "refill_sec": "0",
    },
    "zones_plus": {
        "fvg_min_atr": "0.3",
        "pivot_k": "3",
        "cluster_radius_atr": "0.3",
        "cluster_min_pivots": "2",
        "fvg_ttl_min": "120",
        "cluster_ttl_min": "60",
        "heartbeat_sec": "300",
    },
    "oi": {
        "poll_sec": "30",
        "approach_pct": "0.15",
        "watch_min": "3",
        "oi_breakout_pct": "0.5",
        "liq_burst_min": "3",
        "liq_window_sec": "60",
        "watch_arm_limit": "3",
    },
    # ЛИМИТЫ БИРЖИ («Квотник»). Binance считает вес запросов в минуту и
    # частоту ордеров; потолки берём вдвое НИЖЕ реальных значений биржи
    # (~5000 веса/мин, ~100 ордеров/10с) — запас на неточности таблицы весов.
    "quota": {
        "weight_limit_min": "2000",   # вес/мин (потолок REQUEST_WEIGHT)
        "order_burst_10s": "50",      # ордеров за 10 секунд
        "throttle_frac": "0.75",      # доля пула, при которой гасим фоновые опросы
        # РЕЗЕРВ ДИЛЕРА (12.10.2026): доля весового потолка под дилера
        # (ордера/стопы/баланс). Фон гасится на throttle_frac СВОЕГО пула
        # (потолок × (1-резерв)), поэтому дилеру всегда остаётся резерв и
        # троттлинг фона его не задевает. 0.25 = четверть потолка.
        "reserve_frac": "0.25",
    },
    "conductivity": {
        "panic_weight": "0.45",
        "stubborn_weight": "0.35",
        "impulse_weight": "0.20",
        "stubborn_oi_pct": "0.5",
        "smooth_half_sec": "5",
        "publish_interval": "2",
        "storm_liq_min": "0.5",
        "storm_oi_min": "0.3",
        # УЗЕЛ 8 (ТЗ утверждено): окна и сердцебиение вынесены из тел методов.
        # Окно ликвидаций — НЕ здесь: единый источник правды [oi] liq_window_sec.
        "price_hist_sec": "300",     # история OI/цены для упрямства, сек
        "flow_ttl_sec": "300",       # свежесть карточки состояния Потока, сек
        "liq_burst_multiplier": "2.0",  # был зашитый ×2 к норме лавины
        "min_span_sec": "2",         # был «2с — полигон»: минимальный размах меток
        "heartbeat_sec": "300",      # сердцебиение DRAFT на тихом рынке, сек
        # СВЯЗЬ С ПОЛОТНОМ (утверждено):
        # k_ctx = 1 − ctx_strength·(d·G) — фрактальный контекст упрямства;
        # k_impulse = 1 + r·(1 + impulse_confirm·v) — кинетическая верификация.
        "ctx_strength": "0.5",       # сила контекста: ×1.5 против ветра / ×0.5 по ветру
        "impulse_confirm": "0.5",    # сила верификации: до ×1.5 / ×0.5 на r
        "senior_ttl_sec": "600",     # свежесть g_trend/acceleration старших этажей
    },
    "regression": {
        "window": "60",           # окно регрессии, свечей
        "atr_period": "14",       # период ATR (сила хода в ATR)
        "k_impulse_max": "2.0",   # потолок громкости тренда (k_impulse)
        # G-Channel АлексГровера (эталон 20.10): шаг границ α = 1/g_length.
        # Мёртвые min_lifetime/r2_min УДАЛЕНЫ (фильтры сняты ещё 27.10).
        "g_length": "100",
        # сердцебиение (сек): вынужденная публикация, чтобы ячейки Матрицы
        # не умерли по TTL (30 мин) на тихом рынке
        "heartbeat_sec": "300",
        # уровни полотна через запятую (сек); пусто = ВСЯ биржевая
        # палитра EXCHANGE_SPECTRUM 1м…3д (решение хозяина 20.10)
        "levels_sec": "",
        # Луч ДеМарка (prognosis_beam): геометрия по двум опорным пивотам
        "beam_pivot_k": "3",       # соседей с каждой стороны для пивота
        "beam_fresh_mult": "2.0",  # возраст пивота в «окнах ТФ» → уверенность 0
        "beam_weight_min": "0.5",  # нижний вес масштаба (4ч=1.0, 1м→0.5)
    },
    "tuner": {
        "min_deals": "10",
        "oc_min": "0.75",
        "trust_min": "0.1",
        "trust_max": "0.95",
        "trust_step": "0.05",
    },
    "efficiency": {
        "storm-breakout__trend-vector": "1.8",
        "storm-breakout__fvg-zone": "1.5",
        "storm-breakout__stop-cluster": "0.2",
        "storm-washout__trend-vector": "0.1",
        "storm-washout__stop-cluster": "2.0",
    },
    "storage": {
        "db_path": "data/neiros.db",
        "db_max_mb": "200",
        # ВИДЫ КАРТОЧЕК, НЕ ПИШУЩИЕСЯ В ЖУРНАЛ (хозяин, 19.10): рутинные
        # снимки среды (flow-state — штамп раз в минуту) архив не растят:
        # прогреву матрицы они не нужны (первая карточка придёт через
        # минуту после старта), учёба Тюнера живёт в векторе сценария.
        # Нужен разбор «что видела система» — убрать вид из списка,
        # и журнал включится. Барьеры/дивергенции пишутся всегда.
        "skip_card_kinds": "flow-state",
    },
    # ВЫКЛЮЧАТЕЛЬ МОДУЛЕЙ (27.10). Ключ — имя модуля из manifest.name,
    # значение "false" → модуль не поднимается (autopиск его пропускает).
    # Пустое значение или "true" → поднимается, как раньше. Нужен, чтобы
    # убрать недоделанный или некалиброванный модуль, не комментируя код:
    # раньше единственным способом было вырезать класс из файла.
    "modules": {
        # Нейро-прогноз выключен: прогнозы идут, но проверки нет (labeled=0,
        # trains=0, accuracy=0), поэтому калибровать остальные анализаторы на
        # таком шуме нельзя. Включается флагом, когда появится разметка.
        "neuro-predictor": "false",
    },
    "scenario": {
        "min_net_move_pct": "0.35",
        "net_margin": "1.4",
        "slippage_pct": "0.02",
        "stamp_min_interval": "20",
        "conf_calibration": "1.0",
        "bias_threshold": "0.08",
        "storm_conf_factor": "0.4",
        "storm_veto": "false",
        "risk_pct": "1.5",
        "max_lev": "1.0",
        "oi_weight": "0.02",
        "stop_atr_floor": "1.8",
        "horizon_hours": "24",
        "trailing_activation_atr": "1.5",
        "trailing_distance_atr": "1.0",
        "route_vol_scale_min": "4",
        "route_stop_frac": "0.6",
        "route_stop_noise_mult": "1.3",
        "route_wait_min": "120",
        "route_speed_pct_min": "0.005",
        "route_trail_activate_frac": "0.70",
        "route_trail_distance_frac": "0.20",
        "route_max_dist_pct": "0.9",
        "magnet_weight": "0.50",
        "news_weight": "0.30",
        "fuel_weight": "0.10",
        "trend_weight": "0.20",
        "tape_speed_weight": "0",
        "magnets_dist_k": "4.0",
    },
    "risk": {
        "max_margin_total_pct": "65",
        "margin_high_pct": "75",
        "confidence_high": "0.9",
        "liq_buffer": "1.3",
        "emergency_loss_pct": "6",
        "entry_dev_pct": "0.5",
    },
    "dealer": {
        "recovery_stop_pct": "2.0",
        "recon_tol_usdt": "2.0",
        "slip_max_pct": "0.3",
        "pending_enabled": "true",
        "shield_min_strength": "0.7",
        "shield_pad_pct": "0.02",
    },
    "leader": {
        "window_sec": "300",
        "publish_interval": "15",
        "diverge_gap": "0.6",
        "weight": "0.03",
        "penalty": "0.15",
        "stale_sec": "120",
    },
    "spoof": {
        "wall_multiple": "2.5",
        "vanish_pct": "0.6",
        "vanish_ticks_max": "1",
        "vanish_sec": "3",
        "approach_pct": "0.15",
        "fake_shelf_factor": "0.3",
        "fake_ttl_sec": "300",
        "history_sec": "120",
    },
    "rsi": {
        "period": "14",
        "upper": "70",
        "lower": "30",
        "weight": "0.071",
        "stale_sec": "120",
    },
    "psy": {
        # ПСИХОЗ СРЕДЫ (утверждено): окна жизни показаний.
        # Вязкость стакана и упрямство толпы — мгновенные состояния среды;
        # энтропия по тикам живёт чуть дольше (окно тиков).
        "stale_sec": "90",
        "entropy_window_sec": "45",
        # Порог массы плиты Зон+ (Шаг 3): ниже — рябь, а не цель
        "anomaly_min_mass": "0.35",
        # Ширина облака шума в ATR (Шаг 4): собственная дрожь Линз,
        # усиленная хаосом и жадностью толпы
        "cloud_atr_mult": "1.2",
        # Каскад траектории (Шаг 5): сила стратегической плиты, минимальная
        # ширина облака и шаг между лимитными уровнями перезахода
        "cascade_strat_min": "0.35",
        "cascade_bend_min": "1.0",
        "cascade_step_atr": "0.6",
    },
    "logging": {
        "level": "INFO",
        "rotate_mb": "5",
        "rotate_count": "1",
    },
    "matrix": {
        # СКЛАД-МАТРИЦА (18.10.2026): ритм публикации среза на доску
        # значений и срок жизни ячейки в ОЗУ. История живёт в журнале
        # cards (Хранитель) — матрица хранит только НАСТОЯЩЕЕ и чистит
        # протухшее.
        "publish_interval": "2",
        "stale_sec": "1800",
    },
    "ui": {
        "enabled": "false",
    },
    "trading": {
        "start_balance": "350",
        "fee_pct": "0.05",
        # КОНТУР (строго два, утверждено хозяином):
        #   testnet → биржа-тестнет, ключи [keys_testnet] (фантики)
        #   live    → боевая биржа, ключи [keys_live] (деньги)
        # Третьих состояний нет: contour = shadow означал бы «биржа не
        # выбрана» — это третье состояние ядра, его быть не должно.
        "contour": "testnet",
        # РЕЖИМЫ ИСПОЛНЕНИЯ внутри контура (ровно два):
        #   real   — ордера идут на биржу этого контура
        #   shadow — сделка считается максимально близко к реальной,
        #            но ордер не открывается
        "testnet_mode": "real",
        "live_mode": "shadow",
        # АРМ-РУБИЛЬНИК: отвечает ТОЛЬКО за LIVE-контур. На testnet-реал
        # (тестирование робота в песочнице) НЕ влияет — иначе нельзя было бы
        # гонять робота на фантиках. Страховка от случайного боевого прогона.
        "live_enabled": "false",
    },
}


class KernelConfig:
    """Единая точка доступа к настройкам и ключам."""

    def __init__(self):
        self.root = ROOT
        self._values = {section: dict(kv) for section, kv in DEFAULTS.items()}
        self._log = logging.getLogger("NeirOS.CONFIG")

        parser = configparser.ConfigParser(inline_comment_prefixes=(";", "#"))
        cfg_path = self.root / "config" / "config.ini"
        if cfg_path.exists():
            parser.read(cfg_path, encoding="utf-8")
            for section in parser.sections():
                self._values.setdefault(section, {}).update(parser.items(section))
        else:
            self._log.warning("Файл настроек не найден: %s — работаю на значениях по умолчанию", cfg_path)

        self._keys = configparser.ConfigParser(inline_comment_prefixes=(";", "#"))
        keys_path = self.root / "config" / "keys.ini"
        if keys_path.exists():
            self._keys.read(keys_path, encoding="utf-8")

        # Рабочие папки существуют всегда
        (self.root / "data").mkdir(exist_ok=True)
        (self.root / "logs").mkdir(exist_ok=True)

    # ---------- настройки ----------

    def get_str(self, section: str, key: str, default: str = "") -> str:
        return str(self._values.get(section, {}).get(key, default))

    def get_int(self, section: str, key: str, default: int = 0) -> int:
        try:
            return int(float(self._values.get(section, {}).get(key, default)))
        except (TypeError, ValueError):
            self._log.warning("Настройка %s.%s некорректна — беру %s", section, key, default)
            return default

    def get_float(self, section: str, key: str, default: float = 0.0) -> float:
        try:
            return float(self._values.get(section, {}).get(key, default))
        except (TypeError, ValueError):
            self._log.warning("Настройка %s.%s некорректна — беру %s", section, key, default)
            return default

    def override(self, section: str, key: str, value) -> None:
        """Точечная правка значения (используется тестами)."""
        self._values.setdefault(section, {})[key] = str(value)

    def path(self, relative: str) -> Path:
        """Абсолютный путь к файлу/папке внутри проекта."""
        return self.root / Path(relative)

    # ---------- ключи сервисов ----------

    def key(self, section: str, option: str):
        """Ключ сервиса или None, если не заполнен. Кавычки и пробелы
        по краям срезаются — формат «без кавычек» соблюдаем за пользователя."""
        value = self._keys.get(section, option, fallback=None)
        if value is None:
            return None
        value = value.strip().strip('"').strip("'").strip()
        return value or None

    def news_key(self, option: str):
        """Новостной ключ: секция [keys_news], при отсутствии — старая [news].

        Новостные ключи публичные (средств не касаются), поэтому здесь
        ничего не блокируется — просто единое место чтения.
        """
        return self.key(self.NEWS_SECTION, option) or self.key("news", option)

    # ---------- часто используемые (короткие свойства) ----------

    @property
    def symbol(self) -> str:
        return self.get_str("market", "symbol", "DOGEUSDT")

    @property
    def base_tf_sec(self) -> int:
        return self.get_int("market", "base_tf_sec", 60)

    @property
    def senior_tf_sec(self) -> tuple:
        """Наши не-биржевые ступени (сек). Пусто = только биржевая палитра.

        Любое волокно вне списка достраивается лениво по запросу
        (Collector.get_ring) из младших, делящих нацело — заранее
        такие ступени не считаем и не храним."""
        raw = self.get_str("market", "senior_tf_sec", "")
        try:
            return tuple(int(x.strip()) for x in raw.split(",") if x.strip())
        except ValueError:
            self._log.warning("senior_tf_sec некорректен (%r) — беру пусто: "
                              "только биржевая палитра", raw)
            return ()

    @property
    def history_limit(self) -> int:
        return self.get_int("market", "history_limit", 4320)

    @property
    def rest_limit(self) -> int:
        return self.get_int("market", "rest_limit", 1500)

    @property
    def rest_pages(self) -> int:
        return max(1, self.get_int("market", "rest_pages", 3))

    @property
    def senior_rest_limit(self) -> int:
        return self.get_int("market", "senior_rest_limit", 200)

    @property
    def min_senior_history(self) -> int:
        return self.get_int("market", "min_senior_history", 200)

    @property
    def pyramid_depth(self) -> int:
        """Целевая глубина каждого уровня пирамиды (свечей)."""
        return self.get_int("market", "pyramid_depth", 300)

    @property
    def anchor_bars(self) -> int:
        """Сколько ЗАКРЫТЫХ свечей тянуть якорями с биржи по каждому ТФ (27.10).

        60 — по требованию хозяина. Даёт от 5 часов (на 5m) до 180 суток
        (на 3d) глубины сразу, без ожидания аптайма. Особенно важно для
        1d/3d: синтезом из минуток их не набрать — полотно 1m живёт 7 суток.

        +1 в запросе — не запас, а поправка на незакрытую свечу: Binance
        последним элементом всегда отдаёт УЖЕ НАЧАВШУЮСЯ свечу, а Сборщик
        её отбрасывает. Без +1 кольцо 1d получало 59 баров вместо 60.
        То есть заказываем 61, учитываем 60 — ровно как и договорились."""
        return max(1, self.get_int("market", "anchor_bars", 60)) + 1

    @property
    def archive_tf_sec(self) -> tuple:
        """Архивные ТФ: пишутся в базу для истории тюнера и перезапусков."""
        raw = self.get_str("market", "archive_tf_sec", "900,3600,14400")
        try:
            values = tuple(int(x.strip()) for x in raw.split(",") if x.strip())
            return values or (900, 3600, 14400)
        except ValueError:
            return (900, 3600, 14400)

    # ---------- линзы (поверхность) ----------

    @property
    def lens_scales_min(self) -> tuple:
        """Масштабы линз в минутах (полураспад). Плотная лестница от 0.5 мин
        (30с) до 1 недели со спектром ТФ Binance — утверждено 18.10.2026
        (эталон: modules/_labels.py LENS_SCALES_MIN). Тензор SURFACE берёт
        длину и шкалу отсюда — лестницу меняем одной строкой конфига."""
        raw = self.get_str("lenses", "scales_min",
                           DEFAULTS["lenses"]["scales_min"])
        default = (0.5, 1, 2, 3, 5, 8, 13, 21, 34, 55, 90, 144, 240)
        try:
            values = tuple(float(x.strip()) for x in raw.split(",") if x.strip())
            return values or default
        except ValueError:
            return default

    @property
    def lens_publish_interval(self) -> float:
        return max(0.5, self.get_float("lenses", "publish_interval", 1.0))

    @property
    def lens_half_life_factor(self) -> float:
        """Множитель полураспада линз (ручка точности, ведёт Тюнер).

        Полураспад линзы = её масштаб × этот коэффициент. Вилка 0.5..2.0
        (из _knobs.TUNER_KNOBS): короткая память резче и шумнее, длинная
        плавнее, но с отставанием. 1.0 — полураспад равен масштабу.
        """
        return max(0.5, min(2.0, self.get_float("lenses", "half_life_factor", 1.0)))

    # ---------- поток (CVD, лента, барьеры) ----------

    @property
    def flow_absorption_window_sec(self) -> int:
        return self.get_int("flow", "absorption_window_sec", 60)

    @property
    def flow_absorption_multiple(self) -> float:
        """Барьер: во сколько раз объём окна должен превысить обычный."""
        return self.get_float("flow", "absorption_multiple", 10)

    @property
    def flow_cvd_window_min(self) -> int:
        return self.get_int("flow", "cvd_window_min", 30)

    @property
    def flow_divergence_force_min(self) -> float:
        return self.get_float("flow", "divergence_force_min", 0.5)

    @property
    def flow_burst_multiple(self) -> float:
        """Всплеск ленты: скорость во столько раз выше обычной."""
        return self.get_float("flow", "burst_multiple", 5)

    @property
    def flow_barrier_ttl_min(self) -> int:
        return self.get_int("flow", "barrier_ttl_min", 15)

    @property
    def flow_state_ttl_min(self) -> int:
        return self.get_int("flow", "state_ttl_min", 5)

    @property
    def flow_divergence_ttl_min(self) -> int:
        return self.get_int("flow", "divergence_ttl_min", 10)

    @property
    def flow_eval_every_sec(self) -> float:
        """Как часто осматриваем поток, сек."""
        return max(0.5, self.get_float("flow", "eval_every_sec", 2.0))

    @property
    def flow_state_publish_interval(self) -> float:
        """Как часто штампуется карточка «состояние потока», сек."""
        return max(1.0, self.get_float("flow", "state_publish_interval", 60.0))

    @property
    def flow_typical_half_life_sec(self) -> float:
        """Полураспад «обычного объёма окна» (EWMA), сек."""
        return max(1.0, self.get_float("flow", "typical_half_life_sec", 900.0))

    @property
    def flow_speed_half_life_sec(self) -> float:
        """Полураспад обычной скорости ленты (EWMA), сек."""
        return max(1.0, self.get_float("flow", "speed_half_life_sec", 300.0))

    @property
    def flow_level_tol(self) -> float:
        """Толерантность уровня барьера: тот же барьер ±0.15% — не дублируем."""
        return max(0.0001, self.get_float("flow", "level_tol", 0.0015))

    # ---------- поле (картина, истощение полок) ----------

    @property
    def field_stamp_interval_sec(self) -> float:
        return max(1.0, self.get_float("field", "stamp_interval_sec", 30))

    @property
    def field_zone_ttl_min(self) -> float:
        """Срок жизни зонных карточек (SR и барьеры), минут."""
        return self.get_float("field", "zone_ttl_min", 15)

    @property
    def field_touch_cooldown_sec(self) -> float:
        """Антидребезг: одно касание полки не чаще, сек."""
        return max(0.0, self.get_float("field", "touch_cooldown_sec", 15))

    @property
    def field_exhaust_step(self) -> float:
        return min(0.9, max(0.0, self.get_float("field", "exhaust_step", 0.2)))

    @property
    def field_exhaust_floor(self) -> float:
        return min(1.0, max(0.1, self.get_float("field", "exhaust_floor", 0.4)))

    @property
    def field_refill_frac(self) -> float:
        """Зарядка полки: доля срока жизни зоны тишины на −1 касание."""
        return min(1.0, max(0.05, self.get_float("field", "refill_frac", 0.5)))

    @property
    def field_refill_sec(self) -> float:
        """Явный интервал зарядки, сек (0 — считать от срока жизни зоны)."""
        return max(0.0, self.get_float("field", "refill_sec", 0))

    # ---------- КОНТУРЫ БИРЖИ (строго разделены) ----------

    # Секции ключей. Новая схема — [keys_testnet] и [keys_live] (по паре
    # api_key/api_secret). Старая схема [binance] с testnet_api_key/api_key
    # читается как запасной вариант, чтобы старые keys.ini продолжали работать.
    TESTNET_SECTION = "keys_testnet"
    LIVE_SECTION = "keys_live"
    NEWS_SECTION = "keys_news"

    TESTNET_BASE = "https://testnet.binancefuture.com"
    LIVE_BASE = "https://fapi.binance.com"

    # ХОСТЫ WEBSOCKET (проверено живой сетью 27.10).
    #
    # ВАЖНО. Прежний адрес wss://fstream.binance.com/ws/ — ЛОВУШКА: рукопожатие
    # проходит успешно, в журнале пишется «Поток kline подключён», ошибок нет,
    # но свечи не приходят НИКОГДА. На этом хосте живы только @trade и
    # @bookTicker; @kline, @depth, @aggTrade и !forceOrder@arr молчат как
    # мёртвые (проверено: recv() → TimeoutError на всех четырёх).
    # Из-за этого система годами строила свечи из тиков, считая это нормой.
    #
    # Рабочие адреса: fstream.binancefuture.com (боевой) и
    # stream.binancefuture.com (тестнет) отдают свечи, стакан, сделки,
    # пакет альтов и ликвидации.
    TESTNET_WS_BASE = "wss://stream.binancefuture.com/ws/"
    LIVE_WS_BASE = "wss://fstream.binancefuture.com/ws/"

    def _account_pair(self, section):
        """Пара ключ/секрет из секции keys.ini. None, если не заполнена."""
        key = self.key(section, "api_key")
        secret = self.key(section, "api_secret")
        if not key or not secret:
            return None
        return key, secret

    @property
    def testnet_account(self):
        """Аккаунт биржи-тестнета или None (без приоритетов и подмен)."""
        pair = self._account_pair(self.TESTNET_SECTION)
        if pair is None:
            # запасной путь: старый формат [binance] testnet_api_key/secret
            key = self.key("binance", "testnet_api_key")
            secret = self.key("binance", "testnet_api_secret")
            pair = (key, secret) if key and secret else None
        if pair is None:
            return None
        return {"key": pair[0], "secret": pair[1],
                "base": self.TESTNET_BASE, "testnet": True, "contour": "testnet"}

    @property
    def live_account(self):
        """Аккаунт БОЕВОЙ биржи или None (без приоритетов и подмен)."""
        pair = self._account_pair(self.LIVE_SECTION)
        if pair is None:
            # запасной путь: старый формат [binance] api_key/api_secret
            key = self.key("binance", "api_key")
            secret = self.key("binance", "api_secret")
            pair = (key, secret) if key and secret else None
        if pair is None:
            return None
        return {"key": pair[0], "secret": pair[1],
                "base": self.LIVE_BASE, "testnet": False, "contour": "live"}

    @property
    def ws_base(self) -> str:
        """Базовый адрес WebSocket ТЕКУЩЕГО КОНТУРА.

        WS обязан смотреть туда же, куда и REST, иначе данные берутся с
        одного рынка, а ордера уходят на другой. Контур — тот же, что и
        у ключей: ручка [trading] contour, без подмен и догадок.
        """
        return self.TESTNET_WS_BASE if self.contour == "testnet" else self.LIVE_WS_BASE

    @property
    def rest_base(self) -> str:
        """Базовый адрес REST ТЕКУЩЕГО КОНТУРА (тот же, что и WS)."""
        return self.TESTNET_BASE if self.contour == "testnet" else self.LIVE_BASE

    @property
    def contour(self) -> str:
        """АКТИВНЫЙ КОНТУР: testnet | live. Ровно два, третьего нет.

        Выбирается ЯВНО ручкой [trading] contour — никакого «тестнет
        приоритетнее боевых»: при live читаются ТОЛЬКО боевые ключи,
        при testnet — ТОЛЬКО тестнетовые. Ошибка контура не может молча
        увести торговлю на другую биржу.
        """
        raw = self.get_str("trading", "contour", "testnet").strip().lower()
        if raw in ("live", "боевой", "продакшн"):
            return "live"
        # всё остальное (testnet, тестнет, мусор, прежний shadow) → testnet:
        # безопасная биржа по умолчанию, а не третье состояние
        return "testnet"

    def _mode(self, key, default="shadow"):
        """Режим исполнения: real | shadow. Ровно два, мусор → тень."""
        raw = self.get_str("trading", key, default).strip().lower()
        return raw if raw in ("real", "shadow") else "shadow"

    @property
    def testnet_mode(self) -> str:
        """Режим TESTNET-контура: real (фантики) | shadow."""
        return self._mode("testnet_mode", "real")

    @property
    def live_mode(self) -> str:
        """Режим LIVE-контура: real (деньги) | shadow."""
        return self._mode("live_mode", "shadow")

    @property
    def live_enabled(self) -> bool:
        """АРМ-РУБИЛЬНИК: ТОЛЬКО боевой (LIVE) контур.

        При false боевой реал заблокирован и уходит в тень. На TESTNET-реал
        (тестовые фантики) флаг НЕ влияет — иначе нельзя было бы гонять
        робота в песочнице.
        """
        return self.get_str("trading", "live_enabled", "false").lower() in (
            "1", "true", "yes", "да")

    @property
    def mode(self) -> str:
        """Режим исполнения АКТИВНОГО контура: real | shadow."""
        mode = self.testnet_mode if self.contour == "testnet" else self.live_mode
        if self.contour == "live" and mode == "real" and not self.live_enabled:
            # боевой реал без взведённого рубильника — в тень (молчаливый
            # пониженный режим недопустим: решение хозяина увидит в логе)
            return "shadow"
        return mode

    @property
    def real_allowed(self) -> bool:
        """Итоговое разрешение реальной торговли активного контура."""
        return self.mode == "real"

    @property
    def trading_blocked_reason(self) -> str:
        """Человеческое объяснение, почему реала нет (пусто — можно реал)."""
        contour = self.contour
        mode = self.testnet_mode if contour == "testnet" else self.live_mode
        if mode != "real":
            return f"контур {contour} в режиме {mode} — торговля в тени"
        if contour == "live" and not self.live_enabled:
            return ("БОЕВОЙ КОНТУР: [trading] live_enabled = false — "
                    "реал заблокирован, работаем в тени")
        return ""

    @property
    def binance_account(self):
        """Аккаунт АКТИВНОГО контура или None (None → только тень).

        Обратная совместимость для Дилера и Риска: раньше здесь был
        автоматический выбор контура по наличию ключей.
        """
        if self.contour == "testnet":
            return self.testnet_account
        if self.contour == "live":
            return self.live_account
        return None

    # ---------- зоны+ (FVG, стоп-кластеры) ----------

    @property
    def zp_fvg_min_atr(self) -> float:
        """Минимальная ширина имбаланса в долях ATR."""
        return self.get_float("zones_plus", "fvg_min_atr", 0.3)

    @property
    def zp_pivot_k(self) -> int:
        return max(2, self.get_int("zones_plus", "pivot_k", 3))

    @property
    def zp_cluster_radius_atr(self) -> float:
        return self.get_float("zones_plus", "cluster_radius_atr", 0.3)

    @property
    def zp_cluster_min_pivots(self) -> int:
        return max(2, self.get_int("zones_plus", "cluster_min_pivots", 2))

    @property
    def zp_atr_period(self) -> int:
        """Окно ATR Зон+ (периоды свечей)."""
        return self.get_int("zones_plus", "zp_atr_period", 14)

    @property
    def zp_rows_keep(self) -> int:
        """Глубина памяти свечей на уровень Зон+."""
        return self.get_int("zones_plus", "zp_rows_keep", 400)

    @property
    def zp_mass_bucket(self) -> float:
        """Шаг публикации заполнения FVG (доля, 0..1)."""
        return self.get_float("zones_plus", "zp_mass_bucket", 0.25)

    @property
    def zp_cluster_pad(self) -> float:
        """Радиус кластера в долях cluster_radius_atr."""
        return self.get_float("zones_plus", "zp_cluster_pad", 0.5)

    @property
    def zp_dead_mass(self) -> float:
        """Заполнено почти всё — дальше не публикуем FVG."""
        return self.get_float("zones_plus", "zp_dead_mass", 0.05)

    @property
    def zp_fvg_ttl_min(self) -> int:
        """Срок жизни FVG-карточки в Поле, минут (авторский TTL зоны)."""
        return self.get_int("zones_plus", "fvg_ttl_min", 120)

    @property
    def zp_cluster_ttl_min(self) -> int:
        """Срок жизни стоп-кластера в Поле, минут (авторский TTL зоны)."""
        return self.get_int("zones_plus", "cluster_ttl_min", 60)

    @property
    def zp_heartbeat_sec(self) -> int:
        """Сердцебиение Зон+: зона не молчит дольше heartbeat_sec."""
        return max(30, self.get_int("zones_plus", "heartbeat_sec", 300))

    # ---------- SR-анализатор (зоны-преграды по пирамиде ТФ) ----------

    @property
    def sr_scales(self) -> tuple:
        """Масштабы пирамиды (сек), по которым SR ищет пивоты и зоны-преграды.

        По умолчанию биржевая палитра 1м/5м/15м/1ч/4ч. Мультимасштаб: зоны
        разных ТФ, совпавшие по цене, дают резонанс Нисона. Пусто = только
        базовый ТФ."""
        raw = self.get_str("sr", "scales", "60,300,900,3600,14400")
        try:
            values = tuple(int(x.strip()) for x in raw.split(",") if x.strip())
            return values or (self.base_tf_sec,)
        except ValueError:
            return (60, 300, 900, 3600, 14400)

    @property
    def sr_pivot_k(self) -> int:
        """Полуокно пивота: экстремум среди соседей на k в обе стороны."""
        return max(2, self.get_int("sr", "pivot_k", 3))

    @property
    def sr_cluster_atr(self) -> float:
        """Ширина зоны в долях ATR (склейка близких пивотов в преграду)."""
        return max(0.05, self.get_float("sr", "cluster_atr", 0.4))

    @property
    def sr_top_zones(self) -> int:
        return max(1, self.get_int("sr", "top_zones", 5))

    @property
    def sr_refresh_sec(self) -> int:
        """Карточка штампуется при изменении картины или раз в N сек."""
        return max(10, self.get_int("sr", "refresh_sec", 600))

    @property
    def sr_min_candles(self) -> int:
        return max(10, self.get_int("sr", "min_candles", 60))

    @property
    def sr_keep_candles(self) -> int:
        """Глубина буфера свечей на каждый масштаб (в памяти модуля)."""
        return max(50, self.get_int("sr", "keep_candles", 600))

    @property
    def strategy_tf_sec(self) -> int:
        """Стратегический уровень пирамиды для «Зоны+» и Регрессии.

        Раньше это был 3840с (64 минуты) — интервала, которого биржа не
        знает, поэтому кольцо 64м копилось месяцами синтезом. Теперь
        стратегическим назначен 4h: он есть в биржевом спектре, приходит
        настоящим и сразу полным. Порядок: точнее брать уровень, который
        реально живёт в кольцах."""
        return self.get_int("market", "strategy_tf_sec", 14400)

    # ---------- OI + Каскад ----------

    @property
    def oi_poll_sec(self) -> int:
        """Опрос открытого интереса, сек (медленная бухгалтерия)."""
        return max(10, self.get_int("oi", "poll_sec", 30))

    @property
    def oi_approach_pct(self) -> float:
        """На таком % расстоянии от полки часовой начинает следить."""
        return self.get_float("oi", "approach_pct", 0.15)

    @property
    def oi_watch_min(self) -> float:
        """Окно наблюдения за исходом касания, минут."""
        return self.get_float("oi", "watch_min", 3)

    @property
    def oi_breakout_pct(self) -> float:
        """ΔOI (%), начиная с которого считаем топливо пришедшим."""
        return self.get_float("oi", "oi_breakout_pct", 0.5)

    @property
    def oi_liq_burst_min(self) -> int:
        """Лавина: столько ликвидаций за минуту — уже не норма."""
        return max(1, self.get_int("oi", "liq_burst_min", 3))

    @property
    def oi_liq_window_sec(self) -> float:
        """Окно лавины: за сколько секунд считаем частоту ликвидаций."""
        return max(10.0, self.get_float("oi", "liq_window_sec", 60.0))

    @property
    def oi_watch_arm_limit(self) -> int:
        """Сколько полок держим «на часу» одновременно."""
        return max(1, self.get_int("oi", "watch_arm_limit", 3))

    # ---------- лимиты биржи (Квотник) ----------

    @property
    def quota_weight_limit(self) -> int:
        """Потолок веса запросов в минуту (ниже реального — с запасом)."""
        return max(10, self.get_int("quota", "weight_limit_min", 2000))

    @property
    def quota_order_burst(self) -> int:
        """Потолок частоты ордеров за 10 секунд (ниже реального — с запасом)."""
        return max(1, self.get_int("quota", "order_burst_10s", 50))

    @property
    def quota_throttle_frac(self) -> float:
        """Доля пула, при которой Квотник требует гасить фоновые опросы."""
        return min(0.99, max(0.1, self.get_float("quota", "throttle_frac", 0.75)))

    @property
    def quota_reserve_frac(self) -> float:
        """Доля весового потолка, зарезервированная под дилера (0..0.9).

        Фон гасится на throttle_frac СВОЕГО пула (потолок × (1-резерв)),
        поэтому дилеру всегда остаётся резерв на ордера/стопы/баланс."""
        return min(0.9, max(0.0, self.get_float("quota", "reserve_frac", 0.25)))

    # ---------- проводимость среды ----------

    @property
    def cond_panic_weight(self) -> float:
        return self.get_float("conductivity", "panic_weight", 0.45)

    @property
    def cond_stubborn_weight(self) -> float:
        return self.get_float("conductivity", "stubborn_weight", 0.35)

    @property
    def cond_impulse_weight(self) -> float:
        return self.get_float("conductivity", "impulse_weight", 0.20)

    @property
    def cond_stubborn_oi_pct(self) -> float:
        """ΔOI (%), при котором упрямство толпы считается полным."""
        return max(0.05, self.get_float("conductivity", "stubborn_oi_pct", 0.5))

    @property
    def cond_smooth_half_sec(self) -> float:
        """Плавность R: полураспад сглаживания, сек."""
        return max(1.0, self.get_float("conductivity", "smooth_half_sec", 5))

    @property
    def cond_publish_interval(self) -> float:
        return max(0.5, self.get_float("conductivity", "publish_interval", 2))

    # ---------- проводимость среды: окна и связи с полотном (узел 8) ----------

    @property
    def cond_price_hist_sec(self) -> float:
        """История OI/цены для упрямства и масштаб окна линзы, сек."""
        return max(10.0, self.get_float("conductivity", "price_hist_sec", 300.0))

    @property
    def cond_flow_ttl_sec(self) -> float:
        """Свежесть карточки состояния Потока (CVD), сек."""
        return max(1.0, self.get_float("conductivity", "flow_ttl_sec", 300.0))

    @property
    def cond_liq_burst_multiplier(self) -> float:
        """Множитель нормы лавины для паники (был зашитый ×2 в модуле)."""
        return max(1.0, self.get_float("conductivity", "liq_burst_multiplier", 2.0))

    @property
    def cond_min_span_sec(self) -> float:
        """Минимальный размах меток OI/цены, чтобы ΔOI считался
        (боевой опрос раз в 30с — короче порога не бывает)."""
        return max(0.0, self.get_float("conductivity", "min_span_sec", 2.0))

    @property
    def cond_heartbeat_sec(self) -> int:
        """Сердцебиение DRAFT: вынужденная карточка, чтобы ячейки Матрицы
        не умерли по TTL на тихом рынке, сек."""
        return max(10, self.get_int("conductivity", "heartbeat_sec", 300))

    @property
    def cond_ctx_strength(self) -> float:
        """Сила фрактального контекста упрямства: k_ctx = 1 − ctx·(d·G).
        0.5 → ×1.5 против ветра старших, ×0.5 по ветру, ×1 без ветра."""
        return min(0.95, max(0.0, self.get_float("conductivity", "ctx_strength", 0.5)))

    @property
    def cond_impulse_confirm(self) -> float:
        """Сила кинетической верификации импульса: 1 + confirm·v
        (v = перекос ленты × ускорение старших этажей)."""
        return min(1.0, max(0.0, self.get_float("conductivity", "impulse_confirm", 0.5)))

    @property
    def cond_senior_ttl_sec(self) -> float:
        """Свежесть чисел старших этажей полотна (g_trend/acceleration), сек.
        Дефолт 600 = 2× сердцебиения Регрессии (300)."""
        return max(30.0, self.get_float("conductivity", "senior_ttl_sec", 600.0))

    # ---------- сценарист ----------

    @property
    def scenario_min_net_move_pct(self) -> float:
        """Минимальная ЧИСТАЯ прибыль сценария (% хода цены после издержек)."""
        return self.get_float("scenario", "min_net_move_pct", 0.10)

    @property
    def scenario_stop_atr_floor(self) -> float:
        """Пол стопа в ATR: стоп ближе этого — внутри минутного шума,
        выбивается раньше, чем прогноз успевает сработать."""
        return max(0.5, self.get_float("scenario", "stop_atr_floor", 1.5))

    @property
    def scenario_horizon_hours(self) -> float:
        """Гигиенический потолок жизни сценария на шине (часы). Фактическая
        смерть — исполнение мишени, опровержение стопом или отзыв автора."""
        return max(0.5, self.get_float("scenario", "horizon_hours", 24.0))

    # ---------- МАРШРУТ (ступень 1): цель / поводок / сопровождение ----------

    @property
    def scenario_route_vol_scale_min(self) -> float:
        """Масштаб Линз (минуты) для «дрожи маршрута» — ближайший доступный."""
        return max(1.0, self.get_float("scenario", "route_vol_scale_min", 60.0))

    @property
    def scenario_route_stop_frac(self) -> float:
        """Потолок стопа — доля пути до цели (RR не хуже 1/frac)."""
        return max(0.05, self.get_float("scenario", "route_stop_frac", 0.5))

    @property
    def scenario_route_stop_noise_mult(self) -> float:
        """Пол стопа — сколько «дрожей ожидания» (σ×√T) обязан пережить."""
        return max(0.05, self.get_float("scenario",
                                        "route_stop_noise_mult", 1.5))

    @property
    def scenario_route_wait_min(self) -> float:
        """Сколько сценарий готов ждать цель (минут) — база дрожи."""
        return max(5.0, self.get_float("scenario", "route_wait_min", 120.0))

    @property
    def scenario_route_speed_pct_min(self) -> float:
        """Скорость созревания маршрута (% цены в минуту) — оценка времени
        пути по фактам прошлых сделок (Тюнер калибрует по снимкам)."""
        return max(0.0005, self.get_float("scenario",
                                          "route_speed_pct_min", 0.005))

    @property
    def scenario_route_trail_activate_frac(self) -> float:
        """Активация сопровождения — доля пути до цели."""
        return max(0.05, self.get_float("scenario",
                                        "route_trail_activate_frac", 0.35))

    @property
    def scenario_route_trail_distance_frac(self) -> float:
        """Шаг сопровождения — доля пути до цели."""
        return max(0.02, self.get_float("scenario",
                                        "route_trail_distance_frac", 0.20))

    @property
    def scenario_route_max_dist_pct(self) -> float:
        """Цели дальше этого (% цены) — маршрут неразумен, не берём."""
        return max(0.2, self.get_float("scenario", "route_max_dist_pct", 2.0))

    @property
    def scenario_net_margin(self) -> float:
        """Запас чистого хода над издержками (фильтр хода = издержки × K)."""
        return max(0.5, self.get_float("scenario", "net_margin", 1.4))

    @property
    def scenario_magnet_weight(self) -> float:
        """Вес тяги зон (магнитов) в направлении."""
        return max(0.0, self.get_float("scenario", "magnet_weight", 0.50))

    @property
    def scenario_news_weight(self) -> float:
        """Вес настроения новостей в направлении."""
        return max(0.0, self.get_float("scenario", "news_weight", 0.30))

    @property
    def scenario_fuel_weight(self) -> float:
        """Вес топлива потока в направлении (информации мало — тихий)."""
        return max(0.0, self.get_float("scenario", "fuel_weight", 0.10))

    @property
    def scenario_trend_weight(self) -> float:
        """Вес живой карточки тренда в направлении."""
        return max(0.0, self.get_float("scenario", "trend_weight", 0.20))

    def lens_layer_weight(self, param: str, minutes: int) -> float:
        """Вес ПОСЛОЙНОГО поля пирамиды: lens_{param}_{N}m.

        Плоская ручка `scenario_lens_trend_weight` УДАЛЕНА вместе с костылём
        `_lens_trend()`: один вес на среднее по шкале гасил дивергенцию
        масштабов (мощнейший признак разворота усреднялся в ноль). Теперь
        у каждого масштаба и параметра своя ручка, и её калибрует Тюнер.
        """
        return max(-0.25, min(0.25, self.get_float(
            "lenses", f"lens_{param}_{int(minutes)}m_weight", 0.0)))

    @property
    def scenario_tape_speed_weight(self) -> float:
        """Вес скорости ленты (со знаком; на 1–2 ч данные за малый минус)."""
        return self.get_float("scenario", "tape_speed_weight", -0.05)

    @property
    def scenario_magnets_dist_k(self) -> float:
        """Полураспад голоса зоны по расстоянию (в ATR): w = s/(1+d/K)."""
        return max(0.5, self.get_float("scenario", "magnets_dist_k", 4.0))

    @property
    def scenario_trailing_activation_atr(self) -> float:
        """Активация трейлинга (×ATR): ход от входа, после которого
        стоп начинает подтягиваться за ценой."""
        return max(0.5, self.get_float("scenario",
                                       "trailing_activation_atr", 1.5))

    @property
    def scenario_trailing_distance_atr(self) -> float:
        """Дистанция трейлинга (×ATR): стоп ведёт цену на таком отступе."""
        return max(0.2, self.get_float("scenario",
                                       "trailing_distance_atr", 1.0))

    @property
    def dealer_pending_enabled(self) -> bool:
        """Отложенный вход: сделка открывается при возврате цены в зону
        входа сценария, а не немедленно (в хвост импульса)."""
        return self.get_str("dealer", "pending_enabled", "true").lower() in (
            "1", "true", "yes", "да")

    @property
    def scenario_slippage_pct(self) -> float:
        return self.get_float("scenario", "slippage_pct", 0.02)

    @property
    def scenario_stamp_min_interval(self) -> int:
        return self.get_int("scenario", "stamp_min_interval", 20)

    @property
    def scenario_conf_calibration(self) -> float:
        return self.get_float("scenario", "conf_calibration", 1.0)

    # ---------- риск-менеджер ----------

    @property
    def risk_max_margin_total_pct(self) -> float:
        """Занятость маржи всеми сделками, % баланса."""
        return self.get_float("risk", "max_margin_total_pct", 65)

    @property
    def risk_margin_high_pct(self) -> float:
        """То же при высокой уверенности сценария."""
        return self.get_float("risk", "margin_high_pct", 75)

    @property
    def risk_confidence_high(self) -> float:
        return self.get_float("risk", "confidence_high", 0.9)

    @property
    def risk_liq_buffer(self) -> float:
        """Ликвидация должна быть дальше стопа в столько раз."""
        return self.get_float("risk", "liq_buffer", 1.3)

    @property
    def risk_emergency_loss_pct(self) -> float:
        return self.get_float("risk", "emergency_loss_pct", 6)

    @property
    def dealer_recovery_stop_pct(self) -> float:
        """Стоп для позиций, найденных после перезапуска (сценарий неизвестен)."""
        return self.get_float("dealer", "recovery_stop_pct", 2.0)

    @property
    def dealer_recon_tol_usdt(self) -> float:
        """Допуск сверки баланса, USDT (комиссии/фандинг дают дрейф)."""
        return self.get_float("dealer", "recon_tol_usdt", 2.0)

    @property
    def dealer_slip_max_pct(self) -> float:
        """Порог проскальзывания по стакану: выше — реальный ордер в тень."""
        return self.get_float("dealer", "slip_max_pct", 0.3)

    @property
    def dealer_calm_press_pct(self) -> float:
        """Прижатие стопа в затишье: дистанция стопа от цены, % —
        'забрать пик перед разворотом' (умный трейлинг)."""
        return max(0.01, self.get_float("dealer", "calm_press_pct", 0.08))

    @property
    def risk_entry_dev_pct(self) -> float:
        """Вход сценария дальше этого от живой цены (%) — аннулируется."""
        return self.get_float("risk", "entry_dev_pct", 0.5)

    @property
    def risk_open_conf_threshold(self) -> float:
        """Порог уверенности для РЕАЛЬНОГО входа (тюнер двигает)."""
        return self.get_float("risk", "open_conf_threshold", 0.75)

    @property
    def risk_min_rr(self) -> float:
        """Минимальное соотношение риск/прибыль (ручка тюнера)."""
        return self.get_float("risk", "min_rr", 1.5)

    @property
    def risk_max_risk_pct(self) -> float:
        """Риск сделки (% баланса при стопе) выше этого — только тень."""
        return self.get_float("risk", "max_risk_pct", 2.0)

    @property
    def reanimate_enabled(self) -> bool:
        """Реаниматор (Этап 34): лестница реакции на внезапный разворот
        по открытым РЕАЛЬНЫМ сделкам (в плюс → безубыток → снятие)."""
        return self.get_str("risk", "reanimate_enabled", "true").lower() in (
            "1", "true", "yes", "да")

    @property
    def reanimate_lookback_sec(self) -> float:
        """Окно скорости цены для сигнала внезапного разворота (сек)."""
        return self.get_float("risk", "reanimate_lookback_sec", 2.0)

    @property
    def reanimate_velocity_pct(self) -> float:
        """Ход цены ПРОТИВ позиции за окно (%), с которого разворот
        считается внезапным."""
        return self.get_float("risk", "reanimate_velocity_pct", 0.5)

    @property
    def reanimate_profit_pct(self) -> float:
        """Плюс позиции (% хода от входа), с которого реанимация
        фиксирует прибыль вместо ожидания стопа."""
        return self.get_float("risk", "reanimate_profit_pct", 0.3)

    @property
    def scenario_position_pct(self) -> float:
        """Объём сделки — параметр СЦЕНАРИЯ: доля баланса в марже (%)."""
        return self.get_float("scenario", "position_pct", 1.5)

    @property
    def lab_horizon_bars(self) -> float:
        """Окно идеала Лаборатории: свечей от входа (независимо от факта выхода)."""
        return self.get_float("lab", "horizon_bars", 60)

    @property
    def spoof_verdict_horizon_min(self) -> float:
        """Вердикт спуфинга: цена обязана пройти сквозь уровень за столько минут."""
        return self.get_float("spoof", "verdict_horizon_min", 5)

    @property
    def spoof_vanish_ticks_max(self) -> int:
        """Сито спуфинга (ручка тюнера): исчез за столько тиков = спуфинг."""
        return self.get_int("spoof", "vanish_ticks_max", 1)

    # ---------- TTE-календарь (Волна 3) ----------

    @property
    def tte_horizon_min(self) -> float:
        """Горизонт событий, попадающих в карточку (минут)."""
        return self.get_float("tte", "horizon_min", 1440)

    @property
    def tte_compress_start_min(self) -> float:
        """С этого расстояния до события начинается сжатие (минут)."""
        return self.get_float("tte", "compress_start_min", 120)

    @property
    def tte_max_pressure(self) -> float:
        """Максимальное давление на уверенность у события (0.9 = ×0.1)."""
        return self.get_float("tte", "max_pressure", 0.9)

    @property
    def tte_poll_min(self) -> float:
        """Опрос источника календаря (минут)."""
        return self.get_float("tte", "poll_min", 30)

    @property
    def tte_default_event_time_utc(self) -> str:
        """Время событий по умолчанию (FRED даёт только дату)."""
        return self.get_str("tte", "default_event_time_utc", "12:30")

    @property
    def tte_high_keywords(self) -> str:
        """Имена релизов FRED, считающихся громкими (через запятую)."""
        return self.get_str(
            "tte", "high_keywords",
            "CPI,Employment Situation,Gross Domestic Product,"
            "Personal Income,PCE,Durable Goods,ISM")

    @property
    def tte_low_weight(self) -> float:
        """Громкость событий, не попавших в громкие (0..1)."""
        return self.get_float("tte", "low_weight", 0.2)

    @property
    def tte_shelf_damp(self) -> float:
        """Максимальное гашение веса полок Полем у события (0..1)."""
        return self.get_float("tte", "shelf_damp", 0.5)

    @property
    def fractal_selfsim_weight(self) -> float:
        """Вес факта самоподобия в направлении Сценариста."""
        return self.get_float("fractal", "selfsim_weight", 0.06)

    @property
    def fractal_selfsim_window(self) -> int:
        """Фракталы 2.0: точек младшего слоя в окне самоподобия."""
        return self.get_int("fractal", "selfsim_window", 60)

    @property
    def fractal_selfsim_senior_pts(self) -> int:
        """Фракталы 2.0: точек старшего слоя для сравнения форм."""
        return self.get_int("fractal", "selfsim_senior_pts", 30)

    @property
    def fractal_selfsim_min_corr(self) -> float:
        """Порог корреляции форм для «полного самоподобия» (0..1)."""
        return self.get_float("fractal", "selfsim_min_corr", 0.8)

    # ---------- Пружина (Этап 32) ----------

    @property
    def spring_window_bars(self) -> int:
        """Окно VWAP-якоря (минутных баров)."""
        return self.get_int("spring", "window_bars", 120)

    @property
    def spring_stretch_atr(self) -> float:
        """Растяжение от якоря (в ATR), с которого пружина «натянута»."""
        return self.get_float("spring", "stretch_atr_min", 1.0)

    @property
    def spring_oi_fall_pct(self) -> float:
        """Падение OI за окно (%), с которого топливо считается угасшим."""
        return self.get_float("spring", "oi_fall_pct", 0.2)

    @property
    def spring_oi_window_sec(self) -> float:
        """Окно оценки затухания OI (сек)."""
        return self.get_float("spring", "oi_window_sec", 600)

    @property
    def spring_weight(self) -> float:
        """Вес факта пружины в направлении Сценариста."""
        return self.get_float("spring", "weight", 0.08)

    @property
    def spring_publish_sec(self) -> float:
        """Период публикации карточки пружины (сек)."""
        return self.get_float("spring", "publish_sec", 20)

    # ---------- Новости 2.0 (Этап 31) ----------

    @property
    def news_rss_urls(self) -> str:
        """RSS-ленты второго голоса (без ключа), через запятую."""
        return self.get_str(
            "news", "rss_urls",
            "https://www.coindesk.com/arc/outboundfeeds/rss/,"
            " https://cointelegraph.com/rss")

    @property
    def news_rss_poll_sec(self) -> float:
        """Опрос RSS-лент (сек)."""
        return self.get_float("news", "rss_poll_sec", 300)

    @property
    def news_min_tone(self) -> float:
        """Новость без типа события слабее этого тона (0..1) — карточки нет."""
        return self.get_float("news", "min_tone", 0.05)

    @property
    def news_reaction_window_sec(self) -> float:
        """Окно реакции рынка на событие (сек) — Лаборатория сверяет расписку."""
        return self.get_float("news", "reaction_window_sec", 1800)

    @property
    def news_flat_pct(self) -> float:
        """Ход цены внутри этой полосы (%) = «нет реакции»."""
        return self.get_float("news", "flat_pct", 0.1)

    # ---------- Метакоррелятор (Волна 3) ----------

    @property
    def meta_weight(self) -> float:
        """Вес факта пакета в направлении (пакет с нами — плюс)."""
        return self.get_float("meta", "weight", 0.08)

    @property
    def meta_diverge_penalty(self) -> float:
        """Штраф уверенности при нашем одиночном движении против пакета."""
        return self.get_float("meta", "diverge_penalty", 0.12)

    @property
    def meta_pool(self) -> str:
        """Нити пакета Метакоррелятора (базовые тикеры, через запятую);
        наша нить — точка сравнения и в матрицу не входит (Этап 33:
        расширение пула конфигом, без кода)."""
        return self.get_str("meta", "pool", "BTC,ETH,SOL,PEPE")

    @property
    def meta_stale_sec(self) -> float:
        """Факт пакета старше этого — не учитывается."""
        return self.get_float("meta", "stale_sec", 60)

    @property
    def intuit_weight(self) -> float:
        """Вес факта аналогий Интуита в направлении (старт 0.10)."""
        return self.get_float("intuit", "weight", 0.10)

    @property
    def entropy_high_threshold(self) -> float:
        """Хаос выше этого (0..1) — давление на уверенность."""
        return self.get_float("entropy", "high_threshold", 0.7)

    @property
    def entropy_penalty(self) -> float:
        """Максимальный штраф уверенности при полном хаосе (S=1)."""
        return self.get_float("entropy", "penalty", 0.2)

    # ---------- Фракталы ТФ (Волна 3) ----------

    @property
    def fractal_window_bars(self) -> int:
        """Горизонт минутных баров для фрактальных расчётов (утв. 720 = 12ч)."""
        return max(100, self.get_int("fractal", "window_bars", 720))

    @property
    def fractal_heartbeat_sec(self) -> int:
        """Крайний срок публикации topological-resonance без изменений (сек)."""
        return self.get_int("fractal", "heartbeat_sec", 300)

    @property
    def fractal_hurst_band(self) -> float:
        """Полоса вокруг 0.5, внутри которой рынок = пила."""
        return self.get_float("fractal", "hurst_flat_band", 0.03)

    @property
    def fractal_node_mass(self) -> float:
        """Масса зоны старшего слоя для резонансного узла."""
        return self.get_float("fractal", "node_mass", 0.7)

    @property
    def fractal_node_prox_pct(self) -> float:
        """Цена ближе этого (% к цене) к узлу — точка излома."""
        return self.get_float("fractal", "node_prox_pct", 0.10)

    @property
    def fractal_node_ttl_factor(self) -> float:
        """Сжатие TTL сценария у резонансного узла (0.5 = вдвое)."""
        return self.get_float("fractal", "node_ttl_factor", 0.5)

    @property
    def fractal_hurst_penalty(self) -> float:
        """Штраф уверенности пробойных сценариев в пиле (H ≈ 0.5)."""
        return self.get_float("fractal", "hurst_penalty", 0.15)

    @property
    def fractal_bias_weight(self) -> float:
        """Вес факта инерции старших слоёв в направлении."""
        return self.get_float("fractal", "bias_weight", 0.08)

    @property
    def fractal_publish_sec(self) -> float:
        """Пульс карточки topological-resonance (сек)."""
        return self.get_float("fractal", "publish_sec", 60.0)

    # ---------- Песочница срезов (Волна 3/4) ----------

    @property
    def sandbox_enabled(self) -> bool:
        """Песочница включена (черновые срезы прогоняются по истории)."""
        return self.get_str("sandbox", "enabled", "false").lower() in (
            "1", "true", "yes", "да")

    @property
    def sandbox_slices(self) -> str:
        """Имена встроенных черновых срезов через запятую (не формулы)."""
        return self.get_str("sandbox", "slices", "")

    @property
    def sandbox_sample_n(self) -> int:
        """Сколько последних снимков брать для прогона."""
        return max(10, self.get_int("sandbox", "sample_n", 1000))

    @property
    def scenario_max_lev(self) -> float:
        """Потолок плеча. ДО калибровки тюнером = 1x (утверждено)."""
        return self.get_float("scenario", "max_lev", 1.0)

    @property
    def scenario_oi_weight(self) -> float:
        """Вес прямого OI-фактора (ΔOI) в формуле направления."""
        return self.get_float("scenario", "oi_weight", 0.10)

    @property
    def rsi_period(self) -> int:
        return self.get_int("rsi", "period", 14)

    @property
    def rsi_upper(self) -> float:
        return self.get_float("rsi", "upper", 70)

    @property
    def rsi_lower(self) -> float:
        return self.get_float("rsi", "lower", 30)

    @property
    def rsi_weight(self) -> float:
        """Вес давления RSI в формуле направления Сценариста."""
        return self.get_float("rsi", "weight", 0.071)

    @property
    def dealer_shield_min_strength(self) -> float:
        """Минимальная масса зоны, чтобы работать щитом для стопа."""
        return min(1.0, max(0.0,
                            self.get_float("dealer", "shield_min_strength", 0.7)))

    @property
    def dealer_shield_pad_pct(self) -> float:
        """Запас за массу щита, доля цены (стоп прячется за зоной)."""
        return max(0.0, self.get_float("dealer", "shield_pad_pct", 0.02)) / 100.0

    @property
    def rsi_stale_sec(self) -> float:
        """Факт RSI старше этого — не учитывается."""
        return self.get_float("rsi", "stale_sec", 120)

    @property
    def psy_stale_sec(self) -> float:
        """Сколько живут показания Психоза Среды (секунд).

        Вязкость стакана и упрямство толпы — мгновенные состояния: за
        минуту-две среда меняется целиком, и старое показание вреднее
        отсутствующего.
        """
        return max(5.0, self.get_float("psy", "stale_sec", 90.0))

    @property
    def entropy_window_ticks(self) -> float:
        """Окно жизни показания энтропии (секунд).

        Энтропия считается по скользящему окну тиков; её «свежесть»
        для Сценариста — это время с последней публикации карточки.
        """
        return max(5.0, self.get_float("psy", "entropy_window_sec", 45.0))

    @property
    def psy_anomaly_min_mass(self) -> float:
        """Минимальная масса плиты, чтобы зона Зон+ считалась целью.

        Ниже этого порога аномалия не «плита ликвидности», а рябь: цель
        должна быть настоящей координатой, а не ближайшей точкой.
        """
        return max(0.0, self.get_float("psy", "anomaly_min_mass", 0.35))

    @property
    def psy_cloud_atr_mult(self) -> float:
        """Ширина облака шума в ATR вокруг вектора маршрута.

        Облако = собственная дрожь Линз, усиленная хаосом (psy_chaos) и
        жадностью толпы. Стоп живёт ЗА его внешней границей.
        """
        return max(0.1, self.get_float("psy", "cloud_atr_mult", 1.2))

    @property
    def psy_cascade_strat_min(self) -> float:
        """Сила стратегической плиты (|score| регрессии 4ч) для каскада."""
        return max(0.0, self.get_float("psy", "cascade_strat_min", 0.35))

    @property
    def psy_cascade_bend_min(self) -> float:
        """Минимальная ширина облака (в ATR) для многоразового каскада."""
        return max(0.0, self.get_float("psy", "cascade_bend_min", 1.0))

    @property
    def psy_cascade_step_atr(self) -> float:
        """Шаг между уровнями каскада, в ATR."""
        return max(0.1, self.get_float("psy", "cascade_step_atr", 0.6))

    @property
    def box_size(self) -> int:
        return self.get_int("core", "box_size", 200)

    @property
    def health_interval(self) -> float:
        return self.get_float("core", "health_interval", 30)

    @property
    def init_timeout(self) -> float:
        return self.get_float("core", "init_timeout", 30)

    @property
    def stop_timeout(self) -> float:
        return self.get_float("core", "stop_timeout", 10)

    @property
    def tick_timeout(self) -> float:
        return self.get_float("core", "tick_timeout", 10)

    @property
    def envelope_alarm_sec(self) -> float:
        return self.get_float("core", "envelope_alarm_sec", 15)

    @property
    def envelope_defer_max(self) -> int:
        """Сколько личных конвертов максимум ждут открытия ящика на адресата."""
        return max(1, self.get_int("core", "envelope_defer_max", 200))

    @property
    def envelope_defer_ttl_sec(self) -> float:
        """Сколько секунд конверт может ждать открытия ящика, потом —
        окончательная потеря (с записью в журнал и в здоровье)."""
        return max(1.0, self.get_float("core", "envelope_defer_ttl_sec", 300))

    @property
    def max_trades(self) -> int:
        """Лимит сделок в истории Потока (deque) — ручка [core]."""
        return max(100, self.get_int("core", "max_trades", 20000))

    @property
    def db_path(self) -> Path:
        return self.path(self.get_str("storage", "db_path", "data/neiros.db"))

    @property
    def db_max_mb(self) -> int:
        return self.get_int("storage", "db_max_mb", 200)

    def module_enabled(self, name: str) -> bool:
        """Включён ли модуль флагом [modules] <имя> (27.10).

        Правило: ключа нет или значение не "false" — модуль включён (как
        раньше). Явное "false" — выключен. Так добавление нового модуля не
        требует правки конфига, а выключение не требует правки кода."""
        raw = self.get_str("modules", str(name), "true")
        return str(raw).strip().lower() not in ("false", "off", "no", "нет", "0")

    # Тик-архив (tick_archive_*) УДАЛЁН 27.10 вместе с таблицей
    # raw_trades и методом Keeper.get_trades. Потребителей не было, а цена
    # вопроса — ~150–190 МБ сырых сделок в сутки при лимите базы 200 МБ.
    # Подминутная точность, если понадобится, покупается секундными
    # барами OHLCV (~2–4 МБ/сутки) — отдельным этапом.

    @property
    def log_level(self) -> str:
        return self.get_str("logging", "level", "INFO")

    @property
    def rotate_mb(self) -> int:
        return self.get_int("logging", "rotate_mb", 5)

    @property
    def rotate_count(self) -> int:
        return self.get_int("logging", "rotate_count", 5)

    @property
    def ui_enabled(self) -> bool:
        """Экран по умолчанию ВЫКЛЮЧЕН: на сервере без Qt модуль падал бы
        на старте. На рабочей машине с окном — включить в config.ini."""
        return self.get_str("ui", "enabled", "false").lower() in (
            "1", "true", "yes", "да")

    @property
    def start_balance(self) -> float:
        return self.get_float("trading", "start_balance", 100.0)

    @property
    def max_margin_pct(self) -> float:
        return self.get_float("trading", "max_margin_pct", 50)

    @property
    def fee_pct(self) -> float:
        return self.get_float("trading", "fee_pct", 0.05)

    # NB: live_enabled живёт в блоке КОНТУРОВ БИРЖИ выше — он отвечает
    # ТОЛЬКО за боевой контур и потому стоит рядом с выбором контура.

    @property
    def tuner_min_deals(self) -> int:
        """Минимум выборки для КАЛИБРОВКИ и ОТЧЁТА (не для доверия)."""
        return self.get_int("tuner", "min_deals", 10)

    @property
    def tuner_trust_min(self) -> float:
        """Нижняя граница доверия (утверждено: 0.1 — данные не отключаются)."""
        return self.get_float("tuner", "trust_min", 0.1)

    @property
    def tuner_trust_max(self) -> float:
        """Верхняя граница доверия (утверждено: 0.95 — проверенный громче)."""
        return self.get_float("tuner", "trust_max", 0.95)

    @property
    def tuner_weight_real(self) -> float:
        """Вес исхода РЕАЛЬНОЙ сделки в калибровке Тюнера (факт)."""
        return max(0.1, min(1.0, self.get_float("tuner", "weight_real", 1.0)))

    @property
    def tuner_weight_shadow(self) -> float:
        """Вес исхода ТЕНЕВОЙ сделки (почти факт, ручка)."""
        return max(0.1, min(1.0, self.get_float("tuner", "weight_shadow", 0.8)))

    @property
    def tuner_weight_lab(self) -> float:
        """Вес ЛАБОРАТОРНОГО потока (идеальные настройки снимка —
        контрфакт; по слову хозяина лаба влияет тише факта).

        Поток ОДИН: автономные лабораторные снимки не делятся на реал и
        тень — они не знают о сделках Дилера вовсе.
        """
        return max(0.1, min(1.0,
                            self.get_float("tuner", "weight_lab", 0.5)))

    @property
    def tuner_trust_step(self) -> float:
        """Базовый шаг доверия за один пересчёт (адаптируется от эффективности)."""
        return self.get_float("tuner", "trust_step", 0.05)

    @property
    def cond_storm_liq_min(self) -> float:
        """Порог «ликвидации зашкаливают» для фаз шторма (доля лавины)."""
        return self.get_float("conductivity", "storm_liq_min", 0.5)

    @property
    def cond_storm_oi_min(self) -> float:
        """Порог уверенного движения ΔOI для фаз шторма (в нормах stubborn_oi)."""
        return self.get_float("conductivity", "storm_oi_min", 0.3)

    @property
    def tuner_oc_min(self) -> float:
        """Нижняя граница порога реала (решение хозяина: 0.75 — в тень)."""
        return max(0.0, min(0.99, self.get_float("tuner", "oc_min", 0.75)))

    @property
    def regression_window(self) -> int:
        """Окно регрессии (число свечей в канале)."""
        return max(20, self.get_int("regression", "window", 60))

    @property
    def regression_atr_period(self) -> int:
        """Период ATR для ширины канала."""
        return max(5, self.get_int("regression", "atr_period", 14))

    @property
    def regression_k_impulse_max(self) -> float:
        """Потолок громкости тренда (k_impulse), фазы шторма."""
        return max(1.0, self.get_float("regression", "k_impulse_max", 2.0))

    @property
    def regression_g_length(self) -> int:
        """Длина G-Channel: шаг затухания границ α = 1/длина."""
        return max(10, self.get_int("regression", "g_length", 100))

    @property
    def regression_heartbeat_sec(self) -> float:
        """Сердцебиение Регрессии: вынужденная публикация каждые N сек,
        чтобы полки Матрицы не протухали на тихом рынке (TTL 30 мин)."""
        return max(30.0, self.get_float("regression", "heartbeat_sec", 300.0))

    @property
    def regression_levels_sec(self) -> list:
        """Уровни Регрессии (сек) через запятую. Пусто → модуль сам берёт
        биржевую палитру (ядро не импортирует модули рынка — только текст)."""
        raw = self.get_str("regression", "levels_sec", "")
        out, seen = [], set()
        for part in raw.replace(";", ",").split(","):
            part = part.strip()
            if not part:
                continue
            try:
                v = int(float(part))
            except ValueError:
                continue
            if v > 0 and v not in seen:
                seen.add(v)
                out.append(v)
        return out

    @property
    def regression_beam_pivot_k(self) -> int:
        """Соседей с каждой стороны для подтверждения пивота ДеМарка."""
        return max(1, self.get_int("regression", "beam_pivot_k", 3))

    @property
    def regression_beam_fresh_mult(self) -> float:
        """Возраст пивота в «окнах ТФ», при котором свежесть → 0."""
        return max(0.5, self.get_float("regression", "beam_fresh_mult", 2.0))

    @property
    def regression_beam_weight_min(self) -> float:
        """Нижний вес масштаба луча (стратег=1.0, база→этот вес)."""
        return max(0.0, min(0.99,
                            self.get_float("regression", "beam_weight_min", 0.5)))

    @property
    def scenario_bias_threshold(self) -> float:
        """Ниже этого направления нет — сценарий не штампуется."""
        return self.get_float("scenario", "bias_threshold", 0.08)

    @property
    def scenario_storm_conf_factor(self) -> float:
        """Насколько шторм (R=1) режет уверенность сценария: conf × (1−k×R)."""
        return self.get_float("scenario", "storm_conf_factor", 0.4)

    @property
    def scenario_storm_veto(self) -> bool:
        """Вето: шторм-вынос + сценарий ПО каскаду — не штампуется."""
        return self.get_str("scenario", "storm_veto", "true").lower() in (
            "1", "true", "yes", "да")

    @property
    def scenario_risk_pct(self) -> float:
        """Риск на сделку, % баланса (ручка тюнера: убытки — режет,
        плюс — растит, в пределах потолка)."""
        return self.get_float("scenario", "risk_pct", 1.5)

    @property
    def leader_window_sec(self) -> float:
        """Окно Поводыря (вожак BTC), сек."""
        return self.get_float("leader", "window_sec", 300)

    @property
    def leader_publish_interval(self) -> float:
        return self.get_float("leader", "publish_interval", 15)

    @property
    def leader_diverge_gap(self) -> float:
        """Порог силы углов (%/мин) для маркера расхождения."""
        return self.get_float("leader", "diverge_gap", 0.6)

    @property
    def leader_weight(self) -> float:
        """Вес факта вожака в формуле направления Сценариста."""
        return self.get_float("leader", "weight", 0.10)

    @property
    def leader_penalty(self) -> float:
        """Штраф уверенности при расхождении с вожаком."""
        return self.get_float("leader", "penalty", 0.15)

    @property
    def leader_stale_sec(self) -> float:
        """Факт вожака старше этого — не учитывается."""
        return self.get_float("leader", "stale_sec", 120)

    @property
    def spoof_wall_multiple(self) -> float:
        """Стена = уровень толще медианы в столько раз."""
        return self.get_float("spoof", "wall_multiple", 2.5)

    @property
    def spoof_vanish_pct(self) -> float:
        """Доля снятого объёма стены для вердикта «исчезла»."""
        return self.get_float("spoof", "vanish_pct", 0.6)

    @property
    def spoof_vanish_ticks_max(self) -> int:
        """Мгновенный триггер: исчез за столько тиков стакана (100 мс)."""
        return self.get_int("spoof", "vanish_ticks_max", 1)

    @property
    def spoof_vanish_sec(self) -> float:
        """Внешний потолок времени исчезновения."""
        return self.get_float("spoof", "vanish_sec", 3)

    @property
    def spoof_approach_pct(self) -> float:
        """Подход цены к стене ближе этого — приманка активирована."""
        return self.get_float("spoof", "approach_pct", 0.15)

    @property
    def spoof_fake_shelf_factor(self) -> float:
        """Множитель веса полки, помеченной фальшивой."""
        return self.get_float("spoof", "fake_shelf_factor", 0.3)

    @property
    def spoof_fake_ttl_sec(self) -> float:
        """Сколько секунд полка считается фальшивой."""
        return self.get_float("spoof", "fake_ttl_sec", 300)

    @property
    def spoof_history_sec(self) -> float:
        """Память о стенах, сек."""
        return self.get_float("spoof", "history_sec", 120)

    def efficiency_matrix(self, phase: str) -> dict:
        """Матрица селективной громкости: {kind: множитель} для фазы среды.
        Ключи config.ini [efficiency]: «<фаза>__<kind>». Неизвестная фаза
        или пустая секция — пустой словарь (потребители берут legacy-ручки)."""
        out = {}
        for key, val in self._values.get("efficiency", {}).items():
            if key.startswith(f"{phase}__"):
                try:
                    out[key.split("__", 1)[1]] = float(val)
                except (TypeError, ValueError):
                    continue
        return out

    def trust_directory(self) -> dict:
        """Справочник доверий из config.ini [tuner] (ключи trust__тип)."""
        out = {}
        for key, val in self._values.get("tuner", {}).items():
            if key.startswith("trust__"):
                try:
                    out[str(key.split("__", 1)[1])] = float(val)
                except (IndexError, ValueError, TypeError):
                    continue
        return out
