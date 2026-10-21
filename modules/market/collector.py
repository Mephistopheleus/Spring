"""Сборщик данных — единственный источник рыночных данных NeirOS-M.

Экономия запросов (утверждено): с биржи по потокам идут только
минутные свечи, сделки и стакан; все старшие таймфреймы
(5м, 15м, 30м, 1ч, 2ч, 4ч — список в config.ini) синтезируем сами.

Плотное полотно (утверждено): база — 1 минута, любой ТФ кратный ей
строится и отдаётся срезом. Поверхность держится на трёх опорах:
  1) минутная история из Кладовщика;
  2) постраничный REST-добор минутных (rest_pages × rest_limit);
  3) ЯКОРЯ по всему биржевому спектру — anchor_bars свечей на каждый ТФ,
     который биржа знает (1m 3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d 3d).
     Это единственный способ получить 1d/3d: синтезом из минуток суточное
     кольцо требует суток НЕПРЕРЕРЫВНОЙ работы, а 3d — троих суток.
  4) ступени пирамиды без интервала биржи (2м/8м/32м/512м) — синтез из
     младших; для них якоря не существует, и это штатно.

ЛЕНИВОЕ MATERIALIZATION (27.10). Анализатор зовёт Collector.get_ring(tf) —
а не лезет в self.senior.get(ms) и не получает молчаливый None. Кольца нет
→ оно достраивается синтезом ВВЕРХ из ближайшего младшего, делящего нацело,
и с этого момента наполняется само. Обратный синтез невозможен: внутри 4h
мы не знаем путь цены, поэтому 4h не разбирается на 1h без потери.

Стек: живые потоки (WS) → прогрев (база + REST + якоря) → синтез колец.

ПОТОКИ И КОНТУРЫ (27.10, исправлено). Раньше адрес был зашит:
wss://fstream.binance.com/ws/ — ЛОВУШКА: рукопожатие проходит, в журнале
пишется «Поток kline подключён», ошибок нет, а свечи не приходят НИКОГДА.
На этом хосте живы только @trade и @bookTicker; @kline, @depth, @aggTrade
и !forceOrder@arr молчат. Теперь адрес берётся из конфигурации по ручке
contour — вместе с REST, чтобы данные и ордера были на одном рынке:
    testnet → wss://stream.binancefuture.com  +  testnet.binancefuture.com
    live    → wss://fstream.binancefuture.com  +  fapi.binance.com

ПОТОКИ (WebSocket, без ключей):
  kline_1m  — МИНУТНЫЕ свечи, закрытые + текущая. Единственная нить свечей:
              по WS идёт ТОЛЬКО минутка, весь остальной объём синтезируется
              из неё. Весь спектр по WS был лишним усложнением (27.10).
  aggTrade  — сделки (последняя цена, дельта покупателей/продавцов)
              + страховка: если минутная нить замолчала, минутка собирается
              из сделок (приоритет у настоящей);
  depth20   — стакан (20 уровней, каждые 100 мс);
  btc/alts  — вожак и пакет Метакоррелятора.

Часы сверяются с биржей при старте: расхождение меток сдвигает границы окон.

Реконнект: экспоненциальная пауза 3→60 сек, сброс при успехе.
"""
import asyncio
import time

import aiohttp
import numpy as np
import orjson
import websockets

from core.module import Manifest, NeirModule
from modules._labels import (ALT_PRICE, BTC_LIQ, BTC_PRICE, CANDLES,
                             CANDLES_WARMUP, LIQUIDATIONS, ORDERBOOK, PRICE)

# Адреса берутся из конфигурации по АКТИВНОМУ КОНТУРУ — жёстко зашитых
# хостов здесь больше нет (KernelConfig.ws_base / KernelConfig.rest_base).
# Прежний WS_BASE "wss://fstream.binance.com/ws/" — ловушка: рукопожатие
# проходит, в журнале «Поток kline подключён», а свечей нет НИКОГДА.
# См. подробности в core/config.py, рядом с TESTNET_WS_BASE / LIVE_WS_BASE.

# Часы: метки свечей приходят с биржи, но границы окон и все таймауты
# считаются по локальному времени. Уход часов сдвигает разметку, поэтому
# расхождение меряется постоянно (CLOCK_CHECK_EVERY), а не только при
# старте, и допуск задан явно (CLOCK_TOLERANCE_MS).
CLOCK_TOLERANCE_MS = 2000
CLOCK_CHECK_EVERY = 300.0
ANCHOR_RETRY_EVERY = 30.0   # добивка пустых колец: раз в полминуты


class RingCanvas:
    """Кольцевое полотно свечей. get_last возвращает ровно n последних
    в правильном порядке — баг SWARM-полотна здесь не повторён."""

    __slots__ = ("size", "head", "ts", "o", "h", "l", "c", "v", "d")

    def __init__(self, size):
        self.size = int(size)
        self.head = 0
        self.ts = np.zeros(self.size, dtype=np.int64)
        self.o = np.zeros(self.size)
        self.h = np.zeros(self.size)
        self.l = np.zeros(self.size)
        self.c = np.zeros(self.size)
        self.v = np.zeros(self.size)
        self.d = np.zeros(self.size)

    def push(self, row):
        ts, o, hh, ll, cc, vv, dd = row
        idx = self.head % self.size
        self.ts[idx] = ts
        self.o[idx] = o
        self.h[idx] = hh
        self.l[idx] = ll
        self.c[idx] = cc
        self.v[idx] = vv
        self.d[idx] = dd
        self.head += 1

    def last_ts(self):
        return int(self.ts[(self.head - 1) % self.size]) if self.head else 0

    def get_last(self, n):
        if self.head == 0:
            return None
        n = max(1, min(int(n), self.head, self.size))
        idx = np.arange(self.head - n, self.head) % self.size
        return {"ts": self.ts[idx], "o": self.o[idx], "h": self.h[idx],
                "l": self.l[idx], "c": self.c[idx], "v": self.v[idx],
                "d": self.d[idx]}

    def all_rows(self):
        if self.head == 0:
            return []
        idx = np.arange(max(0, self.head - self.size), self.head) % self.size
        return [(int(self.ts[i]), float(self.o[i]), float(self.h[i]),
                 float(self.l[i]), float(self.c[i]), float(self.v[i]),
                 float(self.d[i])) for i in idx]

    def window_rows(self, win_start, need, base_ms):
        """Строки последнего окна из `need` баров, если оно полное.

        Окно берётся ПРЯМО ИЗ ПОЛОТНА, а не из отдельного накопителя (27.10).
        Так все старшие ТФ читают одно и то же место, и дырка в минутках
        видна сразу: `ts` окна обязаны идти подряд с шагом base_ms и
        начинаться ровно на границе. Нет — возвращаем None, и свеча не
        записывается (ложный бар из неполного окна хуже, чем его отсутствие).

        None также при need > size: такое окно в кольцо не помещается вовсе.
        """
        if self.head < need or need > self.size:
            return None
        idx = np.arange(self.head - need, self.head) % self.size
        ts = self.ts[idx]
        if int(ts[0]) != int(win_start):
            return None
        if need > 1 and not bool(np.all(np.diff(ts) == base_ms)):
            return None              # дырка в минутках внутри окна
        return [(int(self.ts[i]), float(self.o[i]), float(self.h[i]),
                 float(self.l[i]), float(self.c[i]), float(self.v[i]),
                 float(self.d[i])) for i in idx]

    def load_rows(self, rows):
        """Пересобрать полотно из списка строк (склейка истории при прогреве).

        Дубликаты по ts: ПОСЛЕДНЯЯ строка выигрывает. Это ровно то, что
        нужно биржевому волокну: якорь кладётся последним, поэтому его
        свеча вытесняет одноимённую из базы/синтеза (27.10). Раньше
        выигрывала ПЕРВАЯ — и биржа не могла поправить историю, собранную
        старым кодом."""
        self.head = 0
        self.ts[:] = 0
        by_ts = {}
        for row in rows:
            by_ts[int(row[0])] = row      # поздний перезаписывает ранний
        for row in sorted(by_ts.values(), key=lambda r: int(r[0])):
            self.push(row)


# СПЕКТР БИРЖИ (проверено живьём 27.10: 30 интервалов подряд).
# Ровно эти 13 Binance Futures отдаёт по REST и по WebSocket:
#   1m 3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d 3d
# Чего биржа НЕ знает (и никогда не отдаст): 2m, 4m, 6m, 8m, 10m, 12m,
# 20m, 3h, 16m, 32m, 64m, 128m, 256m, 512m, 24h. Ступени нашей пирамиды
# вида 2м/8м/32м — НАШИ, и это не поломка: их нельзя вырастить из
# аптайма быстро (512м требует 8.5 ч непрерывной работы на одну свечу),
# поэтому они строятся синтезом из младших. А вот 1d/3d синтезом не
# спасти — на 1d нужны сутки непрерывных минуток, — их берём якорями.
EXCHANGE_SPECTRUM = {
    60: "1m", 180: "3m", 300: "5m", 900: "15m", 1800: "30m",
    3600: "1h", 7200: "2h", 14400: "4h", 21600: "6h",
    28800: "8h", 43200: "12h", 86400: "1d", 259200: "3d",
}
# Глубина кольца. 200 баров — максимум, который просит Херст (HURST_RING),
# 60 — порог осмысленности R/S. Ниже 200 резать нельзя: get_last() обрежет
# молча, и анализатор получит вдвое меньше данных, не узнав об этом
# (та же тихая деградация, что была с мёртвым kline-потоком). Запас — на
# рост с запасом. По времени это 256 баров: 3.3 ч на 1m, 8.5 суток на 1h,
# 256 суток на 1d — но эти буферы просто не заполнятся целиком, и это
# нормально: кольцо циклическое, «растить» его незачем.
RING_SIZE = 256


def aggregate(rows):
    """Слить несколько младших свечей в одну старшую.

    ТОЛЬКО ВВЕРХ: из мелких в крупные. Обратно нельзя — внутри часа мы
    не знаем путь цены, поэтому 4h не разбирается на 1h без потери:
    восстановятся только O (по началу) и C (по концу), а H/L/V жили
    внутри. Отсюда правило синтеза: кольцо ТФ строится из кольца,
    которое МЕЛЬЧЕ и делит его нацело."""
    first = rows[0]
    last = rows[-1]
    return (int(first[0]), float(first[1]), max(r[2] for r in rows),
            min(r[3] for r in rows), float(last[4]),
            sum(r[5] for r in rows), sum(r[6] for r in rows))


class Collector(NeirModule):
    manifest = Manifest(name="collector", title="Сборщик данных (биржа)",
                        provides=("candles", "orderbook", "price"),
                        requires=("storage",),
                        priority=20, init_timeout=90.0, stop_timeout=10.0)

    async def on_start(self):
        self.symbol = self.kernel.cfg.symbol.upper()
        self.base_ms = self.kernel.cfg.base_tf_sec * 1000
        self.price = 0.0
        self.last_price_ts = 0.0
        self.last_depth_ts = 0.0
        self.live_candles = 0
        self.kline_candles = 0
        self.kline_alien = 0     # сколько сообщений пришло БЕЗ поля "k"
        self.clock_drift_ms = 0.0   # расхождение часов с биржей, мс
        self._clock_bad = False # часы за пределами допуска
        self._clock_fails = 0   # неудачных сверок подряд
        self.ticks = 0
        self.depth_updates = 0
        self._ws_ok = {"kline": False, "trades": False, "depth": False,
                       "force": False, "btc": False, "alts": False}
        self.liquidations = 0
        # Вожак (BTCUSDT): отдельный поток сделок для Поводыря (Этап 20)
        self.btc_price = 0.0
        self.btc_ticks = 0
        # Локальная сборка минутных свечей из сделок — страховка на случай,
        # когда kline-поток молчит (живой прогон 09.09: fstream kline не
        # отвечает из этой сети, trade/depth работают). Приоритет — у
        # официальной свечи kline; локальная публикуется, только если
        # kline за минуту так и не пришёл (дедуп в _absorb_closed).
        self._agg_win = None
        self._agg_row = None
        self._kline_last_ts = 0
        self._agg_note = 0
        self._agg_first_ms = 0.0      # когда включилась страховка
        self._agg_silent_said = False  # ERROR про мёртвый kline уже произнесён
        self._anchor_failed = []   # ТФ, чей якорь не получен (добиваем в фоне)
        self._refill_done = False
        self._topup_retries = 0   # фоновые попытки добрать минуты (06.10)

        base_size = self.kernel.cfg.history_limit + 200
        self.canvas = RingCanvas(base_size)                  # минутные
        # ПОВЕРХНОСТЬ (27.10, схема хозяина):
        #   прогрев (REST) — anchor_bars свечей по КАЖДОМУ интервалу биржи,
        #                     это точная точка отсчёта;
        #   работа (WS)    — ТОЛЬКО минутки, всё остальное синтезируется из них.
        # Биржевые кольца после якоря растут синтезом наравне с нашими:
        # синтез из минуток проверен — расхождений с биржей ноль (27.10).
        spec_ms = [tf * 1000 for tf in EXCHANGE_SPECTRUM
                   if tf > self.kernel.cfg.base_tf_sec]
        self.pyramid_ms = [int(tf) * 1000 for tf in self.kernel.cfg.senior_tf_sec]
        self.archive_ms = [int(tf) * 1000 for tf in self.kernel.cfg.archive_tf_sec]
        self.senior_ms = sorted(set(spec_ms + self.pyramid_ms + self.archive_ms))
        # Биржевые ТФ — только для добивки и для диагностики; наполняет их
        # синтез наравне со всеми.
        self._spec_ms = [tf for tf in self.senior_ms
                         if self._tf_interval_str(tf // 1000) is not None]
        size = max(RING_SIZE, self.kernel.cfg.pyramid_depth)
        self.senior = {tf: RingCanvas(size) for tf in self.senior_ms}
        # Кольца, достроенные по запросу анализатора (ленивоеmaterialization).
        self._materialized = set()
        self._mat_basis = {}
        self._no_anchor_logged = False
        # Сдвиг границ окон, выученный у биржи (см. _learn_align): пусто = все
        # интервалы начинаются от эпохи. У 3d сдвиг есть — 2 суток.
        self._align_shift = {}

        # СЕТЬ — ТОЛЬКО В ФОНЕ (06.10). Закрывает вопрос «подключения»:
        # раньше весь прогрев шёл синхронно в on_start, и в сетевом окне
        # (после прогрева соседнего процесса testnet троттлит IP) сумма
        # якорей + страниц + сверки часов переваливала за init_timeout=90с.
        # Ядро помечало модуль сбоем, а _drop_module_assets отменял ВСЕ
        # задачи модуля — включая фоновые добивки, которые успели бы
        # зарегистрироваться. Спасатели умирали ровно тогда, когда были
        # нужны. Теперь on_start только собирает местную структуру и
        # регистрирует ритмы; сеть идёт в _boot строгим порядком:
        #   якоря → выравнивание → БД → страницы → публикации прогрева →
        #   сверка часов → потоки WS.
        # Потоки открываются в САМОМ КОНЦЕ: живая минутка, пришедшая
        # раньше истории, вытеснила бы её дедупом (ts <= last_ts).
        # Подписчики прогрева (get_last + subscribe) получают батч раньше
        # первого живого — порядок данных не зависит от скорости сети.
        self._booted = False
        self.every(CLOCK_CHECK_EVERY, self._check_clock)
        # ДОБИВКА ПУСТЫХ КОЛЕЦ (27.10): если сеть отвалилась, кольцо
        # осталось пустым — повторяем якорь в фоне, пока не встанет.
        self.every(ANCHOR_RETRY_EVERY, self._refill_empty_rings)
        # ФОНОВЫЙ ДОБОР МИНУТ (06.10): если старт попал в сетевое окно,
        # обе страницы уходили с таймаутом и полотно оставалось пустым
        # навсегда — минут не добирал никто («работает, ошибок нет,
        # данных нет»). Оба ритма ждут _booted, чтобы не мешать порядку
        # прогрева.
        self.every(60.0, self._topup_retry)
        self.spawn(self._boot())

    async def _boot(self):
        """Прогрев и открытие потоков в фоне (06.10) — см. on_start.

        Жёсткий порядок: якоря (параллельно, задают выравнивание окон) →
        история из базы → REST-страницы от свежих к старым → публикации
        прогрева по каждому ТФ → сверка часов → живые потоки. Сетевые
        шаги не ограничены init_timeout — у каждого свой таймаут и
        повторы, а фоновые ритмы добьют упущенное."""
        try:
            await self._warmup()
        except Exception:
            self.log.exception("Прогрев не удался — доберут фоновые ритмы")
        try:
            await self._check_clock()
        except Exception:
            pass
        self._booted = True
        for stream in ("kline", "trades", "depth", "force", "btc", "alts"):
            self.spawn(self._stream_loop(stream))
        self.log.info("Потоки включены: kline_%s, aggTrade, depth20, "
                      "forceOrder, btc (вожак), alts (ETH/SOL/PEPE — пакет "
                      "Метакоррелятора). Хост %s, контур %s",
                      self._interval_str(), self.kernel.cfg.ws_base,
                      self.kernel.cfg.contour)

    def health_note(self):
        """Сводка поверхности для строки Здоровья.

        Пустое биржевое кольцо — это не «ноль свечей», а выключенный
        анализатор на этом ТФ. 27.10 четыре кольца (6h/8h/12h/1d) остались
        пустыми после сетевого сбоя на старте, и в сводке этого не было
        видно вообще. Теперь видно сразу."""
        spec = getattr(self, "_spec_ms", None)
        if not spec:
            return "поверхность: нет колец"
        empty = [tf // 1000 for tf in spec
                 if (self.senior.get(tf) is None
                     or self.senior[tf].head == 0)]
        live = [tf // 1000 for tf in spec
                if self.senior.get(tf) is not None
                and self.senior[tf].head > 0]
        if empty:
            return ("ПОВЕРХНОСТЬ: живых %d из %d биржевых колец, ПУСТЫЕ: %s"
                    % (len(live), len(spec), ",".join(str(t) for t in empty)))
        gaps = getattr(self, "_gaps", None)
        lost = ("; ПОТЕРИ ОКОН: %s"
                % ",".join("%ds:%d" % (tf // 1000, n)
                           for tf, n in sorted(gaps.items()))) if gaps else ""
        return ("ПОВЕРХНОСТЬ: все %d биржевых колец живые (минут %d, "
                "синтез старших %d)%s"
                % (len(spec), self.canvas.head,
                   len(getattr(self, "senior_ms", ())), lost))

    async def _topup_retry(self):
        """Фоновый добор минут (06.10).

        Если старт попал в сетевое окно (после прогрева соседнего
        процесса testnet троттлит IP), обе страницы уходили с таймаутом,
        полотно оставалось пустым — и минут НЕ ДОБИРАЛ НИКТО: синтез
        ступеней стоял, зоны не рождались. У якорей фоновая добивка
        есть с 27.10, у минут не было. Пять попыток раз в минуту,
        дальше — молчим (сеть мертва надолго)."""
        if not getattr(self, "_booted", True):
            return          # прогрев ещё идёт — в его порядок не вмешиваемся
        if self._topup_retries >= 5:
            return
        need = min(self.kernel.cfg.history_limit, 500)
        if self.canvas.head >= need:
            return                      # набралось — хватит
        if self._quota_throttled():
            # Потолок лимитов близко (Квотник) — добор подождёт свой
            # следующий тик. Попыткой это НЕ считается: не жечь повторы.
            return
        self._topup_retries += 1
        added = await self._rest_topup()
        if added:
            self.log.info("Фоновый добор минут: +%d (попытка %d, "
                          "в полотне %d)", added, self._topup_retries,
                          self.canvas.head)
        elif self._topup_retries >= 5:
            self.log.error("Фоновый добор минут не удался после 5 попыток — "
                           "полотно почти пустое (%d), синтез ступеней "
                           "ограничен", self.canvas.head)

    async def _refill_empty_rings(self):
        """Недоборные биржевые кольца пробуем долить якорем.

        Разовая неудача якоря = кольцо без глубины до перезапуска (27.10).
        Здесь повторяем раз в ANCHOR_RETRY_EVERY, пока кольцо ГЛУБЖЕ
        якорной нормы (60 свечей): кольцо, частично наполненное синтезом
        из минуток (например 4h — 42 свечи из недели), тоже недобрано:
        без якоря его глубина никогда не вырастет. Мелкие ТФ синтезом
        набирают норму сами — их добивка не трогает. Молча наполняться
        перестаём только когда все кольца глубже нормы."""
        if not getattr(self, "_booted", True):
            return          # прогрев ещё идёт — в его порядок не вмешиваемся
        if self._quota_throttled():
            # Потолок лимитов близко (Квотник) — добивка подождёт свой
            # следующий тик, чтобы не подстёгивать счётчик веса.
            return
        # Норма = закрытых свечей, сколько реально отдаёт один якорь:
        # биржа получает limit+1 (запас на незакрытую свечу), закрытых
        # выходит anchor_bars. Считать недоборным «строго больше нормы»
        # нельзя — только что долитое кольцо снова ушло бы в добивку.
        norm = max(2, self.kernel.cfg.anchor_bars - 1)
        thin = [tf for tf in self._spec_ms or ()
                if self.senior.get(tf) is not None
                and self.senior[tf].head < norm]
        if not thin:
            if self._anchor_failed and not self._refill_done:
                self._refill_done = True
                self.log.info("Добивка: все кольца глубже якорной нормы "
                              "(недобор был у %s)",
                              ",".join("%ds" % t for t in self._anchor_failed))
                self._anchor_failed = []
            return
        filled = []
        for tf in thin:
            # Повторы ЗДЕСЬ, а не на старте: добивка живёт в фоне и может
            # ждать 1/3/9 секунд сколько угодно — подъём не страдает.
            rows = await self._rest_anchor(tf, retries=3)
            if rows:
                merged = self.senior[tf].all_rows() + rows
                self.senior[tf].load_rows(merged)
                self.publish(f"{CANDLES_WARMUP}:{tf // 1000}",
                             self._batch(tf // 1000, self.senior[tf].all_rows(),
                                         True, True))
                filled.append(tf // 1000)
        if filled:
            self.log.warning("Добивка недоборных колец с биржи: %s (было %s)",
                             filled, [t // 1000 for t in thin])

    # ---------- лимиты биржи (Квотник) ----------

    def _quoter(self):
        """Квотник из ядра или None (тесты без него — молча работать дальше)."""
        return self.kernel.modules.get("quoter")

    def _quota_note(self, weight=1.0):
        """Отметить уходящий REST-запрос в журнале Квотника (до отправки)."""
        q = self._quoter()
        if q is not None:
            q.note(weight)

    def _quota_throttled(self):
        """Квотник требует гасить фоновые опросы (потолок лимитов близко)."""
        q = self._quoter()
        return bool(q and q.throttled())

    @staticmethod
    def _kline_weight(limit):
        """Вес /fapi/v1/klines по таблице биржи (верхняя граница)."""
        if limit < 100:
            return 1
        if limit < 500:
            return 2
        return 5

    async def _check_clock(self):
        """Сверка часов с биржей. Расхождение меток сдвигает границы всех
        окон, поэтому измеряется постоянно, а не только при старте: часы
        могут уйти уже во время работы (перевод времени вручную, правка
        NTP), и до перезапуска система об этом не узнала бы.

        Допуск CLOCK_TOLERANCE_MS. Чтобы журнал не захлебнуло, состояние
        (сходятся / расходятся) логируется только при СМЕНЕ, а сама
        величина обновляется каждый раз. Сетевые сбои молча копятся счётчиком."""
        cfg = self.kernel.cfg
        url = cfg.rest_base + "/fapi/v1/time"
        try:
            self._quota_note(1)         # /fapi/v1/time — вес 1 (до отправки)
            t0 = time.time()
            async with aiohttp.ClientSession() as session:
                async with session.get(url,
                                       timeout=aiohttp.ClientTimeout(total=15)) as r:
                    if r.status != 200:
                        raise OSError("статус %s" % r.status)
                    server_ms = int(orjson.loads(await r.read())["serverTime"])
            # Ответ летел по сети — делим задержку пополам, иначе измеренное
            # расхождение всегда смещено на половину пинга.
            rtt = max(0.0, (time.time() - t0) * 1000.0)
            drift = time.time() * 1000.0 - rtt / 2.0 - server_ms
        except Exception as exc:
            self._clock_fails += 1
            if self._clock_fails in (1, 5) or self._clock_fails % 20 == 0:
                self.log.warning("Сверка времени с биржей не удалась (%d раз подряд): "
                                 "%s: %s", self._clock_fails,
                                 type(exc).__name__, exc)
            return
        self._clock_fails = 0
        self.clock_drift_ms = drift
        bad = abs(drift) > CLOCK_TOLERANCE_MS
        if bad != self._clock_bad:          # смена состояния — говорим
            self._clock_bad = bad
            if bad:
                self.log.error("ВНИМАНИЕ: часы расходятся с биржей на %+.0f мс "
                               "(допуск %d мс) — метки времени и границы окон "
                               "уедут. Синхронизируйте время ОС.",
                               drift, CLOCK_TOLERANCE_MS)
            else:
                self.log.info("Часы сошлись с биржей: расхождение %+.0f мс", drift)

    # ---------- прогрев и синтез ----------

    async def _warmup(self):
        keeper = self.kernel.modules.get("keeper")

        # ЯКОРЯ ИДУТ ПЕРВЫМИ (27.10). Их второй смысл — они задают
        # ВЫРАВНИВАНИЕ окон для синтеза. У Binance 3d-свечи начинаются не с
        # кратного трём суток от эпохи, а со сдвигом в 2 суток (проверено
        # живой сетью). Если сначала поглотить минутки, синтез построит 3d
        # по чужим меткам, и в кольцо ляжет смесь биржевых и синтетических
        # баров за РАЗНЫЕ трёхдневки — с дырой в 2 сутока между каждым
        # биржевым баром.
        # ЯКОРЯ ПАРАЛЛЕЛЬНО (06.10): подряд 12 запросов = 12 TLS-хэндшейков
        # и 38-95 сек прогрева при лимите инициализации 90с — половина
        # живых прогонов падала таймаутом Сборщика. ТФ независимы, каждый
        # _rest_anchor уже держит свой сессию, значит gather безопасен;
        # порядок важен только внутри _learn_align, а он сводит по словарю.
        anchors_list = await asyncio.gather(
            *(self._rest_anchor(tf) for tf in self.senior_ms))
        anchors = dict(zip(self.senior_ms, anchors_list))
        self._learn_align(anchors)

        added_db = 0
        if keeper is not None:
            rows = await keeper.get_last_candles(self.symbol,
                                                 self.kernel.cfg.base_tf_sec,
                                                 self.kernel.cfg.history_limit)
            for row in rows:
                self._absorb_closed(tuple(row), publish=False)
            added_db = self.canvas.head
        else:
            self.log.warning("Кладовщик не найден — прогрев только через REST")
        added_rest = await self._rest_topup()

        # Накопленную историю публикуем на ярлыках прогрева ПО КАЖДОМУ ТФ:
        # доска значений хранит каждую партию отдельно, поздние модули
        # забирают свою ТФ из своего ярлыка
        self.publish(f"{CANDLES_WARMUP}:{self.kernel.cfg.base_tf_sec}",
                     self._batch(self.kernel.cfg.base_tf_sec,
                                 self.canvas.all_rows(), True, True))
        # Старшие ТФ. Синтез из минутных уже лёг в кольца через
        # _absorb_closed (с правильным выравниванием), теперь склеиваем с
        # историей из базы и якорем — дедуп по метке времени, биржа последняя.
        for tf in self.senior_ms:
            db_rows = []
            if keeper is not None:
                db_rows = [tuple(r) for r in await keeper.get_last_candles(
                    self.symbol, tf // 1000, self.senior[tf].size - 50)]
            await self._fill_senior(tf, db_rows, anchors.get(tf) or [])
            self.publish(f"{CANDLES_WARMUP}:{tf // 1000}",
                         self._batch(tf // 1000,
                                     self.senior[tf].all_rows(), True, True))
        depth = {tf // 1000: self.senior[tf].head for tf in self.senior_ms}
        spec = {t: h for t, h in depth.items() if t in EXCHANGE_SPECTRUM}
        own = {t: h for t, h in depth.items() if t not in EXCHANGE_SPECTRUM}
        self.log.info("Прогрев %s: из базы %d, из REST %d минутных; "
                      "ЯКОРЯ биржи (%d ТФ): %s; НАШИ ступени "
                      "(только синтез): %s; после прогрева растёт "
                      "синтез из 1m — во всех кольцах",
                      self._interval_str(), added_db, added_rest,
                      len(spec), spec, own)
        thin = {t: h for t, h in depth.items() if h < 60}
        if thin:
            self.log.info("Колец с глубиной меньше 60 баров (порог Херста): %s",
                          thin)

    def _learn_align(self, anchors):
        """Выучить сдвиг границ окон у биржи (27.10).

        Binance НЕ всегда выравнивает интервалы от эпохи Unix: проверено
        живой сетью, `3d` начинается со сдвигом 2 суток, а остальные 12
        интервалов — с нуля. Раньше сдвиг не учитывался, и в кольцо `3d`
        попадала смесь биржевых и синтетических баров за РАЗНЫЕ трёхдневки:
        метки не пересекались вообще (проверено: пересечение пустое), то есть
        между каждым биржевым баром зияла дыра в 2 суток.

        Источник истины — сам биржевой якорь, а не константа в коде: если
        биржа поменяет выравнивание, сдвиг переучится сам. Класть сдвиг в
        константу — значит зашить в код представление о чужом сервисе."""
        for tf, rows in (anchors or {}).items():
            if not rows or tf <= self.base_ms:
                continue
            # все свечи якоря должны давать ОДИН и тот же остаток — это и
            # проверка согласованности: если остатки разные, биржа перешла
            # на новое выравнивание прямо сейчас, и половина якоря уже по
            # старому. Тогда сдвиг не выучиваем (опаснее не знать вовсе).
            shifts = {int(r[0]) % tf for r in rows}
            if len(shifts) != 1:
                self.log.warning(
                    "ТФ %dс: биржевые метки дают РАЗНЫЕ остатки %s — "
                    "выравнивание не выучено, синтез этого ТФ не доверяем",
                    tf // 1000, sorted(shifts)[:4])
                continue
            shift = shifts.pop()
            if shift:
                self._align_shift[tf] = shift
                self.log.info(
                    "ТФ %dс: биржа выравнивает окна со сдвигом %d мс "
                    "(%.2f суток) — синтез учтёт", tf // 1000, shift,
                    shift / 86_400_000.0)

    def _win_start(self, ts, tf):
        """Начало окна ТФ, содержащего ts, по выравниванию биржи."""
        return ts - (ts - self._align_shift.get(tf, 0)) % tf

    async def _fill_senior(self, tf, extra_rows=(), anchor=()):
        """История уровня: биржа ПЕРЕЗАПИСЫВАЕТ, база только дополняет.

        ТЗ хозяина: биржевое волокно — ИСТОЧНИК. Значит его слово главнее
        всего, что лежит в базе. Раньше было `if len(unique) < anchor_bars` —
        якорь тянулся ТОЛЬКО при нехватке глубины, и потому биржа не могла
        поправить свечи, собранные старым синтезом (дефект 27.10: в базе
        лежали 6h/12h с H/L/C от минуток, и они не вытеснялись никогда).

        Теперь: якорь с биржи тянется ВСЕГДА для ТФ из биржевого спектра и
        встаёт в merged ПОСЛЕДНИМ, поэтому load_rows оставляет его, а не
        отброшенную копию из базы."""
        canv = self.senior[tf]
        merged = list(canv.all_rows()) + list(extra_rows)
        if anchor:
            # Биржа — приоритет: её свечи идут последними и вытесняют
            # одноимённые из базы/синтеза.
            merged += list(anchor)
        canv.load_rows(merged)
        return len(anchor or ())

    async def _rest_anchor(self, tf, retries=0):
        """ЯКОРЬ с биржи: anchor_bars свечей по ТФ, который биржа знает.

        Для ТФ вне биржевого спектра (2м/8м/32м/512м и прочие ступени
        пирамиды) якоря быть не может — там нет интервала. Раньше такой
        запрос уходил с кодом "64h" и получал -1120 в пустой return[].
        Теперь это видно и объяснено один раз, а не молча.

        retries=0 (по умолчанию, на старте) — ОДНА попытка без задержек.
        Подъём не должен висеть: 12 биржевых ТФ × паузы повторов — это
        минуты застоя, а сеть может лежать и дольше. Повторы живут в
        добивке _refill_empty_rings, которая крутится в фоне и не блокирует
        старт: кольцо наполнится, когда сеть оживёт (27.10)."""
        tf_sec = tf // 1000
        interval = self._tf_interval_str(tf_sec)
        if interval is None:
            if not self._no_anchor_logged:
                self._no_anchor_logged = True
                # senior_ms на этом шаге может быть ещё не собран (или
                # Сборщик поднят частично) — показываем то, что есть,
                # и не падаем из-за диагностики.
                steps = [t // 1000 for t in getattr(self, "senior_ms", ())
                         if self._tf_interval_str(t // 1000) is None]
                self.log.info("Ступени без интервала биржи (%s) — их кольца "
                              "растут синтезом из младших, якоря у них нет",
                              ",".join(str(s) for s in steps) or "пока не выявлены")
            return []
        params = {"symbol": self.symbol, "interval": interval,
                  "limit": self.kernel.cfg.anchor_bars}
        # ПАУЗЫ ПОВТОРОВ: 1/3/9 секунды на попытку. Дешёвый обмен на живое
        # кольцо — но только в фоновой добивке, не на старте.
        delays = (0.0, 1.0, 3.0, 9.0)[:retries + 1]
        last_exc = None
        for attempt, delay in enumerate(delays, start=1):
            if delay:
                await asyncio.sleep(delay)
            try:
                self._quota_note(self._kline_weight(params["limit"]))
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                            self._rest_url(), params=params,
                            timeout=aiohttp.ClientTimeout(total=20)) as resp:
                        if resp.status != 200:
                            last_exc = "статус %s" % resp.status
                            continue
                        klines = orjson.loads(await resp.read())
                if not klines:
                    last_exc = "пустой ответ"
                    continue
                # незакрытую (уже начавшуюся) свечу не берём — 27.10
                klines = self._closed_klines(klines)
                if not klines:
                    last_exc = "ответ без закрытых свечей"
                    continue
                rows = [self._kline_row(k) for k in klines]
                if attempt > 1:
                    self.log.info("Якорь %s получен с %d-й попытки "
                                  "(сеть ожила)", interval, attempt)
                else:
                    self.log.info("Якорь %s (%dс): %d свечей с биржи",
                                  interval, tf_sec, len(rows))
                return rows
            except Exception as exc:      # noqa: BLE001 — сеть рвётся по-разному
                last_exc = "%s: %s" % (type(exc).__name__, exc)
        self.log.warning(
            "Якорь ТФ %dс (%s) не получен (%d поп.): %s — кольцо пустое, "
            "встанет в добивке через %.0f с",
            tf_sec, interval, len(delays), last_exc, ANCHOR_RETRY_EVERY)
        self._anchor_failed.append(tf_sec)
        return []

    def _batch(self, tf_sec, rows, closed, warmup=False):
        return {"symbol": self.symbol, "tf_sec": int(tf_sec),
                "candles": [list(r) for r in rows], "closed": bool(closed),
                "warmup": bool(warmup)}

    async def _klines_page(self, session, params, attempts=2):
        """Страница истории с ПОВТОРАМИ (06.10): в окне старта Сборщика
        (~ первые 20-25с) первый сетевой запрос процесса виснет на ~19.7с
        (проверено: TCP 0.06с, TLS 0.08с из чужого процесса — живо, а
        запрос процесса стартует с ровной задержкой; вне окна — 0.3с).
        Старый таймаут 15с обрывал ВЕСЬ прогрев минуток: полотно пустое,
        синтез ступеней стоял. Таймаут 25с переживает окно, а попыток 2
        хватает при бюджете инициализации 90с (якоря параллельно ≤20с +
        25 + 1 + 25 ≤ 71с).

        None — страница не получена после всех попыток (история оборвана,
        но уже накопленные страницы отдаются как есть)."""
        delays = (0.0, 1.0, 3.0)
        last_exc = None
        for attempt in range(attempts):
            if delays[attempt]:
                await asyncio.sleep(delays[attempt])
            try:
                self._quota_note(self._kline_weight(params.get("limit", 0)))
                async with session.get(
                        self._rest_url(), params=params,
                        timeout=aiohttp.ClientTimeout(total=25)) as resp:
                    if resp.status != 200:
                        last_exc = "статус %s" % resp.status
                        continue
                    klines = orjson.loads(await resp.read())
                if not klines:
                    return []                   # конец истории — не ошибка
                return klines
            except (aiohttp.ClientError, asyncio.TimeoutError,
                    OSError) as exc:
                last_exc = "%s: %s" % (type(exc).__name__, exc)
        self.log.warning("REST-страница не удалась после %d попыток: %s",
                         attempts, last_exc)
        return None

    async def _rest_topup(self):
        """Постраничный REST-добор минутной истории (от свежих к старым).
        Страницы собираются целиком, затем поглощаются в хронологическом
        порядке — иначе старые страницы молча отбрасывались бы полотном."""
        want = min(self.kernel.cfg.history_limit,
                   self.kernel.cfg.rest_limit * self.kernel.cfg.rest_pages)
        fetched = []
        try:
            async with aiohttp.ClientSession() as session:
                end_ms = None
                for _ in range(self.kernel.cfg.rest_pages):
                    if len(fetched) >= want:
                        break
                    params = {"symbol": self.symbol,
                              "interval": self._interval_str(),
                              "limit": min(self.kernel.cfg.rest_limit,
                                           want - len(fetched))}
                    if end_ms is not None:
                        params["endTime"] = end_ms
                    klines = await self._klines_page(session, params)
                    if klines is None:
                        break                   # обрыв после всех попыток
                    if not klines:
                        break
                    # незакрытую (уже начавшуюся) свечу не берём — 27.10.
                    # Иначе частичная минутка ляжет в полотно и заблокирует
                    # настоящую закрытую с той же меткой навсегда.
                    fetched = [self._kline_row(k)
                               for k in self._closed_klines(klines)] + fetched
                    end_ms = int(klines[0][0]) - 1
        except Exception:
            self.log.exception("REST-прогрев не удался")
        added = 0
        for row in fetched:
            if row[0] > self.canvas.last_ts():
                self._absorb_closed(row, publish=False)
                added += 1
        return added

    def _interval_str(self):
        return self._tf_interval_str(self.kernel.cfg.base_tf_sec)

    @staticmethod
    def _tf_interval_str(tf_sec):
        """Код интервала биржи для ТФ. None — биржа такого не знает.

        Раньше перевод был механическим («секунды → код»), и это молча
        ломало всё, чего биржа не отдаёт: 3840с превращалось в "64h", и
        REST отвечал -1120 Invalid interval — тихо, в пустом return[]. Теперь
        перевод только по таблице EXCHANGE_SPECTRUM: интервала нет в
        спектре → возвращаем None, и вызывающий видит правду."""
        return EXCHANGE_SPECTRUM.get(int(tf_sec))

    @staticmethod
    def _closed_klines(klines):
        """Отбрать НЕЗАКРЫТЫЕ свечи из ответа REST. 27.10.

        Binance в последнем элементе `klines` отдаёт УЖЕ НАЧАВШУЮСЯ свечу
        (индекс 6 — время её закрытия в мс, оно в будущем). Если её взять,
        в полотно ляжет частичная минутка, и настоящая закрытая минутка с той
        же меткой будет отброшена дедупом `ts <= last_ts` — навсегда.

        Проще не создавать проблему, чем потом её героически решать: заказали
        61 свечу — учитываем 60. Частичная придёт ещё раз с биржи, уже
        закрытой, когда её метка времени реально наступит.

        Отбрасывание — по close_time, а не «последняя строка всегда
        частичная»: у 1m биржа не всегда отдаёт текущую, и жёсткая
        позиция срезала бы закрытую свечу на ровном месте.
        """
        now_ms = int(time.time() * 1000)
        return [k for k in klines if not (len(k) > 6 and int(k[6]) > now_ms)]

    @staticmethod
    def _kline_row(k):
        """REST-формат: [open_time, o, h, l, c, v, ..., taker_buy_base, ...].
        Дельта = куплено покупателями минус продано продавцами."""
        volume = float(k[5])
        taker_buy = float(k[9]) if len(k) > 9 else volume / 2.0
        # Дельта = купили агрессором − продали агрессором. Агрессор-продавец
        # = весь объём − агрессор-покупатель, поэтому delta = taker_buy −
        # (volume − taker_buy) = 2·taker_buy − volume. Было taker_buy − volume
        # (= buy − ВЕСЬ объём = −sell) — знак и величина врали (баг 12.10).
        return (int(k[0]), float(k[1]), float(k[2]), float(k[3]),
                float(k[4]), volume, 2.0 * taker_buy - volume)

    def _absorb_closed(self, row, publish=True):
        """Закрытая МИНУТНАЯ свеча: в полотно + синтез ВСЕХ старших ТФ.

        Схема хозяина (27.10): WS несёт только минутки, всё остальное
        строится из них. Биржевые кольца после якоря растут синтезом
        наравне с нашими — сверка показала, что синтез из минуток бирже
        не уступает (расхождений ноль на полных окнах).

        Окно берётся ПРЯМО ИЗ ПОЛОТНА (RingCanvas.window_rows), а не из
        отдельного накопителя на каждое кольцо. Раньше на каждое кольцо
        жил свой `pending` со своим окном и своей проверкой полноты — 20
        параллельных состояний на одних и тех же минутках. Теперь все
        читают одно место, и дырка в минутках видна сразу.

        Окно берётся только ПОЛНЫМ и начавшимся ровно на границе: иначе
        закрытое окно из 17 минуток записалось бы как час."""
        ts = row[0]
        if self.canvas.head and ts <= self.canvas.last_ts():
            return  # дубликат или устаревшая — пропускаем
        self.canvas.push(row)
        for tf in self.senior_ms:
            ring = self.senior.get(tf)
            if ring is None:
                continue
            win_start = self._win_start(ts, tf)
            # момент закрытия окна ТФ — последняя минутка на его границе
            if ts != win_start + tf - self.base_ms:
                continue
            need = tf // self.base_ms
            rows = self.canvas.window_rows(win_start, need, self.base_ms)
            if rows is None:
                # окно неполное (дырка в минутках) или не помещается —
                # честно говорим, вместо чтобы записать ложную свечу
                self._note_gap(tf, need)
                continue
            agg = aggregate(rows)
            if ring.last_ts() and agg[0] == ring.last_ts():
                # та же метка: якорь отдал незакрытую свечу, а мы
                # только что достроили её закрытой — обновляем
                self._overwrite_last(ring, agg)
            elif not ring.last_ts() or agg[0] > ring.last_ts():
                ring.push(agg)
            if publish and (not ring.last_ts() or agg[0] >= ring.last_ts()):
                self.publish(CANDLES,
                             self._batch(tf // 1000, [agg], True))
        if publish:
            self.publish(CANDLES, self._batch(self.kernel.cfg.base_tf_sec,
                                              [row], True))

    def _note_gap(self, tf, need):
        """Посчитать потерю окна: кольцо не сможет закрыть этот бар.

        Молчать здесь нельзя: кольцо тихо теряет бар, и через сутки анализатор
        получает историю с дыркой, не узнав об этом. Считаем по ТФ и один
        раз показываем в Здоровье."""
        gaps = getattr(self, "_gaps", None)
        if gaps is None:
            gaps = self._gaps = {}
        gaps[tf] = gaps.get(tf, 0) + 1
        n = gaps[tf]
        if n <= 3 or n % 60 == 0:
            self.log.warning(
                "Кольцо %dс: окно не собрано из минуток (нужно %d) — "
                "свеча не записана. Потерь на кольце: %d. Причина: дырка "
                "в минутках или окно не помещается в полотно.",
                tf // 1000, need, n)

    @staticmethod
    def _overwrite_last(ring, row):
        """Записать свечу поверх последней с той же меткой (кольцо кольцевое)."""
        idx = (ring.head - 1) % ring.size
        ring.ts[idx] = row[0]
        ring.o[idx] = row[1]
        ring.h[idx] = row[2]
        ring.l[idx] = row[3]
        ring.c[idx] = row[4]
        ring.v[idx] = row[5]
        ring.d[idx] = row[6]

    # ---------- живые потоки WebSocket ----------

    # ---------- запрос кольца по требованию (ленивое materialization) ----------

    def get_ring(self, tf_sec):
        """Кольцо свечей по запросу анализатора.

        Раньше кольца брались напрямую: `col.senior.get(ms)`. Отсутствие
        кольца было тихим `return None` — та же болезнь, что и с мёртвым
        kline: «модуль работает, ошибок нет, данных нет». Теперь запрос
        явный: кольца нет → оно ДОСТРАИВАЕТСЯ из младшего делящего нацело,
        и с этого момента живёт само (наполняется в _absorb_closed).

        Синтез только ВВЕРХ: basis < tf и tf % basis == 0. Обратно нельзя
        (внутри 4h мы не знаем путь цены). Возвращает RingCanvas или None,
        если построить не из чего.
        """
        tf_ms = int(tf_sec) * 1000
        ring = self.senior.get(tf_ms)
        if ring is not None:
            return ring
        return self._materialize(tf_ms)

    def _materialize(self, tf_ms):
        """Достроить кольцо tf_ms из ближайшего МЕЛЬЧЕГО кольца нацело."""
        basis = self._find_basis(tf_ms)
        if basis is None:
            self.log.info("Кольцо %dс не построено: нет младшего источника "
                          "(нужны минуты или ступень пирамиды)", tf_ms // 1000)
            return None
        src = self.canvas if basis == self.base_ms else self.senior.get(basis)
        src_rows = src.all_rows() if src is not None else []
        if not src_rows:
            return None
        per = tf_ms // basis                       # сколько младших в окне
        ring = RingCanvas(max(RING_SIZE, self.kernel.cfg.pyramid_depth))
        buckets = {}
        for row in src_rows:
            win = self._win_start(row[0], tf_ms)   # детерминированное выравнивание
            buckets.setdefault(win, []).append(row)
        built = 0
        for win in sorted(buckets):
            group = buckets[win]
            # Окно берём только полное и начавшееся ровно на границе: иначе
            # метка уехала бы с первой неполной свечи и уровень «поплыл».
            if len(group) != per or group[0][0] != win:
                continue
            ring.push(aggregate(group))
            built += 1
        if not built:
            return None
        self.senior[tf_ms] = ring
        if tf_ms not in self.senior_ms:
            self.senior_ms.append(tf_ms)
            self.senior_ms.sort()
        self._materialized.add(tf_ms)
        self._mat_basis[tf_ms] = basis
        # Синтез обходит self.senior_ms, поэтому кольцо, достроенное по запросу,
        # наполняется само — вручную в списки добавлять не нужно.
        self.log.info("Кольцо %dс достроено по запросу: %d свечей из %dс "
                      "(источник %dс)", tf_ms // 1000, built, tf_ms,
                      basis // 1000)
        return ring

    def _find_basis(self, tf_ms):
        """Ближайшее кольцо МЕЛЬЧЕ tf_ms, делящее его нацело. None — нечем."""
        best = self.base_ms if self.base_ms < tf_ms and tf_ms % self.base_ms == 0 else None
        for cand in self.senior:
            if cand >= tf_ms or tf_ms % cand:
                continue
            if best is None or cand > best:
                best = cand
        return best

    def _rest_url(self):
        """REST-адрес свечей АКТИВНОГО КОНТУРА (тот же рынок, что и WS)."""
        return self.kernel.cfg.rest_base + "/fapi/v1/klines"

    def _stream_url(self, stream):
        base = self.kernel.cfg.ws_base
        pair = self.symbol.lower()
        if stream == "kline":
            # ТОЛЬКО МИНУТКИ (схема хозяина, 27.10). Весь спектр по WS —
            # лишнее: минутки покрывают всё, старшие растут синтезом из них,
            # а биржевые интервалы приходят якорями при прогреве по REST.
            return base + f"{pair}@kline_{self._interval_str()}"
        if stream == "trades":
            return base + f"{pair}@aggTrade"   # проверено: @aggTrade живой
        if stream == "force":
            return base + "!forceOrder@arr" # ликвидации ВСЕГО рынка (проверено)
        if stream == "btc":
            return base + "btcusdt@aggTrade"  # вожак: сырьё Поводыря
        if stream == "alts":
            # Пакет Метакоррелятора: три нити одним соединением
            # (комбинированный поток Binance: сообщения с полем "stream")
            return (base.replace("/ws/", "/stream?streams=")
                    + "ethusdt@aggTrade/solusdt@aggTrade/pepeusdt@aggTrade")
        return base + f"{pair}@depth20@100ms"

    async def _stream_loop(self, stream):
        failures = 0
        while True:
            try:
                async with websockets.connect(self._stream_url(stream),
                                              ping_interval=20,
                                              ping_timeout=20) as ws:
                    self.log.info("Поток %s подключён (%s), контур %s",
                                  stream,
                                  self._stream_url(stream).split("//")[-1].split("/")[0],
                                  self.kernel.cfg.contour)
                    failures = 0
                    self._ws_ok[stream] = True
                    handler = {"kline": self._pump_kline,
                               "trades": self._pump_trades,
                               "depth": self._pump_depth,
                               "force": self._pump_force,
                               "btc": self._pump_btc,
                               "alts": self._pump_alts}[stream]
                    await handler(ws)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._ws_ok[stream] = False
                failures += 1
                self.kernel.module_error(self.manifest.name)
                delay = min(60.0, 3.0 * (2 ** min(failures, 5)))
                self.log.warning("Поток %s: %s — переподключение через %.0f сек",
                                 stream, exc, delay)
                await asyncio.sleep(delay)

    async def _pump_kline(self, ws):
        """МИНУТНЫЕ свечи с биржи. Всё остальное синтезируется из них.

        Схема хозяина (27.10): WS несёт только минутки. Весь спектр по WS
        был лишним усложнением — минутки покрывают всю поверхность, а
        биржевые интервалы приходят якорями при прогреве по REST.

        Старый молчаливый `if not k: continue` убран 27.10: сообщения без
        поля "k" попадают в журнал, иначе поток мог умереть молча (как и
        случилось с мёртвым хостом)."""
        async for raw in ws:
            msg = orjson.loads(raw)
            k = msg.get("k")
            if not k:
                self.kline_alien += 1
                if self.kline_alien <= 3:
                    self.log.error(
                        "Поток kline: сообщение БЕЗ поля 'k' (#%d). "
                        "Ключи=%s; тело=%s", self.kline_alien,
                        sorted(msg)[:12], raw[:300])
                continue
            row = (int(k["t"]), float(k["o"]), float(k["h"]), float(k["l"]),
                   float(k["c"]), float(k["v"]),
                   # ДельТА = купили агрессором − продали агрессором, как в
                   # REST (_kline_row) и в страховке из сделок (_agg_trade).
                   # WS: "V" = объём тейкер-покупок, "v" = весь объём;
                   # агрессор-продавец = v − V, значит дельта = 2·V − v.
                   # Было V − v (= −sell, всегда ≤0 — п.6 аудита 18.10):
                   # знак дельты в живом потоке врал и расходился с REST.
                   2.0 * float(k.get("V", 0.0)) - float(k["v"]))
            if not k.get("x"):
                # текущая минута — только свежесть для экрана
                self.publish(CANDLES, self._batch(self.base_ms // 1000,
                                                  [row], False), ttl=15.0)
                continue
            self._kline_last_ts = max(self._kline_last_ts, int(k["t"]))
            self.kline_candles += 1
            self._agg_note = 0   # официальный поток жив — страховка не в дело
            self._absorb_closed(row)
            self.live_candles += 1

    def _agg_trade(self, ts_ms, price, qty, is_sell):
        """Сделка в локальный агрегатор минутной свечи. На смене минуты
        предыдущее окно закрывается (см. _agg_close)."""
        if ts_ms <= 0 or price <= 0:
            return
        win = ts_ms - ts_ms % self.base_ms
        if self._agg_win is None or win != self._agg_win:
            prev = self._agg_row
            self._agg_win = win
            self._agg_row = [win, price, price, price, price, 0.0, 0.0]
            if prev is not None:
                self._agg_close(prev)
        r = self._agg_row
        r[2] = max(r[2], price)              # high
        r[3] = min(r[3], price)              # low
        r[4] = price                         # close
        r[5] += qty                          # volume
        r[6] += -qty if is_sell else qty     # дельта: тейкеры купили − продали

    def _agg_silent_note(self):
        """Страховка включилась — значит официальный поток молчит."""
        self.log.error(
            "ВНИМАНИЕ: свечи строятся из сделок — официальный поток kline "
            "не отдал ни одной свечи (kline=%d). Это запасной путь: OHLC "
            "приблизительный. Проверь адрес %s и контур %s.",
            self.kline_candles, self.kernel.cfg.ws_base, self.kernel.cfg.contour)

    def _agg_close(self, row):
        """Минута закрылась потоком сделок: публикуем, только если kline
        за эту минуту так и не пришёл (официальная свеча приоритетна)."""
        if row[5] <= 0:
            return                           # минута без сделок
        if row[0] <= self._kline_last_ts:
            return                           # kline уже поглотил эту минуту
        self._absorb_closed(tuple(row))
        self.live_candles += 1
        self._agg_note += 1
        if self._agg_note == 1:
            # Первая собранная из сделок минута: ещё не приговор — после
            # старта полная минутная свеча с биржи может закрыться через
            # минуту, и страховка успеет отступить сама. Ругаемся ERROR-ом
            # только после ПРОСТОЯ: поток молчал дольше, чем есть в минуте.
            self._agg_first_ms = time.monotonic()
        elif self._agg_note == 2 and time.monotonic() - self._agg_first_ms < 180:
            self.log.info("Страховка: минута собрана из сделок, ждём "
                          "подтверждения от биржи (kline=%d)",
                          self.kline_candles)
        elif self._agg_note % 30 == 0:
            self.log.warning("Свечи по-прежнему строятся из сделок: "
                             "kline отдал %d за %d минут",
                             self.kline_candles, self._agg_note)
        if (self._agg_note >= 3
                and time.monotonic() - self._agg_first_ms > 180.0
                and not self._agg_silent_said):
            self._agg_silent_said = True
            self._agg_silent_note()

    async def _pump_btc(self, ws):
        """Вожак (BTCUSDT): сделка → чистый топик btc_price. Только
        доставка сырых фактов (утверждено): углы/дельты считает Поводырь."""
        async for raw in ws:
            msg = orjson.loads(raw)
            self.btc_price = float(msg.get("p", 0.0))
            self.btc_ticks += 1
            self.publish(BTC_PRICE, {
                "symbol": "BTCUSDT",
                "price": self.btc_price,
                "qty": float(msg.get("q", 0.0)),
                "is_sell": bool(msg.get("m", False)),
                "ts_ms": int(msg.get("T", 0)),
            }, ttl=3.0)

    async def _pump_trades(self, ws):
        async for raw in ws:
            msg = orjson.loads(raw)
            self.price = float(msg.get("p", 0.0))
            self.last_price_ts = time.monotonic()
            self.ticks += 1
            self.publish(PRICE, {
                "symbol": self.symbol,
                "price": self.price,
                "qty": float(msg.get("q", 0.0)),
                "is_sell": bool(msg.get("m", False)),
                "ts_ms": int(msg.get("T", 0)),
            }, ttl=3.0)
            self._agg_trade(int(msg.get("T", 0)), self.price,
                            float(msg.get("q", 0.0)),
                            bool(msg.get("m", False)))

    async def _pump_depth(self, ws):
        async for raw in ws:
            msg = orjson.loads(raw)
            self.last_depth_ts = time.monotonic()
            self.depth_updates += 1
            self.publish(ORDERBOOK, {
                "symbol": self.symbol,
                "bids": msg.get("b", [])[:20],
                "asks": msg.get("a", [])[:20],
                "ts_ms": int(msg.get("E", 0)),
            }, ttl=3.0)

    async def _pump_force(self, ws):
        """Ликвидации всего рынка: наш символ → LIQUIDATIONS,
        вожак BTC → BTC_LIQ (паника вожака, Метакоррелятор)."""
        async for raw in ws:
            msg = orjson.loads(raw)
            order = msg.get("o") or {}
            sym = order.get("s")
            if sym == self.symbol:
                self.liquidations += 1
                self.publish(LIQUIDATIONS, {
                    "symbol": self.symbol,
                    "side": order.get("S"),           # SELL = ликвидирован лонг
                    "qty": float(order.get("q", 0.0)),
                    "price": float(order.get("p", 0.0)),
                    "ts_ms": int(order.get("T", 0)),
                }, ttl=5.0)
            elif sym == "BTCUSDT":
                self.publish(BTC_LIQ, {
                    "symbol": "BTCUSDT",
                    "side": order.get("S"),
                    "qty": float(order.get("q", 0.0)),
                    "price": float(order.get("p", 0.0)),
                    "ts_ms": int(order.get("T", 0)),
                }, ttl=5.0)

    async def _pump_alts(self, ws):
        """Пакет Метакоррелятора (комбинированный поток): сделка альтов
        → чистый топик alt_price. Только доставка сырых фактов —
        корреляции считает meta_corr."""
        async for raw in ws:
            msg = orjson.loads(raw)
            data = msg.get("data") or msg          # combined-stream обёртка
            sym = str(data.get("s", "")).upper()
            price = float(data.get("p", 0.0))
            if not sym or price <= 0:
                continue
            self.publish(ALT_PRICE, {
                "symbol": sym,
                "price": price,
                "qty": float(data.get("q", 0.0)),
                "is_sell": bool(data.get("m", False)),
                "ts_ms": int(data.get("T", 0)),
            }, ttl=3.0)

    async def on_stop(self):
        self.log.info("Итог: живых свечей %d, сделок %d, стаканов %d, "
                      "ликвидаций %d; связь: %s",
                      self.live_candles, self.ticks, self.depth_updates,
                      self.liquidations, self._ws_ok)
