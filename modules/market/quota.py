"""«Квотник» — журнал лимитов биржи: вес запросов и частота ордеров.

Binance (USDⓈ-M Futures) считает по IP три независимых ведра:
- ВЕС ЗАПРОСОВ в минуту (REQUEST_WEIGHT): каждый REST-эндпоинт стоит 1..5;
  превышение — код -1042, при систематическом давлении бан IP на час+;
- ЧАСТОТУ ОРДЕРОВ: окно 10 секунд и минута (суточного потолка у фьючерсов
  нет — он только у спота); превышение — код -1003;
- СЫРЫЕ ЗАПРОСЫ (RAW_REQUESTS): 61 000 / 5 минут.

Квотник живёт один на всё ядро (единая книга QuotaLedger). Каждый модуль,
уходящий в REST, отмечает запрос ДО отправки: биржа начисляет вес по факту
получения, упал ли ответ или нет — неважно. Ордеры отмечаются отдельно
(создание и отмена — оба считаются частотой ордеров).

Защита «не превышать» построена из трёх механизмов:
- РЕЗЕРВ ДИЛЕРА (quota.reserve_frac): доля весового потолка зарезервирована
  под дилера (ордера, стопы, баланс). Фон гасится, заняв throttle_frac
  СВОЕГО (общего) пула — осознанно раньше полного потолка, поэтому дилеру
  всегда остаётся резерв и троттлинг фона его не задевает;
- ПОРОГ ТОРМОЗА (quota.throttle_frac): когда фон занял долю порога своего
  пула, Квотник говорит «тормози», а фоновые опросы (OI, добивка пустых
  колец) пропускают шаг — система сама замедляется;
- СЛОТ ОРДЕРА: перед отправкой ордера дилер ждёт освобождения окна в 10
  секунд. Если слот не нашёлся (таймаут), ордер всё равно уходит, но с
  громким логом: незакрытый стоп опаснее кода -1003.

Веса эндпоинтов — по таблице биржи (оценочно); при сомнениях берём
верхнюю границу. Потолки по умолчанию взяты вдвое НИЖЕ реальных значений
биржи (~5000 веса/мин, ~100 ордеров/10с) — запас на неточности таблицы.

Строка здоровья ядра несёт сегмент «КВОТЫ»: занятость весового и
ордерного потолка в реальном времени.
"""
import asyncio
import collections
import time

from core.module import Manifest, NeirModule


class QuotaLedger:
    """Единая книга лимитов процесса (одна на все модули).

    Окно веса — скользящие 60 секунд: биржа сбрасывает счётчик по своей
    минуте, скользящее окно консервативнее и проще. Ордера считаются в
    скользящем окне 10 секунд — биржа режет ордера только по 10с и минуте;
    суточного потолка у фьючерсов НЕТ (спотовый атавизм убран).

    РЕЗЕРВ ДИЛЕРА (12.10.2026): доля `reserve_frac` весового потолка
    зарезервирована под дилера (ордера, стопы, баланс, приватное). Фон
    (Сборщик, OI) гасится, заняв `throttle_frac` СВОЕГО (общего) пула —
    это осознанно раньше полного потолка, поэтому дилеру всегда остаётся
    резерв, и троттлинг фона его не задевает. Вес дилера копится в
    резервном пуле и отслеживается отдельно (dealer_pressure). Ордера по
    природе шлёт только дилер — их частотное окно целиком принадлежит ему."""

    WEIGHT_WINDOW = 60.0        # сек, окно веса («минута биржи»)
    ORDER_BURST_WINDOW = 10.0   # сек, окно частоты ордеров

    def __init__(self, weight_limit=2000.0, order_burst_limit=50,
                 reserve_frac=0.25):
        self.weight_limit = float(weight_limit)
        self.order_burst_limit = int(order_burst_limit)
        self.reserve_frac = min(0.9, max(0.0, float(reserve_frac)))
        # Общий (фоновый) пул и резерв дилера — две части одного потолка.
        self.bg_weight_limit = self.weight_limit * (1.0 - self.reserve_frac)
        self.dlr_weight_limit = self.weight_limit * self.reserve_frac
        self._weights = collections.deque()   # (mono_ts, вес, dealer?)
        self._orders = collections.deque()    # mono_ts отправленных ордеров

    # ---------- учёт ----------

    def note(self, weight=1.0, dealer=False):
        """Отметить уходящий запрос (вес > 0). Вызывать ДО отправки.

        dealer=True — запрос дилера (приватное/ордера): идёт в резервный
        пул, фон его не трогает. По умолчанию False — фоновый опрос."""
        if weight <= 0:
            return
        now = time.monotonic()
        self._prune(now)
        self._weights.append((now, float(weight), bool(dealer)))

    def note_order(self):
        """Отметить отправку ордера (создание/отмена — биржа считает оба).

        Ордера шлёт только дилер, поэтому их частотное окно целиком его."""
        now = time.monotonic()
        self._prune(now)
        self._orders.append(now)

    # ---------- показания ----------

    def _prune(self, now):
        edge_w = now - self.WEIGHT_WINDOW
        while self._weights and self._weights[0][0] < edge_w:
            self._weights.popleft()
        edge_o = now - self.ORDER_BURST_WINDOW
        while self._orders and self._orders[0] < edge_o:
            self._orders.popleft()

    def weight_last_min(self, dealer=None):
        """Суммарный вес ушедших за последнюю минуту запросов.

        dealer=None — весь вес; False — только фон; True — только дилер."""
        self._prune(time.monotonic())
        if dealer is None:
            return sum(w for _, w, _ in self._weights)
        return sum(w for _, w, d in self._weights if d == dealer)

    def orders_last_10s(self):
        """Ордеров, отправленных за последние 10 секунд (от свежих к старым)."""
        now = time.monotonic()
        n = 0
        for ts in reversed(self._orders):
            if now - ts >= self.ORDER_BURST_WINDOW:
                break
            n += 1
        return n

    def pressure(self):
        """Занятость ПОЛНЫХ потолков, 0..1+: максимум по ресурсам (здоровье)."""
        p = ((self.weight_last_min() / self.weight_limit)
             if self.weight_limit > 0 else 0.0)
        b = ((self.orders_last_10s() / self.order_burst_limit)
             if self.order_burst_limit > 0 else 0.0)
        return max(p, b)

    def dealer_pressure(self):
        """Занятость РЕЗЕРВА дилера (вес), 0..1 — для наблюдения в Здоровье."""
        if self.dlr_weight_limit <= 0:
            return 0.0
        return self.weight_last_min(dealer=True) / self.dlr_weight_limit

    def throttled(self, frac=0.75):
        """Пора гасить ФОНОВЫЕ опросы: фон занял долю frac СВОЕГО пула.

        Именно фон зовёт это (Сборщик, OI). Порог считается от общего
        (фонового) пула, а не полного потолка — разница и есть резерв
        дилеру."""
        bg = (self.bg_weight_limit if self.bg_weight_limit > 0
              else self.weight_limit)
        return (self.weight_last_min(dealer=False) / bg) >= frac

    async def wait_for_order_slot(self, timeout=12.0, step=0.4):
        """Дождаться освобождения окна в частотном лимите ордеров.

        Возвращает True, если слот нашёлся; False — таймаут (вызывающий
        решает: для защитных стопов ордер уходит с громким логом)."""
        if self.order_burst_limit <= 0:
            return True
        deadline = time.monotonic() + timeout
        while self.orders_last_10s() >= self.order_burst_limit:
            if time.monotonic() >= deadline:
                return False
            await asyncio.sleep(step)
        return True


class Quoter(NeirModule):
    """Модуль-обёртка над книгой лимитов (поднимается первым, приоритет 5)."""

    manifest = Manifest(name="quoter", title="Квотник (лимиты биржи)",
                        provides=("quota",), priority=5)

    def __init__(self, kernel):
        super().__init__(kernel)
        self.ledger = None          # строится в on_start из конфига
        self._throttle_frac = 0.75

    async def on_start(self):
        c = self.kernel.cfg
        self._throttle_frac = c.quota_throttle_frac
        self.ledger = QuotaLedger(weight_limit=c.quota_weight_limit,
                                  order_burst_limit=c.quota_order_burst,
                                  reserve_frac=c.quota_reserve_frac)
        self.log.info("Квотник: вес %d/мин (фон %d + резерв дилера %d), "
                      "ордеров %d за 10с, тормоз фона при %.0f%% его пула",
                      c.quota_weight_limit,
                      int(self.ledger.bg_weight_limit),
                      int(self.ledger.dlr_weight_limit),
                      c.quota_order_burst,
                      self._throttle_frac * 100)

    # ---------- API для остальных модулей ----------

    def note(self, weight=1.0, dealer=False):
        """Учёт ушедшего запроса (вызывать ДО отправки). dealer=True — дилер."""
        if self.ledger is not None:
            self.ledger.note(weight, dealer=dealer)

    def note_order(self):
        """Учёт отправленного ордера."""
        if self.ledger is not None:
            self.ledger.note_order()

    async def wait_for_order_slot(self, timeout=12.0):
        """Дождаться слота в частотном лимите ордеров. True/False (таймаут)."""
        if self.ledger is None:
            return True
        return await self.ledger.wait_for_order_slot(timeout)

    def throttled(self):
        """Порог тормоза достигнут — фоновым опросам пропускать шаг."""
        if self.ledger is None:
            return False
        return self.ledger.throttled(self._throttle_frac)

    # ---------- здоровье ядра ----------

    def health_note(self):
        """Строка для Здоровья: фон, резерв дилера и ордера в реальном времени."""
        if self.ledger is None:
            return "КВОТЫ: журнал не построен"
        bg = self.ledger.weight_last_min(dealer=False)
        dlr = self.ledger.weight_last_min(dealer=True)
        b = self.ledger.orders_last_10s()
        note = ("КВОТЫ: фон %.0f/%d, дилер %.0f/%d (резерв %.0f%%), "
                "орд.%ds %d/%d") % (
            bg, int(self.ledger.bg_weight_limit),
            dlr, int(self.ledger.dlr_weight_limit),
            self.ledger.dealer_pressure() * 100,
            int(QuotaLedger.ORDER_BURST_WINDOW), b,
            self.ledger.order_burst_limit)
        if self.throttled():
            note += " — ТОРМОЗ"
        return note
