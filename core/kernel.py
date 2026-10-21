"""Ядро NeirOS-M.

Зоны ответственности (утверждены):
- автопоиск модулей (рекурсивно, без списков);
- интеграция: порядок по зависимостям, таймауты, изоляция сбоев;
- шина (core/bus.py);
- здоровье: сводка в журнал, тревоги по личным конвертам;
- корректная остановка в обратном порядке запуска.
"""
import asyncio
import collections
import logging
import time

from core.bus import Bus
from core.discovery import discover, order_modules

# Состояния модуля
NEW = "новый"
STARTING = "запуск"
RUNNING = "работает"
FAILED = "сбой"
STOPPING = "остановка"
STOPPED = "остановлен"
STOP_FAILED = "ошибка остановки"

KERNEL_OWNER = "KERNEL"   # владелец служебных задач ядра


class Kernel:
    def __init__(self, cfg=None, scan_modules=True):
        if cfg is None:
            from core.config import KernelConfig
            cfg = KernelConfig()
        self.cfg = cfg
        self.scan_modules = scan_modules   # выключатель автопоиска (для тестов)
        self.log = logging.getLogger("NeirOS.KERNEL")
        self.bus = Bus(self)
        self.modules = {}          # имя -> модуль
        self._extra = []           # вручную зарегистрированные (тесты)
        self._order = []           # порядок запуска
        self._states = {}
        self._errors = collections.Counter()
        self._tasks = {}           # владелец -> set(задачи)
        self._subs = {}            # владелец -> [функции отписки]
        self._stop_event = asyncio.Event()
        self._shutdown_done = False

    # ---------- API для модулей (учитывается в здоровье) ----------

    def subscribe(self, owner, label, handler, box_size=None):
        unsub = self.bus.subscribe(owner, label, handler, box_size)
        self._subs.setdefault(owner, []).append(unsub)
        return unsub

    def publish(self, owner, label, payload, ttl=None):
        return self.bus.publish(owner, label, payload, ttl)

    def send(self, owner, recipient, payload, ttl=None):
        return self.bus.send(owner, recipient, payload, ttl)

    def mailbox(self, owner, handler):
        return self.bus.mailbox(owner, handler)

    def spawn(self, owner, coro):
        name = getattr(coro, "__name__", "") or owner
        task = asyncio.create_task(coro, name=f"{owner}:{name}")
        self._tasks.setdefault(owner, set()).add(task)
        task.add_done_callback(lambda t: self._on_task_done(owner, t))
        return task

    def every(self, owner, interval, callback, timeout=None):
        """Свой ритм модуля: периодический вызов с таймаутом шага."""
        timeout = timeout or self.cfg.tick_timeout

        async def _loop():
            while True:
                started = time.monotonic()
                try:
                    await asyncio.wait_for(callback(), timeout=timeout)
                except asyncio.CancelledError:
                    raise
                except asyncio.TimeoutError:
                    # Таймаут ШАГА — не сбой модуля (сеть/биржа не успели):
                    # пропуск тика, счётчик «ош.N» не трогаем — важно для Здоровья.
                    self.log.warning("Шаг задачи модуля %s превысил таймаут "
                                     "%.0f с — пропущен", owner, timeout)
                except Exception:
                    self._errors[owner] += 1
                    self.log.exception("Сбой периодической задачи модуля %s", owner)
                spent = time.monotonic() - started
                await asyncio.sleep(max(0.05, interval - spent))

        return self.spawn(owner, _loop())

    def module_error(self, owner):
        self._errors[owner] += 1

    def register(self, module):
        """Ручная регистрация (для тестов; боевые модули ищутся сами)."""
        self._extra.append(module)

    def _on_task_done(self, owner, task):
        bucket = self._tasks.get(owner)
        if bucket:
            bucket.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            self._errors[owner] += 1
            self.log.error("Фоновая задача модуля %s упала: %r", owner, exc)

    # ---------- фазы жизни ----------

    async def run(self):
        """Запуск модулей и работа до запроса остановки (request_stop)."""
        await self._start()
        self.spawn(KERNEL_OWNER, self._health_loop())
        self.log.info("[RUN] Ядро в рабочем режиме. Активных модулей: %d",
                      len(self.modules))
        try:
            await self._stop_event.wait()
        except asyncio.CancelledError:
            raise   # штатная остановка — через request_stop(); отмену ведёт main.py

    def request_stop(self):
        self.log.info("[STOP] Получен запрос на остановку ядра")
        self._stop_event.set()

    async def shutdown(self):
        """Корректная остановка: on_stop в обратном порядке, снятие подписок,
        отмена хвостовых задач, закрытие шины."""
        if self._shutdown_done:
            return
        self._shutdown_done = True
        self.log.info("[STOP] Корректная остановка: %d модулей, обратный порядок",
                      len(self._order))

        for mod in reversed(self._order):
            name = mod.manifest.name
            if self._states.get(name) == FAILED:
                self._drop_module_assets(name)
                continue
            self._states[name] = STOPPING
            try:
                await asyncio.wait_for(mod.on_stop(),
                                       timeout=mod.manifest.stop_timeout)
                self._states[name] = STOPPED
                self.log.info("[STOP] %s — остановлен", name)
            except asyncio.TimeoutError:
                self._states[name] = STOP_FAILED
                self.log.error("[STOP] %s — таймаут остановки", name)
            except Exception:
                self._states[name] = STOP_FAILED
                self.log.exception("[STOP] %s — ошибка при остановке", name)
            finally:
                self._drop_module_assets(name)

        # Хвостовые задачи (включая здоровье ядра)
        tail = [t for bucket in self._tasks.values() for t in bucket]
        for task in tail:
            task.cancel()
        if tail:
            await asyncio.gather(*tail, return_exceptions=True)

        await self.bus.close()
        self.log.info(self.health_line())
        self.log.info("[STOP] Ядро полностью остановлено.")

    async def _start(self):
        raw = list(self._extra)
        if self.scan_modules:
            raw += discover(self)

        # Защита от дублей по имени: остаётся первый (ручные — раньше найденных)
        unique = []
        seen = set()
        for mod in raw:
            if mod.manifest.name in seen:
                self.log.error("[START] Дубликат имени модуля '%s' (%s) — пропущен",
                               mod.manifest.name, type(mod).__module__)
                continue
            seen.add(mod.manifest.name)
            unique.append(mod)

        ordered = order_modules(unique, self.log)
        self._order = ordered
        providers = {label for m in ordered for label in m.manifest.provides}

        for mod in ordered:
            name = mod.manifest.name
            if name in self.modules:
                self.log.error("[START] Дубликат имени модуля '%s' — пропущен", name)
                continue
            self.modules[name] = mod
            self._states[name] = STARTING
            for label in mod.manifest.requires:
                if label not in providers:
                    self.log.warning("[START] %s: ярлык '%s' пока никто не публикует",
                                     name, label)
            try:
                await asyncio.wait_for(mod.on_start(),
                                       timeout=mod.manifest.init_timeout)
                self._states[name] = RUNNING
                self.log.info("[START] %s — готов", name)
            except asyncio.TimeoutError:
                self._states[name] = FAILED
                self.log.error("[START] %s — таймаут инициализации", name)
                self._drop_module_assets(name)
            except Exception:
                self._states[name] = FAILED
                self.log.exception("[START] %s — сбой инициализации", name)
                self._drop_module_assets(name)

        failed = [n for n, s in self._states.items() if s == FAILED]
        self.log.info("[START] Инициализация завершена: готово %d, сбоев %d",
                      len(self.modules) - len(failed), len(failed))

    def _drop_module_assets(self, name):
        """Снять подписки и отменить задачи модуля (при сбое и при остановке)."""
        for unsub in self._subs.pop(name, []):
            try:
                unsub()
            except Exception:
                self.log.exception("Ошибка снятия подписки %s", name)
        for task in self._tasks.pop(name, []):
            task.cancel()

    # ---------- здоровье ----------

    async def _health_loop(self):
        interval = self.cfg.health_interval
        while True:
            await asyncio.sleep(interval)
            self.log.info(self.health_line())
            pending, oldest = self.bus.personal_pending()
            if oldest > self.cfg.envelope_alarm_sec:
                self.log.warning("[HEALTH] Личные конверты ждут %.1f сек "
                                 "(порог %.1f) — адресат отстаёт!",
                                 oldest, self.cfg.envelope_alarm_sec)
            if self.bus.personal_dropped:
                # потеря конверта = возможно потерянная команда закрытия
                self.log.error("[HEALTH] ПОТЕРЯНО личных конвертов: %d "
                               "(отложено сейчас: %d) — проверьте сделки!",
                               self.bus.personal_dropped,
                               self.bus.personal_deferred)

    def health_line(self) -> str:
        """Однострочная сводка для журнала."""
        delivered = collections.defaultdict(int)
        for subs in self.bus._subs.values():
            for sub in subs:
                delivered[sub.owner] += sub.delivered
        parts = []
        for mod in self._order:
            name = mod.manifest.name
            parts.append("%s=%s(дост.%d,ош.%d)" % (
                name, self._states.get(name, "?"),
                delivered.get(name, 0), self._errors.get(name, 0)))
        pending, oldest = self.bus.personal_pending()
        line = "[HEALTH] " + (" | ".join(parts) if parts else "модулей нет")
        line += (f" || шина: опубл.{self.bus.published} дост.{self.bus.delivered} "
                 f"ttl.{self.bus.dropped_ttl} ящик.{self.bus.dropped_box} "
                 f"ош.{self.bus.handler_errors}")
        line += (f" || личные: отпр.{self.bus.personal_sent} "
                 f"подтв.{self.bus.personal_acked} сбои.{self.bus.personal_failed} "
                 f"отложено.{self.bus.personal_deferred} "
                 f"потеряно.{self.bus.personal_dropped} "
                 f"в_пути.{pending} старейш.{oldest:.1f}с")
        # ПОВЕРХНОСТЬ СВЕЧ (27.10): пустое кольцо = половина анализаторов
        # молчит, и по старым счётчикам это видно только через сутки. Пустые
        # кольца показываем сразу, отдельным сегментом.
        collector = self.modules.get("collector")
        note = getattr(collector, "health_note", None)
        if callable(note):
            line += " || " + note()

        # КВОТЫ БИРЖИ (Квотник): занятость весового и ордерного потолка.
        # Бан IP (-1042/-1003, час+) убивает ВСЕГДЫ, а не половину системы —
        # поэтому лимиты видны в строке здоровья отдельным сегментом.
        quoter = self.modules.get("quoter")
        q_note = getattr(quoter, "health_note", None)
        if callable(q_note):
            line += " || " + q_note()

        return line

    def health_snapshot(self) -> dict:
        """Структура для будущего экрана: всё здоровье в одном словаре."""
        delivered = collections.defaultdict(int)
        for subs in self.bus._subs.values():
            for sub in subs:
                delivered[sub.owner] += sub.delivered
        pending, oldest = self.bus.personal_pending()
        return {
            "modules": [
                {"name": mod.manifest.name,
                 "state": self._states.get(mod.manifest.name, "?"),
                 "delivered": delivered.get(mod.manifest.name, 0),
                 "errors": self._errors.get(mod.manifest.name, 0)}
                for mod in self._order
            ],
            "bus": {
                "published": self.bus.published,
                "delivered": self.bus.delivered,
                "dropped_ttl": self.bus.dropped_ttl,
                "dropped_box": self.bus.dropped_box,
                "handler_errors": self.bus.handler_errors,
                "personal_sent": self.bus.personal_sent,
                "personal_acked": self.bus.personal_acked,
                "personal_failed": self.bus.personal_failed,
                "personal_deferred": self.bus.personal_deferred,
                "personal_dropped": self.bus.personal_dropped,
                "personal_pending": pending,
                "personal_oldest_sec": oldest,
            },
        }

    @property
    def states(self) -> dict:
        return self._states
