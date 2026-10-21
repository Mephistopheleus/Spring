"""Шина NeirOS-M — способ общения модулей.

Утверждённые правила:
1. Шина не знает про содержимое — только конверты с ярлыками.
2. Отправитель публикует на «доску объявлений» — копии в ящики подписчиков
   ярлыка; ЛИБО отправляет личный конверт — он лежит у адресата, пока
   его обработчик не вернётся без ошибки (это и есть подтверждение).
3. Конверты неизменяемы; ящики ограничены, свежее важнее старого.
4. У конверта есть срок жизни (TTL); истёкший — не доставляется.
5. На «доске последних значений» хранится самое свежее значение ярлыка.
6. Шина ничего не решает и не проверяет — только доставка.
7. Подписки объявляют сами модули; конфигурация шину не касается.
"""
import asyncio
import collections
import inspect
import itertools
import logging
import time


class Envelope:
    """Конверт «доски объявлений». После публикации — только чтение."""

    __slots__ = ("seq", "label", "payload", "created", "ttl", "sender")

    def __init__(self, seq, label, payload, created, ttl, sender):
        self.seq = seq
        self.label = label
        self.payload = payload
        self.created = created
        self.ttl = ttl
        self.sender = sender

    def is_expired(self, now=None):
        if self.ttl is None:
            return False
        if now is None:
            now = time.monotonic()
        return now - self.created > self.ttl


class PersonalEnvelope:
    """Личный конверт. Подтверждается, когда обработчик адресата
    вернулся без ошибки."""

    __slots__ = ("seq", "payload", "created", "ttl", "sender",
                 "recipient", "acknowledged")

    def __init__(self, seq, payload, created, ttl, sender, recipient):
        self.seq = seq
        self.payload = payload
        self.created = created
        self.ttl = ttl
        self.sender = sender
        self.recipient = recipient
        self.acknowledged = False

    def is_expired(self, now=None):
        if self.ttl is None:
            return False
        if now is None:
            now = time.monotonic()
        return now - self.created > self.ttl


class Box:
    """Личный ящик подписчика: очередь; при переполнении выбрасывается
    самое старое (свежее важнее)."""

    __slots__ = ("items", "event")

    def __init__(self, maxlen=None):
        self.items = collections.deque(maxlen=maxlen)
        self.event = asyncio.Event()

    def put(self, item):
        overflow = self.items.maxlen is not None and len(self.items) >= self.items.maxlen
        self.items.append(item)
        self.event.set()
        return overflow

    async def get(self):
        while not self.items:
            self.event.clear()
            await self.event.wait()
        return self.items.popleft()


class _Sub:
    """Подписка: владелец, ярлык, ящик, обработчик, счётчик доставок."""

    __slots__ = ("owner", "label", "box", "handler", "delivered")

    def __init__(self, owner, label, box, handler):
        self.owner = owner
        self.label = label
        self.box = box
        self.handler = handler
        self.delivered = 0


class Bus:
    """Шина. Владелец — ядро; модули пользуются ею через NeirModule."""

    def __init__(self, kernel):
        self.kernel = kernel
        self.log = logging.getLogger("NeirOS.BUS")
        self._subs = {}        # ярлык -> [_Sub]
        self._last = {}        # ярлык -> Envelope (доска последних значений)
        self._mailboxes = {}   # имя адресата -> Box (без ограничений)
        self._inflight = {}    # имя адресата -> [кол-во в обработке, время старейшего]
        self._deferred = {}    # имя адресата -> [(время, отправитель, payload, ttl)]
        self._tasks = set()
        self._seq = itertools.count(1)
        # Счётчики для здоровья
        self.published = 0
        self.delivered = 0
        self.dropped_ttl = 0
        self.dropped_box = 0
        self.handler_errors = 0
        self.personal_sent = 0
        self.personal_acked = 0
        self.personal_failed = 0
        self.personal_deferred = 0    # отложено: адресат ещё не открыл ящик
        self.personal_dropped = 0     # потеряно окончательно (лимит ожидания/очереди)

    def _track(self, task):
        """Регистрация задачи шины: учёт и видимость падений в журнале."""
        self._tasks.add(task)

        def _done(finished):
            self._tasks.discard(finished)
            if finished.cancelled():
                return
            exc = finished.exception()
            if exc is not None:
                self.log.error("Задача шины упала: %r", exc)

        task.add_done_callback(_done)

    # ---------- «доска объявлений» ----------

    def publish(self, sender, label, payload, ttl=None):
        env = Envelope(next(self._seq), label, payload, time.monotonic(), ttl, sender)
        self.published += 1
        self._last[label] = env
        subs = self._subs.get(label)
        if subs:
            for sub in list(subs):
                if sub.box.put(env):
                    self.dropped_box += 1
        return env

    def subscribe(self, owner, label, handler, box_size=None):
        sub = _Sub(owner, label, Box(box_size), handler)
        self._subs.setdefault(label, []).append(sub)
        task = asyncio.create_task(self._consume_broadcast(sub),
                                   name=f"bus:{owner}:{label}")
        self._track(task)

        def unsubscribe():
            task.cancel()
            bucket = self._subs.get(label)
            if bucket and sub in bucket:
                bucket.remove(sub)

        return unsubscribe

    async def _consume_broadcast(self, sub):
        try:
            while True:
                env = await sub.box.get()
                if env.is_expired():
                    self.dropped_ttl += 1
                    continue
                try:
                    result = sub.handler(env)
                    if inspect.iscoroutine(result):
                        await result
                    sub.delivered += 1
                    self.delivered += 1
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self.handler_errors += 1
                    self.kernel.module_error(sub.owner)
                    self.log.exception("Ошибка обработчика: %s ← '%s'",
                                       sub.owner, sub.label)
        except asyncio.CancelledError:
            pass

    def get_last(self, label):
        """Последнее опубликованное значение ярлыка (Envelope или None)."""
        return self._last.get(label)

    # ---------- личные конверты ----------

    def mailbox(self, owner, handler):
        """Личный ящик адресата. Хранится до подтверждения, не выбрасывается."""
        if owner in self._mailboxes:
            raise RuntimeError(f"Личный ящик '{owner}' уже открыт")
        box = Box(maxlen=None)          # личное не теряем никогда
        self._mailboxes[owner] = box
        self._inflight[owner] = [0, 0.0]
        task = asyncio.create_task(self._consume_personal(owner, box, handler),
                                   name=f"bus-mail:{owner}")
        self._track(task)

        def close():
            task.cancel()
            self._mailboxes.pop(owner, None)
            self._inflight.pop(owner, None)

        self._flush_deferred(owner)
        return close

    def send(self, sender, recipient, payload, ttl=None):
        """Личный конверт конкретному адресату.

        Адресат может ещё не открыть ящик (старт/перезапуск). Конверт
        НЕ теряется молча: он ждёт в отложенной очереди и уходит, как
        только ящик откроется. Окончательно теряется только по явным
        причинам (переполнение очереди или истёкшее время ожидания) —
        каждая такая потеря считается и видна в здоровье.
        """
        box = self._mailboxes.get(recipient)
        if box is None:
            return self._defer(sender, recipient, payload, ttl)
        env = PersonalEnvelope(next(self._seq), payload, time.monotonic(), ttl,
                               sender, recipient)
        self.personal_sent += 1
        box.put(env)
        return env

    # ---------- отложенные личные конверты ----------

    def _defer_max(self):
        return int(getattr(self.kernel.cfg, "envelope_defer_max", 200))

    def _defer_ttl(self):
        return float(getattr(self.kernel.cfg, "envelope_defer_ttl_sec", 300.0))

    def _defer(self, sender, recipient, payload, ttl):
        """Ящик адресата ещё закрыт — конверт ждёт открытия."""
        queue = self._deferred.setdefault(recipient, [])
        env = PersonalEnvelope(next(self._seq), payload, time.monotonic(), ttl,
                               sender, recipient)
        queue.append(env)
        self.personal_sent += 1
        self.personal_deferred = sum(len(q) for q in self._deferred.values())
        limit = self._defer_max()
        if len(queue) > limit:
            # свежее важнее: уходит самое старое (обычно это уже неактуальная
            # команда), но потеря фиксируется в счётчиках и в журнале
            gone = queue.pop(0)
            self.personal_dropped += 1
            self.log.error("Личный конверт для '%s' ПОТЕРЯН: очередь ожидания "
                           "переполнена (>%d) — отправитель '%s', возраст %.0f с",
                           recipient, limit, sender,
                           time.monotonic() - gone.created)
        else:
            self.log.warning("Личный конверт для '%s' отложен: адресат ещё не "
                             "открыл ящик (ждёт до %.0f с, в очереди %d)",
                             recipient, self._defer_ttl(), len(queue))
        return env

    def _flush_deferred(self, recipient):
        """Ящик открылся — отдаём всё накопленное, в порядке прихода."""
        queue = self._deferred.pop(recipient, None)
        if not queue:
            return
        box = self._mailboxes.get(recipient)
        if box is None:                  # ящик закрыли сразу после открытия
            self._deferred[recipient] = queue
            return
        limit_sec = self._defer_ttl()
        now = time.monotonic()
        expired = 0
        for env in queue:
            if env.ttl is not None and env.is_expired(now):
                self.dropped_ttl += 1
                expired += 1
                continue
            if now - env.created > limit_sec:
                self.personal_dropped += 1
                expired += 1
                self.log.error("Личный конверт для '%s' ПОТЕРЯН: ждал %.0f с "
                               "без ящика (отправитель '%s')",
                               recipient, now - env.created, env.sender)
                continue
            box.put(env)
        self.personal_deferred = sum(len(q) for q in self._deferred.values())
        self.log.info("Отложенные конверты для '%s' доставлены: %d, "
                      "устаревших отброшено: %d",
                      recipient, len(queue) - expired, expired)

    async def _consume_personal(self, owner, box, handler):
        try:
            while True:
                env = await box.get()
                if env.is_expired():
                    self.dropped_ttl += 1
                    continue
                inflight = self._inflight.get(owner)
                if inflight:
                    inflight[0] += 1
                    if inflight[1] == 0.0:
                        inflight[1] = env.created
                try:
                    result = handler(env)
                    if inspect.iscoroutine(result):
                        await result
                    env.acknowledged = True
                    self.personal_acked += 1
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self.personal_failed += 1
                    self.kernel.module_error(owner)
                    self.log.exception("Ошибка обработки личного конверта: %s", owner)
                finally:
                    if inflight:
                        inflight[0] -= 1
                        if inflight[0] <= 0:
                            inflight[0] = 0
                            inflight[1] = 0.0
        except asyncio.CancelledError:
            pass

    def personal_pending(self):
        """Сколько личных конвертов ждут обработки (в очереди ящика + в
        работе), и возраст старейшего в секундах.

        Учитываются ВСЕ недоставленные конверты: и лежащие в очереди
        ящика, и уже взятые обработчиком. Раньше считались только взятые —
        адресат мог упираться в очередь, а здоровье показывало ноль.
        """
        total = sum(len(box.items) for box in self._mailboxes.values())
        total += sum(len(q) for q in self._deferred.values())
        oldest = 0.0
        now = time.monotonic()
        for box in self._mailboxes.values():
            if box.items:
                oldest = max(oldest, now - box.items[0].created)
        for queue in self._deferred.values():
            if queue:
                oldest = max(oldest, now - queue[0].created)
        for count, oldest_created in self._inflight.values():
            total += count
            if oldest_created:
                oldest = max(oldest, now - oldest_created)
        return total, oldest

    # ---------- закрытие ----------

    async def close(self):
        """Отмена всех задач доставки (вызывается ядром при остановке)."""
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)
        self._tasks.clear()
        self._subs.clear()
        # Недоставленные конверты не теряем молча: считаем и пишем в журнал
        waiting = sum(len(box.items) for box in self._mailboxes.values())
        waiting += sum(len(q) for q in self._deferred.values())
        if waiting:
            self.log.error("Шина остановлена с %d недоставленными личными "
                           "конвертами — адресат не успел обработать!", waiting)
            self.personal_dropped += waiting
        self._mailboxes.clear()
        self._inflight.clear()
        self._deferred.clear()
        self.personal_deferred = 0
