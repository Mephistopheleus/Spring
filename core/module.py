"""Контракт модуля NeirOS-M: манифест + жизненный цикл + удобства ядра.

Модуль объявляет манифест (имя, что публикует, что требует, приоритет,
таймауты) и реализует on_start/on_stop. Подписки на ярлыки модуль
объявляет сам — обычно внутри on_start.
"""
import abc
import logging
from dataclasses import dataclass
from typing import Callable, ClassVar, Tuple


@dataclass(frozen=True)
class Manifest:
    """Паспорт модуля. Ядро читает его до запуска."""
    name: str
    title: str = ""
    provides: Tuple[str, ...] = ()   # ярлыки, которые публикует
    requires: Tuple[str, ...] = ()   # ярлыки, которые нужны на входе
    priority: int = 100              # чем меньше, тем раньше запуск
    init_timeout: float = 30.0       # предел времени on_start
    stop_timeout: float = 10.0       # предел времени on_stop
    tick_timeout: float = 10.0       # предел одного шага периодической работы


class NeirModule(abc.ABC):
    manifest: ClassVar[Manifest]

    def __init__(self, kernel):
        self.kernel = kernel
        self.log = logging.getLogger("NeirOS." + self.manifest.name)

    # ---------- жизненный цикл ----------

    async def on_start(self) -> None:
        """Подготовка модуля: подписки, ресурсы."""

    async def on_stop(self) -> None:
        """Освобождение ресурсов. Вызывается в обратном порядке запуска."""

    # ---------- удобные обёртки над ядром (всё учитывается в здоровье) ----------

    def subscribe(self, label: str, handler: Callable, box_size: int = None):
        """Подписка на ярлык «доски объявлений». Возвращает функцию отписки."""
        return self.kernel.subscribe(self.manifest.name, label, handler, box_size)

    def publish(self, label: str, payload, ttl: float = None):
        """Опубликовать конверт на доску объявлений."""
        return self.kernel.publish(self.manifest.name, label, payload, ttl)

    def send(self, recipient: str, payload, ttl: float = None):
        """Личный конверт конкретному модулю (хранится до подтверждения)."""
        return self.kernel.send(self.manifest.name, recipient, payload, ttl)

    def mailbox(self, handler: Callable):
        """Открыть личный ящик этого модуля (для приёма личных конвертов)."""
        return self.kernel.mailbox(self.manifest.name, handler)

    def spawn(self, coro):
        """Фоновая задача модуля; ядро проследит за её завершением."""
        return self.kernel.spawn(self.manifest.name, coro)

    def every(self, interval_sec: float, callback: Callable):
        """Свой ритм работы: периодический вызов с таймаутом шага из манифеста."""
        return self.kernel.every(self.manifest.name, interval_sec, callback,
                                 self.manifest.tick_timeout)

    def get_last(self, label: str):
        """Самое свежее значение ярлыка с доски значений (Envelope или None)."""
        return self.kernel.bus.get_last(label)
