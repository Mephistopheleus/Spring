"""Мост между «главным залом» (asyncio) и потоком экрана (Qt).

Экран только читает готовые данные (утверждено). Мост — простая
полка с замком: главный зал кладёт, поток Qt берёт копию.
Команды пользователя позже пойдут личными конвертами через шину.
"""
import threading


class UiBridge:
    def __init__(self):
        self._lock = threading.Lock()
        self._data = {}
        self.closing = False          # сигнал экрану завершиться

    def update(self, **kwargs):
        with self._lock:
            self._data.update(kwargs)

    def snapshot(self):
        with self._lock:
            return dict(self._data)
