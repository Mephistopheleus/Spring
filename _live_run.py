"""Живой прогон NeirOS-M на реальных данных (демо). Не часть системы.

Поднимает ПОЛНОЕ ядро (автопоиск всех модулей, UI выключен), работает
заданное время, затем корректно останавливается и пишет в журнал
сводку здоровья. Запуск:  python -X utf8 _live_run.py [секунд=240]
"""
import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from core.config import KernelConfig          # noqa: E402
from core.kernel import Kernel                # noqa: E402
from core.logging_setup import setup_logging  # noqa: E402


async def run(seconds: int, ui: bool = False):
    cfg = KernelConfig()
    if not ui:
        cfg.override("ui", "enabled", "false")
    setup_logging(cfg)
    log = logging.getLogger("NeirOS.LIVE")
    log.info("=== ЖИВОЙ ПРОГОН: %d сек, символ %s ===", seconds, cfg.symbol)

    kernel = Kernel(cfg)                     # полный состав, автопоиск
    task = asyncio.create_task(kernel.run())
    try:
        await asyncio.wait_for(asyncio.shield(task), timeout=seconds)
    except asyncio.TimeoutError:
        log.info("Время прогона вышло — останавливаюсь")
    kernel.request_stop()
    try:
        await asyncio.wait_for(task, timeout=cfg.stop_timeout + 15)
    except Exception:
        task.cancel()
    await kernel.shutdown()
    log.info("ИТОГ ПРОГОНА: %s", kernel.health_line())
    log.info("=== ЖИВОЙ ПРОГОН ЗАВЕРШЁН ===")


if __name__ == "__main__":
    secs = int(sys.argv[1]) if len(sys.argv) > 1 else 240
    ui = "--ui" in sys.argv[2:]
    asyncio.run(run(secs, ui))
