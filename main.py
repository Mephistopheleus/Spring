"""NeirOS-M — точка входа. Запуск ядра и корректная остановка по Ctrl+C."""
import asyncio
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))


def force_utf8_console():
    """Русские буквы в консоли Windows при любом способе запуска."""
    if sys.platform != "win32":
        return
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        import ctypes
        ctypes.windll.kernel32.SetConsoleOutputCP(65001)
        ctypes.windll.kernel32.SetConsoleCP(65001)
    except Exception:
        pass
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


from core.config import KernelConfig          # noqa: E402
from core.kernel import Kernel                # noqa: E402
from core.logging_setup import setup_logging  # noqa: E402


async def _finish(kernel: Kernel, main_task: asyncio.Task):
    """Ctrl+C: просим ядро завершиться и дожидаемся полной остановки."""
    log = logging.getLogger("NeirOS.MAIN")
    kernel.request_stop()
    try:
        await asyncio.wait_for(asyncio.shield(main_task),
                               timeout=kernel.cfg.stop_timeout + 5)
    except asyncio.TimeoutError:
        log.error("Ядро не завершилось вовремя — принудительная отмена")
        main_task.cancel()
    except Exception:
        log.exception("Ошибка при остановке ядра")
    await kernel.shutdown()


def main():
    force_utf8_console()
    cfg = KernelConfig()
    setup_logging(cfg)
    log = logging.getLogger("NeirOS.MAIN")
    log.info("=== NeirOS-M запуск ===")

    kernel = Kernel(cfg)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    main_task = loop.create_task(kernel.run())
    try:
        loop.run_until_complete(main_task)
    except KeyboardInterrupt:
        log.info("Получен Ctrl+C — корректная остановка")
    except Exception:
        log.critical("Критическая ошибка ядра", exc_info=True)
    finally:
        if not main_task.done():
            loop.run_until_complete(_finish(kernel, main_task))
        else:
            loop.run_until_complete(kernel.shutdown())
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
        log.info("=== NeirOS-M остановлен ===")


if __name__ == "__main__":
    main()

