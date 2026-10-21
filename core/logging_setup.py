"""Единая настройка журнала: файл с ротацией + консоль.

Правило проекта: конфигурация журнала ровно одна — эта функция.
"""
import logging
import sys
from logging.handlers import RotatingFileHandler


def setup_logging(cfg) -> None:
    logs_dir = cfg.root / "logs"
    logs_dir.mkdir(exist_ok=True)

    root = logging.getLogger()
    root.setLevel(getattr(logging, cfg.log_level.upper(), logging.INFO))

    fmt = logging.Formatter(
        "%(asctime)s.%(msecs)03d [%(levelname)-8s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = RotatingFileHandler(
        logs_dir / "neiros.log",
        maxBytes=cfg.rotate_mb * 1024 * 1024,
        backupCount=cfg.rotate_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(fmt)
    root.addHandler(stream_handler)

    # Чтобы кириллица не ломалась в консоли Windows
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    # Шум стандартных библиотек приглушаем
    logging.getLogger("asyncio").setLevel(logging.WARNING)
