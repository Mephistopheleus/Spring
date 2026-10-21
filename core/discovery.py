"""Автопоиск модулей: рекурсивный скан папки modules/, без всяких списков.

Правила:
- файлы и папки, начинающиеся с «_», а также __init__.py — игнорируются;
- каждый найденный файл исполняется; модульными считаются классы NeirModule,
  определённые именно в этом файле;
- сбой одного файла не мешает остальным (изоляция);
- порядок запуска строится по зависимостям (requires/provides),
  затем по приоритету, затем по имени.
"""
import importlib.util
import logging
import os
import sys
from pathlib import Path

from core.module import NeirModule


def discover(kernel):
    """Находит и создаёт модули. Возвращает список экземпляров."""
    log = logging.getLogger("NeirOS.DISCOVERY")
    root = kernel.cfg.root / "modules"
    if not root.exists():
        log.warning("[DISCOVERY] Папка modules/ не найдена")
        return []

    files = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(d for d in dirnames
                             if not d.startswith("_") and d != "__pycache__")
        for filename in sorted(filenames):
            if not filename.endswith(".py"):
                continue
            if filename.startswith("_") or filename == "__init__.py":
                continue
            files.append(Path(dirpath) / filename)

    modules = []
    errors = 0
    disabled_total = 0
    for path in files:
        rel = path.relative_to(root).with_suffix("")
        modname = "neiros.modules." + ".".join(rel.parts)
        try:
            spec = importlib.util.spec_from_file_location(modname, path)
            module = importlib.util.module_from_spec(spec)
            sys.modules[modname] = module
            spec.loader.exec_module(module)
        except Exception:
            errors += 1
            log.exception("[DISCOVERY] Сбой загрузки файла %s", path)
            continue

        found = 0
        disabled = 0
        for attr in list(vars(module).values()):
            if (isinstance(attr, type) and issubclass(attr, NeirModule)
                    and attr is not NeirModule and attr.__module__ == modname):
                try:
                    instance = attr(kernel)
                except Exception:
                    errors += 1
                    log.exception("[DISCOVERY] Сбой создания модуля %s (%s)",
                                  attr.__name__, path)
                    continue
                # ВЫКЛЮЧАТЕЛЬ (27.10). Фильтр стоит ПОСЛЕ создания экземпляра:
                # имя модуля живёт в manifest.name, а оно известно только
                # после класса. Экземпляр при этом создан, но ничего не
                # запускал — on_start модуля ещё не вызывался, шина его не
                # видела, подписок ноль. Так что побочных эффектов нет.
                if not kernel.cfg.module_enabled(instance.manifest.name):
                    log.info("[DISCOVERY] %s → %s ВЫКЛЮЧЁН флагом [modules]",
                             rel.as_posix(), instance.manifest.name)
                    disabled += 1
                    disabled_total += 1
                    continue
                modules.append(instance)
                found += 1
                log.info("[DISCOVERY] %s → %s", rel.as_posix(), instance.manifest.name)
        # Предупреждение только когда в файле НЕТ ни одного класса
        # NeirModule — выключенные модули тут штатная работа флага, а не сбой
        # поиска, и молчать о них не надо, но и ругаться нельзя.
        if found == 0 and disabled == 0:
            log.warning("[DISCOVERY] %s: классов NeirModule не найдено — пропущен",
                        rel.as_posix())

    log.info("[DISCOVERY] Найдено модулей: %d, сбоев: %d, выключено: %d",
             len(modules), errors, disabled_total)
    return modules


def order_modules(modules, log):
    """Порядок запуска: зависимости → приоритет → имя. Циклы разрубаются."""
    info = {m.manifest.name: m for m in modules}
    providers = {}
    for m in modules:
        for label in m.manifest.provides:
            providers.setdefault(label, m.manifest.name)

    deps = {}
    for m in modules:
        needed = set()
        for label in m.manifest.requires:
            provider = providers.get(label)
            if provider and provider != m.manifest.name:
                needed.add(provider)
            elif label not in providers:
                log.warning("[ORDER] '%s' требует '%s' — пока никто не публикует "
                            "(запустится в общем порядке)", m.manifest.name, label)
        deps[m.manifest.name] = needed

    levels = {}
    resolved = set()
    remaining = set(info)
    level = 0
    while remaining:
        ready = sorted((n for n in remaining if deps[n] <= resolved),
                       key=lambda n: (info[n].manifest.priority, n))
        if not ready:
            log.warning("[ORDER] Цикл зависимостей: %s — разрубаю по приоритету",
                        sorted(remaining))
            ready = sorted(remaining, key=lambda n: (info[n].manifest.priority, n))
            for name in ready:
                levels[name] = level
            break
        for name in ready:
            levels[name] = level
            resolved.add(name)
        remaining.difference_update(ready)
        level += 1

    result = sorted(modules,
                    key=lambda m: (levels[m.manifest.name], m.manifest.priority,
                                   m.manifest.name))
    for m in result:
        log.info("[ORDER] уровень %d | приоритет %3d | %s",
                 levels[m.manifest.name], m.manifest.priority, m.manifest.name)
    return result
