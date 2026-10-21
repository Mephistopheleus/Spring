# -*- coding: utf-8 -*-
"""Эталон полного сброса (не часть системы): собирает config/config.default.ini.

Источник — текущий config/config.ini (структура и комментарии). Поверх:
ручки из modules/_knobs.py — на утверждённые старты, доверия в [tuner] —
в нейтраль 0.5, остальное — как есть.

Полный сброс по решению хозяина:
    1) скопировать config/config.default.ini на config/config.ini;
    2) удалить файл базы Тюнера (лежит рядом со Складом, имя Склада
       с расширением .base, например data/neiros.base) — учёт заново;
    3) Склад (data/neiros.db) чистить по желанию — история сделок.

Запуск: python _dump_defaults.py
"""
import re
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from modules._knobs import ALL_KNOBS            # noqa: E402

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "config" / "config.ini"
DST = ROOT / "config" / "config.default.ini"

SEC_RE = re.compile(r"^\s*\[(?P<name>[^\]]+)\]\s*$")
KEY_RE = re.compile(r"^(?P<indent>\s*)(?P<key>[^=\s;#][^=]*?)\s*=")


def fmt(value):
    """Число — компактно (1.0 -> 1, 0.50 -> 0.5)."""
    try:
        num = float(value)
    except (TypeError, ValueError):
        return str(value)
    if num == int(num):
        return str(int(num))
    return "%g" % num


def main():
    lines = SRC.read_text(encoding="utf-8").splitlines()
    section = ""
    out = []
    touched = 0
    for line in lines:
        m = SEC_RE.match(line)
        if m:
            section = m.group("name").strip().lower()
            out.append(line)
            continue
        value = None
        m = KEY_RE.match(line)
        if m:
            key = m.group("key").strip()
            if section == "tuner" and key.startswith("trust__"):
                value = 0.5
            else:
                for (sec, k, start, _lo, _hi) in ALL_KNOBS.values():
                    if sec == section and k == key:
                        value = start
                        break
        if value is None:
            out.append(line)
            continue
        touched += 1
        out.append("%s%s = %s" % (m.group("indent"), m.group("key"),
                                  fmt(value)))
    header = [
        "# ЭТАЛОН ПОЛНОГО СБРОСА NeirOS-M",
        "# собран _dump_defaults.py %s" % datetime.now().strftime("%d.%m.%Y %H:%M"),
        "#",
        "# Сброс: 1) скопировать файл на config/config.ini;",
        "#        2) удалить файл базы Тюнера (рядом со Складом,",
        "#           имя Склада с расширением .base) — учёт заново;",
        "#        3) Склад (data/neiros.db) чистить по желанию.",
        "# Ручки стоят на утверждённых стартах, доверия — в нейтрали 0.5.",
        "",
    ]
    DST.write_text("\n".join(header + out) + "\n", encoding="utf-8")
    print("Эталон собран: %s (правок: %d)" % (DST, touched))


if __name__ == "__main__":
    main()
