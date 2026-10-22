"""Склад-матрица — НАСТОЯЩЕЕ системы в ОЗУ (утверждённая архитектура 18.10).

Два слоя данных (решение хозяина, REFACTOR_JOURNAL «Архитектура матрицы»):
- Матрица (этот модуль) — кэш текущей картины прогнозов аналитиков и
  среды. Перезапись ячейки = семантика «сейчас». Только ОЗУ: память
  ограничена геометрией ячеек, очистка по TTL.
- Журнал — таблица cards у Хранителя: история живёт ТОЛЬКО там (второй
  базы под прогнозы не строим — урок «neiros.base-призрака»).

Нить между слоями: ячейка хранит seq поглощённой карточки, у карточки
есть lineage. От любого среза матрицы путь в журнал: ячейка → card:N →
lineage → … — «аналитик, анализ, аналитик, анализ…» удлиняется данными,
а не кодом (новая схема «анализа анализов» = новая ступень lineage).

Ячейка: ключ (kind, axis) → {"value", "ts_ms", "seq"}; у полей с tf_sec
ось помечается ТФ («score@60», «score@14400») — ступени полотна живут
на разных полках и не затирают друг друга (20.10).

Адресная модель (Шаг 3 плана кросс-ТФ, 22.10):
- Дыра 3: ось-образующие поля TF_KEYS = ("tf_sec", "horizon_min") —
  прогноз с несколькими горизонтами на одном ТФ живёт на отдельных
  полках «score@60@h15»; карточки без horizon_min адресуются как
  раньше (обратная совместимость прогрева из журнала).
- Дыра 2: dict-поля рекурсивно разворачиваются в составные оси через
  точку («te_profile.fst»); list/tuple остаются значением «как есть».
  Глубина ограничена MAX_NEST (защита от циклов). Карточки журнала НЕ
  меняются — разворот только в адресной модели матрицы.
- Дыра 1: поле zone_id — вторая ось (решётка kind#axis#zone_id@ТФ):
  несколько живых зон одного ТФ сидят на разных ячейках; карточка без
  zone_id адресуется как раньше.

Значение хранится КАК ЕСТЬ (не сплющивается, не усредняется). SURFACE
(линзы) — первый жилец: тензор пишется строками по масштабам. Сценарист
(Узел 6) читает срезы матрицы и строит по ним срезы-прогнозы вместо
голосовалки Σ(вес×знак).
"""
import time

from core.module import Manifest, NeirModule
from modules._labels import CARD, MATRIX, SURFACE

WARMUP_CARDS = 400        # сколько последних карточек журнала берём на старте
TF_KEYS = ("tf_sec", "horizon_min")   # ось-образующие поля (дыра 3, 22.10)
ID_KEY = "zone_id"                    # решётка зон (дыра 1, 22.10)
MAX_NEST = 3                          # глубина разворота dict-полей (дыра 2)


def _fmt_axis_part(v):
    """Стабильный текстовый вид ось-образующего значения: int для целых
    (60, а не '60.0'), иначе round(…, 1) (horizon_min 15.0 → '15')."""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return str(int(f)) if f == int(f) else str(round(f, 1))


def _flatten(fields):
    """Дыра 2: dict-поля карточки → составные оси «a.b.c» (рекурсивно,
    до MAX_NEST уровней; цикл по self/родителю отсекается depth).
    list/tuple/set — значение «как есть», не разворачиваем: это вектор.
    Плоские карточки проходят нетронутыми (обратная совместимость)."""
    out = []

    def walk(prefix, obj, depth):
        if isinstance(obj, dict) and depth < MAX_NEST:
            for k, v in obj.items():
                walk(f"{prefix}.{k}" if prefix else str(k), v, depth + 1)
        else:
            out.append((prefix, obj))

    for axis, value in fields.items():
        walk(str(axis), value, 0)
    return out


class Matrix(NeirModule):
    manifest = Manifest(
        name="matrix", title="Склад-матрица",
        provides=(MATRIX,), requires=(CARD, SURFACE),
        priority=30,        # после Сборщика (20), до анализаторов
    )

    def __init__(self, kernel):
        super().__init__(kernel)
        self._cells = {}                # (kind, axis) -> {"value","ts_ms","seq"}
        self._interval = 2.0            # ритм публикации среза, сек
        self._stale_sec = 1800.0        # TTL ячейки: полчаса без обновлений — мертва
        self.writes = 0                 # счётчик поглощений (для Здоровья)

    async def on_start(self):
        c = self.kernel.cfg
        self._interval = c.get_float("matrix", "publish_interval", 2.0)
        self._stale_sec = c.get_float("matrix", "stale_sec", 1800.0)
        self.subscribe(CARD, self._on_card)
        self.subscribe(SURFACE, self._on_surface)
        self.every(self._interval, self._publish_slice)
        await self._warmup()
        self.log.info("Матрица готова: ячеек %d (прогрев %d карточек), "
                      "TTL %.0f с", len(self._cells), WARMUP_CARDS,
                      self._stale_sec)

    async def _warmup(self):
        """Прогрев из журнала: свежайшие карточки по всем видам — честное
        «настоящее» после рестарта, без своей истории (ее нет и не будет).
        Журнал отдаёт новые→старые, тут — наоборот: пишем от старых к
        новым, чтобы итогом ячейки стала самая свежая карточка."""
        keeper = self.kernel.modules.get("keeper")
        if keeper is None:
            self.log.warning("Хранитель не поднят — матрица стартует пустой")
            return
        try:
            cards = await keeper.get_last_cards(WARMUP_CARDS)
        except Exception:
            self.log.exception("Прогрев матрицы из журнала не удался")
            return
        cards.reverse()
        for card in cards:
            self._absorb(card)

    # ---------- приём измерений ----------

    def _write(self, kind, axis, value, ts_ms, seq):
        """Пишет, если измерение не старше уже лежащего (семантика
        «настоящее» = самое свежее). Отказ от отката — защита от обратного
        порядка доставки (прогрев журнала, гонки в шине): история в
        прошлом не живёт, перетирать свежее старым нельзя."""
        ts_ms = int(ts_ms)
        cur = self._cells.get((kind, axis))
        if cur is not None and cur["ts_ms"] > ts_ms:
            self.log.debug("Матрица: пропуск устаревшего %s/%s (%d < %d)",
                           kind, axis, ts_ms, cur["ts_ms"])
            return
        self._cells[(kind, axis)] = {"value": value, "ts_ms": ts_ms,
                                     "seq": seq}
        self.writes += 1

    def _absorb(self, card):
        """Карточка целиком: каждый факт полей — в свою ячейку (kind, axis).
        Значение хранится как есть; seq — указатель в журнал.

        Адресная модель (Шаг 3, 22.10):
        - dict-поля разворачиваются в составные оси «a.b» (_flatten);
        - ось-образующие поля TF_KEYS помечают адрес: «score@60» и
          «expect_pct@60@h15» — ступени полотна и разные горизонты
          прогноза живут на разных полках (дыра 3: раньше horizon_min
          не попадал в ключ и forecast зтирал forecast);
        - поле zone_id даёт решётку зон: «zone_low#cl8421@60» — несколько
          живых зон одного ТФ не затирают друг друга (дыра 1).
        Карточки без tf_sec/horizon_min/zone_id адресуются точно как
        раньше — обратная совместимость прогрева из журнала."""
        kind = card.get("kind")
        fields = card.get("fields") or {}
        seq = card.get("seq")
        ts_ms = card.get("created_ms") or int(time.time() * 1000)
        suffix = ""
        for fkey in TF_KEYS:
            part = _fmt_axis_part(fields.get(fkey))
            if part is None:
                continue
            # горизонты помечаем 'h', лаги — 't', чтобы @60@15 (сек/мин/сек)
            # не путались глазами; tf_sec остаётся голым @ (обратная совмест.)
            if fkey == "horizon_min":
                suffix += f"@h{part}"
            elif fkey == "tau_sec":
                suffix += f"@t{part}"
            else:
                suffix += f"@{part}"
        zid = fields.get(ID_KEY)
        zid = str(zid).strip() if zid is not None and str(zid).strip() else ""
        for axis, value in _flatten(fields):
            if axis in TF_KEYS or axis == ID_KEY:
                continue    # служебное поле уже в адресе, дублировать нельзя
            key = f"{axis}#{zid}" if zid else axis
            self._write(kind, key + suffix, value, ts_ms, seq)

    async def _on_card(self, env):
        self._absorb(env.payload)

    async def _on_surface(self, env):
        """Поверхность Линз: тензор строками по масштабам — каждый масштаб
        своя ячейка ("lens", "<мин>м"), значение — та же строка, что отдаёт
        lenses.surface_read. Seq нет: публикация SURFACE — не карточка."""
        payload = env.payload
        try:
            from modules.market.lenses import surface_all
        except ImportError:
            return
        ts_ms = payload.get("ts_ms") or int(time.time() * 1000)
        for sec, val in surface_all(payload):
            self._write("lens", f"{sec / 60.0:g}м", val, ts_ms, None)

    # ---------- срез для потребителей ----------

    def slice(self, kind=None):
        """Копия текущих ячеек: {(kind, axis): {"value","ts_ms","seq"}}.
        kind=None — всё. Срез — НАСТОЯЩЕЕ; для прошлого идите по seq в
        журнал карточек (Хранитель)."""
        if kind is None:
            return dict(self._cells)
        return {k: v for k, v in self._cells.items() if k[0] == kind}

    async def _publish_slice(self):
        """Срез на «доску значений» (свежее — поздним читателям) + чистка
        протухших ячеек (история живёт в журнале, матрица — только сейчас)."""
        now = time.time() * 1000.0
        dead = [k for k, v in self._cells.items()
                if now - v["ts_ms"] > self._stale_sec * 1000.0]
        for k in dead:
            del self._cells[k]
        if dead:
            self.log.debug("Матрица: протухло ячеек %d (TTL %.0f с)",
                           len(dead), self._stale_sec)
        self.publish(MATRIX, {"ts_ms": int(now), "cells": dict(self._cells)})

    async def on_stop(self):
        self.log.info("Итог: поглощено измерений %d, ячеек в матрице %d",
                      self.writes, len(self._cells))