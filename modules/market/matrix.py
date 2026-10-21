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
Значение хранится КАК ЕСТЬ (не сплющивается, не усредняется). SURFACE
(линзы) — первый жилец: тензор пишется строками по масштабам. Сценарист
(Узел 6) читает срезы матрицы и строит по ним срезы-прогнозы вместо
голосовалки Σ(вес×знак).
"""
import time

from core.module import Manifest, NeirModule
from modules._labels import CARD, MATRIX, SURFACE

WARMUP_CARDS = 400        # сколько последних карточек журнала берём на старте


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

        ОСЬ @ТФ (20.10, полотно Регрессии): если в полях есть tf_sec,
        адресом становится «поле@ТФ» (score@60, score@14400) — иначе
        тринадцать ступеней писали бы в одну полку и затирали друг друга
        (4ч жил под минутой). Без tf_sec адрес прежний."""
        kind = card.get("kind")
        fields = card.get("fields") or {}
        seq = card.get("seq")
        ts_ms = card.get("created_ms") or int(time.time() * 1000)
        try:
            tf = int(fields.get("tf_sec"))
        except (TypeError, ValueError):
            tf = None
        for axis, value in fields.items():
            key = f"{axis}@{tf}" if tf is not None else axis
            self._write(kind, key, value, ts_ms, seq)

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