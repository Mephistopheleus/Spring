"""Регистратор — паспорта документов NeirOS-M.

Зона ответственности (утверждена): каждый результат анализа получает
паспорт — номер, время, родословную и готовый вес.

Вес = ПРОИЗВЕДЕНИЕ (утверждено, «никаких средних голубцов»):
    weight = confidence × (2 × trust)
Доверие не разбавляет уверенность, а РЕЖЕТ/УСИЛИВАЕТ её:
нейтральное 0.5 → множитель 1.0 (холодный старт не страдает),
0.1 → ×0.2 (почти глухой), 0.95 → ×1.9 (проверенный громче).
Уверенность не может спасти тип карточек с плохой репутацией.
Доверие публикует автотюнер (ярлык TRUST); пока его нет —
нейтральное 0.5, никто не имеет преимущества.

Склад и остальные ничего не считают — берут готовое.
Нумерация сквозная и продолжается после перезапуска (берётся из базы).
"""
import asyncio
import itertools
import logging
import time

from core.module import Manifest, NeirModule
from modules._labels import CARD, DRAFT, TRUST


class Registrar(NeirModule):
    manifest = Manifest(name="registrar", title="Регистратор (паспорта и веса)",
                        provides=("card",), priority=15, tick_timeout=5.0)

    NEUTRAL_TRUST = 0.5

    async def on_start(self):
        # ДОВЕРИЯ — ИЗ CONFIG.INI СРАЗУ (утверждено): то, что Тюнер
        # заработал в прошлых запусках; дальше справочник обновляет TRUST
        self._trust = dict(self.kernel.cfg.trust_directory())
        if self._trust:
            self.log.info("Справочник доверий из config.ini: %d видов",
                          len(self._trust))
        self.issued = 0
        self.bad_drafts = 0

        keeper = self.kernel.modules.get("keeper")
        if keeper is None:
            self.log.warning("Кладовщик не найден — нумерация начнётся с 1 "
                             "и после перезапуска может повторяться!")
            start = 0
        else:
            start = await keeper.max_card_seq()
            self.log.info("Нумерация продолжается: последняя карточка №%d", start)
        self._seq = itertools.count(start + 1)

        self.subscribe(DRAFT, self._on_draft)
        self.subscribe(TRUST, self._on_trust)

    async def on_stop(self):
        self.log.info("Итог: выдано карточек %d, бракованных черновиков %d",
                      self.issued, self.bad_drafts)

    # ---------- справочник доверий (от автотюнера) ----------

    async def _on_trust(self, env):
        data = env.payload
        if isinstance(data, dict):
            data = [data]
        if not isinstance(data, list):
            self.log.warning("Справочник доверий в неверном формате — пропущен")
            return
        changed = 0
        for item in data:
            try:
                kind = str(item["kind"])
                trust = min(1.0, max(0.0, float(item["trust"])))
            except (KeyError, TypeError, ValueError):
                continue
            self._trust[kind] = trust
            changed += 1
        if changed:
            self.log.info("Справочник доверий обновлён: +%d записей (всего %d)",
                          changed, len(self._trust))

    # ---------- черновики → готовые карточки ----------

    async def _on_draft(self, env):
        draft = env.payload
        if not isinstance(draft, dict):
            self.bad_drafts += 1
            self.log.warning("Черновик не словарь — пропущен")
            return
        kind = draft.get("kind")
        author = draft.get("author")
        confidence = draft.get("confidence")
        if not kind or not author or not isinstance(confidence, (int, float)):
            self.bad_drafts += 1
            self.log.warning("Черновик без вида/автора/уверенности — пропущен")
            return

        confidence = min(1.0, max(0.0, float(confidence)))
        seq = next(self._seq)
        trust = self._trust.get(str(kind), self.NEUTRAL_TRUST)
        # Чистое умножение (утверждено): множитель доверия 2×trust,
        # нейтральное 0.5 → ×1.0; вес ограничен снизу нулём
        mult = min(2.0, 2.0 * trust)
        weight = round(max(0.0, confidence * mult), 4)

        card = {
            "seq": seq,
            "kind": str(kind),
            "author": str(author),
            "created_ms": int(time.time() * 1000),
            "confidence": round(confidence, 4),
            "trust": trust,
            "weight": weight,
            "weight_basis": "product",
            "fields": draft.get("fields") or {},
            "lineage": draft.get("lineage") or [],
        }
        # Авторский срок жизни (TTL-консолидация, 20.10): аналитик кладёт
        # ttl_sec в КОРЕНЬ DRAFT — породившая сторона решает, когда зона
        # умерла. Корневое число, не поле (иначе Матрица сделала бы ось).
        ttl = draft.get("ttl_sec")
        if isinstance(ttl, (int, float)) and ttl > 0:
            card["ttl_sec"] = int(ttl)
        self.publish(CARD, card)
        self.issued += 1
        if self.issued == 1 or self.issued % 50 == 0:
            self.log.info("Карточка №%d: %s / %s — вес %.2f (увер. %.2f × "
                          "доверие %.2f → ×%.2f)",
                          seq, card["author"], card["kind"], weight,
                          confidence, trust, mult)
