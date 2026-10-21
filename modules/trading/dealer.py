"""Дилер — исполнитель NeirOS-M. Один модуль, два режима (утверждено).

РЕАЛЬНЫЙ режим: рыночный ордер на Binance Futures (нужны ключи в
config/keys.ini, секция [binance]; тестнет приоритетнее боевых).
ТЕНЕВОЙ: та же сделка на бумаге — то же плечо, те же комиссии и
трейлинг, ордер не уходит на биржу. Вердикт риск-менеджера allow →
реальный ордер, forbid → теневой просчёт с тем же контролем.

Боевой контракт (утверждено):
- двусторонний режим позиций: каждый ордер несёт метку стороны
  (LONG/SHORT) — встречные сделки живут независимо, сценарий каждой
  сделки свой;
- плечо из сценария выставляется на бирже перед открытием (по умолчанию
  биржа держит своё, например 20x кросс — тогда ликвидация не там, где
  посчитали мы); маржа — изолированная: в принудительное улетает только
  маржа сделки, не весь баланс;
- при открытии НА БИРЖУ сразу ставится STOP-MARKET по стопу сценария:
  система упала — биржа закроет по стопу сама; трейлинг = перенос этого
  биржевого стопа (не чаще раза в 5 сек);
- при старте: открытые позиции биржи подхватываются под ведение
  (стоп восстанавливается на recovery_stop_pct — проверить вручную!);
- сверка баланса (валидация): фактический баланс vs расчётный
  (старт + закрытые сделки ± открытые); расхождение сверх допуска —
  тревога в журнал.

Ведёт ВСЕ открытые сделки (реальные и теневые) в реальном времени:
безубыток, трейлинг, выход по стопу или первой цели, команды
риск-менеджера (личные конверты): закрыть немедленно.

Факты: сделка открыта / закрыта (со снимком результата для тюнера) —
через Регистратора. Комиссии учтены в результате.
"""
import asyncio
import hashlib
import hmac
import logging
import os
import time
from urllib.parse import urlencode

import aiohttp
import orjson

from core.module import Manifest, NeirModule
from modules._labels import CARD, CONDUCTIVITY, DRAFT, ORDERBOOK, PRICE


class Dealer(NeirModule):
    manifest = Manifest(name="dealer", title="Дилер (реал + тень)",
                        priority=85, tick_timeout=10.0,
                        # СЕТЕВОЙ СТАРТ: время биржи + фильтры + маржа +
                        # режим позиций + подхват позиций + баланс — на
                        # медленной сети 30с не хватало (таймаут
                        # инициализации оставлял Дилера полуживым: ящик
                        # жив, подписки сняты — сделки не открывались)
                        init_timeout=90.0)

    async def on_start(self):
        self.symbol = self.kernel.cfg.symbol.upper()
        self.fee_pct = self.kernel.cfg.fee_pct / 100.0
        # КОНТУР: выбран ЯВНО в [trading] contour. Никакого «тестнет
        # приоритетнее боевых» — контур = та биржа, ключи которой читаются.
        cfg = self.kernel.cfg
        self.contour = cfg.contour
        acct = cfg.binance_account           # аккаунт активного контура
        self._key = acct["key"] if acct else None
        self._secret = acct["secret"] if acct else None
        self._has_keys = acct is not None
        self._rest_base = acct["base"] if acct else cfg.LIVE_BASE
        self._qty_step = 0.001          # уточнится из exchangeInfo
        self.min_notional = 5.0         # МИНИМУМ ОБЪЁМА (Этап 35): fallback
        self._price_dec = 4
        self._deals = {}                # deal_id -> сделка (открытые)
        self._deal_seq = 0
        # МЕТКА СЕССИИ для clientOrderId: уникальна между запусками,
        # стабильна внутри прогона (см. _cid). Время + pid: два запуска
        # подряд не должны получить одну метку (время на некоторых ОС
        # квантуется).
        self._session_tag = hashlib.sha1(
            f"{self.kernel.cfg.symbol}{time.time_ns()}{os.getpid()}"
            .encode()).hexdigest()[:6]
        self.price = 0.0
        self.opened = 0
        self.closed = 0
        # боевой контур
        self._lev_current = None        # плечо, установленное на бирже
        self._stop_orders = {}          # deal_id -> номер стоп-ордера
        self._stop_price = {}           # deal_id -> цена стопа на бирже
        self._last_stop_move = 0.0      # троттлинг переноса стопа
        self._viscosity = 1.0           # вязкость стакана (Волна 5, Этап 30)
        self._bal_start = None          # баланс на старте (сверка)
        self._real_pnl = 0.0            # реализовано реальными сделками
        self.recovery_stop_pct = self.kernel.cfg.dealer_recovery_stop_pct
        self.recon_tol = self.kernel.cfg.dealer_recon_tol_usdt
        self.session_start_ms = int(time.time() * 1000)
        self.entry_dev = self.kernel.cfg.risk_entry_dev_pct / 100.0
        self.slip_max = self.kernel.cfg.dealer_slip_max_pct / 100.0
        self.press_pct = self.kernel.cfg.dealer_calm_press_pct
        # ЩИТ СТОПА за живой преградой: минимальная масса зоны и запас за
        # её массой (ручки конфига, не магия в коде).
        self.shield_strength = self.kernel.cfg.dealer_shield_min_strength
        self.shield_pad = self.kernel.cfg.dealer_shield_pad_pct
        # РЕЖИМ КОНТУРА: ровно два — real (ордера идут на биржу) | shadow (счёт
        # без ордера). Рубильник live_enabled живёт ВНУТРИ конфигурации и
        # отвечает только за LIVE-контур: на testnet-реал не влияет.
        self.mode = cfg.mode
        self._real_allowed = cfg.real_allowed
        if self.mode == "real" and not self._has_keys:
            self.log.error("Режим РЕАЛ, но ключей активного контура "
                           "('%s') нет — все сделки идут В ТЕНЬ", self.contour)

        self.mailbox(self._on_verdict)          # вердикты — личными конвертами
        # ГОТОВНОСТЬ К СДЕЛКАМ (06.10): вся сеть биржи уходит в фон.
        # Раньше фильтры/режимы/баланс шли синхронно в on_start; в сетевом
        # окне (после прогрева соседнего процесса testnet троттлит IP) сумма
        # таймаутов переваливала за init_timeout=90с — ядро помечало Дилера
        # сбоем и _drop_module_assets снимал его подписки и ящик: вердикты
        # терялись, система оставалась без исполнителя. Теперь on_start
        # только подписывает и регистрирует ритмы; сеть идёт в
        # _boot_network с повторами, а до готовности вердикты копятся
        # в _boot_verdicts и проигрываются FIFO сразу после готовности.
        self._ready = not self._has_keys    # без ключей сеть не нужна
        self._boot_verdicts = []            # конверты, пришедшие до готовности
        if self._has_keys:
            self.spawn(self._boot_network())
        self.subscribe(PRICE, self._on_price)   # пульс: ведение сделок
        self.subscribe(ORDERBOOK, self._on_book)  # Slip Price (аудит контура)
        self.subscribe(CARD, self._on_card)     # вязкость из Потока (Волна 5)
        self._book = None                       # последний срез стакана
        self._time_offset = 0                   # поправка часов (см. ниже)
        self._boot_bal_tries = 0   # фоновые попытки стартового баланса
        # ОТЛОЖЕННЫЕ ВХОДЫ (калибровка): вход при возврате цены в зону
        self.pending_enabled = self.kernel.cfg.dealer_pending_enabled
        self._pending = {}                      # scenario_seq -> отложенная
        self.pending_activated = 0
        self.pending_expired = 0

        if not self._has_keys:   # сеть при ключах — в _boot_network (фон)
            self.log.warning("Ключей биржи нет — реальные сделки исполняются "
                             "В ТЕНИ с пометкой (заполните [%s] в keys.ini)",
                             ("keys_testnet" if self.contour == "testnet"
                              else "keys_live"))
        blocked = cfg.trading_blocked_reason
        if blocked:
            self.log.warning("Режим контура: %s", blocked)
        elif self.mode == "real":
            self.log.warning("ВНИМАНИЕ: контур %s в режиме РЕАЛ — сделки "
                             "исполняются реальными ордерами", self.contour.upper())
        self.every(600, self._reconcile)        # тройная сверка раз в 10 мин
        self.every(1800, self._refresh_time_offset)   # часы плывут — сверяемся
        # ФОНОВЫЙ РЕТРАЙ БАЛАНСА (06.10): одна зависшая попытка при старте
        # оставляла _bal_start=None навсегда («сверка отложена» наступала
        # лишь через 10 минут), а стартовый баланс нужен сразу — сверке
        # и приёмке (тест 19). Раз в 30с, не чаще 10 раз.
        self.every(30.0, self._balance_boot)

    async def _balance_boot(self):
        """Стартовый баланс, если первая попытка упала по сети (06.10)."""
        if self._bal_start or not self._has_keys or self._boot_bal_tries >= 10:
            return
        self._boot_bal_tries += 1
        try:
            bal = await self._fetch_balance()
        except Exception:
            self.log.warning("Фоновая попытка баланса %d не удалась (сеть?)",
                             self._boot_bal_tries)
            return
        if bal:
            self._bal_start = bal
            self.log.info("Баланс старта добыт фоновой попыткой %d: %.2f USDT",
                          self._boot_bal_tries, bal)

    async def _boot_network(self):
        """Сетевая готовность Дилера в фоне (06.10) — см. on_start.

        Порядок как в старом синхронном блоке: часы → фильтры → режимы →
        подхват позиций → баланс. Повторы раз в 15с, до 10 раз: в
        сетевом окне первая же попытка уходит с таймаутом, и раньше это
        роняло весь модуль. Готовность (_ready) открывает приём вердиктов;
        до неё конверты копятся и проигрываются FIFO."""
        acct = self.kernel.cfg.binance_account
        for attempt in range(1, 11):
            try:
                self._time_offset = await self._fetch_time_offset()
                if abs(self._time_offset) > 500:
                    self.log.warning("Локальные часы %s на %d мс относительно "
                                     "биржи — подписи скорректированы",
                                     "отстают" if self._time_offset > 0
                                     else "спешат", abs(self._time_offset))
                await self._load_filters()
                await self._ensure_margin_mode()
                await self._ensure_position_mode()
                await self._adopt_positions()
                bal = await self._fetch_balance()
                if bal is None:
                    raise RuntimeError("в ответе биржи нет USDT")
                self._bal_start = bal
                self._ready = True
                self.log.info("Контур %s, режим %s (%s): фильтры загружены, "
                              "баланс старта %.2f USDT",
                              self.contour.upper(), self.mode.upper(),
                              "ТЕСТНЕТ, фантики" if acct and acct["testnet"]
                              else "БОЕВАЯ БИРЖА, ДЕНЬГИ", bal)
                while self._boot_verdicts:
                    env = self._boot_verdicts.pop(0)
                    await self._on_verdict(env)
                return
            except Exception as exc:
                self.log.warning("Сетевая инициализация Дилера (попытка "
                                 "%d из 10) не удалась: %s: %s",
                                 attempt, type(exc).__name__, exc)
                if attempt < 10:
                    await asyncio.sleep(15)
        self.log.error("Дилер не готов после 10 попыток — сделки "
                       "невозможны, вердикты копятся в _boot_verdicts")

    async def on_stop(self):
        self.log.info("Итог: открыто %d (теневых в том числе), закрыто %d, "
                      "в работе осталось %d; отложенные: активировано %d, "
                      "в тень по TTL %d",
                      self.opened, self.closed, len(self._deals),
                      self.pending_activated, self.pending_expired)
        if self._deals:
            self.log.warning("ALERT: Брошено открытых позиций: %d — "
                             "требуется ручной контроль или подхват при "
                             "перезапуске (биржевые стопы активны)",
                             len(self._deals))

    # ---------- карточки: вязкость стакана (Волна 5) ----------

    async def _on_card(self, env):
        card = env.payload
        if card.get("kind") == "flow-state":
            # Вязкость стакана (Волна 5, утверждено): >1 — среда жидкая
            # (цена течёт), <1 — каменеет (поглощение). Slip-порог плавает
            try:
                self._viscosity = max(0.5, min(2.0, float(
                    (card.get("fields") or {}).get("viscosity") or 1.0)))
            except (TypeError, ValueError):
                self._viscosity = 1.0

    # ---------- вердикты и команды (личные конверты) ----------

    async def _on_verdict(self, env):
        # ДО ГОТОВНОСТИ (06.10): сеть ещё не встала — конверт копим,
        # _boot_network проиграет очередь FIFO сразу после готовности.
        if not getattr(self, "_ready", True):
            self._boot_verdicts.append(env)
            return
        payload = env.payload
        if payload.get("command") == "close":
            deal = self._deals.get(payload.get("deal_id"))
            if deal is not None:
                await self._close(deal, self.price,
                                  payload.get("reason", "снятие"))
            return
        if payload.get("command") == "breakeven":
            deal = self._deals.get(payload.get("deal_id"))
            if deal is not None and not deal.get("be_done"):
                deal["be_done"] = True
                long = deal["direction"] == "long"
                # ПОЛ БЕЗУБЫТКА: не откатываем прибыльный трейлинг-стоп
                # к входу — безубыток ставится только как УЛУЧШЕНИЕ
                improves = ((deal["entry"] > deal["stop"]) if long
                            else (deal["entry"] < deal["stop"]))
                if improves:
                    deal["stop"] = deal["entry"]
                    self.log.warning("%s: РЕАНИМАЦИЯ — стоп в безубыток (%.5f)",
                                     deal["deal_id"], deal["stop"])
                else:
                    self.log.info("%s: безубыток пропущен — стоп уже лучше входа",
                                  deal["deal_id"])
            return
        if payload.get("command") == "cancel-pending":
            # ОТЗЫВ АВТОРА: идея сценария сломана (магнит исчез из картины)
            d = payload.get("direction")
            mag = float(payload.get("magnet", 0) or 0)
            for seq in list(self._pending):
                p = self._pending[seq]
                if p["direction"] != d:
                    continue
                width = max(p["zone_hi"] - p["zone_lo"], 1e-9)
                if mag >= p["zone_lo"] - width and mag <= p["zone_hi"] + width:
                    del self._pending[seq]
                    self.log.info("Отложенная (сценарий №%d) отозвана автором — "
                                  "магнит исчез из картины", seq)
            return
        decision = payload.get("verdict")
        scenario = payload.get("scenario") or {}
        if decision not in ("allow", "forbid"):
            self.log.warning("Вердикт без решения — пропущен")
            return

        mode = "real" if decision == "allow" else "shadow"
        note = payload.get("reason", "")
        if mode == "real" and not self._real_allowed:
            # Режим контура = shadow, либо блокировка рубильником боевого
            # контура. Причина уже названа в журнале на старте — здесь
            # фиксируем её в снимке сделки, чтобы Тюнер видел ЧЕМ исполнено.
            mode = "shadow"
            note = (f"режим контура «{self.mode}» — исполнено в тени: " + note)
        if mode == "real" and not self._has_keys:
            mode = "shadow"
            note = "нет ключей биржи — исполнено в тени: " + note
        # ОТЛОЖЕННЫЙ ВХОД (калибровка): сценарий несёт entry_zone — сделка
        # открывается при ВОЗВРАТЕ цены в зону, а не в хвосте импульса.
        # Без зоны (старые тесты) или с выключенной ручкой — как раньше.
        zone = scenario.get("entry_zone") or []
        if (self.pending_enabled and len(zone) == 2
                and float(zone[0] or 0) > 0 and float(zone[1] or 0) > 0):
            seq = int(payload.get("scenario_seq", 0))
            if (seq and seq not in self._pending
                    and not any(d.get("scenario_seq") == seq
                                for d in self._deals.values())):
                lo = min(float(zone[0]), float(zone[1]))
                hi = max(float(zone[0]), float(zone[1]))
                self._pending[seq] = {
                    "payload": payload, "scenario": scenario,
                    "mode": mode, "note": note,
                    "direction": scenario.get("direction", "long"),
                    "zone_lo": lo, "zone_hi": hi,
                    "atr": float(scenario.get("atr", 0.0) or 0.0),
                    "valid_until": int(scenario.get("valid_until_ms", 0)),
                    # худший для нас ход с момента публикации — начинается
                    # с цены входа (None здесь ломал состояние машины)
                    "worst": float(scenario.get("entry", 0.0) or 0.0),
                    "armed": False,
                }
                self.log.info("ОТЛОЖЕННАЯ %s (сценарий №%d): жду возврат "
                              "цены в зону %.5f..%.5f",
                              mode.upper(), seq, lo, hi)
                return
        await self._open(payload, scenario, mode, note)

    async def _open(self, verdict, scenario, mode, note, force=False):
        """force — исполнить несмотря на протухание/удалённость: так
        санитар отложенных (TTL истёк → сценарий уходит в тень)."""
        entry_s = float(scenario.get("entry", 0.0) or 0.0)
        if self.price <= 0 and entry_s <= 0:
            # ни пульса цены, ни entry у сценария — взять неоткуда (06.10):
            # раньше такой вердикт отбрасывался целиком, хотя entry
            # сценария — честная цена из картины
            self.log.warning("Нет цены и нет entry сценария — "
                             "сделку не открываю")
            return
        # ДВОЙНАЯ ПРОВЕРКА СВЕЖЕСТИ (утверждено, защита в глубину):
        # протухший или далёкий от рынка сценарий не исполняется,
        # даже если вердикт Риска был allow
        now_ms = int(time.time() * 1000)
        if not force and int(scenario.get("valid_until_ms", 0)) < now_ms:
            self.log.warning("Сценарий протух (valid_until истёк) — "
                             "сделку не открываю")
            return
        if (not force and entry_s > 0 and self.price > 0
                and abs(entry_s - self.price) / self.price > self.entry_dev):
            self.log.warning("Вход сценария %.5f далеко от рынка %.5f — "
                             "сделку не открываю", entry_s, self.price)
            return
        self._deal_seq += 1
        deal_id = f"D{self._deal_seq}"
        direction = scenario.get("direction", "long")
        qty = float(verdict.get("quantity", 0.0))
        # цена входа: пульс, а при его отсутствии — entry сценария (06.10)
        entry = self.price if self.price > 0 else entry_s
        # фаза среды на момент входа (для разбивки проскальзывания в Лаборатории)
        env_c = self.get_last(CONDUCTIVITY)
        phase = (str((env_c.payload or {}).get("phase", "calm"))
                 if env_c is not None else "calm")

        if mode == "real":
            try:
                # SLIP PRICE (утверждено, аудит контура): проскальзывание
                # по живому стакану ДО отправки ордера. Порог динамический
                # (Волна 5): вязкая среда — порог шире, каменная — уже
                slip = self._slip_price(qty, direction == "long")
                if slip is not None:
                    slip_cap = self.slip_max * self._viscosity
                    if slip > slip_cap:
                        mode = "shadow"
                        note = (f"стакан тонкий (slip {slip*100:.3f}% > "
                                f"{slip_cap*100:.3f}% при вязкости "
                                f"{self._viscosity:.2f}) — исполнено в тени")
                        self.log.warning("Slip Price %.3f%% > порога %.3f%% "
                                         "(вязкость %.2f) — реальный ордер "
                                         "отменён", slip * 100,
                                         slip_cap * 100, self._viscosity)
                    else:
                        self.log.info("Slip Price: %.4f%% по стакану",
                                      slip * 100)
            except Exception:
                self.log.exception("Оценка Slip Price не удалась — продолжаю")
        if mode == "real":
            try:
                await self._ensure_lev(float(scenario.get("leverage", 1.0)))
                side = "BUY" if direction == "long" else "SELL"
                pside = "LONG" if direction == "long" else "SHORT"
                data = await self._signed("POST", "/fapi/v1/order", {
                    "symbol": self.symbol, "side": side, "type": "MARKET",
                    "quantity": self._fmt_qty(qty), "positionSide": pside,
                    # идемпотентность: повтор входа с тем же id биржа
                    # отвергнет как дубликат, а не откроет вторую позицию
                    "newClientOrderId": self._cid(deal_id, "in")})
                fill = float(data.get("avgPrice") or 0)
                if fill > 0:
                    entry = fill          # факт исполнения, а не наша оценка
            except Exception as exc:
                mode = "shadow"
                note = f"ошибка биржи ({exc}) — исполнено в тени"

        deal = {
            "deal_id": deal_id,
            "mode": mode,
            "direction": direction,
            "entry": entry,
            "stop": float(scenario.get("stop", 0.0)),
            "targets": scenario.get("targets", []),
            "leverage": float(scenario.get("leverage", 1.0)),
            "trailing": scenario.get("trailing", {}),
            "qty": qty,
            "phase": phase,
            "notional": float(verdict.get("notional", qty * entry)),
            "scenario_seq": verdict.get("scenario_seq", 0),
            # ПРИШИТЫЙ СЦЕНАРИЙ (самодостаточный снимок): полный набор полей
            # едет со сделкой до закрытия — Тюнеру и Лабе джойн не нужен
            "scenario_card": {
                "seq": int(verdict.get("scenario_seq", 0) or 0),
                "confidence": float(verdict.get("confidence", 0.0) or 0.0),
                "fields": dict(scenario),
            },
            "opened_ms": int(time.time() * 1000),
            "be_done": False,
            "note": note,
        }
        self._deals[deal_id] = deal
        self.opened += 1
        self.publish(DRAFT, {
            "kind": "deal-opened",
            "author": "dealer",
            "confidence": 1.0,
            "fields": {"deal_id": deal_id, "mode": mode, "note": note,
                       "direction": direction, "entry": round(entry, 8),
                       "qty": round(qty, 6), "leverage": deal["leverage"],
                       "stop": round(deal["stop"], 8),
                       "phase": phase,
                       "scenario_seq": deal["scenario_seq"]},
            "lineage": [f"card:{deal['scenario_seq']}"],
        })
        self.log.info("СДЕЛКА %s открыта [%s]: %s %.6f x%.1f | %s",
                      deal_id, "РЕАЛ" if mode == "real" else "ТЕНЬ",
                      direction, entry, deal["leverage"], note)
        if mode == "real":
            try:
                await self._place_stop(deal)
                self.log.info("Биржевой стоп %s поставлен (%.5f) — позиция "
                              "защищена даже при падении системы",
                              deal_id, deal["stop"])
            except Exception as exc:
                if "-2021" in str(exc):
                    # цена ушла за стоп в момент постановки — стоп уже
                    # фактически сработал: закрываем по рынку штатно
                    self.log.warning("Стоп %s: цена уже за стопом — "
                                     "закрываю по рынку", deal_id)
                    await self._close(deal, self.price,
                                      "стоп (цена ушла за стопом)")
                    return
                self.log.error("Стоп-ордер %s не встал: %s — ЭКСТРЕННОЕ "
                               "закрытие (позиция без стопа недопустима)",
                               deal_id, exc)
                await self._close(deal, self.price,
                                  "экстренно: биржевой стоп не встал")

    # ---------- пульс: ведение всех открытых сделок ----------

    async def _on_price(self, env):
        p = float(env.payload.get("price", 0.0))
        if p <= 0:
            return                 # фиктивный тик (старт) не трогает состояние
        self.price = p
        await self._check_pending()
        for deal in list(self._deals.values()):
            await self._manage(deal, self.price)

    # ---------- отложенные входы (калибровка) ----------

    async def _check_pending(self):
        """Отложенный вход: сделка открывается, когда цена ВЕРНУЛАСЬ в
        зону входа сценария — импульс к полке выдохся, прогноз начал
        отыгрываться (лечение входа в хвост импульса, MFE ≤ 0 17/17).
        TTL — санитар: не дождались до valid_until — сценарий НЕ
        отменяется, а уходит в ТЕНЬ: вход не состоялся, расписка Тюнеру
        нужна («вход взят не там»)."""
        if not self._pending or self.price <= 0:
            return
        now_ms = int(time.time() * 1000)
        for seq in list(self._pending):
            p = self._pending[seq]
            if p["valid_until"] and now_ms > p["valid_until"]:
                del self._pending[seq]
                self.pending_expired += 1
                self.log.warning("Отложенная (сценарий №%d): TTL истёк — "
                                 "уходит в ТЕНЬ (цена не вернулась в зону)",
                                 seq)
                await self._open(p["payload"], p["scenario"], "shadow",
                                 p["note"] + " — TTL истёк: в тень",
                                 force=True)
                continue
            short = p["direction"] != "long"
            lo, hi = p["zone_lo"], p["zone_hi"]
            extreme = p["atr"] if p["atr"] > 0 else max(hi - lo, 1e-12)
            # худший для нас ход с момента публикации: short — вверх,
            # long — вниз; пока он не прошёл extreme — импульс жив
            p["worst"] = (max(p["worst"], self.price) if short
                          else min(p["worst"], self.price))
            if not p["armed"]:
                if (short and p["worst"] > hi + extreme) \
                        or (not short and p["worst"] < lo - extreme):
                    p["armed"] = True     # импульc выдыхается — ждём возврат
            hit = (self.price <= hi) if short else (self.price >= lo)
            # ВХОД ПО МОМЕНТУ (утверждено): только выдохнувшийся импульс
            # (armed + возврат в зону). Вето ленты снято (решение хозяина:
            # анализаторы — датчики: лента участвует в расчёте, не
            # запрещает вход). Догоняющий вход убран — хвост движения.
            if p["armed"] and hit:
                del self._pending[seq]
                self.pending_activated += 1
                await self._open(p["payload"], p["scenario"], p["mode"],
                                 p["note"]
                                 + " — активация: цена вернулась в зону входа")

    async def _on_book(self, env):
        """Последний срез стакана — оценка проскальзывания (Slip Price)."""
        try:
            self._book = {
                "bids": [(float(pr), float(q))
                         for pr, q in env.payload.get("bids", [])],
                "asks": [(float(pr), float(q))
                         for pr, q in env.payload.get("asks", [])],
            }
        except (TypeError, ValueError):
            pass

    def _slip_price(self, qty, long_side):
        """Средневзвешенная цена исполнения по depth20: сколько % цены
        съест наш объём при рыночном ордере. Нет стакана — None."""
        if not self._book or self.price <= 0:
            return None
        levels = self._book["asks"] if long_side else self._book["bids"]
        if not levels:
            return None
        levels = sorted(levels, reverse=not long_side)   # лучшие к нам
        need, filled, cost = qty, 0.0, 0.0
        for pr, q in levels:
            take = min(need, q)
            cost += take * pr
            filled += take
            need -= take
            if need <= 0:
                break
        if filled <= 0:
            return None
        avg = cost / filled
        best = levels[0][0]
        return abs(avg - best) / best

    async def _manage(self, deal, price):
        if price <= 0 or deal["entry"] <= 0:
            return
        long = deal["direction"] == "long"
        sign = 1.0 if long else -1.0
        move = (price - deal["entry"]) / deal["entry"] * sign
        tr = deal["trailing"] or {}

        # БЕЗУБЫТОК-КОСТЫЛЬ УБРАН (решение хозяина): сопровождение сделки —
        # начальный СЛ + трейлинг. Автоперенос стопа во вход на пороге
        # массово закрывал сделки у входа (перебор 489 сделок: PnL +8.3%
        # без него против -13.3% с порогом 0.2%). Перенос в безубыток
        # остаётся ТОЛЬКО по команде реанимации (внезапный разворот).

        # ТРЕЙЛИНГ (утверждено, «за живыми преградами»):
        candidate = None
        if move >= float(tr.get("activation_pct", 0.0)):
            distance = float(tr.get("distance_pct", 0.0)) * deal["entry"]
            candidate = (price - distance) if long else (price + distance)

        # Преграда картины (fvg-zone / stop-cluster с массой >0.7) — щит
        # стопа: прячем СЗАДИ неё. ЩИТ ВСТУПАЕТ ТОЛЬКО ПОСЛЕ ПРОГРЕССА
        # (активация сопровождения): иначе зона прямо под входом
        # прижимала стоп вплотную к цене и шум выбивал сделки за секунды
        # (окно 26.09: факт-стоп 0.06% вместо сценарных 0.30%).
        barrier = None
        if move >= float(tr.get("activation_pct", 0.0)):
            barrier = self._barrier_shield(deal, long)
        if barrier is not None:
            candidate = (max(candidate, barrier) if candidate is not None
                         else barrier) if long else \
                        (min(candidate, barrier) if candidate is not None
                         else barrier)

        # Скоростной адаптив (утверждено): импульс — НЕ расширяем риск
        # (он зафиксирован при входе), а ЗАМОРАЖИВАЕМ подтягивание — стоп
        # стоит, буря стихнет. Затишье (скорость ленты → 0) — прижимаем
        # стоп вплотную к цене, забираем пик перед разворотом.
        # ПРИЖИМАТЬ ЕСТЬ ЧТО ТОЛЬКО ЕСЛИ ПИК ВЫШЕ ОТСТУПА: иначе «прижатие»
        # ставило стоп НИЖЕ входа (окно 26.09: стопы 0.04–0.07% при
        # сценарных 0.15–0.20%) — шум выбивал сделки в микроминус.
        storm = self._tape_storm()
        if storm and candidate is not None:
            candidate = None            # заморозка подтягивания
            if not deal.get("frozen"):
                deal["frozen"] = True
                self.log.info("%s: ШТОРМ — подтягивание стопа заморожено "
                              "(%.5f)", deal["deal_id"], deal["stop"])
        elif not storm and deal.get("frozen"):
            deal["frozen"] = False
            self.log.info("%s: затишье — подтягивание стопа возобновлено",
                          deal["deal_id"])
        calm = self._tape_calm()
        if calm and move > self.press_pct / 100.0:
            press = self.press_pct / 100.0
            near = (price * (1.0 - press) if long
                    else price * (1.0 + press))
            candidate = (max(candidate, near) if candidate is not None
                         else near) if long else \
                        (min(candidate, near) if candidate is not None
                         else near)

        if candidate is not None:
            improves = (candidate > deal["stop"]) if long \
                else (candidate < deal["stop"])
            if improves and barrier is not None \
                    and abs(candidate - barrier) < 1e-12:
                self.log.info("Трейлинг: стоп спрятан за преграду "
                              "[fvg/stop-cluster] на цене %.5f", candidate)
            if long:
                deal["stop"] = max(deal["stop"], candidate)
            else:
                deal["stop"] = min(deal["stop"], candidate)

        # биржевой стоп обязан совпадать с нашим (троттлинг внутри)
        if deal["mode"] == "real":
            await self._sync_stop(deal)

        # выходы: ТОЛЬКО трейлинг/сценарный стоп. Тейк-профитов нет —
        # маршрут цены забирают преграды и трейлинг (решение хозяина)
        if long and price <= deal["stop"]:
            await self._close(deal, deal["stop"],
                              "безубыток" if deal["be_done"] else "стоп")
        elif not long and price >= deal["stop"]:
            await self._close(deal, deal["stop"],
                              "безубыток" if deal["be_done"] else "стоп")

    def _barrier_shield(self, deal, long):
        """Живая преграда (fvg-zone / stop-cluster с массой в Поле >0.7):
        стоп прячется ПОД/НАД всей массой зоны — зона сама щит, цена
        отскочит от неё раньше, чем дойдёт до стопа. long — ПОД зоной
        (низ минус запас), short — НАД зоной (верх плюс запас). Зона
        обязана быть ПОЗАДИ цены по ходу сделки; подтягивание в
        прибыльную сторону гарантирует max/min со сценарием стопа."""
        env = self.get_last("picture")
        if env is None:
            return None
        zones = (env.payload.get("fields") or {}).get("zones") or []
        best = None
        for z in zones:
            kinds = set(z.get("kinds") or ())
            if not kinds & {"fvg-zone", "stop-cluster"}:
                continue
            if float(z.get("strength", 0.0)) <= self.shield_strength:
                continue
            low, high = float(z.get("low", 0.0)), float(z.get("high", 0.0))
            if high < low:
                low, high = high, low
            if long:
                # зона под ценой: щит — ПОД всей массой (низ − запас)
                if self.price > high:
                    shield = low - self.shield_pad * self.price
                    best = max(best, shield) if best is not None else shield
            else:
                # зона над ценой: щит — НАД всей массой (верх + запас)
                if self.price < low:
                    shield = high + self.shield_pad * self.price
                    best = min(best, shield) if best is not None else shield
        return best

    def _tape_storm(self):
        """Импульс ленты (Проводимость: паника/импульс высокая)."""
        env = self.get_last(CONDUCTIVITY)
        if env is None:
            return False
        c = env.payload.get("components") or {}
        try:
            return (float(c.get("panic", 0.0)) > 0.5
                    and float(c.get("impulse", 0.0)) > 0.5)
        except (TypeError, ValueError):
            return False

    def _tape_calm(self):
        """Затишье: скорость ленты упала (импульс < 0.1 при недавнем движении)."""
        env = self.get_last(CONDUCTIVITY)
        if env is None:
            return False
        c = env.payload.get("components") or {}
        try:
            return float(c.get("impulse", 1.0)) < 0.1
        except (TypeError, ValueError):
            return False

    # ---------- закрытие и снимок для тюнера ----------

    async def _close(self, deal, exit_price, reason):
        if self._deals.get(deal["deal_id"]) is None:
            return                       # уже закрыта
        if exit_price <= 0:
            # КОРЕНЬ БРАКА (утверждено): закрытие по фиктивной нулевой цене
            # не выполняем — берём живую цену с биржи; нет и её — закрытие
            # откладывается до следующего тика (позиция под биржевым стопом)
            exit_price = await self._live_price()
            if exit_price is None:
                self.log.warning("Закрытие %s отложено: нет живой цены "
                                 "(ноль фиктивной ценой не считаем)",
                                 deal["deal_id"])
                return
        if deal["mode"] == "real":
            await self._cancel_stop(deal)
            try:
                side = "SELL" if deal["direction"] == "long" else "BUY"
                pside = "LONG" if deal["direction"] == "long" else "SHORT"
                # в двустороннем режиме reduceOnly НЕ нужен (биржа отвечает
                # -1106 «sent when not required»): сторона закрывает сама
                await self._signed("POST", "/fapi/v1/order", {
                    "symbol": self.symbol, "side": side, "type": "MARKET",
                    "quantity": self._fmt_qty(deal["qty"]),
                    "positionSide": pside,
                    # повтор закрытия с тем же id = тот же ордер, не второй
                    "newClientOrderId": self._cid(deal["deal_id"], "out")})
            except Exception as exc:
                text = str(exc)
                if ("-2022" in text or "ReduceOnly" in text
                        or "-2019" in text):
                    self.log.warning("Позиция %s уже закрыта биржей "
                                     "(сработал биржевой стоп)", deal["deal_id"])
                else:
                    self.log.error("Закрытие %s на бирже не прошло: %s — "
                                   "повторю на следующем тике",
                                   deal["deal_id"], exc)
                    try:
                        await self._place_stop(deal)   # позиция жива — верни защиту
                    except Exception:
                        self.log.critical("Стоп не вернулся — позиция %s "
                                          "БЕЗ биржевого стопа!", deal["deal_id"])
                    return                   # сделка остаётся открытой

        long = deal["direction"] == "long"
        sign = 1.0 if long else -1.0
        # БРАК (утверждено): закрытие без цены (exit=0) — PnL фиктивный,
        # запись помечается corrupted, тюнер такие пропускает
        corrupted = exit_price <= 0
        if corrupted:
            pnl_pct, pnl_usdt = None, None
        else:
            move = (exit_price - deal["entry"]) / deal["entry"] * sign
            pnl_pct = round(move * deal["leverage"]
                            - self.fee_pct * 2.0 * deal["leverage"], 5)
            pnl_usdt = round(pnl_pct / deal["leverage"] * deal["notional"], 4)
        duration = (int(time.time() * 1000) - deal["opened_ms"]) / 1000.0

        self._deals.pop(deal["deal_id"], None)
        self.closed += 1
        if deal["mode"] == "real" and not corrupted:
            self._real_pnl += pnl_usdt
        snapshot = {
            "deal_id": deal["deal_id"],
            "mode": deal["mode"],
            "scenario_seq": deal["scenario_seq"],
            "direction": deal["direction"],
            "entry": round(deal["entry"], 8),
            "exit": round(exit_price, 8) if exit_price > 0 else None,
            "stop_final": round(deal["stop"], 8),
            "qty": round(deal["qty"], 6),
            "leverage": deal["leverage"],
            "phase": deal.get("phase", "calm"),
            "opened_ms": deal["opened_ms"],   # путь сделки (для истории)
            "reason": reason,
            "pnl_pct": pnl_pct,
            "pnl_usdt": pnl_usdt,
            "duration_sec": round(duration, 1),
            "status": "corrupted" if corrupted else "ok",
            "note": deal["note"],
            # СТАНДАРТ СНИМКА ДИЛЕРА (поток №1): снимок самодостаточен —
            # Тюнер принимает его вслепую, ничего не зная о происхождении.
            "stream": "deal",              # метка потока внутри снимка
            "contour": self.contour,       # на какой бирже исполнено
            "real": deal["mode"] == "real", # исполнился ордер или счёт
            # ПРИШИТЫЙ СЦЕНАРИЙ: снимок самодостаточен для Тюнера
            "scenario": deal.get("scenario_card"),
        }
        # вектор фактов и настройки момента — копией из пришитого сценария,
        # чтобы слепой потребитель не ковырял вложенный dict
        sf = ((deal.get("scenario_card") or {}).get("fields") or {})
        snapshot["vector"] = sf.get("vector") or {}
        snapshot["facts"] = sf.get("facts") or {}
        snapshot["settings"] = sf.get("settings") or {}
        self.publish(DRAFT, {
            "kind": "deal-closed",
            "author": "dealer",
            "confidence": 1.0,
            "fields": snapshot,
            "lineage": [f"card:{deal['scenario_seq']}"],
        })
        # ПОЛНЫЙ СНИМОК закрытия в журнал (утверждено после тестнет-аудита):
        # сделка обязана восстанавливаться по логу — вход/выход/объём/итог
        self.log.info("СДЕЛКА %s закрыта [%s]: %s | вход %.5f → выход %s, "
                      "%.6f шт, %+.2f%% с плечом, %+.2f USDT (%s)",
                      deal["deal_id"],
                      "РЕАЛ" if deal["mode"] == "real" else "ТЕНЬ",
                      reason, deal["entry"],
                      (f"{exit_price:.5f}" if exit_price > 0 else "НЕТ ЦЕНЫ"),
                      deal["qty"], pnl_pct or 0.0, pnl_usdt or 0.0,
                      "БРАК" if corrupted else "ok")

    # ---------- биржа (реальный режим) ----------

    async def _load_filters(self):
        try:
            # ВАЖНО: тестнет игнорирует параметр symbol в exchangeInfo
            # (возвращает первый попавшийся символ — BTCUSDT!) — тянем
            # весь список и ищем свой символ сами
            self._quota_note(5)      # /fapi/v1/exchangeInfo, вес 5 (верхняя граница)
            async with aiohttp.ClientSession() as session:
                async with session.get(self._rest_base + "/fapi/v1/exchangeInfo",
                                       timeout=aiohttp.ClientTimeout(total=20)) as resp:
                    data = await resp.json(content_type=None)
            info = next((s for s in data.get("symbols", [])
                         if s.get("symbol") == self.symbol), None)
            if info is None:
                self.log.error("Символ %s не найден в exchangeInfo!", self.symbol)
                return
            self._parse_filters(info)
        except Exception:
            self.log.exception("Не удалось получить фильтры биржи")

    def _parse_filters(self, info):
        """Разбор фильтров символа (шаг/точность/минимум объёма)."""
        self._qty_step = 10 ** -int(info.get("quantityPrecision", 3))
        self._price_dec = int(info.get("pricePrecision", 4))
        for f in info.get("filters", []):
            if f.get("filterType") == "PRICE_FILTER":
                # ВАЖНО: знаки цены берём из ШАГА ЦЕНЫ (tickSize),
                # pricePrecision — это точность ОТОБРАЖЕНИЯ и
                # округление до него ломает стопы
                tick = str(f.get("tickSize", "0.0001"))
                if "." in tick and float(tick) > 0:
                    self._price_dec = len(tick.rstrip("0").split(".")[1])
            if f.get("filterType") == "LOT_SIZE":
                self._qty_step = float(f.get("stepSize", 0.001))
            if f.get("filterType") == "MIN_NOTIONAL":
                # МИНИМУМ ОБЪЁМА (Этап 35): живой фильтр биржи —
                # его берут Риск и Сценарист (fallback 5.0 в on_start)
                self.min_notional = float(f.get("notional", 5.0))
        self.log.info("Фильтры биржи (%s): шаг количества %s, цены %d "
                      "знаков, минимум объёма %.1f USDT",
                      self.symbol, self._qty_step, self._price_dec,
                      self.min_notional)

    def _cid(self, deal_id, op):
        """Идентификатор клиентского ордера: СЕССИЯ + deal_id + операция.

        Зачем метка сессии: deal_id внутри прогона начинается с D1, а биржа
        помнит clientOrderId и после перезапуска отвечает «ClientOrderId is
        duplicated» на первый же ордер нового процесса (теряем реальный
        вход). Метка делает id уникальным между ПРОГОНАМИ, но внутри
        прогона он стабилен: повтор того же входа/выхода с тем же id биржа
        считает повтором (а не новым ордером) — двойное исполнение при
        таймауте сети по-прежнему невозможно.
        """
        raw = (f"n{self._session_tag}{self.kernel.cfg.symbol.lower()}"
               f"{deal_id}{op}")
        return raw[:36]

    def _fmt_qty(self, qty):
        if self._qty_step >= 1:
            return str(int(qty // self._qty_step * self._qty_step))
        decimals = max(0, len(str(self._qty_step).split(".")[-1].rstrip("0")))
        return str(round(qty, decimals))

    def _fmt_price(self, price):
        return f"{price:.{self._price_dec}f}"

    async def _fetch_time_offset(self):
        """Смещение локальных часов относительно биржи, мс (публичный
        запрос).         Локальное время спешит → подпись уходит в будущее →
        код биржи -1021 (HTTP 400): воспроизводилось на живом прогоне."""
        try:
            self._quota_note(1)      # /fapi/v1/time — вес 1 (до отправки)
            async with aiohttp.ClientSession() as session:
                async with session.get(
                        f"{self._rest_base}/fapi/v1/time",
                        timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    data = await resp.json(content_type=None)
            server_ms = int(data.get("serverTime", 0))
            if server_ms > 0:
                return server_ms - int(time.time() * 1000)
        except Exception:
            self.log.warning("Не удалось сверить время с биржей — offset 0")
        return 0

    async def _refresh_time_offset(self):
        """Периодическая сверка часов с биржей: дрейф медленный (~мс/час),
        но в марафонской сессии offset, снятый на старте, устаревает."""
        self._time_offset = await self._fetch_time_offset()

    def _quota_note(self, weight=1.0):
        """Учёт ушедшего запроса дилера в журнале Квотника (до отправки).

        Все чтения дилера идут в резервный пул (dealer=True) — фон их не
        отнимает, дилеру всегда остаётся запас до биржи."""
        q = self.kernel.modules.get("quoter")
        if q is not None:
            q.note(weight, dealer=True)

    async def _signed(self, method, path, extra=None):
        """Подписанный запрос к бирже. Нехороший статус → RuntimeError.

        ИДЕМПОТЕНТНОСТЬ: вызывающий передаёт `newClientOrderId` (детерминированный
        от сделки и операции). Таймаут сети после принятия ордера биржой при
        повторе даёт -4015 «Duplicate order sent» — это НЕ ошибка, а подтверждение,
        что ордер уже жив: такой ответ трактуется как успех (dict с
        duplicate=True), чтобы повтор не открыл вторую позицию.

        ЛИМИТ ОРДЕРОВ (Квотник): операции с ордерами (/order, /algoOrder —
        создание и отмена) подпадают под частотный потолок биржи: перед
        отправкой ждём слота в 10-секундном окне, чтобы НЕ ПРЕВЫШАТЬ. Если
        окно не открылось к таймауту, ордер всё равно уходит (незакрытый
        стоп опаснее кода -1003), но с громким логом.
        """
        params = {"timestamp": int(time.time() * 1000) + self._time_offset,
                  "recvWindow": 5000}
        if extra:
            params.update(extra)
        query = urlencode(params)
        signature = hmac.new(self._secret.encode(), query.encode(),
                             hashlib.sha256).hexdigest()
        url = f"{self._rest_base}{path}?{query}&signature={signature}"
        headers = {"X-MBX-APIKEY": self._key}

        # Квотник: учёт ДО отправки (биржа начисляет вес/ордер по получении)
        is_order_op = "order" in path.lower()   # /fapi/v1/order, /algoOrder
        quoter = self.kernel.modules.get("quoter")
        if quoter is not None:
            if is_order_op:
                got_slot = await quoter.wait_for_order_slot(timeout=12.0)
                if not got_slot:
                    # Окно 10с не открылось — отправляем всё равно (незакрытый
                    # стоп опаснее -1003), но делаем из этого событие в журнале.
                    self.log.error("ЛИМИТ ОРДЕРОВ: окна за 10 с не нашлось — "
                                   "отправляю %s, биржа может ответить -1003",
                                   path)
                quoter.note_order()
            else:
                # прочее подписанное (баланс, плечо, режимы) — редкие запросы,
                # вес 1 (верхняя граница таблицы; потолок взят с запасом вдвое).
                # dealer=True: идёт в резерв дилера, фон его не отнимает.
                quoter.note(1, dealer=True)

        async with aiohttp.ClientSession() as session:
            async with session.request(method, url, headers=headers,
                                       timeout=aiohttp.ClientTimeout(total=15)) as resp:
                data = await resp.json(content_type=None)
                if resp.status != 200:
                    if isinstance(data, dict) and data.get("code") == -4015:
                        # повтор того же clientOrderId — ордер уже прошёл
                        dup = dict(data)
                        dup["duplicate"] = True
                        dup.setdefault("clientOrderId",
                                       params.get("newClientOrderId"))
                        return dup
                    raise RuntimeError(f"{path}: {data}")
                return data

    async def _ensure_lev(self, lev):
        """Плечо из сценария → на биржу. Без этого биржа держит своё
        (по умолчанию 20x кросс) и ликвидация не там, где посчитали мы."""
        lev = max(1, int(round(lev)))
        if self._lev_current == lev:
            return
        try:
            await self._signed("POST", "/fapi/v1/leverage",
                               {"symbol": self.symbol, "leverage": lev})
            self.log.info("Плечо на бирже: %dx", lev)
        except Exception as exc:
            self.log.warning("Плечо %dx не выставилось (%s) — работаю с "
                             "прежним плечом биржи", lev, exc)
        self._lev_current = lev

    async def _ensure_margin_mode(self):
        """Изолированная маржа: в принудительное улетает только маржа
        сделки, а не весь баланс."""
        try:
            await self._signed("POST", "/fapi/v1/marginType",
                               {"symbol": self.symbol,
                                "marginType": "ISOLATED"})
            self.log.info("Маржа: изолированная")
        except RuntimeError as exc:
            if "-4046" in str(exc):      # уже изолированная — не ошибка
                self.log.info("Маржа: изолированная (уже была)")
                return
            self.log.warning("Изолированная маржа не задалась: %s", exc)

    async def _ensure_position_mode(self):
        """Двусторонний режим позиций (утверждено): встречные сделки
        независимы, каждый ордер несёт метку LONG/SHORT. Новые аккаунты
        тестнета по умолчанию односторонние — включаем сами."""
        try:
            await self._signed("POST", "/fapi/v1/positionSide/dual",
                               {"dualSidePosition": "true"})
            self.log.info("Режим позиций: двусторонний (hedge) включён")
        except RuntimeError as exc:
            if "-4059" in str(exc):      # уже двусторонний — не ошибка
                self.log.info("Режим позиций: двусторонний (уже был)")
                return
            self.log.warning("Двусторонний режим не включился: %s — "
                             "ордера с positionSide могут не пройти", exc)

    async def _fetch_balance(self):
        data = await self._signed("GET", "/fapi/v2/balance")
        for item in data:
            if item.get("asset") == "USDT":
                return float(item.get("balance", 0.0))
        return None

    async def _live_price(self):
        """Живая цена с биржи (публичный REST) — None, если недоступна."""
        try:
            self._quota_note(2)      # /fapi/v1/ticker/price, вес 2 (верхняя граница)
            async with aiohttp.ClientSession() as session:
                async with session.get(
                        f"{self._rest_base}/fapi/v1/ticker/price",
                        params={"symbol": self.symbol},
                        timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    data = await resp.json(content_type=None)
            px = float(data.get("price", 0.0))
            return px if px > 0 else None
        except Exception:
            return None

    async def _fetch_positions(self):
        data = await self._signed("GET", "/fapi/v2/positionRisk",
                                  {"symbol": self.symbol})
        return data if isinstance(data, list) else []

    async def _adopt_positions(self):
        """Подхват позиций, оставшихся с прошлого запуска. Стоп — по
        recovery_stop_pct (сценарий неизвестен) — ПРОВЕРИТЬ ВРУЧНУЮ."""
        try:
            positions = await self._fetch_positions()
        except Exception:
            self.log.exception("Подхват позиций не удался — ПРОВЕРЬТЕ ВРУЧНУЮ!")
            return
        for p in positions:
            amt = float(p.get("positionAmt", 0.0))
            if abs(amt) < 1e-12:
                continue
            side = "long" if amt > 0 else "short"
            entry = float(p.get("entryPrice", 0.0)) or self.price
            stop = (entry * (1.0 - self.recovery_stop_pct / 100.0)
                    if side == "long"
                    else entry * (1.0 + self.recovery_stop_pct / 100.0))
            self._deal_seq += 1
            deal_id = f"R{self._deal_seq}"
            deal = {"deal_id": deal_id, "mode": "real", "direction": side,
                    "entry": entry, "stop": stop, "targets": [],
                    "leverage": float(p.get("leverage", 1.0)) or 1.0,
                    "trailing": {"activation_pct": 0.008,
                                 "distance_pct": 0.006},
                    "qty": abs(amt), "notional": abs(amt) * entry,
                    "scenario_seq": 0,
                    "opened_ms": int(time.time() * 1000),
                    "be_done": False,
                    "note": "восстановлена после перезапуска "
                            "(стоп по умолчанию — ПРОВЕРЬ ВРУЧНУЮ)"}
            self._deals[deal_id] = deal
            self.opened += 1
            self.log.warning("НАЙДЕНА позиция после перезапуска: %s %.6f по "
                             "%.5f — взята под ведение (%s), стоп %.5f",
                             side, abs(amt), entry, deal_id, stop)
            try:
                await self._place_stop(deal)
            except Exception as exc:
                self.log.error("Стоп для восстановленной %s не встал: %s",
                               deal_id, exc)

    async def _place_stop(self, deal, price=None):
        """STOP-MARKET на бирже (Algo API — классический /order условные
        типы больше не принимает, ошибка -4120). КОЛИЧЕСТВЕННЫЙ стоп
        (на объём сделки): встречные и параллельные сделки одной стороны
        независимы — closePosition запрещал бы второй стоп на сторону
        (ошибка -4130). Позиция защищена даже при падении системы."""
        long = deal["direction"] == "long"
        stop = price if price is not None else deal["stop"]
        # ИДЕМПОТЕНТНОСТЬ СТОПА: номер попытки в рамках сделки. Повтор после
        # таймаута сети получит тот же id и будет отвергнут биржей как
        # дубликат, а не поставит второй стоп на ту же позицию.
        deal["_stop_seq"] = int(deal.get("_stop_seq", 0)) + 1
        data = await self._signed("POST", "/fapi/v1/algoOrder", {
            "symbol": self.symbol,
            "side": "SELL" if long else "BUY",
            "positionSide": "LONG" if long else "SHORT",
            "algotype": "CONDITIONAL",
            "type": "STOP_MARKET",
            "triggerprice": self._fmt_price(stop),
            "quantity": self._fmt_qty(deal["qty"]),
            "clientAlgoId": self._cid(deal["deal_id"], f"s{deal['_stop_seq']}")})
        if data.get("duplicate"):
            self.log.info("Стоп %s: повтор с тем же id отвергнут биржей — "
                          "стоп уже стоит (%.5f)", deal["deal_id"], stop)
        self._stop_orders[deal["deal_id"]] = data.get("algoId")
        self._stop_price[deal["deal_id"]] = stop
        return data

    async def _cancel_stop(self, deal):
        oid = self._stop_orders.pop(deal["deal_id"], None)
        self._stop_price.pop(deal["deal_id"], None)
        if oid is None:
            return
        try:
            await self._signed("DELETE", "/fapi/v1/algoOrder",
                               {"symbol": self.symbol, "algoId": oid})
        except RuntimeError as exc:
            if "-2011" not in str(exc):  # уже исполнен/отменён биржей
                raise

    async def _sync_stop(self, deal):
        """Биржевой стоп обязан совпадать с нашим. Перенос не чаще
        раза в 5 сек; не перенёсся — экстренное закрытие (без стопа
        позицию не оставляем)."""
        now = time.time()
        if now - self._last_stop_move < 5.0:
            return
        if abs(self._stop_price.get(deal["deal_id"], 0.0)
               - deal["stop"]) < 1e-12:
            return                       # на бирже уже этот стоп
        self._last_stop_move = now
        try:
            await self._cancel_stop(deal)
            await self._place_stop(deal)
        except Exception as exc:
            if "-2021" in str(exc):
                # цена уже за стопом — биржа права, стоп фактически сработал
                await self._close(deal, self.price,
                                  "стоп (цена ушла за стопом)")
                return
            self.log.error("Перенос стопа %s не удался: %s — экстренное "
                           "закрытие", deal["deal_id"], exc)
            await self._close(deal, self.price, "экстренно: стоп не перенёсся")

    async def _reconcile(self):
        """Тройная сверка (валидация): биржевой баланс против нашего
        расчёта = старт + закрытые реальные сделки ± открытые. И тень
        сверяется с фактом тюнером по фактам сделок."""
        if not self._has_keys or self._bal_start is None:
            return
        try:
            actual = await self._fetch_balance()
            unreal = 0.0
            for p in await self._fetch_positions():
                unreal += float(p.get("unRealizedProfit", 0.0))
        except Exception:
            self.log.exception("Сверка: биржа не ответила")
            return
        expected = self._bal_start + self._real_pnl + unreal
        diff = actual - expected
        if abs(diff) > self.recon_tol:
            self.log.warning("СВЕРКА: расхождение %+.2f USDT (факт %.2f, "
                             "расчёт %.2f = старт %.2f + закрыто %+.2f + "
                             "открыто %+.2f) — комиссия/фандинг/ручные действия?",
                             diff, actual, expected, self._bal_start,
                             self._real_pnl, unreal)
        else:
            self.log.info("Сверка: сходится (факт %.2f ≈ расчёт %.2f, "
                          "откл. %+.2f USDT)", actual, expected, diff)
