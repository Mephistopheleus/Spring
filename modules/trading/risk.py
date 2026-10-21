"""Риск-менеджер NeirOS-M — единственные ворота к сделке.

Утверждено: решение целиком за риск-менеджером. Разрешил — сделка
идёт в работу; запретил — уходит в теневой просчёт (тоже через Дилера).

Проверки перед вердиктом (Риск — ОЦЕНЩИК, объём не конструирует):
- сценарий жив (не протух) и полон;
- ЖЕЛЕЗНОЕ ПРАВИЛО ПЛЕЧА: ликвидация обязана наступить позже стопа
  (расстояние ликвидации ≈ 1/плечо должно превышать стоп × запас);
- объём — ПАРАМЕТР СЦЕНАРИЯ (утверждено): маржа = баланс × position_pct
  (долю задаёт Сценарист), объём = маржа × плечо; не ниже минимума биржи;
- УРОВЕНЬ РИСКА СДЕЛКИ: потеря при стопе ≤ max_risk_pct (% баланса);
- риск/прибыль: расчётный профит ≥ min_rr × расчётный убыток;
- ПОРОГ РЕАЛА: уверенность сценария ниже порога — только теневой просчёт;
- планки занятости маржи (утверждено): суммарная маржа всех открытых
  РЕАЛЬНЫХ сделок ≤ 65% баланса; при уверенности выше confidence_high
  (0.9) — до 75%; лимита ПО КОЛИЧЕСТВУ нет (утверждено).

"Рука на пульсе": следит за открытыми РЕАЛЬНЫМИ сделками — при
аварийном убытке по позиции шлёт Дилеру команду немедленного закрытия
(личным конвертом — потерять её нельзя).
"""
import hashlib
import hmac
import time
from collections import deque
from urllib.parse import urlencode

import aiohttp
import orjson

from core.module import Manifest, NeirModule
from modules._labels import CARD, DRAFT, PRICE

MIN_NOTIONAL = 5.0      # минимальный объём ордера на Binance Futures, USDT


class RiskManager(NeirModule):
    manifest = Manifest(name="risk-manager", title="Риск-менеджер",
                        requires=("card",), priority=80, tick_timeout=10.0)

    async def on_start(self):
        self.margin_base_pct = self.kernel.cfg.risk_max_margin_total_pct
        self.margin_high_pct = self.kernel.cfg.risk_margin_high_pct
        self.conf_high = self.kernel.cfg.risk_confidence_high
        self.liq_buffer = self.kernel.cfg.risk_liq_buffer
        self.emergency_loss = self.kernel.cfg.risk_emergency_loss_pct / 100.0
        self.open_real = 0
        self._open_deals = {}     # deal_id -> данные для «пульса» и планок
        self.verdicts = 0
        # РЕАНИМАТОР (Этап 34): лестница реакции на внезапный разворот
        self.reanim_enabled = self.kernel.cfg.reanimate_enabled
        self.reanim_lookback = self.kernel.cfg.reanimate_lookback_sec
        self.reanim_velocity = self.kernel.cfg.reanimate_velocity_pct / 100.0
        self.reanim_profit = self.kernel.cfg.reanimate_profit_pct / 100.0
        self._price_hist = deque()            # (ts_ms, price) — окно скорости
        self.reanim_closes = 0
        self.reanim_breakevens = 0
        self.price = 0.0
        self.session_start_ms = int(time.time() * 1000)
        self.entry_dev = self.kernel.cfg.risk_entry_dev_pct / 100.0
        # Фильтры свежести: сценарий жив, пока жив его горизонт (срок
        # смерти по часам убран — решение хозяина)
        self.log.info("Фильтры свежести: сценарии только текущей сессии, "
                      "вход ±%.2f%% от рынка", self.entry_dev * 100)

        # --- баланс: выделено vs доступно (утверждено) ---
        alloc = self.kernel.cfg.start_balance
        self._alloc = alloc                            # для сверки (Этап 34)
        self.balance = alloc if alloc > 0 else 100.0   # запасной для тени
        self._time_offset = 0                          # поправка часов (ниже)
        acct = self.kernel.cfg.binance_account
        if acct is not None:
            self._time_offset = await self._fetch_time_offset(acct)
            if abs(self._time_offset) > 500:
                self.log.warning("Локальные часы %s на %d мс относительно "
                                 "биржи — подписи скорректированы",
                                 "отстают" if self._time_offset > 0
                                 else "спешат", abs(self._time_offset))
            available = await self._available_usdt(acct)
            if available is not None:
                where = "ТЕСТНЕТ" if acct["testnet"] else "биржа"
                if alloc <= 0:
                    self.balance = available
                    self.log.info("Выделено 0 — работаем на ВСЁМ доступном: "
                                  "%.2f USDT (%s)", available, where)
                elif alloc > available:
                    self.log.error("ПРЕВЫШЕНИЕ БАЛАНСА: выделено %.2f USDT, "
                                   "доступно %.2f USDT (%s) — ОСТАНОВКА",
                                   alloc, available, where)
                    self.kernel.request_stop()
                    return
                else:
                    self.log.info("Выделено %.2f из доступных %.2f USDT (%s)",
                                  alloc, available, where)
        else:
            if alloc <= 0:
                self.log.warning("Выделено 0, но ключей биржи нет — "
                                 "уточнить баланс негде, работаю в тени "
                                 "с запасным балансом %.2f USDT", self.balance)
            else:
                self.log.info("Ключей биржи нет — теневой режим, "
                              "выделено %.2f USDT", alloc)

        self.max_margin = self.balance * self.kernel.cfg.max_margin_pct / 100.0
        self.subscribe(CARD, self._on_card)
        self.subscribe(PRICE, self._on_price)
        self.every(600, self._balance_check)   # сверка выделения (Этап 34)
        self.every(1800, self._refresh_time_offset)   # часы плывут — сверяемся
        self.log.info("Баланс %.2f USDT, лимит по количеству — НЕТ "
                      "(планка маржи %.0f%%/%.0f%% при уверенности > %.2f)",
                      self.balance, self.margin_base_pct,
                      self.margin_high_pct, self.conf_high)

    async def _fetch_time_offset(self, acct):
        """Смещение локальных часов относительно биржи, мс (публичный
        запрос).         Локальное время спешит → подпись уходит в будущее →
        код биржи -1021 (HTTP 400): воспроизводилось на живом прогоне."""
        try:
            q = self.kernel.modules.get("quoter")
            if q is not None:
                q.note(1)             # /fapi/v1/time — вес 1 (до отправки)
            async with aiohttp.ClientSession() as session:
                async with session.get(acct["base"] + "/fapi/v1/time",
                                       timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    data = await resp.json(content_type=None)
            server_ms = int(data.get("serverTime", 0))
            if server_ms > 0:
                return server_ms - int(time.time() * 1000)
        except Exception:
            pass
        return 0

    async def _refresh_time_offset(self):
        """Периодическая сверка часов с биржей (см. Дилер)."""
        acct = self.kernel.cfg.binance_account
        if acct is None:
            return
        self._time_offset = await self._fetch_time_offset(acct)

    async def _available_usdt(self, acct):
        """Доступный USDT на фьючерсном счёте (подписанный запрос) или None."""
        try:
            params = {"timestamp": int(time.time() * 1000)
                      + getattr(self, "_time_offset", 0),
                      "recvWindow": 5000}
            query = urlencode(params)
            sig = hmac.new(acct["secret"].encode(), query.encode(),
                           hashlib.sha256).hexdigest()
            url = f"{acct['base']}/fapi/v2/balance?{query}&signature={sig}"
            headers = {"X-MBX-APIKEY": acct["key"]}
            q = self.kernel.modules.get("quoter")
            if q is not None:
                # подписанное чтение баланса, вес 5 (верхняя граница таблицы)
                q.note(5)
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers,
                                       timeout=aiohttp.ClientTimeout(total=8)) as resp:
                    if resp.status != 200:
                        self.log.error("Баланс недоступен: HTTP %s", resp.status)
                        return None
                    data = orjson.loads(await resp.read())
            for entry in data:
                if entry.get("asset") == "USDT":
                    return float(entry.get("availableBalance", 0.0))
            return 0.0
        except Exception as exc:
            # Сетевой сбой — не «сбой модуля»: сверка пропустится,
            # следующий тик попробует снова (стойка трассировки не нужна).
            self.log.warning("Не удалось получить баланс с биржи (%s) — "
                             "сверка пропущена", exc.__class__.__name__)
            return None

    async def _on_card(self, env):
        card = env.payload
        kind = card.get("kind")
        if kind == "scenario":
            await self._evaluate(int(card["seq"]),
                                 float(card.get("confidence", 0.0)),
                                 card.get("fields") or {},
                                 int(card.get("created_ms", 0)))
        elif kind == "deal-opened":
            f = card.get("fields") or {}
            if f.get("mode") == "real":
                self.open_real += 1
            entry = float(f.get("entry", 0.0))
            leverage = float(f.get("leverage", 1.0)) or 1.0
            notional = (float(f.get("notional", 0.0))
                        or float(f.get("quantity", 0.0)) * entry)
            self._open_deals[f.get("deal_id")] = {
                "direction": f.get("direction"),
                "entry": entry,
                "leverage": leverage,
                "mode": f.get("mode"),
                "margin": notional / leverage if notional else 0.0,
            }
        elif kind == "deal-closed":
            f = card.get("fields") or {}
            if f.get("mode") == "real":
                self.open_real = max(0, self.open_real - 1)
            self._open_deals.pop(f.get("deal_id"), None)

    async def _balance_check(self):
        """СВЕРКА ВЫДЕЛЕНИЯ (Этап 34): деньги на счёте обязаны покрывать
        выделенное; выделено 0 — живём на всё доступное, баланс живой."""
        acct = self.kernel.cfg.binance_account
        if acct is None:
            return
        available = await self._available_usdt(acct)
        if available is None:
            self.log.warning("Сверка выделения: биржа не ответила")
            return
        if self._alloc > 0:
            if available < self._alloc:
                self.log.error("СВЕРКА ВЫДЕЛЕНИЯ: доступно %.2f USDT — "
                               "меньше выделенных %.2f USDT (дрейф баланса?)",
                               available, self._alloc)
        elif abs(available - self.balance) > 0.01:
            self.log.info("Выделено 0 — баланс живой: %.2f → %.2f USDT",
                          self.balance, available)
            self.balance = available

    async def _on_price(self, env):
        p = float(env.payload.get("price", 0.0))
        if p <= 0:
            return                     # фиктивная цена не участвует ни в чём
        self.price = p
        # РЕАНИМАТОР (Этап 34): окно скорости цены
        if self.reanim_enabled:
            now_ms = int(time.time() * 1000)
            self._price_hist.append((now_ms, p))
            cutoff = now_ms - self.reanim_lookback * 1000.0
            while self._price_hist and self._price_hist[0][0] < cutoff:
                self._price_hist.popleft()
        for deal_id, deal in list(self._open_deals.items()):
            if deal["mode"] != "real" or deal["entry"] <= 0:
                continue
            sign = 1.0 if deal["direction"] == "long" else -1.0
            loss = ((self.price - deal["entry"]) / deal["entry"] * sign
                    * deal["leverage"])
            if loss <= -self.emergency_loss:
                self.log.warning("АВАРИЯ по %s: %.1f%% маржи — команда закрытия",
                                 deal_id, loss * 100)
                self.send("dealer", {"command": "close", "deal_id": deal_id,
                                     "reason": "аварийное снятие риск-менеджера"})
                continue
            # ЛЕСТНИЦА РЕАНИМАЦИИ: внезапный разворот против позиции
            if not self.reanim_enabled or len(self._price_hist) < 2:
                continue
            p_then = self._price_hist[0][1]
            adverse = (p_then - p) / p_then * sign
            if adverse < self.reanim_velocity:
                continue
            gain = (p - deal["entry"]) / deal["entry"] * sign
            sent = deal.setdefault("_reanim_sent", set())
            if gain >= self.reanim_profit and "close" not in sent:
                sent.add("close")
                self.reanim_closes += 1
                self.log.warning("РЕАНИМАЦИЯ %s: разворот %.2f%% за %.0fс "
                                 "при плюсе %.2f%% — фиксация в плюс",
                                 deal_id, adverse * 100, self.reanim_lookback,
                                 gain * 100)
                self.send("dealer", {"command": "close", "deal_id": deal_id,
                                     "reason": "реанимация: внезапный разворот — "
                                               "фиксация в плюс"})
            elif gain >= 0.0 and "breakeven" not in sent:
                sent.add("breakeven")
                self.reanim_breakevens += 1
                self.log.warning("РЕАНИМАЦИЯ %s: разворот %.2f%% за %.0fс — "
                                 "стоп в безубыток", deal_id, adverse * 100,
                                 self.reanim_lookback)
                self.send("dealer", {"command": "breakeven", "deal_id": deal_id,
                                     "reason": "реанимация: внезапный разворот — "
                                               "стоп в безубыток"})

    # ---------- вердикт ----------

    async def _evaluate(self, scenario_seq, scenario_conf, f, created_ms=0):
        now_ms = int(time.time() * 1000)
        decision, reason, quantity, notional = "forbid", "", 0.0, 0.0

        # ФИЛЬТРЫ СВЕЖЕСТИ (утверждено после тестнет-аудита): склад не
        # имеет права «оживлять» старые сценарии — только текущая сессия,
        # не истёк срок, вход рядом с живой ценой
        if created_ms and created_ms < self.session_start_ms:
            reason = "сценарий чужой сессии (архив склада)"
        elif int(f.get("valid_until_ms", 0)) < now_ms:
            reason = "сценарий протух"
        elif f.get("direction") not in ("long", "short"):
            reason = "некуда идти: нет направления"
        elif (self.price > 0 and float(f.get("entry", 0.0)) > 0
              and abs(float(f["entry"]) - self.price) / self.price
              > self.entry_dev):
            reason = (f"вход далеко от рынка: {f['entry']} vs {self.price}")

        if not reason:
            entry = float(f.get("entry", 0.0))
            stop = float(f.get("stop", 0.0))
            leverage = max(1.0, float(f.get("leverage", 1.0)))
            stop_pct = abs(entry - stop) / entry if entry > 0 else 0.0
            if entry <= 0 or stop_pct <= 0:
                reason = "сценарий без входа/стопа"
            elif 1.0 / leverage <= stop_pct * self.liq_buffer:
                # ЖЕЛЕЗНОЕ ПРАВИЛО: ликвидация раньше стопа — сделки нет
                reason = (f"ликвидация раньше стопа (плечо {leverage:.1f}x, "
                          f"стоп {stop_pct*100:.2f}%)")
            else:
                # ОБЪЁМ — ПАРАМЕТР СЦЕНАРИЯ (утверждено): долю баланса в
                # марже задаёт Сценарист (position_pct). Риск ничего не
                # конструирует — только ОЦЕНИВАЕТ уровень риска сделки.
                pos_pct = float(f.get("position_pct", 1.5))
                margin = self.balance * pos_pct / 100.0
                notional = margin * leverage
                # МИНИМУМ ОБЪЁМА (Этап 35): живой фильтр биржи (Дилер
                # тянет из exchangeInfo); константа — fallback до фильтров
                dealer = self.kernel.modules.get("dealer")
                min_not = float(getattr(dealer, "min_notional", 0.0)
                                or MIN_NOTIONAL)
                below_min = False
                if notional < min_not:
                    # ТЕНЕВОЙ ПРОСЧЁТ ЧЕСТНЫМ ОБЪЁМОМ (утверждено): считаем
                    # с биржевым минимумом — иначе тень бесполезна тюнеру.
                    # РЕАЛ при этом заблокирован (ниже минимума биржи)
                    notional = min_not
                    margin = notional / leverage
                    below_min = True
                quantity = notional / entry
                # Планка занятости маржи: 65%, при высокой уверенности 75%
                used = sum(d.get("margin", 0.0)
                           for d in self._open_deals.values()
                           if d.get("mode") == "real")
                cap_pct = (self.margin_high_pct
                           if scenario_conf >= self.conf_high
                           else self.margin_base_pct)
                cap = self.balance * cap_pct / 100.0
                # УРОВЕНЬ РИСКА СДЕЛКИ (оценка): потеря при стопе, % баланса
                risk_of_balance = (notional * stop_pct
                                   / self.balance * 100.0)
                max_risk = self.kernel.cfg.risk_max_risk_pct
                conf_min = self.kernel.cfg.risk_open_conf_threshold
                min_rr = self.kernel.cfg.risk_min_rr
                exp_p = float(f.get("expected_pnl_pct", 0.0) or 0.0)
                exp_l = float(f.get("expected_loss_pct", 0.0) or 0.0)
                if below_min:
                    reason = (f"маржа {pos_pct:.1f}% x плечо "
                              f"{leverage:.1f} = {margin:.2f} USDT — ниже "
                              f"минимума биржи: РЕАЛ заблокирован, "
                              f"теневой просчёт с {min_not:.0f} USDT")
                elif used + margin > cap:
                    reason = (f"планка маржи {cap_pct:.0f}% исчерпана "
                              f"({used:.2f} из {cap:.2f} USDT)")
                elif risk_of_balance > max_risk:
                    # РИСК ВЫШЕ ЛИМИТА (утверждено)
                    reason = (f"риск сделки {risk_of_balance:.2f}% баланса "
                              f"выше лимита {max_risk:.1f}% — "
                              f"теневой просчёт")
                elif scenario_conf < conf_min:
                    # ПОРОГ РЕАЛА (утверждено): уверенность ниже —
                    # только ТЕНЕВОЙ ПРОСЧЁТ с честным объёмом
                    reason = (f"уверенность {scenario_conf:.2f} < "
                              f"порога реала {conf_min:.2f} — "
                              f"теневой просчёт")
                elif exp_l > 0 and exp_p < exp_l * min_rr:
                    # РИСК НЕ ОКУПАЕТСЯ (утверждено)
                    reason = (f"риск не окупается: +{exp_p:.2f}% "
                              f"против −{exp_l:.2f}% (нужно "
                              f"×{min_rr:.1f}) — теневой просчёт")
                else:
                    decision = "allow"
                    reason = (f"риск {risk_of_balance:.2f}% баланса в "
                              f"норме, маржа {margin:.2f}/{cap:.2f} "
                              f"USDT (планка {cap_pct:.0f}%)")

        self.publish(DRAFT, {
            "kind": "verdict",
            "author": "risk-manager",
            "confidence": scenario_conf,
            "fields": {
                "scenario_seq": scenario_seq,
                "decision": decision,
                "mode": "real" if decision == "allow" else "shadow",
                "reason": reason,
                "quantity": round(quantity, 6),
                "notional": round(notional, 2),
                "balance": self.balance,
                # РАСЧЁТНЫЙ ИТОГ В ДЕНЬГАХ (утверждено): notional × чистый ход
                "expected_pnl_usdt": round(
                    notional * float(f.get("net_move_pct", 0.0)) / 100.0, 2),
                "expected_loss_usdt": round(
                    notional * float(f.get("stop_pct", 0.0)), 2),
                "scenario": f,
            },
            "lineage": [f"card:{scenario_seq}"],
        })
        self.verdicts += 1
        # Решение Дилеру — ЛИЧНЫМ конвертом: потерять вердикт нельзя
        self.send("dealer", {
            "verdict": decision,
            "mode": "real" if decision == "allow" else "shadow",
            "reason": reason,
            "quantity": quantity,
            "balance": self.balance,
            "confidence": scenario_conf,
            "scenario_seq": scenario_seq,
            "scenario": f,
        })
        self.log.info("Вердикт по сценарию №%d: %s (%s) — %s",
                      scenario_seq, decision,
                      "реал" if decision == "allow" else "тень", reason)
