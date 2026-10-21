"""«OI + Каскад» — вердикты над полками и датчик лавины (утверждено).

Двухуровневый датчик:
- БЫСТРЫЙ: поток ликвидаций (!forceOrder) — частота и объём принтов;
  всплеск сверх нормы = ЛАВИНА, карточка выходит мгновенно;
- МЕДЛЕННЫЙ: опрос открытого интереса (/fapi/v1/openInterest, публичный)
  — бухгалтерия позиций, ΔOI за окно наблюдения.

Вердикт над полкой (level-verdict): цена подошла к зоне из картины →
модуль «берёт полку на час» (окно 3 мин) и смотрит исход:
- ПРОБОЙ: цена ушла за уровень и ΔOI вырос ≥ порога — топливо пришло,
  полка переносится на другую сторону;
- ВЫНОС: цена вылетела за уровень и вернулась, ΔOI не вырос (а часто
  лавина ликвидаций была) — полка выгорела;
- НЕЯСНО — карточка не пишется (мусорных вердиктов не бывает).
"""
import collections
import time

import aiohttp
import orjson

from core.module import Manifest, NeirModule
from modules._labels import CARD, DRAFT, LIQUIDATIONS, OI_SNAPSHOT, PICTURE, PRICE

OI_REST = "https://fapi.binance.com/fapi/v1/openInterest"
# Вес эндпоинта по таблице биржи (оценочно; при сомнениях — верхняя граница).
OI_WEIGHT = 5             # вес эндпоинта OI по таблице биржи (метаданные, не ручка)
# Окно лавины (liq_window_sec) и предел полок «на часу» (watch_arm_limit) — в конфиг [oi].


class OiCascade(NeirModule):
    manifest = Manifest(name="oi-cascade", title="OI + Каскад (вердикты полок)",
                        requires=("price", "card"), priority=47,
                        tick_timeout=15.0)

    async def on_start(self):
        self.symbol = self.kernel.cfg.symbol.upper()
        c = self.kernel.cfg
        self.poll_sec = c.oi_poll_sec
        self.approach = c.oi_approach_pct / 100.0
        self.watch_sec = c.oi_watch_min * 60
        self.oi_breakout = c.oi_breakout_pct
        self.liq_burst_min = c.oi_liq_burst_min
        self.liq_window = c.oi_liq_window_sec
        self.watch_arm_limit = c.oi_watch_arm_limit

        self._oi = 0.0
        self.price = 0.0
        self._zones = []          # активные полки из картины
        self._armed = {}          # ключ -> наблюдение
        self._liqs = collections.deque()                # (ts, qty, side)
        self._liq_rate = 0.0      # обычная частота ликвидаций/мин
        self.oi_polls = 0
        self.verdicts = 0
        self.bursts = 0

        self.subscribe(PRICE, self._on_price)
        self.subscribe(CARD, self._on_card)
        self.subscribe(LIQUIDATIONS, self._on_liquidation)
        self.every(self.poll_sec, self._poll_oi)
        self.every(1.0, self._watch_check)
        self.log.info("OI + Каскад включены: опрос OI раз в %dс, подход %.2f%%, "
                      "окно %.0f мин, лавина от %d ликвидаций/мин",
                      self.poll_sec, self.approach * 100,
                      c.oi_watch_min, self.liq_burst_min)

    async def on_stop(self):
        self.log.info("Итог: опросов OI %d, вердиктов %d, лавин %d",
                      self.oi_polls, self.verdicts, self.bursts)

    # ---------- приём событий ----------

    async def _on_price(self, env):
        self.price = float(env.payload.get("price", 0.0))
        # Пересечение уровня фиксируем В МОМЕНТ прихода цены — спайк
        # «пробила и вернулась» между секундными проверками терять нельзя
        for w in self._armed.values():
            if w["pull"] == "up":
                if self.price > w["high"]:
                    w["crossed"] = True
                if w["crossed"] and self.price < w["low"]:
                    w["returned"] = True
            else:
                if self.price < w["low"]:
                    w["crossed"] = True
                if w["crossed"] and self.price > w["high"]:
                    w["returned"] = True

    async def _on_card(self, env):
        card = env.payload
        if card.get("kind") != "picture":
            return
        f = card.get("fields") or {}
        zones = []
        for z in f.get("zones") or []:
            try:
                zones.append({"low": float(z["low"]), "high": float(z["high"]),
                              "pull": z.get("pull", "unknown")})
            except (KeyError, TypeError, ValueError):
                continue
        self._zones = zones

    async def _on_liquidation(self, env):
        p = env.payload
        now = time.time()
        self._liqs.append((now, float(p.get("qty", 0.0)),
                           p.get("side", "")))
        edge = now - self.liq_window
        while self._liqs and self._liqs[0][0] < edge:
            self._liqs.popleft()
        rate = len(self._liqs)          # ликвидаций за минуту
        if rate >= self.liq_burst_min:
            self._publish_burst(now, rate)

    def _publish_burst(self, now, rate):
        volume = sum(q for _, q, _ in self._liqs)
        longs = sum(1 for _, _, s in self._liqs if s == "SELL")
        bias = longs / rate if rate else 0.5     # доля ликвидированных лонгов
        self.publish(DRAFT, {
            "kind": "liquidation-burst",
            "author": "oi-cascade",
            "confidence": round(min(0.9, 0.4 + 0.1 * rate), 3),
            "fields": {
                "rate_per_min": rate,
                "volume": round(volume, 2),
                "long_liquidated_share": round(bias, 3),
                "window_sec": self.liq_window,
            },
            "lineage": [f"trade:{self.symbol}:{int(now)}"],
        })
        self.bursts += 1
        self.log.warning("ЛАВИНА: %d ликвидаций/мин, объём %.0f, "
                         "доля лонгов %.0f%%",
                         rate, volume, bias * 100)

    # ---------- медленный датчик: открытый интерес ----------

    async def _poll_oi(self):
        try:
            quoter = self.kernel.modules.get("quoter")
            if quoter is not None and quoter.throttled():
                return                  # потолок близко — этот опрос пропущен
            if quoter is not None:
                quoter.note(OI_WEIGHT)  # до отправки: биржа начислит вес в любом случае
            async with aiohttp.ClientSession() as session:
                async with session.get(OI_REST,
                                       params={"symbol": self.symbol},
                                       timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status != 200:
                        return
                    data = orjson.loads(await resp.read())
            oi = float(data.get("openInterest", 0.0))
            if oi <= 0:
                return
            self._oi = oi
            self.oi_polls += 1
            # Снимок для Проводимости (один опрос OI на двоих — утверждено)
            self.publish(OI_SNAPSHOT, {"oi": oi,
                                       "ts_ms": int(time.time() * 1000)})
        except Exception:
            self.log.debug("Опрос OI не удался (сеть?)")

    # ---------- час над полкой: вооружение и вердикт ----------

    async def _watch_check(self):
        """Раз в секунду: вооружаем полки у цены и подводим итоги окон."""
        now = time.time()
        if self.price > 0 and self._oi > 0:
            self._arm_zones(now)
        self._finish_watches(now)

    def _arm_zones(self, now):
        for z in self._zones:
            mid = (z["low"] + z["high"]) / 2.0
            if mid <= 0:
                continue
            dist = abs(self.price - mid) / mid
            key = round(mid, 6)
            if dist > self.approach:
                continue
            if key in self._armed:
                continue
            if len(self._armed) >= self.watch_arm_limit:
                continue
            self._armed[key] = {
                "low": z["low"], "high": z["high"], "pull": z["pull"],
                "armed_ts": now, "oi_before": self._oi,
                "price_before": self.price,
                "crossed": False, "extreme": self.price,
                "burst": False,
            }
            self.log.info("Полка %.5f взята на час (%.2f%% от цены)",
                          mid, dist * 100)

    def _finish_watches(self, now):
        for key, w in list(self._armed.items()):
            self._track(w)
            if now - w["armed_ts"] < self.watch_sec:
                continue
            self._armed.pop(key, None)
            self._verdict(key, w)

    def _track(self, w):
        """Лавина в окне наблюдения: частота ликвидаций в окне лавины достигла
        порога (пересечения ловятся в _on_price). Раньше флаг ставился от ЛЮБОЙ
        ликвидации — на активном рынке это почти всегда true, поэтому не отличал
        вынос от пробоя. Теперь — только настоящая лавина."""
        if len(self._liqs) >= self.liq_burst_min:
            w["burst"] = True

    def _verdict(self, key, w):
        if not w["crossed"]:
            return                          # до уровня не дошли — вердикта нет
        # ΔOI = бухгалтерия от момента взятия полки на час
        if w.get("oi_before", 0.0) > 0 and self._oi > 0:
            oi_delta = (self._oi - w["oi_before"]) / w["oi_before"] * 100.0
        else:
            oi_delta = None
        returned = w.get("returned", False)
        if oi_delta is None:
            outcome, prob = "unclear", 0.3
        elif not returned and oi_delta >= self.oi_breakout:
            outcome, prob = "breakout", min(0.9, 0.5 + 0.1 * abs(oi_delta))
        elif returned and (oi_delta <= self.oi_breakout or w["burst"]):
            outcome, prob = "sweep", min(0.9, 0.5 + 0.1 * (abs(oi_delta) + (2.0 if w["burst"] else 0.0)))
        else:
            outcome, prob = "unclear", 0.4
        if outcome == "unclear" and not w["burst"]:
            return                          # мусорных вердиктов не бывает
        self.verdicts += 1
        self.publish(DRAFT, {
            "kind": "level-verdict",
            "author": "oi-cascade",
            "confidence": round(prob, 3),
            "fields": {
                "level": key,
                "zone_low": round(w["low"], 8),
                "zone_high": round(w["high"], 8),
                "outcome": outcome,
                "oi_delta_pct": round(oi_delta, 3) if oi_delta is not None else None,
                "liquidation_burst": bool(w["burst"]),
                "price_returned": bool(returned),
                "window_min": round(self.watch_sec / 60.0, 1),
            },
            "lineage": [f"price:{self.symbol}:{int(w['armed_ts'])}"],
        })
        self.log.warning("ВЕРДИКТ по полке %.5f: %s (ΔOI %s%%, лавина %s, "
                         "возврат %s, уверенность %.2f)",
                         key, outcome,
                         f"{oi_delta:+.2f}" if oi_delta is not None else "н/д",
                         "была" if w["burst"] else "нет",
                         "да" if returned else "нет", prob)

