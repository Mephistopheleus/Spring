"""«Детектор Спуфинга» (Этап 21, утверждено) — фальшивые стены стакана.

Сырьё: ORDERBOOK (20 уровней × обе стороны, каждые 100 мс) + PRICE
(принты — чтобы отличить «съедена» от «снята»).

Механика (утверждено): стена = уровень с объёмом ≥ wall_multiple ×
медиана уровней стороны. Спуфинг = ТРОЙНОЕ СОВПАДЕНИЕ:
1. цена подошла к уровню ближе approach_pct;
2. стена исчезла за ≤ vanish_ticks_max тиков (мгновенный триггер —
   HFT снимает фейки за 100–200 мс; vanish_sec — внешний потолок);
3. в ленте на уровне НЕТ принтов (не съедена — именно снята).

Защита от ложняков: дрейф уровня в пределах допуска — та же стена;
выехал из топ-20 без подхода цены — не спуфинг, просто наблюдение.

Карточка kind: "spoof-alert" (сухие факты: сторона, уровень, пик,
доля снятого, возраст стены, уверенность с повторностью).
Потребитель — Поле: совпавшая полка помечается фальшивой
(fake_shelf_factor, срок жизни вдвое) — картина честно тоньше.
"""
import collections
import time

from core.module import Manifest, NeirModule
from modules._labels import DRAFT, ORDERBOOK, PRICE


class Spoof(NeirModule):
    manifest = Manifest(name="spoof", title="Детектор Спуфинга",
                        priority=47, tick_timeout=5.0)

    async def on_start(self):
        c = self.kernel.cfg
        self.wall_multiple = c.spoof_wall_multiple
        self.vanish_pct = c.spoof_vanish_pct
        self.vanish_ticks_max = c.spoof_vanish_ticks_max
        self.vanish_sec = c.spoof_vanish_sec
        self.approach_pct = c.spoof_approach_pct
        self.history_sec = c.spoof_history_sec
        self.drift = 0.0003               # дрейф уровня = та же стена
        self.issues = 0
        # стены: ключ (side, level) -> состояние
        self._walls = {}
        # принты у уровня: (ts_ms, price) — «съедена, не снята»
        self._prints = collections.deque(maxlen=3000)
        self._seen = {}                   # (side, level) -> счётчик спуфов
        self._price = 0.0                 # последняя цена (из принтов)

        self.subscribe(ORDERBOOK, self._on_book)
        self.subscribe(PRICE, self._on_print)

    async def on_stop(self):
        self.log.info("Итог: спуф-алертов %d, стен под наблюдением %d",
                      self.issues, len(self._walls))

    # ---------- приём ----------

    async def _on_print(self, env):
        p = env.payload
        try:
            self._price = float(p.get("price", 0.0))
            self._prints.append((int(p.get("ts_ms", 0)), self._price))
        except (TypeError, ValueError):
            return

    async def _on_book(self, env):
        p = env.payload
        now_ms = int(p.get("ts_ms", 0)) or int(time.time() * 1000)
        now_s = now_ms / 1000.0
        self._walls = {k: w for k, w in self._walls.items()
                       if now_s - w["last_ts"] < self.history_sec}
        try:
            book = {"bid": [(float(pr), float(q)) for pr, q in p.get("bids", [])],
                    "ask": [(float(pr), float(q)) for pr, q in p.get("asks", [])]}
        except (TypeError, ValueError):
            return

        for side, levels in book.items():
            if not levels:
                continue
            sizes = sorted(q for _, q in levels)
            median = sizes[len(sizes) // 2]
            if median <= 0:
                continue
            seen = set()
            for pr, q in levels:
                key = (side, pr)
                seen.add(key)
                w = self._walls.get(key)
                if w is None:
                    if q >= self.wall_multiple * median:
                        self._walls[key] = {"side": side, "level": pr,
                                            "peak": q, "size": q,
                                            "first_ts": now_s,
                                            "last_ts": now_s, "gone": 0,
                                            "approached": False}
                    continue
                self._walls[key] = self._track(w, key, pr, q, now_s)

            # флаг подхода цены для всех стен этой стороны
            for (s2, lvl), w in self._walls.items():
                if s2 == side:
                    w["approached"] = self.approached(s2, lvl, self._price)

            # стены, которых в этом тике НЕТ на доске
            for key, w in list(self._walls.items()):
                if w["side"] != side or key in seen:
                    continue
                w["gone"] += 1
                w["last_ts"] = now_s
                if w["gone"] == 1:
                    w["size"] = 0.0       # стена снята ЦЕЛИКОМ с доски
                    self._judge(w, key, now_ms)
                if w["gone"] > self.vanish_ticks_max:
                    self._walls.pop(key, None)

    def _track(self, w, key, pr, q, now_s):
        """Стена на доске: обновляем объём/пик; дрейф на соседний уровень
        (в допуске) считаем той же стеной."""
        old = w["level"]
        if abs(pr - old) <= max(old * self.drift, 1e-12):
            w["size"] = q
            w["peak"] = max(w["peak"], q)
            w["gone"] = 0
            w["last_ts"] = now_s
            return w
        # уровень исчез, но рядом похожий объём — стена дрейфанула
        w2 = dict(w)
        w2["level"] = pr
        w2["size"] = q
        w2["peak"] = max(w["peak"], q)
        w2["last_ts"] = now_s
        self._walls.pop(key, None)
        return w2

    # ---------- вердикт ----------

    def _prints_at(self, side, level, now_ms):
        """Принты, СЪЕДАЮЩИЕ стену, за последние vanish_sec: ask-стену
        едят принты НА УРОВНЕ И ВЫШЕ (покупатели дошли), bid-стену — на
        уровне и НИЖЕ. Принт на подходе (не дошёл до уровня) — не еда,
        иначе любой подход ловился бы как «съедена»."""
        edge = now_ms - self.vanish_sec * 1000
        eps = level * 0.0002              # допуск одного шага цены
        for ts, pr in self._prints:
            if ts < edge:
                continue
            if side == "ask" and pr >= level - eps:
                return True
            if side == "bid" and pr <= level + eps:
                return True
        return False

    def approached(self, side, level, price):
        """Подход цены к уровню ближе approach_pct."""
        if price <= 0:
            return False
        gap = abs(price - level) / price
        if side == "ask":
            return price <= level and gap <= self.approach_pct
        return price >= level and gap <= self.approach_pct

    def _judge(self, w, key, now_ms):
        """Стена исчезла из топ-20: спуфинг или честный уход?"""
        gone_pct = (w["peak"] - w["size"]) / w["peak"] if w["peak"] > 0 else 0.0
        age = now_ms / 1000.0 - w["first_ts"]
        if w["gone"] > self.vanish_ticks_max or age > self.vanish_sec:
            return
        if not w["approached"]:
            return                    # цена не подходила — не приманка
        if gone_pct < self.vanish_pct:
            return                    # стена почти цела — не исчезала
        if self._prints_at(w["side"], w["level"], now_ms):
            return                    # съедена сделками — честная стена
        repeats = self._seen.get(key, 0) + 1
        self._seen[key] = repeats
        confidence = round(min(1.0, 0.5 * gone_pct + 0.2 * repeats), 3)
        self.publish(DRAFT, {
            "kind": "spoof-alert",
            "author": "spoof",
            "confidence": confidence,
            "fields": {
                "side": w["side"],
                "level": round(w["level"], 8),
                "peak_qty": round(w["peak"], 2),
                "gone_pct": round(gone_pct, 3),
                "wall_age_sec": round(age, 1),
                "repeats": repeats,
            },
            "lineage": [f"book:{w['side']}:{round(w['level'], 8)}"],
        })
        self.issues += 1
