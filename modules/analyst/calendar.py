"""TTE-Календарь (Волна 3) — измеритель времени до макро-событий.

События НЕ угадываем: направление не предсказываем — только время и
громкость. Как потребители применяют факт — их решение (Сценарист сжимает
уверенность/TTL, Поле гасит полки; ручки у каждого свои).

Пульс kind="tte-state" раз в минуту (пустой список = модуль жив).
USD-only (крипта реагирует на американскую макростатистику).

Источники (двухконтурно, согласовано):
1. Finnhub calendar/economic — если тариф даёт (сейчас 403);
2. FRED releases/dates (бесплатный) — даты релизов; время события =
   дата + default_event_time_utc (большие американские релизы — 12:30 UTC).
Громкость: high-релизы (CPI, NFP, GDP, PCE...) — 1.0, прочие — low_weight.

Опрос источника раз в poll_min минут; minutes_to_event считается от
расписания в памяти, не от момента опроса.
"""
import calendar as _calendar   # timegm: UTC, а не mktime (местный)
import time

import aiohttp

from core.module import Manifest, NeirModule
from modules._labels import DRAFT


class TteCalendar(NeirModule):
    manifest = Manifest(name="calendar", title="TTE-Календарь",
                        priority=45, tick_timeout=15.0)

    async def on_start(self):
        c = self.kernel.cfg
        self.horizon_min = c.tte_horizon_min
        self.poll_min = c.tte_poll_min
        self.default_time = c.tte_default_event_time_utc
        self.high_keywords = [s.strip().lower() for s in
                              c.tte_high_keywords.split(",") if s.strip()]
        self.low_weight = c.tte_low_weight
        self.finnhub_key = c.news_key("finnhub_key")
        self.fred_key = c.news_key("fred_api_key")
        self._events = []         # [{"name","ts","weight"}]
        self._source = "нет"
        self._release_names = {}  # FRED: release_id -> name (кэш на сутки)
        self._names_ts = 0.0
        self.every(60.0, self._pulse)
        self.every(self.poll_min * 60.0, self._poll)
        self.log.info("TTE-календарь включён: горизонт %.0f мин, опрос "
                      "раз в %.0f мин, источники: Finnhub%s, FRED%s",
                      self.horizon_min, self.poll_min,
                      "+ключ" if self.finnhub_key else "—нет ключа",
                      "+ключ" if self.fred_key else "—нет ключа")
        await self._poll()

    # ---------- опрос источников ----------

    async def _poll(self):
        events = None
        if self.finnhub_key:
            events = await self._fetch_finnhub()
            if events is not None:
                self._source = "finnhub"
        if events is None and self.fred_key:
            events = await self._fetch_fred()
            if events is not None:
                self._source = "fred"
        if events is None:
            self.log.warning("Календарь недоступен (Finnhub и FRED) — "
                             "повторю в следующий цикл опроса")
            return
        self._events = events
        self.log.info("Расписание обновлено (%s): будущих событий %d",
                      self._source, len(events))

    async def _fetch_finnhub(self):
        """Finnhub economic calendar (платный тариф) или None."""
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(
                        "https://finnhub.io/api/v1/calendar/economic",
                        params={"token": self.finnhub_key},
                        timeout=aiohttp.ClientTimeout(total=15)) as r:
                    if r.status != 200:
                        return None
                    data = await r.json(content_type=None)
            out = []
            for item in data.get("economicCalendar") or []:
                if str(item.get("country", "")).upper() != "US":
                    continue                      # USD-only
                ts = self._parse_time(item.get("time"))
                if ts is None:
                    continue
                impact = str(item.get("impact", "")).lower()
                weight = (1.0 if impact == "high"
                          else 0.5 if impact == "medium" else self.low_weight)
                out.append({"name": str(item.get("event", "?"))[:60],
                            "ts": ts, "weight": weight})
            return out
        except Exception:
            return None

    async def _fetch_fred(self):
        """FRED releases/dates (бесплатный) или None."""
        try:
            async with aiohttp.ClientSession() as s:
                if time.time() - self._names_ts > 86400:
                    async with s.get(
                            "https://api.stlouisfed.org/fred/releases",
                            params={"api_key": self.fred_key,
                                    "file_type": "json"},
                            timeout=aiohttp.ClientTimeout(total=15)) as r:
                        if r.status != 200:
                            return None
                        data = await r.json(content_type=None)
                    self._release_names = {
                        str(rel.get("id")): str(rel.get("name", "?"))
                        for rel in data.get("releases") or []}
                    self._names_ts = time.time()
                async with s.get(
                        "https://api.stlouisfed.org/fred/releases/dates",
                        params={"api_key": self.fred_key,
                                "file_type": "json",
                                "sort_order": "desc", "limit": 300},
                        timeout=aiohttp.ClientTimeout(total=15)) as r:
                    if r.status != 200:
                        return None
                    data = await r.json(content_type=None)
        except Exception:
            return None
        hh, mm = (self.default_time.split(":") + ["0", "0"])[:2]
        out = []
        now = time.time()
        for item in data.get("release_dates") or []:
            rid = str(item.get("release_id", ""))
            name = self._release_names.get(rid, f"релиз #{rid}")
            # USD-only (утверждено): из бесплатного FRED берём только
            # громкие американские релизы — прочие (в т.ч. иностранные)
            # дали бы ложное сжатие времени
            if not any(k in name.lower() for k in self.high_keywords):
                continue
            try:
                y, mo, d = str(item.get("date", "")).split("-")
                # UTC-КОНТРАКТ (аудит 27.10): ключ конфига объявлен как
                # default_event_time_utc, значит и разбирать надо в UTC.
                # time.mktime читал 12:30 как МЕСТНОЕ время — на не-UTC
                # машине CPI/NFP/FOMC вставали на часы раньше, и
                # minutes_to_event уезжал в минус (событие молча пропадало).
                ts = _calendar.timegm(time.strptime(
                    f"{y}-{mo}-{d} {hh}:{mm}", "%Y-%m-%d %H:%M"))
            except (ValueError, KeyError):
                continue
            if ts < now:                     # прошедшее не публикуем
                continue
            out.append({"name": name[:60], "ts": ts, "weight": 1.0})
        return out

    def _parse_time(self, value):
        """Finnhub time 'YYYY-MM-DD HH:MM:SS' → epoch UTC, None при сбое.

        Finnhub отдаёт время в UTC — разбираем через timegm, иначе mktime
        съедал часовой пояс хоста (аудит 27.10)."""
        try:
            return _calendar.timegm(time.strptime(
                str(value)[:19], "%Y-%m-%d %H:%M:%S"))
        except (ValueError, TypeError):
            return None

    # ---------- пульс раз в минуту ----------

    async def _pulse(self):
        now = time.time()
        evs = []
        for ev in self._events:
            mins = (ev["ts"] - now) / 60.0
            if 0 <= mins <= self.horizon_min:
                evs.append({"event": ev["name"],
                            "minutes_to_event": round(mins, 1),
                            "weight": ev["weight"]})
        evs.sort(key=lambda e: e["minutes_to_event"])
        self.publish(DRAFT, {
            "kind": "tte-state",
            "author": "calendar",
            "confidence": 0.9,
            "fields": {
                "events": evs,           # пусто = событий в горизонте нет
                "source": self._source,
                "ts_ms": int(now * 1000),
            },
            # НИТЬ (протокол lineage, Узел 5): источник событий календаря
            "lineage": [f"calendar:{self._source}"],
        })
        if evs:
            near = evs[0]
            self.log.info("Пульс TTE: ближайшее «%s» через %.0f мин "
                          "(громкость %.1f)", near["event"],
                          near["minutes_to_event"], near["weight"])