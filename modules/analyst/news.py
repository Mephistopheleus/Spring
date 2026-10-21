"""Новостной агрегатор 2.0 (Этап 31) — «событие → сила → направление».

Источники:
- NewsAPI (ключ в config/keys.ini, бесплатный тариф) — задержка до суток;
- RSS-ленты БЕЗ ключа (config: [news] rss_urls; CoinDesk/Cointelegraph) —
  свежесть минутная, второй независимый голос. Дедуп по ссылке общий.

Слой 2.0: заголовок классифицируется — тип события (регуляторное/
макро/хак/листинг/прочее), сила (0..1) и ожидаемое направление реакции
рынка. Это РАСПИСКА: Лаборатория по окну реакции сверит прогноз
с фактическим ходом цены (карточка news-reaction), Тюнер по фактам
двигает доверие news-aggregator (facts_scores).

Тон: VADER + списки усиливающих слов; VADER не обязателен —
типизация работает и без него. Событие с типом даёт карточку даже
при нейтральном тоне (направление обещано типом), «прочее» — только
при тоне выше порога (config: [news] min_tone).
"""
import collections
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit

import aiohttp

from core.module import Manifest, NeirModule
from modules._labels import DRAFT

API_URL = "https://newsapi.org/v2/everything"
NEWSAPI_POLL_SEC = 900   # 96 запросов/сутки — в лимитах бесплатного тарифа
SEEN_MAX = 500
PAGE_SIZE = 20

BULL_WORDS = ("surge", "rally", "bullish", "gain", "jump", "soar", "record",
              "breakout", "adoption", "upgrade", "inflow", "approve")
BEAR_WORDS = ("crash", "plunge", "bearish", "loss", "drop", "hack", "lawsuit",
              "ban", "sell-off", "downgrade", "outflow", "fraud")

# КЛАССИФИКАТОР (Этап 31): тип события → сила → ожидаемая реакция.
# Направление — прогноз с распиской (проверит Лаборатория), не угадывание:
# хак/бан — толпа бежит, листинг/одобрение — топливо, макро — по жёсткости.
HACK_WORDS = ("hack", "exploit", "stolen", "breach", "drained",
              "phishing", "rug pull")
LIST_DOWN_WORDS = ("delist", "dumps", "removes support")
LIST_UP_WORDS = ("listing", "lists", "adds support", "launches trading")
REG_WORDS = ("sec", "cftc", "regulator", "regulation", "regulatory",
             "lawsuit", "ban", "subpoena", "etf", "court", "legal")
REG_DOWN_WORDS = ("ban", "sues", "sued", "fines", "charges", "subpoena",
                  "probe", "crackdown", "sanction", "rejects", "delays")
REG_UP_WORDS = ("approves", "approval", "legalizes", "license",
                "green light", "clears")
MACRO_WORDS = ("cpi", "inflation", "fed", "fomc", "powell", "interest rate",
               "rate cut", "rate hike", "gdp", "jobs", "unemployment",
               "pce", "recession", "dollar", "treasury")
HAWKISH_WORDS = ("hawkish", "rate hike", "higher for longer",
                 "inflation rises", "hot inflation", "holds rates")
DOVISH_WORDS = ("dovish", "rate cut", "cuts rates", "inflation falls",
                "inflation cools", "easing")
SCALE_TERMS = ("billion", "million", "record", "emergency", "historic",
               "massive", "surge", "crash", "plunge")
BASE_STRENGTH = {"hack": 0.8, "regulatory": 0.6, "macro": 0.7, "listing": 0.5}


class NewsAggregator(NeirModule):
    manifest = Manifest(name="news-aggregator", title="Новостной агрегатор",
                        priority=50, tick_timeout=30.0)

    async def on_start(self):
        self._key = self.kernel.cfg.news_key("newsapi_key")
        self._rss_urls = [u.strip() for u in
                          self.kernel.cfg.news_rss_urls.split(",") if u.strip()]
        self._min_tone = self.kernel.cfg.news_min_tone
        if not self._key and not self._rss_urls:
            self.log.warning("Нет ни ключа NewsAPI, ни RSS-лент — модуль спит")
            return
        try:
            from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
            self._vader = SentimentIntensityAnalyzer()
        except ImportError:
            self._vader = None
            self.log.warning("vaderSentiment не установлен — тон по спискам слов")
        self._seen = collections.OrderedDict()   # ссылка -> True (дедуп общий)
        self.issued = 0
        self.polls = 0
        if self._key:
            self.every(NEWSAPI_POLL_SEC, self._poll)
        if self._rss_urls:
            self.every(self.kernel.cfg.news_rss_poll_sec, self._poll_rss)
        self.log.info("Источники: %s",
                      ", ".join((["NewsAPI"] if self._key else [])
                                + [self._feed_name(u) for u in self._rss_urls]))

    # ---------- NewsAPI (первый голос, ключ) ----------

    async def _poll(self):
        self.polls += 1
        params = {"q": "bitcoin OR crypto OR cryptocurrency", "language": "en",
                  "sortBy": "publishedAt", "pageSize": PAGE_SIZE,
                  "apiKey": self._key}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(API_URL, params=params,
                                       timeout=aiohttp.ClientTimeout(total=20)) as resp:
                    if resp.status != 200:
                        self.log.warning("NewsAPI: статус %s (лимит тарифа?) — "
                                         "попробую в следующий раз", resp.status)
                        return
                    payload = await resp.json(content_type=None)
        except Exception:
            self.log.exception("NewsAPI: опрос не удался")
            return

        fresh = 0
        for article in payload.get("articles", []):
            if await self._emit_article(
                    article.get("title") or "",
                    article.get("description") or "",
                    (article.get("source") or {}).get("name", ""),
                    article.get("url") or "",
                    article.get("publishedAt", "")):
                fresh += 1
        if fresh:
            self.log.info("NewsAPI: новых новостных карточек: %d (всего %d)",
                          fresh, self.issued)

    # ---------- RSS (второй голос, без ключа — Этап 31) ----------

    async def _poll_rss(self):
        """RSS-ленты: свежесть минутная, ключа не требуют."""
        for url in self._rss_urls:
            feed = self._feed_name(url)
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(
                            url, timeout=aiohttp.ClientTimeout(total=20),
                            headers={"User-Agent": "NeirOS-M/1.0"}) as resp:
                        if resp.status != 200:
                            self.log.warning("RSS %s: статус %s — попробую позже",
                                             feed, resp.status)
                            continue
                        raw = await resp.read()
            except Exception:
                self.log.exception("RSS %s: опрос не удался", feed)
                continue
            try:
                items = self._parse_rss(raw)
            except ET.ParseError:
                self.log.warning("RSS %s: лента не разобрана (не XML?)", feed)
                continue
            fresh = 0
            for it in items:
                if await self._emit_article(it["title"], "", feed, it["link"],
                                            self._parse_date(it["published"])):
                    fresh += 1
            if fresh:
                self.log.info("RSS %s: новых карточек %d (всего %d)",
                              feed, fresh, self.issued)

    @staticmethod
    def _feed_name(url):
        host = urlsplit(url).netloc.split(".")
        name = next((p for p in host if p.lower() not in
                     ("www", "rss", "feeds", "arc")), "")
        return name.capitalize() if name else url

    @staticmethod
    def _parse_rss(xml_bytes):
        """XML ленты (RSS 2.0 / Atom) → [{title, link, published}].
        Только stdlib; пространства имён срезаются по локальному имени."""
        root = ET.fromstring(xml_bytes)
        items = []
        for el in root.iter():
            if el.tag.rsplit("}", 1)[-1].lower() not in ("item", "entry"):
                continue
            title = link = published = ""
            for ch in el.iter():
                tag = ch.tag.rsplit("}", 1)[-1].lower()
                if tag == "title" and not title and ch.text:
                    title = ch.text.strip()
                elif tag == "link":
                    href = ch.get("href") or (ch.text or "").strip()
                    if href and not link:
                        link = href
                elif (tag in ("pubdate", "published", "updated", "date")
                        and not published and ch.text):
                    published = ch.text.strip()
            if title or link:
                items.append({"title": title, "link": link,
                              "published": published})
        return items

    @staticmethod
    def _parse_date(raw):
        """RFC-822/ISO дату ленты → компактный ISO; не разобралась — как есть."""
        if not raw:
            return ""
        try:
            return parsedate_to_datetime(raw).strftime("%Y-%m-%dT%H:%M:%SZ")
        except (TypeError, ValueError):
            return raw

    # ---------- классификатор «событие → сила → направление» ----------

    @staticmethod
    def _classify_event(text):
        """(тип, сила 0..1, ожидаемое направление | None). None = направление
        не обещано (ответит тон). Порядок проверок важен: хак громче листинга,
        «delisting» содержит «listing» — медвежий листинг проверяем первым."""
        def has(words):
            return any(w in text for w in words)

        if has(HACK_WORDS):
            etype, expected = "hack", "down"
        elif has(LIST_DOWN_WORDS):
            etype, expected = "listing", "down"
        elif has(LIST_UP_WORDS):
            etype, expected = "listing", "up"
        elif has(REG_WORDS):
            etype, expected = "regulatory", None
            if has(REG_DOWN_WORDS):
                expected = "down"
            elif has(REG_UP_WORDS):
                expected = "up"
        elif has(MACRO_WORDS):
            etype, expected = "macro", None
            if has(HAWKISH_WORDS):
                expected = "down"        # жёсткий доллар — крипте больно
            elif has(DOVISH_WORDS):
                expected = "up"
        else:
            return "other", 0.0, None
        scale = 1.25 if has(SCALE_TERMS) else 1.0
        return etype, min(1.0, BASE_STRENGTH[etype] * scale), expected

    # ---------- общий конвейер статьи → карточка ----------

    async def _emit_article(self, title, desc, source, url, published):
        """Дедуп по ссылке (общий для источников), тон + типизация."""
        link = url or title
        if not link or link in self._seen:
            return False
        self._seen[link] = True
        while len(self._seen) > SEEN_MAX:
            self._seen.popitem(last=False)
        title = (title or "").strip()
        desc = (desc or "").strip()
        if not title and not desc:
            return False
        score = 0.0
        if self._vader is not None:
            score = self._vader.polarity_scores(f"{title}. {desc}")["compound"]
        text = f"{title} {desc}".lower()
        if any(w in text for w in BULL_WORDS):
            score = min(1.0, score + 0.2)
        if any(w in text for w in BEAR_WORDS):
            score = max(-1.0, score - 0.2)
        event_type, strength, expected = self._classify_event(text)
        direction = "up" if score > 0.15 else ("down" if score < -0.15 else "flat")
        confidence = min(1.0, abs(score))
        if event_type != "other":
            # типизированное событие звучит, даже если тон нейтральный
            confidence = min(1.0, max(confidence, strength * 0.6))
        if confidence < self._min_tone:
            return False
        self.publish(DRAFT, {
            "kind": "news-sentiment",
            "author": "news-aggregator",
            "confidence": round(confidence, 3),
            "fields": {
                "headline": title or desc,
                "source": source,
                "url": link,
                "sentiment": round(score, 3),
                "direction": direction,
                # НОВОСТИ 2.0 (Этап 31): расписка «тип → сила → направление»
                "event_type": event_type,
                "strength": round(strength, 3),
                "expected_direction": expected or direction,
                "published_at": published,
            },
            "lineage": [f"news:{abs(hash(link)) % 10_000_000_000}"],
        })
        self.issued += 1
        return True

    async def on_stop(self):
        self.log.info("Итог: опросов %d, выдано карточек %d", self.polls,
                      self.issued)
