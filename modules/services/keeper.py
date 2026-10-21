"""Кладовщик — хранение данных NeirOS-M (SQLite, WAL).

Зона ответственности (утверждена): только хранить то, что пришло,
и отдавать историю по запросу. Ничего не вычисляет.

- запись через очередь одним писателем, в отдельном потоке —
  основной поток не блокируется;
- свечи и карточки приходят с шины (ярлыки CANDLES, CARD);
- место: сколько влезло в лимит — столько храним; при переполнении
  самые старые записи убираются плавно; уплотнение (VACUUM) — только
  в потоке писателя, редкими порциями (без гонок — урок аудита SWARM);
- счётчик номеров карточек Регистратор берёт отсюда (MAX(seq)),
  поэтому нумерация продолжается после перезапуска.
"""
import asyncio
import sqlite3
import time

import orjson

from core.module import Manifest, NeirModule
from modules._labels import CARD, CANDLES, CANDLES_WARMUP

_PRUNE_CHUNK = 2000   # за один проход убираем не больше — плавно
# КВОТА ГЛУБИНЫ СВЕЧЕЙ (27.10, правка чистки). Правило прежнее было
# «DELETE FROM candles ORDER BY ts_ms ASC LIMIT 2000» — без пола и без
# оглядки на таймфрейм. Но по факту карточек в базе в 20 раз больше, чем
# свечей, а восстановить бар 512м быстрее, чем за 8.5 часа, нельзя.
# Старое правило каждую минуту съедало 28% таблицы свечей, уничтожая
# ровно ту вершину пирамиды, ради которой всё и затевалось.
# Теперь свечи режутся ТОЛЬКО сверх квоты каждого таймфрейма, а квота
# считается в одинаковых ЧАСАХ истории с полом на мелких барах.
CANDLE_FLOOR_BARS = 60   # столько нужно Херсту (R/S) и Регрессии (окно 60)
CANDLE_QUOTA_HOURS = 68  # одинаковая глубина в часах для мелких таймфреймов


class Keeper(NeirModule):
    manifest = Manifest(name="keeper", title="Кладовщик (база данных)",
                        provides=("storage",), priority=5,
                        init_timeout=20.0, stop_timeout=15.0)

    # ---------- запуск / остановка ----------

    async def on_start(self):
        self.db_path = self.kernel.cfg.db_path
        self.db_max_bytes = self.kernel.cfg.db_max_mb * 1024 * 1024
        self.conn = await asyncio.to_thread(self._connect)
        self._read_lock = asyncio.Lock()   # чтения сериализованы: одно соединение
        self._queue = asyncio.Queue(maxsize=10000)
        self._dropped = 0
        self.candles_written = 0
        self.cards_written = 0
        self.cards_skipped = 0
        # ВИДЫ КАРТОЧЕК ВНЕ ЖУРНАЛА (хозяин, 19.10): рутинные снимки среды
        # архив не растят; включить запись вида — убрать его из списка.
        self._skip_kinds = {k.strip() for k in self.kernel.cfg.get_str(
            "storage", "skip_card_kinds", "").split(",") if k.strip()}
        self.prune_runs = 0
        self._last_vacuum = 0.0
        self._page_size = 4096          # размер страницы SQLite для расчёта
        self.last_pruned = 0            # сколько строк убрала последняя чистка
        # Тик-архив УДАЛЁН (27.10). Потребителей не было: контрфакт
        # Лаборатории и Песочница живут на минутных свечах, а запись сырых
        # сделок стоила ~150–190 МБ/сутки при лимите базы 200 МБ.
        # Замена, когда дойдёт очередь до подминутной точности, —
        # секундные бары OHLCV (~2–4 МБ/сутки, в ~60 раз компактнее).
        self._writer = self.spawn(self._writer_loop())
        self.subscribe(CANDLES, self._on_candles)
        # Подписка на ВСЮ поверхность, а не только на пирамиду: биржевой
        # спектр (1m…3d) добавился в 27.10, и его кольца тоже должны
        # попадать в базу — иначе прогрев следующего запуска будет пустым.
        all_tf = set(self.kernel.cfg.senior_tf_sec) | set(
            self.kernel.cfg.archive_tf_sec)
        try:
            from modules.market.collector import EXCHANGE_SPECTRUM
            all_tf |= {tf for tf in EXCHANGE_SPECTRUM
                       if tf > self.kernel.cfg.base_tf_sec}
        except ImportError:
            pass
        for tf in (self.kernel.cfg.base_tf_sec, *sorted(all_tf)):
            self.subscribe(f"{CANDLES_WARMUP}:{tf}", self._on_candles)
        self.subscribe(CARD, self._on_card)
        self.every(60, self._schedule_prune)
        self.log.info("База: %s (лимит %.0f МБ), виды вне журнала: %s",
                      self.db_path, self.db_max_bytes / 1048576,
                      ", ".join(sorted(self._skip_kinds)) or "нет")

    async def on_stop(self):
        # Дописать очередь, закрыть базу
        await self._queue.put(("stop", None))
        try:
            await asyncio.wait_for(self._writer,
                                   timeout=self.kernel.cfg.stop_timeout)
        except asyncio.TimeoutError:
            self._writer.cancel()
        await asyncio.to_thread(self._close)
        self.log.info("Итог: свечей %d, карточек %d (пропущено по виду %d), "
                      "зачисток %d, отброшено %d",
                      self.candles_written, self.cards_written,
                      self.cards_skipped, self.prune_runs, self._dropped)

    # ---------- приём с шины ----------

    async def _on_candles(self, env):
        data = env.payload
        # НЕЗАКРЫТУЮ свечу в базу не пишем (27.10). Это не «лишняя
        # осторожность», а дыра, которая переживает перезапуск: частичная
        # минутка, попав в базу, при следующем прогреве вернётся в полотно
        # и заблокирует настоящую закрытую свечу с той же меткой времени
        # (дедуп `ts <= last_ts`). Защита стояла у потребителей (fractal,
        # sr, zones_plus), но не у того, кто ПИШЕТ в базу.
        # Текущая свеча нужна только экрану — он берёт её напрямую с шины.
        if not data.get("closed"):
            return
        try:
            symbol = str(data["symbol"]).upper()
            tf_sec = int(data["tf_sec"])
            rows = data["candles"]
            pack = []
            for r in rows:
                pack.append((symbol, tf_sec, int(r[0]),
                             float(r[1]), float(r[2]), float(r[3]),
                             float(r[4]), float(r[5]),
                             float(r[6]) if len(r) > 6 else 0.0))
        except (KeyError, TypeError, ValueError, IndexError):
            self.log.warning("Свечи в неверном формате — пропущены")
            return
        self._enqueue(("candles", pack))

    async def _on_card(self, env):
        card = env.payload
        if not isinstance(card, dict) or "seq" not in card:
            self.log.warning("Карточка без номера — пропущена")
            return
        if card.get("kind") in self._skip_kinds:
            self.cards_skipped += 1
            return
        self._enqueue(("card", card))

    def _enqueue(self, item):
        try:
            self._queue.put_nowait(item)
        except asyncio.QueueFull:
            self._dropped += 1
            if self._dropped % 100 == 1:
                self.log.warning("Очередь записи переполнена, отброшено записей: %d "
                                 "(свежее важнее)", self._dropped)

    # ---------- писатель (один, в отдельном потоке) ----------

    async def _writer_loop(self):
        while True:
            kind, data = await self._queue.get()
            if kind == "stop":
                break
            try:
                if kind == "candles":
                    await asyncio.to_thread(self._write_candles, data)
                elif kind == "card":
                    await asyncio.to_thread(self._write_card, data)
                elif kind == "prune":
                    await asyncio.to_thread(self._prune)
            except Exception:
                if kind == "card":
                    self.log.exception("Ошибка записи карточки: %s / %s",
                                       data.get("author"), data.get("kind"))
                else:
                    self.log.exception("Ошибка записи в базу (%s)", kind)

    def _connect(self):
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS candles (
                symbol TEXT NOT NULL, tf_sec INTEGER NOT NULL, ts_ms INTEGER NOT NULL,
                open REAL, high REAL, low REAL, close REAL, volume REAL,
                delta REAL DEFAULT 0,
                PRIMARY KEY (symbol, tf_sec, ts_ms)
            )""")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS cards (
                seq INTEGER PRIMARY KEY, kind TEXT, author TEXT,
                created_ms INTEGER, confidence REAL, trust REAL, weight REAL,
                payload TEXT, lineage TEXT
            )""")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS snapshots (
                seq INTEGER PRIMARY KEY, deal_seq INTEGER, created_ms INTEGER,
                payload TEXT
            )""")
        cur.execute("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT)")
        # Тик-архив удалён (27.10): таблица raw_trades в старых базах
        # остаётся пустой и больше не используется. Убираем её, чтобы
        # место в базе освобождалось сразу, а не копится мусор.
        try:
            cur.execute("DROP TABLE IF EXISTS raw_trades")
            cur.execute("DROP INDEX IF EXISTS idx_trades_ts")
        except sqlite3.Error as exc:
            self.log.warning("Старая таблица raw_trades не убрана: %s", exc)
        conn.commit()
        cur.close()
        return conn

    def _write_candles(self, rows):
        cur = self.conn.cursor()
        cur.executemany("INSERT OR REPLACE INTO candles VALUES (?,?,?,?,?,?,?,?,?)",
                        rows)
        self.conn.commit()
        cur.close()
        self.candles_written += len(rows)

    def _write_card(self, card):
        payload_json = orjson.dumps(card.get("fields") or {}).decode("utf-8")
        lineage_json = orjson.dumps(card.get("lineage") or []).decode("utf-8")
        cur = self.conn.cursor()
        cur.execute(
            "INSERT OR REPLACE INTO cards "
            "(seq, kind, author, created_ms, confidence, trust, weight, payload, lineage) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (int(card["seq"]), str(card.get("kind", "")), str(card.get("author", "")),
             int(card.get("created_ms", 0)), float(card.get("confidence", 0)),
             float(card.get("trust", 0)), float(card.get("weight", 0)),
             payload_json, lineage_json))
        self.conn.commit()
        cur.close()
        self.cards_written += 1

    # ---------- место в базе: плавная зачистка ----------

    async def _schedule_prune(self):
        self._enqueue(("prune", None))

    def _prune(self):
        cur = self.conn.cursor()
        try:
            size = self.db_path.stat().st_size
        except FileNotFoundError:
            self.conn.commit()
            cur.close()
            return
        self.prune_runs += 1
        # РЕАЛЬНО ЗАНЯТОЕ МЕСТО (27.10), а не размер файла. SQLite не
        # сжимает файл при DELETE: st_size остаётся прежним, и прежняя
        # проверка `size > limit` срабатывала ВЕЧНО — чистка удаляла по
        # 6000 строк каждую минуту, не выходя из вечного цикла.
        # page_count − freelist_count = реально занятые страницы.
        try:
            pages = self.conn.execute("PRAGMA page_count").fetchone()[0]
            free = self.conn.execute("PRAGMA freelist_count").fetchone()[0]
            used_bytes = max(0, pages - free) * self._page_size
        except Exception as exc:
            self.log.error("Хранилище: не смог измерить занятое место: %s: %s",
                           type(exc).__name__, exc, exc_info=True)
            self.conn.commit()
            cur.close()
            return
        file_bytes = self.db_path.stat().st_size
        if used_bytes <= self.db_max_bytes:
            # занятого места хватает — уплотняем файл, если он раздут
            if (file_bytes > self.db_max_bytes
                    and time.monotonic() - self._last_vacuum > 3600):
                self.log.info("Уплотняю базу (VACUUM): файл %.1f МБ, "
                              "занято %.1f МБ ...",
                              file_bytes / 1048576, used_bytes / 1048576)
                cur.execute("VACUUM;")
                cur.close()
                self._last_vacuum = time.monotonic()
                self.log.info("Уплотнение завершено: %.1f МБ",
                              self.db_path.stat().st_size / 1048576)
                return
            self.conn.commit()
            cur.close()
            return

        self.log.warning("База занята %.1f МБ из %.1f МБ — убираю: "
                         "карточки → свечи (сверх квоты ТФ)",
                         used_bytes / 1048576, self.db_max_bytes / 1048576)
        gone = 0
        # ПОРЯДОК: сначала старые карточки (они ничего не решают), и только
        # потом свечи — сверх квоты своего таймфрейма. Тик-архива, который
        # прежде шёл первым, больше нет (27.10).
        gone += cur.execute(
            "DELETE FROM cards WHERE seq IN "
            "(SELECT seq FROM cards ORDER BY seq ASC LIMIT ?)",
            (_PRUNE_CHUNK,)).rowcount
        gone += self._prune_candles(cur)
        self.conn.commit()
        cur.close()
        self.last_pruned = gone
        self.log.info("База почищена: удалено строк %d", gone)
        # Файл после DELETE не сжимается — уплотняем почасовой
        if (self.db_path.stat().st_size > self.db_max_bytes
                and time.monotonic() - self._last_vacuum > 3600):
            self.log.info("Уплотняю базу (VACUUM)...")
            self.conn.execute("VACUUM;")
            self._last_vacuum = time.monotonic()
            self.log.info("Уплотнение завершено: %.1f МБ",
                          self.db_path.stat().st_size / 1048576)

    def _prune_candles(self, cur):
        """Свечи режутся ТОЛЬКО сверх квоты своего таймфрейма.

        Квота = одинаковая глубина в часах, но не меньше
        CANDLE_FLOOR_BARS баров: ровно столько нужно Херсту (R/S по 60
        точкам) и Регрессии (окно 60 свечей). Для мелких ТФ квоты хватает
        с запасом, а для крупных (7680/15360/30720) срабатывает пол — и
        именно они раньше исчезали первыми."""
        gone = 0
        tfs = [r[0] for r in cur.execute(
            "SELECT tf_sec FROM candles GROUP BY tf_sec")]
        for tf in tfs:
            try:
                quota = max(CANDLE_FLOOR_BARS,
                             int(CANDLE_QUOTA_HOURS * 3600 / int(tf)))
            except (TypeError, ValueError, ZeroDivisionError) as exc:
                self.log.error("Хранилище: квота для ТФ %s не вычислена: "
                               "%s: %s", tf, type(exc).__name__, exc,
                               exc_info=True)
                continue
            n = cur.execute("SELECT COUNT(*) FROM candles WHERE tf_sec=?",
                            (tf,)).fetchone()[0]
            if n <= quota:
                continue
            cur.execute(
                "DELETE FROM candles WHERE rowid IN "
                "(SELECT rowid FROM candles WHERE tf_sec=? "
                " ORDER BY ts_ms ASC LIMIT ?)",
                (tf, min(n - quota, _PRUNE_CHUNK)))
            gone += cur.rowcount
        return gone

    def _close(self):
        try:
            self.conn.commit()
            self.conn.close()
        except Exception:
            pass

    # ---------- чтение (история по запросу) ----------

    async def max_card_seq(self) -> int:
        async with self._read_lock:
            return await asyncio.to_thread(self._read_max_seq)

    def _read_max_seq(self) -> int:
        cur = self.conn.execute("SELECT COALESCE(MAX(seq), 0) FROM cards")
        value = cur.fetchone()[0]
        cur.close()
        return int(value or 0)

    async def card_count(self) -> int:
        async with self._read_lock:
            return await asyncio.to_thread(self._read_card_count)

    def _read_card_count(self) -> int:
        cur = self.conn.execute("SELECT COUNT(*) FROM cards")
        value = cur.fetchone()[0]
        cur.close()
        return int(value)

    async def get_last_cards(self, limit: int, kind: str = None):
        async with self._read_lock:
            return await asyncio.to_thread(self._read_last_cards, limit, kind)

    async def get_card(self, seq: int):
        """Карточка по номеру (поиск по родословной — точно, а не окном:
        окно последних карточек старые связи не находит)."""
        async with self._read_lock:
            return await asyncio.to_thread(self._read_card, seq)

    def _read_card(self, seq):
        cur = self.conn.execute(
            "SELECT seq, kind, author, created_ms, confidence, trust, "
            "weight, payload, lineage FROM cards WHERE seq = ?", (int(seq),))
        row = cur.fetchone()
        cur.close()
        if not row:
            return None
        (seq_, kind, author, created, conf, trust, weight, payload,
         lineage) = row
        return {
            "seq": seq_, "kind": kind, "author": author,
            "created_ms": created, "confidence": conf, "trust": trust,
            "weight": weight,
            "fields": orjson.loads(payload) if payload else {},
            "lineage": orjson.loads(lineage) if lineage else [],
        }

    def _read_last_cards(self, limit, kind=None):
        if kind:
            cur = self.conn.execute(
                "SELECT seq, kind, author, created_ms, confidence, trust, "
                "weight, payload, lineage FROM cards WHERE kind = ? "
                "ORDER BY seq DESC LIMIT ?", (str(kind), int(limit)))
        else:
            cur = self.conn.execute(
                "SELECT seq, kind, author, created_ms, confidence, trust, "
                "weight, payload, lineage FROM cards ORDER BY seq DESC "
                "LIMIT ?", (int(limit),))
        rows = cur.fetchall()
        cur.close()
        result = []
        for seq, kind, author, created, conf, trust, weight, payload, lineage in rows:
            result.append({
                "seq": seq, "kind": kind, "author": author, "created_ms": created,
                "confidence": conf, "trust": trust, "weight": weight,
                "fields": orjson.loads(payload) if payload else {},
                "lineage": orjson.loads(lineage) if lineage else [],
            })
        return result

    async def get_last_candles(self, symbol: str, tf_sec: int, limit: int):
        """История свечей от старых к новым (для прогрева Сборщика)."""
        async with self._read_lock:
            return await asyncio.to_thread(self._read_last_candles,
                                           symbol.upper(), tf_sec, limit)

    def _read_last_candles(self, symbol, tf_sec, limit):
        cur = self.conn.execute(
            "SELECT ts_ms, open, high, low, close, volume, delta FROM candles "
            "WHERE symbol = ? AND tf_sec = ? ORDER BY ts_ms DESC LIMIT ?",
            (symbol, int(tf_sec), int(limit)))
        rows = cur.fetchall()
        cur.close()
        return rows[::-1]
