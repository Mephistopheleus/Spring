"""Автотюнер NeirOS-M — распределяет «очки доверия» и калибрует уверенность.

Утверждено: работает вдумчиво, не спеша, копит базу сделок.
- ест снимки закрытых сделок (карточки kind="deal-closed");
- по родословной находит карточки-источники (зоны, новости)
  и их авторов;
- ДОВЕРИЕ (утверждено): каждая закрытая сделка сдвигает доверие
  автора к цели (winrate), но не больше одного шага за пересчёт
  (config: trust_step, база 0.05). Шаг адаптивный: система в плюсе —
  реагирует бодрее (×1.5), в минусе — осторожничает (×0.5);
  вилка доверия 0.1..0.95 (config: trust_min/trust_max) — данные
  никогда не отключаются полностью, проверенный звучит громче;
- публикует справочник доверий (ярлык TRUST) — Регистратор сразу
  применяет его к новым карточкам;
- пишет справочник в config.ini (переживает перезапуск);
- БАЗА (утверждено) — свой файл рядом со Складом: накопления и метки
  учёта живут в нём, никто, кроме Тюнера, файл не читает; каждая
  сделка и каждый снимок считаются РОВНО ОДИН РАЗ (учёт по метке);
- min_deals — только минимальная выборка для КАЛИБРОВКИ и ОТЧЁТА;
- КАЛИБРОВКА УВЕРЕННОСТИ (утверждено): сверяет «обещанное с фактом» —
  если сценарист уверенности ≥0.7 обещал, а выигрывает заметно реже,
  снижает множитель conf_calibration в config.ini (перезапуск учтёт);
- ПОРОГ РЕАЛА (утверждено): двигается по снимкам идеала Лаборатории
  (тени с верной сутью → вниз, промахи → вверх); шаг адаптивный —
  на цыпочках при малой выборке √(n/(n+50)) × единодушие сигнала ×
  самочувствие системы; цель сглаживается — одна серия везения
  ручку не качает;
- ОТЧЁТ К ИДЕАЛУ: winrate, средний PnL и просадка — движение к цели
  (PnL 100%/день, WR 100%, просадка 0%).
"""
import asyncio
import os
import re
import struct
import time

from core.module import Manifest, NeirModule
from modules._knobs import WEIGHT_KNOBS
from modules._labels import CARD, DRAFT, FACT_AUTHORS, TRUST

NEUTRAL = 0.5
CONF_HIGH = 0.7          # (историч.) высокие обещания; калибровка перешла
                         # на относительную верхнюю четверть
CALIB_TOLERANCE = 0.15   # насколько факт может отставать от обещаний
CALIB_FLOOR = 0.5        # ниже множитель не опустится
CALIB_CEIL = 1.5         # выше не поднимется: обещания догоняют факт
FACT_NOISE = 0.05        # данные слабее этого — шум: польза не считается

# РУЧКИ ВЕСОВ — единый реестр в modules/_knobs.py: настройка весов,
# отчёт здоровья и сборка эталона сброса берут ручки оттуда.

# Точечная правка config.ini: меняется только строка ключа,
# комментарии и порядок переживают; запись атомарна (tmp + os.replace)
_INI_SEC = re.compile(r"^\s*\[(?P<name>[^\]]+)\]\s*$")
_INI_KEY = re.compile(r"^\s*(?P<key>[^=\s;#][^=]*?)\s*=")

# ДОВЕРИЕ ПО ФАКТАМ: карта факт→автор — в _labels (единый словарь)
# ФАЙЛ БАЗЫ (утверждено): компактный свой формат — читает и пишет
# только Тюнер. Формат 2: ЧЕТЫРЕ ПОТОКА на каждый вид данных —
# реал/тень (польза факта) и лаб_реал/лаб_тень (идеальные настройки
# из снимков Лаборатории) + маршрут (стоп/активация/шаг) + упущенное
# по причинам Риска. Старый файл читать нечем — база чистая (учёт с
# текущего номера: «копим заново», решение хозяина).
_BASE_MAGIC = b"NEIRBASE2"


def _pack_base(*, data, trail, trust, totals, fstep, trail_at, act_at,
               mark_deal, mark_snap, route, route_at, lost, lev_at):
    """База -> байты: заголовок, накопления, метки."""

    def w_str(buf, text):
        raw = str(text).encode("utf-8")[:65535]
        return buf + struct.pack("<H", len(raw)) + raw

    def w_pairs(buf, source):
        buf += struct.pack("<I", len(source))
        for name, pair in source.items():
            buf = w_str(buf, name)
            buf += struct.pack("<dd", float(pair[0]), float(pair[1]))
        return buf

    def w_streams(buf, source):
        buf += struct.pack("<I", len(source))
        for name, row in source.items():
            buf = w_str(buf, name)
            row = list(row) + [0.0] * (8 - len(row))
            buf += struct.pack("<8d", *[float(x) for x in row[:8]])
        return buf

    def w_floats(buf, source):
        buf += struct.pack("<I", len(source))
        for name, val in source.items():
            buf = w_str(buf, name)
            buf += struct.pack("<d", float(val))
        return buf

    buf = bytearray(_BASE_MAGIC)
    buf += struct.pack("<dqqdd", time.time() * 1000.0,
                       int(mark_deal), int(mark_snap),
                       float(trail_at), float(act_at))
    buf = w_streams(buf, data)
    buf = w_pairs(buf, trail)
    buf = w_floats(buf, trust)
    buf = w_floats(buf, totals)
    buf = w_floats(buf, fstep)
    buf = w_pairs(buf, route)
    buf = w_floats(buf, route_at)
    buf = w_pairs(buf, lost)
    buf = w_floats(buf, lev_at)
    return bytes(buf)


def _unpack_base(blob):
    """Байты -> поля базы; чужой формат или обрезка — ValueError."""

    class _R:
        def __init__(self, raw):
            self.raw, self.at = raw, 0

        def take(self, n):
            if self.at + n > len(self.raw):
                raise ValueError("файл базы обрезан")
            chunk = self.raw[self.at:self.at + n]
            self.at += n
            return chunk

        def u32(self):
            return struct.unpack("<I", self.take(4))[0]

        def f64(self):
            return struct.unpack("<d", self.take(8))[0]

        def q64(self):
            return struct.unpack("<q", self.take(8))[0]

        def text(self):
            return self.take(self.u16()).decode("utf-8")

        def u16(self):
            return struct.unpack("<H", self.take(2))[0]

    r = _R(blob)
    if r.take(len(_BASE_MAGIC)) != _BASE_MAGIC:
        raise ValueError("файл базы чужого формата")
    r.f64()                                 # saved_ms (метка времени)
    mark_deal, mark_snap = r.q64(), r.q64()
    trail_at, act_at = r.f64(), r.f64()

    def r_pairs():
        out = {}
        for _ in range(r.u32()):
            name = r.text()               # имя читается ДО значений
            out[name] = [r.f64(), r.f64()]
        return out

    def r_streams():
        out = {}
        for _ in range(r.u32()):
            name = r.text()
            out[name] = list(struct.unpack("<8d", r.take(64)))
        return out

    def r_floats():
        out = {}
        for _ in range(r.u32()):
            name = r.text()
            out[name] = r.f64()
        return out

    data = r_streams()
    trail = r_pairs()
    trust = r_floats()
    totals = r_floats()
    fstep = r_floats()
    route = r_pairs()
    route_at = r_floats()
    lost = r_pairs()
    lev_at = r_floats()
    return {"data": data, "trail": trail, "trust": trust,
            "totals": totals, "fstep": fstep, "route": route,
            "route_at": route_at, "lost": lost, "lev_at": lev_at,
            "trail_at_step": trail_at, "act_at_step": act_at,
            "mark_deal": mark_deal, "mark_snap": mark_snap}


class AutoTuner(NeirModule):
    manifest = Manifest(name="autotuner", title="Автотюнер",
                        priority=95, tick_timeout=30.0)

    async def on_start(self):
        self.min_deals = self.kernel.cfg.tuner_min_deals
        self.trust_min = self.kernel.cfg.tuner_trust_min
        self.trust_max = self.kernel.cfg.tuner_trust_max
        self.trust_step = self.kernel.cfg.tuner_trust_step
        self.session_start_ms = int(time.time() * 1000)
        self.symbol = self.kernel.cfg.symbol.upper()
        self.tf = self.kernel.cfg.base_tf_sec
        self.open_conf = self.kernel.cfg.risk_open_conf_threshold
        # ПОРОГ РЕАЛА: нижняя граница — из конфига (решение хозяина 29.09:
        # в тень до настройки, oc_min = 0.75 — Тюнер ниже не опустит)
        self.oc_step, self.oc_max = 0.05, 0.95
        self.oc_min = max(0.0, min(self.kernel.cfg.tuner_oc_min,
                                   self.oc_max))
        self._essence_history = []    # суть верна (1/0) по снимкам
        self._oc_smooth = None        # сглаженная цель порога
        self._efficiency = NEUTRAL    # самочувствие системы (для множителя шага)
        self._spoof_stats = {}        # confirmed/refuted для сита спуфинга
        # БАЗА (утверждено): свой файл — никто, кроме Тюнера, его не
        # читает; накопления и метки учёта переживают перезапуск
        self.base_path = self._base_file()
        self._base_ready = False
        self._base_dirty = False
        self._data_base = {}          # имя данных -> [верно, всего]
        self._fact_at_step = {}       # имя данных -> всего на прошлом шаге
        self._base_prev = {}          # тип данных -> полезность до шага
        # БАЗА СОПРОВОЖДЕНИЯ (утверждено): идеал (ориентир из снимков) и
        # факт (шаг/активация сценариев) — Тюнер ведёт трейлинг сам
        self._trail_base = {"ideal": [0.0, 0.0], "step": [0.0, 0.0],
                            "touch": [0.0, 0.0]}
        self._trail_at_step = 0.0     # ideal.n на прошлом шаге шага
        self._act_at_step = 0.0       # touch.n на прошлом шаге активации
        # МАРШРУТ — ИДЕАЛЫ ИЗ СНИМКОВ (утверждено): прогон Лаборатории
        # даёт по каждой сделке свои стоп/активацию/шаг; Тюнер копит их
        # четырьмя потоками и шагает ручками к среднему идеалу
        self._route_base = {"stop": [0.0, 0.0], "act": [0.0, 0.0],
                            "dist": [0.0, 0.0]}
        self._route_at = {"stop": 0.0, "act": 0.0, "dist": 0.0}
        # УПУЩЕННОЕ ПО РИСКУ (перенесено из Лабы, разлепление ролей):
        # тень верна по сути, но Риск её отсёк — копим по причинам
        self._lost_base = {}
        self._lev_at = {"shadows": 0.0, "right": 0.0, "ideal": 0.0}
        # МЕТКИ УЧЁТА (утверждено): каждая сделка и каждый снимок
        # считаются РОВНО ОДИН РАЗ — пересчёты окна базу не раздувают
        self._mark_deal = 0           # номер последней учтённой сделки
        self._mark_snap = 0           # номер последнего учтённого снимка
        self._last_snaps = []         # окно лабораторных снимков (для отчёта)
        self._health_last = {}        # отчёт здоровья механики
        self._health_at = 0           # сделок на прошлом отчёте
        self.spoof_min_samples = 20   # выборка для перекройки сита
        self._last_published = {}     # тип данных -> trust
        self._trust_totals = {}       # тип данных -> фактов при последнем шаге
        # Справочник из config.ini — стартуем с уже заработанных доверий
        self._last_published.update(self.kernel.cfg.trust_directory())
        self.subscribe(CARD, self._on_card)
        self.spawn(self._load_base())
        self.log.info("Тюнер включён: вилка %.2f..%.2f, шаг %.2f "
                      "(калибровка/отчёт — от %d сделок); база: %s",
                      self.trust_min, self.trust_max, self.trust_step,
                      self.min_deals, self.base_path)

    async def on_stop(self):
        self._save_base()
        self.log.info("Итог: база Тюнера — данных %d видов, метки "
                      "сделок/снимков %d/%d",
                      len(self._data_base), self._mark_deal, self._mark_snap)

    # ---------- база: свой файл (никто, кроме Тюнера, его не читает) ----------

    def _base_file(self):
        """Файл базы — рядом со Складом (стенд со своим db_path
        получает свой файл автоматически). Переопределение имени:
        [tuner] base_path в config.ini."""
        raw = self.kernel.cfg.get_str("tuner", "base_path", "").strip()
        if raw:
            return self.kernel.cfg.path(raw)
        db = self.kernel.cfg.path(str(self.kernel.cfg.get_str(
            "storage", "db_path", "data/neiros.db")))
        return db.with_suffix(".base")

    async def _load_base(self):
        """База Тюнера из файла: накопления и метки учёта. Файла нет —
        чистая база: учёт с текущего номера карточек (старая история не
        затягивается — «копим заново» по решению хозяина)."""
        try:
            for _ in range(20):
                if self.kernel.modules.get("keeper") is not None:
                    break
                await asyncio.sleep(3)
            try:
                st = _unpack_base(self.base_path.read_bytes())
            except FileNotFoundError:
                st = None
            except ValueError:
                # Файл ЧИТАЕТСЯ, но записан в прошлом формате (решение
                # хозяина: «копим заново»). Это не сбой — сообщаем тихо,
                # без трассировки: учёт начинается с текущей карточки.
                self.log.info("База Тюнера: прежний формат файла — начинаем "
                              "учёт заново (полный сброс: удалите %s)",
                              self.base_path)
                st = None
            except Exception:
                self.log.exception("База Тюнера: файл повреждён — беру чистую")
                st = None
            if st is None:
                keeper = self.kernel.modules.get("keeper")
                start = 0
                if keeper is not None:
                    try:
                        start = int(await keeper.max_card_seq())
                    except Exception:
                        start = 0
                self._mark_deal = self._mark_snap = start
                self.log.info("База Тюнера: чистая — учёт с карточки №%d",
                              start)
                return
            self._data_base = {k: list(v) for k, v in st["data"].items()}
            for key in ("ideal", "step", "touch"):
                val = st["trail"].get(key)
                if val:
                    self._trail_base[key] = list(val)
            for key in self._route_base:
                val = st["route"].get(key)
                if val:
                    self._route_base[key] = list(val)
            self._route_at.update(st["route_at"])
            self._lost_base = {k: list(v) for k, v in st["lost"].items()}
            self._lev_at.update(st["lev_at"])
            self._last_published.update(st["trust"])
            self._trust_totals.update(st["totals"])
            self._fact_at_step.update(st["fstep"])
            self._trail_at_step = st["trail_at_step"]
            self._act_at_step = st["act_at_step"]
            self._mark_deal = st["mark_deal"]
            self._mark_snap = st["mark_snap"]
            self.log.info("База Тюнера восстановлена: данных %d видов, "
                          "доверий %d, метки сделок/снимков %d/%d",
                          len(self._data_base), len(self._last_published),
                          self._mark_deal, self._mark_snap)
        finally:
            self._base_ready = True

    def _save_base(self):
        """База — на диск атомарно (tmp + os.replace). Сбой записи не
        роняет Тюнера: последний шаг подождёт следующего."""
        if not self._base_dirty:
            return
        try:
            self.base_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.base_path.with_name(self.base_path.name + ".tmp")
            tmp.write_bytes(_pack_base(
                data=self._data_base, trail=self._trail_base,
                trust=self._last_published, totals=self._trust_totals,
                fstep=self._fact_at_step, trail_at=self._trail_at_step,
                act_at=self._act_at_step, mark_deal=self._mark_deal,
                mark_snap=self._mark_snap, route=self._route_base,
                route_at=self._route_at, lost=self._lost_base,
                lev_at=self._lev_at))
            os.replace(tmp, self.base_path)
            self._base_dirty = False
        except Exception:
            self.log.exception("База Тюнера: не удалось сохранить файл")

    def _stream(self, name):
        """Строка базы по виду данных (8 колонок потоков: реал/тень —
        польза факта; 4/5 — лабораторный поток идеальных доверий)."""
        row = self._data_base.get(name)
        if row is None:
            row = [0.0] * 8
            self._data_base[name] = row
        while len(row) < 8:
            row.append(0.0)
        return row

    @staticmethod
    def _dir_value(entry):
        """(знак, применим ли) из записи вектора либо старого факта.
        Нулевые, устаревшие и «направление не применимо» (магнитуды
        вроде Херста/энтропии) в оценке не участвуют, но из вектора не
        выбрасываются — они там КАК ЕСТЬ."""
        if isinstance(entry, dict):
            if entry.get("state") != "fresh":
                return 0, False
            if entry.get("extra", {}).get("directionless"):
                return 0, False
            try:
                v = float(entry.get("value"))
            except (TypeError, ValueError):
                return 0, False
        else:
            try:
                v = float(entry)
            except (TypeError, ValueError):
                return 0, False
        if v == 0:
            return 0, False
        return (1 if v > 0 else -1), True

    def _score_vector(self, scenario, outcome, w):
        """ПОЛЬЗА ФАКТОВ, потоки реал/тень (утверждено — расчёт Тюнера):
        вектор момента против исхода сделки. Считается ровно один раз
        на сделку (метка); направленные свежие данные — в свой поток."""
        if outcome not in ("long", "short"):
            return
        fields = (scenario.get("fields") or {}) \
            if isinstance(scenario, dict) else {}
        vector = fields.get("vector")
        if not isinstance(vector, dict) or not vector:
            vector = fields.get("facts")       # старые сценарии
        if not isinstance(vector, dict):
            return
        want = 1 if outcome == "long" else -1
        for name, entry in vector.items():
            sign, usable = self._dir_value(entry)
            if not usable:
                continue
            row = self._stream(str(name))
            row[0] += w if sign == want else 0.0
            row[1] += w

    def _lab_columns(self, lf):
        """Колонки лабораторного потока по ФЛАГУ КОНТУРА внутри снимка.

        Лаборатория автономна и не знает о сделках, но снимок несёт контур,
        против которого считалась симуляция. Тюнер раскладывает поток по
        своим колонкам: testnet → лаб_реал (4/5), live → лаб_тень (6/7).
        Снимок без флага — в основную колонку лабораторного потока.
        """
        contour = str((lf or {}).get("contour") or "").strip().lower()
        return 6 if contour == "live" else 4

    def _score_ideal(self, lf, w):
        """ЛАБОРАТОРНЫЙ ПОТОК (утверждено): идеальные доверия из
        самодостаточного снимка Лаборатории — какие значения сделали бы
        контекст идеальным. Колонка выбирается по контуру внутри снимка."""
        trusts = ((lf.get("ideal_set") or {}).get("trusts")) or {}
        if not isinstance(trusts, dict):
            return
        base = self._lab_columns(lf)
        for name, val in trusts.items():
            try:
                t = float(val)
            except (TypeError, ValueError):
                continue
            row = self._stream(str(name))
            row[base] += w * t          # колонка лабораторного потока
            row[base + 1] += w

    def _score_route(self, lf, w):
        """МАРШРУТ-ИДЕАЛЫ (утверждено): стоп/активация/шаг из прогона
        Лаборатории — к среднему этих значений Тюнер шагает ручками."""
        route = ((lf.get("ideal_set") or {}).get("route")) or {}
        if not isinstance(route, dict):
            return
        for key, name in (("stop_frac", "stop"), ("act_frac", "act"),
                          ("dist_frac", "dist")):
            try:
                val = float(route.get(key))
            except (TypeError, ValueError):
                continue
            pair = self._route_base[name]
            pair[0] += w * val
            pair[1] += w

    def _kind_streams(self):
        """Потоки по ТИПАМ данных — агрегат базы (доверие ведётся типам)."""
        acc = {}
        for name, row in self._data_base.items():
            ak = FACT_AUTHORS.get(name)
            if ak is None:
                continue
            row = list(row) + [0.0] * (8 - len(row))
            t = acc.setdefault(ak[1], [0.0] * 8)
            for i in range(8):
                t[i] += row[i]
        return acc

    def _type_totals(self):
        """Свод по ТИПАМ данных: (победы, всего) для шага доверий.

        Доверие ведётся по типу карточки (FACT_AUTHORS), а не по отдельному
        виду данных: «тренд-вектор» — один тип, а не reg_score_1m и
        reg_score_4h по отдельности. Победа = факт вёл путь (знак совпал),
        всего = вес всех учтённых сигналов этого типа.
        """
        wins, total = {}, {}
        for name, row in self._data_base.items():
            ak = FACT_AUTHORS.get(name)
            if ak is None:
                continue
            kind = ak[1]
            row = list(row) + [0.0] * (8 - len(row))
            wins[kind] = wins.get(kind, 0.0) + row[0] + row[2] \
                + row[4] + row[6]
            total[kind] = total.get(kind, 0.0) + row[1] + row[3] \
                + row[5] + row[7]
        return {k: (wins[k], total[k]) for k in total}

    def _publish_base_stats(self):
        """Снимок базы — на Склад (витрина: виден в срезах; сама база — в файле)."""
        data = {}
        for name, row in self._data_base.items():
            row = list(row) + [0.0] * (8 - len(row))
            if any(row):
                data[name] = [round(x, 3) for x in row]
        if not data:
            return
        types = {}
        for kind, row in self._kind_streams().items():
            types[kind] = {
                "real": [round(row[0], 2), round(row[1], 2)],
                "shadow": [round(row[2], 2), round(row[3], 2)],
                "lab_real": [round(row[4], 2), round(row[5], 2)],
                "lab_shadow": [round(row[6], 2), round(row[7], 2)],
                "trust": round(self._last_published.get(kind, NEUTRAL), 3),
            }
        self.publish(DRAFT, {
            "kind": "tuner-stats",
            "author": "autotuner",
            "confidence": 0.9,
            "fields": {"data": data,
                       "types": types,
                       "route": {k: [round(v[0], 3), round(v[1], 1)]
                                 for k, v in self._route_base.items()},
                       "lost": {r: [round(v[0], 1), round(v[1], 1),
                                    # БАЗА МОЖЕТ ПРИЙТИ КОРОТКОЙ: старые
                                    # файлы хранили пару без третьего
                                    # счётчика. Не роняем публикацию
                                    # статистики из-за пустого поля.
                                    round(v[2], 3) if len(v) > 2 else 0.0]
                                for r, v in self._lost_base.items()},
                       "trail": {k: [round(v[0], 3), round(v[1], 1)]
                                 for k, v in self._trail_base.items()},
                       "health": self._health_last or None,
                       "updated_ms": int(time.time() * 1000)},
            "lineage": ["tuner:base"],
        })

    def _score_trail(self, lf):
        """СОПРОВОЖДЕНИЕ (утверждено): ориентиры для ручек берём из самого
        лабораторного снимка — идеальное дыхание цены и шаг сопровождения,
        который к нему приводит систему."""
        try:
            ideal = float(lf.get("ideal_trail_pct"))
        except (TypeError, ValueError):
            return
        if ideal > 0.01:
            b = self._trail_base["ideal"]
            b[0] += ideal
            b[1] += 1.0
        route = (lf.get("ideal_set") or {}).get("route") or {}
        try:
            step = float(route.get("dist_frac")) * 100.0
        except (TypeError, ValueError):
            step = 0.0
        if step > 1e-6:
            b = self._trail_base["step"]
            b[0] += step
            b[1] += 1.0
        try:
            mfe = abs(float(lf.get("mfe_pct")))
            act = float(route.get("act_frac")) * 100.0
        except (TypeError, ValueError):
            return
        if act > 1e-6:
            b = self._trail_base["touch"]
            b[1] += 1.0
            if mfe >= act:
                b[0] += 1.0

    def _tune_trail(self):
        """СОПРОВОЖДЕНИЕ — ведёт Тюнер (утверждено): шаг стопа за ценой
        приводится к типичному дыханию цены (идеал), активация — к тому,
        чтобы сопровождение реально работало. Снимок Лабы — только данные,
        решение и правка ручек — Тюнера."""
        ib = self._trail_base["ideal"]
        sb = self._trail_base["step"]
        cur = self.kernel.cfg.get_float("scenario",
                                        "route_trail_distance_frac", 0.20)
        if (ib[1] >= self.min_deals and sb[1] >= self.min_deals
                and ib[1] - self._trail_at_step >= self.min_deals):
            ideal_avg = ib[0] / ib[1]
            step_avg = sb[0] / sb[1]
            new = cur
            if ideal_avg > step_avg * 1.15:
                new = round(min(0.60, cur * 1.05), 3)   # шаг шире дыхания
            elif ideal_avg < step_avg * 0.85:
                new = round(max(0.05, cur * 0.95), 3)   # шаг уже дыхания
            if abs(new - cur) > 1e-9:
                self.kernel.cfg.override("scenario",
                                         "route_trail_distance_frac", new)
                self._write_key("scenario", "route_trail_distance_frac", new)
                self.log.warning(
                    "ШАГ СОПРОВОЖДЕНИЯ: %.2f → %.2f (дыхание цены %.3f%% "
                    "против шага %.3f%%, n=%.0f)",
                    cur, new, ideal_avg, step_avg, ib[1])
            self._trail_at_step = ib[1]
        t = self._trail_base["touch"]
        if (t[1] >= self.min_deals
                and t[1] - self._act_at_step >= self.min_deals):
            share = t[0] / t[1]
            act_cur = self.kernel.cfg.get_float(
                "scenario", "route_trail_activate_frac", 0.70)
            act_new = act_cur
            if share < 0.40:
                act_new = round(max(0.20, act_cur * 0.95), 3)
            elif share > 0.80:
                act_new = round(min(0.90, act_cur * 1.05), 3)
            if abs(act_new - act_cur) > 1e-9:
                self.kernel.cfg.override("scenario",
                                         "route_trail_activate_frac", act_new)
                self._write_key("scenario", "route_trail_activate_frac",
                                act_new)
                self.log.warning(
                    "АКТИВАЦИЯ СОПРОВОЖДЕНИЯ: %.2f → %.2f (доходило %.0f%% "
                    "сделок, n=%.0f)",
                    act_cur, act_new, share * 100, t[1])
            self._act_at_step = t[1]

    async def _outcome_by_candles(self, deal_fields, deal_card):
        """Исход без Лаборатории: куда цена ушла от входа за окно пути
        (60 минут — как окно идеала) — по свечам со Склада."""
        keeper = self.kernel.modules.get("keeper")
        if keeper is None:
            return None
        try:
            opened = int(deal_fields.get("opened_ms", 0) or 0) \
                or int(deal_card.get("created_ms", 0) or 0)
            entry = float(deal_fields.get("entry") or 0.0)
        except (TypeError, ValueError):
            return None
        if entry <= 0 or opened <= 0:
            return None
        try:
            rows = await keeper.get_last_candles(self.symbol, self.tf, 200)
        except Exception:
            return None
        seg = [r for r in rows if opened <= int(r[0]) < opened + 3_600_000]
        if len(seg) < 20:
            return None
        mv = (float(seg[-1][4]) - entry) / entry
        if abs(mv) < 0.0005:
            return None
        return "long" if mv > 0 else "short"

    async def _on_card(self, env):
        kind = env.payload.get("kind")
        if kind == "deal-closed":
            await self._evaluate()
        elif kind == "lab-snapshot":
            # Снимок мог доехать позже сделки (бар пути закрывается
            # вплотную к сделке) — пересчитываем доверия по факту
            await asyncio.sleep(2.0)     # писец в базу — фоновый
            await self._evaluate()
        elif kind == "spoof-verdict":
            self._on_spoof_verdict(env.payload.get("fields") or {})
        elif kind == "lab-report":
            # ПОТОЛОК ПЛЕЧА (Этап 35): сигнал упущенного идеала из
            # below_min-теней — потолок вверх, чтобы бот вышел из тени
            self._maybe_raise_lev(env.payload.get("fields") or {})

    def _maybe_raise_lev(self, fields):
        """ПОТОЛОК ПЛЕЧА (Этап 35): тени, забракованные из-за минимума
        биржи, стабильно суть-верны и несут идеал → потолок вверх шагом
        (бэклог: «тюнер поднимает max_lev»). Сигнал исчез — ручка стоит."""
        lost = fields.get("lost_by_reason") or {}
        shadows = right = 0
        ideal_sum = 0.0
        for reason, st in lost.items():
            if "ниже минимума" not in str(reason):
                continue
            shadows += int(st.get("shadows", 0) or 0)
            right += int(st.get("right", 0) or 0)
            ideal_sum += float(st.get("ideal_sum_pct", 0.0) or 0.0)
        if shadows < 3 or right < 3 or right * 2 < shadows or ideal_sum < 0.1:
            return
        cur = self.kernel.cfg.scenario_max_lev
        new = round(min(10.0, cur + 0.5), 1)
        if new <= cur:
            return
        self._write_lev(new)
        self.log.warning("ПОТОЛОК ПЛЕЧА: %.1f → %.1f (ниже минимума биржи: "
                         "теней %d, суть-верно %d, идеал +%.2f%%)",
                         cur, new, shadows, right, ideal_sum)

    def _write_lev(self, value):
        """Потолок плеча — в config.ini и в живой конфиг (субординация:
        пишет только Тюнер)."""
        self.kernel.cfg.override("scenario", "max_lev", value)
        self._write_ini([("scenario", "max_lev", value)])

    async def _evaluate(self):
        keeper = self.kernel.modules.get("keeper")
        if keeper is None:
            return
        if not self._base_ready:
            return                    # база ещё грузится — метки неизвестны
        before_deal, before_snap = self._mark_deal, self._mark_snap
        cards = await keeper.get_last_cards(2000)   # окно связки сделка→сценарий
        by_seq = {int(c["seq"]): c for c in cards}
        deals = [c for c in cards if c["kind"] == "deal-closed"]
        snaps = [c for c in cards if c["kind"] == "lab-snapshot"]
        # РАННИЙ ВЫХОТ УБРАН ПОЛНОСТЬЮ (утверждено): лабораторные снимки —
        # самостоятельный поток, они приходят и при пустой базе сделок
        # (свежая установка, прогон без торговли). Никакого «без сделок —
        # нечего пересчитывать»: считаем всегда, что пришло.

        # ДВА НЕЗАВИСИМЫХ ПОТОКА (утверждено): снимки Дилера и снимки
        # Лаборатории приходят в общую очередь на общих основаниях. Связи
        # между ними НЕТ: ни join по deal_id, ни родословных между модулями.
        # Каждый снимок самодостаточен и несёт свою метку потока внутри.
        lab_w = self.kernel.cfg.tuner_weight_lab
        self._last_snaps = [(c.get("fields") or {}) for c in snaps]
        for c in snaps:
            seq = int(c.get("seq", -1))
            if seq <= self._mark_snap:
                continue
            lf = c.get("fields") or {}
            self._score_ideal(lf, lab_w)
            self._score_route(lf, lab_w)
            self._score_trail(lf)
            self._mark_snap = max(self._mark_snap, seq)

        # «УПУЩЕННОЕ ПО ПРИЧИНАМ РИСКА» (утверждено): теневые сделки,
        # зарезанные фильтрами Риска. Считается по СНИМКАМ ДИЛЕРА — в них
        # note живёт по своей природе (решение Риска), лабораторные снимки
        # о запретах Риска не знают и знать не должны.
        for deal_c in deals:
            df = deal_c.get("fields") or {}
            if df.get("mode") == "real" or not df.get("note"):
                continue
            stat = self._lost_base.setdefault(str(df["note"]), [0.0, 0.0, 0.0])
            stat[0] += 1.0

        # СДЕЛКИ — УЧЁТ ПО МЕТКЕ (утверждено): польза данных считается
        # ровно один раз на сделку; пересчёты окна базу не раздувают.
        # conf_deals/pnl_list — окно для калибровки и отчёта (не копилка)
        conf_deals = []               # (обещанная уверенность, выигрыш)
        pnl_list = []                 # для отчёта к идеалу
        quad = {"real_ok": 0, "real_miss": 0,
                "shadow_ok": 0, "shadow_miss": 0}
        for deal in deals:
            f = deal["fields"] or {}
            # ЗАЩИТА ОТ БРАКА (утверждено): записи без честной цены выхода
            # (exit=0 при аварийном снятии) портят репутацию — пропускаем
            if (f.get("status") == "corrupted"
                    or f.get("pnl_pct") is None
                    or not f.get("exit")):
                continue
            seq = int(deal.get("seq", -1))
            # ПРИШИТЫЙ СЦЕНАРИЙ: снимок самодостаточен; окно — фолбэк
            emb = f.get("scenario")
            scenario = (emb if isinstance(emb, dict) and emb.get("fields")
                        else by_seq.get(int(f.get("scenario_seq", -1))))
            if scenario is None:
                continue
            win = float(f.get("pnl_pct", 0.0)) > 0
            pnl_list.append(float(f.get("pnl_pct", 0.0)))
            conf_deals.append((float(scenario.get("confidence", 0.0)), win))
            if seq <= self._mark_deal:
                continue              # польза данных этой сделки посчитана
            # ИСХОД СЧИТАЕТ САМ ТЮНЕР по свечам (утверждено): лабораторные
            # снимки больше не привязаны к сделкам и исхода не диктуют —
            # два потока независимы.
            outcome = await self._outcome_by_candles(f, deal)
            if outcome in ("long", "short"):
                # суть сделки: совпало ли направление с исходом
                shadow = f.get("mode") != "real"
                right = (f.get("direction") == outcome)
                self._essence_history.append(1 if right else 0)
                del self._essence_history[:-500]   # метрике хватает окна
                quad[("shadow_" if shadow else "real_")
                     + ("ok" if right else "miss")] += 1
            w = (self.kernel.cfg.tuner_weight_real
                 if f.get("mode") == "real"
                 else self.kernel.cfg.tuner_weight_shadow)
            # ВЕКТОР — из снимка сделки (самодостаточен), фолбэк — факты
            vector = f.get("vector") or (scenario.get("fields") or {}).get("facts")
            self._score_vector({"fields": {"vector": vector,
                                           "facts": f.get("facts") or {}}},
                               outcome, w)
            self._mark_deal = max(self._mark_deal, seq)
            # отладка контура (29.09): почему база не наполняется
            self.log.info("ОЦЕНКА ДАННЫХ: сделка №%d, исход %s, база %d видов",
                          seq, outcome, len(self._data_base))

        # отладка контура (29.09)
        self.log.info("Пересчёт Тюнера: сделок %d, лаб-снимков %d, база %d видов",
                      len(deals), len(snaps), len(self._data_base))

        updates = []
        # адаптивный шаг: система в плюсе — бодрее, в минусе — осторожнее
        wins_all = sum(w for w, _ in conf_deals)
        efficiency = (wins_all / len(conf_deals)) if conf_deals else NEUTRAL
        self._efficiency = efficiency
        step = max(self.trust_step * 0.5,
                   min(self.trust_step * 1.5,
                       self.trust_step * (0.5 + efficiency)))
        # ОЧКИ ДОВЕРИЯ — ДАННЫМ (утверждено): доверие привязано к типу
        # данных (тренд, новости, зоны, лента…), а не к анализатору;
        # полезность — из накопленной базы (шаги сравнимы между собой)
        for kind, (wins, total) in sorted(self._type_totals().items()):
            if total < self.min_deals:
                self.log.info("доверие-отладка: %s пропущен "
                              "(вес=%.2f < min_deals=%d)",
                              kind, total, self.min_deals)
                continue
            # ОДНА СДЕЛКА — ОДИН ШАГ (Этап 34, урок репетиции): шаг только
            # когда данных в базе стало больше (новая сделка)
            last_total = self._trust_totals.get(kind)
            if last_total is not None and total <= last_total:
                self.log.info("доверие-отладка: %s пропущен "
                              "(total=%.0f <= прошлый %.0f)",
                              kind, total, last_total)
                continue
            prev = self._base_prev.get(kind)
            winrate = wins / total
            # цель — фактическая полезность в вилке; движение — не больше шага
            target = max(self.trust_min, min(self.trust_max, winrate))
            cur = self._last_published.get(kind, NEUTRAL)
            trust = round(cur + max(-step, min(step, target - cur)), 3)
            trust = max(self.trust_min, min(self.trust_max, trust))
            self._trust_totals[kind] = total
            self._base_prev[kind] = winrate
            if abs(trust - cur) > 1e-9:
                self._last_published[kind] = trust
                updates.append({"kind": kind, "trust": trust})
                self.log.info(
                    "Доверие данных %s → %.2f (полезность %.0f%%%s, n=%.0f)",
                    kind, trust, winrate * 100,
                    "" if prev is None else ", было %.0f%%" % (prev * 100),
                    total)
            else:
                self.log.info("доверие-отладка: %s без шага "
                              "(trust=%.3f = cur=%.3f, winrate=%.2f)",
                              kind, trust, cur, winrate)

        # отладка контура (29.09)
        self.log.info("Цикл доверий: типов %d, обновлений %d",
                      len(self._type_totals()), len(updates))
        if updates:
            self.publish(TRUST, updates)
            self._write_config(updates)

        self._calibrate_confidence(conf_deals)
        self._report_to_ideal(deals, pnl_list)
        self._adapt_open_conf(quad)
        self._tune_fact_weights()
        self._tune_trail()
        # ЗДОРОВЬЕ МЕХАНИКИ — по метке учёта, а не по окну склада
        if self._mark_deal - self._health_at >= 10:
            self._health_at = self._mark_deal
            self._report_health(deals)
        self._publish_base_stats()
        if (self._mark_deal, self._mark_snap) != (before_deal, before_snap):
            self._base_dirty = True
        self._save_base()             # база — свой файл: переживает перезапуск

    def _adapt_open_conf(self, quad):
        """ПОРОГ РЕАЛА (утверждено): двигается по сути сделок — тени,
        чьё направление совпало с исходом, доказывают точность → порог
        вниз; промахи → вверх.
        ШАГ АДАПТИВНЫЙ (утверждено — «на цыпочках, пока мало фактов»):
        - объём выборки: множитель √(n/(n+50)) — при 10 сделках ~0.4,
          при 200 почти полный;
        - разброс сигнала: 2×|точность−0.5| — единодушие двигает,
          метель 50/50 почти нет;
        - самочувствие системы: в плюсе ×1.5, в минусе ×0.5;
        - цель сглаживается экспоненциально (α = тот же множитель
          выборки) — одна серия везения ручку не качает."""
        n = len(self._essence_history)
        if n:
            acc = sum(self._essence_history) / n
            sample_factor = (n / (n + 50.0)) ** 0.5
            signal = min(1.0, 2.0 * abs(acc - 0.5))   # 0 — метель, 1 — единодушие
            eff_mult = (1.5 if self._efficiency > NEUTRAL
                        else 0.5 if self._efficiency < NEUTRAL else 1.0)
            # цель: точность прогноза отображается в вилку порога
            target = self.oc_max - acc * (self.oc_max - self.oc_min)
            if self._oc_smooth is None:
                self._oc_smooth = self.open_conf
            self._oc_smooth += sample_factor * (target - self._oc_smooth)
            step = self.oc_step * sample_factor * (0.25 + 0.75 * signal) * eff_mult
            delta = max(-step, min(step, self._oc_smooth - self.open_conf))
            new_oc = max(self.oc_min,
                         min(self.oc_max, round(self.open_conf + delta, 3)))
        else:
            new_oc = self.open_conf
        if quad["real_ok"] or quad["real_miss"] or quad["shadow_ok"] \
                or quad["shadow_miss"]:
            self.log.info("Матрица сделок: реал суть-верно %d / мимо %d, "
                          "тень суть-верно %d / мимо %d; "
                          "точность %.0f%% по %d, шаг порога %.3f",
                          quad["real_ok"], quad["real_miss"],
                          quad["shadow_ok"], quad["shadow_miss"],
                          (100.0 * sum(self._essence_history)
                           / n if n else 0.0), n,
                          abs(new_oc - self.open_conf))
        if new_oc == self.open_conf:
            self._tune_spoof_sieve()
            return
        old = self.open_conf
        self.open_conf = new_oc
        self.kernel.cfg.override("risk", "open_conf_threshold", new_oc)
        self._write_threshold(new_oc)
        self.log.warning("ПОРОГ РЕАЛА: %.2f → %.2f (сделок с идеалом %d, "
                         "шаг %.3f — на цыпочках, пока выборка мала)",
                         old, new_oc, n, abs(new_oc - old))
        self._tune_spoof_sieve()

    def _on_spoof_verdict(self, f):
        """Вердикты Лаборатории по спуфинг-алертам — сырьё для сита."""
        verdict = f.get("verdict")
        if verdict in ("confirmed", "refuted"):
            self._spoof_stats[verdict] = self._spoof_stats.get(verdict, 0) + 1

    def _tune_spoof_sieve(self):
        """СИТО СПУФИНГА (утверждено — на откуп Тюнеру): по вердиктам
        Лаборатории vanish_ticks_max сжимается (сито врёт — требуем более
        быстрого исчезновения) или ослабляется (сито точен — ловим больше)."""
        total = (self._spoof_stats.get("confirmed", 0)
                 + self._spoof_stats.get("refuted", 0))
        if total < self.spoof_min_samples:
            return
        refuted_share = self._spoof_stats.get("refuted", 0) / total
        cur = self.kernel.cfg.spoof_vanish_ticks_max
        new = cur
        if refuted_share > 0.6 and cur > 1:
            new = cur - 1
        elif refuted_share < 0.2 and cur < 3:
            new = cur + 1
        if new != cur:
            self.kernel.cfg.override("spoof", "vanish_ticks_max", new)
            self._write_key("spoof", "vanish_ticks_max", new)
            self.log.warning("СИТО СПУФИНГА: vanish_ticks_max %d → %d "
                             "(вердиктов %d, опровержений %.0f%%)",
                             cur, new, total, refuted_share * 100)
        self._spoof_stats = {}            # новый цикл наблюдения

    def _report_health(self, deals):
        """ЗДОРОВЬЕ МЕХАНИКИ (утверждено): отчёт — какой узел мешает
        зарабатывать. Цифры-приговоры: вход (MFE vs MAE), закрытия
        (цель/стоп/безубыток), цель против хода, время до цели,
        молчащие данные. Пишется в журнал и в снимок tuner-stats.
        Лабораторный поток идёт по своим снимкам, сделки — по своим."""
        mfe, mae, trs, rel = [], [], [], []
        for lf in self._last_snaps:
            try:
                if lf.get("mfe_pct") is not None:
                    mfe.append(abs(float(lf["mfe_pct"])))
                if lf.get("mae_pct") is not None:
                    mae.append(abs(float(lf["mae_pct"])))
                if lf.get("t_ratio") is not None:
                    trs.append(float(lf["t_ratio"]))
            except (TypeError, ValueError):
                continue
            try:
                e = float(lf.get("entry"))
                t = (lf.get("targets") or [None])[0]
                m = abs(float(lf.get("mfe_pct") or 0.0)) / 100.0
                if e and t and m > 1e-6:
                    rel.append(abs(float(t) - e) / e / m)
            except (TypeError, ValueError, IndexError):
                continue
        reasons = {}
        for d in deals:
            f = d.get("fields") or {}
            r = str(f.get("reason"))
            if r and r != "None":
                reasons[r] = reasons.get(r, 0) + 1

        def med(a):
            a = sorted(a)
            return a[len(a) // 2] if a else 0.0

        mf, ma = med(mfe), med(mae)
        ideal = med([abs(float(x)) for x in
                     [c.get("fields", {}).get("ideal_pnl_pct") or 0.0
                      for c in self._last_snaps]])
        dead = [name for name in WEIGHT_KNOBS if name not in self._data_base]
        # Строки базы — по 8 колонок (потоки), а не по 2: берём колонку
        # «всего» каждой строки (1/3/5/7 — счётчики веса потоков).
        d_n = sum(sum(row[i] for i in (1, 3, 5, 7))
                  for row in self._data_base.values())
        self._health_last = {
            "mfe_pct": round(mf, 3), "mae_pct": round(ma, 3),
            "closes": reasons, "target_over_move": round(med(rel), 2),
            "ideal_capture_pct": round(ideal, 3),
            "time_fact_over_plan": round(med(trs), 2),
            "data_kinds": len(self._data_base), "data_n": round(d_n, 1),
            "dead_data": dead,
            "updated_ms": int(time.time() * 1000),
        }
        self.log.warning(
            "ЗДОРОВЬЕ МЕХАНИКИ: вход MFE %.3f%% vs MAE %.3f%% (%s) | "
            "закрытия %s | цель дальше хода x%.1f | время до цели "
            "факт/план x%.2f (n=%d) | идеал %.3f%% (лаб) | база "
            "%d видов (n=%.0f) | молчат: %s",
            mf, ma, "вход даёт преимущество" if mf > ma else "ВХОД ПРОТИВ",
            reasons or "нет", med(rel), med(trs), len(trs), ideal,
            len(self._data_base), d_n, ", ".join(dead) or "нет")

    def _tune_fact_weights(self):
        """ВЕСА ДАННЫХ (утверждено): по НАКОПЛЕННОЙ полезности данных
        (база для сравнения — расчёт Тюнера) двигаются веса данных в
        формуле Сценариста — данные, что уводили путь, замолкают;
        проверенные звучат громче. Шаг — раз в min_deals новых данных,
        вилка каждой ручки своя: механизм не уводит вес в абсурд."""
        for fact, (section, key, start, lo, hi) in WEIGHT_KNOBS.items():
            stat = self._data_base.get(fact)
            if not stat or stat[1] < self.min_deals:
                continue
            n = stat[1]
            done = self._fact_at_step.get(fact)
            if done is not None and n - done < self.min_deals:
                continue          # шаг на каждые новые min_deals данных
            acc = stat[0] / stat[1]
            cur = self.kernel.cfg.get_float(section, key, start)
            # ЗНАК ПО ПОЛЕЗНОСТИ (утверждено): сторона веса — со стороны
            # пользы (≥0.5 — данные ведут к пути, <0.5 — использовать
            # наоборот); пусто — запуск, не та сторона — плавный разворот
            target_sign = 1.0 if acc >= 0.5 else -1.0
            new = cur
            if abs(cur) < 0.005:
                if abs(acc - 0.5) >= 0.05:
                    new = round(0.02 * target_sign, 3)
            elif cur * target_sign < 0:
                new = round(cur + 0.02 * target_sign, 3)   # разворот
            elif abs(acc - 0.5) <= 0.05:
                new = round(cur * 0.9, 3)                  # неясно — тише
            elif abs(acc - 0.5) >= 0.15:
                new = round(cur * 1.05, 3)                 # явно — громче
            new = round(max(lo, min(hi, new)), 3)
            if abs(new - cur) > 1e-9:
                self.kernel.cfg.override(section, key, new)
                self._write_key(section, key, new)
                self.log.warning("ВЕС ДАННЫХ %s: %.3f → %.3f "
                                 "(полезность %.0f%%, n=%.0f)",
                                 fact, cur, new, acc * 100, n)
            self._fact_at_step[fact] = n

    # ---------- запись в config.ini ----------

    def _write_ini(self, updates):
        """Пачка правок одним атомарным проходом (субординация: config.ini
        пишет только Тюнер). Меняется только строка нужного ключа —
        комментарии и порядок переживают. updates: [(section, key, value)]."""
        path = self.kernel.cfg.root / "config" / "config.ini"
        try:
            lines = (path.read_text(encoding="utf-8").splitlines()
                     if path.exists() else [])
            for section, key, value in updates:
                self._set_ini(lines, str(section), str(key), str(value))
            tmp = path.with_name(path.name + ".tmp")
            tmp.write_text("\n".join(lines) + "\n", encoding="utf-8")
            os.replace(tmp, path)               # атомарная подмена файла
        except Exception:
            self.log.exception("Не удалось записать %d правок в config.ini",
                               len(updates))

    @staticmethod
    def _set_ini(lines, section, key, value):
        """Точечная правка в списке строк INI: заменить строку ключа.
        Нет секции — секция дописывается в конец; нет ключа — ключ
        вставляется сразу после шапки секции."""
        sec = section.strip().lower()
        key = key.strip().lower()
        lo = None
        hi = len(lines)
        for i, line in enumerate(lines):
            m = _INI_SEC.match(line)
            if not m:
                continue
            if lo is None and m.group("name").strip().lower() == sec:
                lo = i
            elif lo is not None:
                hi = i
                break
        if lo is None:
            if lines and lines[-1].strip():
                lines.append("")
            lines.append("[%s]" % section)
            lines.append("%s = %s" % (key, value))
            return
        for i in range(lo + 1, hi):
            m = _INI_KEY.match(lines[i])
            if m and m.group("key").strip().lower() == key:
                lines[i] = "%s = %s" % (key, value)
                return
        lines.insert(lo + 1, "%s = %s" % (key, value))

    def _write_key(self, section, key, value):
        """Один параметр — в config.ini (субординация: пишет только Тюнер)."""
        self._write_ini([(section, key, value)])

    def _write_threshold(self, value):
        """Порог реала — в config.ini (переживает перезапуск)."""
        self._write_ini([("risk", "open_conf_threshold", value)])

    def _calibrate_confidence(self, conf_deals):
        """Обещанное vs факт (утверждено): справедливость обещаний на
        САМЫХ уверенных сделках — верхняя четверть обещаний, а не жёсткий
        0.70 (он недостижим при верной картине: механизм стоял мёртвым
        и порог реала был закрыт). Факт не хуже обещаний — множитель
        вверх (до потолка), хуже допуска — вниз (до пола)."""
        if len(conf_deals) < self.min_deals:
            return
        confs = sorted(c for c, _ in conf_deals)
        q3 = confs[int(len(confs) * 0.75)]
        high = [(c, w) for c, w in conf_deals if c >= q3]
        if len(high) < max(3, self.min_deals // 2):
            return
        mean_promised = sum(c for c, _ in high) / len(high)
        actual_winrate = sum(1 for _, w in high if w) / len(high)
        cur = self.kernel.cfg.scenario_conf_calibration
        if actual_winrate + CALIB_TOLERANCE < mean_promised:
            new = round(max(CALIB_FLOOR, cur * 0.9), 3)
            if new != cur:
                self._write_calibration(new)
                self.log.warning("Калибровка уверенности: обещали %.2f, факт %.2f "
                                 "(%d сделок) → множитель %.2f",
                                 mean_promised, actual_winrate, len(high), new)
        elif actual_winrate >= mean_promised and cur < CALIB_CEIL:
            new = round(min(CALIB_CEIL, cur * 1.05), 3)
            if new != cur:
                self._write_calibration(new)
                self.log.info("Калибровка уверенности: факт %.2f не хуже обещаний "
                              "%.2f → множитель %.2f",
                              actual_winrate, mean_promised, new)

    def _report_to_ideal(self, deals, pnl_list):
        """Движение к идеалу: PnL 100%/день, WR 100%, просадка 0%."""
        if len(pnl_list) < self.min_deals:
            return
        winrate = sum(1 for p in pnl_list if p > 0) / len(pnl_list)
        avg_pnl = sum(pnl_list) / len(pnl_list)
        equity, peak, drawdown = 1.0, 1.0, 0.0
        for p in pnl_list:
            equity *= (1.0 + p / 100.0)
            peak = max(peak, equity)
            drawdown = max(drawdown, (peak - equity) / peak)
        self.log.info("ОТЧЁТ: сделок %d, WR %.0f%% (идеал 100%%), "
                      "средний PnL %.2f%%, просадка %.1f%% (идеал 0%%)",
                      len(pnl_list), winrate * 100, avg_pnl, drawdown * 100)

    def _write_config(self, updates):
        """Справочник доверий — в config.ini (переживает перезапуск):
        ключ — тип данных (не анализатор)."""
        self._write_ini([("tuner", "trust__%s" % u["kind"], u["trust"])
                         for u in updates])

    def _write_calibration(self, value):
        """Множитель калибровки уверенности — в config.ini."""
        self._write_ini([("scenario", "conf_calibration", value)])
