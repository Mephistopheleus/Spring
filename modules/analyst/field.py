"""Анализатор Поля — собирает карточки в картину рынка.

На общих правах (утверждено): сырьё — не биржа, а карточки со склада
(зоны SR, объёмные барьеры, новости, состояние потока, дивергенции).
Выход — КАРТИНА: список зон-магнитов, куда цена стремится, вектор
настроения и вектор топлива. Картина сама проходит через Регистратора
и становится карточкой со своим весом и паспортом.

Правила сборки (утверждены):
- каждая карточка уже несёт готовый вес (его посчитал Регистратор);
- значимость карточки тает со временем — затухание по типу карточки;
- перекрывающиеся зоны сливаются, силы складываются (потолок 1.0);
- МНОЖИТЕЛЬ ТЕЧЕНИЯ (утверждено): у объёмного барьера вес зависит от
  положения на полотне — линзы показывают уклон рынка на масштабе,
  равном окну жизни барьера; течение против барьера ослабляет его,
  по барьеру — усиливает. Множитель пишется в картину (родословная
  контекста — тюнер откалибрует его по фактам);
- МАТРИЦА ГРОМКОСТИ (утверждено): Проводимость в фазах шторма
  (пробой/вынос) выдаёт свой коэффициент на тип карточки — тренд
  бушует или глохнет в зависимости от физики шторма. Нет записи —
  legacy-ручки (k_shelf глушит полки, k_impulse усиливает импульс);
- направление стремления: зона ниже цены тянет вниз, выше — вверх;
- новости и поток зону не дают — дают картине вектора (настроение,
  топливо);
- штамп картины: смена картины или устаревание (60 секунд).
"""
import time

from core.module import Manifest, NeirModule
from modules._labels import CARD, CONDUCTIVITY, DRAFT, PRICE, SURFACE
from modules.market.lenses import surface_read

STAMP_REFRESH_SEC = 60      # устаревание картины
TOP_ZONES = 6               # сколько магнитов отдаём в картине
MAX_CARDS = 400             # предел памяти на сырьевые карточки
TIDE_K = 0.4                # сила влияния течения на барьер (множитель)
TIDE_MAX = 1.5              # потолок множителя течения

CONSUME_KINDS = ("sr-zone", "news-sentiment", "absorption-barrier",
                 "flow-state", "delta-divergence", "fvg-zone", "stop-cluster",
                 "trend-vector", "g-trend")
ZONE_KINDS = ("sr-zone", "absorption-barrier", "fvg-zone", "stop-cluster")
TTL_BY_KIND = {
    "sr-zone": 15 * 60,
    "absorption-barrier": 15 * 60,
    "flow-state": 5 * 60,
    "delta-divergence": 10 * 60,
    "news-sentiment": 12 * 3600,
    "fvg-zone": 120 * 60,
    "stop-cluster": 60 * 60,
    "trend-vector": 120 * 60,
    "g-trend": 10 * 60,
}


class FieldAnalyzer(NeirModule):
    manifest = Manifest(name="field-analyst", title="Анализатор Поля (картина)",
                        requires=("card",), priority=60, tick_timeout=10.0)

    async def on_start(self):
        self._cards = {}          # номер карточки -> сырьё
        self._signature = None
        self._last_stamp_ms = int(time.time() * 1000)   # устаревание — от старта
        # Анти-спам (06.10): пересборка по событиям (карточка, цена каждые
        # 2с) с непрерывной подписью (pull/fuel) слала картину каждые ~2с —
        # 9к карточек picture в сутки и спам в базу. Отправляем не чаще
        # интервала штампа (stamp_interval_sec), подпись дождётся тика.
        # Структурная сигнатура (уровни/pull зон) от троттла освобождена —
        # см. _check_stamp (06.10).
        self._last_publish_ms = 0
        self._keys_sig_prev = None
        self.price = 0.0
        self.stamps = 0
        self._rewarm_left = 40    # сколько раз можно добирать сырьё из базы

        c = self.kernel.cfg
        self._stamp_every = c.field_stamp_interval_sec
        self._zone_ttl = c.field_zone_ttl_min * 60.0
        self._touch_cd = c.field_touch_cooldown_sec
        self._exh_step = c.field_exhaust_step
        self._exh_floor = c.field_exhaust_floor
        self._refill_frac = c.field_refill_frac
        self._refill_sec = c.field_refill_sec
        self._shelves = {}        # ключ-уровень -> полка (касания, тишина)
        # Спуфинг (утверждено): фальшивые уровни — ключ -> до какого времени
        self._fake = {}
        self.fake_factor = c.spoof_fake_shelf_factor
        self.fake_ttl = c.spoof_fake_ttl_sec
        # TTE (утверждено): полки тают у макро-события
        self.shelf_damp = c.tte_shelf_damp
        self._tte = None
        self._tte_weight = None
        self._last_price_mono = 0.0
        self._last_price_build = 0.0

        self.subscribe(CARD, self._on_card)
        self.subscribe(PRICE, self._on_price)

        # Прогрев: недавние карточки из базы (поздний старт — не беда)
        keeper = self.kernel.modules.get("keeper")
        if keeper is not None:
            for card in await keeper.get_last_cards(200):
                self._remember(card)
            self.log.info("Прогрев поля: %d карточек со склада", len(self._cards))

        self.every(self._stamp_every, self._check_stamp)
        self.log.info("Поле готово: истощение полок вкл. (кулдаун %.0fс, шаг %.2f, "
                      "пол %.2f, зарядка %s)",
                      self._touch_cd, self._exh_step, self._exh_floor,
                      (f"{c.field_refill_sec:.0f}с" if c.field_refill_sec
                       else f"{c.field_refill_frac:.0%} срока жизни зоны"))

    async def on_stop(self):
        self.log.info("Итог: картин собрано %d, карточек в поле %d, полок %d",
                      self.stamps, len(self._cards), len(self._shelves))

    # ---------- приём сырья ----------

    def _remember(self, card):
        kind = card.get("kind")
        if kind not in CONSUME_KINDS:
            return                        # свою картину и чужое не потребляем
        try:
            seq = int(card["seq"])
            self._cards[seq] = {
                "seq": seq,
                "kind": kind,
                "weight": float(card.get("weight", 0.0)),
                "created_ms": int(card.get("created_ms", 0)),
                "fields": card.get("fields") or {},
                # Авторский срок жизни (TTL-консолидация, 20.10): породивший
                # аналитик кладёт ttl_sec в корень карточки — Поле исполняет
                # авторскую жизнь зоны, а не свои таблички. Фолбэк — TTL_BY_KIND.
                "ttl_sec": card.get("ttl_sec"),
            }
        except (KeyError, TypeError, ValueError):
            return
        # Зона = ПОЛКА (утверждено): ключ — уровень, не номер карточки.
        # SR обновляет зону новой карточкой, а полка — одна и та же.
        if kind in ZONE_KINDS:
            f = self._cards[seq]["fields"]
            try:
                low = float(f["zone_low"])
                high = float(f["zone_high"])
            except (KeyError, TypeError, ValueError):
                return
            if high < low:
                low, high = high, low
            key = round((low + high) / 2.0, 6)
            shelf = self._shelves.get(key)
            if shelf is None:
                self._shelves[key] = {"low": low, "high": high, "touches": 0,
                                      "last_touch": 0.0,
                                      "last_card_ms": self._cards[seq]["created_ms"]}
                shelf = self._shelves[key]
            else:
                shelf["low"], shelf["high"] = low, high
                shelf["last_card_ms"] = self._cards[seq]["created_ms"]
                # новая карточка уровня после смерти полки — полка оживает
                if shelf.get("dead_ms") and \
                        self._cards[seq]["created_ms"] > shelf["dead_ms"]:
                    shelf.pop("dead", None)
                    shelf.pop("dead_ms", None)
            # FVG: новая карточка того же имбаланса ЗАМЕНЯЕТ старую (масса
            # уменьшилась с заполнением) — без дублей в картине
            if kind == "fvg-zone":
                old = shelf.get("fvg_seq")
                if old is not None and old != seq and old in self._cards:
                    self._cards.pop(old, None)
                shelf["fvg_seq"] = seq
        while len(self._cards) > MAX_CARDS:
            self._cards.pop(min(self._cards))   # старейший номер уходит первым

    async def _on_card(self, env):
        card = env.payload
        if card.get("kind") == "tte-state":
            # TTE (утверждено): полки тают у макро-события — лимитники
            # снимают перед штормом. Сила гашения масштабируется
            # доверием календарю (вес карточки Регистратора)
            self._tte = card.get("fields") or {}
            try:
                self._tte_weight = float(card.get("weight", 1.0))
            except (TypeError, ValueError):
                self._tte_weight = 1.0
            return
        if card.get("kind") == "spoof-alert":
            # Спуфинг (утверждено): уровень с фальшивой стеной — полка
            # на этом уровне помечается фальшивой (вес ×0.3, срок вдвое)
            try:
                lvl = round(float((card.get("fields") or {}).get("level", 0.0)), 6)
            except (TypeError, ValueError):
                return
            self._fake[lvl] = time.time() + self.fake_ttl
            self.log.warning("Полка %.6f помечена ФАЛЬШИВОЙ (спуфинг, "
                             "повтор %s)", lvl,
                             (card.get("fields") or {}).get("repeats", 1))
            return
        if card.get("kind") == "level-verdict":
            self._apply_verdict(card)
        self._remember(card)
        await self._check_stamp()         # каждое событие — шанс пересобрать

    def _apply_verdict(self, card):
        """Вердикт «OI + Каскада»: вынос — полка выгорела, зоны сняты."""
        f = card.get("fields") or {}
        if f.get("outcome") != "sweep":
            return
        try:
            level = float(f.get("level", 0.0))
        except (TypeError, ValueError):
            return
        shelf = self._shelves.get(round(level, 6))
        if shelf is not None:
            shelf["dead"] = True
            shelf["dead_ms"] = int(card.get("created_ms", time.time() * 1000))
            self.log.info("Полка %.5f выгорела (вердикт OI) — зоны сняты "
                          "до появления новой карточки уровня", level)

    async def _on_price(self, env):
        self.price = float(env.payload.get("price", 0.0))
        now_s = time.time()
        self._last_price_mono = time.monotonic()
        # Касание полки: цена вошла внутрь зоны; антидребезг по уровню
        # (скальперский кулдаун — утверждено)
        for shelf in self._shelves.values():
            if shelf["low"] <= self.price <= shelf["high"]:
                if now_s - shelf["last_touch"] >= self._touch_cd:
                    shelf["touches"] += 1
                    shelf["last_touch"] = now_s
        # Направление полок (pull) зависит от цены — при её движении
        # картина пересобирается (не чаще раза в 2 сек)
        now_m = time.monotonic()
        if now_m - self._last_price_build >= 2.0:
            self._last_price_build = now_m
            await self._check_stamp()

    def _recharge_shelves(self):
        """Полка заряжается обратно в ТИШИНЕ (утверждено): нет сделок
        внутри диапазона зоны при живом общем рынке — касание снимается.
        Рынок молчит целиком — полки не трогаем (нечестно было бы списывать)."""
        if not self._shelves:
            return
        if time.monotonic() - self._last_price_mono > 10.0:
            return                        # рынок мёртв — не считаем тишину
        now_s = time.time()
        now_ms = int(now_s * 1000)
        need = (self._refill_sec if self._refill_sec > 0
                else max(1.0, self._zone_ttl * self._refill_frac))
        for key, shelf in list(self._shelves.items()):
            if now_ms - shelf["last_card_ms"] > 2 * self._zone_ttl * 1000:
                self._shelves.pop(key, None)      # карточек давно нет — полка ушла
                continue
            if shelf["touches"] <= 0:
                continue
            quiet = now_s - shelf["last_touch"]
            if quiet > need:
                removed = int(quiet // need)
                shelf["touches"] = max(0, shelf["touches"] - removed)
                shelf["last_touch"] = now_s - (quiet % need)

    # ---------- сборка картины ----------

    def _zone_atr(self):
        """Ширина типичной полки Поля — мера «локального ATR».

        Поле не держит ATR (оно работает по зонам, а не по барам), а допуск
        спуфинга без него не имеет смысла: ±0.1 ATR — это «настолько близко,
        чтобы считать тем же уровнем». Берём медианную ширину живых зон.
        """
        widths = []
        for card in self._cards.values():
            f = card.get("fields") or {}
            try:
                widths.append(abs(float(f["zone_high"])
                                  - float(f["zone_low"])))
            except (KeyError, TypeError, ValueError):
                continue
        if not widths:
            return None
        widths.sort()
        return widths[len(widths) // 2]

    def _fake_until(self, level, atr=None):
        """Метка «уровень фальшивый» с ДОПУСКОМ ПО ДИАПАЗОНУ (утверждено).

        Раньше здесь было точное совпадение ключа float: уровень стены берётся
        из СТАКАНА (`spoof-alert.level`), а середина зоны Поля — из СВЕЧНЫХ
        high/low. Эти числа не равны никогда — расхождение в копейках молча
        выключало защиту от спуфинга.

        Теперь уровень фальшив, если он попадает внутрь границ зоны с
        допуском ±0.1 ATR: микро-дрейф цены не должен ломать защиту.
        """
        try:
            lvl = float(level)
        except (TypeError, ValueError):
            return 0.0
        tol = 0.1 * float(atr) if atr and float(atr) > 0 else 0.0
        if tol <= 0:
            tol = max(1e-9, abs(lvl) * 0.0005)   # без ATR — 0.05% от уровня
        now = time.time()
        best = 0.0
        for fake_lvl, until in self._fake.items():
            try:
                fl = float(fake_lvl)
            except (TypeError, ValueError):
                continue
            if abs(fl - lvl) <= tol and until > now:
                best = max(best, float(until))
        return best

    def _fake_factor_at(self, level, atr=None):
        """Множитель веса полки: fake_factor, если уровень фальшивый."""
        return (self.fake_factor
                if self._fake_until(level, atr) > time.time() else 1.0)

    def _active_cards(self, now_ms):
        """Живые карточки с коэффициентом затухания; сгоревшие — вон."""
        active = []
        atr = self._zone_atr()
        for card in list(self._cards.values()):
            ttl = card.get("ttl_sec") or 0
            if not ttl:
                # фолбэк для видов без авторского срока (обратная совместимость)
                if card["kind"] in ("sr-zone", "absorption-barrier"):
                    ttl = self._zone_ttl
                else:
                    ttl = TTL_BY_KIND.get(card["kind"], 15 * 60)
            # Спуфинг: фальшивая полка живёт вдвое меньше (утверждено).
            # Проверка по ДИАПАЗОНУ ±0.1 ATR, а не точным числом (см.
            # _fake_until): уровень из стакана и середина зоны не равны.
            try:
                mid = (float(card["fields"]["zone_low"])
                       + float(card["fields"]["zone_high"])) / 2.0
                if self._fake_until(mid, atr) > now_ms / 1000.0:
                    ttl *= 0.5
            except (KeyError, TypeError, ValueError):
                pass
            age = (now_ms - card["created_ms"]) / 1000.0
            if age >= ttl:
                self._cards.pop(card["seq"], None)
                continue
            decay = max(0.0, 1.0 - age / ttl)
            active.append((card, decay))
        return active

    def _env_mult(self, kind):
        """Множитель среды для карточки (утверждено).
        Матрица селективной громкости (Проводимость, фазы шторма) —
        свой коэффициент на тип карточки. Нет записи для типа —
        legacy-ручки: полки глушит k_shelf, вектора — нейтраль 1.0.
        Нет данных среды (тихий старт) — нейтральная 1.0."""
        env = self.get_last(CONDUCTIVITY)
        if env is None:
            return 1.0
        p = env.payload
        eff = p.get("efficiency") or {}
        if kind in eff:
            try:
                return max(0.1, min(2.0, float(eff[kind])))
            except (TypeError, ValueError):
                return 1.0
        if kind in ZONE_KINDS:      # legacy: шторм глушит полки
            try:
                k = float(p.get("k_shelf", 1.0))
            except (TypeError, ValueError):
                return 1.0
            return max(0.1, min(1.0, k))
        return 1.0

    def _tide_mult(self, card, ttl_sec):
        """Множитель течения для объёмного барьера (утверждено).
        Линза с масштабом, ближайшим к окну жизни барьера, показывает
        уклон рынка; течение против барьера ослабляет его вес, по
        барьеру — усиливает. Не голосовалка: сырая математика линз."""
        env = self.get_last(SURFACE)
        if env is None or card["kind"] != "absorption-barrier":
            return 1.0
        lens = surface_read(env.payload, ttl_sec)
        if lens is None:
            return 1.0
        tide = lens["trend_z"]               # ±3: уклон полотна
        attacker = card["fields"].get("attacker")  # кто бил в барьер
        if attacker == "sell":               # били вниз — течение вниз атакует
            aligned_attack = -tide
        else:                                # били вверх — течение вверх атакует
            aligned_attack = tide
        # течение против барьера — вес вниз; по барьеру — вверх
        mult = 1.0 - TIDE_K * aligned_attack
        return max(0.3, min(TIDE_MAX, mult))

    def _tte_shelf_factor(self):
        """Множитель веса полок по TTE (утверждено): у макро-события
        лимитники снимают — полки тают. Гашение = shelf_damp × громкость
        × близость, масштабировано доверием календарю (вес Регистратора).
        Нет факта / нет событий в окне сжатия — 1.0."""
        if not self._tte or self.shelf_damp <= 0:
            return 1.0
        worst = 0.0
        now_s = time.time()
        compress_start = self.kernel.cfg.tte_compress_start_min * 60.0
        for ev in self._tte.get("events") or []:
            try:
                secs = float(ev.get("minutes_to_event", 1e9)) * 60.0
                w = float(ev.get("weight", 0.0))
            except (TypeError, ValueError):
                continue
            if 0 <= secs < compress_start:
                prox = 1.0 - secs / compress_start
                worst = max(worst, w * prox)
        if worst <= 0:
            return 1.0
        trust_mult = 1.0
        if self._tte_weight is not None:
            trust_mult = max(0.1, min(1.0, self._tte_weight / 0.9))
        return max(0.0, 1.0 - self.shelf_damp * worst * trust_mult)

    def _build_picture(self):
        now_ms = int(time.time() * 1000)
        active = self._active_cards(now_ms)
        # сброс сквозного пропуска тренда (для Интуита, утверждено)
        self._trend_best_w = 0.0
        self._trend_angle = 0.0
        self._trend_decay = 0.0

        zone_parts = []
        mood_num = 0.0
        mood_w = 0.0
        fuel_num = 0.0
        fuel_w = 0.0
        trend_num = 0.0
        trend_w = 0.0
        for card, decay in active:
            w = card["weight"] * decay
            if card["kind"] in ZONE_KINDS:
                f = card["fields"]
                # TTE (утверждено): полки тают у макро-события — сила
                # гашения = shelf_damp × громкость × близость × доверие
                tte_shelf_f = self._tte_shelf_factor()
                try:
                    low = float(f["zone_low"])
                    high = float(f["zone_high"])
                except (KeyError, TypeError, ValueError):
                    continue
                if high < low:
                    low, high = high, low
                tide = self._tide_mult(card, self._zone_ttl)
                env_f = self._env_mult(card["kind"])   # матрица громкости / k_shelf
                key = round((low + high) / 2.0, 6)
                shelf = self._shelves.get(key, {})
                if shelf.get("dead"):
                    continue              # полка выгорела (вердикт OI)
                # Спуфинг: полка на фальшивом уровне — вес режется.
                # Проверка по ДИАПАЗОНУ ±0.1 ATR (тот же хелпер, что и в
                # _active_cards): уровень стены из стакана и середина зоны
                # из свечей расходятся на копейки — точное сравнение float
                # молча выключало защиту.
                fake_f = self._fake_factor_at(key, self._zone_atr())
                if card["kind"] == "fvg-zone":
                    # ИСТОЩЕНИЕ FVG (утверждено): пропорционально заполнению
                    try:
                        exhaust = max(0.0, min(1.0,
                                       float(f.get("mass_left", 1.0))))
                    except (TypeError, ValueError):
                        exhaust = 1.0
                    touches = 0
                else:
                    # истощение касаниями (полка съедается тестами)
                    touches = shelf.get("touches", 0)
                    exhaust = max(self._exh_floor,
                                  1.0 / (1.0 + self._exh_step * max(0, touches - 1)))
                zone_parts.append({"low": low, "high": high,
                                   "w": min(1.0, w * tide * exhaust * env_f
                                            * fake_f * tte_shelf_f),
                                   "tide": tide, "exhaust": exhaust,
                                   "env": env_f, "fake": fake_f,
                                   "tte": tte_shelf_f,
                                   "touches": touches, "own": card["weight"],
                                   "decay": decay,
                                   "kinds": (card["kind"],),
                                   "barrier": card["kind"] == "absorption-barrier"})
            elif card["kind"] == "flow-state":
                try:
                    bias = float(card["fields"].get("direction_bias", 0.5))
                except (TypeError, ValueError):
                    continue
                fuel_num += (bias - 0.5) * 2.0 * w     # −1..1: топливо вверх/вниз
                fuel_w += w
            elif card["kind"] in ("trend-vector", "g-trend"):
                f = card["fields"]
                try:
                    if card["kind"] == "trend-vector":
                        score = float(f.get("score", 0.0))
                    else:
                        # g-trend: настоящее поле G-Channel g_trend (±1);
                        # "value" — формат старых карточек, принимаем оба.
                        score = float(f.get("g_trend",
                                            f.get("value", 0.0)))
                except (TypeError, ValueError):
                    continue
                # Шов с Проводимостью: громкость по типу из матрицы
                # (фазы шторма), нет записи — legacy k_impulse (1.0..2.0)
                env = self.get_last(CONDUCTIVITY)
                mult = None
                if env is not None:
                    eff = env.payload.get("efficiency") or {}
                    if card["kind"] in eff:
                        try:
                            mult = max(0.1, min(2.0, float(eff[card["kind"]])))
                        except (TypeError, ValueError):
                            mult = None
                if mult is None:
                    k_imp = 1.0
                    if env is not None:
                        try:
                            k_imp = max(1.0, min(2.0,
                                        float(env.payload.get("k_impulse", 1.0))))
                        except (TypeError, ValueError):
                            k_imp = 1.0
                    mult = k_imp
                trend_num += score * w * mult
                trend_w += w * mult
                # Сквозной пропуск для Интуита (утверждено): угол/затухание
                # доминирующей тренд-карточки (по весу) в картину
                if w * mult >= self._trend_best_w:
                    self._trend_best_w = w * mult
                    try:
                        self._trend_angle = float(f.get("angle_pct", 0.0))
                    except (TypeError, ValueError):
                        self._trend_angle = 0.0
                    try:
                        self._trend_decay = float(f.get("decay_rate", 0.0))
                    except (TypeError, ValueError):
                        self._trend_decay = 0.0
            elif card["kind"] == "delta-divergence":
                f = card["fields"]
                try:
                    force = float(f.get("force", 0.0))
                    # price_dir теперь число ±1; старые карточки из журнала
                    # несут строку "down" — принимаем оба формата.
                    raw_dir = f.get("price_dir")
                    pdir = -1.0 if (raw_dir == "down" or raw_dir == -1) else 1.0
                except (TypeError, ValueError):
                    continue
                fuel_num += -pdir * force * w          # топливо иссякает против хода
                fuel_w += w
            else:
                # НОВОСТИ 2.0 (Этап 31): у типизированного события голос —
                # ожидаемое направление × сила события; без типа — тон.
                f = card["fields"]
                etype = f.get("event_type") or ""
                expected = f.get("expected_direction")
                if etype and etype != "other" and expected in ("up", "down"):
                    try:
                        strength = max(0.0, min(1.0,
                                       float(f.get("strength", 0.0))))
                    except (TypeError, ValueError):
                        strength = 0.0
                    value = strength * (1.0 if expected == "up" else -1.0)
                else:
                    try:
                        value = float(f.get("sentiment", 0))
                    except (TypeError, ValueError):
                        continue
                mood_num += value * w
                mood_w += w

        # Перекрывающиеся зоны сливаются: силы складываются (потолок 1.0)
        zone_parts.sort(key=lambda z: z["low"])
        merged = []
        for z in zone_parts:
            if merged and z["low"] <= merged[-1]["high"]:
                top = merged[-1]
                top["low"] = min(top["low"], z["low"])
                top["high"] = max(top["high"], z["high"])
                top["w"] = min(1.0, top["w"] + z["w"])
                top["cards"] += 1
                top["tide"] = max(top["tide"], z["tide"])
                top["exhaust"] = min(top["exhaust"], z["exhaust"])
                top["env"] = min(top["env"], z["env"])
                top["fake"] = min(top.get("fake", 1.0), z.get("fake", 1.0))
                top["touches"] = max(top["touches"], z["touches"])
                top["own"] = max(top["own"], z["own"])
                top["decay"] = max(top["decay"], z["decay"])
                top["kinds"] = tuple(dict.fromkeys(
                    top.get("kinds", ()) + z.get("kinds", ())))
                top["barrier"] = top["barrier"] or z["barrier"]
            else:
                merged.append({"low": z["low"], "high": z["high"],
                               "w": z["w"], "cards": 1, "tide": z["tide"],
                               "exhaust": z["exhaust"], "env": z["env"],
                               "fake": z.get("fake", 1.0),
                               "touches": z["touches"], "own": z["own"],
                               "decay": z["decay"],
                               "kinds": tuple(z.get("kinds", ())),
                               "barrier": z["barrier"]})

        for m in merged:
            mid = (m["low"] + m["high"]) / 2.0
            m["pull"] = ("down" if mid < self.price else "up") if self.price else "unknown"
        merged.sort(key=lambda m: m["w"], reverse=True)
        merged = merged[:TOP_ZONES]

        mood_score = round(mood_num / mood_w, 3) if mood_w > 0 else 0.0
        mood_direction = ("up" if mood_score > 0.15
                          else ("down" if mood_score < -0.15 else "flat"))
        fuel_score = round(fuel_num / fuel_w, 3) if fuel_w > 0 else 0.0
        trend_score = round(trend_num / trend_w, 3) if trend_w > 0 else 0.0
        zone_evidence = min(1.0, sum(m["w"] for m in merged) / 2.0)
        # Уверенность: зоны — главные; вектора (настроение/топливо/тренд)
        # добавляют по мере наличия (утверждённые веса)
        conf = 0.55 * zone_evidence
        rest = []
        if mood_w > 0:
            rest.append((0.15, abs(mood_score)))
        if fuel_w > 0:
            rest.append((0.15, abs(fuel_score)))
        if trend_w > 0:
            rest.append((0.15, abs(trend_score)))
        if rest:
            used = 0.55
            for wt, val in rest:
                conf += wt * val
                used += wt
            conf = conf * (0.55 + sum(w for w, _ in rest)) / used if used else conf
        confidence = round(min(1.0, conf), 3)

        mood = {"score": mood_score, "direction": mood_direction,
                "voices": round(mood_w, 3)}
        fuel = {"score": fuel_score, "voices": round(fuel_w, 3)}
        trend = {"score": trend_score, "voices": round(trend_w, 3),
                 "angle_pct": round(getattr(self, "_trend_angle", 0.0), 3),
                 "decay_rate": round(getattr(self, "_trend_decay", 0.0), 3)}
        return merged, mood, fuel, trend, confidence, len(active)

    # ---------- штамп картины (сдвиг картины + устаревание) ----------

    async def _check_stamp(self):
        self._recharge_shelves()      # полки заряжаются в тишине (утверждено)
        # Добор сырья: на старте карточки могли ещё не доехать до базы
        if (not any(c["kind"] in ZONE_KINDS for c in self._cards.values())
                and self._rewarm_left > 0):
            self._rewarm_left -= 1
            keeper = self.kernel.modules.get("keeper")
            if keeper is not None:
                for card in await keeper.get_last_cards(200):
                    self._remember(card)

        now_ms = int(time.time() * 1000)
        # КАРТИНА БЕЗ ЦЕНЫ — ЛОЖНАЯ (06.10): при price=0 направление pull
        # всегда unknown, и потребители (OI-каскад) получали полку без
        # направления — вердикт падал в unclear. Ждём первую цену.
        if self.price <= 0:
            return
        zones, mood, fuel, trend, confidence, sources = self._build_picture()
        stale = (now_ms - self._last_stamp_ms) >= STAMP_REFRESH_SEC * 1000
        if not zones and not stale:
            return    # пустую картину не штампуем, пока не устарела прежняя

        signature = (
            tuple((round(z["low"], 6), round(z["high"], 6),
                   round(z["w"], 3), z["pull"]) for z in zones),
            mood["direction"], fuel["score"],
        )
        if signature == self._signature and not stale:
            return
        # СТРУКТУРНАЯ СИГНАТУРА (06.10): уровни и направления зон —
        # дискретные величины. Их смена (зона родилась/умерла, pull
        # сменился) — СМЫСЛОВОЕ событие: публикуем сразу (антидребезг 1с).
        # Прочее (strength, топливо, настроение — непрерывные числа)
        # штампуется не чаще интервала штампа: старый троттл «всё
        # подряд» резал и структурные обновления — OI-каскад читал
        # устаревшие зоны, тесты 5/14 не дожидались картин (06.10).
        keys_sig = tuple(sorted(
            (round(z["low"], 6), round(z["high"], 6), str(z["pull"]))
            for z in zones))
        structure_changed = keys_sig != self._keys_sig_prev
        if structure_changed:
            if (self._last_publish_ms
                    and now_ms - self._last_publish_ms < 1000):
                return                  # антидребезг структуры: ≥1 раза в сек
        elif (self._last_publish_ms
              and now_ms - self._last_publish_ms
              < self._stamp_every * 1000):
            return                      # прочее: не чаще интервала штампа

        lineage = [f"card:{seq}" for seq in sorted(self._cards)][:40]
        self.publish(DRAFT, {
            "kind": "picture",
            "author": "field-analyst",
            "confidence": confidence,
            "fields": {
                "price": self.price,
                "zones": [{"low": round(z["low"], 8),
                           "high": round(z["high"], 8),
                           "strength": round(z["w"], 3),
                           "pull": z["pull"],
                           "cards": z["cards"],
                           "kinds": list(z.get("kinds", ())),
                           "tide_mult": round(z["tide"], 3),
                           "barrier": z["barrier"],
                           "factors": {"own": round(z["own"], 3),
                                       "decay": round(z["decay"], 3),
                                       "tide": round(z["tide"], 3),
                                       "exhaust": round(z["exhaust"], 3),
                                       "env": round(z["env"], 3),
                                       "fake": round(z.get("fake", 1.0), 3),
                                       "touches": z["touches"]}} for z in zones],
                "mood": mood,
                "fuel": fuel,
                "trend": trend,
                "sources": sources,
                "valid_until_ms": now_ms + STAMP_REFRESH_SEC * 2000,
            },
            "lineage": lineage,
        })
        self._signature = signature
        self._keys_sig_prev = keys_sig
        self._last_stamp_ms = now_ms
        self._last_publish_ms = now_ms
        self.stamps += 1
        if self.stamps == 1 or self.stamps % 10 == 0:
            self.log.info("Картина №%d: магнитов %d, настроение %s (%.2f), "
                          "уверенность %.2f",
                          self.stamps, len(zones), mood["direction"],
                          mood["score"], confidence)
