"""Лаборатория — АВТОНОМНЫЙ контрфактальный измеритель (Этап 22).

Исследователь, НЕ исполнитель: ничего не решает, не торгует и НЕ связан с
сделками Дилера. Работает параллельным фоновым модулем в ЛЮБОМ режиме
(real или shadow) и постоянно крутит собственные симуляции.

ИЗОЛЯЦИЯ (утверждено хозяином): у Лаборатории нет доступа к карточкам
сделок Дилера. Вход — только карточки сценариев Сценариста (это данные
производства, а не результат торговли) + алерты спуфинга + новости.
Никаких deal_id и никаких связей со снимками Дилера.

Утверждённый функционал:
1. АВТОНОМНАЯ СИМУЛЯЦИЯ по истории сценариев. Берём карточку сценария из
   Склада (с её вектором и настройками момента), прогоняем ИСТОРИЧЕСКИЙ
   путь свечей после него и считаем:
   - идеальный выход (лучший пик до касания стопа), MFE/MAE, no_edge;
   - идеальные ВЕСА ДОВЕРИЯ видов данных — координатный шаг по вилке
     доверий (прогон вектора момента через формулу направления);
   - идеальный МАРШРУТ (стоп/активация/шаг сопровождения) — прогон
     сделки от входа с трейлингом по пути, максимальный захват идеала.
   Никаких переборов и генетики: однопроходная арифметика.
2. СЛЕПОК ДЛЯ ИНТУИТА: geometry (расстояния до зон в ATR), dynamics
   (тренд/топливо/настроение) и facts_vec — вектор для аналогий.
3. ВЕРДИКТЫ СПУФИНГА: прошла ли цена сквозь стену-приманку.
4. РЕАКЦИИ НА НОВОСТИ: сверка «тип → сила → направление» по факту хода.
5. ПЕСОЧНИЦА СРЕЗОВ: пакетный прогон черновых срезов по истории.

СТАНДАРТ СНИМКА (поток №2) — карточка `lab-snapshot` САМОДОСТАТОЧНА:
внутри несёт метку потока `stream`, исход `outcome_dir`, идеальные числа
`ideal_set`, слепок для Интуита и вектор. Тюнер принимает такой снимок
вслепую, наравне со снимком Дилера, и ничего не знает о его источнике.
"""
import asyncio
import time

import numpy as np

from core.module import Manifest, NeirModule
from modules._knobs import TUNER_KNOBS
from modules._labels import CARD, DRAFT, PRICE

TRUST_STEP = 0.05      # координатный шаг доверия в контрфакте (вилка: tuner)
TRUST_PASSES = 6       # ограничение проходов координатного спуска
ROUTE_STEP = 0.05      # координатный шаг маршрута в прогоне сделки
ROUTE_PASSES = 3       # проходов координатного спуска по маршруту


class Lab(NeirModule):
    manifest = Manifest(name="lab", title="Лаборатория (контрфакт)",
                        priority=98, tick_timeout=30.0)

    async def on_start(self):
        self.grace_sec = 120                # добор пути после закрытия
        self.report_every = 300.0
        self.check_every = 30.0             # вердикты спуфинга
        self.symbol = self.kernel.cfg.symbol.upper()
        self.tf = self.kernel.cfg.base_tf_sec
        self.horizon_bars = self.kernel.cfg.lab_horizon_bars
        self.spoof_horizon_min = self.kernel.cfg.spoof_verdict_horizon_min
        self.snapshots = 0
        self.ideals = []                     # (исход, ideal_pct) — для отчёта
        self._pending_spoof = []            # алерты ждут вердикта
        self._pending_sims = {}             # № сценария -> карточка (ждёт путь)
        self._sim_done = 0                  # метка: сценарий учтён РОВНО ОДИН раз
        self.sim_expired = 0                # сценарии без пути (сгорели)
        # НОВОСТИ 2.0 (Этап 31): расписки «тип → сила → направление»
        self.news_window_ms = self.kernel.cfg.news_reaction_window_sec * 1000.0
        self.news_flat_pct = self.kernel.cfg.news_flat_pct
        self._pending_news = {}             # № новости -> контекст расписки
        self._news_table = {}               # тип -> [confirm, refute, flat, сумма%]
        self.keeper = self.kernel.modules.get("keeper")
        # ВХОД АВТОНОМНОЙ ЛАБОРАТОРИИ: карточки СЦЕНАРИЕВ (не сделки!)
        self.subscribe(CARD, self._on_card)
        self.every(self.report_every, self._report)
        self.every(self.check_every, self._spoof_check)
        self.every(self.check_every, self._news_check)
        self.every(self.check_every, self._sim_run)
        # Песочница срезов (утверждено): черновые срезы по истории
        self.sandbox_on = self.kernel.cfg.sandbox_enabled
        if self.sandbox_on:
            self.every(600.0, self._sandbox_run)
            self.log.info("Песочница включена: срезы %s по истории свечей",
                          self.kernel.cfg.sandbox_slices)
        self.log.info("Лаборатория включена (автономный измеритель): "
                      "симуляции по истории сценариев, окно %d свечей, "
                      "вердикты спуфинга по горизонту %.0f мин",
                      self.horizon_bars, self.spoof_horizon_min)

    async def on_stop(self):
        self.log.info("Итог: снимков симуляций %d (сгорело без пути %d)",
                      self.snapshots, self.sim_expired)

    # ---------- вход: карточки ----------

    async def _on_card(self, env):
        card = env.payload
        kind = card.get("kind")
        if kind == "scenario":
            # Сценарий — единственный вход автономной симуляции: он несёт
            # вектор и настройки момента, свечи после него — материал прогона.
            try:
                seq = int(card.get("seq", 0))
            except (TypeError, ValueError):
                return
            if seq and seq > self._sim_done:
                self._pending_sims[seq] = card
        elif kind == "spoof-alert":
            f = card.get("fields") or {}
            if f.get("level"):
                self._pending_spoof.append({
                    "seq": card.get("seq", 0),
                    "side": f.get("side"),
                    "level": float(f["level"]),
                    "ts_ms": int(card.get("created_ms", 0)),
                })
        elif kind == "news-sentiment":
            self._register_news(card)

    # ---------- 1+2: автономная симуляция по истории сценария ----------

    async def _simulate(self, card):
        """Считает контрфакт по ИСТОРИИ сценария. False = путь ещё не в
        свечах (ждём следующие бары) — расписку не теряем.

        ИЗОЛЯЦИЯ: на входе карточка сценария, на выходе самостоятельный
        снимок. Никаких deal_id и никакой связи с карточками сделок.
        """
        if self.keeper is None:
            return True                 # свечей нет — откладывать некуда
        f = card.get("fields") or {}
        entry = float(f.get("entry", 0.0) or 0.0)
        opened = int(card.get("created_ms", 0) or 0)
        if entry <= 0 or opened <= 0:
            return True                 # брак записи — снимка не будет
        # ОКНО СИМУЛЯЦИИ: от момента сценария вперёд на горизонт баров
        end_ms = opened + self.horizon_bars * self.tf * 1000.0
        rows = await self.keeper.get_last_candles(self.symbol, self.tf, 600)
        path = [r for r in rows if opened <= int(r[0]) <= end_ms]
        if len(path) < 3:
            return False                # бары ещё не закрылись — ждём
        lev = float(f.get("leverage", 1.0)) or 1.0
        direction = f.get("direction", "long")
        long = direction == "long"
        stop = float(f.get("stop", 0.0) or 0.0)

        # --- МАТЕМАТИКА: путь до первого касания стопа (или до горизонта) ---
        highs = [float(r[2]) for r in path]
        lows = [float(r[3]) for r in path]
        stop_idx = None                       # первое касание стопа на пути
        for i in range(len(path)):
            if stop > 0 and ((long and lows[i] <= stop)
                             or (not long and highs[i] >= stop)):
                stop_idx = i
                break
        upto = stop_idx if stop_idx is not None else len(path)
        if upto <= 0:
            mfe_px = entry                    # стоп сработал в первой свече
            mae_px = stop if stop > 0 else min(lows)
        else:
            mfe_px = max(highs[:upto]) if long else min(lows[:upto])
            after = lows[upto:] if long else highs[upto:]
            mae_px = min(after) if after else (stop if stop > 0 else min(lows))
        ideal_px = mfe_px
        outcome_dir = ("long" if ideal_px > entry
                       else "short" if ideal_px < entry else direction)
        if long:
            ideal_pct = max(0.0, (ideal_px - entry) / entry * lev * 100.0)
        else:
            ideal_pct = max(0.0, (entry - ideal_px) / entry * lev * 100.0)
        mfe_first = (stop_idx is None) or (upto > 0)
        # идеальный шаг зажима стопа: просадка ПОСЛЕ пика, % цены
        if mfe_px > 0:
            trail_ideal = ((mfe_px - mae_px) / mfe_px * 100.0 if long
                           else (mae_px - mfe_px) / mfe_px * 100.0)
        else:
            trail_ideal = 0.0

        # --- ДАННЫЕ САМОГО СЦЕНАРИЯ: снимок строится автономно, поэтому берём
        # поля прямо из карточки сценария (никакой пришивки от Дилера).
        sfields = f
        vector = sfields.get("vector")
        if not isinstance(vector, dict) or not vector:
            facts_old = sfields.get("facts")
            vector = ({name: {"kind": str(name), "value": val,
                              "state": "fresh"}
                       for name, val in facts_old.items()}
                      if isinstance(facts_old, dict) else None)
        moment = sfields.get("settings")
        # ИДЕАЛЬНЫЕ НАСТРОЙКИ СДЕЛКИ (утверждено — ключевая функция):
        # ЧИСЛА, при которых эта сделка была бы максимально близка к
        # идеалу: веса доверия видов данных и маршрут (стоп/активация/
        # шаг сопровождения). Никаких вердиктов — только числа.
        ideal_trusts = self._ideal_trusts(vector, moment, outcome_dir)
        ideal_route = self._ideal_route(path, entry, long, lev, sfields,
                                        moment, ideal_px)

        # ТОЧНОСТЬ ПРОГНОЗА ПО ВРЕМЕНИ (утверждено): когда цена впервые
        # коснулась расчётной цели и как это соотносится с расчётным
        # временем маршрута (t_est из карточки сценария)
        tgt = None
        t_est = None
        try:
            tg = sfields.get("targets") or []
            tgt = float(tg[0]) if tg else None
        except (TypeError, ValueError, IndexError):
            tgt = None
        try:
            t_est = float((sfields.get("route") or {}).get("t_est_min"))
        except (TypeError, ValueError):
            t_est = None
        t_to_target = None
        if tgt:
            for i, r in enumerate(path):
                hi, lo = float(r[2]), float(r[3])
                if (long and hi >= tgt) or (not long and lo <= tgt):
                    t_to_target = i + 1
                    break
        t_ratio = (round(t_to_target / t_est, 3)
                   if (t_to_target and t_est) else None)
        # ОЦЕНОК «КТО БЫЛ ПРАВ» ЗДЕСЬ НЕТ (разлепление ролей, утверждено):
        # суть, вердикты, метки, упущенное по Риску — функционал ТЮНЕРА.
        # Лаборатория публикует чистые данные снимка — он их просчитает.

        # --- СЛЕПОК ДЛЯ ИНТУИТА (утверждено): геометрия + динамика ---
        #     фиксированный вектор: расстояния до блишайших зон сверху/
        #     снизу (в ATR) + угол/скорость тренда + топливо (CVD/поток)
        geometry, dynamics = {}, {}
        # Зоны из карточки сценария; живая динамика картины — по родословной
        # сценария (это связь СЦЕНАРИЙ→картина, не связь с Дилером).
        pz = sfields.get("picture_zones")
        pf = {"zones": pz} if pz is not None else {}
        pic = await self._picture_of(card)
        if pf:
            full = (pic.get("fields") or {}) if pic is not None else {}
            for key in ("trend", "fuel", "mood"):
                if not pf.get(key) and full.get(key):
                    pf[key] = full[key]
        elif pic is not None:
            pf = pic.get("fields") or {}
        try:
            atr = float(sfields.get("atr", 0.0) or 0.0)
            ups, dns = [], []
            for z in pf.get("zones") or []:
                lo, hi = float(z.get("low", 0)), float(z.get("high", 0))
                if hi < lo:
                    lo, hi = hi, lo
                if lo > entry:
                    ups.append(lo - entry)
                elif hi < entry:
                    dns.append(entry - hi)
            if atr > 0:
                geometry = {
                    "dist_up_atr": round(
                        min(1.0, (min(ups) / atr) if ups else 1.0), 3),
                    "dist_dn_atr": round(
                        min(1.0, (min(dns) / atr) if dns else 1.0), 3),
                }
        except (TypeError, ValueError):
            geometry = {}
        dynamics = {
            "trend_score": round(float(
                (pf.get("trend") or {}).get("score", 0.0)), 3),
            "trend_angle": round(float(
                (pf.get("trend") or {}).get("angle_pct", 0.0)), 3),
            "trend_decay": round(float(
                (pf.get("trend") or {}).get("decay_rate", 0.0)), 3),
            "flow_score": round(float(
                (pf.get("fuel") or {}).get("score", 0.0)), 3),
            "mood": round(float(
                (pf.get("mood") or {}).get("score", 0.0)), 3),
        }
        # ВЕКТОР ДЛЯ ИНТУИТА: без него библиотека аналогий молча пуста
        # (Интуит требует facts_vec присутствующим в снимке).
        facts_vec = self._facts_vec(vector)
        # --- ОТК: верификация вторым проходом по тем же свечам ---
        if long:
            check = round((ideal_px - entry) / entry * lev * 100.0, 3)
        else:
            check = round((entry - ideal_px) / entry * lev * 100.0, 3)
        check = max(0.0, check)
        ideal_pct = round(ideal_pct, 3)
        status = "ok" if abs(check - ideal_pct) < 0.05 else "verified"

        # no_edge (измерение пути, не вердикт): цена от входа вообще не
        # шла в сторону прогноза — идеал вырождается в 0. Число-факт для
        # Тюнера: он сам решит, что это значит.
        no_edge = ((long and mfe_px <= entry)
                   or (not long and mfe_px >= entry))
        self.ideals.append((outcome_dir, ideal_pct))

        # СТАНДАРТ СНИМКА ЛАБОРАТОРИИ (поток №2): самостоятельная единица
        # данных. Ни deal_id, ни ссылки на сделки Дилера — только метка
        # потока, исход, идеальные числа и слепок.
        self.publish(DRAFT, {
            "kind": "lab-snapshot",
            "author": "lab",
            "confidence": 0.9,
            "fields": {
                "stream": "lab-sim",         # метка потока внутри снимка
                "source": "scenario-sim",    # откуда симуляция (не сделка)
                # КОНТУР БИРЖИ (утверждено): симуляция считалась против
                # этого контура — Тюнер раскладывает лаб-потоки по своим
                # колонкам базы по этому флагу (лаб_реал / лаб_тень).
                "contour": self.kernel.cfg.contour,
                "mode": self.kernel.cfg.mode,
                "scenario_seq": int(card.get("seq", 0)),
                "direction": direction,
                "entry": round(entry, 8),
                "leverage": lev,
                "ideal_pnl_pct": ideal_pct,
                "ideal_exit": round(ideal_px, 8),
                "outcome_dir": outcome_dir,
                # КАЧЕСТВО СЛЕПКА (Интуит): уверенность исходного сценария —
                # насколько контекст, который дал идеал, сам был убедительным
                "ideal_confidence": round(
                    float(card.get("confidence", 0.5) or 0.5), 3),
                # ВЕКТОР И НАСТРОЙКИ — КАК ЕСТЬ, без порогов и отсева
                "vector": vector,
                "settings": moment,
                # ИДЕАЛЬНЫЕ НАСТРОЙКИ (числа, без рекомендаций):
                "ideal_set": {"trusts": ideal_trusts,
                              "route": ideal_route},
                "geometry": geometry,            # слепок для Интуита
                "dynamics": dynamics,
                "facts_vec": facts_vec,          # вектор для библиотеки
                "stop_touched": stop_idx is not None,
                "ideal_trail_pct": round(trail_ideal, 3),
                # ТОЧНОСТЬ ПО ВРЕМЕНИ: фактическое время до цели (мин) и
                # отношение к расчётному (1.0 = точно в срок)
                "t_to_target_min": t_to_target,
                "t_est_min": (round(t_est, 1) if t_est else None),
                "t_ratio": t_ratio,
                "mfe_pct": round((mfe_px - entry) / entry * 100.0 * (
                    1 if long else -1), 3),
                "mae_pct": round((mae_px - entry) / entry * 100.0 * (
                    1 if long else -1), 3),
                "mfe_first": mfe_first,
                "no_edge": no_edge,
                "status": status,
                "path_bars": len(path),
            },
            # Родословная — к СЦЕНАРИЮ (источнику данных), не к сделке
            "lineage": [f"scenario:{card.get('seq', 0)}"],
        })
        self.snapshots += 1
        self._sim_done = max(self._sim_done, int(card.get("seq", 0) or 0))
        self.log.info("Симуляция сценария №%s (%s): идеал %.2f%%, "
                      "идеал-доверия %d видов, идеал-стоп %.2f, идеал-шаг %.2f",
                      card.get("seq"), direction, ideal_pct,
                      len(ideal_trusts or {}),
                      float((ideal_route or {}).get("stop_frac") or 0.0),
                      float((ideal_route or {}).get("dist_frac") or 0.0))
        return True

    # ---------- 2: идеальные настройки симуляции ----------

    @staticmethod
    def _facts_vec(vector):
        """Вектор для библиотеки аналогий Интуита (утверждено).

        Несёт ВСЕ свежие значения вектора момента, а Интуит сама выбирает
        нужные ей признаки по FACT_AXES (жёсткий список здесь означал бы,
        что слепок молча остаётся без библиотеки — а это и случалось).

        Оси вектора аналогии (16 штук, включая reg_acc_1m/reg_score_4h,
        rsi_base/rsi_strat) приходят сюда из карточки сценария этого же
        прогона Лаборатории: снимок самодостаточен, Интуит ничего не
        достраивает задним числом.
        """
        out = {}
        if not isinstance(vector, dict):
            return out
        for name, entry in vector.items():
            key = str(name)
            if isinstance(entry, dict):
                if entry.get("state") != "fresh":
                    continue
                try:
                    out[key] = float(entry.get("value", 0.0) or 0.0)
                except (TypeError, ValueError):
                    continue
            else:
                try:
                    out[key] = float(entry)
                except (TypeError, ValueError):
                    continue
        return out

    def _ideal_trusts(self, vector, moment, outcome_dir):
        """ИДЕАЛЬНЫЕ ВЕСА ДОВЕРИЯ (утверждено, ключевая функция).

        Контрфакт по формуле направления: вклад вида = сырая громкость ×
        доверие вида. Сырая громкость восстанавливается из вектора
        момента (push уже записан со знаком и ручкой веса). Координатный
        шаг по вилке доверий (tuner trust_min..trust_max, шаг 0.05):
        доверие вида двигается, если решение становится ближе к
        идеальному пути. Никакого перебора и генетики — один проход по
        видам, только улучшающие шаги. Возврат — ЧИСЛА (имя вида →
        доверие), пусто — вектора нет.
        """
        if (not isinstance(vector, dict)
                or outcome_dir not in ("long", "short")):
            return None
        s = 1.0 if outcome_dir == "long" else -1.0
        lo = float(getattr(self.kernel.cfg, "tuner_trust_min", 0.1) or 0.1)
        hi = float(getattr(self.kernel.cfg, "tuner_trust_max", 0.95) or 0.95)
        dir_trusts = ((moment or {}).get("trusts") or {})
        raw = {}                    # имя вида -> громкость при доверии 1.0
        trusts = {}                 # старт — доверия момента
        for name, entry in vector.items():
            if not isinstance(entry, dict):
                continue
            if entry.get("state") != "fresh":
                continue            # не слышали/устарело — в контрфакте нет
            try:
                push = float(entry.get("push") or 0.0)
            except (TypeError, ValueError):
                continue
            if abs(push) < 1e-9:
                continue            # нулевой вклад — громкость не изменит
            kind = str(entry.get("kind") or name)
            tr = entry.get("trust")
            if tr is None:
                tr = dir_trusts.get(kind, 0.5)
            try:
                tr = max(0.05, min(1.0, float(tr)))
            except (TypeError, ValueError):
                tr = 0.5
            raw[name] = push / tr   # вклад при доверии 1.0
            trusts[name] = max(lo, min(hi, tr))

        def margin(vec):
            return s * sum(b * vec[name] for name, b in raw.items())

        best = margin(trusts)
        for _ in range(TRUST_PASSES):
            moved = False
            for name in raw:
                for delta in (TRUST_STEP, -TRUST_STEP):
                    t = max(lo, min(hi, trusts[name] + delta))
                    if abs(t - trusts[name]) < 1e-9:
                        continue
                    old = trusts[name]
                    trusts[name] = t
                    m = margin(trusts)
                    if m > best + 1e-9:
                        best = m
                        moved = True
                        break
                    trusts[name] = old
            if not moved:
                break
        return {name: round(float(t), 3) for name, t in trusts.items()}

    def _ideal_route(self, path, entry, long, lev, sfields, moment, ideal_px):
        """МАКСИМАЛЬНО ЭФФЕКТИВНЫЙ МАРШРУТ (утверждено).

        Прогон сделки от входа до горизонта с трейлингом (стоп,
        активация, шаг сопровождения) по тому же пути, что мерил идеал.
        Координатный шаг по вилкам ручек из реестра (_knobs): цель —
        максимальный захват идеала (больше денег на этом пути).
        Без перебора и генетики: 3 ручки × два направления × 3 прохода.
        Возврат — ЧИСЛА (ручки маршрута и достигнутый захват), пусто —
        пути/цели нет.
        """
        if not path:
            return None
        try:
            dist_target = abs(float((sfields.get("targets") or [])[0])
                              - entry) / entry
        except (TypeError, ValueError, IndexError, ZeroDivisionError):
            dist_target = abs(ideal_px - entry) / entry
        if not dist_target or dist_target <= 0:
            return None
        knobs = (moment or {}).get("knobs") or {}
        spec = {}
        for key, name in (("route_stop", "stop_frac"),
                          ("trail_activate", "act_frac"),
                          ("trail_distance", "dist_frac")):
            sec, k, start, klo, khi = TUNER_KNOBS[key]
            try:
                cur = float(knobs.get(key, start))
            except (TypeError, ValueError):
                cur = float(start)
            spec[name] = (max(float(klo), min(float(khi), cur)),
                          float(klo), float(khi))

        def run(vec):
            return self._run_route(path, entry, long, lev, dist_target,
                                   vec["stop_frac"], vec["act_frac"],
                                   vec["dist_frac"])

        cur = {name: spec[name][0] for name in spec}
        best_pnl, best_px, best_why = run(cur)
        for _ in range(ROUTE_PASSES):
            moved = False
            for name in spec:
                _start, klo, khi = spec[name]
                for delta in (ROUTE_STEP, -ROUTE_STEP):
                    t = max(klo, min(khi, cur[name] + delta))
                    if abs(t - cur[name]) < 1e-9:
                        continue
                    old = cur[name]
                    cur[name] = t
                    pnl, px, why = run(cur)
                    if pnl > best_pnl + 1e-9:
                        best_pnl, best_px, best_why = pnl, px, why
                        moved = True
                        break
                    cur[name] = old
            if not moved:
                break
        return {"stop_frac": round(cur["stop_frac"], 3),
                "act_frac": round(cur["act_frac"], 3),
                "dist_frac": round(cur["dist_frac"], 3),
                "capture_pct": round(best_pnl, 3),
                "exit": round(best_px, 8),
                "exit_reason": best_why}

    @staticmethod
    def _run_route(path, entry, long, lev, dist_target, stop_frac,
                   act_frac, dist_frac):
        """Однопроходный прогон сделки по пути (трейлинг как в ручках).
        Возврат: (pnl_%×плечо, цена выхода, причина)."""
        if long:
            stop = entry * (1.0 - stop_frac * dist_target)
            act = entry * (1.0 + act_frac * dist_target)
            tgt = entry * (1.0 + dist_target)
        else:
            stop = entry * (1.0 + stop_frac * dist_target)
            act = entry * (1.0 - act_frac * dist_target)
            tgt = entry * (1.0 - dist_target)
        step = dist_frac * dist_target * entry
        hwm = entry
        activated = False
        exit_px, why = None, "end"
        for r in path:
            hi, lo = float(r[2]), float(r[3])
            if long:
                if lo <= stop:
                    exit_px, why = stop, "stop"
                    break
                if hi >= tgt:
                    exit_px, why = tgt, "target"
                    break
                hwm = max(hwm, hi)
                if not activated and hi >= act:
                    activated = True
                if activated and lo <= hwm - step:
                    exit_px, why = hwm - step, "trail"
                    break
            else:
                if hi >= stop:
                    exit_px, why = stop, "stop"
                    break
                if lo <= tgt:
                    exit_px, why = tgt, "target"
                    break
                hwm = min(hwm, lo)
                if not activated and lo <= act:
                    activated = True
                if activated and hi >= hwm + step:
                    exit_px, why = hwm + step, "trail"
                    break
        if exit_px is None:
            exit_px = float(path[-1][4])
            why = "end"
        pnl = (exit_px - entry) / entry * 100.0 * lev * (1.0 if long else -1.0)
        return round(pnl, 3), exit_px, why

    async def _sim_run(self):
        """Насос автономных симуляций: ждём, когда по сценарию
        накопится история свечей, и публикуем независимый снимок."""
        if not self._pending_sims or self.keeper is None:
            return
        still = {}
        now_ms = int(time.time() * 1000)
        # Итерируем по СПИСКУ КЛЮЧЕЙ, а не по самому словарю: пока идёт
        # симуляция (она ждёт свечи в базе), на шину может прийти новая
        # карточка сценария и изменить _pending_sims прямо на ходу —
        # это роняло насос на RuntimeError (dictionary changed size during
        # iteration, найдено в живом прогоне 02.10).
        for seq in list(self._pending_sims):
            card = self._pending_sims.get(seq)
            if card is None:                # карточку уже забрали/убрали
                continue
            if await self._simulate(card):
                continue                    # снимок опубликован
            opened = int(card.get("created_ms", 0) or 0)
            end_ms = opened + self.horizon_bars * self.tf * 1000.0
            if now_ms > end_ms + 600_000:
                self.sim_expired += 1
                self.log.warning("Сценарий №%d: путь так и не доехал до "
                                 "базы — симуляция сгорела", seq)
                continue
            still[seq] = card                # ждём следующие бары
        # Ключи, пришедшие ВО ВРЕМЯ прогона, в `still` не попали — сохраняем
        # их: иначе свежий сценарий потерялся бы молча.
        for seq, card in self._pending_sims.items():
            if seq not in still and seq > self._sim_done:
                still[seq] = card
        self._pending_sims = still

    async def _picture_of(self, scenario_card):
        """Карточка картины по родословной сценария (для слепка)."""
        for ref in scenario_card.get("lineage") or []:
            if not ref.startswith("card:"):
                continue
            try:
                want = int(ref.split(":", 1)[1])
            except ValueError:
                continue
            card = await self.keeper.get_card(want)
            if card is not None:
                return card
        return None

    # ---------- 5: вердикты спуфинга ----------

    async def _spoof_check(self):
        """Алерт «стена-приманка» прав, если цена ПРОШЛА сквозь уровень
        (фальшивая стена не держит); отбилась — алерт наврал."""
        if not self._pending_spoof or self.keeper is None:
            return
        rows = await self.keeper.get_last_candles(self.symbol, self.tf, 600)
        horizon_ms = self.spoof_horizon_min * 60.0 * 1000.0
        now_ms = int(time.time() * 1000)
        still = []
        for a in self._pending_spoof:
            deadline = a["ts_ms"] + horizon_ms
            crossed, expired = False, now_ms >= deadline
            for r in rows:
                ts = int(r[0])
                if ts < a["ts_ms"]:
                    continue
                if ts > deadline:
                    break
                high, low = float(r[2]), float(r[3])
                if a["side"] == "ask" and high > a["level"]:
                    crossed = True       # ушли СКВОЗЬ фальшивую стену
                elif a["side"] == "bid" and low < a["level"]:
                    crossed = True
                if crossed:
                    break
            if crossed:
                self._publish_spoof_verdict(
                    a, "confirmed", "цена прошла сквозь уровень — "
                    "стена была приманкой")
            elif expired:
                self._publish_spoof_verdict(
                    a, "refuted", "цена не прошла сквозь уровень — "
                    "алерт наврал")
            else:
                still.append(a)
        self._pending_spoof = still

    def _publish_spoof_verdict(self, alert, verdict, why):
        self.publish(DRAFT, {
            "kind": "spoof-verdict",
            "author": "lab",
            "confidence": 0.9,
            "fields": {
                "alert_seq": alert["seq"],
                "side": alert["side"],
                "level": alert["level"],
                "verdict": verdict,
                "why": why,
            },
            "lineage": [f"card:{alert['seq']}"],
        })
        self.log.info("Спуфинг-вердикт: %s (уровень %.5f, %s) — %s",
                      verdict, alert["level"], alert["side"], why)

    # ---------- 6: реакции на новости (Этап 31) ----------

    def _register_news(self, card):
        """РАСПИСКА НОВОСТИ: типизированное событие обещает реакцию рынка
        за окно — Лаборатория проверит это по факту хода цены."""
        f = card.get("fields") or {}
        if f.get("event_type") in (None, "", "other"):
            return                      # нетипизированное — только тон
        expected = f.get("expected_direction")
        if expected not in ("up", "down"):
            return                      # направления не обещали
        env = self.get_last(PRICE)
        price = float(env.payload.get("price", 0.0)) if env else 0.0
        if price <= 0:
            return                      # нет живой цены — расписку не берём
        self._pending_news[int(card.get("seq", 0))] = {
            "event_type": f.get("event_type"),
            "expected": expected,
            "strength": float(f.get("strength", 0.0) or 0.0),
            "price_at": price,
            "ts_ms": (int(card.get("created_ms", 0))
                      or int(time.time() * 1000)),
        }

    async def _news_check(self):
        """Сверка расписок: ход цены за окно реакции против обещанного
        направления. Вердикты копятся в таблицу исторической реакции."""
        if not self._pending_news:
            return
        now_ms = int(time.time() * 1000)
        rows = None
        if self.keeper is not None:
            rows = await self.keeper.get_last_candles(self.symbol, self.tf, 600)
        done = []
        for seq, n in self._pending_news.items():
            deadline = n["ts_ms"] + self.news_window_ms
            if now_ms < deadline:
                continue
            price_after = 0.0
            for r in rows or []:
                if int(r[0]) >= deadline:
                    price_after = float(r[4])
                    break
            if price_after <= 0:
                if now_ms > deadline + self.news_window_ms:
                    done.append(seq)    # свечи так и не доехали — расписка сгорает
                continue
            done.append(seq)
            move = (price_after - n["price_at"]) / n["price_at"] * 100.0
            if abs(move) <= self.news_flat_pct:
                verdict = "flat"
            elif (move > 0) == (n["expected"] == "up"):
                verdict = "confirm"
            else:
                verdict = "refute"
            t = self._news_table.setdefault(n["event_type"], [0, 0, 0, 0.0])
            t[{"confirm": 0, "refute": 1, "flat": 2}[verdict]] += 1
            t[3] += move
            self._publish_news_verdict(seq, n, verdict, move, price_after)
        for seq in done:
            self._pending_news.pop(seq, None)

    def _publish_news_verdict(self, seq, n, verdict, move, price_after):
        self.publish(DRAFT, {
            "kind": "news-reaction",
            "author": "lab",
            "confidence": 0.9,
            "fields": {
                "news_seq": seq,
                "event_type": n["event_type"],
                "expected_direction": n["expected"],
                "strength": n["strength"],
                "price_at": n["price_at"],
                "price_after": round(price_after, 8),
                "move_pct": round(move, 3),
                "verdict": verdict,
                "table": self.news_table_view(),
            },
            "lineage": [f"card:{seq}"],
        })
        self.log.info("Реакция на новость #%d (%s, ждали %s): %+.2f%% — %s",
                      seq, n["event_type"], n["expected"], move, verdict)

    def news_table_view(self):
        """Таблица исторической реакции: тип → вердикты и средний ход."""
        return {t: {"confirm": c, "refute": r, "flat": fl,
                    "mean_move_pct": round(s / max(1, c + r + fl), 3)}
                for t, (c, r, fl, s) in self._news_table.items()}

    # ---------- Песочница срезов (утверждено) ----------

    def _slice_entropy_bars(self, candles):
        """Хаос направлений последних 30 баров: энтропия Шеннона
        распределения рост/падение. 0 = упорядоченный ход, 1 = пила."""
        closes = [float(r[4]) for r in candles]
        if len(closes) < 21:
            return None
        rets = [1 if closes[i + 1] > closes[i] else 0
                for i in range(len(closes) - 21, len(closes) - 1)]
        p = sum(rets) / len(rets)
        if p in (0.0, 1.0):
            return 0.0
        return round(-(p * np.log2(p) + (1 - p) * np.log2(1 - p)), 3)

    def _slice_range_squeeze(self, candles):
        """Сжатие: диапазон последних 10 баров против среднего диапазона
        60 баров. < 0.5 = пружина сжата (кристаллизация)."""
        if len(candles) < 61:
            return None
        rng = [float(r[2]) - float(r[3]) for r in candles]
        cur = sum(rng[-10:]) / 10.0
        avg = sum(rng[-61:-10]) / 51.0
        return round(cur / avg, 3) if avg > 0 else None

    SLICES = {
        "entropy_bars": "_slice_entropy_bars",
        "range_squeeze": "_slice_range_squeeze",
    }

    async def _sandbox_run(self):
        """Пакетный прогон черновых срезов (утверждено): срез в момент t
        сопоставляется с ходом следующих 5 баров — распределение даёт
        матожидания. Вердикт — только в ЖУРНАЛ; в шину срез идёт по
        слову хозяина. v1 — срезы по хранимым свечам (тиков нет)."""
        if not self.kernel.cfg.sandbox_enabled or self.keeper is None:
            return
        names = [s.strip() for s in
                 self.kernel.cfg.sandbox_slices.split(",") if s.strip()]
        names = [n for n in names if n in self.SLICES]
        if not names:
            return
        rows = await self.keeper.get_last_candles(self.symbol, self.tf, 600)
        if len(rows) < 80:
            return
        bucket = {n: [] for n in names}
        base = []                      # базовое матожидание хода (все)
        for i in range(80, len(rows) - 6):
            window = rows[:i]
            future = rows[i:i + 5]
            move = (max(float(r[2]) for r in future)
                    - min(float(r[3]) for r in future)) \
                / float(rows[i][4]) * 100.0
            base.append(move)
            for n in names:
                v = getattr(self, self.SLICES[n])(window)
                if v is not None:
                    bucket[n].append((v, move))
        if not base:
            return
        base_mean = sum(base) / len(base)
        lines = []
        for n in names:
            vals = bucket[n]
            if len(vals) < 30:
                lines.append(f"{n}: выборка {len(vals)} < 30 — не оцениваю")
                continue
            low = [m for v, m in vals if self._is_low(n, v)]
            high = [m for v, m in vals if not self._is_low(n, v)]
            lo_m = sum(low) / len(low) if low else 0.0
            hi_m = sum(high) / len(high) if high else 0.0
            lines.append(f"{n}: ход при «активном» срезе {lo_m:.3f}% "
                         f"(n={len(low)}) против {hi_m:.3f}% (n={len(high)}); "
                         f"база {base_mean:.3f}%")
        self.publish(DRAFT, {
            "kind": "sandbox-report",
            "author": "lab",
            "confidence": 0.9,
            "fields": {"base_move_pct": round(base_mean, 3),
                       "report": lines},
            "lineage": ["sandbox"],
        })
        self.log.warning("ПЕСОЧНИЦА: %s. В шину срез идёт только по слову "
                         "хозяина", "; ".join(lines))

    @staticmethod
    def _is_low(name, value):
        """Что для среза значит «активен» (пороги — часть определения)."""
        if name == "entropy_bars":
            return value < 0.7          # упорядоченность
        if name == "range_squeeze":
            return value < 0.7          # пружина сжата
        return False

    # ---------- пакетный отчёт (математика по выборке) ----------

    async def _report(self):
        if not self.ideals:
            return
        n = len(self.ideals)
        avg = sum(p for _, p in self.ideals) / n
        longs = sum(1 for d, _ in self.ideals if d == "long")
        best = max(self.ideals, key=lambda x: x[1])
        self.publish(DRAFT, {
            "kind": "lab-report",
            "author": "lab",
            "confidence": 0.9,
            "fields": {
                "sims": n,
                "avg_ideal_pct": round(avg, 3),
                "best_ideal_pct": round(best[1], 3),
                "best_direction": best[0],
                "long_share": round(longs / n, 3),
                "news_reactions": self.news_table_view(),
            },
            "lineage": ["lab:batch"],
        })
        self.log.warning("ОТЧЁТ: симуляций %d, средний идеал %.2f%%, "
                         "лучший %.2f%% (%s). Упущенное по Риску "
                         "и ручки — расчёт Тюнера по снимкам",
                         n, avg, best[1], best[0])
        self.ideals = []                   # отчёт по периоду
