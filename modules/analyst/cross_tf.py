"""Кросс-ТФ физика (ШАГ 0 плана кросс-ТФ, 22.10) — перенос влияния между
ступенями полотна. Три метрики, три числа на пару, НИКАКИХ вердиктов:

- Transfer Entropy TE(X→Y) — сколько бит прогноза Y даёт прошлое X сверх
  автокорреляции самой Y. Дискретизация: q равных квантильных бинов по
  доходностям (q=4 — учебный дефолт Марсилли). Симметризация
  NET_TE = TE(X→Y) − TE(Y→X): знак говорит, КТО ведёт (>0 — src ведёт dst).
- Volatility Concordance VC = corr(|r|_src, |r|_dst) — совместны ли режимы
  волатильности ступеней (0..1; низкий = режимы живут отдельно, связь хрупкая).
- Time Distance τ* — лаг (в барах МЛАДШЕЙ ступени), максимизирующий fwd-TE;
  в секундах: tau_sec = τ · tf_src. «Кто кого ведёт и на каком опережении».

Пары — соседние ступени лестницы [cross_tf] pairs_sec ("60:300" = 1м↔5м);
источник — всегда младшая ступень пары (гипотеза «младшая ведёт» — NET_TE её
проверяет числом, а не кодом). Доходности считаются из закрытых свечей
полотна Collector.get_ring() — своих колец не держим (урок дублирования).

Выход — карточка DRAFT kind="cross-tf":
  {tf_sec(src), pair, src_tf, dst_tf, te_fwd, te_rev, te_net, vc,
   tau_bars, tau_sec, tau_te, n}
Матрица кладёт каждое поле на полку «поле@{src_tf}» (после дыры 3);
τ как ось включается флагом [cross_tf] tau_in_axis → модуль добавляет
«tau_sec» в matrix.TF_KEYS до первого поглощения (после дыры 2 dict-поля
разложились бы по осям сами — оставили плоские: одной карточки на пару
хватает, решётка @tf уже разделяет пары).

Формулы заморожены конфигом [cross_tf]: bins(q), window(N баров),
lag_max(L баров), min_samples. Окно берётся с запасом на лаг:
ring.get_last(window + lag_max + 1) → ровно N наблюдений на любом τ
(лаговский хвост не «съедает» выборку — все τ сравнимы).

Алгоритм TE — эталон Marsilli (2014), joint-таблицы через np.bincount,
стоимость O(N) на пару за скан. Тесты физики: tests/test_cross_tf.py
(известные процессы: AR-ведущий → TE>0, белый шум → TE≈0).
"""
import numpy as np

from core.module import Manifest, NeirModule
from modules._labels import DRAFT


def _quantile_bins(x, q):
    """Ранговая дискретизация в q бинов (равные квантили, средние ранги при
    совпадениях — иначе ties косят квантиль-границы). Возвращает int 0..q-1."""
    x = np.asarray(x, dtype=float)
    n = len(x)
    if n == 0:
        return np.empty(0, dtype=int)
    order = np.argsort(x, kind="stable")
    ranks = np.empty(n, dtype=float)
    ranks[order] = np.arange(n, dtype=float)
    uniq, inv, cnt = np.unique(x, return_inverse=True, return_counts=True)
    sums = np.zeros(len(uniq))
    np.add.at(sums, inv, ranks)
    ranks = (sums / cnt)[inv]
    b = np.floor(ranks * q / n).astype(int)
    return np.clip(b, 0, q - 1)


def _bias_grassberger(m, n_occ, n_samp):
    """Аналитическое смещение оценки энтропии (Grassberger 2014, формула для
    отсечённой дигаммы psi_D), взвешенное по вероятностям ячеек:

        B = Σ_i p_i · [ psi_D(n·p̂_i) − psi(n) + (n − n·p̂_i)/(n−1) ]  (натты)

    где суммирование по занятым ячейкам, p̂_i = count_i/n. Это современная
    (2014) несмещённая коррекция вместо грубого члена Миллера (2005).
    psi_D(x) = psi(x) при x >= 1000, иначе точная сумма гармонических чисел."""
    occ = np.asarray(n_occ, dtype=float)
    if m <= 1 or occ.size == 0:
        return 0.0
    tot = float(occ.sum())
    if tot <= 1:
        return 0.0
    # psi(n) через digamma scipy-free: ln(n) - 1/(2n) - 1/(12n^2) (асимптотика
    # точна до ~1e-10 при n>=10)
    psi_n = np.log(tot) - 0.5 / tot - 1.0 / (12.0 * tot * tot)
    psi_d = np.empty_like(occ)
    big = occ >= 1000
    psi_d[big] = np.log(occ[big]) - 0.5 / occ[big]
    small = ~big
    if small.any():
        vals = occ[small].astype(int)
        # psi(k) = -gamma + H(k-1), точные гармонические суммы
        hs = np.zeros(len(vals))
        for kk in range(1, int(vals.max() or 1)):
            mask = vals > kk
            hs[mask] += 1.0 / kk
        psi_d[small] = -0.5772156649015329 + hs
    p = occ / tot
    b = psi_d - psi_n + (tot - occ) / (tot - 1.0)
    return float((p * b).sum()) / np.log(2.0)     # натты -> биты


def _te_pair(yp, ys, xs, q):
    """Одна точка TE(X → Y_t | Y_{t-1}, X_τ) − TE(X → Y_t | Y_{t-1}), т.е.
    TE(X→Y | Y_история) в каноническом виде (Schreiber 2000 / Marsilli 2014),
    со снятием смещения оценки аналитической поправкой Grassberger (2014) —
    современной заменой члена Миллера (2005). Условие — ПАРА (Y_{t-1}, X_τ),
    кодовое пространство q^2; целевая переменная Y_t (q состояний).

    Реализация через разность условных энтропий:
        TE = H(Y_t | Y_{t-1}) - H(Y_t | Y_{t-1}, X_τ)
    Каждая энтропия корректируется своим Grassberger-членом по Counts соответствующих
    условных таблиц. Стоимость O(N) на пару за скан."""
    m = len(ys)
    if m < 2:
        return 0.0
    # --- H(Y_t | Y_{t-1}): условие q состояний, цель q состояний
    c_yy = np.bincount(yp * q + ys, minlength=q * q).astype(float)
    cy = c_yy.reshape(q, q)
    h_y_given_yp = 0.0
    for i in range(q):
        row = cy[i]
        tot = row.sum()
        if tot == 0:
            continue
        p_cond = row[row > 0] / tot
        h_i = -float((p_cond * np.log2(p_cond)).sum())
        bias_i = _bias_grassberger(int(tot), row[row > 0], int(tot))
        h_y_given_yp += (tot / m) * (h_i - bias_i)
    # --- H(Y_t | Y_{t-1}, X_τ): условие q^2 состояний, цель q состояний
    j2 = yp * q + xs
    c_yx = np.bincount(j2 * q + ys, minlength=q ** 3).astype(float)
    h_y_given_yp_xs = 0.0
    for g in range(q * q):
        col = c_yx[g * q:(g + 1) * q]
        tot = col.sum()
        if tot == 0:
            continue
        p_cond = col[col > 0] / tot
        h_g = -float((p_cond * np.log2(p_cond)).sum())
        bias_g = _bias_grassberger(int(tot), col[col > 0], int(tot))
        h_y_given_yp_xs += (tot / m) * (h_g - bias_g)
    te = h_y_given_yp - h_y_given_yp_xs
    return te if np.isfinite(te) else 0.0


def _cond_entropy(target, groups, q_levels, m):
    """H(target | групповые колонки) c поправкой Grassberger на каждую группу.
    target/groups — целочисленные массивы бинов; возвращает биты/символ."""
    idx = np.zeros(m, dtype=np.int64)
    for g in groups:
        idx = idx * q_levels + g
    key = idx * q_levels + target
    cnt = np.bincount(key, minlength=(q_levels ** (len(groups) + 1))
                      ).astype(float).reshape(-1, q_levels)
    h = 0.0
    for row in cnt:
        tot = row.sum()
        if tot == 0:
            continue
        nz = row[row > 0]
        p = nz / tot
        h_i = -float((p * np.log2(p)).sum())
        h += (tot / m) * (h_i - _bias_grassberger(int(tot), nz, int(tot)))
    return h


def _lag_profile_raw(xb, yb, tau_max, q, k_y=1):
    """Лаг-профиль условной TE(X_{t-τ} → Y_t | Y_{t-k_y}..Y_{t-1}), τ=0..tau_max.

    Общая сетка наблюдений t = tau_max+1 .. n-1 строится ОДНИМ срезом для
    всех τ (урок 26.10: src = xb[L-τ : L-τ+m] — скользящее окно одного и
    того же ряда, а не скалярная broadcast-константа; разные срезы на
    разных τ делают точки профиля несравнимыми и пик уезжает в шум).
    Условие — последние k_y баров цели (Марсилли: собственная история Y
    обязательна). История ИСТОЧНИКА X в условие НЕ входит: при скане τ
    X_{t-τ} сам становится условием для последующих лагов, joint-таблица
    раздувается до q^(k_x+k_y+2) и на N≈800 оценка тонет в шуме
    разреженности (проверено 26.10: белый шум давал до 1.07 бит).
    Стоимость O((τ_max+1)·q^(k_y+2)) на пару за скан."""
    n = len(xb)
    if n < tau_max + k_y + 8 or len(yb) != n:
        return None
    L = tau_max                       # стартовая точка общей сетки
    prof = np.zeros(tau_max + 1)
    tgt = yb[L + 1:]                  # Y_t, одна сетка t для всех τ
    m = len(tgt)
    if m < 2:
        return None
    gy = [yb[L + 1 - i] for i in range(k_y, 0, -1)]     # Y_{t-k_y}..Y_{t-1}
    h_small = _cond_entropy(tgt, gy, q, m)
    for tau in range(tau_max + 1):
        src = xb[L - tau:L - tau + m]                   # X_{t-τ} на общей сетке
        te = h_small - _cond_entropy(tgt, gy + [src], q, m)
        prof[tau] = te if np.isfinite(te) and te > 0 else 0.0
    return prof


def transfer_entropy(x, y, q, lag=1):
    """TE(X_{t-lag} → Y_t | Y_{t-1}) в битах — оценка Марсилли со снятием
    смещения (см. _te_pair). Ряды побинаются по полным хвостам (общая сетка
    квантилей). Может быть слегка отрицательной — это норма для скорректированной
    оценки на малом сигнале; clamp нуля ломал симметрию «ведущий/ведомый».

    Важно: lag здесь означает ЗАДЕРЖКУ между X и Y, а не индекс в профиле.
    Для процесса y[t]=f(x[t-tau]) нужно передавать lag=tau, чтобы получить
    TE(X_{t-tau}→Y_t|Y_{t-1})."""
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    n = min(len(x), len(y))
    if n - lag < 4:
        return 0.0
    xb = _quantile_bins(x[len(x) - n:], q)
    yb = _quantile_bins(y[len(y) - n:], q)
    prof = _lag_profile_raw(xb, yb, int(lag), q)
    return 0.0 if prof is None else float(prof[int(lag)])


def volatility_concordance(rx, ry):
    """corr(|r_x|, |r_y|) по общим барам, 0..1; None — вырожденный ряд."""
    m = min(len(rx), len(ry))
    if m < 8:
        return None
    a = np.abs(np.asarray(rx[-m:], dtype=float))
    b = np.abs(np.asarray(ry[-m:], dtype=float))
    if a.std() <= 0 or b.std() <= 0:
        return None
    c = float(np.corrcoef(a, b)[0, 1])
    if not np.isfinite(c):
        return None
    return round(max(0.0, min(1.0, c)), 3)


def te_lag_profile(x, y, q, lag_max, surrogate=0):
    """Профиль TE(X_{t-τ}→Y_t | Y_{t-1}) по τ=0..lag_max.

    Точки профиля — из одной фиксированной сетки наблюдений (_lag_profile_raw),
    смещение оценки снято аналитически (_te_pair), поэтому перестановочные
    суррогаты больше не нужны: медиана из 5 случайных проб сама была шумом
    ±0.05 бит и ломала пик τ* (surrogate=0 — дефолт; >0 оставляет грубую
    проверку для диагностики). Возвращает (τ*, TE(τ*))."""
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    n = min(len(x), len(y))
    if n < lag_max + 8:
        return 0, 0.0
    xb = _quantile_bins(x[len(x) - n:], q)
    yb = _quantile_bins(y[len(y) - n:], q)
    raw = _lag_profile_raw(xb, yb, int(lag_max), q)
    if raw is None:
        return 0, 0.0
    best_tau, best_te = 0, -np.inf
    for tau in range(int(lag_max) + 1):
        te = raw[tau]
        if surrogate:
            rng = np.random.default_rng(12345 + tau)
            ys_ = yb[int(lag_max) + 1:]
            yp_ = yb[int(lag_max):int(lag_max) + len(ys_)]
            null_vals = []
            for _ in range(int(surrogate)):
                xp = rng.permutation(xb)       # перемешали бины — связь рвана
                xs_ = xp[int(lag_max) - tau:int(lag_max) - tau + len(ys_)]
                null_vals.append(_te_pair(yp_, ys_, xs_, q))
            te -= float(np.median(null_vals))
        if te > best_te:
            best_te, best_tau = te, tau
    return best_tau, max(best_te, 0.0)


class CrossTF(NeirModule):
    manifest = Manifest(name="cross-tf", title="Кросс-ТФ физика (TE/VC/τ*)",
                        provides=(DRAFT,),
                        priority=52, tick_timeout=10.0)

    async def on_start(self):
        c = self.kernel.cfg
        self.symbol = c.symbol.upper()
        self.bins = max(2, int(c.get_float("cross_tf", "bins", 4)))
        self.window = max(30, int(c.get_float("cross_tf", "window", 200)))
        self.lag_max = max(1, int(c.get_float("cross_tf", "lag_max", 10)))
        self.min_samples = max(30, int(c.get_float("cross_tf",
                                                   "min_samples", 60)))
        self.publish_every = max(5.0, c.get_float("cross_tf",
                                                  "publish_sec", 60.0))
        self.pairs = []
        for spec in c.get_str("cross_tf", "pairs_sec",
                              "60:300,300:900,900:3600").split(","):
            spec = spec.strip()
            if not spec or ":" not in spec:
                continue
            try:
                a, b = (int(float(v)) for v in spec.split(":", 1))
            except ValueError:
                self.log.warning("Кросс-ТФ: пара %r битая — пропускаю", spec)
                continue
            if a > 0 and b > 0 and a != b:
                self.pairs.append((min(a, b), max(a, b)))
        # τ* отдельной полкой в Матрице (ось @h-подобная): флаг включает
        # TF_KEYS+tau ДО первого поглощения карточки (см. matrix.TF_KEYS)
        if c.get_str("cross_tf", "tau_in_axis", "false").strip().lower() in (
                "1", "true", "yes", "да"):
            from modules.market import matrix as _mx
            if "tau_sec" not in _mx.TF_KEYS:
                _mx.TF_KEYS = _mx.TF_KEYS + ("tau_sec",)
                self.log.info("Кросс-ТФ: tau_sec добавлен в ось-образующие "
                              "TF_KEYS (полки «поле@tf@t{сек}») — включите "
                              "до прогрева матрицы, иначе старые карточки "
                              "останутся без суффикса")
        self.stamps = 0
        self.errors = 0
        self.every(self.publish_every, self._scan_and_publish)
        self.log.info("Кросс-ТФ готов: пары %s, q=%d, окно %d баров, "
                      "лаг ±%d, минимум %d, пульс %.0f с",
                      ["%dм:%dм" % (a // 60, b // 60) for a, b in self.pairs],
                      self.bins, self.window, self.lag_max,
                      self.min_samples, self.publish_every)

    async def on_stop(self):
        self.log.info("Итог кросс-ТФ: карточек %d, ошибок расчёта %d",
                      self.stamps, self.errors)

    # ---------- данные ----------

    def _returns(self, tf_sec):
        """Доходности (%) закрытий ступени ИЗ ПОЛОТНА Сборщика.
        window+lag_max+1 баров — ровно window наблюдений после diff даже
        на максимальном лаге."""
        collector = self.kernel.modules.get("collector")
        if collector is None:
            return None
        ring = collector.get_ring(tf_sec)
        if ring is None or ring.head < self.min_samples + 1:
            return None
        data = ring.get_last(self.window + self.lag_max + 1)
        if data is None or len(data["c"]) < 3:
            return None
        cl = data["c"]
        with np.errstate(divide="ignore", invalid="ignore"):
            r = np.diff(cl) / np.maximum(cl[:-1], 1e-12) * 100.0
        r = r[np.isfinite(r)]
        if len(r) < self.min_samples:
            return None
        return r

    # ---------- расчёт и публикация ----------

    def _pair_card(self, src_tf, dst_tf):
        rx, ry = self._returns(src_tf), self._returns(dst_tf)
        if rx is None or ry is None:
            return None
        te_fwd = transfer_entropy(rx, ry, self.bins)   # src→dst
        te_rev = transfer_entropy(ry, rx, self.bins)   # dst→src
        tau, te_best = te_lag_profile(rx, ry, self.bins, self.lag_max)
        vc = volatility_concordance(rx, ry)
        n = int(min(len(rx), len(ry)))
        return {
            "tf_sec": int(src_tf),           # полка Матрицы: поле@{src_tf}
            "pair": "%dm:%dm" % (src_tf // 60, dst_tf // 60),
            "src_tf": int(src_tf), "dst_tf": int(dst_tf),
            "te_fwd": round(te_fwd, 5), "te_rev": round(te_rev, 5),
            "te_net": round(te_fwd - te_rev, 5),
            "vc": vc,
            "tau_bars": int(tau), "tau_sec": int(tau * src_tf),
            "tau_te": round(te_best, 5),
            "n": n,
        }

    async def _scan_and_publish(self):
        ok = 0
        for src_tf, dst_tf in self.pairs:
            try:
                fields = self._pair_card(src_tf, dst_tf)
            except (TypeError, ValueError, IndexError,
                    FloatingPointError) as exc:
                self.errors += 1
                self.log.error("Кросс-ТФ пара %d/%d: %s: %s",
                               src_tf, dst_tf, type(exc).__name__, exc,
                               exc_info=True)
                fields = None
            if fields is None:
                continue      # ступень не прогрета — молча ждём следующий пульс
            ok += 1
            self.stamps += 1
            self.publish(DRAFT, {
                "kind": "cross-tf",
                "author": "cross-tf",
                "confidence": round(min(1.0, fields["n"] /
                                        float(self.window)), 3),
                "fields": fields,
                "lineage": [f"candle:{self.symbol}:{src_tf}",
                            f"candle:{self.symbol}:{dst_tf}"],
            })
        if ok:
            self.log.debug("Кросс-ТФ: карточек %d/%d", ok, len(self.pairs))
