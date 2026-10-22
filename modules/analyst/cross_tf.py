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


def _te_from_bins(y_prev, y_cur, x_prev, q):
    """TE по биновым последовательностям (формула Marsilli 2014):
    Σ p(y',y,x) · log2 [ p(y|y',x) / p(y|y') ] — условные вероятности из
    joint-таблиц bincount. Вход: выровненные массивы одинаковой длины m≥2.
    Оценка со смещением O(q²/N) — на малых выборках даёт отрицательный пол;
    возвращаем max(0, сырое значение): TE по построению неотрицательна."""
    m = len(y_cur)
    if m < 2:
        return 0.0
    j3 = (y_prev * q + y_cur) * q + x_prev     # тройка (y', y, x), длина m
    c3 = np.bincount(j3, minlength=q ** 3).astype(float)
    c2yy = np.bincount(y_prev * q + y_cur, minlength=q ** 2).astype(float)
    c2yx = np.bincount(y_prev * q + x_prev, minlength=q ** 2).astype(float)
    i3 = np.flatnonzero(c3 > 0)                 # ненулевые ячейки таблицы
    if not len(i3):
        return 0.0
    # декодируем плоский индекс ячейки обратно в (y', y, x):
    # j3 = (y_prev*q + y_cur)*q + x_prev ⇒ x=i3%q, y=(i3//q)%q, y'=i3//q//q
    yp = i3 // q // q                           # y_{t-1}
    dy, dx = (i3 // q) % q, i3 % q              # y_t, x_{t-1}
    yy_idx = yp * q + dy                         # пара (y_{t-1}, y_t)
    yx_idx = yp * q + dx                         # пара (y_{t-1}, x_{t-1})
    c3m = c3[i3]
    den_both = c2yx[yx_idx]
    den_hist = c2yy[yy_idx]
    # страховка от деления на 0: теоретически c3>0 ⇒ знаменатели >0, но при
    # целочисленном переполнении индекса оставим только валидные ячейки
    ok = (den_both > 0) & (den_hist > 0)
    if not ok.all():
        c3m = c3m[ok]
        den_both = den_both[ok]
        den_hist = den_hist[ok]
        if not len(c3m):
            return 0.0
    # p(y_t | y_{t-1}, x_{t-1}) и p(y_t | y_{t-1})
    p_y_given_both = c3m / den_both
    p_y_given_hist = c3m / den_hist
    term = c3m / m * (p_y_given_both
                      * np.log2(p_y_given_both / p_y_given_hist))
    raw = float(term.sum())
    return raw if np.isfinite(raw) else 0.0


def transfer_entropy(x, y, q):
    """TE(X→Y): прошлое X против настоящего Y при условии прошлого Y (биты).
    x, y — ряды доходностей одного шага; берём общий хвост, сдвиг классический:
    X_{t-1} → Y_t при Y_{t-1} в регрессии.

    Возвращает СЫРУЮ оценку (может быть отрицательной из-за смещения O(q²/N)).
    Вычитание автозависимости Y и калибровка шума — на уровне NET_TE и
    te_lag_profile (см. docstring модуля), а не здесь: clamp'инг нуля тут
    ломал симметрию «ведущий/ведомый»."""
    m = min(len(x), len(y))
    if m < 4:
        return 0.0
    xb = _quantile_bins(np.asarray(x[-m:], dtype=float), q)
    yb = _quantile_bins(np.asarray(y[-m:], dtype=float), q)
    return _te_from_bins(yb[:-1], yb[1:], xb[:-1], q)


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


def _surrogate_te(x, y, q, n_perm, rng):
    """Нулевая TE на перестановках ряда X (разрушает связь, сохраняет
    распределение): медиана по n_perm прогонам — оценка смещения оценки."""
    vals = []
    for _ in range(n_perm):
        xp = rng.permutation(np.asarray(x, dtype=float))
        vals.append(transfer_entropy(xp, y, q))
    return float(np.median(vals)) if vals else 0.0


def te_lag_profile(x, y, q, lag_max, surrogate=5):
    """Профиль TE(X_{t-τ}→Y_t) по τ=0..lag_max (шаги x). Для каждого τ пары
    строятся ТОЧНО: x_pair[t] = x[t-τ], y_pair[t] = y[t] при t = τ..m-1
    (никаких оконных сдвигов — иначе τ* уезжает на мусор).

    На каждом τ сырая TE вычитается нулевая TE того же размера выборки
    (перестановочный суррогат x_pair): смещение оценки O(q²/N) зависит от
    длины, поэтому калибруется отдельно для каждого τ — иначе малые τ
    систематически перевешивают и τ* липнет к 0. Профили fwd и rev
    калибруются одними и теми же суррогатами (общий rng, один проход по τ),
    поэтому NET_TE = fwd − rev остаётся честной разностью.
    Возвращает (τ*, скорректированная TE(τ*))."""
    rng = np.random.default_rng(12345)      # детерминизм: τ* не дрожит между пульсами
    best_tau, best_te = 0, -np.inf
    lx, ly = len(x), len(y)
    for tau in range(0, int(lag_max) + 1):
        if lx - tau < 4 or ly - tau < 4:
            break
        xs = x[:lx - tau] if tau else x
        ys = y[tau:] if tau else y
        raw = transfer_entropy(xs, ys, q)
        null = _surrogate_te(xs, ys, q, surrogate, rng) if surrogate else 0.0
        te = raw - null
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
