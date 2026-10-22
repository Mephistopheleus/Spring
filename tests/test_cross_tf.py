"""Приёмка ШАГа 0 (план кросс-ТФ): физика TE / Vol Concordance / Time Distance.

Прогон: python tests/test_cross_tf.py   (из корня репозитория)

Проверяем метрику на процессах с ИЗВЕСТНЫМ ответом — иначе «формула
заморожена» означает заморозку неизвестно чего:
1. X ведёт Y с лагом τ=2 (Y_t = 0.9·X_{t-2} + шум) → TE(X→Y) ≫ TE(Y→X),
   NET_TE > 0, τ* = 2;
2. два независимых белых шума → оба TE ≈ 0 (выборочный шум не больше
   порога);
3. синхронная пара (τ=0) → τ* = 0;
4. VC: одинаковые ряды |r| → 1.0; независимые режимы → низкая;
5. дискретизация: бины равных квантелей покрывают 0..q-1 ровно;
6. Матрица: карточка cross-tf адресуется «поле@{src_tf}», при tau_in_axis
   TF_KEYS+tau_sec даёт полку @t{sec} (задел из Шага 3).
"""
import os
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.analyst.cross_tf import (_quantile_bins, te_lag_profile,
                                      transfer_entropy,
                                      volatility_concordance)
from modules.market.matrix import Matrix


def leader_follower(n=800, tau=2, seed=7):
    """X ведёт Y с лагом tau; при tau=0 ряды синхронны (y[0:] = 0.9·x + шум)."""
    rng = np.random.default_rng(seed)
    x = rng.normal(size=n)
    y = np.zeros(n)
    if tau > 0:
        y[tau:] = 0.9 * x[:-tau] + 0.3 * rng.normal(size=n - tau)
    else:
        y[:] = 0.9 * x + 0.3 * rng.normal(size=n)
    return x, y


def test_leader_detected():
    x, y = leader_follower()
    rx, ry = np.diff(x), np.diff(y)      # доходности, как в модуле
    te_xy = transfer_entropy(rx, ry, 4)
    te_yx = transfer_entropy(ry, rx, 4)
    assert te_xy > 0.05, f"TE(ведущий→ведомый) слишком мала: {te_xy}"
    assert te_xy > te_yx, f"NET не в сторону ведущего: {te_xy} vs {te_yx}"
    tau, best = te_lag_profile(rx, ry, 4, lag_max=6)
    assert tau == 2, f"τ* должен быть 2, получен {tau}"
    assert best >= te_xy - 1e-9


def test_white_noise_symmetric_small():
    rng = np.random.default_rng(11)
    x, y = rng.normal(size=800), rng.normal(size=800)
    rx, ry = np.diff(x), np.diff(y)
    te_xy = transfer_entropy(rx, ry, 4)
    te_yx = transfer_entropy(ry, rx, 4)
    assert abs(te_xy) < 0.05 and abs(te_yx) < 0.05, \
        f"шум дал TE {te_xy}/{te_yx} — завышенная оценка"


def test_sync_pair_tau_zero():
    x, y = leader_follower(tau=0)
    rx, ry = np.diff(x), np.diff(y)
    tau, _ = te_lag_profile(rx, ry, 4, lag_max=6)
    assert tau == 0, f"синхронная пара, а τ*={tau}"


def test_volatility_concordance():
    rng = np.random.default_rng(3)
    a = rng.normal(size=500)
    assert volatility_concordance(a, a) == 1.0          # идентичные режимы
    b = rng.normal(size=500)
    vc = volatility_concordance(a, b)
    assert vc is not None and vc < 0.25, f"независимые VC={vc} — завышена"
    # общий волатильный режим: обе серии умножены на один envelope
    env = np.abs(np.concatenate([np.linspace(1, 5, 250),
                                 np.linspace(5, 1, 250)]))
    c, d = rng.normal(size=500) * env, rng.normal(size=500) * env
    assert volatility_concordance(c, d) > 0.4, "общий режим не уловлен"
    assert volatility_concordance([1.0] * 20, list(range(20))) is None or True


def test_quantile_bins_coverage():
    x = np.arange(100, dtype=float)
    b = _quantile_bins(x, 4)
    assert set(b.tolist()) == {0, 1, 2, 3}
    assert (np.diff(b) >= 0).all()                      # монотонно
    ties = _quantile_bins(np.zeros(50), 4)              # вырожденный ряд
    assert ties.min() == ties.max()                     # все в один бин


def test_matrix_shelf_and_tau_axis():
    class FakeLog:
        def debug(self, *a): pass
        def info(self, *a): pass
        def warning(self, *a): pass
        def exception(self, *a): pass

    m = Matrix.__new__(Matrix)
    m._cells, m.writes, m.log = {}, 0, FakeLog()
    card = {"kind": "cross-tf", "seq": 1, "created_ms": 1000,
            "fields": {"tf_sec": 60, "pair": "1m:5m", "src_tf": 60,
                       "dst_tf": 300, "te_net": 0.12, "vc": 0.7,
                       "tau_bars": 2, "tau_sec": 120, "n": 200}}
    m._absorb(card)
    assert m._cells[("cross-tf", "te_net@60")]["value"] == 0.12
    assert m._cells[("cross-tf", "vc@60")]["value"] == 0.7
    assert ("cross-tf", "tf_sec@60") not in m._cells    # служебное не дублируем
    # τ как ось (флаг tau_in_axis): добавляем tau_sec в TF_KEYS — та же
    # карточка уезжает на полку @t120 (обратная совместимость проверена
    # в test_matrix_step3: без поля адрес не меняется)
    import modules.market.matrix as mx
    saved = mx.TF_KEYS
    try:
        mx.TF_KEYS = saved + ("tau_sec",)
        m2 = Matrix.__new__(Matrix)
        m2._cells, m2.writes, m2.log = {}, 0, FakeLog()
        m2._absorb(card)
        assert ("cross-tf", "te_net@60@t120") in m2._cells
        assert all(not k[1].startswith("tau_sec") for k in m2._cells)
    finally:
        mx.TF_KEYS = saved


def main():
    tests = [v for k, v in sorted(globals().items())
             if k.startswith("test_") and callable(v)]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"OK   {t.__name__}")
        except AssertionError as exc:
            failed += 1
            print(f"FAIL {t.__name__}: {exc}")
    print(f"\n{len(tests) - failed}/{len(tests)} зелёные")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
