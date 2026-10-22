"""Приёмка Шага 3 (план кросс-ТФ): адресная модель Матрицы — дыры 1/2/3.

Прогон: python tests/test_matrix_step3.py   (из корня репозитория)
Матрицу строим через __new__ — без ядра; карточки подаём в _absorb напрямую,
форм-факторы соответствуют реальным публикациям sr/zones-plus/flow/oi-cascade/
regression (22.10).
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.market.matrix import Matrix, _flatten, _fmt_axis_part


class FakeLog:
    def debug(self, *a): pass
    def info(self, *a): pass
    def warning(self, *a): pass
    def exception(self, *a): pass


def mk():
    m = Matrix.__new__(Matrix)
    m._cells = {}
    m.writes = 0
    m.log = FakeLog()
    return m


def test_d3_horizons_get_own_shelves():
    m = mk()
    m._absorb({"kind": "trend-forecast", "seq": 1, "created_ms": 1000,
               "fields": {"tf_sec": 60, "horizon_min": 15.0, "expect_pct": 0.4}})
    m._absorb({"kind": "trend-forecast", "seq": 2, "created_ms": 1000,
               "fields": {"tf_sec": 60, "horizon_min": 60.0, "expect_pct": 1.1}})
    a = m._cells[("trend-forecast", "expect_pct@60@h15")]
    b = m._cells[("trend-forecast", "expect_pct@60@h60")]
    assert a["value"] == 0.4 and b["value"] == 1.1          # обе живы
    assert all(not k[1].startswith(("tf_sec", "horizon_min")) for k in m._cells)


def test_d3_backcompat_and_broken_tf():
    m = mk()
    m._absorb({"kind": "trend-vector", "seq": 3, "created_ms": 1000,
               "fields": {"tf_sec": 300, "score": 7}})
    assert m._cells[("trend-vector", "score@300")]["value"] == 7   # как раньше
    m._absorb({"kind": "x", "seq": 4, "created_ms": 1,
               "fields": {"tf_sec": "abc", "score": 1}})
    assert ("x", "score") in m._cells                             # битый tf → плоский


def test_d2_dict_flattened_lists_as_is():
    fl = dict(_flatten({"a": {"b": 1}, "c": [1, 2], "d": {"e": {"f": {"g": 9}}}}))
    assert fl["a.b"] == 1                     # dict → составная ось
    assert fl["c"] == [1, 2]                  # list — значение «как есть»
    assert fl["d.e.f.g"] == 9 and "d.e.f" not in fl   # глубина = MAX_NEST
    cyc = {"p": {}}
    cyc["p"]["q"] = cyc["p"]                  # цикл не должен убить прогон
    dict(_flatten(cyc))
    m = mk()
    m._absorb({"kind": "meta-corr", "seq": 5, "created_ms": 1000,
               "fields": {"te_profile": {"fst": 0.3, "lag": [1, 2]}, "tf_sec": 3600}})
    assert m._cells[("meta-corr", "te_profile.fst@3600")]["value"] == 0.3


def test_d1_zone_lattice():
    m = mk()
    card = lambda zid, lo, hi, pw, ts: {"kind": "stop-cluster", "seq": None,
        "created_ms": ts, "fields": {"zone_id": zid, "zone_low": lo,
        "zone_high": hi, "tf_sec": 60, "zone_power": pw}}
    m._absorb(dict(card("cl8421.5", 100.0, 101.0, 2.1, 1000), seq=6))
    m._absorb(dict(card("cl9999.1", 150.0, 151.0, 5.5, 1000), seq=7))
    assert m._cells[("stop-cluster", "zone_low#cl8421.5@60")]["value"] == 100.0
    assert m._cells[("stop-cluster", "zone_low#cl9999.1@60")]["value"] == 150.0
    assert all(not k[1].startswith("zone_id") for k in m._cells)   # id не полка
    m._absorb(dict(card("cl8421.5", 100.0, 101.0, 2.4, 2000), seq=8))  # та же зона
    c = m._cells[("stop-cluster", "zone_power#cl8421.5@60")]       # обновилась
    assert c["value"] == 2.4 and c["seq"] == 8 and len(m._cells) == 6


def test_d1_backcompat_write_guard():
    m = mk()
    m._absorb({"kind": "sr-zone", "seq": 9, "created_ms": 1000,
               "fields": {"zone_low": 5.0, "touches": 3}})           # старая карточка журнала
    assert m._cells[("sr-zone", "zone_low")]["value"] == 5.0
    m._absorb({"kind": "sr-zone", "seq": 10, "created_ms": 500,      # откат по ts
               "fields": {"zone_low": 4.0}})
    assert m._cells[("sr-zone", "zone_low")]["seq"] == 9             # не перетёрто


def test_real_card_shapes():
    m = mk()
    m._absorb({"kind": "sr-zone", "seq": 1, "created_ms": 1000, "fields": {
        "zone_id": "sr%.6g" % ((100.0 + 100.5) / 2), "zone_low": 100.0,
        "zone_high": 100.5, "touches": 4}})
    assert ("sr-zone", "zone_low#sr100.25") in m._cells
    m._absorb({"kind": "absorption-barrier", "seq": 2, "created_ms": 1000,
               "fields": {"zone_id": "abs8421.5", "level": 8421.5}})
    assert m._cells[("absorption-barrier", "level#abs8421.5")]["value"] == 8421.5
    m._absorb({"kind": "level-verdict", "seq": 3, "created_ms": 1000,
               "fields": {"zone_id": "lvl1234.5", "outcome": "sweep"}})
    assert m._cells[("level-verdict", "outcome#lvl1234.5")]["value"] == "sweep"
    m._absorb({"kind": "fvg-zone", "seq": 4, "created_ms": 1000, "fields": {
        "zone_id": "fvg1700000000000", "tf_sec": 300, "zone_power": 3.3}})
    assert m._cells[("fvg-zone", "zone_power#fvg1700000000000@300")]["value"] == 3.3


def test_fmt_axis_part():
    assert _fmt_axis_part(60) == "60"          # int-представление целых
    assert _fmt_axis_part(15.0) == "15"
    assert _fmt_axis_part(7.5) == "7.5"
    assert _fmt_axis_part("abc") is None


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    failed = 0
    for fn in fns:
        try:
            fn()
            print("PASS", fn.__name__)
        except Exception as e:
            failed += 1
            print("FAIL", fn.__name__, type(e).__name__, e)
    print("\n%d/%d тестов пройдено" % (len(fns) - failed, len(fns)))
    sys.exit(1 if failed else 0)
