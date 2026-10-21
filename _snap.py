"""СЛЕПОК ЖИВОЙ КАРТОЧКИ СЦЕНАРИЯ (доказательство из БД, без украшений).

Показывает: timestamp/seq последней карточки, состав вектора, сухие значения
выбранных полей и состояние ИСТОЧНИКОВ (почему поле может быть missing).
Запуск: python _snap.py [поле ...]
"""
import json
import sqlite3
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from core.config import KernelConfig              # noqa: E402

WANT = sys.argv[1:] or ["psy_viscosity", "reg_acc_1m"]
conn = sqlite3.connect(str(KernelConfig().path("data/neiros.db")))


def latest(kind):
    return conn.execute(
        "SELECT seq, created_ms, payload, lineage FROM cards "
        "WHERE kind=? ORDER BY seq DESC LIMIT 1", (kind,)).fetchone()


def ts(ms):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ms / 1000))


row = latest("scenario")
if not row:
    print("СЦЕНАРИЕВ В БАЗЕ НЕТ")
    raise SystemExit(1)
seq, created, payload, lineage = row
fields = json.loads(payload)
vec = fields.get("vector") or {}

print("=" * 68)
print("ДОКАЗАТЕЛЬСТВО ИЗ БАЗЫ")
print(f"  seq      : {seq}")
print(f"  создан   : {ts(created)}  (локальное время БД)")
print(f"  возраст  : {int(time.time()) - created // 1000} с")
print(f"  lineage  : {lineage}")
print("=" * 68)

groups = {}
for k in vec:
    g = ("lens_" if k.startswith("lens_") else
         "reg_" if k.startswith("reg_") else
         "psy_" if k.startswith("psy_") else "прочие")
    groups.setdefault(g, []).append(k)
print(f"ВЕКТОР МОМЕНТА: {len(vec)} полей")
for g, ks in sorted(groups.items()):
    print(f"  {g:9} {len(ks):3}")

print("-" * 68)
print("СУХИЕ ЗНАЧЕНИЯ ЗАПРОШЕННЫХ ПОЛЕЙ")
for name in WANT:
    e = vec.get(name)
    if e is None:
        print(f"  {name:16} ОТСУТСТВУЕТ")
        continue
    print(f"  {name:16} value={e.get('value')}  state={e.get('state')}"
          f"  kind={e.get('kind')}  age={e.get('age_sec')}")

print("-" * 68)
print("СОСТОЯНИЕ ИСТОЧНИКОВ (почему поле может быть missing)")
for kind, field in (("flow-state", "viscosity"),
                    ("entropy-state", "entropy"),
                    ("trend-vector", "score"),
                    ("conductivity", "components")):
    r = latest(kind)
    if not r:
        print(f"  {kind:14} КАРТОЧЕК НЕТ")
        continue
    age = int(time.time()) - r[1] // 1000
    raw = json.loads(r[2])
    fld = raw.get("fields", raw)
    val = fld.get(field, "—")
    if field == "components":
        val = fld.get("conditions", {}).get("components", "—")
    if field == "score":
        val = f"{fld.get('tf_sec')}с: {fld.get('score')}"
    shown = str(val)
    if len(shown) > 60:
        shown = shown[:60] + "…"
    print(f"  {kind:14} {age:6} с назад  {field}={shown}")

print("-" * 68)
print("БАЛЛИСТИЧЕСКИЕ КООРДИНАТЫ")
for k in ("entry", "targets", "stop", "atr", "leverage",
          "free_trail", "multi_entry", "cascade_levels", "direction"):
    print(f"  {k:15} {fields.get(k)}")
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
from core.config import KernelConfig                      # noqa: E402

secs = int(sys.argv[1]) if len(sys.argv) > 1 else 90
cfg = KernelConfig()
print(f"контур={cfg.contour} режим={cfg.mode} "
      f"(real_allowed={cfg.real_allowed})")
if cfg.trading_blocked_reason:
    print(f"почему не реал: {cfg.trading_blocked_reason}")

proc = subprocess.Popen([sys.executable, "-X", "utf8", "-u", "main.py"],
                        cwd=str(ROOT), stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT, text=True,
                        encoding="utf-8", errors="replace")
try:
    time.sleep(secs)
finally:
    proc.terminate()
    try:
        out, _ = proc.communicate(timeout=25)
    except subprocess.TimeoutExpired:
        proc.kill()
        out, _ = proc.communicate()

lines = (out or "").splitlines()
log = ROOT / "data" / "_live_probe.log"
log.write_text("\n".join(lines), encoding="utf-8")
tb = sum(1 for ln in lines if "Traceback" in ln)
err = sum(1 for ln in lines if "[ERROR" in ln)
print(f"строк: {len(lines)} | трассировок: {tb} | ERROR: {err}")
print(f"лог: {log}")
import json
import sqlite3
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from core.config import KernelConfig  # noqa: E402

db = KernelConfig().path("data/neiros.db")
conn = sqlite3.connect(str(db))
rows = conn.execute(
    "SELECT seq, kind, created_ms, payload, lineage FROM cards "
    "WHERE kind IN ('scenario','deal-closed','lab-snapshot') "
    "ORDER BY seq DESC LIMIT 500").fetchall()

scen = [r for r in rows if r[1] == "scenario"]
deals = [r for r in rows if r[1] == "deal-closed"]
snaps = [r for r in rows if r[1] == "lab-snapshot"]
print(f"в окне: сценариев {len(scen)}, сделок {len(deals)}, "
      f"лаб-снимков {len(snaps)}")
if scen:
    age = int(time.time()) - scen[0][2] // 1000
    print(f"последнему сценарию {age} с назад "
          f"(снимку нужно ≥3 закрытых бара после него)")

deal_seqs = {r[0] for r in deals}
for seq, _kind, _ms, payload, lineage in reversed(snaps[-3:]):
    f = json.loads(payload)
    trusts = (f.get("ideal_set") or {}).get("trusts") or {}
    route = (f.get("ideal_set") or {}).get("route") or {}
    lin = json.loads(lineage or "[]")
    # ИЗОЛЯЦИЯ: снимок лабы не должен ссылаться на карточку сделки
    linked_deal = [x for x in lin if x.startswith("card:")
                   and int(x.split(":")[1]) in deal_seqs]
    print(f"снимок seq={seq}: stream={f.get('stream')} "
          f"исход={f.get('outcome_dir')} идеал={f.get('ideal_pnl_pct')}% "
          f"доверий={len(trusts)} маршрут={bool(route)} "
          f"facts_vec={bool(f.get('facts_vec'))} "
          f"deal_id={'deal_id' in f} связь_со_сделкой={linked_deal}")