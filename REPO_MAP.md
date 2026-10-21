<!-- Aider Studio repo map — auto-generated. Do not edit by hand. -->
# Repository Map

_127 files indexed · 0 summarized. Summaries, outlines (with line numbers) and metadata only — NOT file contents. To work on a file's actual code, `/read` or `/add` it by the path shown._

## _disc.txt
- RAWDATA

## _dump_defaults.py
- def fmt(value):  ·L32
- def main():  ·L43

## _key_check.py
- def signed_query(key, secret, extra=None):  ·L25
- async def raw(name, base, path, key, secret):  ·L34
- def via_connector(key, secret, base):  ·L46
- def via_python_binance(key, secret):  ·L58
- async def main():  ·L69
- def show(label, acct):  ·L77

## _live_run.py
- async def run(seconds: int, ui: bool = False):  ·L19

## _snap.py
- def latest(kind):  ·L21
- def ts(ms):  ·L27

## _start.bat
- @echo off

## .aider.chat.history.md
- # aider chat started at 2026-10-15 15:55:01  ·L2
- ## 📊 Аудит проекта — приоритетные файлы для детального анализа  ·L131
- ### 1. **core/bus.py** ⭐⭐⭐  ·L133
- ### 2. **core/kernel.py** ⭐⭐⭐  ·L140
- ### 3. **modules/market/collector.py** ⭐⭐  ·L147
- ### 4. **modules/market/quota.py** ⭐⭐  ·L152
- ### 5. **core/module.py** ⭐⭐  ·L157
- ### 6. **tests/smoke_collector_init.py** ⭐⭐  ·L162
- ## 🎯 Что я найду при детальном анализе каждого файла:  ·L168
- ## 📋 АУДИТ ПРОЕКТА — ПОДРОБНЫЙ ОТЧЁТ  ·L238

## .aider.input.history
- # 2026-10-15 15:55:09.473852

## .gitignore
- # СЕКРЕТЫ — никогда не в VCS

## 🟡 Риски
- # В _consume_broadcast и _consume_personal:

## AUDIT_2026-10-01.md
- # АУДИТ NeirOS-M — 01.10.2026 (Muse Spark 1.3 Contributor)  ·L1
- ## ✅ P0 — РИСК ПОТЕРИ ДЕНЕГ (ИСПРАВЛЕНО)  ·L14
- ### 1. Аварийная команда могла потеряться (исправлено)  ·L16
- ### 2. Здоровье личных конвертов было неполным (исправлено)  ·L30
- ### 3. Не было идемпотентности ордеров (исправлено)  ·L36
- ### 4. Не было раздельных контуров и рубильника (исправлено)  ·L48
- ## ✅ P1 — ЭКСПЛУАТАЦИЯ (ИСПРАВЛЕНО)  ·L76
- ### 5. Эталон сброса рассинхронизирован с кодом (исправлено)  ·L78
- ### 6. Экран не закрывался при остановке ядра (исправлено)  ·L90
- ### 7. Экран был включён по умолчанию (исправлено)  ·L98

## audit_plan_fact.md
- # ПОЛНЫЙ АУДИТ АНАЛИТИЧЕСКОГО КОНТУРА (ПЛАН VS ФАКТ)  ·L1
- ## 🛠️ ВОЛНА 1: ТОПЛИВО И МАГНИТЫ (100% Готово)  ·L6
- ## 📐 ВОЛНА 2: НАПРАВЛЕНИЕ И ФОН (100% Готово)  ·L14
- ## 🧬 РУБЕЖИ — ВСЕ СНЯТЫ (замысел → факт)  ·L22

## BACKLOG.md
- # NeirOS-M — бэклог идей и зафиксированные решения  ·L1
- ## Аудит 01.10.2026 (полный разбор проекта)  ·L8
- ## Решения архитектуры (зафиксировано, не пересматривать)  ·L60
- ## Волна 3 — порядок работ (согласовано)  ·L95
- ### 1. TTE-календарь (Time-To-Event) — первый  ·L97
- ### 2. Метакоррелятор — второй (паспорт утверждён)  ·L106
- ### 3. Интуит (аналогии) — третий  ·L126
- ### 4. Фракталы ТФ + паттерны — четвёртый  ·L135
- ### Далее (приоритет ниже)  ·L151
- ## Волна 5 → Этапы 30–34: полная реализация (план утверждён)  ·L163

## config.old
- [core]

## config/config.default.ini
- # ЭТАЛОН ПОЛНОГО СБРОСА NeirOS-M

## config/config.ini
- [core]

## config/keys.example.ini
- ; ============================================================

## config/keys.ini
- ; ============================================================

## core/__init__.py
- """Ядро NeirOS-M: автопоиск модулей, интеграция, шина, здоровье, остановка.

## core/bus.py
- class Envelope:  ·L22
- def __init__(self, seq, label, payload, created, ttl, sender):  ·L27
- def is_expired(self, now=None):  ·L35
- class PersonalEnvelope:  ·L43
- def __init__(self, seq, payload, created, ttl, sender, recipient):  ·L50
- def is_expired(self, now=None):  ·L59
- class Box:  ·L67
- def __init__(self, maxlen=None):  ·L73
- def put(self, item):  ·L77
- async def get(self):  ·L83

## core/config.py
- class KernelConfig:  ·L274
- def __init__(self):  ·L277
- def get_str(self, section: str, key: str, default: str = "") -> str:  ·L302
- def get_int(self, section: str, key: str, default: int = 0) -> int:  ·L305
- def get_float(self, section: str, key: str, default: float = 0.0) -> float:  ·L312
- def override(self, section: str, key: str, value) -> None:  ·L319
- def path(self, relative: str) -> Path:  ·L323
- def key(self, section: str, option: str):  ·L329
- def news_key(self, option: str):  ·L338
- def symbol(self) -> str:  ·L349

## core/discovery.py
- def discover(kernel):  ·L20
- def order_modules(modules, log):  ·L93

## core/env.py
- class Env:  ·L1
- def __init__(self, payload):  ·L2

## core/kernel.py
- class Kernel:  ·L30
- def __init__(self, cfg=None, scan_modules=True):  ·L31
- def subscribe(self, owner, label, handler, box_size=None):  ·L51
- def publish(self, owner, label, payload, ttl=None):  ·L56
- def send(self, owner, recipient, payload, ttl=None):  ·L59
- def mailbox(self, owner, handler):  ·L62
- def spawn(self, owner, coro):  ·L65
- def every(self, owner, interval, callback, timeout=None):  ·L72
- async def _loop():  ·L76
- def module_error(self, owner):  ·L96

## core/logging_setup.py
- def setup_logging(cfg) -> None:  ·L10

## core/module.py
- class Manifest:  ·L14
- class NeirModule(abc.ABC):  ·L26
- def __init__(self, kernel):  ·L29
- async def on_start(self) -> None:  ·L35
- async def on_stop(self) -> None:  ·L38
- def subscribe(self, label: str, handler: Callable, box_size: int = None):  ·L43
- def publish(self, label: str, payload, ttl: float = None):  ·L47
- def send(self, recipient: str, payload, ttl: float = None):  ·L51
- def mailbox(self, handler: Callable):  ·L55
- def spawn(self, coro):  ·L59

## logs/_live900.err
- (empty)

## logs/_live900.out
- 2026-10-09 17:32:30.192 [INFO    ] [NeirOS.LIVE] === ЖИВОЙ ПРОГОН: 900 сек, символ DOGEUSDT ===

## logs/neiros.log
- 2026-10-14 23:53:00.525 [INFO    ] [NeirOS.zones-plus] Зоны+ включены: уровни [60, 14400], FVG от…

## logs/neiros.log.1
- (large file, 5120 KB — not indexed)

## logs/test_fail_smoke_stage12.log
- 2026-10-07 19:58:56.576 [INFO    ] [NeirOS.DISCOVERY] [DISCOVERY] analyst/calendar → calendar

## logs/test_fail_smoke_stage36.log
- (empty)

## logs/test_fail_smoke_stage6.log
- 2026-10-07 21:58:55.388 [INFO    ] [NeirOS.KERNEL] [START] rsi — готов

## logs/test_fail_smoke_stage7.log
- 2026-10-07 22:07:01.060 [INFO    ] [NeirOS.scenario-writer] [BLOCK_2] Выход: зон=4 цена=0.08861 с…

## main.py
- def force_utf8_console():  ·L11
- async def _finish(kernel: Kernel, main_task: asyncio.Task):  ·L34
- def main():  ·L49

## modules/__init__.py
- """Модули системы NeirOS-M. Ядро находит их здесь автоматически."""

## modules/_knobs.py
- def lens_knob(param, minutes):  ·L47
- def register_lens_knobs(scales=None, target=None):  ·L52
- def reg_knob(param, tf_label):  ·L89
- def rsi_knob(key):  ·L94
- def register_reg_knobs(tfs=None, target=None):  ·L99
- def psy_knob(param):  ·L134
- def register_psy_knobs(target=None):  ·L139

## modules/_labels.py
- def lens_field(param, minutes):  ·L94
- def lens_scales(default=(1, 2, 4, 8, 16, 32, 64, 128, 256, 512)):  ·L99
- def register_lens_fields(scales=None):  ·L104
- def reg_field(param, tf_label):  ·L134
- def register_reg_fields(tfs=None):  ·L139
- def register_rsi_fields():  ·L148
- def fract_field(param, tf_label):  ·L168
- def register_fractal_fields(tfs=None):  ·L173
- def register_hurst_fields(tfs=None):  ·L187
- def meta_field(metric, symbol):  ·L208

## modules/analyst/__init__.py
- """Аналитические модули NeirOS-M."""

## modules/analyst/calendar.py
- class TteCalendar(NeirModule):  ·L28
- async def on_start(self):  ·L32
- async def _poll(self):  ·L57
- async def _fetch_finnhub(self):  ·L75
- async def _fetch_fred(self):  ·L102
- def _parse_time(self, value):  ·L157
- async def _pulse(self):  ·L170

## modules/analyst/conductivity.py
- class Conductivity(NeirModule):  ·L33
- async def on_start(self):  ·L37
- async def on_stop(self):  ·L70
- async def _on_liq(self, env):  ·L78
- async def _on_oi(self, env):  ·L86
- async def _on_price(self, env):  ·L100
- async def _on_card(self, env):  ·L111
- def _component_panic(self):  ·L127
- def _component_stubborn(self):  ·L132
- def _component_impulse(self):  ·L151

## modules/analyst/entropy.py
- class TickEntropy(NeirModule):  ·L20
- async def on_start(self):  ·L24
- async def _on_tick(self, env):  ·L35
- def _entropy(self):  ·L40
- async def _publish(self):  ·L64

## modules/analyst/field.py
- class FieldAnalyzer(NeirModule):  ·L56
- async def on_start(self):  ·L60
- async def on_stop(self):  ·L113
- def _remember(self, card):  ·L119
- async def _on_card(self, env):  ·L170
- def _apply_verdict(self, card):  ·L199
- async def _on_price(self, env):  ·L215
- def _recharge_shelves(self):  ·L233
- def _zone_atr(self):  ·L259
- def _fake_until(self, level, atr=None):  ·L279

## modules/analyst/flow.py
- class FlowAnalyst(NeirModule):  ·L36
- async def on_start(self):  ·L40
- async def on_stop(self):  ·L75
- async def _on_tick(self, env):  ·L82
- def _window(self, ts, seconds):  ·L102
- def _vol_expect_60(self):  ·L107
- async def _evaluate(self):  ·L117
- async def _check_absorption(self, now, total_vol, buy_vol, sell_vol,  ·L163
- async def _check_divergence(self, now, p_ref):  ·L213
- def _viscosity(self, now, win_trades, price_now):  ·L256

## modules/analyst/fractal.py
- def _tf_label_to_sec(label):  ·L39
- def _rs_hurst(series):  ·L58
- def selfsim_corr(junior, senior):  ·L97
- class Fractals(NeirModule):  ·L111
- async def on_start(self):  ·L115
- def _ring(self, tf):  ·L153
- def _fail(self, what, exc):  ·L193
- def _layer_hurst(self, tf):  ·L201
- def _pyramid_hurst(self):  ·L226
- async def on_stop(self):  ·L243

## modules/analyst/intuit.py
- def tf_label(tf_sec):  ·L26
- def _signed_rsi(value, upper, lower):  ·L36
- def _norm(vec):  ·L60
- def _cosine(a, b):  ·L64
- def _axis_value(name, src, norm, geom=None, dyn=None, fv=None,  ·L71
- class Intuit(NeirModule):  ·L105
- async def on_start(self):  ·L109
- async def on_stop(self):  ·L140
- async def _on_candles(self, env):  ·L145
- def _calc_atr(self):  ·L156

## modules/analyst/lab.py
- class Lab(NeirModule):  ·L48
- async def on_start(self):  ·L52
- async def on_stop(self):  ·L89
- async def _on_card(self, env):  ·L95
- async def _simulate(self, card):  ·L121
- def _facts_vec(vector):  ·L358
- def _ideal_trusts(self, vector, moment, outcome_dir):  ·L389
- def margin(vec):  ·L432
- def _ideal_route(self, path, entry, long, lev, sfields, moment, ideal_px):  ·L455
- def run(vec):  ·L488

## modules/analyst/leader.py
- class Leader(NeirModule):  ·L28
- async def on_start(self):  ·L32
- async def on_stop(self):  ·L47
- async def _on_btc(self, env):  ·L54
- async def _on_alt(self, env):  ·L66
- def _trim(self, dq, now_ts):  ·L81
- def _angle_pct_min(self, dq):  ·L92
- def _delta(self, dq):  ·L103
- async def _tick(self):  ·L112

## modules/analyst/meta_corr.py
- def _ranks(arr):  ·L44
- def _pearson(a, b):  ·L56
- def _spearman(a, b):  ·L62
- def _kendall(a, b):  ·L67
- def _dcor(a, b):  ·L80
- class MetaCorr(NeirModule):  ·L102
- async def on_start(self):  ·L106
- async def on_stop(self):  ·L133
- def _fail(self, what, exc):  ·L137
- def _feed_tick(self, symbol, price, qty, is_sell, ts_ms):  ·L145

## modules/analyst/news.py
- class NewsAggregator(NeirModule):  ·L64
- async def on_start(self):  ·L68
- async def _poll(self):  ·L95
- async def _poll_rss(self):  ·L128
- def _feed_name(url):  ·L160
- def _parse_rss(xml_bytes):  ·L167
- def _parse_date(raw):  ·L193
- def _classify_event(text):  ·L205
- def has(words):  ·L209
- async def _emit_article(self, title, desc, source, url, published):  ·L237

## modules/analyst/regression.py
- def _fit(rows):  ·L43
- def _atr(rows, period):  ·L60
- def _sma(closes, period):  ·L72
- def _pivot_idx(vals, k, kind):  ·L78
- def _prognosis_beam(rows, tf, slope, strat_tf, pivot_k, fresh_mult, weight_min):  ·L98
- class Regression(NeirModule):  ·L141
- async def on_start(self):  ·L145
- async def on_stop(self):  ·L184
- async def _on_candles(self, env):  ·L189
- async def _on_price(self, env):  ·L209

## modules/analyst/rsi.py
- def rsi_value(closes, period):  ·L22
- class RsiState(NeirModule):  ·L39
- async def on_start(self):  ·L43
- async def on_stop(self):  ·L59
- async def _on_candles(self, env):  ·L62
- def _emit(self, tf):  ·L87

## modules/analyst/spoof.py
- class Spoof(NeirModule):  ·L28
- async def on_start(self):  ·L32
- async def on_stop(self):  ·L52
- async def _on_print(self, env):  ·L58
- async def _on_book(self, env):  ·L66
- def _track(self, w, key, pr, q, now_s):  ·L117
- def _prints_at(self, side, level, now_ms):  ·L138
- def approached(self, side, level, price):  ·L154
- def _judge(self, w, key, now_ms):  ·L163

## modules/analyst/spring.py
- def vwap_value(rows):  ·L24
- def oi_slope_pct(oi_deque, window_ms, now_ms):  ·L32
- class Spring(NeirModule):  ·L48
- async def on_start(self):  ·L52
- async def on_stop(self):  ·L79
- async def _on_candles(self, env):  ·L84
- async def _on_oi(self, env):  ·L105
- async def _publish(self):  ·L117

## modules/analyst/sr.py
- def _true_atr(rows):  ·L31
- class SrAnalyzer(NeirModule):  ·L48
- async def on_start(self):  ·L52
- async def on_stop(self):  ·L80
- async def _on_candles(self, env):  ·L85
- def _maybe_stamp(self, force=False):  ·L112
- def _zones_for_scale(self, tf):  ·L149
- def _find_barriers(self):  ·L195

## modules/analyst/zones_plus.py
- def _atr(rows, period):  ·L30
- class ZonesPlus(NeirModule):  ·L42
- async def on_start(self):  ·L46
- async def on_stop(self):  ·L80
- def _resonance(self, tf, low, high):  ·L85
- async def _on_candles(self, env):  ·L96
- def _publish_fvg(self, key, f):  ·L219

## modules/market/__init__.py
- """Рыночные модули NeirOS-M: сбор и синтез данных."""

## modules/market/collector.py
- class RingCanvas:  ·L76
- def __init__(self, size):  ·L82
- def push(self, row):  ·L93
- def last_ts(self):  ·L105
- def get_last(self, n):  ·L108
- def all_rows(self):  ·L117
- def window_rows(self, win_start, need, base_ms):  ·L125
- def load_rows(self, rows):  ·L148
- def aggregate(rows):  ·L189
- class Collector(NeirModule):  ·L204

## modules/market/lenses.py
- class Lens:  ·L47
- def __init__(self, half_life_sec):  ·L53
- def update(self, ts_sec, price, qty, is_sell):  ·L64
- def warm(self, ts_sec, close, volume, delta):  ·L89
- def read_row(self):  ·L116
- class Lenses(NeirModule):  ·L132
- async def on_start(self):  ·L137
- async def _on_warmup(self, env):  ·L164
- async def _on_tick(self, env):  ·L195
- def _snapshot(self, ts_ms):  ·L214

## modules/market/oi_cascade.py
- class OiCascade(NeirModule):  ·L32
- async def on_start(self):  ·L37
- async def on_stop(self):  ·L68
- async def _on_price(self, env):  ·L74
- async def _on_card(self, env):  ·L90
- async def _on_liquidation(self, env):  ·L104
- def _publish_burst(self, now, rate):  ·L116
- async def _poll_oi(self):  ·L139
- async def _watch_check(self):  ·L166
- def _arm_zones(self, now):  ·L173

## modules/market/quota.py
- class QuotaLedger:  ·L41
- def __init__(self, weight_limit=2000.0, order_burst_limit=50,  ·L60
- def note(self, weight=1.0, dealer=False):  ·L73
- def note_order(self):  ·L84
- def _prune(self, now):  ·L94
- def weight_last_min(self, dealer=None):  ·L102
- def orders_last_10s(self):  ·L111
- def pressure(self):  ·L121
- def dealer_pressure(self):  ·L129
- def throttled(self, frac=0.75):  ·L135

## modules/scenario/__init__.py
- """Торговые модули NeirOS-M: сценарии, риски, исполнение."""

## modules/scenario/writer.py
- def tf_label(tf_sec):  ·L79
- def topup_leverage(leverage, lev_cap, min_notional, margin_usdt):  ·L93
- def _vec_entry(*, kind, value, weight, push, age_sec, state,  ·L109
- class ScenarioWriter(NeirModule):  ·L152
- async def on_start(self):  ·L156
- async def on_stop(self):  ·L323
- async def _lookup_picture(self):  ·L330
- async def _on_card(self, env):  ·L346
- async def _on_candles(self, env):  ·L487
- async def _on_price(self, env):  ·L509

## modules/services/__init__.py
- """Служебные модули NeirOS-M: хранение и учёт."""

## modules/services/keeper.py
- class Keeper(NeirModule):  ·L37
- async def on_start(self):  ·L44
- async def on_stop(self):  ·L82
- async def _on_candles(self, env):  ·L97
- async def _on_card(self, env):  ·L123
- def _enqueue(self, item):  ·L130
- async def _writer_loop(self):  ·L141
- def _connect(self):  ·L160
- def _write_candles(self, rows):  ·L198
- def _write_card(self, card):  ·L206

## modules/services/registrar.py
- class Registrar(NeirModule):  ·L27
- async def on_start(self):  ·L33
- async def on_stop(self):  ·L56
- async def _on_trust(self, env):  ·L62
- async def _on_draft(self, env):  ·L84

## modules/trading/__init__.py
- """Торговые модули NeirOS-M: сценарии, риски, исполнение."""

## modules/trading/dealer.py
- class Dealer(NeirModule):  ·L48
- async def on_start(self):  ·L58
- async def _balance_boot(self):  ·L157
- async def _boot_network(self):  ·L173
- async def on_stop(self):  ·L217
- async def _on_card(self, env):  ·L231
- async def _on_verdict(self, env):  ·L244
- async def _open(self, verdict, scenario, mode, note, force=False):  ·L335
- async def _on_price(self, env):  ·L474
- async def _check_pending(self):  ·L485

## modules/trading/risk.py
- class RiskManager(NeirModule):  ·L38
- async def on_start(self):  ·L42
- async def _fetch_time_offset(self, acct):  ·L115
- async def _refresh_time_offset(self):  ·L134
- async def _available_usdt(self, acct):  ·L141
- async def _on_card(self, env):  ·L174
- async def _balance_check(self):  ·L203
- async def _on_price(self, env):  ·L223
- async def _evaluate(self, scenario_seq, scenario_conf, f, created_ms=0):  ·L278

## modules/tuner/__init__.py
- """Модуль автотюнера NeirOS-M."""

## modules/tuner/tuner.py
- def _pack_base(*, data, trail, trust, totals, fstep, trail_at, act_at,  ·L67
- def w_str(buf, text):  ·L71
- def w_pairs(buf, source):  ·L75
- def w_streams(buf, source):  ·L82
- def w_floats(buf, source):  ·L90
- def _unpack_base(blob):  ·L113
- class _R:  ·L116
- def __init__(self, raw):  ·L117
- def take(self, n):  ·L120
- def u32(self):  ·L127

## modules/ui/__init__.py
- """Экранные модули NeirOS-M."""

## modules/ui/screen.py
- class Screen(NeirModule):  ·L17
- async def on_start(self):  ·L22
- def _qt_worker(self, window_cls):  ·L53
- def _closing_pulse(self):  ·L71
- async def _on_candles(self, env):  ·L85
- async def _on_price(self, env):  ·L106
- async def _on_card_kind(self, env):  ·L111
- async def _pull_health(self):  ·L118
- async def on_stop(self):  ·L128

## project.env
- OPENAI_BASE_URL=http://localhost:1234/v1

## README.md
- # NeirOS-M  ·L1
- ## Быстрый старт  ·L6
- # cp config/keys.example.ini config/keys.ini     # Linux/macOS  ·L11
- ### Секции ключей  ·L19
- ## Как это устроено  ·L39
- ## Торговля: контуры и режимы  ·L59
- ### Что делает рубильник  ·L89
- ### Защита позиции  ·L113
- ## Проверки  ·L120
- ## Настройки  ·L132

## requirements.txt
- # NeirOS-M — библиотеки проекта. Установка: pip install -r requirements.txt

## risks
- """Систематизированные риски проекта NeirOS-M.

## tests/__init__.py
- """Тесты ядра NeirOS-M. Не являются частью системы."""

## tests/run_all.py
- def run_one(name, timeout):  ·L34
- def main():  ·L65

## tests/smoke_collector_init.py
- class _FakeResp:  ·L40
- async def json(self, content_type=None):  ·L48
- async def read(self):  ·L51
- async def __aenter__(self):  ·L54
- async def __aexit__(self, *exc):  ·L57
- class _FakeSession:  ·L61
- def __init__(self, *a, **kw):  ·L62
- def get(self, url, params=None, timeout=None, **kw):  ·L65
- async def __aenter__(self):  ·L70
- async def __aexit__(self, *exc):  ·L73

## tests/smoke_core.py
- class Pump(NeirModule):  ·L18
- async def on_start(self):  ·L23
- async def _beat(self):  ·L27
- class Sink(NeirModule):  ·L34
- async def on_start(self):  ·L40
- async def _on_num(self, env):  ·L46
- async def _on_personal(self, env):  ·L50
- async def run_test():  ·L54
- def main():  ·L100

## tests/smoke_quota.py
- async def run_test():  ·L31
- def main():  ·L149

## tests/smoke_stage10.py
- def db_cards(cfg, kind):  ·L32
- def db_seq_stats(cfg):  ·L46
- async def run_test():  ·L58

## tests/smoke_stage11.py
- def db_count(cfg, kind):  ·L25
- async def run_test():  ·L37

## tests/smoke_stage12.py
- def db_cards(cfg, kind):  ·L23
- def db_seq_stats(cfg):  ·L37
- async def run_test():  ·L49

## tests/smoke_stage13.py
- def last_picture(cfg):  ·L30
- def _clean_db(cfg):  ·L43
- class ZoneSource(NeirModule):  ·L50
- async def on_start(self):  ·L55
- async def _beat(self):  ·L59
- async def price(kernel, value, ttl=2.0):  ·L74
- async def run_test():  ·L80
- async def settle(sec=1.5):  ·L102

## tests/smoke_stage14.py
- def db_cards(cfg, kind):  ·L29
- def db_seq_stats(cfg):  ·L42
- def _clean_db(cfg):  ·L54
- def row(ts, o, h, l, c):  ·L61
- def make_candles():  ·L65
- async def run_test():  ·L100
- async def price(p):  ·L119
- async def feed(rows):  ·L130

## tests/smoke_stage15.py
- def db_cards(cfg, kind):  ·L32
- def _clean_db(cfg):  ·L45
- class Source(NeirModule):  ·L52
- async def on_start(self):  ·L56
- async def _beat(self):  ·L60
- async def run_test():  ·L95
- async def price(v):  ·L120
- async def liq(qty):  ·L125

## tests/smoke_stage16.py
- async def run_test():  ·L23
- async def price(v):  ·L36
- async def liq():  ·L41
- async def oi(v):  ·L46
- def snap():  ·L50

## tests/smoke_stage17.py
- class Probe(NeirModule):  ·L23
- def __init__(self, kernel):  ·L27
- async def on_start(self):  ·L31
- def _on(self, env):  ·L34
- def batch(ts0, start, step, n=60, zigzag=0.0):  ·L38
- async def run_test():  ·L52
- async def feed(start, step, zigzag=0.0):  ·L68
- def drafts(kind):  ·L77
- def last_trend():  ·L80
- def beam_rows(ts0):  ·L143

## tests/smoke_stage18.py
- class Probe(NeirModule):  ·L23
- def __init__(self, kernel):  ·L27
- async def on_start(self):  ·L31
- def _on(self, env):  ·L34
- def batch(ts0, n=60):  ·L38
- def picture(seq, below):  ·L51
- async def run_test():  ·L78
- def price(p):  ·L99
- def env(r, phase, speed, efficiency=None):  ·L105
- def scenarios():  ·L114

## tests/smoke_stage19.py
- class Probe(NeirModule):  ·L30
- def __init__(self, kernel):  ·L33
- async def on_start(self):  ·L37
- def _on(self, env):  ·L40
- async def market_price(symbol):  ·L44
- async def run_test():  ·L51
- async def price(p):  ·L74

## tests/smoke_stage2.py
- def read_cards(cfg):  ·L26
- def _clean_db(cfg):  ·L45
- class Source(NeirModule):  ·L52
- async def on_start(self):  ·L57
- async def _beat(self):  ·L61
- def read_test_cards(cfg):  ·L76
- def db_seq_stats(cfg):  ·L97
- async def run_phase(cfg, expect_cards):  ·L110
- async def run_test():  ·L130
- def main():  ·L177

## tests/smoke_stage20.py
- class Probe(NeirModule):  ·L25
- def __init__(self, kernel):  ·L28
- async def on_start(self):  ·L32
- def _on(self, env):  ·L35
- async def run_test():  ·L39
- def feed(label, price, qty, is_sell, ts):  ·L55

## tests/smoke_stage21.py
- class Probe(NeirModule):  ·L28
- def __init__(self, kernel):  ·L31
- async def on_start(self):  ·L35
- def _on(self, env):  ·L39
- def book(mid, wall=None, wall_side="ask", wall_qty=500.0):  ·L43
- async def run_test():  ·L61
- def feed_book(mid, wall=None, wall_qty=500.0):  ·L80
- def feed_print(p, is_sell=True):  ·L84
- def alerts():  ·L90

## tests/smoke_stage22.py
- class Probe(NeirModule):  ·L30
- def __init__(self, kernel):  ·L33
- async def on_start(self):  ·L37
- def _on(self, env):  ·L40
- async def run_test():  ·L44
- def snap():  ·L92
- async def wait_snap(n, timeout=20.0):  ·L95

## tests/smoke_stage23.py
- class Probe(NeirModule):  ·L26
- def __init__(self, kernel):  ·L29
- async def on_start(self):  ·L33
- def _on(self, env):  ·L36
- def candles(ts0, closes):  ·L40
- async def run_test():  ·L48
- def _rows(first_close, next_closes, ts0):  ·L129

## tests/smoke_stage24.py
- class Probe(NeirModule):  ·L28
- def __init__(self, kernel):  ·L31
- async def on_start(self):  ·L35
- def _on(self, env):  ·L38
- async def run_test():  ·L42

## tests/smoke_stage25.py
- class Probe(NeirModule):  ·L29
- def __init__(self, kernel):  ·L32
- async def on_start(self):  ·L36
- def _on(self, env):  ·L39
- async def run_test():  ·L43

## tests/smoke_stage26.py
- class Probe(NeirModule):  ·L28
- def __init__(self, kernel):  ·L31
- async def on_start(self):  ·L35
- def _on(self, env):  ·L38
- async def run_test():  ·L42

## tests/smoke_stage27.py
- class Probe(NeirModule):  ·L28
- def __init__(self, kernel):  ·L31
- async def on_start(self):  ·L35
- def _on(self, env):  ·L38
- def snap(seq, dist_up, dist_dn, trend, flow, package, outcome, quality):  ·L42
- async def run_test():  ·L62

## tests/smoke_stage29.py
- class Probe(NeirModule):  ·L25
- def __init__(self, kernel):  ·L28
- async def on_start(self):  ·L32
- def _on(self, env):  ·L35
- def bars(prices, ts0):  ·L39
- class FakeCollector:  ·L48
- def __init__(self, rows):  ·L53
- def get_ring(self, tf_sec):  ·L60
- async def run_test():  ·L78

## tests/smoke_stage3.py
- class Observer(NeirModule):  ·L27
- async def on_start(self, ):  ·L31
- async def _on_candles(self, env):  ·L47
- async def _on_price(self, env):  ·L51
- async def _on_book(self, env):  ·L54
- def db_candles(cfg, symbol, tf_sec, limit=5):  ·L58
- async def run_test():  ·L72
- def main():  ·L149

## tests/smoke_stage30.py
- class Probe(NeirModule):  ·L27
- def __init__(self, kernel):  ·L30
- async def on_start(self):  ·L34
- def _on(self, env):  ·L37
- async def run_test():  ·L41

## tests/smoke_stage31.py
- class Probe(NeirModule):  ·L52
- def __init__(self, kernel):  ·L55
- async def on_start(self):  ·L59
- def _on(self, env):  ·L62
- class FakeKeeper:  ·L66
- def __init__(self, rows):  ·L69
- async def get_last_candles(self, symbol, tf, limit):  ·L72
- def make_news(kernel):  ·L76
- async def run_test():  ·L87
- def news_card(seq, etype_, expected_):  ·L178

## tests/smoke_stage32.py
- class Probe(NeirModule):  ·L32
- def __init__(self, kernel):  ·L35
- async def on_start(self):  ·L39
- def _on(self, env):  ·L42
- def make_spring(kernel):  ·L46
- class FakeCollector:  ·L64
- def __init__(self, rows):  ·L68
- def get_ring(self, tf_sec):  ·L75
- def make_fractals(kernel, closes):  ·L93
- async def run_test():  ·L120

## tests/smoke_stage33.py
- class Probe(NeirModule):  ·L37
- def __init__(self, kernel):  ·L40
- async def on_start(self):  ·L44
- def _on(self, env):  ·L47
- def table_exists(cfg, name):  ·L51
- def row_count(cfg, table):  ·L60
- async def run_test():  ·L67
- def main():  ·L196

## tests/smoke_stage34.py
- class Env:  ·L26
- def __init__(self, payload):  ·L27
- def make_risk(kernel):  ·L31
- async def fake_available(value):  ·L51
- def commands(sent):  ·L55
- async def run_test():  ·L59

## tests/smoke_stage35.py
- def make_risk(kernel, min_notional):  ·L29
- def make_tuner(kernel):  ·L50
- async def run_test():  ·L65

## tests/smoke_stage36.py
- async def bus_checks(checks):  ·L41
- async def handler(e):  ·L69
- async def drop_checks(checks, bus):  ·L92
- async def handler(e):  ·L96
- def contour_checks(checks):  ·L119
- def setmode(contour, mode, live_enabled):  ·L129
- def trading_checks(checks):  ·L216
- def tag(ts, pid):  ·L251
- async def duplicate_check(checks):  ·L269
- class _Resp:  ·L283

## tests/smoke_stage4.py
- def db_cards(cfg, kind):  ·L26
- def db_seq_stats(cfg):  ·L40
- async def run_test():  ·L52
- def main():  ·L128

## tests/smoke_stage5.py
- def db_cards(cfg, kind):  ·L26
- def db_seq_stats(cfg):  ·L40
- async def run_test():  ·L52
- def main():  ·L121

## tests/smoke_stage6.py
- def db_cards(cfg, kind):  ·L27
- def db_seq_stats(cfg):  ·L41
- async def run_test():  ·L53
- def main():  ·L133

## tests/smoke_stage7.py
- def db_count(cfg, kind):  ·L29
- def db_last(cfg, kind):  ·L41
- class Observer(NeirModule):  ·L56
- async def on_start(self):  ·L60
- async def _beat(self):  ·L65
- async def run_test():  ·L74
- def main():  ·L172

## tests/smoke_stage8.py
- def db_count(cfg, kind):  ·L31
- class Observer(NeirModule):  ·L43
- async def on_start(self):  ·L46
- async def _beat(self):  ·L51
- async def run_test():  ·L60
- def main():  ·L153

## tests/smoke_stage9.py
- def main():  ·L17

## tools/_rep2_tmp.py
- def fix_mojibake(txt):  ·L10
- def load_cards():  ·L19
- def med(v):  ·L34
- def main():  ·L39

## tools/_sim_tmp.py
- def load_paths(c):  ·L22
- def run_variant(paths, T, S, B, A, D):  ·L51
- def dd_of(outs):  ·L97
- def main():  ·L106

## tools/_sim2_tmp.py
- def filtered(paths, delay, max_adv):  ·L11
- def best_variant(P):  ·L27
- def main():  ·L43

## tools/_survival_tmp.py
- def scen_fields(f):  ·L18
- def main():  ·L26

## tools/config.old.bak
- [core]

## ui/__init__.py
- """Экран NeirOS-M: мост данных + окно Qt в отдельном потоке."""

## ui/bridge.py
- class UiBridge:  ·L10
- def __init__(self):  ·L11
- def update(self, **kwargs):  ·L16
- def snapshot(self):  ·L20

## ui/miniterm.py
- class MiniTerm(QWidget):  ·L24
- def __init__(self, bridge, symbol):  ·L25
- def _redraw(self):  ·L50
- def _check_close(self):  ·L100

## ui/widgets/__init__.py
- """Элементы экрана NeirOS-M."""

## ЖУРНАЛ.md
- # ЖУРНАЛ ПРОЕКТА NeirOS-M  ·L1
- ## 12.10.2026 — Переломный момент: роли контуров и консенсус по матрице  ·L9
- ### Контекст на начало  ·L11
- ### Сделано  ·L18
- ### Решено (КАНОН)  ·L26
- ### БД и чистка  ·L52
- ### Отложено в BACKLOG (замечания хозяина)  ·L60
- ### Решено обдумать / открыто  ·L71
- ### ПЛАН  ·L79
- ### Статус  ·L92

## идеи_для_анализа_на_интеграцию.md
- # Идеи для анализа на интеграцию (legacy)  ·L1
- ## Статусы  ·L15
- ## 1. Старые идеи — итоговый статус  ·L27
- ### Иерархия датчиков Manifest 2.0 → ✅ Реализовано  ·L29
- ### Четыре среза GLM (старый диалог)  ·L36
- ### Прочие старые идеи (из `ЖУРНАЛ.md`)  ·L45
- ## 2. Система хранения данных — 🔍 К анализу (до этого не разбиралась)  ·L55
- ## 3. Кандидаты на интеграцию (сначала ревалидация)  ·L84
- ## Правила работы с этим файлом  ·L94
