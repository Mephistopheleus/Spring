"""Диагностика ключей Binance. Не часть системы.

Берёт пару из config/keys.ini [binance] и прогоняет её тремя путями
(raw aiohttp, binance-futures-connector, python-binance) против двух
тестнетов: ФЬЮЧЕРСНОГО и СПОТОВОГО. Печатает дословные ответы биржи.
Запуск:  python -X utf8 _key_check.py
"""
import asyncio
import hashlib
import hmac
import sys
import time
from pathlib import Path
from urllib.parse import urlencode

import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parent))
from core.config import KernelConfig  # noqa: E402

FUT = "https://testnet.binancefuture.com"
SPOT = "https://testnet.binance.vision"


def signed_query(key, secret, extra=None):
    params = {"timestamp": int(time.time() * 1000), "recvWindow": 5000}
    if extra:
        params.update(extra)
    q = urlencode(params)
    sig = hmac.new(secret.encode(), q.encode(), hashlib.sha256).hexdigest()
    return f"{q}&signature={sig}"


async def raw(name, base, path, key, secret):
    url = f"{base}{path}?{signed_query(key, secret)}"
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, headers={"X-MBX-APIKEY": key},
                             timeout=aiohttp.ClientTimeout(total=15)) as r:
                print(f"  [{name}] HTTP {r.status}: "
                      f"{(await r.text())[:110]}")
    except Exception as e:
        print(f"  [{name}] связь: {type(e).__name__}: {str(e)[:90]}")


def via_connector(key, secret, base):
    try:
        from binance.um_futures import UMFutures
        c = UMFutures(key=key, secret=secret, base_url=base)
        bal = c.balance()
        usdt = next((b for b in bal if b.get("asset") == "USDT"), {})
        print(f"  [binance-futures-connector] OK! баланс USDT: "
              f"{usdt.get('balance')}")
    except Exception as e:
        print(f"  [binance-futures-connector] отказ: {str(e)[:110]}")


def via_python_binance(key, secret):
    try:
        from binance import Client
        c = Client(key, secret, testnet=True)
        bal = c.futures_account_balance()
        usdt = next((b for b in bal if b.get("asset") == "USDT"), {})
        print(f"  [python-binance] OK! баланс USDT: {usdt.get('balance')}")
    except Exception as e:
        print(f"  [python-binance] отказ: {type(e).__name__}: {str(e)[:100]}")


async def main():
    cfg = KernelConfig()
    # Диагностика по КОНТУРАМ: какая биржа выбрана и какие ключи есть.
    print("=== КОНТУРЫ ===")
    print(f"активный контур: {cfg.contour.upper()}  режим: {cfg.mode.upper()}")
    if cfg.trading_blocked_reason:
        print(f"почему не реал: {cfg.trading_blocked_reason}")

    def show(label, acct):
        if acct is None:
            print(f"  {label}: ключей НЕТ")
        else:
            print(f"  {label}: ключ есть (длина {len(acct['key'])}, "
                  f"префикс {acct['key'][:6]}...) → {acct['base']}")

    show("testnet", cfg.testnet_account)
    show("live   ", cfg.live_account)

    contour = sys.argv[1].strip().lower() if len(sys.argv) > 1 else cfg.contour
    acct = cfg.binance_account
    if acct is None:
        print(f"Для контура '{contour}' ключей нет — проверять нечего. "
              f"Заполните секцию [{cfg.TESTNET_SECTION if contour == 'testnet' else cfg.LIVE_SECTION}] в config/keys.ini")
        return
    key, secret = acct["key"], acct["secret"]
    base = acct["base"]
    print(f"Ключ: {key[:6]}...{key[-4:]} (длина {len(key)}), "
          f"секрет: длина {len(secret)}")
    print(f"--- {base} ---")
    await raw("raw aiohttp", base, "/fapi/v2/balance", key, secret)
    if acct["testnet"]:
        print("--- СПОТОВЫЙ тестнет (testnet.binance.vision) ---")
        await raw("raw aiohttp", SPOT, "/api/v3/account", key, secret)
    print("Итог: где увидишь баланс — те ключи и тот тестнет рабочие.")


if __name__ == "__main__":
    asyncio.run(main())
