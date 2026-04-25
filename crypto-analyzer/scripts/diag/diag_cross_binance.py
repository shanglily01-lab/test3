"""
诊断 CROSS/USDT 实盘真相 - 直接查币安 API (只读, 不下/不撤任何单)

排查问题: 远程库里 CROSS 唯一一条记录是 11:25 paper LIMIT, 13:26 timeout 取消,
live_sync_status=NULL, fill_time=NULL. 但用户在币安 U 本位看到了实盘 CROSS 仓位.
要确认实盘那个仓位的真实开仓时间/订单 ID/触发路径.

输出:
1. 当前 CROSSUSDT 实盘持仓 (positionRisk)
2. 当前所有挂单 (openOrders)
3. 近 24h 所有订单 (allOrders) - 含 LIMIT/MARKET/SL/TP
4. 近 24h 成交记录 (userTrades) - 实际成交价/手续费/时间
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import sys
import time
from urllib.parse import urlencode

import requests

# UTF-8 输出
if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# 加载 .env
ENV_PATH = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
if os.path.exists(ENV_PATH):
    with open(ENV_PATH, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

API_KEY = os.getenv("BINANCE_API_KEY", "").strip()
API_SECRET = os.getenv("BINANCE_API_SECRET", "").strip()
BASE = "https://fapi.binance.com"
SYMBOL = "CROSSUSDT"

if not API_KEY or not API_SECRET:
    print("ERROR: BINANCE_API_KEY / BINANCE_API_SECRET 未配置")
    sys.exit(1)


def signed_get(path: str, params: dict | None = None) -> dict | list:
    p = dict(params or {})
    p["timestamp"] = int(time.time() * 1000)
    p["recvWindow"] = 10000
    qs = urlencode(p)
    sig = hmac.new(API_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()
    url = f"{BASE}{path}?{qs}&signature={sig}"
    r = requests.get(url, headers={"X-MBX-APIKEY": API_KEY}, timeout=10)
    r.raise_for_status()
    return r.json()


def section(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def fmt_ts(ms: int | None) -> str:
    if not ms:
        return "(none)"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ms / 1000))


def main() -> None:
    section(f"[1] 当前持仓 positionRisk - {SYMBOL}")
    try:
        rows = signed_get("/fapi/v2/positionRisk", {"symbol": SYMBOL})
        if not rows:
            print("  (无持仓数据)")
        for r in rows:
            amt = float(r.get("positionAmt", 0))
            if amt == 0:
                # 双向持仓模式下会有 LONG/SHORT 两条空记录
                print(f"  [空] side={r.get('positionSide')} amt=0")
                continue
            print(
                f"  >>> 持仓 side={r.get('positionSide')} amt={amt} "
                f"entry={r.get('entryPrice')} mark={r.get('markPrice')} "
                f"upnl={r.get('unRealizedProfit')} lev={r.get('leverage')} "
                f"updateTime={fmt_ts(int(r.get('updateTime', 0)))}"
            )
    except Exception as e:
        print(f"  ERR: {e}")

    section(f"[2] 当前挂单 openOrders - {SYMBOL}")
    try:
        rows = signed_get("/fapi/v1/openOrders", {"symbol": SYMBOL})
        print(f"  共 {len(rows)} 单")
        for r in rows:
            print(
                f"  orderId={r.get('orderId')} clientOrderId={r.get('clientOrderId')}\n"
                f"      side={r.get('side')} positionSide={r.get('positionSide')} "
                f"type={r.get('type')} status={r.get('status')}\n"
                f"      price={r.get('price')} stopPrice={r.get('stopPrice')} "
                f"qty={r.get('origQty')} executed={r.get('executedQty')}\n"
                f"      time={fmt_ts(int(r.get('time', 0)))} "
                f"updateTime={fmt_ts(int(r.get('updateTime', 0)))}"
            )
    except Exception as e:
        print(f"  ERR: {e}")

    section(f"[3] 近 48h 所有订单 allOrders - {SYMBOL} (含已成交/已撤销)")
    try:
        start_ms = int(time.time() * 1000) - 48 * 3600 * 1000
        rows = signed_get(
            "/fapi/v1/allOrders",
            {"symbol": SYMBOL, "startTime": start_ms, "limit": 200},
        )
        print(f"  共 {len(rows)} 单")
        for r in rows:
            print(
                f"  orderId={r.get('orderId')} side={r.get('side')} "
                f"posSide={r.get('positionSide')} type={r.get('type')} "
                f"status={r.get('status')}"
            )
            print(
                f"      price={r.get('price')} stopPrice={r.get('stopPrice')} "
                f"avgPrice={r.get('avgPrice')} qty={r.get('origQty')} "
                f"executed={r.get('executedQty')} cumQuote={r.get('cumQuote')}"
            )
            print(
                f"      reduceOnly={r.get('reduceOnly')} closePosition={r.get('closePosition')} "
                f"timeInForce={r.get('timeInForce')} workingType={r.get('workingType')}"
            )
            print(
                f"      time={fmt_ts(int(r.get('time', 0)))} "
                f"updateTime={fmt_ts(int(r.get('updateTime', 0)))} "
                f"origType={r.get('origType')}"
            )
            print(
                f"      clientOrderId={r.get('clientOrderId')!r}"
            )
            print()
    except Exception as e:
        print(f"  ERR: {e}")

    section(f"[4] 近 48h 成交记录 userTrades - {SYMBOL}")
    try:
        start_ms = int(time.time() * 1000) - 48 * 3600 * 1000
        rows = signed_get(
            "/fapi/v1/userTrades",
            {"symbol": SYMBOL, "startTime": start_ms, "limit": 100},
        )
        print(f"  共 {len(rows)} 笔成交")
        for r in rows:
            print(
                f"  tradeId={r.get('id')} orderId={r.get('orderId')} "
                f"side={r.get('side')} posSide={r.get('positionSide')}"
            )
            print(
                f"      price={r.get('price')} qty={r.get('qty')} "
                f"quoteQty={r.get('quoteQty')} commission={r.get('commission')}{r.get('commissionAsset')} "
                f"realizedPnl={r.get('realizedPnl')}"
            )
            print(
                f"      maker={r.get('maker')} buyer={r.get('buyer')} "
                f"time={fmt_ts(int(r.get('time', 0)))}"
            )
            print()
    except Exception as e:
        print(f"  ERR: {e}")


if __name__ == "__main__":
    main()
