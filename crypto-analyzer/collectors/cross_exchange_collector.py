#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
cross_exchange_collector.py
============================
每分钟采集 OKX / Bybit / Binance 的价格和资金费率，
写入 cross_exchange_prices 表，用于跨交易所溢价计算。

用法:
  .venv/Scripts/python.exe collectors/cross_exchange_collector.py
"""

import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).parent.parent))

import pymysql
import requests
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

_DB_CFG = {
    "host":      os.getenv("DB_HOST", "localhost"),
    "port":      int(os.getenv("DB_PORT", 3306)),
    "user":      os.getenv("DB_USER", "root"),
    "password":  os.getenv("DB_PASSWORD", ""),
    "database":  os.getenv("DB_NAME", "binance-data"),
    "charset":   "utf8mb4",
    "autocommit": True,
}

INTERVAL_SECS = 60

# 采集的标的（Binance 格式 → OKX/Bybit 格式映射）
SYMBOLS = {
    "BTC/USDT":     {"okx": "BTC-USDT-SWAP",  "bybit": "BTCUSDT",  "binance": "BTCUSDT"},
    "ETH/USDT":     {"okx": "ETH-USDT-SWAP",  "bybit": "ETHUSDT",  "binance": "ETHUSDT"},
    "BNB/USDT":     {"okx": "BNB-USDT-SWAP",  "bybit": "BNBUSDT",  "binance": "BNBUSDT"},
    "SOL/USDT":     {"okx": "SOL-USDT-SWAP",  "bybit": "SOLUSDT",  "binance": "SOLUSDT"},
    "XRP/USDT":     {"okx": "XRP-USDT-SWAP",  "bybit": "XRPUSDT",  "binance": "XRPUSDT"},
    "DOGE/USDT":    {"okx": "DOGE-USDT-SWAP", "bybit": "DOGEUSDT", "binance": "DOGEUSDT"},
    "AVAX/USDT":    {"okx": "AVAX-USDT-SWAP", "bybit": "AVAXUSDT", "binance": "AVAXUSDT"},
}

_session = requests.Session()
_session.headers.update({"User-Agent": "crypto-analyzer/1.0"})


def _get(url: str, params: dict = None, timeout: int = 5) -> Optional[dict]:
    try:
        r = _session.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logger.warning(f"GET {url} failed: {e}")
        return None


def fetch_binance(symbol: str) -> Optional[dict]:
    """Binance perp mark price + funding rate"""
    data = _get("https://fapi.binance.com/fapi/v1/premiumIndex", {"symbol": symbol})
    if not data:
        return None
    return {
        "price":        float(data.get("markPrice", 0)),
        "funding_rate": float(data.get("lastFundingRate", 0)),
    }


def fetch_okx(inst_id: str) -> Optional[dict]:
    """OKX perpetual swap ticker"""
    data = _get("https://www.okx.com/api/v5/market/ticker", {"instId": inst_id})
    if not data or data.get("code") != "0" or not data.get("data"):
        return None
    d = data["data"][0]
    # Funding rate from separate endpoint
    fr_data = _get("https://www.okx.com/api/v5/public/funding-rate", {"instId": inst_id})
    fr = 0.0
    if fr_data and fr_data.get("code") == "0" and fr_data.get("data"):
        fr = float(fr_data["data"][0].get("fundingRate", 0))
    return {
        "price":        float(d.get("last", 0)),
        "funding_rate": fr,
    }


def fetch_bybit(symbol: str) -> Optional[dict]:
    """Bybit linear perp ticker"""
    data = _get("https://api.bybit.com/v5/market/tickers",
                {"category": "linear", "symbol": symbol})
    if not data or data.get("retCode") != 0:
        return None
    lst = data.get("result", {}).get("list", [])
    if not lst:
        return None
    d = lst[0]
    return {
        "price":        float(d.get("lastPrice", 0)),
        "funding_rate": float(d.get("fundingRate", 0)),
    }


def collect_once() -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = []

    for sym, ids in SYMBOLS.items():
        binance = fetch_binance(ids["binance"])
        okx     = fetch_okx(ids["okx"])
        bybit   = fetch_bybit(ids["bybit"])

        if not binance or not binance["price"]:
            continue

        bp = binance["price"]
        okx_price  = okx["price"]  if okx  else None
        bybit_price = bybit["price"] if bybit else None

        okx_spread  = ((okx_price  / bp - 1) * 100) if okx_price  else None
        bybit_spread = ((bybit_price / bp - 1) * 100) if bybit_price else None

        rows.append((
            sym, bp, okx_price, bybit_price,
            okx_spread, bybit_spread,
            okx["funding_rate"]   if okx   else None,
            bybit["funding_rate"] if bybit else None,
            now,
        ))

        if okx_spread is not None and abs(okx_spread) > 0.1:
            logger.info(
                f"{sym}: Binance={bp:.4f}  OKX={okx_price:.4f}  "
                f"spread={okx_spread:+.4f}%"
            )

    if not rows:
        return

    try:
        conn = pymysql.connect(**_DB_CFG)
        with conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO cross_exchange_prices
                   (symbol, binance_price, okx_price, bybit_price,
                    okx_spread_pct, bybit_spread_pct,
                    okx_funding_rate, bybit_funding_rate, collected_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                rows,
            )
        conn.close()
        logger.debug(f"Inserted {len(rows)} cross-exchange price rows")
    except Exception as e:
        logger.error(f"DB write failed: {e}")


def run() -> None:
    logger.info(f"Cross-exchange collector started. Interval: {INTERVAL_SECS}s")
    while True:
        try:
            collect_once()
        except Exception as e:
            logger.error(f"collect_once error: {e}")
        time.sleep(INTERVAL_SECS)


if __name__ == "__main__":
    run()
