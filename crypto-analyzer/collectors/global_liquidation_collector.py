#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
global_liquidation_collector.py
================================
订阅 Binance 全网强平 WebSocket 流，写入 global_liquidations 表。

  wss://fstream.binance.com/ws/!forceOrder@arr

SELL side = 多头被强平
BUY  side = 空头被强平

用法:
  .venv/Scripts/python.exe collectors/global_liquidation_collector.py
"""

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).parent.parent))

import pymysql
import websocket
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

WS_URL      = "wss://fstream.binance.com/ws/!forceOrder@arr"
RECONNECT_S = 10
_conn       = None


def _get_conn():
    global _conn
    try:
        if _conn and _conn.open:
            return _conn
    except Exception:
        pass
    _conn = pymysql.connect(**_DB_CFG)
    return _conn


def _insert(symbol: str, side: str, qty: float, price: float,
            avg_fill: float, usd_val: float, event_ms: int) -> None:
    event_dt = datetime.utcfromtimestamp(event_ms / 1000)
    sql = """INSERT INTO global_liquidations
             (symbol, side, quantity, price, avg_fill_price, usd_value, event_time)
             VALUES (%s,%s,%s,%s,%s,%s,%s)"""
    try:
        conn = _get_conn()
        with conn.cursor() as cur:
            cur.execute(sql, (symbol, side, qty, price, avg_fill, usd_val, event_dt))
    except Exception as e:
        logger.error(f"DB insert failed: {e}")


def on_message(ws, message: str) -> None:
    try:
        data = json.loads(message)
        # Stream sends {"e":"forceOrder","E":...,"o":{...}}
        if isinstance(data, dict):
            events = [data]
        else:
            events = data  # array on combined stream

        for evt in events:
            if evt.get("e") != "forceOrder":
                continue
            o       = evt["o"]
            symbol  = o["s"]            # BTCUSDT
            side    = o["S"]            # SELL or BUY
            qty     = float(o["q"])
            price   = float(o["p"])
            avg_fill = float(o.get("ap", o["p"]))
            usd_val  = avg_fill * qty
            event_ms = int(o["T"])

            # Convert to /USDT format for consistency
            sym_slash = symbol.replace("USDT", "/USDT") if not symbol.endswith("/USDT") else symbol

            _insert(sym_slash, side, qty, price, avg_fill, usd_val, event_ms)

            if usd_val >= 500_000:
                direction = "LONG liquidated" if side == "SELL" else "SHORT liquidated"
                logger.info(
                    f"LARGE LIQ: {sym_slash} {direction}  "
                    f"qty={qty:.4f}  price={price:.4f}  usd=${usd_val:,.0f}"
                )
    except Exception as e:
        logger.warning(f"Message parse error: {e} — raw: {message[:200]}")


def on_error(ws, error) -> None:
    logger.error(f"WebSocket error: {error}")


def on_close(ws, code, msg) -> None:
    logger.warning(f"WebSocket closed (code={code}). Reconnecting in {RECONNECT_S}s...")


def on_open(ws) -> None:
    logger.info(f"Connected to {WS_URL}")


def run() -> None:
    while True:
        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            logger.error(f"WebSocket run failed: {e}")
        logger.info(f"Reconnecting in {RECONNECT_S}s...")
        time.sleep(RECONNECT_S)


if __name__ == "__main__":
    logger.info("Starting global liquidation collector...")
    run()
