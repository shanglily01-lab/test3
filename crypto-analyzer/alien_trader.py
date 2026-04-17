#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
alien_trader.py
===============
基于 Alien Lens 回测发现的两个跨标的高胜率信号：

  BUY  : 4h gradient > +0.006 (宏观上行) + 1h DISSIPATIVE/DRIVEN_DOWN (局部耗散)
  SELL : 4h gradient < -0.006 (宏观下行) + 1h ACCUMULATIVE/DRIVEN_UP  (局部假反弹)

回测结果（BTC/ETH/BNB/SOL，90天，2160根/币）：
  BUY  信号胜率: 64-68%（4币均值~66%）
  SELL 信号胜率: 61-69%（即UP率31-39%的反向）

SL/TP 基于近期振幅动态计算：
  SL = 1.5x 近6h平均振幅（空间）
  TP = 2.5x 近6h平均振幅（空间）
  → R:R ≈ 1:1.67，期望值正向

用法:
  .venv/Scripts/python.exe alien_trader.py
  .venv/Scripts/python.exe alien_trader.py --dry-run
  .venv/Scripts/python.exe alien_trader.py --symbols BTC/USDT ETH/USDT
"""

import argparse
import json
import os
import sys
import time
import threading
from datetime import datetime, timedelta
from pathlib import Path

import pymysql
import requests as req
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ── 配置 ───────────────────────────────────────────────────────────────────────

API_BASE         = "http://localhost:9021"
ACCOUNT_ID       = 2
MARGIN_PER_TRADE = 1000.0
LEVERAGE         = 5
MAX_HOLD_HOURS   = 3
CHECK_INTERVAL   = 300
FAPI             = "https://fapi.binance.com/fapi/v1/klines"

# 信号参数（与回测完全一致）
W1          = 6      # 1h 梯度窗口
W4          = 3      # 4h 梯度窗口
MACRO_THR   = 0.006  # 4h 梯度阈值
MICRO_THR   = 0.003  # 1h 梯度阈值
FLUX_THR    = 0.47   # 通量方向阈值

# SL/TP 振幅倍数
SL_MULT = 1.5
TP_MULT = 2.5
SL_MIN  = 0.003   # 最小SL 0.3%
SL_MAX  = 0.015   # 最大SL 1.5%

DEFAULT_SYMBOLS = [
    "BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT"
]

SESSION_DIR = Path(__file__).parent / "discovery_sessions"
SESSION_DIR.mkdir(exist_ok=True)

_DB_CFG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 3306)),
    "user":     os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "binance-data"),
    "charset":  "utf8mb4",
    "autocommit": True,
}


# ── 数据拉取 ───────────────────────────────────────────────────────────────────

def _binance_sym(sym: str) -> str:
    return sym.replace("/", "")

def fetch_klines(symbol: str, interval: str, limit: int) -> list[dict]:
    sym = _binance_sym(symbol)
    r = req.get(FAPI, params={"symbol": sym, "interval": interval, "limit": limit},
                timeout=10)
    r.raise_for_status()
    return [{"open": float(x[1]), "high": float(x[2]),
             "low":  float(x[3]), "close": float(x[4]),
             "vol":  float(x[5]), "buy_vol": float(x[9])}
            for x in r.json()]


# ── 工具 ───────────────────────────────────────────────────────────────────────

def _pround(value: float, ref: float) -> float:
    """Price-aware rounding: prevents round(0.000062, 4)==0.0001 for micro-price tokens."""
    import math
    if ref <= 0 or value <= 0:
        return round(value, 8)
    mag = math.floor(math.log10(ref))
    return round(value, max(4, -mag + 3))


# ── 三个维度 ───────────────────────────────────────────────────────────────────

def gradient(cs: list, n: int) -> float:
    if len(cs) < n: return 0.0
    s   = sum(c["close"] - c["open"] for c in cs[-n:])
    ref = cs[-1]["close"]
    return s / ref if ref else 0.0

def flux(cs: list, n: int) -> float:
    if len(cs) < n: return 0.5
    rs = [c["buy_vol"] / c["vol"] for c in cs[-n:] if c["vol"] > 0]
    return sum(rs) / len(rs) if rs else 0.5

def amplitude(cs: list, n: int) -> float:
    if len(cs) < n: return 0.0
    avg = sum(c["high"] - c["low"] for c in cs[-n:]) / n
    ref = cs[-1]["close"]
    return avg / ref if ref else 0.0

def classify(g: float, f: float) -> str:
    if g < -MICRO_THR:
        if f < FLUX_THR:  return "DRIVEN_DOWN"
        if f > (1-FLUX_THR): return "CONFLICT_DOWN"
        return "DISSIPATIVE"
    if g > MICRO_THR:
        if f > (1-FLUX_THR): return "DRIVEN_UP"
        if f < FLUX_THR:  return "CONFLICT_UP"
        return "ACCUMULATIVE"
    return "EQUILIBRIUM"


# ── 信号计算 ───────────────────────────────────────────────────────────────────

def compute_signal(symbol: str) -> dict | None:
    """
    返回 {"direction": "LONG"/"SHORT", "sl": float, "tp": float, "price": float,
           "g4": float, "g1": float, "phase1": str}
    或 None（无信号）
    """
    try:
        cs1h = fetch_klines(symbol, "1h", W1 + 2)
        cs4h = fetch_klines(symbol, "4h", W4 + 2)
    except Exception as e:
        logger.warning(f"{symbol}: fetch failed: {e}")
        return None

    g4     = gradient(cs4h, W4)
    g1     = gradient(cs1h, W1)
    f1     = flux(cs1h, W1)
    phase1 = classify(g1, f1)
    price  = cs1h[-1]["close"]
    amp    = amplitude(cs1h, W1)

    # 宏观判断
    macro_up   = g4 > MACRO_THR
    macro_down = g4 < -MACRO_THR

    # 信号条件
    buy_signal  = macro_up   and phase1 in ("DISSIPATIVE", "DRIVEN_DOWN")
    sell_signal = macro_down and phase1 in ("ACCUMULATIVE", "DRIVEN_UP")

    if not buy_signal and not sell_signal:
        return None

    direction = "LONG" if buy_signal else "SHORT"

    # 动态 SL/TP
    amp_clamped = max(SL_MIN, min(SL_MAX, amp))
    sl_dist = amp_clamped * SL_MULT
    tp_dist = amp_clamped * TP_MULT

    if direction == "LONG":
        sl = price * (1 - sl_dist)
        tp = price * (1 + tp_dist)
    else:
        sl = price * (1 + sl_dist)
        tp = price * (1 - tp_dist)

    return {
        "direction": direction,
        "price":     price,
        "sl":        _pround(sl, price),
        "tp":        _pround(tp, price),
        "g4":        round(g4, 5),
        "g1":        round(g1, 5),
        "f1":        round(f1, 4),
        "phase1":    phase1,
        "amp":       round(amp, 5),
    }


# ── API 工具 ───────────────────────────────────────────────────────────────────

def _api(method: str, path: str, **kwargs) -> dict:
    resp = req.request(method, f"{API_BASE}{path}", timeout=15, **kwargs)
    resp.raise_for_status()
    return resp.json()

def get_open_symbols() -> set[str]:
    conn = pymysql.connect(**_DB_CFG)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT symbol FROM futures_positions WHERE status='open' AND account_id=%s",
            (ACCOUNT_ID,)
        )
        syms = {r[0] for r in cur.fetchall()}
    conn.close()
    return syms

def open_position(symbol: str, direction: str, qty: float,
                  sl: float, tp: float, dry_run: bool = False) -> dict:
    payload = {
        "account_id":        ACCOUNT_ID,
        "symbol":            symbol,
        "position_side":     direction,
        "quantity":          round(qty, 6),
        "leverage":          LEVERAGE,
        "stop_loss_price":   sl,
        "take_profit_price": tp,
        "max_hold_minutes":  MAX_HOLD_HOURS * 60,
        "source":            "alien_trader",
    }
    if dry_run:
        logger.info(f"[DRY-RUN] {json.dumps(payload)}")
        return {"success": True, "data": {"position_id": -1}}
    return _api("POST", "/api/futures/open", json=payload)

def set_max_hold(position_id: int, minutes: int) -> None:
    try:
        conn = pymysql.connect(**_DB_CFG)
        with conn.cursor() as cur:
            cur.execute("UPDATE futures_positions SET max_hold_minutes=%s WHERE id=%s",
                        (minutes, position_id))
        conn.close()
    except Exception as e:
        logger.error(f"set_max_hold failed: {e}")

def get_position_detail(pid: int) -> dict | None:
    try:
        data = _api("GET", f"/api/futures/positions/{pid}")
        if isinstance(data, dict) and data.get("success"):
            return data.get("data")
        return data if isinstance(data, dict) else None
    except Exception:
        return None

def close_position(pid: int, reason: str = "max_hold_expired") -> dict:
    return _api("POST", f"/api/futures/close/{pid}", json={"reason": reason})


# ── 监控线程（复用 discovery_trader 逻辑）──────────────────────────────────────

def _get_still_open(trades: list) -> list:
    return [t["position_id"] for t in trades
            if t.get("position_id", 0) > 0
            and (lambda d: d and d.get("status") == "open")(
                get_position_detail(t["position_id"]))]

def _force_close_all(trades: list, dry_run: bool) -> None:
    for t in trades:
        pid = t.get("position_id")
        if not pid or pid <= 0: continue
        detail = get_position_detail(pid)
        if detail and detail.get("status") == "open":
            if dry_run:
                logger.info(f"[DRY-RUN] Would close {pid}")
            else:
                try:
                    close_position(pid, "alien_max_hold")
                    t["force_closed"] = True
                except Exception as e:
                    logger.error(f"Close {pid} failed: {e}")

def _record_pnl(session: dict, sf: Path) -> None:
    total = 0.0
    for t in session["trades"]:
        pid = t.get("position_id")
        if pid and pid > 0:
            d = get_position_detail(pid)
            if d:
                pnl = float(d.get("realized_pnl") or d.get("unrealized_pnl") or 0)
                t["final_pnl"] = pnl
                total += pnl
    session["total_pnl"] = round(total, 4)
    session["status"]    = "completed"
    session["completed_at"] = datetime.now().isoformat()
    with open(sf, "w", encoding="utf-8") as f:
        json.dump(session, f, ensure_ascii=False, indent=2)
    logger.info(f"Session complete. Total P&L: {total:+.2f} USDT")
    _print_summary(session)

def _monitor_loop(session: dict, dry_run: bool) -> None:
    deadline   = datetime.fromisoformat(session["deadline_utc"])
    sf         = Path(session["session_file"])
    trades     = session["trades"]
    logger.info(f"Monitor started. Deadline: {deadline.strftime('%Y-%m-%d %H:%M UTC')}")
    while True:
        now       = datetime.now()
        remaining = (deadline - now).total_seconds()
        if remaining <= 0:
            logger.info("Deadline reached. Force-closing...")
            _force_close_all(trades, dry_run)
            _record_pnl(session, sf)
            return
        open_ids = _get_still_open(trades)
        if not open_ids:
            logger.info("All positions closed by SL/TP.")
            _record_pnl(session, sf)
            return
        logger.info(f"Monitor: {len(open_ids)} open, "
                    f"{remaining/3600:.1f}h remaining")
        time.sleep(min(CHECK_INTERVAL, remaining))

def _print_summary(session: dict) -> None:
    sep = "=" * 68
    print(f"\n{sep}")
    print(f"  ALIEN TRADER SESSION RESULTS")
    print(f"  Started : {session.get('started_at','')[:19]}")
    print(f"  Ended   : {session.get('completed_at','')[:19]}")
    print(sep)
    wins = 0
    for t in session["trades"]:
        pnl = t.get("final_pnl")
        if pnl is None:
            pnl_str, status = "---", "OPEN"
        else:
            status  = "WIN" if pnl > 0 else ("LOSS" if pnl < 0 else "FLAT")
            pnl_str = f"{pnl:+8.2f} USDT"
            if pnl > 0: wins += 1
        print(f"  {t['symbol']:14s} {t['direction']:6s}  "
              f"phase={t.get('phase','?'):14s}  P&L: {pnl_str}  [{status}]")
    n = len(session["trades"])
    closed = sum(1 for t in session["trades"] if t.get("final_pnl") is not None)
    print(sep)
    print(f"  Total P&L : {session.get('total_pnl', 0) or 0:+.2f} USDT")
    if closed:
        print(f"  Win rate  : {wins}/{closed} = {wins/closed*100:.0f}%")
    print(f"{sep}\n")


# ── 主流程 ─────────────────────────────────────────────────────────────────────

def run(symbols: list[str] = None, dry_run: bool = False) -> None:
    if symbols is None:
        symbols = DEFAULT_SYMBOLS

    now_utc  = datetime.now()
    ts_str   = now_utc.strftime("%Y-%m-%d_%H%M")
    deadline = now_utc + timedelta(hours=MAX_HOLD_HOURS)

    open_syms = get_open_symbols()
    logger.info(f"Already open: {open_syms or 'none'}")

    session_trades = []

    for sym in symbols:
        if sym in open_syms:
            logger.info(f"{sym}: already has open position, skipping")
            continue

        logger.info(f"{sym}: computing signal...")
        sig = compute_signal(sym)

        if sig is None:
            logger.info(f"{sym}: no signal  "
                        f"(need MACRO_UP+DISSIPATIVE or MACRO_DOWN+ACCUMULATIVE)")
            continue

        direction = sig["direction"]
        price     = sig["price"]
        sl        = sig["sl"]
        tp        = sig["tp"]
        qty       = (MARGIN_PER_TRADE * LEVERAGE) / price

        logger.info(
            f"{sym} {direction}  price={price:.4f}  "
            f"g4={sig['g4']:+.5f}  g1={sig['g1']:+.5f}  "
            f"phase={sig['phase1']}  amp={sig['amp']:.5f}  "
            f"SL={sl:.4f}  TP={tp:.4f}  qty={qty:.6f}"
        )

        try:
            res = open_position(sym, direction, qty, sl, tp, dry_run=dry_run)
        except Exception as e:
            logger.error(f"{sym}: open_position failed: {e}")
            res = {"success": False, "message": str(e)}

        position_id = None
        if res.get("success"):
            data = res.get("data", {})
            if isinstance(data, dict):
                position_id = data.get("position_id") or data.get("id")
            logger.info(f"{sym}: opened position_id={position_id}")
            if position_id and not dry_run:
                set_max_hold(position_id, MAX_HOLD_HOURS * 60)
            open_syms.add(sym)
        else:
            logger.error(f"{sym}: open failed: {res.get('message') or res.get('error')}")

        session_trades.append({
            "symbol":      sym,
            "direction":   direction,
            "entry_price": price,
            "quantity":    qty,
            "margin":      MARGIN_PER_TRADE,
            "leverage":    LEVERAGE,
            "stop_loss":   sl,
            "take_profit": tp,
            "position_id": position_id,
            "open_time":   now_utc.isoformat(),
            "phase":       sig["phase1"],
            "g4":          sig["g4"],
            "g1":          sig["g1"],
            "amp":         sig["amp"],
            "final_pnl":   None,
        })

    if not session_trades:
        logger.warning("No signals fired. Nothing opened.")
        return

    session = {
        "session_id":   ts_str,
        "session_type": "alien",
        "started_at":   now_utc.isoformat(),
        "deadline_utc": deadline.isoformat(),
        "max_hold_h":   MAX_HOLD_HOURS,
        "status":       "active",
        "total_pnl":    None,
        "trades":       session_trades,
        "session_file": str(SESSION_DIR / f"alien_session_{ts_str}.json"),
    }

    sf = Path(session["session_file"])
    with open(sf, "w", encoding="utf-8") as f:
        json.dump(session, f, ensure_ascii=False, indent=2)
    logger.info(f"Session saved: {sf}")
    logger.info(f"{len(session_trades)} positions opened. "
                f"Auto-close at {deadline.strftime('%Y-%m-%d %H:%M UTC')}")

    t = threading.Thread(target=_monitor_loop, args=(session, dry_run), daemon=True)
    t.start()
    logger.info("Monitor running. Press Ctrl+C to stop.")
    try:
        t.join()
    except KeyboardInterrupt:
        logger.warning("Monitor interrupted. Positions remain open.")


# ── 信号扫描（只看不下单）──────────────────────────────────────────────────────

def scan_signals(symbols: list[str] = None) -> None:
    if symbols is None:
        symbols = DEFAULT_SYMBOLS
    print(f"\n{'='*68}")
    print(f"  ALIEN SIGNAL SCAN  [{datetime.now().strftime('%Y-%m-%d %H:%M')}]")
    print(f"{'='*68}")
    print(f"  {'Symbol':14s}  {'Dir':6s}  {'g4':>8}  {'g1':>8}  "
          f"{'phase':14s}  {'SL':>8}  {'TP':>8}")
    print(f"  {'-'*62}")
    for sym in symbols:
        sig = compute_signal(sym)
        if sig:
            print(f"  {sym:14s}  {sig['direction']:6s}  "
                  f"{sig['g4']:>+8.5f}  {sig['g1']:>+8.5f}  "
                  f"{sig['phase1']:14s}  {sig['sl']:>8.4f}  {sig['tp']:>8.4f}")
        else:
            # 打印当前状态
            try:
                cs1h = fetch_klines(sym, "1h", W1+2)
                cs4h = fetch_klines(sym, "4h", W4+2)
                g4 = gradient(cs4h, W4)
                g1 = gradient(cs1h, W1)
                f1 = flux(cs1h, W1)
                ph = classify(g1, f1)
                macro = "UP" if g4 > MACRO_THR else ("DOWN" if g4 < -MACRO_THR else "FLAT")
                print(f"  {sym:14s}  {'--':6s}  "
                      f"{g4:>+8.5f}  {g1:>+8.5f}  "
                      f"{ph:14s}  macro={macro}")
            except Exception:
                print(f"  {sym:14s}  ERROR")
    print(f"{'='*68}\n")


# ── 入口 ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Alien Lens signal trader")
    parser.add_argument("--dry-run",  action="store_true")
    parser.add_argument("--scan",     action="store_true",
                        help="Only scan signals, do not trade")
    parser.add_argument("--symbols",  nargs="+", default=None,
                        help="Override symbol list")
    args = parser.parse_args()

    syms = args.symbols or DEFAULT_SYMBOLS

    if args.scan:
        scan_signals(syms)
    else:
        logger.info(f"Alien Trader started. Scanning every {CHECK_INTERVAL}s. Symbols: {syms}")
        while True:
            try:
                run(symbols=syms, dry_run=args.dry_run)
            except Exception as e:
                logger.error(f"run() error: {e}")
            logger.info(f"Next scan in {CHECK_INTERVAL}s...")
            time.sleep(CHECK_INTERVAL)
