#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
market_trader.py
================
扫描 Binance 全市场 USDT 永续合约，用 PatternDiscoveryAgent 发现方向，
按 trading_symbol_rating 决定保证金，振幅动态计算 SL/TP。

保证金规则：
  rating_level 0 (默认/白名单) : 1000 USDT
  rating_level 1 (黑名单1级)   :  500 USDT
  rating_level 2/3 (黑名单2/3) : 跳过，不开仓

用法:
  .venv/Scripts/python.exe market_trader.py
  .venv/Scripts/python.exe market_trader.py --dry-run
  .venv/Scripts/python.exe market_trader.py --batch-size 8
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
import yaml
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ─── 配置 ─────────────────────────────────────────────────────────────────────

API_BASE          = "http://localhost:9021"
ACCOUNT_ID        = 2
MARGIN_DEFAULT    = 1000.0   # rating_level 0
MARGIN_LEVEL1     = 500.0    # rating_level 1（黑名单1级）
LEVERAGE          = 5
MAX_HOLD_HOURS    = 3
CHECK_INTERVAL    = 300
BATCH_SIZE        = 6

FAPI_KLINES       = "https://fapi.binance.com/fapi/v1/klines"
AMP_WINDOW        = 6        # 近6根1h K线振幅
SL_MULT           = 1.5
TP_MULT           = 2.5
SL_MIN            = 0.008    # 最小SL 0.8%
SL_MAX            = 0.020    # 最大SL 2.0%

CONFIG_PATH = Path(__file__).parent / "config.yaml"
SESSION_DIR = Path(__file__).parent / "discovery_sessions"
SESSION_DIR.mkdir(exist_ok=True)


def _pround(value: float, ref: float) -> float:
    """Price-aware rounding: uses more decimal places for small-price tokens.
    Prevents round(0.000062, 4) == 0.0001 bug for micro-price coins.
    """
    import math
    if ref <= 0 or value <= 0:
        return round(value, 8)
    mag = math.floor(math.log10(ref))
    decimals = max(4, -mag + 3)
    return round(value, decimals)

_DB_CFG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 3306)),
    "user":     os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "binance-data"),
    "charset":  "utf8mb4",
    "autocommit": True,
}

# ─── 数据获取 ─────────────────────────────────────────────────────────────────

def get_symbols_from_config() -> list[str]:
    """从 config.yaml 的 symbols 字段读取 U本位交易对列表"""
    with open(CONFIG_PATH, encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    syms = cfg.get("symbols", [])
    # 只保留 USDT 结尾的现货/合约对
    return [s for s in syms if str(s).endswith("/USDT")]


def get_symbol_margins(symbols: list[str]) -> dict[str, float]:
    """
    查询 trading_symbol_rating，返回每个标的的保证金额度。
    level 0 (或不在表中) -> MARGIN_DEFAULT
    level 1              -> MARGIN_LEVEL1
    level 2/3            -> 不在返回值中（调用方跳过）
    """
    conn = pymysql.connect(**_DB_CFG)
    with conn.cursor() as cur:
        cur.execute("SELECT symbol, rating_level FROM trading_symbol_rating")
        ratings = {r[0]: int(r[1]) for r in cur.fetchall()}
    conn.close()

    result = {}
    skipped = 0
    for sym in symbols:
        level = ratings.get(sym, 0)
        if level >= 2:
            skipped += 1
            continue
        result[sym] = MARGIN_LEVEL1 if level == 1 else MARGIN_DEFAULT
    logger.info(f"Symbol margins: {len(result)} tradable "
                f"({sum(1 for v in result.values() if v == MARGIN_DEFAULT)} default, "
                f"{sum(1 for v in result.values() if v == MARGIN_LEVEL1)} level1-reduced), "
                f"{skipped} skipped (level 2/3)")
    return result


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


def set_max_hold_minutes(position_id: int, minutes: int) -> None:
    try:
        conn = pymysql.connect(**_DB_CFG)
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE futures_positions SET max_hold_minutes=%s WHERE id=%s",
                (minutes, position_id)
            )
        conn.close()
    except Exception as e:
        logger.error(f"set_max_hold_minutes failed for {position_id}: {e}")


# ─── 振幅计算 ─────────────────────────────────────────────────────────────────

def _fetch_klines_1h(symbol: str, limit: int = 8) -> list:
    sym = symbol.replace("/", "")
    r = req.get(FAPI_KLINES,
                params={"symbol": sym, "interval": "1h", "limit": limit},
                timeout=10)
    r.raise_for_status()
    return [{"high": float(x[2]), "low": float(x[3]), "close": float(x[4])}
            for x in r.json()]


def _calc_amplitude(cs: list, n: int) -> float:
    if len(cs) < n:
        return 0.01
    avg = sum(c["high"] - c["low"] for c in cs[-n:]) / n
    ref = cs[-1]["close"]
    return avg / ref if ref else 0.01


# ─── API 工具 ─────────────────────────────────────────────────────────────────

def _api(method: str, path: str, **kwargs) -> dict:
    resp = req.request(method, f"{API_BASE}{path}", timeout=15, **kwargs)
    resp.raise_for_status()
    return resp.json()


def get_current_price(symbol: str) -> float:
    sym = symbol.replace("/", "")
    data = _api("GET", f"/api/futures/price/{sym}")
    if isinstance(data, dict):
        price = data.get("price") or data.get("data", {}).get("price")
        if price:
            return float(price)
    raise ValueError(f"Cannot get price for {symbol}: {data}")


def open_position(symbol: str, direction: str, qty: float,
                  sl: float, tp: float, margin: float,
                  dry_run: bool = False) -> dict:
    payload = {
        "account_id":        ACCOUNT_ID,
        "symbol":            symbol,
        "position_side":     direction.upper(),
        "quantity":          round(qty, 6),
        "leverage":          LEVERAGE,
        "stop_loss_price":   sl,
        "take_profit_price": tp,
        "max_hold_minutes":  MAX_HOLD_HOURS * 60,
        "source":            "market_trader",
    }
    if dry_run:
        logger.info(f"[DRY-RUN] {json.dumps(payload)}")
        return {"success": True, "data": {"position_id": -1}}
    return _api("POST", "/api/futures/open", json=payload)


def close_position(position_id: int, reason: str = "max_hold_expired") -> dict:
    return _api("POST", f"/api/futures/close/{position_id}", json={"reason": reason})


def get_position_detail(position_id: int) -> dict | None:
    try:
        data = _api("GET", f"/api/futures/positions/{position_id}")
        if isinstance(data, dict) and data.get("success"):
            return data.get("data")
        return data if isinstance(data, dict) else None
    except Exception:
        return None


# ─── 监控线程 ─────────────────────────────────────────────────────────────────

def _get_still_open(trades: list) -> list:
    still_open = []
    for t in trades:
        pid = t.get("position_id")
        if pid and pid > 0:
            detail = get_position_detail(pid)
            if detail and detail.get("status") == "open":
                still_open.append(pid)
    return still_open


def _force_close_all(trades: list, dry_run: bool) -> None:
    for t in trades:
        pid = t.get("position_id")
        if not pid or pid <= 0:
            continue
        detail = get_position_detail(pid)
        if detail and detail.get("status") == "open":
            if dry_run:
                logger.info(f"[DRY-RUN] Would force-close position {pid}")
            else:
                try:
                    result = close_position(pid, reason="market_max_hold")
                    logger.info(f"Force-closed position {pid}: {result}")
                    t["force_closed"] = True
                    t["force_close_time"] = datetime.now().isoformat()
                except Exception as e:
                    logger.error(f"Failed to close position {pid}: {e}")


def _record_final_pnl(session: dict, session_file: Path) -> None:
    total_pnl = 0.0
    for t in session["trades"]:
        pid = t.get("position_id")
        if pid and pid > 0:
            detail = get_position_detail(pid)
            if detail:
                pnl = detail.get("realized_pnl") or detail.get("unrealized_pnl") or 0
                t["final_pnl"] = float(pnl)
                total_pnl += float(pnl)
    session["total_pnl"] = round(total_pnl, 4)
    session["status"] = "completed"
    session["completed_at"] = datetime.now().isoformat()
    with open(session_file, "w", encoding="utf-8") as f:
        json.dump(session, f, ensure_ascii=False, indent=2)
    logger.info(f"Session complete. Total P&L: {total_pnl:+.2f} USDT")
    _print_summary(session)


def _monitor_loop(session: dict, dry_run: bool) -> None:
    deadline     = datetime.fromisoformat(session["deadline_utc"])
    session_file = Path(session["session_file"])
    trades       = session["trades"]
    logger.info(f"Monitor started. Deadline: {deadline.strftime('%Y-%m-%d %H:%M UTC')}")
    while True:
        now       = datetime.now()
        remaining = (deadline - now).total_seconds()
        if remaining <= 0:
            logger.info("Deadline reached. Force-closing remaining positions...")
            _force_close_all(trades, dry_run)
            _record_final_pnl(session, session_file)
            logger.info("All positions closed. Session complete.")
            return
        open_ids = _get_still_open(trades)
        if not open_ids:
            logger.info("All positions closed (SL/TP triggered). Session complete.")
            _record_final_pnl(session, session_file)
            return
        next_check = min(CHECK_INTERVAL, remaining)
        logger.info(
            f"Monitor: {len(open_ids)} positions open, "
            f"{remaining/3600:.1f}h remaining, "
            f"next check in {next_check/60:.0f}min"
        )
        time.sleep(next_check)


def _print_summary(session: dict) -> None:
    sep = "=" * 72
    print(f"\n{sep}")
    print(f"  MARKET TRADER SESSION RESULTS")
    print(f"  Started : {session.get('started_at', '')[:19]}")
    print(f"  Ended   : {session.get('completed_at', '')[:19]}")
    print(sep)
    wins = 0
    for t in session["trades"]:
        pnl = t.get("final_pnl")
        if pnl is None:
            status, pnl_str = "OPEN", "---"
        else:
            status  = "WIN" if pnl > 0 else ("LOSS" if pnl < 0 else "FLAT")
            pnl_str = f"{pnl:+8.2f} USDT"
            if pnl > 0:
                wins += 1
        margin_tag = f"[{t.get('margin',0):.0f}U]"
        print(f"  {t['symbol']:14s} {t['direction']:6s} {margin_tag:7s}  "
              f"P&L: {pnl_str:20s}  [{status}]")
    n      = len(session["trades"])
    closed = sum(1 for t in session["trades"] if t.get("final_pnl") is not None)
    print(sep)
    print(f"  Total P&L : {session.get('total_pnl', 0) or 0:+.2f} USDT")
    if closed:
        print(f"  Win rate  : {wins}/{closed} = {wins/closed*100:.0f}%")
    print(f"{sep}\n")


# ─── 主流程 ────────────────────────────────────────────────────────────────────

def _is_market_trader_enabled() -> bool:
    """读 system_settings.market_trader_enabled，不存在或非'1'则返回 False。"""
    try:
        conn = pymysql.connect(**_DB_CFG)
        with conn.cursor() as c:
            c.execute("SELECT setting_value FROM system_settings WHERE setting_key='market_trader_enabled'")
            row = c.fetchone()
        conn.close()
        return row is not None and str(row[0]).strip() == '1'
    except Exception as e:
        logger.warning(f"_is_market_trader_enabled check failed: {e}")
        return True  # 查不到就放行，避免误杀


def run(dry_run: bool = False, batch_size: int = BATCH_SIZE) -> None:
    if not _is_market_trader_enabled():
        logger.info("market_trader_enabled=0 in system_settings, skipping all opens.")
        return

    sys.path.insert(0, str(Path(__file__).parent))
    from app.services.pattern_discovery_agent import PatternDiscoveryAgent

    now_utc  = datetime.now()
    ts_str   = now_utc.strftime("%Y-%m-%d_%H%M")
    deadline = now_utc + timedelta(hours=MAX_HOLD_HOURS)

    # Step 1: 从 config.yaml 获取标的列表 + 保证金映射
    logger.info(f"Loading symbols from {CONFIG_PATH.name}...")
    try:
        all_syms = get_symbols_from_config()
    except Exception as e:
        logger.error(f"Failed to load config.yaml: {e}")
        return
    logger.info(f"Total symbols from config: {len(all_syms)}")

    symbol_margins = get_symbol_margins(all_syms)   # {sym: margin} — level2/3 excluded
    open_symbols   = get_open_symbols()
    logger.info(f"Already open: {open_symbols or 'none'}")

    pending = [s for s in symbol_margins if s not in open_symbols]
    logger.info(f"Symbols to analyze: {len(pending)}")

    # Step 2: 分批运行 PatternDiscoveryAgent
    all_plans = []
    batches   = [pending[i:i+batch_size] for i in range(0, len(pending), batch_size)]
    logger.info(f"Running {len(batches)} batches of up to {batch_size} symbols each...")

    for idx, batch in enumerate(batches, 1):
        logger.info(f"Batch {idx}/{len(batches)}: {batch}")
        try:
            agent  = PatternDiscoveryAgent(symbols=batch)
            result = agent.run(save=True)
            plans  = result.get("trade_plans", [])
            for p in plans:
                raw  = p.get("symbol", "")
                base = raw.split("/")[0].upper().replace("USDT", "").strip()
                p["_symbol_full"] = f"{base}/USDT"
            all_plans.extend(plans)
            actionable = sum(1 for p in plans
                             if p.get("direction", "").upper() not in ("SKIP", ""))
            logger.info(f"Batch {idx} done: {len(plans)} plans, {actionable} actionable")
        except Exception as e:
            logger.error(f"Batch {idx} failed: {e}")
        if idx < len(batches):
            time.sleep(3)

    logger.info(f"All batches done. Total plans: {len(all_plans)}")

    # Step 3: 开仓
    logger.info("Opening positions...")
    open_symbols   = get_open_symbols()   # 刷新
    session_trades = []

    for plan in all_plans:
        sym_full  = plan.get("_symbol_full", "")
        direction = plan.get("direction", "SKIP").upper()

        if direction == "SKIP":
            logger.info(f"{sym_full}: SKIP")
            continue

        if sym_full in open_symbols:
            logger.warning(f"{sym_full}: already has open position, skipping")
            continue

        margin = symbol_margins.get(sym_full)
        if margin is None:
            logger.info(f"{sym_full}: not in tradable list (level 2/3), skipping")
            continue

        # 获取价格 + 振幅
        try:
            price = get_current_price(sym_full)
            cs1h  = _fetch_klines_1h(sym_full, AMP_WINDOW + 2)
            amp   = _calc_amplitude(cs1h, AMP_WINDOW)
        except Exception as e:
            logger.error(f"{sym_full}: price/kline fetch failed: {e}")
            continue

        # 振幅动态 SL/TP
        amp_clamped = max(SL_MIN, min(SL_MAX, amp))
        sl_dist     = amp_clamped * SL_MULT
        tp_dist     = amp_clamped * TP_MULT
        if direction == "LONG":
            sl = _pround(price * (1 - sl_dist), price)
            tp = _pround(price * (1 + tp_dist), price)
        else:
            sl = _pround(price * (1 + sl_dist), price)
            tp = _pround(price * (1 - tp_dist), price)

        qty = (margin * LEVERAGE) / price
        logger.info(
            f"{sym_full} {direction} [{margin:.0f}U]: price=${price:.4f}  "
            f"qty={qty:.6f}  SL=${sl:.4f}({sl_dist*100:.2f}%)  "
            f"TP=${tp:.4f}({tp_dist*100:.2f}%)"
        )

        try:
            res = open_position(sym_full, direction, qty, sl, tp, margin, dry_run=dry_run)
        except Exception as e:
            logger.error(f"{sym_full}: open failed: {e}")
            res = {"success": False, "message": str(e)}

        position_id = None
        if res.get("success"):
            data = res.get("data", {})
            if isinstance(data, dict):
                position_id = data.get("position_id") or data.get("id")
            logger.info(f"{sym_full} opened: position_id={position_id}")
            if position_id and not dry_run:
                set_max_hold_minutes(position_id, MAX_HOLD_HOURS * 60)
            open_symbols.add(sym_full)
        else:
            logger.error(f"{sym_full} open failed: "
                         f"{res.get('message') or res.get('error')}")

        session_trades.append({
            "symbol":       sym_full,
            "direction":    direction,
            "entry_price":  price,
            "quantity":     qty,
            "margin":       margin,
            "leverage":     LEVERAGE,
            "stop_loss":    sl,
            "take_profit":  tp,
            "position_id":  position_id,
            "open_time":    now_utc.isoformat(),
            "win_rate_pct": plan.get("win_rate_pct"),
            "confidence":   plan.get("confidence"),
            "final_pnl":    None,
        })

    if not session_trades:
        logger.warning("No positions were opened.")
        return

    session = {
        "session_id":   ts_str,
        "session_type": "market",
        "started_at":   now_utc.isoformat(),
        "deadline_utc": deadline.isoformat(),
        "max_hold_h":   MAX_HOLD_HOURS,
        "status":       "active",
        "total_pnl":    None,
        "trades":       session_trades,
        "session_file": str(SESSION_DIR / f"market_session_{ts_str}.json"),
    }

    sf = Path(session["session_file"])
    with open(sf, "w", encoding="utf-8") as f:
        json.dump(session, f, ensure_ascii=False, indent=2)
    logger.info(f"Session saved: {sf}")

    t = threading.Thread(target=_monitor_loop, args=(session, dry_run), daemon=True)
    t.start()
    logger.info(
        f"Monitor running. {len(session_trades)} positions open. "
        f"Auto-close at {deadline.strftime('%Y-%m-%d %H:%M UTC')}."
    )
    logger.info("Press Ctrl+C to stop monitoring (positions remain open).")
    try:
        t.join()
    except KeyboardInterrupt:
        logger.warning("Monitor interrupted. Positions remain open.")


# ─── 入口 ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Full market scanner & trader")
    parser.add_argument("--dry-run",    action="store_true")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    args = parser.parse_args()
    run(dry_run=args.dry_run, batch_size=args.batch_size)
