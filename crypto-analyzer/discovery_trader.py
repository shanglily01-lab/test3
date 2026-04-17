#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
discovery_trader.py
===================
发现信号 -> 自动下单 -> 12小时强平 -> 记录结果

每次运行完整流程：
  1. 运行 PatternDiscoveryAgent 生成交易计划
  2. 对每个 LONG/SHORT 信号开仓（1000U 保证金，5倍杠杆）
  3. 设置止损/止盈（来自信号的 stop_loss / target1）
  4. 后台监控：距开仓 12h 后强制平仓所有本次开的仓位
  5. 输出本次交易日志

用法:
  .venv/Scripts/python.exe discovery_trader.py           # 完整运行
  .venv/Scripts/python.exe discovery_trader.py --dry-run # 仅模拟，不实际下单
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

# ─── DB 直连（用于开仓后设置 max_hold_minutes）────────────────────────────────

_DB_CFG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 3306)),
    "user":     os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "binance-data"),
    "charset":  "utf8mb4",
    "autocommit": True,
}


def _set_max_hold_minutes(position_id: int, minutes: int) -> None:
    """直接更新 DB，确保 smart_exit_optimizer 使用正确的持仓时间限制。"""
    try:
        conn = pymysql.connect(**_DB_CFG)
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE futures_positions SET max_hold_minutes=%s WHERE id=%s",
                (minutes, position_id)
            )
        conn.close()
        logger.info(f"Set max_hold_minutes={minutes} for position {position_id}")
    except Exception as e:
        logger.error(f"Failed to set max_hold_minutes for position {position_id}: {e}")

# Fix Windows terminal encoding
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ─── 配置 ────────────────────────────────────────────────────────────────────

API_BASE         = "http://localhost:9021"
ACCOUNT_ID       = 2
MARGIN_PER_TRADE = 1000.0    # 每笔交易保证金（USDT）
LEVERAGE         = 5         # 固定杠杆倍数
MAX_HOLD_HOURS   = 3         # 最大持仓时间（小时）
CHECK_INTERVAL   = 300       # 监控间隔（秒）

FAPI     = "https://fapi.binance.com/fapi/v1/klines"

# 振幅动态 SL/TP（Gemini 只提供方向，我们自己算止损止盈）
AMP_WINDOW = 6       # 近6根1h K线振幅
SL_MULT    = 1.5     # SL = 1.5x 振幅
TP_MULT    = 2.5     # TP = 2.5x 振幅
SL_MIN     = 0.008   # 最小SL 0.8%
SL_MAX     = 0.020   # 最大SL 2.0%

SESSION_DIR = Path(__file__).parent / "discovery_sessions"
SESSION_DIR.mkdir(exist_ok=True)

# ─── API 工具 ─────────────────────────────────────────────────────────────────

def _api(method: str, path: str, **kwargs) -> dict:
    url = f"{API_BASE}{path}"
    resp = req.request(method, url, timeout=15, **kwargs)
    resp.raise_for_status()
    return resp.json()


def get_current_price(symbol: str) -> float:
    """通过 API 获取当前价格 /api/futures/price/{symbol}"""
    sym = symbol.replace("/", "")
    data = _api("GET", f"/api/futures/price/{sym}")
    if isinstance(data, dict):
        price = data.get("price") or data.get("data", {}).get("price")
        if price:
            return float(price)
    raise ValueError(f"Cannot get price for {symbol}: {data}")


def get_open_positions() -> list:
    """获取当前所有开仓"""
    data = _api("GET", f"/api/futures/positions?account_id={ACCOUNT_ID}&status=open")
    return data.get("data", []) if isinstance(data, dict) else []


def open_position(symbol: str, direction: str, qty: float,
                  sl: float, tp: float, dry_run: bool = False) -> dict:
    """开仓，返回 {success, position_id, message}"""
    payload = {
        "account_id":       ACCOUNT_ID,
        "symbol":           symbol,
        "position_side":    direction.upper(),  # LONG or SHORT
        "quantity":         round(qty, 6),
        "leverage":         LEVERAGE,
        "stop_loss_price":  round(sl, 4),
        "take_profit_price": round(tp, 4),
        "max_hold_minutes": MAX_HOLD_HOURS * 60,
        "source":           "discovery_trader",
    }
    if dry_run:
        logger.info(f"[DRY-RUN] Would open: {json.dumps(payload)}")
        return {"success": True, "position_id": -1, "message": "dry-run"}

    result = _api("POST", "/api/futures/open", json=payload)
    return result


def close_position(position_id: int, reason: str = "max_hold_expired") -> dict:
    """强制平仓"""
    return _api("POST", f"/api/futures/close/{position_id}",
                json={"reason": reason})


def get_position_detail(position_id: int) -> dict | None:
    """获取单个持仓详情"""
    try:
        data = _api("GET", f"/api/futures/positions/{position_id}")
        if isinstance(data, dict) and data.get("success"):
            return data.get("data")
        return data if isinstance(data, dict) else None
    except Exception:
        return None

# ─── 数量计算 ─────────────────────────────────────────────────────────────────

def calc_quantity(price: float, margin: float = MARGIN_PER_TRADE,
                  leverage: int = LEVERAGE) -> float:
    """
    position_value = margin * leverage
    qty = position_value / price
    """
    return (margin * leverage) / price


def _fetch_klines_1h(symbol: str, limit: int = 8) -> list:
    """从 Binance 获取 1h K线数据"""
    sym = symbol.replace("/", "")
    r = req.get(FAPI, params={"symbol": sym, "interval": "1h", "limit": limit}, timeout=10)
    r.raise_for_status()
    return [{"high": float(x[2]), "low": float(x[3]), "close": float(x[4])} for x in r.json()]


def _calc_amplitude(cs: list, n: int) -> float:
    """计算近 n 根 K 线的平均振幅比（相对价格）"""
    if len(cs) < n:
        return 0.01
    avg = sum(c["high"] - c["low"] for c in cs[-n:]) / n
    ref = cs[-1]["close"]
    return avg / ref if ref else 0.01


# ─── 12h 监控线程 ─────────────────────────────────────────────────────────────

def _monitor_loop(session: dict, dry_run: bool) -> None:
    """
    后台线程：每 CHECK_INTERVAL 秒检查一次，超过 MAX_HOLD_HOURS 的仓位强制平仓。
    """
    deadline = datetime.fromisoformat(session["deadline_utc"])
    session_file = Path(session["session_file"])
    trades = session["trades"]

    logger.info(f"Monitor started. Deadline: {deadline.strftime('%Y-%m-%d %H:%M UTC')}")

    while True:
        now = datetime.now()
        remaining = (deadline - now).total_seconds()

        if remaining <= 0:
            logger.info("12h deadline reached. Force-closing remaining positions...")
            _force_close_all(trades, dry_run)
            _record_final_pnl(session, session_file)
            logger.info("All positions closed. Session complete.")
            return

        # 也检查是否还有仓位（有可能全部触发SL/TP提前关闭了）
        open_ids = _get_still_open(trades)
        if not open_ids:
            logger.info("All positions already closed (SL/TP triggered). Session complete.")
            _record_final_pnl(session, session_file)
            return

        next_check = min(CHECK_INTERVAL, remaining)
        logger.info(
            f"Monitor: {len(open_ids)} positions open, "
            f"{remaining/3600:.1f}h remaining, "
            f"next check in {next_check/60:.0f}min"
        )
        time.sleep(next_check)


def _get_still_open(trades: list) -> list:
    """返回仍然处于开仓状态的 position_id 列表"""
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
                    result = close_position(pid, reason="max_hold_12h")
                    logger.info(f"Force-closed position {pid}: {result}")
                    t["force_closed"] = True
                    t["force_close_time"] = datetime.now().isoformat()
                except Exception as e:
                    logger.error(f"Failed to close position {pid}: {e}")


def _record_final_pnl(session: dict, session_file: Path) -> None:
    """从 API 拉取最终 P&L 并更新 session 文件"""
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


def _print_summary(session: dict) -> None:
    sep = "=" * 66
    print(f"\n{sep}")
    print(f"  DISCOVERY TRADE SESSION RESULTS")
    print(f"  Started : {session.get('started_at', '')[:19]}")
    print(f"  Ended   : {session.get('completed_at', '')[:19]}")
    print(sep)
    wins = 0
    for t in session["trades"]:
        pnl = t.get("final_pnl", 0.0)
        status = "WIN" if pnl > 0 else ("LOSS" if pnl < 0 else "FLAT")
        if pnl > 0:
            wins += 1
        print(f"  {t['symbol']:12s} {t['direction']:6s}  P&L: {pnl:+8.2f} USDT  [{status}]")
    n = len(session["trades"])
    print(f"{sep}")
    print(f"  Total P&L : {session.get('total_pnl', 0):+.2f} USDT")
    if n:
        print(f"  Win rate  : {wins}/{n} = {wins/n*100:.0f}%")
    print(f"{sep}\n")


# ─── 主流程 ───────────────────────────────────────────────────────────────────

def _is_discovery_trader_enabled() -> bool:
    """读 system_settings.discovery_trader_enabled，不存在或非'1'则返回 False。"""
    try:
        conn = pymysql.connect(**_DB_CFG)
        with conn.cursor() as c:
            c.execute("SELECT setting_value FROM system_settings WHERE setting_key='discovery_trader_enabled'")
            row = c.fetchone()
        conn.close()
        return row is not None and str(row[0]).strip() == '1'
    except Exception as e:
        logger.warning(f"_is_discovery_trader_enabled check failed: {e}")
        return True  # 查不到就放行


def run(dry_run: bool = False) -> None:
    if not _is_discovery_trader_enabled():
        logger.info("discovery_trader_enabled=0 in system_settings, skipping.")
        return

    now_utc = datetime.now()
    ts_str  = now_utc.strftime("%Y-%m-%d_%H%M")

    # Step 1: 运行发现
    logger.info("Step 1/3: Running PatternDiscoveryAgent...")
    sys.path.insert(0, str(Path(__file__).parent))
    from app.services.pattern_discovery_agent import PatternDiscoveryAgent
    agent = PatternDiscoveryAgent()
    result = agent.run(save=True)

    plans = result.get("trade_plans", [])
    if not plans:
        logger.error("No trade plans returned. Aborting.")
        return

    # Step 2: 检查当前持仓（避免重复）
    logger.info("Step 2/3: Checking existing positions...")
    existing = get_open_positions()
    existing_symbols = {
        p.get("symbol", "").replace("/", "").replace("USDT", "") + "/USDT"
        for p in existing
    }
    logger.info(f"Existing positions: {existing_symbols or 'none'}")

    # Step 3: 开仓
    logger.info("Step 3/3: Opening positions...")
    session_trades = []
    deadline = now_utc + timedelta(hours=MAX_HOLD_HOURS)

    for plan in plans:
        raw_sym   = plan.get("symbol", "")
        sym_short = raw_sym.split("/")[0].upper().replace("USDT", "").strip()
        sym_full  = f"{sym_short}/USDT"
        direction = plan.get("direction", "SKIP").upper()

        if direction == "SKIP":
            logger.info(f"{sym_full}: SKIP — {plan.get('invalidation', '')[:80]}")
            continue

        if sym_full in existing_symbols:
            logger.warning(f"{sym_full}: already has open position, skipping")
            continue

        # 获取实时价格 + 振幅（Gemini 只提供方向，SL/TP 自己算）
        try:
            price = get_current_price(sym_full)
            cs1h  = _fetch_klines_1h(sym_full, AMP_WINDOW + 2)
            amp   = _calc_amplitude(cs1h, AMP_WINDOW)
        except Exception as e:
            logger.error(f"{sym_full}: price/kline fetch failed: {e}")
            continue

        # 振幅动态 SL/TP（与 alien_trader 逻辑一致）
        amp_clamped = max(SL_MIN, min(SL_MAX, amp))
        sl_dist = amp_clamped * SL_MULT
        tp_dist = amp_clamped * TP_MULT
        if direction == "LONG":
            sl = round(price * (1 - sl_dist), 4)
            tp = round(price * (1 + tp_dist), 4)
        else:
            sl = round(price * (1 + sl_dist), 4)
            tp = round(price * (1 - tp_dist), 4)

        qty = calc_quantity(price)
        logger.info(
            f"{sym_full} {direction}: price=${price:.4f}  "
            f"qty={qty:.6f}  margin=${MARGIN_PER_TRADE}  leverage={LEVERAGE}x  "
            f"SL=${sl:.4f}({sl_dist*100:.2f}%)  TP=${tp:.4f}({tp_dist*100:.2f}%)"
        )

        try:
            res = open_position(sym_full, direction, qty, sl, tp, dry_run=dry_run)
        except Exception as e:
            logger.error(f"{sym_full}: open_position failed: {e}")
            res = {"success": False, "message": str(e)}

        position_id = None
        if res.get("success"):
            # 从响应中提取 position_id
            data = res.get("data", {})
            if isinstance(data, dict):
                position_id = data.get("position_id") or data.get("id")
            logger.info(f"{sym_full} opened: position_id={position_id}")
            # Patch max_hold_minutes so smart_exit_optimizer respects our hold limit
            if position_id and not dry_run:
                _set_max_hold_minutes(position_id, MAX_HOLD_HOURS * 60)
        else:
            logger.error(f"{sym_full} open failed: {res.get('message') or res.get('error')}")

        session_trades.append({
            "symbol":       sym_full,
            "direction":    direction,
            "entry_price":  price,
            "quantity":     qty,
            "margin":       MARGIN_PER_TRADE,
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
        "session_id":  ts_str,
        "started_at":  now_utc.isoformat(),
        "deadline_utc": deadline.isoformat(),
        "max_hold_h":  MAX_HOLD_HOURS,
        "status":      "active",
        "total_pnl":   None,
        "trades":      session_trades,
        "session_file": str(SESSION_DIR / f"session_{ts_str}.json"),
    }

    session_file = Path(session["session_file"])
    with open(session_file, "w", encoding="utf-8") as f:
        json.dump(session, f, ensure_ascii=False, indent=2)
    logger.info(f"Session saved: {session_file}")

    # Step 4: 启动后台监控线程
    t = threading.Thread(
        target=_monitor_loop,
        args=(session, dry_run),
        daemon=True
    )
    t.start()
    logger.info(
        f"Monitor running. {len(session_trades)} positions open. "
        f"Auto-close at {deadline.strftime('%Y-%m-%d %H:%M UTC')}."
    )
    logger.info("Press Ctrl+C to stop monitoring (positions remain open).")

    # 等待监控完成
    try:
        t.join()
    except KeyboardInterrupt:
        logger.warning("Monitor interrupted. Positions remain open; run with --check to view status.")


# ─── 查看最近 session ──────────────────────────────────────────��──────────────

def show_latest() -> None:
    files = sorted(SESSION_DIR.glob("session_*.json"), reverse=True)
    if not files:
        print("No session files found.")
        return
    with open(files[0], encoding="utf-8") as f:
        session = json.load(f)
    _print_summary(session)


# ─── 入口 ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Discovery-driven auto trader")
    parser.add_argument("--dry-run", action="store_true",
                        help="Simulate without placing actual orders")
    parser.add_argument("--check", action="store_true",
                        help="Show latest session results and exit")
    args = parser.parse_args()

    if args.check:
        show_latest()
    else:
        run(dry_run=args.dry_run)
