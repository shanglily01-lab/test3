"""
strategy_bigmid - 中大市值币种策略引擎

针对 BIG (成交量 >= $500M) 和 MID ($100M~$500M) 两档品种，
按波动特征缩放阈值、时间框架与 SL/TP。

与 strategy_live 共用 account_id=2、futures_positions 表，通过：
  - strategy_state 表 strategy='bigmid' 隔离状态机
  - futures_orders / futures_positions.source LIKE 'strategy_bigmid:%' 隔离操作

MVP: CHASE + DUMP。Climax / TopShort / BottomLong / Whale 留 v2。
"""
from __future__ import annotations

import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import datetime
import logging
import os
import time
from typing import Optional

import pymysql
import requests as req
from dotenv import load_dotenv

load_dotenv()

from strategy_state_db import (
    ensure_table,
    get_or_create,
    update_state,
)

# ── 基础配置 ────────────────────────────────────────────────────
API_BASE   = "http://localhost:9021"
ACCOUNT_ID = 2                # 共用 strategy_live 账户
LEVERAGE   = 5
MARGIN     = 500.0            # 每笔保证金 (USDT)

POLL_SECS              = 60
SYM_REFRESH_SECS       = 15 * 60
LIMIT_PENDING_MAX_S    = 2 * 3600   # 大币行动慢，限价挂 2h

# 分档阈值（成交量 USDT）
TIER_BIG_MIN_VOL = 500_000_000
TIER_MID_MIN_VOL = 100_000_000

# 排除：股票/商品衍生品、meme-1000 系（仅放行 1000PEPE）
BIGMID_EXCLUDES = {
    "XAU/USDT", "XAG/USDT", "CL/USDT", "TSLA/USDT",
    "PIEVERSE/USDT",  # 数据不全
}
MEME_1000_WHITELIST = {"1000PEPE/USDT"}

# 与 strategy_live.SYMBOL_BLACKLIST 同步（反复止损 / 即将下架）
SHARED_BLACKLIST_BASE = {
    "DENT/USDT", "XAN/USDT", "SUPER/USDT", "GUN/USDT", "UAI/USDT",
    "AAVE/USD", "BTC/USD", "XVG/USDT", "TRU/USDT", "DEGO/USDT",
    "ZRO/USDT", "RIVER/USDT", "Q/USDT", "CHIP/USDT", "SPK/USDT", "UB/USDT",
}
_db_bl_cache_bm = {'syms': set(), 'ts': 0.0}
_DB_BL_REFRESH_S_BM = 300.0

def _refresh_db_bl_bm() -> set:
    import time as _t
    now = _t.time()
    if (now - _db_bl_cache_bm['ts']) < _DB_BL_REFRESH_S_BM:
        return _db_bl_cache_bm['syms']
    try:
        conn2 = _db_conn()
        try:
            with conn2.cursor() as c:
                c.execute("SELECT symbol FROM symbol_blacklist WHERE is_active=1")
                _db_bl_cache_bm['syms'] = {r['symbol'] for r in c.fetchall()}
        finally:
            conn2.close()
        _db_bl_cache_bm['ts'] = now
    except Exception as e:
        log.debug("读 symbol_blacklist 失败(旧缓存): %s", e)
    return _db_bl_cache_bm['syms']

def _effective_blacklist_bm() -> set:
    return SHARED_BLACKLIST_BASE | _refresh_db_bl_bm()

# BIG 档硬白名单：只信这些长期稳定的主流币
# 即便成交量达到 BIG 门槛，不在此白名单也降级到 MID 档（或直接排除，见 refresh_symbols）
BIG_WHITELIST = {
    "BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT",
    "XRP/USDT", "DOGE/USDT", "ADA/USDT", "SUI/USDT",
}

# 24h pump/dump 异常过滤（避免抓刚被拉爆或刚被砸穿的币）
PUMP_EXCLUDE_PCT = 50.0   # change_24h > +50% 排除
DUMP_EXCLUDE_PCT = -20.0  # change_24h < -20% 排除


# ── 分档参数表 ──────────────────────────────────────────────────
# BIG 档用 Whale 多维评分（funding/LSR/OI/放量/taker/触发器），阈值按真实数据分布缩放
# MID 档沿用 CHASE/DUMP 趋势追踪，15m 时间框架，阈值按小币 * 0.5 缩放
#
# 72h 回测结论（记录用）：BIG 档 CHASE/DUMP 两轮尝试均为负期望
#   SL=2.5%/TP=5%/24h  → 全部 timeout，均值 -0.77%
#   SL=1%/TP=2.5%/8h   → 6/8 触 SL，均值 -0.61%
# 根因：主流币 1h 涨 3% 不是趋势信号，回踩 1% 是常态 → 追涨追跌天然失效
# 结论：BIG 档改走 Whale 多维评分

# MID 档动态 trail 分档（与 strategy_live/whale 同；BIG 档因 TP 只有 2% 不适用）
DYNAMIC_TRAIL_TIERS = [
    (0.10, 0.03),  # peak ≥ 10% → 回落 3%
    (0.05, 0.02),  # peak ≥ 5%  → 回落 2%
    (0.03, 0.01),  # peak ≥ 3%  → 回落 1%
]


def _dynamic_trail_pullback(peak_pct: float) -> float:
    for threshold, pullback in DYNAMIC_TRAIL_TIERS:
        if peak_pct >= threshold:
            return pullback
    return float('inf')


# 早期止损 / 保本止损（MID 档适用；BIG 档 SL=1% 已比这严，不适用）
# 2026-04-24 breakeven 启动门槛 3%→1.5%：补 peak 1-3% 的保护盲区
EARLY_SL_PCT             = 0.03
BREAKEVEN_AFTER_PEAK_PCT = 0.015
BREAKEVEN_SL_PCT         = -0.005


TIER_PARAMS = {
    # MID 档：沿用 CHASE/DUMP，阈值按小币波动 × 0.5 缩放
    "MID": {
        "kind":                "trend",    # 策略类型标识
        "tf":                  "15m",
        "bars_chase":          24,          # 回看 6h
        "bars_dump":           48,          # 回看 12h
        "chase_pump_pct":      0.06,
        "chase_leader_pct":    0.015,
        "chase_exhaust_dd":    0.03,
        "dump_drop_pct":       0.05,
        "dump_bounce_max":     0.04,
        "sl_pct":              0.05,
        "hard_tp_pct":         0.10,
        "trail_tp_start":      0.06,
        "trail_tp_pullback":   0.01,
        "limit_offset_pct":    0.015,
        "reverse_slippage":    0.0075,
        "hold_min":            12 * 60,
    },
    # BIG 档：Whale 多维评分，阈值按 BIG 币真实分布 (近 7 天) 缩放
    "BIG": {
        "kind":                "whale",
        "tf":                  "1h",        # 触发器/放量/taker 均基于 1h K
        # funding rate：BIG 币 7 天 p5~p95 约 ±0.01%
        "fr_extreme_high":     0.00005,     # +3 分（极端多头，做空信号）
        "fr_high":             0.00003,     # +2
        "fr_mild_high":        0.00001,     # +1
        "fr_extreme_low":     -0.00005,     # +3（极端空头，做多信号）
        "fr_low":             -0.00003,     # +2
        "fr_mild_low":        -0.00001,     # +1
        # LSR (long_account)：BIG 币差异大（BTC p50=0.45 vs DOGE p50=0.73），弱化评分
        "ls_long_extreme":     0.75,        # +2
        "ls_long_high":        0.70,        # +1
        "ls_short_extreme":    0.55,        # +2（short_account > 0.55 ≈ long < 0.45）
        "ls_short_high":       0.50,        # +1
        # OI 4h 变化：p5~p95 约 ±3%
        "oi_drop_strong":     -0.025,       # +2
        "oi_drop_mild":       -0.010,       # +1
        "oi_rise_strong":      0.025,       # +2（做多）
        "oi_rise_mild":        0.010,       # +1
        # 放量滞涨/滞跌
        "vol_ratio_strong":    2.0,         # +3
        "vol_ratio_mild":      1.5,         # +2
        "stale_price_pct":     0.010,       # 3h 价格变化 < 1% → 滞涨/滞跌
        # Taker buy ratio
        "taker_sell_thresh":   0.45,        # +1 做空
        "taker_buy_thresh":    0.55,        # +1 做多
        # 触发器（BTC 1h body p99=1.44%, ETH=2.08%, SOL=1.42%）
        # 2026-04-24: 48h 只触发 1 笔信号，主流币日内 1h 实体极少超 0.8%
        # 放宽到 0.5%（BTC/SOL/BNB/DOGE/ADA/SUI 单根 1h 都能到 0.5%+）
        "trigger_candle_pct":  0.005,       # 1h 实体 ≥ 0.5%（放宽自 0.8%）
        "trigger_breakout":    0.0015,      # 突破 4h 高低点 0.15%
        # 入场评分门槛（whale 原为 5，BIG 维度较少；2026-04-24 从 4 放宽到 3，配合 trigger 一起提高频率）
        "entry_score_min":     3,
        # 风控：BTC/ETH 1h 波动 p99 ≈ 1.5%；SL 略宽一点避免被 p99 扫
        "sl_pct":              0.010,
        "hard_tp_pct":         0.020,
        "trail_tp_start":      0.012,
        "trail_tp_pullback":   0.003,
        "limit_offset_pct":    0.0,         # 市价入场
        "reverse_slippage":    0.003,
        "hold_min":            4 * 60,      # 4h 持仓，抓快速反应
    },
}


# ── 从 system_settings 动态加载的参数 ──────────────────────────
DISABLE_SL_TP_HOLD = False  # 总开关: 新开仓不设 SL/TP/timeout, 且跳过进程内硬TP/SL/移动TP检查


# ── 日志 ────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    datefmt='%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('strategy_bigmid.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("strategy_bigmid")


def _load_bigmid_config() -> None:
    """从 system_settings 读取总开关。进程启动时调用一次。"""
    global DISABLE_SL_TP_HOLD
    try:
        conn = _db_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT setting_key, setting_value FROM system_settings "
                    "WHERE setting_key='disable_sl_tp_hold'"
                )
                rows = {r['setting_key']: r['setting_value'] for r in cur.fetchall()}
        finally:
            conn.close()
        _raw = str(rows.get('disable_sl_tp_hold', '0')).strip().lower()
        DISABLE_SL_TP_HOLD = _raw in ('1', 'true', 'yes', 'on')
        log.info("strategy_bigmid 参数已加载: disable_sl_tp_hold=%s", DISABLE_SL_TP_HOLD)
        if DISABLE_SL_TP_HOLD:
            log.warning("!!! DISABLE_SL_TP_HOLD=ON: 新开仓将不设 SL/TP/timeout, 硬TP/SL/移动TP检查跳过 !!!")
    except Exception as exc:
        log.error("_load_bigmid_config 失败，使用默认值: %s", exc)


# ── DB 工具 ─────────────────────────────────────────────────────
def _db_conn():
    return pymysql.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", ""),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", ""),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=5,
    )


# ── API 工具 ────────────────────────────────────────────────────
def _api(method: str, path: str, **kwargs):
    r = req.request(method, f"{API_BASE}{path}", timeout=10, **kwargs)
    r.raise_for_status()
    return r.json()


def get_price(sym: str) -> float:
    d = _api("GET", f"/api/futures/price/{sym}")
    return float(d["price"])


# ── 品种池 ──────────────────────────────────────────────────────
_sym_cache: dict = {"bigs": [], "mids": [], "updated_at": 0.0}


def classify_tier(sym: str, vol: float) -> Optional[str]:
    """按成交量 + BIG 白名单统一判定档位。refresh/fill/monitor 都用这个函数，避免漂移"""
    if vol >= TIER_BIG_MIN_VOL and sym in BIG_WHITELIST:
        return "BIG"
    if vol >= TIER_MID_MIN_VOL:
        return "MID"
    return None


def get_tier(sym: str, vol_map: dict) -> Optional[str]:
    return classify_tier(sym, vol_map.get(sym, 0))


def refresh_symbols(cur) -> tuple[list, list, dict]:
    """返回 (BIG 列表, MID 列表, {sym: volume_usdt})"""
    now = time.time()
    if now - _sym_cache["updated_at"] < SYM_REFRESH_SECS and _sym_cache["bigs"]:
        return _sym_cache["bigs"], _sym_cache["mids"], _sym_cache["vol_map"]

    cur.execute("""
        SELECT symbol, quote_volume_24h, change_24h FROM price_stats_24h
        WHERE symbol LIKE '%%/USDT'
          AND quote_volume_24h >= %s
    """, (TIER_MID_MIN_VOL,))
    rows = cur.fetchall()

    bigs, mids, vol_map = [], [], {}
    _bl = _effective_blacklist_bm()  # 合并硬编码 BASE + DB 表（5 分钟缓存）
    for r in rows:
        sym = r["symbol"]
        vol = float(r["quote_volume_24h"] or 0)
        chg = float(r["change_24h"] or 0)
        # 1) 基础硬排除（股票/衍生品/数据不全）
        if sym in BIGMID_EXCLUDES:
            continue
        # 2) 与 strategy_live 共享的反复止损黑名单（含 DB 动态黑名单）
        if sym in _bl:
            continue
        # 3) 1000* meme 前缀（除白名单）
        if sym.startswith("1000") and sym not in MEME_1000_WHITELIST:
            continue
        # 4) pump/dump 异常过滤
        if chg > PUMP_EXCLUDE_PCT or chg < DUMP_EXCLUDE_PCT:
            continue
        vol_map[sym] = vol
        # 5) BIG 档硬白名单：不在白名单的即便成交量够也不进 BIG
        if vol >= TIER_BIG_MIN_VOL and sym in BIG_WHITELIST:
            bigs.append(sym)
        elif vol >= TIER_MID_MIN_VOL:
            mids.append(sym)

    _sym_cache.update(bigs=bigs, mids=mids, vol_map=vol_map, updated_at=now)
    log.info("品种池刷新: BIG=%d MID=%d", len(bigs), len(mids))
    return bigs, mids, vol_map


# ── 通用风控 / 状态查询 ─────────────────────────────────────────
def _has_any_open(sym: str) -> bool:
    """该标的是否已有任意策略的 open 持仓或 PENDING 挂单"""
    try:
        with _db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM futures_positions "
                    "WHERE account_id=%s AND symbol=%s AND status='open' LIMIT 1",
                    (ACCOUNT_ID, sym),
                )
                if cur.fetchone():
                    return True
                cur.execute(
                    "SELECT id FROM futures_orders "
                    "WHERE account_id=%s AND symbol=%s AND status='PENDING' LIMIT 1",
                    (ACCOUNT_ID, sym),
                )
                return bool(cur.fetchone())
    except Exception as e:
        log.error("_has_any_open %s error: %s", sym, e)
        return True  # 失败时保守拒绝开仓


def _get_24h_stats(cur, sym: str):
    cur.execute(
        "SELECT high_24h, low_24h FROM price_stats_24h WHERE symbol=%s LIMIT 1",
        (sym,),
    )
    r = cur.fetchone()
    return (float(r["high_24h"]), float(r["low_24h"])) if r and r["high_24h"] else (None, None)


def _calc_limit_price(side: str, cur_p: float, h24, l24, pct: float) -> float:
    if side == "LONG":
        lp = cur_p * (1 - pct)
        if l24 and l24 > 0:
            lp = max(lp, float(l24))
    else:
        lp = cur_p * (1 + pct)
        if h24 and h24 > 0:
            lp = min(lp, float(h24))
    return round(lp, 8)


# ── 开仓 ────────────────────────────────────────────────────────
def open_order(sym: str, direction: str, entry_p: float, tier: str, tag: str,
               limit_p: Optional[float] = None):
    """开仓。返回 (pid, oid, is_pending)。失败返回 (None, None, False)"""
    if _has_any_open(sym):
        log.info("跳过开%s %s [%s]: 已有持仓/挂单", direction, sym, tier)
        return None, None, False

    p = TIER_PARAMS[tier]
    price_ref = limit_p if (limit_p and limit_p > 0) else entry_p
    qty = round(MARGIN * LEVERAGE / price_ref, 6)
    if direction == "LONG":
        tp = round(price_ref * (1 + p["hard_tp_pct"]), 8)
        sl = round(price_ref * (1 - p["sl_pct"]), 8)
    else:
        tp = round(price_ref * (1 - p["hard_tp_pct"]), 8)
        sl = round(price_ref * (1 + p["sl_pct"]), 8)

    # 总开关 disable_sl_tp_hold 开启时: 裸奔,不写 SL/TP/timeout
    if DISABLE_SL_TP_HOLD:
        sl_out, tp_out, hold_out = None, None, 0
    else:
        sl_out, tp_out, hold_out = sl, tp, p["hold_min"]

    payload = {
        "account_id":        ACCOUNT_ID,
        "symbol":            sym,
        "position_side":     direction,
        "quantity":          qty,
        "leverage":          LEVERAGE,
        "stop_loss_price":   sl_out,
        "take_profit_price": tp_out,
        "max_hold_minutes":  hold_out,
        "source":            f"strategy_bigmid:{tag}",
    }
    if limit_p and limit_p > 0:
        payload["limit_price"] = limit_p

    try:
        res = _api("POST", "/api/futures/open", json=payload)
    except Exception as e:
        log.error("开仓请求失败 %s %s: %s", sym, direction, e)
        return None, None, False

    data = res.get("data") or {}
    pid  = data.get("position_id") or data.get("id")
    oid  = data.get("order_id")
    pending = (data.get("status") == "PENDING") or (not pid and bool(oid))
    log.info("开仓 %s %s [%s] entry=%.6f lp=%s SL=%.6f TP=%.6f qty=%.4f pid=%s oid=%s %s",
             sym, direction, tier, entry_p, limit_p, sl, tp, qty, pid, oid,
             "[PENDING]" if pending else "")
    return pid, oid, pending


# ── BIG 档：Whale 多维评分 ──────────────────────────────────────
def _get_funding(cur, sym: str) -> Optional[float]:
    cur.execute(
        "SELECT funding_rate FROM funding_rate_data "
        "WHERE symbol=%s AND timestamp >= NOW()-INTERVAL 2 HOUR "
        "ORDER BY timestamp DESC LIMIT 1", (sym,),
    )
    r = cur.fetchone()
    return float(r["funding_rate"]) if r else None


def _get_ls(cur, sym: str) -> Optional[tuple]:
    cur.execute(
        "SELECT long_account, short_account FROM futures_long_short_ratio "
        "WHERE symbol=%s AND timestamp >= NOW()-INTERVAL 4 HOUR "
        "ORDER BY timestamp DESC LIMIT 1", (sym,),
    )
    r = cur.fetchone()
    if not r:
        return None
    la = float(r["long_account"])
    sa = float(r["short_account"])
    # 归一化到 0-1（某些数据源返回 0-100）
    if la > 1.0: la /= 100
    if sa > 1.0: sa /= 100
    return la, sa


def _get_oi_4h_change(cur, sym: str) -> Optional[float]:
    cur.execute(
        "SELECT open_interest_value FROM futures_open_interest "
        "WHERE symbol=%s ORDER BY timestamp DESC LIMIT 5", (sym,),
    )
    rows = cur.fetchall()
    if len(rows) < 4:
        return None
    latest = float(rows[0]["open_interest_value"] or 0)
    oldest = float(rows[-1]["open_interest_value"] or 0)
    if oldest <= 0:
        return None
    return (latest - oldest) / oldest


def _get_1h_bars(cur, sym: str, limit: int = 30) -> list:
    import time as _t
    now_ms = int(_t.time() * 1000)
    cur.execute(
        "SELECT open_time, open_price, high_price, low_price, close_price, "
        "       volume, taker_buy_base_volume "
        "FROM kline_data WHERE symbol=%s AND timeframe='1h' "
        "  AND open_time + 3600000 < %s "
        "ORDER BY open_time DESC LIMIT %s",
        (sym, now_ms, limit),
    )
    return list(reversed(cur.fetchall()))


def compute_whale_score(cur, sym: str, direction: str, p: dict,
                         bars: Optional[list] = None,
                         cur_price: Optional[float] = None) -> tuple:
    """
    BIG 档 Whale 评分。返回 (score, has_trigger, detail)。
    direction ∈ {'short', 'long'}
    """
    score = 0
    detail = {}

    # 1. funding rate
    fr = _get_funding(cur, sym)
    if fr is not None:
        detail["fr"] = round(fr * 100, 4)
        if direction == "short":
            if   fr >= p["fr_extreme_high"]: score += 3
            elif fr >= p["fr_high"]:         score += 2
            elif fr >= p["fr_mild_high"]:    score += 1
        else:
            if   fr <= p["fr_extreme_low"]:  score += 3
            elif fr <= p["fr_low"]:          score += 2
            elif fr <= p["fr_mild_low"]:     score += 1

    # 2. LSR
    ls = _get_ls(cur, sym)
    if ls:
        la, sa = ls
        detail["long_pct"] = round(la, 3)
        if direction == "short":
            if   la >= p["ls_long_extreme"]: score += 2
            elif la >= p["ls_long_high"]:    score += 1
        else:
            if   sa >= p["ls_short_extreme"]: score += 2
            elif sa >= p["ls_short_high"]:    score += 1

    # 3. OI 4h 变化
    oi_chg = _get_oi_4h_change(cur, sym)
    if oi_chg is not None:
        detail["oi_4h"] = round(oi_chg * 100, 2)
        if direction == "short":
            if   oi_chg <= p["oi_drop_strong"]: score += 2
            elif oi_chg <= p["oi_drop_mild"]:   score += 1
        else:
            if   oi_chg >= p["oi_rise_strong"]: score += 2
            elif oi_chg >= p["oi_rise_mild"]:   score += 1

    # 4+5. 1h K 数据：放量滞涨/滞跌 + taker
    if bars is None:
        bars = _get_1h_bars(cur, sym, 30)
    if len(bars) >= 24:
        avg_vol = sum(float(b["volume"] or 0) for b in bars[:-3]) / max(len(bars) - 3, 1)
        last3_vol = sum(float(b["volume"] or 0) for b in bars[-3:]) / 3
        vr = last3_vol / avg_vol if avg_vol > 0 else 1.0
        detail["vol_ratio"] = round(vr, 2)

        first_c = float(bars[-3]["close_price"])
        last_c  = float(bars[-1]["close_price"])
        pc = (last_c - first_c) / first_c if first_c > 0 else 0
        detail["pc_3h"] = round(pc * 100, 2)

        diverged = (vr >= p["vol_ratio_mild"]
                    and abs(pc) < p["stale_price_pct"]
                    and ((direction == "short" and pc > -0.015)
                         or (direction == "long" and pc < 0.015)))
        if diverged:
            score += 3 if vr >= p["vol_ratio_strong"] else 2

        # taker
        taker_ratios = []
        for b in bars[-3:]:
            v = float(b["volume"] or 0); tb = float(b["taker_buy_base_volume"] or 0)
            if v > 0: taker_ratios.append(tb / v)
        if taker_ratios:
            taker = sum(taker_ratios) / len(taker_ratios)
            detail["taker"] = round(taker, 3)
            if direction == "short" and taker < p["taker_sell_thresh"]:
                score += 1
            elif direction == "long" and taker > p["taker_buy_thresh"]:
                score += 1

    # 6. 触发器
    has_trigger = False
    if len(bars) >= 5 and cur_price:
        last = bars[-1]
        o, c = float(last["open_price"]), float(last["close_price"])
        lo4 = min(float(b["low_price"])  for b in bars[-5:-1])
        hi4 = max(float(b["high_price"]) for b in bars[-5:-1])
        if direction == "short":
            big_candle = o > 0 and (o - c) / o >= p["trigger_candle_pct"]
            breakout   = cur_price < lo4 * (1 - p["trigger_breakout"])
            has_trigger = big_candle or breakout
        else:
            big_candle = o > 0 and (c - o) / o >= p["trigger_candle_pct"]
            breakout   = cur_price > hi4 * (1 + p["trigger_breakout"])
            has_trigger = big_candle or breakout

    detail["score"] = score
    detail["trigger"] = has_trigger
    return score, has_trigger, detail


def big_whale_tick(conn, cur, sym: str):
    """BIG 档主逻辑：多/空各算一次评分，取较高方且过门槛+有触发器则开仓"""
    p = TIER_PARAMS["BIG"]

    # 状态机
    ss = get_or_create(conn, "bigmid", sym, "whale",
                       {"state": "IDLE", "pid": 0, "order_id": 0, "entry_p": 0.0,
                        "peak_pnl_pct": 0.0, "entry_time": 0.0, "done_time": 0.0,
                        "last_reason": ""})
    if ss["state"] != "IDLE":
        return
    if ss.get("done_time") and time.time() - float(ss["done_time"]) < COOLDOWN_S_WHALE:
        return

    try:
        price = get_price(sym)
    except Exception as e:
        log.warning("%s 取价失败: %s", sym, e)
        return

    bars = _get_1h_bars(cur, sym, 30)
    if len(bars) < 24:
        return

    s_short, trig_short, d_short = compute_whale_score(cur, sym, "short", p, bars, price)
    s_long,  trig_long,  d_long  = compute_whale_score(cur, sym, "long",  p, bars, price)

    # 双向都评分，择优开仓
    candidates = []
    if s_short >= p["entry_score_min"] and trig_short:
        candidates.append(("SHORT", s_short, d_short))
    if s_long >= p["entry_score_min"] and trig_long:
        candidates.append(("LONG", s_long, d_long))
    if not candidates:
        return
    # 若两边都过门槛，取高分方；同分偏向 short（空头保护更重要）
    candidates.sort(key=lambda x: (-x[1], 0 if x[0] == "SHORT" else 1))
    direction, sc, detail = candidates[0]

    log.info("BIG WHALE 信号 %s %s score=%d detail=%s price=%.6f",
             sym, direction, sc, detail, price)

    lp = None  # 市价入场
    pid, oid, pending = open_order(sym, direction, price, "BIG", "whale-entry", lp)
    if oid or pid:
        update_state(conn, "bigmid", sym, "whale",
                     state="PENDING" if pending else direction,
                     pid=pid or 0, order_id=oid or 0,
                     entry_p=price, entry_time=time.time())


# BIG Whale 冷却（平仓后）
COOLDOWN_S_WHALE = 4 * 3600


# ── MID 档信号：CHASE（追涨）─────────────────────────────────────
def _load_bars(cur, sym: str, tf: str, n: int) -> list:
    cur.execute(
        "SELECT open_time, open_price, high_price, low_price, close_price "
        "FROM kline_data WHERE symbol=%s AND timeframe=%s "
        "ORDER BY open_time DESC LIMIT %s",
        (sym, tf, n + 2),
    )
    rows = list(cur.fetchall())  # pymysql 某些版本 fetchall 返回 tuple，强转 list 以便排序
    rows.sort(key=lambda r: r["open_time"])
    # 丢弃最新一根（可能未收盘）
    return rows[:-1] if rows else []


def chase_tick(conn, cur, sym: str, tier: str):
    p = TIER_PARAMS[tier]
    bars = _load_bars(cur, sym, p["tf"], p["bars_chase"])
    if len(bars) < p["bars_chase"]:
        return
    window = bars[-p["bars_chase"]:]
    o0 = float(window[0]["open_price"])
    c_last = float(window[-1]["close_price"])
    if o0 <= 0:
        return

    pump = (c_last - o0) / o0
    if pump < p["chase_pump_pct"]:
        return

    recent_high = max(float(b["high_price"]) for b in window)
    dd_from_peak = (recent_high - c_last) / recent_high if recent_high > 0 else 0
    if dd_from_peak > p["chase_exhaust_dd"]:
        return

    leader_gain = 0.0
    for b in window:
        o, c = float(b["open_price"]), float(b["close_price"])
        if o > 0:
            g = (c - o) / o
            if g > leader_gain:
                leader_gain = g
    if leader_gain < p["chase_leader_pct"]:
        return

    ss = get_or_create(conn, "bigmid", sym, "chase",
                       {"state": "IDLE", "pid": 0, "order_id": 0, "entry_p": 0.0,
                        "peak_pnl_pct": 0.0, "entry_time": 0.0, "done_time": 0.0,
                        "last_reason": ""})
    if ss["state"] != "IDLE":
        return
    # 冷却（平仓后等 4h）
    if ss.get("done_time") and time.time() - float(ss["done_time"]) < 4 * 3600:
        return

    try:
        price = get_price(sym)
    except Exception as e:
        log.warning("%s 取价失败: %s", sym, e)
        return

    # limit_offset=0 表示市价单（BIG 档），否则按档位偏移挂限价
    if p["limit_offset_pct"] > 0:
        h24, l24 = _get_24h_stats(cur, sym)
        lp = _calc_limit_price("LONG", price, h24, l24, p["limit_offset_pct"])
    else:
        lp = None

    log.info("CHASE 信号 %s [%s] pump=%.2f%% leader=%.2f%% dd=%.2f%% price=%.6f lp=%s",
             sym, tier, pump * 100, leader_gain * 100, dd_from_peak * 100, price, lp if lp else "MARKET")
    pid, oid, pending = open_order(sym, "LONG", price, tier, "chase-entry", lp)
    if oid:
        update_state(conn, "bigmid", sym, "chase",
                     state="PENDING", pid=pid or 0, order_id=oid,
                     entry_p=lp, entry_time=time.time())


# ── 主信号：DUMP（追跌）─────────────────────────────────────────
def dump_tick(conn, cur, sym: str, tier: str):
    p = TIER_PARAMS[tier]
    bars = _load_bars(cur, sym, p["tf"], p["bars_dump"])
    if len(bars) < p["bars_dump"]:
        return
    window = bars[-p["bars_dump"]:]
    o0 = float(window[0]["open_price"])
    c_last = float(window[-1]["close_price"])
    if o0 <= 0:
        return

    drop = (o0 - c_last) / o0
    if drop < p["dump_drop_pct"]:
        return

    min_low = min(float(b["low_price"]) for b in window)
    if min_low <= 0:
        return
    bounce = (c_last - min_low) / min_low
    if bounce > p["dump_bounce_max"]:
        return

    ss = get_or_create(conn, "bigmid", sym, "dump",
                       {"state": "IDLE", "pid": 0, "order_id": 0, "entry_p": 0.0,
                        "peak_pnl_pct": 0.0, "entry_time": 0.0, "done_time": 0.0,
                        "last_reason": ""})
    if ss["state"] != "IDLE":
        return
    if ss.get("done_time") and time.time() - float(ss["done_time"]) < 4 * 3600:
        return

    try:
        price = get_price(sym)
    except Exception as e:
        log.warning("%s 取价失败: %s", sym, e)
        return

    if p["limit_offset_pct"] > 0:
        h24, l24 = _get_24h_stats(cur, sym)
        lp = _calc_limit_price("SHORT", price, h24, l24, p["limit_offset_pct"])
    else:
        lp = None

    log.info("DUMP 信号 %s [%s] drop=%.2f%% bounce=%.2f%% price=%.6f lp=%s",
             sym, tier, drop * 100, bounce * 100, price, lp if lp else "MARKET")
    pid, oid, pending = open_order(sym, "SHORT", price, tier, "dump-entry", lp)
    if oid:
        update_state(conn, "bigmid", sym, "dump",
                     state="PENDING", pid=pid or 0, order_id=oid,
                     entry_p=lp, entry_time=time.time())


# ── 限价填充 + 反向滑点熔断 ─────────────────────────────────────
def _fill_pending_orders(conn):
    """扫描本策略（source LIKE 'strategy_bigmid:%'）的 PENDING 订单，按 tier 熔断或填充"""
    cur = conn.cursor()
    cur.execute("""
        SELECT id, order_id, symbol, side, leverage, quantity,
               price AS limit_price, stop_loss_price, take_profit_price,
               order_source, created_at
        FROM futures_orders
        WHERE account_id=%s AND status='PENDING' AND order_type='LIMIT'
          AND order_source LIKE 'strategy_bigmid:%%'
        ORDER BY created_at ASC
    """, (ACCOUNT_ID,))
    orders = cur.fetchall()
    cur.close()
    if not orders:
        return

    for o in orders:
        sym     = o["symbol"]
        side    = o["side"]
        limit_p = float(o["limit_price"] or 0)
        if limit_p <= 0:
            continue

        # 超时撤单
        if o["created_at"]:
            age = (datetime.datetime.now() - o["created_at"]).total_seconds()
            if age > LIMIT_PENDING_MAX_S:
                c2 = conn.cursor()
                c2.execute("""UPDATE futures_orders SET status='CANCELLED',
                              cancellation_reason='timeout', canceled_at=NOW(),
                              updated_at=NOW() WHERE id=%s""", (o["id"],))
                conn.commit(); c2.close()
                log.info("超时撤单 %s %s oid=%s age=%.0fs", sym, side, o["order_id"], age)
                continue

        # 查 tier 以取阈值
        cur2 = conn.cursor()
        cur2.execute("SELECT quote_volume_24h FROM price_stats_24h WHERE symbol=%s LIMIT 1", (sym,))
        vr = cur2.fetchone(); cur2.close()
        vol = float(vr["quote_volume_24h"] or 0) if vr else 0
        tier = classify_tier(sym, vol)
        if tier is None:
            c2 = conn.cursor()
            c2.execute("""UPDATE futures_orders SET status='CANCELLED',
                          cancellation_reason='tier_downgrade', canceled_at=NOW(),
                          updated_at=NOW() WHERE id=%s""", (o["id"],))
            conn.commit(); c2.close()
            log.info("tier 降档撤单 %s vol=%.0fM", sym, vol / 1e6)
            continue
        rev_limit = TIER_PARAMS[tier]["reverse_slippage"]

        try:
            cur_p = get_price(sym)
        except Exception:
            continue

        pos_side = side.replace("OPEN_", "") if side.startswith("OPEN_") else side
        triggered = (pos_side == "LONG" and cur_p <= limit_p) or \
                    (pos_side == "SHORT" and cur_p >= limit_p)
        if not triggered:
            continue

        # 反向滑点熔断
        rev_slip = ((limit_p - cur_p) / limit_p) if pos_side == "LONG" else ((cur_p - limit_p) / limit_p)
        if rev_slip > rev_limit:
            c2 = conn.cursor()
            c2.execute("""UPDATE futures_orders SET status='CANCELLED',
                          cancellation_reason=%s, canceled_at=NOW(),
                          updated_at=NOW() WHERE id=%s""",
                       (f"reverse_slippage_{rev_slip:.4f}", o["id"]))
            conn.commit(); c2.close()
            log.info("反向滑点熔断 %s %s [%s] limit=%.6f cur=%.6f 偏离=%.2f%% (>%.2f%%)",
                     sym, side, tier, limit_p, cur_p, rev_slip * 100, rev_limit * 100)
            continue

        # 乐观锁：PENDING -> FILLING
        c2 = conn.cursor()
        affected = c2.execute("""UPDATE futures_orders SET status='FILLING',
                                 updated_at=NOW() WHERE id=%s AND status='PENDING'""",
                              (o["id"],))
        conn.commit(); c2.close()
        if not affected:
            continue

        # 以实际成交价重算 SL/TP
        sl = float(o["stop_loss_price"] or 0) or None
        tp = float(o["take_profit_price"] or 0) or None
        if sl and tp and abs(cur_p - limit_p) / limit_p > 0.001:
            if pos_side == "LONG":
                sl_r = (limit_p - sl) / limit_p
                tp_r = (tp - limit_p) / limit_p
                sl = round(cur_p * (1 - sl_r), 8); tp = round(cur_p * (1 + tp_r), 8)
            else:
                sl_r = (sl - limit_p) / limit_p
                tp_r = (limit_p - tp) / limit_p
                sl = round(cur_p * (1 + sl_r), 8); tp = round(cur_p * (1 - tp_r), 8)

        try:
            qty = float(o["quantity"] or 0)
            lev = int(o["leverage"] or LEVERAGE)
            # 总开关: 裸奔模式下,限价单成交也不写 SL/TP/timeout
            if DISABLE_SL_TP_HOLD:
                sl_out, tp_out, hold_out = None, None, 0
            else:
                sl_out, tp_out, hold_out = sl, tp, TIER_PARAMS[tier]["hold_min"]
            payload = {
                "account_id": ACCOUNT_ID, "symbol": sym,
                "position_side": pos_side, "quantity": qty, "leverage": lev,
                "stop_loss_price": sl_out, "take_profit_price": tp_out,
                "source": o.get("order_source") or "strategy_bigmid:limit-fill",
                "fill_price": cur_p,
                "max_hold_minutes": hold_out,
            }
            res  = _api("POST", "/api/futures/open", json=payload)
            data = res.get("data") or {}
            pid  = data.get("position_id") or data.get("id")
            if pid:
                c2 = conn.cursor()
                c2.execute("""UPDATE futures_orders SET status='FILLED',
                              avg_fill_price=%s, fill_time=NOW(),
                              executed_quantity=quantity, executed_value=total_value,
                              position_id=%s, updated_at=NOW() WHERE id=%s""",
                           (cur_p, pid, o["id"]))
                conn.commit(); c2.close()
                log.info("限价单成交 %s %s [%s] @ %.6f pid=%s", sym, side, tier, cur_p, pid)
            else:
                c2 = conn.cursor()
                c2.execute("UPDATE futures_orders SET status='PENDING', updated_at=NOW() WHERE id=%s",
                           (o["id"],))
                conn.commit(); c2.close()
        except Exception as e:
            log.warning("填充异常回退 PENDING %s %s: %s", sym, side, e)
            c2 = conn.cursor()
            c2.execute("UPDATE futures_orders SET status='PENDING', updated_at=NOW() WHERE id=%s",
                       (o["id"],))
            conn.commit(); c2.close()


# ── 持仓监控（SL / TP / trail / timeout）────────────────────────
def _monitor_positions(conn):
    """扫描本策略持仓，执行 SL/TP/trail-tp/timeout 平仓。仅处理 source LIKE 'strategy_bigmid:%'"""
    cur = conn.cursor()
    cur.execute("""
        SELECT id, symbol, position_side, entry_price, stop_loss_price, take_profit_price,
               source, open_time, timeout_at
        FROM futures_positions
        WHERE account_id=%s AND status='open'
          AND source LIKE 'strategy_bigmid:%%'
    """, (ACCOUNT_ID,))
    rows = cur.fetchall()
    cur.close()

    for r in rows:
        sym = r["symbol"]
        side = r["position_side"]
        entry = float(r["entry_price"])
        try:
            cur_p = get_price(sym)
        except Exception:
            continue

        pnl_pct = (cur_p - entry) / entry if side == "LONG" else (entry - cur_p) / entry

        # 总开关开启: 裸奔,不执行硬TP/SL/移动TP检查(存量仓 SL/TP 由 PositionSLTPMonitor 服务按行内价格触发,不受此影响)
        if DISABLE_SL_TP_HOLD:
            continue

        # tier 参数
        c2 = conn.cursor()
        c2.execute("SELECT quote_volume_24h FROM price_stats_24h WHERE symbol=%s LIMIT 1", (sym,))
        vr = c2.fetchone(); c2.close()
        vol = float(vr["quote_volume_24h"] or 0) if vr else 0
        tier = classify_tier(sym, vol) or "MID"   # 降档后仍给 MID 参数处理（保守）
        p = TIER_PARAMS[tier]

        # 硬止盈
        if pnl_pct >= p["hard_tp_pct"]:
            _close(r["id"], cur_p, "hard-tp"); continue
        # 止损
        if pnl_pct <= -p["sl_pct"]:
            _close(r["id"], cur_p, "stop_loss"); continue

        # 移动止盈 — 读状态机 peak_pnl_pct
        # stype 按 source 推断：BIG whale 用 'whale'，MID 按方向用 'chase'/'dump'
        src = r.get("source") or ""
        if "whale-entry" in src:
            stype = "whale"
        else:
            stype = "chase" if side == "LONG" else "dump"
        ss = get_or_create(conn, "bigmid", sym, stype, {"peak_pnl_pct": 0.0})
        peak = max(float(ss.get("peak_pnl_pct") or 0.0), pnl_pct)
        if peak != float(ss.get("peak_pnl_pct") or 0.0):
            update_state(conn, "bigmid", sym, stype, peak_pnl_pct=peak)
        # MID 档使用与 strategy_live/whale 一致的动态 trail（peak 3%/5%/10% → 回落 1%/2%/3%）
        # BIG 档 TP 仅 2%，peak 基本到不了 3%，保留原单档阈值兜底
        if tier == "BIG":
            if peak >= p["trail_tp_start"] and (peak - pnl_pct) >= p["trail_tp_pullback"]:
                _close(r["id"], cur_p, "trail-tp"); continue
        else:
            pullback_thresh = _dynamic_trail_pullback(peak)
            if (peak - pnl_pct) >= pullback_thresh:
                _close(r["id"], cur_p, "trail-tp"); continue
            # 保本止损（曾浮盈 >= 3% 的单，回吐到 -0.5% 平）
            if peak >= BREAKEVEN_AFTER_PEAK_PCT and pnl_pct <= BREAKEVEN_SL_PCT:
                _close(r["id"], cur_p, "breakeven-sl"); continue
            # 早期止损（浮亏达 3%，比 MID 档硬 SL 5% 提前）
            if pnl_pct <= -EARLY_SL_PCT:
                _close(r["id"], cur_p, "early-sl"); continue


def _close(pid: int, close_p: float, reason: str):
    try:
        resp = req.post(f"{API_BASE}/api/futures/close/{pid}",
                        json={"reason": reason, "close_price": close_p}, timeout=10)
        if resp.ok:
            log.info("平仓 pid=%d reason=%s @%.6f", pid, reason, close_p)
        else:
            log.warning("平仓失败 pid=%d: %s", pid, resp.text[:100])
    except Exception as e:
        log.error("平仓异常 pid=%d: %s", pid, e)


# ── 平仓 → 重置状态机 ──────────────────────────────────────────
def _settle_closed_positions(conn):
    """扫描本策略 closed 持仓，若状态机仍是 LONG/SHORT 则标记 DONE 并启动冷却"""
    cur = conn.cursor()
    cur.execute("""
        SELECT id, symbol, position_side, source, notes, close_time
        FROM futures_positions
        WHERE account_id=%s AND status='closed'
          AND source LIKE 'strategy_bigmid:%%'
          AND close_time >= NOW() - INTERVAL 30 MINUTE
    """, (ACCOUNT_ID,))
    rows = cur.fetchall()
    cur.close()
    for r in rows:
        src = r.get("source") or ""
        if "whale-entry" in src:
            stype = "whale"
        else:
            stype = "chase" if r["position_side"] == "LONG" else "dump"
        ss = get_or_create(conn, "bigmid", r["symbol"], stype, {"state": "IDLE"})
        if ss.get("state") in ("LONG", "SHORT", "PENDING"):
            update_state(conn, "bigmid", r["symbol"], stype,
                         state="DONE", done_time=time.time(),
                         last_reason=r.get("notes") or "")


# ── 主循环 ──────────────────────────────────────────────────────
def main():
    _load_bigmid_config()
    log.info("=" * 60)
    log.info("strategy_bigmid 启动 account=%d LEVERAGE=%dx MARGIN=%.0f",
             ACCOUNT_ID, LEVERAGE, MARGIN)
    for tier, p in TIER_PARAMS.items():
        if p["kind"] == "whale":
            log.info("  %s (whale) tf=%s score_min=%d trigger=%.2f%% FR_extreme=%.4f%% "
                     "SL=%.1f%% TP=%.1f%% hold=%dh",
                     tier, p["tf"], p["entry_score_min"],
                     p["trigger_candle_pct"] * 100, p["fr_extreme_high"] * 100,
                     p["sl_pct"] * 100, p["hard_tp_pct"] * 100, p["hold_min"] // 60)
        else:
            log.info("  %s (trend) tf=%s chase=%.1f%% dump=%.1f%% SL=%.1f%% TP=%.1f%% "
                     "rev_slip=%.2f%% hold=%dh",
                     tier, p["tf"], p["chase_pump_pct"] * 100, p["dump_drop_pct"] * 100,
                     p["sl_pct"] * 100, p["hard_tp_pct"] * 100,
                     p["reverse_slippage"] * 100, p["hold_min"] // 60)
    log.info("=" * 60)

    conn = _db_conn()
    try:
        ensure_table(conn)
    except Exception as e:
        log.error("ensure_table 失败: %s", e)

    while True:
        try:
            cur = conn.cursor()
            bigs, mids, vol_map = refresh_symbols(cur)
            cur.close()

            _fill_pending_orders(conn)
            _monitor_positions(conn)
            _settle_closed_positions(conn)

            # BIG 档走 Whale 评分（funding/LSR/OI/放量/taker/触发器）
            for sym in bigs:
                try:
                    cur = conn.cursor()
                    big_whale_tick(conn, cur, sym)
                    cur.close()
                except Exception as e:
                    log.warning("BIG %s whale_tick 异常: %s", sym, e)
            # MID 档继续 CHASE/DUMP（趋势追踪）
            for sym in mids:
                try:
                    cur = conn.cursor()
                    chase_tick(conn, cur, sym, "MID")
                    dump_tick(conn, cur, sym, "MID")
                    cur.close()
                except Exception as e:
                    log.warning("MID %s tick 异常: %s", sym, e)
        except pymysql.err.Error as e:
            log.error("主循环 DB 错误，重连: %s", e)
            try: conn.close()
            except Exception: pass
            time.sleep(5)
            conn = _db_conn()
        except Exception as e:
            log.error("主循环异常: %s", e)

        time.sleep(POLL_SECS)


if __name__ == "__main__":
    main()
