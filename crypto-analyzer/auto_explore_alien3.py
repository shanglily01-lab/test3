#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
auto_explore_alien3.py
======================
第三批非人类原语策略探索。

前两批已覆盖:
  Batch1: wick_asym / body_entropy / sell_saturation / spatial_close /
          momentum_ratio / cross_residual / vol_absorption / candle_dna
  Batch2: saturation_velocity / path_tortuosity / amplitude_regime /
          shadow_strength / vol_concentration / time_pressure /
          price_memory / flux_momentum

本批新维度（净流量 + 实体结构 + 流量记忆 + 量能加速 + 蜡烛一致性）：

  order_flow_delta(cs, n)         -- 订单净流量: (buy_vol-sell_vol)/total_vol 归一化
  body_dominance(cs, n)           -- 实体主导度: avg(abs(c-o)/(h-l)) 方向感强弱
  flux_memory(cs, n)              -- 流量记忆: flux在近期区间内的相对位置 (仿price_memory)
  vol_momentum(cs, n, lag)        -- 成交量加速度: vol_now/vol_past - 1
  close_consistency(cs, n)        -- 收盘一致性: 收盘偏上(>0.5)的K线占比
  amplitude_skew(cs, n)           -- 振幅偏斜: 上影线均值/下影线均值 - 1
  price_velocity(cs, n, amp_n)    -- 价格速度: gradient(n)/amplitude(amp_n) 振幅归一化
  entropy_velocity(cs, n, lag)    -- 熵速率: body_entropy变化量(负=市场变有序)

自动部署：
  每通过10个策略 -> 写入 strategy_params DB + 追加 alien_signals.py + 文档
"""

import argparse
import bisect
import csv
import math
import os
import random
import sys
import textwrap
from datetime import datetime
from pathlib import Path

import pymysql
from dotenv import load_dotenv

from explored_filter import load_deployed_names, filter_new_strategies

load_dotenv()

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ── 常量 ──────────────────────────────────────────────────────────────────────

HOLD_BARS   = 3
SL_MIN, SL_MAX, SL_MULT, TP_MULT = 0.005, 0.020, 1.5, 2.5
TRAIN_RATIO = 0.70
TEST_MIN_N  = 10

BIG4 = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT"]
STAGE1_MIN_N, STAGE1_MIN_WR = 5,  0.57
STAGE2_MIN_N, STAGE2_MIN_WR = 15, 0.55
STAGE3_MIN_N, STAGE3_MIN_WR = 30, 0.57

AUTO_DEPLOY_BATCH = 10

ALT99 = [
    "ETH/USDT","SOL/USDT","ZEC/USDT","XRP/USDT","DOGE/USDT","HYPE/USDT",
    "1000PEPE/USDT","BNB/USDT","TAO/USDT","ENJ/USDT","ADA/USDT","ENA/USDT",
    "AVAX/USDT","LINK/USDT","SUI/USDT","DOT/USDT","WLD/USDT","AAVE/USDT",
    "NEAR/USDT","FIL/USDT","LTC/USDT","BCH/USDT","UNI/USDT","TRX/USDT",
    "1000SHIB/USDT","PENGU/USDT","FET/USDT","CRV/USDT","1000BONK/USDT",
    "APT/USDT","WIF/USDT","VIRTUAL/USDT","LDO/USDT","GALA/USDT","TON/USDT",
    "HBAR/USDT","NEIRO/USDT","ARB/USDT","ONDO/USDT","XLM/USDT","ALGO/USDT",
    "OP/USDT","RENDER/USDT","ETC/USDT","JTO/USDT","DRIFT/USDT","ORDI/USDT",
    "TRU/USDT","XMR/USDT","CAKE/USDT","ONT/USDT","TIA/USDT","DUSK/USDT",
    "AXS/USDT","ICP/USDT","ZRO/USDT","POL/USDT","CHZ/USDT","ATOM/USDT",
    "SEI/USDT","BLUR/USDT","INJ/USDT","BOME/USDT","SAND/USDT","ETHFI/USDT",
    "STRK/USDT","CTSI/USDT","PENDLE/USDT","EDU/USDT","JUP/USDT",
    "W/USDT","XVG/USDT","IMX/USDT","SUPER/USDT","ID/USDT","SNX/USDT",
    "COMP/USDT","IOTA/USDT","VET/USDT","ANKR/USDT","ROSE/USDT","XTZ/USDT",
    "1000LUNC/USDT","PYTH/USDT","ARKM/USDT","APE/USDT",
]

_DB_CFG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 3306)),
    "user":     os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "binance-data"),
    "charset":  "utf8mb4", "autocommit": True,
}

BASE_DIR   = Path(__file__).parent
LOG_DIR    = BASE_DIR / "logs"
SIGNALS_PY = BASE_DIR / "alien_signals.py"
DOC_FILE   = BASE_DIR / "alien_strategies_doc.md"


# ════════════════════════════════════════════════════════════════════════════════
# 数据加载
# ════════════════════════════════════════════════════════════════════════════════

def load_data(symbols):
    conn = pymysql.connect(**_DB_CFG)
    d1h = {}; d4h = {}
    try:
        with conn.cursor() as cur:
            for sym in symbols:
                for tf, store in [("1h", d1h), ("4h", d4h)]:
                    cur.execute("""
                        SELECT timestamp, open_price, high_price, low_price,
                               close_price, volume, taker_buy_base_volume
                        FROM kline_data
                        WHERE symbol=%s AND timeframe=%s
                          AND taker_buy_base_volume IS NOT NULL AND volume > 0
                        ORDER BY timestamp ASC
                    """, (sym, tf))
                    rows = cur.fetchall()
                    if rows:
                        store[sym] = [{"t": r[0],
                                       "open":    float(r[1]),
                                       "high":    float(r[2]),
                                       "low":     float(r[3]),
                                       "close":   float(r[4]),
                                       "vol":     float(r[5]),
                                       "buy_vol": float(r[6])} for r in rows]
    finally:
        conn.close()
    return d1h, d4h


# ════════════════════════════════════════════════════════════════════════════════
# 基础工具
# ════════════════════════════════════════════════════════════════════════════════

def _amplitude(cs, n):
    if len(cs) < n: return 0.0
    avg = sum(c["high"] - c["low"] for c in cs[-n:]) / n
    ref = cs[-1]["close"]
    return avg / ref if ref else 0.0

def sl_tp(cs):
    amp = _amplitude(cs, 6)
    amp = max(SL_MIN, min(SL_MAX, amp))
    return amp * SL_MULT, amp * TP_MULT

def gradient(cs, n):
    if len(cs) < n: return 0.0
    s = sum(c["close"] - c["open"] for c in cs[-n:])
    ref = cs[-1]["close"]
    return s / ref if ref else 0.0

def flux(cs, n):
    if len(cs) < n: return 0.5
    rs = [c["buy_vol"] / c["vol"] for c in cs[-n:] if c["vol"] > 0]
    return sum(rs) / len(rs) if rs else 0.5

def sell_saturation(cs, n):
    if len(cs) < n: return 0.5
    rs = [(c["vol"] - c["buy_vol"]) / c["vol"]
          for c in cs[-n:] if c["vol"] > 0]
    return sum(rs) / len(rs) if rs else 0.5

def spatial_close(cs, n):
    if len(cs) < n: return 0.5
    scores = []
    for c in cs[-n:]:
        rng = c["high"] - c["low"]
        scores.append((c["close"] - c["low"]) / rng if rng > 0 else 0.5)
    return sum(scores) / len(scores) if scores else 0.5

_align4h_cache: dict = {}   # id(cs4h) -> sorted timestamp list

def align4h(cs1h, cs4h, i, _keep=30):
    """返回 cs4h 中 t <= cs1h[i].t 的最后 _keep 根（O(log N) 用 bisect）"""
    cs4h_id = id(cs4h)
    if cs4h_id not in _align4h_cache:
        _align4h_cache[cs4h_id] = [c["t"] for c in cs4h]
    times = _align4h_cache[cs4h_id]
    t1 = cs1h[i]["t"]
    idx = bisect.bisect_right(times, t1)  # O(log N)
    start = max(0, idx - _keep)
    return cs4h[start:idx]


# ════════════════════════════════════════════════════════════════════════════════
# [新原语] 第三批非人类指标
# ════════════════════════════════════════════════════════════════════════════════

def order_flow_delta(cs, n):
    """
    订单净流量 (Order Flow Delta)
    定义: sum(buy_vol - sell_vol) / sum(vol) over n bars
    正值 -> 净买方主导; 负值 -> 净卖方主导
    与 sell_saturation 的区别: 这是累积净量占比，不是逐根均值
    """
    if len(cs) < n: return 0.0
    net   = sum(c["buy_vol"] - (c["vol"] - c["buy_vol"]) for c in cs[-n:])
    total = sum(c["vol"] for c in cs[-n:])
    return net / total if total > 0 else 0.0


def body_dominance(cs, n):
    """
    实体主导度 (Body Dominance)
    定义: avg(abs(close - open) / (high - low)) over n bars
    高值(>0.6) -> 每根K线都有方向感(实体大，趋势明确)
    低值(<0.3) -> 影线主导，方向不清（横盘/犹豫）
    """
    if len(cs) < n: return 0.5
    scores = []
    for c in cs[-n:]:
        rng = c["high"] - c["low"]
        scores.append(abs(c["close"] - c["open"]) / rng if rng > 0 else 0.0)
    return sum(scores) / len(scores) if scores else 0.5


def flux_memory(cs, n):
    """
    流量记忆系数 (Flux Memory)
    定义: (flux_now - flux_min) / (flux_max - flux_min) over n bars
    1.0 -> 当前买方比例在近期最高点（买压极端，可能耗竭）
    0.0 -> 当前买方比例在近期最低点（卖压极端，可能投降）
    类比 price_memory，但作用于 flux(买卖比)
    """
    if len(cs) < n: return 0.5
    fluxes = [c["buy_vol"] / c["vol"] for c in cs[-n:] if c["vol"] > 0]
    if len(fluxes) < max(2, n // 2): return 0.5
    hi = max(fluxes); lo = min(fluxes)
    rng = hi - lo
    if rng <= 0: return 0.5
    return (fluxes[-1] - lo) / rng


def vol_momentum(cs, n, lag):
    """
    成交量动量 (Volume Momentum)
    定义: avg_vol(now, n) / avg_vol(lag bars ago, n) - 1
    正值 -> 成交量在放大（趋势在加速）
    负值 -> 成交量在萎缩（趋势在减速/信号减弱）
    """
    if len(cs) < n + lag: return 0.0
    v_now  = sum(c["vol"] for c in cs[-n:]) / n
    v_past = sum(c["vol"] for c in cs[-n-lag:-lag]) / n
    return (v_now / v_past - 1) if v_past > 0 else 0.0


def close_consistency(cs, n):
    """
    收盘一致性 (Close Consistency)
    定义: 最近n根K线中收盘价偏上半区(>50%位置)的比例
    高值(>0.75) -> 持续收在区间上半部（看涨）
    低值(<0.25) -> 持续收在区间下半部（看跌）
    """
    if len(cs) < n: return 0.5
    count = 0.0
    for c in cs[-n:]:
        rng = c["high"] - c["low"]
        if rng <= 0:
            count += 0.5
        elif (c["close"] - c["low"]) / rng > 0.5:
            count += 1.0
    return count / n


def amplitude_skew(cs, n):
    """
    振幅偏斜度 (Amplitude Skew)
    定义: avg(upper_wick/range) - avg(lower_wick/range) over n bars
    正值 -> 上影线更大（上方承压，卖方在高位拦截）
    负值 -> 下影线更大（下方支撑，买方在低位托底）
    """
    if len(cs) < n: return 0.0
    upper = []
    lower = []
    for c in cs[-n:]:
        rng = c["high"] - c["low"]
        if rng <= 0: continue
        bt = max(c["open"], c["close"])
        bb = min(c["open"], c["close"])
        upper.append((c["high"] - bt) / rng)
        lower.append((bb - c["low"])  / rng)
    if not upper: return 0.0
    return sum(upper) / len(upper) - sum(lower) / len(lower)


def price_velocity(cs, n, amp_n):
    """
    价格速度 (Price Velocity, 振幅归一化动量)
    定义: gradient(n) / amplitude(amp_n)
    消除不同标的绝对波动率差异，统一衡量"相对于波动率的动量强度"
    高正值 -> 相对波动率的大幅上涨（过度延伸，空头信号）
    高负值 -> 相对波动率的大幅下跌（过度延伸，多头信号）
    """
    if len(cs) < max(n, amp_n) + 2: return 0.0
    g   = gradient(cs, n)
    amp = _amplitude(cs, amp_n)
    return g / amp if amp > 0 else 0.0


def entropy_velocity(cs, n, lag):
    """
    熵速率 (Entropy Velocity)
    定义: body_entropy(now) - body_entropy(lag bars ago)
    正值 -> 市场在变混乱（多空共识瓦解，震荡加剧）
    负值 -> 市场在变有序（方向共识形成，即将定向突破）
    """
    if len(cs) < n + lag: return 0.0
    def _body_entropy(window):
        if len(window) < 2: return 0.0
        up = sum(1 for c in window if c["close"] >= c["open"]) / len(window)
        dn = 1.0 - up
        if up <= 0 or dn <= 0: return 0.0
        return -(up * math.log(up) + dn * math.log(dn))
    return _body_entropy(cs[-n:]) - _body_entropy(cs[-n-lag:-lag])


# ════════════════════════════════════════════════════════════════════════════════
# 回测引擎（与 batch2 完全一致）
# ════════════════════════════════════════════════════════════════════════════════

def bt_mtf(fn, cs1h, cs4h, i_start=22, i_end=None):
    stats = {"n": 0, "win": 0, "pnl": []}
    n = len(cs1h)
    if i_end is None:
        i_end = n - HOLD_BARS
    for i in range(i_start, i_end):
        s4 = align4h(cs1h, cs4h, i)
        if len(s4) < 4:
            continue
        try:
            sig = fn(cs1h[:i + 1], s4)
        except Exception:
            continue
        if sig not in ("LONG", "SHORT"):
            continue
        entry = cs1h[i]["close"]
        sl_pct, tp_pct = sl_tp(cs1h[:i + 1])
        sl_abs = entry * sl_pct
        tp_abs = entry * tp_pct
        outcome = None
        for j in range(1, HOLD_BARS + 1):
            if i + j >= n:
                break
            nxt = cs1h[i + j]
            if sig == "LONG":
                if entry - nxt["low"] >= sl_abs:
                    outcome = -sl_pct; break
                if nxt["high"] - entry >= tp_abs:
                    outcome = tp_pct; break
            else:
                if nxt["high"] - entry >= sl_abs:
                    outcome = -sl_pct; break
                if entry - nxt["low"] >= tp_abs:
                    outcome = tp_pct; break
        if outcome is None:
            lj = min(HOLD_BARS, n - i - 1)
            if lj > 0:
                outcome = (cs1h[i + lj]["close"] - entry) / entry
                if sig == "SHORT":
                    outcome = -outcome
        if outcome is None:
            continue
        stats["n"] += 1
        stats["pnl"].append(outcome)
        if outcome > 0:
            stats["win"] += 1
    return stats


def run_strat(fn, mode, d1h, d4h, symbols, phase="all"):
    agg = {"n": 0, "win": 0, "pnl": []}
    per = {}
    for sym in symbols:
        cs1h = d1h.get(sym, [])
        if len(cs1h) < 30:
            continue
        n1h = len(cs1h)
        split_i = int(n1h * TRAIN_RATIO)
        if phase == "train":   i_start, i_end = 22, split_i
        elif phase == "test":  i_start, i_end = split_i, n1h - HOLD_BARS
        else:                  i_start, i_end = 22, None
        try:
            if mode == "mtf_self":
                cs4h = d4h.get(sym, [])
                if len(cs4h) < 10: continue
                s = bt_mtf(fn, cs1h, cs4h, i_start, i_end)
            elif mode == "mtf_btc":
                if sym == "BTC/USDT": continue
                btc4h = d4h.get("BTC/USDT", [])
                if len(btc4h) < 10: continue
                s = bt_mtf(fn, cs1h, btc4h, i_start, i_end)
            else:
                continue
        except Exception:
            continue
        agg["n"] += s["n"]
        agg["win"] += s["win"]
        agg["pnl"] += s["pnl"]
        per[sym] = s
    return agg, per


# ════════════════════════════════════════════════════════════════════════════════
# 四阶段验证
# ════════════════════════════════════════════════════════════════════════════════

def validate_4stage(strategies, d1h, d4h):
    print(f"\n  --- S1 [Big4 train] ---")
    s1_pass = []
    for st in strategies:
        agg, _ = run_strat(st["fn"], st["mode"], d1h, d4h, BIG4, "train")
        n = agg["n"]
        wr = agg["win"] / n if n > 0 else 0
        ev = sum(agg["pnl"]) / n * 100 if n > 0 else 0
        ok = n >= STAGE1_MIN_N and wr >= STAGE1_MIN_WR
        tag = "PASS" if ok else "----"
        print(f"  {tag}  {st['name']:42s}  n={n:4d}  wr={wr*100:5.1f}%  ev={ev:+.2f}%")
        if ok:
            st["s1"] = {"n": n, "wr": wr}
            s1_pass.append(st)
    print(f"  S1: {len(s1_pass)}/{len(strategies)} passed")
    if not s1_pass:
        return []

    candidates = [s for s in ALT99 if s not in set(BIG4)]
    sample10   = random.sample(candidates, min(10, len(candidates)))
    print(f"\n  --- S2 [10 alts train] ---")
    s2_pass = []
    for st in s1_pass:
        agg, _ = run_strat(st["fn"], st["mode"], d1h, d4h, sample10, "train")
        n = agg["n"]
        wr = agg["win"] / n if n > 0 else 0
        ev = sum(agg["pnl"]) / n * 100 if n > 0 else 0
        ok = n >= STAGE2_MIN_N and wr >= STAGE2_MIN_WR
        tag = "PASS" if ok else "----"
        print(f"  {tag}  {st['name']:42s}  n={n:4d}  wr={wr*100:5.1f}%  ev={ev:+.2f}%")
        if ok:
            st["s2"] = {"n": n, "wr": wr}
            s2_pass.append(st)
    print(f"  S2: {len(s2_pass)}/{len(s1_pass)} passed")
    if not s2_pass:
        return []

    print(f"\n  --- S3 [All {len(ALT99)} alts train] ---")
    s3_pass = []
    for st in s2_pass:
        agg, per = run_strat(st["fn"], st["mode"], d1h, d4h, ALT99, "train")
        n = agg["n"]
        wr = agg["win"] / n if n > 0 else 0
        ev = sum(agg["pnl"]) / n * 100 if n > 0 else 0
        ok = n >= STAGE3_MIN_N and wr >= STAGE3_MIN_WR
        tag = "PASS" if ok else "----"
        top = sorted(
            [(sym, ss["n"], ss["win"] / ss["n"] if ss["n"] > 0 else 0)
             for sym, ss in per.items() if ss["n"] >= 3],
            key=lambda x: -x[2]
        )
        print(f"  {tag}  {st['name']:42s}  n={n:5d}  wr={wr*100:5.1f}%  ev={ev:+.2f}%")
        if top:
            print(f"        Top: {', '.join(f'{s}({w*100:.0f}%/{c})' for s,c,w in top[:5])}")
        if ok:
            st["s3"] = {"n": n, "wr": wr, "ev": ev}
            s3_pass.append(st)
    print(f"  S3: {len(s3_pass)}/{len(s2_pass)} passed")
    if not s3_pass:
        return []

    print(f"\n  --- S4 [test 30% walk-forward] ---")
    passed = []
    for st in s3_pass:
        agg_t, _ = run_strat(st["fn"], st["mode"], d1h, d4h, ALT99, "test")
        nt  = agg_t["n"]
        wrt = agg_t["win"] / nt if nt > 0 else 0
        evt = sum(agg_t["pnl"]) / nt * 100 if nt > 0 else 0
        s3  = st["s3"]
        if   nt < TEST_MIN_N:           verdict = "LOW-N "
        elif wrt >= STAGE3_MIN_WR:      verdict = "PASS  "
        elif wrt >= STAGE3_MIN_WR-0.05: verdict = "border"
        else:                           verdict = "FAIL  "
        print(f"  {verdict}  {st['name']:42s}  "
              f"train={s3['wr']*100:5.1f}%  |  "
              f"TEST n={nt:4d} wr={wrt*100:5.1f}%  ev={evt:+.2f}%")
        if verdict.strip() == "PASS":
            passed.append({
                "name":    st["name"],
                "mode":    st["mode"],
                "fn":      st["fn"],
                "theme":   st.get("theme", "unknown"),
                "doc":     st.get("doc", ""),
                "code":    st.get("code", ""),
                "s3_n":    s3["n"],  "s3_wr":  s3["wr"],  "s3_ev":  s3["ev"],
                "test_n":  nt,       "test_wr": wrt,       "test_ev": evt,
            })
    return passed


# ════════════════════════════════════════════════════════════════════════════════
# 自动部署
# ════════════════════════════════════════════════════════════════════════════════

def _strategy_params_exist(name):
    conn = pymysql.connect(**_DB_CFG)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM strategy_params WHERE strategy_name=%s", (name,))
            return cur.fetchone() is not None
    finally:
        conn.close()


def deploy_strategies(passed_batch):
    if not passed_batch:
        return
    conn = pymysql.connect(**_DB_CFG)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    inserted = []
    try:
        with conn.cursor() as cur:
            for p in passed_batch:
                name = p["name"]
                if _strategy_params_exist(name):
                    print(f"  [DEPLOY] SKIP {name} (already in DB)")
                    continue
                sl_pct = round((SL_MIN + SL_MAX) / 2 * SL_MULT, 4)
                tp_pct = round((SL_MIN + SL_MAX) / 2 * TP_MULT, 4)
                hold_h = HOLD_BARS
                notes  = (f"alien3_{p['theme']} | "
                          f"train_wr={p['s3_wr']*100:.1f}% n={p['s3_n']} | "
                          f"test_wr={p['test_wr']*100:.1f}% n={p['test_n']}")
                cur.execute("""
                    INSERT INTO strategy_params
                        (strategy_name, sl_pct, tp_pct, hold_h, source,
                         signal_count, backtest_wr, notes, updated_at, created_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                        backtest_wr=VALUES(backtest_wr),
                        notes=VALUES(notes),
                        updated_at=VALUES(updated_at)
                """, (name, sl_pct, tp_pct, hold_h, "auto_explore_alien3",
                      p["s3_n"], round(p["test_wr"], 4), notes, now, now))
                inserted.append(name)
        conn.commit()
    finally:
        conn.close()

    if inserted:
        print(f"  [DEPLOY] DB: {len(inserted)} 个策略写入 strategy_params")

    _ensure_signals_file()
    with open(SIGNALS_PY, "a", encoding="utf-8") as f:
        for p in passed_batch:
            if p["name"] not in inserted:
                continue
            code = p.get("code", "")
            if code:
                f.write(f"\n\n# [AUTO] {p['name']}  "
                        f"test_wr={p['test_wr']*100:.1f}%  "
                        f"deployed={now[:10]}\n")
                f.write(code)
    print(f"  [DEPLOY] Code: alien_signals.py 已更新")

    with open(DOC_FILE, "a", encoding="utf-8") as f:
        for p in passed_batch:
            if p["name"] not in inserted:
                continue
            f.write(f"\n## {p['name']}\n\n")
            f.write(f"- **主题**: {p['theme']}\n")
            f.write(f"- **方向**: {'LONG' if 'LONG' in p['doc'] else 'SHORT'}\n")
            f.write(f"- **训练胜率**: {p['s3_wr']*100:.1f}% (n={p['s3_n']})\n")
            f.write(f"- **测试胜率**: {p['test_wr']*100:.1f}% (n={p['test_n']})\n")
            f.write(f"- **期望值**: {p['test_ev']:+.2f}%/笔\n")
            if p.get("doc"):
                f.write(f"\n{p['doc']}\n")
            f.write("\n---\n")
    print(f"  [DEPLOY] Doc: alien_strategies_doc.md 已更新")


def _ensure_signals_file():
    if not SIGNALS_PY.exists():
        with open(SIGNALS_PY, "w", encoding="utf-8") as f:
            f.write('#!/usr/bin/env python3\n# -*- coding: utf-8 -*-\n')
            f.write('"""\nalien_signals.py\n由 auto_explore_alien*.py 自动生成。\n"""\n\n')
    if not DOC_FILE.exists():
        with open(DOC_FILE, "w", encoding="utf-8") as f:
            f.write("# Alien Strategy Registry\n\n")
            f.write("由 `auto_explore_alien*.py` 自动生成。\n\n---\n")


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 1] OrderFlowPolarization
# 订单净流量极化: 净买/卖方力量到达极端 -> 即将反转
# 物理类比: 电荷极化 — 电场强度到达临界值后，必然发生跃迁
# ════════════════════════════════════════════════════════════════════════════════

def make_ofd_long(n=5, ofd_th=-0.10, mac_n=4, mac_th=0.001):
    """
    OFDLong: 净卖方极端 + 4h上行 = 投降式抛售 -> 多
    条件: order_flow_delta < -ofd_th (净卖压过重) + 宏观上行
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        if order_flow_delta(cs1h, n) >= -abs(ofd_th):
            return None
        return "LONG"
    return signal


def make_ofd_short(n=5, ofd_th=0.10, mac_n=4, mac_th=-0.001):
    """
    OFDShort: 净买方极端 + 4h下行 = 虚假买盘 -> 空
    条件: order_flow_delta > ofd_th (净买压过重) + 宏观下行
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        if order_flow_delta(cs1h, n) <= ofd_th:
            return None
        return "SHORT"
    return signal


def theme_order_flow_polarization():
    strats = []
    for n in [3, 5, 8]:
        for th in [0.10, 0.15, 0.20, 0.25]:
            name = f"OFD_L_n{n}_t{int(th*100)}"
            fn   = make_ofd_long(n=n, ofd_th=th)
            doc  = f"净流量极化 LONG: {n}根净卖压>{th*100:.0f}% + 4h上行"
            strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                            "theme": "OrderFlowPolarization", "doc": doc, "code": ""})
            name = f"OFD_S_n{n}_t{int(th*100)}"
            fn   = make_ofd_short(n=n, ofd_th=th)
            doc  = f"净流量极化 SHORT: {n}根净买压>{th*100:.0f}% + 4h下行"
            strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                            "theme": "OrderFlowPolarization", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 2] BodyDominanceBurst
# 实体突变: 从犹豫(低body_dominance)到方向确认 -> 趋势启动
# 物理类比: 相变 — 从无序液态到有序晶态的临界转变
# ════════════════════════════════════════════════════════════════════════════════

def make_body_indecision_long(hist_n=6, now_n=2, bd_hi_th=0.25, mac_n=4, mac_th=0.001):
    """
    BodyIndecisionLong: 历史低实体主导(犹豫) + 宏观上行 = 蓄力完成即将向上
    条件: 过去 hist_n 根 body_dominance 低(<bd_hi_th) + 4h上行
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < hist_n + now_n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        if body_dominance(cs1h[:-now_n], hist_n) >= bd_hi_th:
            return None
        # 最近几根开始有方向
        if gradient(cs1h, now_n) <= 0:
            return None
        return "LONG"
    return signal


def make_body_indecision_short(hist_n=6, now_n=2, bd_hi_th=0.25, mac_n=4, mac_th=-0.001):
    def signal(cs1h, cs4h):
        if len(cs1h) < hist_n + now_n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        if body_dominance(cs1h[:-now_n], hist_n) >= bd_hi_th:
            return None
        if gradient(cs1h, now_n) >= 0:
            return None
        return "SHORT"
    return signal


def make_body_conviction_short(n=5, bd_lo_th=0.55, mac_n=4, mac_th=-0.001):
    """
    BodyConvictionShort: 高实体主导 + 方向向下 + 4h下行 = 强力下跌延续
    条件: body_dominance 高 + 近期下跌 + 宏观下行
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        if body_dominance(cs1h, n) <= bd_lo_th:
            return None
        # 多数K线为阴线
        bearish = sum(1 for c in cs1h[-n:] if c["close"] < c["open"]) / n
        if bearish <= 0.6:
            return None
        return "SHORT"
    return signal


def make_body_conviction_long(n=5, bd_lo_th=0.55, mac_n=4, mac_th=0.001):
    def signal(cs1h, cs4h):
        if len(cs1h) < n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        if body_dominance(cs1h, n) <= bd_lo_th:
            return None
        bullish = sum(1 for c in cs1h[-n:] if c["close"] >= c["open"]) / n
        if bullish <= 0.6:
            return None
        return "LONG"
    return signal


def theme_body_dominance_burst():
    strats = []
    for hist_n in [4, 6, 8]:
        for bd_th in [0.20, 0.30]:
            name = f"BD_Indc_L_h{hist_n}_t{int(bd_th*100)}"
            fn   = make_body_indecision_long(hist_n=hist_n, bd_hi_th=bd_th)
            doc  = f"实体犹豫突变 LONG: {hist_n}根实体<{bd_th*100:.0f}%后方向确认 + 4h上行"
            strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                            "theme": "BodyDominanceBurst", "doc": doc, "code": ""})
            name = f"BD_Indc_S_h{hist_n}_t{int(bd_th*100)}"
            fn   = make_body_indecision_short(hist_n=hist_n, bd_hi_th=bd_th)
            doc  = f"实体犹豫突变 SHORT: {hist_n}根实体<{bd_th*100:.0f}%后方向确认 + 4h下行"
            strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                            "theme": "BodyDominanceBurst", "doc": doc, "code": ""})
    for n in [4, 6]:
        for bd_th in [0.50, 0.60, 0.65]:
            name = f"BD_Conv_S_n{n}_t{int(bd_th*100)}"
            fn   = make_body_conviction_short(n=n, bd_lo_th=bd_th)
            doc  = f"强力实体延续 SHORT: {n}根实体>{bd_th*100:.0f}%+阴线>60% + 4h下行"
            strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                            "theme": "BodyDominanceBurst", "doc": doc, "code": ""})
            name = f"BD_Conv_L_n{n}_t{int(bd_th*100)}"
            fn   = make_body_conviction_long(n=n, bd_lo_th=bd_th)
            doc  = f"强力实体延续 LONG: {n}根实体>{bd_th*100:.0f}%+阳线>60% + 4h上行"
            strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                            "theme": "BodyDominanceBurst", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 3] FluxMemoryExtreme
# 流量记忆极端: 买方力量在近期区间内处于极端位置 -> 均值回归
# 物理类比: 势阱中的粒子 — 到达势垒边界时动能耗尽，必然反弹
# ════════════════════════════════════════════════════════════════════════════════

def make_flux_mem_short(n=12, fm_hi=0.85, mac_n=4, mac_th=-0.001, sc_th=0.50):
    """
    FluxMemShort: flux在近期最高点(>fm_hi) + 4h下行 + 最近收盘偏低
    = 买方力量顶部耗竭 -> 空
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        if flux_memory(cs1h, n) <= fm_hi:
            return None
        if spatial_close(cs1h, 2) >= sc_th:
            return None
        return "SHORT"
    return signal


def make_flux_mem_long(n=12, fm_lo=0.15, mac_n=4, mac_th=0.001, sc_th=0.50):
    """
    FluxMemLong: flux在近期最低点(<fm_lo) + 4h上行 + 最近收盘偏高
    = 卖方力量底部耗竭 -> 多
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        if flux_memory(cs1h, n) >= fm_lo:
            return None
        if spatial_close(cs1h, 2) <= sc_th:
            return None
        return "LONG"
    return signal


def theme_flux_memory_extreme():
    strats = []
    for n in [8, 12, 16, 20]:
        for hi_th in [0.80, 0.85, 0.90]:
            name = f"FluxMem_S_n{n}_hi{int(hi_th*100)}"
            fn   = make_flux_mem_short(n=n, fm_hi=hi_th)
            doc  = f"流量记忆极端 SHORT: {n}根内flux>{hi_th*100:.0f}%位 + 4h下行"
            strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                            "theme": "FluxMemoryExtreme", "doc": doc, "code": ""})
        for lo_th in [0.10, 0.15, 0.20]:
            name = f"FluxMem_L_n{n}_lo{int(lo_th*100)}"
            fn   = make_flux_mem_long(n=n, fm_lo=lo_th)
            doc  = f"流量记忆极端 LONG: {n}根内flux<{lo_th*100:.0f}%位 + 4h上行"
            strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                            "theme": "FluxMemoryExtreme", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 4] VolMomentumDivergence
# 量能与价格背离: 成交量加速 vs 价格方向的错配
# 物理类比: 波动能量与位移不同向 — 能量在积累但方向未到
# ════════════════════════════════════════════════════════════════════════════════

def make_vol_mom_long(n=3, lag=2, vm_th=0.20, mac_n=4, mac_th=0.001, g_th=-0.002):
    """
    VolMomDivLong: 价格下跌 + 成交量加速放大 + 4h上行
    = 恐慌抛售量在放大，但宏观向上 -> 投降式底部
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + lag + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        if gradient(cs1h, n) >= g_th:
            return None
        if vol_momentum(cs1h, n, lag) <= vm_th:
            return None
        return "LONG"
    return signal


def make_vol_mom_short(n=3, lag=2, vm_th=-0.20, mac_n=4, mac_th=-0.001, g_th=0.002):
    """
    VolMomDivShort: 价格上涨 + 成交量萎缩 + 4h下行
    = 无量上涨，买盘不可持续 -> 虚假反弹
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + lag + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        if gradient(cs1h, n) <= g_th:
            return None
        if vol_momentum(cs1h, n, lag) >= vm_th:
            return None
        return "SHORT"
    return signal


def theme_vol_momentum_divergence():
    strats = []
    for n in [3, 5]:
        for lag in [2, 3, 4]:
            for vm_th in [0.20, 0.40]:
                name = f"VolMom_L_n{n}_l{lag}_v{int(vm_th*100)}"
                fn   = make_vol_mom_long(n=n, lag=lag, vm_th=vm_th)
                doc  = f"量能背离 LONG: {n}根价跌+量加速>{vm_th*100:.0f}% + 4h上行"
                strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                "theme": "VolMomentumDivergence", "doc": doc, "code": ""})
                name = f"VolMom_S_n{n}_l{lag}_v{int(vm_th*100)}"
                fn   = make_vol_mom_short(n=n, lag=lag, vm_th=-vm_th)
                doc  = f"量能背离 SHORT: {n}根价涨+量萎缩>{vm_th*100:.0f}% + 4h下行"
                strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                "theme": "VolMomentumDivergence", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 5] CloseConsistency
# 收盘一致性极端: 持续偏向一侧的收盘 -> 即将反转
# 物理类比: 橡皮筋张力 — 持续偏向一侧拉伸，松手时弹回幅度更大
# ════════════════════════════════════════════════════════════════════════════════

def make_close_consist_long(n=8, cc_lo=0.25, mac_n=4, mac_th=0.001):
    """
    CloseConsistLong: 持续收在下半区(<cc_lo) + 4h上行 = 超卖反转
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        if close_consistency(cs1h, n) >= cc_lo:
            return None
        return "LONG"
    return signal


def make_close_consist_short(n=8, cc_hi=0.75, mac_n=4, mac_th=-0.001):
    """
    CloseConsistShort: 持续收在上半区(>cc_hi) + 4h下行 = 超买反转
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        if close_consistency(cs1h, n) <= cc_hi:
            return None
        return "SHORT"
    return signal


def theme_close_consistency():
    strats = []
    for n in [6, 8, 10, 12]:
        for lo_th in [0.20, 0.25, 0.30]:
            name = f"CC_L_n{n}_lo{int(lo_th*100)}"
            fn   = make_close_consist_long(n=n, cc_lo=lo_th)
            doc  = f"收盘一致 LONG: {n}根收盘偏下<{lo_th*100:.0f}% + 4h上行"
            strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                            "theme": "CloseConsistency", "doc": doc, "code": ""})
        for hi_th in [0.70, 0.75, 0.80]:
            name = f"CC_S_n{n}_hi{int(hi_th*100)}"
            fn   = make_close_consist_short(n=n, cc_hi=hi_th)
            doc  = f"收盘一致 SHORT: {n}根收盘偏上>{hi_th*100:.0f}% + 4h下行"
            strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                            "theme": "CloseConsistency", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 6] AmplitudeSkewSignal
# 振幅偏斜信号: 上/下影线不对称持续 -> 方向性力量确认
# 物理类比: 浮力-重力不平衡 — 净力方向决定最终运动方向
# ════════════════════════════════════════════════════════════════════════════════

def make_amp_skew_short(n=6, skew_th=0.15, mac_n=4, mac_th=-0.001):
    """
    AmpSkewShort: 上影线持续大于下影线 + 4h下行
    = 卖方在高位持续拦截，阻力强 -> 空
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        if amplitude_skew(cs1h, n) <= skew_th:
            return None
        return "SHORT"
    return signal


def make_amp_skew_long(n=6, skew_th=-0.15, mac_n=4, mac_th=0.001):
    """
    AmpSkewLong: 下影线持续大于上影线 + 4h上行
    = 买方在低位持续托底，支撑强 -> 多
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        if amplitude_skew(cs1h, n) >= skew_th:
            return None
        return "LONG"
    return signal


def theme_amplitude_skew_signal():
    strats = []
    for n in [4, 6, 8]:
        for th in [0.10, 0.15, 0.20, 0.25]:
            name = f"AmpSkew_S_n{n}_t{int(th*100)}"
            fn   = make_amp_skew_short(n=n, skew_th=th)
            doc  = f"振幅偏斜 SHORT: {n}根上影线-下影线>{th} + 4h下行"
            strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                            "theme": "AmplitudeSkewSignal", "doc": doc, "code": ""})
            name = f"AmpSkew_L_n{n}_t{int(th*100)}"
            fn   = make_amp_skew_long(n=n, skew_th=-th)
            doc  = f"振幅偏斜 LONG: {n}根下影线-上影线>{th} + 4h上行"
            strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                            "theme": "AmplitudeSkewSignal", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 7] PriceVelocityExhaustion
# 价格速度耗竭: 振幅归一化动量过大 -> 趋势力竭反转
# 物理类比: 射体达到最大速度后能量耗尽，因重力/阻力必然减速
# ════════════════════════════════════════════════════════════════════════════════

def make_pvel_short(n=3, amp_n=8, pv_th=1.5, mac_n=4, mac_th=-0.001):
    """
    PVelShort: 价格速度很高(上涨过快,超出振幅) + 4h下行
    = 相对波动率的过度上涨 -> 空
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < max(n, amp_n) + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        if price_velocity(cs1h, n, amp_n) <= pv_th:
            return None
        return "SHORT"
    return signal


def make_pvel_long(n=3, amp_n=8, pv_th=-1.5, mac_n=4, mac_th=0.001):
    """
    PVelLong: 价格速度很低(下跌过快,超出振幅) + 4h上行
    = 相对波动率的过度下跌 -> 多
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < max(n, amp_n) + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        if price_velocity(cs1h, n, amp_n) >= pv_th:
            return None
        return "LONG"
    return signal


def theme_price_velocity_exhaustion():
    strats = []
    for n in [3, 5]:
        for amp_n in [6, 10]:
            for pv_th in [1.5, 2.0, 3.0]:
                name = f"PVel_S_n{n}_a{amp_n}_v{int(pv_th*10)}"
                fn   = make_pvel_short(n=n, amp_n=amp_n, pv_th=pv_th)
                doc  = f"价格速度耗竭 SHORT: {n}根归一化动量>{pv_th} + 4h下行"
                strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                "theme": "PriceVelocityExhaustion", "doc": doc, "code": ""})
                name = f"PVel_L_n{n}_a{amp_n}_v{int(pv_th*10)}"
                fn   = make_pvel_long(n=n, amp_n=amp_n, pv_th=-pv_th)
                doc  = f"价格速度耗竭 LONG: {n}根归一化动量<-{pv_th} + 4h上行"
                strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                "theme": "PriceVelocityExhaustion", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 8] EntropyVelocityBreak
# 熵速率突破: 市场从混乱快速进入有序 -> 方向性突破前兆
# 物理类比: 自组织临界 — 沙堆崩塌前熵在快速下降，系统自组织
# ════════════════════════════════════════════════════════════════════════════════

def make_entropy_vel_long(n=5, lag=2, ev_th=-0.05, mac_n=4, mac_th=0.001):
    """
    EntropyVelLong: 熵在快速下降(变有序) + 最近价格方向向上 + 4h上行
    = 市场共识形成，方向向上 -> 多
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + lag + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        if entropy_velocity(cs1h, n, lag) >= ev_th:
            return None
        if gradient(cs1h, 2) <= 0:
            return None
        return "LONG"
    return signal


def make_entropy_vel_short(n=5, lag=2, ev_th=-0.05, mac_n=4, mac_th=-0.001):
    """
    EntropyVelShort: 熵在快速下降(变有序) + 最近价格方向向下 + 4h下行
    = 市场共识形成，方向向下 -> 空
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + lag + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        if entropy_velocity(cs1h, n, lag) >= ev_th:
            return None
        if gradient(cs1h, 2) >= 0:
            return None
        return "SHORT"
    return signal


def theme_entropy_velocity_break():
    strats = []
    for n in [4, 6, 8]:
        for lag in [2, 3]:
            for ev_th in [-0.05, -0.10, -0.15]:
                name = f"EntVel_L_n{n}_l{lag}_e{int(abs(ev_th)*100)}"
                fn   = make_entropy_vel_long(n=n, lag=lag, ev_th=ev_th)
                doc  = (f"熵速率突破 LONG: {n}根熵下降>{abs(ev_th)} + "
                        f"价格向上 + 4h上行")
                strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                "theme": "EntropyVelocityBreak", "doc": doc, "code": ""})
                name = f"EntVel_S_n{n}_l{lag}_e{int(abs(ev_th)*100)}"
                fn   = make_entropy_vel_short(n=n, lag=lag, ev_th=ev_th)
                doc  = (f"熵速率突破 SHORT: {n}根熵下降>{abs(ev_th)} + "
                        f"价格向下 + 4h下行")
                strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                "theme": "EntropyVelocityBreak", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# 主题注册表
# ════════════════════════════════════════════════════════════════════════════════

EXPLORATION_THEMES = [
    # ("OrderFlowPolarization",   theme_order_flow_polarization),   # done: 8 pass
    # ("BodyDominanceBurst",      theme_body_dominance_burst),       # done: 0 pass
    # ("FluxMemoryExtreme",       theme_flux_memory_extreme),        # done: 0 pass
    # ("VolMomentumDivergence",   theme_vol_momentum_divergence),    # done: 12 pass
    ("CloseConsistency",        theme_close_consistency),
    ("AmplitudeSkewSignal",     theme_amplitude_skew_signal),
    ("PriceVelocityExhaustion", theme_price_velocity_exhaustion),
    ("EntropyVelocityBreak",    theme_entropy_velocity_break),
]


# ════════════════════════════════════════════════════════════════════════════════
# 主运行逻辑
# ════════════════════════════════════════════════════════════════════════════════

class _Tee:
    def __init__(self, *files): self.files = files
    def write(self, data):
        for f in self.files: f.write(data)
    def flush(self):
        for f in self.files: f.flush()


def run_exploration(theme_filter=None, out_path=None, no_deploy=False, force=False):
    ts      = datetime.now().strftime("%Y%m%d_%H%M")
    LOG_DIR.mkdir(exist_ok=True)
    deployed_names = set() if force else load_deployed_names()
    if out_path is None:
        out_path = LOG_DIR / f"alien3_{ts}.log"
    csv_path = Path(str(out_path).replace(".log", "_passed.csv"))

    print(f"Loading data ({len(ALT99)+4} symbols)...")
    all_syms = list(set(["BTC/USDT"] + BIG4 + ALT99))
    d1h, d4h = load_data(all_syms)
    print(f"Loaded {len(d1h)} 1h sets, {len(d4h)} 4h sets.\n")

    log_file = open(out_path, "w", encoding="utf-8", buffering=1)  # line-buffered
    _orig    = sys.stdout
    sys.stdout = _Tee(_orig, log_file)

    sep = "=" * 80
    all_passed   = []
    deploy_queue = []

    try:
        print(f"\n{sep}")
        print(f"  ALIEN EXPLORE 3  [{datetime.now().strftime('%Y-%m-%d %H:%M')}]")
        print(f"  新原语: order_flow_delta / body_dominance / flux_memory /")
        print(f"          vol_momentum / close_consistency / amplitude_skew /")
        print(f"          price_velocity / entropy_velocity")
        print(f"  验证门槛: S1>={STAGE1_MIN_WR*100:.0f}%  S2>={STAGE2_MIN_WR*100:.0f}%  "
              f"S3/S4>={STAGE3_MIN_WR*100:.0f}%  HOLD={HOLD_BARS}根")
        print(f"  自动部署: 每通过 {AUTO_DEPLOY_BATCH} 个策略写入 DB + 代码 + 文档")
        print(sep)

        for theme_name, theme_fn in EXPLORATION_THEMES:
            if theme_filter and theme_name != theme_filter:
                continue

            strategies_all = theme_fn()
            strategies = filter_new_strategies(strategies_all, deployed_names, force=force)
            skipped = len(strategies_all) - len(strategies)
            print(f"\n{'='*80}")
            if skipped > 0:
                print(f"  主题: {theme_name}  "
                      f"({len(strategies)} 待跑 / {len(strategies_all)} 总候选，"
                      f"跳过 {skipped} 个已部署)")
            else:
                print(f"  主题: {theme_name}  ({len(strategies)} 个候选策略)")
            print(f"{'='*80}")

            if not strategies:
                print(f"  [{theme_name}] 全部已在 strategy_params，跳过。--force 可强制重跑。")
                continue

            t0 = datetime.now()
            theme_passed = validate_4stage(strategies, d1h, d4h)
            elapsed = (datetime.now() - t0).total_seconds()

            print(f"\n  [{theme_name}] 耗时 {elapsed:.0f}s  |  "
                  f"{len(theme_passed)}/{len(strategies)} 通过四阶段验证")
            for p in sorted(theme_passed, key=lambda x: -x["test_wr"]):
                print(f"    [PASS] {p['name']:45s}  "
                      f"train={p['s3_wr']*100:.1f}%  test={p['test_wr']*100:.1f}%  "
                      f"S3_n={p['s3_n']}  test_n={p['test_n']}  ev={p['test_ev']:+.2f}%")

            all_passed.extend(theme_passed)
            deploy_queue.extend(theme_passed)

            if not no_deploy and len(deploy_queue) >= AUTO_DEPLOY_BATCH:
                print(f"\n  [AUTO-DEPLOY] {len(deploy_queue)} 个策略触发部署...")
                deploy_strategies(deploy_queue)
                deploy_queue.clear()

        if not no_deploy and deploy_queue:
            print(f"\n  [AUTO-DEPLOY] 剩余 {len(deploy_queue)} 个策略部署...")
            deploy_strategies(deploy_queue)
            deploy_queue.clear()

        print(f"\n{sep}")
        print(f"  总结  [{datetime.now().strftime('%Y-%m-%d %H:%M')}]")
        print(f"  通过四阶段验证: {len(all_passed)} 个策略")
        print(sep)

        if all_passed:
            print(f"\n  按测试胜率排名 (前20):")
            for i, p in enumerate(sorted(all_passed, key=lambda x: -x["test_wr"])[:20], 1):
                print(f"  {i:2d}. {p['name']:50s}  "
                      f"[{p['mode']:10s}]  "
                      f"train={p['s3_wr']*100:.1f}%  test={p['test_wr']*100:.1f}%  "
                      f"n={p['s3_n']}/{p['test_n']}")

        if not no_deploy:
            print(f"\n  日志: {out_path}")
            print(f"  CSV:  {csv_path}")
            print(f"  Code: {SIGNALS_PY}")
            print(f"  Doc:  {DOC_FILE}")
        print(sep)

    finally:
        sys.stdout = _orig
        log_file.close()

    if all_passed:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f, fieldnames=["name","theme","mode","s3_n","s3_wr_pct",
                               "s3_ev_pct","test_n","test_wr_pct","test_ev_pct","doc"])
            writer.writeheader()
            for p in sorted(all_passed, key=lambda x: -x["test_wr"]):
                writer.writerow({
                    "name": p["name"], "theme": p["theme"], "mode": p["mode"],
                    "s3_n": p["s3_n"],
                    "s3_wr_pct":  f"{p['s3_wr']*100:.1f}",
                    "s3_ev_pct":  f"{p['s3_ev']:+.2f}",
                    "test_n": p["test_n"],
                    "test_wr_pct": f"{p['test_wr']*100:.1f}",
                    "test_ev_pct": f"{p['test_ev']:+.2f}",
                    "doc": p["doc"],
                })

    return all_passed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Alien Explore 3 - Batch 3 strategy discovery")
    parser.add_argument("--theme",     default=None, help="Run only this theme")
    parser.add_argument("--out",       default=None, help="Output log path")
    parser.add_argument("--no-deploy", action="store_true", help="Skip DB deploy")
    parser.add_argument("--force",     action="store_true",
                        help="Re-run strategies already in strategy_params (bypass dedup)")
    args = parser.parse_args()

    run_exploration(
        theme_filter=args.theme,
        out_path=Path(args.out) if args.out else None,
        no_deploy=args.no_deploy,
        force=args.force,
    )
