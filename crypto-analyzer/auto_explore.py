#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
auto_explore.py
===============
夜间自动策略探索器

用法:
  .venv/Scripts/python.exe auto_explore.py                         # 运行所有主题
  .venv/Scripts/python.exe auto_explore.py --theme FluxAcceleration # 只跑一个主题
  .venv/Scripts/python.exe auto_explore.py --list-themes            # 列出主题及数量

结果:
  logs/explore_YYYYMMDD_HHMM.log        -- 完整日志
  logs/explore_YYYYMMDD_HHMM_passed.csv -- 通过策略（可 Excel 打开）

新增探索方向:
  1. 写工厂函数 make_xxx(param, ...) -> signal_fn
  2. 写主题生成器 theme_xxx() -> list[{name, fn, mode}]
  3. 在 EXPLORATION_THEMES 末尾加一行 ("主题名", theme_xxx)
  详见 exploration_plan.md
"""

import argparse
import csv
import os
import random
import sys
from datetime import datetime
from pathlib import Path

import pymysql
from dotenv import load_dotenv

load_dotenv()

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ── 常量（与 test_long_manual*.py 完全一致）─────────────────────────────────
HOLD_BARS   = 3
SL_MIN, SL_MAX, SL_MULT, TP_MULT = 0.005, 0.020, 1.5, 2.5
TRAIN_RATIO = 0.70
TEST_MIN_N  = 10

BIG4 = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT"]
STAGE1_MIN_N, STAGE1_MIN_WR = 5,  0.57
STAGE2_MIN_N, STAGE2_MIN_WR = 15, 0.55
STAGE3_MIN_N, STAGE3_MIN_WR = 30, 0.57

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


# ── 指标函数 ──────────────────────────────────────────────────────────────────

def gradient(cs, n):
    if len(cs) < n: return 0.0
    s = sum(c["close"] - c["open"] for c in cs[-n:])
    ref = cs[-1]["close"]
    return s / ref if ref else 0.0

def flux(cs, n):
    if len(cs) < n: return 0.5
    rs = [c["buy_vol"] / c["vol"] for c in cs[-n:] if c["vol"] > 0]
    return sum(rs) / len(rs) if rs else 0.5

def amplitude(cs, n):
    if len(cs) < n: return 0.0
    avg = sum(c["high"] - c["low"] for c in cs[-n:]) / n
    ref = cs[-1]["close"]
    return avg / ref if ref else 0.0

def sl_tp(cs):
    amp = amplitude(cs, 6)
    amp = max(SL_MIN, min(SL_MAX, amp))
    return amp * SL_MULT, amp * TP_MULT


# ── 数据加载 ──────────────────────────────────────────────────────────────────

def load_data(symbols):
    conn = pymysql.connect(**_DB_CFG)
    d1h = {}; d4h = {}
    try:
        with conn.cursor() as cur:
            for sym in symbols:
                for tf, store in [("1h", d1h), ("4h", d4h)]:
                    cur.execute("""
                        SELECT timestamp,open_price,high_price,low_price,
                               close_price,volume,taker_buy_base_volume
                        FROM kline_data
                        WHERE symbol=%s AND timeframe=%s
                          AND taker_buy_base_volume IS NOT NULL AND volume>0
                        ORDER BY timestamp ASC
                    """, (sym, tf))
                    rows = cur.fetchall()
                    if rows:
                        store[sym] = [{"t": r[0], "open": float(r[1]),
                                       "high": float(r[2]), "low": float(r[3]),
                                       "close": float(r[4]), "vol": float(r[5]),
                                       "buy_vol": float(r[6])} for r in rows]
    finally:
        conn.close()
    return d1h, d4h


# ── 回测引擎 ──────────────────────────────────────────────────────────────────

def align4h(cs1h, cs4h, i):
    t1 = cs1h[i]["t"]
    return [c for c in cs4h if c["t"] <= t1]

def bt_mtf(fn, cs1h, cs4h, i_start=22, i_end=None):
    stats = {"n": 0, "win": 0, "pnl": []}
    n = len(cs1h)
    if i_end is None: i_end = n - HOLD_BARS
    for i in range(i_start, i_end):
        s4 = align4h(cs1h, cs4h, i)
        if len(s4) < 4: continue
        try:
            sig = fn(cs1h[:i+1], s4)
        except Exception:
            continue
        if sig not in ("LONG", "SHORT"): continue
        entry = cs1h[i]["close"]
        sl_pct, tp_pct = sl_tp(cs1h[:i+1])
        sl_abs = entry * sl_pct; tp_abs = entry * tp_pct
        outcome = None
        for j in range(1, HOLD_BARS + 1):
            if i + j >= n: break
            nxt = cs1h[i + j]
            if sig == "LONG":
                if entry - nxt["low"] >= sl_abs:   outcome = -sl_pct; break
                if nxt["high"] - entry >= tp_abs:  outcome =  tp_pct; break
            else:
                if nxt["high"] - entry >= sl_abs:  outcome = -sl_pct; break
                if entry - nxt["low"] >= tp_abs:   outcome =  tp_pct; break
        if outcome is None:
            lj = min(HOLD_BARS, n - i - 1)
            if lj > 0:
                outcome = (cs1h[i + lj]["close"] - entry) / entry
                if sig == "SHORT": outcome = -outcome
        if outcome is None: continue
        stats["n"] += 1; stats["pnl"].append(outcome)
        if outcome > 0: stats["win"] += 1
    return stats

def run_strat(fn, mode, d1h, d4h, symbols, phase="all"):
    agg = {"n": 0, "win": 0, "pnl": []}; per = {}
    for sym in symbols:
        cs1h = d1h.get(sym, [])
        if len(cs1h) < 30: continue
        n1h = len(cs1h); split_i = int(n1h * TRAIN_RATIO)
        if phase == "train":  i_start, i_end = 22, split_i
        elif phase == "test": i_start, i_end = split_i, n1h - HOLD_BARS
        else:                 i_start, i_end = 22, None
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
        agg["n"] += s["n"]; agg["win"] += s["win"]; agg["pnl"] += s["pnl"]
        per[sym] = s
    return agg, per


# ── 四阶段验证 ────────────────────────────────────────────────────────────────

def validate_4stage(strategies, d1h, d4h):
    """运行四阶段验证，打印进度，返回通过策略列表。"""

    # ── Stage 1: Big4 ──
    print(f"\n  --- S1 [Big4] ---")
    s1_pass = []
    for st in strategies:
        agg, _ = run_strat(st["fn"], st["mode"], d1h, d4h, BIG4, "train")
        n = agg["n"]; wr = agg["win"] / n if n > 0 else 0
        ev = sum(agg["pnl"]) / n * 100 if n > 0 else 0
        ok = n >= STAGE1_MIN_N and wr >= STAGE1_MIN_WR
        tag = "PASS" if ok else "----"
        print(f"  {tag}  {st['name']:42s}  n={n:4d}  wr={wr*100:5.1f}%  ev={ev:+.2f}%")
        if ok: st["s1"] = {"n": n, "wr": wr}; s1_pass.append(st)
    print(f"  S1: {len(s1_pass)}/{len(strategies)} passed")
    if not s1_pass: return []

    # ── Stage 2: 10 random alts ──
    candidates = [s for s in ALT99 if s not in set(BIG4)]
    sample10   = random.sample(candidates, min(10, len(candidates)))
    print(f"\n  --- S2 [10 alts] ---")
    s2_pass = []
    for st in s1_pass:
        agg, _ = run_strat(st["fn"], st["mode"], d1h, d4h, sample10, "train")
        n = agg["n"]; wr = agg["win"] / n if n > 0 else 0
        ev = sum(agg["pnl"]) / n * 100 if n > 0 else 0
        ok = n >= STAGE2_MIN_N and wr >= STAGE2_MIN_WR
        tag = "PASS" if ok else "----"
        print(f"  {tag}  {st['name']:42s}  n={n:4d}  wr={wr*100:5.1f}%  ev={ev:+.2f}%")
        if ok: st["s2"] = {"n": n, "wr": wr}; s2_pass.append(st)
    print(f"  S2: {len(s2_pass)}/{len(s1_pass)} passed")
    if not s2_pass: return []

    # ── Stage 3: All 86 alts ──
    print(f"\n  --- S3 [All {len(ALT99)} alts] ---")
    s3_pass = []
    for st in s2_pass:
        agg, per = run_strat(st["fn"], st["mode"], d1h, d4h, ALT99, "train")
        n = agg["n"]; wr = agg["win"] / n if n > 0 else 0
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
        if ok: st["s3"] = {"n": n, "wr": wr, "ev": ev}; s3_pass.append(st)
    print(f"  S3: {len(s3_pass)}/{len(s2_pass)} passed")
    if not s3_pass: return []

    # ── Stage 4: Walk-forward test ──
    print(f"\n  --- S4 [test 30%] ---")
    passed = []
    for st in s3_pass:
        agg_t, _ = run_strat(st["fn"], st["mode"], d1h, d4h, ALT99, "test")
        nt  = agg_t["n"]; wrt = agg_t["win"] / nt if nt > 0 else 0
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
                "s3_n":    s3["n"],  "s3_wr":   s3["wr"],  "s3_ev":   s3["ev"],
                "test_n":  nt,       "test_wr":  wrt,       "test_ev": evt,
            })
    return passed


# ════════════════════════════════════════════════════════════════════════════════
# 策略工厂函数
# 每个工厂接受超参数，返回一个 (cs1h, cs4h) -> "LONG"/"SHORT"/None 的信号函数
# ════════════════════════════════════════════════════════════════════════════════

def make_decel_bounce_long(h_n, mac_n=4, mac_th=0.003, hist_th=0.002,
                            f_min=0.53, amp1_max=None, amp1_n=2, amp4_min=None):
    """
    DecelBounce LONG（已验证家族的通用工厂）
    逻辑：4h上涨(mac_n根>mac_th) + 1h回调(h_n根<-hist_th) + 2根反转 + flux>f_min
    """
    min1 = max(h_n + 3, amp1_n + 1, 10)
    min4 = max(mac_n + 1, 8)
    def signal(cs1h, cs4h):
        if len(cs1h) < min1 or len(cs4h) < min4:            return None
        if gradient(cs4h, mac_n) <= mac_th:                  return None
        if amp4_min is not None and amplitude(cs4h, 4) < amp4_min: return None
        if gradient(cs1h, h_n) >= -hist_th:                  return None
        if gradient(cs1h, 2) <= 0:                           return None
        if flux(cs1h, 2) <= f_min:                           return None
        if amp1_max is not None and amplitude(cs1h, amp1_n) > amp1_max: return None
        return "LONG"
    return signal


def make_flux_acceleration_long(mac_n=4, mac_th=0.003, hist_n=6, hist_th=0.001,
                                  f_abs=0.50, f_accel=0.97):
    """
    FluxAcceleration LONG
    逻辑：4h上涨 + 1h回调 + 买压连续加速上升 flux(2)>flux(4)>flux(8)
    f_accel: 允许的最小比值（0.97=可有轻微噪声）
    """
    min1 = max(hist_n + 3, 10)
    min4 = max(mac_n + 1, 8)
    def signal(cs1h, cs4h):
        if len(cs1h) < min1 or len(cs4h) < min4: return None
        if gradient(cs4h, mac_n) <= mac_th:       return None
        if gradient(cs1h, hist_n) >= -hist_th:    return None
        f2 = flux(cs1h, 2); f4 = flux(cs1h, 4); f8 = flux(cs1h, 8)
        if not (f2 > f4 * f_accel and f4 > f8 * f_accel): return None
        if f2 <= f_abs: return None
        return "LONG"
    return signal


def make_oversold_deep_bounce_long(h_n=6, mac_n=4, mac_th=0.003,
                                    depth_th=0.010, f_min=0.54):
    """
    OversoldDeepBounce LONG
    逻辑：4h上涨 + 1h极深超跌(h_n根<-depth_th) + 反转 + 强买压
    depth_th 比普通 DecelBounce 的 hist_th(0.002) 大 5-10 倍
    """
    min1 = max(h_n + 3, 10)
    min4 = max(mac_n + 1, 8)
    def signal(cs1h, cs4h):
        if len(cs1h) < min1 or len(cs4h) < min4: return None
        if gradient(cs4h, mac_n) <= mac_th:       return None
        if gradient(cs1h, h_n) >= -depth_th:      return None
        if gradient(cs1h, 2) <= 0:                return None
        if flux(cs1h, 2) <= f_min:                return None
        return "LONG"
    return signal


def make_btc_lead_alt_long(btc_mac_th=0.007, alt_hist_n=6, alt_hist_th=0.002,
                             f_min=0.51):
    """
    BTCLeadAlt LONG  (mtf_btc 模式)
    逻辑：BTC 4h强上行(>btc_mac_th) + 山寨1h仍回调 + 山寨买压回升
    """
    min1 = max(alt_hist_n + 3, 10)
    def signal(cs_alt, cs_btc4h):
        if len(cs_alt) < min1 or len(cs_btc4h) < 8: return None
        if gradient(cs_btc4h, 4) <= btc_mac_th:      return None
        if gradient(cs_alt, alt_hist_n) >= -alt_hist_th: return None
        if gradient(cs_alt, 2) <= 0:                  return None
        if flux(cs_alt, 2) <= f_min:                  return None
        return "LONG"
    return signal


def make_vol_compression_breakout_long(mac_n=4, mac_th=0.002,
                                         compress_ratio=0.7, breakout_g=0.004,
                                         f_min=0.52):
    """
    VolCompressionBreakout LONG
    逻辑：振幅持续收缩(amp2 < amp8 * compress_ratio) + 突破性上涨 + 宏观看多
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < 14 or len(cs4h) < max(mac_n + 1, 8): return None
        if gradient(cs4h, mac_n) <= mac_th:                    return None
        amp_recent = amplitude(cs1h, 2); amp_hist = amplitude(cs1h, 8)
        if amp_hist <= 0 or amp_recent >= amp_hist * compress_ratio: return None
        if gradient(cs1h, 2) <= breakout_g: return None
        if flux(cs1h, 2) <= f_min:          return None
        return "LONG"
    return signal


# ════════════════════════════════════════════════════════════════════════════════
# 主题生成器（每个返回一批待测策略）
# ════════════════════════════════════════════════════════════════════════════════

def theme_decel_bounce_extended():
    """
    DecelBounce 参数扩展
    在已验证家族基础上，探索：
      - 超长回调 h_n=15-25
      - 更高 flux 要求 f_min=0.55-0.60
      - 更强宏观 mac_n=5-8
      - 更深历史回调 hist_th=0.003-0.006
    """
    strategies = []

    # 超长回调
    for h_n in [15, 16, 18, 20, 25]:
        strategies.append({"name": f"DB_h{h_n}",
                            "fn": make_decel_bounce_long(h_n=h_n), "mode": "mtf_self"})

    # 高 flux 要求（对已知有效的 h 值）
    for h_n in [6, 7, 8, 10, 12, 14]:
        for f_min in [0.55, 0.57, 0.60]:
            strategies.append({"name": f"DB_h{h_n}_f{int(f_min*100)}",
                                "fn": make_decel_bounce_long(h_n=h_n, f_min=f_min),
                                "mode": "mtf_self"})

    # 更强宏观窗口
    for h_n in [7, 8, 10, 12]:
        for mac_n in [5, 6, 8]:
            strategies.append({"name": f"DB_h{h_n}_mac{mac_n}",
                                "fn": make_decel_bounce_long(h_n=h_n, mac_n=mac_n),
                                "mode": "mtf_self"})

    # 更深历史回调要求
    for h_n in [6, 7, 8, 10, 12, 14]:
        for hist_th in [0.003, 0.004, 0.005, 0.006]:
            strategies.append({"name": f"DB_h{h_n}_ht{int(hist_th*1000)}",
                                "fn": make_decel_bounce_long(h_n=h_n, hist_th=hist_th),
                                "mode": "mtf_self"})

    return strategies


def theme_flux_acceleration():
    """
    FluxAcceleration LONG
    假设：买压加速上升比单纯 flux>阈值 更能预测反弹持续性
    """
    strategies = []
    for hist_n in [4, 6, 8, 10]:
        for mac_th in [0.002, 0.003, 0.005]:
            for f_abs in [0.49, 0.51, 0.53]:
                strategies.append({
                    "name": f"FluxAccel_h{hist_n}_mac{int(mac_th*1000)}_f{int(f_abs*100)}",
                    "fn":   make_flux_acceleration_long(hist_n=hist_n, mac_th=mac_th, f_abs=f_abs),
                    "mode": "mtf_self"
                })
    return strategies


def theme_oversold_deep_bounce():
    """
    OversoldDeepBounce LONG
    假设：1h 极深超跌(-1%~-2%)后反弹力度更强、胜率更高
    """
    strategies = []
    for h_n in [4, 6, 8, 10]:
        for depth_th in [0.008, 0.010, 0.012, 0.015, 0.020]:
            for f_min in [0.53, 0.55, 0.57]:
                strategies.append({
                    "name": f"OvrSold_h{h_n}_d{int(depth_th*1000)}_f{int(f_min*100)}",
                    "fn":   make_oversold_deep_bounce_long(h_n=h_n, depth_th=depth_th, f_min=f_min),
                    "mode": "mtf_self"
                })
    return strategies


def theme_btc_lead_alt():
    """
    BTCLeadAlt LONG  (mtf_btc)
    假设：BTC 宏观强势时，山寨1h仍在回调 = 滞后跟涨机会
    """
    strategies = []
    for btc_th in [0.005, 0.007, 0.010]:
        for alt_h in [4, 6, 8]:
            for f_min in [0.50, 0.52, 0.54]:
                strategies.append({
                    "name": f"BTCLead_b{int(btc_th*1000)}_h{alt_h}_f{int(f_min*100)}",
                    "fn":   make_btc_lead_alt_long(btc_mac_th=btc_th, alt_hist_n=alt_h, f_min=f_min),
                    "mode": "mtf_btc"
                })
    return strategies


def theme_vol_compression_breakout():
    """
    VolCompressionBreakout LONG
    假设：振幅收缩后方向性突破往往更可持续
    """
    strategies = []
    for compress_ratio in [0.60, 0.70, 0.80]:
        for breakout_g in [0.003, 0.005, 0.007]:
            for mac_th in [0.002, 0.004]:
                strategies.append({
                    "name": f"VolComp_c{int(compress_ratio*10)}_bg{int(breakout_g*1000)}_m{int(mac_th*1000)}",
                    "fn":   make_vol_compression_breakout_long(
                                compress_ratio=compress_ratio,
                                breakout_g=breakout_g, mac_th=mac_th),
                    "mode": "mtf_self"
                })
    return strategies


# ════════════════════════════════════════════════════════════════════════════════
# 主题注册表
# 新增探索方向：在这里加一行 ("主题名", theme_函数)
# ════════════════════════════════════════════════════════════════════════════════

EXPLORATION_THEMES = [
    ("DecelBounce_Extended",   theme_decel_bounce_extended),
    ("FluxAcceleration",        theme_flux_acceleration),
    ("OversoldDeepBounce",     theme_oversold_deep_bounce),
    ("BTCLeadAlt",              theme_btc_lead_alt),
    ("VolCompressionBreakout", theme_vol_compression_breakout),
    # ("我的新方向",             theme_我的新方向),
]


# ── 主运行逻辑 ─────────────────────────────────────────────────────────────────

class _Tee:
    """同时写到终端和文件。"""
    def __init__(self, *files): self.files = files
    def write(self, data):
        for f in self.files: f.write(data)
    def flush(self):
        for f in self.files: f.flush()


def run_exploration(theme_filter=None, out_path=None):
    ts      = datetime.now().strftime("%Y%m%d_%H%M")
    log_dir = Path(__file__).parent / "logs"
    log_dir.mkdir(exist_ok=True)
    if out_path is None:
        out_path = log_dir / f"explore_{ts}.log"
    csv_path = Path(str(out_path).replace(".log", "_passed.csv"))

    # 加载数据（所有 symbol 一次性加载）
    print(f"Loading data ({len(ALT99)+4} symbols)...")
    all_syms = list(set(["BTC/USDT"] + BIG4 + ALT99))
    d1h, d4h = load_data(all_syms)
    print(f"Loaded {len(d1h)} 1h sets, {len(d4h)} 4h sets.\n")

    log_file = open(out_path, "w", encoding="utf-8")
    _orig    = sys.stdout
    sys.stdout = _Tee(_orig, log_file)

    sep = "=" * 80
    all_passed = []

    try:
        print(f"\n{sep}")
        print(f"  AUTO EXPLORE  [{datetime.now().strftime('%Y-%m-%d %H:%M')}]")
        print(f"  验证门槛: S1 wr>={STAGE1_MIN_WR*100:.0f}% | "
              f"S2 wr>={STAGE2_MIN_WR*100:.0f}% | "
              f"S3/S4 wr>={STAGE3_MIN_WR*100:.0f}%  |  HOLD={HOLD_BARS}根")
        print(sep)

        for theme_name, theme_fn in EXPLORATION_THEMES:
            if theme_filter and theme_name != theme_filter:
                continue

            strategies = theme_fn()
            print(f"\n{'='*80}")
            print(f"  主题: {theme_name}  ({len(strategies)} 个候选策略)")
            print(f"{'='*80}")

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

        # 总结
        total_tested = sum(len(fn()) for _, fn in EXPLORATION_THEMES
                           if not theme_filter or name == theme_filter)
        print(f"\n{sep}")
        print(f"  总结  [{datetime.now().strftime('%Y-%m-%d %H:%M')}]")
        print(f"  通过四阶段验证: {len(all_passed)} 个策略")
        print(sep)

        if all_passed:
            print("\n  按测试胜率排名 (前20):")
            for i, p in enumerate(sorted(all_passed, key=lambda x: -x["test_wr"])[:20], 1):
                print(f"  {i:2d}. {p['name']:45s}  [{p['mode']:10s}]  "
                      f"train={p['s3_wr']*100:.1f}%  test={p['test_wr']*100:.1f}%  "
                      f"n={p['s3_n']}/{p['test_n']}")

        print(f"\n  日志: {out_path}")
        print(f"  CSV:  {csv_path}")
        print(sep)

    finally:
        sys.stdout = _orig
        log_file.close()

    # 写 CSV（供 Excel 分析）
    if all_passed:
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=[
                "name", "mode", "s3_n", "s3_wr_pct", "s3_ev_pct",
                "test_n", "test_wr_pct", "test_ev_pct"
            ])
            w.writeheader()
            for p in sorted(all_passed, key=lambda x: -x["test_wr"]):
                w.writerow({
                    "name":         p["name"],
                    "mode":         p["mode"],
                    "s3_n":         p["s3_n"],
                    "s3_wr_pct":    f"{p['s3_wr']*100:.1f}",
                    "s3_ev_pct":    f"{p['s3_ev']:+.2f}",
                    "test_n":       p["test_n"],
                    "test_wr_pct":  f"{p['test_wr']*100:.1f}",
                    "test_ev_pct":  f"{p['test_ev']:+.2f}",
                })

    return all_passed


# ── 入口 ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Auto Strategy Explorer")
    parser.add_argument("--theme",       default=None,
                        help="只运行指定主题名（默认运行所有）")
    parser.add_argument("--out",         default=None,
                        help="输出日志路径（默认 logs/explore_YYYYMMDD_HHMM.log）")
    parser.add_argument("--list-themes", action="store_true",
                        help="列出所有主题及候选策略数量，然后退出")
    args = parser.parse_args()

    if args.list_themes:
        print("可用探索主题:")
        total = 0
        for name, fn in EXPLORATION_THEMES:
            count = len(fn())
            print(f"  {name:35s}  {count:3d} 个候选策略")
            total += count
        print(f"  {'合计':35s}  {total:3d} 个候选策略")
        sys.exit(0)

    run_exploration(theme_filter=args.theme, out_path=args.out)
