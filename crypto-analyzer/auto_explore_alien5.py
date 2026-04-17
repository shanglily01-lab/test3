#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
auto_explore_alien5.py
======================
第五批非人类原语策略探索 -- 核心主题：爆发前蓄力状态检测

前四批已覆盖（共227个集成策略 A1-A12系列）：
  Batch1: wick_asym / body_entropy / sell_saturation / spatial_close /
          momentum_ratio / cross_residual / vol_absorption / candle_dna
  Batch2: saturation_velocity / path_tortuosity / amplitude_regime /
          shadow_strength / vol_concentration / time_pressure /
          price_memory / flux_momentum
  Batch3: order_flow_delta / vol_momentum / close_consistency /
          price_velocity (amplitude_skew/entropy_velocity 全淘汰)
  Batch4: gap_bias / vol_climax / vwap_dev / wick_pressure /
          body_decel / vol_dir_asym

本批核心思路：
  以上信号捕捉的是「已经发生的状态」（放量、偏离、耗竭），
  本批转向「爆发前的蓄力状态」— 弹簧压缩、能量积聚、临界待发。

新维度（均为爆发前征兆）：

  vol_energy_density(cs, n_near, n_far)
    -- 量价能量密度比: (近N均量/近N价格区间) / (远N均量/远N价格区间)
       高值 = 大量成交但价格区间小 = 能量被弹簧吸收，待释放

  amplitude_silence(cs, n)
    -- 振幅静默比: 近N根中，振幅<前根振幅 的比例
       高值 = 持续收缩 = 弹簧压缩中

  doji_density(cs, n, body_ratio_max)
    -- 十字星密度: 近N根中，实体/振幅<阈值 的比例
       高值 = 多空均衡、拉锯密集 = 临界态，即将爆发

  price_cluster_density(cs, n, zone_pct)
    -- 价格引力密度: 近N根中，收盘在当前价±zone_pct 内的比例
       高值 = 价格被"引力井"束缚在此位 = 积聚后必然突破

  taker_sustain(cs, n, threshold)
    -- 主动流量持续占比: 近N根中 buy_vol/vol > threshold 的比例
       高值 = 买方持续积累（但价格未涨）= 吸货
       低值 = 卖方持续主导（但价格未跌）= 派发

  seq_contract(cs)
    -- 序列收缩长度: 从当前往前，连续 H-L < 前根 H-L 的K线数
       高值 = 连续收缩 = 极致压缩 = 即将爆发

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

_align4h_cache: dict = {}

def align4h(cs1h, cs4h, i, _keep=30):
    cs4h_id = id(cs4h)
    if cs4h_id not in _align4h_cache:
        _align4h_cache[cs4h_id] = [c["t"] for c in cs4h]
    times = _align4h_cache[cs4h_id]
    t1 = cs1h[i]["t"]
    idx = bisect.bisect_right(times, t1)
    start = max(0, idx - _keep)
    return cs4h[start:idx]


# ════════════════════════════════════════════════════════════════════════════════
# [新原语] 第五批 -- 爆发前蓄力状态检测
# ════════════════════════════════════════════════════════════════════════════════

def vol_energy_density(cs, n_near, n_far):
    """
    量价能量密度比 (Volume-Price Energy Density Ratio)
    定义: (近N均量/近N总价格区间) / (远N均量/远N总价格区间)
    高值 = 近期大量成交但价格区间收窄 = 能量被弹簧吸收 = 即将释放
    低值 = 量价同步移动 = 正常趋势延续
    物理类比: 弹性势能 — 外力做功但形变减少，能量转化为弹性储存
    """
    total = n_near + n_far
    if len(cs) < total + 1: return 1.0
    ref = cs[-1]["close"]
    if ref <= 0: return 1.0

    near_bars = cs[-n_near:]
    far_bars  = cs[-total:-n_near]

    near_avg_vol = sum(c["vol"] for c in near_bars) / n_near
    far_avg_vol  = sum(c["vol"] for c in far_bars)  / n_far

    near_range_pct = (max(c["high"] for c in near_bars) - min(c["low"] for c in near_bars)) / ref
    far_range_pct  = (max(c["high"] for c in far_bars)  - min(c["low"] for c in far_bars))  / ref

    if near_range_pct <= 0 or far_range_pct <= 0 or far_avg_vol <= 0: return 1.0

    near_density = near_avg_vol / near_range_pct
    far_density  = far_avg_vol  / far_range_pct
    return near_density / far_density


def amplitude_silence(cs, n):
    """
    振幅静默比 (Amplitude Silence Ratio)
    定义: 近N根中，H-L < 前根 H-L 的比例
    高值(>0.65) = 振幅持续收缩 = 弹簧正在压缩
    低值 = 振幅扩张中（已经爆发）
    物理类比: 弹性压缩 — 形变在持续积累，临界点逼近
    """
    if len(cs) < n + 1: return 0.5
    count = 0
    for i in range(-n, 0):
        curr = cs[i]["high"] - cs[i]["low"]
        prev = cs[i - 1]["high"] - cs[i - 1]["low"]
        if curr < prev:
            count += 1
    return count / n


def doji_density(cs, n, body_ratio_max=0.25):
    """
    十字星密度 (Doji Density)
    定义: 近N根中，|close-open|/(high-low) < body_ratio_max 的比例
    高值(>0.45) = 多空均衡，多数K线为十字星 = 临界态
    物理类比: 热力学平衡 — 宏观静止但微观能量密集，相变即将发生
    """
    if len(cs) < n: return 0.0
    count = 0
    for c in cs[-n:]:
        rng = c["high"] - c["low"]
        if rng <= 0:
            count += 1  # 无振幅也算
            continue
        body = abs(c["close"] - c["open"])
        if body / rng < body_ratio_max:
            count += 1
    return count / n


def price_cluster_density(cs, n, zone_pct=0.005):
    """
    价格引力密度 (Price Cluster Density)
    定义: 近N根（不含当前）中，close 在当前 close ± zone_pct 内的比例
    高值(>0.35) = 价格被"引力井"束缚在此价位 = 积聚后必然突破
    物理类比: 引力势阱 — 粒子反复回到同一位置，临界逃逸能量积累
    """
    if len(cs) < n + 1: return 0.0
    ref = cs[-1]["close"]
    if ref <= 0: return 0.0
    zone = ref * zone_pct
    count = sum(1 for c in cs[-n - 1:-1] if abs(c["close"] - ref) <= zone)
    return count / n


def taker_sustain(cs, n, threshold=0.55):
    """
    主动流量持续占比 (Taker Flow Sustain Ratio)
    定义: 近N根中 buy_vol/vol > threshold 的比例
    高值(>0.70) = 买方在每根K线都持续主导 = 系统性吸货行为
    低值(<0.30) = 卖方在每根K线都持续主导 = 系统性派发行为
    物理类比: 电荷极化积累 — 持续单向极化导致场强积累至击穿
    """
    if len(cs) < n: return 0.5
    valids = [c for c in cs[-n:] if c["vol"] > 0]
    if not valids: return 0.5
    count = sum(1 for c in valids if c["buy_vol"] / c["vol"] > threshold)
    return count / len(valids)


def seq_contract(cs, max_n=12):
    """
    序列收缩长度 (Sequential Contraction Count)
    定义: 从当前往前，连续 H-L < 前根 H-L 的K线数量
    高值(>=4) = 已连续收缩多根 = 弹簧处于极致压缩状态
    物理类比: 弹性疲劳临界 — 持续压缩后任何微小扰动都会导致释放
    """
    if len(cs) < 2: return 0
    count = 0
    for i in range(-1, -max_n - 1, -1):
        if len(cs) < -i + 1: break
        curr = cs[i]["high"] - cs[i]["low"]
        prev = cs[i - 1]["high"] - cs[i - 1]["low"]
        if curr < prev:
            count += 1
        else:
            break
    return count


# ════════════════════════════════════════════════════════════════════════════════
# 回测引擎（与 Batch4 相同）
# ════════════════════════════════════════════════════════════════════════════════

def bt_mtf(fn, cs1h, cs4h, i_start=22, i_end=None):
    if i_end is None:
        i_end = len(cs1h) - HOLD_BARS
    stats = {"n": 0, "win": 0, "pnl": []}
    for i in range(i_start, i_end):
        c4h = align4h(cs1h, cs4h, i)
        if len(c4h) < 6: continue
        sig = fn(cs1h[:i+1], c4h)
        if not sig: continue
        entry = cs1h[i]["close"]
        sl_a, tp_a = sl_tp(cs1h[:i+1])
        sl_abs = entry * sl_a
        tp_abs = entry * tp_a
        outcome = None
        for j in range(1, HOLD_BARS + 1):
            if i + j >= len(cs1h): break
            nxt = cs1h[i + j]
            if sig == "LONG":
                if nxt["low"]  <= entry - sl_abs: outcome = -sl_a; break
                if nxt["high"] >= entry + tp_abs: outcome =  tp_a; break
            else:
                if nxt["high"] >= entry + sl_abs: outcome = -sl_a; break
                if nxt["low"]  <= entry - tp_abs: outcome =  tp_a; break
        if outcome is None:
            lj = min(HOLD_BARS, len(cs1h) - i - 1)
            if lj > 0:
                outcome = (cs1h[i + lj]["close"] - entry) / entry
                if sig == "SHORT":
                    outcome = -outcome
        if outcome is None: continue
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
        if len(cs1h) < 30: continue
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
                sl_pct = 0.02
                tp_pct = 0.03
                hold_h = 3
                notes  = (f"alien5_{p['theme']} | "
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
                """, (name, sl_pct, tp_pct, hold_h, "auto_explore_alien5",
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
# [Theme 1] KineticEnergyBuild
# 量价能量密度: 大量成交但价格区间收窄 = 能量积聚待爆发
# 物理类比: 弹性势能 — 外力做功但形变减小，能量转化为弹性储存
# ════════════════════════════════════════════════════════════════════════════════

def make_ke_build_long(n_near=6, n_far=16, density_th=2.0, mac_n=4, mac_th=0.001):
    """
    KEBuildLong: 能量积聚(高量低振幅) + 4h上行 = 即将向上爆发
    """
    def signal(cs1h, cs4h):
        total = n_near + n_far
        if len(cs1h) < total + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        if vol_energy_density(cs1h, n_near, n_far) < density_th:
            return None
        return "LONG"
    return signal


def make_ke_build_short(n_near=6, n_far=16, density_th=2.0, mac_n=4, mac_th=-0.001):
    """
    KEBuildShort: 能量积聚(高量低振幅) + 4h下行 = 即将向下爆发
    """
    def signal(cs1h, cs4h):
        total = n_near + n_far
        if len(cs1h) < total + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        if vol_energy_density(cs1h, n_near, n_far) < density_th:
            return None
        return "SHORT"
    return signal


def theme_kinetic_energy_build():
    strats = []
    for n_near, n_far in [(4, 12), (4, 20), (6, 16), (6, 20)]:
        for th in [1.5, 2.0, 2.5]:
            for direction, maker, tag in [("LONG", make_ke_build_long, "L"),
                                           ("SHORT", make_ke_build_short, "S")]:
                name = f"KEBuild_{tag}_nn{n_near}_nf{n_far}_d{int(th*10)}"
                fn   = maker(n_near=n_near, n_far=n_far, density_th=th)
                doc  = (f"能量积聚 {direction}: "
                        f"近{n_near}根量密度/{n_far}根>{th:.1f}倍 + 4h{'上行' if direction == 'LONG' else '下行'}")
                strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                "theme": "KineticEnergyBuild", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 2] AmplitudeSilenceBreak
# 振幅静默突破: 振幅持续收缩 = 弹簧压缩 = 方向爆发
# 物理类比: 弹性压缩 — 形变持续积累至临界点后释放
# ════════════════════════════════════════════════════════════════════════════════

def make_amp_silence_long(n=8, silence_th=0.65, mac_n=4, mac_th=0.001):
    """
    AmpSilenceLong: 振幅持续收缩 + 4h上行 = 向上爆发
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        if amplitude_silence(cs1h, n) < silence_th:
            return None
        return "LONG"
    return signal


def make_amp_silence_short(n=8, silence_th=0.65, mac_n=4, mac_th=-0.001):
    """
    AmpSilenceShort: 振幅持续收缩 + 4h下行 = 向下爆发
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        if amplitude_silence(cs1h, n) < silence_th:
            return None
        return "SHORT"
    return signal


def theme_amplitude_silence_break():
    strats = []
    for n in [6, 8, 10, 12]:
        for th in [0.60, 0.65, 0.70, 0.75]:
            for direction, maker, tag in [("LONG", make_amp_silence_long, "L"),
                                           ("SHORT", make_amp_silence_short, "S")]:
                name = f"AmpSilence_{tag}_n{n}_t{int(th*100)}"
                fn   = maker(n=n, silence_th=th)
                doc  = (f"振幅静默 {direction}: "
                        f"{n}根中{th*100:.0f}%振幅收缩 + 4h{'上行' if direction == 'LONG' else '下行'}")
                strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                "theme": "AmplitudeSilenceBreak", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 3] DojiDensitySignal
# 十字星密集爆发: 多空均衡密集 = 热力学临界态 = 相变爆发
# 物理类比: 过冷液体 — 表面平静但内部势能极高，微小扰动触发相变
# ════════════════════════════════════════════════════════════════════════════════

def make_doji_density_long(n=8, density_th=0.45, body_max=0.25, mac_n=4, mac_th=0.001):
    """
    DojiDensityLong: 十字星密集 + 4h上行 = 均衡打破向上
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        if doji_density(cs1h, n, body_max) < density_th:
            return None
        return "LONG"
    return signal


def make_doji_density_short(n=8, density_th=0.45, body_max=0.25, mac_n=4, mac_th=-0.001):
    """
    DojiDensityShort: 十字星密集 + 4h下行 = 均衡打破向下
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        if doji_density(cs1h, n, body_max) < density_th:
            return None
        return "SHORT"
    return signal


def theme_doji_density_signal():
    strats = []
    for n in [6, 8, 12]:
        for density_th in [0.40, 0.50, 0.60]:
            for body_max in [0.20, 0.30]:
                for direction, maker, tag in [("LONG", make_doji_density_long, "L"),
                                               ("SHORT", make_doji_density_short, "S")]:
                    name = f"DojiDens_{tag}_n{n}_d{int(density_th*100)}_b{int(body_max*100)}"
                    fn   = maker(n=n, density_th=density_th, body_max=body_max)
                    doc  = (f"十字星密度 {direction}: "
                            f"{n}根中{density_th*100:.0f}%实体<振幅{body_max*100:.0f}% + "
                            f"4h{'上行' if direction == 'LONG' else '下行'}")
                    strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                    "theme": "DojiDensitySignal", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 4] PriceGravityWell
# 价格引力井突破: 价格反复聚集于此 = 引力势阱 = 临界逃逸爆发
# 物理类比: 引力势阱 — 粒子逃逸需要积累足够能量,一旦超越则急速远离
# ════════════════════════════════════════════════════════════════════════════════

def make_gravity_well_long(n=16, zone_pct=0.005, cluster_th=0.35, mac_n=4, mac_th=0.001):
    """
    GravityWellLong: 价格密集聚集 + 4h上行 = 引力逃逸向上
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        if price_cluster_density(cs1h, n, zone_pct) < cluster_th:
            return None
        return "LONG"
    return signal


def make_gravity_well_short(n=16, zone_pct=0.005, cluster_th=0.35, mac_n=4, mac_th=-0.001):
    """
    GravityWellShort: 价格密集聚集 + 4h下行 = 引力逃逸向下
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        if price_cluster_density(cs1h, n, zone_pct) < cluster_th:
            return None
        return "SHORT"
    return signal


def theme_price_gravity_well():
    strats = []
    for n in [12, 16, 20]:
        for zone_pct in [0.004, 0.006, 0.008]:
            for cluster_th in [0.25, 0.35, 0.45]:
                zone_str = int(zone_pct * 1000)
                for direction, maker, tag in [("LONG", make_gravity_well_long, "L"),
                                               ("SHORT", make_gravity_well_short, "S")]:
                    name = f"GravWell_{tag}_n{n}_z{zone_str}_c{int(cluster_th*100)}"
                    fn   = maker(n=n, zone_pct=zone_pct, cluster_th=cluster_th)
                    doc  = (f"价格引力 {direction}: "
                            f"{n}根中{cluster_th*100:.0f}%聚集在±{zone_pct*100:.1f}% + "
                            f"4h{'上行' if direction == 'LONG' else '下行'}")
                    strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                    "theme": "PriceGravityWell", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 5] TakerSustainBreak
# 主动流量持续极化 + 爆发: 单向持续主导 = 电荷极化积累至击穿
# 物理类比: 介质击穿 — 单向电场持续施加,电荷积累至绝缘体击穿
# ════════════════════════════════════════════════════════════════════════════════

def make_taker_sustain_short(n=10, taker_th=0.55, sustain_th=0.70, mac_n=4, mac_th=-0.001):
    """
    TakerSustainShort: 买方持续主导每根K线(系统性积累/过度乐观) + 4h下行 = 顶部做空
    逻辑: 持续买方主导但4h已转弱 = 散户追多被主力做空
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        if taker_sustain(cs1h, n, taker_th) < sustain_th:
            return None
        return "SHORT"
    return signal


def make_taker_sustain_long(n=10, taker_th=0.45, sustain_th=0.70, mac_n=4, mac_th=0.001):
    """
    TakerSustainLong: 卖方持续主导每根K线(系统性派发/过度悲观) + 4h上行 = 底部做多
    逻辑: 持续卖方主导但4h已转强 = 散户追空被主力做多
    taker_th < 0.5 表示 buy_vol/vol 低于 taker_th 算卖方主导
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        # 反向: 近N根 buy_vol/vol < taker_th 的比例 >= sustain_th
        valids = [c for c in cs1h[-n:] if c["vol"] > 0]
        if not valids: return None
        sell_sustain = sum(1 for c in valids if c["buy_vol"] / c["vol"] < taker_th) / len(valids)
        if sell_sustain < sustain_th:
            return None
        return "LONG"
    return signal


def theme_taker_sustain_break():
    strats = []
    # SHORT: 买方持续主导 + 4h弱 = 做空
    for n in [8, 10, 14]:
        for taker_th in [0.54, 0.57, 0.60]:
            for sustain_th in [0.65, 0.70, 0.75]:
                name = f"TakerSus_S_n{n}_t{int(taker_th*100)}_s{int(sustain_th*100)}"
                fn   = make_taker_sustain_short(n=n, taker_th=taker_th, sustain_th=sustain_th)
                doc  = (f"主动流量 SHORT: {n}根中{sustain_th*100:.0f}%根 买量占比>{taker_th*100:.0f}% + 4h下行")
                strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                "theme": "TakerSustainBreak", "doc": doc, "code": ""})
    # LONG: 卖方持续主导 + 4h强 = 做多
    for n in [8, 10, 14]:
        for taker_th in [0.40, 0.43, 0.46]:
            for sustain_th in [0.65, 0.70, 0.75]:
                name = f"TakerSus_L_n{n}_t{int(taker_th*100)}_s{int(sustain_th*100)}"
                fn   = make_taker_sustain_long(n=n, taker_th=taker_th, sustain_th=sustain_th)
                doc  = (f"主动流量 LONG: {n}根中{sustain_th*100:.0f}%根 买量占比<{taker_th*100:.0f}% + 4h上行")
                strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                "theme": "TakerSustainBreak", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 6] SeqContractionBreak
# 序列收缩爆发: 连续振幅收缩 = 弹性疲劳临界 = 任何扰动都会触发爆发
# 物理类比: 弹性疲劳 — 持续压缩导致材料内部微裂纹扩展,临界断裂
# ════════════════════════════════════════════════════════════════════════════════

def make_seq_contract_long(min_seq=4, mac_n=4, mac_th=0.001):
    """
    SeqContractLong: 连续振幅收缩 + 4h上行 = 向上爆发
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < min_seq + 5 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        if seq_contract(cs1h) < min_seq:
            return None
        return "LONG"
    return signal


def make_seq_contract_short(min_seq=4, mac_n=4, mac_th=-0.001):
    """
    SeqContractShort: 连续振幅收缩 + 4h下行 = 向下爆发
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < min_seq + 5 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        if seq_contract(cs1h) < min_seq:
            return None
        return "SHORT"
    return signal


def theme_seq_contraction_break():
    strats = []
    for min_seq in [3, 4, 5, 6, 7]:
        for direction, maker, tag in [("LONG", make_seq_contract_long, "L"),
                                       ("SHORT", make_seq_contract_short, "S")]:
            name = f"SeqContr_{tag}_s{min_seq}"
            fn   = maker(min_seq=min_seq)
            doc  = (f"序列收缩 {direction}: "
                    f"连续{min_seq}根振幅递减 + 4h{'上行' if direction == 'LONG' else '下行'}")
            strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                            "theme": "SeqContractionBreak", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 7] DirectionEntropyBreak
# 方向熵极值突破: 1h方向序列熵接近最大(完全随机) + 4h有方向 = 临界被宏观打破
# 物理类比: 相变临界点 — 微观涨落达到最大无序度时，宏观外场一旦施加即发生相变
# ════════════════════════════════════════════════════════════════════════════════

def direction_entropy(cs, n):
    """
    方向信息熵 (Direction Information Entropy)
    定义: H = -(p_up * log2(p_up) + p_dn * log2(p_dn))
    其中 p_up = 近N根中阳线(close>open)比例
    最大值 1.0 (p_up=0.5) = 多空完全随机均衡 = 临界态
    最小值 0.0 (全阳或全阴) = 单边主导
    物理意义: 信息熵极大 = 系统处于最无序态，任何外部力量都可打破对称
    """
    if len(cs) < n: return 0.0
    up = sum(1 for c in cs[-n:] if c["close"] > c["open"])
    p_up = up / n
    p_dn = 1.0 - p_up
    if p_up <= 0 or p_dn <= 0: return 0.0
    return -(p_up * math.log2(p_up) + p_dn * math.log2(p_dn))


def make_dir_entropy_long(n=10, entropy_th=0.90, mac_n=4, mac_th=0.001):
    """
    DirEntropyLong: 1h方向熵≥阈值(接近随机) + 4h上行 = 临界被多方打破
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        if direction_entropy(cs1h, n) < entropy_th:
            return None
        return "LONG"
    return signal


def make_dir_entropy_short(n=10, entropy_th=0.90, mac_n=4, mac_th=-0.001):
    """
    DirEntropyShort: 1h方向熵≥阈值(接近随机) + 4h下行 = 临界被空方打破
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        if direction_entropy(cs1h, n) < entropy_th:
            return None
        return "SHORT"
    return signal


def theme_direction_entropy_break():
    strats = []
    for n in [8, 10, 12, 16, 20]:
        for entropy_th in [0.85, 0.90, 0.95, 0.98]:
            for direction, maker, tag in [("LONG", make_dir_entropy_long, "L"),
                                           ("SHORT", make_dir_entropy_short, "S")]:
                name = f"DirEntropy_{tag}_n{n}_e{int(entropy_th*100)}"
                fn   = maker(n=n, entropy_th=entropy_th)
                doc  = (f"方向熵 {direction}: "
                        f"{n}根方向熵>={entropy_th:.2f}(最大=1.0) + "
                        f"4h{'上行' if direction == 'LONG' else '下行'}")
                strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                "theme": "DirectionEntropyBreak", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 8] VolDirDecoupling
# 量向解耦爆发: 成交量放大但与方向相关性趋近于零 = 两军胶着 = 临界态
# 物理类比: 布朗运动 — 大量粒子剧烈运动但宏观位移为零，热涨落极大时即将跃迁
# ════════════════════════════════════════════════════════════════════════════════

def vol_dir_correlation(cs, n):
    """
    量向相关系数 (Volume-Direction Pearson Correlation)
    定义: Pearson(bar_vol, bar_direction) over last n bars
    bar_direction = +1(阳线) / -1(阴线) / 0(十字)
    接近 0  = 成交量与方向无关 = 量向解耦 = 多空完全胶着
    接近 +1 = 大量伴随上涨 = 买方主导
    接近 -1 = 大量伴随下跌 = 卖方主导
    物理意义: 力与位移相关性为零 = 输入的功全部转化为内能(热量)而非动能
    """
    if len(cs) < n: return 0.0
    vols = [c["vol"] for c in cs[-n:]]
    dirs = [1.0 if c["close"] > c["open"] else (-1.0 if c["close"] < c["open"] else 0.0)
            for c in cs[-n:]]
    mean_v = sum(vols) / n
    mean_d = sum(dirs) / n
    cov = sum((vols[i] - mean_v) * (dirs[i] - mean_d) for i in range(n)) / n
    var_v = sum((v - mean_v) ** 2 for v in vols) / n
    var_d = sum((d - mean_d) ** 2 for d in dirs) / n
    if var_v <= 0 or var_d <= 0: return 0.0
    return cov / (var_v ** 0.5 * var_d ** 0.5)


def make_vol_decouple_long(n=10, n_hist=20, corr_th=0.15, vol_ratio=1.0, mac_n=4, mac_th=0.001):
    """
    VolDecoupleLong: 量向解耦(|corr|小) + 量能放大 + 4h上行 = 胶着后多方破局
    """
    def signal(cs1h, cs4h):
        total = n + n_hist
        if len(cs1h) < total + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        # 成交量需高于历史均值
        recent_vol = sum(c["vol"] for c in cs1h[-n:]) / n
        hist_vol   = sum(c["vol"] for c in cs1h[-total:-n]) / n_hist
        if hist_vol <= 0 or recent_vol < vol_ratio * hist_vol:
            return None
        # 量向相关性趋近于零
        if abs(vol_dir_correlation(cs1h, n)) > corr_th:
            return None
        return "LONG"
    return signal


def make_vol_decouple_short(n=10, n_hist=20, corr_th=0.15, vol_ratio=1.0, mac_n=4, mac_th=-0.001):
    """
    VolDecoupleShort: 量向解耦(|corr|小) + 量能放大 + 4h下行 = 胶着后空方破局
    """
    def signal(cs1h, cs4h):
        total = n + n_hist
        if len(cs1h) < total + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        recent_vol = sum(c["vol"] for c in cs1h[-n:]) / n
        hist_vol   = sum(c["vol"] for c in cs1h[-total:-n]) / n_hist
        if hist_vol <= 0 or recent_vol < vol_ratio * hist_vol:
            return None
        if abs(vol_dir_correlation(cs1h, n)) > corr_th:
            return None
        return "SHORT"
    return signal


def theme_vol_dir_decoupling():
    strats = []
    for n in [8, 10, 12]:
        for n_hist in [16, 24]:
            for corr_th in [0.10, 0.15, 0.20]:
                for vol_ratio in [0.9, 1.0, 1.2]:
                    for direction, maker, tag in [("LONG", make_vol_decouple_long, "L"),
                                                   ("SHORT", make_vol_decouple_short, "S")]:
                        name = f"VolDecouple_{tag}_n{n}_h{n_hist}_c{int(corr_th*100)}_v{int(vol_ratio*10)}"
                        fn   = maker(n=n, n_hist=n_hist, corr_th=corr_th, vol_ratio=vol_ratio)
                        doc  = (f"量向解耦 {direction}: "
                                f"{n}根|量向相关|<{corr_th:.2f} + 量>{vol_ratio:.1f}x历史 + "
                                f"4h{'上行' if direction == 'LONG' else '下行'}")
                        strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                        "theme": "VolDirDecoupling", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# 主题注册
# ════════════════════════════════════════════════════════════════════════════════

EXPLORATION_THEMES = [
    ("KineticEnergyBuild",    theme_kinetic_energy_build),
    ("AmplitudeSilenceBreak", theme_amplitude_silence_break),
    ("DojiDensitySignal",     theme_doji_density_signal),
    ("PriceGravityWell",      theme_price_gravity_well),
    ("TakerSustainBreak",     theme_taker_sustain_break),
    ("SeqContractionBreak",   theme_seq_contraction_break),
    ("DirectionEntropyBreak", theme_direction_entropy_break),
    ("VolDirDecoupling",      theme_vol_dir_decoupling),
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
    deployed_names = set() if force else load_deployed_names()
    LOG_DIR.mkdir(exist_ok=True)
    if out_path is None:
        out_path = LOG_DIR / f"alien5_{ts}.log"
    csv_path = Path(str(out_path).replace(".log", "_passed.csv"))

    print(f"Loading data ({len(ALT99)+4} symbols)...")
    all_syms = list(set(["BTC/USDT"] + BIG4 + ALT99))
    d1h, d4h = load_data(all_syms)
    print(f"Loaded {len(d1h)} 1h sets, {len(d4h)} 4h sets.\n")

    log_file = open(out_path, "w", encoding="utf-8", buffering=1)
    _orig    = sys.stdout
    sys.stdout = _Tee(_orig, log_file)

    sep = "=" * 80
    all_passed   = []
    deploy_queue = []

    try:
        print(f"\n{sep}")
        print(f"  ALIEN EXPLORE 5  [{datetime.now().strftime('%Y-%m-%d %H:%M')}]")
        print(f"  核心主题: 爆发前蓄力状态 (pre-explosion state detection)")
        print(f"  新原语: vol_energy_density / amplitude_silence / doji_density /")
        print(f"          price_cluster_density / taker_sustain / seq_contract")
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
                print(f"    [PASS] {p['name']:50s}  "
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
                print(f"  {i:2d}. {p['name']:55s}  "
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
    parser = argparse.ArgumentParser(description="Alien Explore 5 - Pre-explosion state detection")
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
