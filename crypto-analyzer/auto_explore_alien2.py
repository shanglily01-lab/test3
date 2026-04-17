#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
auto_explore_alien2.py
======================
第二批非人类原语策略探索。

前批已覆盖:
  wick_asym / body_entropy / sell_saturation / spatial_close /
  momentum_ratio / cross_residual / vol_absorption / candle_dna

本批新维度（二阶效应 + 时间压力 + 流量密度 + 蜡烛几何）：

  saturation_velocity(cs, n, lag)  -- 饱和度变化速率: SS现在 - SS过去
  path_tortuosity(cs, n)           -- 路径曲折度: 方向翻转次数/n [0, 0.5]
  amplitude_regime(cs, short, long) -- 振幅比: 短窗口/长窗口, 收缩<1, 扩张>1
  shadow_strength(cs, n)           -- 影线强度一致性: abs(wick_asym)的均值
  vol_concentration(cs, n, k)      -- 成交量集中度: 前k大成交量占比
  time_pressure(cs, n, amp_th)     -- 时间压力: 连续低振幅蜡烛数 (压缩)
  price_memory(cs, n)              -- 价格记忆系数: 当前价到近期极端的距离
  flux_momentum(cs, short, long)   -- 流量动量: flux变化速率

自动部署：
  每通过10个策略 → 写入 strategy_params DB + 追加 alien_signals.py + 文档
"""

import argparse
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

def align4h(cs1h, cs4h, i):
    t1 = cs1h[i]["t"]
    return [c for c in cs4h if c["t"] <= t1]


# ════════════════════════════════════════════════════════════════════════════════
# [新原语] 第二批非人类指标
# ════════════════════════════════════════════════════════════════════════════════

def saturation_velocity(cs, n, lag):
    """
    饱和度速率 (Saturation Velocity)
    定义: sell_saturation(now, n) - sell_saturation(lag bars ago, n)
    正值 → 卖压在快速上升 (空头信号)
    负值 → 卖压在快速下降 (多头信号，投降接近)
    lag: 参照时间点向前偏移的蜡烛数
    """
    if len(cs) < n + lag:
        return 0.0
    ss_now  = sell_saturation(cs, n)
    ss_past = sell_saturation(cs[:-lag], n)
    return ss_now - ss_past


def path_tortuosity(cs, n):
    """
    路径曲折度 (Path Tortuosity)
    定义: 最近n根蜡烛中方向翻转的次数 / n
    0.0 → 完全单向 (强趋势)
    0.5 → 每根都翻转 (极度震荡)
    用途: 曲折度骤降 = 市场从混沌进入趋势
    """
    if len(cs) < n + 1:
        return 0.5
    dirs = [1 if c["close"] >= c["open"] else -1 for c in cs[-n:]]
    reversals = sum(1 for i in range(1, len(dirs)) if dirs[i] != dirs[i-1])
    return reversals / max(n - 1, 1)


def amplitude_regime(cs, short_n, long_n):
    """
    振幅政权比 (Amplitude Regime Ratio)
    定义: avg_amplitude(short_n) / avg_amplitude(long_n)
    > 1.5 → 近期振幅远超历史均值 (扩张/爆发)
    < 0.5 → 近期振幅远低于历史均值 (压缩/蓄力)
    ~ 1.0 → 平稳
    """
    if len(cs) < long_n + 2:
        return 1.0
    a_short = _amplitude(cs, short_n)
    a_long  = _amplitude(cs, long_n)
    return a_short / a_long if a_long > 0 else 1.0


def shadow_strength(cs, n):
    """
    影线强度 (Shadow Strength)
    定义: avg( abs((lower_wick - upper_wick) / range) ) over n bars
    不同于 wick_asym: 这里只看方向一致性不变，取绝对值
    0.0 → 每根蜡烛都是纯实体，无影线
    1.0 → 极端影线，实体在极端位置
    """
    if len(cs) < n:
        return 0.0
    scores = []
    for c in cs[-n:]:
        rng = c["high"] - c["low"]
        if rng <= 0:
            continue
        bt = max(c["open"], c["close"])
        bb = min(c["open"], c["close"])
        uw = c["high"] - bt
        lw = bb - c["low"]
        scores.append(abs(lw - uw) / rng)
    return sum(scores) / len(scores) if scores else 0.0


def vol_concentration(cs, n, top_k):
    """
    成交量集中度 (Volume Concentration)
    定义: 前 top_k 大成交量 K 线占总成交量的比例
    高值 → 少数几根K线承担了大部分成交 (机构集中操作)
    低值 → 成交量分散 (散户主导)
    """
    if len(cs) < n:
        return 0.5
    vols = sorted([c["vol"] for c in cs[-n:]], reverse=True)
    total = sum(vols)
    if total <= 0:
        return 0.5
    k = min(top_k, len(vols))
    return sum(vols[:k]) / total


def time_pressure(cs, n, amp_th):
    """
    时间压力指数 (Time Pressure Index)
    定义: 在最近n根中, 振幅 < amp_th 的蜡烛占比
    高值 (>0.7) → 长时间低振幅压缩, 即将爆发
    低值 (<0.3) → 振幅放大, 正在释放能量
    amp_th: 相对振幅阈值 (如 0.008 = 0.8%)
    """
    if len(cs) < n:
        return 0.5
    compressed = sum(
        1 for c in cs[-n:]
        if (c["high"] - c["low"]) / c["close"] < amp_th
        if c["close"] > 0
    )
    return compressed / n


def price_memory(cs, n):
    """
    价格记忆系数 (Price Memory)
    定义: (close - n根最低价) / (n根最高价 - n根最低价)
    1.0 → 当前价在近期最高点附近
    0.0 → 当前价在近期最低点附近
    0.5 → 中间位置
    意义: 类似 %K 但无平滑，反映价格位置记忆
    """
    if len(cs) < n:
        return 0.5
    hi = max(c["high"] for c in cs[-n:])
    lo = min(c["low"]  for c in cs[-n:])
    rng = hi - lo
    if rng <= 0:
        return 0.5
    return (cs[-1]["close"] - lo) / rng


def flux_momentum(cs, short_n, long_n):
    """
    流量动量 (Flux Momentum)
    定义: flux(short_n) - flux(long_n)
    正值 → 近期买压比长期均值更高 (新兴买方力量)
    负值 → 近期买压弱于历史 (买压在消退)
    """
    if len(cs) < long_n:
        return 0.0
    return flux(cs, short_n) - flux(cs, long_n)


# ════════════════════════════════════════════════════════════════════════════════
# 回测引擎
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
                notes  = (f"alien2_{p['theme']} | "
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
                """, (name, sl_pct, tp_pct, hold_h, "auto_explore_alien2",
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
            f.write('"""\nalien_signals.py\n')
            f.write('由 auto_explore_alien.py 自动生成的信号函数注册表。\n"""\n\n')
    if not DOC_FILE.exists():
        with open(DOC_FILE, "w", encoding="utf-8") as f:
            f.write("# Alien Strategy Registry\n\n")
            f.write("由 `auto_explore_alien.py` / `auto_explore_alien2.py` 自动生成。\n\n")
            f.write("---\n")


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 1] SaturationVelocity
# 卖方饱和度变化速率: 卖压加速/减速 = 动量信号的二阶效应
# 物理类比: 加速度 — 速度的导数比速度本身更早预警方向变化
# ════════════════════════════════════════════════════════════════════════════════

def make_sat_velocity_long(n=4, lag=3, vel_th=-0.05, mac_n=4, mac_th=0.001):
    """
    SatVelocityLong
    条件:
      1. 宏观 4h 向上
      2. 卖压速率 < vel_th (卖压在快速下降 = 投降加速)
      3. 当前卖压仍高 (>0.50, 确认是从高位下降, 不是低位噪声)
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + lag + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        vel = saturation_velocity(cs1h, n, lag)
        if vel >= vel_th:
            return None
        if sell_saturation(cs1h, n) <= 0.50:
            return None
        return "LONG"
    return signal


def make_sat_velocity_short(n=4, lag=3, vel_th=0.05, mac_n=4, mac_th=-0.001):
    """
    SatVelocityShort
    条件:
      1. 宏观 4h 向下
      2. 买方饱和度在快速下降 (sell_saturation 在快速上升)
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + lag + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        vel = saturation_velocity(cs1h, n, lag)
        if vel <= vel_th:
            return None
        if sell_saturation(cs1h, n) >= 0.50:
            return None
        return "SHORT"
    return signal


def theme_saturation_velocity():
    strats = []
    for n in [3, 4, 6]:
        for lag in [2, 3, 4]:
            for vel_th in [-0.04, -0.06, -0.08]:
                name = f"SatVel_L_n{n}_l{lag}_v{int(abs(vel_th)*100)}"
                fn = make_sat_velocity_long(n=n, lag=lag, vel_th=vel_th)
                doc = f"卖压速率 LONG: 4h上行 + 卖压以>{abs(vel_th)}/bar速率下降 (lag={lag})"
                strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                "theme": "SaturationVelocity", "doc": doc, "code": ""})
            for vel_th in [0.04, 0.06, 0.08]:
                name = f"SatVel_S_n{n}_l{lag}_v{int(vel_th*100)}"
                fn = make_sat_velocity_short(n=n, lag=lag, vel_th=vel_th)
                doc = f"买压速率 SHORT: 4h下行 + 卖压以>{vel_th}/bar速率上升 (lag={lag})"
                strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                "theme": "SaturationVelocity", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 2] TortuosityBreak
# 路径曲折度突变: 市场从震荡进入趋势的临界点
# 物理类比: 相变临界点 — 随机漫步突然变成弹道运动
# ════════════════════════════════════════════════════════════════════════════════

def make_tort_break_long(hist_n=8, now_n=3, hist_th=0.35, now_th=0.20,
                          mac_n=4, mac_th=0.001):
    """
    TortuosityBreakLong
    条件:
      1. 历史曲折度高 (hist_th: 曾经震荡)
      2. 最近曲折度低 (now_th: 现在变趋势了)
      3. 宏观方向向上确认方向
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < hist_n + now_n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        if path_tortuosity(cs1h[-hist_n - now_n:-now_n], hist_n) <= hist_th:
            return None
        if path_tortuosity(cs1h, now_n) >= now_th:
            return None
        return "LONG"
    return signal


def make_tort_break_short(hist_n=8, now_n=3, hist_th=0.35, now_th=0.20,
                           mac_n=4, mac_th=-0.001):
    def signal(cs1h, cs4h):
        if len(cs1h) < hist_n + now_n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        if path_tortuosity(cs1h[-hist_n - now_n:-now_n], hist_n) <= hist_th:
            return None
        if path_tortuosity(cs1h, now_n) >= now_th:
            return None
        return "SHORT"
    return signal


def theme_tortuosity_break():
    strats = []
    for hist_n in [6, 8, 10]:
        for now_n in [2, 3]:
            for hist_th in [0.30, 0.40]:
                for now_th in [0.15, 0.20]:
                    name = f"TortBrk_L_h{hist_n}_n{now_n}_ht{int(hist_th*100)}_nt{int(now_th*100)}"
                    fn = make_tort_break_long(hist_n=hist_n, now_n=now_n,
                                              hist_th=hist_th, now_th=now_th)
                    doc = f"曲折度突变 LONG: 历史曲折>{hist_th} 后近期<{now_th} + 4h上行"
                    strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                   "theme": "TortuosityBreak", "doc": doc, "code": ""})
                    name = f"TortBrk_S_h{hist_n}_n{now_n}_ht{int(hist_th*100)}_nt{int(now_th*100)}"
                    fn = make_tort_break_short(hist_n=hist_n, now_n=now_n,
                                               hist_th=hist_th, now_th=now_th)
                    doc = f"曲折度突变 SHORT: 历史曲折>{hist_th} 后近期<{now_th} + 4h下行"
                    strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                   "theme": "TortuosityBreak", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 3] AmplitudeCompression
# 振幅政权识别: 压缩后扩张 = 能量蓄积后爆发
# 物理类比: 弹簧压缩 — 压得越久, 释放越猛
# ════════════════════════════════════════════════════════════════════════════════

def make_amp_compression_long(short_n=3, long_n=10, comp_th=0.60,
                               mac_n=4, mac_th=0.001):
    """
    AmpCompressionLong
    条件:
      1. 振幅比 (short/long) < comp_th (近期振幅明显小于历史)
      2. 宏观 4h 向上 (确认突破方向)
    解读: 振幅被压缩 + 宏观向上 = 积蓄能量的上行突破
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < long_n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        if amplitude_regime(cs1h, short_n, long_n) >= comp_th:
            return None
        return "LONG"
    return signal


def make_amp_compression_short(short_n=3, long_n=10, comp_th=0.60,
                                mac_n=4, mac_th=-0.001):
    def signal(cs1h, cs4h):
        if len(cs1h) < long_n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        if amplitude_regime(cs1h, short_n, long_n) >= comp_th:
            return None
        return "SHORT"
    return signal


def theme_amplitude_compression():
    strats = []
    for short_n in [2, 3]:
        for long_n in [8, 12, 16]:
            for comp_th in [0.50, 0.60, 0.70]:
                name = f"AmpComp_L_s{short_n}_l{long_n}_c{int(comp_th*100)}"
                fn = make_amp_compression_long(short_n=short_n, long_n=long_n,
                                               comp_th=comp_th)
                doc = f"振幅压缩 LONG: {short_n}根/{long_n}根振幅比<{comp_th} + 4h上行"
                strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                "theme": "AmplitudeCompression", "doc": doc, "code": ""})
                name = f"AmpComp_S_s{short_n}_l{long_n}_c{int(comp_th*100)}"
                fn = make_amp_compression_short(short_n=short_n, long_n=long_n,
                                                comp_th=comp_th)
                doc = f"振幅压缩 SHORT: {short_n}根/{long_n}根振幅比<{comp_th} + 4h下行"
                strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                "theme": "AmplitudeCompression", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 4] ShadowConsensus
# 影线共识策略: 多根蜡烛影线方向一致 = 隐藏的市场共识
# 物理类比: 极化光 — 所有光波方向一致时能量集中
# ════════════════════════════════════════════════════════════════════════════════

def make_shadow_consensus_long(shad_n=4, shad_th=0.30, mac_n=4, mac_th=0.001,
                                sc_th=0.50):
    """
    ShadowConsensusLong
    条件:
      1. 宏观 4h 向上
      2. 影线强度高 (多根蜡烛都有明显下影线) > shad_th
      3. 收盘位置偏上 (买方最终控制收盘)
    解读: 价格反复被拒绝在低位 + 买方推动收盘在上 = 强多头共识
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < shad_n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        if shadow_strength(cs1h, shad_n) <= shad_th:
            return None
        if spatial_close(cs1h, shad_n) <= sc_th:
            return None
        return "LONG"
    return signal


def make_shadow_consensus_short(shad_n=4, shad_th=0.30, mac_n=4, mac_th=-0.001,
                                 sc_th=0.50):
    def signal(cs1h, cs4h):
        if len(cs1h) < shad_n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        if shadow_strength(cs1h, shad_n) <= shad_th:
            return None
        if spatial_close(cs1h, shad_n) >= sc_th:
            return None
        return "SHORT"
    return signal


def theme_shadow_consensus():
    strats = []
    for shad_n in [3, 4, 6]:
        for shad_th in [0.25, 0.35, 0.45]:
            for sc_th in [0.52, 0.55]:
                name = f"ShadCons_L_n{shad_n}_sh{int(shad_th*100)}_sc{int(sc_th*100)}"
                fn = make_shadow_consensus_long(shad_n=shad_n, shad_th=shad_th, sc_th=sc_th)
                doc = f"影线共识 LONG: {shad_n}根影线强度>{shad_th} + 收盘偏上>{sc_th} + 4h上行"
                strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                "theme": "ShadowConsensus", "doc": doc, "code": ""})
            for sc_th in [0.48, 0.45]:
                name = f"ShadCons_S_n{shad_n}_sh{int(shad_th*100)}_sc{int(sc_th*100)}"
                fn = make_shadow_consensus_short(shad_n=shad_n, shad_th=shad_th, sc_th=sc_th)
                doc = f"影线共识 SHORT: {shad_n}根影线强度>{shad_th} + 收盘偏下<{sc_th} + 4h下行"
                strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                "theme": "ShadowConsensus", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 5] VolumeConcentrationShift
# 成交量集中度转变: 成交量突然集中在少数K线 = 机构进场信号
# 物理类比: 暗物质聚集 — 不可见力量在特定时空点汇聚
# ════════════════════════════════════════════════════════════════════════════════

def make_volconc_long(n=6, top_k=2, conc_th=0.55, mac_n=4, mac_th=0.001):
    """
    VolConcentrationLong
    条件:
      1. 成交量高度集中 (前2根承担了>55%成交量)
      2. 宏观 4h 向上
      3. 集中成交的那根 K 线是阳线 (集中买入)
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        if vol_concentration(cs1h, n, top_k) <= conc_th:
            return None
        top_candles = sorted(cs1h[-n:], key=lambda c: c["vol"], reverse=True)[:top_k]
        if not all(c["close"] >= c["open"] for c in top_candles):
            return None
        return "LONG"
    return signal


def make_volconc_short(n=6, top_k=2, conc_th=0.55, mac_n=4, mac_th=-0.001):
    def signal(cs1h, cs4h):
        if len(cs1h) < n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        if vol_concentration(cs1h, n, top_k) <= conc_th:
            return None
        top_candles = sorted(cs1h[-n:], key=lambda c: c["vol"], reverse=True)[:top_k]
        if not all(c["close"] < c["open"] for c in top_candles):
            return None
        return "SHORT"
    return signal


def theme_vol_concentration():
    strats = []
    for n in [5, 6, 8]:
        for top_k in [2, 3]:
            for conc_th in [0.50, 0.55, 0.60]:
                name = f"VolConc_L_n{n}_k{top_k}_c{int(conc_th*100)}"
                fn = make_volconc_long(n=n, top_k=top_k, conc_th=conc_th)
                doc = f"量集中 LONG: {n}根中前{top_k}根占>{conc_th*100:.0f}%且为阳线 + 4h上行"
                strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                "theme": "VolConcentration", "doc": doc, "code": ""})
                name = f"VolConc_S_n{n}_k{top_k}_c{int(conc_th*100)}"
                fn = make_volconc_short(n=n, top_k=top_k, conc_th=conc_th)
                doc = f"量集中 SHORT: {n}根中前{top_k}根占>{conc_th*100:.0f}%且为阴线 + 4h下行"
                strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                "theme": "VolConcentration", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 6] TimePressureRelease
# 时间压力释放: 长期低振幅压缩后的扩张
# 物理类比: 临界沸腾 — 水在100°C时还需要一个成核点才能沸腾
# ════════════════════════════════════════════════════════════════════════════════

def make_time_pressure_long(pres_n=8, pres_amp=0.008, pres_th=0.70,
                             mac_n=4, mac_th=0.001):
    """
    TimePressureLong
    条件:
      1. 连续高压缩状态 (最近n根中>70%振幅低于0.8%)
      2. 宏观 4h 向上 (确认爆发方向)
    解读: 长时间低波动蓄能 + 宏观向上 = 向上爆发在即
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < pres_n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        if time_pressure(cs1h, pres_n, pres_amp) <= pres_th:
            return None
        return "LONG"
    return signal


def make_time_pressure_short(pres_n=8, pres_amp=0.008, pres_th=0.70,
                              mac_n=4, mac_th=-0.001):
    def signal(cs1h, cs4h):
        if len(cs1h) < pres_n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        if time_pressure(cs1h, pres_n, pres_amp) <= pres_th:
            return None
        return "SHORT"
    return signal


def theme_time_pressure():
    strats = []
    for pres_n in [6, 8, 10]:
        for pres_amp in [0.006, 0.008, 0.010]:
            for pres_th in [0.65, 0.75]:
                name = f"TimePres_L_n{pres_n}_a{int(pres_amp*1000)}_t{int(pres_th*100)}"
                fn = make_time_pressure_long(pres_n=pres_n, pres_amp=pres_amp,
                                             pres_th=pres_th)
                doc = f"时间压力 LONG: {pres_n}根中>{pres_th*100:.0f}%振幅<{pres_amp*100:.1f}% + 4h上行"
                strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                "theme": "TimePressure", "doc": doc, "code": ""})
                name = f"TimePres_S_n{pres_n}_a{int(pres_amp*1000)}_t{int(pres_th*100)}"
                fn = make_time_pressure_short(pres_n=pres_n, pres_amp=pres_amp,
                                              pres_th=pres_th)
                doc = f"时间压力 SHORT: {pres_n}根中>{pres_th*100:.0f}%振幅<{pres_amp*100:.1f}% + 4h下行"
                strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                "theme": "TimePressure", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 7] PriceMemoryExtreme
# 价格记忆极端: 价格在近期极端位置的行为预测
# 物理类比: 势阱边界效应 — 粒子在势阱边缘停留时间最短
# ════════════════════════════════════════════════════════════════════════════════

def make_price_memory_long(mem_n=12, mem_lo=0.20, mac_n=4, mac_th=0.001,
                            sc_th=0.50):
    """
    PriceMemoryLong (超卖反弹)
    条件:
      1. 价格记忆系数极低 (在近期最低位附近)
      2. 宏观 4h 向上
      3. 最新一根K线收盘偏上 (初步止跌确认)
    解读: 价格到达近期最低位 + 宏观上行 + 最后一根止跌 = 超卖反弹
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < mem_n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        if price_memory(cs1h, mem_n) >= mem_lo:
            return None
        if spatial_close(cs1h, 2) <= sc_th:
            return None
        return "LONG"
    return signal


def make_price_memory_short(mem_n=12, mem_hi=0.80, mac_n=4, mac_th=-0.001,
                             sc_th=0.50):
    def signal(cs1h, cs4h):
        if len(cs1h) < mem_n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        if price_memory(cs1h, mem_n) <= mem_hi:
            return None
        if spatial_close(cs1h, 2) >= sc_th:
            return None
        return "SHORT"
    return signal


def theme_price_memory():
    strats = []
    for mem_n in [10, 14, 20]:
        for mem_lo in [0.15, 0.20, 0.25]:
            name = f"PriceMem_L_n{mem_n}_lo{int(mem_lo*100)}"
            fn = make_price_memory_long(mem_n=mem_n, mem_lo=mem_lo)
            doc = f"价格记忆 LONG: {mem_n}根内极低位<{mem_lo*100:.0f}% + 初步止跌 + 4h上行"
            strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                            "theme": "PriceMemory", "doc": doc, "code": ""})
        for mem_hi in [0.75, 0.80, 0.85]:
            name = f"PriceMem_S_n{mem_n}_hi{int(mem_hi*100)}"
            fn = make_price_memory_short(mem_n=mem_n, mem_hi=mem_hi)
            doc = f"价格记忆 SHORT: {mem_n}根内极高位>{mem_hi*100:.0f}% + 初步转头 + 4h下行"
            strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                            "theme": "PriceMemory", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 8] FluxMomentumDivergence
# 流量动量背离: flux短期变化速率与价格方向的错配
# 物理类比: 洋流与波浪不同向 — 表面波方向与深层流方向背离
# ════════════════════════════════════════════════════════════════════════════════

def make_flux_momentum_long(short_n=2, long_n=8, fm_th=0.03,
                             mac_n=4, mac_th=0.001, g_th=-0.002):
    """
    FluxMomentumLong
    条件:
      1. 宏观 4h 向上
      2. 1h 近期价格下跌 (短期反跌)
      3. flux 动量为正 (买方力量在悄悄增加)
    解读: 价格下跌但买压在上升 = 量价背离, 即将反弹
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < long_n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        if gradient(cs1h, 3) >= g_th:
            return None
        if flux_momentum(cs1h, short_n, long_n) <= fm_th:
            return None
        return "LONG"
    return signal


def make_flux_momentum_short(short_n=2, long_n=8, fm_th=-0.03,
                              mac_n=4, mac_th=-0.001, g_th=0.002):
    """
    FluxMomentumShort
    条件:
      1. 宏观 4h 向下
      2. 1h 近期价格上涨 (短期反弹)
      3. flux 动量为负 (买压在悄悄减弱)
    解读: 价格上涨但买压在下降 = 量价背离, 反弹末尾
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < long_n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        if gradient(cs1h, 3) <= g_th:
            return None
        if flux_momentum(cs1h, short_n, long_n) >= fm_th:
            return None
        return "SHORT"
    return signal


def theme_flux_momentum():
    strats = []
    for short_n in [2, 3]:
        for long_n in [6, 8, 12]:
            for fm_th_l, fm_th_s in [(0.03, -0.03), (0.04, -0.04), (0.05, -0.05)]:
                name = f"FluxMom_L_s{short_n}_l{long_n}_f{int(fm_th_l*100)}"
                fn = make_flux_momentum_long(short_n=short_n, long_n=long_n, fm_th=fm_th_l)
                doc = f"流量动量 LONG: 4h上行 + 价格下跌 + flux动量>{fm_th_l} (量价背离)"
                strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                "theme": "FluxMomentum", "doc": doc, "code": ""})
                name = f"FluxMom_S_s{short_n}_l{long_n}_f{int(abs(fm_th_s)*100)}"
                fn = make_flux_momentum_short(short_n=short_n, long_n=long_n, fm_th=fm_th_s)
                doc = f"流量动量 SHORT: 4h下行 + 价格上涨 + flux动量<{fm_th_s} (量价背离)"
                strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                "theme": "FluxMomentum", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# 主题注册表
# ════════════════════════════════════════════════════════════════════════════════

EXPLORATION_THEMES = [
    ("SaturationVelocity",    theme_saturation_velocity),
    ("TortuosityBreak",       theme_tortuosity_break),
    ("AmplitudeCompression",  theme_amplitude_compression),
    ("ShadowConsensus",       theme_shadow_consensus),
    ("VolConcentration",      theme_vol_concentration),
    ("TimePressure",          theme_time_pressure),
    ("PriceMemory",           theme_price_memory),
    ("FluxMomentum",          theme_flux_momentum),
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
        out_path = LOG_DIR / f"alien2_{ts}.log"
    csv_path = Path(str(out_path).replace(".log", "_passed.csv"))

    print(f"Loading data ({len(ALT99)+4} symbols)...")
    all_syms = list(set(["BTC/USDT"] + BIG4 + ALT99))
    d1h, d4h = load_data(all_syms)
    print(f"Loaded {len(d1h)} 1h sets, {len(d4h)} 4h sets.\n")

    log_file = open(out_path, "w", encoding="utf-8")
    _orig    = sys.stdout
    sys.stdout = _Tee(_orig, log_file)

    sep = "=" * 80
    all_passed   = []
    deploy_queue = []

    try:
        print(f"\n{sep}")
        print(f"  ALIEN EXPLORE 2  [{datetime.now().strftime('%Y-%m-%d %H:%M')}]")
        print(f"  新原语: saturation_velocity / path_tortuosity / amplitude_regime /")
        print(f"          shadow_strength / vol_concentration / time_pressure /")
        print(f"          price_memory / flux_momentum")
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
            print("\n  按测试胜率排名 (前20):")
            for i, p in enumerate(sorted(all_passed, key=lambda x: -x["test_wr"])[:20], 1):
                print(f"  {i:2d}. {p['name']:48s}  [{p['mode']:10s}]  "
                      f"train={p['s3_wr']*100:.1f}%  test={p['test_wr']*100:.1f}%  "
                      f"n={p['s3_n']}/{p['test_n']}")

        print(f"\n  日志: {out_path}")
        print(f"  CSV:  {csv_path}")
        if not no_deploy:
            print(f"  Code: {SIGNALS_PY}")
            print(f"  Doc:  {DOC_FILE}")
        print(sep)

    finally:
        sys.stdout = _orig
        log_file.close()

    if all_passed:
        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=[
                "name", "theme", "mode",
                "s3_n", "s3_wr_pct", "s3_ev_pct",
                "test_n", "test_wr_pct", "test_ev_pct", "doc"
            ])
            w.writeheader()
            for p in sorted(all_passed, key=lambda x: -x["test_wr"]):
                w.writerow({
                    "name":         p["name"],
                    "theme":        p.get("theme", ""),
                    "mode":         p["mode"],
                    "s3_n":         p["s3_n"],
                    "s3_wr_pct":    f"{p['s3_wr']*100:.1f}",
                    "s3_ev_pct":    f"{p['s3_ev']:+.2f}",
                    "test_n":       p["test_n"],
                    "test_wr_pct":  f"{p['test_wr']*100:.1f}",
                    "test_ev_pct":  f"{p['test_ev']:+.2f}",
                    "doc":          p.get("doc", ""),
                })

    return all_passed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Alien Strategy Explorer - Batch 2")
    parser.add_argument("--theme",       default=None,  help="只运行指定主题")
    parser.add_argument("--out",         default=None,  help="输出日志路径")
    parser.add_argument("--no-deploy",   action="store_true", help="不自动部署到DB")
    parser.add_argument("--list-themes", action="store_true", help="列出主题")
    parser.add_argument("--force",       action="store_true",
                        help="强制重跑 strategy_params 里已有的策略名")
    args = parser.parse_args()

    if args.list_themes:
        total = 0
        for name, fn in EXPLORATION_THEMES:
            count = len(fn())
            print(f"  {name:30s}  {count:4d} 个候选策略")
            total += count
        print(f"  {'合计':30s}  {total:4d} 个候选策略")
        sys.exit(0)

    run_exploration(
        theme_filter=args.theme,
        out_path=Path(args.out) if args.out else None,
        no_deploy=args.no_deploy,
        force=args.force,
    )
