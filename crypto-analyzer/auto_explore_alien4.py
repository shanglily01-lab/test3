#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
auto_explore_alien4.py
======================
第四批非人类原语策略探索。

前三批已覆盖（共48个通过策略 A1-A48）:
  Batch1: wick_asym / body_entropy / sell_saturation / spatial_close /
          momentum_ratio / cross_residual / vol_absorption / candle_dna
  Batch2: saturation_velocity / path_tortuosity / amplitude_regime /
          shadow_strength / vol_concentration / time_pressure /
          price_memory / flux_momentum
  Batch3: order_flow_delta / vol_momentum / close_consistency /
          price_velocity (amplitude_skew/entropy_velocity 全淘汰)

本批新维度（缺口结构 + 量能高潮 + VWAP偏差 + 影线压力 + 实体减速 + 量向不对称）：

  gap_bias(cs, n)               -- 缺口偏向: 近N根开盘>前收盘的比例
  vol_climax(cs, n)             -- 量能高潮比: 当前量/近N均量
  vwap_dev(cs, n)               -- VWAP偏差: (close-vwap)/vwap 过去N根
  wick_pressure(cs, n)          -- 影线压力比: sum(上影线)/sum(下影线)
  body_decel(cs, near_n, far_n) -- 实体减速比: avg_body(near)/avg_body(far)
  vol_dir_asym(cs, n)           -- 量向不对称: avg_vol(上涨K)/avg_vol(下跌K)

自动部署：
  每通过10个策略 -> 写入 strategy_params DB + 追加 alien_signals.py + 文档
"""

import argparse
import bisect
import csv
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
# [新原语] 第四批非人类指标
# ════════════════════════════════════════════════════════════════════════════════

def gap_bias(cs, n):
    """
    开盘缺口偏向 (Gap Bias)
    定义: 近N根K线中 open > prev_close 的比例
    高值(>0.7) -> 持续向上跳空(买方推升开盘),可能超买
    低值(<0.3) -> 持续向下跳空(卖方压低开盘),可能超卖
    """
    if len(cs) < n + 1: return 0.5
    count = 0
    for i in range(-n, 0):
        if cs[i]["open"] > cs[i - 1]["close"]:
            count += 1
    return count / n


def vol_climax(cs, n):
    """
    量能高潮比 (Volume Climax Ratio)
    定义: cs[-1].vol / mean(cs[-n-1:-1].vol)
    高值(>2.0) -> 当前K线成交量显著放大,可能是顶/底
    """
    if len(cs) < n + 1: return 1.0
    mean_v = sum(c["vol"] for c in cs[-n-1:-1]) / n
    return cs[-1]["vol"] / mean_v if mean_v > 0 else 1.0


def vwap_dev(cs, n):
    """
    VWAP偏差 (VWAP Deviation)
    定义: (close[-1] - vwap_n) / vwap_n
    vwap = sum(typical_price * vol) / sum(vol), typical = (h+l+c)/3
    正值 -> 当前价在VWAP之上(买方主导,可能超买)
    负值 -> 当前价在VWAP之下(卖方主导,可能超卖)
    """
    if len(cs) < n: return 0.0
    total_vol = sum(c["vol"] for c in cs[-n:])
    if total_vol <= 0: return 0.0
    vwap = sum((c["high"] + c["low"] + c["close"]) / 3 * c["vol"] for c in cs[-n:]) / total_vol
    return (cs[-1]["close"] - vwap) / vwap if vwap > 0 else 0.0


def wick_pressure(cs, n):
    """
    影线压力比 (Wick Pressure Ratio)
    定义: sum(上影线) / (sum(下影线) + eps)
    高值(>2.0) -> 上影线远大于下影线,上方承压(卖方)
    低值(<0.5) -> 下影线远大于上影线,下方支撑(买方)
    """
    if len(cs) < n: return 1.0
    up_sum = 0.0; dn_sum = 0.0
    for c in cs[-n:]:
        rng = c["high"] - c["low"]
        if rng <= 0: continue
        bt = max(c["open"], c["close"])
        bb = min(c["open"], c["close"])
        up_sum += (c["high"] - bt)
        dn_sum += (bb - c["low"])
    return up_sum / (dn_sum + 1e-10)


def body_decel(cs, near_n, far_n):
    """
    实体减速比 (Body Deceleration)
    定义: avg_body(last near_n) / avg_body(prev far_n bars before that)
    < 1.0 -> 实体在缩小(动量减弱,可能耗竭)
    > 1.0 -> 实体在扩大(动量增强,可能加速)
    """
    total = near_n + far_n
    if len(cs) < total + 1: return 1.0
    near_avg = sum(abs(c["close"] - c["open"]) for c in cs[-near_n:]) / near_n
    far_avg  = sum(abs(c["close"] - c["open"]) for c in cs[-total:-near_n]) / far_n
    ref = cs[-1]["close"]
    if ref <= 0 or far_avg <= 0: return 1.0
    # 归一化：以价格为分母消除标的差异
    return (near_avg / ref) / (far_avg / ref + 1e-10)


def vol_dir_asym(cs, n):
    """
    量向不对称 (Volume Direction Asymmetry)
    定义: avg_vol(上涨K线) / avg_vol(下跌K线)
    高值(>1.5) -> 上涨时成交量远大于下跌时(买方主导)
    低值(<0.67) -> 下跌时成交量远大于上涨时(卖方主导)
    """
    if len(cs) < n: return 1.0
    up_vols  = [c["vol"] for c in cs[-n:] if c["close"] >= c["open"]]
    dn_vols  = [c["vol"] for c in cs[-n:] if c["close"] <  c["open"]]
    if not up_vols or not dn_vols: return 1.0
    return (sum(up_vols) / len(up_vols)) / (sum(dn_vols) / len(dn_vols))


# ════════════════════════════════════════════════════════════════════════════════
# 回测引擎
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
                notes  = (f"alien4_{p['theme']} | "
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
                """, (name, sl_pct, tp_pct, hold_h, "auto_explore_alien4",
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
# [Theme 1] GapBiasSignal
# 缺口偏向反转: 开盘持续向上/向下跳空 -> 过度定价 -> 反转
# 物理类比: 单向施力过久 -> 反弹力积累 -> 系统平衡恢复
# ════════════════════════════════════════════════════════════════════════════════

def make_gap_bias_short(n=6, bias_th=0.70, mac_n=4, mac_th=-0.001):
    """
    GapBiasShort: 持续向上跳空(开盘>前收) + 4h下行 = 买方过度推升开盘,超买做空
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        if gap_bias(cs1h, n) < bias_th:
            return None
        return "SHORT"
    return signal


def make_gap_bias_long(n=6, bias_th=0.30, mac_n=4, mac_th=0.001):
    """
    GapBiasLong: 持续向下跳空(开盘<前收) + 4h上行 = 卖方持续压低开盘,超卖做多
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        if gap_bias(cs1h, n) > bias_th:
            return None
        return "LONG"
    return signal


def theme_gap_bias_signal():
    strats = []
    for n in [4, 6, 8, 10]:
        for th in [0.70, 0.75, 0.80]:
            name = f"GapBias_S_n{n}_t{int(th*100)}"
            fn   = make_gap_bias_short(n=n, bias_th=th)
            doc  = f"缺口偏向 SHORT: {n}根中>{th*100:.0f}%向上跳空 + 4h下行"
            strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                            "theme": "GapBiasSignal", "doc": doc, "code": ""})
        for th in [0.20, 0.25, 0.30]:
            name = f"GapBias_L_n{n}_t{int(th*100)}"
            fn   = make_gap_bias_long(n=n, bias_th=th)
            doc  = f"缺口偏向 LONG: {n}根中<{th*100:.0f}%向上跳空 + 4h上行"
            strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                            "theme": "GapBiasSignal", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 2] VolClimaxReversal
# 量能高潮反转: 极端放量 + 价格方向 -> 顶/底确认
# 物理类比: 超导体临界电流 — 电流峰值过后必然骤降（超导态崩溃）
# ════════════════════════════════════════════════════════════════════════════════

def make_vol_climax_long(n=8, ratio=2.0, price_dir=-1, mac_n=4, mac_th=0.001):
    """
    VolClimaxLong: 极大量 + 价格下跌 + 4h上行 = 投降式放量底部
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        if vol_climax(cs1h, n) < ratio:
            return None
        # 当前K线价格下跌
        if cs1h[-1]["close"] >= cs1h[-1]["open"]:
            return None
        return "LONG"
    return signal


def make_vol_climax_short(n=8, ratio=2.0, mac_n=4, mac_th=-0.001):
    """
    VolClimaxShort: 极大量 + 价格上涨 + 4h下行 = 高潮式放量顶部
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        if vol_climax(cs1h, n) < ratio:
            return None
        # 当前K线价格上涨
        if cs1h[-1]["close"] <= cs1h[-1]["open"]:
            return None
        return "SHORT"
    return signal


def theme_vol_climax_reversal():
    strats = []
    for n in [6, 8, 12]:
        for ratio in [2.0, 2.5, 3.0]:
            name = f"VolClimax_L_n{n}_r{int(ratio*10)}"
            fn   = make_vol_climax_long(n=n, ratio=ratio)
            doc  = f"量能高潮 LONG: 当前量>{ratio:.1f}x均量 + 阴线 + 4h上行"
            strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                            "theme": "VolClimaxReversal", "doc": doc, "code": ""})
            name = f"VolClimax_S_n{n}_r{int(ratio*10)}"
            fn   = make_vol_climax_short(n=n, ratio=ratio)
            doc  = f"量能高潮 SHORT: 当前量>{ratio:.1f}x均量 + 阳线 + 4h下行"
            strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                            "theme": "VolClimaxReversal", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 3] VwapDeviationReturn
# VWAP均值回归: 价格偏离VWAP过远 -> 回归均衡
# 物理类比: 弹簧偏离平衡位置 — 偏移越大,恢复力越强
# ════════════════════════════════════════════════════════════════════════════════

def make_vwap_dev_short(n=12, dev_th=0.015, mac_n=4, mac_th=-0.001):
    """
    VwapDevShort: 价格大幅高于VWAP + 4h下行 = 过度偏离做空
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        if vwap_dev(cs1h, n) < dev_th:
            return None
        return "SHORT"
    return signal


def make_vwap_dev_long(n=12, dev_th=-0.015, mac_n=4, mac_th=0.001):
    """
    VwapDevLong: 价格大幅低于VWAP + 4h上行 = 超卖偏离做多
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        if vwap_dev(cs1h, n) > dev_th:
            return None
        return "LONG"
    return signal


def theme_vwap_deviation_return():
    strats = []
    for n in [8, 12, 16]:
        for th in [0.010, 0.015, 0.020]:
            th_str = f"{int(th*1000)}"
            name = f"VwapDev_S_n{n}_t{th_str}"
            fn   = make_vwap_dev_short(n=n, dev_th=th)
            doc  = f"VWAP偏差 SHORT: {n}根VWAP偏差>{th*100:.1f}% + 4h下行"
            strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                            "theme": "VwapDeviationReturn", "doc": doc, "code": ""})
            name = f"VwapDev_L_n{n}_t{th_str}"
            fn   = make_vwap_dev_long(n=n, dev_th=-th)
            doc  = f"VWAP偏差 LONG: {n}根VWAP偏差<-{th*100:.1f}% + 4h上行"
            strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                            "theme": "VwapDeviationReturn", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 4] WickPressureBalance
# 影线压力均衡: 上/下影线比例失衡 -> 方向压力 -> 反转触发
# 物理类比: 力矩不均衡 — 单侧受力后必然向另一侧倾斜
# ════════════════════════════════════════════════════════════════════════════════

def make_wick_pressure_short(n=6, wp_th=2.0, mac_n=4, mac_th=-0.001):
    """
    WickPressureShort: 上影线远大于下影线 + 4h下行 = 卖方持续压制顶部
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        if wick_pressure(cs1h, n) < wp_th:
            return None
        return "SHORT"
    return signal


def make_wick_pressure_long(n=6, wp_th=0.50, mac_n=4, mac_th=0.001):
    """
    WickPressureLong: 下影线远大于上影线 + 4h上行 = 买方持续托底
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        if wick_pressure(cs1h, n) > wp_th:
            return None
        return "LONG"
    return signal


def theme_wick_pressure_balance():
    strats = []
    for n in [4, 6, 8]:
        for wp_hi in [1.8, 2.2, 2.8]:
            name = f"WickPres_S_n{n}_t{int(wp_hi*10)}"
            fn   = make_wick_pressure_short(n=n, wp_th=wp_hi)
            doc  = f"影线压力 SHORT: {n}根上影/下影>{wp_hi:.1f} + 4h下行"
            strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                            "theme": "WickPressureBalance", "doc": doc, "code": ""})
        for wp_lo in [0.35, 0.45, 0.55]:
            name = f"WickPres_L_n{n}_t{int(wp_lo*100)}"
            fn   = make_wick_pressure_long(n=n, wp_th=wp_lo)
            doc  = f"影线压力 LONG: {n}根上影/下影<{wp_lo:.2f} + 4h上行"
            strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                            "theme": "WickPressureBalance", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 5] BodyDecelerationExhaustion
# 实体减速耗竭: 价格延续但实体缩小 -> 动量耗尽 -> 反转
# 物理类比: 火箭推力衰减 — 速度不变但加速度为零,即将停止
# ════════════════════════════════════════════════════════════════════════════════

def make_body_decel_short(near_n=2, far_n=6, decel_th=0.65, mac_n=4, mac_th=-0.001):
    """
    BodyDecelShort: 价格上涨但实体在缩小 + 4h下行 = 上涨动量耗竭做空
    """
    def signal(cs1h, cs4h):
        total = near_n + far_n
        if len(cs1h) < total + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        # 近far_n根价格总体在上涨
        if gradient(cs1h, far_n) <= 0.002:
            return None
        # 但实体在缩小
        if body_decel(cs1h, near_n, far_n) >= decel_th:
            return None
        return "SHORT"
    return signal


def make_body_decel_long(near_n=2, far_n=6, decel_th=0.65, mac_n=4, mac_th=0.001):
    """
    BodyDecelLong: 价格下跌但实体在缩小 + 4h上行 = 下跌动量耗竭做多
    """
    def signal(cs1h, cs4h):
        total = near_n + far_n
        if len(cs1h) < total + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        # 近far_n根价格总体在下跌
        if gradient(cs1h, far_n) >= -0.002:
            return None
        # 但实体在缩小
        if body_decel(cs1h, near_n, far_n) >= decel_th:
            return None
        return "LONG"
    return signal


def theme_body_decel_exhaustion():
    strats = []
    for near_n, far_n in [(2, 4), (2, 6), (3, 6), (3, 8)]:
        for th in [0.55, 0.65, 0.75]:
            name = f"BodyDecel_S_nr{near_n}_fr{far_n}_t{int(th*100)}"
            fn   = make_body_decel_short(near_n=near_n, far_n=far_n, decel_th=th)
            doc  = f"实体减速耗竭 SHORT: 上涨但近{near_n}根实体<远{far_n}根×{th:.2f} + 4h下行"
            strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                            "theme": "BodyDecelerationExhaustion", "doc": doc, "code": ""})
            name = f"BodyDecel_L_nr{near_n}_fr{far_n}_t{int(th*100)}"
            fn   = make_body_decel_long(near_n=near_n, far_n=far_n, decel_th=th)
            doc  = f"实体减速耗竭 LONG: 下跌但近{near_n}根实体<远{far_n}根×{th:.2f} + 4h上行"
            strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                            "theme": "BodyDecelerationExhaustion", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 6] VolDirectionAsymmetry
# 量向不对称: 上涨K线量 vs 下跌K线量的比例失衡 -> 主力方向暴露 -> 顺势
# 物理类比: 电流方向性 — 电子净流向决定了下一时刻的场方向
# ════════════════════════════════════════════════════════════════════════════════

def make_vol_dir_asym_long(n=8, lo_th=0.60, mac_n=4, mac_th=0.001):
    """
    VolDirAsymLong: 下跌量远大于上涨量(卖方主导) + 4h上行 = 卖方力量在此耗尽
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        # vda < lo_th: 上涨量/下跌量 很低 = 卖方主导
        if vol_dir_asym(cs1h, n) >= lo_th:
            return None
        return "LONG"
    return signal


def make_vol_dir_asym_short(n=8, hi_th=1.60, mac_n=4, mac_th=-0.001):
    """
    VolDirAsymShort: 上涨量远大于下跌量(买方主导) + 4h下行 = 买方力量在此耗尽
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        # vda > hi_th: 上涨量/下跌量 很高 = 买方主导
        if vol_dir_asym(cs1h, n) <= hi_th:
            return None
        return "SHORT"
    return signal


def theme_vol_direction_asymmetry():
    strats = []
    for n in [6, 8, 12]:
        for lo_th in [0.50, 0.60, 0.67]:
            name = f"VolDirAsym_L_n{n}_t{int(lo_th*100)}"
            fn   = make_vol_dir_asym_long(n=n, lo_th=lo_th)
            doc  = f"量向不对称 LONG: {n}根上涨量/下跌量<{lo_th:.2f}(卖方主导) + 4h上行"
            strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                            "theme": "VolDirectionAsymmetry", "doc": doc, "code": ""})
        for hi_th in [1.50, 1.70, 2.00]:
            name = f"VolDirAsym_S_n{n}_t{int(hi_th*100)}"
            fn   = make_vol_dir_asym_short(n=n, hi_th=hi_th)
            doc  = f"量向不对称 SHORT: {n}根上涨量/下跌量>{hi_th:.2f}(买方主导) + 4h下行"
            strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                            "theme": "VolDirectionAsymmetry", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# 主题注册
# ════════════════════════════════════════════════════════════════════════════════

EXPLORATION_THEMES = [
    ("GapBiasSignal",              theme_gap_bias_signal),
    ("VolClimaxReversal",          theme_vol_climax_reversal),
    ("VwapDeviationReturn",        theme_vwap_deviation_return),
    ("WickPressureBalance",        theme_wick_pressure_balance),
    ("BodyDecelerationExhaustion", theme_body_decel_exhaustion),
    ("VolDirectionAsymmetry",      theme_vol_direction_asymmetry),
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
        out_path = LOG_DIR / f"alien4_{ts}.log"
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
        print(f"  ALIEN EXPLORE 4  [{datetime.now().strftime('%Y-%m-%d %H:%M')}]")
        print(f"  新原语: gap_bias / vol_climax / vwap_dev /")
        print(f"          wick_pressure / body_decel / vol_dir_asym")
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
    parser = argparse.ArgumentParser(description="Alien Explore 4 - Batch 4 strategy discovery")
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
