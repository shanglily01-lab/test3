#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
auto_explore_alien.py
=====================
用非人类技术指标原语探索新交易策略。

核心思想: 不用 gradient/flux/amplitude 的变体，
         把市场看作物理/信息系统，使用全新原语:

  wick_asym(cs, n)          -- 影线不对称度: 多空拒绝压力的空间信号
  body_entropy(cs, n)       -- 蜡烛方向熵: 市场混沌度 → 秩序突变
  sell_saturation(cs, n)    -- 卖方累积饱和度: 卖压峰值后的衰退
  spatial_close(cs, n)      -- 收盘位置得分: 价格在振幅内的空间偏向
  momentum_ratio(cs, s, l)  -- 短期/长期动量比: 加速/减速检测
  cross_residual(cs_a, b, n) -- 超额动量: 相对参照资产的剩余强度
  vol_absorption(cs, n)     -- 量价背离系数: 大量小价=吸筹/派发迹象
  candle_dna(cs, n)         -- 蜡烛序列编码: 方向模式的统计规律

自动部署:
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

# 每通过多少策略自动部署一次
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
# 基础 SL/TP 计算（与 dimension_trader 一致）
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


# ════════════════════════════════════════════════════════════════════════════════
# [新原语] 非人类市场指标
# ════════════════════════════════════════════════════════════════════════════════

def wick_asym(cs, n):
    """
    影线不对称度 (Wick Asymmetry)
    定义: avg( (下影线 - 上影线) / 振幅 ) over n bars
    正值 → 市场持续拒绝低价，买方压力在空间维度占优
    负值 → 市场持续拒绝高价，卖方压力占优
    取值范围: [-1, +1]
    """
    if len(cs) < n:
        return 0.0
    scores = []
    for c in cs[-n:]:
        rng = c["high"] - c["low"]
        if rng <= 0:
            continue
        body_top = max(c["open"], c["close"])
        body_bot = min(c["open"], c["close"])
        upper_wick = c["high"] - body_top
        lower_wick = body_bot - c["low"]
        scores.append((lower_wick - upper_wick) / rng)
    return sum(scores) / len(scores) if scores else 0.0


def body_entropy(cs, n):
    """
    蜡烛方向熵 (Body Direction Entropy)
    定义: Shannon 熵，基于 n 根蜡烛的涨/跌方向比例
    H=0   → 全涨或全跌，极度有序
    H=1   → 涨跌各半，最大混沌
    用途: 熵从高 → 低 = 方向性秩序正在建立
    """
    if len(cs) < n:
        return 1.0
    bull = sum(1 for c in cs[-n:] if c["close"] > c["open"])
    bear = n - bull
    if bull == 0 or bear == 0:
        return 0.0
    pb = bull / n
    pp = bear / n
    return -(pb * math.log2(pb) + pp * math.log2(pp))


def sell_saturation(cs, n):
    """
    卖方累积饱和度 (Sell Pressure Saturation)
    定义: 最近 n 根蜡烛中卖方成交量占总成交量的比例
    = 1 - avg(buy_vol / vol)
    高值(>0.55) → 卖压主导
    低值(<0.45) → 买压主导
    """
    if len(cs) < n:
        return 0.5
    rs = [(c["vol"] - c["buy_vol"]) / c["vol"]
          for c in cs[-n:] if c["vol"] > 0]
    return sum(rs) / len(rs) if rs else 0.5


def spatial_close(cs, n):
    """
    空间收盘得分 (Spatial Close Score)
    定义: avg( (close - low) / (high - low) ) over n bars
    1.0 → 每根都收在最高价附近 (极强)
    0.0 → 每根都收在最低价附近 (极弱)
    0.5 → 中性
    意义: 比 flux 更精确 —— 描述价格最终"停在哪里"而不是"多少人在买"
    """
    if len(cs) < n:
        return 0.5
    scores = []
    for c in cs[-n:]:
        rng = c["high"] - c["low"]
        if rng <= 0:
            scores.append(0.5)
        else:
            scores.append((c["close"] - c["low"]) / rng)
    return sum(scores) / len(scores) if scores else 0.5


def momentum_ratio(cs, short_n=2, long_n=8):
    """
    动量比值 (Momentum Ratio)
    定义: gradient(short_n) / gradient(long_n)，归一化
    > 1.5 → 动量正在加速 (短期远超长期均速)
    < 0.3 → 动量急剧衰减 (短期远弱于长期均速)
    ~ 1.0 → 匀速运动
    注: gradient 为 0 时返回 0
    """
    if len(cs) < long_n + 2:
        return 1.0
    g_s = sum(c["close"] - c["open"] for c in cs[-short_n:])
    g_l = sum(c["close"] - c["open"] for c in cs[-long_n:])
    ref = cs[-1]["close"]
    if ref <= 0:
        return 1.0
    gs = g_s / ref
    gl = g_l / ref
    if abs(gl) < 1e-9:
        return 0.0 if abs(gs) < 1e-9 else 2.0
    r = gs / gl
    return r


def cross_residual(cs_alt, cs_ref, n):
    """
    超额动量 (Cross-Asset Residual)
    定义: gradient_alt(n) - gradient_ref(n)
    正值 → ALT 相对参照资产超涨 (独立强势)
    负值 → ALT 相对参照资产滞后 (潜在补涨机会或相对弱势)
    用法:
      mtf_btc 模式下: cs_ref = BTC 4h 数据
      正负解读取决于策略假设
    """
    def grad(cs, k):
        if len(cs) < k:
            return 0.0
        s = sum(c["close"] - c["open"] for c in cs[-k:])
        ref = cs[-1]["close"]
        return s / ref if ref else 0.0

    return grad(cs_alt, n) - grad(cs_ref, n)


def vol_absorption(cs, n):
    """
    量价背离系数 (Volume-Price Absorption)
    定义: avg(vol / (high - low)) 相对于历史基线的比值
    高值 → 大量成交但价格几乎不动 = 吸筹/派发 (机构行为)
    低值 → 小量带动大幅价格 = 流动性真空
    返回: 相对系数 (当前/历史均值)，>1.5 为显著吸筹状态
    """
    if len(cs) < n * 2:
        return 1.0
    def density(subset):
        vals = []
        for c in subset:
            rng = c["high"] - c["low"]
            if rng > 0 and c["vol"] > 0:
                vals.append(c["vol"] / rng)
        return sum(vals) / len(vals) if vals else 0.0
    recent = density(cs[-n:])
    hist   = density(cs[-(n * 2):-n])
    return recent / hist if hist > 0 else 1.0


def candle_dna_score(cs, n, pattern):
    """
    蜡烛序列模式得分 (Candle DNA)
    pattern: 期望的方向序列，如 [-1,-1,-1,1,1] (三跌两涨)
    返回: 0~1，实际序列与期望模式的匹配度
    每根蜡烛: +1=涨, -1=跌, 0=十字星(判为跌)
    """
    if len(cs) < n or len(pattern) != n:
        return 0.0
    actual = [1 if c["close"] > c["open"] else -1 for c in cs[-n:]]
    matches = sum(1 for a, p in zip(actual, pattern) if a == p)
    return matches / n


# ════════════════════════════════════════════════════════════════════════════════
# 回测引擎（复用 auto_explore.py 的结构）
# ════════════════════════════════════════════════════════════════════════════════

def gradient(cs, n):
    """保留，用于宏观方向判断（此处是允许的）"""
    if len(cs) < n:
        return 0.0
    s = sum(c["close"] - c["open"] for c in cs[-n:])
    ref = cs[-1]["close"]
    return s / ref if ref else 0.0


def align4h(cs1h, cs4h, i):
    t1 = cs1h[i]["t"]
    return [c for c in cs4h if c["t"] <= t1]


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
                if len(cs4h) < 10:
                    continue
                s = bt_mtf(fn, cs1h, cs4h, i_start, i_end)
            elif mode == "mtf_btc":
                if sym == "BTC/USDT":
                    continue
                btc4h = d4h.get("BTC/USDT", [])
                if len(btc4h) < 10:
                    continue
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
    """
    批量部署通过验证的策略:
    1. 写入 strategy_params 数据库
    2. 追加信号函数代码到 alien_signals.py
    3. 追加文档到 alien_strategies_doc.md
    """
    if not passed_batch:
        return

    # ── 数据库写入 ─────────────────────────────────────────────────────────────
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
                # SL/TP 参考回测振幅中位数（用 SL_MIN/MAX 中点）
                sl_pct = round((SL_MIN + SL_MAX) / 2 * SL_MULT, 4)   # ~0.0188
                tp_pct = round((SL_MIN + SL_MAX) / 2 * TP_MULT, 4)   # ~0.0313
                hold_h = HOLD_BARS   # 默认与回测一致
                notes  = (f"alien_{p['theme']} | "
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
                """, (name, sl_pct, tp_pct, hold_h, "auto_explore_alien",
                      p["s3_n"], round(p["test_wr"], 4), notes, now, now))
                inserted.append(name)
        conn.commit()
    finally:
        conn.close()

    if inserted:
        print(f"  [DEPLOY] DB: {len(inserted)} 个策略写入 strategy_params")

    # ── 追加代码到 alien_signals.py ───────────────────────────────────────────
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

    # ── 追加文档到 alien_strategies_doc.md ───────────────────────────────────
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
            f.write('由 auto_explore_alien.py 自动生成的信号函数注册表。\n')
            f.write('每个函数签名: (cs1h, cs4h) -> "LONG"/"SHORT"/None\n"""\n\n')
            f.write('from auto_explore_alien import (\n')
            f.write('    wick_asym, body_entropy, sell_saturation,\n')
            f.write('    spatial_close, momentum_ratio, cross_residual,\n')
            f.write('    vol_absorption, candle_dna_score, gradient\n)\n')
    if not DOC_FILE.exists():
        with open(DOC_FILE, "w", encoding="utf-8") as f:
            f.write("# Alien Strategy Registry\n\n")
            f.write("由 `auto_explore_alien.py` 自动生成。\n\n")
            f.write("每个策略均通过四阶段验证 (train→10alts→86alts→walk-forward)。\n\n")
            f.write("---\n")


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 1] WickRejection
# 影线拒绝策略: 连续影线不对称 → 隐藏的买/卖压
# 物理类比: 弹性势能积累 — 价格被反复弹回，最终突破方向明确
# ════════════════════════════════════════════════════════════════════════════════

def make_wick_rejection_long(wa_n=4, wa_th=0.12, mac_n=4, mac_th=0.001,
                               sc_n=3, sc_th=0.52):
    """
    WickRejectionLong
    条件:
      1. 宏观 4h 方向向上 (gradient > mac_th)
      2. 最近 wa_n 根蜡烛的影线不对称度 > wa_th (下影线持续大于上影线)
      3. 空间收盘得分 sc_n 根 > sc_th (价格停在振幅上半段)
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < wa_n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        if wick_asym(cs1h, wa_n) <= wa_th:
            return None
        if spatial_close(cs1h, sc_n) <= sc_th:
            return None
        return "LONG"
    return signal


def make_wick_rejection_short(wa_n=4, wa_th=-0.12, mac_n=4, mac_th=-0.001,
                                sc_n=3, sc_th=0.48):
    """
    WickRejectionShort
    条件:
      1. 宏观 4h 方向向下
      2. 影线不对称度 < wa_th (上影线持续大于下影线，高价被拒绝)
      3. 空间收盘得分 < sc_th (价格停在振幅下半段)
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < wa_n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        if wick_asym(cs1h, wa_n) >= wa_th:
            return None
        if spatial_close(cs1h, sc_n) >= sc_th:
            return None
        return "SHORT"
    return signal


def theme_wick_rejection():
    strats = []
    # LONG
    for wa_n in [3, 4, 6]:
        for wa_th in [0.08, 0.12, 0.16]:
            for sc_th in [0.51, 0.54]:
                name = f"WickRej_L_n{wa_n}_w{int(wa_th*100)}_s{int(sc_th*100)}"
                fn = make_wick_rejection_long(wa_n=wa_n, wa_th=wa_th, sc_th=sc_th)
                code = textwrap.dedent(f"""
                    def sig_{name}(cs1h, cs4h):
                        if len(cs1h) < {wa_n + 4} or len(cs4h) < 6: return None
                        if gradient(cs4h, 4) <= 0.001: return None
                        if wick_asym(cs1h, {wa_n}) <= {wa_th}: return None
                        if spatial_close(cs1h, 3) <= {sc_th}: return None
                        return "LONG"
                """).strip()
                doc = (f"影线拒绝 LONG: 4h上行 + {wa_n}根下影线主导(>{wa_th:.2f}) "
                       f"+ 收盘偏上(>{sc_th})")
                strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                "theme": "WickRejection",
                                "code": code, "doc": doc})
    # SHORT
    for wa_n in [3, 4, 6]:
        for wa_th in [-0.08, -0.12, -0.16]:
            for sc_th in [0.49, 0.46]:
                name = f"WickRej_S_n{wa_n}_w{int(abs(wa_th)*100)}_s{int(sc_th*100)}"
                fn = make_wick_rejection_short(wa_n=wa_n, wa_th=wa_th, sc_th=sc_th)
                code = textwrap.dedent(f"""
                    def sig_{name}(cs1h, cs4h):
                        if len(cs1h) < {wa_n + 4} or len(cs4h) < 6: return None
                        if gradient(cs4h, 4) >= -0.001: return None
                        if wick_asym(cs1h, {wa_n}) >= {wa_th}: return None
                        if spatial_close(cs1h, 3) >= {sc_th}: return None
                        return "SHORT"
                """).strip()
                doc = (f"影线拒绝 SHORT: 4h下行 + {wa_n}根上影线主导(<{wa_th:.2f}) "
                       f"+ 收盘偏下(<{sc_th})")
                strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                "theme": "WickRejection",
                                "code": code, "doc": doc})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 2] EntropyCollapse
# 熵坍缩策略: 混沌市场突然进入有序状态 = 方向性政权建立
# 物理类比: 相变 — 液体凝固时突然产生晶体结构
# ════════════════════════════════════════════════════════════════════════════════

def make_entropy_collapse_long(hist_n=8, now_n=3, ent_hi=0.80, ent_lo=0.35,
                                 mac_n=4, mac_th=0.001, sc_th=0.52):
    """
    EntropyCollapseLong
    条件:
      1. 历史 hist_n 根熵 > ent_hi (曾经混沌)
      2. 最近 now_n 根熵 < ent_lo (现在有序)
      3. 4h 宏观向上
      4. 空间收盘偏上 (最近2根收盘在上半段)
    熵下降 + 宏观向上 = 多头政权建立
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < hist_n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        if body_entropy(cs1h[-hist_n:], hist_n) <= ent_hi:
            return None
        if body_entropy(cs1h, now_n) >= ent_lo:
            return None
        # 最近几根必须偏多
        if spatial_close(cs1h, 2) <= sc_th:
            return None
        return "LONG"
    return signal


def make_entropy_collapse_short(hist_n=8, now_n=3, ent_hi=0.80, ent_lo=0.35,
                                  mac_n=4, mac_th=-0.001, sc_th=0.48):
    """
    EntropyCollapseShort
    条件:
      1. 历史混沌
      2. 最近有序 (有序方向为空头)
      3. 4h 宏观向下
      4. 空间收盘偏下
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < hist_n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        if body_entropy(cs1h[-hist_n:], hist_n) <= ent_hi:
            return None
        if body_entropy(cs1h, now_n) >= ent_lo:
            return None
        if spatial_close(cs1h, 2) >= sc_th:
            return None
        return "SHORT"
    return signal


def theme_entropy_collapse():
    strats = []
    for hist_n in [6, 8, 10]:
        for ent_hi in [0.75, 0.85]:
            for ent_lo in [0.30, 0.40]:
                for sc_th_l, sc_th_s in [(0.52, 0.48), (0.55, 0.45)]:
                    # LONG
                    name = f"EntColl_L_h{hist_n}_hi{int(ent_hi*100)}_lo{int(ent_lo*100)}"
                    fn = make_entropy_collapse_long(hist_n=hist_n, ent_hi=ent_hi,
                                                    ent_lo=ent_lo, sc_th=sc_th_l)
                    doc = (f"熵坍缩 LONG: {hist_n}根历史混沌(>{ent_hi}) → "
                           f"最近3根有序(<{ent_lo}) + 4h上行 + 收盘偏上")
                    strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                   "theme": "EntropyCollapse", "doc": doc, "code": ""})
                    # SHORT
                    name = f"EntColl_S_h{hist_n}_hi{int(ent_hi*100)}_lo{int(ent_lo*100)}"
                    fn = make_entropy_collapse_short(hist_n=hist_n, ent_hi=ent_hi,
                                                     ent_lo=ent_lo, sc_th=sc_th_s)
                    doc = (f"熵坍缩 SHORT: {hist_n}根历史混沌(>{ent_hi}) → "
                           f"最近3根有序(<{ent_lo}) + 4h下行 + 收盘偏下")
                    strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                   "theme": "EntropyCollapse", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 3] SellCapitulation / BuyExhaustion
# 卖方投降 / 买方耗竭
# 物理类比: 化学反应达到平衡点后的方向性突破
# ════════════════════════════════════════════════════════════════════════════════

def make_sell_capitulation_long(sat_n=6, sat_th=0.58, decay_n=2, decay_th=0.50,
                                  mac_n=4, mac_th=0.001):
    """
    SellCapitulationLong
    条件:
      1. 宏观 4h 向上
      2. 中期 sat_n 根卖方饱和度高 (> sat_th, 卖压主导)
      3. 最近 decay_n 根卖压已回落到 < decay_th (卖方耗尽，买方接管)
    这是卖方投降的精确识别: 卖了很多，但卖不动了
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < sat_n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        # 中期卖压高
        if sell_saturation(cs1h, sat_n) <= sat_th:
            return None
        # 近期卖压回落
        if sell_saturation(cs1h, decay_n) >= decay_th:
            return None
        return "LONG"
    return signal


def make_buy_exhaustion_short(sat_n=6, sat_th=0.58, decay_n=2, decay_th=0.50,
                                mac_n=4, mac_th=-0.001):
    """
    BuyExhaustionShort (反向: 买方耗竭)
    条件:
      1. 宏观 4h 向下
      2. 中期买方饱和度高 (1-sell_sat > sat_th, 即 sell_sat < 1-sat_th)
      3. 最近买压回落 (sell_sat > decay_th, 即 buy 下降)
    """
    buy_hi = 1.0 - sat_th
    def signal(cs1h, cs4h):
        if len(cs1h) < sat_n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        # 中期买压高
        if sell_saturation(cs1h, sat_n) >= (1.0 - sat_th):
            return None
        # 近期买压回落 (卖压上升)
        if sell_saturation(cs1h, decay_n) <= decay_th:
            return None
        return "SHORT"
    return signal


def theme_capitulation():
    strats = []
    for sat_n in [4, 6, 8]:
        for sat_th in [0.55, 0.58, 0.62]:
            for decay_th in [0.48, 0.51]:
                # 卖方投降 LONG
                name = f"SellCap_L_n{sat_n}_hi{int(sat_th*100)}_d{int(decay_th*100)}"
                fn = make_sell_capitulation_long(sat_n=sat_n, sat_th=sat_th,
                                                  decay_th=decay_th)
                doc = (f"卖方投降 LONG: {sat_n}根卖压>{sat_th} 后"
                       f"最近2根卖压<{decay_th} + 4h上行")
                strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                "theme": "SellCapitulation", "doc": doc, "code": ""})
                # 买方耗竭 SHORT
                name = f"BuyExh_S_n{sat_n}_hi{int(sat_th*100)}_d{int(decay_th*100)}"
                fn = make_buy_exhaustion_short(sat_n=sat_n, sat_th=sat_th,
                                               decay_th=decay_th)
                doc = (f"买方耗竭 SHORT: {sat_n}根买压高 后"
                       f"最近2根买压回落>{decay_th} + 4h下行")
                strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                "theme": "BuyExhaustion", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 4] MomentumDecayShort
# 动量衰减做空: 上涨动量快速衰减 = 买方已无法维持推力
# 物理类比: 火箭推力减弱时，引力开始主导
# ════════════════════════════════════════════════════════════════════════════════

def make_momentum_decay_short(short_n=2, long_n=8, ratio_max=0.40,
                                mac_n=4, mac_th=-0.0005, sc_th=0.50):
    """
    MomentumDecayShort
    条件:
      1. 宏观 4h 方向为负或中性 (不强烈向上)
      2. 动量比值 < ratio_max (短期动量远小于长期均速 → 快速衰减)
      3. 收盘位置 <= sc_th (最近价格停在振幅下半段)
      4. 上升方向确认 (长期 gradient > 0, 说明确实曾经上涨)
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < long_n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= 0.003:
            return None
        # 必须确实曾经上涨 (有东西可以衰减)
        if gradient(cs1h, long_n) <= 0.001:
            return None
        r = momentum_ratio(cs1h, short_n=short_n, long_n=long_n)
        if r >= ratio_max:
            return None
        if spatial_close(cs1h, 3) >= sc_th:
            return None
        return "SHORT"
    return signal


def make_momentum_surge_long(short_n=2, long_n=8, ratio_min=1.5,
                               mac_n=4, mac_th=0.001, sc_th=0.55):
    """
    MomentumSurgeLong (反向: 动量加速做多)
    条件:
      1. 宏观 4h 向上
      2. 动量比值 > ratio_min (短期加速超过长期均速)
      3. 收盘偏上
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < long_n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        if gradient(cs1h, long_n) <= 0:
            return None
        r = momentum_ratio(cs1h, short_n=short_n, long_n=long_n)
        if r <= ratio_min:
            return None
        if spatial_close(cs1h, 2) <= sc_th:
            return None
        return "LONG"
    return signal


def theme_momentum_decay():
    strats = []
    for long_n in [6, 8, 10]:
        for ratio_max in [0.30, 0.40, 0.50]:
            name = f"MomDecay_S_l{long_n}_r{int(ratio_max*100)}"
            fn = make_momentum_decay_short(long_n=long_n, ratio_max=ratio_max)
            doc = (f"动量衰减 SHORT: 4h中性/下行 + {long_n}根曾上行 + "
                   f"动量比<{ratio_max} + 收盘偏下")
            strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                            "theme": "MomentumDecay", "doc": doc, "code": ""})
        for ratio_min in [1.5, 2.0, 2.5]:
            name = f"MomSurge_L_l{long_n}_r{int(ratio_min*10)}"
            fn = make_momentum_surge_long(long_n=long_n, ratio_min=ratio_min)
            doc = (f"动量加速 LONG: 4h上行 + 短期动量比>{ratio_min}x + 收盘偏上")
            strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                            "theme": "MomentumSurge", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 5] VolAbsorption
# 量价背离策略: 大量小价 = 机构在悄悄吃单
# 物理类比: 恒星塌缩 — 大量能量被压缩进极小空间，等待爆发
# ════════════════════════════════════════════════════════════════════════════════

def make_vol_absorption_long(abs_n=3, abs_th=1.4, mac_n=4, mac_th=0.001,
                               sc_th=0.50):
    """
    VolAbsorptionLong
    条件:
      1. 宏观 4h 向上
      2. 量价背离系数 > abs_th (最近3根大量+小价 = 吸筹迹象)
      3. 收盘位置偏上 (吸筹完成后向上收盘)
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < abs_n * 2 + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        if vol_absorption(cs1h, abs_n) <= abs_th:
            return None
        if spatial_close(cs1h, abs_n) <= sc_th:
            return None
        return "LONG"
    return signal


def make_vol_absorption_short(abs_n=3, abs_th=1.4, mac_n=4, mac_th=-0.001,
                                sc_th=0.50):
    """
    VolAbsorptionShort (派发迹象)
    大量小价在下跌背景下 = 机构在悄悄卖出 (派发)
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < abs_n * 2 + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        if vol_absorption(cs1h, abs_n) <= abs_th:
            return None
        if spatial_close(cs1h, abs_n) >= sc_th:
            return None
        return "SHORT"
    return signal


def theme_vol_absorption():
    strats = []
    for abs_n in [3, 4, 5]:
        for abs_th in [1.3, 1.5, 1.8, 2.0]:
            for sc_th_l, sc_th_s in [(0.52, 0.48), (0.55, 0.45)]:
                name = f"VolAbs_L_n{abs_n}_t{int(abs_th*10)}_s{int(sc_th_l*100)}"
                fn = make_vol_absorption_long(abs_n=abs_n, abs_th=abs_th, sc_th=sc_th_l)
                doc = f"量价背离 LONG (吸筹): {abs_n}根背离>{abs_th}x + 4h上行 + 收盘偏上"
                strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                "theme": "VolAbsorption", "doc": doc, "code": ""})

                name = f"VolAbs_S_n{abs_n}_t{int(abs_th*10)}_s{int(sc_th_s*100)}"
                fn = make_vol_absorption_short(abs_n=abs_n, abs_th=abs_th, sc_th=sc_th_s)
                doc = f"量价背离 SHORT (派发): {abs_n}根背离>{abs_th}x + 4h下行 + 收盘偏下"
                strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                "theme": "VolAbsorption", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 6] SpatialDivergence
# 空间背离策略: 收盘位置与量流方向背离 = 市场结构裂缝
# 物理类比: 应力累积 — 地壳板块相向运动的断层
# ════════════════════════════════════════════════════════════════════════════════

def make_spatial_flux_diverge_long(sc_n=4, sc_th=0.45, sat_th=0.52,
                                     mac_n=4, mac_th=0.001):
    """
    SpatialFluxDivergeLong
    背离定义: 收盘位置偏低(sc < 0.45)但卖压不高(sell_sat < 0.52)
    解读: 价格跌到下半段，但买方仍在支撑 → 假跌，即将反转
    需要宏观向上确认
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < sc_n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        # 收盘偏低 (似乎弱势)
        if spatial_close(cs1h, sc_n) >= sc_th:
            return None
        # 但卖压其实不强 (背离信号)
        if sell_saturation(cs1h, sc_n) >= sat_th:
            return None
        return "LONG"
    return signal


def make_spatial_flux_diverge_short(sc_n=4, sc_th=0.55, sat_th=0.48,
                                      mac_n=4, mac_th=-0.001):
    """
    SpatialFluxDivergeShort
    背离定义: 收盘位置偏高(sc > 0.55)但买压不高(sell_sat > 0.48, 即buy_sat<0.52)
    解读: 价格涨到上半段，但买方力量不足 → 假涨，即将回落
    """
    def signal(cs1h, cs4h):
        if len(cs1h) < sc_n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        # 收盘偏高 (似乎强势)
        if spatial_close(cs1h, sc_n) <= sc_th:
            return None
        # 但买压其实不强 (背离信号)
        if sell_saturation(cs1h, sc_n) <= sat_th:
            return None
        return "SHORT"
    return signal


def theme_spatial_divergence():
    strats = []
    for sc_n in [3, 4, 5]:
        for sc_th_l, sc_th_s in [(0.43, 0.57), (0.46, 0.54)]:
            for sat_th_l, sat_th_s in [(0.50, 0.50), (0.52, 0.48)]:
                name = f"SpatDiv_L_n{sc_n}_sc{int(sc_th_l*100)}_sat{int(sat_th_l*100)}"
                fn = make_spatial_flux_diverge_long(sc_n=sc_n, sc_th=sc_th_l,
                                                     sat_th=sat_th_l)
                doc = (f"空间背离 LONG: 收盘偏低<{sc_th_l} 但卖压不高<{sat_th_l}"
                       f" (假跌) + 4h上行")
                strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                "theme": "SpatialDivergence", "doc": doc, "code": ""})

                name = f"SpatDiv_S_n{sc_n}_sc{int(sc_th_s*100)}_sat{int(sat_th_s*100)}"
                fn = make_spatial_flux_diverge_short(sc_n=sc_n, sc_th=sc_th_s,
                                                      sat_th=sat_th_s)
                doc = (f"空间背离 SHORT: 收盘偏高>{sc_th_s} 但买压不足>{sat_th_s}"
                       f" (假涨) + 4h下行")
                strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                                "theme": "SpatialDivergence", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 7] CrossResidual (mtf_btc 模式)
# 超额动量策略: ALT相对BTC的剩余强度
# 物理类比: 一个物体在引力场中的相对速度 — 脱离了基准场的独立运动
# ════════════════════════════════════════════════════════════════════════════════

def make_cross_residual_long(res_n=4, res_th=0.002, btc_mac_n=4, btc_mac_th=0.002,
                               sc_th=0.52):
    """
    CrossResidualLong (mtf_btc)
    条件:
      1. BTC 4h 向上 (btc_mac_th)
      2. ALT 相对 BTC 的超额动量 > res_th (ALT 独立偏强)
      3. ALT 空间收盘偏上
    解读: BTC 上涨但 ALT 涨得更快 = ALT 独立强势，继续跟进
    """
    def signal(cs_alt, cs_btc4h):
        if len(cs_alt) < res_n + 4 or len(cs_btc4h) < btc_mac_n + 2:
            return None
        if gradient(cs_btc4h, btc_mac_n) <= btc_mac_th:
            return None
        res = cross_residual(cs_alt, cs_btc4h, res_n)
        if res <= res_th:
            return None
        if spatial_close(cs_alt, 2) <= sc_th:
            return None
        return "LONG"
    return signal


def make_cross_residual_lag_long(res_n=4, res_th=-0.003, btc_mac_n=4,
                                   btc_mac_th=0.005, sc_th=0.50):
    """
    CrossResidualLagLong (mtf_btc)
    条件:
      1. BTC 4h 强烈上涨 (> btc_mac_th, 高阈值)
      2. ALT 相对 BTC 的超额动量 < res_th (ALT 滞后于 BTC)
      3. ALT 最近有轻微买压
    解读: BTC 拉得很猛但 ALT 没跟上 = 滞后补涨机会
    """
    def signal(cs_alt, cs_btc4h):
        if len(cs_alt) < res_n + 4 or len(cs_btc4h) < btc_mac_n + 2:
            return None
        if gradient(cs_btc4h, btc_mac_n) <= btc_mac_th:
            return None
        res = cross_residual(cs_alt, cs_btc4h, res_n)
        if res >= res_th:
            return None
        if spatial_close(cs_alt, 2) <= sc_th:
            return None
        return "LONG"
    return signal


def theme_cross_residual():
    strats = []
    for res_n in [3, 4, 6]:
        for btc_mac_th in [0.002, 0.004]:
            # ALT 独立强势
            for res_th in [0.001, 0.003, 0.005]:
                name = f"CrossRes_L_n{res_n}_b{int(btc_mac_th*1000)}_r{int(res_th*1000)}"
                fn = make_cross_residual_long(res_n=res_n, res_th=res_th,
                                               btc_mac_th=btc_mac_th)
                doc = (f"超额动量 LONG (独立强): BTC 4h>{btc_mac_th} + "
                       f"ALT超额>{res_th} ({res_n}根)")
                strats.append({"name": name, "fn": fn, "mode": "mtf_btc",
                                "theme": "CrossResidual", "doc": doc, "code": ""})
            # ALT 滞后补涨
            for btc_hi in [0.005, 0.008]:
                for lag_th in [-0.002, -0.004]:
                    name = f"CrossLag_L_n{res_n}_b{int(btc_hi*1000)}_l{int(abs(lag_th)*1000)}"
                    fn = make_cross_residual_lag_long(res_n=res_n, res_th=lag_th,
                                                      btc_mac_th=btc_hi)
                    doc = (f"超额动量 LONG (滞后补涨): BTC 4h强>{btc_hi} + "
                           f"ALT 滞后<{lag_th} ({res_n}根)")
                    strats.append({"name": name, "fn": fn, "mode": "mtf_btc",
                                   "theme": "CrossResidual", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# [Theme 8] CandleDNA
# 蜡烛序列DNA: 特定方向模式后的统计规律
# 物理类比: 密码子 — DNA三联体编码特定氨基酸，市场三联蜡烛编码特定走势
# ════════════════════════════════════════════════════════════════════════════════

def make_candle_dna_long(pattern, dna_th=0.75, mac_n=4, mac_th=0.001, sc_th=0.52):
    """
    CandleDNALong
    检测特定蜡烛方向序列 + 宏观 + 空间得分
    pattern 示例: [-1,-1,-1,1] = 三跌一涨 (底部确认)
    """
    n = len(pattern)
    def signal(cs1h, cs4h):
        if len(cs1h) < n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) <= mac_th:
            return None
        if candle_dna_score(cs1h, n, pattern) < dna_th:
            return None
        if spatial_close(cs1h, 2) <= sc_th:
            return None
        return "LONG"
    return signal


def make_candle_dna_short(pattern, dna_th=0.75, mac_n=4, mac_th=-0.001,
                           sc_th=0.48):
    n = len(pattern)
    def signal(cs1h, cs4h):
        if len(cs1h) < n + 4 or len(cs4h) < mac_n + 2:
            return None
        if gradient(cs4h, mac_n) >= mac_th:
            return None
        if candle_dna_score(cs1h, n, pattern) < dna_th:
            return None
        if spatial_close(cs1h, 2) >= sc_th:
            return None
        return "SHORT"
    return signal


def theme_candle_dna():
    strats = []
    # 多头模式: N跌后反转
    long_patterns = [
        ([-1,-1,1],      "3b_reversal"),       # 两跌一涨
        ([-1,-1,-1,1],   "4b_reversal"),       # 三跌一涨
        ([-1,1,-1,1],    "alternating_L"),     # 交替收敛多头
        ([-1,-1,1,1],    "double_bottom"),     # 双底形态
        ([-1,-1,-1,1,1], "5b_reversal"),       # 五棒反转
    ]
    # 空头模式: N涨后反转
    short_patterns = [
        ([1,1,-1],       "3u_reversal"),
        ([1,1,1,-1],     "4u_reversal"),
        ([1,-1,1,-1],    "alternating_S"),
        ([1,1,-1,-1],    "double_top"),
        ([1,1,1,-1,-1],  "5u_reversal"),
    ]
    for pattern, pname in long_patterns:
        for mac_th in [0.001, 0.003]:
            name = f"DNA_L_{pname}_m{int(mac_th*1000)}"
            fn = make_candle_dna_long(pattern=pattern, mac_th=mac_th)
            doc = f"蜡烛DNA LONG: 模式{pattern} + 4h>{mac_th}"
            strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                            "theme": "CandleDNA", "doc": doc, "code": ""})
    for pattern, pname in short_patterns:
        for mac_th in [-0.001, -0.003]:
            name = f"DNA_S_{pname}_m{int(abs(mac_th)*1000)}"
            fn = make_candle_dna_short(pattern=pattern, mac_th=mac_th)
            doc = f"蜡烛DNA SHORT: 模式{pattern} + 4h<{mac_th}"
            strats.append({"name": name, "fn": fn, "mode": "mtf_self",
                            "theme": "CandleDNA", "doc": doc, "code": ""})
    return strats


# ════════════════════════════════════════════════════════════════════════════════
# 主题注册表
# ════════════════════════════════════════════════════════════════════════════════

EXPLORATION_THEMES = [
    ("WickRejection",     theme_wick_rejection),
    ("EntropyCollapse",   theme_entropy_collapse),
    ("Capitulation",      theme_capitulation),
    ("MomentumDecay",     theme_momentum_decay),
    ("VolAbsorption",     theme_vol_absorption),
    ("SpatialDivergence", theme_spatial_divergence),
    ("CrossResidual",     theme_cross_residual),
    ("CandleDNA",         theme_candle_dna),
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
        out_path = LOG_DIR / f"alien_{ts}.log"
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
        print(f"  ALIEN EXPLORE  [{datetime.now().strftime('%Y-%m-%d %H:%M')}]")
        print(f"  非人类指标原语: wick_asym / body_entropy / sell_saturation /")
        print(f"                  spatial_close / momentum_ratio / cross_residual /")
        print(f"                  vol_absorption / candle_dna")
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

            # 每积累 AUTO_DEPLOY_BATCH 个就自动部署
            if not no_deploy and len(deploy_queue) >= AUTO_DEPLOY_BATCH:
                print(f"\n  [AUTO-DEPLOY] {len(deploy_queue)} 个策略触发部署...")
                deploy_strategies(deploy_queue)
                deploy_queue.clear()

        # 部署剩余
        if not no_deploy and deploy_queue:
            print(f"\n  [AUTO-DEPLOY] 剩余 {len(deploy_queue)} 个策略部署...")
            deploy_strategies(deploy_queue)
            deploy_queue.clear()

        # 总结
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


# ── 入口 ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Alien Strategy Explorer")
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

    run_exploration(theme_filter=args.theme, out_path=args.out,
                    no_deploy=args.no_deploy, force=args.force)
