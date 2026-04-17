#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gemini_backtest.py
==================
用 Gemini 作为纯信号源的策略回测。

流程：
  1. 对每个标的取1h K线历史数据
  2. 在训练集/测试集中随机抽样若干时间点
  3. 将每个时间点前 N 根K线（纯数字，无技术标签）发给 Gemini
  4. Gemini 返回 LONG / SHORT / NEUTRAL
  5. 按标准 SL/TP 框架统计胜率

目标：WR > 57% 则视为有效信号，可集成到实盘。

用法:
  python gemini_backtest.py --bars 10 --samples 50 --symbols 5 --out logs/gemini_test.log
  python gemini_backtest.py --bars 12 --samples 200 --phase train  # 正式训练集验证
"""

import argparse
import os
import queue
import random
import sys
import threading
import time
import warnings
from datetime import datetime
from pathlib import Path

import pymysql
from dotenv import load_dotenv

warnings.filterwarnings("ignore")
load_dotenv()

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import google.generativeai as genai
from google.generativeai.types import HarmCategory, HarmBlockThreshold
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

# ── 常量 ──────────────────────────────────────────────────────────────────────

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "models/gemini-3-flash-preview")
HOLD_BARS    = 3
SL_MIN, SL_MAX, SL_MULT, TP_MULT = 0.005, 0.020, 1.5, 2.5
TRAIN_RATIO  = 0.70

# 顺序调用 (Gemini API rate limit — 不并发，避免触发限制)
MAX_WORKERS  = 1
RETRY_WAIT   = 2.0
CALL_INTERVAL = 0.3  # 每次调用后最小间隔秒数

_DB_CFG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 3306)),
    "user":     os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "binance-data"),
    "charset":  "utf8mb4",
}

ALT99 = [
    "BTC/USDT","ETH/USDT","SOL/USDT","XRP/USDT","BNB/USDT",
    "DOGE/USDT","ADA/USDT","AVAX/USDT","LINK/USDT","SUI/USDT",
    "DOT/USDT","NEAR/USDT","LTC/USDT","BCH/USDT","UNI/USDT",
    "1000PEPE/USDT","HYPE/USDT","TAO/USDT","ENA/USDT","AAVE/USDT",
    "WLD/USDT","FIL/USDT","TRX/USDT","1000SHIB/USDT","FET/USDT",
    "APT/USDT","WIF/USDT","ARB/USDT","TON/USDT","HBAR/USDT",
]


# ════════════════════════════════════════════════════════════════════════════════
# 数据加载
# ════════════════════════════════════════════════════════════════════════════════

def load_symbol(sym):
    conn = pymysql.connect(**_DB_CFG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT timestamp, open_price, high_price, low_price, close_price, volume
                FROM kline_data
                WHERE symbol=%s AND timeframe='1h'
                  AND volume > 0
                ORDER BY timestamp ASC
            """, (sym,))
            rows = cur.fetchall()
        return [{"t": r[0],
                 "open":  float(r[1]), "high":  float(r[2]),
                 "low":   float(r[3]), "close": float(r[4]),
                 "vol":   float(r[5])} for r in rows]
    finally:
        conn.close()


# ════════════════════════════════════════════════════════════════════════════════
# SL/TP
# ════════════════════════════════════════════════════════════════════════════════

def sl_tp(cs, n=6):
    amp = sum(c["high"] - c["low"] for c in cs[-n:]) / n / cs[-1]["close"]
    amp = max(SL_MIN, min(SL_MAX, amp))
    return amp * SL_MULT, amp * TP_MULT


# ════════════════════════════════════════════════════════════════════════════════
# Gemini 调用
# ════════════════════════════════════════════════════════════════════════════════

_model = genai.GenerativeModel(GEMINI_MODEL)

_SAFETY_SETTINGS = {
    HarmCategory.HARM_CATEGORY_HARASSMENT:       HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_HATE_SPEECH:       HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
}

def _parse_candidate_text(resp) -> str:
    """安全读取 candidates[0] 文本，避免 response.text 快捷访问器因 finish_reason 报错。"""
    try:
        cands = resp.candidates
        if cands and cands[0].content and cands[0].content.parts:
            return cands[0].content.parts[0].text.strip().upper()
    except Exception:
        pass
    return ""

def ask_gemini(cs_window):
    """
    cs_window: list of bar dicts (最后 N 根 1h K线)
    返回: "LONG" / "SHORT" / None(失败)
    价格归一化为相对比率，避免触发 Gemini 金融安全过滤器。
    """
    ref = cs_window[0]["close"]
    if ref <= 0:
        return None

    rows_txt = "\n".join(
        f"t{i+1}: o={c['open']/ref:.5f} h={c['high']/ref:.5f} l={c['low']/ref:.5f} "
        f"c={c['close']/ref:.5f} v={c['vol']/1000:.1f}"
        for i, c in enumerate(cs_window)
    )
    last_c = cs_window[-1]["close"] / ref

    prompt = (
        f"You are analyzing a time series of {len(cs_window)} measurements. "
        f"Format: o=open h=high l=low c=close v=activity_index\n\n"
        f"{rows_txt}\n\n"
        f"Based on the numerical pattern, predict whether the 'c' value "
        f"will be higher or lower 3 steps ahead compared to the last c={last_c:.5f}.\n"
        f"Reply with exactly one word: UP or DOWN"
    )

    for attempt in range(3):
        try:
            resp = _model.generate_content(
                prompt,
                generation_config={"max_output_tokens": 16, "temperature": 0.1},
                safety_settings=_SAFETY_SETTINGS,
            )
            time.sleep(CALL_INTERVAL)
            text = _parse_candidate_text(resp)
            if "UP" in text and "DOWN" not in text: return "LONG"
            if "DOWN" in text: return "SHORT"
            return None
        except Exception as e:
            wait = RETRY_WAIT * (attempt + 1)
            time.sleep(wait)
    return None


# ════════════════════════════════════════════════════════════════════════════════
# 单标的回测
# ════════════════════════════════════════════════════════════════════════════════

def backtest_symbol(sym, cs, n_bars, n_samples, phase):
    """
    cs: full 1h candle list
    n_bars: how many bars to feed Gemini
    n_samples: how many random time points to sample
    phase: "train" | "test" | "all"
    """
    total = len(cs)
    split = int(total * TRAIN_RATIO)

    if phase == "train":
        valid_range = range(n_bars + 1, split - HOLD_BARS)
    elif phase == "test":
        valid_range = range(max(n_bars + 1, split), total - HOLD_BARS)
    else:
        valid_range = range(n_bars + 1, total - HOLD_BARS)

    valid_range = list(valid_range)
    if len(valid_range) < 5:
        return {"n": 0, "win": 0, "pnl": [], "skip": 0}

    sample_idx = random.sample(valid_range, min(n_samples, len(valid_range)))
    sample_idx.sort()

    results = {"n": 0, "win": 0, "pnl": [], "skip": 0}

    for i in sample_idx:
        window = cs[i - n_bars: i + 1]
        sig = ask_gemini(window)

        if sig is None or sig == "NEUTRAL":
            results["skip"] += 1
            continue

        entry   = cs[i]["close"]
        sl_a, tp_a = sl_tp(cs[:i+1])
        sl_abs  = entry * sl_a
        tp_abs  = entry * tp_a
        outcome = None

        for j in range(1, HOLD_BARS + 1):
            if i + j >= total: break
            nxt = cs[i + j]
            if sig == "LONG":
                if nxt["low"]  <= entry - sl_abs: outcome = -sl_a; break
                if nxt["high"] >= entry + tp_abs: outcome =  tp_a; break
            else:
                if nxt["high"] >= entry + sl_abs: outcome = -sl_a; break
                if nxt["low"]  <= entry - tp_abs: outcome =  tp_a; break

        if outcome is None:
            lj = min(HOLD_BARS, total - i - 1)
            if lj > 0:
                outcome = (cs[i + lj]["close"] - entry) / entry
                if sig == "SHORT":
                    outcome = -outcome

        if outcome is None: continue
        results["n"] += 1
        results["pnl"].append(outcome)
        if outcome > 0:
            results["win"] += 1

    return results


# ════════════════════════════════════════════════════════════════════════════════
# 多标的并行
# ════════════════════════════════════════════════════════════════════════════════

def run_backtest(symbols, n_bars, n_samples, phase, log_path=None):
    print(f"\nLoading {len(symbols)} symbols...")
    data = {}
    for sym in symbols:
        cs = load_symbol(sym)
        if len(cs) > n_bars + HOLD_BARS + 10:
            data[sym] = cs
    print(f"Loaded {len(data)} symbols with sufficient data.\n")

    agg = {"n": 0, "win": 0, "pnl": [], "skip": 0}
    per_sym = {}

    task_q  = queue.Queue()
    result_q = queue.Queue()

    for sym, cs in data.items():
        task_q.put((sym, cs))

    lock = threading.Lock()
    done_count = [0]

    def worker():
        while True:
            try:
                sym, cs = task_q.get_nowait()
            except queue.Empty:
                break
            r = backtest_symbol(sym, cs, n_bars, n_samples, phase)
            result_q.put((sym, r))
            with lock:
                done_count[0] += 1
                n = r["n"]
                wr = r["win"] / n * 100 if n > 0 else 0
                ev = sum(r["pnl"]) / n * 100 if n > 0 else 0
                print(f"  [{done_count[0]:2d}/{len(data)}] {sym:20s}  "
                      f"n={n:3d}  wr={wr:5.1f}%  ev={ev:+.2f}%  skip={r['skip']}")
            task_q.task_done()

    threads = [threading.Thread(target=worker) for _ in range(min(MAX_WORKERS, len(data)))]
    for t in threads: t.start()
    for t in threads: t.join()

    while not result_q.empty():
        sym, r = result_q.get()
        per_sym[sym] = r
        agg["n"]    += r["n"]
        agg["win"]  += r["win"]
        agg["pnl"]  += r["pnl"]
        agg["skip"] += r["skip"]

    total_n = agg["n"]
    wr = agg["win"] / total_n * 100 if total_n > 0 else 0
    ev = sum(agg["pnl"]) / total_n * 100 if total_n > 0 else 0

    sep = "=" * 70
    print(f"\n{sep}")
    print(f"  Gemini Strategy Backtest [{datetime.now().strftime('%Y-%m-%d %H:%M')}]")
    print(f"  Model:   {GEMINI_MODEL}")
    print(f"  Bars:    {n_bars}  |  Samples/sym: {n_samples}  |  Phase: {phase}")
    print(f"  Symbols: {len(data)}  |  Total trades: {total_n}  |  Skipped: {agg['skip']}")
    print(f"  WR:      {wr:.2f}%   EV: {ev:+.3f}%/trade")
    print(sep)

    return agg, per_sym


# ════════════════════════════════════════════════════════════════════════════════
# 主入口
# ════════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gemini Strategy Backtest")
    parser.add_argument("--bars",    type=int, default=16,    help="K线窗口长度 (default=16)")
    parser.add_argument("--samples", type=int, default=50,    help="每标的采样数 (default=50)")
    parser.add_argument("--symbols", type=int, default=5,     help="标的数量 (default=5)")
    parser.add_argument("--phase",   default="train",         help="train|test|all")
    parser.add_argument("--seed",    type=int, default=42,    help="随机种子")
    parser.add_argument("--out",     default=None,            help="输出日志路径")
    args = parser.parse_args()

    random.seed(args.seed)
    syms = random.sample(ALT99, min(args.symbols, len(ALT99)))
    print(f"Selected symbols: {syms}")

    run_backtest(
        symbols=syms,
        n_bars=args.bars,
        n_samples=args.samples,
        phase=args.phase,
    )
