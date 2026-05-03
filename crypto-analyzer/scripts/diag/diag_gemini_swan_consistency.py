#!/usr/bin/env python3
"""
连续跑 N 轮 diag_gemini_swan_now, 聚合每个 symbol 的判定一致性.

目的: 单次调 Gemini 是 stochastic 的, 同 prompt 不同次结果有差异.
      多轮跑 (universe 也会因为 1 分钟更新而微变化) 能筛出 **稳定信号** —
      在多数轮里被反复标为同一类别的 symbol, 而非偶发拍脑袋.

逻辑:
  for round in 1..N:
      抓 universe (gainers/losers/funding_pos/funding_neg) -> 喂 Gemini
      把每轮 verdicts 累加到 symbol 维度

  聚合输出:
      - 每个 symbol 出现在多少轮 universe 里 (universe_count)
      - 每个 symbol 被标 black/red/skip 的轮次数
      - 平均 confidence (仅在被标 black/red 时累计)
      - 一致性等级:
          * STRONG    : >= ceil(N*0.7) 轮同类别 (例如 3/3, 5/4)
          * MODERATE  : 多数 (>= ceil(N/2)) 轮同类别
          * WEAK      : 单轮出现, 视为噪声

依赖: 与 diag_gemini_swan_now.py 一致.
用法:
  cd crypto-analyzer
  python scripts/diag/diag_gemini_swan_consistency.py [--rounds 3] [--interval 60]
"""
from __future__ import annotations

import argparse
import json
import math
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
DIAG_DIR = Path(__file__).resolve().parent
if str(DIAG_DIR) not in sys.path:
    sys.path.insert(0, str(DIAG_DIR))

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.stderr.encoding != "utf-8":
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import pymysql

import diag_gemini_swan_now as swan  # noqa: E402


def run_round(round_idx: int) -> dict | None:
    """单轮: 抓 universe + 调 Gemini, 返回 {symbol: verdict_dict, _universe: {...}}."""
    cfg = swan.load_remote_db_cfg()
    conn = pymysql.connect(**cfg)
    try:
        with conn.cursor() as cur:
            gainers, losers = swan.fetch_movers_24h(cur, swan.TOP_MOVER)
            fund_pos, fund_neg = swan.fetch_extreme_funding(cur, swan.TOP_FUNDING)
    finally:
        conn.close()
    universe = swan.merge_universe(gainers, losers, fund_pos, fund_neg)
    print(f"[round {round_idx}] universe_size={len(universe)} "
          f"(g={len(gainers)} l={len(losers)} fp={len(fund_pos)} fn={len(fund_neg)})",
          file=sys.stderr)
    if not universe:
        return None

    out = swan.call_gemini(universe)
    if not out:
        return None
    verdicts = out.get("verdicts") or []
    by_symbol = {}
    for v in verdicts:
        sym = str(v.get("symbol", "")).upper().strip()
        if sym:
            by_symbol[sym] = v
    return {
        "verdicts": by_symbol,
        "summary_zh": out.get("summary_zh", ""),
        "universe": {s: v for s, v in universe.items()},
    }


def aggregate(rounds: list[dict], n_rounds: int) -> dict:
    """聚合每个 symbol 的多轮判定."""
    universe_count = defaultdict(int)        # 在多少轮 universe 里出现
    cat_count = defaultdict(lambda: defaultdict(int))  # symbol -> {black/red/skip: n}
    conf_sum = defaultdict(lambda: defaultdict(float))  # symbol -> {cat: sum_conf}
    last_verdict = {}                         # symbol -> 最后一次详细 verdict (用于呈现)
    last_signal = defaultdict(list)           # symbol -> [data_signal,...]
    last_catalyst = defaultdict(list)         # symbol -> [catalyst,...]
    triggers_seen = defaultdict(set)          # symbol -> {"24h_gainer", ...}
    summaries = []

    for r in rounds:
        if not r:
            continue
        if r.get("summary_zh"):
            summaries.append(r["summary_zh"])
        for sym, info in r["universe"].items():
            universe_count[sym] += 1
            for t in info.get("triggers", []):
                triggers_seen[sym].add(t)
        for sym, v in r["verdicts"].items():
            cat = str(v.get("category", "")).lower().strip() or "skip"
            if cat not in ("black_swan", "red_swan", "skip"):
                cat = "skip"
            cat_count[sym][cat] += 1
            try:
                c = float(v.get("confidence") or 0.0)
            except (TypeError, ValueError):
                c = 0.0
            conf_sum[sym][cat] += c
            if v.get("catalyst"):
                last_catalyst[sym].append(str(v["catalyst"]))
            if v.get("data_signal"):
                last_signal[sym].append(str(v["data_signal"]))
            last_verdict[sym] = v

    strong_threshold = max(2, math.ceil(n_rounds * 0.7))
    moderate_threshold = max(2, math.ceil(n_rounds / 2))

    aggregated = []
    for sym, counts in cat_count.items():
        # 主类别 = 出现次数最多 (skip 不优先, 同票时优先 black/red)
        order = ["black_swan", "red_swan", "skip"]
        main_cat = max(order, key=lambda c: (counts.get(c, 0),
                                             -order.index(c) if counts.get(c, 0) > 0 else -100))
        main_n = counts.get(main_cat, 0)
        avg_conf = (conf_sum[sym].get(main_cat, 0.0) / main_n) if main_n else 0.0
        if main_cat == "skip":
            level = "SKIP"
        elif main_n >= strong_threshold:
            level = "STRONG"
        elif main_n >= moderate_threshold:
            level = "MODERATE"
        else:
            level = "WEAK"
        aggregated.append({
            "symbol": sym,
            "main_category": main_cat,
            "consistency_level": level,
            "rounds_total": n_rounds,
            "universe_appearances": universe_count[sym],
            "black_count": counts.get("black_swan", 0),
            "red_count": counts.get("red_swan", 0),
            "skip_count": counts.get("skip", 0),
            "avg_confidence": round(avg_conf, 3),
            "triggers": sorted(triggers_seen[sym]),
            "catalyst_samples": last_catalyst[sym][-3:],
            "data_signal_samples": last_signal[sym][-3:],
        })

    aggregated.sort(key=lambda x: (
        {"STRONG": 0, "MODERATE": 1, "WEAK": 2, "SKIP": 3}[x["consistency_level"]],
        -x["avg_confidence"],
        -max(x["black_count"], x["red_count"]),
    ))
    return {
        "rounds": n_rounds,
        "strong_threshold_rounds": strong_threshold,
        "moderate_threshold_rounds": moderate_threshold,
        "summaries_per_round": summaries,
        "aggregated": aggregated,
    }


def print_summary(agg: dict):
    print("\n--- consistency table ---", file=sys.stderr)
    cols = "{:<14} {:<10} {:<8} {:>3}/{:<3} {:>5} {:>5} {:>5} {:>6}".format(
        "symbol", "category", "level", "uni", "all", "black", "red", "skip", "avgC")
    print(cols, file=sys.stderr)
    for row in agg["aggregated"]:
        if row["consistency_level"] == "SKIP" and row["main_category"] == "skip":
            continue  # 跳过纯 skip 噪声
        print("{:<14} {:<10} {:<8} {:>3}/{:<3} {:>5} {:>5} {:>5} {:>6}".format(
            row["symbol"][:14],
            row["main_category"][:10],
            row["consistency_level"],
            row["universe_appearances"],
            row["rounds_total"],
            row["black_count"],
            row["red_count"],
            row["skip_count"],
            f"{row['avg_confidence']:.2f}",
        ), file=sys.stderr)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rounds", type=int, default=3, help="跑几轮 (默认 3)")
    parser.add_argument("--interval", type=int, default=60,
                        help="每轮间隔秒 (默认 60, 让 universe 数据微变化)")
    args = parser.parse_args()

    print(f"[consistency] rounds={args.rounds} interval={args.interval}s "
          f"model={swan.GEMINI_MODEL}", file=sys.stderr)

    rounds = []
    for i in range(args.rounds):
        print(f"\n=== round {i+1}/{args.rounds} ===", file=sys.stderr)
        t0 = time.time()
        r = run_round(i + 1)
        rounds.append(r)
        if r is None:
            print(f"[round {i+1}] failed, skipped", file=sys.stderr)
        if i < args.rounds - 1:
            elapsed = time.time() - t0
            wait = max(0, args.interval - elapsed)
            if wait > 0:
                print(f"[wait] {wait:.1f}s before next round", file=sys.stderr)
                time.sleep(wait)

    valid = [r for r in rounds if r is not None]
    if not valid:
        print("ERROR: 所有轮次都失败", file=sys.stderr)
        sys.exit(1)

    agg = aggregate(rounds, args.rounds)
    print_summary(agg)

    out = {
        "asof_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "model": swan.GEMINI_MODEL,
        "params": {
            "rounds": args.rounds,
            "interval_s": args.interval,
            "min_quote_volume_usdt": swan.MIN_QUOTE_VOLUME,
            "excluded_bases": sorted(swan.EXCLUDE_BASES | swan.STABLECOINS),
        },
        **agg,
        "raw_rounds": [
            {
                "round": i + 1,
                "summary_zh": (r["summary_zh"] if r else ""),
                "verdicts": (list(r["verdicts"].values()) if r else []),
                "universe": (list(r["universe"].values()) if r else []),
            }
            for i, r in enumerate(rounds)
        ],
    }
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = swan.OUTPUT_DIR / f"gemini_swan_consistency_{ts}.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n[done] -> {out_path}", file=sys.stderr)

    strong = [r for r in agg["aggregated"]
              if r["consistency_level"] == "STRONG" and r["main_category"] != "skip"]
    moderate = [r for r in agg["aggregated"]
                if r["consistency_level"] == "MODERATE" and r["main_category"] != "skip"]
    print(f"[summary] STRONG={len(strong)} MODERATE={len(moderate)}", file=sys.stderr)


if __name__ == "__main__":
    main()
