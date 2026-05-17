#!/usr/bin/env python3
"""
黑盒红黑天鹅 hit rate 回算 CLI 入口 (薄壳).

逻辑全在 app/services/blackbox_swan_worker.py: run_hit_rate_check().
该入口仅方便手动跑、调试不同 lookback / threshold 组合.

用法:
  cd crypto-analyzer

  # 默认: 回算 7 天前的 STRONG verdict, 阈值 10% (black 跌 >=10% / red 涨 >=10% 算 hit)
  python scripts/diag/diag_blackbox_hit_rate.py

  # 不同 lookback (天数) + 阈值 (%)
  python scripts/diag/diag_blackbox_hit_rate.py --lookback-days 3 --threshold 5

输出:
  写入本地 binance-data.blackbox_swan_hit_rate 表
  日志打印每个 main_category 的累计 hit rate
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.stderr.encoding != "utf-8":
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from app.services.blackbox_swan_worker import run_hit_rate_check


def main():
    parser = argparse.ArgumentParser(
        description="黑盒红黑天鹅 hit rate 回算 (本地, 不调度)"
    )
    parser.add_argument(
        "--lookback-days", type=int, default=7,
        help="回算 N 天前的 STRONG verdict (默认 7)",
    )
    parser.add_argument(
        "--threshold", type=float, default=10.0,
        help="hit 门槛百分比 (默认 10.0, 即 black >= -10%% 或 red >= +10%% 算 hit)",
    )
    args = parser.parse_args()

    n = run_hit_rate_check(
        triggered_by="manual",
        lookback_days=args.lookback_days,
        hit_threshold_pct=args.threshold,
    )
    print(f"inserted={n}")


if __name__ == "__main__":
    main()
