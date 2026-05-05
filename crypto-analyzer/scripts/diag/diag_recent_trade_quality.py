#!/usr/bin/env python3
"""
最近 N 天 paper 平仓单按子策略分组的质量诊断.

按 futures_positions.source 分组 (strategy_whale:swan / strategy_whale:rev4d /
strategy_live / strategy_bigmid / strategy_f3 / 等), 统计:
  - 总笔数 / 胜负 / 胜率
  - PnL 总和 / 平均 / 中位数 / 最大盈 / 最大亏
  - 平均持仓时长 (分钟)
  - 平仓原因分布 (notes 字段: 止损/止盈/超时...)
  - 死亡时间分布 (前 5min / 5-30min / 30min-2h / 2h+)

用法:
  cd crypto-analyzer
  python scripts/diag/diag_recent_trade_quality.py [--days 7]

只读, 不下单, 不改 DB.
"""
from __future__ import annotations

import argparse
import os
import re
import sys
import statistics
from collections import defaultdict, Counter
from pathlib import Path

import pymysql

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def load_db_cfg() -> dict:
    cfg = {"port": 3306, "user": "admin", "password": "Yintao@110",
           "database": "dimesion", "charset": "utf8mb4",
           "cursorclass": pymysql.cursors.DictCursor}
    env = os.getenv("DIMENSION_DB_HOST", "").strip()
    if env:
        cfg["host"] = env
        return cfg
    head = (ROOT / "table_schemas.txt").read_text(encoding="utf-8").splitlines()[:15]
    for line in head:
        m = re.match(r"\s*host\s*[:=]\s*([\d\.]+)", line)
        if m:
            cfg["host"] = m.group(1)
            break
    return cfg


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7)
    args = ap.parse_args()

    cfg = load_db_cfg()
    conn = pymysql.connect(**cfg)

    print(f"=== 最近 {args.days} 天 paper 平仓单质量 (UTC) ===\n")
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
              source, symbol, position_side,
              entry_price, mark_price,
              quantity, leverage, margin,
              realized_pnl, notes,
              open_time, close_time,
              TIMESTAMPDIFF(SECOND, open_time, close_time) AS hold_s
            FROM futures_positions
            WHERE status = 'closed'
              AND close_time >= UTC_TIMESTAMP() - INTERVAL {int(args.days)} DAY
              AND realized_pnl IS NOT NULL
            ORDER BY close_time DESC
            """
        )
        rows = cur.fetchall()

    print(f"窗口内总平仓单: {len(rows)}\n")

    # --- 全局汇总 ---
    total_pnl = sum(float(r["realized_pnl"] or 0) for r in rows)
    total_margin = sum(float(r["margin"] or 0) for r in rows)
    wins = [r for r in rows if float(r["realized_pnl"] or 0) > 0]
    losses = [r for r in rows if float(r["realized_pnl"] or 0) < 0]
    print(
        f"总 PnL = {total_pnl:+.2f} U   "
        f"占用保证金合计 ≈ {total_margin:.0f} U   "
        f"胜 {len(wins)}  负 {len(losses)}  平 {len(rows) - len(wins) - len(losses)}   "
        f"胜率 {len(wins)/len(rows)*100 if rows else 0:.1f}%\n"
    )

    # --- 按 source 分组 ---
    bucket: dict = defaultdict(list)
    for r in rows:
        src = (r["source"] or "unknown").strip()
        bucket[src].append(r)

    rows_for_table = []
    for src, lst in bucket.items():
        pnls = [float(r["realized_pnl"] or 0) for r in lst]
        holds = [int(r["hold_s"] or 0) for r in lst]
        win_n = sum(1 for p in pnls if p > 0)
        loss_n = sum(1 for p in pnls if p < 0)
        avg_pnl = statistics.mean(pnls) if pnls else 0
        med_pnl = statistics.median(pnls) if pnls else 0
        max_win = max(pnls) if pnls else 0
        max_loss = min(pnls) if pnls else 0
        sum_pnl = sum(pnls)
        avg_hold_min = (statistics.mean(holds) / 60) if holds else 0
        med_hold_min = (statistics.median(holds) / 60) if holds else 0

        # 死亡时间分布
        bins = Counter()
        for s in holds:
            if s < 5 * 60:
                bins["<5m"] += 1
            elif s < 30 * 60:
                bins["5-30m"] += 1
            elif s < 2 * 3600:
                bins["30m-2h"] += 1
            elif s < 12 * 3600:
                bins["2-12h"] += 1
            else:
                bins[">12h"] += 1

        # 平仓原因
        reasons = Counter((r["notes"] or "").strip() or "无" for r in lst)

        rows_for_table.append({
            "src": src, "n": len(lst),
            "win_n": win_n, "loss_n": loss_n,
            "win_rate": win_n / len(lst) * 100 if lst else 0,
            "sum_pnl": sum_pnl, "avg_pnl": avg_pnl, "med_pnl": med_pnl,
            "max_win": max_win, "max_loss": max_loss,
            "avg_hold_min": avg_hold_min, "med_hold_min": med_hold_min,
            "bins": bins, "reasons": reasons,
        })

    rows_for_table.sort(key=lambda x: x["sum_pnl"])

    print("=== 按 source 分组 (按总 PnL 升序, 最差在最上) ===\n")
    print(f"{'source':<32} {'n':>4} {'胜':>4} {'负':>4} {'胜率':>6} "
          f"{'总PnL':>9} {'均PnL':>8} {'中位':>8} {'最大赢':>9} {'最大亏':>9} "
          f"{'均持仓':>8} {'中位持仓':>10}")
    print("-" * 130)
    for x in rows_for_table:
        print(f"{x['src'][:32]:<32} "
              f"{x['n']:>4} {x['win_n']:>4} {x['loss_n']:>4} "
              f"{x['win_rate']:>5.1f}% "
              f"{x['sum_pnl']:>+9.1f} "
              f"{x['avg_pnl']:>+8.2f} "
              f"{x['med_pnl']:>+8.2f} "
              f"{x['max_win']:>+9.1f} "
              f"{x['max_loss']:>+9.1f} "
              f"{x['avg_hold_min']:>6.0f}m "
              f"{x['med_hold_min']:>8.0f}m")

    print("\n=== 死亡时间分布 (按 source) ===")
    for x in rows_for_table:
        b = x["bins"]
        print(f"  {x['src'][:32]:<32} "
              f"<5m={b.get('<5m',0):>3}  5-30m={b.get('5-30m',0):>3}  "
              f"30m-2h={b.get('30m-2h',0):>3}  2-12h={b.get('2-12h',0):>3}  "
              f">12h={b.get('>12h',0):>3}")

    print("\n=== 平仓原因 (按 source) ===")
    for x in rows_for_table:
        rs = ", ".join(f"{k}={v}" for k, v in x["reasons"].most_common())
        print(f"  {x['src'][:32]:<32} {rs}")

    # --- 极端样本: 全窗口 top5 亏损单 ---
    rows_sorted = sorted(rows, key=lambda r: float(r["realized_pnl"] or 0))
    print("\n=== 全窗口 Top 5 单笔亏损 ===")
    for r in rows_sorted[:5]:
        hold_min = int(r["hold_s"] or 0) / 60
        print(f"  {r['close_time']}  {r['symbol']:<14} {r['position_side']:<6} "
              f"src={(r['source'] or '')[:30]:<30}  "
              f"pnl={float(r['realized_pnl']):+.1f}U  "
              f"hold={hold_min:>5.0f}m  reason={(r['notes'] or '').strip()[:18]}")

    print("\n=== 全窗口 Top 5 单笔盈利 ===")
    for r in rows_sorted[-5:][::-1]:
        hold_min = int(r["hold_s"] or 0) / 60
        print(f"  {r['close_time']}  {r['symbol']:<14} {r['position_side']:<6} "
              f"src={(r['source'] or '')[:30]:<30}  "
              f"pnl={float(r['realized_pnl']):+.1f}U  "
              f"hold={hold_min:>5.0f}m  reason={(r['notes'] or '').strip()[:18]}")

    # --- 极短持仓占比 (<5min 通常是入场即被反向干掉) ---
    very_short = [r for r in rows if (r["hold_s"] or 0) < 5 * 60]
    very_short_loss = [r for r in very_short if float(r["realized_pnl"] or 0) < 0]
    if rows:
        print(f"\n=== 极短持仓 <5min 占比 ===")
        print(f"  全窗口: {len(very_short)}/{len(rows)} = {len(very_short)/len(rows)*100:.1f}%")
        print(f"  其中亏损: {len(very_short_loss)}/{len(very_short) or 1} = "
              f"{len(very_short_loss)/(len(very_short) or 1)*100:.1f}%")
        if very_short_loss:
            short_loss_pnl = sum(float(r["realized_pnl"] or 0) for r in very_short_loss)
            print(f"  极短亏损总额: {short_loss_pnl:+.1f}U")

    conn.close()


if __name__ == "__main__":
    main()
