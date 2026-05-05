#!/usr/bin/env python3
"""
查 5-04 08:12 ~ 08:13 TAG/USDT SWAN SHORT 单的完整数据库记录,
排查为什么持仓显示成 "0M".

定位线索:
  - symbol: TAG/USDT
  - sub_strategy: swan (strategy_whale:swan)
  - 方向: SHORT
  - 入场价: 0.001749, 离场价: 0.001782
  - PnL: -49.31 U, -9.85%
  - 时间: 2026-05-04 08:12 ~ 08:13 (CST? UTC?)

输出:
  - 实际 quantity / 名义本金 / 保证金
  - 找出是否真的下了一张零量单 (quantity ≈ 0) 还是显示层把数字格式化丢了
"""
from __future__ import annotations

import os
import re
import sys
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
    cfg = load_db_cfg()
    conn = pymysql.connect(**cfg)

    # 时间窗: 用户给的是 5-04 08:12 ~ 08:13 (CST = UTC+8), 远程库是 UTC, 即 UTC 00:12 ~ 00:13
    # 但保险起见整个 5-04 当天 (UTC) 全拉
    print("=== futures_orders: TAG/USDT 5-04 (UTC) ===")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, account_id, symbol, side, order_type, status,
                   leverage, price, quantity, executed_quantity,
                   margin, total_value, executed_value,
                   avg_fill_price, fill_time,
                   stop_loss_price, take_profit_price,
                   entry_signal_type, order_source, notes,
                   realized_pnl, pnl_pct,
                   position_id, created_at, updated_at,
                   live_sync_status
            FROM futures_orders
            WHERE symbol = 'TAG/USDT'
              AND created_at >= '2026-05-04 00:00:00'
              AND created_at <  '2026-05-05 00:00:00'
            ORDER BY id
            """
        )
        rows = cur.fetchall()

    print(f"命中 {len(rows)} 笔\n")
    for r in rows:
        print(f"id={r['id']}  acc={r['account_id']}  side={r['side']:<14}  type={r['order_type']:<14}  status={r['status']:<10}")
        print(f"  entry_signal_type = {r['entry_signal_type']}")
        print(f"  order_source      = {r['order_source']}")
        print(f"  notes             = {r['notes']}")
        print(f"  leverage          = {r['leverage']}")
        print(f"  price (limit)     = {r['price']}")
        print(f"  quantity (commit) = {r['quantity']}")
        print(f"  executed_quantity = {r['executed_quantity']}")
        print(f"  avg_fill_price    = {r['avg_fill_price']}")
        print(f"  margin            = {r['margin']}")
        print(f"  total_value       = {r['total_value']}")
        print(f"  executed_value    = {r['executed_value']}")
        print(f"  stop_loss_price   = {r['stop_loss_price']}")
        print(f"  take_profit_price = {r['take_profit_price']}")
        print(f"  realized_pnl      = {r['realized_pnl']}")
        print(f"  pnl_pct           = {r['pnl_pct']}")
        print(f"  position_id       = {r['position_id']}")
        print(f"  created_at        = {r['created_at']}")
        print(f"  fill_time         = {r['fill_time']}")
        print(f"  live_sync_status  = {r['live_sync_status']}")

        # 自检: quantity * price 应该 ≈ total_value (名义本金)
        try:
            q = float(r["executed_quantity"] or r["quantity"] or 0)
            p = float(r["avg_fill_price"] or r["price"] or 0)
            calc_notional = q * p
            db_notional = float(r["total_value"] or 0)
            db_margin = float(r["margin"] or 0)
            lev = float(r["leverage"] or 1)
            print(f"  >> CHECK: q * p = {calc_notional:.4f} U")
            print(f"  >> CHECK: total_value(notional) = {db_notional:.4f} U")
            print(f"  >> CHECK: margin = {db_margin:.4f} U  | notional/margin = {(db_notional/db_margin) if db_margin else 0:.2f} (应≈lev={lev})")
        except Exception as e:
            print(f"  >> CHECK skipped: {e}")
        print()

    # 找到对应 position 看 quantity / margin
    pids = sorted({r["position_id"] for r in rows if r.get("position_id")})
    if pids:
        print("=== futures_positions 对应 position ===")
        ph = ",".join(["%s"] * len(pids))
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, account_id, symbol, side, status,
                       leverage, quantity, entry_price, mark_price,
                       margin, position_value,
                       realized_pnl, unrealized_pnl,
                       liquidation_price, opened_at, closed_at, close_reason
                FROM futures_positions
                WHERE id IN ({ph})
                """,
                pids,
            )
            for r in cur.fetchall():
                for k, v in r.items():
                    print(f"  {k} = {v}")
                print()

    # strategy_state 里 swan 状态
    print("=== strategy_state strategy=whale stype=swan symbol=TAG/USDT ===")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT strategy, stype, symbol, state, pid, order_id, entry_p,
                   side, entry_time, done_time, peak_pnl_pct, updated_at
            FROM strategy_state
            WHERE strategy = 'whale'
              AND stype    = 'swan'
              AND symbol   = 'TAG/USDT'
            """
        )
        for r in cur.fetchall():
            for k, v in r.items():
                print(f"  {k} = {v}")
            print()

    conn.close()


if __name__ == "__main__":
    main()
