"""
诊断 BSB/USDT 实盘 "开仓 2 分钟就平仓" 事件
排查目标:
1. 用户感知 45M 入场保护期被绕过
   实际 ENTRY_GRACE_MIN 只屏蔽 early-sl + breakeven-sl
   不屏蔽: 硬 SL / trail-tp / 风控熔断 / 手动平仓 / binance 端 SL/TP 单
2. 找出 BSB 这单到底走的哪条出场路径

输出:
1. futures_orders BSB 全部订单 (近 3 天) - 关注 entry_signal_type 与平仓 reason
2. futures_positions BSB 持仓记录 - open_time vs close_time, notes 含出场原因
3. trades 平仓单 cancellation_reason / notes / pnl 关联到 entry
"""

import pymysql
import sys
from datetime import datetime

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

DB_CONFIG = {
    "host": "54.179.112.251",
    "port": 3306,
    "user": "admin",
    "password": "Yintao@110",
    "database": "dimesion",
    "charset": "utf8mb4",
}


def section(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def main() -> None:
    conn = pymysql.connect(**DB_CONFIG)
    cur = conn.cursor(pymysql.cursors.DictCursor)

    section("[1] futures_positions BSB (近 3 天)")
    cur.execute(
        """
        SELECT id, account_id, user_id, strategy_id, symbol, position_side,
               leverage, quantity, entry_price, mark_price, status, source,
               open_time, close_time, signal_id, entry_signal_type,
               live_position_id, notes, entry_reason,
               TIMESTAMPDIFF(SECOND, open_time, close_time) AS hold_sec
        FROM futures_positions
        WHERE symbol LIKE '%BSB%'
          AND (status = 'open' OR open_time > NOW() - INTERVAL 3 DAY)
        ORDER BY id DESC
        LIMIT 30
        """
    )
    positions = cur.fetchall()
    print(f"  共 {len(positions)} 条")
    for r in positions:
        hold_min = (r["hold_sec"] / 60) if r["hold_sec"] else None
        print(
            f"  pid={r['id']} {r['symbol']} {r['position_side']} qty={r['quantity']} "
            f"entry={r['entry_price']} status={r['status']} source={r['source']!r}"
        )
        print(
            f"      strategy_id={r['strategy_id']} user_id={r['user_id']} "
            f"account_id={r['account_id']} live_pos_id={r['live_position_id']}"
        )
        print(
            f"      open={r['open_time']} close={r['close_time']} 持仓={hold_min}min "
            f"entry_signal_type={r['entry_signal_type']!r}"
        )
        print(f"      entry_reason={r['entry_reason']!r}")
        print(f"      notes={r['notes']!r}")
        print()

    section("[2] futures_orders BSB 全部订单 (近 3 天, 倒序)")
    cur.execute(
        """
        SELECT id, order_id, account_id, user_id, strategy_id,
               symbol, side, order_type, price, quantity, executed_quantity,
               status, live_sync_status, live_synced_at, live_position_id,
               order_source, entry_signal_type, signal_id,
               created_at, fill_time, canceled_at, cancellation_reason
        FROM futures_orders
        WHERE symbol LIKE '%BSB%'
          AND created_at > NOW() - INTERVAL 3 DAY
        ORDER BY created_at DESC
        LIMIT 100
        """
    )
    orders = cur.fetchall()
    print(f"  共 {len(orders)} 条")
    for r in orders:
        print(
            f"  oid={r['id']} order_id={r['order_id']} {r['symbol']} "
            f"{r['side']} type={r['order_type']} qty={r['quantity']} "
            f"price={r['price']} status={r['status']} "
            f"live_sync={r['live_sync_status']} live_pos_id={r['live_position_id']}"
        )
        print(
            f"      strategy_id={r['strategy_id']} signal_id={r['signal_id']} "
            f"signal_type={r['entry_signal_type']!r} source={r['order_source']!r}"
        )
        print(
            f"      created={r['created_at']} fill={r['fill_time']} "
            f"sync_at={r['live_synced_at']} cancel={r['canceled_at']}({r['cancellation_reason']})"
        )
        print()

    section("[3] trades BSB 平仓详情 (含 pnl + reason)")
    try:
        cur.execute(
            """
            SELECT id, position_id, symbol, side, quantity, entry_price, exit_price,
                   pnl, pnl_pct, fee, close_reason, opened_at, closed_at,
                   TIMESTAMPDIFF(SECOND, opened_at, closed_at) AS hold_sec
            FROM trades
            WHERE symbol LIKE '%BSB%'
              AND closed_at > NOW() - INTERVAL 3 DAY
            ORDER BY closed_at DESC
            LIMIT 30
            """
        )
        trades = cur.fetchall()
        print(f"  共 {len(trades)} 条")
        for r in trades:
            hold_min = (r["hold_sec"] / 60) if r["hold_sec"] else None
            print(
                f"  tid={r['id']} pid={r['position_id']} {r['symbol']} {r['side']} "
                f"qty={r['quantity']} entry={r['entry_price']} exit={r['exit_price']}"
            )
            print(
                f"      pnl={r['pnl']} pnl_pct={r['pnl_pct']} fee={r['fee']} "
                f"close_reason={r['close_reason']!r}"
            )
            print(
                f"      opened={r['opened_at']} closed={r['closed_at']} "
                f"持仓={hold_min}min"
            )
            print()
    except Exception as e:
        print(f"  查询 trades 表失败 (可能字段名不同): {e}")

    section("[4] 同 live_position_id 的 entry + close 配对 (溯源 close_reason)")
    cur.execute(
        """
        SELECT live_position_id, COUNT(*) AS cnt,
               GROUP_CONCAT(CONCAT(side, ':', order_type, ':', status, ':',
                            COALESCE(cancellation_reason, ''), ':',
                            DATE_FORMAT(created_at, '%H:%i:%s'))
                            ORDER BY id SEPARATOR ' | ') AS chain
        FROM futures_orders
        WHERE symbol LIKE '%BSB%'
          AND created_at > NOW() - INTERVAL 3 DAY
          AND live_position_id IS NOT NULL
        GROUP BY live_position_id
        ORDER BY MAX(id) DESC
        """
    )
    pairs = cur.fetchall()
    for r in pairs:
        print(f"  live_pid={r['live_position_id']} 共{r['cnt']}单")
        print(f"      链: {r['chain']}")
        print()

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
