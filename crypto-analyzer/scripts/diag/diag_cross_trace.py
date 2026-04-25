"""
诊断 CROSS/USDT 实盘开仓溯源
排查问题: 用户在币安 U 本位永续看到 CROSSUSDT 实盘仓位, 但本地 4 个策略
日志(live/whale/bigmid/f3) 全部对 CROSS "跳过" 未下单, 也没有 TG 通知.

需要在服务器端运行 (本地连不上远程库 13.212.252.171).

输出:
1. futures_orders 中 CROSS 所有订单 (近 7 天) - 含 order_source / live_sync_status
2. futures_positions 中 CROSS 所有持仓 (open + 近 7 天 closed)
3. 重点: PENDING LIMIT 单 -> FILLED 转换 (是否旧挂单今日被填)
4. 重点: 哪个 strategy / 哪个 source / 哪个 user 下的
"""

import pymysql
import sys
from datetime import datetime

# UTF-8 输出 (Windows 终端兼容)
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


def print_rows(rows, fields):
    if not rows:
        print("  (无记录)")
        return
    for r in rows:
        parts = [f"{k}={r.get(k)}" for k in fields if k in r]
        print("  " + " | ".join(parts))


def main() -> None:
    conn = pymysql.connect(**DB_CONFIG)
    cur = conn.cursor(pymysql.cursors.DictCursor)

    section("[1] futures_orders 中 CROSS 所有订单 (近 7 天, 倒序)")
    cur.execute(
        """
        SELECT id, order_id, account_id, user_id, strategy_id,
               symbol, side, order_type, price, quantity, executed_quantity,
               status, live_sync_status, live_synced_at, live_position_id,
               order_source, entry_signal_type, signal_id,
               created_at, fill_time, canceled_at, cancellation_reason
        FROM futures_orders
        WHERE symbol LIKE '%CROSS%'
          AND created_at > NOW() - INTERVAL 7 DAY
        ORDER BY created_at DESC
        LIMIT 100
        """
    )
    orders = cur.fetchall()
    print(f"  共 {len(orders)} 条")
    for r in orders:
        print(
            f"  id={r['id']} order_id={r['order_id']} symbol={r['symbol']} "
            f"side={r['side']} type={r['order_type']} qty={r['quantity']} "
            f"price={r['price']} status={r['status']} "
            f"live_sync={r['live_sync_status']} live_pos_id={r['live_position_id']}"
        )
        print(
            f"      strategy_id={r['strategy_id']} user_id={r['user_id']} "
            f"account_id={r['account_id']} signal_id={r['signal_id']} "
            f"signal_type={r['entry_signal_type']}"
        )
        print(
            f"      source={r['order_source']!r}"
        )
        print(
            f"      created={r['created_at']} fill={r['fill_time']} "
            f"sync_at={r['live_synced_at']} cancel={r['canceled_at']}({r['cancellation_reason']})"
        )
        print()

    section("[2] futures_positions 中 CROSS (open + 近 7 天 closed)")
    cur.execute(
        """
        SELECT id, account_id, user_id, strategy_id, symbol, position_side,
               leverage, quantity, entry_price, mark_price, status, source,
               open_time, close_time, signal_id, entry_signal_type,
               live_position_id, notes, entry_reason
        FROM futures_positions
        WHERE symbol LIKE '%CROSS%'
          AND (status = 'open' OR open_time > NOW() - INTERVAL 7 DAY)
        ORDER BY id DESC
        LIMIT 50
        """
    )
    positions = cur.fetchall()
    print(f"  共 {len(positions)} 条")
    for r in positions:
        print(
            f"  id={r['id']} {r['symbol']} {r['position_side']} qty={r['quantity']} "
            f"entry={r['entry_price']} status={r['status']} source={r['source']!r}"
        )
        print(
            f"      strategy_id={r['strategy_id']} user_id={r['user_id']} "
            f"account_id={r['account_id']} live_pos_id={r['live_position_id']} "
            f"signal_id={r['signal_id']}"
        )
        print(
            f"      open={r['open_time']} close={r['close_time']} "
            f"entry_signal_type={r['entry_signal_type']!r}"
        )
        print(f"      entry_reason={r['entry_reason']!r}")
        print(f"      notes={r['notes']!r}")
        print()

    section("[3] 重点排查: 旧 LIMIT 挂单今日被填 (created < 今日 但 fill_time 在今日)")
    cur.execute(
        """
        SELECT id, order_id, symbol, side, order_type, price, quantity, status,
               live_sync_status, created_at, fill_time, order_source, strategy_id
        FROM futures_orders
        WHERE symbol LIKE '%CROSS%'
          AND order_type = 'LIMIT'
          AND fill_time IS NOT NULL
          AND DATE(fill_time) >= CURDATE()
          AND DATE(created_at) < CURDATE()
        ORDER BY fill_time DESC
        """
    )
    rows = cur.fetchall()
    print(f"  共 {len(rows)} 条 (旧挂单今日成交)")
    for r in rows:
        delta = r["fill_time"] - r["created_at"] if r["fill_time"] and r["created_at"] else None
        print(
            f"  id={r['id']} {r['symbol']} {r['side']} qty={r['quantity']} "
            f"price={r['price']} created={r['created_at']} fill={r['fill_time']} "
            f"挂{delta} 后成交 strategy_id={r['strategy_id']} sync={r['live_sync_status']}"
        )
        print(f"      source={r['order_source']!r}")

    section("[4] 当前 PENDING / NEW LIMIT 挂单 (CROSS, 还没成交的)")
    cur.execute(
        """
        SELECT id, order_id, symbol, side, order_type, price, quantity,
               status, live_sync_status, created_at, order_source, strategy_id
        FROM futures_orders
        WHERE symbol LIKE '%CROSS%'
          AND status IN ('PENDING', 'NEW', 'PARTIALLY_FILLED')
        ORDER BY created_at DESC
        """
    )
    rows = cur.fetchall()
    print(f"  共 {len(rows)} 条")
    for r in rows:
        print(
            f"  id={r['id']} {r['symbol']} {r['side']} type={r['order_type']} "
            f"qty={r['quantity']} price={r['price']} status={r['status']} "
            f"sync={r['live_sync_status']} created={r['created_at']}"
        )
        print(f"      source={r['order_source']!r} strategy_id={r['strategy_id']}")

    section("[5] live_futures_orders 实盘订单镜像表 (如果存在)")
    try:
        cur.execute(
            """
            SELECT * FROM live_futures_orders
            WHERE symbol LIKE '%CROSS%'
              AND created_at > NOW() - INTERVAL 7 DAY
            ORDER BY created_at DESC LIMIT 30
            """
        )
        rows = cur.fetchall()
        print(f"  共 {len(rows)} 条")
        for r in rows:
            print(f"  {r}")
    except pymysql.err.ProgrammingError as e:
        print(f"  (表不存在或字段不同: {e})")

    section("[6] live_futures_positions 实盘持仓镜像表 (如果存在)")
    try:
        cur.execute(
            """
            SELECT * FROM live_futures_positions
            WHERE symbol LIKE '%CROSS%'
            ORDER BY id DESC LIMIT 20
            """
        )
        rows = cur.fetchall()
        print(f"  共 {len(rows)} 条")
        for r in rows:
            print(f"  {r}")
    except pymysql.err.ProgrammingError as e:
        print(f"  (表不存在或字段不同: {e})")

    section("[7] 按 strategy_id 反查 strategy 名称")
    strategy_ids = {r["strategy_id"] for r in orders if r.get("strategy_id")}
    strategy_ids |= {r["strategy_id"] for r in positions if r.get("strategy_id")}
    if strategy_ids:
        ids_sql = ",".join(str(int(x)) for x in strategy_ids)
        try:
            cur.execute(f"SELECT id, name, status FROM strategies WHERE id IN ({ids_sql})")
            for r in cur.fetchall():
                print(f"  strategy_id={r['id']} name={r['name']} status={r['status']}")
        except pymysql.err.ProgrammingError:
            cur.execute(f"SHOW TABLES LIKE 'strategy%'")
            print(f"  (没有 strategies 表, 候选: {cur.fetchall()})")
    else:
        print("  (CROSS 订单/持仓没有 strategy_id)")

    conn.close()
    print("\n[done]", datetime.now())


if __name__ == "__main__":
    main()
