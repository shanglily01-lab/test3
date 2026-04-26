"""
诊断 BSB dump-entry 入场保护期失效:
对比 strategy_state.entry_time vs futures_positions.open_time vs 实际 fill_time
验证 hypothesis: LIMIT 挂单瞬间就写 entry_time, 53min 后 fill 时 grace 已经"用完"
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

    section("[1] strategy_state BSB 当前所有 stype 行 (live)")
    cur.execute(
        """
        SELECT id, strategy, symbol, stype, state, pid, order_id,
               entry_p, entry_time, done_time, peak_pnl_pct, last_reason,
               created_at, updated_at,
               FROM_UNIXTIME(entry_time) AS entry_time_dt,
               FROM_UNIXTIME(done_time) AS done_time_dt
        FROM strategy_state
        WHERE strategy='live' AND symbol LIKE '%BSB%'
        ORDER BY id DESC
        """
    )
    for r in cur.fetchall():
        print(f"  id={r['id']} stype={r['stype']} state={r['state']} pid={r['pid']}")
        print(f"    entry_p={r['entry_p']} entry_time={r['entry_time']} ({r['entry_time_dt']})")
        print(f"    done_time={r['done_time']} ({r['done_time_dt']}) last_reason={r['last_reason']!r}")
        print(f"    updated_at={r['updated_at']}")
        print()

    section("[2] futures_positions BSB pid=25225 (本次 2 分钟平仓那笔)")
    cur.execute(
        """
        SELECT id, symbol, position_side, entry_price, quantity,
               open_time, close_time, status, source, notes,
               TIMESTAMPDIFF(SECOND, open_time, close_time) AS hold_sec
        FROM futures_positions
        WHERE id = 25225
        """
    )
    r = cur.fetchone()
    if r:
        print(f"  pid={r['id']} {r['symbol']} {r['position_side']}")
        print(f"  open_time={r['open_time']}  close_time={r['close_time']}")
        print(f"  持仓秒数={r['hold_sec']}  notes={r['notes']!r}")
        print(f"  source={r['source']!r}  status={r['status']}")

    section("[3] futures_orders pid=25225 关联订单链")
    cur.execute(
        """
        SELECT id, order_id, symbol, side, order_type, status,
               quantity, price, avg_fill_price,
               created_at, fill_time,
               TIMESTAMPDIFF(SECOND, created_at, fill_time) AS pending_sec,
               order_source, cancellation_reason
        FROM futures_orders
        WHERE symbol='BSB/USDT'
          AND created_at > '2026-04-26 03:00:00'
          AND created_at < '2026-04-26 06:00:00'
        ORDER BY id ASC
        """
    )
    for r in cur.fetchall():
        print(f"  oid={r['id']} {r['side']} {r['order_type']} status={r['status']}")
        print(f"    qty={r['quantity']} price={r['price']} avg_fill={r['avg_fill_price']}")
        print(f"    created={r['created_at']}  fill={r['fill_time']}  pending={r['pending_sec']}s")
        print(f"    source={r['order_source']!r}  cancel_reason={r['cancellation_reason']!r}")
        print()

    section("[4] 交叉验证 grace 计算: now=close_time vs entry_time(state) vs open_time(pos)")
    cur.execute(
        """
        SELECT
            ss.entry_time AS state_entry_time,
            FROM_UNIXTIME(ss.entry_time) AS state_entry_dt,
            fp.open_time AS pos_open_time,
            fp.close_time AS pos_close_time,
            UNIX_TIMESTAMP(fp.close_time) - ss.entry_time AS grace_using_state_sec,
            UNIX_TIMESTAMP(fp.close_time) - UNIX_TIMESTAMP(fp.open_time) AS actual_hold_sec
        FROM futures_positions fp
        LEFT JOIN strategy_state ss
          ON ss.symbol = fp.symbol AND ss.strategy='live' AND ss.stype='dump'
        WHERE fp.id = 25225
        """
    )
    r = cur.fetchone()
    if r:
        gs = r['grace_using_state_sec']
        ah = r['actual_hold_sec']
        print(f"  state.entry_time      = {r['state_entry_time']} ({r['state_entry_dt']})")
        print(f"  position.open_time    = {r['pos_open_time']}")
        print(f"  position.close_time   = {r['pos_close_time']}")
        print(f"  按 state 计算的 'grace 已用秒数' = {gs}s ({gs/60 if gs else 0:.1f}min)")
        print(f"  实际持仓秒数 (close - open)     = {ah}s ({ah/60 if ah else 0:.1f}min)")
        print(f"  ENTRY_GRACE_MIN = 45  -> {45*60}s")
        if gs and gs > 45*60:
            print(f"  >> 按 state 计算 grace 已超期 (in_grace=False), early-sl 解锁")
            print(f"  >> 但实际持仓只 {ah/60:.1f}min, 这是 grace 失效 bug")
        else:
            print(f"  >> grace 还在有效期内, early-sl 不应触发, 走的可能是别的路径")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
