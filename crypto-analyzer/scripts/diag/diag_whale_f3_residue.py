"""查 strategy_whale:* / strategy_f3:* 在 DB 残留的 PENDING / open 仓位."""
import sys, pymysql

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

REMOTE_DB = {
    "host": "54.179.112.251", "port": 3306, "user": "admin",
    "password": "Yintao@110", "database": "dimesion", "charset": "utf8mb4",
}


def main():
    conn = pymysql.connect(**REMOTE_DB)
    cur = conn.cursor(pymysql.cursors.DictCursor)

    print("=" * 60)
    print("strategy_whale:* + strategy_f3:* 残留扫描")
    print("=" * 60)

    # 1. PENDING 限价单
    cur.execute(
        """SELECT id, symbol, side, order_source, status, created_at, fill_time
           FROM futures_orders
           WHERE account_id=2 AND status IN ('PENDING','FILLING')
             AND (order_source LIKE 'strategy_whale:%%'
               OR order_source LIKE 'strategy_f3:%%')
           ORDER BY created_at DESC LIMIT 50"""
    )
    pending = cur.fetchall()
    print(f"\n[1] PENDING/FILLING 单: {len(pending)} 笔")
    for o in pending:
        print(f"  id={o['id']} {o['symbol']:<14} {o['side']:<12} src={o['order_source']!r:<32} created={o['created_at']}")

    # 2. 还在 open 的持仓
    cur.execute(
        """SELECT id, symbol, position_side, source, status, open_time, timeout_at
           FROM futures_positions
           WHERE account_id=2 AND status='open'
             AND (source LIKE 'strategy_whale:%%'
               OR source LIKE 'strategy_f3:%%')
           ORDER BY open_time DESC LIMIT 50"""
    )
    open_pos = cur.fetchall()
    print(f"\n[2] open 持仓: {len(open_pos)} 笔")
    for p in open_pos:
        print(f"  pid={p['id']} {p['symbol']:<14} {p['position_side']:<5} src={p['source']!r:<32} open={p['open_time']} timeout_at={p['timeout_at']}")

    # 3. strategy_state 表里 active 状态
    cur.execute(
        """SELECT strategy, stype, symbol, state, pid, order_id, done_time
           FROM strategy_state
           WHERE strategy IN ('whale','f3')
             AND state IN ('PENDING','LONG','SHORT','FILLING','SIG_WAIT')
           ORDER BY strategy, stype, symbol LIMIT 100"""
    )
    states = cur.fetchall()
    print(f"\n[3] strategy_state active 行: {len(states)} 行")
    for s in states:
        print(f"  {s['strategy']}:{s['stype']:<14} {s['symbol']:<14} state={s['state']:<8} pid={s['pid']} oid={s['order_id']}")

    print("\n=" * 60)
    print(f"汇总: {len(pending)} PENDING + {len(open_pos)} open + {len(states)} state-active")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
