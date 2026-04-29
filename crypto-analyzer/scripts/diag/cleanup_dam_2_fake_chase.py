"""
清理 DAM/USDT 04-28 chase-entry "幽灵 fill" 产生的两笔假数据

REMOTE (dimesion @ 54.179.112.251):
  position pid=25263       (LONG, +783.60 假止损)
  orders   81279,81288,81289

LOCAL (binance-data @ localhost):
  position pid=25187       (LONG, +501.48 假止损)
  orders   80846,80852,80853

不动:
  REMOTE oid=81247         (dump-entry SHORT timeout, 真实策略行为)
  LOCAL  pid=25183 / oid=80701/80803/80806/80807 / 80822
                          (SHORT dump-entry trail-tp +85, 真实策略行为)
  strategy_state           (两边对应 chase 行已是 IDLE, 不影响)

用法:
  python scripts/diag/cleanup_dam_2_fake_chase.py             # dry-run, 只 SELECT
  python scripts/diag/cleanup_dam_2_fake_chase.py --execute   # 真 DELETE
"""
import sys
import argparse
import pymysql
from pathlib import Path

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

REMOTE_DB = {
    "host": "54.179.112.251", "port": 3306, "user": "admin",
    "password": "Yintao@110", "database": "dimesion", "charset": "utf8mb4",
}

# 要删的明细 — 写死 ID, 防止误伤
TARGETS = {
    "REMOTE": {
        "positions": [25263],
        "orders":    [81279, 81288, 81289],
    },
    "LOCAL": {
        "positions": [25187],
        "orders":    [80846, 80852, 80853],
    },
}


def load_local_db():
    env_path = Path(__file__).resolve().parents[2] / ".env"
    cfg = {"host": "localhost", "port": 3306, "charset": "utf8mb4"}
    with env_path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k, v = k.strip(), v.strip()
            if k == "DB_HOST": cfg["host"] = v
            elif k == "DB_PORT": cfg["port"] = int(v)
            elif k == "DB_USER": cfg["user"] = v
            elif k == "DB_PASSWORD": cfg["password"] = v
            elif k == "DB_NAME": cfg["database"] = v
    return cfg


def section(t):
    print("\n" + "=" * 90)
    print(t)
    print("=" * 90)


def select_orders(cur, oids):
    if not oids:
        return []
    ph = ",".join(["%s"] * len(oids))
    cur.execute(
        f"""
        SELECT id, symbol, side, order_type, status, quantity, price, avg_fill_price,
               realized_pnl, created_at, fill_time, position_id, order_source, notes
        FROM futures_orders WHERE id IN ({ph}) ORDER BY id
        """,
        tuple(oids),
    )
    return cur.fetchall()


def select_positions(cur, pids):
    if not pids:
        return []
    ph = ",".join(["%s"] * len(pids))
    cur.execute(
        f"""
        SELECT id, symbol, position_side, status, quantity, entry_price,
               stop_loss_price, take_profit_price, realized_pnl, source,
               open_time, close_time, notes
        FROM futures_positions WHERE id IN ({ph}) ORDER BY id
        """,
        tuple(pids),
    )
    return cur.fetchall()


def show(rows, kind):
    if not rows:
        print(f"  {kind}: 0 条 (已不存在?)")
        return
    print(f"  {kind}: {len(rows)} 条")
    for r in rows:
        if kind == "futures_orders":
            print(f"    oid={r['id']:<7} {r['side']:<14} {r['order_type']:<10} {r['status']:<10} qty={r['quantity']} price={r['price']} avg_fill={r['avg_fill_price']}")
            print(f"      pid={r['position_id']} pnl={r['realized_pnl']} src={r['order_source']!r} notes={r['notes']!r}")
            print(f"      created={r['created_at']} fill={r['fill_time']}")
        else:
            print(f"    pid={r['id']} {r['position_side']} {r['status']} entry={r['entry_price']} qty={r['quantity']} pnl={r['realized_pnl']}")
            print(f"      SL={r['stop_loss_price']} TP={r['take_profit_price']} src={r['source']!r} notes={r['notes']!r}")
            print(f"      open={r['open_time']} close={r['close_time']}")


def run(label, conn, execute):
    cur = conn.cursor(pymysql.cursors.DictCursor)
    t = TARGETS[label]
    section(f"[{label}] 即将操作的对象")
    orders = select_orders(cur, t["orders"])
    positions = select_positions(cur, t["positions"])
    show(orders, "futures_orders")
    show(positions, "futures_positions")

    if not execute:
        print(f"\n  [{label}] DRY-RUN: 不删除. 加 --execute 才真删.")
        cur.close()
        return

    # FK: futures_orders.position_id ON DELETE SET NULL → orders 不会被级联,
    # 安全顺序: 先 DELETE orders, 再 DELETE positions.
    print(f"\n  [{label}] EXECUTE: 开始删除...")
    if t["orders"]:
        ph = ",".join(["%s"] * len(t["orders"]))
        n = cur.execute(f"DELETE FROM futures_orders WHERE id IN ({ph})", tuple(t["orders"]))
        print(f"    DELETE futures_orders: {n} 行")
    if t["positions"]:
        ph = ",".join(["%s"] * len(t["positions"]))
        n = cur.execute(f"DELETE FROM futures_positions WHERE id IN ({ph})", tuple(t["positions"]))
        print(f"    DELETE futures_positions: {n} 行")
    conn.commit()

    # 验证
    print(f"  [{label}] 删除后再 SELECT 验证 (应全 0):")
    after_orders = select_orders(cur, t["orders"])
    after_positions = select_positions(cur, t["positions"])
    print(f"    futures_orders 残留: {len(after_orders)}")
    print(f"    futures_positions 残留: {len(after_positions)}")
    cur.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--execute", action="store_true", help="真删 (默认 dry-run)")
    args = ap.parse_args()

    section(f"模式: {'EXECUTE (真删)' if args.execute else 'DRY-RUN (只 SELECT)'}")

    rconn = pymysql.connect(**REMOTE_DB)
    try:
        run("REMOTE", rconn, args.execute)
    finally:
        rconn.close()

    try:
        lconn = pymysql.connect(**load_local_db())
        try:
            run("LOCAL", lconn, args.execute)
        finally:
            lconn.close()
    except Exception as e:
        print(f"\n  LOCAL 连接失败: {e!r}")


if __name__ == "__main__":
    main()
