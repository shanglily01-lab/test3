"""
守卫上线后 8 笔仓位的开仓链路检查:
  对每个仓位, 拿到 LIMIT 挂单时刻 + 开仓时刻, 看哪个在 guard_start 之前.
  如果 LIMIT 单 created_at < guard_start 但成交于之后, 说明守卫漏过 (只拦新信号不拦旧挂单成交).
只读.
"""
import sys
from datetime import datetime
import pymysql

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DB = dict(host='13.212.252.171', port=3306, user='admin', password='Yintao@110',
          db='dimesion', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

GUARD_START = datetime(2026, 4, 24, 21, 18, 0)


def main():
    conn = pymysql.connect(**DB); cur = conn.cursor()
    try:
        cur.execute(
            """SELECT p.id AS pid, p.symbol, p.position_side, p.source,
                      p.entry_price, p.open_time, p.realized_pnl, p.notes
               FROM futures_positions p
               WHERE p.source LIKE 'strategy_live:%%'
                 AND p.open_time >= %s
               ORDER BY p.open_time ASC""",
            (GUARD_START,),
        )
        positions = cur.fetchall()
        print(f"\n守卫上线分界: {GUARD_START}")
        print(f"守卫后仓位: {len(positions)}\n")

        print("=" * 110)
        print(f"{'pid':>5}  {'symbol':<14}{'side':<6}{'open_time':<19}  "
              f"{'LIMIT created_at':<20}  {'MARKET created':<20}  {'src suffix'}")
        print("=" * 110)

        for p in positions:
            # 查这笔仓位的所有开仓订单
            cur.execute(
                """SELECT id, order_id, side, order_type, status,
                          created_at, fill_time, order_source
                   FROM futures_orders
                   WHERE position_id=%s
                     AND side IN ('OPEN_LONG', 'OPEN_SHORT')
                   ORDER BY id ASC""",
                (p['pid'],),
            )
            orders = cur.fetchall()
            limit_created = None
            market_created = None
            for o in orders:
                if o['order_type'] == 'LIMIT':
                    limit_created = o['created_at']
                elif o['order_type'] == 'MARKET':
                    market_created = o['created_at']
            src = (p['source'] or '').replace('strategy_live:', '')
            lc = str(limit_created)[:19] if limit_created else '(no LIMIT)'
            mc = str(market_created)[:19] if market_created else '(no MARKET)'

            # 判断: LIMIT 先于守卫 -> 漏过
            bypass = ''
            if limit_created and limit_created < GUARD_START:
                bypass = '  << 旧挂单 漏过!'
            elif limit_created and limit_created >= GUARD_START:
                bypass = '  (新挂单 守卫应生效)'

            print(f"{p['pid']:>5}  {p['symbol']:<14}{p['position_side']:<6}"
                  f"{str(p['open_time'])[:19]:<19}  "
                  f"{lc:<20}  {mc:<20}  "
                  f"{src}{bypass}")

        # 如果都是新挂单但守卫失效 → 可能是进程没重启
        print()
        all_new = all(
            _check_all_new(cur, p['pid'], GUARD_START) for p in positions
        )
        if all_new:
            print(">>> 全部 LIMIT 挂单都在守卫之后, 守卫理应生效但没挡住 → "
                  "最可能是 strategy_live 进程未重启, 或守卫代码有 bug")
        else:
            print(">>> 有些是守卫上线前的旧挂单成交, 守卫不拦此类 (正常)")
    finally:
        conn.close()


def _check_all_new(cur, pid, guard_start):
    cur.execute(
        """SELECT MIN(created_at) AS first_order
           FROM futures_orders
           WHERE position_id=%s AND order_type='LIMIT'""",
        (pid,),
    )
    r = cur.fetchone()
    if not r or not r['first_order']:
        return True  # 无 LIMIT, 当作新单
    return r['first_order'] >= guard_start


if __name__ == '__main__':
    main()
