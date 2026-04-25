"""
对守卫上线后 8 笔仓位, 精确模拟"当时"守卫会看到什么:
  - 取 LIMIT 挂单的 created_at 作为"守卫检查时刻" (guard_ms)
  - 用 guard_ms 往前 3h 查 kline_data 15m 区间
  - 检查 kline_data 在该窗口的数据完整性
  - 用 LIMIT 的 price 作为 cur_price, 模拟算 pos_pct
  - 对比守卫规则 (pct>100 / <0 / LONG>90 / SHORT<10)

目的: 找出为什么守卫没拦住这 8 笔.
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
BAR_MS_15M = 15 * 60 * 1000
LOOKBACK_BARS = 12


def main():
    conn = pymysql.connect(**DB); cur = conn.cursor()
    try:
        # 取守卫上线后的 8 笔
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

        print(f"守卫上线: {GUARD_START}")
        print(f"模拟守卫在 LIMIT 挂单时刻会看到什么\n")
        print("=" * 120)

        for p in positions:
            # 取这笔的 LIMIT 挂单
            cur.execute(
                """SELECT id, order_id, side, order_type, status, price,
                          avg_fill_price, created_at, fill_time
                   FROM futures_orders
                   WHERE position_id=%s AND order_type='LIMIT'
                     AND side IN ('OPEN_LONG','OPEN_SHORT')
                   ORDER BY id ASC LIMIT 1""",
                (p['pid'],),
            )
            lo = cur.fetchone()
            if not lo:
                print(f"#{p['pid']} {p['symbol']} - 无 LIMIT 挂单")
                continue

            # 获取 UTC 毫秒 (用 TIMESTAMPDIFF 从 MySQL 直接算, 避免时区)
            cur.execute(
                "SELECT UNIX_TIMESTAMP(%s) * 1000 AS ms", (lo['created_at'],)
            )
            guard_ms = int(cur.fetchone()['ms'])
            start_ms = guard_ms - LOOKBACK_BARS * BAR_MS_15M

            # 查 kline_data 15m 在该窗口
            cur.execute(
                """SELECT COUNT(*) AS n,
                          MAX(high_price) AS h, MIN(low_price) AS l,
                          MIN(open_time) AS earliest, MAX(open_time) AS latest
                   FROM kline_data
                   WHERE symbol=%s AND timeframe='15m'
                     AND open_time >= %s AND open_time < %s""",
                (p['symbol'], start_ms, guard_ms),
            )
            r = cur.fetchone()

            # guard 用的是 cur_price (当时的 get_price) ≈ LIMIT price
            # 因为 LIMIT = cur_price * (1 - 0.03) 或 (1 + 0.03)
            lp = float(lo['price']) if lo['price'] else 0
            side = p['position_side']
            # 近似还原 guard 看到的 cur_price
            if side == 'LONG':
                guard_cur_price = lp / (1 - 0.03)
            else:
                guard_cur_price = lp / (1 + 0.03)

            print(f"#{p['pid']} {p['symbol']:<14}{side:<6}"
                  f"  LIMIT created={lo['created_at']}  "
                  f"limit_price={lp}")

            if r['n'] == 0 or r['h'] is None:
                print(f"    kline_data[{p['symbol']}][15m] 在 [{start_ms} ~ {guard_ms}) "
                      f"区间为空 -> 守卫返回 None -> 放行 (这就是漏的原因!)")
                continue

            hi = float(r['h']); lo_v = float(r['l'])
            pct_limit = (lp - lo_v) / (hi - lo_v) * 100 if hi > lo_v else 50
            pct_guess_cur = (guard_cur_price - lo_v) / (hi - lo_v) * 100 if hi > lo_v else 50
            print(f"    kline: {r['n']} 根  range=[{lo_v}, {hi}]  "
                  f"earliest_open={r['earliest']}  latest_open={r['latest']}")
            print(f"    守卫 cur_price 估计={guard_cur_price:.6f}  "
                  f"pct={pct_guess_cur:+.0f}%")
            print(f"    limit_price={lp}  pct_of_limit={pct_limit:+.0f}%")

            # 守卫判定 (用 cur_price 估计)
            if pct_guess_cur > 100:
                verdict = "理应拒绝 (破顶)"
            elif pct_guess_cur < 0:
                verdict = "理应拒绝 (破底)"
            elif side == 'LONG' and pct_guess_cur > 90:
                verdict = "理应拒绝 (追高)"
            elif side == 'SHORT' and pct_guess_cur < 10:
                verdict = "理应拒绝 (踩底)"
            else:
                verdict = "守卫会放行"
            print(f"    → {verdict}")
            print()

        # 另外检查: ZBT kline 数据整体覆盖情况
        print("\n" + "=" * 120)
        print("附加检查: 守卫上线后出现的 symbol 在 kline_data 15m 的完整性")
        print("=" * 120)
        for sym in ['TRADOOR/USDT', 'ZBT/USDT', 'PIEVERSE/USDT', 'KAT/USDT',
                     'OPN/USDT', 'APE/USDT', 'MAGMA/USDT']:
            cur.execute(
                """SELECT COUNT(*) AS n, MIN(open_time) AS first,
                          MAX(open_time) AS last
                   FROM kline_data WHERE symbol=%s AND timeframe='15m'""",
                (sym,),
            )
            r = cur.fetchone()
            if r['n'] == 0:
                print(f"  {sym:<14} 无 15m kline 数据!")
                continue
            cur.execute(
                "SELECT FROM_UNIXTIME(%s/1000) AS t",
                (int(r['first']),),
            )
            first_dt = cur.fetchone()['t']
            cur.execute(
                "SELECT FROM_UNIXTIME(%s/1000) AS t",
                (int(r['last']),),
            )
            last_dt = cur.fetchone()['t']
            print(f"  {sym:<14} 15m 总数 {r['n']:>5}  "
                  f"earliest {first_dt}  latest {last_dt}")
    finally:
        conn.close()


if __name__ == '__main__':
    main()
