"""
当前所有 strategy_* 在持仓位画像:
  - 持仓时长 / 浮盈浮亏 / 距入场价多少
  - 入场时刻的 3h 位置百分位 (用 LIMIT created_at 锚点) — 看守卫为什么放行
  - 入场时刻的 24h 涨跌
只读.
"""
import sys
from datetime import datetime
import pymysql

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DB = dict(host='13.212.252.171', port=3306, user='admin', password='Yintao@110',
          db='dimesion', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

BAR_MS_15M = 15 * 60 * 1000


def main():
    conn = pymysql.connect(**DB); cur = conn.cursor()
    try:
        cur.execute(
            """SELECT id, symbol, position_side, source, entry_price,
                      mark_price, unrealized_pnl, unrealized_pnl_pct,
                      stop_loss_price, take_profit_price,
                      max_profit_pct, max_profit_price,
                      open_time, max_hold_minutes, timeout_at,
                      TIMESTAMPDIFF(MINUTE, open_time, NOW()) AS hold_min
               FROM futures_positions
               WHERE status='open'
               ORDER BY open_time ASC"""
        )
        positions = cur.fetchall()
        print(f"\n当前在持仓位: {len(positions)} 个\n")
        if not positions:
            return

        for p in positions:
            print("=" * 100)
            print(f"#{p['id']} {p['symbol']:<14} {p['position_side']:<6} {p['source']}")
            print(f"  entry={p['entry_price']}  mark={p['mark_price']}  "
                  f"unrealized={p['unrealized_pnl']} ({float(p['unrealized_pnl_pct'] or 0):+.2f}% margin)")
            print(f"  SL={p['stop_loss_price']}  TP={p['take_profit_price']}  "
                  f"max_profit_pct={p['max_profit_pct']}")
            print(f"  open={p['open_time']}  hold={p['hold_min']}min  "
                  f"max_hold={p['max_hold_minutes']}min  timeout_at={p['timeout_at']}")

            # 取 LIMIT 挂单时刻 (守卫检查时刻)
            cur.execute(
                """SELECT id, order_id, order_type, status, price, avg_fill_price,
                          created_at, fill_time
                   FROM futures_orders
                   WHERE position_id=%s AND side IN ('OPEN_LONG','OPEN_SHORT')
                   ORDER BY id ASC""",
                (p['id'],),
            )
            orders = cur.fetchall()
            limit_o = next((o for o in orders if o['order_type'] == 'LIMIT'), None)
            market_o = next((o for o in orders if o['order_type'] == 'MARKET'), None)
            if limit_o:
                print(f"  LIMIT created={limit_o['created_at']}  price={limit_o['price']}  "
                      f"status={limit_o['status']}  filled={limit_o['fill_time']}")
                # 守卫时刻的 3h 位置
                cur.execute(
                    "SELECT UNIX_TIMESTAMP(%s) * 1000 AS ms",
                    (limit_o['created_at'],),
                )
                guard_ms = int(cur.fetchone()['ms'])
                start_ms = guard_ms - 12 * BAR_MS_15M
                cur.execute(
                    """SELECT MAX(high_price) AS h, MIN(low_price) AS l, COUNT(*) AS n
                       FROM kline_data WHERE symbol=%s AND timeframe='15m'
                         AND open_time >= %s AND open_time < %s""",
                    (p['symbol'], start_ms, guard_ms),
                )
                r = cur.fetchone()
                if r and r['h'] is not None:
                    hi = float(r['h']); lo = float(r['l'])
                    lp = float(limit_o['price'] or 0)
                    side = p['position_side']
                    cur_p_at_check = lp / (1 - 0.03) if side == 'LONG' else lp / (1 + 0.03)
                    pct = (cur_p_at_check - lo) / (hi - lo) * 100 if hi > lo else 50
                    print(f"  守卫检查时 3h 区间 [{lo}, {hi}]  ({r['n']}根)")
                    print(f"  估计 cur_price={cur_p_at_check:.6f}  "
                          f"3h pos={pct:+.0f}%  "
                          f"(LONG>90 追高 / SHORT<10 踩底 / >100 破顶 / <0 破底)")

            if market_o:
                print(f"  MARKET created={market_o['created_at']}  "
                      f"fill={market_o['avg_fill_price']}  "
                      f"status={market_o['status']}")

            # 24h 涨跌 (当前)
            cur.execute(
                "SELECT change_24h FROM price_stats_24h WHERE symbol=%s",
                (p['symbol'],),
            )
            r = cur.fetchone()
            if r and r['change_24h'] is not None:
                print(f"  当前 24h 涨跌: {float(r['change_24h']):+.2f}%")
            print()
    finally:
        conn.close()


if __name__ == '__main__':
    main()
