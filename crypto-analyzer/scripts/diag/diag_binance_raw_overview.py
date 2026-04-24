"""
看一眼 binance_trades_raw 拉到什么, 总账多少, 时间范围多大.
只读.
"""
import sys
import pymysql

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DB = dict(host='13.212.252.171', port=3306, user='admin', password='Yintao@110',
          db='dimesion', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)


def main():
    conn = pymysql.connect(**DB); cur = conn.cursor()
    try:
        # 1. 总览
        cur.execute(
            """SELECT COUNT(*) AS n,
                      COUNT(DISTINCT symbol) AS n_sym,
                      COUNT(DISTINCT order_id) AS n_order,
                      MIN(trade_time) AS earliest,
                      MAX(trade_time) AS latest,
                      SUM(COALESCE(realized_pnl, 0)) AS pnl_total,
                      SUM(CASE WHEN commission_asset='USDT' THEN COALESCE(commission,0) ELSE 0 END) AS comm_total
               FROM binance_trades_raw""")
        r = cur.fetchone()
        print("\n" + "=" * 90)
        print("[1] binance_trades_raw 总览")
        print("=" * 90)
        print(f"  总成交条数:    {r['n']}")
        print(f"  symbol 个数:   {r['n_sym']}")
        print(f"  订单个数:      {r['n_order']}")
        print(f"  最早:          {r['earliest']}")
        print(f"  最新:          {r['latest']}")
        print(f"  已实现 pnl:    {float(r['pnl_total'] or 0):+.4f}")
        print(f"  手续费 (USDT): {float(r['comm_total'] or 0):.4f}")
        print(f"  净 (pnl-fee):  {float(r['pnl_total'] or 0) - float(r['comm_total'] or 0):+.4f}")
        print()

        # 2. 按天分组
        print("=" * 90)
        print("[2] 按天分组 (UTC)")
        print("=" * 90)
        cur.execute(
            """SELECT DATE(trade_time) AS d,
                      COUNT(*) AS n,
                      COUNT(DISTINCT symbol) AS n_sym,
                      SUM(CASE WHEN side='BUY' THEN 1 ELSE 0 END) AS buys,
                      SUM(CASE WHEN side='SELL' THEN 1 ELSE 0 END) AS sells,
                      SUM(COALESCE(realized_pnl,0)) AS pnl,
                      SUM(CASE WHEN commission_asset='USDT' THEN COALESCE(commission,0) ELSE 0 END) AS comm
               FROM binance_trades_raw
               GROUP BY DATE(trade_time)
               ORDER BY d ASC""")
        print(f"  {'date':<12}{'trades':>7}{'sym':>5}{'buy':>5}{'sell':>5}"
              f"{'realized':>12}{'comm':>10}{'净':>12}")
        for row in cur.fetchall():
            rp = float(row['pnl'] or 0); cm = float(row['comm'] or 0)
            print(f"  {str(row['d']):<12}{row['n']:>7}{row['n_sym']:>5}"
                  f"{row['buys']:>5}{row['sells']:>5}"
                  f"{rp:>+12.4f}{cm:>10.4f}{rp-cm:>+12.4f}")
        print()

        # 3. 按 symbol 分组 (全量)
        print("=" * 90)
        print("[3] 按 symbol 分组 (按 realized 升序)")
        print("=" * 90)
        cur.execute(
            """SELECT symbol,
                      COUNT(*) AS n,
                      COUNT(DISTINCT order_id) AS n_order,
                      SUM(CASE WHEN side='BUY' THEN 1 ELSE 0 END) AS buys,
                      SUM(CASE WHEN side='SELL' THEN 1 ELSE 0 END) AS sells,
                      SUM(COALESCE(realized_pnl,0)) AS pnl,
                      SUM(CASE WHEN commission_asset='USDT' THEN COALESCE(commission,0) ELSE 0 END) AS comm,
                      MIN(trade_time) AS earliest,
                      MAX(trade_time) AS latest
               FROM binance_trades_raw
               GROUP BY symbol
               ORDER BY pnl ASC""")
        print(f"  {'symbol':<16}{'n':>4}{'ord':>4}{'buy':>4}{'sell':>4}"
              f"{'realized':>12}{'comm':>9}{'净':>11}  {'earliest':<17}")
        for row in cur.fetchall():
            rp = float(row['pnl'] or 0); cm = float(row['comm'] or 0)
            print(f"  {row['symbol']:<16}{row['n']:>4}{row['n_order']:>4}"
                  f"{row['buys']:>4}{row['sells']:>4}"
                  f"{rp:>+12.4f}{cm:>9.4f}{rp-cm:>+11.4f}  "
                  f"{str(row['earliest'])[:16]}")
        print()

        # 4. 按 position_side 分
        print("=" * 90)
        print("[4] 按 position_side 分布")
        print("=" * 90)
        cur.execute(
            """SELECT position_side,
                      COUNT(*) AS n,
                      SUM(COALESCE(realized_pnl,0)) AS pnl
               FROM binance_trades_raw
               GROUP BY position_side""")
        for row in cur.fetchall():
            ps = row['position_side'] or '(none)'
            print(f"  {ps:<10} n={row['n']:>4}  pnl={float(row['pnl'] or 0):+.4f}")
        print()

        # 5. 现有实盘仓位对账: 按 order_id JOIN futures_orders.live_position_id
        print("=" * 90)
        print("[5] 对账 futures_orders (paper SYNCED)")
        print("=" * 90)
        cur.execute(
            """SELECT COUNT(DISTINCT btr.order_id) AS binance_orders,
                      COUNT(DISTINCT fo.live_position_id) AS paper_live_ids
               FROM binance_trades_raw btr""")
        r = cur.fetchone()
        print(f"  binance_trades_raw 里 distinct order_id: {r['binance_orders']}")
        cur.execute(
            """SELECT COUNT(DISTINCT live_position_id) AS n
               FROM futures_orders
               WHERE live_sync_status='SYNCED' AND live_position_id IS NOT NULL""")
        r2 = cur.fetchone()
        print(f"  futures_orders 里 live_sync_status=SYNCED 的 distinct live_position_id: {r2['n']}")
        print()

        # 6. realized_pnl 非零的单子 (成交了真的盈亏的条目)
        print("=" * 90)
        print("[6] realized_pnl 非零的条目 (真正产生盈亏的成交, top 20 亏 + top 10 赢)")
        print("=" * 90)
        cur.execute(
            """SELECT trade_id, order_id, symbol, side, position_side,
                      price, qty, realized_pnl, commission, trade_time
               FROM binance_trades_raw
               WHERE realized_pnl IS NOT NULL AND realized_pnl <> 0
               ORDER BY realized_pnl ASC
               LIMIT 20""")
        print("  -- Top 20 亏 --")
        for row in cur.fetchall():
            print(f"  #{row['trade_id']} ord={row['order_id']} {row['symbol']:<14}"
                  f"{row['side']:<5}/{row['position_side'] or '?':<5}"
                  f" p={row['price']} qty={row['qty']}"
                  f" pnl={float(row['realized_pnl']):+.4f}"
                  f" fee={row['commission']}"
                  f" @ {row['trade_time']}")
        print()
        cur.execute(
            """SELECT trade_id, order_id, symbol, side, position_side,
                      price, qty, realized_pnl, commission, trade_time
               FROM binance_trades_raw
               WHERE realized_pnl IS NOT NULL AND realized_pnl > 0
               ORDER BY realized_pnl DESC
               LIMIT 10""")
        print("  -- Top 10 赢 --")
        for row in cur.fetchall():
            print(f"  #{row['trade_id']} ord={row['order_id']} {row['symbol']:<14}"
                  f"{row['side']:<5}/{row['position_side'] or '?':<5}"
                  f" p={row['price']} qty={row['qty']}"
                  f" pnl={float(row['realized_pnl']):+.4f}"
                  f" fee={row['commission']}"
                  f" @ {row['trade_time']}")
    finally:
        conn.close()


if __name__ == '__main__':
    main()
