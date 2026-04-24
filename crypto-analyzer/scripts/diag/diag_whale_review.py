"""
strategy_whale 全盘诊断.
  1. 总账: 总仓位 / 胜率 / 净 pnl / 按天走势
  2. 分子策略: whale-short / whale-long / whale-entry / w-bottom
  3. 平仓原因分布
  4. 按 symbol 排序 top 盈 / top 亏
  5. 信号 -> 入场 -> 出场的链路: 挂单成交率, 超时率
  6. 和 strategy_live 对比
只读.
"""
import sys
from datetime import date, timedelta
from collections import defaultdict
import pymysql

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DB = dict(host='13.212.252.171', port=3306, user='admin', password='Yintao@110',
          db='dimesion', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)


def main():
    conn = pymysql.connect(**DB); cur = conn.cursor()
    try:
        # 1. 总账
        print("=" * 90)
        print("[1] strategy_whale 总账")
        print("=" * 90)
        cur.execute("""
            SELECT COUNT(*) AS n,
                   SUM(CASE WHEN realized_pnl>0 THEN 1 ELSE 0 END) AS wins,
                   SUM(CASE WHEN realized_pnl<0 THEN 1 ELSE 0 END) AS losses,
                   SUM(COALESCE(realized_pnl,0)) AS pnl_sum,
                   AVG(realized_pnl) AS pnl_avg,
                   MAX(realized_pnl) AS pnl_max,
                   MIN(realized_pnl) AS pnl_min,
                   MIN(open_time) AS earliest,
                   MAX(close_time) AS latest
            FROM futures_positions
            WHERE source LIKE 'strategy_whale:%%' AND status IN ('closed','liquidated')
        """)
        r = cur.fetchone()
        if not r or r['n'] == 0:
            print("  strategy_whale 无已平仓位")
            return
        print(f"  总已平仓位: {r['n']}  胜 {r['wins']}  亏 {r['losses']}  "
              f"胜率 {r['wins']/r['n']*100:.1f}%")
        print(f"  净 pnl:      {float(r['pnl_sum'] or 0):+.2f} USDT")
        print(f"  单笔均:      {float(r['pnl_avg'] or 0):+.2f} / 最赢 {float(r['pnl_max'] or 0):+.2f} / 最亏 {float(r['pnl_min'] or 0):+.2f}")
        print(f"  时间范围:    {r['earliest']} ~ {r['latest']}")
        # 在持仓位
        cur.execute("""
            SELECT COUNT(*) AS n, SUM(unrealized_pnl) AS upnl
            FROM futures_positions
            WHERE source LIKE 'strategy_whale:%%' AND status='open'
        """)
        r2 = cur.fetchone()
        print(f"  仍持仓:      {r2['n']}  浮盈 {float(r2['upnl'] or 0):+.2f}")
        print()

        # 2. 按天分组
        print("=" * 90)
        print("[2] 按天分组")
        print("=" * 90)
        cur.execute("""
            SELECT DATE(close_time) AS d,
                   COUNT(*) AS n,
                   SUM(CASE WHEN realized_pnl>0 THEN 1 ELSE 0 END) AS w,
                   SUM(realized_pnl) AS pnl
            FROM futures_positions
            WHERE source LIKE 'strategy_whale:%%' AND status IN ('closed','liquidated')
              AND close_time IS NOT NULL
            GROUP BY DATE(close_time) ORDER BY d ASC
        """)
        rows = cur.fetchall()
        print(f"  {'date':<12}{'n':>4}{'wins':>5}{'win%':>7}{'pnl':>11}")
        for row in rows:
            wr = row['w']/row['n']*100 if row['n'] else 0
            print(f"  {str(row['d']):<12}{row['n']:>4}{row['w']:>5}"
                  f"{wr:>6.1f}%{float(row['pnl']):>+11.2f}")
        print()

        # 3. 按子策略 (source 后缀)
        print("=" * 90)
        print("[3] 按子策略分组")
        print("=" * 90)
        cur.execute("""
            SELECT source,
                   COUNT(*) AS n,
                   SUM(CASE WHEN realized_pnl>0 THEN 1 ELSE 0 END) AS w,
                   SUM(realized_pnl) AS pnl,
                   AVG(TIMESTAMPDIFF(MINUTE, open_time, close_time)) AS avg_hold_min
            FROM futures_positions
            WHERE source LIKE 'strategy_whale:%%' AND status IN ('closed','liquidated')
            GROUP BY source ORDER BY pnl ASC
        """)
        print(f"  {'source':<32}{'n':>4}{'wins':>5}{'win%':>7}{'pnl':>11}{'avg hold':>10}")
        for r in cur.fetchall():
            wr = r['w']/r['n']*100 if r['n'] else 0
            hold = float(r['avg_hold_min'] or 0)
            print(f"  {r['source']:<32}{r['n']:>4}{r['w']:>5}"
                  f"{wr:>6.1f}%{float(r['pnl']):>+11.2f}"
                  f"{hold:>7.0f}m")
        print()

        # 4. 平仓原因 (notes)
        print("=" * 90)
        print("[4] 平仓原因分布 (按 notes)")
        print("=" * 90)
        cur.execute("""
            SELECT COALESCE(notes,'(null)') AS reason,
                   COUNT(*) AS n,
                   SUM(realized_pnl) AS pnl,
                   AVG(realized_pnl) AS avg_pnl
            FROM futures_positions
            WHERE source LIKE 'strategy_whale:%%' AND status IN ('closed','liquidated')
            GROUP BY notes ORDER BY n DESC LIMIT 20
        """)
        print(f"  {'reason':<30}{'n':>4}{'pnl':>11}{'avg':>9}")
        for r in cur.fetchall():
            rn = (r['reason'] or '(null)').strip().replace('\n',' ')[:30]
            print(f"  {rn:<30}{r['n']:>4}{float(r['pnl']):>+11.2f}"
                  f"{float(r['avg_pnl']):>+9.2f}")
        print()

        # 5. 按方向 LONG / SHORT
        print("=" * 90)
        print("[5] 按方向")
        print("=" * 90)
        cur.execute("""
            SELECT position_side,
                   COUNT(*) AS n,
                   SUM(CASE WHEN realized_pnl>0 THEN 1 ELSE 0 END) AS w,
                   SUM(realized_pnl) AS pnl
            FROM futures_positions
            WHERE source LIKE 'strategy_whale:%%' AND status IN ('closed','liquidated')
            GROUP BY position_side
        """)
        for r in cur.fetchall():
            wr = r['w']/r['n']*100 if r['n'] else 0
            print(f"  {r['position_side']:<8}n={r['n']:>4} wins={r['w']:>3} "
                  f"({wr:.1f}%)  pnl={float(r['pnl']):+.2f}")
        print()

        # 6. top 亏损 + top 盈利
        print("=" * 90)
        print("[6] top 10 亏损 + top 10 盈利")
        print("=" * 90)
        cur.execute("""
            SELECT id, symbol, position_side, source, entry_price,
                   realized_pnl, notes, open_time, close_time,
                   TIMESTAMPDIFF(MINUTE, open_time, close_time) AS hold
            FROM futures_positions
            WHERE source LIKE 'strategy_whale:%%' AND status IN ('closed','liquidated')
            ORDER BY realized_pnl ASC LIMIT 10
        """)
        print("  -- top 10 亏 --")
        for r in cur.fetchall():
            print(f"  #{r['id']} {r['symbol']:<12}{r['position_side']:<5}"
                  f"pnl={float(r['realized_pnl']):>+8.2f} hold={r['hold']:>4}m "
                  f"src={r['source']:<28} notes={(r['notes'] or '').strip()[:30]}")
        cur.execute("""
            SELECT id, symbol, position_side, source, entry_price,
                   realized_pnl, notes, open_time, close_time,
                   TIMESTAMPDIFF(MINUTE, open_time, close_time) AS hold
            FROM futures_positions
            WHERE source LIKE 'strategy_whale:%%' AND status IN ('closed','liquidated')
              AND realized_pnl > 0
            ORDER BY realized_pnl DESC LIMIT 10
        """)
        print("  -- top 10 赢 --")
        for r in cur.fetchall():
            print(f"  #{r['id']} {r['symbol']:<12}{r['position_side']:<5}"
                  f"pnl={float(r['realized_pnl']):>+8.2f} hold={r['hold']:>4}m "
                  f"src={r['source']:<28} notes={(r['notes'] or '').strip()[:30]}")
        print()

        # 7. 按 symbol 分组 (前 15 亏损户 + 前 10 盈利户)
        print("=" * 90)
        print("[7] 按 symbol 分组")
        print("=" * 90)
        cur.execute("""
            SELECT symbol,
                   COUNT(*) AS n,
                   SUM(CASE WHEN realized_pnl>0 THEN 1 ELSE 0 END) AS w,
                   SUM(realized_pnl) AS pnl
            FROM futures_positions
            WHERE source LIKE 'strategy_whale:%%' AND status IN ('closed','liquidated')
            GROUP BY symbol ORDER BY pnl ASC
        """)
        rows = cur.fetchall()
        print(f"  -- 亏损户 top 15 --")
        print(f"  {'sym':<14}{'n':>4}{'w':>4}{'win%':>7}{'pnl':>10}")
        for r in rows[:15]:
            wr = r['w']/r['n']*100 if r['n'] else 0
            print(f"  {r['symbol']:<14}{r['n']:>4}{r['w']:>4}{wr:>6.1f}%"
                  f"{float(r['pnl']):>+10.2f}")
        print(f"  -- 盈利户 top 10 --")
        for r in sorted(rows, key=lambda x: -float(x['pnl']))[:10]:
            if float(r['pnl']) <= 0: break
            wr = r['w']/r['n']*100 if r['n'] else 0
            print(f"  {r['symbol']:<14}{r['n']:>4}{r['w']:>4}{wr:>6.1f}%"
                  f"{float(r['pnl']):>+10.2f}")
        print()

        # 8. 挂单成交率 — 看是不是挂了很多限价单从来没成交
        print("=" * 90)
        print("[8] strategy_whale 订单成交率")
        print("=" * 90)
        cur.execute("""
            SELECT status, COUNT(*) AS n
            FROM futures_orders
            WHERE order_source LIKE 'strategy_whale:%%'
            GROUP BY status ORDER BY n DESC
        """)
        total = 0; stats = {}
        for r in cur.fetchall():
            stats[r['status']] = r['n']; total += r['n']
        for st, n in sorted(stats.items(), key=lambda x: -x[1]):
            print(f"  {st:<12}{n:>6} ({n/total*100:.1f}%)")
        print()

        # 9. 取消原因
        print("=" * 90)
        print("[9] 取消原因分布")
        print("=" * 90)
        cur.execute("""
            SELECT COALESCE(cancellation_reason,'(null)') AS reason,
                   COUNT(*) AS n
            FROM futures_orders
            WHERE order_source LIKE 'strategy_whale:%%'
              AND status='CANCELLED'
            GROUP BY cancellation_reason ORDER BY n DESC
        """)
        for r in cur.fetchall():
            print(f"  {(r['reason'] or '-'):<30}n={r['n']}")
        print()

        # 10. 对比 strategy_live 同期
        print("=" * 90)
        print("[10] 对比 strategy_live 同期 (whale 时间范围内)")
        print("=" * 90)
        cur.execute("""
            SELECT
                CASE
                    WHEN source LIKE 'strategy_whale:%%' THEN 'whale'
                    WHEN source LIKE 'strategy_live:%%'  THEN 'live'
                    WHEN source LIKE 'strategy_bigmid:%%' THEN 'bigmid'
                    WHEN source LIKE 'strategy_f3:%%' THEN 'f3'
                    ELSE 'other'
                END AS strat,
                COUNT(*) AS n,
                SUM(CASE WHEN realized_pnl>0 THEN 1 ELSE 0 END) AS w,
                SUM(realized_pnl) AS pnl,
                AVG(realized_pnl) AS avg
            FROM futures_positions
            WHERE status IN ('closed','liquidated')
              AND open_time >= (
                  SELECT MIN(open_time) FROM futures_positions
                  WHERE source LIKE 'strategy_whale:%%'
              )
            GROUP BY strat ORDER BY pnl DESC
        """)
        print(f"  {'strat':<10}{'n':>5}{'wins':>5}{'win%':>7}{'pnl_sum':>11}{'pnl_avg':>9}")
        for r in cur.fetchall():
            wr = r['w']/r['n']*100 if r['n'] else 0
            print(f"  {r['strat']:<10}{r['n']:>5}{r['w']:>5}"
                  f"{wr:>6.1f}%{float(r['pnl'] or 0):>+11.2f}"
                  f"{float(r['avg'] or 0):>+9.2f}")

    finally:
        conn.close()


if __name__ == '__main__':
    main()
