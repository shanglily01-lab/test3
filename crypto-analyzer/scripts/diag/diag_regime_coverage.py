"""
验证 regime / kline_scores / price_stats_24h 三张表对"最近 SL 单的 symbol"的覆盖率.
回答: 这些表到底有没有这些币的数据?
"""
import sys
from datetime import date, timedelta
import pymysql

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DB = dict(host='13.212.252.171', port=3306, user='admin', password='Yintao@110',
          db='dimesion', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)


def main():
    conn = pymysql.connect(**DB); cur = conn.cursor()
    try:
        # 1. 三张表总体覆盖
        print("=" * 80)
        print("[1] 三张表总体状态")
        print("=" * 80)
        for tbl, col in [('market_regime', 'detected_at'),
                         ('coin_kline_scores', 'updated_at'),
                         ('price_stats_24h', 'updated_at')]:
            cur.execute(f"SELECT COUNT(DISTINCT symbol) AS sym_cnt, MAX({col}) AS latest, "
                        f"MIN({col}) AS oldest, COUNT(*) AS row_cnt FROM {tbl}")
            r = cur.fetchone()
            print(f"  {tbl:<22} 总行数={r['row_cnt']:>8}  distinct_sym={r['sym_cnt']:>4}  "
                  f"latest={r['latest']}  oldest={r['oldest']}")
        print()

        # 2. 最近 7 天 SL 仓位的 symbol 列表
        print("=" * 80)
        print("[2] 最近 7 天 SL 仓位涉及的 symbol 覆盖情况")
        print("=" * 80)
        end = date.today(); start = end - timedelta(days=6)
        cur.execute(
            """SELECT DISTINCT symbol FROM futures_positions
               WHERE DATE(close_time) BETWEEN %s AND %s
                 AND status IN ('closed','liquidated')
                 AND realized_pnl < 0
                 AND (notes LIKE '%%early-sl%%' OR notes LIKE '%%止损%%' OR notes LIKE '%%breakeven%%')""",
            (start.isoformat(), end.isoformat()),
        )
        sl_syms = [r['symbol'] for r in cur.fetchall()]
        print(f"  SL 涉及 {len(sl_syms)} 个 symbol: {sl_syms[:20]}{'...' if len(sl_syms)>20 else ''}")
        print()

        # 3. 每张表对这些 symbol 的覆盖率
        for tbl, col in [('market_regime', 'detected_at'),
                         ('coin_kline_scores', 'updated_at'),
                         ('price_stats_24h', 'updated_at')]:
            placeholders = ','.join(['%s'] * len(sl_syms))
            cur.execute(
                f"SELECT symbol, COUNT(*) AS n, MAX({col}) AS latest "
                f"FROM {tbl} WHERE symbol IN ({placeholders}) GROUP BY symbol",
                tuple(sl_syms),
            )
            covered = {r['symbol']: r for r in cur.fetchall()}
            print(f"  -- {tbl} 覆盖: {len(covered)}/{len(sl_syms)} --")
            missing = [s for s in sl_syms if s not in covered]
            if missing:
                print(f"    未覆盖: {missing}")
            if covered:
                lst = sorted(covered.values(), key=lambda r: r['latest'] or '')
                print(f"    最旧: {lst[0]['symbol']} @ {lst[0]['latest']}  "
                      f"最新: {lst[-1]['symbol']} @ {lst[-1]['latest']}")
            print()

        # 4. market_regime 按 timeframe 分组看覆盖
        print("=" * 80)
        print("[3] market_regime 按 timeframe 覆盖")
        print("=" * 80)
        cur.execute(
            "SELECT timeframe, COUNT(DISTINCT symbol) AS n, MAX(detected_at) AS latest "
            "FROM market_regime GROUP BY timeframe"
        )
        for r in cur.fetchall():
            print(f"  tf={r['timeframe']:<6} symbols={r['n']:>4}  latest={r['latest']}")
        print()

        # 5. market_regime 覆盖到哪些 symbol
        print("=" * 80)
        print("[4] market_regime 实际覆盖的 symbol (最新 50)")
        print("=" * 80)
        cur.execute(
            """SELECT symbol, MAX(detected_at) AS latest, COUNT(*) AS n
               FROM market_regime WHERE timeframe='15m'
               GROUP BY symbol ORDER BY latest DESC LIMIT 50"""
        )
        for r in cur.fetchall():
            print(f"  {r['symbol']:<14} latest={r['latest']}  rows={r['n']}")
        print()

    finally:
        conn.close()


if __name__ == '__main__':
    main()
