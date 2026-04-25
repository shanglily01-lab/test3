"""
调查 INUSDT 当前/最近这单开仓的原因:
  - position 来源 (strategy_live / strategy_whale 等)
  - LIMIT 守卫时刻的 3h / 24h / 15M 区间位置
  - 当时 24h 涨跌、最近 K 线动量
  - 看看到底是什么信号判定开 LONG, 用户直觉觉得应该 SHORT 的依据是否成立
只读.
"""
import sys
import pymysql

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DB = dict(host='54.179.112.251', port=3306, user='admin', password='Yintao@110',
          db='dimesion', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

SYMBOL = 'IN/USDT'
BAR_MS_15M = 15 * 60 * 1000


def fmt(v, n=6):
    if v is None:
        return 'NULL'
    try:
        return f"{float(v):.{n}f}"
    except Exception:
        return str(v)


def main():
    conn = pymysql.connect(**DB); cur = conn.cursor()
    try:
        # 1) 找该 symbol 最近的持仓 (含已平)
        cur.execute(
            """SELECT id, symbol, position_side, source, status,
                      entry_price, mark_price,
                      unrealized_pnl, unrealized_pnl_pct, realized_pnl,
                      stop_loss_price, take_profit_price,
                      max_profit_pct, max_profit_price,
                      open_time, close_time,
                      max_hold_minutes, timeout_at,
                      entry_signal_type, entry_score, entry_reason,
                      signal_components, signal_version, strategy_id, signal_id
               FROM futures_positions
               WHERE symbol=%s
               ORDER BY open_time DESC
               LIMIT 5""",
            (SYMBOL,),
        )
        positions = cur.fetchall()
        if not positions:
            print(f"[!] {SYMBOL} 未找到任何 futures_positions 记录")
            return

        print(f"\n=== {SYMBOL} 最近 {len(positions)} 笔持仓 ===\n")
        for p in positions:
            print("=" * 100)
            print(f"#{p['id']}  {p['position_side']}  source={p['source']}  status={p['status']}  strategy_id={p['strategy_id']} signal_id={p['signal_id']}")
            print(f"  entry={fmt(p['entry_price'])}  mark={fmt(p['mark_price'])}")
            print(f"  unrealized={fmt(p['unrealized_pnl'],4)}({fmt(p['unrealized_pnl_pct'],2)}% margin)  realized={fmt(p['realized_pnl'],4)}")
            print(f"  SL={fmt(p['stop_loss_price'])}  TP={fmt(p['take_profit_price'])}  max_profit={fmt(p['max_profit_pct'],2)}%")
            print(f"  open={p['open_time']}  close={p['close_time']}")
            print(f"  entry_signal_type={p['entry_signal_type']}")
            print(f"  entry_reason={p['entry_reason']}")
            print(f"  entry_score={p['entry_score']}  signal_version={p['signal_version']}")
            sc = p['signal_components']
            if sc:
                sc_short = (sc[:400] + '...') if len(sc) > 400 else sc
                print(f"  signal_components={sc_short}")

            # 取所有相关订单
            cur.execute(
                """SELECT id, order_id, side, order_type, status, price, quantity,
                          avg_fill_price, created_at, fill_time, live_sync_status
                   FROM futures_orders
                   WHERE position_id=%s
                   ORDER BY id ASC""",
                (p['id'],),
            )
            for o in cur.fetchall():
                print(f"    [order #{o['id']}] {o['side']:<12} {o['order_type']:<8} {o['status']:<10} "
                      f"price={fmt(o['price'])} fill={fmt(o['avg_fill_price'])} "
                      f"created={o['created_at']} sync={o['live_sync_status']}")

            # 找开仓 LIMIT 作为守卫时刻
            cur.execute(
                """SELECT id, price, created_at
                   FROM futures_orders
                   WHERE position_id=%s AND side IN ('OPEN_LONG','OPEN_SHORT') AND order_type='LIMIT'
                   ORDER BY id ASC LIMIT 1""",
                (p['id'],),
            )
            limit_o = cur.fetchone()
            if not limit_o:
                # 没有 LIMIT 就用 MARKET
                cur.execute(
                    """SELECT id, avg_fill_price AS price, created_at
                       FROM futures_orders
                       WHERE position_id=%s AND side IN ('OPEN_LONG','OPEN_SHORT')
                       ORDER BY id ASC LIMIT 1""",
                    (p['id'],),
                )
                limit_o = cur.fetchone()
            if not limit_o:
                print("  [!] 没有任何开仓订单, 跳过行情分析")
                print()
                continue

            cur.execute("SELECT UNIX_TIMESTAMP(%s)*1000 AS ms", (limit_o['created_at'],))
            check_ms = int(cur.fetchone()['ms'])

            # 各时间窗口的高低
            for win_label, bars in [('3h', 12), ('12h', 48), ('24h', 96)]:
                start = check_ms - bars * BAR_MS_15M
                cur.execute(
                    """SELECT MAX(high_price) AS h, MIN(low_price) AS l, COUNT(*) AS n
                       FROM kline_data WHERE symbol=%s AND timeframe='15m'
                         AND open_time >= %s AND open_time < %s""",
                    (SYMBOL, start, check_ms),
                )
                r = cur.fetchone()
                if r and r['h'] is not None:
                    hi, lo = float(r['h']), float(r['l'])
                    lp = float(limit_o['price'] or 0)
                    pct = (lp - lo) / (hi - lo) * 100 if hi > lo else 50
                    print(f"  {win_label} 区间 [{lo}, {hi}]  ({r['n']}根)  入场价 pos={pct:+.0f}%")

            # 入场前最近 8 根 15m K 线 (看动量)
            cur.execute(
                """SELECT open_time, open_price, high_price, low_price, close_price, volume
                   FROM kline_data WHERE symbol=%s AND timeframe='15m'
                     AND open_time < %s
                   ORDER BY open_time DESC LIMIT 8""",
                (SYMBOL, check_ms),
            )
            recent = list(cur.fetchall())[::-1]
            if recent:
                print(f"  入场前 8 根 15m K线 (越往下越接近入场):")
                for k in recent:
                    o, h, l, c = float(k['open_price']), float(k['high_price']), float(k['low_price']), float(k['close_price'])
                    chg = (c - o) / o * 100 if o else 0
                    sign = '+' if chg >= 0 else ''
                    print(f"    {k['open_time']}  O={o:.6f} H={h:.6f} L={l:.6f} C={c:.6f}  ({sign}{chg:.2f}%) vol={float(k['volume']):.0f}")

            # 入场后到现在的最高/最低 (看判断对错)
            cur.execute(
                """SELECT MAX(high_price) AS h, MIN(low_price) AS l, COUNT(*) AS n
                   FROM kline_data WHERE symbol=%s AND timeframe='15m'
                     AND open_time >= %s""",
                (SYMBOL, check_ms),
            )
            r = cur.fetchone()
            if r and r['h'] is not None:
                lp = float(limit_o['price'] or 0)
                up = (float(r['h']) - lp) / lp * 100 if lp else 0
                dn = (float(r['l']) - lp) / lp * 100 if lp else 0
                print(f"  入场后区间 [{r['l']}, {r['h']}]  ({r['n']}根)  最高 {up:+.2f}% / 最低 {dn:+.2f}%  (vs 入场价)")

            # 当时 24h 涨跌
            cur.execute("SELECT change_24h, last_price FROM price_stats_24h WHERE symbol=%s", (SYMBOL,))
            r = cur.fetchone()
            if r:
                print(f"  当前 price_stats_24h: change_24h={fmt(r['change_24h'],2)}%  last={fmt(r['last_price'])}")

            print()

        # 2) 查 strategy_live / strategy_whale 在 INUSDT 的最近信号 (如果有信号表)
        # 先确认表是否存在
        cur.execute("SHOW TABLES LIKE 'strategy_signals%'")
        sig_tables = [list(row.values())[0] for row in cur.fetchall()]
        print(f"\n[信号表候选]: {sig_tables}")
        for t in sig_tables:
            cur.execute(f"SHOW COLUMNS FROM `{t}`")
            cols = [c['Field'] for c in cur.fetchall()]
            print(f"  {t} cols: {cols[:15]}...")

    finally:
        conn.close()


if __name__ == '__main__':
    main()
