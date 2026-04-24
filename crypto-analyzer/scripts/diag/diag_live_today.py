"""
今天实盘账户的全貌: 账户余额/风控限额 + 所有实盘仓位 (开着+平掉的) + 所有实盘订单 + 成交.
只读.

用法: python scripts/diag/diag_live_today.py [YYYY-MM-DD]
"""
import sys
from datetime import date
import pymysql

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DB = dict(host='13.212.252.171', port=3306, user='admin', password='Yintao@110',
          db='dimesion', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

TARGET = sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat()


def main():
    conn = pymysql.connect(**DB)
    cur = conn.cursor()
    try:
        # 1. 账户
        print("=" * 90)
        print("[1] live_trading_accounts")
        print("=" * 90)
        cur.execute(
            """SELECT id, account_name, exchange, max_position_value, max_daily_loss,
                      max_total_positions, max_leverage,
                      total_balance, available_balance, unrealized_pnl,
                      total_trades, winning_trades, losing_trades,
                      total_realized_pnl, status, is_default, last_sync_time, updated_at
               FROM live_trading_accounts ORDER BY id"""
        )
        for a in cur.fetchall():
            print(f"  account#{a['id']} [{a['status']}] default={a['is_default']}  "
                  f"name={a['account_name']}  exchange={a['exchange']}")
            print(f"    balance total={a['total_balance']} available={a['available_balance']} "
                  f"unrealized={a['unrealized_pnl']}")
            print(f"    risk caps: pos_value<=${a['max_position_value']}  daily_loss<=${a['max_daily_loss']}  "
                  f"max_pos={a['max_total_positions']}  max_lev={a['max_leverage']}x")
            print(f"    total_trades={a['total_trades']} wins={a['winning_trades']} "
                  f"losses={a['losing_trades']} realized_pnl={a['total_realized_pnl']}")
            print(f"    last_sync={a['last_sync_time']}  updated={a['updated_at']}")
            print()

        # 2. 今天相关的所有实盘仓位: 开仓或平仓或仍未平
        print("=" * 90)
        print(f"[2] live_futures_positions  (open or close today={TARGET}, or status=OPEN)")
        print("=" * 90)
        cur.execute(
            """SELECT id, account_id, symbol, position_side, leverage, quantity,
                      entry_price, close_price, mark_price,
                      realized_pnl, unrealized_pnl, margin, notional_value,
                      stop_loss_price, take_profit_price,
                      trailing_stop_activated, max_profit_pct,
                      open_time, close_time, status, close_reason, source,
                      paper_position_id, notes
               FROM live_futures_positions
               WHERE DATE(open_time) = %s OR DATE(close_time) = %s
                     OR status IN ('OPEN', 'PENDING')
               ORDER BY COALESCE(close_time, open_time) ASC""",
            (TARGET, TARGET),
        )
        rows = cur.fetchall()
        print(f"  共 {len(rows)} 个仓位\n")
        open_rows = [r for r in rows if r['status'] == 'OPEN']
        closed_rows = [r for r in rows if r['status'] not in ('OPEN', 'PENDING')]

        if open_rows:
            print(f"  -- 仍在持仓: {len(open_rows)} 个 --")
            unreal_sum = 0.0
            for r in open_rows:
                ur = float(r['unrealized_pnl'] or 0)
                unreal_sum += ur
                print(f"    #{r['id']} {r['symbol']:<14} {r['position_side']:<5} "
                      f"entry={r['entry_price']} mark={r['mark_price']} "
                      f"qty={r['quantity']} lev={r['leverage']}x margin={r['margin']}")
                print(f"        unrealized={ur:+.4f}  SL={r['stop_loss_price']} "
                      f"TP={r['take_profit_price']}  trail_act={r['trailing_stop_activated']}")
                print(f"        open={r['open_time']}  paper_pos={r['paper_position_id']} "
                      f"source={r['source']}")
                if r['notes']:
                    print(f"        notes: {r['notes'][:200]}")
            print(f"  -- 持仓浮盈浮亏合计: {unreal_sum:+.4f} --\n")
        else:
            print("  -- 无仍在持仓的仓位 --\n")

        if closed_rows:
            print(f"  -- 今天已平仓: {len(closed_rows)} 个 --")
            real_sum = 0.0
            for r in closed_rows:
                rp = float(r['realized_pnl'] or 0)
                real_sum += rp
                print(f"    #{r['id']} {r['symbol']:<14} {r['position_side']:<5} "
                      f"entry={r['entry_price']} close={r['close_price']} "
                      f"realized={rp:+.4f} reason={r['close_reason']}")
                print(f"        open={r['open_time']}  close={r['close_time']} "
                      f"paper_pos={r['paper_position_id']}")
                if r['notes']:
                    print(f"        notes: {r['notes'][:200]}")
            print(f"  -- 已平实现 pnl 合计: {real_sum:+.4f} --\n")
        else:
            print("  -- 今天无已平仓 --\n")

        # 3. 今天所有实盘订单
        print("=" * 90)
        print(f"[3] live_futures_orders  (created today)")
        print("=" * 90)
        cur.execute(
            """SELECT id, symbol, side, position_side, order_type, price, stop_price,
                      quantity, avg_fill_price, executed_quantity, status,
                      realized_pnl, commission, source, strategy_id,
                      order_time, fill_time, created_at
               FROM live_futures_orders
               WHERE DATE(COALESCE(order_time, created_at)) = %s
               ORDER BY id ASC""",
            (TARGET,),
        )
        orders = cur.fetchall()
        print(f"  共 {len(orders)} 条订单\n")
        by_status = {}
        comm_sum = 0.0
        pnl_sum = 0.0
        for o in orders:
            by_status.setdefault(o['status'], []).append(o)
            if o['commission']:
                comm_sum += float(o['commission'])
            if o['realized_pnl']:
                pnl_sum += float(o['realized_pnl'])
        for status, lst in sorted(by_status.items(), key=lambda kv: -len(kv[1])):
            print(f"  -- status={status}: {len(lst)} 条 --")
            for o in lst:
                pnl = f" pnl={float(o['realized_pnl']):+.4f}" if o['realized_pnl'] else ""
                print(f"    #{o['id']} {o['symbol']:<14} {o['side']:<5}/"
                      f"{o['position_side'] or '?':<5} {o['order_type']:<22} "
                      f"qty={o['quantity']} price={o['price']} "
                      f"fill={o['avg_fill_price']}{pnl}")
                print(f"        src={o['source']} strat={o['strategy_id']} "
                      f"order_time={o['order_time']} fill_time={o['fill_time']}")
            print()
        print(f"## 订单表 pnl 合计: {pnl_sum:+.4f}, 手续费合计: {comm_sum:.4f} ##\n")

        # 4. 成交记录 (真金白银)
        print("=" * 90)
        print(f"[4] live_futures_trades  (trade_time = {TARGET})")
        print("=" * 90)
        cur.execute(
            """SELECT id, symbol, side, position_side, price, quantity, quote_quantity,
                      commission, realized_pnl, trade_time
               FROM live_futures_trades
               WHERE DATE(trade_time) = %s
               ORDER BY id ASC""",
            (TARGET,),
        )
        trades = cur.fetchall()
        print(f"  共 {len(trades)} 条成交\n")
        trade_pnl = 0.0
        trade_fee = 0.0
        for t in trades:
            rp = float(t['realized_pnl'] or 0)
            fee = float(t['commission'] or 0)
            trade_pnl += rp
            trade_fee += fee
            print(f"  #{t['id']} {t['symbol']:<14} {t['side']:<5}/"
                  f"{t['position_side']:<5} p={t['price']} qty={t['quantity']} "
                  f"quote={t['quote_quantity']} pnl={rp:+.4f} fee={fee:.4f}  "
                  f"@ {t['trade_time']}")
        print(f"\n## 成交合计: 已实现 pnl={trade_pnl:+.4f}  手续费={trade_fee:.4f}  "
              f"净={trade_pnl - trade_fee:+.4f} ##\n")

        # 5. 汇总行: 今天实盘真实净亏/盈
        print("=" * 90)
        print("[汇总]")
        print("=" * 90)
        print(f"  已平仓位实现 pnl:    {sum(float(r['realized_pnl'] or 0) for r in closed_rows):+.4f}")
        print(f"  持仓浮动 pnl:        {sum(float(r['unrealized_pnl'] or 0) for r in open_rows):+.4f}")
        print(f"  成交表实现 pnl 合计: {trade_pnl:+.4f}")
        print(f"  成交表手续费合计:    {trade_fee:.4f}")
        print(f"  净损益 (实现-费):    {trade_pnl - trade_fee:+.4f}")
    finally:
        conn.close()


if __name__ == '__main__':
    main()
