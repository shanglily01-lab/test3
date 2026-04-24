"""
查 strategy_whale 的 HIGH/USDT 5 笔仓位 (#24918, 24919, 24920, 24921, 24989):
  - stop_loss_price / take_profit_price 是否写入
  - timeout_at 是否设置
  - max_profit_pct / trailing_stop_activated 是否刷新
  - 持仓期间 15M kline 是否穿过了理论 TP 价格
  - 对应的 futures_orders 开仓单是否 stop_loss_price / take_profit_price
只读.
"""
import sys
import pymysql

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DB = dict(host='13.212.252.171', port=3306, user='admin', password='Yintao@110',
          db='dimesion', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

PIDS = [24918, 24919, 24920, 24921, 24989]


def main():
    conn = pymysql.connect(**DB); cur = conn.cursor()
    try:
        for pid in PIDS:
            print("=" * 100)
            print(f"[仓位 #{pid}]")
            print("=" * 100)
            # 1. 仓位本体
            cur.execute("""
                SELECT id, symbol, position_side, leverage, quantity, margin,
                       entry_price, mark_price, avg_entry_price,
                       stop_loss_price, take_profit_price,
                       stop_loss_pct, take_profit_pct,
                       unrealized_pnl, unrealized_pnl_pct,
                       realized_pnl, max_profit_pct, max_profit_price, max_profit_time,
                       trailing_stop_activated, trailing_stop_price,
                       max_hold_minutes, timeout_at,
                       open_time, close_time, status, notes, source,
                       entry_signal_type, entry_score, entry_reason
                FROM futures_positions WHERE id=%s
            """, (pid,))
            p = cur.fetchone()
            if not p:
                print(f"  仓位 {pid} 不存在")
                continue
            entry = float(p['entry_price'])
            print(f"  symbol={p['symbol']}  side={p['position_side']}  source={p['source']}")
            print(f"  entry={entry}  avg_entry={p['avg_entry_price']}  leverage={p['leverage']}x  qty={p['quantity']}  margin={p['margin']}")
            print(f"  stop_loss_price = {p['stop_loss_price']}   (pct={p['stop_loss_pct']})")
            print(f"  take_profit_price = {p['take_profit_price']}  (pct={p['take_profit_pct']})")
            print(f"  max_hold_minutes = {p['max_hold_minutes']}    timeout_at = {p['timeout_at']}")
            print(f"  max_profit_pct = {p['max_profit_pct']}  max_profit_price = {p['max_profit_price']}  max_profit_time = {p['max_profit_time']}")
            print(f"  trailing_stop_activated = {p['trailing_stop_activated']}  trailing_stop_price = {p['trailing_stop_price']}")
            print(f"  open={p['open_time']}  close={p['close_time']}  status={p['status']}")
            print(f"  realized_pnl = {p['realized_pnl']}  unrealized_pnl_pct = {p['unrealized_pnl_pct']}")
            print(f"  notes = {p['notes']}")
            print(f"  entry_signal_type = {p['entry_signal_type']}  score={p['entry_score']}")
            if p['entry_reason']:
                print(f"  entry_reason = {p['entry_reason']}")

            # 计算理论 TP 价 (按 hard-tp 20% 假设)
            if p['position_side'] == 'LONG':
                theo_tp = entry * 1.20
                theo_sl = entry * 0.90
            else:
                theo_tp = entry * 0.80
                theo_sl = entry * 1.10
            print(f"  理论 TP (价格+20%): {theo_tp:.6f}")
            print(f"  理论 SL (价格-10%): {theo_sl:.6f}")

            # 2. 开仓单 futures_orders
            cur.execute("""
                SELECT id, order_id, order_type, side, status,
                       price, stop_price, avg_fill_price,
                       stop_loss_price, take_profit_price,
                       order_source, created_at, fill_time, notes
                FROM futures_orders
                WHERE position_id=%s AND side IN ('OPEN_LONG','OPEN_SHORT')
                ORDER BY id ASC
            """, (pid,))
            open_orders = cur.fetchall()
            print(f"  开仓单 ({len(open_orders)}):")
            for o in open_orders:
                print(f"    #{o['id']} {o['side']} {o['order_type']:<10} status={o['status']:<10} "
                      f"price={o['price']} fill={o['avg_fill_price']}  "
                      f"sl={o['stop_loss_price']} tp={o['take_profit_price']}  "
                      f"src={o['order_source']}")

            # 3. 平仓单
            cur.execute("""
                SELECT id, order_id, order_type, side, status,
                       price, avg_fill_price, stop_price,
                       realized_pnl, pnl_pct,
                       created_at, fill_time, cancellation_reason, notes
                FROM futures_orders
                WHERE position_id=%s AND side IN ('CLOSE_LONG','CLOSE_SHORT')
                ORDER BY id ASC
            """, (pid,))
            close_orders = cur.fetchall()
            print(f"  平仓单 ({len(close_orders)}):")
            for o in close_orders:
                print(f"    #{o['id']} {o['side']} {o['order_type']:<10} status={o['status']:<10} "
                      f"fill={o['avg_fill_price']} pnl={o['realized_pnl']}  "
                      f"created={o['created_at']} filled={o['fill_time']}  "
                      f"notes={(o['notes'] or '').strip()[:60]}")

            # 4. 持仓期间 15M K 线的 high/low 穿越情况
            if p['open_time'] and p['close_time']:
                from datetime import datetime as _dt
                open_ms = int(p['open_time'].timestamp() * 1000)
                close_ms = int(p['close_time'].timestamp() * 1000)
                cur.execute("""
                    SELECT open_time, high_price, low_price, close_price
                    FROM kline_data
                    WHERE symbol=%s AND timeframe='15m'
                      AND open_time >= %s AND open_time <= %s
                    ORDER BY open_time ASC
                """, (p['symbol'], open_ms, close_ms))
                bars = cur.fetchall()
                if not bars:
                    print(f"  持仓期间 15M kline: 无数据")
                    continue
                peak_up = max(float(b['high_price']) for b in bars)
                trough_down = min(float(b['low_price']) for b in bars)
                if p['position_side'] == 'LONG':
                    max_pnl_pct = (peak_up - entry) / entry * 100
                    max_dd_pct = (entry - trough_down) / entry * 100
                else:
                    max_pnl_pct = (entry - trough_down) / entry * 100
                    max_dd_pct = (peak_up - entry) / entry * 100
                print(f"  持仓期间 15M: {len(bars)} 根 / 区间 [{trough_down:.6f}, {peak_up:.6f}]")
                print(f"  理论最大浮盈 (价格): +{max_pnl_pct:.2f}%")
                print(f"  理论最大浮亏 (价格): -{max_dd_pct:.2f}%")

                # 是否穿过理论 TP
                if p['position_side'] == 'LONG':
                    if peak_up >= theo_tp:
                        for b in bars:
                            if float(b['high_price']) >= theo_tp:
                                print(f"  >>> 价格曾到达理论 TP {theo_tp:.6f}  @ 15M bar {b['open_time']}  high={b['high_price']}  (如果 hard-tp 写入, 应该在此刻平仓)")
                                break
                else:
                    if trough_down <= theo_tp:
                        for b in bars:
                            if float(b['low_price']) <= theo_tp:
                                print(f"  >>> 价格曾到达理论 TP {theo_tp:.6f}  @ 15M bar {b['open_time']}  low={b['low_price']}")
                                break

                # 是否穿过 DB 里的 take_profit_price
                if p['take_profit_price']:
                    db_tp = float(p['take_profit_price'])
                    if p['position_side'] == 'LONG':
                        crossed = peak_up >= db_tp
                    else:
                        crossed = trough_down <= db_tp
                    print(f"  DB take_profit_price={db_tp}  持仓期间是否被穿过: {crossed}")

                # 是否穿过 DB 里的 stop_loss_price
                if p['stop_loss_price']:
                    db_sl = float(p['stop_loss_price'])
                    if p['position_side'] == 'LONG':
                        crossed_sl = trough_down <= db_sl
                    else:
                        crossed_sl = peak_up >= db_sl
                    print(f"  DB stop_loss_price={db_sl}  持仓期间是否被穿过: {crossed_sl}")
            print()

    finally:
        conn.close()


if __name__ == '__main__':
    main()
