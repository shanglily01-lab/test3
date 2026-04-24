"""
今天 paper 仓位的深度剖析:
  对每个已平仓位 -> 拉完整订单链 (开仓市价/限价 + 止损/止盈单 + 平仓单)
  特别关注: early-sl 仓位是哪个价位被打掉的, 止损距离开仓价多少
  AKE -217 手动平仓单独输出最详细信息

只读, 不改任何表.
用法: python scripts/diag/diag_today_deep.py [YYYY-MM-DD]
"""
import sys
from datetime import date
import pymysql

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DB_CFG = dict(
    host='13.212.252.171', port=3306,
    user='admin', password='Yintao@110',
    db='dimesion', charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor,
)

TARGET_DATE = sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat()


def main():
    conn = pymysql.connect(**DB_CFG)
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id, symbol, position_side, entry_price, stop_loss_price,
                   take_profit_price, quantity, leverage, margin,
                   max_profit_pct, max_profit_price, max_profit_time,
                   trailing_stop_activated, trailing_stop_price,
                   realized_pnl, open_time, close_time, notes,
                   entry_signal_type, entry_score, entry_reason,
                   signal_components
            FROM futures_positions
            WHERE DATE(close_time) = %s AND status IN ('closed','liquidated')
            ORDER BY close_time ASC
            """,
            (TARGET_DATE,),
        )
        positions = cur.fetchall()
        print(f"\n### {TARGET_DATE} 共 {len(positions)} 个已平 paper 仓位 ###\n")

        for p in positions:
            pid = p['id']
            sym = p['symbol']
            side = p['position_side']
            entry = float(p['entry_price']) if p['entry_price'] else 0
            sl = float(p['stop_loss_price']) if p['stop_loss_price'] else None
            tp = float(p['take_profit_price']) if p['take_profit_price'] else None
            pnl = float(p['realized_pnl'] or 0)
            notes = (p['notes'] or '').strip()
            open_t = p['open_time']
            close_t = p['close_time']
            hold_min = int((close_t - open_t).total_seconds() / 60) if close_t and open_t else -1

            # 计算止损距离
            sl_dist_pct = None
            if sl and entry:
                if side == 'LONG':
                    sl_dist_pct = (entry - sl) / entry * 100
                else:
                    sl_dist_pct = (sl - entry) / entry * 100

            tp_dist_pct = None
            if tp and entry:
                if side == 'LONG':
                    tp_dist_pct = (tp - entry) / entry * 100
                else:
                    tp_dist_pct = (entry - tp) / entry * 100

            print("-" * 90)
            marker = "[WIN] " if pnl > 0 else "[LOSS]"
            print(f"{marker} #{pid} {sym} {side}  pnl={pnl:+.2f}  hold={hold_min}min  notes={notes}")
            print(f"   entry={entry} SL={sl} ({sl_dist_pct:+.2f}% dist)  "
                  f"TP={tp} ({tp_dist_pct:+.2f}% dist)" if sl_dist_pct is not None and tp_dist_pct is not None
                  else f"   entry={entry} SL={sl} TP={tp}")
            print(f"   open={open_t}  close={close_t}")
            if p['entry_signal_type']:
                print(f"   signal={p['entry_signal_type']}  score={p['entry_score']}")
            if p['entry_reason']:
                print(f"   reason={p['entry_reason'][:150]}")

            # 订单链
            cur.execute(
                """
                SELECT id, order_id, side, order_type, price, avg_fill_price,
                       stop_price, quantity, status, fill_time, created_at,
                       canceled_at, cancellation_reason, realized_pnl, pnl_pct,
                       order_source, notes
                FROM futures_orders
                WHERE position_id = %s
                ORDER BY id ASC
                """,
                (pid,),
            )
            orders = cur.fetchall()
            print(f"   订单链 ({len(orders)} 个):")
            for o in orders:
                fill = o['avg_fill_price']
                sp = o['stop_price']
                stat = o['status']
                action = o['side']
                otype = o['order_type']
                when = o['fill_time'] or o['canceled_at'] or o['created_at']
                reason = o['cancellation_reason'] or ''
                pnl_str = f" pnl={o['realized_pnl']}" if o['realized_pnl'] is not None else ""
                print(f"     - {action:<13} {otype:<20} stop={sp} fill={fill} "
                      f"status={stat:<10} {when} {reason}{pnl_str}")
                if o['notes']:
                    nt = o['notes'].strip().replace('\n', ' ')[:140]
                    print(f"        notes: {nt}")

            # 如果是亏损单且标记了 early-sl, 计算"从开仓到被打止损"的价差
            if pnl < 0 and 'early-sl' in notes.lower():
                sl_order = next((o for o in orders if o['status'] == 'FILLED'
                                 and o['side'] in ('CLOSE_LONG', 'CLOSE_SHORT')), None)
                if sl_order and sl_order['avg_fill_price']:
                    closed_at = float(sl_order['avg_fill_price'])
                    if side == 'LONG':
                        move_pct = (closed_at - entry) / entry * 100
                    else:
                        move_pct = (entry - closed_at) / entry * 100
                    print(f"   >> 开仓 -> 止损成交: 价格逆行 {move_pct:.2f}% "
                          f"({entry} -> {closed_at})")

            print()

        # AKE 特别区块
        ake = [p for p in positions if 'AKE' in p['symbol']]
        if ake:
            print("=" * 90)
            print("[AKE 专项]")
            print("=" * 90)
            for p in ake:
                pid = p['id']
                # 拿到开仓前后同品种的所有订单 (包括未关联 position 的 限价单)
                cur.execute(
                    """
                    SELECT id, order_id, side, order_type, price, avg_fill_price,
                           stop_price, quantity, status, fill_time, created_at,
                           canceled_at, cancellation_reason, realized_pnl,
                           order_source, entry_signal_type, notes
                    FROM futures_orders
                    WHERE symbol = %s
                      AND (created_at BETWEEN %s AND %s
                           OR fill_time BETWEEN %s AND %s)
                    ORDER BY id ASC
                    """,
                    (p['symbol'], p['open_time'], p['close_time'],
                     p['open_time'], p['close_time']),
                )
                all_ords = cur.fetchall()
                print(f"  AKE position #{pid}: entry={p['entry_price']} pnl={p['realized_pnl']}")
                print(f"  开仓时信号: {p['entry_signal_type']}  score={p['entry_score']}")
                if p['entry_reason']:
                    print(f"  开仓理由: {p['entry_reason']}")
                if p['signal_components']:
                    print(f"  信号组成: {p['signal_components'][:400]}")
                print(f"  所有 AKE 订单 (持仓区间):")
                for o in all_ords:
                    print(f"    #{o['id']} {o['side']:<13}{o['order_type']:<20} "
                          f"qty={o['quantity']} stop={o['stop_price']} "
                          f"price={o['price']} fill={o['avg_fill_price']} "
                          f"{o['status']} src={o['order_source']}")
                    if o['notes']:
                        nt = o['notes'].strip().replace('\n', ' ')[:200]
                        print(f"      notes: {nt}")
                print()
    finally:
        conn.close()


if __name__ == '__main__':
    main()
