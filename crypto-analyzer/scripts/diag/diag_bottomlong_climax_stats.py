"""
统计 strategy_live:bottomlong-climax 信号的历史胜率与盈亏比.
对照组: strategy_live:topshort-climax (镜像信号), 看反转类信号整体表现.
只读.
"""
import sys
import pymysql
from collections import defaultdict
from datetime import datetime

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DB = dict(host='54.179.112.251', port=3306, user='admin', password='Yintao@110',
          db='dimesion', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)


def stats_for_source(cur, source_pattern: str, label: str):
    cur.execute(
        """SELECT id, symbol, position_side, source, status,
                  entry_price, mark_price,
                  unrealized_pnl, unrealized_pnl_pct,
                  realized_pnl, max_profit_pct,
                  open_time, close_time,
                  TIMESTAMPDIFF(MINUTE, open_time, COALESCE(close_time, NOW())) AS hold_min
           FROM futures_positions
           WHERE source LIKE %s
           ORDER BY open_time ASC""",
        (source_pattern,),
    )
    rows = cur.fetchall()
    print("=" * 100)
    print(f"=== {label}  (source LIKE '{source_pattern}')  共 {len(rows)} 笔 ===")
    if not rows:
        print("  无记录")
        return

    closed = [r for r in rows if r['status'] == 'closed']
    opened = [r for r in rows if r['status'] == 'open']
    print(f"  状态: closed={len(closed)}  open={len(opened)}")
    print(f"  时间范围: {rows[0]['open_time']}  ~  {rows[-1]['open_time']}")

    if closed:
        wins = [r for r in closed if float(r['realized_pnl'] or 0) > 0]
        losses = [r for r in closed if float(r['realized_pnl'] or 0) < 0]
        flats = [r for r in closed if float(r['realized_pnl'] or 0) == 0]
        win_rate = len(wins) / len(closed) * 100 if closed else 0
        sum_win = sum(float(r['realized_pnl'] or 0) for r in wins)
        sum_loss = sum(float(r['realized_pnl'] or 0) for r in losses)
        net = sum_win + sum_loss
        avg_win = sum_win / len(wins) if wins else 0
        avg_loss = sum_loss / len(losses) if losses else 0
        pf = (sum_win / abs(sum_loss)) if sum_loss != 0 else float('inf')
        max_profits = [float(r['max_profit_pct'] or 0) for r in closed]
        avg_hold = sum(int(r['hold_min'] or 0) for r in closed) / len(closed)

        print(f"\n  [已平仓 {len(closed)} 笔]")
        print(f"    胜率:    {win_rate:5.1f}%  ({len(wins)}胜 / {len(losses)}负 / {len(flats)}平)")
        print(f"    净利润:   {net:+.2f} USDT")
        print(f"    平均盈:   {avg_win:+.2f} USDT  (合计 {sum_win:+.2f})")
        print(f"    平均亏:   {avg_loss:+.2f} USDT  (合计 {sum_loss:+.2f})")
        print(f"    盈亏比 PF: {pf:.2f}  (>1 才赚钱)")
        print(f"    平均持仓: {avg_hold:.0f} min ({avg_hold/60:.1f}h)")
        print(f"    平均 max_profit_pct (margin%):  {sum(max_profits)/len(max_profits):+.2f}%")

        # 按方向分
        for side in ('LONG', 'SHORT'):
            sub = [r for r in closed if r['position_side'] == side]
            if not sub:
                continue
            sub_wins = [r for r in sub if float(r['realized_pnl'] or 0) > 0]
            sub_net = sum(float(r['realized_pnl'] or 0) for r in sub)
            print(f"      {side}: {len(sub)}笔  胜率{len(sub_wins)/len(sub)*100:5.1f}%  净{sub_net:+.2f}")

        # 列出每一笔
        print(f"\n  [明细]")
        print(f"    {'ID':<6} {'sym':<14} {'side':<5} {'pnl':>9} {'pnl%':>7} {'maxP%':>7} {'hold':>6}  open ~ close")
        for r in closed[-20:]:  # 最近 20 笔
            pnl = float(r['realized_pnl'] or 0)
            mp = float(r['max_profit_pct'] or 0)
            up = float(r['unrealized_pnl_pct'] or 0)
            print(f"    {r['id']:<6} {r['symbol']:<14} {r['position_side']:<5} "
                  f"{pnl:>+9.2f} {up:>+7.2f} {mp:>+7.2f} {int(r['hold_min'] or 0):>5}m  "
                  f"{r['open_time']} -> {r['close_time']}")

    if opened:
        print(f"\n  [当前在持 {len(opened)} 笔]")
        for r in opened:
            up_pnl = float(r['unrealized_pnl'] or 0)
            up_pct = float(r['unrealized_pnl_pct'] or 0)
            mp = float(r['max_profit_pct'] or 0)
            print(f"    #{r['id']} {r['symbol']:<14} {r['position_side']:<5} "
                  f"unrealized={up_pnl:+.2f}({up_pct:+.2f}%) max={mp:+.2f}% "
                  f"hold={int(r['hold_min'] or 0)}min  open={r['open_time']}")
    print()


def main():
    conn = pymysql.connect(**DB); cur = conn.cursor()
    try:
        # 主角: bottomlong-climax (LONG, 镜像反转做多)
        stats_for_source(cur, 'strategy_live:bottomlong-climax%', 'bottomlong-climax (底部反转做多)')
        # 镜像对照: topshort-climax (SHORT, 顶部反转做空)
        stats_for_source(cur, 'strategy_live:topshort-climax%', 'topshort-climax (顶部反转做空)')
        # 整个 strategy_live 总览作参考
        stats_for_source(cur, 'strategy_live:%', 'strategy_live 全部信号 (参考基线)')
    finally:
        conn.close()


if __name__ == '__main__':
    main()
