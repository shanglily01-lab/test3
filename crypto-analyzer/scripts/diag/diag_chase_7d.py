"""
chase-entry 过去 7 天战绩复盘.
按日分组: 开仓数 / 成交数 / 胜 / 亏 / 净 pnl / 最大单亏
整体: 胜率 / 期望 / 单笔最大亏损 / 亏损集中度 (是不是靠几个大坑拉低)
只读, 不改任何代码或配置.

用法: python scripts/diag/diag_chase_7d.py [DAYS] [END_DATE]
默认 DAYS=7, END_DATE=today.
"""
import sys
from datetime import date, timedelta
import pymysql

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DB = dict(host='13.212.252.171', port=3306, user='admin', password='Yintao@110',
          db='dimesion', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

DAYS = int(sys.argv[1]) if len(sys.argv) > 1 else 7
END = date.fromisoformat(sys.argv[2]) if len(sys.argv) > 2 else date.today()
START = END - timedelta(days=DAYS - 1)


def main():
    conn = pymysql.connect(**DB)
    cur = conn.cursor()
    try:
        # 拉区间内所有 chase-entry 的 position (通过 futures_orders.order_source 反查)
        cur.execute(
            """
            SELECT DISTINCT p.id, p.symbol, p.position_side, p.entry_price,
                   p.realized_pnl, p.notes, p.open_time, p.close_time, p.status,
                   TIMESTAMPDIFF(MINUTE, p.open_time, p.close_time) as hold_min,
                   p.max_profit_pct, p.trailing_stop_activated
            FROM futures_positions p
            INNER JOIN futures_orders o ON o.position_id = p.id
            WHERE o.order_source LIKE '%%chase-entry%%'
              AND DATE(p.open_time) BETWEEN %s AND %s
            ORDER BY p.open_time ASC
            """,
            (START.isoformat(), END.isoformat()),
        )
        positions = cur.fetchall()

        print(f"\n### chase-entry 复盘  {START} ~ {END}  ({DAYS}天)  共 {len(positions)} 仓 ###\n")

        # 按日分组统计
        by_day = {}
        for p in positions:
            d = p['open_time'].date().isoformat()
            by_day.setdefault(d, []).append(p)

        print(f"{'日期':<12} {'仓数':>4} {'已平':>4} {'赢':>3} {'亏':>3} {'未平':>4} "
              f"{'净pnl':>10} {'最大赢':>9} {'最大亏':>9} {'平均持仓min':>12}")
        print("-" * 90)
        for d in sorted(by_day.keys()):
            lst = by_day[d]
            closed = [p for p in lst if p['close_time'] is not None]
            opn = len(lst) - len(closed)
            wins = [p for p in closed if float(p['realized_pnl'] or 0) > 0]
            losses = [p for p in closed if float(p['realized_pnl'] or 0) < 0]
            pnl_sum = sum(float(p['realized_pnl'] or 0) for p in closed)
            max_w = max((float(p['realized_pnl']) for p in closed if p['realized_pnl']), default=0)
            min_l = min((float(p['realized_pnl']) for p in closed if p['realized_pnl']), default=0)
            avg_hold = sum(p['hold_min'] or 0 for p in closed) / len(closed) if closed else 0
            print(f"{d:<12} {len(lst):>4} {len(closed):>4} {len(wins):>3} {len(losses):>3} "
                  f"{opn:>4} {pnl_sum:>+10.2f} {max_w:>+9.2f} {min_l:>+9.2f} {avg_hold:>12.1f}")

        # 整体统计
        closed_all = [p for p in positions if p['close_time'] is not None]
        if closed_all:
            pnls = [float(p['realized_pnl'] or 0) for p in closed_all]
            wins = [x for x in pnls if x > 0]
            losses = [x for x in pnls if x < 0]
            total_pnl = sum(pnls)
            win_rate = len(wins) / len(closed_all) * 100

            print("\n" + "=" * 90)
            print(f"[{DAYS}天整体汇总]")
            print("=" * 90)
            print(f"  已平仓位: {len(closed_all)}  (赢 {len(wins)} / 亏 {len(losses)})  胜率 {win_rate:.1f}%")
            print(f"  净 pnl:   {total_pnl:+.2f}")
            if wins:
                print(f"  平均赢:   {sum(wins)/len(wins):+.2f}  最大赢: {max(wins):+.2f}")
            if losses:
                print(f"  平均亏:   {sum(losses)/len(losses):+.2f}  最大亏: {min(losses):+.2f}")
            # 期望 = 胜率 * 平均赢 + 败率 * 平均亏
            avg_w = sum(wins)/len(wins) if wins else 0
            avg_l = sum(losses)/len(losses) if losses else 0
            expectancy = win_rate/100 * avg_w + (1 - win_rate/100) * avg_l
            print(f"  单笔期望: {expectancy:+.2f}")

            # 集中度: 最大 3 笔亏损占总亏损比例
            if losses:
                top3_loss = sum(sorted(losses)[:3])
                total_loss = sum(losses)
                print(f"  亏损集中度: top3 亏损 {top3_loss:+.2f} / 总亏损 {total_loss:+.2f} = {top3_loss/total_loss*100:.1f}%")

            # 按 symbol 分: 哪些币种是亏损主力
            by_sym = {}
            for p in closed_all:
                sym = p['symbol']
                by_sym.setdefault(sym, []).append(float(p['realized_pnl'] or 0))
            print("\n  -- 按 symbol 汇总 (按净 pnl 升序, 只显示前 10) --")
            sym_stats = sorted(
                [(s, sum(v), len(v), sum(1 for x in v if x > 0)) for s, v in by_sym.items()],
                key=lambda x: x[1]
            )
            print(f"  {'symbol':<15} {'净pnl':>10} {'仓数':>4} {'赢':>3}")
            for s, pnl, n, w in sym_stats[:10]:
                print(f"  {s:<15} {pnl:>+10.2f} {n:>4} {w:>3}")
            if len(sym_stats) > 10:
                print(f"  ... ({len(sym_stats) - 10} more)")
            print()
            # top winners
            print("  -- 按 symbol (按净 pnl 降序, top 5 盈利) --")
            for s, pnl, n, w in sorted(sym_stats, key=lambda x: -x[1])[:5]:
                print(f"  {s:<15} {pnl:>+10.2f} {n:>4} {w:>3}")

            # 平仓原因分布
            print("\n  -- 平仓原因分布 --")
            reason_stat = {}
            for p in closed_all:
                notes = (p['notes'] or '').strip()
                if '手动' in notes:
                    k = '手动平仓'
                elif 'early-sl' in notes:
                    k = 'early-sl'
                elif 'trail-tp' in notes:
                    k = 'trail-tp'
                elif 'breakeven' in notes:
                    k = 'breakeven-sl'
                elif 'hard-sl' in notes or 'hard-tp' in notes:
                    k = notes
                else:
                    k = notes[:20] or '(empty)'
                reason_stat.setdefault(k, []).append(float(p['realized_pnl'] or 0))
            for k, lst in sorted(reason_stat.items(), key=lambda kv: -len(kv[1])):
                n = len(lst)
                pnl = sum(lst)
                avg = pnl / n
                print(f"    {k:<25} {n:>3} 笔  净 {pnl:>+9.2f}  均 {avg:>+7.2f}")

    finally:
        conn.close()


if __name__ == '__main__':
    main()
