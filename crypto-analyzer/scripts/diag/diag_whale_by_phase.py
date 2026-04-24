"""
strategy_whale 按出场机制演进阶段分期诊断.
分界点 (按 open_time):
  phase A 裸奔:       < 2026-04-22 12:48:00  (无 trail-tp, 全靠手动或硬 SL)
  phase B 老 trail:   2026-04-22 12:48:00 ~ 2026-04-23 21:30:00
                      (trail-tp 12%/2% 单档, 无 early-sl / breakeven)
  phase C 新完整链:   >= 2026-04-23 21:30:00
                      (tiered trail 3/5/10% + early-sl 3% + breakeven 1.5% + Monitor 1s 轮询)

只读.
"""
import sys
from collections import defaultdict
from datetime import datetime
import pymysql

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DB = dict(host='13.212.252.171', port=3306, user='admin', password='Yintao@110',
          db='dimesion', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

PHASE_B_START = datetime(2026, 4, 22, 12, 48, 0)
PHASE_C_START = datetime(2026, 4, 23, 21, 30, 0)


def phase_of(dt: datetime) -> str:
    if dt < PHASE_B_START:
        return 'A 裸奔'
    if dt < PHASE_C_START:
        return 'B 老trail'
    return 'C 完整链'


def main():
    conn = pymysql.connect(**DB); cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id, symbol, position_side, source, entry_price,
                   realized_pnl, notes, open_time, close_time,
                   TIMESTAMPDIFF(MINUTE, open_time, close_time) AS hold_min,
                   max_profit_pct, trailing_stop_activated,
                   stop_loss_price, take_profit_price
            FROM futures_positions
            WHERE source LIKE 'strategy_whale:%%' AND status IN ('closed','liquidated')
            ORDER BY open_time ASC
        """)
        rows = cur.fetchall()
        if not rows:
            print("无 whale 仓位")
            return

        # 分期
        by_phase = defaultdict(list)
        for r in rows:
            by_phase[phase_of(r['open_time'])].append(r)

        # 汇总
        print("=" * 100)
        print(f"[strategy_whale 共 {len(rows)} 笔已平 按出场机制演进阶段分期]")
        print("=" * 100)
        print(f"{'phase':<12}{'n':>5}{'wins':>5}{'win%':>7}"
              f"{'pnl_sum':>11}{'pnl_avg':>9}"
              f"{'max':>10}{'min':>10}")
        for phase in ['A 裸奔', 'B 老trail', 'C 完整链']:
            lst = by_phase.get(phase, [])
            if not lst:
                print(f"{phase:<12}  (无)")
                continue
            n = len(lst)
            wins = sum(1 for r in lst if float(r['realized_pnl'] or 0) > 0)
            pnl_sum = sum(float(r['realized_pnl'] or 0) for r in lst)
            pnl_avg = pnl_sum / n
            mx = max(float(r['realized_pnl'] or 0) for r in lst)
            mn = min(float(r['realized_pnl'] or 0) for r in lst)
            wr = wins / n * 100
            print(f"{phase:<12}{n:>5}{wins:>5}{wr:>6.1f}%"
                  f"{pnl_sum:>+11.2f}{pnl_avg:>+9.2f}"
                  f"{mx:>+10.2f}{mn:>+10.2f}")
        print()

        # 每个 phase 的详细明细
        for phase in ['A 裸奔', 'B 老trail', 'C 完整链']:
            lst = by_phase.get(phase, [])
            if not lst:
                continue
            print("-" * 100)
            print(f"[{phase}] 明细  共 {len(lst)} 笔")
            print("-" * 100)
            print(f"  {'id':>5} {'symbol':<14}{'side':<6}{'pnl':>9} {'hold':>5} "
                  f"{'trail':<5}{'maxP%':>7}  {'notes':<18}  {'open':<17}{'source(suffix)':<20}")
            for r in sorted(lst, key=lambda x: float(x['realized_pnl'] or 0)):
                src = (r['source'] or '').replace('strategy_whale:', '')[:20]
                trail = 'Y' if r['trailing_stop_activated'] else '-'
                mp = float(r['max_profit_pct'] or 0)
                print(f"  {r['id']:>5} {r['symbol']:<14}{r['position_side']:<6}"
                      f"{float(r['realized_pnl'] or 0):>+9.2f} "
                      f"{r['hold_min'] or 0:>4}m "
                      f"{trail:<5}{mp:>+6.1f}%"
                      f"  {(r['notes'] or '').strip()[:18]:<18}  "
                      f"{str(r['open_time'])[:16]:<17}{src:<20}")
            print()

        # Phase C 详细 — 最近 trail-tp 上线后
        c_list = by_phase.get('C 完整链', [])
        if c_list:
            print("=" * 100)
            print(f"[Phase C 完整链 深度拆] —— 你最关心的就是这段")
            print("=" * 100)
            print(f"  时间起点: {PHASE_C_START}")
            print(f"  仓位数: {len(c_list)}")
            # 按 notes 分布
            by_notes = defaultdict(list)
            for r in c_list:
                k = (r['notes'] or '').strip() or '(null)'
                by_notes[k].append(r)
            print(f"  按出场原因分布:")
            print(f"    {'reason':<25}{'n':>4}{'pnl':>11}")
            for k, sublist in sorted(by_notes.items(), key=lambda x: -len(x[1])):
                pnl = sum(float(r['realized_pnl'] or 0) for r in sublist)
                print(f"    {k:<25}{len(sublist):>4}{pnl:>+11.2f}")
            # 按子策略
            by_src = defaultdict(list)
            for r in c_list:
                k = (r['source'] or '').replace('strategy_whale:', '')
                by_src[k].append(r)
            print(f"  按子策略分布:")
            print(f"    {'source':<22}{'n':>4}{'wins':>5}{'pnl':>11}")
            for k, sublist in sorted(by_src.items()):
                wins = sum(1 for r in sublist if float(r['realized_pnl'] or 0) > 0)
                pnl = sum(float(r['realized_pnl'] or 0) for r in sublist)
                print(f"    {k:<22}{len(sublist):>4}{wins:>5}{pnl:>+11.2f}")
            # 按方向
            by_side = defaultdict(list)
            for r in c_list:
                by_side[r['position_side']].append(r)
            print(f"  按方向:")
            for side, sublist in sorted(by_side.items()):
                wins = sum(1 for r in sublist if float(r['realized_pnl'] or 0) > 0)
                pnl = sum(float(r['realized_pnl'] or 0) for r in sublist)
                print(f"    {side:<6}n={len(sublist):>3} wins={wins:>2} "
                      f"({wins/len(sublist)*100:.0f}%)  pnl={pnl:+.2f}")

        # 同期 strategy_live 对比 (phase C)
        print()
        print("=" * 100)
        print(f"[同期 (phase C >= {PHASE_C_START}) 其他策略对比]")
        print("=" * 100)
        cur.execute("""
            SELECT
                CASE
                    WHEN source LIKE 'strategy_whale:%%'  THEN 'whale'
                    WHEN source LIKE 'strategy_live:%%'   THEN 'live'
                    WHEN source LIKE 'strategy_bigmid:%%' THEN 'bigmid'
                    WHEN source LIKE 'strategy_f3:%%'     THEN 'f3'
                    ELSE 'other'
                END AS strat,
                COUNT(*) AS n,
                SUM(CASE WHEN realized_pnl>0 THEN 1 ELSE 0 END) AS w,
                SUM(realized_pnl) AS pnl,
                AVG(realized_pnl) AS avg
            FROM futures_positions
            WHERE status IN ('closed','liquidated')
              AND open_time >= %s
            GROUP BY strat ORDER BY pnl DESC
        """, (PHASE_C_START,))
        print(f"  {'strat':<8}{'n':>5}{'wins':>5}{'win%':>7}{'pnl':>11}{'avg':>9}")
        for r in cur.fetchall():
            wr = r['w']/r['n']*100 if r['n'] else 0
            print(f"  {r['strat']:<8}{r['n']:>5}{r['w']:>5}"
                  f"{wr:>6.1f}%{float(r['pnl'] or 0):>+11.2f}"
                  f"{float(r['avg'] or 0):>+9.2f}")

    finally:
        conn.close()


if __name__ == '__main__':
    main()
