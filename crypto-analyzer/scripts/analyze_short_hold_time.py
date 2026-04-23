# -*- coding: utf-8 -*-
"""
分析做空持仓时长与利润的关系，找出最优持仓时长。
"""

import sys
from pathlib import Path
from dotenv import dotenv_values
import pymysql

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

env = dotenv_values(ROOT / '.env')


def get_conn():
    return pymysql.connect(
        host=env.get('DB_HOST', 'localhost'),
        port=int(env.get('DB_PORT', 3306)),
        user=env.get('DB_USER', 'root'),
        password=env.get('DB_PASSWORD', ''),
        database=env.get('DB_NAME', ''),
        cursorclass=pymysql.cursors.DictCursor,
    )


def main():
    conn = get_conn()
    cur = conn.cursor()

    # 查所有已平仓 SHORT 仓位，含持仓时长和盈亏
    cur.execute("""
        SELECT
            symbol,
            source,
            entry_price,
            realized_pnl,
            realized_pnl / NULLIF(margin, 0) AS pnl_pct,
            open_time,
            close_time,
            notes,
            TIMESTAMPDIFF(MINUTE, open_time, close_time) AS hold_minutes
        FROM futures_positions
        WHERE position_side = 'LONG'
          AND status = 'closed'
          AND close_time IS NOT NULL
          AND open_time IS NOT NULL
        ORDER BY open_time DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    if not rows:
        print("没有找到已平仓的 SHORT 仓位记录")
        return

    print(f"共找到 {len(rows)} 条已平仓 SHORT 记录\n")

    # 按持仓时长分桶（小时）
    buckets = {}
    for r in rows:
        hold_h = (r['hold_minutes'] or 0) / 60
        bucket = int(hold_h)  # 向下取整到小时
        pnl = float(r['realized_pnl'] or 0)
        if bucket not in buckets:
            buckets[bucket] = {'pnls': [], 'wins': 0, 'losses': 0}
        buckets[bucket]['pnls'].append(pnl)
        if pnl > 0:
            buckets[bucket]['wins'] += 1
        else:
            buckets[bucket]['losses'] += 1

    print(f"{'持仓时长':>10} {'笔数':>6} {'胜率':>8} {'平均盈亏':>12} {'累计盈亏':>12} {'最大盈':>10} {'最大亏':>10}")
    print('-' * 72)

    cum = 0.0
    for h in sorted(buckets.keys()):
        b = buckets[h]
        pnls = b['pnls']
        avg = sum(pnls) / len(pnls)
        total = sum(pnls)
        cum += total
        win_rate = b['wins'] / len(pnls) * 100
        print(f"{h:>8}h~{h+1}h  {len(pnls):>5}笔  {win_rate:>6.1f}%  {avg:>+11.4f}  {total:>+11.4f}  {max(pnls):>+9.4f}  {min(pnls):>+9.4f}")

    print()
    # 找出平均盈亏最高的时段
    best_h = max(buckets, key=lambda h: sum(buckets[h]['pnls']) / len(buckets[h]['pnls']))
    best_pnls = buckets[best_h]['pnls']
    print(f"平均盈亏最高: {best_h}h~{best_h+1}h  (avg={sum(best_pnls)/len(best_pnls):+.4f}  n={len(best_pnls)})")

    best_wr_h = max(buckets, key=lambda h: buckets[h]['wins'] / len(buckets[h]['pnls']))
    wr_b = buckets[best_wr_h]
    print(f"胜率最高:     {best_wr_h}h~{best_wr_h+1}h  (胜率={wr_b['wins']/len(wr_b['pnls'])*100:.1f}%  n={len(wr_b['pnls'])})")

    # 按 close_reason 统计
    print("\n--- 按平仓原因统计 ---")
    reasons = {}
    for r in rows:
        rr = r.get('notes') or r.get('source') or 'unknown'
        pnl = float(r['realized_pnl'] or 0)
        hold_h = (r['hold_minutes'] or 0) / 60
        if rr not in reasons:
            reasons[rr] = {'pnls': [], 'hold_hs': []}
        reasons[rr]['pnls'].append(pnl)
        reasons[rr]['hold_hs'].append(hold_h)

    for rr, d in sorted(reasons.items(), key=lambda x: -len(x[1]['pnls'])):
        avg_pnl = sum(d['pnls']) / len(d['pnls'])
        avg_hold = sum(d['hold_hs']) / len(d['hold_hs'])
        wins = sum(1 for p in d['pnls'] if p > 0)
        print(f"  {rr:<25} n={len(d['pnls']):>4}  avg_pnl={avg_pnl:>+8.4f}  avg_hold={avg_hold:>5.1f}h  胜率={wins/len(d['pnls'])*100:.0f}%")


if __name__ == '__main__':
    main()
