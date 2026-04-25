"""
entry-guard 上线后 strategy_live 表现诊断.
分界点: 2026-04-24 21:18 (commit 159dd466 提交时刻).
实际生效时刻以用户重启进程为准, 可能稍晚.

输出:
  1. 守卫上线后所有已平 + 在持仓位
  2. 按子策略 / 出场原因 / 方向
  3. 每笔 top 亏 的 3h 位置百分位 + 24h — 看守卫有没有漏
  4. 和守卫上线前同等时长 (24h) 对比
只读.
"""
import sys
from datetime import datetime, timedelta
from collections import defaultdict
import pymysql

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DB = dict(host='13.212.252.171', port=3306, user='admin', password='Yintao@110',
          db='dimesion', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

GUARD_START = datetime(2026, 4, 24, 21, 18, 0)
BAR_MS_15M = 15 * 60 * 1000


def entry_pct(cur, sym, entry, open_time_ms, bars=12):
    start_ms = open_time_ms - bars * BAR_MS_15M
    cur.execute(
        """SELECT MAX(high_price) AS h, MIN(low_price) AS l
           FROM kline_data
           WHERE symbol=%s AND timeframe='15m'
             AND open_time >= %s AND open_time < %s""",
        (sym, start_ms, open_time_ms),
    )
    r = cur.fetchone()
    if not r or r['h'] is None: return None
    hi = float(r['h']); lo = float(r['l'])
    if hi <= lo: return 50.0
    return (entry - lo) / (hi - lo) * 100


def ch24(cur, sym, open_time_ms):
    ref = open_time_ms - 24*3600*1000
    cur.execute(
        """SELECT close_price FROM kline_data
           WHERE symbol=%s AND timeframe='15m' AND open_time <= %s
           ORDER BY open_time DESC LIMIT 1""", (sym, ref))
    r1 = cur.fetchone()
    cur.execute(
        """SELECT close_price FROM kline_data
           WHERE symbol=%s AND timeframe='15m' AND open_time <= %s
           ORDER BY open_time DESC LIMIT 1""", (sym, open_time_ms))
    r2 = cur.fetchone()
    if not r1 or not r2: return None
    p1 = float(r1['close_price']); p2 = float(r2['close_price'])
    if p1 <= 0: return None
    return (p2 - p1)/p1 * 100


def analyze(cur, rows, label):
    print("=" * 90)
    print(f"[{label}]  {len(rows)} 笔")
    print("=" * 90)
    if not rows:
        return
    closed = [r for r in rows if r['status'] in ('closed','liquidated')]
    opened = [r for r in rows if r['status'] == 'open']
    print(f"  已平 {len(closed)}  在持 {len(opened)}")
    if closed:
        wins = [r for r in closed if float(r['realized_pnl'] or 0) > 0]
        pnl = sum(float(r['realized_pnl'] or 0) for r in closed)
        print(f"  胜率 {len(wins)/len(closed)*100:.1f}%  净 {pnl:+.2f}  "
              f"均 {pnl/len(closed):+.2f}")

    # 按子策略
    by_src = defaultdict(list)
    for r in rows:
        src = (r['source'] or '').replace('strategy_live:', '')[:24]
        by_src[src].append(r)
    print("  -- 按子策略 --")
    for src, lst in sorted(by_src.items()):
        c = [r for r in lst if r['status'] in ('closed','liquidated')]
        o = [r for r in lst if r['status'] == 'open']
        pn = sum(float(r['realized_pnl'] or 0) for r in c)
        w = sum(1 for r in c if float(r['realized_pnl'] or 0) > 0)
        print(f"    {src:<24}n={len(lst):>3} (已平{len(c)} 在持{len(o)}) "
              f"胜{w} pnl={pn:+.2f}")

    # 按出场 notes
    print("  -- 按出场原因 --")
    by_n = defaultdict(list)
    for r in closed:
        k = (r['notes'] or '').strip()[:20] or '(null)'
        by_n[k].append(r)
    for k, lst in sorted(by_n.items(), key=lambda x: -len(x[1])):
        w = sum(1 for r in lst if float(r['realized_pnl'] or 0) > 0)
        pn = sum(float(r['realized_pnl'] or 0) for r in lst)
        print(f"    {k:<22}n={len(lst):>3}  w={w:>2}  pnl={pn:+.2f}")

    # top 5 最亏 + 入场位置画像 (最关键看有没有离谱入场漏过)
    sorted_pnl = sorted(closed, key=lambda r: float(r['realized_pnl'] or 0))
    print("  -- top 5 最亏 + 入场位置 (检查守卫是否漏放) --")
    for r in sorted_pnl[:5]:
        ot_ms = int(r['open_time'].timestamp() * 1000)
        pct = entry_pct(cur, r['symbol'], float(r['entry_price']), ot_ms)
        ch = ch24(cur, r['symbol'], ot_ms)
        ps = f"{pct:+.0f}%" if pct is not None else '?'
        cs = f"{ch:+.1f}%" if ch is not None else '?'
        src = (r['source'] or '').replace('strategy_live:', '')[:20]
        print(f"    #{r['id']} {r['symbol']:<13}{r['position_side']:<6}"
              f"pnl={float(r['realized_pnl'] or 0):>+8.2f}  pos={ps:>7}  24h={cs:>7}  "
              f"{(r['notes'] or '').strip()[:14]:<14}  {src}")

    # top 3 最赢
    if len(sorted_pnl) >= 3:
        print("  -- top 3 最赢 --")
        for r in sorted_pnl[-3:][::-1]:
            if float(r['realized_pnl'] or 0) <= 0:
                break
            ot_ms = int(r['open_time'].timestamp() * 1000)
            pct = entry_pct(cur, r['symbol'], float(r['entry_price']), ot_ms)
            ps = f"{pct:+.0f}%" if pct is not None else '?'
            print(f"    #{r['id']} {r['symbol']:<13}{r['position_side']:<6}"
                  f"pnl={float(r['realized_pnl'] or 0):>+8.2f}  pos={ps:>7}  "
                  f"{(r['notes'] or '').strip()[:14]}")

    # 在持仓位
    if opened:
        print("  -- 在持仓位明细 --")
        for r in opened:
            ot_ms = int(r['open_time'].timestamp() * 1000)
            pct = entry_pct(cur, r['symbol'], float(r['entry_price']), ot_ms)
            ps = f"{pct:+.0f}%" if pct is not None else '?'
            src = (r['source'] or '').replace('strategy_live:', '')[:20]
            print(f"    #{r['id']} {r['symbol']:<13}{r['position_side']:<6}"
                  f"entry={r['entry_price']} pos={ps:>7}  "
                  f"open={r['open_time']}  {src}")


def main():
    conn = pymysql.connect(**DB); cur = conn.cursor()
    try:
        # 守卫上线后
        cur.execute(
            """SELECT id, symbol, position_side, source, entry_price,
                      realized_pnl, notes, open_time, close_time, status
               FROM futures_positions
               WHERE source LIKE 'strategy_live:%%'
                 AND open_time >= %s
               ORDER BY open_time ASC""",
            (GUARD_START,),
        )
        post = cur.fetchall()

        # 守卫上线前等时长对比
        duration = datetime.now() - GUARD_START
        before_end = GUARD_START
        before_start = GUARD_START - duration
        cur.execute(
            """SELECT id, symbol, position_side, source, entry_price,
                      realized_pnl, notes, open_time, close_time, status
               FROM futures_positions
               WHERE source LIKE 'strategy_live:%%'
                 AND open_time >= %s AND open_time < %s
               ORDER BY open_time ASC""",
            (before_start, before_end),
        )
        before = cur.fetchall()

        print(f"分界点: {GUARD_START}  当前: {datetime.now()}")
        print(f"区间时长: {duration}\n")

        analyze(cur, before, f"对照组 ({before_start} ~ {before_end})")
        print()
        analyze(cur, post, f"守卫上线后 (>= {GUARD_START})")
    finally:
        conn.close()


if __name__ == '__main__':
    main()
