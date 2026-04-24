"""
位置百分位过滤回放:
  对最近 N 天所有已平仓位, 算开仓时刻 symbol 在三个时间窗 (3h/12h/3d) 的 15M 区间百分位
  模拟过滤规则: LONG 时 pct > X 拒绝 (追高), SHORT 时 pct < Y 拒绝 (踩底)
  算出"如果启用此过滤, 能挡住多少笔, 挽回多少 pnl, 误伤多少盈利单"

百分位定义:
  pct = (entry - window_low) / (window_high - window_low) * 100
  0% = 区间最低, 100% = 区间最高, >100% = 已突破区间最高, <0% = 已跌穿区间最低

时间窗:
  短: 12 根 15M = 3 小时
  中: 48 根 15M = 12 小时
  长: 288 根 15M = 3 天

用法:
  python scripts/diag/replay_position_percentile.py [DAYS]  # 默认 7
"""
import sys
from datetime import date, timedelta
import pymysql

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DB = dict(host='13.212.252.171', port=3306, user='admin', password='Yintao@110',
          db='dimesion', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

DAYS = int(sys.argv[1]) if len(sys.argv) > 1 else 7
WINDOWS = [('3h', 12), ('12h', 48), ('3d', 288)]  # (label, bars)
BAR_MS = 15 * 60 * 1000


def percentile_in_window(entry: float, high: float, low: float) -> float:
    if high <= low:
        return 50.0
    return (entry - low) / (high - low) * 100


def classify(pct: float) -> str:
    if pct is None:
        return '?'
    if pct > 100:
        return '破顶 >100%'
    if pct >= 90:
        return '近顶 >=90%'
    if pct >= 70:
        return '偏高 70-90%'
    if pct >= 30:
        return '中间 30-70%'
    if pct >= 10:
        return '偏低 10-30%'
    if pct >= 0:
        return '近底 <10%'
    return '破底 <0%'


def main():
    conn = pymysql.connect(**DB); cur = conn.cursor()
    try:
        end = date.today(); start = end - timedelta(days=DAYS - 1)
        cur.execute(
            """SELECT id, symbol, position_side, entry_price, realized_pnl,
                      open_time, close_time, notes,
                      TIMESTAMPDIFF(MINUTE, open_time, close_time) as hold_min
               FROM futures_positions
               WHERE DATE(close_time) BETWEEN %s AND %s
                 AND status IN ('closed','liquidated')
               ORDER BY open_time ASC""",
            (start.isoformat(), end.isoformat()),
        )
        positions = cur.fetchall()
        print(f"\n### {start} ~ {end}  共 {len(positions)} 个已平仓位 ###\n")

        # 逐笔算 3 个时间窗的百分位
        enriched = []
        miss = 0
        for p in positions:
            sym = p['symbol']
            entry = float(p['entry_price'])
            ot_ms = int(p['open_time'].timestamp() * 1000)
            pcts = {}
            for label, bars in WINDOWS:
                look_start = ot_ms - bars * BAR_MS
                cur.execute(
                    """SELECT MAX(high_price) AS h, MIN(low_price) AS l, COUNT(*) AS n
                       FROM kline_data
                       WHERE symbol=%s AND timeframe='15m'
                         AND open_time >= %s AND open_time < %s""",
                    (sym, look_start, ot_ms),
                )
                r = cur.fetchone()
                if not r or r['n'] == 0 or r['h'] is None:
                    pcts[label] = None
                else:
                    pcts[label] = percentile_in_window(entry, float(r['h']), float(r['l']))
            if pcts['3h'] is None:
                miss += 1
            enriched.append({'p': p, 'pcts': pcts})

        print(f"有 kline 数据的仓位: {len(enriched) - miss}/{len(enriched)}  (缺数据 {miss} 笔)\n")

        # 按 3h 窗口百分位分桶
        print("=" * 90)
        print("[按 3h 窗口百分位分桶]  (pct = 开仓价在前 3h 15M 区间的位置)")
        print("=" * 90)
        buckets = {}
        for e in enriched:
            pct = e['pcts']['3h']
            if pct is None:
                continue
            key = (classify(pct), e['p']['position_side'])
            buckets.setdefault(key, []).append(e)
        print(f"{'bucket':<20}{'side':<6}{'n':>4}{'wins':>5}{'losses':>7}{'net_pnl':>11}{'avg':>9}")
        for (bucket, side), lst in sorted(buckets.items(), key=lambda x: x[0]):
            wins = sum(1 for e in lst if float(e['p']['realized_pnl'] or 0) > 0)
            losses = sum(1 for e in lst if float(e['p']['realized_pnl'] or 0) < 0)
            pnl = sum(float(e['p']['realized_pnl'] or 0) for e in lst)
            print(f"{bucket:<20}{side:<6}{len(lst):>4}{wins:>5}{losses:>7}{pnl:>+11.2f}{pnl/len(lst):>+9.2f}")
        print()

        # 模拟不同过滤阈值
        print("=" * 90)
        print("[过滤规则回测]  (模拟启用位置过滤后的效果)")
        print("=" * 90)
        rules = [
            ('LONG 时 3h pct > 90 拒绝', lambda e: e['p']['position_side'] == 'LONG' and e['pcts']['3h'] is not None and e['pcts']['3h'] > 90),
            ('LONG 时 3h pct > 85 拒绝', lambda e: e['p']['position_side'] == 'LONG' and e['pcts']['3h'] is not None and e['pcts']['3h'] > 85),
            ('LONG 时 3h pct > 80 拒绝', lambda e: e['p']['position_side'] == 'LONG' and e['pcts']['3h'] is not None and e['pcts']['3h'] > 80),
            ('SHORT 时 3h pct < 10 拒绝', lambda e: e['p']['position_side'] == 'SHORT' and e['pcts']['3h'] is not None and e['pcts']['3h'] < 10),
            ('SHORT 时 3h pct < 20 拒绝', lambda e: e['p']['position_side'] == 'SHORT' and e['pcts']['3h'] is not None and e['pcts']['3h'] < 20),
            ('双向: LONG pct>90 或 SHORT pct<10', lambda e: (e['p']['position_side'] == 'LONG' and e['pcts']['3h'] is not None and e['pcts']['3h'] > 90) or (e['p']['position_side'] == 'SHORT' and e['pcts']['3h'] is not None and e['pcts']['3h'] < 10)),
            ('双向 (更严): LONG >85 或 SHORT <15', lambda e: (e['p']['position_side'] == 'LONG' and e['pcts']['3h'] is not None and e['pcts']['3h'] > 85) or (e['p']['position_side'] == 'SHORT' and e['pcts']['3h'] is not None and e['pcts']['3h'] < 15)),
            ('破顶破底: pct>100 或 pct<0', lambda e: e['pcts']['3h'] is not None and (e['pcts']['3h'] > 100 or e['pcts']['3h'] < 0)),
        ]

        base_pnl = sum(float(e['p']['realized_pnl'] or 0) for e in enriched)
        base_losses = sum(float(e['p']['realized_pnl'] or 0) for e in enriched if float(e['p']['realized_pnl'] or 0) < 0)
        base_wins = sum(float(e['p']['realized_pnl'] or 0) for e in enriched if float(e['p']['realized_pnl'] or 0) > 0)
        print(f"基线 (不过滤):  总笔数={len(enriched)}  总 pnl={base_pnl:+.2f}  盈利合计={base_wins:+.2f}  亏损合计={base_losses:+.2f}\n")

        print(f"{'rule':<42}{'挡单':>6}{'避开亏损':>11}{'误伤盈利':>11}{'净改善':>11}{'改善率':>8}")
        for rule_name, predicate in rules:
            blocked = [e for e in enriched if predicate(e)]
            blocked_pnl = sum(float(e['p']['realized_pnl'] or 0) for e in blocked)
            blocked_losses = sum(float(e['p']['realized_pnl'] or 0) for e in blocked if float(e['p']['realized_pnl'] or 0) < 0)
            blocked_wins = sum(float(e['p']['realized_pnl'] or 0) for e in blocked if float(e['p']['realized_pnl'] or 0) > 0)
            # 启用过滤后的 pnl = 基线 pnl - blocked_pnl (不开的仓就没这些盈亏)
            new_pnl = base_pnl - blocked_pnl
            delta = new_pnl - base_pnl   # 正数 = 改善
            improve_rate = (delta / abs(base_pnl) * 100) if base_pnl else 0
            print(f"{rule_name:<42}{len(blocked):>6}"
                  f"{-blocked_losses:>+11.2f}{-blocked_wins:>+11.2f}"
                  f"{delta:>+11.2f}{improve_rate:>+7.1f}%")

        # 细看破顶破底 (pct > 100 或 < 0) 的仓位明细
        print()
        print("=" * 90)
        print("[破顶破底明细 (pct > 100 或 < 0) - 这批是最离谱的]")
        print("=" * 90)
        extreme = [e for e in enriched if e['pcts']['3h'] is not None and (e['pcts']['3h'] > 100 or e['pcts']['3h'] < 0)]
        extreme.sort(key=lambda e: float(e['p']['realized_pnl'] or 0))
        print(f"  共 {len(extreme)} 笔")
        print(f"  {'id':>5} {'symbol':<14}{'side':<6}{'3h%':>7}{'12h%':>7}{'3d%':>7}  "
              f"{'pnl':>9}  notes")
        for e in extreme[:30]:
            p = e['p']; pcts = e['pcts']
            pnl = float(p['realized_pnl'] or 0)
            def fmt(v): return f"{v:>+7.1f}" if v is not None else "   N/A"
            print(f"  {p['id']:>5} {p['symbol']:<14}{p['position_side']:<6}"
                  f"{fmt(pcts['3h'])}{fmt(pcts['12h'])}{fmt(pcts['3d'])}  "
                  f"{pnl:>+9.2f}  {(p['notes'] or '').strip()[:40]}")
    finally:
        conn.close()


if __name__ == '__main__':
    main()
