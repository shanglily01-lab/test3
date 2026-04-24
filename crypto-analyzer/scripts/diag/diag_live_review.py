"""
strategy_live 最近深度诊断.
目标: 找出 "最近都做不对单" 具体是哪里错了.

三个时间切片:
  - 最近 7 天 (基线分布)
  - Phase C 完整出场链上线后 (2026-04-23 21:30) (最近行为)

分析维度:
  1. 总账 / 按子策略 / 按出场原因 / 按方向
  2. top 10 最亏 + top 10 最赢  (含入场 15M 位置百分位)
  3. 按 symbol 分组 (主力输家)
  4. 入场位置画像 (3h 百分位 + 24h 涨跌 + 近端动量)
  5. 按 "子策略 × 出场原因" 矩阵 —— 看哪个子策略的哪种出场方式最烂
只读.
"""
import sys
from datetime import date, datetime, timedelta
from collections import defaultdict
import pymysql

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DB = dict(host='13.212.252.171', port=3306, user='admin', password='Yintao@110',
          db='dimesion', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

PHASE_C_START = datetime(2026, 4, 23, 21, 30, 0)
BAR_MS_15M = 15 * 60 * 1000


def fmt_src(src: str) -> str:
    if not src:
        return '?'
    return src.replace('strategy_live:', '')[:26]


def fmt_notes(n: str) -> str:
    if not n:
        return '(null)'
    s = n.strip().replace('\n', ' ')[:22]
    return s or '(empty)'


def classify_entry(pct):
    if pct is None:
        return '?'
    if pct > 100:  return '破顶>100%'
    if pct >= 90:  return '近顶>=90%'
    if pct >= 70:  return '偏高70-90'
    if pct >= 30:  return '中间30-70'
    if pct >= 10:  return '偏低10-30'
    if pct >= 0:   return '近底<10'
    return '破底<0'


def entry_percentile(cur, sym, entry, open_time_ms, lookback_bars=12):
    """算开仓前 lookback_bars 根 15M 的区间, entry 价在其中的百分位."""
    start_ms = open_time_ms - lookback_bars * BAR_MS_15M
    cur.execute(
        """SELECT MAX(high_price) AS h, MIN(low_price) AS l
           FROM kline_data
           WHERE symbol=%s AND timeframe='15m'
             AND open_time >= %s AND open_time < %s""",
        (sym, start_ms, open_time_ms),
    )
    r = cur.fetchone()
    if not r or r['h'] is None or r['l'] is None:
        return None
    hi = float(r['h']); lo = float(r['l'])
    if hi <= lo:
        return 50.0
    return (entry - lo) / (hi - lo) * 100


def ch_24h(cur, sym, open_time_ms):
    """24h 涨跌"""
    ref_ms = open_time_ms - 24 * 3600 * 1000
    cur.execute(
        """SELECT close_price FROM kline_data
           WHERE symbol=%s AND timeframe='15m' AND open_time <= %s
           ORDER BY open_time DESC LIMIT 1""",
        (sym, ref_ms),
    )
    ref = cur.fetchone()
    cur.execute(
        """SELECT close_price FROM kline_data
           WHERE symbol=%s AND timeframe='15m' AND open_time <= %s
           ORDER BY open_time DESC LIMIT 1""",
        (sym, open_time_ms),
    )
    cur_r = cur.fetchone()
    if not ref or not cur_r:
        return None
    rp = float(ref['close_price']); cp = float(cur_r['close_price'])
    if rp <= 0: return None
    return (cp - rp) / rp * 100


def phase_of(dt: datetime) -> str:
    return 'C' if dt >= PHASE_C_START else 'pre-C'


def analyze(cur, positions, label):
    print("=" * 100)
    print(f"[{label}]  共 {len(positions)} 笔")
    print("=" * 100)
    if not positions:
        return
    wins = [p for p in positions if float(p['realized_pnl'] or 0) > 0]
    pnl_sum = sum(float(p['realized_pnl'] or 0) for p in positions)
    avg = pnl_sum / len(positions)
    print(f"  胜率 {len(wins)/len(positions)*100:.1f}%  净 pnl {pnl_sum:+.2f}  均 {avg:+.2f}")
    print()

    # 按子策略
    by_src = defaultdict(list)
    for p in positions:
        by_src[fmt_src(p['source'])].append(p)
    print(f"  -- 按子策略 --")
    print(f"  {'source':<28}{'n':>4}{'w':>4}{'win%':>7}{'pnl':>10}{'avg':>9}")
    for src, lst in sorted(by_src.items(), key=lambda x: sum(float(p['realized_pnl'] or 0) for p in x[1])):
        w = sum(1 for p in lst if float(p['realized_pnl'] or 0) > 0)
        pn = sum(float(p['realized_pnl'] or 0) for p in lst)
        wr = w/len(lst)*100
        print(f"  {src:<28}{len(lst):>4}{w:>4}{wr:>6.1f}%{pn:>+10.2f}{pn/len(lst):>+9.2f}")
    print()

    # 按出场原因
    by_notes = defaultdict(list)
    for p in positions:
        by_notes[fmt_notes(p['notes'])].append(p)
    print(f"  -- 按出场原因 --")
    print(f"  {'reason':<24}{'n':>4}{'w':>4}{'win%':>7}{'pnl':>10}{'avg':>9}")
    for rs, lst in sorted(by_notes.items(), key=lambda x: -len(x[1])):
        w = sum(1 for p in lst if float(p['realized_pnl'] or 0) > 0)
        pn = sum(float(p['realized_pnl'] or 0) for p in lst)
        wr = w/len(lst)*100 if lst else 0
        print(f"  {rs:<24}{len(lst):>4}{w:>4}{wr:>6.1f}%{pn:>+10.2f}{pn/len(lst):>+9.2f}")
    print()

    # 按方向
    by_side = defaultdict(list)
    for p in positions:
        by_side[p['position_side']].append(p)
    print(f"  -- 按方向 --")
    for sd, lst in sorted(by_side.items()):
        w = sum(1 for p in lst if float(p['realized_pnl'] or 0) > 0)
        pn = sum(float(p['realized_pnl'] or 0) for p in lst)
        wr = w/len(lst)*100
        print(f"    {sd:<6}n={len(lst):>3} wins={w:>2} ({wr:.0f}%)  pnl={pn:+.2f}")
    print()

    # 子策略 x 出场原因 矩阵
    print(f"  -- 子策略 x 出场原因 矩阵 --")
    matrix = defaultdict(lambda: defaultdict(list))
    for p in positions:
        src = fmt_src(p['source'])
        rs = fmt_notes(p['notes'])
        matrix[src][rs].append(p)
    for src in sorted(matrix.keys()):
        items = matrix[src]
        total_pnl = sum(float(p['realized_pnl'] or 0)
                        for lst in items.values() for p in lst)
        total_n = sum(len(lst) for lst in items.values())
        print(f"    [{src}]  n={total_n}  pnl={total_pnl:+.2f}")
        for rs, lst in sorted(items.items(), key=lambda x: -len(x[1])):
            w = sum(1 for p in lst if float(p['realized_pnl'] or 0) > 0)
            pn = sum(float(p['realized_pnl'] or 0) for p in lst)
            print(f"       {rs:<24}n={len(lst):>3}  w={w:>2}  pnl={pn:>+8.2f}")
    print()

    # top 10 最亏
    sorted_by_pnl = sorted(positions, key=lambda p: float(p['realized_pnl'] or 0))
    print(f"  -- top 10 最亏 (含入场 3h 位置百分位 + 24h%) --")
    for p in sorted_by_pnl[:10]:
        ot_ms = int(p['open_time'].timestamp() * 1000)
        pct = entry_percentile(cur, p['symbol'], float(p['entry_price']), ot_ms)
        ch = ch_24h(cur, p['symbol'], ot_ms)
        pct_s = f"{pct:>+6.0f}%" if pct is not None else '  ?   '
        ch_s = f"{ch:>+5.1f}%" if ch is not None else '  ? '
        print(f"    #{p['id']} {p['symbol']:<14}{p['position_side']:<6}"
              f"pnl={float(p['realized_pnl']):>+8.2f}  "
              f"pos={pct_s}  24h={ch_s}  "
              f"{fmt_notes(p['notes']):<20}  {fmt_src(p['source'])}")
    print()

    # top 10 最赢
    print(f"  -- top 10 最赢 --")
    for p in sorted_by_pnl[-10:][::-1]:
        if float(p['realized_pnl'] or 0) <= 0:
            break
        ot_ms = int(p['open_time'].timestamp() * 1000)
        pct = entry_percentile(cur, p['symbol'], float(p['entry_price']), ot_ms)
        ch = ch_24h(cur, p['symbol'], ot_ms)
        pct_s = f"{pct:>+6.0f}%" if pct is not None else '  ?   '
        ch_s = f"{ch:>+5.1f}%" if ch is not None else '  ? '
        print(f"    #{p['id']} {p['symbol']:<14}{p['position_side']:<6}"
              f"pnl={float(p['realized_pnl']):>+8.2f}  "
              f"pos={pct_s}  24h={ch_s}  "
              f"{fmt_notes(p['notes']):<20}  {fmt_src(p['source'])}")
    print()

    # 按 symbol 聚合 (按净 pnl 升序)
    by_sym = defaultdict(list)
    for p in positions:
        by_sym[p['symbol']].append(p)
    sym_stats = sorted(
        [(s, len(lst), sum(1 for p in lst if float(p['realized_pnl'] or 0) > 0),
          sum(float(p['realized_pnl'] or 0) for p in lst))
         for s, lst in by_sym.items()],
        key=lambda x: x[3],
    )
    print(f"  -- 币种 top 10 亏损户 --")
    for s, n, w, pn in sym_stats[:10]:
        print(f"    {s:<14}n={n:>3} w={w:>3}  pnl={pn:>+9.2f}")
    print(f"  -- 币种 top 10 盈利户 --")
    for s, n, w, pn in sorted(sym_stats, key=lambda x: -x[3])[:10]:
        if pn <= 0: break
        print(f"    {s:<14}n={n:>3} w={w:>3}  pnl={pn:>+9.2f}")

    # 入场位置画像 (仅对已平仓位算百分位)
    pos_buckets = defaultdict(list)
    for p in positions:
        ot_ms = int(p['open_time'].timestamp() * 1000)
        pct = entry_percentile(cur, p['symbol'], float(p['entry_price']), ot_ms)
        if pct is None:
            continue
        pos_buckets[(classify_entry(pct), p['position_side'])].append(p)
    print()
    print(f"  -- 入场位置 x 方向 (3h 百分位) --")
    print(f"  {'bucket':<16}{'side':<6}{'n':>3}{'w':>3}{'win%':>7}{'pnl':>10}")
    for (bk, sd), lst in sorted(pos_buckets.items(), key=lambda x: sum(float(p['realized_pnl'] or 0) for p in x[1])):
        w = sum(1 for p in lst if float(p['realized_pnl'] or 0) > 0)
        pn = sum(float(p['realized_pnl'] or 0) for p in lst)
        wr = w/len(lst)*100
        print(f"  {bk:<16}{sd:<6}{len(lst):>3}{w:>3}{wr:>6.1f}%{pn:>+10.2f}")


def main():
    conn = pymysql.connect(**DB); cur = conn.cursor()
    try:
        now = datetime.now()
        week_ago = now - timedelta(days=7)
        cur.execute("""
            SELECT id, symbol, position_side, source, entry_price,
                   realized_pnl, notes, open_time, close_time,
                   TIMESTAMPDIFF(MINUTE, open_time, close_time) AS hold_min,
                   max_profit_pct, trailing_stop_activated,
                   stop_loss_price, take_profit_price
            FROM futures_positions
            WHERE source LIKE 'strategy_live:%%' AND status IN ('closed','liquidated')
              AND open_time >= %s
            ORDER BY open_time ASC
        """, (week_ago,))
        rows_7d = cur.fetchall()

        # 分 phase
        phase_c = [r for r in rows_7d if r['open_time'] >= PHASE_C_START]

        analyze(cur, rows_7d, "最近 7 天 strategy_live")
        print("\n")
        analyze(cur, phase_c, f"Phase C 完整出场链 (>= {PHASE_C_START}) strategy_live")
    finally:
        conn.close()


if __name__ == '__main__':
    main()
