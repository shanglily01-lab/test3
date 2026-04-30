"""
深挖 chase-entry + f3-entry 入场质量, 回答 "什么样的 K 线特征导致开错方向".

七个分析切片:
  S1. 两个 source 的基线 (笔数 / 胜率 / 净 pnl)
  S2. 入场前涨跌幅: 24h / 4h / 1h 各自的赢单 vs 亏单分布
  S3. 入场点在多周期极值的位置 (1h / 4h / 24h)
  S4. 入场后实际走势: 30m / 1h / 3h 价格变化 vs 入场方向
       (入场即反向 = 开仓后 30m 内已经反向 0.5%+)
  S5. chase-entry: trail-tp 赢单 vs early-sl 亏单的入场前特征对比
  S6. f3-entry:    trail-tp 赢单 vs early-sl 亏单的入场前特征对比
  S7. 给出可识别的 "赚钱 setup" 和 "亏钱 setup" 模式

DB 配置从 table_schemas.txt 头部解析.
只读, 不开仓, 不改 DB.
用法: python scripts/diag/diag_chase_f3_entry_quality.py
"""
import sys
import os
import re
from collections import defaultdict
from statistics import mean, median
import pymysql

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')


def _load_db_from_schema() -> dict:
    schema_path = os.path.join(os.path.dirname(__file__), '..', '..', 'table_schemas.txt')
    cfg = dict(host='54.179.112.251', port=3306, user='admin',
               password='Yintao@110', db='dimesion')
    try:
        with open(schema_path, 'r', encoding='utf-8', errors='replace') as f:
            head = f.read(2000)
        for k, pat in [('host', r'host:\s*([\d.]+)'),
                       ('port', r'port:\s*(\d+)'),
                       ('user', r'user:\s*(\S+)'),
                       ('password', r'password:\s*(\S+)'),
                       ('db', r'database:\s*(\S+)')]:
            m = re.search(pat, head)
            if m:
                cfg[k] = int(m.group(1)) if k == 'port' else m.group(1)
    except Exception as e:
        print(f"[警告] 读 table_schemas.txt 失败: {e}")
    cfg['charset'] = 'utf8mb4'
    cfg['cursorclass'] = pymysql.cursors.DictCursor
    return cfg


DB = _load_db_from_schema()
ACCOUNT_ID = 2
DAYS = 7
TARGET_SUBS = ('chase-entry', 'f3-entry')  # 只挖这两类


def fetch_target_positions(cur):
    """拉 chase-entry / f3-entry 全部 closed paper 仓位."""
    cur.execute("""
        SELECT
            p.id            AS pid,
            p.symbol,
            p.position_side AS side,
            p.entry_price,
            p.realized_pnl,
            p.holding_hours,
            p.open_time,
            p.close_time,
            p.notes,
            (SELECT o.order_source FROM futures_orders o
              WHERE o.position_id = p.id
                AND o.side IN ('OPEN_LONG', 'OPEN_SHORT')
              ORDER BY o.id ASC LIMIT 1) AS open_order_source
        FROM futures_positions p
        WHERE p.account_id = %s
          AND p.status = 'closed'
          AND p.close_time >= NOW() - INTERVAL %s DAY
        ORDER BY p.open_time ASC
    """, (ACCOUNT_ID, DAYS))
    rows = cur.fetchall()
    out = []
    for r in rows:
        src = r.get('open_order_source') or ''
        if not any(t in src for t in TARGET_SUBS):
            continue
        sub = 'chase-entry' if 'chase-entry' in src else 'f3-entry'
        notes = r.get('notes') or ''
        nl = notes.lower()
        if 'breakeven' in nl: cr = 'breakeven-sl'
        elif 'early-sl' in nl: cr = 'early-sl'
        elif 'trail-tp' in nl: cr = 'trail-tp'
        elif 'hard-tp' in nl: cr = 'hard-tp'
        elif 'timeout' in nl: cr = 'timeout'
        elif '止损' in notes: cr = 'sl'
        elif '止盈' in notes: cr = 'hard-tp'
        elif 'manual' in nl or '手动' in notes: cr = 'manual'
        else: cr = 'unknown'
        out.append({
            'pid': r['pid'],
            'symbol': r['symbol'],
            'side': r['side'],
            'entry': float(r['entry_price'] or 0),
            'pnl': float(r['realized_pnl'] or 0),
            'open_time': r['open_time'],
            'close_time': r['close_time'],
            'sub_tag': sub,
            'close_reason': cr,
            'is_win': float(r['realized_pnl'] or 0) > 0,
        })
    return out


def fetch_prior_kline_stats(cur, sym: str, open_ms: int) -> dict:
    """开仓时刻前的多周期统计."""
    out = {}
    # 24h 1h K 线
    cur.execute("""
        SELECT MIN(low_price) lo, MAX(high_price) hi,
               (SELECT close_price FROM kline_data
                  WHERE symbol=%s AND timeframe='1h' AND open_time < %s
                  ORDER BY open_time DESC LIMIT 1 OFFSET 23) AS open_24h_ago
        FROM kline_data
        WHERE symbol=%s AND timeframe='1h'
          AND open_time >= %s - 24*3600*1000
          AND open_time <  %s
    """, (sym, open_ms, sym, open_ms, open_ms))
    r = cur.fetchone() or {}
    out['lo_24h'] = float(r['lo']) if r.get('lo') else None
    out['hi_24h'] = float(r['hi']) if r.get('hi') else None
    out['p_24h_ago'] = float(r['open_24h_ago']) if r.get('open_24h_ago') else None

    # 4h 5m K 线
    cur.execute("""
        SELECT MIN(low_price) lo, MAX(high_price) hi,
               (SELECT close_price FROM kline_data
                  WHERE symbol=%s AND timeframe='5m' AND open_time < %s
                  ORDER BY open_time DESC LIMIT 1 OFFSET 47) AS p_4h_ago
        FROM kline_data
        WHERE symbol=%s AND timeframe='5m'
          AND open_time >= %s - 4*3600*1000
          AND open_time <  %s
    """, (sym, open_ms, sym, open_ms, open_ms))
    r = cur.fetchone() or {}
    out['lo_4h'] = float(r['lo']) if r.get('lo') else None
    out['hi_4h'] = float(r['hi']) if r.get('hi') else None
    out['p_4h_ago'] = float(r['p_4h_ago']) if r.get('p_4h_ago') else None

    # 1h 5m K 线
    cur.execute("""
        SELECT MIN(low_price) lo, MAX(high_price) hi,
               (SELECT close_price FROM kline_data
                  WHERE symbol=%s AND timeframe='5m' AND open_time < %s
                  ORDER BY open_time DESC LIMIT 1 OFFSET 11) AS p_1h_ago
        FROM kline_data
        WHERE symbol=%s AND timeframe='5m'
          AND open_time >= %s - 3600*1000
          AND open_time <  %s
    """, (sym, open_ms, sym, open_ms, open_ms))
    r = cur.fetchone() or {}
    out['lo_1h'] = float(r['lo']) if r.get('lo') else None
    out['hi_1h'] = float(r['hi']) if r.get('hi') else None
    out['p_1h_ago'] = float(r['p_1h_ago']) if r.get('p_1h_ago') else None

    return out


def fetch_post_entry_moves(cur, sym: str, open_ms: int, entry: float) -> dict:
    """开仓后 30m / 1h / 3h 的价格."""
    out = {'p_30m': None, 'p_1h': None, 'p_3h': None,
           'low_30m': None, 'high_30m': None}
    # 用 5m K 线第 1, 6, 11, 35 根的 close
    cur.execute("""
        SELECT open_time, close_price, high_price, low_price
        FROM kline_data
        WHERE symbol=%s AND timeframe='5m'
          AND open_time >= %s
          AND open_time <  %s + 4*3600*1000
        ORDER BY open_time ASC LIMIT 50
    """, (sym, open_ms, open_ms))
    bars = cur.fetchall()
    if not bars:
        return out
    # 5m bars 第 6 根 (idx 5) ≈ 30m, 第 12 (idx 11) ≈ 1h, 第 36 (idx 35) ≈ 3h
    if len(bars) >= 6:
        out['p_30m'] = float(bars[5]['close_price'])
        out['low_30m']  = min(float(b['low_price']) for b in bars[:6])
        out['high_30m'] = max(float(b['high_price']) for b in bars[:6])
    if len(bars) >= 12:
        out['p_1h'] = float(bars[11]['close_price'])
    if len(bars) >= 36:
        out['p_3h'] = float(bars[35]['close_price'])
    return out


def enrich(cur, positions: list):
    """给每笔补 prior_stats + post_moves."""
    enriched = []
    for p in positions:
        if not p['open_time'] or p['entry'] <= 0:
            continue
        open_ms = int(p['open_time'].timestamp() * 1000)
        try:
            prior = fetch_prior_kline_stats(cur, p['symbol'], open_ms)
            post  = fetch_post_entry_moves(cur, p['symbol'], open_ms, p['entry'])
        except Exception as e:
            print(f"  [skip] {p['symbol']} pid={p['pid']}: {e}")
            continue
        # 派生指标
        ch_24h = (p['entry'] - prior['p_24h_ago']) / prior['p_24h_ago'] if prior['p_24h_ago'] else None
        ch_4h  = (p['entry'] - prior['p_4h_ago'])  / prior['p_4h_ago']  if prior['p_4h_ago']  else None
        ch_1h  = (p['entry'] - prior['p_1h_ago'])  / prior['p_1h_ago']  if prior['p_1h_ago']  else None
        rel_24h = (p['entry'] - prior['lo_24h']) / (prior['hi_24h'] - prior['lo_24h']) \
                  if prior['hi_24h'] and prior['lo_24h'] and prior['hi_24h'] > prior['lo_24h'] else None
        rel_4h  = (p['entry'] - prior['lo_4h']) / (prior['hi_4h'] - prior['lo_4h']) \
                  if prior['hi_4h'] and prior['lo_4h'] and prior['hi_4h'] > prior['lo_4h'] else None
        # 入场后变化(对方向化, LONG 的"顺向" = 价格涨, SHORT 的"顺向" = 价格跌)
        sign = 1 if p['side'] == 'LONG' else -1
        move_30m = sign * (post['p_30m'] - p['entry']) / p['entry'] if post['p_30m'] else None
        move_1h  = sign * (post['p_1h']  - p['entry']) / p['entry'] if post['p_1h']  else None
        move_3h  = sign * (post['p_3h']  - p['entry']) / p['entry'] if post['p_3h']  else None
        # 入场后 30m 内反向极值 (LONG 用 low, SHORT 用 high)
        if p['side'] == 'LONG' and post['low_30m']:
            adverse_30m = (post['low_30m'] - p['entry']) / p['entry']  # 负值越深越糟
        elif p['side'] == 'SHORT' and post['high_30m']:
            adverse_30m = -(post['high_30m'] - p['entry']) / p['entry']
        else:
            adverse_30m = None
        enriched.append({
            **p,
            'ch_24h': ch_24h, 'ch_4h': ch_4h, 'ch_1h': ch_1h,
            'rel_24h': rel_24h, 'rel_4h': rel_4h,
            'move_30m': move_30m, 'move_1h': move_1h, 'move_3h': move_3h,
            'adverse_30m': adverse_30m,
        })
    return enriched


def section(title: str):
    print(); print("=" * 100); print(f" {title}"); print("=" * 100)


def s1_baseline(positions: list):
    section("S1. chase-entry / f3-entry 基线")
    by_sub = defaultdict(list)
    for p in positions:
        by_sub[p['sub_tag']].append(p)
    print(f"  {'sub':<14} {'笔数':>5} {'赢':>4} {'胜率':>7} {'净PnL':>11} {'均PnL':>10}")
    for sub in TARGET_SUBS:
        ps = by_sub.get(sub, [])
        if not ps:
            continue
        n = len(ps)
        wins = sum(1 for p in ps if p['is_win'])
        net = sum(p['pnl'] for p in ps)
        avg = net / n
        print(f"  {sub:<14} {n:>5d} {wins:>4d} {wins/n*100:>6.1f}% {net:>+10.2f}U {avg:>+9.2f}U")


def _stat_line(label: str, values: list, fmt='{:+.2f}%', mult=100):
    if not values:
        return f"  {label:<40} N/A"
    vs = [v * mult for v in values]
    return (f"  {label:<40} n={len(vs):>3d}  "
            f"中位 {fmt.format(median(vs))}  "
            f"均值 {fmt.format(mean(vs))}  "
            f"min {fmt.format(min(vs))}  max {fmt.format(max(vs))}")


def s2_pre_entry_changes(positions: list):
    section("S2. 入场前涨跌幅: 24h / 4h / 1h 分布 (赢单 vs 亏单)")
    for sub in TARGET_SUBS:
        print(f"\n  [{sub}]")
        ps = [p for p in positions if p['sub_tag'] == sub]
        wins = [p for p in ps if p['is_win']]
        losses = [p for p in ps if not p['is_win']]
        for window, key in [('24h', 'ch_24h'), ('4h', 'ch_4h'), ('1h', 'ch_1h')]:
            print(f"    入场前 {window} 涨跌幅:")
            print(_stat_line(f"      赢单 ({sub})",   [p[key] for p in wins   if p[key] is not None]))
            print(_stat_line(f"      亏单 ({sub})",   [p[key] for p in losses if p[key] is not None]))


def s3_entry_position(positions: list):
    section("S3. 入场点在多周期极值的相对位置 (rel: 0=底部, 1=顶部)")
    for sub in TARGET_SUBS:
        print(f"\n  [{sub}]")
        ps = [p for p in positions if p['sub_tag'] == sub]
        for window, key in [('4h', 'rel_4h'), ('24h', 'rel_24h')]:
            print(f"    入场点在 {window} 区间相对位置:")
            for label, mask in [('LONG 赢', lambda x: x['side'] == 'LONG' and x['is_win']),
                                ('LONG 亏', lambda x: x['side'] == 'LONG' and not x['is_win']),
                                ('SHORT 赢', lambda x: x['side'] == 'SHORT' and x['is_win']),
                                ('SHORT 亏', lambda x: x['side'] == 'SHORT' and not x['is_win'])]:
                vs = [p[key] for p in ps if mask(p) and p[key] is not None]
                print(_stat_line(f"      {label}", vs, '{:.2f}', 1))


def s4_post_entry(positions: list):
    section("S4. 入场后 30m / 1h / 3h 顺向价格变化 (正=赚, 负=反向)")
    for sub in TARGET_SUBS:
        print(f"\n  [{sub}]")
        ps = [p for p in positions if p['sub_tag'] == sub]
        for window, key in [('30m', 'move_30m'), ('1h', 'move_1h'), ('3h', 'move_3h')]:
            vs = [p[key] for p in ps if p[key] is not None]
            print(_stat_line(f"    全部 {window} 顺向变化", vs))
        # 30m 内反向极值
        adv = [p['adverse_30m'] for p in ps if p['adverse_30m'] is not None]
        if adv:
            adv_pct = [v * 100 for v in adv]
            below_neg1 = sum(1 for v in adv_pct if v < -1)
            below_neg2 = sum(1 for v in adv_pct if v < -2)
            print(f"    30m 内最大反向幅度: 中位 {median(adv_pct):.2f}% / "
                  f"≤-1% {below_neg1}/{len(adv_pct)} / ≤-2% {below_neg2}/{len(adv_pct)}")


def s5_s6_compare_winning_vs_losing_setup(positions: list, sub_tag: str, section_no: str):
    section(f"{section_no}. {sub_tag} 赢单 (trail-tp) vs 亏单 (early-sl) 入场特征对比")
    ps = [p for p in positions if p['sub_tag'] == sub_tag]
    win_ps = [p for p in ps if p['close_reason'] == 'trail-tp']
    lose_ps = [p for p in ps if p['close_reason'] in ('early-sl', 'breakeven-sl', 'sl')]

    print(f"  trail-tp 赢单 n={len(win_ps)},  early/breakeven/sl 亏单 n={len(lose_ps)}")
    if not win_ps or not lose_ps:
        print("  样本不足.")
        return

    print()
    print("  【入场前 1h 涨跌幅】(短线动能)")
    print(_stat_line("    赢单",  [p['ch_1h'] for p in win_ps  if p['ch_1h'] is not None]))
    print(_stat_line("    亏单",  [p['ch_1h'] for p in lose_ps if p['ch_1h'] is not None]))

    print("  【入场前 4h 涨跌幅】")
    print(_stat_line("    赢单",  [p['ch_4h'] for p in win_ps  if p['ch_4h'] is not None]))
    print(_stat_line("    亏单",  [p['ch_4h'] for p in lose_ps if p['ch_4h'] is not None]))

    print("  【入场前 24h 涨跌幅】(中线趋势)")
    print(_stat_line("    赢单",  [p['ch_24h'] for p in win_ps  if p['ch_24h'] is not None]))
    print(_stat_line("    亏单",  [p['ch_24h'] for p in lose_ps if p['ch_24h'] is not None]))

    print("  【入场点在 4h 区间位置】(rel: 0=底, 1=顶)")
    print(_stat_line("    赢单",  [p['rel_4h'] for p in win_ps  if p['rel_4h'] is not None], '{:.2f}', 1))
    print(_stat_line("    亏单",  [p['rel_4h'] for p in lose_ps if p['rel_4h'] is not None], '{:.2f}', 1))

    print("  【入场点在 24h 区间位置】")
    print(_stat_line("    赢单",  [p['rel_24h'] for p in win_ps  if p['rel_24h'] is not None], '{:.2f}', 1))
    print(_stat_line("    亏单",  [p['rel_24h'] for p in lose_ps if p['rel_24h'] is not None], '{:.2f}', 1))

    print("  【30m 内最大反向】")
    print(_stat_line("    赢单",  [p['adverse_30m'] for p in win_ps  if p['adverse_30m'] is not None]))
    print(_stat_line("    亏单",  [p['adverse_30m'] for p in lose_ps if p['adverse_30m'] is not None]))


def s7_pattern_summary(positions: list):
    section("S7. 可复用的 setup 模式 (基于上面切片的归纳)")

    for sub in TARGET_SUBS:
        ps = [p for p in positions if p['sub_tag'] == sub]
        wins = [p for p in ps if p['close_reason'] == 'trail-tp']
        losses = [p for p in ps if p['close_reason'] in ('early-sl', 'breakeven-sl', 'sl')]
        if not wins or not losses:
            continue
        # 算赢/亏单的关键中位差
        def _med(arr, key):
            vs = [p[key] for p in arr if p[key] is not None]
            return median(vs) if vs else None

        print(f"\n  [{sub}]  赢单 (trail-tp) vs 亏单 (sl 类) 中位数对比:")
        for label, key, fmt in [
            ('入场前 1h%',   'ch_1h',  '{:+.2f}%'),
            ('入场前 4h%',   'ch_4h',  '{:+.2f}%'),
            ('入场前 24h%',  'ch_24h', '{:+.2f}%'),
            ('入场点 rel_4h','rel_4h', '{:.2f}'),
            ('入场点 rel_24h','rel_24h','{:.2f}'),
        ]:
            mw = _med(wins, key)
            ml = _med(losses, key)
            mult = 100 if '%' in fmt else 1
            mws = fmt.format(mw * mult) if mw is not None else 'N/A'
            mls = fmt.format(ml * mult) if ml is not None else 'N/A'
            diff = ''
            if mw is not None and ml is not None:
                d = (mw - ml) * mult
                if abs(d) > (3 if '%' in fmt else 0.1):
                    diff = f"  <-- 差 {d:+.2f}{'%' if '%' in fmt else ''}"
            print(f"    {label:<18} 赢中位 {mws:>9}  亏中位 {mls:>9}{diff}")


def main():
    print(f"连库 {DB['user']}@{DB['host']}:{DB['port']}/{DB['db']}")
    conn = pymysql.connect(**DB)
    cur = conn.cursor()
    try:
        positions = fetch_target_positions(cur)
        if not positions:
            print(f"近 {DAYS} 天无 chase-entry / f3-entry 单.")
            return
        print(f"取到 {len(positions)} 笔目标单, 富化 K 线特征...")
        positions = enrich(cur, positions)
        print(f"富化完成 {len(positions)} 笔.")

        s1_baseline(positions)
        s2_pre_entry_changes(positions)
        s3_entry_position(positions)
        s4_post_entry(positions)
        s5_s6_compare_winning_vs_losing_setup(positions, 'chase-entry', 'S5')
        s5_s6_compare_winning_vs_losing_setup(positions, 'f3-entry',    'S6')
        s7_pattern_summary(positions)
    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    main()
