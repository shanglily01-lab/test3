"""
诊断近 7 天 (account_id=2 paper) 的策略亏损来源, 重点回答 "为什么会开错方向".

七个分析切片:
  S1. 总体: 笔数 / 胜率 / 净 pnl / 平均盈亏比
  S2. 按 source 分组: 各 sub-strategy 胜率 / 净 pnl
  S3. 按 close_reason 分组: trail-tp / early-sl / breakeven-sl / 硬止盈/止损 / timeout / 手动
  S4. 按日: 哪天集体崩盘 (4-29 重点)
  S5. 早期止损深度: max_profit_pct 分布 -> 区分 "入场即反向" vs "短暂顺势后反转"
  S6. 4-29 BTC 1h 走势 vs 当天开仓方向: 是否反 BTC 趋势
  S7. 入场点位置: entry_price 在当时 24h 区间的相对位置 (高位 LONG / 低位 SHORT 即追高杀跌)

只读 dimesion 库, 不开仓, 不改 DB.
用法: python scripts/diag/diag_recent_loss_analysis.py
"""
import sys
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta
import pymysql

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')


# ── DB 配置: 优先解析 table_schemas.txt 头部 (IP 会变) ───────────────────
def _load_db_from_schema() -> dict:
    """从 table_schemas.txt 头部读 host/port/user/password/database. 兜底 hardcode."""
    schema_path = os.path.join(os.path.dirname(__file__), '..', '..', 'table_schemas.txt')
    cfg = dict(host='54.179.112.251', port=3306, user='admin',
               password='Yintao@110', db='dimesion')
    try:
        with open(schema_path, 'r', encoding='utf-8', errors='replace') as f:
            head = f.read(2000)
        m_host = re.search(r'host:\s*([\d.]+)', head)
        m_port = re.search(r'port:\s*(\d+)', head)
        m_user = re.search(r'user:\s*(\S+)', head)
        m_pwd  = re.search(r'password:\s*(\S+)', head)
        m_db   = re.search(r'database:\s*(\S+)', head)
        if m_host: cfg['host']     = m_host.group(1)
        if m_port: cfg['port']     = int(m_port.group(1))
        if m_user: cfg['user']     = m_user.group(1)
        if m_pwd:  cfg['password'] = m_pwd.group(1)
        if m_db:   cfg['db']       = m_db.group(1)
    except Exception as e:
        print(f"[警告] 读 table_schemas.txt 失败, 用 hardcode 兜底: {e}")
    cfg['charset'] = 'utf8mb4'
    cfg['cursorclass'] = pymysql.cursors.DictCursor
    return cfg


DB = _load_db_from_schema()
ACCOUNT_ID = 2
DAYS = 7  # 回看天数


def _fmt_pct(v):
    return f"{v*100:+.1f}%" if v is not None else "  N/A"


def _fmt_pnl(v):
    return f"{v:+8.2f}U" if v is not None else "    N/A "


def _parse_source(order_source: str) -> tuple:
    """从 order_source 解析 (strategy, sub_tag).
    例: 'strategy_live:chase-entry' -> ('strategy_live', 'chase-entry')
    """
    if not order_source:
        return ('unknown', 'unknown')
    parts = order_source.split(':', 1)
    if len(parts) == 2:
        return (parts[0], parts[1].split(',')[0].strip())
    return (parts[0], 'unknown')


def _parse_close_reason(notes: str) -> str:
    """从 notes 字段提取平仓原因关键字."""
    if not notes:
        return 'unknown'
    n = notes.lower()
    # 顺序敏感: breakeven 在 sl 前
    if 'breakeven' in n: return 'breakeven-sl'
    if 'early-sl' in n or 'early_sl' in n: return 'early-sl'
    if 'trail-tp' in n or 'trail_tp' in n: return 'trail-tp'
    if 'hard-tp' in n or 'hard_tp' in n: return 'hard-tp'
    if 'timeout' in n: return 'timeout'
    if 'manual' in n or '手动' in notes: return 'manual'
    if '止盈' in notes: return 'hard-tp'
    if '止损' in notes: return 'sl'
    return 'unknown'


def fetch_positions(cur, days: int):
    """拉近 N 天已平仓的 paper 仓位 + 关联 order_source."""
    cur.execute(f"""
        SELECT
            p.id            AS pid,
            p.symbol,
            p.position_side AS side,
            p.entry_price,
            p.realized_pnl,
            p.max_profit_pct,
            p.holding_hours,
            p.open_time,
            p.close_time,
            p.notes,
            p.source        AS p_source,
            (SELECT o.order_source FROM futures_orders o
              WHERE o.position_id = p.id
                AND o.side IN ('OPEN_LONG', 'OPEN_SHORT')
              ORDER BY o.id ASC LIMIT 1) AS open_order_source
        FROM futures_positions p
        WHERE p.account_id = %s
          AND p.status = 'closed'
          AND p.close_time >= NOW() - INTERVAL %s DAY
        ORDER BY p.open_time ASC
    """, (ACCOUNT_ID, days))
    rows = cur.fetchall()
    out = []
    for r in rows:
        src = r.get('open_order_source') or r.get('p_source') or ''
        strat, sub = _parse_source(src)
        out.append({
            'pid': r['pid'],
            'symbol': r['symbol'],
            'side': r['side'],
            'entry': float(r['entry_price'] or 0),
            'pnl': float(r['realized_pnl'] or 0),
            'max_profit_pct': float(r['max_profit_pct'] or 0),
            'hold_h': float(r['holding_hours'] or 0),
            'open_time': r['open_time'],
            'close_time': r['close_time'],
            'notes': r['notes'] or '',
            'src_full': src,
            'strategy': strat,
            'sub_tag': sub,
            'close_reason': _parse_close_reason(r['notes']),
        })
    return out


def section_header(title: str):
    print()
    print("=" * 100)
    print(f" {title}")
    print("=" * 100)


def s1_overall(positions: list):
    section_header("S1. 总体")
    n = len(positions)
    if n == 0:
        print("  无数据.")
        return
    wins = [p for p in positions if p['pnl'] > 0]
    losses = [p for p in positions if p['pnl'] <= 0]
    total_pnl = sum(p['pnl'] for p in positions)
    win_pnl = sum(p['pnl'] for p in wins)
    loss_pnl = sum(p['pnl'] for p in losses)
    avg_win = win_pnl / len(wins) if wins else 0
    avg_loss = loss_pnl / len(losses) if losses else 0
    win_rate = len(wins) / n
    print(f"  时间窗口: 近 {DAYS} 天, account_id={ACCOUNT_ID} (paper)")
    print(f"  总笔数:   {n}")
    print(f"  赢:       {len(wins):3d} 笔  胜率 {win_rate*100:5.1f}%")
    print(f"  亏:       {len(losses):3d} 笔  败率 {(1-win_rate)*100:5.1f}%")
    print(f"  净 PnL:   {total_pnl:+9.2f} U")
    print(f"  赢单总:   {win_pnl:+9.2f} U  (平均 {avg_win:+.2f} / 笔)")
    print(f"  亏单总:   {loss_pnl:+9.2f} U  (平均 {avg_loss:+.2f} / 笔)")
    if losses:
        ratio = -avg_win / avg_loss if avg_loss != 0 else float('inf')
        print(f"  盈亏比:   {ratio:.2f} (avg_win / |avg_loss|)")
    long_n = sum(1 for p in positions if p['side'] == 'LONG')
    short_n = n - long_n
    long_w = sum(1 for p in positions if p['side'] == 'LONG' and p['pnl'] > 0)
    short_w = sum(1 for p in positions if p['side'] == 'SHORT' and p['pnl'] > 0)
    print(f"  LONG:     {long_n:3d} 笔  胜率 {long_w/long_n*100:5.1f}% (若做多市场, 应该高)" if long_n else "  LONG: 0 笔")
    print(f"  SHORT:    {short_n:3d} 笔  胜率 {short_w/short_n*100:5.1f}%" if short_n else "  SHORT: 0 笔")


def s2_by_source(positions: list):
    section_header("S2. 按 source (sub-strategy) 分组")
    groups = defaultdict(list)
    for p in positions:
        key = f"{p['strategy']}:{p['sub_tag']}"
        groups[key].append(p)
    rows = []
    for key, ps in groups.items():
        n = len(ps)
        wins = sum(1 for p in ps if p['pnl'] > 0)
        net = sum(p['pnl'] for p in ps)
        rows.append((key, n, wins, wins / n, net))
    rows.sort(key=lambda r: r[4])  # 按净 PnL 升序 (亏的在前)
    print(f"  {'source':<35} {'笔数':>5} {'赢':>4} {'胜率':>7} {'净PnL':>11}")
    for key, n, wins, wr, net in rows:
        flag = "  <-- 重点" if net < -100 else ""
        print(f"  {key:<35} {n:>5d} {wins:>4d} {wr*100:>6.1f}% {net:>+10.2f}U{flag}")


def s3_by_close_reason(positions: list):
    section_header("S3. 按 close_reason 分组 (notes 解析)")
    groups = defaultdict(list)
    for p in positions:
        groups[p['close_reason']].append(p)
    rows = []
    for key, ps in groups.items():
        n = len(ps)
        wins = sum(1 for p in ps if p['pnl'] > 0)
        net = sum(p['pnl'] for p in ps)
        avg = net / n if n else 0
        rows.append((key, n, wins, net, avg))
    rows.sort(key=lambda r: r[3])
    print(f"  {'close_reason':<18} {'笔数':>5} {'赢':>4} {'净PnL':>11} {'均PnL':>10}")
    for key, n, wins, net, avg in rows:
        print(f"  {key:<18} {n:>5d} {wins:>4d} {net:>+10.2f}U {avg:>+9.2f}U")


def s4_by_day(positions: list):
    section_header("S4. 按日分组 (找崩盘日)")
    groups = defaultdict(list)
    for p in positions:
        if not p['open_time']:
            continue
        day = p['open_time'].strftime('%Y-%m-%d')
        groups[day].append(p)
    rows = []
    for day, ps in sorted(groups.items()):
        n = len(ps)
        wins = sum(1 for p in ps if p['pnl'] > 0)
        net = sum(p['pnl'] for p in ps)
        rows.append((day, n, wins, net))
    print(f"  {'日期':<12} {'笔数':>5} {'赢':>4} {'胜率':>7} {'净PnL':>11}")
    for day, n, wins, net in rows:
        wr = wins / n if n else 0
        flag = "  <-- 崩盘" if net < -200 else ""
        print(f"  {day:<12} {n:>5d} {wins:>4d} {wr*100:>6.1f}% {net:>+10.2f}U{flag}")


def s5_early_sl_depth(positions: list):
    section_header("S5. early-sl 深度: max_profit_pct 分布")
    early_sl = [p for p in positions if p['close_reason'] == 'early-sl']
    if not early_sl:
        print("  无 early-sl 单.")
        return
    print(f"  共 {len(early_sl)} 笔 early-sl, 平均 PnL {sum(p['pnl'] for p in early_sl)/len(early_sl):+.2f}U")
    print()
    # 分桶
    bucket_lt_0_5 = [p for p in early_sl if p['max_profit_pct'] < 0.5]
    bucket_0_5_1_5 = [p for p in early_sl if 0.5 <= p['max_profit_pct'] < 1.5]
    bucket_1_5_3 = [p for p in early_sl if 1.5 <= p['max_profit_pct'] < 3]
    bucket_3_plus = [p for p in early_sl if p['max_profit_pct'] >= 3]
    print(f"  入场即反向 (max_profit < 0.5%):     {len(bucket_lt_0_5):3d} 笔  --> 方向判错占比 {len(bucket_lt_0_5)/len(early_sl)*100:.0f}%")
    print(f"  短暂顺势 (max_profit 0.5%-1.5%):   {len(bucket_0_5_1_5):3d} 笔")
    print(f"  小赚后反转 (max_profit 1.5%-3%):   {len(bucket_1_5_3):3d} 笔  --> SL 太严可能误杀")
    print(f"  顺势 3%+ 后反转 (max_profit ≥ 3%):  {len(bucket_3_plus):3d} 笔  --> 应该是 trail-tp 漏触发")
    print()
    print("  按 source 拆分入场即反向占比:")
    by_src = defaultdict(lambda: [0, 0])  # total, immediate
    for p in early_sl:
        key = f"{p['strategy']}:{p['sub_tag']}"
        by_src[key][0] += 1
        if p['max_profit_pct'] < 0.5:
            by_src[key][1] += 1
    for key, (tot, imm) in sorted(by_src.items(), key=lambda x: -x[1][1]):
        if tot < 2:
            continue
        print(f"    {key:<35} {imm:>2d}/{tot:>2d} ({imm/tot*100:.0f}%)")


def s6_collapse_day_vs_btc(cur, positions: list):
    section_header("S6. 4-29 集体亏损 vs BTC 1h 走势")
    # 找当天开仓的所有单
    target_day = '2026-04-29'
    day_positions = [p for p in positions
                     if p['open_time'] and p['open_time'].strftime('%Y-%m-%d') == target_day]
    if not day_positions:
        print(f"  {target_day} 无开仓.")
        return
    # 取 BTC 1h K 线 (4-29 全天 + 4-30 前 12h)
    cur.execute("""
        SELECT open_time, open_price, close_price, high_price, low_price
        FROM kline_data
        WHERE symbol='BTC/USDT' AND timeframe='1h'
          AND open_time >= UNIX_TIMESTAMP('2026-04-29 00:00:00') * 1000
          AND open_time <  UNIX_TIMESTAMP('2026-04-30 12:00:00') * 1000
        ORDER BY open_time ASC
    """)
    btc_bars = cur.fetchall()
    if not btc_bars:
        print(f"  缺 BTC/USDT 1h 数据.")
        return
    btc_first = float(btc_bars[0]['open_price'])
    btc_last  = float(btc_bars[-1]['close_price'])
    btc_change = (btc_last - btc_first) / btc_first
    btc_high = max(float(b['high_price']) for b in btc_bars)
    btc_low  = min(float(b['low_price'])  for b in btc_bars)
    print(f"  BTC 4-29 00:00 -> 4-30 12:00:")
    print(f"    open  = {btc_first:.2f}")
    print(f"    close = {btc_last:.2f}  ({btc_change*100:+.2f}%)")
    print(f"    high  = {btc_high:.2f}  low = {btc_low:.2f}  振幅 {(btc_high-btc_low)/btc_first*100:.2f}%")
    print()

    # 4-29 当天开仓单按方向分组, 看胜率
    n = len(day_positions)
    long_ps = [p for p in day_positions if p['side'] == 'LONG']
    short_ps = [p for p in day_positions if p['side'] == 'SHORT']
    long_wins = sum(1 for p in long_ps if p['pnl'] > 0)
    short_wins = sum(1 for p in short_ps if p['pnl'] > 0)
    print(f"  4-29 当天总开仓: {n} 笔, 净 {sum(p['pnl'] for p in day_positions):+.2f}U")
    if long_ps:
        print(f"    LONG  {len(long_ps):2d} 笔  胜率 {long_wins/len(long_ps)*100:.0f}%  净 {sum(p['pnl'] for p in long_ps):+.2f}U")
    if short_ps:
        print(f"    SHORT {len(short_ps):2d} 笔  胜率 {short_wins/len(short_ps)*100:.0f}%  净 {sum(p['pnl'] for p in short_ps):+.2f}U")
    if btc_change < -0.01 and long_wins / max(len(long_ps), 1) < 0.3:
        print(f"  --> BTC 当天跌 {btc_change*100:.2f}%, LONG 胜率 {long_wins/max(len(long_ps),1)*100:.0f}% 极差: 反 BTC 趋势开多")
    elif btc_change > 0.01 and short_wins / max(len(short_ps), 1) < 0.3:
        print(f"  --> BTC 当天涨 {btc_change*100:.2f}%, SHORT 胜率极差: 反 BTC 趋势开空")
    else:
        print(f"  --> 不是简单的反 BTC 大盘问题, 看具体单 (S7)")
    print()
    print("  4-29 当天每笔细节 (按时间排序):")
    print(f"    {'时间':<6} {'symbol':<14} {'side':<6} {'src':<28} {'reason':<14} {'pnl':>9} {'max_profit':>10}")
    for p in sorted(day_positions, key=lambda x: x['open_time']):
        t = p['open_time'].strftime('%H:%M')
        src_short = p['sub_tag'][:28]
        print(f"    {t:<6} {p['symbol']:<14} {p['side']:<6} {src_short:<28} {p['close_reason']:<14} "
              f"{p['pnl']:>+8.2f} {p['max_profit_pct']:>9.2f}%")


def s7_entry_position_in_24h_range(cur, positions: list):
    section_header("S7. 入场点在当时 24h 区间的相对位置 (追高杀跌检测)")
    # 思路: 用入场时刻最近的 1h K 线区间近似 24h_high/low
    # entry_pos = (entry - low_24h) / (high_24h - low_24h) ∈ [0, 1]
    # LONG 入场点 > 0.7 = 高位追多 (追高)
    # SHORT 入场点 < 0.3 = 低位追空 (杀跌)

    long_high, long_mid, long_low = [], [], []
    short_high, short_mid, short_low = [], [], []
    skipped = 0

    for p in positions:
        if not p['open_time']:
            skipped += 1; continue
        open_ms = int(p['open_time'].timestamp() * 1000)
        # 取 entry 时刻前 24h 的 1h K 线
        cur.execute("""
            SELECT MIN(low_price) AS lo, MAX(high_price) AS hi
            FROM kline_data
            WHERE symbol=%s AND timeframe='1h'
              AND open_time >= %s - 24*3600*1000
              AND open_time <  %s
        """, (p['symbol'], open_ms, open_ms))
        r = cur.fetchone()
        if not r or r['lo'] is None or r['hi'] is None:
            skipped += 1; continue
        lo, hi = float(r['lo']), float(r['hi'])
        if hi <= lo:
            skipped += 1; continue
        rel = (p['entry'] - lo) / (hi - lo)
        if p['side'] == 'LONG':
            if rel > 0.7:   long_high.append((p, rel))
            elif rel > 0.3: long_mid.append((p, rel))
            else:           long_low.append((p, rel))
        else:
            if rel < 0.3:   short_low.append((p, rel))
            elif rel < 0.7: short_mid.append((p, rel))
            else:           short_high.append((p, rel))

    print(f"  跳过 {skipped} 笔 (缺 1h K 线或区间无效)")
    print()

    def _summary(label, lst):
        if not lst:
            return f"  {label:<30} 0 笔"
        n = len(lst)
        wins = sum(1 for p, _ in lst if p['pnl'] > 0)
        net = sum(p['pnl'] for p, _ in lst)
        return f"  {label:<30} {n:>3d} 笔  胜率 {wins/n*100:>5.1f}%  净 {net:>+8.2f}U"

    print("  LONG (做多):")
    print(_summary("  高位追多 (rel > 0.7)", long_high))
    print(_summary("  中位 (0.3 - 0.7)", long_mid))
    print(_summary("  低位接刀 (rel < 0.3)", long_low))
    print()
    print("  SHORT (做空):")
    print(_summary("  低位杀跌 (rel < 0.3)", short_low))
    print(_summary("  中位 (0.3 - 0.7)", short_mid))
    print(_summary("  高位顶部空 (rel > 0.7)", short_high))
    print()
    print("  解读: 高位追多 / 低位杀跌 胜率明显低于其它 -> 入场点选择问题")


def main():
    print(f"连库 {DB['user']}@{DB['host']}:{DB['port']}/{DB['db']}")
    conn = pymysql.connect(**DB)
    cur = conn.cursor()
    try:
        positions = fetch_positions(cur, DAYS)
        if not positions:
            print(f"近 {DAYS} 天 account_id={ACCOUNT_ID} 无 closed 仓位.")
            return
        s1_overall(positions)
        s2_by_source(positions)
        s3_by_close_reason(positions)
        s4_by_day(positions)
        s5_early_sl_depth(positions)
        s6_collapse_day_vs_btc(cur, positions)
        s7_entry_position_in_24h_range(cur, positions)
    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    main()
