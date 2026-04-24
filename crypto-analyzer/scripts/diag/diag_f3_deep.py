"""
F3 (W 底小涨带量做多) 深挖:
  对 7 天内所有 F3 命中信号, 按多维特征分桶, 找出高胜率特征组合.

特征:
  1. 前期最大跌幅 (drop_pct)      —— 跌 20-30% / 30-50% / 50%+
  2. 触发价在 7 天区间的位置 (pos)  —— 10-30% / 30-60% / 60%+
  3. 24h 涨跌 (ch_24h)            —— 横盘 / 反弹 / 强反弹
  4. 触发阳线幅度 (body_pct)       —— 1-2% / 2-5% / 5%+
  5. 触发量比 (vol_ratio vs 24h 均量) —— 1.5-2x / 2-3x / 3x+
  6. 符号成交额级别 (quote_vol_24h) —— 从 price_stats_24h 来

输出:
  - 按单维度分桶: 每桶 n, 胜率, 期望
  - 按两两组合分桶: 找出 top 10 高胜率组合
  - 符号黑/白名单候选

只读. 用法: python scripts/diag/diag_f3_deep.py [DAYS]  默认 7
"""
import sys
from datetime import datetime, timezone
from collections import defaultdict
import pymysql

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DB = dict(host='13.212.252.171', port=3306, user='admin', password='Yintao@110',
          db='dimesion', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

# F3 参数 (和 replay_4_forms.py 一致)
F3_DROP_LOOKBACK = 7 * 24 * 4
F3_MIN_DROP = 0.20
F3_RECENT_24H_MIN = -0.05
F3_TRIGGER_BULLISH_PCT = 0.01
F3_VOL_MULT = 1.5

BAR_MS_15M = 15 * 60 * 1000
SL_PCT = 0.05
TP_PCT = 0.10
MAX_HOLD_BARS = 48


def detect_f3(bars):
    """F3 识别 (精确复刻, 返回特征 dict 或 None)."""
    if len(bars) < F3_DROP_LOOKBACK:
        return None
    window = bars[-F3_DROP_LOOKBACK:]
    highs = [float(b['high_price']) for b in window]
    lows = [float(b['low_price']) for b in window]
    closes = [float(b['close_price']) for b in window]
    vols = [float(b['volume'] or 0) for b in window]
    n = len(window)
    w_high = max(highs); w_low = min(lows)
    if w_high <= 0:
        return None
    drop = (w_high - w_low) / w_high
    if drop < F3_MIN_DROP:
        return None
    if n >= 96:
        recent24 = closes[-96:]
        rec_low = min(recent24); rec_last = recent24[-1]
        rec_change = (rec_last - recent24[0]) / recent24[0] if recent24[0] > 0 else 0
        if rec_change < F3_RECENT_24H_MIN:
            return None
        if rec_last < rec_low * 1.01:
            return None
    last = window[-1]
    o = float(last['open_price']); c = float(last['close_price']); v = float(last['volume'] or 0)
    if c <= o:
        return None
    body_pct = (c - o) / o if o > 0 else 0
    if body_pct < F3_TRIGGER_BULLISH_PCT:
        return None
    vol_ratio = 0
    if n >= 96:
        avg_vol = sum(vols[-96:]) / 96
        if avg_vol <= 0 or v < avg_vol * F3_VOL_MULT:
            return None
        vol_ratio = v / avg_vol

    # 7 天区间内当前价的位置
    pos_pct = (c - w_low) / (w_high - w_low) * 100 if w_high > w_low else 50
    # 24h 涨跌
    ch_24h = 0
    if n >= 96:
        ch_24h = (closes[-1] - closes[-96]) / closes[-96] if closes[-96] > 0 else 0

    return {
        'entry_price': c,
        'drop_pct': drop,
        'pos_pct': pos_pct,
        'ch_24h': ch_24h,
        'body_pct': body_pct,
        'vol_ratio': vol_ratio,
    }


def simulate(cur, sym, signal_ms, entry):
    end_ms = signal_ms + MAX_HOLD_BARS * BAR_MS_15M
    cur.execute(
        """SELECT high_price, low_price, close_price
           FROM kline_data
           WHERE symbol=%s AND timeframe='15m'
             AND open_time >= %s AND open_time < %s
           ORDER BY open_time ASC""",
        (sym, signal_ms, end_ms),
    )
    bars = cur.fetchall()
    if not bars:
        return None
    for b in bars:
        hi = float(b['high_price']); lo = float(b['low_price'])
        if (entry - lo) / entry >= SL_PCT:
            return {'reason': 'sl', 'pnl': -SL_PCT}
        if (hi - entry) / entry >= TP_PCT:
            return {'reason': 'tp', 'pnl': TP_PCT}
    exit_p = float(bars[-1]['close_price'])
    return {'reason': 'timeout', 'pnl': (exit_p - entry) / entry}


def bucket(val, edges, labels):
    for i, e in enumerate(edges):
        if val < e:
            return labels[i]
    return labels[-1]


def bucket_drop(v):
    return bucket(v, [0.30, 0.50], ['20-30%', '30-50%', '50%+'])

def bucket_pos(v):
    return bucket(v, [30, 60], ['近底(0-30)', '中段(30-60)', '偏顶(60+)'])

def bucket_ch24(v):
    return bucket(v, [-0.02, 0.03, 0.08], ['仍跌', '横盘', '反弹', '强反弹'])

def bucket_body(v):
    return bucket(v, [0.02, 0.05], ['1-2%', '2-5%', '5%+'])

def bucket_vol(v):
    return bucket(v, [2.0, 3.0], ['1.5-2x', '2-3x', '3x+'])


def print_bucket(name, results, key_fn):
    by_key = defaultdict(list)
    for r in results:
        by_key[key_fn(r)].append(r)
    print(f"\n-- 按 [{name}] 分桶 --")
    print(f"  {'bucket':<14}{'n':>5}{'w':>5}{'win%':>7}{'avg%':>8}{'sum%':>9}")
    order = sorted(by_key.keys())
    for k in order:
        lst = by_key[k]
        wins = sum(1 for r in lst if r['pnl'] > 0)
        avg = sum(r['pnl'] for r in lst) / len(lst)
        s = sum(r['pnl'] for r in lst)
        wr = wins / len(lst) * 100
        print(f"  {k:<14}{len(lst):>5}{wins:>5}{wr:>6.1f}%{avg*100:>+7.2f}%{s*100:>+8.1f}%")


def main():
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 7
    conn = pymysql.connect(**DB); cur = conn.cursor()
    try:
        end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        start_ms = end_ms - days * 24 * 3600 * 1000
        pre_ms = start_ms - 7 * 24 * 3600 * 1000

        cur.execute(
            """SELECT DISTINCT symbol FROM kline_data
               WHERE timeframe='15m' AND open_time >= %s""",
            (start_ms,),
        )
        symbols = [r['symbol'] for r in cur.fetchall()]
        print(f"### F3 深挖 {days} 天 / {len(symbols)} symbols ###\n")

        # 拉 price_stats_24h 的成交额
        cur.execute("SELECT symbol, quote_volume_24h FROM price_stats_24h")
        vol_map = {r['symbol']: float(r['quote_volume_24h'] or 0) for r in cur.fetchall()}

        results = []
        COOLDOWN_BARS = 8
        for sym in symbols:
            try:
                cur.execute(
                    """SELECT open_time, open_price, high_price, low_price, close_price, volume
                       FROM kline_data
                       WHERE symbol=%s AND timeframe='15m'
                         AND open_time >= %s AND open_time < %s
                       ORDER BY open_time ASC""",
                    (sym, pre_ms, end_ms),
                )
                bars = cur.fetchall()
            except Exception:
                continue
            if len(bars) < F3_DROP_LOOKBACK:
                continue

            last_hit = -999
            for i in range(F3_DROP_LOOKBACK, len(bars)):
                if bars[i]['open_time'] + BAR_MS_15M < start_ms:
                    continue
                if i - last_hit < COOLDOWN_BARS:
                    continue
                sub = bars[:i + 1]
                sig = detect_f3(sub)
                if not sig:
                    continue
                last_hit = i
                signal_ms = bars[i]['open_time'] + BAR_MS_15M
                sim = simulate(cur, sym, signal_ms, sig['entry_price'])
                if not sim:
                    continue
                results.append({
                    'sym': sym, 'signal_ms': signal_ms,
                    'quote_vol': vol_map.get(sym, 0),
                    **sig, **sim,
                })

        print(f"F3 命中总数: {len(results)}")
        if not results:
            return
        wins = [r for r in results if r['pnl'] > 0]
        avg = sum(r['pnl'] for r in results) / len(results)
        wr = len(wins) / len(results) * 100
        print(f"整体胜率: {wr:.1f}%  均 pnl: {avg*100:+.2f}%  累计: {sum(r['pnl'] for r in results)*100:+.1f}%")
        print()

        # 分桶
        print_bucket('drop_pct 前期跌幅', results, lambda r: bucket_drop(r['drop_pct']))
        print_bucket('pos_pct 7d区间位置', results, lambda r: bucket_pos(r['pos_pct']))
        print_bucket('ch_24h 24h涨跌', results, lambda r: bucket_ch24(r['ch_24h']))
        print_bucket('body_pct 触发阳线幅度', results, lambda r: bucket_body(r['body_pct']))
        print_bucket('vol_ratio 触发量比', results, lambda r: bucket_vol(r['vol_ratio']))

        # 币成交额分桶
        def bucket_qv(v):
            if v < 5e6: return '小<5M'
            if v < 50e6: return '中5-50M'
            if v < 200e6: return '大50-200M'
            return '超大200M+'
        print_bucket('quote_vol 币成交额', results, lambda r: bucket_qv(r['quote_vol']))

        # 两两组合: pos_pct × ch_24h
        print("\n-- 两两组合 [pos_pct × ch_24h] --")
        combo = defaultdict(list)
        for r in results:
            key = (bucket_pos(r['pos_pct']), bucket_ch24(r['ch_24h']))
            combo[key].append(r)
        print(f"  {'pos':<14}{'ch24h':<10}{'n':>5}{'w':>5}{'win%':>7}{'avg%':>8}{'sum%':>9}")
        combo_sorted = sorted(combo.items(),
                              key=lambda x: -sum(r['pnl'] for r in x[1]))
        for (pos, ch), lst in combo_sorted[:10]:
            if len(lst) < 3:
                continue
            w = sum(1 for r in lst if r['pnl'] > 0)
            avg = sum(r['pnl'] for r in lst) / len(lst)
            s = sum(r['pnl'] for r in lst)
            print(f"  {pos:<14}{ch:<10}{len(lst):>5}{w:>5}{w/len(lst)*100:>6.1f}%"
                  f"{avg*100:>+7.2f}%{s*100:>+8.1f}%")

        # 两两组合: drop × pos
        print("\n-- 两两组合 [drop × pos] --")
        combo = defaultdict(list)
        for r in results:
            key = (bucket_drop(r['drop_pct']), bucket_pos(r['pos_pct']))
            combo[key].append(r)
        print(f"  {'drop':<10}{'pos':<14}{'n':>5}{'w':>5}{'win%':>7}{'avg%':>8}{'sum%':>9}")
        for (drop, pos), lst in sorted(combo.items(),
                                         key=lambda x: -sum(r['pnl'] for r in x[1])):
            if len(lst) < 3:
                continue
            w = sum(1 for r in lst if r['pnl'] > 0)
            avg = sum(r['pnl'] for r in lst) / len(lst)
            s = sum(r['pnl'] for r in lst)
            print(f"  {drop:<10}{pos:<14}{len(lst):>5}{w:>5}{w/len(lst)*100:>6.1f}%"
                  f"{avg*100:>+7.2f}%{s*100:>+8.1f}%")

        # 按 symbol 分桶（白/黑名单候选）
        by_sym = defaultdict(list)
        for r in results:
            by_sym[r['sym']].append(r)
        sym_stats = [(s, len(rs), sum(1 for r in rs if r['pnl']>0),
                      sum(r['pnl'] for r in rs))
                     for s, rs in by_sym.items() if len(rs) >= 3]
        print(f"\n-- 币种分桶 (仅显示样本 >=3 的) --")
        print("  top 10 白名单候选 (累计 pnl 高)")
        print(f"  {'sym':<14}{'n':>4}{'w':>4}{'win%':>7}{'sum%':>9}")
        for s in sorted(sym_stats, key=lambda x: -x[3])[:10]:
            print(f"  {s[0]:<14}{s[1]:>4}{s[2]:>4}{s[2]/s[1]*100:>6.1f}%{s[3]*100:>+8.1f}%")
        print("  bottom 10 黑名单候选 (累计 pnl 低)")
        for s in sorted(sym_stats, key=lambda x: x[3])[:10]:
            print(f"  {s[0]:<14}{s[1]:>4}{s[2]:>4}{s[2]/s[1]*100:>6.1f}%{s[3]*100:>+8.1f}%")
    finally:
        conn.close()


if __name__ == '__main__':
    main()
