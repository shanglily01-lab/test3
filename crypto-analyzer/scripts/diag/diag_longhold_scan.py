"""
扫所有活跃 symbol 当下是否存在 longhold 子策略 (W底/M顶 2 周窗口) 命中.
用 strategy_whale.detect_w_bottom_lh / detect_m_top_lh 同款逻辑 (1h x 14 天).

只读 kline_data, 不开仓, 不改 DB.
用于 longhold_enabled 开启之前的命中数 / 质量审计.

用法: python scripts/diag/diag_longhold_scan.py
"""
import sys
import pymysql

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DB = dict(host='13.212.252.171', port=3306, user='admin', password='Yintao@110',
          db='dimesion', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

# ── strategy_whale.detect_*_lh 阈值 (与 strategy_whale.py 内 LH_* 保持同步) ─────
LH_DATA_MIN_BARS        = 336    # 1h x 14 天
LH_REBOUND_MIN_PCT      = 0.08
LH_BOTTOM_DIFF_PCT      = 0.05
LH_B2_TO_NECK_MIN_BARS  = 12
LH_TIME_GAP_MIN_BARS    = 48
LH_TIME_GAP_MAX_BARS    = 336
LH_BREAK_NECK_PCT       = 0.005


def detect_w_bottom_lh(bars):
    n = len(bars)
    if n < LH_DATA_MIN_BARS:
        return None
    lows   = [float(b['low_price'])   for b in bars]
    highs  = [float(b['high_price'])  for b in bars]
    closes = [float(b['close_price']) for b in bars]
    i1 = min(range(n), key=lambda i: lows[i])
    b1 = lows[i1]
    if b1 <= 0 or n - i1 < LH_B2_TO_NECK_MIN_BARS * 2:
        return None
    after_b1_highs = highs[i1 + 1:]
    if not after_b1_highs:
        return None
    ic_rel = max(range(len(after_b1_highs)), key=lambda i: after_b1_highs[i])
    ic = i1 + 1 + ic_rel
    c  = highs[ic]
    rebound = (c - b1) / b1
    if rebound < LH_REBOUND_MIN_PCT:
        return None
    after_c_lows = lows[ic + 1:]
    if not after_c_lows:
        return None
    ib2_rel = min(range(len(after_c_lows)), key=lambda i: after_c_lows[i])
    ib2 = ic + 1 + ib2_rel
    b2  = lows[ib2]
    if (ib2 - ic) < LH_B2_TO_NECK_MIN_BARS:
        return None
    if abs(b2 - b1) / b1 > LH_BOTTOM_DIFF_PCT:
        return None
    gap_bars = ib2 - i1
    if gap_bars < LH_TIME_GAP_MIN_BARS or gap_bars > LH_TIME_GAP_MAX_BARS:
        return None
    cur_price = closes[-1]
    if cur_price < c * (1 + LH_BREAK_NECK_PCT):
        return None
    return dict(b1=b1, neck=c, b2=b2, cur=cur_price,
                rebound=rebound, bottom_diff=abs(b2 - b1) / b1,
                gap_bars=gap_bars, break_pct=(cur_price - c) / c)


def detect_m_top_lh(bars):
    n = len(bars)
    if n < LH_DATA_MIN_BARS:
        return None
    lows   = [float(b['low_price'])   for b in bars]
    highs  = [float(b['high_price'])  for b in bars]
    closes = [float(b['close_price']) for b in bars]
    i1 = max(range(n), key=lambda i: highs[i])
    h1 = highs[i1]
    if h1 <= 0 or n - i1 < LH_B2_TO_NECK_MIN_BARS * 2:
        return None
    after_h1_lows = lows[i1 + 1:]
    if not after_h1_lows:
        return None
    id_rel = min(range(len(after_h1_lows)), key=lambda i: after_h1_lows[i])
    id_idx = i1 + 1 + id_rel
    d  = lows[id_idx]
    if d <= 0:
        return None
    pullback = (h1 - d) / h1
    if pullback < LH_REBOUND_MIN_PCT:
        return None
    after_d_highs = highs[id_idx + 1:]
    if not after_d_highs:
        return None
    ih2_rel = max(range(len(after_d_highs)), key=lambda i: after_d_highs[i])
    ih2 = id_idx + 1 + ih2_rel
    h2  = highs[ih2]
    if (ih2 - id_idx) < LH_B2_TO_NECK_MIN_BARS:
        return None
    if abs(h2 - h1) / h1 > LH_BOTTOM_DIFF_PCT:
        return None
    gap_bars = ih2 - i1
    if gap_bars < LH_TIME_GAP_MIN_BARS or gap_bars > LH_TIME_GAP_MAX_BARS:
        return None
    cur_price = closes[-1]
    if cur_price > d * (1 - LH_BREAK_NECK_PCT):
        return None
    return dict(h1=h1, neck=d, h2=h2, cur=cur_price,
                pullback=pullback, top_diff=abs(h2 - h1) / h1,
                gap_bars=gap_bars, break_pct=(d - cur_price) / d)


def main():
    conn = pymysql.connect(**DB); cur = conn.cursor()
    try:
        cur.execute("""
            SELECT symbol FROM price_stats_24h
            WHERE updated_at >= NOW() - INTERVAL 1 HOUR
              AND quote_volume_24h > 5e6
            ORDER BY quote_volume_24h DESC LIMIT 250
        """)
        symbols = [r['symbol'] for r in cur.fetchall()]
        print(f"\n扫 {len(symbols)} 个活跃 symbol 找 longhold 命中 (1h x 14天)")
        print(f"  阈值: 反弹/回撤 >= {LH_REBOUND_MIN_PCT*100:.0f}%, 两底/两顶差 <= {LH_BOTTOM_DIFF_PCT*100:.0f}%, "
              f"B1->B2 >= {LH_TIME_GAP_MIN_BARS}h, 突破/跌破颈线 {LH_BREAK_NECK_PCT*100:.1f}%\n")

        w_hits = []
        m_hits = []
        for sym in symbols:
            cur.execute("""
                SELECT open_time, open_price, high_price, low_price, close_price, volume
                FROM kline_data
                WHERE symbol=%s AND timeframe='1h'
                  AND open_time + 3600000 < UNIX_TIMESTAMP(NOW()) * 1000
                ORDER BY open_time DESC LIMIT %s
            """, (sym, LH_DATA_MIN_BARS + 24))
            bars = list(reversed(cur.fetchall()))
            wb = detect_w_bottom_lh(bars)
            if wb:
                w_hits.append({'sym': sym, **wb})
            mt = detect_m_top_lh(bars)
            if mt:
                m_hits.append({'sym': sym, **mt})

        print("=" * 110)
        print(f"[longhold-W (做多 W 底): {len(w_hits)} 命中]")
        print("=" * 110)
        if not w_hits:
            print("  当前无 W 底命中. 若长期 0 命中, 考虑放宽 longhold_rebound_pct (默认 0.08).")
        else:
            print(f"  {'sym':<14}{'B1':>12}{'neck':>12}{'B2':>12}{'cur':>12}"
                  f"{'反弹%':>8}{'两底差%':>10}{'gap_h':>8}{'突破%':>8}")
            for h in sorted(w_hits, key=lambda x: -x['break_pct']):
                print(f"  {h['sym']:<14}{h['b1']:>12.6f}{h['neck']:>12.6f}"
                      f"{h['b2']:>12.6f}{h['cur']:>12.6f}"
                      f"{h['rebound']*100:>7.1f}%{h['bottom_diff']*100:>9.2f}%"
                      f"{h['gap_bars']:>8d}{h['break_pct']*100:>7.2f}%")

        print()
        print("=" * 110)
        print(f"[longhold-M (做空 M 顶): {len(m_hits)} 命中]")
        print("=" * 110)
        if not m_hits:
            print("  当前无 M 顶命中. 若长期 0 命中, 考虑放宽 longhold_rebound_pct (默认 0.08).")
        else:
            print(f"  {'sym':<14}{'H1':>12}{'neck':>12}{'H2':>12}{'cur':>12}"
                  f"{'回撤%':>8}{'两顶差%':>10}{'gap_h':>8}{'跌破%':>8}")
            for h in sorted(m_hits, key=lambda x: -x['break_pct']):
                print(f"  {h['sym']:<14}{h['h1']:>12.6f}{h['neck']:>12.6f}"
                      f"{h['h2']:>12.6f}{h['cur']:>12.6f}"
                      f"{h['pullback']*100:>7.1f}%{h['top_diff']*100:>9.2f}%"
                      f"{h['gap_bars']:>8d}{h['break_pct']*100:>7.2f}%")

        print()
        print(f"汇总: W 底 {len(w_hits)} / M 顶 {len(m_hits)} (扫 {len(symbols)} 个 symbol).")
        print("命中过多 (>20) 收紧 longhold_rebound_pct, 命中为 0 放宽 (system_settings).")
    finally:
        cur.close(); conn.close()


if __name__ == '__main__':
    main()
