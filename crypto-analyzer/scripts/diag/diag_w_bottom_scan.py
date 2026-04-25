"""
扫所有活跃 symbol 找当下真实存在的 W 底.
用 strategy_whale.detect_w_bottom 的同款逻辑 (3.5 天窗口 / 5% 反弹 / B2 ±5% / 颈线 +0.5%).
对找到的 W 底, 检查 F3 的判定 + 给出 F3 漏掉的原因.

只读 kline_data.
用法: python scripts/diag/diag_w_bottom_scan.py
"""
import sys
from datetime import datetime
import pymysql

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DB = dict(host='13.212.252.171', port=3306, user='admin', password='Yintao@110',
          db='dimesion', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

# ── strategy_whale.detect_w_bottom 阈值 ─────────────────────────
WB_DATA_MIN_BARS    = 14 * 24    # 336 根 15m = 3.5 天
WB_REBOUND_MIN_PCT  = 0.05       # B1→C 反弹 ≥ 5%
WB_BOTTOM_DIFF_PCT  = 0.05       # |B2 - B1| / B1 ≤ 5%
WB_B2_TO_NECK_MIN_H = 4          # B2 距 C ≥ 4 根 = 1h
WB_TIME_GAP_MIN_H   = 24         # B1→B2 ≥ 24 根 = 6h
WB_TIME_GAP_MAX_H   = 14 * 24    # B1→B2 ≤ 336 根 = 3.5 天
WB_BREAK_NECK_PCT   = 0.005      # 当前 > C * 1.005

# ── F3 阈值 ────────────────────────────────────────────────
F3_LOOKBACK = 7 * 24 * 4
F3_RECENT24 = 24 * 4
F3_MIN_DROP = 0.20
F3_R24_MIN  = -0.05
F3_CH24_MAX = 0.02
F3_BODY_MIN = 0.01
F3_BODY_MAX = 0.03
F3_VOL_MIN  = 1.5
F3_VOL_MAX  = 3.0


def detect_w_bottom(bars):
    """精简版, 复刻 strategy_whale.detect_w_bottom 关键逻辑.
    bars: 升序的 15m kline (含 high/low/close/open/volume).
    """
    if len(bars) < WB_DATA_MIN_BARS:
        return None
    window = bars[-WB_DATA_MIN_BARS:]
    lows  = [float(b['low_price']) for b in window]
    highs = [float(b['high_price']) for b in window]
    closes = [float(b['close_price']) for b in window]
    n = len(window)
    cur_p = closes[-1]
    if cur_p <= 0:
        return None

    # 1. B1 = 全窗口最低
    i1 = lows.index(min(lows))
    b1 = lows[i1]
    if b1 <= 0:
        return None

    # 2. C = B1 之后的最高
    if i1 + 1 >= n:
        return None
    after_b1_highs = highs[i1 + 1:]
    ic_rel = after_b1_highs.index(max(after_b1_highs))
    ic = i1 + 1 + ic_rel
    c_high = highs[ic]
    rebound = (c_high - b1) / b1
    if rebound < WB_REBOUND_MIN_PCT:
        return None

    # 3. B2 = C 之后的最低
    if ic + 1 >= n:
        return None
    after_c_lows = lows[ic + 1:]
    if not after_c_lows:
        return None
    ib2_rel = after_c_lows.index(min(after_c_lows))
    ib2 = ic + 1 + ib2_rel
    b2 = lows[ib2]

    # 4. B2 距 C 至少 4 根
    if (ib2 - ic) < WB_B2_TO_NECK_MIN_H:
        return None

    # 5. B2 ±5% B1
    bottom_diff = abs(b2 - b1) / b1
    if bottom_diff > WB_BOTTOM_DIFF_PCT:
        return None

    # 6. B1 → B2 时间间隔
    gap = ib2 - i1
    if gap < WB_TIME_GAP_MIN_H or gap > WB_TIME_GAP_MAX_H:
        return None

    # 7. 突破颈线 +0.5%
    if cur_p < c_high * (1 + WB_BREAK_NECK_PCT):
        return None

    return {
        'i1': i1, 'b1': b1,
        'ic': ic, 'c': c_high,
        'ib2': ib2, 'b2': b2,
        'cur': cur_p,
        'rebound': rebound,
        'bottom_diff': bottom_diff,
        'gap_h': gap,        # 单位 = 15m 根
    }


def detect_f3(bars):
    """F3 判定 + 失败原因."""
    if len(bars) < F3_LOOKBACK:
        return None, 'bars_not_enough'
    w = bars[-F3_LOOKBACK:]
    highs = [float(b['high_price']) for b in w]
    lows  = [float(b['low_price'])  for b in w]
    closes = [float(b['close_price']) for b in w]
    vols  = [float(b['volume'] or 0) for b in w]
    n = len(w)

    w_high = max(highs); w_low = min(lows)
    if w_high <= 0:
        return None, 'no_high'
    drop = (w_high - w_low) / w_high
    if drop < F3_MIN_DROP:
        return None, f'drop={drop*100:.1f}%<20'
    if n < F3_RECENT24:
        return None, 'r24_not_enough'
    r24 = w[-F3_RECENT24:]
    r24_open = float(r24[0]['open_price'])
    r24_low  = min(float(b['low_price']) for b in r24)
    r24_last = float(r24[-1]['close_price'])
    if r24_open <= 0:
        return None, 'r24_open_zero'
    ch24 = (r24_last - r24_open) / r24_open
    if ch24 < F3_R24_MIN:
        return None, f'still_dropping ch24={ch24*100:.1f}%'
    if r24_last < r24_low * 1.01:
        return None, f'at_24h_low'
    if ch24 > F3_CH24_MAX:
        return None, f'already_bounced ch24=+{ch24*100:.1f}%'
    last = w[-1]
    o = float(last['open_price']); c = float(last['close_price']); v = float(last['volume'] or 0)
    if o <= 0:
        return None, 'last_o_zero'
    if c <= o:
        return None, 'last_not_bullish'
    body = (c - o) / o
    if body < F3_BODY_MIN:
        return None, f'body={body*100:.2f}%<1'
    if body >= F3_BODY_MAX:
        return None, f'body={body*100:.2f}%>=3'
    avg_vol = sum(vols[-F3_RECENT24:]) / F3_RECENT24
    if avg_vol <= 0:
        return None, 'avg_vol_zero'
    vr = v / avg_vol
    if vr < F3_VOL_MIN:
        return None, f'vol={vr:.2f}x<1.5'
    if vr >= F3_VOL_MAX:
        return None, f'vol={vr:.2f}x>=3'
    return {'drop': drop, 'ch24': ch24, 'body': body, 'vol_ratio': vr}, 'OK'


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
        print(f"\n扫 {len(symbols)} 个活跃 symbol 找 W 底\n")

        wb_hits = []
        for sym in symbols:
            cur.execute("""
                SELECT open_time, open_price, high_price, low_price, close_price, volume
                FROM kline_data
                WHERE symbol=%s AND timeframe='15m'
                  AND open_time + 900000 < UNIX_TIMESTAMP(NOW()) * 1000
                ORDER BY open_time DESC LIMIT %s
            """, (sym, WB_DATA_MIN_BARS + 10))
            bars = list(reversed(cur.fetchall()))
            wb = detect_w_bottom(bars)
            if not wb:
                continue
            f3, f3_reason = detect_f3(bars)
            wb_hits.append({'sym': sym, 'wb': wb, 'f3': f3, 'f3_reason': f3_reason,
                            'last_close': float(bars[-1]['close_price'])})

        print("=" * 100)
        print(f"[找到 W 底候选: {len(wb_hits)} 个]")
        print("=" * 100)
        if not wb_hits:
            print("当前没有符合 whale.detect_w_bottom 条件的 W 底.")
            print("  (3.5 天 / 反弹 ≥5% / B2 ±5% / B1→B2 6h-3.5d / 突破颈线 +0.5%)")
        else:
            print(f"  {'sym':<14}{'B1':>10}{'C(neck)':>10}{'B2':>10}{'cur':>10}"
                  f"{'反弹%':>8}{'两底差%':>9}{'gap_h':>7}  F3")
            for h in wb_hits:
                wb = h['wb']
                gap_h = wb['gap_h'] / 4  # 15m -> hours
                f3_str = 'OK' if h['f3'] else f"miss: {h['f3_reason']}"
                print(f"  {h['sym']:<14}{wb['b1']:>10.6f}{wb['c']:>10.6f}"
                      f"{wb['b2']:>10.6f}{wb['cur']:>10.6f}"
                      f"{wb['rebound']*100:>7.1f}%{wb['bottom_diff']*100:>8.2f}%"
                      f"{gap_h:>7.1f}  {f3_str}")

        # F3 漏掉的 W 底, 按原因分组
        if wb_hits:
            f3_miss_reasons = {}
            for h in wb_hits:
                if h['f3']:
                    continue
                # 提取原因前缀
                rk = h['f3_reason'].split(' ')[0] if ' ' in h['f3_reason'] else h['f3_reason']
                f3_miss_reasons.setdefault(rk, []).append(h['sym'])
            print()
            print("=" * 100)
            print("[F3 漏掉 W 底的原因分布]")
            print("=" * 100)
            for k, syms in sorted(f3_miss_reasons.items(), key=lambda x: -len(x[1])):
                print(f"  {k:<30}{len(syms):>4}  {', '.join(syms[:8])}")

    finally:
        conn.close()


if __name__ == '__main__':
    main()
