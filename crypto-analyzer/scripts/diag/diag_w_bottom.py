"""
诊断 W 双底策略为何无触发。

做法: 复刻 strategy_whale.detect_w_bottom 的流程, 但每一步都记录失败原因,
最后汇总各原因的淘汰数, 并列出最接近触发的候选 (通过前 6 步但未突破颈线)。

用法: python scripts/diag/diag_w_bottom.py
"""
import os
import sys
from pathlib import Path

import pymysql
from dotenv import load_dotenv

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

ROOT = Path(__file__).resolve().parents[2]
load_dotenv(ROOT / '.env')

# 与 strategy_whale.py 保持一致的参数
WB_DATA_MIN_BARS    = 14 * 24
WB_REBOUND_MIN_PCT  = 0.05
WB_BOTTOM_DIFF_PCT  = 0.05
WB_B2_TO_NECK_MIN_H = 4
WB_TIME_GAP_MIN_H   = 24
WB_TIME_GAP_MAX_H   = 14 * 24
WB_BREAK_NECK_PCT   = 0.005


def get_db():
    return pymysql.connect(
        host=os.getenv('DB_HOST'), port=int(os.getenv('DB_PORT', 3306)),
        user=os.getenv('DB_USER'), password=os.getenv('DB_PASSWORD', ''),
        db=os.getenv('DB_NAME'), charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor,
    )


def get_universe(cur):
    cur.execute("""
        SELECT symbol FROM price_stats_24h
        WHERE updated_at >= NOW() - INTERVAL 30 MINUTE
          AND quote_volume_24h > 5e6
        ORDER BY quote_volume_24h DESC
        LIMIT 200
    """)
    return [r['symbol'] for r in cur.fetchall()]


def get_15m_bars(cur, sym, limit):
    import time
    now_ms = int(time.time() * 1000)
    cur.execute("""
        SELECT open_time, low_price, high_price, close_price
        FROM kline_data
        WHERE symbol=%s AND timeframe='15m'
          AND open_time + 900000 < %s
        ORDER BY open_time DESC LIMIT %s
    """, (sym, now_ms, limit))
    return list(reversed(cur.fetchall()))


REASONS = [
    'bars_insufficient',      # 数据不够 336 根
    'b1_invalid',             # 全局最低 <= 0
    'b1_too_recent',          # B1 距末尾 < 48h, 没空间走颈线+二次探底
    'no_highs_after_b1',
    'rebound_too_small',      # 颈线反弹 < 5%
    'no_lows_after_neck',
    'b2_too_close_to_neck',   # B2 距颈线 < 4h
    'bottoms_unaligned',      # |B2-B1|/B1 > 5%
    'gap_too_short',          # B1->B2 < 24h
    'gap_too_long',           # B1->B2 > 336h
    'neck_not_broken',        # 当前价 < 颈线*1.005
    'passed',
]


def diagnose(bars):
    """返回 (reason, detail_dict)"""
    n = len(bars)
    if n < WB_DATA_MIN_BARS:
        return 'bars_insufficient', {'n': n}

    lows   = [float(b['low_price'])   for b in bars]
    highs  = [float(b['high_price'])  for b in bars]
    closes = [float(b['close_price']) for b in bars]

    i1 = min(range(n), key=lambda i: lows[i])
    b1 = lows[i1]
    if b1 <= 0:
        return 'b1_invalid', {}

    tail_after_b1 = n - i1
    if tail_after_b1 < 48:
        return 'b1_too_recent', {'i1': i1, 'tail': tail_after_b1}

    after_b1_highs = highs[i1 + 1:]
    if not after_b1_highs:
        return 'no_highs_after_b1', {}
    ic_rel = max(range(len(after_b1_highs)), key=lambda i: after_b1_highs[i])
    ic = i1 + 1 + ic_rel
    c = highs[ic]
    rebound = (c - b1) / b1
    if rebound < WB_REBOUND_MIN_PCT:
        return 'rebound_too_small', {'rebound_pct': rebound * 100}

    after_c_lows = lows[ic + 1:]
    if not after_c_lows:
        return 'no_lows_after_neck', {}
    ib2_rel = min(range(len(after_c_lows)), key=lambda i: after_c_lows[i])
    ib2 = ic + 1 + ib2_rel
    b2 = lows[ib2]
    b2_to_neck = ib2 - ic
    if b2_to_neck < WB_B2_TO_NECK_MIN_H:
        return 'b2_too_close_to_neck', {'hours': b2_to_neck}

    bottom_diff = abs(b2 - b1) / b1
    if bottom_diff > WB_BOTTOM_DIFF_PCT:
        return 'bottoms_unaligned', {
            'bottom_diff_pct': bottom_diff * 100,
            'b1': b1, 'b2': b2,
        }

    gap_h = ib2 - i1
    if gap_h < WB_TIME_GAP_MIN_H:
        return 'gap_too_short', {'gap_h': gap_h}
    if gap_h > WB_TIME_GAP_MAX_H:
        return 'gap_too_long', {'gap_h': gap_h}

    cur_price = closes[-1]
    break_pct = (cur_price - c) / c
    if cur_price < c * (1 + WB_BREAK_NECK_PCT):
        return 'neck_not_broken', {
            'cur_price': cur_price,
            'neck': c,
            'break_pct': break_pct * 100,
            'b1': b1, 'b2': b2,
            'rebound_pct': rebound * 100,
            'bottom_diff_pct': bottom_diff * 100,
            'gap_h': gap_h,
        }

    return 'passed', {
        'b1': b1, 'b2': b2, 'neck': c, 'cur_price': cur_price,
        'rebound_pct': rebound * 100,
        'bottom_diff_pct': bottom_diff * 100,
        'gap_h': gap_h, 'break_pct': break_pct * 100,
    }


def main():
    conn = get_db()
    cur = conn.cursor()
    universe = get_universe(cur)
    print(f"扫描品种数: {len(universe)}")

    counts = {r: 0 for r in REASONS}
    near_miss = []   # 只差突破颈线
    passed_syms = [] # 完全通过

    for sym in universe:
        bars = get_15m_bars(cur, sym, WB_DATA_MIN_BARS + 24)
        reason, detail = diagnose(bars)
        counts[reason] += 1
        if reason == 'neck_not_broken':
            near_miss.append((sym, detail))
        elif reason == 'passed':
            passed_syms.append((sym, detail))

    cur.close()
    conn.close()

    print("\n=== 淘汰原因分布 ===")
    for r in REASONS:
        if counts[r]:
            print(f"  {r:26s}  {counts[r]:4d}")

    print(f"\n=== 差一步 (已成 W 形, 未突破颈线 +0.5%) ===")
    near_miss.sort(key=lambda x: -x[1]['break_pct'])
    for sym, d in near_miss[:20]:
        print(
            f"  {sym:16s}  break={d['break_pct']:+.2f}%  "
            f"反弹{d['rebound_pct']:.1f}%  两底差{d['bottom_diff_pct']:.2f}%  "
            f"gap={d['gap_h'] * 0.25:.1f}h"
        )
    if not near_miss:
        print("  (无)")

    print(f"\n=== 完全通过 ===")
    for sym, d in passed_syms:
        print(
            f"  {sym:16s}  B1={d['b1']:.6f}  B2={d['b2']:.6f}  neck={d['neck']:.6f}  "
            f"cur={d['cur_price']:.6f}  反弹{d['rebound_pct']:.1f}%  "
            f"gap={d['gap_h'] * 0.25:.1f}h  突破{d['break_pct']:+.2f}%"
        )
    if not passed_syms:
        print("  (无)")


if __name__ == '__main__':
    main()
