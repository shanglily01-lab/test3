"""
F3 运行时诊断:
  1. strategy_state 里有没有 strategy='f3' 的记录 (进程有没有碰过状态机)
  2. futures_orders 里有没有 source LIKE 'strategy_f3:%' 的记录 (开过单吗)
  3. 现在扫 paper 账户活跃 symbol 池, 计算当下有多少能命中 F3 条件 —— 如果数量 > 0 但实际没开单, 就是代码/配置问题; 如果 0 就是市场暂时没信号
只读.
"""
import sys
from datetime import datetime, timedelta, timezone
import pymysql

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DB = dict(host='13.212.252.171', port=3306, user='admin', password='Yintao@110',
          db='dimesion', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

# 复刻 strategy_f3.py 的阈值
F3_LOOKBACK_BARS = 7 * 24 * 4
F3_RECENT_24H_BARS = 24 * 4
F3_MIN_DROP = 0.20
F3_RECENT_24H_MIN = -0.05
F3_CH_24H_MAX = 0.02
F3_BODY_MIN = 0.01
F3_BODY_MAX = 0.03
F3_VOL_MULT_MIN = 1.5
F3_VOL_MULT_MAX = 3.0

F3_BLACKLIST = {'PENGU/USDT', 'EVAA/USDT', 'IR/USDT', 'DUSK/USDT',
                 'GPS/USDT', 'MYX/USDT', 'AAVE/USD'}
F3_WHITELIST = {'SPK/USDT', 'NEIRO/USDT', 'AVNT/USDT', 'ZBT/USDT', 'KERNEL/USDT',
                 'TREE/USDT', 'STRK/USDT', 'ENJ/USDT', 'TRIA/USDT'}
GLOBAL_BL = {'DENT/USDT', 'XAN/USDT', 'SUPER/USDT', 'GUN/USDT', 'UAI/USDT',
             'AAVE/USD', 'BTC/USD', 'XVG/USDT', 'TRU/USDT', 'DEGO/USDT',
             'ZRO/USDT', 'RIVER/USDT', 'Q/USDT', 'CHIP/USDT', 'SPK/USDT', 'UB/USDT'}


def effective_bl():
    return (GLOBAL_BL | F3_BLACKLIST) - F3_WHITELIST


def detect_f3_now(bars):
    """参数同策略代码"""
    if len(bars) < F3_LOOKBACK_BARS:
        return None, 'bars_not_enough'
    window = bars[-F3_LOOKBACK_BARS:]
    highs = [float(b['high_price']) for b in window]
    lows = [float(b['low_price']) for b in window]
    closes = [float(b['close_price']) for b in window]
    vols = [float(b['volume'] or 0) for b in window]
    w_high = max(highs); w_low = min(lows)
    if w_high <= 0:
        return None, 'no_high'
    drop = (w_high - w_low) / w_high
    if drop < F3_MIN_DROP:
        return None, 'drop_lt_20'
    n = len(window)
    if n < F3_RECENT_24H_BARS:
        return None, 'recent24_not_enough'
    recent24 = window[-F3_RECENT_24H_BARS:]
    r24_open = float(recent24[0]['open_price'])
    r24_low = min(float(b['low_price']) for b in recent24)
    r24_last = float(recent24[-1]['close_price'])
    if r24_open <= 0:
        return None, 'r24_open_zero'
    ch24 = (r24_last - r24_open) / r24_open
    if ch24 < F3_RECENT_24H_MIN:
        return None, f'still_dropping_24h={ch24*100:.1f}%'
    if r24_last < r24_low * 1.01:
        return None, 'at_24h_low'
    if ch24 > F3_CH_24H_MAX:
        return None, f'already_bounced_24h=+{ch24*100:.1f}%'
    last = window[-1]
    o = float(last['open_price']); c = float(last['close_price']); v = float(last['volume'] or 0)
    if o <= 0:
        return None, 'last_open_zero'
    if c <= o:
        return None, 'last_bar_not_bullish'
    body = (c - o) / o
    if body < F3_BODY_MIN:
        return None, f'body_too_small={body*100:.2f}%'
    if body >= F3_BODY_MAX:
        return None, f'body_too_large={body*100:.2f}%'
    avg_vol = sum(vols[-F3_RECENT_24H_BARS:]) / F3_RECENT_24H_BARS
    if avg_vol <= 0:
        return None, 'avg_vol_zero'
    vol_ratio = v / avg_vol
    if vol_ratio < F3_VOL_MULT_MIN:
        return None, f'vol_too_small={vol_ratio:.2f}x'
    if vol_ratio >= F3_VOL_MULT_MAX:
        return None, f'vol_too_large={vol_ratio:.2f}x'
    return {
        'drop': drop, 'ch24': ch24, 'body': body, 'vol_ratio': vol_ratio,
        'entry': c,
    }, 'OK'


def main():
    conn = pymysql.connect(**DB); cur = conn.cursor()
    try:
        # 1. strategy_state 看进程有没有碰过
        print("=" * 80)
        print("[1] strategy_state strategy='f3' 状态")
        print("=" * 80)
        cur.execute(
            "SELECT COUNT(*) AS n FROM strategy_state WHERE strategy='f3'"
        )
        total = cur.fetchone()['n']
        print(f"  总行数: {total}")
        if total > 0:
            cur.execute(
                """SELECT state, COUNT(*) AS n
                   FROM strategy_state WHERE strategy='f3'
                   GROUP BY state"""
            )
            for r in cur.fetchall():
                print(f"    state={r['state']:<12} n={r['n']}")
            # 最近更新
            cur.execute(
                """SELECT symbol, state, pid, order_id, entry_p,
                          created_at, updated_at
                   FROM strategy_state WHERE strategy='f3'
                   ORDER BY updated_at DESC LIMIT 10"""
            )
            print("  最近 10 行:")
            for r in cur.fetchall():
                print(f"    {r['symbol']:<14} state={r['state']:<8} pid={r['pid']} "
                      f"oid={r['order_id']} entry={r['entry_p']} "
                      f"updated={r['updated_at']}")
        print()

        # 2. futures_orders 有没有 F3 开单
        print("=" * 80)
        print("[2] futures_orders strategy_f3: 有没有开过单")
        print("=" * 80)
        cur.execute(
            """SELECT COUNT(*) AS n, MIN(created_at) AS first, MAX(created_at) AS latest
               FROM futures_orders WHERE order_source LIKE 'strategy_f3:%%'"""
        )
        r = cur.fetchone()
        print(f"  F3 订单总数: {r['n']}  first={r['first']}  latest={r['latest']}")
        if r['n'] > 0:
            cur.execute(
                """SELECT status, COUNT(*) AS n
                   FROM futures_orders WHERE order_source LIKE 'strategy_f3:%%'
                   GROUP BY status"""
            )
            for rr in cur.fetchall():
                print(f"    status={rr['status']:<12} n={rr['n']}")
            cur.execute(
                """SELECT id, symbol, side, order_type, status,
                          price, avg_fill_price, order_source,
                          created_at, fill_time, cancellation_reason
                   FROM futures_orders
                   WHERE order_source LIKE 'strategy_f3:%%'
                   ORDER BY id DESC LIMIT 10"""
            )
            print("  最近 10 单:")
            for o in cur.fetchall():
                print(f"    #{o['id']} {o['symbol']:<14} {o['side']:<10} "
                      f"{o['order_type']:<10} {o['status']:<10} "
                      f"price={o['price']} fill={o['avg_fill_price']} "
                      f"created={o['created_at']}")
        print()

        # 3. 现在扫一下有多少 symbol 会触发 F3
        print("=" * 80)
        print("[3] 现在扫 paper 账户活跃 symbol (仿 get_universe), 看当下 F3 能否命中")
        print("=" * 80)
        # 等同于 strategy_f3.get_universe 的逻辑
        cur.execute(
            """SELECT symbol FROM price_stats_24h
               WHERE updated_at >= NOW() - INTERVAL 30 MINUTE
                 AND quote_volume_24h > 5e6
               ORDER BY quote_volume_24h DESC LIMIT 200"""
        )
        bl = effective_bl()
        syms = [r['symbol'] for r in cur.fetchall() if r['symbol'] not in bl]
        for w in F3_WHITELIST:
            if w not in syms:
                syms.append(w)
        print(f"  universe 大小: {len(syms)} (黑名单 {len(bl)}, 白名单强制加 {len(F3_WHITELIST)})")

        # 逐个扫 F3
        hit_count = 0
        hits = []
        reason_stat = {}
        for sym in syms:
            cur.execute(
                """SELECT open_price, high_price, low_price, close_price, volume
                   FROM kline_data
                   WHERE symbol=%s AND timeframe='15m'
                     AND open_time + 900000 < UNIX_TIMESTAMP(NOW()) * 1000
                   ORDER BY open_time DESC LIMIT %s""",
                (sym, F3_LOOKBACK_BARS + 10),
            )
            bars = list(reversed(cur.fetchall()))
            sig, reason = detect_f3_now(bars)
            reason_stat[reason] = reason_stat.get(reason, 0) + 1
            if sig:
                hit_count += 1
                hits.append((sym, sig))

        print(f"  扫完 {len(syms)} 个 symbol")
        print(f"  命中 F3 的: {hit_count}")
        print(f"  -- 未命中原因分布 --")
        for rs, n in sorted(reason_stat.items(), key=lambda x: -x[1]):
            print(f"    {rs:<35}n={n}")
        if hits:
            print(f"\n  -- 命中 symbol 明细 --")
            for sym, sig in hits[:20]:
                print(f"    {sym:<14} drop={sig['drop']*100:.1f}%  "
                      f"ch24={sig['ch24']*100:+.2f}%  "
                      f"body={sig['body']*100:.2f}%  "
                      f"vol={sig['vol_ratio']:.2f}x  "
                      f"entry={sig['entry']}")
            if len(hits) > 20:
                print(f"    ... ({len(hits)-20} more)")

    finally:
        conn.close()


if __name__ == '__main__':
    main()
