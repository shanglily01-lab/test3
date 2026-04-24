"""
最近 7 天 SL 涉及的所有 symbol × 当前 price_stats_24h 画像.
分类: 按 24h 涨跌幅 / 24h 波动幅度 / 成交额 / trend 字段.
回答: 这些让策略止损的币到底是什么性格?
"""
import sys
from datetime import date, timedelta
import pymysql

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DB = dict(host='13.212.252.171', port=3306, user='admin', password='Yintao@110',
          db='dimesion', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)


def bucket_range(rp):
    if rp is None: return '?'
    rp = float(rp)
    if rp >= 30: return '>=30% 疯狂'
    if rp >= 15: return '15~30% 剧烈'
    if rp >= 8:  return '8~15% 大波动'
    if rp >= 4:  return '4~8% 中波动'
    return '<4% 窄幅'


def bucket_vol(v):
    if v is None: return '?'
    v = float(v)
    if v >= 1e8:    return '>100M USDT (大币)'
    if v >= 1e7:    return '10~100M (中币)'
    if v >= 1e6:    return '1~10M (小币)'
    if v >= 1e5:    return '100K~1M (微币)'
    return '<100K (极小)'


def main():
    conn = pymysql.connect(**DB); cur = conn.cursor()
    try:
        end = date.today(); start = end - timedelta(days=6)
        # 1. 拉 SL 涉及的所有 symbol + 累计该 symbol 的 SL 亏损
        cur.execute(
            """SELECT symbol,
                      COUNT(*) AS n,
                      SUM(realized_pnl) AS total_pnl,
                      SUM(CASE WHEN position_side='LONG' THEN 1 ELSE 0 END) AS long_n,
                      SUM(CASE WHEN position_side='SHORT' THEN 1 ELSE 0 END) AS short_n
               FROM futures_positions
               WHERE DATE(close_time) BETWEEN %s AND %s
                 AND status IN ('closed','liquidated')
                 AND realized_pnl < 0
                 AND (notes LIKE '%%early-sl%%' OR notes LIKE '%%止损%%' OR notes LIKE '%%breakeven%%')
               GROUP BY symbol
               ORDER BY total_pnl ASC""",
            (start.isoformat(), end.isoformat()),
        )
        sl = cur.fetchall()

        # 2. 拉 price_stats_24h
        syms = [r['symbol'] for r in sl]
        placeholders = ','.join(['%s'] * len(syms))
        cur.execute(
            f"""SELECT symbol, current_price, change_24h, price_range_pct,
                       quote_volume_24h, trend, updated_at
                FROM price_stats_24h WHERE symbol IN ({placeholders})""",
            tuple(syms),
        )
        stats = {r['symbol']: r for r in cur.fetchall()}

        # 3. 合并明细
        print(f"\n### SL 涉及的 {len(sl)} 个币 × 当前 24h 画像 ###\n")
        print(f"{'symbol':<15}{'SL笔数':>6}{'L':>3}{'S':>3}{'累计亏':>10}  "
              f"{'24h%':>7}{'range%':>7}{'vol_24h':>12}  {'trend':<12}{'性格':<15}")
        rows = []
        for s in sl:
            sym = s['symbol']
            st = stats.get(sym, {})
            ch = st.get('change_24h')
            rp = st.get('price_range_pct')
            vol = st.get('quote_volume_24h')
            trend = st.get('trend') or '-'
            rng_bucket = bucket_range(rp)
            vol_bucket = bucket_vol(vol)
            print(f"{sym:<15}{s['n']:>6}{s['long_n']:>3}{s['short_n']:>3}"
                  f"{float(s['total_pnl']):>+10.1f}  "
                  f"{float(ch) if ch is not None else 0:>+7.2f}"
                  f"{float(rp) if rp is not None else 0:>7.2f}"
                  f"{(float(vol) if vol is not None else 0)/1e6:>10.2f}M  "
                  f"{trend:<12}{rng_bucket:<15}")
            rows.append({'sym': sym, 'n': s['n'], 'pnl': float(s['total_pnl']),
                         'ch': float(ch) if ch else 0,
                         'rp': float(rp) if rp else 0,
                         'vol': float(vol) if vol else 0,
                         'trend': trend, 'rng_bucket': rng_bucket,
                         'vol_bucket': vol_bucket})

        # 4. 按波动幅度分桶
        print("\n" + "=" * 80)
        print("[按 24h 波动幅度分类]")
        print("=" * 80)
        by_rng = {}
        for r in rows:
            by_rng.setdefault(r['rng_bucket'], []).append(r)
        print(f"{'bucket':<16}{'币数':>5}{'SL笔数':>7}{'累计亏损':>12}  {'典型币种'}")
        for bucket, lst in sorted(by_rng.items(), key=lambda x: -sum(r['n'] for r in x[1])):
            n_sym = len(lst)
            n_sl = sum(r['n'] for r in lst)
            pnl = sum(r['pnl'] for r in lst)
            examples = ', '.join(r['sym'] for r in sorted(lst, key=lambda r: r['pnl'])[:5])
            print(f"{bucket:<16}{n_sym:>5}{n_sl:>7}{pnl:>+12.1f}  {examples}")

        # 5. 按成交额（币大小）分桶
        print("\n" + "=" * 80)
        print("[按 24h 成交额分类 (币的大小/流动性)]")
        print("=" * 80)
        by_vol = {}
        for r in rows:
            by_vol.setdefault(r['vol_bucket'], []).append(r)
        print(f"{'bucket':<22}{'币数':>5}{'SL笔数':>7}{'累计亏损':>12}  {'典型币种'}")
        for bucket, lst in sorted(by_vol.items(), key=lambda x: -sum(r['n'] for r in x[1])):
            n_sym = len(lst)
            n_sl = sum(r['n'] for r in lst)
            pnl = sum(r['pnl'] for r in lst)
            examples = ', '.join(r['sym'] for r in sorted(lst, key=lambda r: r['pnl'])[:5])
            print(f"{bucket:<22}{n_sym:>5}{n_sl:>7}{pnl:>+12.1f}  {examples}")

        # 6. 按当前 24h 涨跌方向
        print("\n" + "=" * 80)
        print("[按当前 24h 涨跌幅分类]")
        print("=" * 80)
        def ch_bucket(ch):
            if ch >= 20: return '>=20% 暴涨'
            if ch >= 5: return '5~20% 上涨'
            if ch >= -5: return '-5~5% 横盘'
            if ch >= -20: return '-20~-5% 下跌'
            return '<-20% 暴跌'
        by_ch = {}
        for r in rows:
            by_ch.setdefault(ch_bucket(r['ch']), []).append(r)
        print(f"{'bucket':<16}{'币数':>5}{'SL笔数':>7}{'累计亏损':>12}  {'典型币种'}")
        for bucket, lst in sorted(by_ch.items(), key=lambda x: -sum(r['n'] for r in x[1])):
            n_sym = len(lst)
            n_sl = sum(r['n'] for r in lst)
            pnl = sum(r['pnl'] for r in lst)
            examples = ', '.join(r['sym'] for r in sorted(lst, key=lambda r: r['pnl'])[:5])
            print(f"{bucket:<16}{n_sym:>5}{n_sl:>7}{pnl:>+12.1f}  {examples}")

        # 7. 按 price_stats_24h.trend 字段
        print("\n" + "=" * 80)
        print("[按 price_stats_24h.trend 字段]")
        print("=" * 80)
        by_trend = {}
        for r in rows:
            by_trend.setdefault(r['trend'], []).append(r)
        print(f"{'trend':<18}{'币数':>5}{'SL笔数':>7}{'累计亏损':>12}")
        for t, lst in sorted(by_trend.items(), key=lambda x: -len(x[1])):
            n_sym = len(lst)
            n_sl = sum(r['n'] for r in lst)
            pnl = sum(r['pnl'] for r in lst)
            print(f"{t:<18}{n_sym:>5}{n_sl:>7}{pnl:>+12.1f}")
    finally:
        conn.close()


if __name__ == '__main__':
    main()
