"""
最近 N 天止损 (SL / early-sl) 触发的仓位 × 开仓时刻该 symbol 的 regime 状态.
回答一个问题: 止损单集中出现在哪种行情分类里?

数据来源:
  - futures_positions: 拉止损触发的仓位
  - market_regime: 15m regime 记录 (最接近 open_time 的那条)
  - coin_kline_scores: 多周期打分 (最接近 open_time 的那条)
  - price_stats_24h: 24h 涨跌幅和 trend

只读, 不改任何代码.
用法: python scripts/diag/diag_sl_by_regime.py [DAYS]
默认 DAYS=7.
"""
import sys
from datetime import date, timedelta
import pymysql

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DB = dict(host='13.212.252.171', port=3306, user='admin', password='Yintao@110',
          db='dimesion', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

DAYS = int(sys.argv[1]) if len(sys.argv) > 1 else 7
END = date.today()
START = END - timedelta(days=DAYS - 1)


def bucket_24h_change(ch):
    if ch is None:
        return 'unknown'
    ch = float(ch)
    if ch >= 20:
        return '>=20% (飞天)'
    if ch >= 10:
        return '10~20% (暴涨)'
    if ch >= 3:
        return '3~10% (上涨)'
    if ch > -3:
        return '-3~3% (横盘)'
    if ch > -10:
        return '-10~-3% (下跌)'
    if ch > -20:
        return '-20~-10% (暴跌)'
    return '<-20% (崩盘)'


def bucket_range_pct(rp):
    if rp is None:
        return 'unknown'
    rp = float(rp)
    if rp >= 20:
        return '>=20% (剧烈波动)'
    if rp >= 10:
        return '10~20% (大波动)'
    if rp >= 5:
        return '5~10% (中波动)'
    if rp >= 2:
        return '2~5% (小波动)'
    return '<2% (窄幅)'


def main():
    conn = pymysql.connect(**DB)
    cur = conn.cursor()
    try:
        # 1. 拉最近 N 天所有已平仓位, 标记 SL / early-sl / 手动 / trail-tp / 其他
        cur.execute(
            """SELECT id, symbol, position_side, entry_price, realized_pnl,
                      open_time, close_time, notes,
                      TIMESTAMPDIFF(MINUTE, open_time, close_time) as hold_min
               FROM futures_positions
               WHERE DATE(close_time) BETWEEN %s AND %s
                 AND status IN ('closed','liquidated')
               ORDER BY open_time ASC""",
            (START.isoformat(), END.isoformat()),
        )
        all_pos = cur.fetchall()
        sl_pos = []
        for p in all_pos:
            n = (p['notes'] or '').lower()
            if any(k in n for k in ['early-sl', 'hard-sl', 'breakeven-sl', '止损']) and '手动' not in (p['notes'] or ''):
                sl_pos.append(p)
            elif (p['realized_pnl'] or 0) < 0 and ('sl' in n or '止损' in (p['notes'] or '')):
                sl_pos.append(p)

        print(f"\n### {START} ~ {END}  ({DAYS} 天)  共 {len(all_pos)} 仓已平, 其中 SL 类 {len(sl_pos)} 仓 ###\n")

        # 2. 对每个 SL 仓位, 查开仓时刻最近的 regime / kline_scores / 24h_stats
        enriched = []
        for p in sl_pos:
            sym = p['symbol']
            ot = p['open_time']

            # market_regime: 找 open_time 前最近的一条 15m 记录
            cur.execute(
                """SELECT regime_type, regime_score, adx_value, volatility, trend_bars, ema_diff_pct
                   FROM market_regime
                   WHERE symbol=%s AND timeframe='15m' AND detected_at <= %s
                   ORDER BY detected_at DESC LIMIT 1""",
                (sym, ot),
            )
            mr = cur.fetchone() or {}

            # coin_kline_scores: 最接近的一条 (该表是 updated on change)
            cur.execute(
                """SELECT total_score, main_score, direction, strength_level,
                          h1_level, m15_level, updated_at
                   FROM coin_kline_scores
                   WHERE symbol=%s AND updated_at <= %s
                   ORDER BY updated_at DESC LIMIT 1""",
                (sym, ot),
            )
            ks = cur.fetchone() or {}

            # price_stats_24h: 开仓时最近的统计
            cur.execute(
                """SELECT change_24h, price_range_pct, trend, quote_volume_24h, updated_at
                   FROM price_stats_24h
                   WHERE symbol=%s AND updated_at <= %s
                   ORDER BY updated_at DESC LIMIT 1""",
                (sym, ot),
            )
            ps = cur.fetchone() or {}

            enriched.append({
                'pos': p, 'mr': mr, 'ks': ks, 'ps': ps,
            })

        # 3. 明细输出
        print("-- SL 仓位明细 (按时间) --")
        print(f"{'symbol':<14}{'side':<6}{'pnl':>8} {'hold':>5}  {'24h%':>7} {'range%':>7} "
              f"{'trend':<12} {'regime':<18} {'adx':>5} {'kline_dir':<10} {'lvl':<6} {'score':>5}  {'open':<16}")
        for e in enriched:
            p, mr, ks, ps = e['pos'], e['mr'], e['ks'], e['ps']
            ch = ps.get('change_24h')
            rng = ps.get('price_range_pct')
            trend = ps.get('trend') or '-'
            regime = mr.get('regime_type') or '-'
            adx = mr.get('adx_value')
            kdir = ks.get('direction') or '-'
            klvl = ks.get('h1_level') or '-'
            ksc = ks.get('total_score')
            print(f"{p['symbol']:<14}{p['position_side']:<6}{float(p['realized_pnl'] or 0):>+8.1f} "
                  f"{p['hold_min'] or 0:>4}m  "
                  f"{float(ch) if ch is not None else 0:>+7.2f} "
                  f"{float(rng) if rng is not None else 0:>7.2f} "
                  f"{trend:<12} {regime:<18} "
                  f"{float(adx) if adx is not None else 0:>5.1f} "
                  f"{kdir:<10} {klvl:<6} "
                  f"{ksc if ksc is not None else 0:>5}  "
                  f"{str(p['open_time'])[:16]}")

        # 4. 按 regime 分组
        print("\n" + "=" * 90)
        print("[按 market_regime 15m regime_type 分组]")
        print("=" * 90)
        by_regime = {}
        for e in enriched:
            key = (e['mr'].get('regime_type') or '(none)', e['pos']['position_side'])
            by_regime.setdefault(key, []).append(e)
        print(f"{'regime':<20} {'side':<6} {'n':>3} {'net_pnl':>10} {'avg_pnl':>8}")
        for (regime, side), lst in sorted(by_regime.items(), key=lambda x: -len(x[1])):
            pnl = sum(float(e['pos']['realized_pnl'] or 0) for e in lst)
            print(f"{regime:<20} {side:<6} {len(lst):>3} {pnl:>+10.2f} {pnl/len(lst):>+8.2f}")

        # 5. 按 24h 涨跌幅桶分组
        print("\n" + "=" * 90)
        print("[按 24h 涨跌幅分桶]")
        print("=" * 90)
        by_ch = {}
        for e in enriched:
            key = (bucket_24h_change(e['ps'].get('change_24h')), e['pos']['position_side'])
            by_ch.setdefault(key, []).append(e)
        print(f"{'24h_bucket':<20} {'side':<6} {'n':>3} {'net_pnl':>10}")
        for (bucket, side), lst in sorted(by_ch.items(), key=lambda x: -len(x[1])):
            pnl = sum(float(e['pos']['realized_pnl'] or 0) for e in lst)
            print(f"{bucket:<20} {side:<6} {len(lst):>3} {pnl:>+10.2f}")

        # 6. 按 24h 波动幅度桶
        print("\n" + "=" * 90)
        print("[按 24h 波动幅度 price_range_pct 分桶]")
        print("=" * 90)
        by_rng = {}
        for e in enriched:
            key = bucket_range_pct(e['ps'].get('price_range_pct'))
            by_rng.setdefault(key, []).append(e)
        print(f"{'range_bucket':<20} {'n':>3} {'net_pnl':>10}")
        for bucket, lst in sorted(by_rng.items(), key=lambda x: -len(x[1])):
            pnl = sum(float(e['pos']['realized_pnl'] or 0) for e in lst)
            print(f"{bucket:<20} {len(lst):>3} {pnl:>+10.2f}")

        # 7. 按 kline_scores direction vs 仓位方向 (是否与评分方向一致)
        print("\n" + "=" * 90)
        print("[入场方向 vs coin_kline_scores.direction]")
        print("=" * 90)
        aligned = 0
        conflict = 0
        neutral = 0
        align_pnl = 0.0
        conflict_pnl = 0.0
        neutral_pnl = 0.0
        for e in enriched:
            side = e['pos']['position_side']
            kdir = (e['ks'].get('direction') or 'NEUTRAL').upper()
            pnl = float(e['pos']['realized_pnl'] or 0)
            if (side == 'LONG' and kdir == 'LONG') or (side == 'SHORT' and kdir == 'SHORT'):
                aligned += 1; align_pnl += pnl
            elif kdir == 'NEUTRAL':
                neutral += 1; neutral_pnl += pnl
            else:
                conflict += 1; conflict_pnl += pnl
        total = max(1, aligned + conflict + neutral)
        print(f"  和评分方向一致: {aligned:>3} ({aligned/total*100:>5.1f}%)  净 pnl {align_pnl:+.2f}")
        print(f"  和评分方向冲突: {conflict:>3} ({conflict/total*100:>5.1f}%)  净 pnl {conflict_pnl:+.2f}")
        print(f"  评分 NEUTRAL  : {neutral:>3} ({neutral/total*100:>5.1f}%)  净 pnl {neutral_pnl:+.2f}")
        print()

    finally:
        conn.close()


if __name__ == '__main__':
    main()
