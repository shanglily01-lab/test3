"""
04-27 (UTC+8) 那 12 个被 order_trigger_events 守卫拦掉的 LIMIT 信号:
反推"如果在 limit_price 上开仓, 到守卫拦截结束时 / 到当前最新 K, 浮盈浮亏多少".
用来回答"哪些守卫拦得不合理 (其实该开)".

判定规则:
- 拉 futures_orders 里 04-27 (UTC+8) 全天 status=CANCELLED / EXPIRED / REJECTED
  且 order_type=LIMIT, side IN OPEN_LONG/OPEN_SHORT 的订单
  (这些是被守卫拦掉的信号实例; 实际成交的会是 FILLED 状态)
- 对每个 order_id:
  - 取 first/last event_time 作为信号生命周期
  - 取 last_event_time 之后到现在的 K 线 (5m), 算最大顺势涨跌幅 / 最终涨跌幅
- 排序输出
只读. 走远程 dimesion.
"""
import sys
import pymysql

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DB = dict(host='54.179.112.251', port=3306, user='admin', password='Yintao@110',
          db='dimesion', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

# UTC+8 04-27 = UTC0 04-26 16:00 ~ 04-27 16:00
T_START = '2026-04-26 16:00:00'
T_END = '2026-04-27 16:00:00'


def fmt_pct(p):
    if p is None:
        return '   N/A '
    return f"{p:+6.2f}%"


def main():
    conn = pymysql.connect(**DB); cur = conn.cursor()
    try:
        # 1) 拉今天 (UTC+8) 所有有 trigger_event 记录的 order_id, join futures_orders
        cur.execute(
            """SELECT
                   ote.order_id,
                   COUNT(*) AS n_evt,
                   MIN(ote.event_time) AS first_t,
                   MAX(ote.event_time) AS last_t,
                   SUM(ote.event_type='5M_REJECT') AS n_5m_reject,
                   SUM(ote.event_type='TRIGGER_RETREAT') AS n_retreat,
                   SUM(ote.event_type='TRIGGER_OBSERVING') AS n_obs,
                   GROUP_CONCAT(DISTINCT ote.event_type ORDER BY ote.event_type) AS evt_types,
                   MAX(ote.detail) AS sample_detail,
                   fo.symbol, fo.side, fo.order_type, fo.status,
                   fo.price AS limit_price, fo.avg_fill_price,
                   fo.created_at, fo.canceled_at, fo.fill_time,
                   fo.cancellation_reason, fo.order_source
               FROM order_trigger_events ote
               LEFT JOIN futures_orders fo ON fo.order_id = ote.order_id
               WHERE ote.event_time >= %s AND ote.event_time < %s
               GROUP BY ote.order_id
               ORDER BY first_t""",
            (T_START, T_END),
        )
        orders = cur.fetchall()
        print(f">>> UTC+8 04-27 共 {len(orders)} 个被守卫处理过的 LIMIT 单\n")

        # 2) 对每个 order, 取信号末尾时间之后的价格走势
        rows = []
        for o in orders:
            sym = o['symbol']
            side = o['side']  # OPEN_LONG / OPEN_SHORT
            limit_p = float(o['limit_price'] or 0)
            last_t = o['last_t']
            if not sym or not side or limit_p <= 0 or not last_t:
                continue

            # 取 last_t 后到现在的 5m K (升序)
            cur.execute(
                """SELECT open_time, high_price, low_price, close_price
                   FROM kline_data
                   WHERE symbol=%s AND timeframe='5m'
                     AND open_time >= UNIX_TIMESTAMP(%s)*1000
                   ORDER BY open_time ASC LIMIT 60""",
                (sym, last_t),
            )
            ks = cur.fetchall()
            if not ks:
                continue

            highs = [float(k['high_price']) for k in ks]
            lows = [float(k['low_price']) for k in ks]
            last_close = float(ks[-1]['close_price'])
            last_kts_ms = int(ks[-1]['open_time'])

            # 模拟: 假设以 limit_p 开仓, 到现在 (last_close) 浮盈
            # 同时算最大顺势 / 最大不利
            if side == 'OPEN_LONG':
                pnl_now = (last_close - limit_p) / limit_p * 100
                max_fav = (max(highs) - limit_p) / limit_p * 100
                max_adv = (min(lows) - limit_p) / limit_p * 100  # 越负越不利
            else:  # OPEN_SHORT
                pnl_now = (limit_p - last_close) / limit_p * 100
                max_fav = (limit_p - min(lows)) / limit_p * 100
                max_adv = (limit_p - max(highs)) / limit_p * 100  # 越负越不利

            rows.append({
                **o,
                'pnl_now_pct': pnl_now,
                'max_fav_pct': max_fav,
                'max_adv_pct': max_adv,
                'last_close': last_close,
                'n_bars_after': len(ks),
            })

        # 3) 按 pnl_now 排序: 大正数 = 守卫"误拦"(本可赚), 大负数 = 守卫"救人"(否则亏)
        rows.sort(key=lambda x: x['pnl_now_pct'], reverse=True)

        print("=" * 130)
        print(f"{'symbol':<14}{'side':<12}{'src_tag':<10}{'limit':<14}"
              f"{'信号末时刻':<22}{'后续K':<6}"
              f"{'pnl@now':>10}{'max_fav':>10}{'max_adv':>10}"
              f"  evt(o/r/5x)  detail")
        print("=" * 130)

        n_misblocked = 0  # pnl_now >= +1% 视为误拦
        n_saved = 0       # pnl_now <= -1% 视为救人
        n_meh = 0         # |pnl_now| < 1% 视为无所谓

        for r in rows:
            tag = ''
            d = r.get('sample_detail', '') or ''
            if 'WHALE' in d: tag = 'WHALE'
            elif 'BIGMID' in d: tag = 'BIGMID'
            elif 'F3' in d: tag = 'F3'
            else: tag = 'live'

            verdict = ''
            if r['pnl_now_pct'] >= 1.0:
                verdict = '  *** 误拦 (本可赚)'; n_misblocked += 1
            elif r['pnl_now_pct'] <= -1.0:
                verdict = '  --- 救人 (开了会亏)'; n_saved += 1
            else:
                verdict = ''; n_meh += 1

            print(f"{r['symbol']:<14}{r['side']:<12}{tag:<10}{r['limit_price']!s:<14}"
                  f"{str(r['last_t']):<22}{r['n_bars_after']:<6}"
                  f"{fmt_pct(r['pnl_now_pct']):>10}{fmt_pct(r['max_fav_pct']):>10}{fmt_pct(r['max_adv_pct']):>10}"
                  f"  {r['n_obs']}/{r['n_retreat']}/{r['n_5m_reject']}"
                  f"  {d[:60]}{verdict}")

        print("\n" + "=" * 130)
        print(f"统计: 误拦 (pnl>=+1%): {n_misblocked}  |  救人 (pnl<=-1%): {n_saved}  |  无所谓: {n_meh}")
        print("注: pnl@now 假设按 limit_price 在信号触发时开仓, 到信号末时刻后最新 5m close 的浮动.")
        print("    max_fav 期间最大顺势, max_adv 期间最大逆势.")

    finally:
        conn.close()


if __name__ == '__main__':
    main()
