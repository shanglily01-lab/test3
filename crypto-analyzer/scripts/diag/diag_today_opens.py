"""
查今天 (UTC+8) 各策略 paper/live 开仓情况, 用来回答"今天还开不出单吗".
注意: 远程库是 UTC0, 表里 open_time 是 UTC0; 用户视角"今天"是 UTC+8.
UTC+8 04-27 全天 = UTC0 04-26 16:00:00 ~ 04-27 16:00:00.
- futures_positions 今天 open_time 的所有仓, 按 source 分组
- futures_orders live_sync_status='SYNCED' 今天的同步开仓
- 最近 10 笔 paper 开仓 (不限今天) 看断点在哪
只读, 走远程 dimesion.
"""
import sys
from datetime import datetime, timedelta, timezone
import pymysql

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# IP 以 table_schemas.txt 头部为准
DB = dict(host='54.179.112.251', port=3306, user='admin', password='Yintao@110',
          db='dimesion', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)


def utc8_today_window_in_utc0():
    """返回 UTC+8 今天 0:00~24:00 对应的 UTC0 区间 (datetime 字符串)."""
    cn_tz = timezone(timedelta(hours=8))
    now_cn = datetime.now(cn_tz)
    cn_today_start = now_cn.replace(hour=0, minute=0, second=0, microsecond=0)
    cn_today_end = cn_today_start + timedelta(days=1)
    # 转 UTC0
    utc0_start = cn_today_start.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    utc0_end = cn_today_end.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    return utc0_start, utc0_end


def main():
    utc0_start, utc0_end = utc8_today_window_in_utc0()
    print(f"\n>>> UTC+8 今天 = UTC0 [{utc0_start}, {utc0_end})\n")

    conn = pymysql.connect(**DB); cur = conn.cursor()
    try:
        # 1) 今天 paper 端各策略开仓数 (按 source 前缀分组)
        print(f"=== UTC+8 今天 paper 开仓数 by source ===\n")
        cur.execute(
            """SELECT
                   SUBSTRING_INDEX(source, '/', 1) AS strat,
                   COUNT(*) AS n,
                   SUM(position_side='LONG') AS longs,
                   SUM(position_side='SHORT') AS shorts,
                   MIN(open_time) AS first_open,
                   MAX(open_time) AS last_open
               FROM futures_positions
               WHERE open_time >= %s AND open_time < %s
               GROUP BY SUBSTRING_INDEX(source, '/', 1)
               ORDER BY n DESC""",
            (utc0_start, utc0_end),
        )
        rows = cur.fetchall()
        if not rows:
            print("  [!] 今天 0 笔 paper 开仓")
        else:
            for r in rows:
                print(f"  {r['strat']:<25} n={r['n']:<3} L={r['longs']} S={r['shorts']} "
                      f"first(UTC0)={r['first_open']} last(UTC0)={r['last_open']}")

        # 2) 最近 10 笔 paper 开仓 (跨天) - 看断点
        print("\n=== 最近 10 笔 paper 开仓 (任意日期) ===\n")
        cur.execute(
            """SELECT id, symbol, position_side, source, status,
                      entry_price, open_time, close_time
               FROM futures_positions
               ORDER BY open_time DESC LIMIT 10"""
        )
        for r in cur.fetchall():
            print(f"  #{r['id']} {r['open_time']} {r['symbol']:<14} "
                  f"{r['position_side']:<5} status={r['status']:<8} "
                  f"src={r['source']}")

        # 3) 今天 live (futures_orders SYNCED) 开仓数
        print("\n=== 今天 live_sync (SYNCED) 开仓订单数 ===\n")
        cur.execute(
            """SELECT side, COUNT(*) AS n,
                      MIN(created_at) AS first_t, MAX(created_at) AS last_t
               FROM futures_orders
               WHERE DATE(created_at) = CURDATE()
                 AND live_sync_status='SYNCED'
                 AND side IN ('OPEN_LONG', 'OPEN_SHORT')
               GROUP BY side"""
        )
        rows = cur.fetchall()
        if not rows:
            print("  [!] 今天 0 笔 live 同步开仓 (实盘暂停期符合预期)")
        else:
            for r in rows:
                print(f"  {r['side']:<12} n={r['n']:<3} first={r['first_t']} last={r['last_t']}")

        # 4) 当前 paper 在持 (status=OPEN) 总数
        print("\n=== 当前 paper status=OPEN 持仓 ===\n")
        cur.execute(
            """SELECT
                   SUBSTRING_INDEX(source, '/', 1) AS strat,
                   COUNT(*) AS n,
                   SUM(position_side='LONG') AS longs,
                   SUM(position_side='SHORT') AS shorts
               FROM futures_positions
               WHERE status='OPEN'
               GROUP BY SUBSTRING_INDEX(source, '/', 1)
               ORDER BY n DESC"""
        )
        rows = cur.fetchall()
        if not rows:
            print("  [!] 当前无 OPEN 持仓")
        else:
            for r in rows:
                print(f"  {r['strat']:<25} n={r['n']} L={r['longs']} S={r['shorts']}")

        # 5b) 最近 5 天每天 paper 开仓数 (按 UTC+8 日切, by source)
        print("\n=== 最近 5 天 paper 开仓数 (UTC+8 日, by source) ===\n")
        cur.execute(
            """SELECT
                   DATE(DATE_ADD(open_time, INTERVAL 8 HOUR)) AS d_cn,
                   SUBSTRING_INDEX(source, '/', 1) AS strat,
                   COUNT(*) AS n
               FROM futures_positions
               WHERE open_time >= DATE_SUB(NOW(), INTERVAL 6 DAY)
               GROUP BY d_cn, SUBSTRING_INDEX(source, '/', 1)
               ORDER BY d_cn DESC, n DESC"""
        )
        rows = cur.fetchall()
        last_d = None
        for r in rows:
            if last_d != r['d_cn']:
                print(f"\n  -- {r['d_cn']} (UTC+8) --")
                last_d = r['d_cn']
            print(f"    {r['strat']:<25} n={r['n']}")

        # 5) kline_data 今天数据更新时间 (15m) - 看远程数据源是否在更新
        print("\n=== kline_data (15m) 各 symbol 最近 K 时间 (top 5 / bottom 5) ===\n")
        cur.execute(
            """SELECT symbol, MAX(open_time) AS last_kline_ts,
                      FROM_UNIXTIME(MAX(open_time)/1000) AS last_kline_dt
               FROM kline_data
               WHERE timeframe='15m'
               GROUP BY symbol
               ORDER BY last_kline_ts DESC LIMIT 5"""
        )
        print("  --- 最新的 5 个 ---")
        for r in cur.fetchall():
            print(f"    {r['symbol']:<16} {r['last_kline_dt']}")

        cur.execute(
            """SELECT symbol, MAX(open_time) AS last_kline_ts,
                      FROM_UNIXTIME(MAX(open_time)/1000) AS last_kline_dt
               FROM kline_data
               WHERE timeframe='15m'
               GROUP BY symbol
               ORDER BY last_kline_ts ASC LIMIT 5"""
        )
        print("  --- 最旧的 5 个 ---")
        for r in cur.fetchall():
            print(f"    {r['symbol']:<16} {r['last_kline_dt']}")
        print()

    finally:
        conn.close()


if __name__ == '__main__':
    main()
