"""
诊断: price_stats_24h 及 whale_data_collector 写的相关表的陈旧程度

背景: 用户停了 strategy_whale + whale_data_collector 后 paper 也不开单.
怀疑: price_stats_24h.updated_at 已超过 30 分钟, 导致:
  - strategy_f3 get_universe() 第一查询返回 0 行 (要求 updated_at >= NOW()-30 MIN)
  - strategy_live/bigmid 的 24h 涨跌幅过滤拿到陈旧值

数据库: 远程 dimesion @ 54.179.112.251 (UTC 时区)
本脚本只读, 不写不改.
"""
import pymysql
import datetime as dt

DB = dict(
    host="54.179.112.251",
    port=3306,
    user="admin",
    password="Yintao@110",
    database="dimesion",
    charset="utf8mb4",
    cursorclass=pymysql.cursors.DictCursor,
)

# strategy_f3 get_universe() 用的阈值
F3_UNIVERSE_WINDOW_MIN = 30
F3_UNIVERSE_MIN_QVOL = 5e6


def fmt_age(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}s"
    if seconds < 3600:
        return f"{seconds/60:.1f}m"
    if seconds < 86400:
        return f"{seconds/3600:.1f}h"
    return f"{seconds/86400:.1f}d"


def main():
    conn = pymysql.connect(**DB)
    try:
        cur = conn.cursor()

        # 远程时钟基准 (UTC)
        cur.execute("SELECT NOW() AS now_utc")
        now_utc = cur.fetchone()["now_utc"]
        print(f"[REMOTE NOW] {now_utc} UTC")
        print()

        # ---------- price_stats_24h ----------
        print("=" * 70)
        print("price_stats_24h  (whale_data_collector.collect_24h_stats 每5分钟写)")
        print("=" * 70)
        cur.execute(
            "SELECT COUNT(*) AS n, MAX(updated_at) AS mx, MIN(updated_at) AS mn "
            "FROM price_stats_24h"
        )
        r = cur.fetchone()
        total_rows = r["n"]
        max_updated = r["mx"]
        min_updated = r["mn"]
        print(f"总行数              : {total_rows}")
        print(f"updated_at 最早     : {min_updated}")
        print(f"updated_at 最新     : {max_updated}")
        if max_updated:
            age_s = (now_utc - max_updated).total_seconds()
            print(f"距 NOW 多久         : {fmt_age(age_s)}")
        print()

        # strategy_f3 universe 第一查询模拟
        cur.execute(
            f"""SELECT COUNT(*) AS n FROM price_stats_24h
                WHERE updated_at >= NOW() - INTERVAL {F3_UNIVERSE_WINDOW_MIN} MINUTE
                  AND quote_volume_24h > {F3_UNIVERSE_MIN_QVOL}"""
        )
        f3_universe_n = cur.fetchone()["n"]
        print(f"strategy_f3 universe 第一查询命中: {f3_universe_n} 行")
        print(f"  (条件: updated_at >= NOW() - {F3_UNIVERSE_WINDOW_MIN}min "
              f"AND quote_volume_24h > {F3_UNIVERSE_MIN_QVOL:.0e})")
        if f3_universe_n == 0:
            print("  -> f3 已退到 kline_data fallback (universe 退化)")
        print()

        # 抽样几个主流币的 change_24h 快照
        sample_syms = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "BNB/USDT", "DOGE/USDT"]
        cur.execute(
            "SELECT symbol, change_24h, current_price, high_24h, low_24h, "
            "       quote_volume_24h, updated_at "
            "FROM price_stats_24h WHERE symbol IN %s "
            "ORDER BY symbol",
            (tuple(sample_syms),),
        )
        rows = cur.fetchall()
        print("主流币 change_24h 快照 (注意: 这些值是 [冻结时刻 vs 冻结时刻-24h] 的涨跌幅):")
        print(f"{'symbol':<14}{'change_24h':>12}{'current':>14}{'high_24h':>14}"
              f"{'low_24h':>14}{'updated_at':>22}")
        for r in rows:
            ch = r["change_24h"]
            cp = r["current_price"]
            hi = r["high_24h"]
            lo = r["low_24h"]
            ts = r["updated_at"]
            print(f"{r['symbol']:<14}{float(ch):>11.2f}%{float(cp):>14.4f}"
                  f"{float(hi):>14.4f}{float(lo):>14.4f}"
                  f"{str(ts):>22}")
        print()

        # ---------- 其他 3 张 whale 写的表, 顺便对照 ----------
        for tbl, ts_col, comment in [
            ("funding_rate_data",       "timestamp",  "每5分钟"),
            ("futures_open_interest",   "timestamp",  "每30分钟"),
            ("futures_long_short_ratio","timestamp",  "每30分钟"),
        ]:
            print("=" * 70)
            print(f"{tbl}  ({comment} 写)")
            print("=" * 70)
            cur.execute(
                f"SELECT COUNT(*) AS n, MAX({ts_col}) AS mx FROM {tbl}"
            )
            r = cur.fetchone()
            print(f"总行数              : {r['n']}")
            print(f"{ts_col} 最新     : {r['mx']}")
            if r["mx"]:
                age_s = (now_utc - r["mx"]).total_seconds()
                print(f"距 NOW 多久         : {fmt_age(age_s)}")
            print()

        # ---------- kline_data 对照 (确认 fast_collector 是否还在跑) ----------
        print("=" * 70)
        print("kline_data  (fast_collector_service 写, 用户说还在跑)")
        print("=" * 70)
        for tf in ("5m", "15m", "1h"):
            cur.execute(
                "SELECT COUNT(DISTINCT symbol) AS n_sym, MAX(open_time) AS mx_ot "
                "FROM kline_data WHERE timeframe=%s "
                "  AND open_time >= UNIX_TIMESTAMP(NOW()-INTERVAL 1 HOUR)*1000",
                (tf,),
            )
            r = cur.fetchone()
            n_sym = r["n_sym"] or 0
            mx_ot = r["mx_ot"]
            if mx_ot:
                mx_dt = dt.datetime.utcfromtimestamp(mx_ot / 1000)
                age_s = (now_utc - mx_dt).total_seconds()
                print(f"  {tf:>4}: 最近1小时内有 {n_sym} 个symbol; "
                      f"最新open_time={mx_dt} UTC ({fmt_age(age_s)} ago)")
            else:
                print(f"  {tf:>4}: 最近1小时内无数据 (fast_collector 可能也卡了)")
        print()

        # ---------- 最终诊断结论 ----------
        print("=" * 70)
        print("诊断结论")
        print("=" * 70)
        if max_updated:
            age_h = (now_utc - max_updated).total_seconds() / 3600
            if age_h < 0.5:
                print("price_stats_24h 数据新鲜, 不是它的问题, 排查别处")
            elif age_h < 24:
                print(f"price_stats_24h 已陈旧 {age_h:.1f} 小时:")
                print("  - strategy_f3 universe 第一查询失效, 走 kline_data fallback")
                print("  - strategy_live 5 处 24h 过滤拿陈旧 change_24h")
                print("  - strategy_bigmid Gemini 评分输入污染")
            else:
                print(f"price_stats_24h 已陈旧 {age_h:.1f} 小时 (超过24h):")
                print("  - change_24h 完全失真 (现在的 24h 窗口已经和 stale 不重叠)")
                print("  - 4 条策略的 24h 过滤集体失效, paper 不开单很合理")
                print("  - 修复: B) 一次性补采 ticker/24hr 立即恢复")
                print("         C) 把 24hr stats 挪进 data_sync_center 治本")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
