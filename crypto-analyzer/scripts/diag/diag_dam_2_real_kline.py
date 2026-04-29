"""
DAM/USDT 04-28 chase-entry 假成交价取证

诊断目标:
  REMOTE fill_price 0.02948000 (服务器 UTC 04-28 16:16:53)
  LOCAL  fill_price 0.03542000 (北京 04-29 00:13:11 = UTC 04-28 16:13:11)
  这两个价格是否真的在 binance kline 上出现过, 还是 L3 hyperliquid fallback / 缓存 / 数据腐烂?

查 binance 真实 kline (kline_data 表, exchange='binance') 在 UTC 04-28 15:00 ~ 17:00:
  - 5m: 24 根, 看完整 high/low/open/close
  - 1m: 120 根, 重点看 16:13~16:17 这 5 分钟 minute 粒度有没有 0.029 / 0.035

如果两笔的 fill price 都不在 binance 当时 high/low 区间 → 系统从非 binance 源拿到了"幽灵价"
"""
import sys
import pymysql
from datetime import datetime, timezone

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

REMOTE_DB = {
    "host": "54.179.112.251", "port": 3306, "user": "admin",
    "password": "Yintao@110", "database": "dimesion", "charset": "utf8mb4",
}

SYM = "DAM/USDT"

# UTC 时间窗 (两笔事件都在 UTC 04-28 16:13~16:17)
UTC_FROM = datetime(2026, 4, 28, 15, 0, 0, tzinfo=timezone.utc)
UTC_TO   = datetime(2026, 4, 28, 17, 0, 0, tzinfo=timezone.utc)
MS_FROM = int(UTC_FROM.timestamp() * 1000)
MS_TO   = int(UTC_TO.timestamp() * 1000)


def section(t):
    print("\n" + "=" * 100)
    print(t)
    print("=" * 100)


def dump_kline(cur, tf, exch="binance_futures"):
    cur.execute(
        """
        SELECT exchange, timeframe, open_time, FROM_UNIXTIME(open_time/1000) AS open_dt_utc,
               open_price, high_price, low_price, close_price, volume, quote_volume
        FROM kline_data
        WHERE symbol=%s AND exchange=%s AND timeframe=%s
          AND open_time BETWEEN %s AND %s
        ORDER BY open_time ASC
        """,
        (SYM, exch, tf, MS_FROM, MS_TO),
    )
    rows = cur.fetchall()
    if not rows:
        print(f"  ({exch} / {tf}) 0 行")
        return rows
    print(f"  exchange={exch}  timeframe={tf}  共 {len(rows)} 根 ({UTC_FROM.isoformat()} ~ {UTC_TO.isoformat()})")
    print(f"    {'open_time(UTC)':<22} {'open':>12} {'high':>12} {'low':>12} {'close':>12} {'vol':>14} {'quote_vol':>14}")
    for r in rows:
        # 标记是否包含 fill price
        marks = []
        lo = float(r["low_price"]); hi = float(r["high_price"])
        if lo <= 0.02948 <= hi:
            marks.append("<-- 0.02948 落在区间")
        if lo <= 0.03542 <= hi:
            marks.append("<-- 0.03542 落在区间")
        m = "  " + " ".join(marks) if marks else ""
        print(f"    {str(r['open_dt_utc']):<22} {float(r['open_price']):>12.8f} {float(r['high_price']):>12.8f} {float(r['low_price']):>12.8f} {float(r['close_price']):>12.8f} {float(r['volume'] or 0):>14.2f} {float(r['quote_volume'] or 0):>14.2f}{m}")
    return rows


def overall_range(rows):
    if not rows:
        return None
    his = [float(r["high_price"]) for r in rows]
    los = [float(r["low_price"])  for r in rows]
    return min(los), max(his)


def main():
    conn = pymysql.connect(**REMOTE_DB)
    cur = conn.cursor(pymysql.cursors.DictCursor)

    section("DAM/USDT binance_futures 5m K 线 (UTC 04-28 15:00 ~ 17:00)")
    rows_5m = dump_kline(cur, "5m", "binance_futures")

    section("DAM/USDT binance_futures 1m K 线 (注: DAM 只有 5m+, 没有 1m)")
    rows_1m = dump_kline(cur, "1m", "binance_futures")

    section("DAM/USDT binance_futures 15m K 线 (UTC 04-28 15:00 ~ 17:00)")
    rows_15m = dump_kline(cur, "15m", "binance_futures")

    section("DAM/USDT 哪些 exchange × timeframe 在窗口内有数据")
    cur.execute(
        """
        SELECT exchange, timeframe, COUNT(*) AS n,
               MIN(low_price) AS lo, MAX(high_price) AS hi,
               MIN(FROM_UNIXTIME(open_time/1000)) AS ts_min,
               MAX(FROM_UNIXTIME(open_time/1000)) AS ts_max
        FROM kline_data
        WHERE symbol=%s AND open_time BETWEEN %s AND %s
        GROUP BY exchange, timeframe
        ORDER BY exchange, timeframe
        """,
        (SYM, MS_FROM, MS_TO),
    )
    for r in cur.fetchall():
        print(f"  exchange={r['exchange']} tf={r['timeframe']} n={r['n']} low~high={r['lo']}~{r['hi']} {r['ts_min']} ~ {r['ts_max']}")

    section("逻辑判定")
    rng_5m = overall_range(rows_5m)
    rng_1m = overall_range(rows_1m)
    print(f"  binance_futures 5m 全窗 low~high : {rng_5m}")
    print(f"  binance_futures 1m 全窗 low~high : {rng_1m}")
    print(f"  REMOTE  fill_price       : 0.02948000")
    print(f"  LOCAL   fill_price       : 0.03542000")
    if rng_1m:
        lo, hi = rng_1m
        print(f"  -> 0.02948 是否在 binance_futures 1m 真实区间内?  {'YES' if lo <= 0.02948 <= hi else 'NO  (binance 上根本没出现过这个价)'}")
        print(f"  -> 0.03542 是否在 binance_futures 1m 真实区间内?  {'YES' if lo <= 0.03542 <= hi else 'NO  (binance 上根本没出现过这个价)'}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
