"""
Dashboard 快照服务
每5分钟预计算所有 Dashboard 所需数据并存入 dashboard_snapshot 表，
前端调用 GET /api/dashboard/snapshot 可在毫秒内获取完整数据。
"""
import json
import time
import pymysql
import os
from datetime import datetime, timezone
from loguru import logger


def _get_conn():
    return pymysql.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        user=os.getenv('DB_USER', ''),
        password=os.getenv('DB_PASSWORD', ''),
        database=os.getenv('DB_NAME', ''),
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=10
    )


def _ensure_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dashboard_snapshot (
            snapshot_key VARCHAR(50) PRIMARY KEY,
            snapshot_json MEDIUMTEXT NOT NULL,
            updated_at   DATETIME    NOT NULL,
            compute_ms   INT         DEFAULT 0
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)


def _fetch_signals(cursor):
    cursor.execute("""
        SELECT symbol, total_score, direction,
               h1_score, m15_score,
               h1_bullish_count  AS h1_bullish,
               h1_bearish_count  AS h1_bearish,
               m15_bullish_count AS m15_bullish,
               m15_bearish_count AS m15_bearish,
               m5_bullish_count  AS m5_bullish,
               m5_bearish_count  AS m5_bearish,
               strength_level, updated_at
        FROM coin_kline_scores
        WHERE exchange = 'binance_futures'
        ORDER BY ABS(total_score) DESC
        LIMIT 20
    """)
    rows = cursor.fetchall()
    result = []
    for r in rows:
        result.append({
            'symbol':        r['symbol'],
            'total_score':   float(r['total_score']) if r['total_score'] is not None else 0,
            'direction':     r['direction'],
            'h1_score':      float(r['h1_score'])   if r['h1_score']   is not None else None,
            'm15_score':     float(r['m15_score'])  if r['m15_score']  is not None else None,
            'h1_bullish':    int(r['h1_bullish'])   if r['h1_bullish'] is not None else 0,
            'h1_bearish':    int(r['h1_bearish'])   if r['h1_bearish'] is not None else 0,
            'm15_bullish':   int(r['m15_bullish'])  if r['m15_bullish'] is not None else 0,
            'm15_bearish':   int(r['m15_bearish'])  if r['m15_bearish'] is not None else 0,
            'm5_bullish':    int(r['m5_bullish'])   if r['m5_bullish'] is not None else 0,
            'm5_bearish':    int(r['m5_bearish'])   if r['m5_bearish'] is not None else 0,
            'strength_level': r['strength_level'],
            'updated_at':    r['updated_at'].isoformat() if r['updated_at'] else None,
        })
    return result


def _fetch_stats(cursor):
    # 今日开仓总数 + 盈亏 + 胜率（来自 futures_positions）
    # 使用 CURDATE() 与 DB 时区一致，避免 Python UTC 与 MySQL 本地时区不匹配导致统计为 0
    cursor.execute("""
        SELECT
            COUNT(*) AS total_opened,
            SUM(CASE WHEN status <> 'OPEN' THEN COALESCE(realized_pnl, 0) ELSE 0 END) AS today_pnl,
            SUM(CASE WHEN status <> 'OPEN' AND COALESCE(realized_pnl, 0) > 0 THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN status <> 'OPEN' THEN 1 ELSE 0 END) AS closed_count
        FROM futures_positions
        WHERE account_id = 2
          AND DATE(open_time) = CURDATE()
    """)
    r = cursor.fetchone() or {}
    # 当前有信号的交易对数（来自 coin_kline_scores，作为"今日信号数"展示）
    cursor.execute("""
        SELECT COUNT(*) AS sig_count
        FROM coin_kline_scores
        WHERE exchange = 'binance_futures'
    """)
    sig_r = cursor.fetchone() or {}
    closed = int(r.get('closed_count') or 0)
    wins   = int(r.get('wins')         or 0)
    return {
        'today_signals': int(sig_r.get('sig_count')  or 0),   # 当前有信号的交易对数
        'today_open':    int(r.get('total_opened')    or 0),   # 今日开仓总数
        'today_pnl':     float(r.get('today_pnl')    or 0),
        'win_rate':      round(wins / closed * 100, 1) if closed > 0 else None,
    }


def _fetch_futures(cursor):
    # Bulk fetch latest OI per symbol (2 queries total instead of N*2)
    cursor.execute("""
        SELECT t1.symbol, t1.open_interest, t1.timestamp
        FROM futures_open_interest t1
        INNER JOIN (
            SELECT symbol, MAX(timestamp) AS max_ts
            FROM futures_open_interest
            WHERE exchange = 'binance_futures'
            GROUP BY symbol
        ) t2 ON t1.symbol = t2.symbol AND t1.timestamp = t2.max_ts
        WHERE t1.exchange = 'binance_futures'
    """)
    oi_map = {}
    for r in cursor.fetchall():
        oi_map[r['symbol']] = {
            'open_interest': float(r['open_interest']),
            'timestamp':     r['timestamp'].isoformat() if r['timestamp'] else None,
        }

    # Bulk fetch latest LSR per symbol
    cursor.execute("""
        SELECT t1.symbol,
               t1.long_account, t1.short_account, t1.long_short_ratio,
               t1.long_position, t1.short_position, t1.long_short_position_ratio,
               t1.timestamp
        FROM futures_long_short_ratio t1
        INNER JOIN (
            SELECT symbol, MAX(timestamp) AS max_ts
            FROM futures_long_short_ratio
            GROUP BY symbol
        ) t2 ON t1.symbol = t2.symbol AND t1.timestamp = t2.max_ts
    """)
    lsr_map = {}
    for r in cursor.fetchall():
        lsr_map[r['symbol']] = {
            'long_account':  float(r['long_account'])  if r['long_account']  is not None else None,
            'short_account': float(r['short_account']) if r['short_account'] is not None else None,
            'ratio':         float(r['long_short_ratio']) if r['long_short_ratio'] is not None else None,
            'long_position':  float(r['long_position'])  if r['long_position']  is not None else None,
            'short_position': float(r['short_position']) if r['short_position'] is not None else None,
            'position_ratio': float(r['long_short_position_ratio']) if r['long_short_position_ratio'] is not None else None,
            'timestamp':      r['timestamp'].isoformat() if r['timestamp'] else None,
        }

    symbols = set(oi_map) | set(lsr_map)
    result = []
    for sym in sorted(symbols):
        oi  = oi_map.get(sym, {})
        lsr = lsr_map.get(sym, {})
        result.append({
            'symbol':        sym,
            'open_interest': oi.get('open_interest'),
            'timestamp':     oi.get('timestamp') or lsr.get('timestamp'),
            'long_short_ratio': {
                'long_account':  lsr.get('long_account'),
                'short_account': lsr.get('short_account'),
                'ratio':         lsr.get('ratio'),
            } if lsr else None,
            'long_short_position_ratio': {
                'long_position':  lsr.get('long_position'),
                'short_position': lsr.get('short_position'),
                'ratio':          lsr.get('position_ratio'),
            } if lsr else None,
        })
    return result


def _fetch_winrate_history(cursor):
    """近10日每日胜率 + 捕获率，用于看板趋势图"""
    cursor.execute("""
        SELECT date,
               CAST(JSON_UNQUOTE(JSON_EXTRACT(report_json, '$.trading_summary.win_rate'))
                    AS DECIMAL(6,2)) AS win_rate,
               capture_rate
        FROM daily_review_reports
        ORDER BY date DESC
        LIMIT 10
    """)
    rows = cursor.fetchall()
    result = []
    for r in rows:
        d = r['date']
        date_str = d.strftime('%m/%d') if hasattr(d, 'strftime') else str(d)[5:10]
        result.append({
            'date':         date_str,
            'win_rate':     float(r['win_rate'])     if r['win_rate']     is not None else None,
            'capture_rate': float(r['capture_rate']) if r['capture_rate'] is not None else None,
        })
    result.reverse()   # 由旧到新，左到右显示
    return result


def _fetch_news(cursor):
    cursor.execute("""
        SELECT title, source, sentiment, symbols, published_datetime, url
        FROM news_data
        WHERE published_datetime >= NOW() - INTERVAL 24 HOUR
        ORDER BY published_datetime DESC
        LIMIT 20
    """)
    result = []
    for r in cursor.fetchall():
        result.append({
            'title':        r['title'],
            'source':       r['source'],
            'sentiment':    r['sentiment'],
            'symbols':      r['symbols'],
            'published_at': r['published_datetime'].strftime('%Y-%m-%d %H:%M UTC') if r['published_datetime'] else '',
            'url':          r['url'],
        })
    return result


def _fetch_hyperliquid(cursor):
    # Aggregated stats
    cursor.execute("""
        SELECT
            COALESCE(SUM(total_trades), 0) AS total_count,
            COALESCE(SUM(long_trades),  0) AS long_count,
            COALESCE(SUM(short_trades), 0) AS short_count,
            COALESCE(SUM(net_flow),     0) AS net_flow_usd,
            COUNT(DISTINCT symbol)         AS unique_coins,
            MAX(updated_at)                AS last_updated
        FROM hyperliquid_symbol_aggregation
        WHERE period = '24h'
    """)
    agg = cursor.fetchone() or {}
    long_count  = int(agg.get('long_count')  or 0)
    short_count = int(agg.get('short_count') or 0)
    ls_ratio = round(long_count / short_count, 2) if short_count > 0 else 0

    # Unique wallets (uses idx_trade_time index)
    cursor.execute("""
        SELECT COUNT(DISTINCT address) AS unique_wallets
        FROM hyperliquid_wallet_trades
        WHERE trade_time >= NOW() - INTERVAL 24 HOUR
    """)
    wrow = cursor.fetchone() or {}

    # Recent large trades (uses idx_trade_time + idx_notional indexes)
    cursor.execute("""
        SELECT coin, side, price, size, notional_usd, closed_pnl, trade_time
        FROM hyperliquid_wallet_trades
        WHERE trade_time >= NOW() - INTERVAL 24 HOUR
          AND notional_usd >= 100000
        ORDER BY notional_usd DESC
        LIMIT 30
    """)
    trades = []
    for t in cursor.fetchall():
        trades.append({
            'coin':        t['coin'],
            'action':      t['side'],
            'side':        t['side'],
            'price':       float(t['price']),
            'size':        float(t['size']),
            'notional_usd': float(t['notional_usd']),
            'closed_pnl':  float(t['closed_pnl']),
            'timestamp':   t['trade_time'].isoformat() if t['trade_time'] else None,
        })

    return {
        'statistics': {
            'total_count':      int(agg.get('total_count') or 0),
            'long_count':       long_count,
            'short_count':      short_count,
            'net_flow_usd':     float(agg.get('net_flow_usd') or 0),
            'unique_wallets':   int(wrow.get('unique_wallets') or 0),
            'unique_coins':     int(agg.get('unique_coins') or 0),
            'long_short_ratio': ls_ratio,
        },
        'trades': trades,
    }


def update_dashboard_snapshot():
    """
    计算所有 Dashboard 数据并写入 dashboard_snapshot 表。
    调度器每5分钟调用一次，写入耗时通常 <500ms。
    """
    t0 = time.time()
    conn = None
    try:
        conn = _get_conn()
        cursor = conn.cursor()
        _ensure_table(cursor)
        conn.commit()

        signals         = _fetch_signals(cursor)
        stats           = _fetch_stats(cursor)
        futures         = _fetch_futures(cursor)
        news            = _fetch_news(cursor)
        hyperliquid     = _fetch_hyperliquid(cursor)
        winrate_history = _fetch_winrate_history(cursor)

        snapshot = {
            'signals':         signals,
            'stats':           stats,
            'futures':         futures,
            'news':            news,
            'hyperliquid':     hyperliquid,
            'winrate_history': winrate_history,
            'updated_at':      datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }

        snapshot_json = json.dumps(snapshot, ensure_ascii=False, default=str)
        compute_ms = int((time.time() - t0) * 1000)

        cursor.execute("""
            INSERT INTO dashboard_snapshot (snapshot_key, snapshot_json, updated_at, compute_ms)
            VALUES ('main', %s, NOW(), %s)
            ON DUPLICATE KEY UPDATE
                snapshot_json = VALUES(snapshot_json),
                updated_at    = VALUES(updated_at),
                compute_ms    = VALUES(compute_ms)
        """, (snapshot_json, compute_ms))
        conn.commit()
        cursor.close()
        logger.info(f"[dashboard_snapshot] updated in {compute_ms}ms, "
                    f"signals={len(signals)}, futures={len(futures)}, "
                    f"news={len(news)}, hl_trades={len(hyperliquid['trades'])}")
    except Exception as e:
        logger.error(f"[dashboard_snapshot] update failed: {e}")
        raise
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
