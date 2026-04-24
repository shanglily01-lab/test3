"""
拉取币安 U 本位合约实盘历史成交, 写入 dimesion.binance_trades_raw.

数据源: /fapi/v1/userTrades (单次最多 1000 条, 按 symbol + 时间窗分页)
写入: binance_trades_raw 表 (首次运行自动创建, 之后 INSERT IGNORE 幂等)

用法:
  # 拉今天所有 SYNCED 到实盘的 symbol 今天的成交
  python scripts/diag/fetch_binance_history.py today

  # 拉指定日期范围 (YYYY-MM-DD, 按本地时区 UTC+8)
  python scripts/diag/fetch_binance_history.py 2026-04-24 2026-04-24

  # 拉最近 N 天所有 SYNCED 过的 symbol
  python scripts/diag/fetch_binance_history.py --days 7

  # 拉指定 symbol 的最近 N 天
  python scripts/diag/fetch_binance_history.py --symbol BTC/USDT --days 7

只读 DB 里现有表, 只写入 binance_trades_raw 这张新表.
"""
import argparse
import hashlib
import hmac
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

import pymysql
import requests

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# ----------------- 配置 -----------------
ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = ROOT / '.env'

DB_CFG = dict(
    host='13.212.252.171', port=3306, user='admin', password='Yintao@110',
    db='dimesion', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor,
    autocommit=True,
)

BASE_URL = "https://fapi.binance.com"

# ----------------- 建表 -----------------
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS binance_trades_raw (
  trade_id          BIGINT PRIMARY KEY COMMENT '币安 trade id',
  order_id          BIGINT NOT NULL COMMENT '币安订单 id',
  symbol            VARCHAR(30) NOT NULL COMMENT '交易对 (内部格式 XXX/USDT)',
  binance_symbol    VARCHAR(30) NOT NULL COMMENT '币安原始格式 XXXUSDT',
  side              VARCHAR(10) NOT NULL COMMENT 'BUY / SELL',
  position_side     VARCHAR(10) DEFAULT NULL COMMENT 'LONG / SHORT / BOTH',
  price             DECIMAL(24, 10) NOT NULL COMMENT '成交价',
  qty               DECIMAL(24, 10) NOT NULL COMMENT '成交数量 (币)',
  quote_qty         DECIMAL(24, 10) DEFAULT NULL COMMENT '成交金额 (USDT)',
  commission        DECIMAL(24, 10) DEFAULT NULL COMMENT '手续费',
  commission_asset  VARCHAR(10) DEFAULT NULL COMMENT '手续费资产 (通常 USDT)',
  realized_pnl      DECIMAL(24, 10) DEFAULT NULL COMMENT '已实现盈亏',
  trade_time        DATETIME NOT NULL COMMENT '成交时间 (UTC)',
  trade_time_ms     BIGINT NOT NULL COMMENT '成交时间戳 (ms)',
  is_buyer          TINYINT(1) DEFAULT NULL COMMENT '是否买方',
  is_maker          TINYINT(1) DEFAULT NULL COMMENT '是否 maker',
  margin_asset      VARCHAR(10) DEFAULT NULL COMMENT '保证金资产',
  fetched_at        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '拉取时间',
  KEY idx_symbol_time (symbol, trade_time),
  KEY idx_order_id (order_id),
  KEY idx_trade_time (trade_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci
  COMMENT='币安实盘合约成交原始数据 (从 /fapi/v1/userTrades 拉取)'
"""

INSERT_SQL = """
INSERT IGNORE INTO binance_trades_raw
  (trade_id, order_id, symbol, binance_symbol, side, position_side,
   price, qty, quote_qty, commission, commission_asset,
   realized_pnl, trade_time, trade_time_ms,
   is_buyer, is_maker, margin_asset)
VALUES
  (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)


# ----------------- API -----------------
def load_api_credentials() -> tuple[str, str]:
    key = os.environ.get('BINANCE_API_KEY', '')
    secret = os.environ.get('BINANCE_API_SECRET', '')
    if key and secret:
        return key, secret
    if not ENV_FILE.exists():
        raise RuntimeError(f"未找到 .env: {ENV_FILE}")
    for line in ENV_FILE.read_text(encoding='utf-8').splitlines():
        line = line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        k, v = line.split('=', 1)
        k = k.strip(); v = v.strip().strip('"').strip("'")
        if k == 'BINANCE_API_KEY':
            key = v
        elif k == 'BINANCE_API_SECRET':
            secret = v
    if not (key and secret):
        raise RuntimeError("BINANCE_API_KEY/SECRET 未配置")
    return key, secret


def sign(params: Dict[str, Any], secret: str) -> str:
    qs = '&'.join(f"{k}={v}" for k, v in params.items())
    return hmac.new(secret.encode(), qs.encode(), hashlib.sha256).hexdigest()


def api_get(endpoint: str, params: Dict[str, Any], key: str, secret: str) -> Any:
    params = dict(params)
    params['timestamp'] = int(time.time() * 1000)
    params['recvWindow'] = 10000
    params['signature'] = sign(params, secret)
    r = requests.get(BASE_URL + endpoint, params=params,
                     headers={'X-MBX-APIKEY': key}, timeout=15)
    if r.status_code != 200:
        print(f"  [HTTP {r.status_code}] {endpoint} {params.get('symbol', '')}: {r.text[:200]}")
        return None
    return r.json()


def fetch_user_trades(symbol: str, start_ms: int, end_ms: int,
                       key: str, secret: str) -> List[Dict]:
    all_trades: List[Dict] = []
    cur_start = start_ms
    page = 0
    MAX_PAGE = 20
    while cur_start < end_ms and page < MAX_PAGE:
        page += 1
        params = {
            'symbol': symbol.replace('/', ''),
            'startTime': cur_start,
            'endTime': end_ms,
            'limit': 1000,
        }
        result = api_get('/fapi/v1/userTrades', params, key, secret)
        if not isinstance(result, list):
            # 若是 dict(错误) 或 None, 停止这个 symbol
            return all_trades
        if not result:
            break
        all_trades.extend(result)
        if len(result) < 1000:
            break
        last_time = result[-1]['time']
        new_start = last_time + 1
        if new_start <= cur_start:
            break
        cur_start = new_start
        time.sleep(0.2)
    return all_trades


def fetch_symbols_from_db(days_back: int, conn) -> List[str]:
    start = (date.today() - timedelta(days=days_back - 1)).isoformat()
    with conn.cursor() as cur:
        cur.execute(
            """SELECT DISTINCT symbol FROM futures_orders
               WHERE live_sync_status='SYNCED' AND DATE(created_at) >= %s""",
            (start,),
        )
        return [r['symbol'] for r in cur.fetchall()]


# ----------------- 入库 -----------------
def save_trades(conn, symbol: str, trades: List[Dict]) -> int:
    """写入 DB, 返回实际新增行数 (被 IGNORE 的重复行不计)."""
    if not trades:
        return 0
    rows = []
    for t in trades:
        trade_time_ms = int(t['time'])
        dt = datetime.fromtimestamp(trade_time_ms / 1000, tz=timezone.utc)
        rows.append((
            int(t['id']),
            int(t['orderId']),
            symbol,
            str(t['symbol']),
            str(t['side']),
            t.get('positionSide'),
            t['price'],
            t['qty'],
            t.get('quoteQty'),
            t.get('commission'),
            t.get('commissionAsset'),
            t.get('realizedPnl'),
            dt.replace(tzinfo=None),
            trade_time_ms,
            int(bool(t.get('buyer', False))),
            int(bool(t.get('maker', False))),
            t.get('marginAsset'),
        ))
    with conn.cursor() as cur:
        cur.executemany(INSERT_SQL, rows)
        return cur.rowcount


def summarize_from_db(conn, start_ms: int, end_ms: int):
    start_dt = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).replace(tzinfo=None)
    end_dt = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).replace(tzinfo=None)
    with conn.cursor() as cur:
        cur.execute(
            """SELECT symbol,
                      COUNT(*) AS n,
                      SUM(CASE WHEN side='BUY' THEN 1 ELSE 0 END) AS buys,
                      SUM(CASE WHEN side='SELL' THEN 1 ELSE 0 END) AS sells,
                      SUM(COALESCE(realized_pnl, 0)) AS realized,
                      SUM(CASE WHEN commission_asset='USDT'
                               THEN COALESCE(commission,0) ELSE 0 END) AS comm
               FROM binance_trades_raw
               WHERE trade_time >= %s AND trade_time < %s
               GROUP BY symbol
               ORDER BY realized ASC""",
            (start_dt, end_dt),
        )
        rows = cur.fetchall()
        total_n = 0; total_realized = 0.0; total_comm = 0.0
        print(f"\n### 区间内成交摘要 ({start_dt} ~ {end_dt} UTC) ###\n")
        print(f"  {'symbol':<16}{'n':>4}{'buy':>5}{'sell':>5}"
              f"{'realized':>12}{'comm':>10}{'净':>12}")
        for r in rows:
            rp = float(r['realized'] or 0); cm = float(r['comm'] or 0)
            total_n += int(r['n']); total_realized += rp; total_comm += cm
            print(f"  {r['symbol']:<16}{r['n']:>4}{r['buys']:>5}{r['sells']:>5}"
                  f"{rp:>+12.4f}{cm:>10.4f}{rp-cm:>+12.4f}")
        print(f"\n## 合计: {len(rows)} symbols / {total_n} 成交 / "
              f"realized={total_realized:+.4f}  commission={total_comm:.4f}  "
              f"净={total_realized - total_comm:+.4f} ##\n")


# ----------------- 主流程 -----------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('args', nargs='*', help='today / YYYY-MM-DD [YYYY-MM-DD]')
    ap.add_argument('--days', type=int, default=None, help='最近 N 天')
    ap.add_argument('--symbol', type=str, default=None, help='单个 symbol (BTC/USDT)')
    opts = ap.parse_args()

    now = datetime.now(timezone.utc)
    if opts.days:
        start_dt = now - timedelta(days=opts.days)
        end_dt = now
    elif len(opts.args) == 1 and opts.args[0] == 'today':
        today_local = date.today()
        start_dt = datetime(today_local.year, today_local.month, today_local.day,
                             tzinfo=timezone.utc) - timedelta(hours=8)
        end_dt = start_dt + timedelta(days=1)
    elif len(opts.args) >= 1:
        start_d = date.fromisoformat(opts.args[0])
        end_d = date.fromisoformat(opts.args[1]) if len(opts.args) >= 2 else start_d
        start_dt = datetime(start_d.year, start_d.month, start_d.day,
                             tzinfo=timezone.utc) - timedelta(hours=8)
        end_dt = datetime(end_d.year, end_d.month, end_d.day,
                           tzinfo=timezone.utc) - timedelta(hours=8) + timedelta(days=1)
    else:
        ap.print_help()
        sys.exit(1)

    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)
    print(f"\n### 拉取币安实盘成交  {start_dt} ~ {end_dt} (UTC) ###\n")

    try:
        key, secret = load_api_credentials()
        print(f"API Key: ***{key[-6:]}")
    except Exception as e:
        print(f"[致命] 加载 API 凭证失败: {e}")
        sys.exit(2)

    conn = pymysql.connect(**DB_CFG)
    try:
        ensure_table(conn)
        print(f"表 binance_trades_raw 已就绪\n")

        if opts.symbol:
            symbols = [opts.symbol]
        else:
            days_range = max(1, (end_dt - start_dt).days + 1)
            symbols = fetch_symbols_from_db(days_range, conn)
            print(f"从 DB 取到最近 {days_range} 天 SYNCED symbols 共 {len(symbols)} 个\n")

        if not symbols:
            print("无 symbol 可拉")
            sys.exit(0)

        inserted_total = 0
        fetched_total = 0
        for i, sym in enumerate(symbols, 1):
            print(f"[{i}/{len(symbols)}] {sym}", end=' ... ', flush=True)
            try:
                trades = fetch_user_trades(sym, start_ms, end_ms, key, secret)
            except Exception as e:
                print(f"异常: {e}")
                continue
            fetched_total += len(trades)
            if trades:
                try:
                    inserted = save_trades(conn, sym, trades)
                    inserted_total += inserted
                    dup = len(trades) - inserted
                    print(f"拉到 {len(trades)} 条, 新增 {inserted} "
                          f"{'(已有 ' + str(dup) + ' 条跳过)' if dup else ''}")
                except Exception as e:
                    print(f"入库失败: {e}")
            else:
                print("无")
            time.sleep(0.1)

        print(f"\n## 总拉取 {fetched_total} 条, 新增入库 {inserted_total} 条 ##")
        summarize_from_db(conn, start_ms, end_ms)
    finally:
        conn.close()


if __name__ == '__main__':
    main()
