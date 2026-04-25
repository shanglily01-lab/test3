"""
庄家数据采集器 - 独立于主系统运行
采集资金费率、持仓量(OI)、多空比(L/S Ratio) 并存入 DB

运行方式:
    python whale_data_collector.py

采集频率:
    - 资金费率 + 24h价格统计: 每 5 分钟 (批量接口, 1 次请求覆盖全市场)
    - OI历史 + 多空比: 每 15 分钟 (逐品种, 仅采集活跃品种)
"""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import os, re, time, logging, requests, pymysql
from datetime import datetime, timezone
from dotenv import load_dotenv
load_dotenv()

# ── 日志 ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    datefmt='%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('whale_collector.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)

# ── 常量 ─────────────────────────────────────────────────────────────
FAPI = "https://fapi.binance.com"
FUNDING_INTERVAL  = 5  * 60   # 5 分钟 (premiumIndex + 24hr 都是全市场单次请求, 量小)
OI_LS_INTERVAL    = 30 * 60   # 30 分钟 (2026-04-25 砍半: 这是 IP 被封主因, 逐品种 ×N 次)
REQUEST_TIMEOUT   = 10
INTER_REQ_SLEEP   = 0.25       # 0.12 -> 0.25, 节奏拉慢避免集中突发触发 IP 级 -1003
TOP_N_BY_VOLUME   = 200        # 按 24h 成交额取前 N 个品种 (覆盖小币策略命中)

# Binance -1003 / 418：按 IP 的原始请求过多；解禁时间见响应 msg「banned until <ms>」
_BINANCE_BAN_UNTIL_MS: int = 0
_BAN_HIT_THIS_CYCLE: bool = False


def _binance_ban_remaining_s() -> float:
    """距离 Binance 响应中解禁时间还有多少秒；未在封禁窗口则返回 0。"""
    if not _BINANCE_BAN_UNTIL_MS:
        return 0.0
    now_ms = int(time.time() * 1000)
    if now_ms >= _BINANCE_BAN_UNTIL_MS:
        return 0.0
    return max(0.0, (_BINANCE_BAN_UNTIL_MS - now_ms) / 1000.0)


def _record_binance_ban_from_body(status: int, body: str) -> None:
    global _BINANCE_BAN_UNTIL_MS, _BAN_HIT_THIS_CYCLE
    if status != 418 and "-1003" not in body and "Way too many requests" not in body:
        return
    _BAN_HIT_THIS_CYCLE = True
    m = re.search(r"banned until (\d+)", body)
    if m:
        _BINANCE_BAN_UNTIL_MS = int(m.group(1))
        until_utc = datetime.fromtimestamp(_BINANCE_BAN_UNTIL_MS / 1000, tz=timezone.utc)
        log.error(
            "Binance IP 限速/封禁 (-1003)，请勿在封禁期内继续请求 REST；"
            "解禁时间(UTC): %s（约 %.0f 分钟后）。"
            "建议：主程序用 WebSocket 做行情、拉长全市场 REST 轮询间隔、勿多进程重复打同一 IP。",
            until_utc.strftime("%Y-%m-%d %H:%M:%S"),
            _binance_ban_remaining_s() / 60.0,
        )
    else:
        _BINANCE_BAN_UNTIL_MS = int((time.time() + 3600) * 1000)
        log.error("Binance 限速 (-1003)，未解析到解禁时间，按 1 小时 backoff")


# ── DB ───────────────────────────────────────────────────────────────
def get_db():
    return pymysql.connect(
        host=os.getenv('DB_HOST'), port=int(os.getenv('DB_PORT', 3306)),
        user=os.getenv('DB_USER'), password=os.getenv('DB_PASSWORD', ''),
        db=os.getenv('DB_NAME'), charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )

# ── 工具 ─────────────────────────────────────────────────────────────
def _get(url: str, params: dict = None) -> list | dict | None:
    global _BINANCE_BAN_UNTIL_MS
    rem = _binance_ban_remaining_s()
    if rem > 0:
        log.warning("仍在 Binance 封禁窗口内 (剩余约 %.0f 秒)，跳过: %s", rem, url)
        return None
    try:
        r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            _BINANCE_BAN_UNTIL_MS = 0
            return r.json()
        body = r.text[:500]
        log.warning("HTTP %d  %s  %s", r.status_code, url, body)
        _record_binance_ban_from_body(r.status_code, body)
    except Exception as e:
        log.warning("请求失败: %s  %s", url, e)
    return None

def _binance_sym(sym: str) -> str:
    """BTC/USDT -> BTCUSDT"""
    return sym.replace('/', '')

def _std_sym(binance_sym: str) -> str:
    """BTCUSDT -> BTC/USDT  (假定均以 USDT 结尾)"""
    if binance_sym.endswith('USDT'):
        base = binance_sym[:-4]
        return f"{base}/USDT"
    return binance_sym

# ── 1. 批量采集资金费率 ───────────────────────────────────────────────
def collect_funding_rates(conn):
    """
    使用 /fapi/v1/premiumIndex (无 symbol 参数) 一次拉全市场数据.
    只保存 USDT 永续合约.
    """
    data = _get(f"{FAPI}/fapi/v1/premiumIndex")
    if not data:
        if _binance_ban_remaining_s() > 0:
            log.error("资金费率批量接口无返回（当前为 Binance IP 封禁/限速，见上文 -1003）")
        else:
            log.error("资金费率批量接口无返回")
        return 0

    rows = []
    for d in data:
        sym = d.get('symbol', '')
        if not sym.endswith('USDT'):
            continue
        funding_time = int(d.get('time', 0))
        if funding_time == 0:
            continue
        rows.append((
            _std_sym(sym),
            'binance',
            float(d.get('lastFundingRate', 0)),
            funding_time,
            datetime.fromtimestamp(funding_time / 1000),
            float(d.get('markPrice', 0)),
            float(d.get('indexPrice', 0)),
            int(d.get('nextFundingTime', 0)),
        ))

    if not rows:
        return 0

    cur = conn.cursor()
    sql = """
        INSERT INTO funding_rate_data
            (symbol, exchange, funding_rate, funding_time, timestamp,
             mark_price, index_price, next_funding_time)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
            funding_rate = VALUES(funding_rate),
            timestamp    = VALUES(timestamp),
            mark_price   = VALUES(mark_price),
            index_price  = VALUES(index_price),
            next_funding_time = VALUES(next_funding_time)
    """
    # funding_rate_data 没有 unique key, 先检查再插 (防重)
    # 用 funding_time + symbol 去重
    cur.execute("SELECT symbol, MAX(funding_time) as mt FROM funding_rate_data GROUP BY symbol")
    existing = {r['symbol']: r['mt'] for r in cur.fetchall()}

    insert_rows = [r for r in rows if existing.get(r[0], 0) < r[3]]
    if insert_rows:
        cur.executemany("""
            INSERT INTO funding_rate_data
                (symbol, exchange, funding_rate, funding_time, timestamp,
                 mark_price, index_price, next_funding_time)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, insert_rows)
        conn.commit()
    log.info("资金费率更新: %d 个品种 (新增 %d 条)", len(rows), len(insert_rows))
    return len(insert_rows)

# ── 2. 批量采集 24h 价格统计 ─────────────────────────────────────────
def collect_24h_stats(conn) -> list:
    """
    更新 price_stats_24h 并返回按 quoteVolume 排序的前 TOP_N 品种列表.
    """
    data = _get(f"{FAPI}/fapi/v1/ticker/24hr")
    if not data:
        return []

    # 仅保留 USDT 永续
    usdt = [d for d in data if d.get('symbol', '').endswith('USDT')]
    usdt.sort(key=lambda x: float(x.get('quoteVolume', 0)), reverse=True)

    rows = []
    for d in usdt:
        sym  = _std_sym(d['symbol'])
        curr = float(d.get('lastPrice', 0))
        p24  = float(d.get('openPrice', 0))
        chg  = float(d.get('priceChangePercent', 0))
        rows.append((
            sym,
            curr,
            p24,
            chg,
            abs(curr - p24),
            float(d.get('highPrice', 0)),
            float(d.get('lowPrice', 0)),
            min(float(d.get('volume', 0)), 9.99e11),       # 防超出 decimal(20,8)
            min(float(d.get('quoteVolume', 0)), 9.99e15),
            int(d.get('count', 0)),
            datetime.now(),
        ))

    if rows:
        cur = conn.cursor()
        cur.executemany("""
            INSERT INTO price_stats_24h
                (symbol, current_price, price_24h_ago,
                 change_24h, change_24h_abs, high_24h, low_24h,
                 volume_24h, quote_volume_24h, trades_count_24h, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
                current_price   = VALUES(current_price),
                change_24h      = VALUES(change_24h),
                change_24h_abs  = VALUES(change_24h_abs),
                high_24h        = VALUES(high_24h),
                low_24h         = VALUES(low_24h),
                volume_24h      = VALUES(volume_24h),
                quote_volume_24h= VALUES(quote_volume_24h),
                trades_count_24h= VALUES(trades_count_24h),
                updated_at      = VALUES(updated_at)
        """, rows)
        conn.commit()
        log.info("24h统计更新: %d 个品种", len(rows))

    return [_std_sym(d['symbol']) for d in usdt[:TOP_N_BY_VOLUME]]

# ── 3. 逐品种采集 OI 历史 (1h, 近12根) ──────────────────────────────
def collect_oi_history(conn, symbols: list):
    """
    从 /futures/data/openInterestHist 拉取 1h OI, 存入 futures_open_interest.
    为避免重复, 仅插入比 DB 中最新 timestamp 更新的记录.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT symbol, MAX(timestamp) as mt FROM futures_open_interest
        WHERE exchange = 'binance' GROUP BY symbol
    """)
    latest = {r['symbol']: r['mt'] for r in cur.fetchall()}

    inserted_total = 0
    for sym in symbols:
        bsym = _binance_sym(sym)
        data = _get(f"{FAPI}/futures/data/openInterestHist",
                    {'symbol': bsym, 'period': '1h', 'limit': 12})
        time.sleep(INTER_REQ_SLEEP)
        if not data or not isinstance(data, list):
            continue

        last_dt = latest.get(sym)
        rows = []
        for d in data:
            ts = datetime.fromtimestamp(int(d['timestamp']) / 1000)
            if last_dt and ts <= last_dt:
                continue
            oi_val = float(d.get('sumOpenInterestValue', 0))
            oi_qty = float(d.get('sumOpenInterest', 0))
            rows.append((sym, 'binance', oi_qty, oi_val, ts))

        if rows:
            cur.executemany("""
                INSERT INTO futures_open_interest
                    (symbol, exchange, open_interest, open_interest_value, timestamp)
                VALUES (%s,%s,%s,%s,%s)
            """, rows)
            inserted_total += len(rows)

    conn.commit()
    log.info("OI历史更新: %d 条 (覆盖 %d 个品种)", inserted_total, len(symbols))

# ── 4. 逐品种采集多空比 (1h, 近6根) ──────────────────────────────────
def collect_ls_ratio(conn, symbols: list):
    """
    从 /futures/data/globalLongShortAccountRatio 拉取, 存入 futures_long_short_ratio.
    并非所有品种都有此数据 (小币种没有), 失败则跳过.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT symbol, MAX(timestamp) as mt FROM futures_long_short_ratio
        WHERE exchange = 'binance' GROUP BY symbol
    """)
    latest = {r['symbol']: r['mt'] for r in cur.fetchall()}

    inserted_total = 0
    for sym in symbols:
        bsym = _binance_sym(sym)
        data = _get(f"{FAPI}/futures/data/globalLongShortAccountRatio",
                    {'symbol': bsym, 'period': '1h', 'limit': 6})
        time.sleep(INTER_REQ_SLEEP)
        if not data or not isinstance(data, list) or not data:
            continue

        last_dt = latest.get(sym)
        rows = []
        for d in data:
            ts = datetime.fromtimestamp(int(d['timestamp']) / 1000)
            if last_dt and ts <= last_dt:
                continue
            rows.append((
                sym, 'binance', '1h',
                float(d.get('longAccount',  0)),
                float(d.get('shortAccount', 0)),
                0.0, 0.0,  # position data not in this endpoint
                float(d.get('longShortRatio', 0)),
                float(d.get('longShortRatio', 0)),
                ts,
            ))

        if rows:
            cur.executemany("""
                INSERT INTO futures_long_short_ratio
                    (symbol, exchange, period,
                     long_account, short_account,
                     long_position, short_position,
                     long_short_position_ratio, long_short_ratio,
                     timestamp)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, rows)
            inserted_total += len(rows)

    conn.commit()
    log.info("多空比更新: %d 条 (覆盖 %d 个品种)", inserted_total, len(symbols))

# ── 5. 清理旧数据 (防止 DB 膨胀) ─────────────────────────────────────
def cleanup_old_data(conn):
    cur = conn.cursor()
    # 保留最近 7 天 OI 和 L/S 数据
    cur.execute("DELETE FROM futures_open_interest   WHERE timestamp < NOW() - INTERVAL 7 DAY")
    cur.execute("DELETE FROM futures_long_short_ratio WHERE timestamp < NOW() - INTERVAL 7 DAY")
    # 资金费率: 保留最近 30 天
    cur.execute("DELETE FROM funding_rate_data WHERE timestamp < NOW() - INTERVAL 30 DAY")
    conn.commit()
    log.info("旧数据清理完成")

# ── 获取采集品种列表 ──────────────────────────────────────────────────
def get_target_symbols(top200: list) -> list:
    """
    合并策略: Binance前200 (by quoteVolume).
    top200 来自 collect_24h_stats() 的返回值.
    """
    return list(dict.fromkeys(top200))  # 去重保序

# ── 主循环 ───────────────────────────────────────────────────────────
def main():
    log.info("=" * 56)
    log.info("Whale Data Collector 启动")
    log.info("资金费率 / 24h统计: 每 %d 分钟", FUNDING_INTERVAL // 60)
    log.info("OI历史 / 多空比:     每 %d 分钟", OI_LS_INTERVAL  // 60)
    log.info("=" * 56)

    last_funding = 0.0
    last_oi_ls   = 0.0
    target_syms  = []
    cleanup_counter = 0

    while True:
        now = time.time()

        try:
            conn = get_db()

            # -- 资金费率 + 24h 统计 (每 5 分钟) --
            if now - last_funding >= FUNDING_INTERVAL:
                global _BAN_HIT_THIS_CYCLE
                _BAN_HIT_THIS_CYCLE = False
                top200 = collect_24h_stats(conn)
                # 若 24hr 已触发 -1003，不再打 premiumIndex，避免无效请求
                if not _BAN_HIT_THIS_CYCLE:
                    collect_funding_rates(conn)
                else:
                    log.warning("本周期已判定 Binance 封禁/限速，跳过资金费率请求")
                target_syms = get_target_symbols(top200)
                # 封禁导致的空周期不推进时钟，解禁后尽快补采
                if not (not top200 and _BAN_HIT_THIS_CYCLE):
                    last_funding = now

            # -- OI 历史 + 多空比 (每 15 分钟) --
            if now - last_oi_ls >= OI_LS_INTERVAL and target_syms:
                collect_oi_history(conn, target_syms)
                collect_ls_ratio(conn, target_syms)
                last_oi_ls = now

                cleanup_counter += 1
                if cleanup_counter % 4 == 0:  # 每小时清理一次
                    cleanup_old_data(conn)

            conn.close()

        except Exception as e:
            log.error("采集循环异常: %s", e, exc_info=True)

        # 封禁期内拉长休眠，减少无意义轮询
        ban_rem = _binance_ban_remaining_s()
        if ban_rem > 0:
            sleep_s = min(max(60.0, ban_rem + 5.0), 600.0)
            log.info("封禁窗口内，本轮休眠 %.0f 秒后再检查", sleep_s)
            time.sleep(sleep_s)
        else:
            time.sleep(60)

if __name__ == '__main__':
    main()
