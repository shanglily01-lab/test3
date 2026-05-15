"""
strategy_live - 实盘/paper 策略运行器, 真实下单到 localhost:9021
2026-05-15 极简化重构: 仅保留 topshort 子策略 (顶部反转做空).
  - 信号: 1H K 线, 48h 涨 >= 80% + N 根无新高 -> 限价开 SHORT
  - 主循环每 60s 调一次, topshort_tick 每 5 轮调一次
  - SL/TP/trail/early-sl/breakeven 通用风控保留
  - account_id=2, source 前缀 strategy_live:*
"""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
import time, os, datetime, logging
import pymysql, requests as req
from dotenv import load_dotenv
load_dotenv()

from strategy_state_db import (
    ensure_table,
    get_or_create,
    update_state,
    list_active,
    list_all_stype,
    ensure_cooldown_anchor_epoch,
)

# ── 配置 ─────────────────────────────────────────────────────────
API_BASE    = "http://localhost:9021"
ACCOUNT_ID  = 2
LEVERAGE    = 5
MARGIN      = 500.0   # 每笔保证金 (USDT)

# 品种黑名单（BASE 硬编码 + DB 动态：symbol_blacklist 表每 5 分钟刷新，合并生效）
SYMBOL_BLACKLIST_BASE = {'DENT/USDT', 'XAN/USDT', 'SUPER/USDT', 'GUN/USDT', 'UAI/USDT', 'AAVE/USD', 'BTC/USD', 'XVG/USDT', 'TRU/USDT', 'DEGO/USDT', 'ZRO/USDT', 'RIVER/USDT', 'Q/USDT', 'CHIP/USDT', 'SPK/USDT', 'UB/USDT'}
_db_blacklist_cache = {'syms': set(), 'ts': 0.0}
_DB_BLACKLIST_REFRESH_S = 300.0  # 5 分钟刷新一次

def _refresh_db_blacklist() -> set:
    """每 5 分钟从 symbol_blacklist 表读 is_active=1 的记录"""
    import time as _t
    now = _t.time()
    if (now - _db_blacklist_cache['ts']) < _DB_BLACKLIST_REFRESH_S:
        return _db_blacklist_cache['syms']
    try:
        conn2 = pymysql.connect(
            host=os.getenv("DB_HOST","localhost"), port=int(os.getenv("DB_PORT","3306")),
            user=os.getenv("DB_USER",""), password=os.getenv("DB_PASSWORD",""),
            database=os.getenv("DB_NAME",""), charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor, connect_timeout=3,
        )
        try:
            with conn2.cursor() as c2:
                c2.execute("SELECT symbol FROM symbol_blacklist WHERE is_active=1")
                _db_blacklist_cache['syms'] = {r['symbol'] for r in c2.fetchall()}
        finally:
            conn2.close()
        _db_blacklist_cache['ts'] = now
    except Exception as e:
        log.debug("读 symbol_blacklist 失败(使用旧缓存): %s", e)
    return _db_blacklist_cache['syms']

def get_effective_blacklist() -> set:
    """合并 BASE + DB（供品种池筛选用）"""
    return SYMBOL_BLACKLIST_BASE | _refresh_db_blacklist()

# 动态品种缓存
_sym_cache: dict = {'syms': [], 'updated_at': 0.0}
SYM_REFRESH_SECS = 15 * 60


def get_active_symbols(cur) -> list:
    """从 kline_data 动态获取过去30分钟有实时数据的所有品种."""
    now = time.time()
    if now - _sym_cache['updated_at'] < SYM_REFRESH_SECS and _sym_cache['syms']:
        return _sym_cache['syms']
    cur.execute("""
        SELECT DISTINCT symbol FROM kline_data
        WHERE timeframe = '5m'
          AND open_time >= UNIX_TIMESTAMP(NOW() - INTERVAL 30 MINUTE) * 1000
        ORDER BY symbol
    """)
    _bl = get_effective_blacklist()
    syms = [r['symbol'] for r in cur.fetchall()
            if r['symbol'] not in _bl]
    _sym_cache['syms'] = syms
    _sym_cache['updated_at'] = now
    log.info("品种列表刷新: %d 个活跃品种", len(syms))
    return syms

# 2026-05-15 极简化重构: 仅保留 topshort + 通用基础设施.
# 删除的子策略 (chase / dump / topshort-climax / bottomlong-climax) 的常量已一并删除.

# 入场位置守卫 (基于 3h 15m K 线区间百分位)
ENTRY_POS_LOOKBACK_BARS  = 12
ENTRY_POS_LONG_MAX       = 90.0
ENTRY_POS_SHORT_MIN      = 10.0

LONG_HOLD_MIN   = 6 * 60
SHORT_HOLD_MIN  = 6 * 60
POST_CLOSE_COOLDOWN_S = 4 * 3600
SYMBOL_MAX_DAILY_SL = 2
RECENT_SL_COOLDOWN_MIN = 240

# 顶部做空参数
TOP_PUMP_THRESH = 0.80
TOP_NO_NEW_H    = 6
TOP_LOOKBACK_H  = 48
TOP_HOLD_H      = 6
TOP_SL_PCT      = 0.12
TOP_SIGNAL_AGE  = 6 * 3600
TOPSHORT_COOLDOWN = POST_CLOSE_COOLDOWN_S
TOPSHORT_MIN_HISTORY_DAYS = 12
TOPSHORT_MIN_HISTORY_MS = TOPSHORT_MIN_HISTORY_DAYS * 24 * 60 * 60 * 1000
TOP_MIN_24H_CHANGE_PCT = -15.0  # 24h 已跌过此阈值不再开空

# 移动止盈参数
HARD_TP_PCT       = 0.20  # 硬止盈: 盈利达到即平仓
# 动态移动止盈：按 peak 分档决定回落阈值，越赚让利润跑得越远
#   peak 3%-5%  → 回落 1% 触发（小赚紧盯）
#   peak 5%-10% → 回落 2% 触发（中赚适度松）
#   peak ≥ 10% → 回落 3% 触发（大赚让它跑）
#   peak < 3%  → 不启动 trail，靠 SL 兜底
TRAIL_TP_TIERS = [
    (0.10, 0.03),  # 大赚档
    (0.05, 0.02),  # 中赚档
    (0.03, 0.01),  # 小赚档
]
# 早期止损 / 保本止损
#   EARLY_SL_PCT: 价格反向 3% 即早期止损（比硬 SL 10% 提前）
#   BREAKEVEN_AFTER_PEAK_PCT: 峰值浮盈达到此值后进入"赚过钱"状态
#     2026-04-24 从 3% 降到 1.5%——数据显示大量单 peak 1-3% 没有保护，被 early-sl -3% 扫掉
#   BREAKEVEN_SL_PCT: 在"赚过钱"状态下，若回吐到此阈值（-0.5%）平仓保本
#   ENTRY_GRACE_MIN: 入场保护期。前 N 分钟内 early-sl / breakeven 不触发，仅硬 SL 兜底
#     2026-04-24 新增：数据显示 38% 的 early-sl 在 5m 内触发（入场瞬间均值回归），
#     给仓位 45 分钟"呼吸空间"避免被瞬时抖动扫出局（从 30m 上调）
EARLY_SL_PCT             = 0.03
BREAKEVEN_AFTER_PEAK_PCT = 0.015
BREAKEVEN_SL_PCT         = -0.005
ENTRY_GRACE_MIN          = 45


def _dynamic_trail_pullback(peak_pct: float) -> float:
    """返回当前 peak 允许的最大回落; peak 不足最低档返回 inf (不触发 trail)"""
    for threshold, pullback in TRAIL_TP_TIERS:
        if peak_pct >= threshold:
            return pullback
    return float('inf')


# 从 system_settings 动态加载的参数 (运行时覆盖上方常量)
LIVE_SL_PCT           = 0.10
LIVE_HARD_TP_PCT      = HARD_TP_PCT
LIVE_LIMIT_OFFSET_PCT = 0.03
LIVE_HOLD_H           = 6
DISABLE_SL_TP_HOLD    = False
DISABLE_5M_CONFIRM    = False

# topshort 信号等待期 (默认 OFF, 30 min)
TOPSHORT_SIG_WAIT_ENABLED  = False
TOPSHORT_SIG_WAIT_MIN      = 30
TOPSHORT_SIG_ADVERSE_PCT   = 0.02


def _load_live_config() -> None:
    """从 system_settings 读取 topshort + 通用守卫参数. 主循环每 60s 调一次, 改 DB 后无需重启.
    2026-05-15 极简化: chase/dump/climax/bottomlong 相关 setting 已删除 (migration 035)."""
    global LIVE_SL_PCT, LIVE_HARD_TP_PCT, LIVE_LIMIT_OFFSET_PCT, LIVE_HOLD_H
    global TOP_SL_PCT, HARD_TP_PCT, LONG_HOLD_MIN, SHORT_HOLD_MIN, TOP_HOLD_H
    global DISABLE_SL_TP_HOLD, DISABLE_5M_CONFIRM
    global TOPSHORT_SIG_WAIT_ENABLED, TOPSHORT_SIG_WAIT_MIN, TOPSHORT_SIG_ADVERSE_PCT
    try:
        import pymysql as _pym
        conn = _pym.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "3306")),
            user=os.getenv("DB_USER", ""),
            password=os.getenv("DB_PASSWORD", ""),
            database=os.getenv("DB_NAME", ""),
            charset="utf8mb4",
            cursorclass=_pym.cursors.DictCursor,
            connect_timeout=5,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT setting_key, setting_value FROM system_settings "
                    "WHERE setting_key IN ('live_sl_pct','live_hard_tp_pct',"
                    "'live_limit_offset_pct','live_hold_hours','disable_sl_tp_hold',"
                    "'disable_5m_confirm',"
                    "'topshort_signal_wait_enabled','topshort_signal_wait_min','topshort_signal_adverse_pct')"
                )
                rows = {r['setting_key']: r['setting_value'] for r in cur.fetchall()}
        finally:
            conn.close()
        LIVE_SL_PCT           = float(rows.get('live_sl_pct',           LIVE_SL_PCT))
        LIVE_HARD_TP_PCT      = float(rows.get('live_hard_tp_pct',      LIVE_HARD_TP_PCT))
        LIVE_LIMIT_OFFSET_PCT = float(rows.get('live_limit_offset_pct', LIVE_LIMIT_OFFSET_PCT))
        LIVE_HOLD_H           = int(  rows.get('live_hold_hours',        LIVE_HOLD_H))
        DISABLE_SL_TP_HOLD = str(rows.get('disable_sl_tp_hold', '0')).strip().lower() in ('1','true','yes','on')
        DISABLE_5M_CONFIRM = str(rows.get('disable_5m_confirm', '0')).strip().lower() in ('1','true','yes','on')
        TOP_SL_PCT     = LIVE_SL_PCT
        HARD_TP_PCT    = LIVE_HARD_TP_PCT
        LONG_HOLD_MIN  = LIVE_HOLD_H * 60
        SHORT_HOLD_MIN = LIVE_HOLD_H * 60
        TOP_HOLD_H     = LIVE_HOLD_H
        log.info(
            "strategy_live 参数已加载: SL=%.0f%% TP=%.0f%% offset=%.1f%% hold=%dh disable_sl_tp_hold=%s disable_5m_confirm=%s",
            LIVE_SL_PCT * 100, LIVE_HARD_TP_PCT * 100, LIVE_LIMIT_OFFSET_PCT * 100, LIVE_HOLD_H,
            DISABLE_SL_TP_HOLD, DISABLE_5M_CONFIRM,
        )
        if DISABLE_SL_TP_HOLD:
            log.warning("!!! DISABLE_SL_TP_HOLD=ON: 新开仓将不设 SL/TP/timeout, 硬TP/移动TP检查跳过 !!!")
        if DISABLE_5M_CONFIRM:
            log.warning("!!! DISABLE_5M_CONFIRM=ON: 限价单触发即成交, 跳过 5m 阴/阳确认 !!!")

        TOPSHORT_SIG_WAIT_ENABLED = str(rows.get('topshort_signal_wait_enabled', '0')).strip().lower() in ('1','true','yes','on')
        TOPSHORT_SIG_WAIT_MIN = int(rows.get('topshort_signal_wait_min', TOPSHORT_SIG_WAIT_MIN))
        TOPSHORT_SIG_ADVERSE_PCT = float(rows.get('topshort_signal_adverse_pct', TOPSHORT_SIG_ADVERSE_PCT))
        log.info("topshort signal wait: enabled=%s wait=%dmin adverse=%.1f%%",
                 TOPSHORT_SIG_WAIT_ENABLED, TOPSHORT_SIG_WAIT_MIN, TOPSHORT_SIG_ADVERSE_PCT * 100)
    except Exception as exc:
        log.error("_load_live_config 失败, 使用默认值: %s", exc)


POLL_SECS       = 60
TOPSHORT_EVERY  = 5
# 各子策略 LIMIT 挂单在 futures_orders 中保持 PENDING 的最长时间，超时由 _fill_pending_orders 标为取消
LIMIT_PENDING_MAX_S = 3 * 60 * 60   # 2026-04-25 1h→2h; 04-26 2h→3h, 信号过短常常没等到

# 反向滑点熔断阈值：LIMIT 触发时若价格向不利方向偏离超过此幅度，撤单不填充
# LONG  cur_p < limit_p*(1-X) → 价格继续下跌，追多是逆势
# SHORT cur_p > limit_p*(1+X) → 价格继续上涨，做空是逆势
REVERSE_SLIPPAGE_LIMIT = 0.015

# 限价单触发后的观察确认期：价格触发挂单价后不立即成交，等 N 秒再看是否仍触发；
# 若仍触发才成交（过滤瞬穿），若已回撤则清除观察、继续挂单。
# 2026-04-24 新增：实测限价单在下跌/上涨途中被瞬穿成交，进场即接飞刀。
TRIGGER_CONFIRM_S = 30
_trigger_first_seen: dict[int, float] = {}

# ── 日志 ─────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    datefmt='%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('strategy_live.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)

# ── API 工具 ─────────────────────────────────────────────────────
def _api(method, path, **kwargs):
    r = req.request(method, f"{API_BASE}{path}", timeout=10, **kwargs)
    r.raise_for_status()
    return r.json()


def _log_order_event(conn, order_id: str, event_type: str,
                     cur_price=None, limit_price=None,
                     bar_open=None, bar_close=None, detail: str = ''):
    """LIMIT 中间事件入库 (order_trigger_events 表). 写失败不阻塞主流程.
    迁移: scripts/migrations/024_order_trigger_events.sql"""
    try:
        c = conn.cursor()
        c.execute("""
            INSERT INTO order_trigger_events
                (order_id, event_type, cur_price, limit_price,
                 bar_open_price, bar_close_price, detail)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (order_id, event_type, cur_price, limit_price,
              bar_open, bar_close, (detail[:255] if detail else None)))
        conn.commit()
        c.close()
    except Exception as e:
        log.warning("_log_order_event %s %s err: %s", event_type, order_id, e)

def get_price(sym):
    d = _api("GET", f"/api/futures/price/{sym}")
    return float(d["price"])

def _symbol_daily_sl_count(sym: str) -> int:
    """查询该标的今日（UTC）已止损平仓次数，用于日内熔断。"""
    try:
        conn = pymysql.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "3306")),
            user=os.getenv("DB_USER", ""),
            password=os.getenv("DB_PASSWORD", ""),
            database=os.getenv("DB_NAME", ""),
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=5,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) AS cnt FROM futures_positions "
                    "WHERE account_id=%s AND symbol=%s "
                    "  AND status='closed' "
                    "  AND close_time >= CURDATE() "
                    "  AND notes='stop_loss'",
                    (ACCOUNT_ID, sym),
                )
                row = cur.fetchone()
                return int(row["cnt"]) if row else 0
        finally:
            conn.close()
    except Exception as e:
        log.error("_symbol_daily_sl_count %s error: %s", sym, e)
        return 0


def _symbol_recent_sl_minutes(sym: str) -> float:
    """返回该标的最近一次止损距今分钟数；无记录或异常返回 9999.0"""
    try:
        conn = pymysql.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "3306")),
            user=os.getenv("DB_USER", ""),
            password=os.getenv("DB_PASSWORD", ""),
            database=os.getenv("DB_NAME", ""),
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=5,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT TIMESTAMPDIFF(SECOND, close_time, NOW()) AS secs "
                    "FROM futures_positions "
                    "WHERE account_id=%s AND symbol=%s "
                    "  AND status='closed' AND notes='stop_loss' "
                    "  AND close_time >= DATE_SUB(NOW(), INTERVAL %s MINUTE) "
                    "ORDER BY close_time DESC LIMIT 1",
                    (ACCOUNT_ID, sym, RECENT_SL_COOLDOWN_MIN),
                )
                row = cur.fetchone()
                if row and row["secs"] is not None:
                    return float(row["secs"]) / 60.0
                return 9999.0
        finally:
            conn.close()
    except Exception as e:
        log.error("_symbol_recent_sl_minutes %s error: %s", sym, e)
        return 9999.0


def _has_any_open(sym: str) -> bool:
    """检查 DB 里是否已有任意方向的 open 持仓或 PENDING 挂单。有则返回 True。"""
    try:
        conn = pymysql.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "3306")),
            user=os.getenv("DB_USER", ""),
            password=os.getenv("DB_PASSWORD", ""),
            database=os.getenv("DB_NAME", ""),
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=5,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM futures_positions "
                    "WHERE account_id=%s AND symbol=%s AND status='open' LIMIT 1",
                    (ACCOUNT_ID, sym),
                )
                if cur.fetchone():
                    return True
                cur.execute(
                    "SELECT id FROM futures_orders "
                    "WHERE account_id=%s AND symbol=%s AND status='PENDING' LIMIT 1",
                    (ACCOUNT_ID, sym),
                )
                return bool(cur.fetchone())
        finally:
            conn.close()
    except Exception as e:
        log.error("_has_any_open %s check error: %s", sym, e)
        return False


def open_order(sym, direction, entry_price, tp_pct, sl_pct, hold_min, tag, limit_price=None):
    """开仓. 返回 (position_id, order_id, is_pending)"""
    if _has_any_open(sym):
        log.info("跳过开%s %s: 已有持仓", direction, sym)
        return None, None, False
    recent_min = _symbol_recent_sl_minutes(sym)
    if recent_min < RECENT_SL_COOLDOWN_MIN:
        log.info("跳过开%s %s: 止损后%.0f分钟，冷却%d小时内不开新仓", direction, sym, recent_min, RECENT_SL_COOLDOWN_MIN // 60)
        return None, None, False
    daily_sl = _symbol_daily_sl_count(sym)
    if daily_sl >= SYMBOL_MAX_DAILY_SL:
        log.info("跳过开%s %s: 今日已止损 %d 次，暂停当日交易", direction, sym, daily_sl)
        return None, None, False
    price_ref = limit_price if (limit_price and limit_price > 0) else entry_price
    qty = round(MARGIN * LEVERAGE / price_ref, 6)
    if direction == "LONG":
        tp = round(price_ref * (1 + tp_pct), 6)
        sl = round(price_ref * (1 - sl_pct), 6)
    else:
        tp = round(price_ref * (1 - tp_pct), 6)
        sl = round(price_ref * (1 + sl_pct), 6)
    # 总开关 disable_sl_tp_hold 开启时: 裸奔,不写 SL/TP/timeout
    if DISABLE_SL_TP_HOLD:
        sl_out, tp_out, hold_out = None, None, 0
    else:
        sl_out, tp_out, hold_out = sl, tp, hold_min
    payload = {
        "account_id":        ACCOUNT_ID,
        "symbol":            sym,
        "position_side":     direction,
        "quantity":          qty,
        "leverage":          LEVERAGE,
        "stop_loss_price":   sl_out,
        "take_profit_price": tp_out,
        "max_hold_minutes":  hold_out,
        "source":            f"strategy_live:{tag}",
    }
    if limit_price and limit_price > 0:
        payload["limit_price"] = limit_price
    res  = _api("POST", "/api/futures/open", json=payload)
    data = res.get("data") or {}
    pid  = data.get("position_id") or data.get("id")
    oid  = data.get("order_id")
    is_pending = (data.get("status") == "PENDING") or (not pid and bool(oid))
    return pid, oid, is_pending

# ── 24H 最优限价辅助 ─────────────────────────────────────────────
def _get_24h_stats(cur, sym):
    cur.execute("""
        SELECT high_24h, low_24h FROM price_stats_24h
        WHERE symbol=%s ORDER BY updated_at DESC LIMIT 1
    """, (sym,))
    r = cur.fetchone()
    return (float(r['high_24h']), float(r['low_24h'])) if r else (None, None)


def _get_4h_stats(cur, sym):
    """取最近 4 小时 5m K 线 (48 根) 的 high/low 区间. 用于七上八下限价 (2026-04-25).
    数据不足时返回 (None, None), 限价回退默认 3% 偏移.
    """
    cur.execute("""
        SELECT MAX(high_price) AS h, MIN(low_price) AS l
        FROM kline_data
        WHERE symbol=%s AND timeframe='5m'
          AND open_time >= UNIX_TIMESTAMP(NOW() - INTERVAL 4 HOUR) * 1000
    """, (sym,))
    r = cur.fetchone()
    if not r or r.get('h') is None:
        return (None, None)
    return (float(r['h']), float(r['l']))


_topshort_hist_cache: dict[str, tuple[bool, float]] = {}
_TOPSHORT_HIST_TTL_SEC = 15 * 60


def _topshort_has_min_listed_history(cur, sym: str, now_ms: int) -> bool:
    """顶空新开仓：要求库内 1h K 线最早一根距今至少 TOPSHORT_MIN_HISTORY_DAYS 天。"""
    t = time.time()
    ent = _topshort_hist_cache.get(sym)
    if ent is not None and (t - ent[1]) < _TOPSHORT_HIST_TTL_SEC:
        return ent[0]
    cur.execute(
        """
        SELECT MIN(open_time) AS tmin FROM kline_data
        WHERE timeframe='1h' AND symbol=%s
        """,
        (sym,),
    )
    r = cur.fetchone() or {}
    tmin = r.get('tmin')
    if tmin is None:
        _topshort_hist_cache[sym] = (False, t)
        return False
    ok = (now_ms - int(tmin)) >= TOPSHORT_MIN_HISTORY_MS
    _topshort_hist_cache[sym] = (ok, t)
    return ok

def _close_overdue(conn):
    """关闭本账户所有超时持仓，每个主循环调一次。"""
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, symbol, position_side FROM futures_positions "
            "WHERE account_id=%s AND status='open' "
            "  AND timeout_at IS NOT NULL AND timeout_at <= NOW()",
            (ACCOUNT_ID,)
        )
        rows = cur.fetchall()
        cur.close()
    except Exception as e:
        log.error("_close_overdue 查询失败: %s", e)
        return
    for r in rows:
        try:
            # 用本地 WS 价格平仓，避免 paper engine 去调 Binance API 失败导致无法平仓
            close_price = None
            try:
                close_price = get_price(r['symbol'])
            except Exception as pe:
                log.warning("超时平仓取价失败 %s: %s，不传 close_price 让引擎自行获取", r['symbol'], pe)
            payload = {"reason": "timeout"}
            if close_price:
                payload["close_price"] = close_price
            resp = req.post(
                f"{API_BASE}/api/futures/close/{r['id']}",
                json=payload,
                timeout=10,
            )
            if resp.ok:
                log.info("超时平仓: %s %s pid=%d @ %.6f", r['symbol'], r['position_side'], r['id'], close_price or 0)
            else:
                log.warning("超时平仓失败 pid=%d: %s", r['id'], resp.text[:100])
        except Exception as e:
            log.error("超时平仓异常 pid=%d: %s", r['id'], e)


def _calc_limit_price(side, cur_price, high_24h, low_24h, pct=0.003,
                      high_4h=None, low_4h=None):
    """限价挂单 (2026-04-25 七上八下原则):
       SHORT: 优先 4h_high × 0.80; 若小于 cur×(1+pct), 用 cur×(1+pct). 受 24h_high 压制.
       LONG:  优先 4h_low  × 1.30; 若大于 cur×(1-pct), 用 cur×(1-pct). 受 24h_low  支撑.
       4h 数据缺失时回退到 ±pct 偏移.
    """
    if side == 'LONG':
        fallback = cur_price * (1 - pct)
        if low_4h and low_4h > 0:
            qi_shang = low_4h * 1.30                  # 七上 = 4h 低点 × 1.30
            lp = min(qi_shang, fallback)              # 取更低 (更保守做多)
        else:
            lp = fallback
        if low_24h and low_24h > 0:
            lp = max(lp, float(low_24h))
    else:  # SHORT
        fallback = cur_price * (1 + pct)
        if high_4h and high_4h > 0:
            ba_xia = high_4h * 0.80                   # 八下 = 4h 高点 × 0.80
            lp = max(ba_xia, fallback)                # 取更高 (更保守做空)
        else:
            lp = fallback
        if high_24h and high_24h > 0:
            lp = min(lp, float(high_24h))
    return round(lp, 8)


# ── 入场位置守卫 (所有子策略共用) ────────────────────────────────
def _entry_position_pct(cur, sym, cur_price, lookback_bars=ENTRY_POS_LOOKBACK_BARS):
    """当前价在 15M 最近 lookback_bars 根 K 线区间的百分位 (0=最低, 100=最高).
    > 100 表示已经突破区间上沿; < 0 表示已跌穿下沿. 无数据返回 None (放行).
    """
    import time as _t
    now_ms = int(_t.time() * 1000)
    start_ms = now_ms - lookback_bars * 15 * 60 * 1000
    cur.execute(
        """SELECT MAX(high_price) AS h, MIN(low_price) AS l
           FROM kline_data
           WHERE symbol=%s AND timeframe='15m'
             AND open_time >= %s AND open_time < %s""",
        (sym, start_ms, now_ms),
    )
    r = cur.fetchone()
    if not r or r.get('h') is None or r.get('l') is None:
        return None
    hi = float(r['h']); lo = float(r['l'])
    if hi <= lo:
        return 50.0
    return (cur_price - lo) / (hi - lo) * 100


def _check_entry_position(cur, sym, side, cur_price, tag=''):
    """入场位置守卫. 返回 (ok, reason).
    规则 (2026-04-24 基于 strategy_live Phase C 回测):
      - pos > 100: 破顶, 任何方向都拒绝 (已突破 3h 区间上沿)
      - pos < 0:   破底, 任何方向都拒绝
      - LONG pos > 90: 追高, 拒绝
      - SHORT pos < 10: 踩底, 拒绝
    kline 数据不足时放行 (ok=True, reason=None).
    """
    pct = _entry_position_pct(cur, sym, cur_price)
    if pct is None:
        return True, None
    if pct > 100.0:
        return False, "破顶 pos=%.0f%% %s" % (pct, tag)
    if pct < 0.0:
        return False, "破底 pos=%.0f%% %s" % (pct, tag)
    if side == 'LONG' and pct > ENTRY_POS_LONG_MAX:
        return False, "追高 pos=%.0f%% %s" % (pct, tag)
    if side == 'SHORT' and pct < ENTRY_POS_SHORT_MIN:
        return False, "踩底 pos=%.0f%% %s" % (pct, tag)
    return True, None


# ── 挂单检查 (DB 版) ─────────────────────────────────────────────
def _sync_state_on_cancel(conn, sym: str, order_source) -> None:
    """限价单被 _fill_pending_orders 撤掉后, 按 order_source 同步把对应
    strategy_state 行设 DONE. 否则 PENDING 卡死, 子策略 active_count 永远满槽.

    本函数会跨进程更新 (例如 strategy_live 撤掉了 strategy_whale:swan 挂的单,
    这里也会写 strategy='whale' stype='swan' 那行). DB 行锁兜底, 安全.
    """
    src = (order_source or "").strip()
    if ":" not in src:
        return
    strategy, _, stype = src.partition(":")
    strategy = strategy.replace("strategy_", "").strip()
    stype = stype.strip()
    if not strategy or not stype:
        return
    try:
        update_state(
            conn, strategy, sym, stype,
            state="DONE", pid=None, order_id=None,
            done_time=time.time(), last_reason="cancel",
        )
    except Exception as e:
        log.warning("[live-cancel-sync] %s %s:%s 同步 DONE 失败: %s",
                    sym, strategy, stype, e)


def _fill_pending_orders(conn):
    """扫描 PENDING 限价单, 价格到位则以市价成交"""
    cur = conn.cursor()
    cur.execute("""
        SELECT id, order_id, symbol, side, leverage, quantity,
               price AS limit_price, stop_loss_price, take_profit_price,
               order_source, created_at
        FROM futures_orders
        WHERE account_id=%s AND status='PENDING' AND order_type='LIMIT'
        ORDER BY created_at ASC
    """, (ACCOUNT_ID,))
    orders = cur.fetchall()
    cur.close()
    if not orders:
        return
    for o in orders:
        sym     = o['symbol']
        side    = o['side']
        limit_p = float(o['limit_price'] or 0)
        if limit_p <= 0:
            continue
        if o['created_at']:
            age_s = (datetime.datetime.now() - o['created_at']).total_seconds()
            if age_s > LIMIT_PENDING_MAX_S:
                c2 = conn.cursor()
                c2.execute("""UPDATE futures_orders
                    SET status='CANCELLED', cancellation_reason='timeout',
                        canceled_at=NOW(), updated_at=NOW() WHERE id=%s""", (o['id'],))
                conn.commit(); c2.close()
                log.info(
                    "限价单超时取消(>%dm) %s %s oid=%s",
                    LIMIT_PENDING_MAX_S // 60,
                    sym,
                    side,
                    o['order_id'],
                )
                _sync_state_on_cancel(conn, sym, o.get('order_source'))
                continue
        try:
            cur_p = get_price(sym)
        except Exception:
            continue
        pos_side = side.replace('OPEN_', '') if side.startswith('OPEN_') else side
        triggered = (pos_side == 'LONG' and cur_p <= limit_p) or (pos_side == 'SHORT' and cur_p >= limit_p)
        if not triggered:
            # 价格回撤到触发线另一侧 → 清除已有观察记录，继续挂单等下次触发
            if _trigger_first_seen.pop(o['id'], None) is not None:
                log.info("限价单触发回撤，重新等待 %s %s cur=%.5f limit=%.5f",
                         sym, side, cur_p, limit_p)
                _log_order_event(conn, o['order_id'], 'TRIGGER_RETREAT',
                                 cur_price=cur_p, limit_price=limit_p,
                                 detail=f"side={side} pos_side={pos_side}")
            continue
        # 已触发: 等下一根 5m K 线收盘, 方向确认才成交
        # SHORT 需要阴线 (close < open), LONG 需要阳线 (close > open), 平 K 算逆向
        # 2026-04-25 替代原 30s 时间确认
        # 2026-04-27: DISABLE_5M_CONFIRM=ON 时整段跳过, 触发即成交
        if DISABLE_5M_CONFIRM:
            _trigger_first_seen.pop(o['id'], None)
        else:
            first_seen_ms = _trigger_first_seen.get(o['id'])
            if first_seen_ms is None:
                _trigger_first_seen[o['id']] = int(time.time() * 1000)
                log.info("限价单触发观察 %s %s cur=%.5f limit=%.5f (等下根 5m %s线收盘确认)",
                         sym, side, cur_p, limit_p,
                         '阴' if pos_side == 'SHORT' else '阳')
                _log_order_event(conn, o['order_id'], 'TRIGGER_OBSERVING',
                                 cur_price=cur_p, limit_price=limit_p,
                                 detail=f"side={side} 等{('阴' if pos_side == 'SHORT' else '阳')}线收盘确认")
                continue
            # 算下一根 5m bar 的起止 ms (5m bar = 300000 ms)
            next_bar_open_ms  = (int(first_seen_ms) // 300000) * 300000 + 300000
            next_bar_close_ms = next_bar_open_ms + 300000
            if int(time.time() * 1000) < next_bar_close_ms:
                continue  # 还没到下根 5m 收盘
            # 取这根 5m bar
            c_bar = conn.cursor()
            c_bar.execute(
                """SELECT open_price, close_price FROM kline_data
                   WHERE symbol=%s AND timeframe='5m' AND open_time=%s LIMIT 1""",
                (sym, next_bar_open_ms),
            )
            bar_row = c_bar.fetchone()
            c_bar.close()
            if not bar_row:
                continue  # kline 数据延迟, 下一轮再查
            bar_o = float(bar_row['open_price'])
            bar_c = float(bar_row['close_price'])
            confirm_ok = (pos_side == 'SHORT' and bar_c < bar_o) \
                         or (pos_side == 'LONG' and bar_c > bar_o)
            if not confirm_ok:
                log.info("限价 5m 反向(%s) 不成交, 等下次触发: %s %s bar[o=%.5f c=%.5f]",
                         '阴未现' if pos_side == 'SHORT' else '阳未现',
                         sym, side, bar_o, bar_c)
                _log_order_event(conn, o['order_id'], '5M_REJECT',
                                 cur_price=cur_p, limit_price=limit_p,
                                 bar_open=bar_o, bar_close=bar_c,
                                 detail=f"side={side} 需{('阴' if pos_side == 'SHORT' else '阳')}线 实际 close={bar_c} open={bar_o}")
                _trigger_first_seen.pop(o['id'], None)
                continue
            # 5m K 线方向确认通过, 进入成交流程
            _trigger_first_seen.pop(o['id'], None)
        # 反向滑点熔断：LIMIT 被反向穿越过大时撤单，避免逆势进场
        if pos_side == 'LONG':
            reverse_slip = (limit_p - cur_p) / limit_p
        else:
            reverse_slip = (cur_p - limit_p) / limit_p
        if reverse_slip > REVERSE_SLIPPAGE_LIMIT:
            c2 = conn.cursor()
            c2.execute("""UPDATE futures_orders
                SET status='CANCELLED', cancellation_reason=%s,
                    canceled_at=NOW(), updated_at=NOW() WHERE id=%s""",
                (f'reverse_slippage_{reverse_slip:.4f}', o['id']))
            conn.commit(); c2.close()
            log.info("反向滑点熔断撤单 %s %s limit=%.5f cur=%.5f 偏离=%.2f%% (>%.1f%%)",
                     sym, side, limit_p, cur_p, reverse_slip * 100, REVERSE_SLIPPAGE_LIMIT * 100)
            _sync_state_on_cancel(conn, sym, o.get('order_source'))
            continue
        # 先把订单标成 FILLING，防止同一订单被重复触发（API 超时/异常后下一 tick 再捞到）
        c2 = conn.cursor()
        affected = c2.execute("""UPDATE futures_orders
            SET status='FILLING', updated_at=NOW()
            WHERE id=%s AND status='PENDING'""", (o['id'],))
        conn.commit(); c2.close()
        if not affected:
            # 被其他并发路径抢先处理了，跳过
            log.info("限价单已被处理，跳过 %s %s oid=%s", sym, side, o['order_id'])
            continue
        pos_id = None
        try:
            qty = float(o['quantity'] or 0)
            lev = int(o['leverage'] or LEVERAGE)
            sl  = float(o['stop_loss_price']  or 0) or None
            tp  = float(o['take_profit_price'] or 0) or None
            # 以实际成交价重算 SL/TP：限价被穿越时 fill_price 可能远偏离 limit_price，
            # 若继续用原止损价则实际 SL 幅度大幅压缩，容易被秒扫
            if sl and tp and limit_p > 0 and cur_p > 0 and abs(cur_p - limit_p) / limit_p > 0.001:
                if pos_side == 'LONG':
                    sl_ratio = (limit_p - sl) / limit_p
                    tp_ratio = (tp - limit_p) / limit_p
                else:
                    sl_ratio = (sl - limit_p) / limit_p
                    tp_ratio = (limit_p - tp) / limit_p
                if sl_ratio > 0 and tp_ratio > 0:
                    orig_sl, orig_tp = sl, tp
                    if pos_side == 'LONG':
                        sl = round(cur_p * (1 - sl_ratio), 8)
                        tp = round(cur_p * (1 + tp_ratio), 8)
                    else:
                        sl = round(cur_p * (1 + sl_ratio), 8)
                        tp = round(cur_p * (1 - tp_ratio), 8)
                    log.info("SL/TP重算 %s %s fill=%.5f limit=%.5f SL %.5f->%.5f TP %.5f->%.5f",
                             sym, side, cur_p, limit_p, orig_sl, sl, orig_tp, tp)
            src = (o.get('order_source') or 'strategy_live:limit-fill')
            max_hold = LONG_HOLD_MIN if pos_side == 'LONG' else SHORT_HOLD_MIN
            # 总开关: 裸奔模式下,限价单成交也不写 SL/TP/timeout
            if DISABLE_SL_TP_HOLD:
                sl_out, tp_out, hold_out = None, None, 0
            else:
                sl_out, tp_out, hold_out = sl, tp, max_hold
            payload = {
                "account_id": ACCOUNT_ID, "symbol": sym,
                "position_side": pos_side, "quantity": qty, "leverage": lev,
                "stop_loss_price": sl_out, "take_profit_price": tp_out, "source": src,
                "fill_price": cur_p, "max_hold_minutes": hold_out,
            }
            res    = _api("POST", "/api/futures/open", json=payload)
            data   = res.get("data") or {}
            pos_id = data.get("position_id") or data.get("id")
            if pos_id:
                c2 = conn.cursor()
                c2.execute("""UPDATE futures_orders
                    SET status='FILLED', avg_fill_price=%s, fill_time=NOW(),
                        executed_quantity=quantity, executed_value=total_value,
                        position_id=%s, updated_at=NOW()
                    WHERE id=%s""", (cur_p, pos_id, o['id']))
                conn.commit(); c2.close()
                log.info("限价单成交 %s %s @ %.5f  pid=%s  oid=%s",
                         sym, side, cur_p, pos_id, o['order_id'])
            else:
                # API 没返回 pos_id，改回 PENDING 让下一 tick 重试
                c2 = conn.cursor()
                c2.execute("UPDATE futures_orders SET status='PENDING', updated_at=NOW() WHERE id=%s", (o['id'],))
                conn.commit(); c2.close()
                log.warning("限价单成交无 pos_id，回退 PENDING %s %s oid=%s", sym, side, o['order_id'])
        except Exception as e:
            # API 调用失败，回退 PENDING
            try:
                c2 = conn.cursor()
                c2.execute("UPDATE futures_orders SET status='PENDING', updated_at=NOW() WHERE id=%s", (o['id'],))
                conn.commit(); c2.close()
            except Exception:
                pass
            log.warning("限价单成交异常，回退 PENDING %s %s: %s", sym, side, e)

def get_pos_status(pid):
    """返回 (status, realized_pnl, notes) 或 (None, None, None)"""
    try:
        d = _api("GET", f"/api/futures/positions/{pid}")
        pos = d.get("data") or d
        if isinstance(pos, list):
            pos = pos[0] if pos else {}
        return pos.get("status"), pos.get("realized_pnl", 0), pos.get("notes", "")
    except Exception:
        return None, None, None

def close_order(pid, reason="manual"):
    try:
        _api("POST", f"/api/futures/close/{pid}", json={"reason": reason})
    except Exception as e:
        log.warning("close_order %d failed: %s", pid, e)

def _trail_tp_check(conn, account, strategy, sym, pid, side, entry_p, peak_pct, entry_time_s=None):
    """移动止盈/硬止盈检查。触发则平仓并返回 True。"""
    if not entry_p:
        return False
    # 总开关开启: 裸奔,不执行硬TP/移动TP检查
    if DISABLE_SL_TP_HOLD:
        return False
    try:
        cur_p = get_price(sym)
    except Exception:
        return False
    pnl_pct = (cur_p - entry_p) / entry_p if side == 'LONG' else (entry_p - cur_p) / entry_p
    new_peak = max(float(peak_pct or 0.0), pnl_pct)
    if new_peak > float(peak_pct or 0.0):
        update_state(conn, account, sym, strategy, peak_pnl_pct=new_peak)
    if pnl_pct >= HARD_TP_PCT:
        close_order(pid, "hard-tp")
        log.info("硬止盈 [%s] %-18s  pnl=+%.1f%%", strategy.upper(), sym, pnl_pct * 100)
        return True
    # 动态 trail：按 peak 分档取回落阈值
    pullback_thresh = _dynamic_trail_pullback(new_peak)
    if (new_peak - pnl_pct) >= pullback_thresh:
        close_order(pid, "trail-tp")
        log.info("移动止盈 [%s] %-18s  pnl=+%.1f%%  peak=+%.1f%%  回撤%.1f%%  阈值%.1f%%",
                 strategy.upper(), sym, pnl_pct * 100, new_peak * 100,
                 (new_peak - pnl_pct) * 100, pullback_thresh * 100)
        return True
    # 入场保护期：开仓 ENTRY_GRACE_MIN 分钟内，early-sl 和 breakeven 都不触发（只靠硬 SL 兜底）
    import time as _t
    in_grace = entry_time_s and (_t.time() - float(entry_time_s)) < ENTRY_GRACE_MIN * 60
    if not in_grace:
        # 保本止损（曾浮盈 >= 1.5% 的单，回吐到 -0.5% 即平）
        if new_peak >= BREAKEVEN_AFTER_PEAK_PCT and pnl_pct <= BREAKEVEN_SL_PCT:
            close_order(pid, "breakeven-sl")
            log.info("保本止损 [%s] %-18s  pnl=%.1f%%  peak=+%.1f%%",
                     strategy.upper(), sym, pnl_pct * 100, new_peak * 100)
            return True
        # 早期止损（浮亏达 3%，比硬 SL 10% 提前）
        if pnl_pct <= -EARLY_SL_PCT:
            close_order(pid, "early-sl")
            log.info("早期止损 [%s] %-18s  pnl=%.1f%%", strategy.upper(), sym, pnl_pct * 100)
            return True
    return False

# ── DB ────────────────────────────────────────────────────────────
def get_db():
    return pymysql.connect(
        host=os.getenv('DB_HOST'), port=int(os.getenv('DB_PORT', 3306)),
        user=os.getenv('DB_USER'), password=os.getenv('DB_PASSWORD', ''),
        db=os.getenv('DB_NAME'), charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

def now_s():
    return time.time()

# ── B. 顶部做空 ──────────────────────────────────────────────────
def _check_topshort_standard_signal(cur, conn, sym: str, now_ms: int):
    """检测 topshort standard 信号 (48h pump >= 80% + N-bar no new high). 触发返回 dict, 否则 None.
    抽自 topshort_tick 原内联代码 (2026-04-30 重构, 为 SIG_WAIT 等待期复用).
    cur 必须是已 open 的 cursor.
    """
    if not _topshort_has_min_listed_history(cur, sym, now_ms):
        return None
    cur.execute("""
        SELECT open_time, high_price, low_price, close_price FROM kline_data
        WHERE timeframe='1h' AND symbol=%s
          AND open_time >= UNIX_TIMESTAMP(NOW()-INTERVAL 4 DAY)*1000
          AND open_time + 3600000 < %s
        ORDER BY open_time ASC
    """, (sym, now_ms))
    bars = cur.fetchall()
    n = len(bars)
    if n < TOP_LOOKBACK_H + TOP_NO_NEW_H + 2:
        return None

    h  = [float(b['high_price'])  for b in bars]
    lo = [float(b['low_price'])   for b in bars]
    ts = [b['open_time']          for b in bars]

    for i in range(n - TOP_NO_NEW_H - 2,
                   max(0, n - TOP_LOOKBACK_H - TOP_NO_NEW_H - 10) - 1, -1):
        lo_win = min(lo[max(0, i - TOP_LOOKBACK_H):i]) if i > 0 else lo[0]
        if lo_win == 0:
            continue
        pump = (h[i] - lo_win) / lo_win
        if pump < TOP_PUMP_THRESH:
            continue
        peak = h[i]
        if i + TOP_NO_NEW_H >= n:
            continue
        if not all(h[i+j] < peak for j in range(1, TOP_NO_NEW_H + 1)):
            continue
        ei = i + TOP_NO_NEW_H
        entry_ts = ts[ei]
        if now_ms - entry_ts > TOP_SIGNAL_AGE * 1000:
            return None  # 信号太老

        try:
            price = get_price(sym)
        except Exception:
            return None
        if price <= lo_win:
            log.info("TOPSHORT 跳过  %-18s  现价%.5f <= 启动价%.5f", sym, price, lo_win)
            return None
        dd = (peak - price) / peak
        if dd > 0.50:
            log.info("TOPSHORT 跳过  %-18s  从峰值已跌%.0f%%, 回落过深", sym, dd * 100)
            return None
        cur.execute("SELECT change_24h FROM price_stats_24h WHERE symbol=%s", (sym,))
        _r = cur.fetchone()
        if _r and _r.get('change_24h') is not None:
            _ch24 = float(_r['change_24h'])
            if _ch24 < TOP_MIN_24H_CHANGE_PCT:
                log.info("TOPSHORT 跳过 %-18s: 24h=%.1f%% < %.0f%%, 已跌过多不再做空",
                         sym, _ch24, TOP_MIN_24H_CHANGE_PCT)
                return None

        ok_pos, reason = _check_entry_position(cur, sym, 'SHORT', price, tag='topshort')
        if not ok_pos:
            log.info("TOPSHORT 跳过 %-18s: %s", sym, reason)
            return None
        h24, l24 = _get_24h_stats(cur, sym)
        h4,  l4  = _get_4h_stats(cur, sym)
        lp = _calc_limit_price("SHORT", price, h24, l24, pct=LIVE_LIMIT_OFFSET_PCT,
                                high_4h=h4, low_4h=l4)
        return {'price': price, 'lp': lp, 'pump': pump, 'dd': dd,
                'peak': peak, 'entry_ts': entry_ts}
    return None


def topshort_tick(conn, active_syms):
    now_ms = int(now_s() * 1000)
    nowt = now_s()

    # 顶空 DONE 冷却（平仓/撤单后），到期再 IDLE
    for row in list_all_stype(conn, 'live', 'topshort'):
        if row.get('state') != 'DONE':
            continue
        sym = row['symbol']
        anchor = ensure_cooldown_anchor_epoch(conn, 'live', sym, 'topshort', row, nowt)
        if nowt - anchor > TOPSHORT_COOLDOWN:
            update_state(conn, 'live', sym, 'topshort', state='IDLE', pid=None, order_id=None)

    # 检查已有顶空仓位
    active_rows = list_active(conn, 'live', 'topshort')
    for pos in active_rows:
        sym = pos['symbol']
        if pos.get('state') == 'DONE':
            continue
        if pos.get('state') == 'SIG_WAIT':
            # SIG_WAIT 由下方独立循环处理, 这里跳过 (2026-04-30)
            continue
        # 挂单状态: 等待限价单成交
        if pos.get('order_id') and not pos.get('pid'):
            cur = conn.cursor()
            cur.execute(
                "SELECT status, position_id FROM futures_orders WHERE order_id=%s LIMIT 1",
                (pos['order_id'],)
            )
            row = cur.fetchone()
            cur.close()
            if row:
                st     = (row.get('status') or '').upper()
                pos_id = row.get('position_id')
                if st == 'FILLED' and pos_id:
                    t_fill = now_s()
                    update_state(conn, 'live', sym, 'topshort',
                                 pid=int(pos_id), order_id=None, entry_time=t_fill)
                    log.info("TOPSHORT 限价单成交 %-18s  pid=%d", sym, int(pos_id))
                    pos = {**pos, 'pid': int(pos_id), 'order_id': None, 'entry_time': t_fill}
                elif st in ('CANCELLED', 'REJECTED'):
                    log.info("TOPSHORT 限价单取消 %-18s  oid=%s -> DONE 冷却", sym, pos.get('order_id'))
                    update_state(
                        conn,
                        'live',
                        sym,
                        'topshort',
                        state='DONE',
                        pid=None,
                        order_id=None,
                        done_time=nowt,
                        last_reason='cancel',
                    )
                    continue
            if not pos.get('pid'):
                continue  # 仍在挂单中
        if not pos.get('pid'):
            log.warning("TOPSHORT 异常无 pid %-18s -> DONE 冷却", sym)
            update_state(
                conn,
                'live',
                sym,
                'topshort',
                state='DONE',
                pid=None,
                order_id=None,
                done_time=nowt,
                last_reason='orphan',
            )
            continue
        status, pnl, notes = get_pos_status(pos['pid'])
        if status is None:
            continue  # API 错误，保留状态
        if status == 'open':
            _trail_tp_check(conn, 'live', 'topshort', sym, pos['pid'],
                            'SHORT', pos.get('entry_p', 0), pos.get('peak_pnl_pct', 0),
                            pos.get('entry_time', 0))
            continue
        else:
            pnl_pct = (pnl or 0) / MARGIN * 100
            reason = "手动" if (notes and '手动' in str(notes)) else status
            lr = 'manual' if (notes and '手动' in str(notes)) else ('TP' if (pnl or 0) > 0 else 'SL')
            log.info(
                "TOPSHORT 平仓  %-18s  pid=%d  pnl=%+.1f%%  reason=%s  冷却%dh",
                sym,
                pos['pid'],
                pnl_pct,
                reason,
                TOPSHORT_COOLDOWN // 3600,
            )
            update_state(
                conn,
                'live',
                sym,
                'topshort',
                state='DONE',
                pid=None,
                order_id=None,
                done_time=nowt,
                last_reason=lr,
            )

    # SIG_WAIT 状态: 等待 N min 后重判信号 (2026-04-30 新增)
    # 注意 list_active 会把 SIG_WAIT 行也包含进 open_syms (state != IDLE), 防止新信号
    # 扫描在等待期间重复进入. 这里独立处理 SIG_WAIT 行的转移逻辑.
    cur_sw = conn.cursor()
    try:
        for row in list_all_stype(conn, 'live', 'topshort'):
            if row.get('state') != 'SIG_WAIT':
                continue
            sym_sw = row['symbol']
            sig_p = float(row.get('entry_p') or 0)
            sig_t = float(row.get('entry_time') or 0)
            if sig_p <= 0 or sig_t <= 0:
                update_state(conn, 'live', sym_sw, 'topshort',
                             state='IDLE', entry_p=0, entry_time=0)
                continue
            try:
                cur_p_now = get_price(sym_sw)
            except Exception:
                continue
            # 反向: SHORT 信号反向 = 价格涨过 sig_p × (1 + adverse)
            if cur_p_now >= sig_p * (1 + TOPSHORT_SIG_ADVERSE_PCT):
                log.info("TOPSHORT 信号反向失效 %-18s sig=%.5f cur=%.5f (+%.2f%%)",
                         sym_sw, sig_p, cur_p_now, (cur_p_now - sig_p) / sig_p * 100)
                update_state(conn, 'live', sym_sw, 'topshort',
                             state='IDLE', entry_p=0, entry_time=0,
                             last_reason='sig_adverse')
                continue
            elapsed = nowt - sig_t
            if elapsed < TOPSHORT_SIG_WAIT_MIN * 60:
                continue
            sig = _check_topshort_standard_signal(cur_sw, conn, sym_sw, now_ms)
            if not sig:
                log.info("TOPSHORT 等待期满信号已失效 %-18s, 回 IDLE (等待 %dmin)",
                         sym_sw, int(elapsed / 60))
                update_state(conn, 'live', sym_sw, 'topshort',
                             state='IDLE', entry_p=0, entry_time=0,
                             last_reason='sig_expired')
                continue
            pid, oid, pending = open_order(sym_sw, "SHORT", sig['price'], HARD_TP_PCT,
                                           TOP_SL_PCT, TOP_HOLD_H * 60, "topshort",
                                           sig['lp'])
            if not pid and not oid:
                update_state(conn, 'live', sym_sw, 'topshort',
                             state='IDLE', entry_p=0, entry_time=0)
                continue
            log.info("TOPSHORT 等待期满入场 %-18s @ %.5f (限价%.5f) 峰=%.5f(泵%.0f%%) 回落%.1f%% 等待%dmin pid=%s oid=%s",
                     sym_sw, sig['price'], sig['lp'], sig['peak'],
                     sig['pump'] * 100, sig['dd'] * 100, int(elapsed / 60), pid, oid)
            update_state(conn, 'live', sym_sw, 'topshort',
                         state='SHORT', pid=pid, order_id=oid,
                         entry_p=sig['lp'] if pending else sig['price'],
                         peak_pnl_pct=0.0, peak=sig['peak'], pump_pct=sig['pump'],
                         entry_ts=sig['entry_ts'])
    finally:
        cur_sw.close()

    # 扫描新信号 (2026-05-15 极简化: climax 路径整段删除, 仅留 standard)
    open_syms = {r['symbol'] for r in list_active(conn, 'live', 'topshort')}

    cur = conn.cursor()
    for sym in active_syms:
        if sym in open_syms:
            continue

        sig = _check_topshort_standard_signal(cur, conn, sym, now_ms)
        if not sig:
            continue

        # 检查是否已有相同 entry_ts 的信号（避免重复入场）
        existing = get_or_create(conn, 'live', sym, 'topshort', {})
        if existing.get('entry_ts') == sig['entry_ts'] and existing.get('state') != 'IDLE':
            continue

        if not TOPSHORT_SIG_WAIT_ENABLED:
            # 现有路径: 立即下单
            pid, oid, pending = open_order(sym, "SHORT", sig['price'], HARD_TP_PCT,
                                           TOP_SL_PCT, TOP_HOLD_H * 60, "topshort",
                                           sig['lp'])
            if not pid and not oid:
                continue
            log.info("TOPSHORT 入场  %-18s @ %.5f (限价%.5f)  峰=%.5f(泵%.0f%%)  回落%.1f%%  pid=%s oid=%s",
                     sym, sig['price'], sig['lp'], sig['peak'], sig['pump'] * 100,
                     sig['dd'] * 100, pid, oid)
            update_state(conn, 'live', sym, 'topshort',
                         state='SHORT', pid=pid, order_id=oid,
                         entry_p=sig['lp'] if pending else sig['price'],
                         peak_pnl_pct=0.0, peak=sig['peak'], pump_pct=sig['pump'],
                         entry_ts=sig['entry_ts'])
        else:
            # 新路径: 进 SIG_WAIT 等 N min 重判
            log.info("TOPSHORT 信号观察期开始 %-18s @ %.5f (等 %d min 重判, 反向阈值 %.1f%%)",
                     sym, sig['price'], TOPSHORT_SIG_WAIT_MIN,
                     TOPSHORT_SIG_ADVERSE_PCT * 100)
            update_state(conn, 'live', sym, 'topshort',
                         state='SIG_WAIT', entry_p=sig['price'], entry_time=nowt,
                         side='SHORT', peak=sig['peak'], pump_pct=sig['pump'],
                         entry_ts=sig['entry_ts'])
        open_syms.add(sym)
    cur.close()


# ── 启动同步 ─────────────────────────────────────────────────────
def _sync_state(conn):
    """启动时从 API 拉取已有 strategy_live:topshort 仓位, 防止重启重复开单.
    2026-05-15 极简化后仅同步 topshort, 其它 stype 都已删除."""
    try:
        d = _api("GET", "/api/futures/positions?status=open")
        for p in (d.get("data") or []):
            src  = p.get("source") or ""
            if not src.startswith("strategy_live:"):
                continue
            sym  = p["symbol"]
            side = p["position_side"]
            if "topshort" in src and side == "SHORT":
                existing = get_or_create(conn, 'live', sym, 'topshort', {})
                if existing.get('state') not in ('SHORT',):
                    update_state(conn, 'live', sym, 'topshort',
                                 state='SHORT', pid=p["id"],
                                 entry_p=p["entry_price"],
                                 peak=p["entry_price"], pump_pct=0, entry_ts=0)
                    log.info("同步已有顶空仓位: %s pid=%d @ %.5f", sym, p["id"], p["entry_price"])
    except Exception as e:
        log.warning("同步持仓失败: %s", e)


# ── 主循环 ───────────────────────────────────────────────────────
def main():
    _load_live_config()
    log.info("=" * 56)
    log.info("Strategy Live Runner  实盘模拟  (极简化, 仅 topshort)")
    log.info("仅运行 topshort: 48h 涨>=80%% + N 根 1h 无新高 -> 限价开 SHORT")
    log.info("账户=%d  杠杆=%dx  每笔保证金=%.0f USDT", ACCOUNT_ID, LEVERAGE, MARGIN)
    log.info("=" * 56)

    init_conn = get_db()
    ensure_table(init_conn)
    _sync_state(init_conn)
    init_conn.close()

    poll_count = 0
    _last_cfg_reload = 0
    while True:
        try:
            conn = get_db()
            cur  = conn.cursor()
            poll_count += 1

            try:
                if time.time() - _last_cfg_reload >= 60:
                    _load_live_config()
                    _last_cfg_reload = time.time()
            except Exception as e:
                log.warning("配置重载失败: %s", e)

            try:
                _fill_pending_orders(conn)
            except Exception as e:
                log.warning("_fill_pending_orders 异常: %s", e)

            try:
                _close_overdue(conn)
            except Exception as e:
                log.warning("_close_overdue 异常: %s", e)

            active_syms = get_active_symbols(cur)

            if poll_count % TOPSHORT_EVERY == 1:
                try:
                    topshort_tick(conn, active_syms)
                except Exception as e:
                    log.warning("topshort_tick error: %s", e)

            top_active = list_active(conn, 'live', 'topshort')
            if top_active:
                summary = " | ".join(
                    "top:%s %s pid=%s" % (r['symbol'], r.get('state'), r.get('pid'))
                    for r in top_active[:8]
                )
                log.info("持仓[%d]: %s", len(top_active), summary)
            elif poll_count % 10 == 1:
                log.info("当前无持仓, 等待信号...")

            cur.close()
            conn.close()

        except Exception as e:
            import traceback
            log.error("主循环错误: %s\n%s", e, traceback.format_exc())

        time.sleep(POLL_SECS)


if __name__ == '__main__':
    main()
