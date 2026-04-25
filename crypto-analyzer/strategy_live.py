"""
实盘策略运行器 - 真实下单到 localhost:9021
A. 追击: 5m K线检测涨幅>=4% -> 真实开多, TP梯度5%-10%, SL 8%转空
B. 顶部做空: (1) 48h涨>=80% + 6h无新高(须12d+1h数据) 或 (2) 1H climax 见顶(数据可放宽) -> 真实开空

每5分钟轮询:
  - 检查已有仓位是否平掉 (TP/SL/超时)
  - 扫描新信号并下单
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

# 追击参数
CHASE_PUMP_BARS = 24
CHASE_PUMP_PCT  = 0.12
CHASE_SL_PCT             = 0.08
CHASE_EXHAUST_MAX_DD     = 0.06  # 近期峰值到当前收盘的最大回撤，超过则视为耗竭，跳过进场
CHASE_LEADER_BAR_MIN_PCT = 0.03  # 窗口内至少一根 5m bar 单 bar 涨幅须 >= 3%，排除慢速爬升
CHASE_MIN_24H_CHANGE_PCT = -12.0 # 24h 跌幅超过阈值则不追（避免抓反弹飞刀，2026-04-24；2026-04-25 -10→-12 放宽 2pt）
CHASE_MAX_24H_CHANGE_PCT =  15.0 # 24h 涨幅超过阈值则不追（避免追顶接棒，2026-04-24）
DUMP_MIN_24H_CHANGE_PCT  = -15.0 # dump SHORT: 24h 已跌超此阈值则不追跌（避免接飞刀）
TOP_MIN_24H_CHANGE_PCT     = -15.0 # topshort SHORT: 24h 已跌过此阈值不再开空（已跌不该再做空，2026-04-25）
BOTLONG_MAX_24H_CHANGE_PCT =  15.0 # bottomlong LONG: 24h 已涨过此阈值不再开多（已涨不是底，2026-04-25）

# 入场位置守卫 (所有子策略共用, 基于 3h 15m K 线区间百分位)
ENTRY_POS_LOOKBACK_BARS  = 12     # 3h 回看 (12 根 15m)
ENTRY_POS_LONG_MAX       = 90.0   # LONG: 入场位 > 90% 视为追高, 拒绝
ENTRY_POS_SHORT_MIN      = 10.0   # SHORT: 入场位 < 10% 视为踩底, 拒绝
# 破顶破底硬规则: pos > 100% 或 < 0% 任何方向都拒绝 (写死在 _check_entry_position)

LONG_HOLD_MIN   = 6 * 60
SHORT_HOLD_MIN  = 6 * 60
CHASE_MAX_HOLD  = LONG_HOLD_MIN
# 各子策略平仓/撤单后同标的再开仓最短间隔（秒）
POST_CLOSE_COOLDOWN_S = 4 * 3600
CHASE_COOLDOWN = POST_CLOSE_COOLDOWN_S
SYMBOL_MAX_DAILY_SL = 2        # 同标的当日止损 >= 2 次则暂停该标的当日所有新开仓
RECENT_SL_COOLDOWN_MIN = 240  # 止损后 4 小时内禁止同标的开新仓，防止连续被扫

# 顶部做空参数
TOP_PUMP_THRESH = 0.80
TOP_NO_NEW_H    = 6
TOP_LOOKBACK_H  = 48
TOP_HOLD_H      = 6
TOP_SL_PCT      = 0.12
TOP_SIGNAL_AGE  = 6 * 3600
TOPSHORT_COOLDOWN = POST_CLOSE_COOLDOWN_S
# 顶空依赖「长期顶部结构」；1h K 最早一根距今不足该天数则不做新开顶空（追跌/追击不受影响）
TOPSHORT_MIN_HISTORY_DAYS = 12
TOPSHORT_MIN_HISTORY_MS = TOPSHORT_MIN_HISTORY_DAYS * 24 * 60 * 60 * 1000
# topshort-climax：1h 根数与最早 K 距今门槛低于「经典顶空 12d」，便于 BSB 等上市/入库较短标的
TOPSHORT_CLIMAX_MIN_BARS = 28
TOPSHORT_CLIMAX_MIN_SPAN_MS = int(1.25 * 24 * 60 * 60 * 1000)

# ── 全局开关：climax 系列信号（topshort-climax + bottomlong-climax）──
# 2026-04-25 禁用：3 天样本统计 PF 0.46/0.58，胜率 33-44%，显著拖累整体 PF。
# 改回 True 即可恢复，无需其它修改。
CLIMAX_SIGNALS_ENABLED = False

# 巨量见顶（1H）：(1) 大阳实体 + 巨量 或 (2) 长上影 + 巨量（庄家冲高砸盘，收盘可阴可阳）
# 单根 K 振幅 (high-low)/open >= TOPCLI_MIN_RANGE_FULL_PCT（默认 4.5%，可改）；且放量
# 筋骨：在最近 LEADER_LOOKBACK 根内，大阳须为「阳线里振幅最大」、上影须为「全 K 振幅最大」；
# 且领袖 K 的索引须 <= n-1-POST_LEADER_WAIT_BARS：即该 K 收盘后再过 N 根 1H，才确认没有更大阳/更大振幅，再允许开空（默认 2 根=2 小时）。
# 之后价格从高点回落；不等「48h 低点涨幅 + 6h 无新高」
TOPCLI_LOOKBACK_BARS   = 40
TOPCLI_LEADER_LOOKBACK = 24
TOPCLI_POST_LEADER_WAIT_BARS = 2
TOPCLI_MAX_PENDING         = 1    # 全局最多同时挂几张 climax 限价单
TOPCLI_VOL_LOOKBACK    = 20
TOPCLI_VOL_MULT        = 2.0
TOPCLI_MIN_BODY_VS_O = 0.025
TOPCLI_MIN_RANGE_FULL_PCT = 0.045
TOPCLI_MIN_BODY_OF_RANGE = 0.42
TOPCLI_PULLBACK_FR     = 0.012
TOPCLI_MAX_DD_FR       = 0.48
TOPCLI_SIGNAL_AGE_MS   = 22 * 3600 * 1000
TOPCLI_MAX_OPEN_AGE_MS = 26 * 3600 * 1000

# 底部做多（bottomlong-climax）：大阴线/长下影 + 放量 → 底部做多（topshort-climax 镜像）
BOTLONG_LOOKBACK_BARS            = 40
BOTLONG_LEADER_LOOKBACK          = 24
BOTLONG_POST_LEADER_WAIT_BARS    = 2
BOTLONG_MAX_PENDING              = 1    # 全局最多同时挂几张 climax 做多限价单
BOTLONG_VOL_LOOKBACK             = 20
BOTLONG_VOL_MULT                 = 2.0
BOTLONG_MIN_BODY_VS_O            = 0.025   # |close-open|/open 阴线实体门槛
BOTLONG_MIN_RANGE_FULL_PCT       = 0.045   # (high-low)/open 振幅门槛
BOTLONG_MIN_BODY_OF_RANGE        = 0.42    # 阴线实体/振幅
BOTLONG_MIN_LOWER_WICK_OF_RANGE  = 0.34   # 下影/振幅（下影模式）
BOTLONG_MIN_DROP_TO_LOW_VS_O     = 0.020   # (open-low)/open 下影模式最低跌幅
BOTLONG_PULLBACK_FR              = 0.012   # 反弹确认：现价须距低点 >= 此比例
BOTLONG_MAX_DD_FR                = 0.48    # 现价反弹上限（距低点过高则放弃）
BOTLONG_SIGNAL_AGE_MS            = 22 * 3600 * 1000
BOTLONG_MAX_OPEN_AGE_MS          = 26 * 3600 * 1000
BOTLONG_SL_PCT                   = 0.12
BOTLONG_HOLD_H                   = 6
BOTLONG_COOLDOWN                 = POST_CLOSE_COOLDOWN_S

# 追跌参数
DUMP_BARS     = 48
DUMP_PCT      = 0.10
DUMP_SL_PCT   = 0.08
DUMP_MAX_HOLD = SHORT_HOLD_MIN

# 移动止盈参数（三个策略共用）
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
    """返回当前 peak 允许的最大回落；peak 不足最低档返回 inf（不触发 trail）"""
    for threshold, pullback in TRAIL_TP_TIERS:
        if peak_pct >= threshold:
            return pullback
    return float('inf')
DUMP_COOLDOWN = POST_CLOSE_COOLDOWN_S

# 从 system_settings 动态加载的参数（运行时覆盖上方常量）
LIVE_SL_PCT           = 0.10   # 统一止损（覆盖各子策略 *_SL_PCT）
LIVE_HARD_TP_PCT      = HARD_TP_PCT
LIVE_LIMIT_OFFSET_PCT = 0.03   # 限价单挂单偏移
LIVE_HOLD_H           = 6      # 最大持仓时长（小时）
DISABLE_SL_TP_HOLD    = False  # 总开关: 新开仓不设 SL/TP/timeout, 且跳过进程内硬TP/移动TP检查


def _load_live_config() -> None:
    """从 system_settings 读取策略参数，覆盖模块级常量。进程启动时调用一次。"""
    global LIVE_SL_PCT, LIVE_HARD_TP_PCT, LIVE_LIMIT_OFFSET_PCT, LIVE_HOLD_H
    global CHASE_SL_PCT, TOP_SL_PCT, BOTLONG_SL_PCT, DUMP_SL_PCT
    global HARD_TP_PCT, LONG_HOLD_MIN, SHORT_HOLD_MIN
    global CHASE_MAX_HOLD, DUMP_MAX_HOLD, TOP_HOLD_H, BOTLONG_HOLD_H
    global DISABLE_SL_TP_HOLD
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
                    "'live_limit_offset_pct','live_hold_hours','disable_sl_tp_hold')"
                )
                rows = {r['setting_key']: r['setting_value'] for r in cur.fetchall()}
        finally:
            conn.close()
        LIVE_SL_PCT           = float(rows.get('live_sl_pct',           LIVE_SL_PCT))
        LIVE_HARD_TP_PCT      = float(rows.get('live_hard_tp_pct',      LIVE_HARD_TP_PCT))
        LIVE_LIMIT_OFFSET_PCT = float(rows.get('live_limit_offset_pct', LIVE_LIMIT_OFFSET_PCT))
        LIVE_HOLD_H           = int(  rows.get('live_hold_hours',        LIVE_HOLD_H))
        _raw_disable = str(rows.get('disable_sl_tp_hold', '0')).strip().lower()
        DISABLE_SL_TP_HOLD = _raw_disable in ('1', 'true', 'yes', 'on')
        CHASE_SL_PCT   = LIVE_SL_PCT
        TOP_SL_PCT     = LIVE_SL_PCT
        BOTLONG_SL_PCT = LIVE_SL_PCT
        DUMP_SL_PCT    = LIVE_SL_PCT
        HARD_TP_PCT    = LIVE_HARD_TP_PCT
        LONG_HOLD_MIN  = LIVE_HOLD_H * 60
        SHORT_HOLD_MIN = LIVE_HOLD_H * 60
        CHASE_MAX_HOLD = LONG_HOLD_MIN
        DUMP_MAX_HOLD  = SHORT_HOLD_MIN
        TOP_HOLD_H     = LIVE_HOLD_H
        BOTLONG_HOLD_H = LIVE_HOLD_H
        log.info(
            "strategy_live 参数已加载: SL=%.0f%% TP=%.0f%% offset=%.1f%% hold=%dh disable_sl_tp_hold=%s",
            LIVE_SL_PCT * 100, LIVE_HARD_TP_PCT * 100, LIVE_LIMIT_OFFSET_PCT * 100, LIVE_HOLD_H,
            DISABLE_SL_TP_HOLD,
        )
        if DISABLE_SL_TP_HOLD:
            log.warning("!!! DISABLE_SL_TP_HOLD=ON: 新开仓将不设 SL/TP/timeout, 硬TP/移动TP检查跳过 !!!")
    except Exception as exc:
        log.error("_load_live_config 失败，使用默认值: %s", exc)


POLL_SECS       = 60
TOPSHORT_EVERY  = 5
# 各子策略 LIMIT 挂单在 futures_orders 中保持 PENDING 的最长时间，超时由 _fill_pending_orders 标为取消
LIMIT_PENDING_MAX_S = 2 * 60 * 60   # 2026-04-25 1h → 2h, 给信号更多成交机会

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
_topshort_climax_hist_cache: dict[str, tuple[bool, float]] = {}


def _topshort_has_min_history_for_climax(cur, sym: str, now_ms: int) -> bool:
    """topshort-climax 专用：满 12d 仍优先；否则至少 TOPSHORT_CLIMAX_MIN_BARS 根 1h 且最早 K 距今 >= CLIMAX_MIN_SPAN。"""
    if _topshort_has_min_listed_history(cur, sym, now_ms):
        return True
    t = time.time()
    ent = _topshort_climax_hist_cache.get(sym)
    if ent is not None and (t - ent[1]) < _TOPSHORT_HIST_TTL_SEC:
        return ent[0]
    cur.execute(
        """
        SELECT COUNT(*) AS cnt, MIN(open_time) AS tmin FROM kline_data
        WHERE timeframe='1h' AND symbol=%s
        """,
        (sym,),
    )
    r = cur.fetchone() or {}
    cnt = int(r.get("cnt") or 0)
    tmin = r.get("tmin")
    if tmin is None or cnt < TOPSHORT_CLIMAX_MIN_BARS:
        _topshort_climax_hist_cache[sym] = (False, t)
        return False
    ok = (now_ms - int(tmin)) >= TOPSHORT_CLIMAX_MIN_SPAN_MS
    _topshort_climax_hist_cache[sym] = (ok, t)
    return ok


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
def _check_pending_db(conn, sym, stype):
    """检查限价挂单是否成交/取消。返回 (should_continue, row)。
    should_continue=False 表示仍在挂单中，本 tick 跳过。"""
    row = get_or_create(conn, 'live', sym, stype, {})
    oid = row.get('order_id')
    if not oid:
        return True, row
    if row.get('pid'):
        update_state(conn, 'live', sym, stype, order_id=None)
        return True, {**row, 'order_id': None}
    cur = conn.cursor()
    cur.execute(
        "SELECT status, position_id FROM futures_orders WHERE order_id=%s LIMIT 1", (oid,)
    )
    order = cur.fetchone()
    cur.close()
    if not order:
        update_state(conn, 'live', sym, stype, order_id=None)
        return True, {**row, 'order_id': None}
    st     = (order.get('status') or '').upper()
    pos_id = order.get('position_id')
    if st == 'FILLED' and pos_id:
        update_state(conn, 'live', sym, stype, pid=int(pos_id), order_id=None)
        log.info("限价单成交 (%s) -> pid=%d  oid=%s", stype, int(pos_id), oid)
        return True, {**row, 'pid': int(pos_id), 'order_id': None}
    if st in ('CANCELLED', 'REJECTED'):
        t = now_s()
        update_state(
            conn,
            'live',
            sym,
            stype,
            state='DONE',
            pid=None,
            order_id=None,
            done_time=t,
            last_reason='cancel',
        )
        return True, {**row, 'state': 'DONE', 'pid': None, 'order_id': None, 'done_time': t, 'last_reason': 'cancel'}
    return False, row  # still PENDING

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
            continue
        # 已触发: 等下一根 5m K 线收盘, 方向确认才成交
        # SHORT 需要阴线 (close < open), LONG 需要阳线 (close > open), 平 K 算逆向
        # 2026-04-25 替代原 30s 时间确认
        first_seen_ms = _trigger_first_seen.get(o['id'])
        if first_seen_ms is None:
            _trigger_first_seen[o['id']] = int(time.time() * 1000)
            log.info("限价单触发观察 %s %s cur=%.5f limit=%.5f (等下根 5m %s线收盘确认)",
                     sym, side, cur_p, limit_p,
                     '阴' if pos_side == 'SHORT' else '阳')
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

def get_5m_bars(cur, sym, limit=80):
    cur.execute("""
        SELECT open_time, open_price, high_price, low_price, close_price
        FROM kline_data WHERE timeframe='5m' AND symbol=%s
        ORDER BY open_time DESC LIMIT %s
    """, (sym, limit))
    return list(reversed(cur.fetchall()))

def get_1h_bars(cur, sym, limit=80):
    cur.execute("""
        SELECT open_time, open_price, high_price, low_price, close_price
        FROM kline_data WHERE timeframe='1h' AND symbol=%s
        ORDER BY open_time DESC LIMIT %s
    """, (sym, limit))
    return list(reversed(cur.fetchall()))


def _topshort_leader_idx_max_range(rows, win_lo: int, win_hi: int, bull_only: bool):
    """在 [win_lo, win_hi] 内找振幅 (high-low) 最大的索引；bull_only 时仅统计阳线。
    并列取更靠后的 K（更近）。无合法 K 返回 None。"""
    best_j = None
    best_hl = -1.0
    for j in range(win_lo, win_hi + 1):
        b = rows[j]
        try:
            o = float(b.get("open_price") or 0)
            h = float(b.get("high_price") or 0)
            l = float(b.get("low_price") or 0)
            c = float(b.get("close_price") or 0)
        except (TypeError, ValueError):
            continue
        if o <= 0:
            continue
        if bull_only and not (c > o):
            continue
        hl = h - l
        if hl <= 0:
            continue
        if hl > best_hl or (abs(hl - best_hl) <= 1e-12 and best_j is not None and j > best_j):
            best_hl, best_j = hl, j
    return best_j


def _bottomlong_leader_idx_max_range(rows, win_lo: int, win_hi: int, bear_only: bool):
    """在 [win_lo, win_hi] 内找振幅 (high-low) 最大的索引；bear_only 时仅统计阴线。
    并列取更靠后的 K（更近）。无合法 K 返回 None。"""
    best_j = None
    best_hl = -1.0
    for j in range(win_lo, win_hi + 1):
        b = rows[j]
        try:
            o = float(b.get("open_price") or 0)
            h = float(b.get("high_price") or 0)
            l = float(b.get("low_price") or 0)
            c = float(b.get("close_price") or 0)
        except (TypeError, ValueError):
            continue
        if o <= 0:
            continue
        if bear_only and not (c < o):
            continue
        hl = h - l
        if hl <= 0:
            continue
        if hl > best_hl or (abs(hl - best_hl) <= 1e-12 and best_j is not None and j > best_j):
            best_hl, best_j = hl, j
    return best_j


def evaluate_topshort_climax_signal(rows: list, now_ms: int, price: float):
    """
    纯逻辑：给定升序 1h K 与「当前价」、回放时刻 now_ms，判断是否满足 topshort-climax（不含 DB 去重与下单）。
    返回 (True, detail_dict) 或 (False, detail_dict)，detail 含 leaders、失败原因等。
    """
    detail: dict = {}
    n = len(rows)
    wait = TOPCLI_POST_LEADER_WAIT_BARS
    if n < TOPCLI_VOL_LOOKBACK + wait + 3:
        return False, {"fail": "not_enough_rows", "n": n}

    def _f(b, k):
        v = b.get(k)
        try:
            return float(v) if v is not None else 0.0
        except (TypeError, ValueError):
            return 0.0

    last = rows[-1]
    last_c = _f(last, "close_price")

    win_hi = n - 1 - wait
    if win_hi < 0:
        return False, {"fail": "win_hi"}
    win_lo = max(0, win_hi - (TOPCLI_LEADER_LOOKBACK - 1))
    if win_hi < win_lo:
        return False, {"fail": "empty_window"}
    bull_leader = _topshort_leader_idx_max_range(rows, win_lo, win_hi, bull_only=True)
    detail["win_lo"], detail["win_hi"] = win_lo, win_hi
    detail["bull_leader"] = bull_leader

    try_ci = [bull_leader] if bull_leader is not None else []

    for ci in try_ci:
        b = rows[ci]
        o, h, l, c = _f(b, "open_price"), _f(b, "high_price"), _f(b, "low_price"), _f(b, "close_price")
        v = _f(b, "volume")
        if o <= 0 or h <= 0:
            continue
        hl = h - l
        if hl <= 0:
            continue
        rng = hl / o
        body = (c - o) / o
        upper = h - max(o, c)
        upper_ratio = upper / hl
        bull_climax = (
            bull_leader is not None
            and ci == bull_leader
            and c > o
            and body >= TOPCLI_MIN_BODY_VS_O
            and rng >= TOPCLI_MIN_RANGE_FULL_PCT
            and (c - o) / hl >= TOPCLI_MIN_BODY_OF_RANGE
        )
        if not bull_climax:
            continue

        lo_i = max(0, ci - TOPCLI_VOL_LOOKBACK)
        past = rows[lo_i:ci]
        # 过滤零量（数据缺口），要求至少 10 根有效基准量，避免 ci=0 时除零
        vols = [_f(x, "volume") for x in past if _f(x, "volume") > 0]
        if len(vols) < 10:
            detail["fail"] = "vol_past_short"
            detail["ci"] = ci
            continue
        avg_v = sum(vols) / len(vols)
        if avg_v <= 0 or v < TOPCLI_VOL_MULT * avg_v:
            detail["fail"] = "volume_ratio"
            detail["ci"] = ci
            detail["vol_ratio"] = v / avg_v if avg_v else 0
            continue

        climax_open = int(b["open_time"])
        bar_close_ms = climax_open + 3600000
        if now_ms - bar_close_ms > TOPCLI_SIGNAL_AGE_MS:
            detail["fail"] = "signal_stale"
            continue
        if now_ms - climax_open > TOPCLI_MAX_OPEN_AGE_MS:
            detail["fail"] = "open_too_old"
            continue

        if last_c >= c:
            detail["fail"] = "last_c_not_weak_bull"
            detail["last_c"], detail["climax_c"] = last_c, c
            continue
        if last_c >= h * (1.0 - TOPCLI_PULLBACK_FR):
            detail["fail"] = "last_c_bull_pull"
            continue

        peak = h
        if price >= peak * (1.0 - TOPCLI_PULLBACK_FR):
            detail["fail"] = "price_pullback"
            detail["price"], detail["need_below"] = price, peak * (1.0 - TOPCLI_PULLBACK_FR)
            continue
        dd = (peak - price) / peak if peak else 0.0
        if dd > TOPCLI_MAX_DD_FR:
            detail["fail"] = "drawdown_too_deep"
            continue
        if price <= l * 0.999:
            detail["fail"] = "below_climax_low"
            continue

        mode = "bull"
        detail.update(
            {
                "ok": True,
                "ci": ci,
                "mode": mode,
                "climax_open": climax_open,
                "peak": peak,
                "vol_ratio": v / avg_v,
                "upper_ratio": upper_ratio,
                "body": body,
                "avg_v": avg_v,
            }
        )
        return True, detail

    return False, detail if detail.get("fail") else {"fail": "no_pattern"}


def evaluate_bottomlong_climax_signal(rows: list, now_ms: int, price: float):
    """
    纯逻辑：大阴线/长下影 + 放量 → 底部做多（topshort-climax 镜像）。
    形态 A：大阴实体 + 放量（庄家大量打压，底部蓄力）；
    形态 B：长下影 + 放量（打压后强力反弹，庄家吸筹）。
    返回 (True, detail_dict) 或 (False, detail_dict)。
    """
    detail: dict = {}
    n = len(rows)
    wait = BOTLONG_POST_LEADER_WAIT_BARS
    if n < BOTLONG_VOL_LOOKBACK + wait + 3:
        return False, {"fail": "not_enough_rows", "n": n}

    def _f(b, k):
        v = b.get(k)
        try:
            return float(v) if v is not None else 0.0
        except (TypeError, ValueError):
            return 0.0

    last = rows[-1]
    last_c = _f(last, "close_price")

    win_hi = n - 1 - wait
    if win_hi < 0:
        return False, {"fail": "win_hi"}
    win_lo = max(0, win_hi - (BOTLONG_LEADER_LOOKBACK - 1))
    if win_hi < win_lo:
        return False, {"fail": "empty_window"}
    bear_leader  = _bottomlong_leader_idx_max_range(rows, win_lo, win_hi, bear_only=True)
    range_leader = _bottomlong_leader_idx_max_range(rows, win_lo, win_hi, bear_only=False)
    detail["win_lo"], detail["win_hi"] = win_lo, win_hi
    detail["bear_leader"], detail["range_leader"] = bear_leader, range_leader

    try_ci = []
    if bear_leader is not None:
        try_ci.append(bear_leader)
    if range_leader is not None and range_leader not in try_ci:
        try_ci.append(range_leader)

    for ci in try_ci:
        b = rows[ci]
        o, h, l, c = _f(b, "open_price"), _f(b, "high_price"), _f(b, "low_price"), _f(b, "close_price")
        v = _f(b, "volume")
        if o <= 0 or h <= 0:
            continue
        hl = h - l
        if hl <= 0:
            continue
        rng = hl / o
        body = (o - c) / o                         # 阴线实体比（阴线时为正）
        lower_wick = min(o, c) - l                 # 下影长度
        lower_ratio = lower_wick / hl if hl else 0

        bear_climax = (
            bear_leader is not None
            and ci == bear_leader
            and c < o                              # 阴线
            and body >= BOTLONG_MIN_BODY_VS_O
            and rng >= BOTLONG_MIN_RANGE_FULL_PCT
            and (o - c) / hl >= BOTLONG_MIN_BODY_OF_RANGE
        )
        lower_climax = (
            range_leader is not None
            and ci == range_leader
            and rng >= BOTLONG_MIN_RANGE_FULL_PCT
            and lower_ratio >= BOTLONG_MIN_LOWER_WICK_OF_RANGE
            and (o - l) / o >= BOTLONG_MIN_DROP_TO_LOW_VS_O
        )
        if not bear_climax and not lower_climax:
            continue

        lo_i = max(0, ci - BOTLONG_VOL_LOOKBACK)
        past = rows[lo_i:ci]
        vols = [_f(x, "volume") for x in past if _f(x, "volume") > 0]
        if len(vols) < 10:
            detail["fail"] = "vol_past_short"
            detail["ci"] = ci
            continue
        avg_v = sum(vols) / len(vols)
        if avg_v <= 0 or v < BOTLONG_VOL_MULT * avg_v:
            detail["fail"] = "volume_ratio"
            detail["ci"] = ci
            detail["vol_ratio"] = v / avg_v if avg_v else 0
            continue

        climax_open = int(b["open_time"])
        bar_close_ms = climax_open + 3600000
        if now_ms - bar_close_ms > BOTLONG_SIGNAL_AGE_MS:
            detail["fail"] = "signal_stale"
            continue
        if now_ms - climax_open > BOTLONG_MAX_OPEN_AGE_MS:
            detail["fail"] = "open_too_old"
            continue

        if bear_climax:
            # 最后一根须已收复阴线收盘价：确认反弹
            if last_c <= c:
                detail["fail"] = "last_c_not_bounce_bear"
                detail["last_c"], detail["climax_c"] = last_c, c
                continue
            if last_c <= l * (1.0 + BOTLONG_PULLBACK_FR):
                detail["fail"] = "last_c_bear_pull"
                continue
        else:  # lower_climax
            # 最后一根须已收复下影实体下沿
            if last_c <= min(o, c):
                detail["fail"] = "last_c_lower"
                continue
            if last_c <= l * (1.0 + BOTLONG_PULLBACK_FR):
                detail["fail"] = "last_c_lower_pull"
                continue

        trough = l
        if price <= trough * (1.0 + BOTLONG_PULLBACK_FR):
            detail["fail"] = "price_pullback"
            detail["price"], detail["need_above"] = price, trough * (1.0 + BOTLONG_PULLBACK_FR)
            continue
        bounce = (price - trough) / trough if trough else 0.0
        if bounce > BOTLONG_MAX_DD_FR:
            detail["fail"] = "bounce_too_far"
            continue
        if price >= h * 1.001:
            detail["fail"] = "above_climax_high"
            continue

        mode = "bear" if bear_climax else "lower"
        detail.update({
            "ok": True,
            "ci": ci,
            "mode": mode,
            "climax_open": climax_open,
            "trough": trough,
            "vol_ratio": v / avg_v,
            "lower_ratio": lower_ratio,
            "body": body,
            "avg_v": avg_v,
        })
        return True, detail

    return False, detail if detail.get("fail") else {"fail": "no_pattern"}


def _topshort_try_climax_volume(cur, conn, sym, now_ms):
    """
    1H 巨量后见顶走弱 → 顶空 SHORT。命中则下单并写 state，返回 True。
    形态 A：阳线 + 大阳实体 + 放量；形态 B：长上影 + 放量（冲高回落，庄家砸盘）。
    共性：(high-low)/open >= TOPCLI_MIN_RANGE_FULL_PCT（默认 4.5%）；
    量 >= 前 VOL_LOOKBACK 均量 * VOL_MULT；巨 K 收盘后 SIGNAL_AGE_MS 内；
    现价从高点回撤 >= PULLBACK；最后一根已完成 1H 体现弱势。
    大阳/上影候选 K 须在 LEADER_LOOKBACK 窗口内为对应意义的「振幅最大」一根，
    且须在最新已收盘 1H 之前至少再隔 POST_LEADER_WAIT_BARS 根 1H（默认 2），
    才确认其后未出现更大阳/更大振幅 K，再允许结合走弱与现价开空。
    """
    if not CLIMAX_SIGNALS_ENABLED:
        return False
    cur.execute(
        """
        SELECT open_time, open_price, high_price, low_price, close_price, volume
        FROM kline_data
        WHERE timeframe='1h' AND symbol=%s
          AND open_time + 3600000 < %s
        ORDER BY open_time DESC
        LIMIT %s
        """,
        (sym, now_ms, TOPCLI_LOOKBACK_BARS),
    )
    rows = list(reversed(cur.fetchall()))
    try:
        price = get_price(sym)
    except Exception:
        return False
    ok, det = evaluate_topshort_climax_signal(rows, now_ms, price)
    if not ok:
        return False

    def _f(b, k):
        v = b.get(k)
        try:
            return float(v) if v is not None else 0.0
        except (TypeError, ValueError):
            return 0.0

    ci = det["ci"]
    b = rows[ci]
    o, h, l, c = _f(b, "open_price"), _f(b, "high_price"), _f(b, "low_price"), _f(b, "close_price")
    v = _f(b, "volume")
    body = det["body"]
    climax_open = det["climax_open"]
    upper_ratio = det["upper_ratio"]
    peak = det["peak"]
    dd = (peak - price) / peak if peak else 0.0

    existing = get_or_create(conn, "live", sym, "topshort", {})
    # 同一根 climax K 仅允许开仓一次（含平仓冷却期后），防止对同一顶部重复做空
    if existing.get("entry_ts") == climax_open:
        return False

    # 24h 已跌过多不再开空 (2026-04-25)
    cur.execute("SELECT change_24h FROM price_stats_24h WHERE symbol=%s", (sym,))
    _r = cur.fetchone()
    if _r and _r.get('change_24h') is not None:
        _ch24 = float(_r['change_24h'])
        if _ch24 < TOP_MIN_24H_CHANGE_PCT:
            log.info("TOPSHORT-CLIMAX 跳过 %-18s: 24h=%.1f%% < %.0f%%, 已跌过多不再做空",
                     sym, _ch24, TOP_MIN_24H_CHANGE_PCT)
            return False

    # 入场位置守卫 (2026-04-24)
    ok_pos, reason = _check_entry_position(cur, sym, 'SHORT', price, tag='topshort-climax')
    if not ok_pos:
        log.info("TOPSHORT-CLIMAX 跳过 %-18s: %s", sym, reason)
        return False

    h24, l24 = _get_24h_stats(cur, sym)
    h4,  l4  = _get_4h_stats(cur, sym)
    lp = _calc_limit_price("SHORT", price, h24, l24, pct=LIVE_LIMIT_OFFSET_PCT,
                            high_4h=h4, low_4h=l4)
    tag = "topshort-climax"
    pid, oid, pending = open_order(
        sym,
        "SHORT",
        price,
        HARD_TP_PCT,
        TOP_SL_PCT,
        TOP_HOLD_H * 60,
        tag,
        lp,
    )
    if not pid and not oid:
        return False
    mode = "巨量阳"
    log.info(
        "TOPSHORT 入场(%s) %-18s @ %.5f (限价%.5f) 顶=%.5f 回撤=%.1f%% 量比~%.1fx 上影占比=%.0f%%  pid=%s oid=%s",
        mode,
        sym,
        price,
        lp,
        peak,
        dd * 100,
        det["vol_ratio"],
        upper_ratio * 100,
        pid,
        oid,
    )
    update_state(
        conn,
        "live",
        sym,
        "topshort",
        state="SHORT",
        pid=pid,
        order_id=oid,
        entry_p=lp if pending else price,
        peak_pnl_pct=0.0,
        peak=peak,
        pump_pct=body,
        entry_ts=climax_open,
    )
    return True


def fmt(t):
    return datetime.datetime.fromtimestamp(t / 1000).strftime('%m-%d %H:%M')

def now_s():
    return time.time()

# ── A. 追击策略 ──────────────────────────────────────────────────
def chase_tick(conn, sym):
    cs = get_or_create(conn, 'live', sym, 'chase', {
        'state': 'IDLE', 'pid': None, 'order_id': None, 'entry_p': 0.0,
        'peak_pnl_pct': 0.0, 'entry_time': 0, 'done_time': 0,
    })
    s = cs.get('state') or 'IDLE'

    if s == 'DONE':
        anchor = ensure_cooldown_anchor_epoch(conn, 'live', sym, 'chase', cs, now_s())
        if now_s() - anchor > CHASE_COOLDOWN:
            update_state(conn, 'live', sym, 'chase', state='IDLE')
            s = 'IDLE'
        else:
            return

    ok, cs = _check_pending_db(conn, sym, 'chase')
    if not ok:
        return
    s = cs.get('state') or 'IDLE'

    if s in ('LONG', 'SHORT') and cs.get('pid'):
        status, pnl, notes = get_pos_status(cs['pid'])
        if status is None:
            return
        if status == 'open':
            _trail_tp_check(conn, 'live', 'chase', sym, cs['pid'],
                            s, cs.get('entry_p', 0), cs.get('peak_pnl_pct', 0),
                            cs.get('entry_time', 0))
            return

        pnl = pnl or 0
        if notes and '手动' in str(notes):
            log.info("CHASE 手动平仓 -> DONE %-18s  pnl=%+.2f  不重开", sym, pnl)
            update_state(conn, 'live', sym, 'chase', state='DONE', pid=None, done_time=now_s())
            return

        win = pnl > 0
        label = "TP" if win else "SL"
        log.info("CHASE %s %s -> DONE %-18s  pnl=%+.2f  冷却%dh",
                 s, label, sym, pnl, CHASE_COOLDOWN // 3600)
        update_state(conn, 'live', sym, 'chase',
                     state='DONE', pid=None, order_id=None, done_time=now_s())
        return

    if s != 'IDLE':
        return

    # 顶空仓位冲突检查：同标的已有 topshort 持仓/挂单时，不追多，避免双向对冲
    ts_row = get_or_create(conn, 'live', sym, 'topshort', {'state': 'IDLE'})
    ts_state = (ts_row.get('state') or 'IDLE').upper()
    if ts_state not in ('IDLE', 'DONE'):
        log.info("CHASE 跳过 %-18s: 顶空 state=%s，避免双向对冲", sym, ts_state)
        return

    now_ms = int(now_s() * 1000)
    BAR_MS = 5 * 60 * 1000
    cur = conn.cursor()

    # 24h 趋势过滤：日线大跌超阈值时不追涨，避免抓熊市反弹飞刀
    cur.execute("SELECT change_24h FROM price_stats_24h WHERE symbol=%s", (sym,))
    r = cur.fetchone()
    if r and r['change_24h'] is not None:
        ch24 = float(r['change_24h'])
        if ch24 < CHASE_MIN_24H_CHANGE_PCT:
            log.info("CHASE 跳过 %-18s: 24h=%.1f%% < %.0f%%，熊市反弹不追",
                     sym, ch24, CHASE_MIN_24H_CHANGE_PCT)
            cur.close()
            return
        if ch24 > CHASE_MAX_24H_CHANGE_PCT:
            log.info("CHASE 跳过 %-18s: 24h=%.1f%% > %.0f%%，已涨过多不追顶",
                     sym, ch24, CHASE_MAX_24H_CHANGE_PCT)
            cur.close()
            return

    bars = get_5m_bars(cur, sym, 80)
    if len(bars) < CHASE_PUMP_BARS + 2:
        cur.close()
        return

    completed = [b for b in bars if b['open_time'] + BAR_MS < now_ms]
    if not completed:
        cur.close()
        return

    i = len(completed) - 1
    if i < CHASE_PUMP_BARS:
        cur.close()
        return
    c  = [float(b['close_price']) for b in completed]
    ts = [b['open_time'] for b in completed]

    wo = float(completed[max(0, i - CHASE_PUMP_BARS)]['open_price'])
    pump = (c[i] - wo) / wo
    if pump < CHASE_PUMP_PCT:
        cur.close()
        return

    # 耗竭过滤：若当前收盘已从窗口内最高点回撤超过阈值，视为顶部反转，不追
    window_bars = completed[max(0, i - CHASE_PUMP_BARS):]
    recent_high = max(float(b['high_price']) for b in window_bars)
    dd_from_peak = (recent_high - c[i]) / recent_high if recent_high > 0 else 0
    if dd_from_peak > CHASE_EXHAUST_MAX_DD:
        log.info("CHASE 跳过 %-18s: 高点回撤 %.1f%% > %.0f%%，疑似顶部耗竭",
                 sym, dd_from_peak * 100, CHASE_EXHAUST_MAX_DD * 100)
        cur.close()
        return

    # 急拉验证：窗口内须有至少一根 5m bar 单 bar 涨幅 >= 阈值，排除慢速爬升
    leader_gain = max(
        (float(b['close_price']) - float(b['open_price'])) / float(b['open_price'])
        for b in window_bars
    )
    if leader_gain < CHASE_LEADER_BAR_MIN_PCT:
        log.info("CHASE 跳过 %-18s: 最大单 bar 涨幅 %.1f%% < %.0f%%，慢速爬升不追",
                 sym, leader_gain * 100, CHASE_LEADER_BAR_MIN_PCT * 100)
        cur.close()
        return

    bar_close_ms = ts[i] + BAR_MS
    bar_age_s = (now_ms - bar_close_ms) / 1000
    if bar_age_s > 300:
        cur.close()
        return

    price = get_price(sym)
    # 入场位置守卫: 追高 / 破顶 / 破底 过滤 (2026-04-24)
    ok_pos, reason = _check_entry_position(cur, sym, 'LONG', price, tag='chase')
    if not ok_pos:
        log.info("CHASE 跳过 %-18s: %s", sym, reason)
        cur.close()
        return
    h24, l24 = _get_24h_stats(cur, sym)
    h4,  l4  = _get_4h_stats(cur, sym)
    cur.close()
    lp = _calc_limit_price("LONG", price, h24, l24, pct=LIVE_LIMIT_OFFSET_PCT,
                            high_4h=h4, low_4h=l4)
    pid, oid, pending = open_order(sym, "LONG", price, HARD_TP_PCT, CHASE_SL_PCT,
                                   CHASE_MAX_HOLD, "chase-entry", lp)
    if not pid and not oid:
        return
    log.info("CHASE 入场 LONG  %-18s @ %.5f (限价%.5f)  泵%.1f%%  pid=%s oid=%s",
             sym, price, lp, pump*100, pid, oid)
    update_state(conn, 'live', sym, 'chase',
                 state='LONG', pid=pid, order_id=oid,
                 entry_p=lp if pending else price,
                 peak_pnl_pct=0.0, entry_time=now_s())

# ── B. 顶部做空 ──────────────────────────────────────────────────
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
                    update_state(conn, 'live', sym, 'topshort',
                                 pid=int(pos_id), order_id=None)
                    log.info("TOPSHORT 限价单成交 %-18s  pid=%d", sym, int(pos_id))
                    pos = {**pos, 'pid': int(pos_id), 'order_id': None}
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

    # 扫描新信号
    open_syms = {r['symbol'] for r in list_active(conn, 'live', 'topshort')}

    cur = conn.cursor()
    cur.execute(
        """SELECT COUNT(*) AS cnt FROM futures_orders
           WHERE account_id=%s AND status='PENDING' AND order_type='LIMIT'
             AND order_source LIKE 'topshort-climax%%'""",
        (ACCOUNT_ID,),
    )
    _climax_pending_cnt = (cur.fetchone() or {}).get("cnt", 0)

    for sym in active_syms:
        if sym in open_syms:
            continue
        if _topshort_has_min_history_for_climax(cur, sym, now_ms):
            if _climax_pending_cnt >= TOPCLI_MAX_PENDING:
                pass  # 已达上限，本轮跳过
            elif _topshort_try_climax_volume(cur, conn, sym, now_ms):
                _climax_pending_cnt += 1
                open_syms.add(sym)
                continue
        if not _topshort_has_min_listed_history(cur, sym, now_ms):
            continue

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
            continue

        h  = [float(b['high_price'])  for b in bars]
        lo = [float(b['low_price'])   for b in bars]
        c  = [float(b['close_price']) for b in bars]
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
                break

            # 检查是否已有相同 entry_ts 的信号（避免重复入场）
            existing = get_or_create(conn, 'live', sym, 'topshort', {})
            if existing.get('entry_ts') == entry_ts and existing.get('state') != 'IDLE':
                break

            price = get_price(sym)
            if price <= lo_win:
                log.info("TOPSHORT 跳过  %-18s  现价%.5f <= 启动价%.5f", sym, price, lo_win)
                break
            dd = (peak - price) / peak
            if dd > 0.50:
                log.info("TOPSHORT 跳过  %-18s  从峰值已跌%.0f%%, 回落过深", sym, dd * 100)
                break
            # 24h 已跌过多不再开空 (2026-04-25)
            cur.execute("SELECT change_24h FROM price_stats_24h WHERE symbol=%s", (sym,))
            _r = cur.fetchone()
            if _r and _r.get('change_24h') is not None:
                _ch24 = float(_r['change_24h'])
                if _ch24 < TOP_MIN_24H_CHANGE_PCT:
                    log.info("TOPSHORT 跳过 %-18s: 24h=%.1f%% < %.0f%%, 已跌过多不再做空",
                             sym, _ch24, TOP_MIN_24H_CHANGE_PCT)
                    break

            # 入场位置守卫 (2026-04-24)
            ok_pos, reason = _check_entry_position(cur, sym, 'SHORT', price, tag='topshort')
            if not ok_pos:
                log.info("TOPSHORT 跳过 %-18s: %s", sym, reason)
                break
            h24, l24 = _get_24h_stats(cur, sym)
            h4,  l4  = _get_4h_stats(cur, sym)
            lp = _calc_limit_price("SHORT", price, h24, l24, pct=LIVE_LIMIT_OFFSET_PCT,
                                    high_4h=h4, low_4h=l4)
            pid, oid, pending = open_order(sym, "SHORT", price, HARD_TP_PCT, TOP_SL_PCT,
                                           TOP_HOLD_H * 60, "topshort", lp)
            if not pid and not oid:
                break
            log.info("TOPSHORT 入场  %-18s @ %.5f (限价%.5f)  峰=%.5f(泵%.0f%%)  回落%.1f%%  pid=%s oid=%s",
                     sym, price, lp, peak, pump*100, dd*100, pid, oid)
            update_state(conn, 'live', sym, 'topshort',
                         state='SHORT', pid=pid, order_id=oid,
                         entry_p=lp if pending else price,
                         peak_pnl_pct=0.0, peak=peak, pump_pct=pump, entry_ts=entry_ts)
            open_syms.add(sym)
            break
    cur.close()


def _bottomlong_try_climax_volume(cur, conn, sym, now_ms):
    """
    1H 巨量后底部走强 → 做多 LONG。命中则下单并写 state，返回 True。
    形态 A：阴线 + 大阴实体 + 放量；形态 B：长下影 + 放量（打压后反弹）。
    """
    if not CLIMAX_SIGNALS_ENABLED:
        return False
    cur.execute(
        """
        SELECT open_time, open_price, high_price, low_price, close_price, volume
        FROM kline_data
        WHERE timeframe='1h' AND symbol=%s
          AND open_time + 3600000 < %s
        ORDER BY open_time DESC
        LIMIT %s
        """,
        (sym, now_ms, BOTLONG_LOOKBACK_BARS),
    )
    rows = list(reversed(cur.fetchall()))
    try:
        price = get_price(sym)
    except Exception:
        return False
    ok, det = evaluate_bottomlong_climax_signal(rows, now_ms, price)
    if not ok:
        return False

    def _f(b, k):
        v = b.get(k)
        try:
            return float(v) if v is not None else 0.0
        except (TypeError, ValueError):
            return 0.0

    ci = det["ci"]
    bear_climax = det["mode"] == "bear"
    b = rows[ci]
    l = _f(b, "low_price")
    body = det["body"]
    climax_open = det["climax_open"]
    lower_ratio = det["lower_ratio"]
    trough = det["trough"]
    bounce = (price - trough) / trough if trough else 0.0

    existing = get_or_create(conn, "live", sym, "bottomlong", {})
    if existing.get("entry_ts") == climax_open:
        return False

    # 24h 已涨过多不再开多 (2026-04-25, 修 API3 +49% 还做多的漏洞)
    cur.execute("SELECT change_24h FROM price_stats_24h WHERE symbol=%s", (sym,))
    _r = cur.fetchone()
    if _r and _r.get('change_24h') is not None:
        _ch24 = float(_r['change_24h'])
        if _ch24 > BOTLONG_MAX_24H_CHANGE_PCT:
            log.info("BOTTOMLONG 跳过 %-18s: 24h=%.1f%% > %.0f%%, 已涨过多不是底",
                     sym, _ch24, BOTLONG_MAX_24H_CHANGE_PCT)
            return False

    # 入场位置守卫 (2026-04-24)
    ok_pos, reason = _check_entry_position(cur, sym, 'LONG', price,
                                            tag=('bottomlong-climax' if bear_climax
                                                 else 'bottomlong-climax-wick'))
    if not ok_pos:
        log.info("BOTTOMLONG 跳过 %-18s: %s", sym, reason)
        return False

    h24, l24 = _get_24h_stats(cur, sym)
    h4,  l4  = _get_4h_stats(cur, sym)
    lp = _calc_limit_price("LONG", price, h24, l24, pct=LIVE_LIMIT_OFFSET_PCT,
                            high_4h=h4, low_4h=l4)
    tag = "bottomlong-climax" if bear_climax else "bottomlong-climax-wick"
    pid, oid, pending = open_order(
        sym,
        "LONG",
        price,
        HARD_TP_PCT,
        BOTLONG_SL_PCT,
        BOTLONG_HOLD_H * 60,
        tag,
        lp,
    )
    if not pid and not oid:
        return False
    mode = "巨量阴" if bear_climax else "下影线"
    log.info(
        "BOTLONG 入场(%s) %-18s @ %.5f (限价%.5f) 底=%.5f 反弹=%.1f%% 量比~%.1fx 下影占比=%.0f%%  pid=%s oid=%s",
        mode,
        sym,
        price,
        lp,
        trough,
        bounce * 100,
        det["vol_ratio"],
        lower_ratio * 100,
        pid,
        oid,
    )
    update_state(
        conn,
        "live",
        sym,
        "bottomlong",
        state="LONG",
        pid=pid,
        order_id=oid,
        entry_p=lp if pending else price,
        peak_pnl_pct=0.0,
        peak=trough,
        pump_pct=body,
        entry_ts=climax_open,
    )
    return True


# ── D. 底部做多（bottomlong-climax）────────────────────────────────
def bottomlong_tick(conn, active_syms):
    now_ms = int(now_s() * 1000)
    nowt = now_s()

    # DONE 冷却到期 → IDLE
    for row in list_all_stype(conn, 'live', 'bottomlong'):
        if row.get('state') != 'DONE':
            continue
        sym = row['symbol']
        anchor = ensure_cooldown_anchor_epoch(conn, 'live', sym, 'bottomlong', row, nowt)
        if nowt - anchor > BOTLONG_COOLDOWN:
            update_state(conn, 'live', sym, 'bottomlong', state='IDLE', pid=None, order_id=None)

    # 检查已有做多仓位
    active_rows = list_active(conn, 'live', 'bottomlong')
    for pos in active_rows:
        sym = pos['symbol']
        if pos.get('state') == 'DONE':
            continue
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
                    update_state(conn, 'live', sym, 'bottomlong',
                                 pid=int(pos_id), order_id=None)
                    log.info("BOTLONG 限价单成交 %-18s  pid=%d", sym, int(pos_id))
                    pos = {**pos, 'pid': int(pos_id), 'order_id': None}
                elif st in ('CANCELLED', 'REJECTED'):
                    log.info("BOTLONG 限价单取消 %-18s  oid=%s -> DONE 冷却", sym, pos.get('order_id'))
                    update_state(
                        conn, 'live', sym, 'bottomlong',
                        state='DONE', pid=None, order_id=None,
                        done_time=nowt, last_reason='cancel',
                    )
                    continue
            if not pos.get('pid'):
                continue
        if not pos.get('pid'):
            log.warning("BOTLONG 异常无 pid %-18s -> DONE 冷却", sym)
            update_state(
                conn, 'live', sym, 'bottomlong',
                state='DONE', pid=None, order_id=None,
                done_time=nowt, last_reason='orphan',
            )
            continue
        status, pnl, notes = get_pos_status(pos['pid'])
        if status is None:
            continue
        if status == 'open':
            _trail_tp_check(conn, 'live', 'bottomlong', sym, pos['pid'],
                            'LONG', pos.get('entry_p', 0), pos.get('peak_pnl_pct', 0),
                            pos.get('entry_time', 0))
            continue
        pnl_pct = (pnl or 0) / MARGIN * 100
        reason = "手动" if (notes and '手动' in str(notes)) else status
        lr = 'manual' if (notes and '手动' in str(notes)) else ('TP' if (pnl or 0) > 0 else 'SL')
        log.info(
            "BOTLONG 平仓  %-18s  pid=%d  pnl=%+.1f%%  reason=%s  冷却%dh",
            sym, pos['pid'], pnl_pct, reason, BOTLONG_COOLDOWN // 3600,
        )
        update_state(
            conn, 'live', sym, 'bottomlong',
            state='DONE', pid=None, order_id=None,
            done_time=nowt, last_reason=lr,
        )

    # 扫描新信号
    open_syms = {r['symbol'] for r in list_active(conn, 'live', 'bottomlong')}

    cur = conn.cursor()
    cur.execute(
        """SELECT COUNT(*) AS cnt FROM futures_orders
           WHERE account_id=%s AND status='PENDING' AND order_type='LIMIT'
             AND order_source LIKE 'bottomlong-climax%%'""",
        (ACCOUNT_ID,),
    )
    _botlong_pending_cnt = (cur.fetchone() or {}).get("cnt", 0)

    for sym in active_syms:
        if sym in open_syms:
            continue
        if not _topshort_has_min_history_for_climax(cur, sym, now_ms):
            continue
        if _botlong_pending_cnt >= BOTLONG_MAX_PENDING:
            break
        if _bottomlong_try_climax_volume(cur, conn, sym, now_ms):
            _botlong_pending_cnt += 1
            open_syms.add(sym)
    cur.close()


# ── C. 追跌策略 ──────────────────────────────────────────────────
def dump_tick(conn, sym):
    """追跌: 检测4h跌幅>=DUMP_PCT直接入场做空, 镜像追多逻辑."""
    # chase 已有持仓时跳过, 避免同一标的双向冲突
    chase_row = get_or_create(conn, 'live', sym, 'chase', {})
    if chase_row.get('state') in ('LONG', 'SHORT'):
        return

    ds = get_or_create(conn, 'live', sym, 'dump', {
        'state': 'IDLE', 'pid': None, 'order_id': None, 'entry_p': 0.0,
        'peak_pnl_pct': 0.0, 'entry_time': 0, 'done_time': 0,
    })
    s = ds.get('state') or 'IDLE'

    if s == 'DONE':
        anchor = ensure_cooldown_anchor_epoch(conn, 'live', sym, 'dump', ds, now_s())
        if now_s() - anchor > DUMP_COOLDOWN:
            update_state(conn, 'live', sym, 'dump', state='IDLE')
            s = 'IDLE'
        else:
            return

    ok, ds = _check_pending_db(conn, sym, 'dump')
    if not ok:
        return
    s = ds.get('state') or 'IDLE'

    if s in ('SHORT', 'LONG') and ds.get('pid'):
        status, pnl, notes = get_pos_status(ds['pid'])
        if status is None:
            return
        if status == 'open':
            _trail_tp_check(conn, 'live', 'dump', sym, ds['pid'],
                            s, ds.get('entry_p', 0), ds.get('peak_pnl_pct', 0),
                            ds.get('entry_time', 0))
            return

        pnl = pnl or 0
        if notes and '手动' in str(notes):
            log.info("DUMP  手动平仓 -> DONE %-18s  pnl=%+.2f  不重开", sym, pnl)
            update_state(conn, 'live', sym, 'dump', state='DONE', pid=None, done_time=now_s())
            return

        label = "TP" if pnl > 0 else "SL"
        log.info("DUMP %s %s -> DONE %-18s  pnl=%+.2f  冷却%dh",
                 s, label, sym, pnl, DUMP_COOLDOWN // 3600)
        update_state(conn, 'live', sym, 'dump',
                     state='DONE', pid=None, order_id=None, done_time=now_s())
        return

    if s != 'IDLE':
        return

    now_ms = int(now_s() * 1000)
    BAR_MS = 5 * 60 * 1000
    cur = conn.cursor()
    bars = get_5m_bars(cur, sym, 80)
    if len(bars) < DUMP_BARS + 2:
        cur.close()
        return

    completed = [b for b in bars if b['open_time'] + BAR_MS < now_ms]
    if not completed:
        cur.close()
        return

    i = len(completed) - 1
    if i < DUMP_BARS:
        cur.close()
        return
    c  = [float(b['close_price']) for b in completed]
    ts = [b['open_time'] for b in completed]

    wo   = float(completed[max(0, i - DUMP_BARS)]['open_price'])
    dump = (wo - c[i]) / wo
    if dump < DUMP_PCT:
        cur.close()
        return

    lo_slice = [float(b['low_price']) for b in completed[max(0, i - DUMP_BARS):]]
    win_low  = min(lo_slice)
    bounce   = (c[i] - win_low) / win_low
    if bounce > 0.08:
        cur.close()
        return

    bar_close_ms = ts[i] + BAR_MS
    bar_age_s = (now_ms - bar_close_ms) / 1000
    if bar_age_s > 300:
        cur.close()
        return

    price = get_price(sym)
    # 24h 已跌过多不追 (避免接飞刀, 2026-04-24)
    cur.execute("SELECT change_24h FROM price_stats_24h WHERE symbol=%s", (sym,))
    _r = cur.fetchone()
    if _r and _r.get('change_24h') is not None:
        _ch24 = float(_r['change_24h'])
        if _ch24 < DUMP_MIN_24H_CHANGE_PCT:
            log.info("DUMP  跳过 %-18s: 24h=%.1f%% < %.0f%%，已跌过多不追",
                     sym, _ch24, DUMP_MIN_24H_CHANGE_PCT)
            cur.close()
            return
    # 入场位置守卫: 踩底 / 破顶 / 破底 过滤 (2026-04-24)
    ok_pos, reason = _check_entry_position(cur, sym, 'SHORT', price, tag='dump')
    if not ok_pos:
        log.info("DUMP  跳过 %-18s: %s", sym, reason)
        cur.close()
        return
    h24, l24 = _get_24h_stats(cur, sym)
    h4,  l4  = _get_4h_stats(cur, sym)
    cur.close()
    lp = _calc_limit_price("SHORT", price, h24, l24, pct=LIVE_LIMIT_OFFSET_PCT,
                            high_4h=h4, low_4h=l4)
    pid, oid, pending = open_order(sym, "SHORT", price, HARD_TP_PCT, DUMP_SL_PCT,
                                   DUMP_MAX_HOLD, "dump-entry", lp)
    if not pid and not oid:
        return
    log.info("DUMP  入场 SHORT %-18s @ %.5f (限价%.5f)  跌%.1f%%  pid=%s oid=%s",
             sym, price, lp, dump*100, pid, oid)
    update_state(conn, 'live', sym, 'dump',
                 state='SHORT', pid=pid, order_id=oid,
                 entry_p=lp if pending else price,
                 peak_pnl_pct=0.0, entry_time=now_s())



# ── 启动同步 ─────────────────────────────────────────────────────
def _sync_state(conn):
    """启动时从 API 拉取已有 strategy_live 仓位, 防止重启重复开单"""
    try:
        d = _api("GET", "/api/futures/positions?status=open")
        for p in (d.get("data") or []):
            src  = p.get("source") or ""
            if not src.startswith("strategy_live:"):
                continue
            sym  = p["symbol"]
            side = p["position_side"]

            if "dump-" in src and side == "SHORT":
                existing = get_or_create(conn, 'live', sym, 'dump', {})
                if existing.get('state') not in ('SHORT', 'LONG'):
                    update_state(conn, 'live', sym, 'dump',
                                 state='SHORT', pid=p["id"],
                                 entry_p=p["entry_price"],
                                 peak_pnl_pct=0.0, entry_time=now_s(), done_time=0)
                    log.info("同步已有追跌空仓: %s pid=%d @ %.5f", sym, p["id"], p["entry_price"])
            elif "dump-" in src and side == "LONG":
                existing = get_or_create(conn, 'live', sym, 'dump', {})
                if existing.get('state') not in ('SHORT', 'LONG'):
                    update_state(conn, 'live', sym, 'dump',
                                 state='LONG', pid=p["id"],
                                 entry_p=p["entry_price"],
                                 peak_pnl_pct=0.0, entry_time=now_s(), done_time=0)
                    log.info("同步已有追跌翻多仓: %s pid=%d @ %.5f", sym, p["id"], p["entry_price"])
            if "chase-" in src or "chase-entry" in src:
                existing = get_or_create(conn, 'live', sym, 'chase', {})
                if existing.get('state') not in ('LONG', 'SHORT'):
                    mapped = "LONG" if side == "LONG" else "SHORT"
                    update_state(conn, 'live', sym, 'chase',
                                 state=mapped, pid=p["id"],
                                 entry_p=p["entry_price"],
                                 peak_pnl_pct=0.0, entry_time=now_s(), done_time=0)
                    log.info("同步已有追击仓位: %s %s pid=%d @ %.5f",
                             sym, mapped, p["id"], p["entry_price"])
            elif "bottomlong" in src and side == "LONG":
                existing = get_or_create(conn, 'live', sym, 'bottomlong', {})
                if existing.get('state') not in ('LONG',):
                    update_state(conn, 'live', sym, 'bottomlong',
                                 state='LONG', pid=p["id"],
                                 entry_p=p["entry_price"],
                                 peak=p["entry_price"], pump_pct=0, entry_ts=0)
                    log.info("同步已有底多仓位: %s pid=%d @ %.5f", sym, p["id"], p["entry_price"])
            elif "topshort" in src and side == "SHORT":
                existing = get_or_create(conn, 'live', sym, 'topshort', {})
                if existing.get('state') not in ('SHORT',):
                    update_state(conn, 'live', sym, 'topshort',
                                 state='SHORT', pid=p["id"],
                                 entry_p=p["entry_price"],
                                 peak=p["entry_price"], pump_pct=0, entry_ts=0)
                    log.info("同步已有顶空仓位: %s pid=%d @ %.5f", sym, p["id"], p["entry_price"])
            else:
                if side == "SHORT":
                    existing = get_or_create(conn, 'live', sym, 'topshort', {})
                    if existing.get('state') not in ('SHORT',):
                        update_state(conn, 'live', sym, 'topshort',
                                     state='SHORT', pid=p["id"],
                                     entry_p=p["entry_price"],
                                     peak=p["entry_price"], pump_pct=0, entry_ts=0)
                        log.info("同步未知空仓(兜底): %s pid=%d src=%s", sym, p["id"], src)
    except Exception as e:
        log.warning("同步持仓失败: %s", e)

# ── 主循环 ───────────────────────────────────────────────────────
def main():
    _load_live_config()
    log.info("=" * 56)
    log.info("Strategy Live Runner  实盘下单模式")
    log.info(
        "A: 追多(2h涨>=12%%, 持仓4h)  B: 顶空(80%%泵+6h无新高, >=%d天1h数据)  C: 追跌(4h跌>=10%%, 持仓12h)",
        TOPSHORT_MIN_HISTORY_DAYS,
    )
    log.info("账户=%d  杠杆=%dx  每笔保证金=%.0f USDT", ACCOUNT_ID, LEVERAGE, MARGIN)
    log.info("=" * 56)

    # 建表 + 同步已有持仓
    init_conn = get_db()
    ensure_table(init_conn)
    _sync_state(init_conn)
    init_conn.close()

    poll_count = 0

    while True:
        try:
            conn = get_db()
            cur  = conn.cursor()
            poll_count += 1

            try:
                _fill_pending_orders(conn)
            except Exception as e:
                log.warning("_fill_pending_orders 异常: %s", e)

            try:
                _close_overdue(conn)
            except Exception as e:
                log.warning("_close_overdue 异常: %s", e)

            active_syms = get_active_symbols(cur)

            for sym in active_syms:
                try:
                    chase_tick(conn, sym)
                except Exception as e:
                    log.warning("chase_tick %s error: %s", sym, e)
                try:
                    dump_tick(conn, sym)
                except Exception as e:
                    log.warning("dump_tick %s error: %s", sym, e)

            if poll_count % TOPSHORT_EVERY == 1:
                try:
                    topshort_tick(conn, active_syms)
                except Exception as e:
                    log.warning("topshort_tick error: %s", e)
                try:
                    bottomlong_tick(conn, active_syms)
                except Exception as e:
                    log.warning("bottomlong_tick error: %s", e)

            # 汇总当前持仓
            chase_active   = list_active(conn, 'live', 'chase')
            dump_active    = list_active(conn, 'live', 'dump')
            top_active     = list_active(conn, 'live', 'topshort')
            botlong_active = list_active(conn, 'live', 'bottomlong')
            if chase_active or dump_active or top_active or botlong_active:
                summary = []
                for r in chase_active:
                    summary.append("chase:%s %s pid=%s" % (r['symbol'], r['state'], r.get('pid')))
                for r in dump_active:
                    summary.append("dump:%s %s pid=%s" % (r['symbol'], r['state'], r.get('pid')))
                for r in top_active:
                    summary.append("top:%s SHORT pid=%s" % (r['symbol'], r.get('pid')))
                for r in botlong_active:
                    summary.append("botlong:%s LONG pid=%s" % (r['symbol'], r.get('pid')))
                log.info("持仓: %s", " | ".join(summary))
            else:
                log.info("当前无持仓, 等待信号...")

            cur.close()
            conn.close()

        except Exception as e:
            import traceback
            log.error("主循环错误: %s\n%s", e, traceback.format_exc())

        time.sleep(POLL_SECS)

if __name__ == '__main__':
    main()
