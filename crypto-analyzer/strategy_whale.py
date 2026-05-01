"""
庄家对抗策略 - 独立运行, 不依赖 strategy_live
账户: account_id=4 (WhaleStrategy, 模拟盘, 10万初始)

策略 A. 跟砸盘做空 (distribution → dump):
    资金费率极端正 + 放量滞涨 + 支撑跌破 → SHORT
    止盈梯度: 8% → 12% → 16%  止损: 10%

策略 B. 跟拉盘做多 (accumulation → pump):
    资金费率极端负 + 放量滞跌 + 阻力突破 → LONG
    止盈梯度: 8% → 12% → 16%  止损: 10%

信号打分 (score >= ENTRY_SCORE_MIN 才开仓):
    资金费率极端   +1~+3
    多空比极端     +1~+2
    OI趋势偏差    +1~+2
    放量滞涨/滞跌 +2~+3  (主要信号)
    隐性大单压力   +1
    入场触发器     必须 (支撑跌破 or 大阴线 / 阻力突破 or 大阳线)
"""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import os, time, logging, datetime
import pymysql, requests as req
from dotenv import load_dotenv
load_dotenv()

from strategy_state_db import (
    ensure_table,
    get_or_create,
    update_state,
    list_active,
    ensure_cooldown_anchor_epoch,
)

# ── 账户与 API ────────────────────────────────────────────────────────
API_BASE    = "http://localhost:9021"
ACCOUNT_ID  = 2
LEVERAGE    = 5
MARGIN      = 500.0   # USDT per trade

# ── 信号阈值 ──────────────────────────────────────────────────────────
ENTRY_SCORE_MIN   = 5        # 最低入场分数

# 资金费率极端阈值
FR_EXTREME_HIGH   =  0.0005  # 0.05%  极端多头 → +3
FR_HIGH           =  0.0003  # 0.03%  偏多      → +2
FR_MILD_HIGH      =  0.0001  # 0.01%  温和多头  → +1
FR_EXTREME_LOW    = -0.0005  # 极端空头 → +3
FR_LOW            = -0.0003
FR_MILD_LOW       = -0.0001

# 多空比阈值
LS_LONG_EXTREME   = 0.65     # 多头占 65%+ → +2
LS_LONG_HIGH      = 0.60     # 60%+         → +1
LS_SHORT_EXTREME  = 0.65     # 空头占 65%+ → +2 (for long)
LS_SHORT_HIGH     = 0.60

# OI 变化阈值 (过去 4h)
OI_DROP_STRONG    = -0.03    # -3%+ 减少 → +2
OI_DROP_MILD      = -0.01    # -1%+ 减少 → +1
OI_RISE_STRONG    =  0.03
OI_RISE_MILD      =  0.01

# 放量阈值 (volume_ratio = 近3根1h均量 / 20根1h均量)
VOL_RATIO_STRONG  = 2.5      # 2.5x → +3
VOL_RATIO_MILD    = 1.8      # 1.8x → +1
# 滞涨/滞跌: 放量期间价格变化不超过阈值
STALE_PRICE_PCT   = 0.015    # 1.5%

# 隐性大单: taker_buy_ratio 极值 (空方主导)
TAKER_SELL_THRESH = 0.42     # < 42% 买入压力 → 隐性卖压 +1
TAKER_BUY_THRESH  = 0.58     # > 58% 买入压力 → 隐性买压 +1

# 入场触发器
TRIGGER_CANDLE_PCT  = 0.025  # 单根 2.5%+ 大阴/阳线
TRIGGER_BREAKOUT    = 0.005  # 0.5% 有效突破(高低点)

# ── 仓位参数 ──────────────────────────────────────────────────────────
SL_PCT            = 0.10
HARD_TP_PCT       = 0.20  # 硬止盈
# 动态移动止盈：按 peak 分档（与 strategy_live 同）
#   peak 3%-5%  → 回落 1%
#   peak 5%-10% → 回落 2%
#   peak ≥ 10% → 回落 3%
#   peak < 3%  → 不启动 trail
TRAIL_TP_TIERS = [
    (0.10, 0.03),
    (0.05, 0.02),
    (0.03, 0.01),
]
# 早期止损 / 保本止损（与 strategy_live 同；2026-04-24 breakeven 启动 3%→1.5%）
# ENTRY_GRACE_MIN 入场保护期：前 45 分钟 early-sl/breakeven 不触发，仅硬 SL 兜底
EARLY_SL_PCT             = 0.03
BREAKEVEN_AFTER_PEAK_PCT = 0.015
BREAKEVEN_SL_PCT         = -0.005
ENTRY_GRACE_MIN          = 45


def _dynamic_trail_pullback(peak_pct: float) -> float:
    for threshold, pullback in TRAIL_TP_TIERS:
        if peak_pct >= threshold:
            return pullback
    return float('inf')
SHORT_HOLD_H  = 6    # 做空持仓 6小时
LONG_HOLD_H   = 6    # 做多持仓 6小时
COOLDOWN_S    = 6 * 3600
COOLDOWN_SL_S = 12 * 3600

# 限价单触发后的观察确认期（2026-04-24）：价格穿过挂单价时不立即成交，等 N 秒再看是否仍触发
# 避免急跌/急涨瞬穿即成交（接飞刀）。若价格在观察期内回撤到另一侧则清除观察、继续挂单。
TRIGGER_CONFIRM_S = 30
_trigger_first_seen: dict[int, float] = {}

# ── W 型双底子策略（做多、短持、不设 SL/TP）────────────────────────────
# 数据要求：至少 3.5 天 15m K 线（336 根）
# 形态识别流程（2026-04-24：从 1h 改为 15m，所有 bar 数常量不变，实际时间尺度变为 1/4）
#   1. 定位 3.5 天内最低点 B1
#   2. B1 之后反弹到颈线 C，幅度 ≥ 5%
#   3. C 之后再次探底 B2，价格在 B1 ± 5%
#   4. B2 距 C 至少 4 根（1h，过滤假探底）
#   5. B1 → B2 时间间隔 6h - 3.5 天
#   6. 当前价 > 颈线 C × 1.005（突破确认）
# 注: 下方常量名保留 _H 后缀仅为历史兼容, 数值单位现在是 "15m K 线根数"
WB_DATA_MIN_BARS       = 14 * 24     # 至少 336 根 15m K 线 = 3.5 天
WB_REBOUND_MIN_PCT     = 0.05        # 颈线反弹幅度最小 5%
WB_BOTTOM_DIFF_PCT     = 0.05        # 两底价差 ± 5%（2026-04-24 从 3% 放宽）
WB_B2_TO_NECK_MIN_H    = 4           # B2 距颈线至少 4 根（1h）
WB_TIME_GAP_MIN_H      = 24          # B1→B2 最少 24 根（6h）
WB_TIME_GAP_MAX_H      = 14 * 24     # B1→B2 最多 336 根（3.5 天）
WB_BREAK_NECK_PCT      = 0.005       # 突破颈线 +0.5% 确认（2026-04-24 从 1% 放宽）
WB_HOLD_MIN            = 1 * 24 * 60 # 持仓上限 1 天（timeout 兜底，2026-04-24 从 3 天缩短）
WB_COOLDOWN_S          = 3 * 24 * 3600  # 同品种触发后冷却 3 天
WB_MAX_OPEN_POSITIONS  = 3           # W 双底子策略全局最多同时 3 笔

# 从 system_settings 动态加载的参数（运行时覆盖上方常量）
WHALE_SL_PCT           = SL_PCT
WHALE_HARD_TP_PCT      = HARD_TP_PCT
WHALE_LIMIT_OFFSET_PCT = 0.003  # 限价单挂单偏移（0.3%）
WHALE_HOLD_H           = 6
DISABLE_SL_TP_HOLD     = False  # 总开关: 新开仓不设 SL/TP/timeout, 且跳过进程内硬TP/移动TP检查
# 2026-04-27: 总开关-禁用 5m 阴/阳收盘确认守卫(触发即成交). 走 system_settings.disable_5m_confirm.
# 默认 False, 由 _load_whale_config 启动时从 DB 加载. 设置后 LIMIT 触发即按 limit_price 成交.
DISABLE_5M_CONFIRM     = False

# ── 长持子策略 longhold (W 底/M 顶 2 周窗口, 2026-04-29 新增) ────────────
# 数据: 1h x 14 天 = 336 根 K 线
# 入场: 2 周窗口 W 底做多 / M 顶做空
# 仓位: TP 20% / SL 4% / 限价 cur_price ± 3% / 持仓上限 168h(1 周)
# 默认 disabled, 通过 system_settings.longhold_enabled 总开关控制 (先 paper 观察)
LH_ENABLED              = False         # 总开关, 默认 OFF, 由 system_settings 覆盖
LH_DATA_MIN_BARS        = 336           # 1h x 14 天
LH_REBOUND_MIN_PCT      = 0.08          # 颈线反弹/回撤 ≥ 8% (14 天波动比 3.5 天大, 阈值放大)
LH_BOTTOM_DIFF_PCT      = 0.05          # 两底/两顶价差 ≤ 5%
LH_B2_TO_NECK_MIN_BARS  = 12            # B2 距颈线 ≥ 12 根 1h = 12h
LH_TIME_GAP_MIN_BARS    = 48            # B1→B2 ≥ 48h (2 天)
LH_TIME_GAP_MAX_BARS    = 336           # B1→B2 ≤ 14 天
LH_BREAK_NECK_PCT       = 0.005         # 突破/跌破颈线 0.5% 确认
LH_SL_PCT               = 0.04
LH_HARD_TP_PCT          = 0.20
LH_LIMIT_OFFSET_PCT     = 0.03
LH_HOLD_MIN             = 7 * 24 * 60   # 168 h = 1 周
LH_LIMIT_TTL_S          = 24 * 3600     # 限价单 24h 未成交撤掉
LH_COOLDOWN_S           = 7 * 24 * 3600 # 同品种触发后冷却 7 天
LH_MAX_OPEN_POSITIONS   = 3             # 全局上限 (longhold-w + longhold-m 合计)


def _load_whale_config() -> None:
    """从 system_settings 读取策略参数，覆盖模块级常量。进程启动时调用一次。"""
    global WHALE_SL_PCT, WHALE_HARD_TP_PCT, WHALE_LIMIT_OFFSET_PCT, WHALE_HOLD_H
    global SL_PCT, HARD_TP_PCT, SHORT_HOLD_H, LONG_HOLD_H
    global DISABLE_SL_TP_HOLD, DISABLE_5M_CONFIRM
    global LH_ENABLED, LH_SL_PCT, LH_HARD_TP_PCT, LH_LIMIT_OFFSET_PCT, LH_HOLD_MIN, LH_REBOUND_MIN_PCT
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
                    "WHERE setting_key IN ('whale_sl_pct','whale_hard_tp_pct',"
                    "'whale_limit_offset_pct','whale_hold_hours','disable_sl_tp_hold',"
                    "'disable_5m_confirm',"
                    "'longhold_enabled','longhold_sl_pct','longhold_tp_pct',"
                    "'longhold_limit_offset_pct','longhold_hold_hours','longhold_rebound_pct')"
                )
                rows = {r['setting_key']: r['setting_value'] for r in cur.fetchall()}
        finally:
            conn.close()
        WHALE_SL_PCT           = float(rows.get('whale_sl_pct',           WHALE_SL_PCT))
        WHALE_HARD_TP_PCT      = float(rows.get('whale_hard_tp_pct',      WHALE_HARD_TP_PCT))
        WHALE_LIMIT_OFFSET_PCT = float(rows.get('whale_limit_offset_pct', WHALE_LIMIT_OFFSET_PCT))
        WHALE_HOLD_H           = int(  rows.get('whale_hold_hours',        WHALE_HOLD_H))
        _raw_disable = str(rows.get('disable_sl_tp_hold', '0')).strip().lower()
        DISABLE_SL_TP_HOLD = _raw_disable in ('1', 'true', 'yes', 'on')
        _raw_5m = str(rows.get('disable_5m_confirm', '0')).strip().lower()
        DISABLE_5M_CONFIRM = _raw_5m in ('1', 'true', 'yes', 'on')
        SL_PCT      = WHALE_SL_PCT
        HARD_TP_PCT = WHALE_HARD_TP_PCT
        SHORT_HOLD_H = WHALE_HOLD_H
        LONG_HOLD_H  = WHALE_HOLD_H

        # longhold 子策略参数
        _raw_lh = str(rows.get('longhold_enabled', '0')).strip().lower()
        LH_ENABLED          = _raw_lh in ('1', 'true', 'yes', 'on')
        LH_SL_PCT           = float(rows.get('longhold_sl_pct',           LH_SL_PCT))
        LH_HARD_TP_PCT      = float(rows.get('longhold_tp_pct',           LH_HARD_TP_PCT))
        LH_LIMIT_OFFSET_PCT = float(rows.get('longhold_limit_offset_pct', LH_LIMIT_OFFSET_PCT))
        _lh_hold_h          = int(  rows.get('longhold_hold_hours',        LH_HOLD_MIN // 60))
        LH_HOLD_MIN         = _lh_hold_h * 60
        LH_REBOUND_MIN_PCT  = float(rows.get('longhold_rebound_pct',      LH_REBOUND_MIN_PCT))

        log.info(
            "strategy_whale 参数已加载: SL=%.0f%% TP=%.0f%% offset=%.1f%% hold=%dh disable_sl_tp_hold=%s disable_5m_confirm=%s",
            WHALE_SL_PCT * 100, WHALE_HARD_TP_PCT * 100, WHALE_LIMIT_OFFSET_PCT * 100, WHALE_HOLD_H,
            DISABLE_SL_TP_HOLD, DISABLE_5M_CONFIRM,
        )
        log.info(
            "longhold 参数: enabled=%s SL=%.1f%% TP=%.0f%% offset=%.1f%% hold=%dh rebound>=%.1f%%",
            LH_ENABLED, LH_SL_PCT * 100, LH_HARD_TP_PCT * 100, LH_LIMIT_OFFSET_PCT * 100,
            LH_HOLD_MIN // 60, LH_REBOUND_MIN_PCT * 100,
        )
        if DISABLE_SL_TP_HOLD:
            log.warning("!!! DISABLE_SL_TP_HOLD=ON: 新开仓将不设 SL/TP/timeout, 硬TP/移动TP检查跳过 !!!")
        if DISABLE_5M_CONFIRM:
            log.warning("!!! DISABLE_5M_CONFIRM=ON: 限价单触发即成交, 跳过 5m 阴/阳确认 !!!")
    except Exception as exc:
        log.error("_load_whale_config 失败，使用默认值: %s", exc)


POLL_SECS    = 90
MAX_POS_PER_SIDE = 3   # 同时最多持 3 个多/空

# ── 日志 ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    datefmt='%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('strategy_whale.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)

# ── DB ────────────────────────────────────────────────────────────────
def get_db():
    return pymysql.connect(
        host=os.getenv('DB_HOST'), port=int(os.getenv('DB_PORT', 3306)),
        user=os.getenv('DB_USER'), password=os.getenv('DB_PASSWORD', ''),
        db=os.getenv('DB_NAME'), charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

def _api(method, path, **kw):
    r = req.request(method, f"{API_BASE}{path}", timeout=10, **kw)
    r.raise_for_status()
    return r.json()


def _log_order_event(conn, order_id: str, event_type: str,
                     cur_price=None, limit_price=None,
                     bar_open=None, bar_close=None, detail: str = ''):
    """LIMIT 中间事件入库 (order_trigger_events 表). 失败不阻塞主流程.
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


def get_price(sym: str) -> float:
    d = _api("GET", f"/api/futures/price/{sym}")
    return float(d["price"])

def get_pos_status(pid: int):
    """返回 (status, pnl, notes) 或 (None, None, None)"""
    try:
        d = _api("GET", f"/api/futures/positions/{pid}")
        pos = d.get("data") or d
        if isinstance(pos, list):
            pos = pos[0] if pos else {}
        return pos.get("status"), pos.get("realized_pnl", 0), pos.get("notes", "")
    except Exception:
        return None, None, None

def _close_pos(pid: int, reason: str = "manual"):
    try:
        _api("POST", f"/api/futures/close/{pid}", json={"reason": reason})
    except Exception as e:
        log.warning("_close_pos %d failed: %s", pid, e)

def _trail_tp_check(conn, sym: str, pid: int, side: str, entry_p: float, peak_pct: float, entry_time_s: float = 0) -> bool:
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
        update_state(conn, 'whale', sym, 'whale', peak_pnl_pct=new_peak)
    if pnl_pct >= HARD_TP_PCT:
        _close_pos(pid, "hard-tp")
        log.info("硬止盈 [WHALE] %-18s  pnl=+%.1f%%", sym, pnl_pct * 100)
        return True
    pullback_thresh = _dynamic_trail_pullback(new_peak)
    if (new_peak - pnl_pct) >= pullback_thresh:
        _close_pos(pid, "trail-tp")
        log.info("移动止盈 [WHALE] %-18s  pnl=+%.1f%%  peak=+%.1f%%  回撤%.1f%%  阈值%.1f%%",
                 sym, pnl_pct * 100, new_peak * 100,
                 (new_peak - pnl_pct) * 100, pullback_thresh * 100)
        return True
    # 入场保护期：前 ENTRY_GRACE_MIN 分钟 early-sl/breakeven 不触发
    in_grace = entry_time_s and (now_s() - float(entry_time_s)) < ENTRY_GRACE_MIN * 60
    if not in_grace:
        # 保本止损
        if new_peak >= BREAKEVEN_AFTER_PEAK_PCT and pnl_pct <= BREAKEVEN_SL_PCT:
            _close_pos(pid, "breakeven-sl")
            log.info("保本止损 [WHALE] %-18s  pnl=%.1f%%  peak=+%.1f%%",
                     sym, pnl_pct * 100, new_peak * 100)
            return True
        # 早期止损
        if pnl_pct <= -EARLY_SL_PCT:
            _close_pos(pid, "early-sl")
            log.info("早期止损 [WHALE] %-18s  pnl=%.1f%%", sym, pnl_pct * 100)
            return True
    return False

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
            resp = req.post(
                f"{API_BASE}/api/futures/close/{r['id']}",
                json={"reason": "timeout"},
                timeout=10,
            )
            if resp.ok:
                log.info("超时平仓: %s %s pid=%d", r['symbol'], r['position_side'], r['id'])
            else:
                log.warning("超时平仓失败 pid=%d: %s", r['id'], resp.text[:100])
        except Exception as e:
            log.error("超时平仓异常 pid=%d: %s", r['id'], e)


def _has_any_open(sym: str) -> bool:
    """检查 DB 里是否已有任意方向的 open 持仓或 PENDING 挂单，有则返回 True。"""
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


def open_order(sym, side, price, tp_pct, sl_pct, hold_min, tag, limit_price=None):
    """开仓. 返回 (position_id, order_id, is_pending)

    tp_pct / sl_pct = None 表示本策略强制不设 SL/TP（如 W 双底长持）
    hold_min = 0 / None 表示不设 timeout
    """
    if _has_any_open(sym):
        log.info("跳过开%s %s: 已有持仓", side, sym)
        return None, None, False
    price_ref = limit_price if (limit_price and limit_price > 0) else price
    qty = round(MARGIN * LEVERAGE / price_ref, 6)

    # 子策略显式裸奔（如 W 双底）：无视 DISABLE_SL_TP_HOLD 全局开关
    strategy_naked = (tp_pct is None or sl_pct is None)

    if strategy_naked:
        sl_out, tp_out = None, None
        hold_out = hold_min if hold_min else 0
    else:
        if side == "LONG":
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
        "account_id": ACCOUNT_ID, "symbol": sym,
        "position_side": side, "quantity": qty, "leverage": LEVERAGE,
        "stop_loss_price": sl_out, "take_profit_price": tp_out,
        "max_hold_minutes": hold_out,
        "source": f"strategy_whale:{tag}",
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
    cur.execute("SELECT high_24h, low_24h FROM price_stats_24h WHERE symbol=%s ORDER BY updated_at DESC LIMIT 1", (sym,))
    r = cur.fetchone()
    return (float(r['high_24h']), float(r['low_24h'])) if r else (None, None)


def _get_4h_stats(cur, sym):
    """取最近 4h 5m K 线 max/min, 用于七上八下限价 (2026-04-25)."""
    cur.execute("""
        SELECT MAX(high_price) AS h, MIN(low_price) AS l
        FROM kline_data WHERE symbol=%s AND timeframe='5m'
          AND open_time >= UNIX_TIMESTAMP(NOW() - INTERVAL 4 HOUR) * 1000
    """, (sym,))
    r = cur.fetchone()
    if not r or r.get('h') is None:
        return (None, None)
    return (float(r['h']), float(r['l']))


def _calc_limit_price(side, cur_price, high_24h, low_24h, high_4h=None, low_4h=None):
    """限价挂单 (2026-04-25 七上八下原则):
       SHORT: 优先 4h_high × 0.80; 若小于 cur×(1+offset), 用 cur×(1+offset). 受 24h_high 压制.
       LONG:  优先 4h_low  × 1.30; 若大于 cur×(1-offset), 用 cur×(1-offset). 受 24h_low  支撑.
    """
    if side == 'LONG':
        fallback = cur_price * (1 - WHALE_LIMIT_OFFSET_PCT)
        if low_4h and low_4h > 0:
            qi_shang = low_4h * 1.30
            lp = min(qi_shang, fallback)
        else:
            lp = fallback
        if low_24h and low_24h > 0:
            lp = max(lp, float(low_24h))
    else:
        fallback = cur_price * (1 + WHALE_LIMIT_OFFSET_PCT)
        if high_4h and high_4h > 0:
            ba_xia = high_4h * 0.80
            lp = max(ba_xia, fallback)
        else:
            lp = fallback
        if high_24h and high_24h > 0:
            lp = min(lp, float(high_24h))
    return round(lp, 8)

def _check_pending_db(conn, sym):
    """检查限价挂单是否成交/取消。返回 (should_continue, row)。
    should_continue=False 表示仍在挂单中，本 tick 跳过。"""
    row = get_or_create(conn, 'whale', sym, 'whale', {})
    oid = row.get('order_id')
    if not oid:
        return True, row
    if row.get('pid'):
        update_state(conn, 'whale', sym, 'whale', order_id=None)
        return True, {**row, 'order_id': None}
    cur = conn.cursor()
    cur.execute("SELECT status, position_id FROM futures_orders WHERE order_id=%s LIMIT 1", (oid,))
    order = cur.fetchone()
    cur.close()
    if not order:
        update_state(conn, 'whale', sym, 'whale', order_id=None)
        return True, {**row, 'order_id': None}
    st     = (order.get('status') or '').upper()
    pos_id = order.get('position_id')
    if st == 'FILLED' and pos_id:
        t_fill = now_s()
        update_state(conn, 'whale', sym, 'whale', pid=int(pos_id), order_id=None, entry_time=t_fill)
        log.info("WHALE 限价单成交 -> pid=%d  oid=%s", int(pos_id), oid)
        return True, {**row, 'pid': int(pos_id), 'order_id': None, 'entry_time': t_fill}
    if st in ('CANCELLED', 'REJECTED'):
        ts = time.time()
        update_state(
            conn,
            'whale',
            sym,
            'whale',
            state='DONE',
            pid=None,
            order_id=None,
            done_time=ts,
            last_reason='cancel',
        )
        return True, {
            **row,
            'state': 'DONE',
            'pid': None,
            'order_id': None,
            'done_time': ts,
            'last_reason': 'cancel',
        }
    return False, row

def _fill_pending_orders(conn):
    """扫描并成交 PENDING 限价单 (策略独立)"""
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
            import datetime as _dt
            age_s = (_dt.datetime.now() - o['created_at']).total_seconds()
            # longhold 子策略限价单 TTL 24h, 其它 (whale/w-bottom/m-top) TTL 3h
            ttl_s = LH_LIMIT_TTL_S if 'longhold' in (o.get('order_source') or '') else 3 * 3600
            if age_s > ttl_s:
                c2 = conn.cursor()
                c2.execute("UPDATE futures_orders SET status='CANCELLED', cancellation_reason='timeout', canceled_at=NOW(), updated_at=NOW() WHERE id=%s", (o['id'],))
                conn.commit(); c2.close()
                log.info("WHALE 限价单超时取消 %s %s  oid=%s  age=%.1fh ttl=%.1fh",
                         sym, side, o['order_id'], age_s / 3600.0, ttl_s / 3600.0)
                continue
        try:
            cur_p = get_price(sym)
        except Exception:
            continue
        # side 在 DB 里存的是 OPEN_LONG / OPEN_SHORT，转成 LONG / SHORT
        pos_side = side.replace('OPEN_', '') if side.startswith('OPEN_') else side
        triggered = (pos_side == 'LONG' and cur_p <= limit_p) or (pos_side == 'SHORT' and cur_p >= limit_p)
        if not triggered:
            if _trigger_first_seen.pop(o['id'], None) is not None:
                log.info("WHALE 限价单触发回撤，重新等待 %s %s cur=%.5f limit=%.5f",
                         sym, side, cur_p, limit_p)
                _log_order_event(conn, o['order_id'], 'TRIGGER_RETREAT',
                                 cur_price=cur_p, limit_price=limit_p,
                                 detail=f"WHALE side={side} pos_side={pos_side}")
            continue
        # 已触发: 等下根 5m K 线收盘, 方向确认才成交 (2026-04-25 替代 30s 时间确认)
        # 2026-04-27: DISABLE_5M_CONFIRM=ON 时整段跳过, 触发即成交
        if DISABLE_5M_CONFIRM:
            _trigger_first_seen.pop(o['id'], None)
        else:
            first_seen_ms = _trigger_first_seen.get(o['id'])
            if first_seen_ms is None:
                _trigger_first_seen[o['id']] = int(time.time() * 1000)
                log.info("WHALE 限价单触发观察 %s %s cur=%.5f limit=%.5f (等下根 5m %s线收盘确认)",
                         sym, side, cur_p, limit_p,
                         '阴' if pos_side == 'SHORT' else '阳')
                _log_order_event(conn, o['order_id'], 'TRIGGER_OBSERVING',
                                 cur_price=cur_p, limit_price=limit_p,
                                 detail=f"WHALE side={side} 等{('阴' if pos_side == 'SHORT' else '阳')}线收盘确认")
                continue
            next_bar_open_ms  = (int(first_seen_ms) // 300000) * 300000 + 300000
            next_bar_close_ms = next_bar_open_ms + 300000
            if int(time.time() * 1000) < next_bar_close_ms:
                continue
            c_bar = conn.cursor()
            c_bar.execute(
                """SELECT open_price, close_price FROM kline_data
                   WHERE symbol=%s AND timeframe='5m' AND open_time=%s LIMIT 1""",
                (sym, next_bar_open_ms),
            )
            bar_row = c_bar.fetchone()
            c_bar.close()
            if not bar_row:
                continue
            bar_o = float(bar_row['open_price'])
            bar_c = float(bar_row['close_price'])
            confirm_ok = (pos_side == 'SHORT' and bar_c < bar_o) \
                         or (pos_side == 'LONG' and bar_c > bar_o)
            if not confirm_ok:
                log.info("WHALE 限价 5m 反向不成交, 等下次触发: %s %s bar[o=%.5f c=%.5f]",
                         sym, side, bar_o, bar_c)
                _log_order_event(conn, o['order_id'], '5M_REJECT',
                                 cur_price=cur_p, limit_price=limit_p,
                                 bar_open=bar_o, bar_close=bar_c,
                                 detail=f"WHALE side={side} 需{('阴' if pos_side == 'SHORT' else '阳')}线 实际 close={bar_c} open={bar_o}")
                _trigger_first_seen.pop(o['id'], None)
                continue
            _trigger_first_seen.pop(o['id'], None)
        # 先把订单标成 FILLING，防止同一订单被重复触发
        c2 = conn.cursor()
        affected = c2.execute("""UPDATE futures_orders
            SET status='FILLING', updated_at=NOW()
            WHERE id=%s AND status='PENDING'""", (o['id'],))
        conn.commit(); c2.close()
        if not affected:
            log.info("WHALE 限价单已被处理，跳过 %s %s oid=%s", sym, side, o['order_id'])
            continue
        pos_id = None
        try:
            # longhold 子策略限价成交后 hold 用 LH_HOLD_MIN(168h), 其它沿用 whale 默认
            _src = (o.get('order_source') or '')
            if 'longhold' in _src:
                max_hold = LH_HOLD_MIN
            else:
                max_hold = LONG_HOLD_H * 60 if pos_side == 'LONG' else SHORT_HOLD_H * 60
            _sl_raw = float(o['stop_loss_price']  or 0) or None
            _tp_raw = float(o['take_profit_price'] or 0) or None
            # 总开关: 裸奔模式下,限价单成交也不写 SL/TP/timeout
            if DISABLE_SL_TP_HOLD:
                sl_out, tp_out, hold_out = None, None, 0
            else:
                sl_out, tp_out, hold_out = _sl_raw, _tp_raw, max_hold
            payload = {
                "account_id": ACCOUNT_ID, "symbol": sym,
                "position_side": pos_side,
                "quantity": float(o['quantity'] or 0),
                "leverage": int(o['leverage'] or LEVERAGE),
                "stop_loss_price":   sl_out,
                "take_profit_price": tp_out,
                "source": (o.get('order_source') or 'strategy_whale:limit-fill'),
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
                log.info("WHALE 限价单成交 %s %s @ %.5f  pid=%s  oid=%s",
                         sym, side, cur_p, pos_id, o['order_id'])
            else:
                c2 = conn.cursor()
                c2.execute("UPDATE futures_orders SET status='PENDING', updated_at=NOW() WHERE id=%s", (o['id'],))
                conn.commit(); c2.close()
                log.warning("WHALE 限价单成交无 pos_id，回退 PENDING %s %s oid=%s", sym, side, o['order_id'])
        except Exception as e:
            try:
                c2 = conn.cursor()
                c2.execute("UPDATE futures_orders SET status='PENDING', updated_at=NOW() WHERE id=%s", (o['id'],))
                conn.commit(); c2.close()
            except Exception:
                pass
            log.warning("WHALE 限价单成交异常 %s: %s", sym, e)

def now_s() -> float:
    return time.time()

# ── 信号计算 ──────────────────────────────────────────────────────────
def _get_funding(cur, sym: str) -> float | None:
    """最新资金费率 (仅取 15 分钟内的数据)"""
    cur.execute("""
        SELECT funding_rate FROM funding_rate_data
        WHERE symbol=%s AND timestamp >= NOW()-INTERVAL 15 MINUTE
        ORDER BY timestamp DESC LIMIT 1
    """, (sym,))
    r = cur.fetchone()
    return float(r['funding_rate']) if r else None

def _get_ls(cur, sym: str) -> tuple | None:
    """最新多空比 (long_pct, short_pct), 取 2h 内最新"""
    cur.execute("""
        SELECT long_account, short_account FROM futures_long_short_ratio
        WHERE symbol=%s AND timestamp >= NOW()-INTERVAL 2 HOUR
        ORDER BY timestamp DESC LIMIT 1
    """, (sym,))
    r = cur.fetchone()
    return (float(r['long_account']), float(r['short_account'])) if r else None

def _get_oi_change(cur, sym: str) -> float | None:
    """4h OI 变化率 (当前/4h前 - 1), 需要至少 4 条 OI 记录"""
    cur.execute("""
        SELECT open_interest_value FROM futures_open_interest
        WHERE symbol=%s ORDER BY timestamp DESC LIMIT 5
    """, (sym,))
    rows = cur.fetchall()
    if len(rows) < 4:
        return None
    latest = float(rows[0]['open_interest_value'])
    oldest = float(rows[-1]['open_interest_value'])
    if oldest == 0:
        return None
    return (latest - oldest) / oldest

def _get_1h_bars(cur, sym: str, limit: int = 30) -> list:
    """最近 N 根完成的 1h K线"""
    now_ms = int(now_s() * 1000)
    cur.execute("""
        SELECT open_price, high_price, low_price, close_price, volume, taker_buy_base_volume
        FROM kline_data
        WHERE symbol=%s AND timeframe='1h'
          AND open_time + 3600000 < %s
        ORDER BY open_time DESC LIMIT %s
    """, (sym, now_ms, limit))
    return list(reversed(cur.fetchall()))


def _get_15m_bars(cur, sym: str, limit: int) -> list:
    """最近 N 根完成的 15m K线（W 双底子策略专用）"""
    now_ms = int(now_s() * 1000)
    cur.execute("""
        SELECT open_price, high_price, low_price, close_price, volume, taker_buy_base_volume
        FROM kline_data
        WHERE symbol=%s AND timeframe='15m'
          AND open_time + 900000 < %s
        ORDER BY open_time DESC LIMIT %s
    """, (sym, now_ms, limit))
    return list(reversed(cur.fetchall()))

def _vol_divergence(bars: list, direction: str) -> tuple:
    """
    检测放量滞涨(direction='short')或放量滞跌(direction='long').
    返回 (volume_ratio, price_change_abs, diverged: bool)
    """
    if len(bars) < 24:
        return 1.0, 0.0, False

    avg_vol  = sum(float(b['volume'] or 0) for b in bars[:-3]) / max(len(bars) - 3, 1)
    last3_vol = sum(float(b['volume'] or 0) for b in bars[-3:]) / 3
    vol_ratio = last3_vol / avg_vol if avg_vol > 0 else 1.0

    first_c = float(bars[-3]['close_price'])
    last_c  = float(bars[-1]['close_price'])
    price_chg = (last_c - first_c) / first_c if first_c > 0 else 0

    if direction == 'short':
        # 放量滞涨: 量大但价格没明显涨
        diverged = vol_ratio >= VOL_RATIO_MILD and abs(price_chg) < STALE_PRICE_PCT and price_chg > -0.03
    else:
        # 放量滞跌: 量大但价格没明显跌
        diverged = vol_ratio >= VOL_RATIO_MILD and abs(price_chg) < STALE_PRICE_PCT and price_chg < 0.03

    return vol_ratio, price_chg, diverged

def _taker_pressure(bars: list) -> float:
    """最近 3 根的平均 taker_buy_ratio"""
    if len(bars) < 3:
        return 0.5
    ratios = []
    for b in bars[-3:]:
        vol = float(b['volume'] or 0)
        buy = float(b['taker_buy_base_volume'] or 0)
        if vol > 0:
            ratios.append(buy / vol)
    return sum(ratios) / len(ratios) if ratios else 0.5

def _entry_trigger(bars: list, direction: str, cur_price: float) -> bool:
    """
    入场触发器:
    direction='short': 大阴线 or 跌破近 4 根最低价
    direction='long' : 大阳线 or 突破近 4 根最高价
    """
    if len(bars) < 5:
        return False
    last = bars[-1]
    o, c = float(last['open_price']), float(last['close_price'])
    lo4 = min(float(b['low_price'])  for b in bars[-5:-1])
    hi4 = max(float(b['high_price']) for b in bars[-5:-1])

    if direction == 'short':
        big_candle = (o - c) / o >= TRIGGER_CANDLE_PCT  # 大阴线
        breakout   = cur_price < lo4 * (1 - TRIGGER_BREAKOUT)
        return big_candle or breakout
    else:
        big_candle = (c - o) / o >= TRIGGER_CANDLE_PCT
        breakout   = cur_price > hi4 * (1 + TRIGGER_BREAKOUT)
        return big_candle or breakout

def compute_score(cur, sym: str, direction: str) -> tuple:
    """
    计算开仓评分.
    direction: 'short' 跟砸盘 | 'long' 跟拉盘
    返回 (score:int, detail:dict, has_trigger:bool)
    """
    score  = 0
    detail = {}

    # 1. 资金费率
    fr = _get_funding(cur, sym)
    if fr is not None:
        detail['funding'] = round(fr * 100, 4)
        if direction == 'short':
            if   fr >= FR_EXTREME_HIGH: score += 3
            elif fr >= FR_HIGH:         score += 2
            elif fr >= FR_MILD_HIGH:    score += 1
        else:
            if   fr <= FR_EXTREME_LOW: score += 3
            elif fr <= FR_LOW:         score += 2
            elif fr <= FR_MILD_LOW:    score += 1

    # 2. 多空比
    ls = _get_ls(cur, sym)
    if ls:
        long_pct, short_pct = ls
        detail['long_pct'] = round(long_pct * 100, 1)
        if direction == 'short':
            if   long_pct >= LS_LONG_EXTREME: score += 2
            elif long_pct >= LS_LONG_HIGH:    score += 1
        else:
            if   short_pct >= LS_SHORT_EXTREME: score += 2
            elif short_pct >= LS_SHORT_HIGH:    score += 1

    # 3. OI 趋势
    oi_chg = _get_oi_change(cur, sym)
    if oi_chg is not None:
        detail['oi_chg'] = round(oi_chg * 100, 2)
        if direction == 'short':
            if   oi_chg <= OI_DROP_STRONG: score += 2
            elif oi_chg <= OI_DROP_MILD:   score += 1
        else:
            if   oi_chg >= OI_RISE_STRONG: score += 2
            elif oi_chg >= OI_RISE_MILD:   score += 1

    # 4. 放量滞涨/滞跌 (主要信号)
    bars = _get_1h_bars(cur, sym, 30)
    if len(bars) >= 24:
        vol_ratio, price_chg, diverged = _vol_divergence(bars, direction)
        detail['vol_ratio']  = round(vol_ratio, 2)
        detail['price_chg3h'] = round(price_chg * 100, 2)
        if diverged:
            if   vol_ratio >= VOL_RATIO_STRONG: score += 3
            else:                               score += 2

        # 5. 隐性大单压力
        taker = _taker_pressure(bars)
        detail['taker_ratio'] = round(taker, 3)
        if direction == 'short' and taker < TAKER_SELL_THRESH:
            score += 1
        elif direction == 'long'  and taker > TAKER_BUY_THRESH:
            score += 1

    detail['score'] = score

    # 入场触发器
    has_trigger = False
    if bars:
        try:
            price = get_price(sym)
            has_trigger = _entry_trigger(bars, direction, price)
        except Exception:
            pass

    return score, detail, has_trigger


# ── W 型双底检测 ──────────────────────────────────────────────────────
def detect_w_bottom(bars_15m: list) -> dict | None:
    """
    识别最近 3.5 天（336 根 15m K 线）的 W 型双底。
    返回 None 表示不成立；成立返回 dict 含各关键点位。
    """
    n = len(bars_15m)
    if n < WB_DATA_MIN_BARS:
        return None

    lows   = [float(b['low_price'])   for b in bars_15m]
    highs  = [float(b['high_price'])  for b in bars_15m]
    closes = [float(b['close_price']) for b in bars_15m]

    # 1. 3.5 天最低点 B1
    i1 = min(range(n), key=lambda i: lows[i])
    b1 = lows[i1]
    if b1 <= 0:
        return None

    # 2. B1 之后必须有足够空间形成颈线 + 二次探底
    if n - i1 < 48:  # 至少 48 根后续 = 12h
        return None

    # 3. B1 之后的局部高点 = 颈线 C
    after_b1_highs = highs[i1 + 1:]
    if not after_b1_highs:
        return None
    ic_rel = max(range(len(after_b1_highs)), key=lambda i: after_b1_highs[i])
    ic = i1 + 1 + ic_rel
    c  = highs[ic]
    rebound = (c - b1) / b1
    if rebound < WB_REBOUND_MIN_PCT:
        return None

    # 4. C 之后的最低点 = 第二次探底 B2
    after_c_lows = lows[ic + 1:]
    if not after_c_lows:
        return None
    ib2_rel = min(range(len(after_c_lows)), key=lambda i: after_c_lows[i])
    ib2 = ic + 1 + ib2_rel
    b2  = lows[ib2]
    if (ib2 - ic) < WB_B2_TO_NECK_MIN_H:
        return None

    # 5. 两底对齐：B2 价格在 B1 ± WB_BOTTOM_DIFF_PCT
    if abs(b2 - b1) / b1 > WB_BOTTOM_DIFF_PCT:
        return None

    # 6. B1→B2 时间间隔合理
    gap_h = ib2 - i1
    if gap_h < WB_TIME_GAP_MIN_H or gap_h > WB_TIME_GAP_MAX_H:
        return None

    # 7. 当前价必须突破颈线 + WB_BREAK_NECK_PCT
    cur_price = closes[-1]
    if cur_price < c * (1 + WB_BREAK_NECK_PCT):
        return None

    return {
        'b1_idx':    i1,   'b1':    b1,
        'neck_idx':  ic,   'neck':  c,
        'b2_idx':    ib2,  'b2':    b2,
        'cur_price': cur_price,
        'rebound_pct':  rebound,
        'bottom_diff':  abs(b2 - b1) / b1,
        'gap_h':        gap_h,
        'break_pct':    (cur_price - c) / c,
    }


def _wb_active_count(conn) -> int:
    """当前 W 双底子策略的 active 持仓数（state!=IDLE）"""
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(1) AS n FROM strategy_state "
            "WHERE strategy='whale' AND stype='w-bottom' AND state!='IDLE'"
        )
        r = cur.fetchone()
        cur.close()
        return int(r['n']) if r else 0
    except Exception:
        return 0


def w_bottom_tick(conn, sym: str):
    """W 双底子策略每个品种的扫描"""
    ss = get_or_create(conn, 'whale', sym, 'w-bottom', {
        'state': 'IDLE', 'pid': None, 'order_id': None, 'entry_p': 0.0,
        'peak_pnl_pct': 0.0, 'side': None,
        'entry_time': 0.0, 'done_time': 0.0,
    })
    s = ss.get('state') or 'IDLE'

    # 冷却：同品种 3 天内不重复触发
    if s == 'DONE':
        anchor = ensure_cooldown_anchor_epoch(conn, 'whale', sym, 'w-bottom', ss, now_s())
        if now_s() - anchor > WB_COOLDOWN_S:
            update_state(conn, 'whale', sym, 'w-bottom', state='IDLE')
        return
    if s != 'IDLE':
        # PENDING/LONG 持仓中由 _close_overdue + 手动管理
        return

    # 全局持仓数上限
    if _wb_active_count(conn) >= WB_MAX_OPEN_POSITIONS:
        return

    # 取 3.5 天 15m K 线（336 + 24 根缓冲）
    cur = conn.cursor()
    try:
        bars = _get_15m_bars(cur, sym, limit=WB_DATA_MIN_BARS + 24)
    finally:
        cur.close()
    if len(bars) < WB_DATA_MIN_BARS:
        return

    sig = detect_w_bottom(bars)
    if not sig:
        return

    try:
        price = get_price(sym)
    except Exception:
        return
    cur2 = conn.cursor()
    try:
        h24, l24 = _get_24h_stats(cur2, sym)
        h4,  l4  = _get_4h_stats(cur2, sym)
    finally:
        cur2.close()
    lp = _calc_limit_price('LONG', price, h24, l24, high_4h=h4, low_4h=l4)

    # 不设 SL/TP（tp_pct=sl_pct=None），hold_min=3天，timeout 兜底
    pid, oid, pending = open_order(
        sym, 'LONG', price,
        tp_pct=None, sl_pct=None,
        hold_min=WB_HOLD_MIN,
        tag='w-bottom', limit_price=lp,
    )
    if not (pid or oid):
        return

    log.info(
        "[W-BOTTOM] %s LONG  B1=%.6f B2=%.6f neck=%.6f cur=%.6f  "
        "反弹%.1f%%  两底差%.2f%%  gap %.1fh  突破%.2f%%  lp=%.6f",
        sym, sig['b1'], sig['b2'], sig['neck'], sig['cur_price'],
        sig['rebound_pct'] * 100, sig['bottom_diff'] * 100,
        sig['gap_h'] * 0.25, sig['break_pct'] * 100, lp,  # 15m bar → 小时
    )
    update_state(
        conn, 'whale', sym, 'w-bottom',
        state='PENDING' if pending else 'LONG',
        pid=pid, order_id=oid, side='LONG',
        entry_p=lp if pending else price,
        peak_pnl_pct=0.0, entry_time=now_s(),
    )


# ── M 型双顶检测 (W 底镜像, 2026-04-25 新增) ────────────────────────
def detect_m_top(bars_15m: list) -> dict | None:
    """
    识别最近 3.5 天 (336 根 15m K 线) 的 M 型双顶. 镜像 detect_w_bottom 逻辑.
    返回 None 表示不成立; 成立返回 dict 含各关键点位.
    """
    n = len(bars_15m)
    if n < WB_DATA_MIN_BARS:
        return None

    lows   = [float(b['low_price'])   for b in bars_15m]
    highs  = [float(b['high_price'])  for b in bars_15m]
    closes = [float(b['close_price']) for b in bars_15m]

    # 1. 3.5 天最高点 H1
    i1 = max(range(n), key=lambda i: highs[i])
    h1 = highs[i1]
    if h1 <= 0:
        return None

    # 2. H1 之后必须有足够空间形成颈线 + 二次冲高
    if n - i1 < 48:  # 至少 48 根后续 = 12h
        return None

    # 3. H1 之后的局部低点 = 颈线 D
    after_h1_lows = lows[i1 + 1:]
    if not after_h1_lows:
        return None
    id_rel = min(range(len(after_h1_lows)), key=lambda i: after_h1_lows[i])
    id_idx = i1 + 1 + id_rel
    d  = lows[id_idx]
    if d <= 0:
        return None
    pullback = (h1 - d) / h1
    if pullback < WB_REBOUND_MIN_PCT:
        return None

    # 4. D 之后的最高点 = 第二次冲高 H2
    after_d_highs = highs[id_idx + 1:]
    if not after_d_highs:
        return None
    ih2_rel = max(range(len(after_d_highs)), key=lambda i: after_d_highs[i])
    ih2 = id_idx + 1 + ih2_rel
    h2  = highs[ih2]
    if (ih2 - id_idx) < WB_B2_TO_NECK_MIN_H:
        return None

    # 5. 两顶对齐: H2 价格在 H1 ± WB_BOTTOM_DIFF_PCT
    if abs(h2 - h1) / h1 > WB_BOTTOM_DIFF_PCT:
        return None

    # 6. H1→H2 时间间隔合理
    gap_h = ih2 - i1
    if gap_h < WB_TIME_GAP_MIN_H or gap_h > WB_TIME_GAP_MAX_H:
        return None

    # 7. 当前价必须跌破颈线 - WB_BREAK_NECK_PCT (镜像)
    cur_price = closes[-1]
    if cur_price > d * (1 - WB_BREAK_NECK_PCT):
        return None

    return {
        'h1_idx':    i1,      'h1':    h1,
        'neck_idx':  id_idx,  'neck':  d,
        'h2_idx':    ih2,     'h2':    h2,
        'cur_price': cur_price,
        'pullback_pct': pullback,
        'top_diff':     abs(h2 - h1) / h1,
        'gap_h':        gap_h,
        'break_pct':    (d - cur_price) / d,
    }


def _mt_active_count(conn) -> int:
    """当前 M 双顶子策略的 active 持仓数 (state!=IDLE)"""
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(1) AS n FROM strategy_state "
            "WHERE strategy='whale' AND stype='m-top' AND state!='IDLE'"
        )
        r = cur.fetchone()
        cur.close()
        return int(r['n']) if r else 0
    except Exception:
        return 0


def m_top_tick(conn, sym: str):
    """M 双顶子策略每个品种的扫描 (做空, 不设 SL/TP, 1 天持仓)"""
    ss = get_or_create(conn, 'whale', sym, 'm-top', {
        'state': 'IDLE', 'pid': None, 'order_id': None, 'entry_p': 0.0,
        'peak_pnl_pct': 0.0, 'side': None,
        'entry_time': 0.0, 'done_time': 0.0,
    })
    s = ss.get('state') or 'IDLE'

    # 冷却: 同品种 3 天内不重复触发
    if s == 'DONE':
        anchor = ensure_cooldown_anchor_epoch(conn, 'whale', sym, 'm-top', ss, now_s())
        if now_s() - anchor > WB_COOLDOWN_S:
            update_state(conn, 'whale', sym, 'm-top', state='IDLE')
        return
    if s != 'IDLE':
        return

    # 全局持仓数上限 (复用 W 底参数)
    if _mt_active_count(conn) >= WB_MAX_OPEN_POSITIONS:
        return

    # 取 3.5 天 15m K 线
    cur = conn.cursor()
    try:
        bars = _get_15m_bars(cur, sym, limit=WB_DATA_MIN_BARS + 24)
    finally:
        cur.close()
    if len(bars) < WB_DATA_MIN_BARS:
        return

    sig = detect_m_top(bars)
    if not sig:
        return

    try:
        price = get_price(sym)
    except Exception:
        return
    cur2 = conn.cursor()
    try:
        h24, l24 = _get_24h_stats(cur2, sym)
        h4,  l4  = _get_4h_stats(cur2, sym)
    finally:
        cur2.close()
    lp = _calc_limit_price('SHORT', price, h24, l24, high_4h=h4, low_4h=l4)

    # 不设 SL/TP, hold_min=1天, timeout 兜底
    pid, oid, pending = open_order(
        sym, 'SHORT', price,
        tp_pct=None, sl_pct=None,
        hold_min=WB_HOLD_MIN,
        tag='m-top', limit_price=lp,
    )
    if not (pid or oid):
        return

    log.info(
        "[M-TOP] %s SHORT  H1=%.6f H2=%.6f neck=%.6f cur=%.6f  "
        "回撤%.1f%%  两顶差%.2f%%  gap %.1fh  跌破%.2f%%  lp=%.6f",
        sym, sig['h1'], sig['h2'], sig['neck'], sig['cur_price'],
        sig['pullback_pct'] * 100, sig['top_diff'] * 100,
        sig['gap_h'] * 0.25, sig['break_pct'] * 100, lp,
    )
    update_state(
        conn, 'whale', sym, 'm-top',
        state='PENDING' if pending else 'SHORT',
        pid=pid, order_id=oid, side='SHORT',
        entry_p=lp if pending else price,
        peak_pnl_pct=0.0, entry_time=now_s(),
    )


# ── 长持子策略 longhold (W底/M顶 2 周窗口, 1h K 线) ──────────────────────
# 形态识别框架沿用 detect_w_bottom / detect_m_top, 但常量改用 LH_* (1h bar 单位)
def detect_w_bottom_lh(bars_1h: list) -> dict | None:
    """
    识别最近 14 天 (336 根 1h K 线) 的 W 型双底.
    返回 None 表示不成立; 成立返回 dict 含各关键点位.
    """
    n = len(bars_1h)
    if n < LH_DATA_MIN_BARS:
        return None

    lows   = [float(b['low_price'])   for b in bars_1h]
    highs  = [float(b['high_price'])  for b in bars_1h]
    closes = [float(b['close_price']) for b in bars_1h]

    # 1. 14 天最低点 B1
    i1 = min(range(n), key=lambda i: lows[i])
    b1 = lows[i1]
    if b1 <= 0:
        return None

    # 2. B1 之后必须有足够空间形成颈线 + 二次探底
    if n - i1 < LH_B2_TO_NECK_MIN_BARS * 2:
        return None

    # 3. B1 之后的局部高点 = 颈线 C
    after_b1_highs = highs[i1 + 1:]
    if not after_b1_highs:
        return None
    ic_rel = max(range(len(after_b1_highs)), key=lambda i: after_b1_highs[i])
    ic = i1 + 1 + ic_rel
    c  = highs[ic]
    rebound = (c - b1) / b1
    if rebound < LH_REBOUND_MIN_PCT:
        return None

    # 4. C 之后的最低点 = 第二次探底 B2
    after_c_lows = lows[ic + 1:]
    if not after_c_lows:
        return None
    ib2_rel = min(range(len(after_c_lows)), key=lambda i: after_c_lows[i])
    ib2 = ic + 1 + ib2_rel
    b2  = lows[ib2]
    if (ib2 - ic) < LH_B2_TO_NECK_MIN_BARS:
        return None

    # 5. 两底对齐
    if abs(b2 - b1) / b1 > LH_BOTTOM_DIFF_PCT:
        return None

    # 6. B1->B2 时间间隔合理
    gap_bars = ib2 - i1
    if gap_bars < LH_TIME_GAP_MIN_BARS or gap_bars > LH_TIME_GAP_MAX_BARS:
        return None

    # 7. 当前价突破颈线
    cur_price = closes[-1]
    if cur_price < c * (1 + LH_BREAK_NECK_PCT):
        return None

    return {
        'b1_idx':    i1,   'b1':    b1,
        'neck_idx':  ic,   'neck':  c,
        'b2_idx':    ib2,  'b2':    b2,
        'cur_price': cur_price,
        'rebound_pct':  rebound,
        'bottom_diff':  abs(b2 - b1) / b1,
        'gap_bars':     gap_bars,
        'break_pct':    (cur_price - c) / c,
    }


def detect_m_top_lh(bars_1h: list) -> dict | None:
    """识别最近 14 天 (336 根 1h K 线) 的 M 型双顶 (W 底镜像)."""
    n = len(bars_1h)
    if n < LH_DATA_MIN_BARS:
        return None

    lows   = [float(b['low_price'])   for b in bars_1h]
    highs  = [float(b['high_price'])  for b in bars_1h]
    closes = [float(b['close_price']) for b in bars_1h]

    # 1. 14 天最高点 H1
    i1 = max(range(n), key=lambda i: highs[i])
    h1 = highs[i1]
    if h1 <= 0:
        return None

    # 2. H1 之后必须有足够空间
    if n - i1 < LH_B2_TO_NECK_MIN_BARS * 2:
        return None

    # 3. H1 之后的局部低点 = 颈线 D
    after_h1_lows = lows[i1 + 1:]
    if not after_h1_lows:
        return None
    id_rel = min(range(len(after_h1_lows)), key=lambda i: after_h1_lows[i])
    id_idx = i1 + 1 + id_rel
    d  = lows[id_idx]
    if d <= 0:
        return None
    pullback = (h1 - d) / h1
    if pullback < LH_REBOUND_MIN_PCT:
        return None

    # 4. D 之后的最高点 = 第二次冲高 H2
    after_d_highs = highs[id_idx + 1:]
    if not after_d_highs:
        return None
    ih2_rel = max(range(len(after_d_highs)), key=lambda i: after_d_highs[i])
    ih2 = id_idx + 1 + ih2_rel
    h2  = highs[ih2]
    if (ih2 - id_idx) < LH_B2_TO_NECK_MIN_BARS:
        return None

    # 5. 两顶对齐
    if abs(h2 - h1) / h1 > LH_BOTTOM_DIFF_PCT:
        return None

    # 6. H1->H2 时间间隔合理
    gap_bars = ih2 - i1
    if gap_bars < LH_TIME_GAP_MIN_BARS or gap_bars > LH_TIME_GAP_MAX_BARS:
        return None

    # 7. 当前价跌破颈线
    cur_price = closes[-1]
    if cur_price > d * (1 - LH_BREAK_NECK_PCT):
        return None

    return {
        'h1_idx':    i1,      'h1':    h1,
        'neck_idx':  id_idx,  'neck':  d,
        'h2_idx':    ih2,     'h2':    h2,
        'cur_price': cur_price,
        'pullback_pct': pullback,
        'top_diff':     abs(h2 - h1) / h1,
        'gap_bars':     gap_bars,
        'break_pct':    (d - cur_price) / d,
    }


def _lh_active_count(conn) -> int:
    """longhold 子策略 active 持仓数 (longhold-w + longhold-m 合计)"""
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(1) AS n FROM strategy_state "
            "WHERE strategy='whale' AND stype IN ('longhold-w','longhold-m') "
            "  AND state!='IDLE'"
        )
        r = cur.fetchone()
        cur.close()
        return int(r['n']) if r else 0
    except Exception:
        return 0


def _any_whale_active(conn, sym: str) -> bool:
    """同 symbol 是否有任意 whale 系列 stype 处于 active. longhold 入场前互斥检查."""
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM strategy_state WHERE strategy='whale' AND symbol=%s "
            "  AND stype IN ('whale','w-bottom','m-top','longhold-w','longhold-m') "
            "  AND state!='IDLE' LIMIT 1",
            (sym,),
        )
        r = cur.fetchone()
        cur.close()
        return bool(r)
    except Exception:
        return False


def longhold_w_tick(conn, sym: str):
    """longhold-w 子策略: 14 天 W 底做多, 持仓 1 周, TP 20% / SL 4%, 限价 -3%."""
    if not LH_ENABLED:
        return

    ss = get_or_create(conn, 'whale', sym, 'longhold-w', {
        'state': 'IDLE', 'pid': None, 'order_id': None, 'entry_p': 0.0,
        'peak_pnl_pct': 0.0, 'side': None,
        'entry_time': 0.0, 'done_time': 0.0,
    })
    s = ss.get('state') or 'IDLE'

    # 同品种 7 天冷却
    if s == 'DONE':
        anchor = ensure_cooldown_anchor_epoch(conn, 'whale', sym, 'longhold-w', ss, now_s())
        if now_s() - anchor > LH_COOLDOWN_S:
            update_state(conn, 'whale', sym, 'longhold-w', state='IDLE')
        return
    if s != 'IDLE':
        # PENDING/LONG 中由 _close_overdue + _fill_pending_orders 管理
        return

    # 互斥: 同 symbol 已有 whale 系任意子策略 active
    if _any_whale_active(conn, sym):
        return

    # 全局上限
    if _lh_active_count(conn) >= LH_MAX_OPEN_POSITIONS:
        return

    cur = conn.cursor()
    try:
        bars = _get_1h_bars(cur, sym, limit=LH_DATA_MIN_BARS + 24)
    finally:
        cur.close()
    if len(bars) < LH_DATA_MIN_BARS:
        return

    sig = detect_w_bottom_lh(bars)
    if not sig:
        return

    try:
        price = get_price(sym)
    except Exception:
        return

    lp = round(price * (1 - LH_LIMIT_OFFSET_PCT), 8)

    pid, oid, pending = open_order(
        sym, 'LONG', price,
        tp_pct=LH_HARD_TP_PCT, sl_pct=LH_SL_PCT,
        hold_min=LH_HOLD_MIN,
        tag='longhold-w', limit_price=lp,
    )
    if not (pid or oid):
        return

    log.info(
        "[LONGHOLD-W] %s LONG  B1=%.6f B2=%.6f neck=%.6f cur=%.6f  "
        "反弹%.1f%%  两底差%.2f%%  gap %dh  突破%.2f%%  lp=%.6f (TP=%.0f%% SL=%.0f%% hold=%dh)",
        sym, sig['b1'], sig['b2'], sig['neck'], sig['cur_price'],
        sig['rebound_pct'] * 100, sig['bottom_diff'] * 100,
        sig['gap_bars'], sig['break_pct'] * 100, lp,
        LH_HARD_TP_PCT * 100, LH_SL_PCT * 100, LH_HOLD_MIN // 60,
    )
    update_state(
        conn, 'whale', sym, 'longhold-w',
        state='PENDING' if pending else 'LONG',
        pid=pid, order_id=oid, side='LONG',
        entry_p=lp if pending else price,
        peak_pnl_pct=0.0, entry_time=now_s(),
    )


def longhold_m_tick(conn, sym: str):
    """longhold-m 子策略: 14 天 M 顶做空, 持仓 1 周, TP 20% / SL 4%, 限价 +3%."""
    if not LH_ENABLED:
        return

    ss = get_or_create(conn, 'whale', sym, 'longhold-m', {
        'state': 'IDLE', 'pid': None, 'order_id': None, 'entry_p': 0.0,
        'peak_pnl_pct': 0.0, 'side': None,
        'entry_time': 0.0, 'done_time': 0.0,
    })
    s = ss.get('state') or 'IDLE'

    if s == 'DONE':
        anchor = ensure_cooldown_anchor_epoch(conn, 'whale', sym, 'longhold-m', ss, now_s())
        if now_s() - anchor > LH_COOLDOWN_S:
            update_state(conn, 'whale', sym, 'longhold-m', state='IDLE')
        return
    if s != 'IDLE':
        return

    if _any_whale_active(conn, sym):
        return

    if _lh_active_count(conn) >= LH_MAX_OPEN_POSITIONS:
        return

    cur = conn.cursor()
    try:
        bars = _get_1h_bars(cur, sym, limit=LH_DATA_MIN_BARS + 24)
    finally:
        cur.close()
    if len(bars) < LH_DATA_MIN_BARS:
        return

    sig = detect_m_top_lh(bars)
    if not sig:
        return

    try:
        price = get_price(sym)
    except Exception:
        return

    lp = round(price * (1 + LH_LIMIT_OFFSET_PCT), 8)

    pid, oid, pending = open_order(
        sym, 'SHORT', price,
        tp_pct=LH_HARD_TP_PCT, sl_pct=LH_SL_PCT,
        hold_min=LH_HOLD_MIN,
        tag='longhold-m', limit_price=lp,
    )
    if not (pid or oid):
        return

    log.info(
        "[LONGHOLD-M] %s SHORT  H1=%.6f H2=%.6f neck=%.6f cur=%.6f  "
        "回撤%.1f%%  两顶差%.2f%%  gap %dh  跌破%.2f%%  lp=%.6f (TP=%.0f%% SL=%.0f%% hold=%dh)",
        sym, sig['h1'], sig['h2'], sig['neck'], sig['cur_price'],
        sig['pullback_pct'] * 100, sig['top_diff'] * 100,
        sig['gap_bars'], sig['break_pct'] * 100, lp,
        LH_HARD_TP_PCT * 100, LH_SL_PCT * 100, LH_HOLD_MIN // 60,
    )
    update_state(
        conn, 'whale', sym, 'longhold-m',
        state='PENDING' if pending else 'SHORT',
        pid=pid, order_id=oid, side='SHORT',
        entry_p=lp if pending else price,
        peak_pnl_pct=0.0, entry_time=now_s(),
    )


# ── 仓位管理 ──────────────────────────────────────────────────────────
def whale_tick(conn, sym: str):
    """每个品种的主逻辑"""
    ss = get_or_create(conn, 'whale', sym, 'whale', {
        'state': 'IDLE', 'pid': None, 'order_id': None, 'entry_p': 0.0,
        'peak_pnl_pct': 0.0, 'side': None,
        'entry_time': 0.0, 'done_time': 0.0,
    })
    s = ss.get('state') or 'IDLE'

    # 冷却（done_time 为 0 时不得用 now-0，否则恒判定为已冷却）
    if s == 'DONE':
        anchor = ensure_cooldown_anchor_epoch(conn, 'whale', sym, 'whale', ss, now_s())
        cd = COOLDOWN_SL_S if ss.get('last_reason') == 'SL' else COOLDOWN_S
        if now_s() - anchor > cd:
            update_state(conn, 'whale', sym, 'whale', state='IDLE')
        return

    # 挂单检查
    ok, ss = _check_pending_db(conn, sym)
    if not ok:
        return
    s = ss.get('state') or 'IDLE'

    # 检查持仓状态
    if s in ('SHORT', 'LONG') and ss.get('pid'):
        status, pnl, notes = get_pos_status(ss['pid'])
        if status is None:
            return
        if status == 'open':
            _trail_tp_check(conn, sym, ss['pid'],
                            ss.get('side') or s, ss.get('entry_p', 0), ss.get('peak_pnl_pct', 0),
                            ss.get('entry_time', 0))
            return

        pnl = float(pnl or 0)
        if notes and '手动' in str(notes):
            log.info("WHALE 手动平仓 -> DONE %-18s  pnl=%+.1f", sym, pnl)
            update_state(conn, 'whale', sym, 'whale',
                         state='DONE', pid=None, done_time=now_s(), last_reason='manual')
            return

        side = ss.get('side') or s
        label = "TP" if pnl > 0 else "SL"
        _cd = COOLDOWN_SL_S if label == "SL" else COOLDOWN_S
        log.info("WHALE %s %s -> DONE %-18s  %-5s  pnl=%+.1f  冷却%dh",
                 label, s, sym, side, pnl, _cd // 3600)
        update_state(conn, 'whale', sym, 'whale',
                     state='DONE', pid=None, order_id=None,
                     done_time=now_s(), last_reason=label)
        return

    if s != 'IDLE':
        return

    active_rows = list_active(conn, 'whale', 'whale')
    short_cnt = sum(1 for r in active_rows if r.get('side') == 'SHORT')

    cur = conn.cursor()
    # 仅做空
    if short_cnt < MAX_POS_PER_SIDE:
        score_s, detail_s, trig_s = compute_score(cur, sym, 'short')
        if score_s >= ENTRY_SCORE_MIN and trig_s:
            try:
                price = get_price(sym)
                h24, l24 = _get_24h_stats(cur, sym)
                h4,  l4  = _get_4h_stats(cur, sym)
                lp = _calc_limit_price('SHORT', price, h24, l24, high_4h=h4, low_4h=l4)
                hold = SHORT_HOLD_H * 60
                pid, oid, pending = open_order(sym, 'SHORT', price, HARD_TP_PCT, SL_PCT, hold, 'whale-short', lp)
                if not pid and not oid:
                    raise ValueError("blocked by opposite position")
                log.info("WHALE SHORT %-18s @ %.5f (限价%.5f)  score=%d %s  pid=%s oid=%s",
                         sym, price, lp, score_s, detail_s, pid, oid)
                update_state(conn, 'whale', sym, 'whale',
                             state='SHORT', side='SHORT', pid=pid, order_id=oid,
                             entry_p=lp if pending else price,
                             peak_pnl_pct=0.0, entry_time=now_s())
            except Exception as e:
                log.warning("开空失败 %s: %s", sym, e)
    cur.close()

# ── 品种列表 ──────────────────────────────────────────────────────────
_sym_cache: dict = {'syms': [], 'ts': 0.0}
_SYM_BLACKLIST_BASE = {'XVG/USDT', 'TRU/USDT', 'DEGO/USDT', 'ZRO/USDT', 'RIVER/USDT', 'DENT/USDT', 'XAN/USDT', 'SUPER/USDT', 'GUN/USDT', 'UAI/USDT', 'Q/USDT', 'CHIP/USDT', 'SPK/USDT', 'UB/USDT'}  # 币安即将下架 + 反复止损
_db_bl_cache = {'syms': set(), 'ts': 0.0}
_DB_BL_REFRESH_S = 300.0

def _refresh_db_bl() -> set:
    import time as _t
    now = _t.time()
    if (now - _db_bl_cache['ts']) < _DB_BL_REFRESH_S:
        return _db_bl_cache['syms']
    try:
        c2 = pymysql.connect(
            host=os.getenv("DB_HOST","localhost"), port=int(os.getenv("DB_PORT","3306")),
            user=os.getenv("DB_USER",""), password=os.getenv("DB_PASSWORD",""),
            db=os.getenv("DB_NAME",""), charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor, connect_timeout=3,
        )
        try:
            with c2.cursor() as cc:
                cc.execute("SELECT symbol FROM symbol_blacklist WHERE is_active=1")
                _db_bl_cache['syms'] = {r['symbol'] for r in cc.fetchall()}
        finally:
            c2.close()
        _db_bl_cache['ts'] = now
    except Exception as e:
        log.debug("读 symbol_blacklist 失败(旧缓存): %s", e)
    return _db_bl_cache['syms']

def _effective_blacklist() -> set:
    return _SYM_BLACKLIST_BASE | _refresh_db_bl()

def get_universe(cur) -> list:
    """
    按 Binance 24h quoteVolume 排序的前 TOP_N 活跃品种.
    每 30 分钟从 price_stats_24h 刷新一次.
    """
    now = now_s()
    if now - _sym_cache['ts'] < 30 * 60 and _sym_cache['syms']:
        return _sym_cache['syms']

    cur.execute("""
        SELECT symbol FROM price_stats_24h
        WHERE updated_at >= NOW() - INTERVAL 30 MINUTE
          AND quote_volume_24h > 5e6
        ORDER BY quote_volume_24h DESC
        LIMIT 200
    """)
    _bl = _effective_blacklist()
    syms = [r['symbol'] for r in cur.fetchall() if r['symbol'] not in _bl]

    # 补充: 活跃 kline 品种 (防 price_stats 尚未更新)
    # 2026-04-28 修复: 原 syms = [...] 写在 if 外面无缩进, 导致 query 1 已 drain
    # 后再次 fetchall() 返回空, syms 被空列表覆盖, log 永远 "品种列表刷新: 0 个".
    if len(syms) < 10:
        cur.execute("""
            SELECT DISTINCT symbol FROM kline_data
            WHERE timeframe='1h'
              AND open_time >= UNIX_TIMESTAMP(NOW()-INTERVAL 3 HOUR)*1000
            LIMIT 200
        """)
        _bl = _effective_blacklist()
        syms = [r['symbol'] for r in cur.fetchall() if r['symbol'] not in _bl]

    _sym_cache.update({'syms': syms, 'ts': now})
    log.info("品种列表刷新: %d 个", len(syms))
    return syms

# ── 启动同步 ──────────────────────────────────────────────────────────
def _sync_state(conn):
    """启动时从 API 拉取已有 strategy_whale 仓位写入 DB，防止重启重复开单"""
    try:
        d = _api("GET", "/api/futures/positions?status=open")
        for p in (d.get("data") or []):
            src = p.get("source") or ""
            if not src.startswith("strategy_whale:"):
                continue
            # 按 source 后缀分配 stype: longhold-w/-m 必须先判 (子串包含 'w-bottom'/'m-top' 风险)
            if "longhold-w" in src:
                stype = 'longhold-w'
            elif "longhold-m" in src:
                stype = 'longhold-m'
            elif "w-bottom" in src:
                stype = 'w-bottom'
            elif "m-top" in src:
                stype = 'm-top'
            else:
                stype = 'whale'
            sym  = p['symbol']
            side = p['position_side']
            existing = get_or_create(conn, 'whale', sym, stype, {})
            if existing.get('state') not in ('SHORT', 'LONG'):
                update_state(conn, 'whale', sym, stype,
                             state=side, side=side, pid=p['id'],
                             entry_p=float(p['entry_price']),
                             peak_pnl_pct=0.0, entry_time=now_s(), done_time=0.0)
                log.info("同步已有仓位: %s %s pid=%d", sym, side, p['id'])
    except Exception as e:
        log.warning("同步失败: %s", e)

# ── 主循环 ────────────────────────────────────────────────────────────
def main():
    _load_whale_config()
    log.info("=" * 60)
    log.info("Strategy Whale  庄家对抗策略  实盘模拟")
    log.info("A: 跟砸盘做空  B: 跟拉盘做多  账户=%d  杠杆=%dx  保证金=%.0fU",
             ACCOUNT_ID, LEVERAGE, MARGIN)
    trail_desc = " / ".join(f"peak>={t*100:.0f}%回落{p*100:.0f}%" for t, p in TRAIL_TP_TIERS)
    log.info("入场门槛: score>=%d  硬SL=%.0f%%  早期SL=%.0f%%  保本SL(peak>=%.0f%%后回到%.1f%%)  硬TP=%.0f%%",
             ENTRY_SCORE_MIN, SL_PCT*100, EARLY_SL_PCT*100,
             BREAKEVEN_AFTER_PEAK_PCT*100, BREAKEVEN_SL_PCT*100,
             HARD_TP_PCT*100)
    log.info("动态移动止盈: %s", trail_desc)
    log.info("=" * 60)

    init_conn = get_db()
    ensure_table(init_conn)
    _sync_state(init_conn)
    init_conn.close()

    poll_count = 0
    _last_cfg_reload = 0  # 主循环每 60s 重读 system_settings, 改 DB 后无需重启
    while True:
        try:
            conn = get_db()
            cur  = conn.cursor()

            # 动态重载配置 (每 60s 一次)
            try:
                if time.time() - _last_cfg_reload >= 60:
                    _load_whale_config()
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

            universe = get_universe(cur)
            processed = 0
            for sym in universe:
                try:
                    whale_tick(conn, sym)
                except Exception as e:
                    log.warning("whale_tick %s error: %s", sym, e)
                # W 双底子策略（做多、长持）——并行扫，不影响 whale_tick
                try:
                    w_bottom_tick(conn, sym)
                except Exception as e:
                    log.warning("w_bottom_tick %s error: %s", sym, e)
                # M 双顶子策略 (做空、长持, 2026-04-25 新增)
                # 当前禁用: 14 天回测严格阈值 0 命中, 宽松版负期望 -0.40%.
                # 市场处于反弹环境, 没有完整 M 顶形态. 顶部形态明显时取消注释启用.
                # try:
                #     m_top_tick(conn, sym)
                # except Exception as e:
                #     log.warning("m_top_tick %s error: %s", sym, e)
                # longhold 子策略 (W底/M顶 2 周窗口, 1 周持仓, TP20%/SL4%, 2026-04-29 新增)
                # LH_ENABLED=False (system_settings.longhold_enabled) 时 tick 内立即 return
                try:
                    longhold_w_tick(conn, sym)
                except Exception as e:
                    log.warning("longhold_w_tick %s error: %s", sym, e)
                try:
                    longhold_m_tick(conn, sym)
                except Exception as e:
                    log.warning("longhold_m_tick %s error: %s", sym, e)
                processed += 1

            poll_count += 1
            if poll_count % 10 == 1:
                active = list_active(conn, 'whale', 'whale')
                if active:
                    summary = ' | '.join(
                        f"{r['symbol']}:{r.get('side')} pid={r.get('pid')}"
                        for r in active[:8])
                    log.info("持仓[%d]: %s", len(active), summary)
                else:
                    log.info("当前无持仓  扫描品种=%d", processed)

            cur.close()
            conn.close()

        except Exception as e:
            log.error("主循环异常: %s", e, exc_info=True)

        time.sleep(POLL_SECS)

if __name__ == '__main__':
    main()
