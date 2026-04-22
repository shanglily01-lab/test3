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
TRAIL_TP_START    = 0.12  # 移动止盈激活阈值
TRAIL_TP_PULLBACK = 0.02  # 从峰值盈利回落多少触发
SHORT_HOLD_H  = 6    # 做空持仓 6小时
LONG_HOLD_H   = 6    # 做多持仓 6小时
COOLDOWN_S    = 6 * 3600
COOLDOWN_SL_S = 12 * 3600

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

def _trail_tp_check(conn, sym: str, pid: int, side: str, entry_p: float, peak_pct: float) -> bool:
    """移动止盈/硬止盈检查。触发则平仓并返回 True。"""
    if not entry_p:
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
    if new_peak >= TRAIL_TP_START and (new_peak - pnl_pct) >= TRAIL_TP_PULLBACK:
        _close_pos(pid, "trail-tp")
        log.info("移动止盈 [WHALE] %-18s  pnl=+%.1f%%  peak=+%.1f%%  回撤%.1f%%",
                 sym, pnl_pct * 100, new_peak * 100, (new_peak - pnl_pct) * 100)
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
    """开仓. 返回 (position_id, order_id, is_pending)"""
    if _has_any_open(sym):
        log.info("跳过开%s %s: 已有持仓", side, sym)
        return None, None, False
    price_ref = limit_price if (limit_price and limit_price > 0) else price
    qty = round(MARGIN * LEVERAGE / price_ref, 6)
    if side == "LONG":
        tp = round(price_ref * (1 + tp_pct), 6)
        sl = round(price_ref * (1 - sl_pct), 6)
    else:
        tp = round(price_ref * (1 - tp_pct), 6)
        sl = round(price_ref * (1 + sl_pct), 6)
    payload = {
        "account_id": ACCOUNT_ID, "symbol": sym,
        "position_side": side, "quantity": qty, "leverage": LEVERAGE,
        "stop_loss_price": sl, "take_profit_price": tp,
        "max_hold_minutes": hold_min,
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

def _calc_limit_price(side, cur_price, high_24h, low_24h):
    if side == 'LONG':
        lp = cur_price * 0.997
        if low_24h and low_24h > 0:
            lp = max(lp, float(low_24h))
    else:
        lp = cur_price * 1.003
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
        update_state(conn, 'whale', sym, 'whale', pid=int(pos_id), order_id=None)
        log.info("WHALE 限价单成交 -> pid=%d  oid=%s", int(pos_id), oid)
        return True, {**row, 'pid': int(pos_id), 'order_id': None}
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
            if age_s > 2 * 3600:  # whale 信号窗口短，2小时未成交即取消
                c2 = conn.cursor()
                c2.execute("UPDATE futures_orders SET status='CANCELLED', cancellation_reason='timeout', canceled_at=NOW(), updated_at=NOW() WHERE id=%s", (o['id'],))
                conn.commit(); c2.close()
                log.info("WHALE 限价单超时取消 %s %s  oid=%s", sym, side, o['order_id'])
                continue
        try:
            cur_p = get_price(sym)
        except Exception:
            continue
        # side 在 DB 里存的是 OPEN_LONG / OPEN_SHORT，转成 LONG / SHORT
        pos_side = side.replace('OPEN_', '') if side.startswith('OPEN_') else side
        triggered = (pos_side == 'LONG' and cur_p <= limit_p) or (pos_side == 'SHORT' and cur_p >= limit_p)
        if not triggered:
            continue
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
            max_hold = LONG_HOLD_H * 60 if pos_side == 'LONG' else SHORT_HOLD_H * 60
            payload = {
                "account_id": ACCOUNT_ID, "symbol": sym,
                "position_side": pos_side,
                "quantity": float(o['quantity'] or 0),
                "leverage": int(o['leverage'] or LEVERAGE),
                "stop_loss_price":   float(o['stop_loss_price']  or 0) or None,
                "take_profit_price": float(o['take_profit_price'] or 0) or None,
                "source": (o.get('order_source') or 'strategy_whale:limit-fill'),
                "fill_price": cur_p, "max_hold_minutes": max_hold,
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
                            ss.get('side') or s, ss.get('entry_p', 0), ss.get('peak_pnl_pct', 0))
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
                lp = _calc_limit_price('SHORT', price, h24, l24)
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
_SYM_BLACKLIST = {'XVG/USDT', 'TRU/USDT', 'DEGO/USDT', 'ZRO/USDT', 'RIVER/USDT', 'DENT/USDT', 'XAN/USDT', 'SUPER/USDT', 'GUN/USDT', 'UAI/USDT'}  # 币安即将下架

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
    syms = [r['symbol'] for r in cur.fetchall() if r['symbol'] not in _SYM_BLACKLIST]

    # 补充: 活跃 kline 品种 (防 price_stats 尚未更新)
    if len(syms) < 10:
        cur.execute("""
            SELECT DISTINCT symbol FROM kline_data
            WHERE timeframe='1h'
              AND open_time >= UNIX_TIMESTAMP(NOW()-INTERVAL 3 HOUR)*1000
            LIMIT 200
        """)
        syms = [r['symbol'] for r in cur.fetchall() if r['symbol'] not in _SYM_BLACKLIST]

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
            sym  = p['symbol']
            side = p['position_side']
            existing = get_or_create(conn, 'whale', sym, 'whale', {})
            if existing.get('state') not in ('SHORT', 'LONG'):
                update_state(conn, 'whale', sym, 'whale',
                             state=side, side=side, pid=p['id'],
                             entry_p=float(p['entry_price']),
                             peak_pnl_pct=0.0, entry_time=now_s(), done_time=0.0)
                log.info("同步已有仓位: %s %s pid=%d", sym, side, p['id'])
    except Exception as e:
        log.warning("同步失败: %s", e)

# ── 主循环 ────────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("Strategy Whale  庄家对抗策略  实盘模拟")
    log.info("A: 跟砸盘做空  B: 跟拉盘做多  账户=%d  杠杆=%dx  保证金=%.0fU",
             ACCOUNT_ID, LEVERAGE, MARGIN)
    log.info("入场门槛: score>=%d  SL=%.0f%%  硬TP=%.0f%%  移动TP: >=%.0f%%后回落%.0f%%触发",
             ENTRY_SCORE_MIN, SL_PCT*100, HARD_TP_PCT*100, TRAIL_TP_START*100, TRAIL_TP_PULLBACK*100)
    log.info("=" * 60)

    init_conn = get_db()
    ensure_table(init_conn)
    _sync_state(init_conn)
    init_conn.close()

    poll_count = 0
    while True:
        try:
            conn = get_db()
            cur  = conn.cursor()

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
