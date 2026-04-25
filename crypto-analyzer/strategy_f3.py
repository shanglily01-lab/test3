"""
Strategy F3 - W 底小涨带量 做多 (独立进程)
====================================================
基于 2026-04-24 历史回测数据提炼的形态:
  1. 前期 7 天最大跌幅 >= 20%
  2. 最近 24h 未续跌超 5% 且已脱离最低点 (筑底完成)
  3. 24h 涨跌 <= +2% (关键: 还未反弹, F3 抓的是"反弹前一刻")
  4. 最后一根 15m 阳线, 幅度 1%~3% (小涨优于大涨)
  5. 触发 bar 量比 24h 均量 1.5~3.0x (微放量优于爆量)
  6. 不在 F3 专属黑名单

7 天回测 474 笔 / 胜率 45% / 期望 +0.33%/笔
加过滤条件后样本 (24h<=2% + 量 1.5-3x + 小阳) 期望 +1.2%/笔左右

参数:
  SL 5% / TP 10% / 持仓 12h / 冷却 4h / 限价下挂 0.5%
  全局最多 3 仓
  account_id = 2 (共享 paper 账户, 和 strategy_live/whale/bigmid 同)
  source 前缀: strategy_f3:f3-entry

架构仿照 strategy_whale.py - 独立 main 循环, 不依赖其他策略文件.
"""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import os
import time
import logging
import datetime as _dt
import pymysql
import requests as req
from dotenv import load_dotenv
load_dotenv()

from strategy_state_db import (
    ensure_table,
    get_or_create,
    update_state,
    list_active,
    ensure_cooldown_anchor_epoch,
)

# ═════════════════════════ 日志 ═════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    datefmt='%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('strategy_f3.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger('f3')


# ═════════════════════════ 账户与 API ═════════════════════════
API_BASE   = "http://localhost:9021"
ACCOUNT_ID = 2
LEVERAGE   = 5
MARGIN     = 500.0   # 每笔保证金 USDT

POLL_SECS = 60       # 主循环间隔 (秒)


# ═════════════════════════ F3 信号阈值 ═════════════════════════
# 数据窗口
F3_LOOKBACK_BARS        = 7 * 24 * 4     # 672 根 15m = 7 天
F3_RECENT_24H_BARS      = 24 * 4         # 96 根 = 24h

# 筑底条件
F3_MIN_DROP_PCT         = 0.20           # 7 天最大跌幅 >= 20%
F3_RECENT_24H_MIN_PCT   = -0.05          # 24h 跌不超 5%
F3_NOT_AT_LOW_MULT      = 1.01           # 当前价 > 24h 最低 * 1.01 (脱离最低)

# 反弹前置条件 (核心!)
F3_CH_24H_MAX           = 0.02           # 24h 涨跌 <= +2%, 未已反弹

# 触发 bar 条件
F3_BODY_MIN             = 0.01           # 阳线 >= 1%
F3_BODY_MAX             = 0.03           # 阳线 < 3% (剔除大阳)
F3_VOL_MULT_MIN         = 1.5            # 量比 >= 1.5x (微放量)
F3_VOL_MULT_MAX         = 3.0            # 量比 < 3x (剔除爆量)


# ═════════════════════════ 仓位参数 ═════════════════════════
F3_SL_PCT            = 0.05
F3_TP_PCT            = 0.10
F3_HOLD_MIN          = 12 * 60           # 12h
F3_COOLDOWN_S        = 4 * 3600          # 4h
F3_MAX_OPEN          = 3                 # 全局最多 3 仓
F3_LIMIT_OFFSET_PCT  = 0.005             # 下挂 0.5%

# 限价单管理
LIMIT_PENDING_MAX_S  = 3 * 3600          # 3h 未成交撤单 (2026-04-25 1h → 3h)
                                         # F3 抓"反弹前的小阳带量", 底部震荡等待期长, 给信号更多成交机会
TRIGGER_CONFIRM_S    = 30                # 限价触发 30s 观察
_trigger_first_seen: dict = {}


# ═════════════════════════ F3 专属黑白名单 ═════════════════════════
# 基于 2026-04-24 7 天回测数据 (replay_4_forms + diag_f3_deep):
# BLACKLIST: 7 天样本 >=3 且累计 pnl <= -10%, F3 在这些币上系统性亏钱
F3_BLACKLIST = {
    'PENGU/USDT', 'EVAA/USDT', 'IR/USDT', 'DUSK/USDT',
    'GPS/USDT', 'MYX/USDT', 'AAVE/USD',
}
# WHITELIST: F3 形态在这些币上有实证正期望 (7 天样本 >=3, 高胜率 / 高净收益)
# 白名单优先级 > 任何黑名单: F3 的形态识别和 live/whale 用的趋势信号不同,
# 即使某币在全局黑名单里 (trend 策略翻过车), F3 形态下仍可做.
F3_WHITELIST = {
    'SPK/USDT', 'NEIRO/USDT', 'AVNT/USDT', 'ZBT/USDT', 'KERNEL/USDT',
    'TREE/USDT', 'STRK/USDT', 'ENJ/USDT', 'TRIA/USDT',
}

# 全局永久黑名单 (复用 strategy_live 的 BASE)
# 注: SPK/UB/Q/CHIP 等是别的策略的教训, F3 白名单可以覆盖.
GLOBAL_BLACKLIST_BASE = {
    'DENT/USDT', 'XAN/USDT', 'SUPER/USDT', 'GUN/USDT', 'UAI/USDT',
    'AAVE/USD', 'BTC/USD', 'XVG/USDT', 'TRU/USDT', 'DEGO/USDT',
    'ZRO/USDT', 'RIVER/USDT', 'Q/USDT', 'CHIP/USDT', 'SPK/USDT', 'UB/USDT',
}

_DB_BL_CACHE = {'syms': set(), 'ts': 0.0}
_DB_BL_REFRESH_S = 300.0


def _refresh_db_bl() -> set:
    now = time.time()
    if now - _DB_BL_CACHE['ts'] < _DB_BL_REFRESH_S:
        return _DB_BL_CACHE['syms']
    try:
        conn = get_db()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT symbol FROM symbol_blacklist WHERE is_active=1"
                )
                syms = {r['symbol'] for r in cur.fetchall()}
        finally:
            conn.close()
    except Exception:
        syms = _DB_BL_CACHE['syms']
    _DB_BL_CACHE.update({'syms': syms, 'ts': now})
    return syms


def _effective_blacklist() -> set:
    """F3 黑名单 = 全局 BASE + F3 专属 + DB 动态, 但白名单覆盖之 (白名单最高优先级)."""
    merged = GLOBAL_BLACKLIST_BASE | F3_BLACKLIST | _refresh_db_bl()
    return merged - F3_WHITELIST


# ═════════════════════════ 基础设施 ═════════════════════════
def now_s() -> float:
    return time.time()


def get_db():
    return pymysql.connect(
        host=os.getenv('DB_HOST'), port=int(os.getenv('DB_PORT', 3306)),
        user=os.getenv('DB_USER'), password=os.getenv('DB_PASSWORD', ''),
        db=os.getenv('DB_NAME'), charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor,
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


def _has_any_open(sym: str) -> bool:
    """检查 account_id=2 下是否已有该 symbol 的 open/PENDING (跨策略)"""
    try:
        conn = get_db()
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
        log.error("_has_any_open %s error: %s", sym, e)
        return False


def _get_24h_stats(cur, sym):
    cur.execute(
        "SELECT high_24h, low_24h FROM price_stats_24h WHERE symbol=%s "
        "ORDER BY updated_at DESC LIMIT 1",
        (sym,),
    )
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
       F3 仅 LONG.
    """
    if side == 'LONG':
        fallback = cur_price * (1 - F3_LIMIT_OFFSET_PCT)
        if low_4h and low_4h > 0:
            qi_shang = low_4h * 1.30
            lp = min(qi_shang, fallback)
        else:
            lp = fallback
        if low_24h and low_24h > 0:
            lp = max(lp, float(low_24h))
    else:
        fallback = cur_price * (1 + F3_LIMIT_OFFSET_PCT)
        if high_4h and high_4h > 0:
            ba_xia = high_4h * 0.80
            lp = max(ba_xia, fallback)
        else:
            lp = fallback
        if high_24h and high_24h > 0:
            lp = min(lp, float(high_24h))
    return round(lp, 8)


def _get_15m_bars(cur, sym: str, limit: int) -> list:
    """最近 N 根已完成的 15m K 线 (含 volume)"""
    now_ms = int(now_s() * 1000)
    cur.execute(
        """SELECT open_price, high_price, low_price, close_price, volume
           FROM kline_data
           WHERE symbol=%s AND timeframe='15m'
             AND open_time + 900000 < %s
           ORDER BY open_time DESC LIMIT %s""",
        (sym, now_ms, limit),
    )
    return list(reversed(cur.fetchall()))


def open_order(sym, price, limit_price):
    """F3 开 LONG 仓. 返回 (position_id, order_id, is_pending)."""
    if _has_any_open(sym):
        log.info("F3 跳过 %-18s: 已有持仓", sym)
        return None, None, False
    price_ref = limit_price if (limit_price and limit_price > 0) else price
    qty = round(MARGIN * LEVERAGE / price_ref, 6)
    tp = round(price_ref * (1 + F3_TP_PCT), 8)
    sl = round(price_ref * (1 - F3_SL_PCT), 8)
    payload = {
        "account_id":        ACCOUNT_ID,
        "symbol":            sym,
        "position_side":     "LONG",
        "quantity":          qty,
        "leverage":          LEVERAGE,
        "stop_loss_price":   sl,
        "take_profit_price": tp,
        "max_hold_minutes":  F3_HOLD_MIN,
        "source":            "strategy_f3:f3-entry",
    }
    if limit_price and limit_price > 0:
        payload["limit_price"] = limit_price
    try:
        res = _api("POST", "/api/futures/open", json=payload)
    except Exception as e:
        log.warning("F3 open_order %s 异常: %s", sym, e)
        return None, None, False
    data = res.get("data") or {}
    pid  = data.get("position_id") or data.get("id")
    oid  = data.get("order_id")
    is_pending = (data.get("status") == "PENDING") or (not pid and bool(oid))
    return pid, oid, is_pending


# ═════════════════════════ 限价单填充 ═════════════════════════
def _check_pending_db(conn, sym):
    """检查本策略的挂单是否成交/取消, 转换状态"""
    row = get_or_create(conn, 'f3', sym, 'f3', {})
    oid = row.get('order_id')
    if not oid:
        return True, row
    if row.get('pid'):
        update_state(conn, 'f3', sym, 'f3', order_id=None)
        return True, {**row, 'order_id': None}
    cur = conn.cursor()
    cur.execute(
        "SELECT status, position_id FROM futures_orders WHERE order_id=%s LIMIT 1",
        (oid,),
    )
    order = cur.fetchone()
    cur.close()
    if not order:
        update_state(conn, 'f3', sym, 'f3', order_id=None)
        return True, {**row, 'order_id': None}
    st     = (order.get('status') or '').upper()
    pos_id = order.get('position_id')
    if st == 'FILLED' and pos_id:
        update_state(conn, 'f3', sym, 'f3', state='LONG',
                     pid=int(pos_id), order_id=None)
        log.info("F3 限价单成交 %-18s  pid=%d  oid=%s",
                 sym, int(pos_id), oid)
        return True, {**row, 'state': 'LONG', 'pid': int(pos_id), 'order_id': None}
    if st in ('CANCELLED', 'REJECTED'):
        ts = now_s()
        update_state(conn, 'f3', sym, 'f3',
                     state='DONE', pid=None, order_id=None,
                     done_time=ts, last_reason='cancel')
        return True, {**row, 'state': 'DONE', 'pid': None, 'order_id': None,
                      'done_time': ts, 'last_reason': 'cancel'}
    return False, row


def _fill_pending_orders(conn):
    """扫描 F3 自己的 PENDING 限价单, 价格到位则成交"""
    cur = conn.cursor()
    cur.execute(
        """SELECT id, order_id, symbol, side, leverage, quantity,
                  price AS limit_price, stop_loss_price, take_profit_price,
                  order_source, created_at
           FROM futures_orders
           WHERE account_id=%s AND status='PENDING' AND order_type='LIMIT'
             AND order_source LIKE 'strategy_f3:%%'
           ORDER BY created_at ASC""",
        (ACCOUNT_ID,),
    )
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
        # 超时撤单
        if o['created_at']:
            age_s = (_dt.datetime.now() - o['created_at']).total_seconds()
            if age_s > LIMIT_PENDING_MAX_S:
                c2 = conn.cursor()
                c2.execute(
                    "UPDATE futures_orders SET status='CANCELLED', "
                    "cancellation_reason='timeout', canceled_at=NOW(), "
                    "updated_at=NOW() WHERE id=%s",
                    (o['id'],),
                )
                conn.commit()
                c2.close()
                log.info("F3 限价单超时撤单 %-18s oid=%s", sym, o['order_id'])
                continue
        try:
            cur_p = get_price(sym)
        except Exception:
            continue
        pos_side = side.replace('OPEN_', '') if side.startswith('OPEN_') else side
        triggered = (pos_side == 'LONG' and cur_p <= limit_p) \
                    or (pos_side == 'SHORT' and cur_p >= limit_p)
        if not triggered:
            if _trigger_first_seen.pop(o['id'], None) is not None:
                log.info("F3 触发回撤, 重新等待 %-18s cur=%.6f limit=%.6f",
                         sym, cur_p, limit_p)
            continue
        # 已触发: 等下根 5m K 线收盘, F3 仅做多, 需要阳线确认 (2026-04-25)
        first_seen_ms = _trigger_first_seen.get(o['id'])
        if first_seen_ms is None:
            _trigger_first_seen[o['id']] = int(now_s() * 1000)
            log.info("F3 触发观察 %-18s cur=%.6f limit=%.6f (等下根 5m 阳线收盘确认)",
                     sym, cur_p, limit_p)
            continue
        next_bar_open_ms  = (int(first_seen_ms) // 300000) * 300000 + 300000
        next_bar_close_ms = next_bar_open_ms + 300000
        if int(now_s() * 1000) < next_bar_close_ms:
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
        # F3 是 LONG, 必须阳线 (close > open) 才确认
        if bar_c <= bar_o:
            log.info("F3 限价 5m 阳线未现, 不成交, 等下次触发: %-18s bar[o=%.6f c=%.6f]",
                     sym, bar_o, bar_c)
            _trigger_first_seen.pop(o['id'], None)
            continue
        _trigger_first_seen.pop(o['id'], None)
        # 乐观锁锁定订单
        c2 = conn.cursor()
        affected = c2.execute(
            """UPDATE futures_orders SET status='FILLING', updated_at=NOW()
               WHERE id=%s AND status='PENDING'""",
            (o['id'],),
        )
        conn.commit()
        c2.close()
        if not affected:
            continue
        pos_id = None
        try:
            _sl_raw = float(o['stop_loss_price']  or 0) or None
            _tp_raw = float(o['take_profit_price'] or 0) or None
            payload = {
                "account_id":        ACCOUNT_ID,
                "symbol":            sym,
                "position_side":     pos_side,
                "quantity":          float(o['quantity'] or 0),
                "leverage":          int(o['leverage'] or LEVERAGE),
                "stop_loss_price":   _sl_raw,
                "take_profit_price": _tp_raw,
                "max_hold_minutes":  F3_HOLD_MIN,
                "source":            (o.get('order_source') or 'strategy_f3:limit-fill'),
                "fill_price":        cur_p,
            }
            res  = _api("POST", "/api/futures/open", json=payload)
            data = res.get("data") or {}
            pos_id = data.get("position_id") or data.get("id")
            if pos_id:
                c2 = conn.cursor()
                c2.execute(
                    """UPDATE futures_orders
                       SET status='FILLED', avg_fill_price=%s, fill_time=NOW(),
                           executed_quantity=quantity, executed_value=total_value,
                           position_id=%s, updated_at=NOW()
                       WHERE id=%s""",
                    (cur_p, pos_id, o['id']),
                )
                conn.commit()
                c2.close()
                log.info("F3 限价单成交 %-18s @ %.6f  pid=%s  oid=%s",
                         sym, cur_p, pos_id, o['order_id'])
            else:
                c2 = conn.cursor()
                c2.execute(
                    "UPDATE futures_orders SET status='PENDING', updated_at=NOW() "
                    "WHERE id=%s",
                    (o['id'],),
                )
                conn.commit()
                c2.close()
                log.warning("F3 成交无 pos_id, 回退 PENDING %-18s oid=%s",
                            sym, o['order_id'])
        except Exception as e:
            try:
                c2 = conn.cursor()
                c2.execute(
                    "UPDATE futures_orders SET status='PENDING', updated_at=NOW() "
                    "WHERE id=%s",
                    (o['id'],),
                )
                conn.commit()
                c2.close()
            except Exception:
                pass
            log.warning("F3 成交异常 %-18s: %s", sym, e)


def _close_overdue(conn):
    """关本账户本策略所有超时持仓"""
    try:
        cur = conn.cursor()
        cur.execute(
            """SELECT id, symbol, position_side FROM futures_positions
               WHERE account_id=%s AND status='open' AND source LIKE 'strategy_f3:%%'
                 AND timeout_at IS NOT NULL AND timeout_at <= NOW()""",
            (ACCOUNT_ID,),
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
                log.info("F3 超时平仓 %s %s pid=%d",
                         r['symbol'], r['position_side'], r['id'])
        except Exception as e:
            log.warning("F3 超时平仓失败 pid=%s: %s", r['id'], e)


# ═════════════════════════ F3 形态识别 ═════════════════════════
def detect_f3(bars: list) -> dict | None:
    """
    识别 F3 形态. 返回特征 dict 或 None.
    bars 须是按时间升序的 15m K 线, 至少 F3_LOOKBACK_BARS 根.
    """
    if len(bars) < F3_LOOKBACK_BARS:
        return None
    window = bars[-F3_LOOKBACK_BARS:]
    highs  = [float(b['high_price']) for b in window]
    lows   = [float(b['low_price'])  for b in window]
    closes = [float(b['close_price']) for b in window]
    vols   = [float(b['volume'] or 0) for b in window]

    # 1. 7 天最大跌幅
    w_high = max(highs); w_low = min(lows)
    if w_high <= 0:
        return None
    drop_pct = (w_high - w_low) / w_high
    if drop_pct < F3_MIN_DROP_PCT:
        return None

    # 2. 最近 24h 未续跌 + 已脱离 24h 最低
    n = len(window)
    if n < F3_RECENT_24H_BARS:
        return None
    recent24_bars  = window[-F3_RECENT_24H_BARS:]
    r24_open_first = float(recent24_bars[0]['open_price'])
    r24_low        = min(float(b['low_price']) for b in recent24_bars)
    r24_close_last = float(recent24_bars[-1]['close_price'])
    if r24_open_first <= 0:
        return None
    ch_24h = (r24_close_last - r24_open_first) / r24_open_first
    if ch_24h < F3_RECENT_24H_MIN_PCT:
        return None                                # 24h 仍在续跌
    if r24_close_last < r24_low * F3_NOT_AT_LOW_MULT:
        return None                                # 仍在最低点附近

    # 3. 24h 未反弹 (核心)
    if ch_24h > F3_CH_24H_MAX:
        return None

    # 4. 最后一根阳线, 幅度 1%~3%
    last = window[-1]
    o = float(last['open_price']); c = float(last['close_price'])
    v = float(last['volume'] or 0)
    if o <= 0 or c <= o:
        return None
    body_pct = (c - o) / o
    if body_pct < F3_BODY_MIN or body_pct >= F3_BODY_MAX:
        return None

    # 5. 量比 1.5~3.0x
    avg_vol = sum(vols[-F3_RECENT_24H_BARS:]) / F3_RECENT_24H_BARS
    if avg_vol <= 0:
        return None
    vol_ratio = v / avg_vol
    if vol_ratio < F3_VOL_MULT_MIN or vol_ratio >= F3_VOL_MULT_MAX:
        return None

    return {
        'drop_pct': drop_pct,
        'ch_24h': ch_24h,
        'body_pct': body_pct,
        'vol_ratio': vol_ratio,
        'entry_price': c,
    }


def _f3_active_count(conn) -> int:
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(1) AS n FROM strategy_state "
            "WHERE strategy='f3' AND stype='f3' AND state IN ('PENDING','LONG')"
        )
        r = cur.fetchone()
        cur.close()
        return int(r['n']) if r else 0
    except Exception:
        return 0


def f3_tick(conn, sym: str):
    """F3 每个品种的扫描 + 状态机"""
    # 黑名单硬拒
    if sym in _effective_blacklist():
        return

    ss = get_or_create(conn, 'f3', sym, 'f3', {
        'state': 'IDLE', 'pid': None, 'order_id': None,
        'entry_p': 0.0, 'peak_pnl_pct': 0.0,
        'entry_time': 0.0, 'done_time': 0.0,
    })
    s = ss.get('state') or 'IDLE'

    # DONE 冷却 4h
    if s == 'DONE':
        anchor = ensure_cooldown_anchor_epoch(conn, 'f3', sym, 'f3', ss, now_s())
        if now_s() - anchor > F3_COOLDOWN_S:
            update_state(conn, 'f3', sym, 'f3', state='IDLE')
            s = 'IDLE'
        else:
            return

    # 处理挂单 / 持仓状态
    ok, ss = _check_pending_db(conn, sym)
    if not ok:
        return
    s = ss.get('state') or 'IDLE'

    # 持仓中: 让 PositionSLTPMonitor 按 SL/TP 自动平仓;
    # F3 自己只监听仓位是否已关闭, 转 DONE 启动冷却.
    if s == 'LONG' and ss.get('pid'):
        status, pnl, notes = get_pos_status(ss['pid'])
        if status is None:
            return
        if status == 'open':
            return
        # 仓位已关闭 → DONE
        pnl_v = pnl or 0
        if notes and '手动' in str(notes):
            reason = 'manual'
        elif pnl_v > 0:
            reason = 'TP/trail'
        else:
            reason = 'SL/timeout'
        log.info("F3 %-18s %s -> DONE  pnl=%+.2f  notes=%s",
                 sym, reason, pnl_v, (notes or '')[:30])
        update_state(conn, 'f3', sym, 'f3',
                     state='DONE', pid=None, order_id=None,
                     done_time=now_s(),
                     last_reason=('SL' if pnl_v <= 0 else 'TP'))
        return

    if s != 'IDLE':
        return

    # 全局 F3 仓位数限制
    if _f3_active_count(conn) >= F3_MAX_OPEN:
        return

    # 拉 15m kline 识别 F3
    cur = conn.cursor()
    try:
        bars = _get_15m_bars(cur, sym, limit=F3_LOOKBACK_BARS + 10)
    finally:
        cur.close()
    if len(bars) < F3_LOOKBACK_BARS:
        return
    sig = detect_f3(bars)
    if not sig:
        return

    # 取限价
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

    # 开仓
    pid, oid, pending = open_order(sym, price, lp)
    if not (pid or oid):
        return

    wl_tag = ' [WL]' if sym in F3_WHITELIST else ''
    log.info(
        "F3 入场 LONG  %-18s%s  @ %.6f (限价 %.6f)  "
        "drop=%.1f%%  24h=%+.1f%%  body=%.2f%%  vol=%.2fx  "
        "pid=%s oid=%s",
        sym, wl_tag, price, lp,
        sig['drop_pct'] * 100, sig['ch_24h'] * 100,
        sig['body_pct'] * 100, sig['vol_ratio'],
        pid, oid,
    )
    update_state(
        conn, 'f3', sym, 'f3',
        state='PENDING' if pending else 'LONG',
        pid=pid, order_id=oid, side='LONG',
        entry_p=(lp if pending else price),
        peak_pnl_pct=0.0, entry_time=now_s(),
    )


# ═════════════════════════ 品种池 ═════════════════════════
_sym_cache = {'syms': [], 'ts': 0.0}
SYM_REFRESH_SECS = 30 * 60


def get_universe(cur) -> list:
    """按 24h quoteVolume 排前 200, 每 30 分钟刷新"""
    now = now_s()
    if now - _sym_cache['ts'] < SYM_REFRESH_SECS and _sym_cache['syms']:
        return _sym_cache['syms']
    bl = _effective_blacklist()
    cur.execute(
        """SELECT symbol FROM price_stats_24h
           WHERE updated_at >= NOW() - INTERVAL 30 MINUTE
             AND quote_volume_24h > 5e6
           ORDER BY quote_volume_24h DESC
           LIMIT 200"""
    )
    syms = [r['symbol'] for r in cur.fetchall() if r['symbol'] not in bl]
    if len(syms) < 10:
        cur.execute(
            """SELECT DISTINCT symbol FROM kline_data
               WHERE timeframe='15m'
                 AND open_time >= UNIX_TIMESTAMP(NOW()-INTERVAL 3 HOUR)*1000
               LIMIT 200"""
        )
        syms = [r['symbol'] for r in cur.fetchall() if r['symbol'] not in bl]

    # 白名单强制加入 (即使不在 top 200 volume 也要扫)
    existing = set(syms)
    for w in F3_WHITELIST:
        if w not in existing:
            syms.append(w)

    _sym_cache.update({'syms': syms, 'ts': now})
    log.info("F3 品种列表刷新: %d 个 (黑名单 %d, 白名单 %d 强制加入)",
             len(syms), len(bl), len(F3_WHITELIST))
    return syms


# ═════════════════════════ 启动同步 ═════════════════════════
def _sync_state(conn):
    """启动时从 API 同步已有 F3 仓位, 防止重启重复开单"""
    try:
        d = _api("GET", "/api/futures/positions?status=open")
        for p in (d.get("data") or []):
            src = p.get("source") or ""
            if not src.startswith("strategy_f3:"):
                continue
            sym  = p['symbol']
            side = p['position_side']
            existing = get_or_create(conn, 'f3', sym, 'f3', {})
            if existing.get('state') not in ('LONG', 'SHORT', 'PENDING'):
                update_state(conn, 'f3', sym, 'f3',
                             state=side, pid=p['id'],
                             entry_p=float(p['entry_price']),
                             peak_pnl_pct=0.0,
                             entry_time=now_s(), done_time=0.0)
                log.info("F3 同步已有仓位 %s %s pid=%d", sym, side, p['id'])
    except Exception as e:
        log.warning("F3 _sync_state 异常: %s", e)


# ═════════════════════════ 主循环 ═════════════════════════
def main():
    log.info("=" * 60)
    log.info("Strategy F3  W 底小涨带量 做多  (paper 模拟)")
    log.info("账户=%d  杠杆=%dx  保证金=%.0fU  SL=%.0f%%  TP=%.0f%%  持仓=%dh",
             ACCOUNT_ID, LEVERAGE, MARGIN,
             F3_SL_PCT * 100, F3_TP_PCT * 100, F3_HOLD_MIN // 60)
    log.info("入场阈值: drop>=%.0f%%  24h>=%.0f%% 且 <=%.0f%%  body 1-3%%  vol 1.5-3x",
             F3_MIN_DROP_PCT * 100, F3_RECENT_24H_MIN_PCT * 100, F3_CH_24H_MAX * 100)
    log.info("全局最多 %d 仓  冷却 %dh  限价下挂 %.1f%%",
             F3_MAX_OPEN, F3_COOLDOWN_S // 3600, F3_LIMIT_OFFSET_PCT * 100)
    log.info("F3 专属黑名单 (%d): %s",
             len(F3_BLACKLIST), ', '.join(sorted(F3_BLACKLIST)))
    log.info("F3 白名单仅日志 (%d): %s",
             len(F3_WHITELIST), ', '.join(sorted(F3_WHITELIST)))
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
                    f3_tick(conn, sym)
                except Exception as e:
                    log.warning("f3_tick %s error: %s", sym, e)
                processed += 1

            poll_count += 1
            if poll_count % 10 == 1:
                active = list_active(conn, 'f3', 'f3')
                if active:
                    summary = ' | '.join(
                        f"{r['symbol']}:{r.get('state')} pid={r.get('pid')}"
                        for r in active[:8])
                    log.info("F3 当前活跃[%d]: %s", len(active), summary)
                else:
                    log.info("F3 无持仓  扫描品种=%d", processed)

            cur.close()
            conn.close()
        except Exception as e:
            log.error("F3 主循环异常: %s", e, exc_info=True)

        time.sleep(POLL_SECS)


if __name__ == '__main__':
    main()
