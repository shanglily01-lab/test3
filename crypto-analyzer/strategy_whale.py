"""
Strategy Whale - monitor-only 尾仓善后进程
====================================================
2026-05-15 极简化重构: 删除全部 6 个子策略 (whale-short / w-bottom / m-top /
longhold-w / longhold-m / rev4d / swan). 当前职责仅有两个:
  1. _fill_pending_orders - 把残留的 strategy_whale:* PENDING 限价单按价格触发/超时撤单
  2. _close_overdue       - 把超时的 open 仓位关掉 (按 timeout_at)

所有 strategy_whale:* 尾仓平掉后, 这个进程可以手动 kill 下线.

account_id = 2, source 前缀 'strategy_whale:*'.
"""
import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import os, time, logging
import pymysql, requests as req
from dotenv import load_dotenv
load_dotenv()

from strategy_state_db import (
    ensure_table,
    update_state,
)

# ── 账户与 API ────────────────────────────────────────────────────────
API_BASE    = "http://localhost:9021"
ACCOUNT_ID  = 2
LEVERAGE    = 5
MARGIN      = 500.0

POLL_SECS   = 60

# 限价单管理 (尾仓善后)
TRIGGER_CONFIRM_S = 30
_trigger_first_seen: dict[int, float] = {}

# 残留 longhold 限价单的 TTL 与 fill 后 hold (沿用原值)
LH_LIMIT_TTL_S = 24 * 3600
LH_HOLD_MIN    = 7 * 24 * 60

# 其它 (whale-short / w-bottom / m-top / rev4d / swan) 残留单 TTL
DEFAULT_LIMIT_TTL_S = 3 * 3600

# 通用守卫开关 (system_settings, 60s reload)
DISABLE_SL_TP_HOLD = False
DISABLE_5M_CONFIRM = True   # 默认放开 5m 确认, 让尾仓尽快出清

# 通用 hold 时长 (非 longhold 残留单 fill 时使用, 沿用原 6h)
SHORT_HOLD_H = 6
LONG_HOLD_H  = 6


def _load_whale_config() -> None:
    """仅读两个通用守卫开关. 6 个子策略相关 setting 已 migration 035 删除."""
    global DISABLE_SL_TP_HOLD, DISABLE_5M_CONFIRM
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
                    "SELECT setting_key, setting_value FROM system_settings "
                    "WHERE setting_key IN ('disable_sl_tp_hold','disable_5m_confirm')"
                )
                rows = {r['setting_key']: r['setting_value'] for r in cur.fetchall()}
        finally:
            conn.close()
        raw_a = str(rows.get('disable_sl_tp_hold', '0')).strip().lower()
        DISABLE_SL_TP_HOLD = raw_a in ('1', 'true', 'yes', 'on')
        raw_b = str(rows.get('disable_5m_confirm', '1')).strip().lower()
        DISABLE_5M_CONFIRM = raw_b in ('1', 'true', 'yes', 'on')
        log.info("strategy_whale monitor-only 已加载: disable_sl_tp_hold=%s disable_5m_confirm=%s",
                 DISABLE_SL_TP_HOLD, DISABLE_5M_CONFIRM)
    except Exception as exc:
        log.error("_load_whale_config 失败, 使用默认值: %s", exc)


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
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", ""),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", ""),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=5,
    )


def _api(method, path, **kw):
    r = req.request(method, f"{API_BASE}{path}", timeout=10, **kw)
    r.raise_for_status()
    return r.json()


def _log_order_event(conn, order_id: str, event_type: str,
                     cur_price=None, limit_price=None,
                     bar_open=None, bar_close=None, detail: str = ''):
    """LIMIT 中间事件入库. 失败不阻塞."""
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


# ── 限价单填充 + 超时 ─────────────────────────────────────────────────
def _close_overdue(conn):
    """关闭本账户所有超时持仓 (按 timeout_at)."""
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


def _fill_pending_orders(conn):
    """扫描 strategy_whale:* PENDING 限价单, 价格到位则成交, 超时撤单."""
    cur = conn.cursor()
    cur.execute("""
        SELECT id, order_id, symbol, side, leverage, quantity,
               price AS limit_price, stop_loss_price, take_profit_price,
               order_source, created_at
        FROM futures_orders
        WHERE account_id=%s AND status='PENDING' AND order_type='LIMIT'
          AND order_source LIKE 'strategy_whale:%%'
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
            ttl_s = LH_LIMIT_TTL_S if 'longhold' in (o.get('order_source') or '') else DEFAULT_LIMIT_TTL_S
            if age_s > ttl_s:
                c2 = conn.cursor()
                c2.execute("UPDATE futures_orders SET status='CANCELLED', cancellation_reason='timeout', canceled_at=NOW(), updated_at=NOW() WHERE id=%s", (o['id'],))
                conn.commit(); c2.close()
                log.info("WHALE 限价单超时取消 %s %s  oid=%s  age=%.1fh ttl=%.1fh",
                         sym, side, o['order_id'], age_s / 3600.0, ttl_s / 3600.0)
                src = (o.get('order_source') or '')
                if src.startswith('strategy_whale:'):
                    stype = src.split(':', 1)[1].strip() or 'whale'
                    try:
                        update_state(
                            conn, 'whale', sym, stype,
                            state='DONE', pid=None, order_id=None,
                            done_time=time.time(), last_reason='cancel',
                        )
                    except Exception as _e:
                        log.warning("[whale-cancel-sync] %s stype=%s 同步 DONE 失败: %s",
                                    sym, stype, _e)
                continue
        try:
            cur_p = get_price(sym)
        except Exception:
            continue
        pos_side = side.replace('OPEN_', '') if side.startswith('OPEN_') else side
        triggered = (pos_side == 'LONG' and cur_p <= limit_p) or (pos_side == 'SHORT' and cur_p >= limit_p)
        if not triggered:
            if _trigger_first_seen.pop(o['id'], None) is not None:
                log.info("WHALE 限价单触发回撤, 重新等待 %s %s cur=%.5f limit=%.5f",
                         sym, side, cur_p, limit_p)
                _log_order_event(conn, o['order_id'], 'TRIGGER_RETREAT',
                                 cur_price=cur_p, limit_price=limit_p,
                                 detail=f"WHALE side={side} pos_side={pos_side}")
            continue
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
        c2 = conn.cursor()
        affected = c2.execute("""UPDATE futures_orders
            SET status='FILLING', updated_at=NOW()
            WHERE id=%s AND status='PENDING'""", (o['id'],))
        conn.commit(); c2.close()
        if not affected:
            log.info("WHALE 限价单已被处理, 跳过 %s %s oid=%s", sym, side, o['order_id'])
            continue
        pos_id = None
        try:
            _src = (o.get('order_source') or '')
            if 'longhold' in _src:
                max_hold = LH_HOLD_MIN
            else:
                max_hold = LONG_HOLD_H * 60 if pos_side == 'LONG' else SHORT_HOLD_H * 60
            _sl_raw = float(o['stop_loss_price']  or 0) or None
            _tp_raw = float(o['take_profit_price'] or 0) or None
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
                log.warning("WHALE 限价单成交无 pos_id, 回退 PENDING %s %s oid=%s", sym, side, o['order_id'])
        except Exception as e:
            try:
                c2 = conn.cursor()
                c2.execute("UPDATE futures_orders SET status='PENDING', updated_at=NOW() WHERE id=%s", (o['id'],))
                conn.commit(); c2.close()
            except Exception:
                pass
            log.warning("WHALE 限价单成交异常 %s: %s", sym, e)


# ── 主循环 ────────────────────────────────────────────────────────────
def main():
    _load_whale_config()
    log.info("=" * 60)
    log.info("Strategy Whale  monitor-only  (尾仓善后, 不再新开仓)")
    log.info("职责: _fill_pending_orders + _close_overdue, source=strategy_whale:*")
    log.info("=" * 60)

    init_conn = get_db()
    ensure_table(init_conn)
    init_conn.close()

    _last_cfg_reload = 0
    while True:
        try:
            conn = get_db()
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

            conn.close()
        except Exception as e:
            log.error("主循环异常: %s", e, exc_info=True)

        time.sleep(POLL_SECS)


if __name__ == '__main__':
    main()
