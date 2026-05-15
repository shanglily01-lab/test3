"""
Strategy F3 - monitor-only 尾仓善后进程
====================================================
2026-05-15 极简化重构: 删除 F3 子策略 (detect_f3 / f3_tick / 黑白名单 / 信号阈值).
当前职责仅有两个:
  1. _fill_pending_orders - 把残留的 strategy_f3:* PENDING 限价单按价格触发/超时撤单
  2. _close_overdue       - 把超时的 strategy_f3:* open 仓位关掉

所有 strategy_f3:* 尾仓平掉后, 这个进程可以手动 kill 下线.

account_id = 2, source 前缀 'strategy_f3:*'.
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
MARGIN     = 500.0

POLL_SECS = 60

# 限价单管理 (尾仓善后用)
LIMIT_PENDING_MAX_S = 5 * 3600  # 5h 未成交撤单
TRIGGER_CONFIRM_S   = 30
_trigger_first_seen: dict = {}

# 已删除子策略时残留 PENDING 单触发后的 5m 阳线确认开关
# True (默认) = 触发即成交, 让尾仓尽快出清. system_settings.disable_5m_confirm 仍可覆盖.
DISABLE_5M_CONFIRM = True

# limit-fill 时给 max_hold_minutes 用; 12h 是历史 F3 持仓上限, 残留单按此沿用
LIMIT_FILL_HOLD_MIN = 12 * 60


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


def _load_f3_config() -> None:
    """仅读 disable_5m_confirm 这一个通用守卫. 子策略开关 (f3_strategy_enabled) 已不存在."""
    global DISABLE_5M_CONFIRM
    try:
        conn = get_db()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT setting_value FROM system_settings WHERE setting_key='disable_5m_confirm'"
                )
                row = cur.fetchone()
        finally:
            conn.close()
        if row is not None:
            raw = str(row.get('setting_value', '0')).strip().lower()
            DISABLE_5M_CONFIRM = raw in ('1', 'true', 'yes', 'on')
        log.info("strategy_f3 monitor-only 已加载: disable_5m_confirm=%s", DISABLE_5M_CONFIRM)
    except Exception as exc:
        log.error("_load_f3_config 失败, 使用默认值: %s", exc)


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


# ═════════════════════════ 限价单填充 (尾仓) ═════════════════════════
def _fill_pending_orders(conn):
    """扫描 F3 自己残留的 PENDING 限价单, 价格到位则成交; 超时撤单."""
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
                try:
                    update_state(
                        conn, 'f3', sym, 'f3',
                        state='DONE', pid=None, order_id=None,
                        done_time=now_s(), last_reason='cancel',
                    )
                except Exception as _e:
                    log.warning("[f3-cancel-sync] %s 同步 DONE 失败: %s", sym, _e)
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
                _log_order_event(conn, o['order_id'], 'TRIGGER_RETREAT',
                                 cur_price=cur_p, limit_price=limit_p,
                                 detail=f"F3 side={side} pos_side={pos_side}")
            continue
        if DISABLE_5M_CONFIRM:
            _trigger_first_seen.pop(o['id'], None)
        else:
            first_seen_ms = _trigger_first_seen.get(o['id'])
            if first_seen_ms is None:
                _trigger_first_seen[o['id']] = int(now_s() * 1000)
                log.info("F3 触发观察 %-18s cur=%.6f limit=%.6f (等下根 5m 阳线收盘确认)",
                         sym, cur_p, limit_p)
                _log_order_event(conn, o['order_id'], 'TRIGGER_OBSERVING',
                                 cur_price=cur_p, limit_price=limit_p,
                                 detail=f"F3 等阳线收盘确认")
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
            if bar_c <= bar_o:
                log.info("F3 限价 5m 阳线未现, 不成交, 等下次触发: %-18s bar[o=%.6f c=%.6f]",
                         sym, bar_o, bar_c)
                _log_order_event(conn, o['order_id'], '5M_REJECT',
                                 cur_price=cur_p, limit_price=limit_p,
                                 bar_open=bar_o, bar_close=bar_c,
                                 detail=f"F3 LONG 需阳线 实际 close={bar_c} open={bar_o}")
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
                "max_hold_minutes":  LIMIT_FILL_HOLD_MIN,
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
            log.warning("F3 超时平仓异常 %s: %s", r['symbol'], e)


# ═════════════════════════ 主循环 ═════════════════════════
def main():
    log.info("=" * 60)
    log.info("Strategy F3  monitor-only  (尾仓善后, 不再新开仓)")
    log.info("职责: _fill_pending_orders (PENDING 限价单填充/撤) + _close_overdue (超时平仓)")
    log.info("=" * 60)
    _load_f3_config()

    init_conn = get_db()
    ensure_table(init_conn)
    init_conn.close()

    _last_cfg_reload = 0
    while True:
        try:
            conn = get_db()
            try:
                if time.time() - _last_cfg_reload >= 60:
                    _load_f3_config()
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
            log.error("F3 主循环异常: %s", e, exc_info=True)

        time.sleep(POLL_SECS)


if __name__ == '__main__':
    main()
