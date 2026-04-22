"""
实盘策略运行器 - 真实下单到 localhost:9021
A. 追击: 5m K线检测涨幅>=4% -> 真实开多, TP梯度5%-10%, SL 8%转空
B. 顶部做空: 48h涨>=80% + 6h无新高 -> 真实开空, 24h固定平仓

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

from strategy_state_db import ensure_table, get_or_create, update_state, delete_state, list_active

# ── 配置 ─────────────────────────────────────────────────────────
API_BASE    = "http://localhost:9021"
ACCOUNT_ID  = 2
LEVERAGE    = 5
MARGIN      = 500.0   # 每笔保证金 (USDT)

# 品种黑名单
SYMBOL_BLACKLIST = {'DENT/USDT', 'XAN/USDT', 'SUPER/USDT', 'GUN/USDT', 'UAI/USDT', 'AAVE/USD', 'BTC/USD', 'XVG/USDT', 'TRU/USDT', 'DEGO/USDT', 'ZRO/USDT', 'RIVER/USDT'}

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
    syms = [r['symbol'] for r in cur.fetchall()
            if r['symbol'] not in SYMBOL_BLACKLIST]
    _sym_cache['syms'] = syms
    _sym_cache['updated_at'] = now
    log.info("品种列表刷新: %d 个活跃品种", len(syms))
    return syms

# 追击参数
CHASE_PUMP_BARS = 24
CHASE_PUMP_PCT  = 0.12
CHASE_SL_PCT    = 0.08
LONG_HOLD_MIN   = 12 * 60
SHORT_HOLD_MIN  = 24 * 60
CHASE_MAX_HOLD  = LONG_HOLD_MIN
CHASE_COOLDOWN  = 2 * 3600

# 顶部做空参数
TOP_PUMP_THRESH = 0.80
TOP_NO_NEW_H    = 6
TOP_LOOKBACK_H  = 48
TOP_HOLD_H      = 16
TOP_SL_PCT      = 0.12
TOP_SIGNAL_AGE  = 6 * 3600

# 追跌参数
DUMP_BARS     = 48
DUMP_PCT      = 0.10
DUMP_SL_PCT   = 0.08
DUMP_MAX_HOLD = SHORT_HOLD_MIN

# 移动止盈参数（三个策略共用）
HARD_TP_PCT       = 0.20  # 硬止盈: 盈利达到即平仓
TRAIL_TP_START    = 0.12  # 移动止盈激活阈值
TRAIL_TP_PULLBACK = 0.02  # 从峰值盈利回落多少触发
DUMP_COOLDOWN = 4 * 3600


POLL_SECS       = 60
TOPSHORT_EVERY  = 5

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
    price_ref = limit_price if (limit_price and limit_price > 0) else entry_price
    qty = round(MARGIN * LEVERAGE / price_ref, 6)
    if direction == "LONG":
        tp = round(price_ref * (1 + tp_pct), 6)
        sl = round(price_ref * (1 - sl_pct), 6)
    else:
        tp = round(price_ref * (1 - tp_pct), 6)
        sl = round(price_ref * (1 + sl_pct), 6)
    payload = {
        "account_id":        ACCOUNT_ID,
        "symbol":            sym,
        "position_side":     direction,
        "quantity":          qty,
        "leverage":          LEVERAGE,
        "stop_loss_price":   sl,
        "take_profit_price": tp,
        "max_hold_minutes":  hold_min,
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


def _calc_limit_price(side, cur_price, high_24h, low_24h, pct=0.003):
    """限价挂单: LONG 往下 pct; SHORT 往上 pct; 受 24H 区间约束"""
    if side == 'LONG':
        lp = cur_price * (1 - pct)
        if low_24h and low_24h > 0:
            lp = max(lp, float(low_24h))
    else:
        lp = cur_price * (1 + pct)
        if high_24h and high_24h > 0:
            lp = min(lp, float(high_24h))
    return round(lp, 8)

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
        update_state(conn, 'live', sym, stype,
                     state='IDLE', pid=None, order_id=None, done_time=now_s())
        return True, {**row, 'state': 'IDLE', 'pid': None, 'order_id': None}
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
            if age_s > 6 * 3600:  # 追击单6小时未成交即取消
                c2 = conn.cursor()
                c2.execute("""UPDATE futures_orders
                    SET status='CANCELLED', cancellation_reason='timeout',
                        canceled_at=NOW(), updated_at=NOW() WHERE id=%s""", (o['id'],))
                conn.commit(); c2.close()
                log.info("限价单超时取消 %s %s  oid=%s", sym, side, o['order_id'])
                continue
        try:
            cur_p = get_price(sym)
        except Exception:
            continue
        pos_side = side.replace('OPEN_', '') if side.startswith('OPEN_') else side
        triggered = (pos_side == 'LONG' and cur_p <= limit_p) or (pos_side == 'SHORT' and cur_p >= limit_p)
        if not triggered:
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
            src = (o.get('order_source') or 'strategy_live:limit-fill')
            max_hold = LONG_HOLD_MIN if pos_side == 'LONG' else SHORT_HOLD_MIN
            payload = {
                "account_id": ACCOUNT_ID, "symbol": sym,
                "position_side": pos_side, "quantity": qty, "leverage": lev,
                "stop_loss_price": sl, "take_profit_price": tp, "source": src,
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

def _trail_tp_check(conn, account, strategy, sym, pid, side, entry_p, peak_pct):
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
        update_state(conn, account, sym, strategy, peak_pnl_pct=new_peak)
    if pnl_pct >= HARD_TP_PCT:
        close_order(pid, "hard-tp")
        log.info("硬止盈 [%s] %-18s  pnl=+%.1f%%", strategy.upper(), sym, pnl_pct * 100)
        return True
    if new_peak >= TRAIL_TP_START and (new_peak - pnl_pct) >= TRAIL_TP_PULLBACK:
        close_order(pid, "trail-tp")
        log.info("移动止盈 [%s] %-18s  pnl=+%.1f%%  peak=+%.1f%%  回撤%.1f%%",
                 strategy.upper(), sym, pnl_pct * 100, new_peak * 100, (new_peak - pnl_pct) * 100)
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
        if now_s() - (cs.get('done_time') or 0) > CHASE_COOLDOWN:
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
                            s, cs.get('entry_p', 0), cs.get('peak_pnl_pct', 0))
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

    now_ms = int(now_s() * 1000)
    BAR_MS = 5 * 60 * 1000
    cur = conn.cursor()
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

    bar_close_ms = ts[i] + BAR_MS
    bar_age_s = (now_ms - bar_close_ms) / 1000
    if bar_age_s > 300:
        cur.close()
        return

    price = get_price(sym)
    h24, l24 = _get_24h_stats(cur, sym)
    cur.close()
    lp = _calc_limit_price("LONG", price, h24, l24, pct=0.03)
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

    # 检查已有顶空仓位
    active_rows = list_active(conn, 'live', 'topshort')
    for pos in active_rows:
        sym = pos['symbol']
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
                    log.info("TOPSHORT 限价单取消 %-18s  oid=%s -> 丢弃", sym, pos.get('order_id'))
                    delete_state(conn, 'live', sym, 'topshort')
                    continue
            if not pos.get('pid'):
                continue  # 仍在挂单中
        if not pos.get('pid'):
            delete_state(conn, 'live', sym, 'topshort')
            continue
        status, pnl, notes = get_pos_status(pos['pid'])
        if status is None:
            continue  # API 错误，保留状态
        if status == 'open':
            _trail_tp_check(conn, 'live', 'topshort', sym, pos['pid'],
                            'SHORT', pos.get('entry_p', 0), pos.get('peak_pnl_pct', 0))
            continue
        else:
            pnl_pct = (pnl or 0) / MARGIN * 100
            reason = "手动" if (notes and '手动' in str(notes)) else status
            log.info("TOPSHORT 平仓  %-18s  pid=%d  pnl=%+.1f%%  reason=%s",
                     sym, pos['pid'], pnl_pct, reason)
            delete_state(conn, 'live', sym, 'topshort')

    # 扫描新信号
    open_syms = {r['symbol'] for r in list_active(conn, 'live', 'topshort')}

    cur = conn.cursor()
    for sym in active_syms:
        if sym in open_syms:
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
            h24, l24 = _get_24h_stats(cur, sym)
            lp = _calc_limit_price("SHORT", price, h24, l24, pct=0.03)
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
        if now_s() - (ds.get('done_time') or 0) > DUMP_COOLDOWN:
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
                            s, ds.get('entry_p', 0), ds.get('peak_pnl_pct', 0))
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
    h24, l24 = _get_24h_stats(cur, sym)
    cur.close()
    lp = _calc_limit_price("SHORT", price, h24, l24, pct=0.03)
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
    log.info("=" * 56)
    log.info("Strategy Live Runner  实盘下单模式")
    log.info("A: 追多(2h涨>=12%%, 持仓4h)  B: 顶空(80%%泵+6h无新高)  C: 追跌(4h跌>=10%%, 持仓12h)")
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

            # 汇总当前持仓
            chase_active = list_active(conn, 'live', 'chase')
            dump_active  = list_active(conn, 'live', 'dump')
            top_active   = list_active(conn, 'live', 'topshort')
            if chase_active or dump_active or top_active:
                summary = []
                for r in chase_active:
                    summary.append("chase:%s %s pid=%s" % (r['symbol'], r['state'], r.get('pid')))
                for r in dump_active:
                    summary.append("dump:%s %s pid=%s" % (r['symbol'], r['state'], r.get('pid')))
                for r in top_active:
                    summary.append("top:%s SHORT pid=%s" % (r['symbol'], r.get('pid')))
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
