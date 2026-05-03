"""
strategy_bigmid - Gemini AI 决策策略 (2026-04-30 重写)

每 6 小时一轮, 给 top-30 大币种发送结构化市场数据 (15 天日线 / 4 天 1h /
最近 8h 15m+1h K 线, RSI, 成交量), 让 Google Gemini 给 long/short/skip
+ 预期 PnL 建议. 满足 expected_pnl >= 1% 即下限价单 cur ± 0.5%, 持仓 6h,
TP 3% / SL 2%.

历史: 原 strategy_bigmid (BIG whale + MID chase/dump) 近 7 天 12 笔净亏 -77U,
2026-04-30 改造为 Gemini 决策, 进程名 / log 文件名 / 状态机 strategy='bigmid' 不变.

灰度: 默认 disabled (system_settings.gemini_strategy_enabled=0). 总开关打开后才会
每 6h 调 Gemini 一次. _fill_pending_orders / _close_overdue / _settle_closed_positions
继续运行 (处理 PENDING 限价单 / 超时 / 状态翻转).
"""
from __future__ import annotations

import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import datetime
import json
import logging
import os
import time
from typing import Optional

import pymysql
import requests as req
from dotenv import load_dotenv

load_dotenv()

from strategy_state_db import (
    ensure_cooldown_anchor_epoch,
    ensure_table,
    get_or_create,
    list_active,
    list_all_stype,
    update_state,
)

# ── 基础配置 ────────────────────────────────────────────────────
API_BASE   = "http://localhost:9021"
ACCOUNT_ID = 2                # 共用 strategy_live 账户
LEVERAGE   = 5
MARGIN     = 500.0            # 每笔保证金 (USDT)

POLL_SECS              = 60
LIMIT_PENDING_MAX_S    = 3 * 3600   # 限价单 PENDING 超时

# 限价单触发后的观察确认期: 价格穿过挂单价时不立即成交, 等下根 5m K 线收盘方向
# 确认才成交, 避免瞬穿即成交 (接飞刀)
TRIGGER_CONFIRM_S = 30
_trigger_first_seen: dict[int, float] = {}


# ── Gemini API 配置 ─────────────────────────────────────────────
GEMINI_API_KEY    = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL_NAME = os.getenv("GEMINI_MODEL", "gemini-3-flash-preview")

# 标的: top 28 大市值币种 (CMC 排名 + Binance 永续合约都活跃)
# SHIB/PEPE 在 Binance 用 1000SHIB/1000PEPE 形式存在, 暂不入列 (用户可手动加)
GEMINI_TOP30 = [
    "BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT", "XRP/USDT",
    "DOGE/USDT", "ADA/USDT", "TRX/USDT", "AVAX/USDT", "LINK/USDT",
    "TON/USDT", "DOT/USDT", "SUI/USDT", "BCH/USDT", "LTC/USDT",
    "NEAR/USDT", "ICP/USDT", "UNI/USDT", "ETC/USDT", "ATOM/USDT",
    "APT/USDT", "ARB/USDT", "OP/USDT", "FIL/USDT", "HBAR/USDT",
    "INJ/USDT", "RENDER/USDT", "STX/USDT",
]

# ── 仓位/风控参数 (从 system_settings 加载, 这里是默认值) ────────
GEMINI_SL_PCT             = 0.02     # 止损 2%
GEMINI_HARD_TP_PCT        = 0.03     # 止盈 3%
GEMINI_LIMIT_OFFSET_PCT   = 0.005    # 限价 ±0.5%
GEMINI_HOLD_MIN           = 6 * 60   # 持仓 6h
GEMINI_ROUND_INTERVAL_S   = 6 * 3600 # 每 6h 一轮
GEMINI_MIN_PNL_PCT        = 0.01     # 预期 PnL < 1% 跳过
GEMINI_MAX_OPEN_POSITIONS = 5        # 全局同时持仓上限
GEMINI_SYMBOL_COOLDOWN_S  = 24 * 3600 # 同 symbol 入场后 24h 冷却
GEMINI_API_TIMEOUT_S      = 30        # 单次 API 调用超时
GEMINI_PER_SYMBOL_DELAY_S = 4         # 速率限制兜底, 单个 symbol 之间 sleep

# ── 总开关 (system_settings.gemini_strategy_enabled, 默认 OFF) ─
GEMINI_ENABLED = False

# ── 全局总开关 (沿用 strategy_live 同款) ────────────────────────
DISABLE_SL_TP_HOLD = False
DISABLE_5M_CONFIRM = False


# ── 日志 ────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(message)s',
    datefmt='%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('strategy_bigmid.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("strategy_bigmid")


def now_s() -> float:
    return time.time()


# ── DB 工具 ─────────────────────────────────────────────────────
def _db_conn():
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


# ── system_settings 加载 ────────────────────────────────────────
def _load_bigmid_config() -> None:
    """从 system_settings 读取 Gemini 策略全部参数. 进程启动调一次."""
    global DISABLE_SL_TP_HOLD, DISABLE_5M_CONFIRM
    global GEMINI_ENABLED, GEMINI_SL_PCT, GEMINI_HARD_TP_PCT
    global GEMINI_LIMIT_OFFSET_PCT, GEMINI_HOLD_MIN, GEMINI_MIN_PNL_PCT
    global GEMINI_MAX_OPEN_POSITIONS, GEMINI_SYMBOL_COOLDOWN_S
    try:
        conn = _db_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT setting_key, setting_value FROM system_settings "
                    "WHERE setting_key IN ('disable_sl_tp_hold','disable_5m_confirm',"
                    "'gemini_strategy_enabled','gemini_sl_pct','gemini_tp_pct',"
                    "'gemini_limit_offset_pct','gemini_hold_hours','gemini_min_pnl_pct',"
                    "'gemini_max_open_positions','gemini_symbol_cooldown_hours')"
                )
                rows = {r['setting_key']: r['setting_value'] for r in cur.fetchall()}
        finally:
            conn.close()
        _raw = str(rows.get('disable_sl_tp_hold', '0')).strip().lower()
        DISABLE_SL_TP_HOLD = _raw in ('1', 'true', 'yes', 'on')
        _raw_5m = str(rows.get('disable_5m_confirm', '0')).strip().lower()
        DISABLE_5M_CONFIRM = _raw_5m in ('1', 'true', 'yes', 'on')

        _raw_g = str(rows.get('gemini_strategy_enabled', '0')).strip().lower()
        GEMINI_ENABLED = _raw_g in ('1', 'true', 'yes', 'on')
        GEMINI_SL_PCT             = float(rows.get('gemini_sl_pct',           GEMINI_SL_PCT))
        GEMINI_HARD_TP_PCT        = float(rows.get('gemini_tp_pct',           GEMINI_HARD_TP_PCT))
        GEMINI_LIMIT_OFFSET_PCT   = float(rows.get('gemini_limit_offset_pct', GEMINI_LIMIT_OFFSET_PCT))
        _hold_h                   = int(  rows.get('gemini_hold_hours',       GEMINI_HOLD_MIN // 60))
        GEMINI_HOLD_MIN           = _hold_h * 60
        GEMINI_MIN_PNL_PCT        = float(rows.get('gemini_min_pnl_pct',      GEMINI_MIN_PNL_PCT))
        GEMINI_MAX_OPEN_POSITIONS = int(  rows.get('gemini_max_open_positions', GEMINI_MAX_OPEN_POSITIONS))
        _cd_h                     = int(  rows.get('gemini_symbol_cooldown_hours',
                                                   GEMINI_SYMBOL_COOLDOWN_S // 3600))
        GEMINI_SYMBOL_COOLDOWN_S  = _cd_h * 3600

        log.info(
            "strategy_bigmid (Gemini) 参数: enabled=%s SL=%.0f%% TP=%.0f%% offset=%.1f%% "
            "hold=%dh round=每%.0fh min_pnl=%.0f%% max_open=%d cooldown=%dh",
            GEMINI_ENABLED, GEMINI_SL_PCT * 100, GEMINI_HARD_TP_PCT * 100,
            GEMINI_LIMIT_OFFSET_PCT * 100, GEMINI_HOLD_MIN // 60,
            GEMINI_ROUND_INTERVAL_S / 3600, GEMINI_MIN_PNL_PCT * 100,
            GEMINI_MAX_OPEN_POSITIONS, GEMINI_SYMBOL_COOLDOWN_S // 3600,
        )
        if DISABLE_SL_TP_HOLD:
            log.warning("!!! DISABLE_SL_TP_HOLD=ON: 新开仓不设 SL/TP/timeout !!!")
        if DISABLE_5M_CONFIRM:
            log.warning("!!! DISABLE_5M_CONFIRM=ON: 限价单触发即成交 !!!")
        if not GEMINI_ENABLED:
            log.info("!!! gemini_strategy_enabled=0 (OFF): 6h 询问轮不会触发, 仅持仓监控运行 !!!")
    except Exception as exc:
        log.error("_load_bigmid_config 失败, 使用默认值: %s", exc)


# ── API 工具 ────────────────────────────────────────────────────
def _api(method: str, path: str, **kwargs):
    r = req.request(method, f"{API_BASE}{path}", timeout=10, **kwargs)
    r.raise_for_status()
    return r.json()


def _log_order_event(conn, order_id: str, event_type: str,
                     cur_price=None, limit_price=None,
                     bar_open=None, bar_close=None, detail: str = ''):
    """LIMIT 中间事件入库 (order_trigger_events 表). 失败不阻塞."""
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


# ── 通用风控 ────────────────────────────────────────────────────
def _has_any_open(sym: str) -> bool:
    """该标的是否已有任意策略的 open 持仓或 PENDING 挂单"""
    try:
        with _db_conn() as conn:
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
    except Exception as e:
        log.error("_has_any_open %s error: %s", sym, e)
        return True


def _gemini_active_count(conn) -> int:
    """gemini 子策略当前 active 持仓数 (PENDING / SHORT / LONG, 不含 DONE).
    2026-05-02 修: 之前 state != IDLE 把 DONE 也算上, 5 个 DONE 在 cooldown 中也会
    把 active 占满, 死锁 gemini_round. DONE 是冷却态, 不该占 max_open 配额.
    """
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(1) AS n FROM strategy_state "
            "WHERE strategy='bigmid' AND stype='gemini' "
            "  AND state IN ('PENDING','SHORT','LONG')"
        )
        r = cur.fetchone()
        cur.close()
        return int(r['n']) if r else 0
    except Exception:
        return 0


def _close_overdue(conn):
    """关闭本账户所有超时持仓"""
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, symbol, position_side FROM futures_positions "
            "WHERE account_id=%s AND status='open' "
            "  AND source LIKE 'strategy_bigmid:%%' "
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
                log.info("超时平仓 %s %s pid=%d", r['symbol'], r['position_side'], r['id'])
            else:
                log.warning("超时平仓失败 pid=%d: %s", r['id'], resp.text[:100])
        except Exception as e:
            log.error("超时平仓异常 pid=%d: %s", r['id'], e)


# ── 开仓 ────────────────────────────────────────────────────────
def open_order(sym: str, direction: str, entry_p: float,
               sl_pct: float, tp_pct: float, hold_min: int, tag: str,
               limit_p: Optional[float] = None):
    """开仓. 返回 (pid, oid, is_pending). 失败 (None, None, False)."""
    if _has_any_open(sym):
        log.info("跳过开 %s %s: 已有持仓/挂单", direction, sym)
        return None, None, False

    price_ref = limit_p if (limit_p and limit_p > 0) else entry_p
    qty = round(MARGIN * LEVERAGE / price_ref, 6)
    if direction == "LONG":
        tp = round(price_ref * (1 + tp_pct), 8)
        sl = round(price_ref * (1 - sl_pct), 8)
    else:
        tp = round(price_ref * (1 - tp_pct), 8)
        sl = round(price_ref * (1 + sl_pct), 8)

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
        "source":            f"strategy_bigmid:{tag}",
    }
    if limit_p and limit_p > 0:
        payload["limit_price"] = limit_p

    try:
        res = _api("POST", "/api/futures/open", json=payload)
    except Exception as e:
        log.error("开仓请求失败 %s %s: %s", sym, direction, e)
        return None, None, False

    data = res.get("data") or {}
    pid  = data.get("position_id") or data.get("id")
    oid  = data.get("order_id")
    pending = (data.get("status") == "PENDING") or (not pid and bool(oid))
    log.info("开仓 %s %s [gemini] entry=%.6f lp=%s SL=%.6f TP=%.6f qty=%.4f pid=%s oid=%s %s",
             sym, direction, entry_p, limit_p, sl, tp, qty, pid, oid,
             "[PENDING]" if pending else "")
    return pid, oid, pending


# ── Gemini 集成 ─────────────────────────────────────────────────
def _init_gemini_client():
    """进程启动时调一次, 返回 google.genai Client 实例 (失败返回 None).
    用 google-genai 新 SDK (老版 google.generativeai 已 deprecated, 2026-04-30 切换).
    """
    if not GEMINI_API_KEY:
        log.error("GEMINI_API_KEY 未设置, Gemini 策略无法工作")
        return None
    try:
        from google import genai
    except ImportError:
        log.error("google-genai 库未安装, 请 pip install google-genai")
        return None
    try:
        client = genai.Client(api_key=GEMINI_API_KEY)
        log.info("Gemini 客户端就绪: model=%s (google-genai SDK)", GEMINI_MODEL_NAME)
        return client
    except Exception as e:
        log.error("Gemini 客户端初始化失败: %s", e)
        return None


def _fetch_klines(cur, sym: str, timeframe: str, limit: int) -> list:
    """从 kline_data 表拉 K 线, 排除最后一根未完成的. 返回 oldest -> newest."""
    tf_ms = {'15m': 15*60*1000, '1h': 60*60*1000, '1d': 24*60*60*1000}.get(timeframe, 0)
    if not tf_ms:
        return []
    now_ms = int(now_s() * 1000)
    cur.execute("""
        SELECT open_time, open_price, high_price, low_price, close_price, volume
        FROM kline_data
        WHERE symbol=%s AND timeframe=%s
          AND open_time + %s < %s
        ORDER BY open_time DESC LIMIT %s
    """, (sym, timeframe, tf_ms, now_ms, limit))
    return list(reversed(cur.fetchall()))


def _calc_rsi(closes: list[float], period: int = 14) -> Optional[float]:
    """简化版 14 周期 RSI, 返回最新值 (Wilder smoothing)."""
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i-1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100 - 100 / (1 + rs), 2)


def _fetch_market_data(cur, sym: str) -> Optional[dict]:
    """准备喂给 Gemini 的市场数据. 数据不足返回 None."""
    daily_15d = _fetch_klines(cur, sym, '1d', 15)
    h1_4d     = _fetch_klines(cur, sym, '1h', 96)   # 4 天 1h
    m15_8h    = _fetch_klines(cur, sym, '15m', 32)  # 8h 15m
    h1_8h     = _fetch_klines(cur, sym, '1h', 8)    # 8h 1h

    if len(daily_15d) < 14 or len(h1_4d) < 80 or len(m15_8h) < 24 or len(h1_8h) < 6:
        log.debug("Gemini %s 数据不足: daily=%d h1_4d=%d m15=%d h1_8h=%d",
                  sym, len(daily_15d), len(h1_4d), len(m15_8h), len(h1_8h))
        return None

    # 取当前价
    try:
        cur_p = get_price(sym)
    except Exception as e:
        log.warning("Gemini %s 取价失败: %s", sym, e)
        return None

    # 取 24h 涨跌幅
    cur.execute("SELECT change_24h, volume_24h FROM price_stats_24h WHERE symbol=%s LIMIT 1", (sym,))
    r = cur.fetchone()
    change_24h = float(r['change_24h']) if r and r.get('change_24h') is not None else 0.0
    vol_24h = float(r['volume_24h']) if r and r.get('volume_24h') is not None else 0.0

    # RSI
    rsi_1h = _calc_rsi([float(b['close_price']) for b in h1_4d], 14)
    rsi_daily = _calc_rsi([float(b['close_price']) for b in daily_15d], 14) if len(daily_15d) >= 15 else None

    def _to_dicts(bars: list, tf: str) -> list:
        out = []
        for b in bars:
            out.append({
                't': datetime.datetime.fromtimestamp(b['open_time'] / 1000)
                       .strftime('%Y-%m-%d %H:%M' if tf != '1d' else '%Y-%m-%d'),
                'o': round(float(b['open_price']), 8),
                'h': round(float(b['high_price']), 8),
                'l': round(float(b['low_price']), 8),
                'c': round(float(b['close_price']), 8),
                'v': round(float(b['volume'] or 0), 2),
            })
        return out

    return {
        'symbol':         sym,
        'current_price':  round(cur_p, 8),
        'change_24h_pct': round(change_24h, 2),
        'volume_24h':     round(vol_24h, 2),
        'rsi_1h':         rsi_1h,
        'rsi_daily':      rsi_daily,
        'daily_15d':      _to_dicts(daily_15d, '1d'),
        'h1_4d':          _to_dicts(h1_4d, '1h'),
        'm15_8h':         _to_dicts(m15_8h, '15m'),
        'h1_8h':          _to_dicts(h1_8h, '1h'),
    }


def _build_gemini_prompt(data: dict) -> str:
    """构造 prompt, 强制 Gemini 输出 JSON."""
    sym = data['symbol']

    def _fmt_klines(klines: list, header: str) -> str:
        lines = [header]
        lines.append(f"{'time':<17} {'open':>14} {'high':>14} {'low':>14} {'close':>14} {'vol':>14}")
        for k in klines:
            lines.append(f"{k['t']:<17} {k['o']:>14} {k['h']:>14} {k['l']:>14} {k['c']:>14} {k['v']:>14}")
        return "\n".join(lines)

    return f"""You are a quantitative crypto futures trading analyst. Analyze the following market data for {sym} and recommend a 6-hour trade decision.

CURRENT STATE
  current_price: {data['current_price']}
  24h change: {data['change_24h_pct']}%
  24h volume: {data['volume_24h']}
  RSI(14, 1h): {data['rsi_1h']}
  RSI(14, daily): {data['rsi_daily']}

{_fmt_klines(data['daily_15d'], "DAILY (15 days, oldest -> newest):")}

{_fmt_klines(data['h1_4d'], "1H KLINES (last 4 days, oldest -> newest):")}

{_fmt_klines(data['m15_8h'], "15M KLINES (last 8h, oldest -> newest):")}

{_fmt_klines(data['h1_8h'], "1H KLINES (last 8h, oldest -> newest):")}

TASK
Decide whether to open a SHORT or LONG futures position with these constraints:
  - Hold time: 6 hours
  - Take profit: +3% from entry
  - Stop loss: -2% from entry
  - Limit order at current_price * (1 - 0.005) for LONG, or current_price * (1 + 0.005) for SHORT

Output ONLY a single valid JSON object, no markdown fence, no extra text:
{{
  "direction": "long" | "short" | "skip",
  "expected_pnl_pct": <float, between 0 and 0.05, expected PnL within the 6h hold>,
  "confidence": <float between 0 and 1>,
  "reason": "<brief 1-sentence reason in Chinese>"
}}

If conviction is low or market is unclear, return direction="skip" with expected_pnl_pct=0.
"""


def _call_gemini(client, prompt: str) -> Optional[dict]:
    """调用 Gemini, 解析 JSON. 任何错误返回 None.
    用 google-genai 新 SDK 的 client.models.generate_content + types.GenerateContentConfig.
    """
    if not client:
        return None
    text = ''
    try:
        from google.genai import types
        config = types.GenerateContentConfig(
            response_mime_type='application/json',
            http_options=types.HttpOptions(timeout=GEMINI_API_TIMEOUT_S * 1000),  # 毫秒
        )
        resp = client.models.generate_content(
            model=GEMINI_MODEL_NAME,
            contents=prompt,
            config=config,
        )
        text = (resp.text or "").strip()
        # 兜底: 偶尔 ```json...``` 包裹
        if text.startswith("```"):
            text = text.strip("`").lstrip("json").strip()
        sig = json.loads(text)
        d = str(sig.get("direction", "")).lower()
        if d not in ("long", "short", "skip"):
            log.warning("Gemini 返回非法 direction=%s text=%s", d, text[:200])
            return None
        sig["direction"] = d
        sig["expected_pnl_pct"] = float(sig.get("expected_pnl_pct", 0) or 0)
        sig["confidence"]       = float(sig.get("confidence", 0) or 0)
        sig["reason"]           = str(sig.get("reason", ""))[:200]
        return sig
    except json.JSONDecodeError:
        log.warning("Gemini 返回非 JSON: %s", (text or "")[:200])
        return None
    except Exception as e:
        log.warning("Gemini API 错误: %s", e)
        return None


def gemini_round(conn, model):
    """每 6h 一轮主入口: 扫 GEMINI_TOP30, 调 Gemini, 满足条件下单."""
    if not GEMINI_ENABLED:
        return
    if not model:
        log.warning("gemini_round 跳过 (model 未就绪)")
        return

    active = _gemini_active_count(conn)
    if active >= GEMINI_MAX_OPEN_POSITIONS:
        log.info("gemini_round 跳过 (active=%d 已达上限 %d)", active, GEMINI_MAX_OPEN_POSITIONS)
        return

    log.info("=== Gemini 一轮开始, 当前 active=%d, 上限 %d ===", active, GEMINI_MAX_OPEN_POSITIONS)
    skipped, opened = 0, 0
    for sym in GEMINI_TOP30:
        if _has_any_open(sym):
            continue
        # 同 symbol cooldown 检查 (gemini stype 单独维护)
        ss = get_or_create(conn, 'bigmid', sym, 'gemini', {
            'state': 'IDLE', 'pid': None, 'order_id': None, 'entry_p': 0.0,
            'peak_pnl_pct': 0.0, 'entry_time': 0, 'done_time': 0,
        })
        s = ss.get('state') or 'IDLE'
        if s == 'DONE':
            anchor = ensure_cooldown_anchor_epoch(conn, 'bigmid', sym, 'gemini', ss, now_s())
            if now_s() - anchor < GEMINI_SYMBOL_COOLDOWN_S:
                continue
            update_state(conn, 'bigmid', sym, 'gemini', state='IDLE')
            s = 'IDLE'
        if s != 'IDLE':
            continue  # PENDING/LONG/SHORT 不重复入场

        # 准备数据
        cur = conn.cursor()
        try:
            data = _fetch_market_data(cur, sym)
        finally:
            cur.close()
        if not data:
            continue

        # 调 Gemini
        prompt = _build_gemini_prompt(data)
        signal = _call_gemini(model, prompt)
        if GEMINI_PER_SYMBOL_DELAY_S > 0:
            time.sleep(GEMINI_PER_SYMBOL_DELAY_S)
        if not signal:
            continue
        if signal['direction'] == 'skip':
            log.info("Gemini SKIP %-14s exp=%.2f%% conf=%.2f reason=%s",
                     sym, signal['expected_pnl_pct'] * 100, signal['confidence'],
                     signal['reason'][:80])
            skipped += 1
            continue
        if signal['expected_pnl_pct'] < GEMINI_MIN_PNL_PCT:
            log.info("Gemini 跳过 %-14s 预期 %.2f%% < %.0f%% (dir=%s reason=%s)",
                     sym, signal['expected_pnl_pct'] * 100, GEMINI_MIN_PNL_PCT * 100,
                     signal['direction'], signal['reason'][:60])
            skipped += 1
            continue

        # 下单
        side = 'LONG' if signal['direction'] == 'long' else 'SHORT'
        try:
            price = get_price(sym)
        except Exception:
            continue
        lp = price * (1 - GEMINI_LIMIT_OFFSET_PCT) if side == 'LONG' \
             else price * (1 + GEMINI_LIMIT_OFFSET_PCT)
        lp = round(lp, 8)

        pid, oid, pending = open_order(sym, side, price,
                                       GEMINI_SL_PCT, GEMINI_HARD_TP_PCT,
                                       GEMINI_HOLD_MIN, "gemini", lp)
        if not (pid or oid):
            continue

        log.info("[GEMINI] %-14s %s @ %.6f lp=%.6f exp=+%.2f%% conf=%.2f reason=%s",
                 sym, side, price, lp, signal['expected_pnl_pct'] * 100,
                 signal['confidence'], signal['reason'][:80])
        update_state(conn, 'bigmid', sym, 'gemini',
                     state='PENDING' if pending else side,
                     pid=pid, order_id=oid, side=side,
                     entry_p=lp if pending else price,
                     peak_pnl_pct=0.0, entry_time=now_s(),
                     last_reason=f"exp={signal['expected_pnl_pct']:.3f},conf={signal['confidence']:.2f}")
        opened += 1
        if active + opened >= GEMINI_MAX_OPEN_POSITIONS:
            log.info("gemini_round 提前结束 (达到 max_open=%d)", GEMINI_MAX_OPEN_POSITIONS)
            break

    log.info("=== Gemini 一轮结束: opened=%d skipped=%d total=%d ===",
             opened, skipped, len(GEMINI_TOP30))


# ── 限价单 fill 流程 ────────────────────────────────────────────
def _fill_pending_orders(conn):
    """扫描本策略 PENDING 限价单, 按规则成交或撤单"""
    cur = conn.cursor()
    cur.execute("""
        SELECT id, order_id, symbol, side, leverage, quantity,
               price AS limit_price, stop_loss_price, take_profit_price,
               order_source, created_at
        FROM futures_orders
        WHERE account_id=%s AND status='PENDING' AND order_type='LIMIT'
          AND order_source LIKE 'strategy_bigmid:%%'
        ORDER BY created_at ASC
    """, (ACCOUNT_ID,))
    orders = cur.fetchall()
    cur.close()
    if not orders:
        return

    for o in orders:
        sym     = o["symbol"]
        side    = o["side"]
        limit_p = float(o["limit_price"] or 0)
        if limit_p <= 0:
            continue

        # 超时撤单
        if o["created_at"]:
            age = (datetime.datetime.now() - o["created_at"]).total_seconds()
            if age > LIMIT_PENDING_MAX_S:
                c2 = conn.cursor()
                c2.execute("""UPDATE futures_orders SET status='CANCELLED',
                              cancellation_reason='timeout', canceled_at=NOW(),
                              updated_at=NOW() WHERE id=%s""", (o["id"],))
                conn.commit(); c2.close()
                log.info("超时撤单 %s %s oid=%s age=%.0fs", sym, side, o["order_id"], age)
                # 同步回收 state, 防御性显式写一遍 (主 tick 也会走 _settle_cancelled_pending
                # 兜底, 但写两次幂等无害, 早一步释放槽位).
                try:
                    update_state(
                        conn, "bigmid", sym, "gemini",
                        state="IDLE", pid=None, order_id=None,
                        entry_p=0, entry_time=0,
                        last_reason="order_cancelled",
                    )
                except Exception as _e:
                    log.warning("[bigmid-cancel-sync] %s 同步 IDLE 失败: %s", sym, _e)
                continue

        try:
            cur_p = get_price(sym)
        except Exception:
            continue

        pos_side = side.replace("OPEN_", "") if side.startswith("OPEN_") else side
        triggered = (pos_side == "LONG" and cur_p <= limit_p) or \
                    (pos_side == "SHORT" and cur_p >= limit_p)
        if not triggered:
            if _trigger_first_seen.pop(o["id"], None) is not None:
                log.info("BIGMID 限价单触发回撤, 重新等待 %s %s cur=%.6f limit=%.6f",
                         sym, side, cur_p, limit_p)
                _log_order_event(conn, o['order_id'], 'TRIGGER_RETREAT',
                                 cur_price=cur_p, limit_price=limit_p,
                                 detail=f"BIGMID side={side} pos_side={pos_side}")
            continue

        # 5m 收盘方向确认 (DISABLE_5M_CONFIRM=ON 时跳过)
        if DISABLE_5M_CONFIRM:
            _trigger_first_seen.pop(o["id"], None)
        else:
            first_seen_ms = _trigger_first_seen.get(o["id"])
            if first_seen_ms is None:
                _trigger_first_seen[o["id"]] = int(time.time() * 1000)
                log.info("BIGMID 限价单触发观察 %s %s cur=%.6f limit=%.6f (等下根 5m %s线收盘确认)",
                         sym, side, cur_p, limit_p,
                         '阴' if pos_side == 'SHORT' else '阳')
                _log_order_event(conn, o['order_id'], 'TRIGGER_OBSERVING',
                                 cur_price=cur_p, limit_price=limit_p,
                                 detail=f"BIGMID side={side} 等{('阴' if pos_side == 'SHORT' else '阳')}线收盘确认")
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
                log.info("BIGMID 限价 5m 反向不成交, 等下次触发: %s %s bar[o=%.6f c=%.6f]",
                         sym, side, bar_o, bar_c)
                _log_order_event(conn, o['order_id'], '5M_REJECT',
                                 cur_price=cur_p, limit_price=limit_p,
                                 bar_open=bar_o, bar_close=bar_c,
                                 detail=f"BIGMID side={side} 需{('阴' if pos_side == 'SHORT' else '阳')}线 实际 close={bar_c} open={bar_o}")
                _trigger_first_seen.pop(o["id"], None)
                continue
            _trigger_first_seen.pop(o["id"], None)

        # 乐观锁: PENDING -> FILLING
        c2 = conn.cursor()
        affected = c2.execute("""UPDATE futures_orders SET status='FILLING',
                                 updated_at=NOW() WHERE id=%s AND status='PENDING'""",
                              (o["id"],))
        conn.commit(); c2.close()
        if not affected:
            continue

        # SL/TP 按实际成交价重算 (limit_p 滑点保护)
        sl = float(o["stop_loss_price"] or 0) or None
        tp = float(o["take_profit_price"] or 0) or None
        if sl and tp and abs(cur_p - limit_p) / limit_p > 0.001:
            if pos_side == "LONG":
                sl_r = (limit_p - sl) / limit_p
                tp_r = (tp - limit_p) / limit_p
                sl = round(cur_p * (1 - sl_r), 8); tp = round(cur_p * (1 + tp_r), 8)
            else:
                sl_r = (sl - limit_p) / limit_p
                tp_r = (limit_p - tp) / limit_p
                sl = round(cur_p * (1 + sl_r), 8); tp = round(cur_p * (1 - tp_r), 8)

        try:
            qty = float(o["quantity"] or 0)
            lev = int(o["leverage"] or LEVERAGE)
            if DISABLE_SL_TP_HOLD:
                sl_out, tp_out, hold_out = None, None, 0
            else:
                sl_out, tp_out, hold_out = sl, tp, GEMINI_HOLD_MIN
            payload = {
                "account_id": ACCOUNT_ID, "symbol": sym,
                "position_side": pos_side, "quantity": qty, "leverage": lev,
                "stop_loss_price": sl_out, "take_profit_price": tp_out,
                "source": o.get("order_source") or "strategy_bigmid:gemini-fill",
                "fill_price": cur_p,
                "max_hold_minutes": hold_out,
            }
            res  = _api("POST", "/api/futures/open", json=payload)
            data = res.get("data") or {}
            pid  = data.get("position_id") or data.get("id")
            if pid:
                c2 = conn.cursor()
                c2.execute("""UPDATE futures_orders SET status='FILLED',
                              avg_fill_price=%s, fill_time=NOW(),
                              executed_quantity=quantity, executed_value=total_value,
                              position_id=%s, updated_at=NOW() WHERE id=%s""",
                           (cur_p, pid, o["id"]))
                conn.commit(); c2.close()
                log.info("限价单成交 %s %s @ %.6f pid=%s", sym, side, cur_p, pid)
                # 更新 strategy_state: PENDING -> SHORT/LONG, pid
                update_state(conn, 'bigmid', sym, 'gemini',
                             state=pos_side, pid=int(pid), order_id=None,
                             entry_p=cur_p, entry_time=now_s())
            else:
                c2 = conn.cursor()
                c2.execute("UPDATE futures_orders SET status='PENDING', updated_at=NOW() WHERE id=%s",
                           (o["id"],))
                conn.commit(); c2.close()
        except Exception as e:
            log.warning("填充异常回退 PENDING %s %s: %s", sym, side, e)
            c2 = conn.cursor()
            c2.execute("UPDATE futures_orders SET status='PENDING', updated_at=NOW() WHERE id=%s",
                       (o["id"],))
            conn.commit(); c2.close()


# ── 平仓 -> 重置状态机 ──────────────────────────────────────────
def _settle_closed_positions(conn):
    """扫描本策略 closed 持仓, 翻 strategy_state DONE 启动冷却"""
    cur = conn.cursor()
    cur.execute("""
        SELECT id, symbol, position_side, source, notes, close_time
        FROM futures_positions
        WHERE account_id=%s AND status='closed'
          AND source LIKE 'strategy_bigmid:%%'
          AND close_time >= NOW() - INTERVAL 30 MINUTE
    """, (ACCOUNT_ID,))
    rows = cur.fetchall()
    cur.close()
    for r in rows:
        ss = get_or_create(conn, "bigmid", r["symbol"], "gemini", {"state": "IDLE"})
        if ss.get("state") in ("LONG", "SHORT", "PENDING"):
            update_state(conn, "bigmid", r["symbol"], "gemini",
                         state="DONE", done_time=now_s(),
                         last_reason=(r.get("notes") or "")[:32])


def _settle_cancelled_pending(conn):
    """清理: strategy_state.state=PENDING 但对应 futures_orders 已 CANCELLED.
    把卡死的 PENDING 翻成 IDLE, 让下一轮 gemini_round 能重新入场.

    2026-05-01 修复: 限价单超时撤单后状态机没翻, 导致 active_count 永远=5 卡死轮次.
    2026-05-02 改: 拆两步查询, 避免 strategy_state.order_id (utf8mb4_0900_ai_ci) 和
                   futures_orders.order_id (utf8mb4_unicode_ci) 跨表比较 collation 冲突.
    """
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT symbol, order_id FROM strategy_state
            WHERE strategy='bigmid' AND stype='gemini' AND state='PENDING'
              AND order_id IS NOT NULL
        """)
        pending_rows = cur.fetchall()
        cur.close()
        if not pending_rows:
            return
        for r in pending_rows:
            c2 = conn.cursor()
            try:
                c2.execute("SELECT status FROM futures_orders WHERE order_id=%s LIMIT 1",
                           (r['order_id'],))
                order = c2.fetchone()
            finally:
                c2.close()
            if order and (order.get('status') or '').upper() in ('CANCELLED', 'REJECTED'):
                log.info("Gemini 限价单已撤, 清空状态机让下次重试: %s oid=%s",
                         r['symbol'], r['order_id'])
                update_state(conn, "bigmid", r['symbol'], "gemini",
                             state="IDLE", pid=None, order_id=None,
                             entry_p=0, entry_time=0,
                             last_reason='order_cancelled')
    except Exception as e:
        log.warning("_settle_cancelled_pending 异常: %s", e)


# ── 主循环 ──────────────────────────────────────────────────────
def main():
    _load_bigmid_config()
    log.info("=" * 60)
    log.info("strategy_bigmid (Gemini AI) 启动 account=%d LEVERAGE=%dx MARGIN=%.0f",
             ACCOUNT_ID, LEVERAGE, MARGIN)
    log.info("Top-%d symbol: %s", len(GEMINI_TOP30),
             ", ".join(GEMINI_TOP30[:5]) + " ...")
    log.info("=" * 60)

    model = _init_gemini_client()

    init_conn = _db_conn()
    try:
        ensure_table(init_conn)
    except Exception as e:
        log.error("ensure_table 失败: %s", e)
    init_conn.close()

    last_round_ts = 0  # 启动后立即跑首轮 (开发友好). 生产可改 now_s() 让首轮等 6h
    _last_cfg_reload = 0  # 主循环每 60s 重读 system_settings, 改 DB 后无需重启
    while True:
        try:
            conn = _db_conn()

            # 动态重载配置 (每 60s 一次)
            try:
                if time.time() - _last_cfg_reload >= 60:
                    _load_bigmid_config()
                    _last_cfg_reload = time.time()
            except Exception as e:
                log.warning("配置重载失败: %s", e)

            try: _fill_pending_orders(conn)
            except Exception as e: log.warning("_fill_pending_orders %s", e)
            try: _close_overdue(conn)
            except Exception as e: log.warning("_close_overdue %s", e)
            try: _settle_closed_positions(conn)
            except Exception as e: log.warning("_settle_closed_positions %s", e)
            try: _settle_cancelled_pending(conn)
            except Exception as e: log.warning("_settle_cancelled_pending %s", e)

            # 每 6h 调 Gemini 一轮
            if GEMINI_ENABLED and (now_s() - last_round_ts >= GEMINI_ROUND_INTERVAL_S):
                try:
                    gemini_round(conn, model)
                except Exception as e:
                    log.error("gemini_round 异常: %s", e, exc_info=True)
                last_round_ts = now_s()

            conn.close()
        except pymysql.err.Error as e:
            log.error("主循环 DB 错误: %s", e)
            time.sleep(5)
        except Exception as e:
            log.error("主循环异常: %s", e)

        time.sleep(POLL_SECS)


if __name__ == "__main__":
    main()
