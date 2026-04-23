# -*- coding: utf-8 -*-
"""
strategy_bigmid 执行式回测 - 带挂单/填充/SL/TP/持仓/冷却的完整模拟

回答根本问题：如果过去 72h 这些信号真下单，会赚还是亏？

模拟逻辑：
  1. 信号判定用 tier 对应 tf (BIG=1h, MID=15m) 的每根 K 收盘时点
  2. 挂限价单 (cur * (1 ± limit_offset))
  3. 用 5m K 线遍历信号之后的时间：
     - limit 被触及 → 填充 (检查反向滑点熔断)
     - 填充后 5m 粒度判 SL/TP/超时/移动止盈
  4. 一个子策略 (chase/dump) 同时最多一笔仓位 + 一张挂单
  5. 平仓后 4h 冷却
  6. 挂单 2h 超时撤单
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pymysql

from strategy_bigmid import (
    TIER_PARAMS, TIER_BIG_MIN_VOL, TIER_MID_MIN_VOL,
    BIGMID_EXCLUDES, MEME_1000_WHITELIST,
    SHARED_BLACKLIST, BIG_WHITELIST,
    PUMP_EXCLUDE_PCT, DUMP_EXCLUDE_PCT,
    MARGIN, LEVERAGE, LIMIT_PENDING_MAX_S,
)


COOLDOWN_S = 4 * 3600
BACKTEST_HOURS = 72


def db():
    return pymysql.connect(
        host="13.212.252.171", port=3306,
        user="admin", password="Yintao@110", database="dimesion",
        charset="utf8mb4", cursorclass=pymysql.cursors.DictCursor,
    )


def refresh_pool(cur):
    cur.execute(
        "SELECT symbol, quote_volume_24h, change_24h FROM price_stats_24h "
        "WHERE symbol LIKE '%%/USDT' AND quote_volume_24h >= %s",
        (TIER_MID_MIN_VOL,),
    )
    bigs, mids = [], []
    for r in cur.fetchall():
        sym = r["symbol"]
        vol = float(r["quote_volume_24h"] or 0)
        chg = float(r["change_24h"] or 0)
        if sym in BIGMID_EXCLUDES: continue
        if sym in SHARED_BLACKLIST: continue
        if sym.startswith("1000") and sym not in MEME_1000_WHITELIST: continue
        if chg > PUMP_EXCLUDE_PCT or chg < DUMP_EXCLUDE_PCT: continue
        if vol >= TIER_BIG_MIN_VOL and sym in BIG_WHITELIST:
            bigs.append(sym)
        elif vol >= TIER_MID_MIN_VOL:
            mids.append(sym)
    return bigs, mids


def load_klines(cur, sym: str, tf: str, start_ms: int, end_ms: int):
    cur.execute(
        "SELECT open_time, open_price, high_price, low_price, close_price, "
        "       volume, taker_buy_base_volume "
        "FROM kline_data WHERE symbol=%s AND timeframe=%s "
        "  AND open_time >= %s AND open_time <= %s ORDER BY open_time ASC",
        (sym, tf, start_ms, end_ms),
    )
    return [{
        "t": int(r["open_time"]),
        "o": float(r["open_price"]),
        "h": float(r["high_price"]),
        "l": float(r["low_price"]),
        "c": float(r["close_price"]),
        "v": float(r["volume"] or 0),
        "tb": float(r["taker_buy_base_volume"] or 0),
    } for r in cur.fetchall()]


def load_funding_series(cur, sym: str, start_ms: int, end_ms: int) -> list:
    """返回按时间升序的 funding_rate 序列 [(ts_ms, rate), ...]"""
    cur.execute(
        "SELECT timestamp, funding_rate FROM funding_rate_data "
        "WHERE symbol=%s AND timestamp >= FROM_UNIXTIME(%s/1000) "
        "  AND timestamp <= FROM_UNIXTIME(%s/1000) ORDER BY timestamp ASC",
        (sym, start_ms, end_ms),
    )
    out = []
    for r in cur.fetchall():
        ts_ms = int(r["timestamp"].replace(tzinfo=timezone.utc).timestamp() * 1000)
        out.append((ts_ms, float(r["funding_rate"])))
    return out


def load_ls_series(cur, sym: str, start_ms: int, end_ms: int) -> list:
    cur.execute(
        "SELECT timestamp, long_account, short_account FROM futures_long_short_ratio "
        "WHERE symbol=%s AND timestamp >= FROM_UNIXTIME(%s/1000) "
        "  AND timestamp <= FROM_UNIXTIME(%s/1000) ORDER BY timestamp ASC",
        (sym, start_ms, end_ms),
    )
    out = []
    for r in cur.fetchall():
        ts_ms = int(r["timestamp"].replace(tzinfo=timezone.utc).timestamp() * 1000)
        la = float(r["long_account"]); sa = float(r["short_account"])
        if la > 1.0: la /= 100
        if sa > 1.0: sa /= 100
        out.append((ts_ms, la, sa))
    return out


def load_oi_series(cur, sym: str, start_ms: int, end_ms: int) -> list:
    cur.execute(
        "SELECT timestamp, open_interest_value FROM futures_open_interest "
        "WHERE symbol=%s AND timestamp >= FROM_UNIXTIME(%s/1000) "
        "  AND timestamp <= FROM_UNIXTIME(%s/1000) ORDER BY timestamp ASC",
        (sym, start_ms, end_ms),
    )
    out = []
    for r in cur.fetchall():
        ts_ms = int(r["timestamp"].replace(tzinfo=timezone.utc).timestamp() * 1000)
        out.append((ts_ms, float(r["open_interest_value"] or 0)))
    return out


def latest_before(series: list, ts_ms: int, idx: int = 1, max_age_ms: int = 4 * 3600 * 1000):
    """返回 series 里最后一个 ts <= ts_ms 的值（取元组的 idx 位）；过旧返回 None"""
    last = None
    for item in series:
        if item[0] <= ts_ms: last = item
        else: break
    if last and (ts_ms - last[0]) <= max_age_ms:
        return last[idx] if idx is not None else last
    return None


def oi_4h_change_at(oi_series: list, ts_ms: int):
    """用 ts_ms 时刻前的 OI 序列算 4h 变化率"""
    past = [x for x in oi_series if x[0] <= ts_ms]
    if len(past) < 4: return None
    latest = past[-1][1]
    oldest = past[-4][1]
    if oldest <= 0: return None
    return (latest - oldest) / oldest


def compute_whale_score_at(bars_1h: list, cur_price: float, p: dict, direction: str,
                           fr_series: list, ls_series: list, oi_series: list, ts_ms: int) -> tuple:
    """BIG 档信号在 ts_ms 时刻的回测评分。与 strategy_bigmid.compute_whale_score 保持一致"""
    score = 0

    fr = latest_before(fr_series, ts_ms, 1, max_age_ms=2 * 3600 * 1000)
    if fr is not None:
        if direction == "short":
            if   fr >= p["fr_extreme_high"]: score += 3
            elif fr >= p["fr_high"]:         score += 2
            elif fr >= p["fr_mild_high"]:    score += 1
        else:
            if   fr <= p["fr_extreme_low"]:  score += 3
            elif fr <= p["fr_low"]:          score += 2
            elif fr <= p["fr_mild_low"]:     score += 1

    ls_item = latest_before(ls_series, ts_ms, None, max_age_ms=4 * 3600 * 1000)
    if ls_item:
        _, la, sa = ls_item
        if direction == "short":
            if   la >= p["ls_long_extreme"]: score += 2
            elif la >= p["ls_long_high"]:    score += 1
        else:
            if   sa >= p["ls_short_extreme"]: score += 2
            elif sa >= p["ls_short_high"]:    score += 1

    oi_chg = oi_4h_change_at(oi_series, ts_ms)
    if oi_chg is not None:
        if direction == "short":
            if   oi_chg <= p["oi_drop_strong"]: score += 2
            elif oi_chg <= p["oi_drop_mild"]:   score += 1
        else:
            if   oi_chg >= p["oi_rise_strong"]: score += 2
            elif oi_chg >= p["oi_rise_mild"]:   score += 1

    # 放量 + taker + 触发器 ← 从 1h bars 计算
    if len(bars_1h) >= 24:
        avg_vol = sum(b["v"] for b in bars_1h[:-3]) / max(len(bars_1h) - 3, 1)
        last3_vol = sum(b["v"] for b in bars_1h[-3:]) / 3
        vr = last3_vol / avg_vol if avg_vol > 0 else 1.0
        first_c = bars_1h[-3]["c"]; last_c = bars_1h[-1]["c"]
        pc = (last_c - first_c) / first_c if first_c > 0 else 0
        diverged = (vr >= p["vol_ratio_mild"]
                    and abs(pc) < p["stale_price_pct"]
                    and ((direction == "short" and pc > -0.015)
                         or (direction == "long" and pc < 0.015)))
        if diverged:
            score += 3 if vr >= p["vol_ratio_strong"] else 2

        taker_ratios = [b["tb"] / b["v"] for b in bars_1h[-3:] if b["v"] > 0]
        if taker_ratios:
            taker = sum(taker_ratios) / len(taker_ratios)
            if direction == "short" and taker < p["taker_sell_thresh"]:
                score += 1
            elif direction == "long" and taker > p["taker_buy_thresh"]:
                score += 1

    # 触发器
    has_trigger = False
    if len(bars_1h) >= 5:
        last = bars_1h[-1]
        o, c = last["o"], last["c"]
        lo4 = min(b["l"] for b in bars_1h[-5:-1])
        hi4 = max(b["h"] for b in bars_1h[-5:-1])
        if direction == "short":
            big_candle = o > 0 and (o - c) / o >= p["trigger_candle_pct"]
            breakout   = cur_price < lo4 * (1 - p["trigger_breakout"])
            has_trigger = big_candle or breakout
        else:
            big_candle = o > 0 and (c - o) / o >= p["trigger_candle_pct"]
            breakout   = cur_price > hi4 * (1 + p["trigger_breakout"])
            has_trigger = big_candle or breakout

    return score, has_trigger


def chase_signal(window, p) -> bool:
    if len(window) < p["bars_chase"]: return False
    o0, c = window[0]["o"], window[-1]["c"]
    if o0 <= 0: return False
    pump = (c - o0) / o0
    if pump < p["chase_pump_pct"]: return False
    rh = max(b["h"] for b in window)
    if rh > 0 and (rh - c) / rh > p["chase_exhaust_dd"]: return False
    leader = max((b["c"] - b["o"]) / b["o"] if b["o"] > 0 else 0 for b in window)
    return leader >= p["chase_leader_pct"]


def dump_signal(window, p) -> bool:
    if len(window) < p["bars_dump"]: return False
    o0, c = window[0]["o"], window[-1]["c"]
    if o0 <= 0: return False
    drop = (o0 - c) / o0
    if drop < p["dump_drop_pct"]: return False
    ml = min(b["l"] for b in window)
    if ml <= 0: return False
    bounce = (c - ml) / ml
    return bounce <= p["dump_bounce_max"]


def simulate_one(sym: str, tier: str, direction: str, signal_ts: int,
                 signal_price: float, p: Dict, k5: List[Dict]) -> Optional[Dict]:
    """
    模拟一笔交易从信号到平仓的完整过程。
    返回 None 表示挂单没成交或被熔断，否则返回 dict 含 outcome/pnl_pct 等。
    """
    # 1. 入场价确定
    # limit_offset=0 -> 市价入场，在信号后第一根 5m K 的开盘价成交
    # limit_offset>0 -> 限价挂单，等触及 limit_price 再成交
    market_mode = (p["limit_offset_pct"] <= 0.0)
    if direction == "LONG":
        limit_p = signal_price * (1 - p["limit_offset_pct"])
    else:
        limit_p = signal_price * (1 + p["limit_offset_pct"])

    # 2. 在 5m K 线里找填充点
    fill_price = None
    fill_ts = None
    if market_mode:
        # 市价：找信号后第一根 5m K 的开盘价立即成交
        for bar in k5:
            if bar["t"] >= signal_ts:
                fill_price = bar["o"]
                fill_ts = bar["t"]
                break
        if fill_price is None:
            return {"outcome": "no_5m_data", "pnl_pct": 0.0, "fill_price": None}
    for bar in k5:
        if market_mode: break     # 市价模式已在上面决定 fill，跳过限价搜索
        if bar["t"] < signal_ts: continue
        age = (bar["t"] - signal_ts) / 1000
        if age > LIMIT_PENDING_MAX_S:
            return {"outcome": "timeout_no_fill", "pnl_pct": 0.0, "fill_price": None}
        # LONG: 挂低价买，low 触及 limit 则成交
        # SHORT: 挂高价卖，high 触及 limit 则成交
        if direction == "LONG" and bar["l"] <= limit_p:
            # 先检查反向滑点（用 bar 开盘价作为当前价）
            cur_p = bar["o"]
            if cur_p < limit_p:
                rev_slip = (limit_p - cur_p) / limit_p
                if rev_slip > p["reverse_slippage"]:
                    return {"outcome": "reverse_slippage_cancel", "pnl_pct": 0.0,
                            "rev_slip": rev_slip, "fill_price": None}
            fill_price = min(bar["o"], limit_p)  # 开盘已经低于 limit 就以开盘价成交
            fill_ts = bar["t"]
            break
        if direction == "SHORT" and bar["h"] >= limit_p:
            cur_p = bar["o"]
            if cur_p > limit_p:
                rev_slip = (cur_p - limit_p) / limit_p
                if rev_slip > p["reverse_slippage"]:
                    return {"outcome": "reverse_slippage_cancel", "pnl_pct": 0.0,
                            "rev_slip": rev_slip, "fill_price": None}
            fill_price = max(bar["o"], limit_p)
            fill_ts = bar["t"]
            break
    if fill_price is None:
        return {"outcome": "not_filled_in_range", "pnl_pct": 0.0, "fill_price": None}

    # 3. 持仓追踪
    sl_price = fill_price * (1 - p["sl_pct"]) if direction == "LONG" else fill_price * (1 + p["sl_pct"])
    tp_price = fill_price * (1 + p["hard_tp_pct"]) if direction == "LONG" else fill_price * (1 - p["hard_tp_pct"])
    peak_pnl = 0.0

    for bar in k5:
        if bar["t"] <= fill_ts: continue
        age_min = (bar["t"] - fill_ts) / 60000
        if age_min > p["hold_min"]:
            # 超时：按 bar 收盘价平仓
            close_p = bar["c"]
            pnl_pct = (close_p - fill_price) / fill_price if direction == "LONG" else (fill_price - close_p) / fill_price
            return {"outcome": "timeout", "pnl_pct": pnl_pct, "fill_price": fill_price,
                    "close_price": close_p, "close_ts": bar["t"]}

        # 先判 SL（保守假设 SL 先于 TP 触发，若同根 K 线都触及）
        if direction == "LONG":
            if bar["l"] <= sl_price:
                return {"outcome": "stop_loss", "pnl_pct": -p["sl_pct"], "fill_price": fill_price,
                        "close_price": sl_price, "close_ts": bar["t"]}
            if bar["h"] >= tp_price:
                return {"outcome": "hard_tp", "pnl_pct": p["hard_tp_pct"], "fill_price": fill_price,
                        "close_price": tp_price, "close_ts": bar["t"]}
            # 移动止盈
            high_pnl = (bar["h"] - fill_price) / fill_price
            peak_pnl = max(peak_pnl, high_pnl)
            close_pnl = (bar["c"] - fill_price) / fill_price
            if peak_pnl >= p["trail_tp_start"] and (peak_pnl - close_pnl) >= p["trail_tp_pullback"]:
                return {"outcome": "trail_tp", "pnl_pct": close_pnl, "fill_price": fill_price,
                        "close_price": bar["c"], "close_ts": bar["t"]}
        else:  # SHORT
            if bar["h"] >= sl_price:
                return {"outcome": "stop_loss", "pnl_pct": -p["sl_pct"], "fill_price": fill_price,
                        "close_price": sl_price, "close_ts": bar["t"]}
            if bar["l"] <= tp_price:
                return {"outcome": "hard_tp", "pnl_pct": p["hard_tp_pct"], "fill_price": fill_price,
                        "close_price": tp_price, "close_ts": bar["t"]}
            low_pnl = (fill_price - bar["l"]) / fill_price
            peak_pnl = max(peak_pnl, low_pnl)
            close_pnl = (fill_price - bar["c"]) / fill_price
            if peak_pnl >= p["trail_tp_start"] and (peak_pnl - close_pnl) >= p["trail_tp_pullback"]:
                return {"outcome": "trail_tp", "pnl_pct": close_pnl, "fill_price": fill_price,
                        "close_price": bar["c"], "close_ts": bar["t"]}

    # 5m 数据跑完都没平仓 → 视为 mark-to-market
    last = k5[-1]
    pnl_pct = (last["c"] - fill_price) / fill_price if direction == "LONG" else (fill_price - last["c"]) / fill_price
    return {"outcome": "open_at_end", "pnl_pct": pnl_pct, "fill_price": fill_price,
            "close_price": last["c"], "close_ts": last["t"]}


def run_tier_trend(cur, tier: str, pool: List, hours: int) -> List[Dict]:
    """MID 档（kind=trend）回测：CHASE + DUMP"""
    p = TIER_PARAMS[tier]
    tf = p["tf"]
    tf_sec = {"1h": 3600, "15m": 900, "5m": 300}[tf]

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    kline_start = now_ms - (hours + max(p["bars_chase"], p["bars_dump"]) + 4) * tf_sec * 1000
    kline_end = now_ms

    trades = []
    for sym in pool:
        bars = load_klines(cur, sym, tf, kline_start, kline_end)
        if len(bars) < max(p["bars_chase"], p["bars_dump"]) + 2:
            continue
        k5 = load_klines(cur, sym, "5m", kline_start, kline_end)
        last_close_ts = {"chase": 0, "dump": 0}
        needed = max(p["bars_chase"], p["bars_dump"])
        for i in range(needed, len(bars) - 1):
            ts_close = bars[i - 1]["t"] + tf_sec * 1000
            if ts_close < now_ms - hours * 3600 * 1000: continue
            w_chase = bars[i - p["bars_chase"]: i]
            w_dump  = bars[i - p["bars_dump"]: i] if i >= p["bars_dump"] else None
            price_now = bars[i - 1]["c"]
            if ts_close > last_close_ts["chase"] + COOLDOWN_S * 1000 and chase_signal(w_chase, p):
                res = simulate_one(sym, tier, "LONG", ts_close, price_now, p, k5)
                if res:
                    res.update(symbol=sym, tier=tier, direction="LONG",
                               signal_ts=ts_close, signal_price=price_now, stype="chase")
                    trades.append(res)
                    last_close_ts["chase"] = res.get("close_ts") or (ts_close + LIMIT_PENDING_MAX_S * 1000)
            if w_dump and ts_close > last_close_ts["dump"] + COOLDOWN_S * 1000 and dump_signal(w_dump, p):
                res = simulate_one(sym, tier, "SHORT", ts_close, price_now, p, k5)
                if res:
                    res.update(symbol=sym, tier=tier, direction="SHORT",
                               signal_ts=ts_close, signal_price=price_now, stype="dump")
                    trades.append(res)
                    last_close_ts["dump"] = res.get("close_ts") or (ts_close + LIMIT_PENDING_MAX_S * 1000)
    return trades


def run_tier_whale(cur, tier: str, pool: List, hours: int) -> List[Dict]:
    """BIG 档（kind=whale）回测：每根 1h 收盘计算多/空评分"""
    p = TIER_PARAMS[tier]
    tf = p["tf"]            # 'BIG' 的 tf='1h'
    assert tf == "1h"
    tf_sec = 3600

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    kline_start = now_ms - (hours + 30) * tf_sec * 1000
    kline_end = now_ms

    trades = []
    for sym in pool:
        bars = load_klines(cur, sym, "1h", kline_start, kline_end)
        if len(bars) < 30: continue
        k5 = load_klines(cur, sym, "5m", kline_start, kline_end)
        fr_series = load_funding_series(cur, sym, kline_start, kline_end)
        ls_series = load_ls_series(cur, sym, kline_start, kline_end)
        oi_series = load_oi_series(cur, sym, kline_start, kline_end)

        last_close_ts = 0
        for i in range(24, len(bars) - 1):  # 需要 24 根 1h 做 vol/taker
            ts_close = bars[i - 1]["t"] + tf_sec * 1000
            if ts_close < now_ms - hours * 3600 * 1000: continue
            if ts_close <= last_close_ts + COOLDOWN_S * 1000: continue

            bars_1h = bars[i - 24: i]   # 最近 24 根 1h，已收盘
            price_now = bars[i - 1]["c"]

            s_short, trig_short = compute_whale_score_at(bars_1h, price_now, p, "short",
                                                          fr_series, ls_series, oi_series, ts_close)
            s_long, trig_long   = compute_whale_score_at(bars_1h, price_now, p, "long",
                                                          fr_series, ls_series, oi_series, ts_close)
            candidates = []
            if s_short >= p["entry_score_min"] and trig_short:
                candidates.append(("SHORT", s_short))
            if s_long >= p["entry_score_min"] and trig_long:
                candidates.append(("LONG", s_long))
            if not candidates: continue
            candidates.sort(key=lambda x: (-x[1], 0 if x[0] == "SHORT" else 1))
            direction, sc = candidates[0]

            res = simulate_one(sym, tier, direction, ts_close, price_now, p, k5)
            if res:
                res.update(symbol=sym, tier=tier, direction=direction,
                           signal_ts=ts_close, signal_price=price_now,
                           stype="whale", score=sc)
                trades.append(res)
                last_close_ts = res.get("close_ts") or (ts_close + 4 * 3600 * 1000)
    return trades


def run_tier(cur, tier: str, pool: List, hours: int) -> List[Dict]:
    p = TIER_PARAMS[tier]
    pool_syms = [s[0] if isinstance(s, (list, tuple)) else s for s in pool]
    if p["kind"] == "whale":
        return run_tier_whale(cur, tier, pool_syms, hours)
    return run_tier_trend(cur, tier, pool_syms, hours)


def fmt(ms): return datetime.utcfromtimestamp(ms / 1000).strftime("%m-%d %H:%M")


def report(trades: List[Dict], tier: str):
    if not trades:
        print(f"  {tier} 无交易")
        return
    filled = [t for t in trades if t.get("fill_price")]
    not_filled = [t for t in trades if not t.get("fill_price")]
    wins = [t for t in filled if t["pnl_pct"] > 0]
    losses = [t for t in filled if t["pnl_pct"] < 0]
    pnl_usdt = sum(t["pnl_pct"] * MARGIN * LEVERAGE for t in filled)

    print(f"\n▼ {tier} 档汇总:")
    print(f"  信号总数: {len(trades)}  (成交 {len(filled)}  未成交 {len(not_filled)})")
    cnt = {}
    for t in trades:
        cnt[t["outcome"]] = cnt.get(t["outcome"], 0) + 1
    for o, c in sorted(cnt.items(), key=lambda x: -x[1]):
        print(f"    {o:26s} {c}")
    if filled:
        avg_pnl = sum(t["pnl_pct"] for t in filled) / len(filled)
        print(f"  成交胜率: {len(wins)}/{len(filled)} = {len(wins)/len(filled)*100:.0f}%")
        print(f"  平均 PnL%: {avg_pnl*100:+.2f}%")
        print(f"  累计 PnL (USDT, margin×leverage={MARGIN*LEVERAGE:.0f}): {pnl_usdt:+.2f}")
        print(f"\n  成交明细 (按信号时间):")
        for t in sorted(filled, key=lambda x: x["signal_ts"]):
            print(f"    {fmt(t['signal_ts'])} {t['symbol']:13s} {t['direction']:5s} "
                  f"sig={t['signal_price']:>10.5f} fill={t['fill_price']:>10.5f} "
                  f"close={t.get('close_price', 0):>10.5f} "
                  f"PnL={t['pnl_pct']*100:+6.2f}% [{t['outcome']}]")
        if not_filled:
            print(f"\n  未成交明细:")
            for t in sorted(not_filled, key=lambda x: x["signal_ts"]):
                extra = f" rev_slip={t.get('rev_slip', 0)*100:.2f}%" if t.get("rev_slip") else ""
                print(f"    {fmt(t['signal_ts'])} {t['symbol']:13s} {t['direction']:5s} "
                      f"sig={t['signal_price']:>10.5f} [{t['outcome']}]{extra}")


def main():
    conn = db()
    cur = conn.cursor()
    bigs, mids = refresh_pool(cur)
    print(f"池: BIG={bigs} MID={len(mids)}")

    print(f"\n{'='*80}")
    print(f"strategy_bigmid 过去 {BACKTEST_HOURS}h 执行式回测")
    print(f"{'='*80}")

    big_trades = run_tier(cur, "BIG", bigs, BACKTEST_HOURS)
    mid_trades = run_tier(cur, "MID", mids, BACKTEST_HOURS)

    report(big_trades, "BIG")
    report(mid_trades, "MID")

    all_filled = [t for t in big_trades + mid_trades if t.get("fill_price")]
    if all_filled:
        total_pnl = sum(t["pnl_pct"] * MARGIN * LEVERAGE for t in all_filled)
        total_wins = sum(1 for t in all_filled if t["pnl_pct"] > 0)
        print(f"\n{'='*80}")
        print(f"整体 (过去 {BACKTEST_HOURS}h, 所有档位合计):")
        print(f"  成交交易数: {len(all_filled)}  胜率: {total_wins}/{len(all_filled)} = {total_wins/len(all_filled)*100:.0f}%")
        print(f"  累计 PnL (USDT): {total_pnl:+.2f}")
        print(f"{'='*80}")

    conn.close()


if __name__ == "__main__":
    main()
