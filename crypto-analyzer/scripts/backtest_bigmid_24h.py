# -*- coding: utf-8 -*-
"""
strategy_bigmid 过去 24 小时干式回测

独立计算每根 K 收盘后的 CHASE/DUMP 信号判定结果，不下单不走状态机。
输出：每个 tier 触发次数、触发时间点、各币覆盖情况。
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone, timedelta
from typing import List, Dict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pymysql

from strategy_bigmid import (
    TIER_PARAMS,
    TIER_BIG_MIN_VOL,
    TIER_MID_MIN_VOL,
    BIGMID_EXCLUDES,
    MEME_1000_WHITELIST,
    SHARED_BLACKLIST,
    BIG_WHITELIST,
    PUMP_EXCLUDE_PCT,
    DUMP_EXCLUDE_PCT,
)


def db():
    return pymysql.connect(
        host="13.212.252.171", port=3306,
        user="admin", password="Yintao@110", database="dimesion",
        charset="utf8mb4", cursorclass=pymysql.cursors.DictCursor,
    )


def refresh_pool(cur) -> tuple[list, list]:
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
        if sym in BIGMID_EXCLUDES:
            continue
        if sym in SHARED_BLACKLIST:
            continue
        if sym.startswith("1000") and sym not in MEME_1000_WHITELIST:
            continue
        if chg > PUMP_EXCLUDE_PCT or chg < DUMP_EXCLUDE_PCT:
            continue
        if vol >= TIER_BIG_MIN_VOL and sym in BIG_WHITELIST:
            bigs.append((sym, vol))
        elif vol >= TIER_MID_MIN_VOL:
            mids.append((sym, vol))
    bigs.sort(key=lambda x: -x[1])
    mids.sort(key=lambda x: -x[1])
    return bigs, mids


def load_bars_range(cur, sym: str, tf: str, start_ms: int, end_ms: int) -> List[Dict]:
    cur.execute(
        "SELECT open_time, open_price, high_price, low_price, close_price "
        "FROM kline_data WHERE symbol=%s AND timeframe=%s "
        "  AND open_time >= %s AND open_time <= %s "
        "ORDER BY open_time ASC",
        (sym, tf, start_ms, end_ms),
    )
    return cur.fetchall()


def chase_check(window: List[Dict], p: Dict) -> Dict:
    """返回 {triggered: bool, pump, leader, dd}；若触发，triggered=True"""
    if len(window) < p["bars_chase"]:
        return {"triggered": False}
    o0 = float(window[0]["open_price"])
    c_last = float(window[-1]["close_price"])
    if o0 <= 0:
        return {"triggered": False}
    pump = (c_last - o0) / o0
    recent_high = max(float(b["high_price"]) for b in window)
    dd = (recent_high - c_last) / recent_high if recent_high > 0 else 0
    leader = 0.0
    for b in window:
        o, c = float(b["open_price"]), float(b["close_price"])
        if o > 0:
            g = (c - o) / o
            if g > leader:
                leader = g
    ok = (pump >= p["chase_pump_pct"]
          and dd <= p["chase_exhaust_dd"]
          and leader >= p["chase_leader_pct"])
    return {"triggered": ok, "pump": pump, "leader": leader, "dd": dd}


def dump_check(window: List[Dict], p: Dict) -> Dict:
    if len(window) < p["bars_dump"]:
        return {"triggered": False}
    o0 = float(window[0]["open_price"])
    c_last = float(window[-1]["close_price"])
    if o0 <= 0:
        return {"triggered": False}
    drop = (o0 - c_last) / o0
    min_low = min(float(b["low_price"]) for b in window)
    if min_low <= 0:
        return {"triggered": False}
    bounce = (c_last - min_low) / min_low
    ok = drop >= p["dump_drop_pct"] and bounce <= p["dump_bounce_max"]
    return {"triggered": ok, "drop": drop, "bounce": bounce}


def run_for_tier(cur, tier: str, pool: list, hours: int = 24):
    p = TIER_PARAMS[tier]
    tf = p["tf"]
    tf_sec = 3600 if tf == "1h" else (900 if tf == "15m" else 300)
    needed = max(p["bars_chase"], p["bars_dump"])

    # 回测区间
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    load_start = now_ms - (hours + needed + 2) * tf_sec * 1000
    load_end   = now_ms

    chase_hits = []  # (sym, ts, pump, leader, dd)
    dump_hits  = []

    for sym, vol in pool:
        bars = load_bars_range(cur, sym, tf, load_start, load_end)
        if len(bars) < needed + 2:
            continue
        # 每根收盘后判定一次（模拟轮询命中收盘后的窗口）
        # 最新一根可能未收盘，跳过
        for i in range(needed, len(bars) - 1):
            window_chase = bars[i - p["bars_chase"]: i]
            window_dump  = bars[i - p["bars_dump"]: i] if i >= p["bars_dump"] else None
            ts_close = int(bars[i - 1]["open_time"]) + tf_sec * 1000
            if ts_close < now_ms - hours * 3600 * 1000:
                continue

            r = chase_check(window_chase, p)
            if r["triggered"]:
                chase_hits.append((sym, ts_close, r["pump"], r["leader"], r["dd"]))
            if window_dump is not None:
                r = dump_check(window_dump, p)
                if r["triggered"]:
                    dump_hits.append((sym, ts_close, r["drop"], r["bounce"]))

    return chase_hits, dump_hits


def fmt_ts(ms):
    return datetime.utcfromtimestamp(ms / 1000).strftime("%m-%d %H:%M UTC")


def main():
    conn = db()
    cur = conn.cursor()

    bigs, mids = refresh_pool(cur)
    print(f"池: BIG={len(bigs)}  MID={len(mids)}")
    print()
    print("BIG (vol ≥ $500M):")
    for s, v in bigs:
        print(f"  {s:18s} ${v/1e6:>7.0f}M")
    print()
    print("MID (vol $100M~$500M):")
    for s, v in mids[:15]:
        print(f"  {s:18s} ${v/1e6:>7.0f}M")
    if len(mids) > 15:
        print(f"  ... 共 {len(mids)} 个")
    print()

    print("=" * 80)
    print("过去 24h 干式回测（不下单）")
    print("=" * 80)

    for tier, pool in [("BIG", bigs), ("MID", mids)]:
        print()
        print(f"▼ {tier} 档  tf={TIER_PARAMS[tier]['tf']}  (pool={len(pool)})")
        chase_hits, dump_hits = run_for_tier(cur, tier, pool, hours=24)
        print(f"  CHASE 信号: {len(chase_hits)} 次")
        for sym, ts, pump, leader, dd in chase_hits[:15]:
            print(f"    {fmt_ts(ts)}  {sym:15s}  pump={pump*100:+5.2f}% leader={leader*100:+5.2f}% dd={dd*100:.2f}%")
        if len(chase_hits) > 15:
            print(f"    ... (还有 {len(chase_hits)-15} 条)")

        print(f"  DUMP 信号:  {len(dump_hits)} 次")
        for sym, ts, drop, bounce in dump_hits[:15]:
            print(f"    {fmt_ts(ts)}  {sym:15s}  drop={drop*100:+5.2f}% bounce={bounce*100:.2f}%")
        if len(dump_hits) > 15:
            print(f"    ... (还有 {len(dump_hits)-15} 条)")

    conn.close()


if __name__ == "__main__":
    main()
