"""
四种形态识别器 + 历史回放
  形态 1  持续冲高未回调 → 回调后 LONG
  形态 2  M 顶 (二次冲高不创新高 + 量 < 第一次 60%) → SHORT
  形态 3  W 底 (跌后筑底 + 小涨带量) → LONG  (占位版, 等细化)
  形态 4  高位磨顶 (6h 振幅 < 5% 且仍在顶部) → SHORT

扫所有活跃 symbol 的 15m kline, 每根 bar 运行一次识别. 命中即模拟开仓:
  SL 5% / TP 10% / 最大持仓 48 根 15m (12h)

只读 kline_data, 不改策略代码.
用法: python scripts/diag/replay_4_forms.py [DAYS]  默认 14
"""
import sys
from datetime import datetime, timezone
from collections import defaultdict
from typing import Optional
import pymysql

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DB = dict(host='13.212.252.171', port=3306, user='admin', password='Yintao@110',
          db='dimesion', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

BAR_MS_15M = 15 * 60 * 1000

# ────────────── 形态 1 (中等) ──────────────
F1_WINDOW_BARS = 3 * 24 * 4     # 3 天 15m = 288
F1_MIN_RISE = 0.30              # 3 天累计 >= 30%
F1_MAX_DRAWDOWN_BEFORE = 0.06   # 窗口内最大回撤 <= 6%
F1_PULLBACK_MIN = 0.05          # 从高点回调 >= 5% 触发
F1_BULLISH_WITHIN = 3           # 回调后近 3 根 15m 需有阳线

# ────────────── 形态 2 (M 顶) ──────────────
F2_MIN_RISE_TO_PEAK1 = 0.30     # 到 peak1 之前有显著涨幅
F2_VALLEY_MIN = 0.03            # 两 peak 之间回撤 >= 3%
F2_P1_P2_MIN_BARS = 24          # peak1 到 peak2 >= 6h (24 根 15m)
F2_P2_LT_P1 = True              # peak2 < peak1
F2_VOL_RATIO_MAX = 0.60         # peak2 量 < peak1 量 * 60%
F2_LOOKBACK_BARS = 3 * 24 * 4   # 3 天窗口

# ────────────── 形态 3 (W 底占位) ──────────────
F3_DROP_LOOKBACK = 7 * 24 * 4   # 7 天
F3_MIN_DROP = 0.20              # 从高到低 >= 20%
F3_RECENT_24H_MIN = -0.05       # 最近 24h 未继续跌超 5%
F3_TRIGGER_BULLISH_PCT = 0.01   # 最后一根 15m 阳线 >= 1%
F3_VOL_MULT = 1.5               # 触发 bar 量 > 24h 均量 * 1.5

# ────────────── 形态 4 (高位磨顶) ──────────────
F4_TIGHT_BARS = 6 * 4           # 6h = 24 根 15m
F4_MAX_RANGE_PCT = 0.05         # (hi-lo)/lo < 5%
F4_POSITION_PCT = 0.97          # 仍在 3 天高点 97% 以上
F4_3D_WINDOW_BARS = 3 * 24 * 4

# ────────────── 回测仓位参数 ──────────────
SL_PCT = 0.05
TP_PCT_DEFAULT = 0.10
TP_PCT_F4 = 0.03                # 磨顶后下跌幅度有限, 改小 TP
MAX_HOLD_BARS = 48              # 12h


def tp_for_form(form_name: str) -> float:
    if form_name.startswith('F4_'):
        return TP_PCT_F4
    return TP_PCT_DEFAULT


def fetch_bars(cur, sym: str, lookback_ms: int, end_ms: int):
    """取 sym 的 15m kline (open_time >= lookback_ms 且 < end_ms) ascending."""
    cur.execute(
        """SELECT open_time, open_price, high_price, low_price, close_price, volume
           FROM kline_data
           WHERE symbol=%s AND timeframe='15m'
             AND open_time >= %s AND open_time < %s
           ORDER BY open_time ASC""",
        (sym, lookback_ms, end_ms),
    )
    return cur.fetchall()


# ═════════════════ 形态识别器 ═════════════════

def detect_form1(bars: list) -> Optional[dict]:
    """形态 1: 3 天 >= 30% + 回撤 <=6% + 从高点 -5% + 近 3 根有阳线"""
    if len(bars) < F1_WINDOW_BARS:
        return None
    window = bars[-F1_WINDOW_BARS:]
    opens = [float(b['open_price']) for b in window]
    highs = [float(b['high_price']) for b in window]
    lows = [float(b['low_price']) for b in window]
    closes = [float(b['close_price']) for b in window]

    cur_close = closes[-1]
    start_open = opens[0]
    if start_open <= 0:
        return None
    rise = (cur_close - start_open) / start_open
    if rise < F1_MIN_RISE:
        return None

    # 计算从 start 累计, 任一时刻从峰值下撤的最大值
    peak = opens[0]
    max_dd = 0.0
    for p in closes:
        peak = max(peak, p)
        if peak > 0:
            dd = (peak - p) / peak
            if dd > max_dd:
                max_dd = dd
    if max_dd > F1_MAX_DRAWDOWN_BEFORE:
        # 其实这里是"还没有单次大回调" 的过滤; 但我们要找 "正在回调" 的时刻
        # 所以这里如果 max_dd > 6% 则 skip (说明之前已经大回调过, 不是"持续冲高")
        return None

    # 找窗口最高价
    window_high = max(highs)
    pullback = (window_high - cur_close) / window_high if window_high > 0 else 0
    if pullback < F1_PULLBACK_MIN:
        return None

    # 近 3 根有无阳线
    recent3 = window[-F1_BULLISH_WITHIN:]
    has_bullish = any(float(b['close_price']) > float(b['open_price']) for b in recent3)
    if not has_bullish:
        return None

    return {
        'form': 'F1_持续冲高回调做多',
        'direction': 'LONG',
        'rise_pct': rise, 'pullback_pct': pullback,
        'entry_price': cur_close,
    }


def detect_form2(bars: list) -> Optional[dict]:
    """形态 2: M 顶. 找两个 peak, peak2<peak1, peak2 量<peak1 量*60%"""
    if len(bars) < F2_LOOKBACK_BARS:
        return None
    window = bars[-F2_LOOKBACK_BARS:]
    opens = [float(b['open_price']) for b in window]
    highs = [float(b['high_price']) for b in window]
    lows = [float(b['low_price']) for b in window]
    closes = [float(b['close_price']) for b in window]
    vols = [float(b['volume'] or 0) for b in window]
    n = len(window)

    # 先检查起点到窗口 max_high 的涨幅 (要是高位)
    w_high = max(highs)
    start_low = min(lows[:24]) if len(lows) > 24 else min(lows)
    if start_low <= 0:
        return None
    rise_to_high = (w_high - start_low) / start_low
    if rise_to_high < F2_MIN_RISE_TO_PEAK1:
        return None

    # 找 peak2 (近期高点, 在最后 48 根内)
    recent_slice = max(24, min(n - 24, n // 3))
    i2 = n - 1 - (vols[-recent_slice:][::-1].index(max(vols[-recent_slice:])) if max(vols[-recent_slice:]) > 0 else 0)
    # 简化: peak2 = 最后 48 根的最高点
    window_tail = range(max(0, n - 48), n)
    i2 = max(window_tail, key=lambda k: highs[k])
    peak2_high = highs[i2]

    # 找 peak1: peak2 之前, 距离 >= F2_P1_P2_MIN_BARS, 且到 peak2 之间有 valley 回撤 >= 3%
    i1 = None
    best_p1 = -1
    for i in range(0, i2 - F2_P1_P2_MIN_BARS):
        if highs[i] > best_p1:
            # 检查 i 到 i2 之间是否有 valley
            valley = min(lows[i:i2])
            if valley <= 0:
                continue
            v_dd = (highs[i] - valley) / highs[i]
            if v_dd >= F2_VALLEY_MIN:
                best_p1 = highs[i]
                i1 = i

    if i1 is None or best_p1 <= 0:
        return None
    peak1_high = best_p1

    if not (peak2_high < peak1_high):
        return None

    # peak1 和 peak2 所在 bar 的量
    vol1 = vols[i1]
    vol2 = vols[i2]
    if vol1 <= 0:
        return None
    if vol2 >= vol1 * F2_VOL_RATIO_MAX:
        return None

    # 确认当前 bar 就在 peak2 附近 (peak2 是最后 48 根内的最高)
    if i2 < n - 6:
        # peak2 已经过去 >= 6 根, 入场时机已晚
        return None

    return {
        'form': 'F2_M顶双头衰竭',
        'direction': 'SHORT',
        'peak1_high': peak1_high, 'peak2_high': peak2_high,
        'vol_ratio': vol2 / vol1 if vol1 > 0 else 0,
        'entry_price': closes[-1],
    }


def detect_form3(bars: list) -> Optional[dict]:
    """形态 3 占位: 7 天内从高到低 >=20% + 最近 24h 未继续跌 + 最后 15m 阳线放量"""
    if len(bars) < F3_DROP_LOOKBACK:
        return None
    window = bars[-F3_DROP_LOOKBACK:]
    highs = [float(b['high_price']) for b in window]
    lows = [float(b['low_price']) for b in window]
    closes = [float(b['close_price']) for b in window]
    vols = [float(b['volume'] or 0) for b in window]
    n = len(window)

    w_high = max(highs)
    w_low = min(lows)
    if w_high <= 0:
        return None
    drop = (w_high - w_low) / w_high
    if drop < F3_MIN_DROP:
        return None

    # 最近 24h (96 根 15m) 未继续跌超 5%
    if n >= 96:
        recent24 = closes[-96:]
        rec_high = max(recent24); rec_low = min(recent24); rec_last = recent24[-1]
        if rec_high > 0:
            rec_change = (rec_last - recent24[0]) / recent24[0]
            if rec_change < F3_RECENT_24H_MIN:
                return None
        # 价位不应在最低点附近 (筑底完成, 不是新低)
        if rec_last < rec_low * 1.01:
            return None

    # 最后一根 15m 阳线且量 > 24h 均量 * 1.5
    last = window[-1]
    o = float(last['open_price']); c = float(last['close_price']); v = float(last['volume'] or 0)
    if c <= o:
        return None
    body_pct = (c - o) / o if o > 0 else 0
    if body_pct < F3_TRIGGER_BULLISH_PCT:
        return None
    if n >= 96:
        avg_vol = sum(vols[-96:]) / 96
        if avg_vol <= 0 or v < avg_vol * F3_VOL_MULT:
            return None

    return {
        'form': 'F3_W底小涨带量',
        'direction': 'LONG',
        'drop_pct': drop, 'body_pct': body_pct,
        'entry_price': c,
    }


def detect_form4(bars: list) -> Optional[dict]:
    """形态 4: 近 6h 振幅 < 5% 且仍在 3 天高点 97% 以上"""
    if len(bars) < F4_3D_WINDOW_BARS:
        return None
    window3d = bars[-F4_3D_WINDOW_BARS:]
    window6h = bars[-F4_TIGHT_BARS:]

    highs6 = [float(b['high_price']) for b in window6h]
    lows6 = [float(b['low_price']) for b in window6h]
    closes = [float(b['close_price']) for b in window6h]
    hi = max(highs6); lo = min(lows6)
    if lo <= 0:
        return None
    rng = (hi - lo) / lo
    if rng >= F4_MAX_RANGE_PCT:
        return None

    # 位置
    w3d_high = max(float(b['high_price']) for b in window3d)
    cur_close = closes[-1]
    if w3d_high <= 0 or cur_close < w3d_high * F4_POSITION_PCT:
        return None

    return {
        'form': 'F4_高位磨顶',
        'direction': 'SHORT',
        'range_pct': rng, 'cur_pos_of_3d_high': cur_close / w3d_high,
        'entry_price': cur_close,
    }


DETECTORS = [
    ('F1', detect_form1),
    ('F2', detect_form2),
    ('F3', detect_form3),
    ('F4', detect_form4),
]


# ═════════════════ 回测 ═════════════════

def simulate_exit(cur, sym: str, signal_ms: int, direction: str,
                   entry: float, tp_pct: float):
    """SL 5% / TP 按形态传入 / max_hold 48 bars."""
    end_ms = signal_ms + MAX_HOLD_BARS * BAR_MS_15M
    cur.execute(
        """SELECT open_time, high_price, low_price, close_price
           FROM kline_data
           WHERE symbol=%s AND timeframe='15m'
             AND open_time >= %s AND open_time < %s
           ORDER BY open_time ASC""",
        (sym, signal_ms, end_ms),
    )
    bars = cur.fetchall()
    if not bars:
        return None

    for b in bars:
        hi = float(b['high_price']); lo = float(b['low_price'])
        if direction == 'LONG':
            adverse = (entry - lo) / entry
            favorable = (hi - entry) / entry
        else:
            adverse = (hi - entry) / entry
            favorable = (entry - lo) / entry
        if adverse >= SL_PCT:
            return {'exit_reason': 'sl', 'pnl_pct': -SL_PCT, 'hold_bars': bars.index(b) + 1}
        if favorable >= tp_pct:
            return {'exit_reason': 'tp', 'pnl_pct': tp_pct, 'hold_bars': bars.index(b) + 1}
    # 超时
    last = bars[-1]
    exit_p = float(last['close_price'])
    if direction == 'LONG':
        pnl = (exit_p - entry) / entry
    else:
        pnl = (entry - exit_p) / entry
    return {'exit_reason': 'timeout', 'pnl_pct': pnl, 'hold_bars': len(bars)}


# ═════════════════ 主扫描 ═════════════════

def main():
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 7
    conn = pymysql.connect(**DB); cur = conn.cursor()
    try:
        end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        start_ms = end_ms - days * 24 * 3600 * 1000

        # 取活跃 symbol
        cur.execute(
            """SELECT DISTINCT symbol FROM kline_data
               WHERE timeframe='15m' AND open_time >= %s""",
            (start_ms,),
        )
        symbols = [r['symbol'] for r in cur.fetchall()]
        print(f"### 四形态回测 {days} 天 / {len(symbols)} symbols ###")
        print(f"### SL={SL_PCT*100:.0f}% TP F1/F2/F3={TP_PCT_DEFAULT*100:.0f}% F4={TP_PCT_F4*100:.0f}% "
              f"max_hold={MAX_HOLD_BARS*15/60:.0f}h ###\n")

        # 每个 symbol, 向前走, 每 15m 运行一次检测
        # 为了效率, 一次性拉整段, 然后滑窗扫描
        all_results = []
        for idx, sym in enumerate(symbols, 1):
            try:
                # 多拉一些往前的, 形态 3 需要 7 天数据
                pre_ms = start_ms - 7 * 24 * 3600 * 1000
                bars = fetch_bars(cur, sym, pre_ms, end_ms)
            except Exception as e:
                print(f"  {sym} 拉 kline 失败: {e}")
                continue

            if len(bars) < F3_DROP_LOOKBACK:
                continue

            # 用字典防止同一 bar 多形态命中 (只取第一个)
            # 但为了比较各形态效果, 允许同 bar 多形态命中 — 独立统计
            prev_forms_signal_ms = {}   # (form, ts) -> 防止相邻几根 bar 重复触发

            # 从 F3_DROP_LOOKBACK 开始扫 (需要足够前置数据)
            # 每 1 根 15m 扫一次; 但为了避免同一形态重复触发, 每个形态独立冷却 2h (8 根)
            COOLDOWN_BARS = 8
            last_hit_bar = {name: -999 for name, _ in DETECTORS}

            for i in range(F3_DROP_LOOKBACK, len(bars)):
                cur_bar = bars[i]
                signal_ms = cur_bar['open_time'] + BAR_MS_15M  # 用 bar 收盘时间
                if signal_ms < start_ms:
                    continue

                sub_bars = bars[:i + 1]
                for name, detector in DETECTORS:
                    if i - last_hit_bar[name] < COOLDOWN_BARS:
                        continue
                    try:
                        sig = detector(sub_bars)
                    except Exception:
                        sig = None
                    if not sig:
                        continue
                    last_hit_bar[name] = i
                    # 模拟
                    exit_info = simulate_exit(cur, sym, signal_ms,
                                               sig['direction'], sig['entry_price'],
                                               tp_for_form(sig['form']))
                    if not exit_info:
                        continue
                    all_results.append({
                        'sym': sym, 'form': sig['form'],
                        'direction': sig['direction'],
                        'signal_ms': signal_ms,
                        'entry': sig['entry_price'],
                        **exit_info,
                        **{k: v for k, v in sig.items()
                           if k not in ('form', 'direction', 'entry_price')}
                    })

        print(f"总命中信号: {len(all_results)}\n")

        # ─── 汇总 ───
        by_form = defaultdict(list)
        for r in all_results:
            by_form[r['form']].append(r)

        print(f"{'form':<22}{'dir':<6}{'n':>5}{'win%':>7}{'tp':>5}{'sl':>5}"
              f"{'to':>5}{'avg%':>8}{'expect%':>9}")
        for form, lst in sorted(by_form.items()):
            wins = [r for r in lst if r['pnl_pct'] > 0]
            sls = [r for r in lst if r['exit_reason'] == 'sl']
            tps = [r for r in lst if r['exit_reason'] == 'tp']
            tos = [r for r in lst if r['exit_reason'] == 'timeout']
            losses = [r for r in lst if r['pnl_pct'] <= 0]
            n = len(lst)
            wr = len(wins) / n * 100 if n else 0
            avg = sum(r['pnl_pct'] for r in lst) / n if n else 0
            avg_w = sum(r['pnl_pct'] for r in wins) / len(wins) if wins else 0
            avg_l = sum(r['pnl_pct'] for r in losses) / len(losses) if losses else 0
            exp = (len(wins)/n)*avg_w + (len(losses)/n)*avg_l if n else 0
            dir_label = lst[0]['direction'] if lst else '?'
            print(f"{form:<22}{dir_label:<6}{n:>5}{wr:>6.1f}%{len(tps):>5}{len(sls):>5}"
                  f"{len(tos):>5}{avg*100:>+7.2f}%{exp*100:>+8.2f}%")
        print()

        # 每个形态 top 5 盈 / top 5 亏的 symbol
        for form in sorted(by_form.keys()):
            lst = by_form[form]
            by_sym = defaultdict(list)
            for r in lst:
                by_sym[r['sym']].append(r)
            sym_stats = [(s, len(rs), sum(1 for r in rs if r['pnl_pct'] > 0),
                          sum(r['pnl_pct'] for r in rs))
                         for s, rs in by_sym.items()]
            print(f"[{form}] top 5 winners / top 5 losers")
            for s in sorted(sym_stats, key=lambda x: -x[3])[:5]:
                print(f"  {s[0]:<14} n={s[1]:>3} w={s[2]:>3} sum={s[3]*100:>+7.1f}%")
            for s in sorted(sym_stats, key=lambda x: x[3])[:5]:
                print(f"  {s[0]:<14} n={s[1]:>3} w={s[2]:>3} sum={s[3]*100:>+7.1f}%")

    finally:
        conn.close()


if __name__ == '__main__':
    main()
