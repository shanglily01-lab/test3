"""
M 顶子策略历史回测 (做空, 不设 SL/TP, 1 天持仓 timeout 兜底).
扫所有活跃 symbol, 滑窗每根 15m 调用 detect_m_top, 命中即模拟开仓.
出场: 1 天 timeout 平仓 (按 timeout 时刻的 close_price). 因为 M 顶不设 SL/TP.

参考阈值与 strategy_whale.detect_m_top / strategy_whale.detect_w_bottom 一致.
只读 kline_data.
用法: python scripts/diag/replay_m_top.py [DAYS]  默认 14
"""
import sys
from datetime import datetime, timezone
from collections import defaultdict
import pymysql

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DB = dict(host='13.212.252.171', port=3306, user='admin', password='Yintao@110',
          db='dimesion', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

# 同 strategy_whale.WB_*
WB_DATA_MIN_BARS    = 14 * 24    # 336 根 15m = 3.5 天
WB_REBOUND_MIN_PCT  = 0.05
WB_BOTTOM_DIFF_PCT  = 0.05
WB_B2_TO_NECK_MIN_H = 4
WB_TIME_GAP_MIN_H   = 24
WB_TIME_GAP_MAX_H   = 14 * 24
WB_BREAK_NECK_PCT   = 0.005
WB_HOLD_MIN         = 1 * 24 * 60   # 1 天
COOLDOWN_BARS       = 12 * 4         # 同币命中后冷却 12h (避免重复触发)

BAR_MS_15M = 15 * 60 * 1000


def detect_m_top(bars, pullback_min=WB_REBOUND_MIN_PCT, top_diff_max=WB_BOTTOM_DIFF_PCT,
                  break_neck=WB_BREAK_NECK_PCT, fail_log=None):
    """同 strategy_whale.detect_m_top, 但允许覆盖阈值用于敏感度分析.
    fail_log: 可选 dict, 每次失败时增加对应原因计数.
    """
    def _fail(reason):
        if fail_log is not None:
            fail_log[reason] = fail_log.get(reason, 0) + 1
        return None

    n = len(bars)
    if n < WB_DATA_MIN_BARS:
        return _fail('bars_not_enough')
    lows   = [float(b['low_price'])   for b in bars]
    highs  = [float(b['high_price'])  for b in bars]
    closes = [float(b['close_price']) for b in bars]

    i1 = max(range(n), key=lambda i: highs[i])
    h1 = highs[i1]
    if h1 <= 0:
        return _fail('h1_zero')
    if n - i1 < 48:
        return _fail('h1_too_recent')
    after_h1_lows = lows[i1+1:]
    if not after_h1_lows:
        return _fail('no_after_h1')
    id_rel = min(range(len(after_h1_lows)), key=lambda i: after_h1_lows[i])
    id_idx = i1 + 1 + id_rel
    d = lows[id_idx]
    if d <= 0:
        return _fail('d_zero')
    pullback = (h1 - d) / h1
    if pullback < pullback_min:
        return _fail('pullback_too_small')
    after_d_highs = highs[id_idx+1:]
    if not after_d_highs:
        return _fail('no_after_d')
    ih2_rel = max(range(len(after_d_highs)), key=lambda i: after_d_highs[i])
    ih2 = id_idx + 1 + ih2_rel
    h2 = highs[ih2]
    if (ih2 - id_idx) < WB_B2_TO_NECK_MIN_H:
        return _fail('h2_to_neck_too_close')
    if abs(h2 - h1) / h1 > top_diff_max:
        return _fail('top_diff_too_large')
    gap_h = ih2 - i1
    if gap_h < WB_TIME_GAP_MIN_H:
        return _fail('gap_too_short')
    if gap_h > WB_TIME_GAP_MAX_H:
        return _fail('gap_too_long')
    cur_p = closes[-1]
    if cur_p > d * (1 - break_neck):
        return _fail('not_break_neck')
    return {
        'h1': h1, 'd': d, 'h2': h2, 'cur_price': cur_p,
        'pullback': pullback,
        'top_diff': abs(h2-h1)/h1,
        'gap_h': gap_h,
    }


def simulate_short_timeout(cur, sym, signal_ms, entry_price):
    """M 顶不设 SL/TP, 模拟 1 天后 timeout 平仓的 pnl%.
    返回 dict 含 exit_reason, pnl_pct, max_profit_pct, max_drawdown_pct.
    """
    end_ms = signal_ms + WB_HOLD_MIN * 60 * 1000   # 1 天
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
    max_profit = 0.0
    max_dd = 0.0
    for b in bars:
        hi = float(b['high_price']); lo = float(b['low_price'])
        # SHORT: 价跌为盈, 价涨为亏
        favorable = (entry_price - lo) / entry_price
        adverse   = (hi - entry_price) / entry_price
        if favorable > max_profit:
            max_profit = favorable
        if adverse > max_dd:
            max_dd = adverse
    last_close = float(bars[-1]['close_price'])
    pnl_pct = (entry_price - last_close) / entry_price
    return {
        'exit_reason': 'timeout_1d',
        'pnl_pct': pnl_pct,
        'max_profit_pct': max_profit,
        'max_dd_pct': max_dd,
        'hold_bars': len(bars),
    }


def run_replay(cur, symbols, start_ms, end_ms, pre_ms,
                pullback_min, top_diff_max, break_neck, label, do_simulate=True):
    fail_log = {}
    all_results = []
    for sym in symbols:
        try:
            cur.execute(
                """SELECT open_time, open_price, high_price, low_price, close_price, volume
                   FROM kline_data
                   WHERE symbol=%s AND timeframe='15m'
                     AND open_time >= %s AND open_time < %s
                   ORDER BY open_time ASC""",
                (sym, pre_ms, end_ms),
            )
            bars = cur.fetchall()
        except Exception:
            continue
        if len(bars) < WB_DATA_MIN_BARS:
            continue
        last_hit = -999
        for i in range(WB_DATA_MIN_BARS, len(bars)):
            bar_close_ms = bars[i]['open_time'] + BAR_MS_15M
            if bar_close_ms < start_ms:
                continue
            if i - last_hit < COOLDOWN_BARS:
                continue
            sub = bars[:i + 1]
            sig = detect_m_top(sub, pullback_min=pullback_min,
                                top_diff_max=top_diff_max,
                                break_neck=break_neck,
                                fail_log=fail_log)
            if not sig:
                continue
            last_hit = i
            signal_ms = bar_close_ms
            if do_simulate:
                exit_info = simulate_short_timeout(cur, sym, signal_ms, sig['cur_price'])
                if not exit_info:
                    continue
            else:
                exit_info = {}
            all_results.append({
                'sym': sym,
                'signal_ms': signal_ms,
                **sig, **exit_info,
            })
    return all_results, fail_log


def print_summary(results, label):
    print("=" * 80)
    print(f"[{label}]  命中 {len(results)}")
    print("=" * 80)
    if not results:
        return
    wins = [r for r in results if r.get('pnl_pct', 0) > 0]
    losses = [r for r in results if r.get('pnl_pct', 0) <= 0]
    n = len(results)
    avg = sum(r['pnl_pct'] for r in results) / n
    s = sum(r['pnl_pct'] for r in results)
    wr = len(wins) / n * 100 if n else 0
    avg_w = sum(r['pnl_pct'] for r in wins) / len(wins) if wins else 0
    avg_l = sum(r['pnl_pct'] for r in losses) / len(losses) if losses else 0
    exp = wr/100 * avg_w + (1 - wr/100) * avg_l
    print(f"  胜率 {wr:.1f}%  均 pnl {avg*100:+.2f}%  累计 {s*100:+.1f}%  期望 {exp*100:+.2f}%")
    if wins:
        print(f"  平均赢 {avg_w*100:+.2f}%  最大赢 {max(r['pnl_pct'] for r in results)*100:+.2f}%")
    if losses:
        print(f"  平均亏 {avg_l*100:+.2f}%  最大亏 {min(r['pnl_pct'] for r in results)*100:+.2f}%")


def main():
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 14
    conn = pymysql.connect(**DB); cur = conn.cursor()
    try:
        end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        start_ms = end_ms - days * 24 * 3600 * 1000
        pre_ms = start_ms - 4 * 24 * 3600 * 1000

        cur.execute(
            """SELECT DISTINCT symbol FROM kline_data
               WHERE timeframe='15m' AND open_time >= %s""",
            (start_ms,),
        )
        symbols = [r['symbol'] for r in cur.fetchall()]
        print(f"\n### M 顶回测 {days} 天 / {len(symbols)} symbols ###\n")

        # 严格版
        strict, sf_log = run_replay(cur, symbols, start_ms, end_ms, pre_ms,
                                     pullback_min=0.05, top_diff_max=0.05, break_neck=0.005,
                                     label='strict')
        print_summary(strict, '严格版 反弹5% 两顶差5% 跌破颈线-0.5%')
        print()
        if not strict:
            print("严格版 0 命中, 失败原因分布 (前 5):")
            for k, v in sorted(sf_log.items(), key=lambda x: -x[1])[:5]:
                print(f"  {k:<30}n={v}")
            print()

        # 中等版
        mid, mf_log = run_replay(cur, symbols, start_ms, end_ms, pre_ms,
                                  pullback_min=0.04, top_diff_max=0.07, break_neck=0.002,
                                  label='medium')
        print_summary(mid, '中等版 反弹4% 两顶差7% 跌破颈线-0.2%')
        print()

        # 宽松版
        loose, lf_log = run_replay(cur, symbols, start_ms, end_ms, pre_ms,
                                    pullback_min=0.03, top_diff_max=0.10, break_neck=0.0,
                                    label='loose')
        print_summary(loose, '宽松版 反弹3% 两顶差10% 跌破颈线 0%')

        # 中等版按 symbol 看
        if mid:
            print()
            print("=" * 80)
            print("[中等版按 symbol top 8 盈利]")
            print("=" * 80)
            by_sym = defaultdict(list)
            for r in mid:
                by_sym[r['sym']].append(r)
            stats = sorted([(s_, len(rs), sum(1 for r in rs if r['pnl_pct'] > 0),
                             sum(r['pnl_pct'] for r in rs))
                            for s_, rs in by_sym.items()],
                           key=lambda x: -x[3])
            for s_, n, w, pn in stats[:8]:
                print(f"  {s_:<14} n={n:>3} w={w:>3}  累计 {pn*100:>+7.1f}%")
            return  # main 提前返回, 跳过原来的 all_results 处理

        print(f"总命中信号: {len(all_results)}\n")
        if not all_results:
            return

        # 整体汇总
        wins = [r for r in all_results if r['pnl_pct'] > 0]
        losses = [r for r in all_results if r['pnl_pct'] <= 0]
        avg = sum(r['pnl_pct'] for r in all_results) / len(all_results)
        avg_w = sum(r['pnl_pct'] for r in wins) / len(wins) if wins else 0
        avg_l = sum(r['pnl_pct'] for r in losses) / len(losses) if losses else 0
        s = sum(r['pnl_pct'] for r in all_results)
        wr = len(wins) / len(all_results) * 100
        exp = wr/100 * avg_w + (1 - wr/100) * avg_l
        print("=" * 80)
        print("[M 顶回测整体]")
        print("=" * 80)
        print(f"  信号总数:   {len(all_results)}")
        print(f"  胜率:       {wr:.1f}% ({len(wins)}/{len(all_results)})")
        print(f"  平均 pnl%:  {avg*100:+.2f}%")
        print(f"  累计 pnl%:  {s*100:+.1f}%")
        print(f"  平均赢:     {avg_w*100:+.2f}%   平均亏:     {avg_l*100:+.2f}%")
        print(f"  最大赢:     {max(r['pnl_pct'] for r in all_results)*100:+.2f}%")
        print(f"  最大亏:     {min(r['pnl_pct'] for r in all_results)*100:+.2f}%")
        print(f"  单笔期望:   {exp*100:+.2f}%")
        print()

        # 按 symbol 分组 top
        by_sym = defaultdict(list)
        for r in all_results:
            by_sym[r['sym']].append(r)
        sym_stats = sorted(
            [(s_, len(rs), sum(1 for r in rs if r['pnl_pct'] > 0),
              sum(r['pnl_pct'] for r in rs))
             for s_, rs in by_sym.items()],
            key=lambda x: -x[3]
        )
        print("=" * 80)
        print(f"[按 symbol top 10 盈利 / top 10 亏损]")
        print("=" * 80)
        print(f"  {'sym':<14}{'n':>4}{'wins':>5}{'sum%':>10}")
        for s_, n, w, pn in sym_stats[:10]:
            print(f"  {s_:<14}{n:>4}{w:>5}{pn*100:>+9.1f}%")
        print("  ---")
        for s_, n, w, pn in sorted(sym_stats, key=lambda x: x[3])[:10]:
            print(f"  {s_:<14}{n:>4}{w:>5}{pn*100:>+9.1f}%")
        print()

        # 按 max_profit_pct 看 (策略不设 TP, 看到底浮盈过多少)
        print("=" * 80)
        print("[浮盈分布 — 让我们知道是否设 TP 能改善]")
        print("=" * 80)
        bins = [(0, 0.02), (0.02, 0.05), (0.05, 0.10), (0.10, 0.20), (0.20, 1.0)]
        for lo, hi in bins:
            n = sum(1 for r in all_results if lo <= r['max_profit_pct'] < hi)
            print(f"  max_profit ∈ [{lo*100:.0f}%, {hi*100:.0f}%): {n}")
        # 浮盈达 10% 以上的占比
        n_10pct = sum(1 for r in all_results if r['max_profit_pct'] >= 0.10)
        print(f"  浮盈达 ≥10% 的: {n_10pct}/{len(all_results)} ({n_10pct/len(all_results)*100:.1f}%)")

    finally:
        conn.close()


if __name__ == '__main__':
    main()
