"""
假设验证: 当 CHASE 被 24h<-10% 过滤拒绝时, 那一刻 SHORT 的表现如何?

场景定义 (完全匹配 strategy_live.chase_tick 前三步, 只是不看 24h 方向):
  1. 最近 24 根 5m K 线 (=2h) 涨幅 >= 12%  (pump 条件)
  2. 窗口内从高点回撤 <= 6%               (未耗竭)
  3. 窗口内至少一根 5m bar 涨幅 >= 3%      (急拉验证)
  4. 24h 变化 <= -10%                     (熊市反弹 — 原策略拒绝开多的这批)

对每个命中的时刻 T, 看:
  - T+30min / T+1h / T+3h / T+6h / T+12h 的价格变化
  - "假设 T 时 SHORT" 的理论最大浮盈 (peak drop) 和最大浮亏 (peak rise)
  - 按止损 8% / 止盈 20% 模拟: 胜率 + 期望

只读 kline_data, 不改策略代码.
用法: python scripts/diag/replay_bear_bounce_short.py [DAYS]
默认 DAYS=14
"""
import sys
from datetime import date, datetime, timedelta, timezone
from collections import defaultdict
import pymysql

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DB = dict(host='13.212.252.171', port=3306, user='admin', password='Yintao@110',
          db='dimesion', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

# 复刻 strategy_live 常量
CHASE_PUMP_BARS = 24          # 2h
CHASE_PUMP_PCT = 0.12         # 12%
CHASE_EXHAUST_MAX_DD = 0.06   # 6%
CHASE_LEADER_BAR_MIN_PCT = 0.03  # 3%
CHASE_24H_THRESH = -0.10      # 24h 跌幅阈值 (用户方案的触发区)

BAR_MS = 5 * 60 * 1000

# 模拟仓位参数 (和 chase 做空版一样)
SHORT_SL_PCT = 0.08           # 止损 8%
SHORT_TP_PCT = 0.20           # 止盈 20%
MAX_HOLD_BARS = 72            # 最大持仓 72 根 5m = 6 小时

DAYS = int(sys.argv[1]) if len(sys.argv) > 1 else 14


def scan_symbol(cur, sym: str, start_ms: int, end_ms: int):
    """
    扫 sym 的 5m kline, 找所有符合 "2h pump>=12% & 24h<=-10% & 其他 chase 条件" 的时刻.
    返回 list of (signal_time, signal_price)
    """
    # 取区间内所有 5m kline
    cur.execute(
        """SELECT open_time, open_price, high_price, low_price, close_price
           FROM kline_data
           WHERE symbol=%s AND timeframe='5m'
             AND open_time BETWEEN %s AND %s
           ORDER BY open_time ASC""",
        (sym, start_ms - 24 * 3600 * 1000, end_ms),  # 往前多取 24h 做 24h change
    )
    bars = cur.fetchall()
    if len(bars) < CHASE_PUMP_BARS + 2:
        return []

    # 提前建立一个 open_time -> index 的 map 用于 24h 查找
    signals = []
    for i in range(CHASE_PUMP_BARS + 2, len(bars)):
        b = bars[i]
        if b['open_time'] < start_ms:
            continue
        # 取 CHASE_PUMP_BARS 根作为窗口
        window = bars[i - CHASE_PUMP_BARS:i]
        if len(window) < CHASE_PUMP_BARS:
            continue
        wo = float(window[0]['open_price'])
        if wo <= 0:
            continue
        cur_close = float(b['close_price'])
        pump = (cur_close - wo) / wo
        if pump < CHASE_PUMP_PCT:
            continue

        # 耗竭
        recent_high = max(float(x['high_price']) for x in window)
        if recent_high <= 0:
            continue
        dd = (recent_high - cur_close) / recent_high
        if dd > CHASE_EXHAUST_MAX_DD:
            continue

        # 急拉
        leader = max(
            (float(x['close_price']) - float(x['open_price'])) / float(x['open_price'])
            for x in window if float(x['open_price']) > 0
        )
        if leader < CHASE_LEADER_BAR_MIN_PCT:
            continue

        # 24h 变化 (往前找 ~288 根 5m 的 open_price)
        target_ms = b['open_time'] - 24 * 3600 * 1000
        # 在 bars 里二分或顺序找
        ref = None
        for j in range(i - 1, -1, -1):
            if bars[j]['open_time'] <= target_ms:
                ref = bars[j]
                break
        if not ref:
            continue
        ref_p = float(ref['open_price'])
        if ref_p <= 0:
            continue
        ch24 = (cur_close - ref_p) / ref_p
        if ch24 > CHASE_24H_THRESH:
            continue

        # 信号命中: 符合 "被 24h 过滤拒绝追多" 的场景
        signals.append({
            'sym': sym,
            'signal_ms': b['open_time'] + BAR_MS,  # bar 收盘时刻
            'signal_price': cur_close,
            'pump_pct': pump,
            'ch24': ch24,
            'dd_from_peak': dd,
            'leader_bar': leader,
        })
    return signals


def simulate_short(cur, sym: str, signal_ms: int, entry: float):
    """假设 T 时刻以 entry 价 SHORT, 看后续 72 根 5m 价格, 算结果."""
    end_ms = signal_ms + MAX_HOLD_BARS * BAR_MS
    cur.execute(
        """SELECT open_time, high_price, low_price, close_price
           FROM kline_data
           WHERE symbol=%s AND timeframe='5m'
             AND open_time >= %s AND open_time < %s
           ORDER BY open_time ASC""",
        (sym, signal_ms, end_ms),
    )
    bars = cur.fetchall()
    if not bars:
        return None

    max_profit = 0.0   # SHORT: 价格下跌 -> 浮盈正
    max_loss = 0.0     # SHORT: 价格上涨 -> 浮亏 (绝对值)
    hit_sl = False
    hit_tp = False
    exit_reason = 'timeout'
    exit_price = entry
    exit_ms = signal_ms

    for b in bars:
        hi = float(b['high_price'])
        lo = float(b['low_price'])
        # SHORT 的浮亏 = 价涨; 浮盈 = 价跌
        adverse = (hi - entry) / entry   # 价涨幅度
        favorable = (entry - lo) / entry  # 价跌幅度
        max_loss = max(max_loss, adverse)
        max_profit = max(max_profit, favorable)
        # 严格顺序: 同 bar 内看 HL 哪个先到 — 保守假设先看 adverse
        if adverse >= SHORT_SL_PCT:
            hit_sl = True
            exit_reason = 'sl'
            exit_price = entry * (1 + SHORT_SL_PCT)
            exit_ms = b['open_time']
            break
        if favorable >= SHORT_TP_PCT:
            hit_tp = True
            exit_reason = 'tp'
            exit_price = entry * (1 - SHORT_TP_PCT)
            exit_ms = b['open_time']
            break
    else:
        # 超时
        last = bars[-1]
        exit_price = float(last['close_price'])
        exit_ms = last['open_time']

    pnl_pct = (entry - exit_price) / entry  # SHORT 盈亏率
    return {
        'exit_reason': exit_reason,
        'exit_price': exit_price,
        'exit_ms': exit_ms,
        'pnl_pct': pnl_pct,
        'max_profit': max_profit,
        'max_loss': max_loss,
        'hold_bars': len(bars),
    }


def main():
    conn = pymysql.connect(**DB); cur = conn.cursor()
    try:
        end_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        start_ms = end_ms - DAYS * 24 * 3600 * 1000

        # 取近 N 天活跃过的 symbol (在 kline_data 里有 5m 记录)
        cur.execute(
            """SELECT DISTINCT symbol FROM kline_data
               WHERE timeframe='5m' AND open_time >= %s""",
            (start_ms,),
        )
        symbols = [r['symbol'] for r in cur.fetchall()]
        print(f"\n### 回放 'CHASE 被 24h<-10% 过滤拒绝 那一刻假设 SHORT' ###")
        print(f"### 窗口: 最近 {DAYS} 天 / 候选 {len(symbols)} 个 symbol ###\n")

        all_signals = []
        for i, sym in enumerate(symbols, 1):
            if i % 50 == 0:
                print(f"  ...已扫 {i}/{len(symbols)}")
            try:
                sigs = scan_symbol(cur, sym, start_ms, end_ms)
                all_signals.extend(sigs)
            except Exception as e:
                print(f"  {sym} 扫描失败: {e}")

        print(f"\n总命中信号: {len(all_signals)} 次\n")

        if not all_signals:
            print("无信号命中, 结束")
            return

        # 对每个信号模拟 SHORT
        results = []
        for s in all_signals:
            sim = simulate_short(cur, s['sym'], s['signal_ms'], s['signal_price'])
            if sim:
                results.append({**s, **sim})

        print(f"有后续 kline 的信号: {len(results)}\n")

        # 汇总
        wins = [r for r in results if r['pnl_pct'] > 0]
        losses = [r for r in results if r['pnl_pct'] <= 0]
        sl_hits = [r for r in results if r['exit_reason'] == 'sl']
        tp_hits = [r for r in results if r['exit_reason'] == 'tp']
        timeouts = [r for r in results if r['exit_reason'] == 'timeout']

        def avg_pct(lst, key='pnl_pct'):
            return sum(r[key] for r in lst) / len(lst) if lst else 0

        total_pnl_pct = sum(r['pnl_pct'] for r in results)
        avg_pnl_pct = total_pnl_pct / len(results) if results else 0
        win_rate = len(wins) / len(results) * 100 if results else 0

        print("=" * 90)
        print("[结果汇总: 假设每次都 SHORT, SL=8%, TP=20%, 最大持仓 6h]")
        print("=" * 90)
        print(f"  信号总数:     {len(results)}")
        print(f"  盈利 (win):   {len(wins)}   ({win_rate:.1f}%)")
        print(f"  亏损 (loss):  {len(losses)}")
        print(f"  - 出场方式: TP 止盈 {len(tp_hits)}  /  SL 止损 {len(sl_hits)}  /  超时 {len(timeouts)}")
        print(f"  平均 pnl%:    {avg_pnl_pct*100:+.2f}%")
        print(f"  累计 pnl%:    {total_pnl_pct*100:+.2f}% (按比例, 不含杠杆)")
        if wins:
            print(f"  赢家平均:     {avg_pct(wins)*100:+.2f}%  最大: {max(r['pnl_pct'] for r in wins)*100:+.2f}%")
        if losses:
            print(f"  输家平均:     {avg_pct(losses)*100:+.2f}%  最差: {min(r['pnl_pct'] for r in losses)*100:+.2f}%")
        # 期望: 胜率 * 平均赢 + 败率 * 平均亏
        avg_w = avg_pct(wins); avg_l = avg_pct(losses)
        exp = (len(wins)/len(results))*avg_w + (len(losses)/len(results))*avg_l if results else 0
        print(f"  单笔期望 %:   {exp*100:+.2f}%")
        print()

        # 按 symbol 看哪些赚
        by_sym = defaultdict(list)
        for r in results:
            by_sym[r['sym']].append(r)
        print("=" * 90)
        print("[按 symbol 分组 top 15 盈利 + top 15 亏损]")
        print("=" * 90)
        sym_stats = []
        for sym, rs in by_sym.items():
            pnl_sum = sum(r['pnl_pct'] for r in rs)
            sym_stats.append({
                'sym': sym, 'n': len(rs), 'pnl_sum': pnl_sum,
                'wins': sum(1 for r in rs if r['pnl_pct'] > 0),
            })
        print(f"  {'symbol':<16}{'n':>4}{'wins':>5}{'net_pnl%':>11}{'avg%':>9}")
        for s in sorted(sym_stats, key=lambda x: -x['pnl_sum'])[:15]:
            print(f"  {s['sym']:<16}{s['n']:>4}{s['wins']:>5}"
                  f"{s['pnl_sum']*100:>+11.2f}{s['pnl_sum']/s['n']*100:>+9.2f}")
        print("  ---")
        for s in sorted(sym_stats, key=lambda x: x['pnl_sum'])[:15]:
            print(f"  {s['sym']:<16}{s['n']:>4}{s['wins']:>5}"
                  f"{s['pnl_sum']*100:>+11.2f}{s['pnl_sum']/s['n']*100:>+9.2f}")
        print()

        # 最差 10 笔, 帮助看 SHORT 被轧的情形
        print("=" * 90)
        print("[最差 10 笔]")
        print("=" * 90)
        print(f"  {'symbol':<16}{'pump%':>7}{'24h%':>7}{'pnl%':>7}{'maxL%':>7}{'exit':<10}{'signal_time'}")
        for r in sorted(results, key=lambda x: x['pnl_pct'])[:10]:
            sig_dt = datetime.fromtimestamp(r['signal_ms']/1000, tz=timezone.utc).strftime('%m-%d %H:%M')
            print(f"  {r['sym']:<16}{r['pump_pct']*100:>7.1f}{r['ch24']*100:>7.1f}"
                  f"{r['pnl_pct']*100:>+7.1f}{r['max_loss']*100:>7.1f}"
                  f"  {r['exit_reason']:<8}{sig_dt}")
        print()

        # 最好 10 笔
        print("=" * 90)
        print("[最好 10 笔]")
        print("=" * 90)
        print(f"  {'symbol':<16}{'pump%':>7}{'24h%':>7}{'pnl%':>7}{'maxP%':>7}{'exit':<10}{'signal_time'}")
        for r in sorted(results, key=lambda x: -x['pnl_pct'])[:10]:
            sig_dt = datetime.fromtimestamp(r['signal_ms']/1000, tz=timezone.utc).strftime('%m-%d %H:%M')
            print(f"  {r['sym']:<16}{r['pump_pct']*100:>7.1f}{r['ch24']*100:>7.1f}"
                  f"{r['pnl_pct']*100:>+7.1f}{r['max_profit']*100:>7.1f}"
                  f"  {r['exit_reason']:<8}{sig_dt}")
    finally:
        conn.close()


if __name__ == '__main__':
    main()
