"""
排查今天 (本地时区) 被"反转+移动止损"割掉的单子。

目标回答三个问题:
1. 今天一共开了多少单? 多少盈 / 多少亏?
2. "反转单"有多少? 定义: max_profit_pct >= 0.5% 但 realized_pnl < 0 (吃过浮盈又被打回亏损)
3. 移动止损触发率 / 移动止损触发后多久又走回原方向?

分 paper (futures_positions) / live (live_futures_positions) 两块看。
直连远程 dimesion 库, 不读 .env (本地 .env 是过时开发库).

用法: python scripts/diag/diag_today_reversal.py [YYYY-MM-DD]
默认今天.
"""
import sys
from datetime import datetime, date
from pathlib import Path

import pymysql

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DB_CFG = dict(
    host='13.212.252.171', port=3306,
    user='admin', password='Yintao@110',
    db='dimesion', charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor,
)

TARGET_DATE = sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat()
DAY_START = f"{TARGET_DATE} 00:00:00"
DAY_END = f"{TARGET_DATE} 23:59:59"


def connect():
    return pymysql.connect(**DB_CFG)


def fmt_pct(v):
    if v is None:
        return "   N/A"
    try:
        return f"{float(v):+6.2f}%"
    except Exception:
        return str(v)


def fmt_money(v):
    if v is None:
        return "   N/A"
    try:
        return f"{float(v):+9.2f}"
    except Exception:
        return str(v)


def analyze_paper(cur):
    print("=" * 80)
    print(f"[PAPER] futures_positions  (close_time in {TARGET_DATE})")
    print("=" * 80)
    # 注意: futures_positions 没有 close_reason 列, 只有 status + notes
    cur.execute(
        """
        SELECT id, symbol, position_side, strategy_id,
               entry_price, quantity, leverage,
               stop_loss_price, take_profit_price,
               trailing_stop_activated, trailing_stop_price,
               max_profit_pct, max_profit_price, max_profit_time,
               realized_pnl, unrealized_pnl_pct,
               open_time, close_time, status, notes
        FROM futures_positions
        WHERE close_time BETWEEN %s AND %s
          AND status IN ('closed', 'liquidated')
        ORDER BY close_time ASC
        """,
        (DAY_START, DAY_END),
    )
    rows = cur.fetchall()
    if not rows:
        print("  (no closed paper positions today)\n")
        return []

    total = len(rows)
    wins = sum(1 for r in rows if (r['realized_pnl'] or 0) > 0)
    losses = sum(1 for r in rows if (r['realized_pnl'] or 0) < 0)
    total_pnl = sum(float(r['realized_pnl'] or 0) for r in rows)
    trailing_triggered = sum(1 for r in rows if r['trailing_stop_activated'])
    # 反转定义: 吃过 >= 0.5% 的浮盈, 最终亏损平仓
    reversals = [
        r for r in rows
        if float(r['max_profit_pct'] or 0) >= 0.5
        and float(r['realized_pnl'] or 0) < 0
    ]

    print(f"  total={total}  wins={wins}  losses={losses}  pnl_sum={total_pnl:+.2f}")
    print(f"  trailing_stop_activated={trailing_triggered}/{total}  reversals(maxP>=0.5% & lost)={len(reversals)}")
    print()

    print("  -- 仓位明细 (按平仓时间) --")
    print(f"  {'id':>5} {'symbol':<14}{'side':<6} {'entry':>10} {'maxP%':>7} {'pnl':>9} "
          f"{'trail':<5} {'open':<16} {'close':<16}")
    for r in rows:
        trail = 'YES' if r['trailing_stop_activated'] else '-'
        print(
            f"  {r['id']:>5} {r['symbol']:<14}{r['position_side']:<6} "
            f"{float(r['entry_price']):>10.4f} {fmt_pct(r['max_profit_pct'])} "
            f"{fmt_money(r['realized_pnl'])} {trail:<5} "
            f"{str(r['open_time'])[:16]:<16} {str(r['close_time'])[:16]:<16}"
        )
        if r['notes']:
            notes = r['notes'].strip().replace('\n', ' ')[:180]
            print(f"        notes: {notes}")
    print()

    if reversals:
        print(f"  -- 反转单 ({len(reversals)} 笔, 吃过 >=0.5% 浮盈后被打回亏损) --")
        for r in reversals:
            entry = float(r['entry_price'])
            max_p_price = r['max_profit_price']
            side = r['position_side']
            # 计算浮盈从哪个价位掉下来的
            print(
                f"  #{r['id']} {r['symbol']} {side}  entry={entry:.4f} "
                f"maxP={fmt_pct(r['max_profit_pct'])} at {max_p_price} "
                f"(time={r['max_profit_time']})  "
                f"realized={fmt_money(r['realized_pnl'])}"
            )
            if r['trailing_stop_activated']:
                print(f"      trailing_stop was ACTIVATED, trail_price={r['trailing_stop_price']}")
        print()

    return rows


def analyze_live(cur):
    print("=" * 80)
    print(f"[LIVE] live_futures_positions  (close_time in {TARGET_DATE})")
    print("=" * 80)
    cur.execute(
        """
        SELECT id, symbol, position_side, strategy_id,
               entry_price, close_price, quantity, leverage,
               stop_loss_price, take_profit_price,
               trailing_stop_activated, trailing_stop_price,
               max_profit_pct, max_profit_price,
               realized_pnl, close_reason,
               open_time, close_time, status, notes
        FROM live_futures_positions
        WHERE close_time BETWEEN %s AND %s
          AND status IN ('CLOSED', 'LIQUIDATED')
        ORDER BY close_time ASC
        """,
        (DAY_START, DAY_END),
    )
    rows = cur.fetchall()
    if not rows:
        print("  (no closed live positions today)\n")
        return []

    total = len(rows)
    wins = sum(1 for r in rows if (r['realized_pnl'] or 0) > 0)
    losses = sum(1 for r in rows if (r['realized_pnl'] or 0) < 0)
    total_pnl = sum(float(r['realized_pnl'] or 0) for r in rows)
    trailing_triggered = sum(1 for r in rows if r['trailing_stop_activated'])
    reversals = [
        r for r in rows
        if float(r['max_profit_pct'] or 0) >= 0.5
        and float(r['realized_pnl'] or 0) < 0
    ]

    print(f"  total={total}  wins={wins}  losses={losses}  pnl_sum={total_pnl:+.4f}")
    print(f"  trailing_stop_activated={trailing_triggered}/{total}  reversals(maxP>=0.5% & lost)={len(reversals)}")
    print()

    # 按 close_reason 分组
    reason_stat = {}
    for r in rows:
        reason = r['close_reason'] or '(none)'
        reason_stat.setdefault(reason, []).append(r)
    print("  -- 按 close_reason 分组 --")
    for reason, lst in sorted(reason_stat.items(), key=lambda kv: -len(kv[1])):
        pnl = sum(float(x['realized_pnl'] or 0) for x in lst)
        print(f"    {reason:<30} {len(lst):>3} 笔  pnl={pnl:+.4f}")
    print()

    print("  -- 仓位明细 --")
    print(f"  {'id':>5} {'symbol':<14}{'side':<6} {'entry':>10} {'close':>10} "
          f"{'maxP%':>7} {'pnl':>11} {'trail':<5} {'reason':<20} {'open':<16}")
    for r in rows:
        trail = 'YES' if r['trailing_stop_activated'] else '-'
        close_p = float(r['close_price']) if r['close_price'] else 0
        print(
            f"  {r['id']:>5} {r['symbol']:<14}{r['position_side']:<6} "
            f"{float(r['entry_price']):>10.4f} {close_p:>10.4f} "
            f"{fmt_pct(r['max_profit_pct'])} {fmt_money(r['realized_pnl'])} "
            f"{trail:<5} {(r['close_reason'] or '-')[:20]:<20} "
            f"{str(r['open_time'])[:16]:<16}"
        )
    print()

    if reversals:
        print(f"  -- 反转单 ({len(reversals)} 笔) --")
        for r in reversals:
            entry = float(r['entry_price'])
            close_p = float(r['close_price']) if r['close_price'] else 0
            peak = r['max_profit_price']
            side = r['position_side']
            # 估算从峰值到平仓价的回撤 (相对开仓价百分比)
            if peak and entry:
                if side == 'LONG':
                    drawdown_pct = (float(peak) - close_p) / entry * 100
                else:
                    drawdown_pct = (close_p - float(peak)) / entry * 100
            else:
                drawdown_pct = None
            print(
                f"  #{r['id']} {r['symbol']} {side}  entry={entry:.4f} peak={peak} "
                f"close={close_p:.4f}  maxP={fmt_pct(r['max_profit_pct'])} "
                f"final={fmt_money(r['realized_pnl'])} "
                f"reason={r['close_reason']}"
            )
            if drawdown_pct is not None:
                print(f"      peak→close drawdown: {drawdown_pct:.2f}% (of entry)  trail_activated={bool(r['trailing_stop_activated'])}")
        print()

    return rows


def analyze_by_strategy(cur, paper_rows, live_rows):
    print("=" * 80)
    print("[STRATEGY 汇总]")
    print("=" * 80)
    all_rows = [('paper', r) for r in paper_rows] + [('live', r) for r in live_rows]
    by_strat = {}
    for env, r in all_rows:
        sid = r.get('strategy_id') or 0
        by_strat.setdefault((env, sid), []).append(r)
    for (env, sid), lst in sorted(by_strat.items()):
        pnl = sum(float(x['realized_pnl'] or 0) for x in lst)
        wins = sum(1 for x in lst if (x['realized_pnl'] or 0) > 0)
        rev = sum(
            1 for x in lst
            if float(x['max_profit_pct'] or 0) >= 0.5
            and float(x['realized_pnl'] or 0) < 0
        )
        print(f"  env={env:<5} strategy_id={sid}  n={len(lst):>3} wins={wins:>2} reversals={rev:>2} pnl={pnl:+.4f}")
    print()


def main():
    conn = connect()
    try:
        cur = conn.cursor()
        print(f"\n### 复盘日期: {TARGET_DATE} ###\n")
        paper = analyze_paper(cur)
        live = analyze_live(cur)
        analyze_by_strategy(cur, paper, live)
    finally:
        conn.close()


if __name__ == '__main__':
    main()
