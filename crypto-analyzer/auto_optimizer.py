#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
auto_optimizer.py - 每小时自动分析交易表现并优化参数
- 分析已平仓交易的信号表现
- 自动调整 signal_scoring_weights（+/-10%，有上下限保护）
- 自动调整 threshold（胜率过低则收紧，信号过少则放宽）
- 输出详细报告到 logs/optimizer_YYYY-MM-DD.log
- 必要时重启 smart_trader_service
"""
import os
import sys
import json
import time
import datetime
import pymysql
import psutil
import subprocess
from collections import defaultdict
from dotenv import load_dotenv

os.chdir(os.path.dirname(os.path.abspath(__file__)))
load_dotenv()

# ── DB 连接 ──────────────────────────────────────────────────────────
DB_CONFIG = {
    'host':     os.getenv('DB_HOST', 'localhost'),
    'port':     int(os.getenv('DB_PORT', '3306')),
    'user':     os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', ''),
    'database': os.getenv('DB_NAME', 'binance-data'),
    'charset':  'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor,
    'autocommit': False,
}

ACCOUNT_ID = 2

# ── 调整幅度限制 ──────────────────────────────────────────────────────
WEIGHT_ADJUST_STEP   = 0.10   # 每次最多调整 10%
WEIGHT_MIN_RATIO     = 0.50   # 相对 base_weight 最低 50%
WEIGHT_MAX_RATIO     = 2.00   # 相对 base_weight 最高 200%
THRESHOLD_RAISE_STEP = 3      # 胜率差时收紧阈值步长
THRESHOLD_LOWER_STEP = 3      # 信号太少时放宽阈值步长
THRESHOLD_MIN        = 55     # 阈值下限
THRESHOLD_MAX        = 80     # 阈值上限
WIN_RATE_RAISE_GATE  = 0.40   # 胜率低于此值时收紧
WIN_RATE_LOWER_GATE  = 0.65   # 胜率高于此值且样本足够时放宽
MIN_TRADES_FOR_ADJ   = 5      # 至少 N 笔已平仓才做权重调整
ANALYSIS_WINDOW_H    = 4      # 分析最近 N 小时

# ── 日志 ─────────────────────────────────────────────────────────────
LOG_DIR  = 'logs'
today    = datetime.date.today().strftime('%Y-%m-%d')
LOG_FILE = os.path.join(LOG_DIR, f'optimizer_{today}.log')


def log(msg: str):
    ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f"{ts} | {msg}"
    print(line)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(line + '\n')


# ── 数据库工具 ────────────────────────────────────────────────────────
def get_conn():
    return pymysql.connect(**DB_CONFIG)


def get_setting(cur, key: str, default):
    cur.execute(
        "SELECT setting_value FROM system_settings WHERE setting_key=%s",
        (key,)
    )
    row = cur.fetchone()
    if row:
        try:
            return type(default)(row['setting_value'])
        except Exception:
            return default
    return default


def set_setting(conn, cur, key: str, value):
    cur.execute(
        "UPDATE system_settings SET setting_value=%s, updated_at=NOW() WHERE setting_key=%s",
        (str(value), key)
    )
    if cur.rowcount == 0:
        cur.execute(
            "INSERT INTO system_settings (setting_key, setting_value, updated_by) VALUES (%s,%s,'auto_optimizer')",
            (key, str(value))
        )
    conn.commit()


# ── 核心分析 ──────────────────────────────────────────────────────────
def analyze_closed_trades(cur, hours: int):
    """分析最近 hours 小时内已平仓交易"""
    since = datetime.datetime.now() - datetime.timedelta(hours=hours)
    cur.execute("""
        SELECT symbol, position_side, entry_score, signal_components,
               realized_pnl, margin, close_time, open_time
        FROM futures_positions
        WHERE account_id=%s AND status='closed' AND close_time >= %s
        ORDER BY close_time DESC
    """, (ACCOUNT_ID, since))
    trades = cur.fetchall()

    if not trades:
        return {
            'count': 0, 'wins': 0, 'losses': 0,
            'win_rate': None, 'profit_factor': None,
            'total_pnl': 0.0, 'avg_win': 0.0, 'avg_loss': 0.0,
            'signal_stats': {}, 'trades': []
        }

    wins   = [t for t in trades if float(t['realized_pnl'] or 0) > 0]
    losses = [t for t in trades if float(t['realized_pnl'] or 0) <= 0]

    total_win  = sum(float(t['realized_pnl']) for t in wins)
    total_loss = abs(sum(float(t['realized_pnl']) for t in losses)) or 0.001

    # 每个信号组件的胜/负贡献统计
    sig_wins   = defaultdict(int)
    sig_losses = defaultdict(int)
    for t in trades:
        is_win  = float(t['realized_pnl'] or 0) > 0
        try:
            comps = json.loads(t['signal_components'] or '{}')
        except Exception:
            comps = {}
        for sig in comps:
            if is_win:
                sig_wins[sig]   += 1
            else:
                sig_losses[sig] += 1

    signal_stats = {}
    all_sigs = set(sig_wins) | set(sig_losses)
    for sig in all_sigs:
        w = sig_wins[sig]
        l = sig_losses[sig]
        total = w + l
        signal_stats[sig] = {
            'wins': w, 'losses': l, 'total': total,
            'win_rate': w / total if total else None
        }

    return {
        'count':         len(trades),
        'wins':          len(wins),
        'losses':        len(losses),
        'win_rate':      len(wins) / len(trades),
        'profit_factor': total_win / total_loss,
        'total_pnl':     total_win - total_loss + total_loss,  # net
        'total_pnl_net': sum(float(t['realized_pnl'] or 0) for t in trades),
        'avg_win':       total_win  / len(wins)   if wins   else 0,
        'avg_loss':      -total_loss / len(losses) if losses else 0,
        'signal_stats':  signal_stats,
        'trades':        trades,
    }


def analyze_open_positions(cur):
    """分析当前持仓状态"""
    cur.execute("""
        SELECT symbol, position_side, entry_score, unrealized_pnl,
               margin, open_time, stop_loss_price, take_profit_price,
               entry_price, signal_components
        FROM futures_positions
        WHERE account_id=%s AND status='open'
        ORDER BY unrealized_pnl ASC
    """, (ACCOUNT_ID,))
    positions = cur.fetchall()

    total_pnl = sum(float(p['unrealized_pnl'] or 0) for p in positions)
    total_margin = sum(float(p['margin'] or 0) for p in positions)

    return {
        'count':        len(positions),
        'total_pnl':    total_pnl,
        'total_margin': total_margin,
        'positions':    positions,
    }


def manage_blacklist(conn, cur):
    """
    自动管理交易对黑名单 (trading_symbol_rating 表)
    规则：
      近 24h 内 SL 次数 >= 2  → 升至 level 1 (半仓，持续 6h)
      近 48h 内 SL 次数 >= 3  → 升至 level 2 (四分之一仓，持续 12h)
      总胜率 < 30% 且 >= 5 次  → 升至 level 1
      自动解除：level_changed_at 超过对应冷静期 → 降回 level 0
    返回变更列表
    """
    changes = []
    now = datetime.datetime.now()

    # ── 1. 自动解除过期黑名单 ─────────────────────────────────────────
    cur.execute("""
        SELECT id, symbol, rating_level, level_changed_at
        FROM trading_symbol_rating
        WHERE rating_level >= 1
    """)
    for row in cur.fetchall():
        changed_at = row['level_changed_at']
        if not changed_at:
            continue
        hours_elapsed = (now - changed_at).total_seconds() / 3600
        cooldown = 6.0 if row['rating_level'] == 1 else 12.0
        if hours_elapsed >= cooldown:
            cur.execute("""
                UPDATE trading_symbol_rating
                SET rating_level=0, previous_level=%s,
                    level_changed_at=NOW(), level_change_reason='auto_expire'
                WHERE id=%s
            """, (row['rating_level'], row['id']))
            conn.commit()
            changes.append(f"UNBLOCK {row['symbol']} level {row['rating_level']}→0 ({hours_elapsed:.1f}h elapsed)")

    # ── 2. 检测近期亏损过多的交易对 ──────────────────────────────────
    cur.execute("""
        SELECT symbol,
               SUM(CASE WHEN realized_pnl < 0 AND close_time >= DATE_SUB(NOW(), INTERVAL 24 HOUR) THEN 1 ELSE 0 END) AS sl_24h,
               SUM(CASE WHEN realized_pnl < 0 AND close_time >= DATE_SUB(NOW(), INTERVAL 48 HOUR) THEN 1 ELSE 0 END) AS sl_48h,
               SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) AS wins,
               COUNT(*) AS total
        FROM futures_positions
        WHERE account_id=%s AND status='closed'
        GROUP BY symbol
        HAVING total >= 2
    """, (ACCOUNT_ID,))
    symbol_stats = cur.fetchall()

    for s in symbol_stats:
        sym = s['symbol']
        sl_24h = int(s['sl_24h'] or 0)
        sl_48h = int(s['sl_48h'] or 0)
        total  = int(s['total'] or 0)
        wins   = int(s['wins'] or 0)
        wr     = wins / total if total else 1.0

        # 确定目标等级
        target_level = 0
        reason = ''
        if sl_48h >= 3:
            target_level = 2
            reason = f"48h内止损{sl_48h}次"
        elif sl_24h >= 2:
            target_level = 1
            reason = f"24h内止损{sl_24h}次"
        elif wr < 0.30 and total >= 5:
            target_level = 1
            reason = f"胜率{wr:.0%}过低(共{total}笔)"

        if target_level == 0:
            continue

        # 检查当前等级
        cur.execute(
            "SELECT id, rating_level FROM trading_symbol_rating WHERE symbol=%s", (sym,)
        )
        existing = cur.fetchone()
        if existing:
            if existing['rating_level'] >= target_level:
                continue  # 已经是更高惩罚，不重复降级
            cur.execute("""
                UPDATE trading_symbol_rating
                SET rating_level=%s, previous_level=%s, level_changed_at=NOW(),
                    level_change_reason=%s, updated_at=NOW()
                WHERE id=%s
            """, (target_level, existing['rating_level'], reason, existing['id']))
        else:
            cur.execute("""
                INSERT INTO trading_symbol_rating
                (symbol, rating_level, previous_level, level_changed_at, level_change_reason)
                VALUES (%s, %s, 0, NOW(), %s)
            """, (sym, target_level, reason))
        conn.commit()
        changes.append(f"BLACKLIST {sym} → level {target_level} ({reason})")

    return changes


def adjust_position_size(conn, cur, closed: dict):
    """
    根据整体表现自动调整 position_size_pct
    表现好（win_rate>55% & PF>1.3）→ 逐步加仓（+0.3%，上限 5%）
    表现差（win_rate<40% 或 PF<0.8） → 缩仓（-0.5%，下限 1.5%）
    数据不足时不调整
    返回 (new_pct, delta, reason)
    """
    if closed['count'] < MIN_TRADES_FOR_ADJ:
        cur.execute("SELECT setting_value FROM system_settings WHERE setting_key='position_size_pct'")
        row = cur.fetchone()
        current = float(row['setting_value']) if row else 0.03
        return current, 0, ''

    wr = closed['win_rate']
    pf = closed.get('profit_factor', 1.0)
    cur.execute("SELECT setting_value FROM system_settings WHERE setting_key='position_size_pct'")
    row = cur.fetchone()
    current = float(row['setting_value']) if row else 0.03

    delta = 0.0
    reason = ''
    if wr > 0.55 and pf > 1.3:
        new_pct = min(0.05, current + 0.003)
        if new_pct != current:
            delta = new_pct - current
            reason = f"win_rate={wr:.0%} PF={pf:.1f} → 加仓"
            set_setting(conn, cur, 'position_size_pct', round(new_pct, 4))
            return new_pct, delta, reason
    elif wr < 0.40 or pf < 0.80:
        new_pct = max(0.015, current - 0.005)
        if new_pct != current:
            delta = new_pct - current
            reason = f"win_rate={wr:.0%} PF={pf:.1f} → 缩仓"
            set_setting(conn, cur, 'position_size_pct', round(new_pct, 4))
            return new_pct, delta, reason

    return current, 0, ''


def get_current_weights(cur):
    """从 DB 读取当前信号权重"""
    cur.execute("""
        SELECT id, signal_component, weight_long, weight_short,
               base_weight, adjustment_count
        FROM signal_scoring_weights
        WHERE is_active=TRUE AND strategy_type='default'
    """)
    rows = cur.fetchall()
    return {r['signal_component']: r for r in rows}


def adjust_weights(conn, cur, signal_stats: dict, weights: dict):
    """
    根据信号胜率调整权重。
    win_rate > 0.65 且 >= 3 次 → 上调 10%
    win_rate < 0.35 且 >= 3 次 → 下调 10%
    样本不足(<3次) → 不调整
    """
    changes = []
    for sig, stats in signal_stats.items():
        if sig not in weights:
            continue
        w = weights[sig]
        total = stats['total']
        if total < 3:
            continue

        win_rate = stats['win_rate']
        base = float(w['base_weight']) if float(w.get('base_weight') or 0) > 0 else max(
            float(w['weight_long']), float(w['weight_short'])
        )
        if base == 0:
            continue

        if win_rate >= 0.65:
            factor = 1.0 + WEIGHT_ADJUST_STEP
            reason = f"win_rate={win_rate:.0%} >= 65% ({total} trades)"
        elif win_rate <= 0.35:
            factor = 1.0 - WEIGHT_ADJUST_STEP
            reason = f"win_rate={win_rate:.0%} <= 35% ({total} trades)"
        else:
            continue

        new_long  = float(w['weight_long'])  * factor
        new_short = float(w['weight_short']) * factor

        # 限制在 base_weight 的 50%~200% 之间
        cap_max = base * WEIGHT_MAX_RATIO
        cap_min = base * WEIGHT_MIN_RATIO
        new_long  = max(cap_min, min(cap_max, new_long))  if float(w['weight_long'])  > 0 else 0
        new_short = max(cap_min, min(cap_max, new_short)) if float(w['weight_short']) > 0 else 0

        old_long  = float(w['weight_long'])
        old_short = float(w['weight_short'])
        if abs(new_long - old_long) < 0.01 and abs(new_short - old_short) < 0.01:
            continue

        cur.execute("""
            UPDATE signal_scoring_weights
            SET weight_long=%s, weight_short=%s, adjustment_count=adjustment_count+1,
                performance_score=%s, updated_at=NOW()
            WHERE id=%s
        """, (round(new_long, 2), round(new_short, 2),
              round(win_rate * 100, 1), w['id']))
        conn.commit()

        changes.append({
            'signal':     sig,
            'old_long':   old_long,  'new_long':   new_long,
            'old_short':  old_short, 'new_short':  new_short,
            'reason':     reason,
        })

    return changes


def adjust_threshold(conn, cur, closed: dict):
    """根据胜率自动调整开仓阈值"""
    current = get_setting(cur, 'entry_threshold', 65)
    change = 0
    reason = ''

    if closed['count'] >= MIN_TRADES_FOR_ADJ:
        wr = closed['win_rate']
        pf = closed.get('profit_factor', 1.0)
        if wr < WIN_RATE_RAISE_GATE:
            new_thresh = min(THRESHOLD_MAX, current + THRESHOLD_RAISE_STEP)
            if new_thresh != current:
                change = new_thresh - current
                reason = f"win_rate={wr:.0%} < {WIN_RATE_RAISE_GATE:.0%} → 收紧阈值"
                set_setting(conn, cur, 'entry_threshold', new_thresh)
        elif wr > WIN_RATE_LOWER_GATE and pf > 1.5:
            new_thresh = max(THRESHOLD_MIN, current - THRESHOLD_LOWER_STEP)
            if new_thresh != current:
                change = new_thresh - current
                reason = f"win_rate={wr:.0%} > {WIN_RATE_LOWER_GATE:.0%} & PF={pf:.1f} → 适度放宽"
                set_setting(conn, cur, 'entry_threshold', new_thresh)
    elif closed['count'] == 0:
        # 没有任何交易数据 - 判断当前 threshold 是否过高
        # 只在满 8h 无交易时才放宽（避免首次启动误调）
        pass

    return current + change, change, reason


def get_big4_status(cur):
    """读取 Big4 最新状态"""
    try:
        cur.execute("""
            SELECT symbol, timeframe, close_price, open_time
            FROM kline_data
            WHERE symbol IN ('BTC/USDT','ETH/USDT','BNB/USDT','SOL/USDT')
              AND timeframe='1h' AND exchange='binance_futures'
              AND open_time = (
                SELECT MAX(open_time) FROM kline_data
                WHERE timeframe='1h' AND exchange='binance_futures'
              )
        """)
        rows = cur.fetchall()
        return rows
    except Exception:
        return []


def restart_smart_trader():
    """杀掉旧进程，启动新进程"""
    killed = []
    for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'create_time']):
        try:
            if proc.name().lower() in ('python.exe', 'python'):
                if 'smart_trader_service' in ' '.join(proc.cmdline() or []):
                    proc.kill()
                    killed.append(proc.pid)
        except Exception:
            pass

    if killed:
        time.sleep(2)

    # 使用 venv Python 启动
    venv_python = os.path.join('.venv', 'Scripts', 'python.exe')
    if not os.path.exists(venv_python):
        venv_python = sys.executable

    proc = subprocess.Popen(
        [venv_python, 'smart_trader_service.py'],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        creationflags=subprocess.DETACHED_PROCESS if sys.platform == 'win32' else 0
    )
    return killed, proc.pid


# ── 报告生成 ──────────────────────────────────────────────────────────
def format_report(closed, open_pos, threshold_info, weight_changes,
                  current_weights, restarted, now, sizing_info=None, blacklist_changes=None):
    thresh_new, thresh_change, thresh_reason = threshold_info
    lines = [
        "=" * 70,
        f"  HOURLY OPTIMIZER REPORT  {now.strftime('%Y-%m-%d %H:%M')}",
        "=" * 70,
        "",
        "[1] CLOSED TRADES (last 4h)",
    ]

    if closed['count'] == 0:
        lines.append("  No closed trades in analysis window")
    else:
        lines += [
            f"  Total:         {closed['count']} trades",
            f"  Win / Loss:    {closed['wins']} / {closed['losses']}",
            f"  Win rate:      {closed['win_rate']:.1%}",
            f"  Profit factor: {closed.get('profit_factor', 0):.2f}",
            f"  Net PnL:       {closed['total_pnl_net']:+.2f} USDT",
            f"  Avg win:       +{closed['avg_win']:.2f} USDT",
            f"  Avg loss:      -{closed['avg_loss']:.2f} USDT",
        ]
        if closed['signal_stats']:
            lines.append("")
            lines.append("  Signal performance (>=3 occurrences):")
            for sig, s in sorted(closed['signal_stats'].items(),
                                  key=lambda x: -(x[1]['win_rate'] or 0)):
                if s['total'] >= 3:
                    lines.append(
                        f"    {sig:<30} win={s['wins']}/{s['total']}  "
                        f"({s['win_rate']:.0%})"
                    )

    lines += ["", "[2] OPEN POSITIONS"]
    op = open_pos
    lines.append(f"  Count:      {op['count']} / 10")
    lines.append(f"  Total PnL:  {op['total_pnl']:+.2f} USDT")
    lines.append(f"  Margin in:  {op['total_margin']:.0f} USDT")
    for p in op['positions']:
        ep    = float(p['entry_price'])
        sl    = float(p['stop_loss_price'])
        tp    = float(p['take_profit_price'])
        pnl   = float(p['unrealized_pnl'])
        margin = float(p['margin'] or 1)
        pnl_pct = pnl / margin * 100
        if p['position_side'] == 'SHORT':
            sl_dist = (sl - ep) / ep * 100
            tp_dist = (ep - tp) / ep * 100
        else:
            sl_dist = (ep - sl) / ep * 100
            tp_dist = (tp - ep) / ep * 100
        health = 'OK'
        if pnl_pct < -1.5:
            health = 'NEAR-SL'
        lines.append(
            f"  {p['symbol']:<16} {p['position_side']:<5} "
            f"score={p['entry_score']:>3}  pnl={pnl:+.2f}({pnl_pct:+.1f}%)  "
            f"sl_dist={sl_dist:.1f}%  tp_dist={tp_dist:.1f}%  [{health}]"
        )

    lines += ["", "[3] THRESHOLD"]
    lines.append(f"  Current: {thresh_new}")
    if thresh_change != 0:
        lines.append(f"  Change:  {thresh_change:+d}  ({thresh_reason})")
    else:
        lines.append("  Change:  0  (no adjustment)")

    lines += ["", "[4] SIGNAL WEIGHT CHANGES"]
    if not weight_changes:
        lines.append("  No changes (insufficient data or already at limits)")
    else:
        for c in weight_changes:
            lines.append(
                f"  {c['signal']:<30}  "
                f"long: {c['old_long']:.1f} -> {c['new_long']:.1f}  "
                f"short: {c['old_short']:.1f} -> {c['new_short']:.1f}  "
                f"({c['reason']})"
            )

    lines += ["", "[5] CURRENT WEIGHTS"]
    for sig, w in sorted(current_weights.items()):
        adj = w.get('adjustment_count', 0) or 0
        lines.append(
            f"  {sig:<30}  long={float(w['weight_long']):>5.1f}  "
            f"short={float(w['weight_short']):>5.1f}  adj_count={adj}"
        )

    lines += ["", "[6] POSITION SIZING"]
    lines.append(f"  position_size_pct: {sizing_info[0]:.1%}"
                 + (f"  change: {sizing_info[1]:+.1%}  ({sizing_info[2]})" if sizing_info[1] else "  (no change)"))

    lines += ["", "[7] BLACKLIST CHANGES"]
    if not blacklist_changes:
        lines.append("  No changes")
    else:
        for c in blacklist_changes:
            lines.append(f"  {c}")

    lines += ["", "[8] SERVICE STATUS"]
    if restarted:
        lines.append(f"  smart_trader restarted  (old PIDs={restarted[0]}, new PID={restarted[1]})")
    else:
        lines.append("  smart_trader: no restart needed")

    lines += ["", "=" * 70, ""]
    return '\n'.join(lines)


# ── 主入口 ────────────────────────────────────────────────────────────
def main():
    now = datetime.datetime.now()
    log(f"=== AUTO OPTIMIZER START {now.strftime('%H:%M:%S')} ===")

    conn = get_conn()
    try:
        cur = conn.cursor()

        # 1. 分析已平仓交易
        closed = analyze_closed_trades(cur, ANALYSIS_WINDOW_H)

        # 2. 分析持仓状态
        open_pos = analyze_open_positions(cur)

        # 3. 读取当前权重
        current_weights = get_current_weights(cur)

        # 4. 调整权重（数据足够才动）
        weight_changes = []
        if closed['count'] >= MIN_TRADES_FOR_ADJ:
            weight_changes = adjust_weights(conn, cur, closed['signal_stats'], current_weights)
            current_weights = get_current_weights(cur)

        # 5. 调整阈值
        threshold_info = adjust_threshold(conn, cur, closed)

        # 6. 调整仓位比例
        sizing_info = adjust_position_size(conn, cur, closed)

        # 7. 管理黑名单
        blacklist_changes = manage_blacklist(conn, cur)

        # 8. 如果有权重变化，重启服务让新权重生效
        restarted = None
        if weight_changes:
            log("  Weights changed - restarting smart_trader_service...")
            killed_pids, new_pid = restart_smart_trader()
            restarted = (killed_pids, new_pid)
            log(f"  Restarted: killed={killed_pids}, new_pid={new_pid}")

        # 9. 生成报告
        report = format_report(
            closed, open_pos, threshold_info,
            weight_changes, current_weights, restarted, now,
            sizing_info=sizing_info,
            blacklist_changes=blacklist_changes,
        )

        # 写入日志
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(report)

        print(report)

        log(f"=== AUTO OPTIMIZER DONE ===")

    except Exception as e:
        import traceback
        log(f"ERROR: {e}\n{traceback.format_exc()}")
    finally:
        conn.close()


if __name__ == '__main__':
    main()
