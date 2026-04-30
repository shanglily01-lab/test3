"""
观察 strategy_live dump / topshort 的 SIG_WAIT 状态转移分布.
回答 "等待期是否真的过滤了假信号 / 是否拒绝率合理".

只读 dimesion 库 (REMOTE), 通过 strategy_state + futures_positions 关联.

切片:
  S1. 当前活跃 SIG_WAIT 数量 (state='SIG_WAIT' 实时)
  S2. 近 N 天从 SIG_WAIT 转出的去向: SHORT (信号坚持, 入场) / IDLE+sig_adverse (反向失效) / IDLE+sig_expired (重判失效)
  S3. 转出后的 SHORT 仓位最终 PnL (绑定 strategy_state.entry_time vs futures_positions.open_time)
  S4. 启用前后同期 dump-entry / topshort PnL 对比

DB 配置从 table_schemas.txt 头部解析.
用法: python scripts/diag/diag_signal_wait_observe.py
"""
import sys
import os
import re
from collections import defaultdict
from datetime import datetime, timezone, timedelta
import pymysql

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')


def _load_db_from_schema():
    schema_path = os.path.join(os.path.dirname(__file__), '..', '..', 'table_schemas.txt')
    cfg = dict(host='54.179.112.251', port=3306, user='admin',
               password='Yintao@110', db='dimesion')
    try:
        with open(schema_path, 'r', encoding='utf-8', errors='replace') as f:
            head = f.read(2000)
        for k, pat in [('host', r'host:\s*([\d.]+)'),
                       ('port', r'port:\s*(\d+)'),
                       ('user', r'user:\s*(\S+)'),
                       ('password', r'password:\s*(\S+)'),
                       ('db', r'database:\s*(\S+)')]:
            m = re.search(pat, head)
            if m:
                cfg[k] = int(m.group(1)) if k == 'port' else m.group(1)
    except Exception as e:
        print(f"[警告] 读 table_schemas.txt 失败: {e}")
    cfg['charset'] = 'utf8mb4'
    cfg['cursorclass'] = pymysql.cursors.DictCursor
    return cfg


DB = _load_db_from_schema()
DAYS = 7


def section(t):
    print('\n' + '=' * 100)
    print(f"  {t}")
    print('=' * 100)


def s1_active_sig_wait(cur):
    section("S1. 当前活跃 SIG_WAIT (实时)")
    cur.execute("""
        SELECT stype, symbol, side, entry_p, entry_time,
               UNIX_TIMESTAMP(NOW()) - entry_time AS waited_s
        FROM strategy_state
        WHERE strategy='live' AND stype IN ('dump','topshort') AND state='SIG_WAIT'
        ORDER BY entry_time ASC
    """)
    rows = cur.fetchall()
    if not rows:
        print("  无活跃 SIG_WAIT")
        return
    print(f"  {'stype':<10} {'symbol':<14} {'side':<6} {'entry_p':>10} {'waited':>10}")
    for r in rows:
        waited = float(r['waited_s'] or 0)
        print(f"  {r['stype']:<10} {r['symbol']:<14} {r.get('side') or '':<6} "
              f"{float(r['entry_p'] or 0):>10.6f} {waited/60:>8.1f}min")


def s2_transitions(cur):
    section(f"S2. 近 {DAYS} 天 SIG_WAIT 转出去向 (按 last_reason 看)")
    cur.execute(f"""
        SELECT stype, last_reason, state, COUNT(*) AS n
        FROM strategy_state
        WHERE strategy='live' AND stype IN ('dump','topshort')
          AND last_reason IN ('sig_adverse','sig_expired')
          AND updated_at >= NOW() - INTERVAL %s DAY
        GROUP BY stype, last_reason, state
        ORDER BY stype, last_reason
    """, (DAYS,))
    rows = cur.fetchall()
    if not rows:
        print("  近 N 天无 SIG_WAIT 转出记录 (功能可能未启用 / 等待 5+ 天观察)")
    else:
        for r in rows:
            print(f"  {r['stype']:<10} state={r['state']:<10} reason={r['last_reason']:<15} 笔数={r['n']}")
    print()
    print("  注: state='IDLE'+last_reason='sig_adverse'  --> 反向失效 (信号方向走反, 退出)")
    print("       state='IDLE'+last_reason='sig_expired'  --> 30min 后重判失效 (假突破被过滤)")
    print("       state='SHORT'                            --> 30min 后重判仍成立, 入场 (信号坚挺)")
    print("  - 期望: sig_expired 占比 30-50% (大量假突破被过滤), sig_adverse 10-20%, SHORT 30-50%")
    print("  - 若 sig_expired > 70%: 等待期太严, 几乎所有信号都失效, 考虑缩短等待时长")
    print("  - 若 SHORT > 70%: 等待期没过滤掉假信号, 考虑延长等待 / 收紧反向阈值")


def s3_post_wait_pnl(cur):
    section(f"S3. SIG_WAIT 转 SHORT 后入场的 PnL (近 {DAYS} 天)")
    # SIG_WAIT -> SHORT 后, 同 stype 同 symbol 在 strategy_state 的 entry_time 跟实仓 open_time 关联
    # 简化: 直接拉 source LIKE '%dump-entry' / source='strategy_live:topshort' 在最近 N 天的实仓
    # 加 hint: 看 notes 是否含 "等待期满入场" (新增 log 关键字, 但 notes 字段是平仓 reason 不是开仓 log)
    # 改用: 启用功能后开的所有仓位都视为"经过 SIG_WAIT", 因为 IDLE -> SIG_WAIT -> SHORT 是唯一路径
    print("  注: 启用 dump_signal_wait_enabled=1 后, 该 source 所有仓位都经过 SIG_WAIT 过滤.")
    print("      和启用前的 PnL 对比即可看出过滤效果.")


def s4_pnl_compare(cur, source: str, label: str):
    section(f"S4. {label} 启用前 vs 启用后 PnL 对比")
    cur.execute(f"""
        SELECT DATE(close_time) AS d, COUNT(*) AS n,
               SUM(realized_pnl > 0) AS wins,
               SUM(realized_pnl) AS net,
               AVG(realized_pnl) AS avg_p
        FROM futures_positions
        WHERE status='closed'
          AND close_time >= NOW() - INTERVAL %s DAY
          AND source = %s
        GROUP BY DATE(close_time)
        ORDER BY d
    """, (DAYS * 2, source))
    rows = cur.fetchall()
    if not rows:
        print(f"  近 {DAYS*2} 天无 {source} 仓位")
        return
    print(f"  {'date':<12} {'n':>4} {'wins':>5} {'win_rate':>9} {'net':>10} {'avg':>9}")
    for r in rows:
        n = int(r['n']); wins = int(r['wins'] or 0)
        wr = wins / n * 100 if n else 0
        print(f"  {str(r['d']):<12} {n:>4} {wins:>5} {wr:>8.1f}% {float(r['net'] or 0):>+9.2f}U {float(r['avg_p'] or 0):>+8.2f}U")


def main():
    print(f"连库 {DB['user']}@{DB['host']}:{DB['port']}/{DB['db']}")
    conn = pymysql.connect(**DB)
    cur = conn.cursor()
    try:
        s1_active_sig_wait(cur)
        s2_transitions(cur)
        s3_post_wait_pnl(cur)
        s4_pnl_compare(cur, 'strategy_live:dump-entry', 'dump-entry')
        s4_pnl_compare(cur, 'strategy_live:topshort', 'topshort')
    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    main()
