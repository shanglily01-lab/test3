"""
最近一段时间的策略盈亏汇总.
- paper 视角: futures_positions status=closed AND source LIKE 'strategy%'
- 实盘视角: 上述基础上要求该 position 关联的开仓 order 有 live_sync_status='SYNCED'
按 7d / 14d / 30d 三档窗口聚合, 并按 source 分组列出.
"""
import sys
import pymysql
from collections import defaultdict
from datetime import datetime, timedelta

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

REMOTE_DB = {
    "host": "54.179.112.251", "port": 3306, "user": "admin",
    "password": "Yintao@110", "database": "dimesion", "charset": "utf8mb4",
}

NOW = datetime.utcnow()
WINDOWS = [
    ("近 7 天",  NOW - timedelta(days=7)),
    ("近 14 天", NOW - timedelta(days=14)),
    ("近 30 天", NOW - timedelta(days=30)),
]


def fetch_closed(cur, since):
    """全部 paper 关闭仓位 (实盘单也都会出现在这里, 多一个标记)."""
    cur.execute(
        """
        SELECT p.id, p.symbol, p.position_side, p.source,
               p.realized_pnl, p.open_time, p.close_time, p.strategy_id,
               TIMESTAMPDIFF(MINUTE, p.open_time, p.close_time) AS hold_min,
               EXISTS (
                   SELECT 1 FROM futures_orders o
                   WHERE o.position_id = p.id
                     AND o.side IN ('OPEN_LONG','OPEN_SHORT')
                     AND o.live_sync_status = 'SYNCED'
               ) AS is_live
        FROM futures_positions p
        WHERE p.status='closed'
          AND p.close_time >= %s
          AND p.source LIKE 'strategy%%'
        ORDER BY p.close_time ASC
        """,
        (since,),
    )
    return cur.fetchall()


def stat(rows):
    if not rows:
        return None
    pnls = [float(r["realized_pnl"] or 0) for r in rows]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    total = sum(pnls)
    pf = (sum(wins) / -sum(losses)) if losses and sum(losses) < 0 else (float("inf") if wins else 0)
    return dict(
        n=len(pnls),
        wins=len(wins),
        losses=len(losses),
        win_rate=len(wins) / len(pnls),
        total=total,
        avg=total / len(pnls),
        max_win=max(pnls),
        max_loss=min(pnls),
        sum_wins=sum(wins),
        sum_losses=sum(losses),
        pf=pf,
    )


def print_stat(label, s):
    if not s:
        print(f"  [{label}] 无样本")
        return
    pf = f"{s['pf']:.2f}" if s["pf"] != float("inf") else "inf"
    print(f"  [{label}] n={s['n']:<3}  胜={s['wins']:<3} 负={s['losses']:<3} "
          f"胜率={s['win_rate']*100:5.1f}%  累计={s['total']:+9.2f}U  "
          f"avg={s['avg']:+7.2f}U  PF={pf}")
    print(f"           最大盈={s['max_win']:+8.2f}U  最大亏={s['max_loss']:+8.2f}U  "
          f"盈和={s['sum_wins']:+8.2f}  亏和={s['sum_losses']:+8.2f}")


def section(title):
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)


def by_source(rows):
    g = defaultdict(list)
    for r in rows:
        g[r["source"] or "(unknown)"].append(r)
    return g


def main():
    conn = pymysql.connect(**REMOTE_DB)
    cur = conn.cursor(pymysql.cursors.DictCursor)

    section(f"REMOTE dimesion @ now UTC = {NOW.isoformat(timespec='seconds')}")

    for label, since in WINDOWS:
        section(f"{label}  (close_time >= {since.isoformat(timespec='seconds')} UTC)")
        rows = fetch_closed(cur, since)
        paper_rows = rows
        live_rows = [r for r in rows if r["is_live"]]

        print("整体 (paper 视角, 包含实盘子集):")
        print_stat("ALL  ", stat(paper_rows))
        print("\n仅实盘 (开仓单 SYNCED):")
        print_stat("LIVE ", stat(live_rows))

        print("\n按 source 分组 (paper 视角):")
        src_g = by_source(paper_rows)
        for src, lst in sorted(src_g.items(), key=lambda kv: -sum(float(r["realized_pnl"] or 0) for r in kv[1])):
            print_stat(f"{src:<28}", stat(lst))

        if live_rows:
            print("\n按 source 分组 (仅实盘):")
            src_g_live = by_source(live_rows)
            for src, lst in sorted(src_g_live.items(), key=lambda kv: -sum(float(r["realized_pnl"] or 0) for r in kv[1])):
                print_stat(f"{src:<28}", stat(lst))

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
