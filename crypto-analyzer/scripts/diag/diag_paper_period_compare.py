"""
对比 REMOTE (服务器 dimesion) vs LOCAL (本地 binance-data)
paper-only 期 (2026-04-25 ~ 2026-04-29) 的策略表现.

两库各自独立跑 strategy_live, 仓位完全独立.
此脚本回答 "最近本地的交易比远程的好" 这个观察是否成立, 好在哪儿.

输出:
  1. 两库整体: 仓位数 / 总 pnl / 胜率 / 平均单笔 / 最大单笔盈亏 / PF
  2. 按 source 分组对比 (chase-entry / dump-entry / topshort / bottomlong / whale-* / ...)
  3. 按 symbol 分组对比 (谁差异最大)
  4. 每库 top5 大赚 / top5 大亏 仓位明细
"""
import sys
import pymysql
from pathlib import Path
from collections import defaultdict

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

REMOTE_DB = {
    "host": "54.179.112.251", "port": 3306, "user": "admin",
    "password": "Yintao@110", "database": "dimesion", "charset": "utf8mb4",
}

SINCE = "2026-04-25 00:00:00"
UNTIL = "2026-04-29 23:59:59"


def load_local_db():
    env_path = Path(__file__).resolve().parents[2] / ".env"
    cfg = {"host": "localhost", "port": 3306, "charset": "utf8mb4"}
    with env_path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k, v = k.strip(), v.strip()
            if k == "DB_HOST": cfg["host"] = v
            elif k == "DB_PORT": cfg["port"] = int(v)
            elif k == "DB_USER": cfg["user"] = v
            elif k == "DB_PASSWORD": cfg["password"] = v
            elif k == "DB_NAME": cfg["database"] = v
    return cfg


def section(t):
    print("\n" + "=" * 100)
    print(t)
    print("=" * 100)


def fetch_closed(cur, label):
    """取期间所有 status=closed 仓位, 含 source/symbol/pnl/notes."""
    cur.execute(
        """
        SELECT id, symbol, position_side, source, notes,
               entry_price, quantity, leverage,
               stop_loss_price, take_profit_price,
               realized_pnl, open_time, close_time,
               TIMESTAMPDIFF(MINUTE, open_time, close_time) AS hold_min
        FROM futures_positions
        WHERE status='closed'
          AND close_time BETWEEN %s AND %s
          AND source LIKE 'strategy%%'
        ORDER BY close_time ASC
        """,
        (SINCE, UNTIL),
    )
    rows = cur.fetchall()
    return rows


def stat_block(rows):
    if not rows:
        return None
    pnls = [float(r["realized_pnl"] or 0) for r in rows]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    total = sum(pnls)
    pf = (sum(wins) / -sum(losses)) if losses and sum(losses) < 0 else float("inf") if wins else 0
    win_rate = len(wins) / len(pnls) if pnls else 0
    return {
        "n": len(pnls),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": win_rate,
        "total_pnl": total,
        "avg_pnl": total / len(pnls) if pnls else 0,
        "max_win": max(pnls) if pnls else 0,
        "max_loss": min(pnls) if pnls else 0,
        "pf": pf,
        "sum_wins": sum(wins),
        "sum_losses": sum(losses),
    }


def print_stat(label, s):
    if not s:
        print(f"  [{label}] 无数据")
        return
    print(f"  [{label}] n={s['n']:<3} wins={s['wins']:<3} losses={s['losses']:<3} "
          f"胜率={s['win_rate']*100:5.1f}%  总pnl={s['total_pnl']:+9.2f}U  "
          f"avg={s['avg_pnl']:+7.2f}U  PF={s['pf']:.2f}")
    print(f"           最大盈={s['max_win']:+8.2f}U  最大亏={s['max_loss']:+8.2f}U  "
          f"盈和={s['sum_wins']:+8.2f}  亏和={s['sum_losses']:+8.2f}")


def group_by(rows, key_fn):
    g = defaultdict(list)
    for r in rows:
        g[key_fn(r)].append(r)
    return g


def main():
    rconn = pymysql.connect(**REMOTE_DB)
    rcur = rconn.cursor(pymysql.cursors.DictCursor)
    r_rows = fetch_closed(rcur, "REMOTE")
    rcur.close(); rconn.close()

    try:
        lconn = pymysql.connect(**load_local_db())
        lcur = lconn.cursor(pymysql.cursors.DictCursor)
        l_rows = fetch_closed(lcur, "LOCAL")
        lcur.close(); lconn.close()
    except Exception as e:
        print(f"LOCAL 连接失败: {e!r}")
        l_rows = []

    section(f"窗口: {SINCE} ~ {UNTIL}  (status=closed, source LIKE 'strategy%')")
    print(f"  REMOTE 仓位数: {len(r_rows)}")
    print(f"  LOCAL  仓位数: {len(l_rows)}")

    section("整体对比")
    r_s = stat_block(r_rows)
    l_s = stat_block(l_rows)
    print_stat("REMOTE", r_s)
    print_stat("LOCAL ", l_s)

    section("按 source 分组对比")
    r_by_src = group_by(r_rows, lambda r: r["source"] or "(unknown)")
    l_by_src = group_by(l_rows, lambda r: r["source"] or "(unknown)")
    all_srcs = sorted(set(r_by_src.keys()) | set(l_by_src.keys()))
    for src in all_srcs:
        print(f"\n  source = {src}")
        print_stat("REMOTE", stat_block(r_by_src.get(src, [])))
        print_stat("LOCAL ", stat_block(l_by_src.get(src, [])))

    section("按 symbol 分组对比 (按 |REMOTE pnl - LOCAL pnl| 降序, 取前 15)")
    r_by_sym = group_by(r_rows, lambda r: r["symbol"])
    l_by_sym = group_by(l_rows, lambda r: r["symbol"])
    all_syms = set(r_by_sym.keys()) | set(l_by_sym.keys())
    sym_diffs = []
    for sym in all_syms:
        r_pnl = sum(float(r["realized_pnl"] or 0) for r in r_by_sym.get(sym, []))
        l_pnl = sum(float(r["realized_pnl"] or 0) for r in l_by_sym.get(sym, []))
        sym_diffs.append((sym, r_pnl, l_pnl, abs(r_pnl - l_pnl), len(r_by_sym.get(sym, [])), len(l_by_sym.get(sym, []))))
    sym_diffs.sort(key=lambda x: -x[3])
    print(f"  {'symbol':<14} {'REMOTE_pnl':>12} {'LOCAL_pnl':>12} {'|diff|':>10} {'R_n':>5} {'L_n':>5}")
    for sym, rp, lp, d, rn, ln in sym_diffs[:15]:
        print(f"  {sym:<14} {rp:>+12.2f} {lp:>+12.2f} {d:>10.2f} {rn:>5} {ln:>5}")

    section("REMOTE top5 大盈 / 大亏")
    sorted_r = sorted(r_rows, key=lambda r: float(r["realized_pnl"] or 0), reverse=True)
    print("  -- top5 大盈 --")
    for r in sorted_r[:5]:
        print(f"    {r['symbol']:<12} {r['position_side']:<5} pnl={float(r['realized_pnl']):+8.2f} src={r['source']!r} notes={r['notes']!r} hold={r['hold_min']}min close={r['close_time']}")
    print("  -- top5 大亏 --")
    for r in sorted_r[-5:]:
        print(f"    {r['symbol']:<12} {r['position_side']:<5} pnl={float(r['realized_pnl']):+8.2f} src={r['source']!r} notes={r['notes']!r} hold={r['hold_min']}min close={r['close_time']}")

    section("LOCAL top5 大盈 / 大亏")
    sorted_l = sorted(l_rows, key=lambda r: float(r["realized_pnl"] or 0), reverse=True)
    print("  -- top5 大盈 --")
    for r in sorted_l[:5]:
        print(f"    {r['symbol']:<12} {r['position_side']:<5} pnl={float(r['realized_pnl']):+8.2f} src={r['source']!r} notes={r['notes']!r} hold={r['hold_min']}min close={r['close_time']}")
    print("  -- top5 大亏 --")
    for r in sorted_l[-5:]:
        print(f"    {r['symbol']:<12} {r['position_side']:<5} pnl={float(r['realized_pnl']):+8.2f} src={r['source']!r} notes={r['notes']!r} hold={r['hold_min']}min close={r['close_time']}")


if __name__ == "__main__":
    main()
