"""
SWAN 子策略 (whale.swan) 诊断脚本.

用户问: "红黑榜为什么不持续下单了?"

排查链 (按 strategy_whale.swan_strategy_tick 的守卫顺序):
  1. 总开关 swan_strategy_enabled 是否开
  2. swan_last_run_id 进度游标 vs dimesion.gemini_swan_runs 最新 run_id (是否新数据未推进)
  3. dimesion.gemini_swan_verdicts 最近几轮的 STRONG / MODERATE / WEAK 信号分布,
     最高 avg_confidence 是否 >= swan_min_confidence (默认 0.70)
  4. SWAN 自身持仓上限 swan_max_open (查 strategy_state stype='swan' active 数)
  5. 黑名单 / whale 系其他子策略占用 / 24h 涨跌过滤 (这步不查, 只统计落地的下单)
  6. 历史下单: futures_orders WHERE strategy='whale' AND tag='swan'

只读, 不下单.

用法:
  cd crypto-analyzer
  python scripts/diag/diag_swan_strategy_status.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pymysql

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

ROOT = Path(__file__).resolve().parents[2]

# ---- 远程 dimesion (Gemini swan 数据源) ----
REMOTE = {
    "host": "54.179.112.251", "port": 3306, "user": "admin",
    "password": "Yintao@110", "database": "dimesion", "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
}

# ---- 本地 binance-data (strategy_whale 进程实际跑的库) ----
def load_local_cfg() -> dict:
    cfg = {"host": "localhost", "port": 3306, "charset": "utf8mb4",
           "cursorclass": pymysql.cursors.DictCursor}
    env_path = ROOT / ".env"
    if not env_path.exists():
        return cfg
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


def section(title: str) -> None:
    print("\n" + "=" * 78)
    print(f"  {title}")
    print("=" * 78)


def main() -> None:
    # ---------------- 1. 本地 SWAN 配置 ----------------
    section("1. 本地 system_settings: SWAN 子策略配置")
    local_cfg = load_local_cfg()
    print(f"  [local DB] {local_cfg.get('host')}:{local_cfg.get('port')} / "
          f"{local_cfg.get('database')}")
    settings = {}
    try:
        with pymysql.connect(**local_cfg) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT setting_key, setting_value FROM system_settings "
                "WHERE setting_key IN "
                "  ('swan_strategy_enabled','swan_min_confidence','swan_position_usdt',"
                "   'swan_leverage','swan_max_open','swan_hold_minutes',"
                "   'swan_cooldown_hours','swan_last_run_id','gemini_swan_enabled')"
            )
            for r in cur.fetchall():
                settings[r["setting_key"]] = r["setting_value"]
    except Exception as e:
        print(f"  [ERR] 连本地 DB 失败: {e}")
        return

    keys_in_order = [
        "swan_strategy_enabled", "swan_min_confidence", "swan_max_open",
        "swan_position_usdt", "swan_leverage", "swan_hold_minutes",
        "swan_cooldown_hours", "swan_last_run_id", "gemini_swan_enabled",
    ]
    for k in keys_in_order:
        v = settings.get(k, "<未设>")
        print(f"  {k:30s} = {v}")

    enabled = str(settings.get("swan_strategy_enabled", "0")).strip().lower() in (
        "1", "true", "yes", "on"
    )
    print(f"\n  [结论] SWAN 总开关: {'开' if enabled else '关'}")
    if not enabled:
        print("  [关键卡点] swan_strategy_enabled != 1, swan_strategy_tick 立即 return.")

    last_run_id = int(settings.get("swan_last_run_id", "0") or 0)
    min_conf = float(settings.get("swan_min_confidence", "0.70") or 0.70)
    max_open = int(settings.get("swan_max_open", "5") or 5)

    # ---------------- 2. 远程 gemini_swan_runs ----------------
    section("2. 远程 dimesion.gemini_swan_runs: 最近 8 次 Gemini 跑批")
    try:
        rconn = pymysql.connect(**REMOTE)
    except Exception as e:
        print(f"  [ERR] 连远程 dimesion 失败: {e}")
        return

    with rconn.cursor() as cur:
        cur.execute(
            "SELECT id, asof_utc, status, rounds, universe_size, "
            "       elapsed_s, triggered_by, "
            "       LEFT(IFNULL(error_msg,''), 80) AS err "
            "FROM gemini_swan_runs ORDER BY id DESC LIMIT 8"
        )
        runs = cur.fetchall()

    if not runs:
        print("  [关键卡点] gemini_swan_runs 一行都没有, 后台 worker 从未成功跑过.")
        return

    print(f"  {'run_id':>7} {'asof_utc':<19} {'status':<8} "
          f"{'rounds':>6} {'univ':>5} {'秒':>6} {'trig':<10} 备注")
    for r in runs:
        note = f" {r['err']}" if r["err"] else ""
        print(f"  {r['id']:>7} {str(r['asof_utc']):<19} {r['status']:<8} "
              f"{r['rounds']:>6} {r['universe_size']:>5} "
              f"{(r['elapsed_s'] or 0):>6.1f} {r['triggered_by']:<10}{note}")

    latest_run_id = runs[0]["id"]
    behind = latest_run_id - last_run_id
    print(f"\n  [游标] swan_last_run_id={last_run_id} vs latest run_id={latest_run_id} "
          f"(差 {behind})")
    if behind <= 0:
        print("  [关键卡点] 本地游标已经 >= 最新 run_id, 没有新 run 给 SWAN 处理.")
        print("            后台 Gemini worker 没产出新数据 / 或者 SWAN 一开机就把所有都吃掉了.")

    # ---------------- 3. 远程 verdicts 分布 (最近 5 轮) ----------------
    section("3. 远程 gemini_swan_verdicts: 最近 5 轮 STRONG/MODERATE/WEAK 分布")
    recent_run_ids = [r["id"] for r in runs[:5]]
    placeholder = ",".join(["%s"] * len(recent_run_ids))
    with rconn.cursor() as cur:
        cur.execute(
            f"SELECT run_id, main_category, consistency_level, "
            f"       COUNT(*) AS n, MAX(avg_confidence) AS max_conf "
            f"FROM gemini_swan_verdicts "
            f"WHERE run_id IN ({placeholder}) "
            f"GROUP BY run_id, main_category, consistency_level "
            f"ORDER BY run_id DESC, main_category, consistency_level",
            tuple(recent_run_ids),
        )
        rows = cur.fetchall()

    print(f"  {'run_id':>7} {'category':<12} {'level':<10} {'n':>4} {'max_conf':>9}")
    for r in rows:
        marker = ""
        if r["consistency_level"] == "STRONG" and r["main_category"] in (
            "red_swan", "black_swan"
        ):
            if float(r["max_conf"]) >= min_conf:
                marker = "  <-- SWAN 候选"
            else:
                marker = f"  <-- STRONG 但 conf<{min_conf}"
        print(f"  {r['run_id']:>7} {r['main_category']:<12} "
              f"{r['consistency_level']:<10} {r['n']:>4} {float(r['max_conf']):>9.3f}"
              f"{marker}")

    # ---------------- 4. 大于游标 + STRONG + 满足置信度 的候选 ----------------
    section(f"4. 应该被 SWAN 处理但还没处理的候选 (run_id > {last_run_id}, "
            f"STRONG, conf >= {min_conf})")
    with rconn.cursor() as cur:
        cur.execute(
            "SELECT run_id, symbol, main_category, avg_confidence, "
            "       black_count, red_count, rounds_total, "
            "       LEFT(IFNULL(catalyst,''), 60) AS catalyst_short "
            "FROM gemini_swan_verdicts "
            "WHERE run_id > %s "
            "  AND main_category IN ('red_swan','black_swan') "
            "  AND consistency_level = 'STRONG' "
            "  AND avg_confidence >= %s "
            "ORDER BY run_id, avg_confidence DESC",
            (last_run_id, min_conf),
        )
        candidates = cur.fetchall()

    if not candidates:
        print("  [无] 游标之后的所有 run 中, 没有 STRONG + conf>=门槛 的红/黑天鹅.")
    else:
        print(f"  共 {len(candidates)} 个候选 ↓")
        print(f"  {'run_id':>7} {'symbol':<14} {'cat':<11} "
              f"{'conf':>5} {'r/b/total':<10}  catalyst")
        for c in candidates:
            rt = f"{c['red_count']}/{c['black_count']}/{c['rounds_total']}"
            print(f"  {c['run_id']:>7} {c['symbol']:<14} "
                  f"{c['main_category']:<11} {float(c['avg_confidence']):>5.2f} "
                  f"{rt:<10}  {c['catalyst_short']}")
    rconn.close()

    # ---------------- 5. 本地 strategy_state 当前 swan 状态分布 ----------------
    section("5. 本地 strategy_state: stype='swan' 当前状态")
    try:
        with pymysql.connect(**local_cfg) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT state, COUNT(*) AS n FROM strategy_state "
                "WHERE strategy='whale' AND stype='swan' "
                "GROUP BY state ORDER BY n DESC"
            )
            states = cur.fetchall()
            cur.execute(
                "SELECT symbol, state, side, updated_at, "
                "       (UNIX_TIMESTAMP(NOW()) - "
                "        UNIX_TIMESTAMP(updated_at))/3600 AS hours_ago "
                "FROM strategy_state "
                "WHERE strategy='whale' AND stype='swan' "
                "ORDER BY updated_at DESC LIMIT 15"
            )
            recent_states = cur.fetchall()
    except Exception as e:
        print(f"  [ERR] 查 strategy_state 失败: {e}")
        states, recent_states = [], []

    if not states:
        print("  [无] stype='swan' 一行都没有 -- SWAN 一次都没真的开过仓 / 写过 state.")
    else:
        active = 0
        for s in states:
            if s["state"] in ("PENDING", "LONG", "SHORT"):
                active += s["n"]
            print(f"  {s['state']:<10} {s['n']:>4}")
        print(f"\n  [active 合计 (PENDING/LONG/SHORT)] {active} / 上限 {max_open}")
        if active >= max_open:
            print("  [关键卡点] SWAN 已达 max_open, 新候选会被全部跳过.")

    if recent_states:
        print(f"\n  最近 15 条 swan state ↓")
        print(f"  {'symbol':<14} {'state':<10} {'side':<6} "
              f"{'updated_at':<19} {'hours_ago':>9}")
        for r in recent_states:
            ha = float(r["hours_ago"] or 0)
            print(f"  {r['symbol']:<14} {r['state']:<10} "
                  f"{(r['side'] or '--'):<6} "
                  f"{str(r['updated_at']):<19} {ha:>9.1f}h")

    # ---------------- 6. 本地 futures_orders order_source 含 swan ----------------
    section("6. 本地 futures_orders: order_source LIKE '%strategy_whale:swan%' "
            "(最近 20 条)")
    try:
        with pymysql.connect(**local_cfg) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT id, symbol, side, status, price, avg_fill_price, "
                "       fill_time, created_at, order_source "
                "FROM futures_orders "
                "WHERE order_source LIKE %s "
                "ORDER BY id DESC LIMIT 20",
                ("%strategy_whale:swan%",),
            )
            orders = cur.fetchall()
            cur.execute(
                "SELECT COUNT(*) AS n FROM futures_orders "
                "WHERE order_source LIKE %s",
                ("%strategy_whale:swan%",),
            )
            total = cur.fetchone()["n"]
    except Exception as e:
        print(f"  [ERR] 查 futures_orders 失败: {e}")
        return

    print(f"  共 {total} 条 swan 下单, 最近 20 ↓")
    if not orders:
        print("  [无] SWAN 从未下过任何单 (futures_orders 里 tag='swan' 一行都没有).")
    else:
        print(f"  {'id':>6} {'symbol':<14} {'side':<6} {'status':<10} "
              f"{'price':>11} {'avg_fill':>11} {'created_at':<19}")
        for o in orders:
            lp = f"{float(o['price']):.6f}" if o['price'] else "--"
            fp = (f"{float(o['avg_fill_price']):.6f}"
                  if o['avg_fill_price'] else "--")
            print(f"  {o['id']:>6} {o['symbol']:<14} {o['side']:<6} "
                  f"{o['status']:<10} {lp:>11} {fp:>11} "
                  f"{str(o['created_at']):<19}")

    # ---------------- 总结 ----------------
    section("总结")
    issues = []
    if not enabled:
        issues.append(
            "swan_strategy_enabled=0 -> swan_strategy_tick 直接 return, "
            "把 system_settings 这行改成 1 (60s reload 自动生效)"
        )
    if behind <= 0:
        issues.append(
            f"游标 swan_last_run_id={last_run_id} 已经 >= 最新 run_id={latest_run_id}, "
            "等下一次 2h Gemini 跑批 (或上 /swan_board 点'立即重跑') 才有新候选"
        )
    if not candidates:
        issues.append(
            f"游标后的所有 run 里, 没有 STRONG + conf >= {min_conf} 的红/黑天鹅 "
            "-- 这是市场原因, 不是 bug. 可以临时降 swan_min_confidence "
            "(比如改 0.60 看看 MODERATE 上下游)"
        )
    if not issues:
        print("  以上 6 项都没明显卡点. 看 strategy_whale.log 里最近 [SWAN] tick 日志,"
              " 通常是被 24h 涨跌守卫 / 黑名单 / 持仓上限 / 个体 cooldown 挡了.")
    else:
        for i, msg in enumerate(issues, 1):
            print(f"  [{i}] {msg}")


if __name__ == "__main__":
    main()
