"""
[A: 短期止血] 回收 strategy_whale 子策略 (swan/rev4d/longhold/w-bottom/m-top)
僵尸 PENDING.

bug: _fill_pending_orders 把限价单超时改 CANCELLED 时, 没同步更新 strategy_state.
PENDING 行卡在那里, _xxx_active_count 永远 >= max_open, 后续候选全部被挡.

代码层修复见 _fill_pending_orders (2026-05-03 同 PR), 此脚本只清理已存在的脏数据.

执行步骤:
  1. SELECT 列出"strategy_state PENDING 但对应 futures_orders 已 CANCELLED" 的行
  2. 等用户输入 yes 确认
  3. UPDATE 把这些 strategy_state 行设为 DONE (pid=NULL, order_id=NULL,
     done_time=NOW, last_reason='cancel'), 走正常 cooldown
  4. 再 SELECT 验证

用法:
  cd crypto-analyzer
  python scripts/diag/fix_swan_zombie_pending.py            # 交互确认
  python scripts/diag/fix_swan_zombie_pending.py --yes      # 直接执行
  python scripts/diag/fix_swan_zombie_pending.py --dry-run  # 只看 plan 不改
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pymysql

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def load_local_cfg() -> dict:
    cfg = {"host": "localhost", "port": 3306, "charset": "utf8mb4",
           "cursorclass": pymysql.cursors.DictCursor}
    env_path = Path(__file__).resolve().parents[2] / ".env"
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


# 哪些 stype 受影响 (主策略 'whale' 已经被 _check_pending_db 单独处理, 不在范围内)
TARGET_STYPES = ("swan", "rev4d", "longhold-w", "longhold-m", "w-bottom", "m-top")


def find_zombies(conn) -> list[dict]:
    """两步查询. strategy_state 和 futures_orders 的 order_id 列 collation 不同
    (utf8mb4_unicode_ci vs utf8mb4_general_ci), 不能直接 JOIN -- 历史踩过坑."""
    # 第 1 步: 子策略 PENDING 行
    placeholder = ",".join(["%s"] * len(TARGET_STYPES))
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id AS state_id, symbol, stype, state, "
            "       order_id, side, updated_at "
            "FROM strategy_state "
            "WHERE strategy='whale' "
            f"  AND stype IN ({placeholder}) "
            "  AND state='PENDING' "
            "ORDER BY updated_at DESC",
            TARGET_STYPES,
        )
        pendings = cur.fetchall()
    if not pendings:
        return []

    # 第 2 步: 这些 order_id 在 futures_orders 的实际状态 (单独查, 无跨表 collation)
    oids = [r["order_id"] for r in pendings if r["order_id"]]
    order_map: dict[str, dict] = {}
    if oids:
        ph = ",".join(["%s"] * len(oids))
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT order_id, id AS order_pk, status, "
                f"       cancellation_reason, canceled_at "
                f"FROM futures_orders WHERE order_id IN ({ph})",
                oids,
            )
            for r in cur.fetchall():
                order_map[r["order_id"]] = r

    # 第 3 步: Python 端筛 -- order=CANCELLED/REJECTED/丢失 的才算僵尸
    zombies: list[dict] = []
    for ss in pendings:
        oid = ss["order_id"]
        fo = order_map.get(oid) if oid else None
        if fo is None:
            ss["order_status"] = "MISSING"
            ss["cancellation_reason"] = None
            zombies.append(ss)
        elif (fo.get("status") or "").upper() in ("CANCELLED", "REJECTED"):
            ss["order_status"] = fo.get("status")
            ss["cancellation_reason"] = fo.get("cancellation_reason")
            zombies.append(ss)
        # else: PENDING/FILLING/FILLED 都不是僵尸, 跳过
    return zombies


def fix_zombies(conn, rows: list[dict]) -> int:
    """把这些 strategy_state 行改成 DONE. 用 UNIX_TIMESTAMP(NOW()) 写 done_time."""
    if not rows:
        return 0
    ids = [r["state_id"] for r in rows]
    placeholder = ",".join(["%s"] * len(ids))
    sql = (
        f"UPDATE strategy_state "
        f"SET state='DONE', pid=NULL, order_id=NULL, "
        f"    done_time=UNIX_TIMESTAMP(NOW()), last_reason='cancel-cleanup' "
        f"WHERE id IN ({placeholder})"
    )
    with conn.cursor() as cur:
        cur.execute(sql, ids)
        affected = cur.rowcount
    conn.commit()
    return affected


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--yes", action="store_true", help="不交互确认")
    ap.add_argument("--dry-run", action="store_true", help="只显示 plan, 不执行")
    args = ap.parse_args()

    cfg = load_local_cfg()
    print(f"[local DB] {cfg.get('host')}:{cfg.get('port')} / "
          f"{cfg.get('database')}\n")

    with pymysql.connect(**cfg) as conn:
        rows = find_zombies(conn)
        if not rows:
            print("[OK] 没有僵尸 PENDING. 不需要清理.")
            return

        print(f"找到 {len(rows)} 个僵尸 PENDING (state=PENDING 但 order 已 "
              f"CANCELLED/REJECTED/丢失):\n")
        print(f"  {'state_id':>8} {'symbol':<14} {'stype':<11} {'side':<6} "
              f"{'order_status':<12} {'reason':<10} {'updated_at':<19}")
        for r in rows:
            print(f"  {r['state_id']:>8} {r['symbol']:<14} "
                  f"{r['stype']:<11} {(r['side'] or '--'):<6} "
                  f"{(r['order_status'] or 'MISSING'):<12} "
                  f"{(r['cancellation_reason'] or '--'):<10} "
                  f"{str(r['updated_at']):<19}")

        # 按 stype 汇总, 强调 max_open 影响
        from collections import Counter
        by_stype = Counter(r["stype"] for r in rows)
        print(f"\n  按 stype 分布: {dict(by_stype)}")
        print("  -> 这些 PENDING 一直占 active_count, 让出后子策略才能开新单.\n")

        if args.dry_run:
            print("[--dry-run] 不执行 UPDATE, 退出.")
            return

        if not args.yes:
            ans = input(
                f"确认把这 {len(rows)} 行 strategy_state 设为 DONE? "
                "(yes/no) "
            ).strip().lower()
            if ans not in ("yes", "y"):
                print("[取消] 未执行.")
                return

        affected = fix_zombies(conn, rows)
        print(f"\n[OK] UPDATE 影响 {affected} 行.")

        # 复查
        remaining = find_zombies(conn)
        if remaining:
            print(f"[WARN] 复查仍有 {len(remaining)} 个僵尸 PENDING, "
                  "可能有并发写入, 请稍后再跑一次.")
        else:
            print("[OK] 复查通过, 所有僵尸 PENDING 已清理.")
            print("\n下一轮策略 tick (60s 内) 应该能吃到新候选, "
                  "用 diag_swan_strategy_status.py 二次确认.")


if __name__ == "__main__":
    main()
