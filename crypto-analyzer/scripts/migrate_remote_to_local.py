#!/usr/bin/env python3
"""
一次性数据迁移: 远程 dimesion → 本地 binance-data.

2026-05-18 用户决策 [[feedback-db-local-only-2026-05-18]]: 全部迁本地.
本地 117 张表 schema 已就绪, 大部分表数据本地已有, 只需补这些:

  - system_settings   : INSERT IGNORE (远程独有的 key 加进本地, 已有的不动)
  - gemini_swan_runs  : 全迁 (本地 0 行, 远程有历史, 给 hit rate 用)
  - gemini_swan_verdicts: 全迁

其他表 (kline_data / funding_rate_data / strategy_state / futures_orders 等)
本地已有大量数据, 不迁.

用法:
  cd crypto-analyzer
  python scripts/migrate_remote_to_local.py            # 干跑预览
  python scripts/migrate_remote_to_local.py --apply    # 真执行

远程 dimesion 连接信息 hardcode 在此脚本 (一次性, 用完即弃, 不再放 table_schemas.txt).
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

import pymysql


REMOTE_CFG = {
    "host": "54.179.112.251",
    "port": 3306,
    "user": "admin",
    "password": "Yintao@110",
    "database": "dimesion",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
    "connect_timeout": 30,
}


def local_cfg() -> dict:
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "3306")),
        "user": os.getenv("DB_USER", "root"),
        "password": os.getenv("DB_PASSWORD", ""),
        "database": os.getenv("DB_NAME", "binance-data"),
        "charset": "utf8mb4",
        "cursorclass": pymysql.cursors.DictCursor,
    }


def _get_columns(conn, table: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(f"SHOW COLUMNS FROM `{table}`")
        return [r["Field"] for r in cur.fetchall()]


def _table_exists(conn, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) AS n FROM information_schema.tables "
            "WHERE table_schema = %s AND table_name = %s",
            (conn.db.decode() if isinstance(conn.db, bytes) else conn.db, table),
        )
        return cur.fetchone()["n"] > 0


def migrate_system_settings(remote, local, apply: bool) -> tuple[int, int, int]:
    """INSERT IGNORE 合并. 返回 (远程总数, 本地已有, 实际插入)."""
    with remote.cursor() as rc:
        rc.execute("SELECT * FROM system_settings")
        remote_rows = rc.fetchall()
    if not remote_rows:
        return 0, 0, 0

    with local.cursor() as lc:
        lc.execute("SELECT setting_key FROM system_settings")
        local_keys = {r["setting_key"] for r in lc.fetchall()}

    new_rows = [r for r in remote_rows if r["setting_key"] not in local_keys]
    if not apply:
        return len(remote_rows), len(local_keys), len(new_rows)

    cols = list(remote_rows[0].keys())
    placeholders = ",".join(["%s"] * len(cols))
    col_list = ",".join(f"`{c}`" for c in cols)
    with local.cursor() as lc:
        for r in new_rows:
            lc.execute(
                f"INSERT IGNORE INTO system_settings ({col_list}) VALUES ({placeholders})",
                tuple(r.get(c) for c in cols),
            )
    local.commit()
    return len(remote_rows), len(local_keys), len(new_rows)


def migrate_table_full(remote, local, table: str, apply: bool) -> tuple[int, int, int]:
    """全量迁移 (本地无该表数据时). 返回 (远程总数, 本地已有, 插入).

    本地已有数据时跳过 (避免重复). 用户要重新迁可先 TRUNCATE local table.
    """
    if not _table_exists(remote, table):
        print(f"  [skip] 远程无表 {table}")
        return 0, 0, 0
    if not _table_exists(local, table):
        print(f"  [skip] 本地无表 {table} (schema 缺失, 不能迁数据)")
        return 0, 0, 0

    with local.cursor() as lc:
        lc.execute(f"SELECT COUNT(*) AS n FROM `{table}`")
        local_n = lc.fetchone()["n"]
    if local_n > 0:
        print(f"  [skip] 本地 {table} 已有 {local_n} 行, 跳过 (避免重复)")
        with remote.cursor() as rc:
            rc.execute(f"SELECT COUNT(*) AS n FROM `{table}`")
            remote_n = rc.fetchone()["n"]
        return remote_n, local_n, 0

    # 取列交集 (远程列名为准, 本地缺的列省略)
    remote_cols = _get_columns(remote, table)
    local_cols = set(_get_columns(local, table))
    cols = [c for c in remote_cols if c in local_cols]
    if not cols:
        print(f"  [skip] {table} 列交集为空")
        return 0, 0, 0

    col_list = ",".join(f"`{c}`" for c in cols)
    placeholders = ",".join(["%s"] * len(cols))

    with remote.cursor() as rc:
        rc.execute(f"SELECT {col_list} FROM `{table}`")
        rows = rc.fetchall()

    if not apply:
        return len(rows), 0, len(rows)

    if not rows:
        return 0, 0, 0

    with local.cursor() as lc:
        batch = [tuple(r.get(c) for c in cols) for r in rows]
        lc.executemany(
            f"INSERT IGNORE INTO `{table}` ({col_list}) VALUES ({placeholders})",
            batch,
        )
    local.commit()
    return len(rows), 0, len(rows)


def main():
    parser = argparse.ArgumentParser(description="一次性数据迁移 dimesion -> binance-data")
    parser.add_argument("--apply", action="store_true",
                        help="真执行 (默认干跑预览)")
    args = parser.parse_args()

    mode = "APPLY (真执行)" if args.apply else "DRY-RUN (预览, 加 --apply 真执行)"
    print(f"=== migrate_remote_to_local: {mode} ===\n")

    print(f"远程: {REMOTE_CFG['user']}@{REMOTE_CFG['host']}:{REMOTE_CFG['port']}/{REMOTE_CFG['database']}")
    lcfg = local_cfg()
    print(f"本地: {lcfg['user']}@{lcfg['host']}:{lcfg['port']}/{lcfg['database']}\n")

    try:
        remote = pymysql.connect(**REMOTE_CFG)
    except Exception as e:
        print(f"ERROR: 远程连接失败: {e}")
        sys.exit(2)
    try:
        local = pymysql.connect(**lcfg)
    except Exception as e:
        print(f"ERROR: 本地连接失败: {e}")
        remote.close()
        sys.exit(3)

    try:
        # 1. system_settings: INSERT IGNORE 合并
        print(">>> system_settings (INSERT IGNORE 合并)")
        r, l, n = migrate_system_settings(remote, local, args.apply)
        print(f"    远程 {r} 行, 本地已有 {l} key, 本次新增 {n} key\n")

        # 2. gemini_swan_runs / verdicts: 全迁 (本地空时)
        for table in ["gemini_swan_runs", "gemini_swan_verdicts"]:
            print(f">>> {table} (全迁 if 本地空)")
            r, l, n = migrate_table_full(remote, local, table, args.apply)
            print(f"    远程 {r} 行, 本地已有 {l} 行, 本次插入 {n} 行\n")

    finally:
        remote.close()
        local.close()

    if not args.apply:
        print("=== DRY-RUN 完成, 加 --apply 真执行 ===")
    else:
        print("=== 迁移完成 ===")


if __name__ == "__main__":
    main()
