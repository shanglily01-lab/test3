"""
关闭 chase_entry / dump_entry 入场开关.

2026-05-15 用户授权: 5/5 web_ui 误开后 7 天累计 -729U (chase -329 + dump -400),
重新置为 0 (策略主循环 60s 内动态 reload, 已有持仓继续 monitor SL/TP).
"""
import sys
import pymysql

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

REMOTE_DB = {
    "host": "54.179.112.251", "port": 3306, "user": "admin",
    "password": "Yintao@110", "database": "dimesion", "charset": "utf8mb4",
}

KEYS = ("chase_entry_enabled", "dump_entry_enabled")
UPDATED_BY = "claude_authorized_by_user_2026-05-15"


def show(cur, when):
    print(f"\n--- {when} ---")
    placeholders = ",".join(["%s"] * len(KEYS))
    cur.execute(
        f"SELECT setting_key, setting_value, updated_by, updated_at "
        f"FROM system_settings WHERE setting_key IN ({placeholders})",
        KEYS,
    )
    for r in cur.fetchall():
        print(f"  {r['setting_key']:<22} = {r['setting_value']!r:<5}  "
              f"updated_by={r['updated_by']!r:<35} at {r['updated_at']}")


def main():
    conn = pymysql.connect(**REMOTE_DB, autocommit=False)
    cur = conn.cursor(pymysql.cursors.DictCursor)
    show(cur, "BEFORE")

    placeholders = ",".join(["%s"] * len(KEYS))
    affected = cur.execute(
        f"UPDATE system_settings SET setting_value='0', updated_by=%s "
        f"WHERE setting_key IN ({placeholders}) AND setting_value <> '0'",
        (UPDATED_BY, *KEYS),
    )
    conn.commit()
    print(f"\n[update] affected_rows={affected}")

    show(cur, "AFTER")
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
