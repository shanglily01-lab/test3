"""
查 system_settings 里 chase / dump 入场开关状态.
"""
import sys
import pymysql

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

REMOTE_DB = {
    "host": "54.179.112.251", "port": 3306, "user": "admin",
    "password": "Yintao@110", "database": "dimesion", "charset": "utf8mb4",
}

KEYS = (
    "chase_entry_enabled",
    "dump_entry_enabled",
)


def main():
    conn = pymysql.connect(**REMOTE_DB)
    cur = conn.cursor(pymysql.cursors.DictCursor)
    placeholders = ",".join(["%s"] * len(KEYS))
    cur.execute(
        f"SELECT setting_key, setting_value, description, updated_by, updated_at "
        f"FROM system_settings WHERE setting_key IN ({placeholders})",
        KEYS,
    )
    rows = {r["setting_key"]: r for r in cur.fetchall()}
    for k in KEYS:
        r = rows.get(k)
        if not r:
            print(f"[{k}] NOT FOUND")
            continue
        print(f"[{k}] value={r['setting_value']!r}  updated_by={r['updated_by']!r}  updated_at={r['updated_at']}")
        print(f"  desc: {r['description']}")
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
