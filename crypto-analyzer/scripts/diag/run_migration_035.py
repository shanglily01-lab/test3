"""执行 migration 035: 删 30+ 个 dead strategy settings."""
import sys, pymysql
from pathlib import Path

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

REMOTE_DB = {
    "host": "54.179.112.251", "port": 3306, "user": "admin",
    "password": "Yintao@110", "database": "dimesion", "charset": "utf8mb4",
}

SQL_FILE = Path(__file__).resolve().parents[1] / "migrations" / "035_drop_dead_strategy_settings.sql"


def main():
    raw = SQL_FILE.read_text(encoding="utf-8")
    # 去掉 -- 单行注释 (保留 IN (...) 等多行内容)
    lines = []
    for ln in raw.splitlines():
        stripped = ln.split("--", 1)[0].rstrip()
        if stripped:
            lines.append(stripped)
    sql_clean = "\n".join(lines)
    statements = [s.strip() for s in sql_clean.split(";") if s.strip()]
    statements = [s for s in statements if s.upper().split(None, 1)[0] in ("DELETE", "UPDATE", "INSERT")]
    print(f"将执行 {len(statements)} 条 DML")
    conn = pymysql.connect(**REMOTE_DB, autocommit=False)
    cur = conn.cursor(pymysql.cursors.DictCursor)

    cur.execute("SELECT COUNT(*) AS n FROM system_settings")
    n_before = cur.fetchone()["n"]
    print(f"before total rows = {n_before}")

    total_affected = 0
    for s in statements:
        n = cur.execute(s)
        total_affected += n
        first_line = s.split("\n", 1)[0][:80]
        print(f"  affected={n:<4} {first_line}...")

    conn.commit()
    cur.execute("SELECT COUNT(*) AS n FROM system_settings")
    n_after = cur.fetchone()["n"]
    print(f"after  total rows = {n_after}")
    print(f"deleted = {n_before - n_after}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
