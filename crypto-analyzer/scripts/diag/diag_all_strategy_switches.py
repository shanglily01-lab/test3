"""列出 strategy_live / strategy_whale / strategy_f3 全部相关 system_settings."""
import sys, pymysql

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

REMOTE_DB = {
    "host": "54.179.112.251", "port": 3306, "user": "admin",
    "password": "Yintao@110", "database": "dimesion", "charset": "utf8mb4",
}

GROUPS = {
    "通用守卫 (所有策略)": [
        "disable_sl_tp_hold", "disable_5m_confirm",
    ],
    "strategy_live 主参数": [
        "live_sl_pct", "live_hard_tp_pct", "live_limit_offset_pct", "live_hold_hours",
    ],
    "strategy_live 子策略开关": [
        "chase_entry_enabled", "dump_entry_enabled",
        "chase_allow_slow",
        "dump_signal_wait_enabled", "dump_signal_wait_min", "dump_signal_adverse_pct",
        "topshort_signal_wait_enabled", "topshort_signal_wait_min", "topshort_signal_adverse_pct",
    ],
    "strategy_whale 主参数": [
        "whale_sl_pct", "whale_hard_tp_pct", "whale_limit_offset_pct", "whale_hold_hours",
    ],
    "strategy_whale 子策略": [
        "longhold_enabled", "longhold_sl_pct", "longhold_tp_pct",
        "longhold_limit_offset_pct", "longhold_hold_hours", "longhold_rebound_pct",
        "rev4d_enabled", "rev4d_threshold_pct", "rev4d_sl_pct", "rev4d_tp_pct",
        "rev4d_hold_hours", "rev4d_cooldown_hours",
        "swan_strategy_enabled", "swan_min_confidence", "swan_position_usdt",
        "swan_leverage", "swan_max_open", "swan_hold_minutes", "swan_cooldown_hours",
    ],
    "strategy_f3": [
        "f3_strategy_enabled",
    ],
}

def main():
    conn = pymysql.connect(**REMOTE_DB)
    cur = conn.cursor(pymysql.cursors.DictCursor)
    all_keys = [k for group in GROUPS.values() for k in group]
    ph = ",".join(["%s"] * len(all_keys))
    cur.execute(
        f"SELECT setting_key, setting_value, updated_by, updated_at "
        f"FROM system_settings WHERE setting_key IN ({ph})",
        all_keys,
    )
    rows = {r["setting_key"]: r for r in cur.fetchall()}
    for title, keys in GROUPS.items():
        print(f"\n=== {title} ===")
        for k in keys:
            r = rows.get(k)
            if not r:
                print(f"  {k:<35} (NOT SET, 用代码默认值)")
                continue
            print(f"  {k:<35} = {r['setting_value']!r:<12}  "
                  f"by={r['updated_by']!r:<25} at {r['updated_at']}")
    cur.close(); conn.close()

if __name__ == "__main__":
    main()
