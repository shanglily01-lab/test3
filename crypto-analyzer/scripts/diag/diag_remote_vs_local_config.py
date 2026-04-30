"""
对比 REMOTE (服务器 dimesion) vs LOCAL (本地) 的配置/数据差异,
回答 "为什么本地亏损少" 的根因.

四个对比维度:
  S1. system_settings 全表 key/value 不一致项
  S2. symbol_blacklist 表差异 (谁拉黑了什么)
  S3. price_stats_24h 当前 universe (quote_volume > 5e6) 差异
  S4. 4-25 ~ 4-29 实际开仓的 symbol 集合差异 (谁多开 / 谁少开)
  S5. strategy_state 表 stype 分布对比 (climax 等 source 残留)

只读, 不改 DB.
用法: python scripts/diag/diag_remote_vs_local_config.py
"""
import sys
import re
from pathlib import Path
from collections import defaultdict
import pymysql

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')


REMOTE_DB = {
    "host": "54.179.112.251", "port": 3306, "user": "admin",
    "password": "Yintao@110", "database": "dimesion", "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
}


def load_local_db():
    env_path = Path(__file__).resolve().parents[2] / ".env"
    cfg = {"host": "localhost", "port": 3306, "charset": "utf8mb4",
           "cursorclass": pymysql.cursors.DictCursor}
    if not env_path.exists():
        raise RuntimeError(f"本地 .env 不存在: {env_path}")
    with env_path.open(encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, v = line.split('=', 1)
            k, v = k.strip(), v.strip().strip('"').strip("'")
            if k == 'DB_HOST': cfg['host'] = v
            elif k == 'DB_PORT': cfg['port'] = int(v)
            elif k == 'DB_USER': cfg['user'] = v
            elif k == 'DB_PASSWORD': cfg['password'] = v
            elif k == 'DB_NAME': cfg['database'] = v
    return cfg


def section(t):
    print('\n' + '=' * 100)
    print(t)
    print('=' * 100)


def fetch_system_settings(cur):
    cur.execute("SELECT setting_key, setting_value FROM system_settings ORDER BY setting_key")
    return {r['setting_key']: (r['setting_value'] or '') for r in cur.fetchall()}


def fetch_symbol_blacklist(cur):
    try:
        cur.execute("SELECT symbol, reason, created_at FROM symbol_blacklist ORDER BY symbol")
        return {r['symbol']: (r.get('reason') or '') for r in cur.fetchall()}
    except Exception:
        return {}


def fetch_universe(cur):
    cur.execute("""
        SELECT symbol, quote_volume_24h, updated_at
        FROM price_stats_24h
        WHERE updated_at >= NOW() - INTERVAL 30 MINUTE
          AND quote_volume_24h > 5e6
        ORDER BY quote_volume_24h DESC
        LIMIT 200
    """)
    return {r['symbol']: float(r.get('quote_volume_24h') or 0) for r in cur.fetchall()}


def fetch_traded_symbols(cur, since: str, until: str):
    cur.execute("""
        SELECT symbol, COUNT(*) AS n, SUM(realized_pnl) AS net_pnl
        FROM futures_positions
        WHERE status='closed'
          AND close_time BETWEEN %s AND %s
          AND source LIKE 'strategy%%'
        GROUP BY symbol
    """, (since, until))
    return {r['symbol']: {'n': int(r['n']), 'net_pnl': float(r['net_pnl'] or 0)}
            for r in cur.fetchall()}


def fetch_strategy_state_stype(cur):
    try:
        cur.execute("""
            SELECT strategy, stype, COUNT(*) AS n, SUM(state!='IDLE') AS active_n
            FROM strategy_state
            GROUP BY strategy, stype
            ORDER BY strategy, stype
        """)
        return [(r['strategy'], r['stype'], int(r['n']), int(r['active_n'] or 0))
                for r in cur.fetchall()]
    except Exception:
        return []


def s1_settings_diff(rs: dict, ls: dict):
    section("S1. system_settings 差异")
    only_remote = sorted(set(rs.keys()) - set(ls.keys()))
    only_local  = sorted(set(ls.keys()) - set(rs.keys()))
    common      = sorted(set(rs.keys()) & set(ls.keys()))

    diffs = [k for k in common if rs[k] != ls[k]]
    print(f"  REMOTE 总数: {len(rs)}, LOCAL 总数: {len(ls)}, 共同 key: {len(common)}, 差异 key: {len(diffs)}")

    if diffs:
        print("\n  [值不同的 key]")
        print(f"  {'key':<35} {'REMOTE':<35} {'LOCAL':<35}")
        for k in diffs:
            rv = (rs[k] or '')[:33]
            lv = (ls[k] or '')[:33]
            print(f"  {k:<35} {rv:<35} {lv:<35}")
    else:
        print("\n  无值差异")

    if only_remote:
        print(f"\n  [仅 REMOTE 有的 key]: {len(only_remote)}")
        for k in only_remote[:30]:
            print(f"    {k} = {rs[k][:60]}")

    if only_local:
        print(f"\n  [仅 LOCAL 有的 key]: {len(only_local)}")
        for k in only_local[:30]:
            print(f"    {k} = {ls[k][:60]}")


def s2_blacklist_diff(rb: dict, lb: dict):
    section("S2. symbol_blacklist 差异")
    only_remote = sorted(set(rb.keys()) - set(lb.keys()))
    only_local  = sorted(set(lb.keys()) - set(rb.keys()))
    print(f"  REMOTE 拉黑: {len(rb)}, LOCAL 拉黑: {len(lb)}")
    print(f"  共同拉黑: {len(set(rb.keys()) & set(lb.keys()))}")
    if only_remote:
        print(f"\n  [仅 REMOTE 拉黑]: {len(only_remote)}")
        for s in only_remote[:30]:
            print(f"    {s}  reason={rb[s][:60]}")
    if only_local:
        print(f"\n  [仅 LOCAL 拉黑]: {len(only_local)}")
        for s in only_local[:30]:
            print(f"    {s}  reason={lb[s][:60]}")
    if not only_remote and not only_local:
        print("  两库拉黑列表完全一致")


def s3_universe_diff(ru: dict, lu: dict):
    section("S3. 当前 universe 差异 (price_stats_24h, quote_volume>5M, 30min 内 updated)")
    print(f"  REMOTE universe: {len(ru)} 个 symbol")
    print(f"  LOCAL  universe: {len(lu)} 个 symbol")
    only_remote = sorted(set(ru.keys()) - set(lu.keys()))
    only_local  = sorted(set(lu.keys()) - set(ru.keys()))
    print(f"  共同: {len(set(ru.keys()) & set(lu.keys()))}")
    if only_remote:
        print(f"\n  [仅 REMOTE universe 有, LOCAL 没有]: {len(only_remote)}")
        for s in only_remote[:20]:
            print(f"    {s:<14} REMOTE_quote_volume={ru[s]/1e6:.1f}M  (LOCAL 缺)")
    if only_local:
        print(f"\n  [仅 LOCAL universe 有, REMOTE 没有]: {len(only_local)}")
        for s in only_local[:20]:
            print(f"    {s:<14} LOCAL_quote_volume={lu[s]/1e6:.1f}M  (REMOTE 缺)")


def s4_traded_diff(rt: dict, lt: dict):
    section("S4. 4-25 ~ 4-29 实际开仓 symbol 差异")
    print(f"  REMOTE 期间开过仓的 symbol: {len(rt)} 个")
    print(f"  LOCAL  期间开过仓的 symbol: {len(lt)} 个")
    only_remote = sorted(set(rt.keys()) - set(lt.keys()))
    only_local  = sorted(set(lt.keys()) - set(rt.keys()))

    if only_remote:
        # 按 REMOTE 净 pnl 升序 (亏的在前)
        items = [(s, rt[s]['n'], rt[s]['net_pnl']) for s in only_remote]
        items.sort(key=lambda x: x[2])
        print(f"\n  [仅 REMOTE 开了, LOCAL 没开]: {len(only_remote)} 个")
        print(f"  {'symbol':<14} {'R_n':>4} {'R_net_pnl':>12}")
        total_lost = 0
        for s, n, p in items[:25]:
            print(f"  {s:<14} {n:>4d} {p:>+11.2f}U")
            total_lost += p
        more_n = sum(rt[s]['n'] for s in only_remote)
        more_pnl = sum(rt[s]['net_pnl'] for s in only_remote)
        print(f"  -- 这些 LOCAL 全没开的 symbol REMOTE 累计 {more_n} 笔, 净 {more_pnl:+.2f}U")

    if only_local:
        items = [(s, lt[s]['n'], lt[s]['net_pnl']) for s in only_local]
        items.sort(key=lambda x: x[2])
        print(f"\n  [仅 LOCAL 开了, REMOTE 没开]: {len(only_local)} 个")
        print(f"  {'symbol':<14} {'L_n':>4} {'L_net_pnl':>12}")
        for s, n, p in items[:15]:
            print(f"  {s:<14} {n:>4d} {p:>+11.2f}U")
        more_n = sum(lt[s]['n'] for s in only_local)
        more_pnl = sum(lt[s]['net_pnl'] for s in only_local)
        print(f"  -- 这些 REMOTE 没开的 symbol LOCAL 累计 {more_n} 笔, 净 {more_pnl:+.2f}U")


def s5_state_stype(rstate: list, lstate: list):
    section("S5. strategy_state stype 分布对比")
    print(f"  {'strategy:stype':<35} {'R_total':>8} {'R_active':>9} {'L_total':>8} {'L_active':>9}")
    rmap = {(s, t): (n, a) for s, t, n, a in rstate}
    lmap = {(s, t): (n, a) for s, t, n, a in lstate}
    keys = sorted(set(rmap.keys()) | set(lmap.keys()))
    for k in keys:
        rn, ra = rmap.get(k, (0, 0))
        ln, la = lmap.get(k, (0, 0))
        flag = ""
        if (rn > 0) != (ln > 0):
            flag = "  <-- 仅一边有"
        print(f"  {k[0] + ':' + k[1]:<35} {rn:>8d} {ra:>9d} {ln:>8d} {la:>9d}{flag}")


def main():
    print("连 REMOTE...")
    rconn = pymysql.connect(**REMOTE_DB); rcur = rconn.cursor()
    rs = fetch_system_settings(rcur)
    rb = fetch_symbol_blacklist(rcur)
    ru = fetch_universe(rcur)
    rt = fetch_traded_symbols(rcur, '2026-04-25 00:00:00', '2026-04-29 23:59:59')
    rstate = fetch_strategy_state_stype(rcur)
    rcur.close(); rconn.close()
    print(f"  REMOTE: {len(rs)} settings / {len(rb)} blacklist / {len(ru)} universe / {len(rt)} traded")

    print("连 LOCAL...")
    try:
        lcfg = load_local_db()
        print(f"  LOCAL host={lcfg.get('host')}:{lcfg.get('port')}/{lcfg.get('database')}")
        lconn = pymysql.connect(**lcfg); lcur = lconn.cursor()
        ls = fetch_system_settings(lcur)
        lb = fetch_symbol_blacklist(lcur)
        lu = fetch_universe(lcur)
        lt = fetch_traded_symbols(lcur, '2026-04-25 00:00:00', '2026-04-29 23:59:59')
        lstate = fetch_strategy_state_stype(lcur)
        lcur.close(); lconn.close()
        print(f"  LOCAL : {len(ls)} settings / {len(lb)} blacklist / {len(lu)} universe / {len(lt)} traded")
    except Exception as e:
        print(f"LOCAL 连接失败: {e!r}")
        return

    s1_settings_diff(rs, ls)
    s2_blacklist_diff(rb, lb)
    s3_universe_diff(ru, lu)
    s4_traded_diff(rt, lt)
    s5_state_stype(rstate, lstate)


if __name__ == '__main__':
    main()
