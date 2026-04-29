"""
诊断 DAM/USDT 04-28 chase-entry #2 LONG +783.60U/+267.68% 反常事件

异常点:
- 出场被打 "止损" tag, 但 entry 0.029480 -> exit 0.045281 是 +53.6% 价格涨幅 (LONG 方向)
- LONG 的硬 SL 应在 entry 下方约 -10%, 价格上行根本不应触发 SL
- 用户提示 "服务器端和本地端都出了这一单" -> 怀疑双库各自跑了一份, 也许有重复开仓/双轨执行

数据源 (按用户规范, 两边都查, 不要混):
- 服务器端: dimesion @ 54.179.112.251 (table_schemas.txt 头部)
- 本地端  : binance-data @ localhost (crypto-analyzer/.env)

对每个库都查:
1. futures_positions: DAM/USDT 04-27..04-29 的全部仓位 (含 #1 #2 #3 ...)
2. futures_orders   : 同区间全部订单 (PENDING/FILLED/CANCELED, 含 source/sync 字段)
3. strategy_state   : strategy='live' & symbol='DAM/USDT' 当前 + 历史
4. 若服务器端 SYNCED -> 拉对应 live_futures_positions 看真实 binance 端口

输出: 按库分组打印 + 关键时间线对齐
"""
import os
import sys
import pymysql
from pathlib import Path

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# 服务器端: 写死 (table_schemas.txt 头部, IP 会变, 真要变了再改这个常量, 不读 memory)
REMOTE_DB = {
    "host": "54.179.112.251",
    "port": 3306,
    "user": "admin",
    "password": "Yintao@110",
    "database": "dimesion",
    "charset": "utf8mb4",
}

# 本地端: 从 .env 读
def load_local_db():
    env_path = Path(__file__).resolve().parents[2] / ".env"
    cfg = {"host": "localhost", "port": 3306, "charset": "utf8mb4"}
    with env_path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            k = k.strip(); v = v.strip()
            if k == "DB_HOST": cfg["host"] = v
            elif k == "DB_PORT": cfg["port"] = int(v)
            elif k == "DB_USER": cfg["user"] = v
            elif k == "DB_PASSWORD": cfg["password"] = v
            elif k == "DB_NAME": cfg["database"] = v
    return cfg


SYM = "DAM/USDT"
SINCE = "2026-04-27 00:00:00"
UNTIL = "2026-04-29 23:59:59"


def section(title):
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)


def dump_positions(cur, label):
    cur.execute(
        """
        SELECT id, symbol, position_side AS side, leverage, quantity,
               entry_price, mark_price, notional_value, margin,
               status, source, notes, entry_reason,
               stop_loss_price, take_profit_price,
               trailing_stop_activated, trailing_stop_price,
               max_profit_pct, max_profit_price, max_profit_time,
               open_time, close_time, last_update_time,
               TIMESTAMPDIFF(MINUTE, open_time, close_time) AS hold_min,
               realized_pnl, unrealized_pnl, live_position_id
        FROM futures_positions
        WHERE symbol = %s AND open_time BETWEEN %s AND %s
        ORDER BY open_time ASC
        """,
        (SYM, SINCE, UNTIL),
    )
    rows = cur.fetchall()
    print(f"  [{label}] futures_positions  共 {len(rows)} 条")
    for i, r in enumerate(rows, 1):
        print(f"  --- #{i}  pid={r['id']}  {r['side']}  status={r['status']}  source={r['source']!r}  live_pid={r['live_position_id']} ---")
        print(f"    open  = {r['open_time']}   close = {r['close_time']}   hold = {r['hold_min']}min   last_upd={r['last_update_time']}")
        print(f"    entry = {r['entry_price']}   mark  = {r['mark_price']}   qty = {r['quantity']}   lev = {r['leverage']}")
        print(f"    notional={r['notional_value']}  margin={r['margin']}")
        print(f"    SL    = {r['stop_loss_price']}   TP    = {r['take_profit_price']}")
        print(f"    trail_active={r['trailing_stop_activated']}  trail_price={r['trailing_stop_price']}")
        print(f"    max_profit_pct={r['max_profit_pct']}  max_profit_price={r['max_profit_price']}  at={r['max_profit_time']}")
        print(f"    notes = {r['notes']!r}")
        print(f"    entry_reason = {r['entry_reason']!r}")
        print(f"    realized_pnl = {r['realized_pnl']}   unrealized_pnl = {r['unrealized_pnl']}")
    return rows


def dump_orders(cur, label, pids=None):
    # LOCAL 库 schema 老, 没有 live_sync_status / live_synced_at / live_position_id
    has_sync_cols = label != "LOCAL"
    extra = (
        ", live_sync_status, live_synced_at, live_position_id"
        if has_sync_cols
        else ", NULL AS live_sync_status, NULL AS live_synced_at, NULL AS live_position_id"
    )
    cur.execute(
        f"""
        SELECT id, order_id, symbol, side, order_type, status,
               quantity, executed_quantity, price, avg_fill_price, leverage,
               stop_price, stop_loss_price, take_profit_price,
               realized_pnl, pnl_pct, fee, notes,
               created_at, fill_time, canceled_at,
               TIMESTAMPDIFF(SECOND, created_at, fill_time) AS pending_sec,
               order_source, cancellation_reason,
               position_id
               {extra}
        FROM futures_orders
        WHERE symbol = %s AND created_at BETWEEN %s AND %s
        ORDER BY id ASC
        """,
        (SYM, SINCE, UNTIL),
    )
    rows = cur.fetchall()
    print(f"\n  [{label}] futures_orders  共 {len(rows)} 条")
    for r in rows:
        marker = ""
        if pids and r.get("position_id") in pids:
            marker = f"  <-- 关联 pid={r['position_id']}"
        print(f"    oid={r['id']:<7} {r['side']:<14} {r['order_type']:<20} {r['status']:<10} src={r['order_source']!r}{marker}")
        print(f"      qty={r['quantity']} exec={r['executed_quantity']} lev={r['leverage']} price={r['price']} avg_fill={r['avg_fill_price']}")
        print(f"      stop={r['stop_price']} SL={r['stop_loss_price']} TP={r['take_profit_price']}")
        print(f"      pnl={r['realized_pnl']} pnl_pct={r['pnl_pct']} fee={r['fee']}")
        print(f"      created={r['created_at']} fill={r['fill_time']} pending={r['pending_sec']}s cancel={r['canceled_at']}")
        print(f"      live_sync={r['live_sync_status']} synced_at={r['live_synced_at']} live_pid={r['live_position_id']}")
        print(f"      cancel_reason={r['cancellation_reason']!r}  notes={r['notes']!r}")
    return rows


def dump_strategy_state(cur, label):
    cur.execute(
        """
        SELECT id, strategy, symbol, stype, state, pid, entry_p,
               entry_time, FROM_UNIXTIME(entry_time) AS entry_time_dt,
               done_time, FROM_UNIXTIME(done_time) AS done_time_dt,
               peak_pnl_pct, last_reason, updated_at
        FROM strategy_state
        WHERE strategy='live' AND symbol=%s
        ORDER BY id DESC LIMIT 10
        """,
        (SYM,),
    )
    rows = cur.fetchall()
    print(f"\n  [{label}] strategy_state(live, {SYM})  共 {len(rows)} 条")
    for r in rows:
        print(f"    id={r['id']} stype={r['stype']} state={r['state']} pid={r['pid']} peak={r['peak_pnl_pct']}")
        print(f"      entry_p={r['entry_p']} entry={r['entry_time_dt']} done={r['done_time_dt']}")
        print(f"      reason={r['last_reason']!r}  updated={r['updated_at']}")


def dump_live_futures_positions(cur, live_pids):
    if not live_pids:
        print("  (没有 SYNCED 的 live_position_id, 跳过 live_futures_positions)")
        return
    placeholders = ",".join(["%s"] * len(live_pids))
    cur.execute(
        f"""
        SELECT id, symbol, position_side AS side, quantity, leverage,
               entry_price, mark_price, close_price, notional_value, margin,
               unrealized_pnl, realized_pnl,
               stop_loss_price, take_profit_price,
               trailing_stop_activated, trailing_stop_price,
               max_profit_pct, max_profit_price,
               binance_order_id, sl_order_id, tp_order_id, paper_position_id,
               status, close_reason, source,
               open_time, close_time,
               TIMESTAMPDIFF(MINUTE, open_time, close_time) AS hold_min,
               notes
        FROM live_futures_positions
        WHERE id IN ({placeholders})
        ORDER BY id ASC
        """,
        tuple(live_pids),
    )
    rows = cur.fetchall()
    print(f"\n  [REMOTE] live_futures_positions (binance 端真实仓位)  共 {len(rows)} 条")
    for r in rows:
        print(f"    live_pid={r['id']} {r['side']} status={r['status']}  close_reason={r['close_reason']!r}  paper_pid={r['paper_position_id']}")
        print(f"      qty={r['quantity']} lev={r['leverage']} entry={r['entry_price']} close={r['close_price']} mark={r['mark_price']}")
        print(f"      notional={r['notional_value']} margin={r['margin']} realized_pnl={r['realized_pnl']} unrealized_pnl={r['unrealized_pnl']}")
        print(f"      SL={r['stop_loss_price']} TP={r['take_profit_price']}")
        print(f"      trail_active={r['trailing_stop_activated']} trail_price={r['trailing_stop_price']}")
        print(f"      max_profit_pct={r['max_profit_pct']} max_profit_price={r['max_profit_price']}")
        print(f"      binance_oid={r['binance_order_id']} sl_oid={r['sl_order_id']} tp_oid={r['tp_order_id']}")
        print(f"      open={r['open_time']} close={r['close_time']} hold={r['hold_min']}min")
        print(f"      source={r['source']!r}  notes={r['notes']!r}")


def main():
    section(f"REMOTE = dimesion @ 54.179.112.251 (服务器端真实生产库)")
    rconn = pymysql.connect(**REMOTE_DB)
    rcur = rconn.cursor(pymysql.cursors.DictCursor)
    r_pos = dump_positions(rcur, "REMOTE")
    r_pids = {r["id"] for r in r_pos}
    r_ord = dump_orders(rcur, "REMOTE", r_pids)
    dump_strategy_state(rcur, "REMOTE")
    live_pids = sorted({o["live_position_id"] for o in r_ord if o.get("live_position_id")})
    dump_live_futures_positions(rcur, live_pids)
    rcur.close(); rconn.close()

    section(f"LOCAL  = binance-data @ localhost (本地开发/影子库)")
    try:
        lconn = pymysql.connect(**load_local_db())
        lcur = lconn.cursor(pymysql.cursors.DictCursor)
        l_pos = dump_positions(lcur, "LOCAL")
        l_pids = {r["id"] for r in l_pos}
        dump_orders(lcur, "LOCAL", l_pids)
        dump_strategy_state(lcur, "LOCAL")
        lcur.close(); lconn.close()
    except Exception as e:
        print(f"  LOCAL 连接失败: {e!r}")

    section("快照对比小结")
    print(f"  REMOTE 04-27..04-29 DAM 仓位数: {len(r_pos)}")
    if 'l_pos' in dir():
        print(f"  LOCAL  04-27..04-29 DAM 仓位数: {len(l_pos)}")
    print("  -> 看上面两库的 #N 仓位是否 open_time/entry/exit/notes 完全一致, 还是双轨独立各开各的")


if __name__ == "__main__":
    main()
