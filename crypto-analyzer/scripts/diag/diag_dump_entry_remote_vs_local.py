"""
专项对比 REMOTE vs LOCAL strategy_live:dump-entry 在 CST 4-25 ~ 4-29 同 UTC 时段
为什么 REMOTE -106U / LOCAL +416U 差 522U.

时区注意: REMOTE 是 UTC, LOCAL 是 CST. 用 UNIX_TIMESTAMP 做窗口避免错位.
只读, 不改 DB.
"""
import sys
import pymysql
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import defaultdict

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

CST = timezone(timedelta(hours=8))
UNIX_START = int(datetime(2026, 4, 25, 0, 0, 0, tzinfo=CST).timestamp())
UNIX_END   = int(datetime(2026, 4, 29, 23, 59, 59, tzinfo=CST).timestamp())


def conn_remote():
    return pymysql.connect(host='54.179.112.251', port=3306, user='admin',
                           password='Yintao@110', database='dimesion', charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)


def conn_local():
    cfg = {'host': 'localhost', 'port': 3306, 'charset': 'utf8mb4',
           'cursorclass': pymysql.cursors.DictCursor}
    with (Path(__file__).resolve().parents[2] / '.env').open(encoding='utf-8') as f:
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
    return pymysql.connect(**cfg)


SQL = """
SELECT p.id, p.symbol, p.position_side AS side,
       p.entry_price, p.stop_loss_price, p.take_profit_price,
       p.realized_pnl, p.max_profit_pct, p.holding_hours, p.notes,
       p.entry_score,
       UNIX_TIMESTAMP(p.open_time)  AS unix_open,
       UNIX_TIMESTAMP(p.close_time) AS unix_close,
       (SELECT o.order_source FROM futures_orders o
         WHERE o.position_id = p.id
           AND o.side IN ('OPEN_LONG','OPEN_SHORT')
         ORDER BY o.id ASC LIMIT 1) AS open_source
FROM futures_positions p
WHERE p.status='closed'
  AND UNIX_TIMESTAMP(p.close_time) BETWEEN %s AND %s
  AND p.source = 'strategy_live:dump-entry'
ORDER BY p.symbol, unix_open
"""


def fetch(conn):
    cur = conn.cursor()
    cur.execute(SQL, (UNIX_START, UNIX_END))
    rows = cur.fetchall()
    cur.close()
    return rows


def fmt_dt(unix_s, tz=CST):
    return datetime.fromtimestamp(unix_s, tz).strftime('%m-%d %H:%M')


def short_notes(s):
    if not s:
        return ''
    s = s.replace('\n', ' ')[:30]
    return s


def main():
    R = conn_remote(); L = conn_local()
    r_rows = fetch(R); l_rows = fetch(L)
    R.close(); L.close()
    print(f"窗口 (CST): 4-25 00:00 ~ 4-29 23:59  (UTC: 4-24 16:00 ~ 4-29 15:59)")
    print(f"REMOTE dump-entry: {len(r_rows)} 笔")
    print(f"LOCAL  dump-entry: {len(l_rows)} 笔")

    # 整体
    def stats(rows, label):
        if not rows: return
        pnls = [float(r['realized_pnl'] or 0) for r in rows]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p < 0]
        print(f"  [{label}] n={len(pnls)} 胜率={len(wins)/len(pnls)*100:.1f}%  "
              f"net={sum(pnls):+.2f}U  avg={sum(pnls)/len(pnls):+.2f}U  "
              f"wins_sum={sum(wins):+.2f}  losses_sum={sum(losses):+.2f}")
    print()
    stats(r_rows, "REMOTE")
    stats(l_rows, "LOCAL ")

    # 1. 全部明细 (CST 时间)
    def print_all(rows, label):
        print(f"\n{'='*100}")
        print(f"  [{label}] 全部 {len(rows)} 笔 dump-entry 明细 (CST 时间, side/entry/sl/tp/score/hold/notes/pnl)")
        print(f"{'='*100}")
        print(f"  {'open(CST)':<12} {'close(CST)':<12} {'symbol':<14} {'side':<5} "
              f"{'entry':>10} {'sl':>10} {'tp':>10} {'sc':>3} {'hold':>5} {'notes':<30} {'pnl':>9}")
        for r in rows:
            entry = float(r['entry_price'] or 0)
            sl = float(r['stop_loss_price'] or 0) if r.get('stop_loss_price') else 0
            tp = float(r['take_profit_price'] or 0) if r.get('take_profit_price') else 0
            sl_pct = (entry - sl) / entry * 100 if entry > 0 and sl > 0 and r['side'] == 'SHORT' else \
                     (sl - entry) / entry * 100 if entry > 0 and sl > 0 else 0
            tp_pct = (entry - tp) / entry * 100 if entry > 0 and tp > 0 and r['side'] == 'SHORT' else \
                     (tp - entry) / entry * 100 if entry > 0 and tp > 0 else 0
            sl_disp = f"{sl_pct:+.1f}%" if sl_pct else 'none'
            tp_disp = f"{tp_pct:+.1f}%" if tp_pct else 'none'
            print(f"  {fmt_dt(r['unix_open']):<12} {fmt_dt(r['unix_close']):<12} "
                  f"{r['symbol']:<14} {r['side']:<5} {entry:>10.6f} "
                  f"{sl_disp:>10} {tp_disp:>10} "
                  f"{(r.get('entry_score') or 0):>3} "
                  f"{(r['holding_hours'] or 0):>5} "
                  f"{short_notes(r.get('notes')):<30} "
                  f"{float(r['realized_pnl'] or 0):>+8.2f}")

    print_all(r_rows, 'REMOTE')
    print_all(l_rows, 'LOCAL ')

    # 2. 共同 symbol 对比
    r_by_sym = defaultdict(list); l_by_sym = defaultdict(list)
    for r in r_rows: r_by_sym[r['symbol']].append(r)
    for r in l_rows: l_by_sym[r['symbol']].append(r)
    common = sorted(set(r_by_sym.keys()) & set(l_by_sym.keys()))
    only_r  = sorted(set(r_by_sym.keys()) - set(l_by_sym.keys()))
    only_l  = sorted(set(l_by_sym.keys()) - set(r_by_sym.keys()))

    print(f"\n{'='*100}")
    print(f"  共同 symbol: {len(common)}, 仅REMOTE: {len(only_r)}, 仅LOCAL: {len(only_l)}")
    print(f"{'='*100}")

    # 共同 symbol 明细
    if common:
        print(f"\n  [共同 symbol — 同信号在两库的处理对比]")
        for sym in common:
            print(f"\n  ── {sym} ──")
            for r in r_by_sym[sym]:
                print(f"    REMOTE  open={fmt_dt(r['unix_open'])}  side={r['side']}  "
                      f"entry={float(r['entry_price']):.6f}  hold={r['holding_hours']}h  "
                      f"notes={short_notes(r.get('notes')):<25}  pnl={float(r['realized_pnl'] or 0):+.2f}")
            for r in l_by_sym[sym]:
                print(f"    LOCAL   open={fmt_dt(r['unix_open'])}  side={r['side']}  "
                      f"entry={float(r['entry_price']):.6f}  hold={r['holding_hours']}h  "
                      f"notes={short_notes(r.get('notes')):<25}  pnl={float(r['realized_pnl'] or 0):+.2f}")

    # 3. 仅 REMOTE 开的 symbol (LOCAL 这段时段没开)
    if only_r:
        print(f"\n{'='*100}")
        print(f"  [仅 REMOTE 开的 symbol — LOCAL 没触发]")
        print(f"{'='*100}")
        items = []
        for sym in only_r:
            for r in r_by_sym[sym]:
                items.append((sym, r))
        items.sort(key=lambda x: float(x[1]['realized_pnl'] or 0))
        net_extra = sum(float(r['realized_pnl'] or 0) for _, r in items)
        for sym, r in items:
            print(f"    {fmt_dt(r['unix_open']):<12}  {sym:<14}  {r['side']:<5}  "
                  f"entry={float(r['entry_price']):.6f}  hold={r['holding_hours']}h  "
                  f"notes={short_notes(r.get('notes')):<25}  pnl={float(r['realized_pnl'] or 0):+.2f}")
        print(f"\n  REMOTE 多开 {len(items)} 笔, 净 PnL = {net_extra:+.2f}U")
        print(f"  (这部分是 REMOTE 比 LOCAL 多扣的 PnL, 占总差距的主要原因)")

    if only_l:
        print(f"\n{'='*100}")
        print(f"  [仅 LOCAL 开的 symbol — REMOTE 没触发]")
        print(f"{'='*100}")
        items = []
        for sym in only_l:
            for r in l_by_sym[sym]:
                items.append((sym, r))
        items.sort(key=lambda x: float(x[1]['realized_pnl'] or 0))
        net_extra = sum(float(r['realized_pnl'] or 0) for _, r in items)
        for sym, r in items:
            print(f"    {fmt_dt(r['unix_open']):<12}  {sym:<14}  {r['side']:<5}  "
                  f"entry={float(r['entry_price']):.6f}  hold={r['holding_hours']}h  "
                  f"notes={short_notes(r.get('notes')):<25}  pnl={float(r['realized_pnl'] or 0):+.2f}")
        print(f"\n  LOCAL 多开 {len(items)} 笔, 净 PnL = {net_extra:+.2f}U")


if __name__ == '__main__':
    main()
