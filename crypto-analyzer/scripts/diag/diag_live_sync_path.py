"""
定位"真钱亏损"的实际数据路径:
1. 今天 paper 单里 live_sync_status 分布 (是否同步到了币安)
2. 今天 paper 单里 live_position_id 是否有填充
3. strategy_trade_records / strategy_state 里今天的活动
4. config.yaml 里实盘开关/API 配置的提示
只读.
"""
import sys
from datetime import date
from pathlib import Path
import pymysql

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DB = dict(host='13.212.252.171', port=3306, user='admin', password='Yintao@110',
          db='dimesion', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

TARGET = sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat()


def main():
    conn = pymysql.connect(**DB)
    cur = conn.cursor()
    try:
        # 1. paper orders 里今天的 live_sync_status 分布
        print("=" * 90)
        print(f"[1] futures_orders 今天 live_sync_status 分布 ({TARGET})")
        print("=" * 90)
        cur.execute(
            """SELECT live_sync_status, COUNT(*) as n
               FROM futures_orders
               WHERE DATE(created_at) = %s
               GROUP BY live_sync_status""",
            (TARGET,),
        )
        rows = cur.fetchall()
        for r in rows:
            print(f"  live_sync_status={r['live_sync_status']!r}: {r['n']} 条")
        print()

        # 1b. SYNCED 的具体订单
        cur.execute(
            """SELECT id, symbol, side, order_type, status, live_sync_status,
                      live_position_id, live_synced_at, avg_fill_price, realized_pnl,
                      order_source, fill_time
               FROM futures_orders
               WHERE DATE(created_at) = %s AND live_sync_status IS NOT NULL
               ORDER BY id ASC""",
            (TARGET,),
        )
        synced = cur.fetchall()
        if synced:
            print(f"  -- 今天有 live_sync_status 填充的 {len(synced)} 条订单 --")
            for o in synced:
                print(f"    #{o['id']} {o['symbol']:<14} {o['side']:<12} {o['order_type']:<20} "
                      f"status={o['status']:<10} sync={o['live_sync_status']:<10} "
                      f"live_pos={o['live_position_id']} "
                      f"fill={o['avg_fill_price']} pnl={o['realized_pnl']}")
                print(f"        synced_at={o['live_synced_at']} src={o['order_source']}")
        print()

        # 2. paper positions 里今天的 live_position_id 分布
        print("=" * 90)
        print(f"[2] futures_positions 今天 live_position_id 分布")
        print("=" * 90)
        cur.execute(
            """SELECT id, symbol, position_side, live_position_id, status,
                      realized_pnl, open_time, close_time, notes
               FROM futures_positions
               WHERE DATE(open_time) = %s OR DATE(close_time) = %s
               ORDER BY id ASC""",
            (TARGET, TARGET),
        )
        poss = cur.fetchall()
        with_live = [p for p in poss if p['live_position_id'] is not None]
        print(f"  今天 paper 仓位共 {len(poss)} 个, 其中 live_position_id 有值的 {len(with_live)} 个\n")
        for p in with_live:
            print(f"    paper#{p['id']} {p['symbol']:<14} {p['position_side']:<5} "
                  f"live#{p['live_position_id']} pnl={p['realized_pnl']} "
                  f"status={p['status']} notes={p['notes']}")
        print()

        # 3. 今天 strategy_trade_records 有没有活动
        print("=" * 90)
        print(f"[3] strategy_trade_records 今天活动")
        print("=" * 90)
        cur.execute("SHOW COLUMNS FROM strategy_trade_records")
        cols = [c['Field'] for c in cur.fetchall()]
        print(f"  表字段: {cols}")
        date_col = None
        for c in ['created_at', 'trade_time', 'open_time', 'record_time', 'timestamp']:
            if c in cols:
                date_col = c
                break
        if date_col:
            cur.execute(
                f"SELECT COUNT(*) AS n FROM strategy_trade_records WHERE DATE({date_col}) = %s",
                (TARGET,),
            )
            print(f"  按 {date_col} 统计今天记录: {cur.fetchone()['n']} 条")
        print()

        # 4. strategy_state 今天状态
        print("=" * 90)
        print(f"[4] strategy_state 当前状态 (env=live)")
        print("=" * 90)
        cur.execute("SHOW COLUMNS FROM strategy_state")
        sscols = [c['Field'] for c in cur.fetchall()]
        print(f"  表字段: {sscols}")
        cur.execute(
            """SELECT * FROM strategy_state
               WHERE env='live' AND state IN ('LONG','SHORT','PENDING','PENDING_LONG','PENDING_SHORT')
               ORDER BY updated_at DESC LIMIT 50"""
        )
        live_active = cur.fetchall()
        print(f"  env=live 且 state 非 IDLE/DONE 的记录: {len(live_active)} 条")
        for s in live_active[:10]:
            print(f"    {s}")
        print()

        # 5. config.yaml 实盘相关设置
        print("=" * 90)
        print("[5] config.yaml 实盘 & 币安相关行 (grep)")
        print("=" * 90)
        cfg = Path(__file__).resolve().parents[2] / 'config.yaml'
        if cfg.exists():
            text = cfg.read_text(encoding='utf-8', errors='replace')
            lines = text.splitlines()
            for i, line in enumerate(lines, 1):
                low = line.lower()
                if any(k in low for k in ['binance', 'live', 'real', 'api_key', '实盘', 'enabled']):
                    # 过滤掉纯 enabled 数据 (太多噪声)
                    if low.strip().startswith('enabled:') and i > 1 and 'binance' not in lines[i-2].lower():
                        continue
                    print(f"  L{i}: {line.rstrip()}")
    finally:
        conn.close()


if __name__ == '__main__':
    main()
