"""
扫描远程 dimesion 库里所有可能记录"信号 skip / 守卫拦截"的表.
table_schemas.txt 是 2026-04-23 生成的快照, 之后新加的表 (如 order_trigger_events)
不在文档里, 必须实际去 SHOW TABLES 看.
只读.
"""
import sys
import pymysql

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DB = dict(host='54.179.112.251', port=3306, user='admin', password='Yintao@110',
          db='dimesion', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

# 关键词命中视为"可能"相关
KEYWORDS = ('signal', 'event', 'trigger', 'skip', 'reject', 'guard', 'filter',
            'funnel', 'block', 'gate', 'audit', 'log', 'history')


def main():
    conn = pymysql.connect(**DB); cur = conn.cursor()
    try:
        cur.execute("SHOW TABLES")
        tables = [list(r.values())[0] for r in cur.fetchall()]
        print(f">>> 远程 dimesion 共 {len(tables)} 张表\n")

        # 1) 找名字命中关键词的表
        print("=== 名字命中可疑关键词的表 ===\n")
        hits = [t for t in tables if any(k in t.lower() for k in KEYWORDS)]
        for t in hits:
            cur.execute(f"SELECT COUNT(*) AS n FROM `{t}`")
            n = cur.fetchone()['n']
            cur.execute(f"DESCRIBE `{t}`")
            cols = [r['Field'] for r in cur.fetchall()]
            print(f"  {t:<40} rows={n}")
            print(f"     cols: {', '.join(cols[:15])}{'...' if len(cols) > 15 else ''}")

        # 2) 单独看 order_trigger_events (04-25/26 新加)
        print("\n=== order_trigger_events 详查 ===\n")
        if 'order_trigger_events' in tables:
            cur.execute("DESCRIBE order_trigger_events")
            print("  schema:")
            for r in cur.fetchall():
                print(f"    {r['Field']:<25} {r['Type']:<25} {r.get('Comment','')}")
            cur.execute("SELECT COUNT(*) AS n FROM order_trigger_events")
            print(f"\n  total rows: {cur.fetchone()['n']}")
            # 最近 10 条
            cur.execute("""SELECT * FROM order_trigger_events
                           ORDER BY id DESC LIMIT 5""")
            print("\n  最近 5 条:")
            for r in cur.fetchall():
                print(f"    {dict(r)}")

            # 04-27 (UTC+8) 全天 = UTC0 04-26 16:00 ~ 04-27 16:00
            print("\n  UTC+8 04-27 各 event_type 计数:")
            cur.execute("""SELECT event_type, COUNT(*) AS n
                           FROM order_trigger_events
                           WHERE event_time >= '2026-04-26 16:00:00'
                             AND event_time <  '2026-04-27 16:00:00'
                           GROUP BY event_type ORDER BY n DESC""")
            for r in cur.fetchall():
                print(f"    {r['event_type']:<30} n={r['n']}")

            # 04-27 按 (event_type, detail 前缀) 分组, 看是哪些守卫在拦
            print("\n  UTC+8 04-27 按 event_type+detail 分组 (top 30):")
            cur.execute("""SELECT event_type,
                                  SUBSTRING_INDEX(detail, ' ', 3) AS detail_head,
                                  COUNT(*) AS n
                           FROM order_trigger_events
                           WHERE event_time >= '2026-04-26 16:00:00'
                             AND event_time <  '2026-04-27 16:00:00'
                           GROUP BY event_type, detail_head
                           ORDER BY n DESC LIMIT 30""")
            for r in cur.fetchall():
                print(f"    [{r['event_type']:<22}] n={r['n']:<3} detail={r['detail_head']}")

            # 04-27 涉及多少不同 order_id (即多少不同信号)
            print("\n  UTC+8 04-27 不同 order_id 数 (= 不同信号实例):")
            cur.execute("""SELECT COUNT(DISTINCT order_id) AS n_orders
                           FROM order_trigger_events
                           WHERE event_time >= '2026-04-26 16:00:00'
                             AND event_time <  '2026-04-27 16:00:00'""")
            print(f"    n_orders = {cur.fetchone()['n_orders']}")

            # 04-27 每个 order_id 经历了什么 event 序列
            print("\n  UTC+8 04-27 各 order_id 的 event 序列 (前 15 个):")
            cur.execute("""SELECT order_id,
                                  GROUP_CONCAT(event_type ORDER BY event_time SEPARATOR ' -> ') AS seq,
                                  COUNT(*) AS n_evt,
                                  MIN(event_time) AS first_t,
                                  MAX(event_time) AS last_t
                           FROM order_trigger_events
                           WHERE event_time >= '2026-04-26 16:00:00'
                             AND event_time <  '2026-04-27 16:00:00'
                           GROUP BY order_id
                           ORDER BY first_t DESC LIMIT 15""")
            for r in cur.fetchall():
                print(f"    {r['order_id']}  ({r['n_evt']} evt, {r['first_t']} ~ {r['last_t']})")
                print(f"      {r['seq']}")
        else:
            print("  [!] 表不存在")
    finally:
        conn.close()


if __name__ == '__main__':
    main()
