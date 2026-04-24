"""
反验 RAVE/CHIP/UB/BSB/SPK 这 5 个重灾户, 每一笔 SL 单的入场时机在 15M 图上是什么位置.

对每笔仓位:
  - 入场前 12 根 15M bar (3 小时): 看趋势方向、局部高低点
  - 入场后到平仓的所有 15M bar: 看策略预判对不对
  - 量化指标: 入场价在"前 12 bar 高低区间"里的百分位 (100=顶, 0=底)
              入场前 3 bar 涨幅 (近端动量)
              入场后 1 bar / 3 bar 价格变化 (第一反应)

得出结论: 这些 SL 单入场时是"追高"(高位做多) / "抄底"(低位做多) / "摸顶"(高位做空) / "踩底"(低位做空)
"""
import sys
from datetime import date, timedelta, datetime
import pymysql

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DB = dict(host='13.212.252.171', port=3306, user='admin', password='Yintao@110',
          db='dimesion', charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)

TOP5 = ['RAVE/USDT', 'CHIP/USDT', 'UB/USDT', 'BSB/USDT', 'SPK/USDT']
LOOKBACK_BARS = 12   # 入场前 12 根 15M = 3 小时
BAR_MS = 15 * 60 * 1000


def main():
    conn = pymysql.connect(**DB); cur = conn.cursor()
    try:
        end = date.today(); start = end - timedelta(days=6)
        # 拉这 5 个币的所有 SL 仓位
        placeholders = ','.join(['%s'] * len(TOP5))
        cur.execute(
            f"""SELECT id, symbol, position_side, entry_price, realized_pnl,
                       open_time, close_time, notes,
                       TIMESTAMPDIFF(MINUTE, open_time, close_time) as hold_min
                FROM futures_positions
                WHERE symbol IN ({placeholders})
                  AND DATE(close_time) BETWEEN %s AND %s
                  AND status IN ('closed','liquidated')
                  AND realized_pnl < 0
                  AND (notes LIKE '%%early-sl%%' OR notes LIKE '%%止损%%' OR notes LIKE '%%breakeven%%')
                ORDER BY symbol, open_time ASC""",
            tuple(TOP5) + (start.isoformat(), end.isoformat()),
        )
        positions = cur.fetchall()
        print(f"\n### {start} ~ {end}  重灾 5 币 SL 单 共 {len(positions)} 笔 ###\n")

        by_sym = {}
        for p in positions:
            by_sym.setdefault(p['symbol'], []).append(p)

        summary_stats = []   # 收集每笔的入场位置百分位, 最后做总表

        for sym in TOP5:
            lst = by_sym.get(sym, [])
            if not lst:
                continue
            print("=" * 100)
            print(f"[{sym}]  {len(lst)} 笔 SL")
            print("=" * 100)

            for p in lst:
                pid = p['id']
                side = p['position_side']
                entry = float(p['entry_price'])
                pnl = float(p['realized_pnl'] or 0)
                ot = p['open_time']
                ct = p['close_time']

                ot_ms = int(ot.timestamp() * 1000)
                ct_ms = int(ct.timestamp() * 1000)
                lookback_start_ms = ot_ms - LOOKBACK_BARS * BAR_MS

                # 拉 15M kline: 入场前 LOOKBACK_BARS 根 + 持仓期间
                cur.execute(
                    """SELECT timestamp, open_price, high_price, low_price, close_price, volume
                       FROM kline_data
                       WHERE symbol=%s AND timeframe='15m'
                         AND open_time BETWEEN %s AND %s
                       ORDER BY open_time ASC""",
                    (sym, lookback_start_ms, ct_ms + BAR_MS * 2),
                )
                bars = cur.fetchall()
                if not bars:
                    print(f"  #{pid} {side} entry={entry} @ {ot}  [NO KLINE DATA]\n")
                    continue

                # 切出入场前 / 持仓期间
                pre = [b for b in bars if int(b['timestamp'].timestamp()*1000) < ot_ms]
                during = [b for b in bars if ot_ms <= int(b['timestamp'].timestamp()*1000) <= ct_ms]
                post = [b for b in bars if int(b['timestamp'].timestamp()*1000) > ct_ms][:2]

                # 计算入场位置百分位: entry 在 pre 的 (min_low, max_high) 区间的百分位
                if pre:
                    pre_hi = max(float(b['high_price']) for b in pre)
                    pre_lo = min(float(b['low_price']) for b in pre)
                    if pre_hi > pre_lo:
                        pct = (entry - pre_lo) / (pre_hi - pre_lo) * 100
                    else:
                        pct = 50
                else:
                    pct = None; pre_hi = pre_lo = 0

                # 近端动量: 最近 3 bar 收盘涨幅
                if len(pre) >= 4:
                    recent = pre[-3:]
                    anchor = float(pre[-4]['close_price'])
                    recent_mom = (float(recent[-1]['close_price']) - anchor) / anchor * 100
                else:
                    recent_mom = None

                # 入场后 1 / 3 bar 方向
                def bar_move(n):
                    if len(during) < n: return None
                    return (float(during[n-1]['close_price']) - entry) / entry * 100

                mv1 = bar_move(1); mv3 = bar_move(3)

                # 趋势标签
                if pct is None:
                    ptag = '?'
                elif pct >= 80:
                    ptag = '近高' if side == 'LONG' else '近顶'
                elif pct <= 20:
                    ptag = '近底' if side == 'SHORT' else '近低'
                elif 40 <= pct <= 60:
                    ptag = '中部'
                elif pct > 60:
                    ptag = '偏高'
                else:
                    ptag = '偏低'

                # 判断入场方向是否"逆近端动量"
                if recent_mom is not None:
                    if side == 'LONG' and recent_mom > 2:
                        align = '追涨'
                    elif side == 'LONG' and recent_mom < -2:
                        align = '抄底'
                    elif side == 'SHORT' and recent_mom < -2:
                        align = '追跌'
                    elif side == 'SHORT' and recent_mom > 2:
                        align = '摸顶'
                    else:
                        align = '平缓入'
                else:
                    align = '?'

                # 简易 ASCII 图: pre(12) + E + during(up to 12) + 平仓 C
                def bar_char(b):
                    o = float(b['open_price']); c = float(b['close_price'])
                    return '↑' if c > o else ('↓' if c < o else '·')
                shape_pre = ''.join(bar_char(b) for b in pre[-LOOKBACK_BARS:])
                shape_during = ''.join(bar_char(b) for b in during[:LOOKBACK_BARS])

                print(f"  #{pid} {side:<5} entry={entry:<10}  pnl={pnl:+.1f}  hold={p['hold_min']}min  notes={p['notes']}")
                print(f"      pre 3h range: [{pre_lo:.6g} ~ {pre_hi:.6g}]  入场位 {pct:.0f}%  ({ptag})")
                print(f"      近端 3bar 动量: {recent_mom:+.2f}%  -> 入场定性: {align}")
                if mv1 is not None:
                    print(f"      入场后 1bar 变动: {mv1:+.2f}%  "
                          f"{'(逆行)' if (side=='LONG' and mv1<0) or (side=='SHORT' and mv1>0) else '(顺行)'}", end='')
                    if mv3 is not None:
                        print(f"  / 3bar: {mv3:+.2f}%")
                    else:
                        print()
                print(f"      15M 形态: 前[{shape_pre}] E [{shape_during}] C   (↑阳 ↓阴 ·平)")
                print(f"      open={ot}  close={ct}")
                print()

                summary_stats.append({
                    'sym': sym, 'side': side, 'pnl': pnl, 'pct': pct,
                    'ptag': ptag, 'mom': recent_mom, 'align': align,
                    'mv1': mv1, 'hold': p['hold_min'],
                })

        # 汇总表: 每个币 + 每种入场定性的分布
        print("=" * 100)
        print("[汇总: 5 币 SL 单的入场位置画像]")
        print("=" * 100)
        print(f"  共 {len(summary_stats)} 笔\n")

        # 按 ptag 分组
        by_ptag = {}
        for s in summary_stats:
            by_ptag.setdefault((s['ptag'], s['side']), []).append(s)
        print(f"  {'入场位置':<10}{'方向':<6}{'笔数':>5}{'累计亏':>10}{'均亏':>9}")
        for (ptag, side), lst in sorted(by_ptag.items(), key=lambda x: -len(x[1])):
            pnl = sum(r['pnl'] for r in lst)
            print(f"  {ptag:<10}{side:<6}{len(lst):>5}{pnl:>+10.1f}{pnl/len(lst):>+9.1f}")

        # 按 align 分组
        print()
        by_align = {}
        for s in summary_stats:
            by_align.setdefault(s['align'], []).append(s)
        print(f"  {'入场定性':<10}{'笔数':>5}{'累计亏':>10}{'均亏':>9}  说明")
        expl = {
            '追涨': 'LONG + 近端上涨 -> 买在冲顶', '追跌': 'SHORT + 近端下跌 -> 卖在破底',
            '抄底': 'LONG + 近端下跌 -> 逆势接刀', '摸顶': 'SHORT + 近端上涨 -> 逆势砸顶',
            '平缓入': '近端无明显动量', '?': '数据不足',
        }
        for align, lst in sorted(by_align.items(), key=lambda x: -len(x[1])):
            pnl = sum(r['pnl'] for r in lst)
            print(f"  {align:<10}{len(lst):>5}{pnl:>+10.1f}{pnl/len(lst):>+9.1f}  {expl.get(align, '')}")
    finally:
        conn.close()


if __name__ == '__main__':
    main()
