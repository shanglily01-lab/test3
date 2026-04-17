#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
分析最近24小时的信号盈亏情况
为超级大脑自我优化提供数据支持
"""

import pymysql
import sys
import io
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
from collections import defaultdict

# Fix Windows console encoding
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

load_dotenv()

conn = pymysql.connect(
    host=os.getenv('DB_HOST'),
    port=int(os.getenv('DB_PORT', 3306)),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    database=os.getenv('DB_NAME'),
    cursorclass=pymysql.cursors.DictCursor,
    charset='utf8mb4'
)

cursor = conn.cursor()

print("=" * 100)
print("最近24小时信号盈亏分析")
print("=" * 100)
print()

# 计算24小时前的时间（UTC）
now_utc = datetime.now()
time_24h_ago = now_utc - timedelta(hours=24)

print(f"分析时间范围: {time_24h_ago.strftime('%Y-%m-%d %H:%M:%S')} UTC ~ {now_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC")
print(f"                (北京时间 {(time_24h_ago + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')} ~ {(now_utc + timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')})")
print()

# 查询最近24小时的交易
cursor.execute('''
    SELECT
        p.symbol,
        p.position_side,
        p.entry_signal_type,
        p.entry_score,
        p.signal_components,
        p.realized_pnl,
        p.entry_price,
        p.mark_price,
        p.unrealized_pnl_pct,
        p.status,
        p.created_at
    FROM futures_positions p
    WHERE p.created_at >= %s
    ORDER BY p.created_at DESC
''', (time_24h_ago,))

positions = cursor.fetchall()

print(f"### 总体统计")
print(f"总交易数: {len(positions)}")
print()

# 按信号类型分组统计
signal_stats = defaultdict(lambda: {
    'count': 0,
    'closed_count': 0,
    'open_count': 0,
    'total_pnl': 0,
    'win_count': 0,
    'loss_count': 0,
    'avg_score': 0,
    'scores': []
})

for pos in positions:
    signal_type = pos['entry_signal_type'] or 'UNKNOWN'
    side = pos['position_side']
    status = pos['status']
    score = pos['entry_score'] or 0

    signal_key = f"{signal_type}_{side}"

    signal_stats[signal_key]['count'] += 1
    signal_stats[signal_key]['scores'].append(score)

    if status == 'closed':
        signal_stats[signal_key]['closed_count'] += 1
        pnl = float(pos['realized_pnl']) if pos['realized_pnl'] else 0
        signal_stats[signal_key]['total_pnl'] += pnl

        if pnl > 0:
            signal_stats[signal_key]['win_count'] += 1
        elif pnl < 0:
            signal_stats[signal_key]['loss_count'] += 1
    else:
        signal_stats[signal_key]['open_count'] += 1

# 计算平均评分
for key in signal_stats:
    scores = signal_stats[key]['scores']
    signal_stats[key]['avg_score'] = sum(scores) / len(scores) if scores else 0

# 按盈亏排序
sorted_signals = sorted(signal_stats.items(), key=lambda x: x[1]['total_pnl'])

print("### 信号类型盈亏排行")
print(f"{'信号类型':<40} {'方向':<8} {'总数':<6} {'已平':<6} {'盈利':<6} {'亏损':<6} {'胜率':<8} {'总盈亏':<12} {'平均评分':<10}")
print("-" * 100)

best_signals = []
worst_signals = []

for signal_key, stats in sorted_signals:
    parts = signal_key.rsplit('_', 1)
    if len(parts) == 2:
        signal_type, side = parts
    else:
        signal_type = signal_key
        side = 'N/A'

    count = stats['count']
    closed = stats['closed_count']
    wins = stats['win_count']
    losses = stats['loss_count']
    pnl = stats['total_pnl']
    avg_score = stats['avg_score']

    win_rate = (wins / closed * 100) if closed > 0 else 0

    print(f"{signal_type:<40} {side:<8} {count:<6} {closed:<6} {wins:<6} {losses:<6} {win_rate:<7.1f}% ${pnl:<11.2f} {avg_score:<10.1f}")

    # 记录最好和最差的信号
    if closed >= 20:  # 至少20笔已平仓才有统计意义（原3笔太少，容易误伤）
        if pnl < -100:
            worst_signals.append((signal_type, side, pnl, win_rate, count))
        elif pnl > 50:
            best_signals.append((signal_type, side, pnl, win_rate, count))

print()
print("### 问题信号（需要优化/禁用）")
print()

if worst_signals:
    print(f"{'信号类型':<40} {'方向':<8} {'总盈亏':<12} {'胜率':<8} {'交易数':<8}")
    print("-" * 80)
    for signal_type, side, pnl, win_rate, count in worst_signals:
        print(f"{signal_type:<40} {side:<8} ${pnl:<11.2f} {win_rate:<7.1f}% {count:<8}")
else:
    print("✓ 没有严重亏损的信号（亏损 < -$50）")

print()
print("### 优秀信号（值得保留/加强）")
print()

if best_signals:
    print(f"{'信号类型':<40} {'方向':<8} {'总盈亏':<12} {'胜率':<8} {'交易数':<8}")
    print("-" * 80)
    for signal_type, side, pnl, win_rate, count in best_signals:
        print(f"{signal_type:<40} {side:<8} ${pnl:<11.2f} {win_rate:<7.1f}% {count:<8}")
else:
    print("✗ 没有表现优异的信号（盈利 > +$50）")

print()
print("### 超级大脑自我优化建议")
print()

# 生成优化建议
optimization_actions = []

for signal_type, side, pnl, win_rate, count in worst_signals:
    if win_rate < 25:  # 原30%太宽松，改为25%（需要更严重才封禁）
        action = f"禁用 {signal_type} {side} 信号（胜率{win_rate:.1f}% < 25%，亏损${pnl:.2f}）"
        optimization_actions.append({
            'action': 'BLACKLIST_SIGNAL',
            'signal_type': signal_type,
            'side': side,
            'order_count': count,
            'reason': f"24H胜率{win_rate:.1f}%,亏损${pnl:.2f}"
        })
    elif win_rate < 40:
        action = f"提高 {signal_type} {side} 信号阈值（胜率{win_rate:.1f}% < 40%，亏损${pnl:.2f}）"
        optimization_actions.append({
            'action': 'RAISE_THRESHOLD',
            'signal_type': signal_type,
            'side': side,
            'current_avg_score': signal_stats[f"{signal_type}_{side}"]['avg_score'],
            'reason': f"24H胜率{win_rate:.1f}%,亏损${pnl:.2f}"
        })
    else:
        action = f"观察 {signal_type} {side} 信号（胜率{win_rate:.1f}%，亏损${pnl:.2f}，可能是市场环境问题）"

    print(f"  • {action}")

if not optimization_actions:
    print("  ✓ 当前所有信号表现正常，无需调整")

print()
print("=" * 100)

# 保存优化建议
if optimization_actions:
    import json
    with open('optimization_actions.json', 'w', encoding='utf-8') as f:
        json.dump({
            'timestamp': now_utc.isoformat(),
            'analysis_period': '24h',
            'actions': optimization_actions
        }, f, indent=2, ensure_ascii=False)

    print()
    print(f"优化建议已保存到: optimization_actions.json")

cursor.close()
conn.close()
