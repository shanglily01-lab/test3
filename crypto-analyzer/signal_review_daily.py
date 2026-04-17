"""
信号每日复盘脚本
分析每个信号组件的开仓次数、胜率、平均盈亏、总贡献。
用法:
  python signal_review_daily.py              # 昨天
  python signal_review_daily.py --days 7    # 最近7天
  python signal_review_daily.py --date 2026-04-11  # 指定日期
  python signal_review_daily.py --save      # 同时写入 signal_performance_daily 表
"""
import sys
import os
import json
import argparse
from datetime import datetime, timedelta, date
from collections import defaultdict

import pymysql
from dotenv import load_dotenv

load_dotenv()

DB_CFG = dict(
    host=os.getenv('DB_HOST', 'localhost'),
    port=int(os.getenv('DB_PORT', 3306)),
    user=os.getenv('DB_USER', 'root'),
    password=os.getenv('DB_PASSWORD', ''),
    db=os.getenv('DB_NAME', 'binance-data'),
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor,
)

SIGNAL_LABELS = {
    # 基础信号
    'position_low': '价格位置低', 'position_high': '价格位置高', 'position_mid': '价格中位',
    'trend_1h_bull': '1H趋势多', 'trend_1h_bear': '1H趋势空',
    'consecutive_bull': '连续阳线', 'consecutive_bear': '连续阴线',
    'volume_power_bull': '量能多', 'volume_power_bear': '量能空',
    'volume_power_1h_bull': '1H量能多', 'volume_power_1h_bear': '1H量能空',
    'momentum_up_3pct': '动量上涨', 'momentum_down_3pct': '动量下跌',
    # 中高级信号
    'rsi_level_bull': 'RSI超卖', 'rsi_level_bear': 'RSI超买',
    'rsi_divergence_bull': 'RSI底背离', 'rsi_divergence_bear': 'RSI顶背离',
    'macd_cross_bull': 'MACD金叉', 'macd_cross_bear': 'MACD死叉',
    'bb_below_lower': 'BB下轨突破', 'bb_near_lower': 'BB近下轨',
    'bb_above_upper': 'BB上轨突破', 'bb_near_upper': 'BB近上轨',
    'kdj_bull': 'KDJ超卖', 'kdj_bear': 'KDJ超买',
    'taker_buy_bull': 'Taker买盘', 'taker_buy_bear': 'Taker卖盘',
    'whale_flow_long': '鲸鱼多头', 'whale_flow_short': '鲸鱼空头',
    'funding_rate_extreme_long': '资金费极端多', 'funding_rate_extreme_short': '资金费极端空',
    'volume_climax_bull': '量价高潮多', 'volume_climax_bear': '量价高潮空',
    'mf_confluence_bull': 'MF共振多', 'mf_confluence_bear': 'MF共振空',
    'vol_strength_bull': '成交量不对称多', 'vol_strength_bear': '成交量不对称空',
    'rsi_mtf_strong_bull': 'RSI多周期强多', 'rsi_mtf_strong_bear': 'RSI多周期强空',
    'rsi_mtf_bull': 'RSI多周期多', 'rsi_mtf_bear': 'RSI多周期空',
    'rs_very_strong': '相对强多', 'rs_very_weak': '相对弱空',
    'ema_triple_bull': 'EMA三线多', 'ema_triple_bear': 'EMA三线空',
    'vol_4h_bull': '4H量多', 'vol_4h_bear': '4H量空',
    'momentum_accel_bull': '动量加速多', 'momentum_accel_bear': '动量加速空',
    'candle_quality_bull': 'K线质量多', 'candle_quality_bear': 'K线质量空',
    'adx_strong_bull': 'ADX强趋多', 'adx_strong_bear': 'ADX强趋空',
    'vol_diverge_bull': '量价背离多', 'vol_diverge_bear': '量价背离空',
    'candle_reversal_bull': 'K线反转多', 'candle_reversal_bear': 'K线反转空',
    'engulfing_bull': '吞噬多', 'engulfing_bear': '吞噬空',
    'higher_lows': '高低点抬升', 'lower_highs': '高低点下移',
    'kdj_j_mtf_bull': 'KDJ_J多周期多', 'kdj_j_mtf_bear': 'KDJ_J多周期空',
    'macd_hist_align_bull': 'MACD多周期多', 'macd_hist_align_bear': 'MACD多周期空',
    'range_breakout_bull': '区间突破多', 'range_breakout_bear': '区间突破空',
    'stoch_rsi_bull': 'StochRSI超卖', 'stoch_rsi_bear': 'StochRSI超买',
    'mtf_candle_bull': '多周期K线共振多', 'mtf_candle_bear': '多周期K线共振空',
    'vwap_bull': 'VWAP偏低多', 'vwap_bear': 'VWAP偏高空',
    'micro_trend_bull': '微观趋势多', 'micro_trend_bear': '微观趋势空',
    'close_chain_bull': '连续收盘多', 'close_chain_bear': '连续收盘空',
    'oi_surge_bull': 'OI暴增多', 'oi_surge_bear': 'OI暴增空', 'oi_drop_reversal': 'OI暴减反转',
    'bb_squeeze_bull': 'BB压缩释放多', 'bb_squeeze_bear': 'BB压缩释放空',
    'ema_dist_bull': 'EMA拉伸回归多', 'ema_dist_bear': 'EMA拉伸回归空',
    'order_flow_bull': '订单流突刺多', 'order_flow_bear': '订单流突刺空',
    'funding_trend_bull': '资金费趋势多', 'funding_trend_bear': '资金费趋势空',
}


def get_conn():
    return pymysql.connect(**DB_CFG)


def fetch_closed_positions(conn, start_dt: datetime, end_dt: datetime):
    """读取区间内的已平仓位"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT symbol, position_side, realized_pnl, signal_components,
                   entry_score, open_time, close_time
            FROM futures_positions
            WHERE status = 'closed'
              AND close_time >= %s AND close_time < %s
              AND account_id = 2
            ORDER BY close_time
        """, (start_dt, end_dt))
        return cur.fetchall()


def analyze_signals(positions):
    """统计每个信号的参与次数和盈亏"""
    stats = defaultdict(lambda: {
        'count': 0, 'wins': 0, 'losses': 0, 'total_pnl': 0.0,
        'win_pnl': 0.0, 'loss_pnl': 0.0
    })

    total_trades = len(positions)
    total_pnl = sum(float(p['realized_pnl'] or 0) for p in positions)

    for pos in positions:
        pnl = float(pos['realized_pnl'] or 0)
        is_win = pnl >= 0
        raw = pos.get('signal_components')
        if not raw:
            continue
        try:
            if isinstance(raw, str):
                components = json.loads(raw)
            else:
                components = raw
        except Exception:
            continue

        for sig_name in components:
            s = stats[sig_name]
            s['count'] += 1
            s['total_pnl'] += pnl
            if is_win:
                s['wins'] += 1
                s['win_pnl'] += pnl
            else:
                s['losses'] += 1
                s['loss_pnl'] += pnl

    return stats, total_trades, total_pnl


def save_to_db(conn, report_date: date, stats: dict):
    """将分析结果写入 signal_performance_daily 表（自动建表）"""
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS signal_performance_daily (
                id INT AUTO_INCREMENT PRIMARY KEY,
                report_date DATE NOT NULL,
                signal_component VARCHAR(64) NOT NULL,
                trade_count INT DEFAULT 0,
                win_count INT DEFAULT 0,
                loss_count INT DEFAULT 0,
                win_rate DECIMAL(5,2) DEFAULT 0,
                total_pnl DECIMAL(12,2) DEFAULT 0,
                avg_pnl DECIMAL(10,2) DEFAULT 0,
                avg_win_pnl DECIMAL(10,2) DEFAULT 0,
                avg_loss_pnl DECIMAL(10,2) DEFAULT 0,
                created_at DATETIME DEFAULT NOW(),
                UNIQUE KEY uq_date_signal (report_date, signal_component)
            )
        """)
        for sig, s in stats.items():
            count = s['count']
            if count == 0:
                continue
            win_rate = round(s['wins'] / count * 100, 2)
            avg_pnl = round(s['total_pnl'] / count, 2)
            avg_win = round(s['win_pnl'] / s['wins'], 2) if s['wins'] else 0
            avg_loss = round(s['loss_pnl'] / s['losses'], 2) if s['losses'] else 0
            cur.execute("""
                INSERT INTO signal_performance_daily
                  (report_date, signal_component, trade_count, win_count, loss_count,
                   win_rate, total_pnl, avg_pnl, avg_win_pnl, avg_loss_pnl)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                  trade_count=VALUES(trade_count), win_count=VALUES(win_count),
                  loss_count=VALUES(loss_count), win_rate=VALUES(win_rate),
                  total_pnl=VALUES(total_pnl), avg_pnl=VALUES(avg_pnl),
                  avg_win_pnl=VALUES(avg_win_pnl), avg_loss_pnl=VALUES(avg_loss_pnl)
            """, (report_date, sig, count, s['wins'], s['losses'],
                  win_rate, round(s['total_pnl'], 2), avg_pnl, avg_win, avg_loss))
    conn.commit()


def print_report(stats: dict, total_trades: int, total_pnl: float,
                 start_dt: datetime, end_dt: datetime):
    """打印复盘报告"""
    print()
    print("=" * 80)
    print(f"  信号每日复盘报告  {start_dt.strftime('%Y-%m-%d %H:%M')} ~ {end_dt.strftime('%Y-%m-%d %H:%M')}")
    print("=" * 80)
    print(f"  总交易: {total_trades} 笔 | 总盈亏: {total_pnl:+.2f}U")
    print()

    if not stats:
        print("  无数据")
        return

    # 按总贡献绝对值排序
    sorted_signals = sorted(stats.items(), key=lambda x: abs(x[1]['total_pnl']), reverse=True)

    header = f"{'信号':30s}  {'次数':>5}  {'胜率':>7}  {'总盈亏':>9}  {'平均':>7}  {'赢均':>7}  {'亏均':>7}"
    print(header)
    print("-" * 80)

    pos_signals = [(sig, s) for sig, s in sorted_signals if s['total_pnl'] >= 0]
    neg_signals = [(sig, s) for sig, s in sorted_signals if s['total_pnl'] < 0]

    def print_row(sig, s):
        count = s['count']
        if count == 0:
            return
        win_rate = s['wins'] / count * 100
        avg_pnl = s['total_pnl'] / count
        avg_win = s['win_pnl'] / s['wins'] if s['wins'] else 0
        avg_loss = s['loss_pnl'] / s['losses'] if s['losses'] else 0
        label = SIGNAL_LABELS.get(sig, sig)
        pnl_str = f"{s['total_pnl']:+.2f}U"
        print(f"  {label:28s}  {count:5d}  {win_rate:6.1f}%  {pnl_str:>9}  {avg_pnl:+6.2f}  {avg_win:+6.2f}  {avg_loss:+6.2f}")

    if pos_signals:
        print("  [盈利贡献信号]")
        for sig, s in pos_signals:
            print_row(sig, s)

    if neg_signals:
        print()
        print("  [亏损贡献信号]")
        for sig, s in neg_signals:
            print_row(sig, s)

    print("=" * 80)

    # 建议：胜率低于40%且亏损大的信号
    problem_signals = [
        (sig, s) for sig, s in stats.items()
        if s['count'] >= 3 and s['wins'] / s['count'] < 0.40 and s['total_pnl'] < -10
    ]
    if problem_signals:
        print()
        print("  [风险信号警告] 胜率<40% 且累计亏损>10U:")
        for sig, s in sorted(problem_signals, key=lambda x: x[1]['total_pnl']):
            wr = s['wins'] / s['count'] * 100
            label = SIGNAL_LABELS.get(sig, sig)
            print(f"    {label:30s} 胜率{wr:.1f}%  盈亏{s['total_pnl']:+.2f}U  ({s['count']}次)")
        print()
        print("  建议: 对上述信号执行 UPDATE signal_scoring_weights SET is_active=0 WHERE signal_component='<name>'")
    print()


def main():
    parser = argparse.ArgumentParser(description='信号每日复盘')
    parser.add_argument('--days', type=int, default=1, help='分析最近N天（默认1=昨天）')
    parser.add_argument('--date', type=str, help='指定日期 YYYY-MM-DD')
    parser.add_argument('--save', action='store_true', help='写入 signal_performance_daily 表')
    args = parser.parse_args()

    if args.date:
        target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        start_dt = datetime(target_date.year, target_date.month, target_date.day)
        end_dt = start_dt + timedelta(days=1)
    else:
        now = datetime.now()
        end_dt = datetime(now.year, now.month, now.day)  # 今天0点
        start_dt = end_dt - timedelta(days=args.days)

    conn = get_conn()
    try:
        positions = fetch_closed_positions(conn, start_dt, end_dt)
        stats, total_trades, total_pnl = analyze_signals(positions)
        print_report(stats, total_trades, total_pnl, start_dt, end_dt)

        if args.save:
            save_to_db(conn, start_dt.date(), stats)
            print(f"  已写入 signal_performance_daily 表 ({start_dt.date()})")
    finally:
        conn.close()


if __name__ == '__main__':
    main()
