#!/usr/bin/env python3
"""
复盘合约(24H) API
Futures Trading Review API

提供24小时模拟合约交易复盘数据：
- 统计摘要
- 成交订单列表
- 取消订单分析
- 开仓/平仓原因分析
- 策略优化建议
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from fastapi import APIRouter, HTTPException, Query
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from loguru import logger
import pymysql

# 创建 Router
router = APIRouter(prefix='/api/futures/review', tags=['futures-review'])

# 加载配置
from app.utils.config_loader import load_config
from app.services.signal_analysis_service import SignalAnalysisService

config = load_config()
db_config = config['database']['mysql']

# 平仓原因中英文映射（基于数据库实际存储格式）
CLOSE_REASON_MAP = {
    'hard_stop_loss': '硬止损',
    'trailing_stop_loss': '移动止损',
    'max_take_profit': '最大止盈',
    'trailing_take_profit': '移动止盈',
    'ema_diff_narrowing_tp': 'EMA差值收窄止盈',
    'death_cross_reversal': '死叉反转平仓',
    'golden_cross_reversal': '金叉反转平仓',
    '5m_death_cross_sl': '5分钟死叉止损',
    '5m_golden_cross_sl': '5分钟金叉止损',
    'ema_direction_reversal_tp': 'EMA方向反转止盈',
    'manual': '手动平仓',
    'manual_close_all': '一键平仓',
    'liquidation': '强制平仓',
    'sync_close': '同步平仓',
    'reversal_warning': '反转预警平仓',
    # 超级大脑新增
    'reverse_signal': '反向信号平仓',
}

# 开仓原因中英文映射（基于 entry_signal_type 字段）
ENTRY_REASON_MAP = {
    'golden_cross': '金叉信号',
    'death_cross': '死叉信号',
    'sustained_trend': '持续趋势',
    'sustained_trend_FORWARD': '顺向持续趋势',
    'sustained_trend_REVERSE': '反转持续趋势',
    'sustained_trend_entry': '趋势入场',
    'ema_trend': 'EMA趋势',
    'limit_order': '限价单',
    'limit_order_trend': '趋势限价单',
    'manual': '手动开仓',
    # 超级大脑决策信号
    'SMART_BRAIN_20': '超级大脑(20分)',
    'SMART_BRAIN_35': '超级大脑(35分)',
    'SMART_BRAIN_40': '超级大脑(40分)',
    'SMART_BRAIN_45': '超级大脑(45分)',
    'SMART_BRAIN_60': '超级大脑(60分)',
}

# 取消原因中英文映射
CANCEL_REASON_MAP = {
    'timeout': '超时取消',
    'validation_failed': '自检未通过',
    'trend_reversal': '趋势转向',
    'ema_direction_changed': 'EMA方向变化',
    'price_invalid': '价格无效',
    'manual': '手动取消',
    'trend_end': '趋势结束',
    'min_ema_diff': 'EMA差值不足',
    'rsi_filter': 'RSI过滤',
    'ema_diff_small': 'EMA差值过小',
    'position_exists': '持仓已存在',
    'execution_failed': '执行失败',
}


def get_db_connection():
    """获取数据库连接"""
    return pymysql.connect(**db_config, autocommit=True)


def parse_close_reason(notes: str) -> tuple:
    """
    解析平仓原因，返回 (代码, 中文名称)

    数据库中的格式示例：
    - "死叉反转(EMA9 > EMA26)"
    - "金叉反转(EMA9 < EMA26)"
    - "manual_close_all"
    - "硬止损"
    - "移动止盈"
    - "5M EMA死叉止损(...)"
    - "移动止盈(距离2.00%，回撤0.79% >= 0.3%)"
    """
    if not notes:
        return 'unknown', '未知'

    notes_lower = notes.lower()

    # 超级大脑智能顶底识别 (优先处理)
    if notes.startswith('TOP_DETECTED('):
        # 提取参数: TOP_DETECTED(高点回落1.4%,盈利-0.4%)
        import re
        match = re.match(r'TOP_DETECTED\((.*?)\)$', notes)
        if match:
            params = match.group(1)
            return 'top_detected', f'智能顶部识别({params})'
        return 'top_detected', '智能顶部识别'

    if notes.startswith('BOTTOM_DETECTED('):
        # 提取参数: BOTTOM_DETECTED(低点反弹1.8%,盈利+1.1%)
        import re
        match = re.match(r'BOTTOM_DETECTED\((.*?)\)$', notes)
        if match:
            params = match.group(1)
            return 'bottom_detected', f'智能底部识别({params})'
        return 'bottom_detected', '智能底部识别'

    # 超级大脑止损止盈（大写格式）
    if notes == 'STOP_LOSS':
        return 'stop_loss', '止损'
    if notes == 'TAKE_PROFIT':
        return 'take_profit', '固定止盈'

    # 超时平仓格式: TIMEOUT_4H(持仓5小时)
    if notes.startswith('TIMEOUT_4H('):
        import re
        match = re.match(r'TIMEOUT_4H\((.*?)\)$', notes)
        if match:
            params = match.group(1)
            return 'timeout_4h', f'超时平仓({params})'
        return 'timeout_4h', '超时平仓'

    # 英文代码直接匹配
    if notes in CLOSE_REASON_MAP:
        return notes, CLOSE_REASON_MAP[notes]

    # 特殊处理一键平仓
    if 'manual_close_all' in notes:
        return 'manual_close_all', '一键平仓'

    # 中文关键字匹配 (按优先级从高到低)
    if '死叉反转' in notes:
        return 'death_cross_reversal', '死叉反转平仓'
    if '金叉反转' in notes:
        return 'golden_cross_reversal', '金叉反转平仓'
    if '硬止损' in notes:
        return 'hard_stop_loss', '硬止损'
    if '移动止损' in notes:
        return 'trailing_stop_loss', '移动止损'
    if '移动止盈' in notes:
        return 'trailing_take_profit', '移动止盈'
    if '最大止盈' in notes or '达到最大' in notes:
        return 'max_take_profit', '最大止盈'
    # 简单的止盈止损 (必须放在具体类型之后匹配)
    if notes == '止盈' or '止盈' in notes:
        return 'take_profit', '止盈'
    if notes == '止损' or '止损' in notes:
        return 'stop_loss', '止损'
    if '5M' in notes and ('死叉' in notes or '金叉' in notes):
        if '死叉' in notes:
            return '5m_death_cross_sl', '5分钟死叉止损'
        else:
            return '5m_golden_cross_sl', '5分钟金叉止损'
    if 'EMA' in notes and '收窄' in notes:
        return 'ema_diff_narrowing_tp', 'EMA差值收窄止盈'
    if '手动' in notes:
        return 'manual', '手动平仓'
    if '强平' in notes or '强制' in notes:
        return 'liquidation', '强制平仓'
    if '同步' in notes:
        return 'sync_close', '同步平仓'

    # 反向信号平仓
    if '|reverse_signal' in notes or 'reverse_signal' in notes:
        return 'reverse_signal', '反向信号平仓'

    # 无法识别，返回原始值（截取前20字符）
    display = notes[:20] + '...' if len(notes) > 20 else notes
    return 'other', display


def parse_entry_reason(entry_reason: str, entry_signal_type: str) -> tuple:
    """
    解析开仓原因，返回 (代码, 中文名称)

    优先使用 entry_signal_type 字段，如果为空则解析 entry_reason
    """
    # 优先使用 entry_signal_type (跳过 "unknown" 字符串)
    if entry_signal_type and entry_signal_type.strip().lower() != 'unknown':
        signal_type = entry_signal_type.strip()

        # 处理震荡市信号 (RANGE_range_trading, RANGE_unknown 等)
        if signal_type.startswith('RANGE_'):
            range_subtype = signal_type.replace('RANGE_', '')
            if range_subtype == 'range_trading':
                return 'range_trading', '震荡市策略'
            elif range_subtype == 'unknown':
                return 'range_unknown', '震荡市策略(旧格式)'
            else:
                return signal_type, f'震荡市-{range_subtype}'

        # 处理反转信号
        if signal_type.startswith('REVERSAL_'):
            if 'TOP_DETECTED' in signal_type:
                return 'reversal_top', '顶部反转做空'
            elif 'BOTTOM_DETECTED' in signal_type:
                return 'reversal_bottom', '底部反转做多'
            else:
                return 'reversal', '反转信号'

        # 处理投资建议信号 (RECOMMENDATION_强烈买入, RECOMMENDATION_买入 等)
        if signal_type.startswith('RECOMMENDATION_'):
            rec_type = signal_type.replace('RECOMMENDATION_', '')
            rec_map = {
                '强烈买入': '强烈看多',
                '买入': '看多',
                '强烈卖出': '强烈看空',
                '卖出': '看空',
                '持有': '观望'
            }
            display_name = rec_map.get(rec_type, rec_type)
            return signal_type, f'投资建议({display_name})'

        # 信号名称映射(用于单信号和组合)
        signal_map = {
            'position_low': '低位',
            'position_mid': '中位',
            'position_high': '高位',
            'trend_1h_bull': '1H看涨',
            'trend_1h_bear': '1H看跌',
            'trend_1d_bull': '1D看涨',
            'trend_1d_bear': '1D看跌',
            'momentum_up_3pct': '涨势3%',
            'momentum_down_3pct': '跌势3%',
            'consecutive_bull': '连阳',
            'consecutive_bear': '连阴',
            'volatility_high': '高波动'
        }

        # 处理新的信号组合格式 (例如: "position_low + trend_1d_bull + trend_1h_bull")
        if ' + ' in signal_type:
            # 信号组合 - 转换为中文
            signals = signal_type.split(' + ')
            chinese_signals = [signal_map.get(s.strip(), s.strip()) for s in signals]
            combo_name = '+'.join(chinese_signals)
            return signal_type, f'信号组合({combo_name})'

        # 处理单个信号(如 "position_high")
        if signal_type in signal_map:
            return signal_type, f'单一信号({signal_map[signal_type]})'

        # 直接匹配
        if signal_type in ENTRY_REASON_MAP:
            return signal_type, ENTRY_REASON_MAP[signal_type]

        # 超级大脑信号类型匹配 (支持整数和浮点数格式) - 兼容旧格式
        if 'SMART_BRAIN_' in signal_type:
            import re
            # 提取分数 (支持 SMART_BRAIN_30 和 SMART_BRAIN_30.0 格式)
            match = re.search(r'SMART_BRAIN[_-]?(\d+(?:\.\d+)?)', signal_type)
            if match:
                score = float(match.group(1))
                score_int = int(score)
                return f'SMART_BRAIN_{score_int}', f'超级大脑({score_int}分-旧格式)'

        # 包含匹配
        if 'sustained_trend' in signal_type:
            if 'FORWARD' in signal_type:
                return 'sustained_trend_FORWARD', '顺向持续趋势'
            elif 'REVERSE' in signal_type:
                return 'sustained_trend_REVERSE', '反转持续趋势'
            else:
                return 'sustained_trend', '持续趋势'
        if 'golden_cross' in signal_type.lower():
            return 'golden_cross', '金叉信号'
        if 'death_cross' in signal_type.lower():
            return 'death_cross', '死叉信号'

    # 解析 entry_reason
    if entry_reason:
        reason = entry_reason.strip()

        # 震荡市策略
        if '[震荡市]' in reason or '震荡市' in reason:
            # 提取关键信息
            if '布林带下轨' in reason and 'RSI超卖' in reason:
                return 'range_bollinger_lower_rsi', '震荡市-布林下轨+RSI超卖'
            elif '布林带上轨' in reason and 'RSI超买' in reason:
                return 'range_bollinger_upper_rsi', '震荡市-布林上轨+RSI超买'
            elif '布林带下轨' in reason:
                return 'range_bollinger_lower', '震荡市-布林下轨反弹'
            elif '布林带上轨' in reason:
                return 'range_bollinger_upper', '震荡市-布林上轨反弹'
            else:
                return 'range_trading', '震荡市策略'

        if '金叉' in reason:
            return 'golden_cross', '金叉信号'
        if '死叉' in reason:
            return 'death_cross', '死叉信号'
        if 'sustained' in reason.lower() or '持续' in reason:
            if 'FORWARD' in reason or '顺向' in reason:
                return 'sustained_trend_FORWARD', '顺向持续趋势'
            elif 'REVERSE' in reason or '反转' in reason:
                return 'sustained_trend_REVERSE', '反转持续趋势'
            return 'sustained_trend', '持续趋势'
        if '预测器' in reason or 'confidence=' in reason:
            import re
            m = re.search(r'confidence=(\d+)', reason)
            conf = m.group(1) if m else ''
            return 'predictor', f'预测神器({conf}分)' if conf else '预测神器'
        if reason.startswith('BTC ') and ('分钟' in reason or '%' in reason):
            return 'btc_momentum', f'BTC动量跟随({reason})'
        if '15M突破' in reason or '15M_BREAKOUT' in reason:
            return '15m_breakout', f'15M破位({reason})'
        if '手动' in reason or 'manual' in reason.lower():
            return 'manual', '手动开仓'
        if '限价' in reason or 'limit' in reason.lower():
            return 'limit_order', '限价单'

    return 'unknown', '未知'


def parse_cancel_reason(reason: str, notes: str = None) -> tuple:
    """
    解析取消原因，返回 (代码, 中文名称)

    会结合 notes 字段来提取详细的取消原因
    """
    if not reason:
        return 'unknown', '未知'

    reason_lower = reason.lower()

    # 从 notes 中提取详细原因
    detail = ''
    if notes:
        # notes 格式: " VALIDATION_FAILED: 趋势末端(差值缩小36.3%); 弱趋势(EMA差值0.048%<0.05%)"
        # 或: " TREND_REVERSAL: 死叉(做多): EMA9=5.7346 < EMA26=5.7347, 差值=0.00%"
        if 'VALIDATION_FAILED:' in notes:
            detail = notes.split('VALIDATION_FAILED:')[-1].strip()
        elif 'TREND_REVERSAL:' in notes:
            detail = notes.split('TREND_REVERSAL:')[-1].strip()
        elif 'RSI_FILTER:' in notes:
            detail = notes.split('RSI_FILTER:')[-1].strip()
        elif 'EMA_DIFF_SMALL:' in notes:
            detail = notes.split('EMA_DIFF_SMALL:')[-1].strip()
        elif 'TIMEOUT' in notes:
            detail = '超时'

    # 直接匹配英文代码
    if reason == 'validation_failed':
        # 解析详细原因
        if detail:
            # 尝试从英文关键词解析
            reasons = []
            if 'EMA' in detail:
                if '<' in detail or '>' in detail:
                    reasons.append('EMA方向不符')
                if '%<' in detail or '差值' in detail or 'diff' in detail.lower():
                    reasons.append('EMA差值过小')
            if '缩小' in detail or 'shrink' in detail.lower() or '末端' in detail:
                reasons.append('趋势末端')
            if '弱' in detail or 'weak' in detail.lower():
                reasons.append('弱趋势')
            if reasons:
                return 'validation_failed', '自检: ' + '+'.join(reasons)
        return 'validation_failed', '自检未通过'

    if reason == 'trend_reversal':
        if detail:
            if 'EMA9' in detail:
                # 解析EMA数值
                import re
                match = re.search(r'EMA9[=:]?\s*([\d.]+).*EMA26[=:]?\s*([\d.]+)', detail)
                if match:
                    ema9 = float(match.group(1))
                    ema26 = float(match.group(2))
                    if ema9 < ema26:
                        return 'trend_reversal', '死叉反转'
                    else:
                        return 'trend_reversal', '金叉反转'
        return 'trend_reversal', '趋势转向'

    if reason == 'timeout':
        return 'timeout', '超时取消'

    if reason == 'rsi_filter':
        if detail and 'RSI' in detail:
            import re
            match = re.search(r'RSI[=:]?\s*([\d.]+)', detail)
            if match:
                rsi = float(match.group(1))
                if rsi > 60:
                    return 'rsi_filter', f'RSI超买({rsi:.0f})'
                elif rsi < 40:
                    return 'rsi_filter', f'RSI超卖({rsi:.0f})'
        return 'rsi_filter', 'RSI过滤'

    if reason == 'ema_diff_small':
        return 'ema_diff_small', 'EMA差值过小'

    if reason == 'position_exists':
        return 'position_exists', '持仓已存在'

    if reason == 'execution_failed':
        return 'execution_failed', '执行失败'

    if reason == 'manual':
        return 'manual', '手动取消'

    # 关键字匹配（兼容旧数据）
    if 'timeout' in reason_lower:
        return 'timeout', '超时取消'
    if 'validation' in reason_lower or '自检' in reason:
        return 'validation_failed', '自检未通过'
    if 'reversal' in reason_lower or '转向' in reason:
        return 'trend_reversal', '趋势转向'
    if 'rsi' in reason_lower:
        return 'rsi_filter', 'RSI过滤'
    if 'ema_diff' in reason_lower or 'EMA差值' in reason:
        return 'ema_diff_small', 'EMA差值过小'
    if 'position' in reason_lower or '持仓' in reason:
        return 'position_exists', '持仓已存在'
    if 'manual' in reason_lower or '手动' in reason:
        return 'manual', '手动取消'

    # 无法识别
    display = reason[:20] + '...' if len(reason) > 20 else reason
    return 'other', display


@router.get("/summary")
async def get_review_summary(
    hours: int = Query(default=24, ge=1, le=168, description="统计时间范围（小时）"),
    account_id: int = Query(default=2, description="账户ID")
):
    """
    获取24H交易统计摘要

    返回:
    - 总订单数、成交数、取消数、成功率
    - 已实现盈亏、未实现盈亏、手续费
    - 盈利单数、亏损单数、胜率、平均盈亏比
    - 最大单笔盈利、最大单笔亏损
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        time_threshold = datetime.now() - timedelta(hours=hours)

        # 订单统计
        cursor.execute("""
            SELECT
                COUNT(*) as total_orders,
                SUM(CASE WHEN status = 'FILLED' THEN 1 ELSE 0 END) as filled_orders,
                SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END) as cancelled_orders,
                SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END) as pending_orders,
                SUM(fee) as total_fee
            FROM futures_orders
            WHERE account_id = %s AND created_at >= %s
        """, (account_id, time_threshold))
        order_stats = cursor.fetchone()

        # 持仓统计（已平仓的）
        cursor.execute("""
            SELECT
                COUNT(*) as total_positions,
                SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as winning_trades,
                SUM(CASE WHEN realized_pnl < 0 THEN 1 ELSE 0 END) as losing_trades,
                SUM(CASE WHEN realized_pnl = 0 THEN 1 ELSE 0 END) as break_even_trades,
                SUM(realized_pnl) as total_realized_pnl,
                AVG(CASE WHEN realized_pnl > 0 THEN realized_pnl ELSE NULL END) as avg_profit,
                AVG(CASE WHEN realized_pnl < 0 THEN realized_pnl ELSE NULL END) as avg_loss,
                MAX(realized_pnl) as max_profit,
                MIN(realized_pnl) as max_loss,
                AVG(TIMESTAMPDIFF(MINUTE, open_time, close_time)) as avg_holding_minutes
            FROM futures_positions
            WHERE account_id = %s AND status = 'CLOSED' AND close_time >= %s
        """, (account_id, time_threshold))
        position_stats = cursor.fetchone()

        # 当前未平仓持仓的未实现盈亏
        cursor.execute("""
            SELECT SUM(unrealized_pnl) as total_unrealized_pnl
            FROM futures_positions
            WHERE account_id = %s AND status = 'OPEN'
        """, (account_id,))
        unrealized = cursor.fetchone()

        # 按交易对统计胜负
        cursor.execute("""
            SELECT
                symbol,
                COUNT(*) as total_trades,
                SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN realized_pnl < 0 THEN 1 ELSE 0 END) as losses,
                SUM(CASE WHEN realized_pnl = 0 THEN 1 ELSE 0 END) as break_even,
                SUM(realized_pnl) as total_pnl,
                AVG(CASE WHEN realized_pnl > 0 THEN realized_pnl ELSE NULL END) as avg_win,
                AVG(CASE WHEN realized_pnl < 0 THEN realized_pnl ELSE NULL END) as avg_loss,
                MAX(realized_pnl) as max_win,
                MIN(realized_pnl) as max_loss
            FROM futures_positions
            WHERE account_id = %s AND status = 'CLOSED' AND close_time >= %s
            GROUP BY symbol
            ORDER BY total_pnl DESC
        """, (account_id, time_threshold))
        symbol_stats = cursor.fetchall()

        cursor.close()
        conn.close()

        # 计算胜率和盈亏比
        total_closed = position_stats['total_positions'] or 0
        winning = position_stats['winning_trades'] or 0
        losing = position_stats['losing_trades'] or 0
        win_rate = (winning / total_closed * 100) if total_closed > 0 else 0

        avg_profit = float(position_stats['avg_profit'] or 0)
        avg_loss = abs(float(position_stats['avg_loss'] or 1))
        profit_loss_ratio = avg_profit / avg_loss if avg_loss > 0 else 0

        total_orders = order_stats['total_orders'] or 0
        filled_orders = order_stats['filled_orders'] or 0
        success_rate = (filled_orders / total_orders * 100) if total_orders > 0 else 0

        # 处理交易对统计数据
        symbol_performance = []
        for row in symbol_stats:
            total = row['total_trades'] or 0
            wins = row['wins'] or 0
            losses = row['losses'] or 0
            win_rate_sym = (wins / total * 100) if total > 0 else 0

            symbol_performance.append({
                "symbol": row['symbol'],
                "total_trades": total,
                "wins": wins,
                "losses": losses,
                "break_even": row['break_even'] or 0,
                "win_rate": round(win_rate_sym, 1),
                "total_pnl": round(float(row['total_pnl'] or 0), 2),
                "avg_win": round(float(row['avg_win'] or 0), 2),
                "avg_loss": round(float(row['avg_loss'] or 0), 2),
                "max_win": round(float(row['max_win'] or 0), 2),
                "max_loss": round(float(row['max_loss'] or 0), 2)
            })

        return {
            "success": True,
            "data": {
                "time_range_hours": hours,
                "order_overview": {
                    "total_orders": total_orders,
                    "filled_orders": filled_orders,
                    "cancelled_orders": order_stats['cancelled_orders'] or 0,
                    "pending_orders": order_stats['pending_orders'] or 0,
                    "success_rate": round(success_rate, 1)
                },
                "pnl_summary": {
                    "realized_pnl": float(position_stats['total_realized_pnl'] or 0),
                    "unrealized_pnl": float(unrealized['total_unrealized_pnl'] or 0),
                    "total_fee": float(order_stats['total_fee'] or 0)
                },
                "win_loss_analysis": {
                    "total_closed_positions": total_closed,
                    "winning_trades": winning,
                    "losing_trades": losing,
                    "break_even_trades": position_stats['break_even_trades'] or 0,
                    "win_rate": round(win_rate, 1),
                    "avg_profit": round(avg_profit, 2),
                    "avg_loss": round(-abs(float(position_stats['avg_loss'] or 0)), 2),
                    "profit_loss_ratio": round(profit_loss_ratio, 2)
                },
                "extremes": {
                    "max_profit": float(position_stats['max_profit'] or 0),
                    "max_loss": float(position_stats['max_loss'] or 0)
                },
                "avg_holding_minutes": round(float(position_stats['avg_holding_minutes'] or 0), 1),
                "symbol_performance": symbol_performance
            }
        }

    except Exception as e:
        logger.error(f"获取复盘摘要失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trades")
async def get_review_trades(
    hours: int = Query(default=24, ge=1, le=168, description="统计时间范围（小时）"),
    account_id: int = Query(default=2, description="账户ID"),
    filter_type: str = Query(default="all", description="筛选类型: all/profit/loss"),
    sort_by: str = Query(default="time", description="排序方式: time/pnl"),
    page: int = Query(default=1, ge=1, description="页码"),
    page_size: int = Query(default=100, ge=10, le=200, description="每页数量")
):
    """
    获取24H成交订单列表（分页）

    包含: 时间、交易对、方向、开仓价、平仓价、数量、杠杆、盈亏、盈亏%、持仓时长、开仓原因、平仓原因
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        time_threshold = datetime.now() - timedelta(hours=hours)

        # 构建筛选条件
        filter_condition = ""
        if filter_type == "profit":
            filter_condition = "AND realized_pnl > 0"
        elif filter_type == "loss":
            filter_condition = "AND realized_pnl < 0"

        # 构建排序
        order_by = "close_time DESC" if sort_by == "time" else "realized_pnl DESC"

        # 获取总数
        cursor.execute(f"""
            SELECT COUNT(*) as total
            FROM futures_positions
            WHERE account_id = %s AND status = 'CLOSED' AND close_time >= %s
            {filter_condition}
        """, (account_id, time_threshold))
        total_count = cursor.fetchone()['total']

        # 分页查询
        offset = (page - 1) * page_size
        cursor.execute(f"""
            SELECT
                id, symbol, position_side, leverage,
                quantity, entry_price, mark_price as close_price,
                realized_pnl, margin,
                holding_hours, entry_reason, notes as close_reason,
                open_time, close_time, entry_signal_type, status
            FROM futures_positions
            WHERE account_id = %s AND status = 'CLOSED' AND close_time >= %s
            {filter_condition}
            ORDER BY {order_by}
            LIMIT %s OFFSET %s
        """, (account_id, time_threshold, page_size, offset))

        positions = cursor.fetchall()

        cursor.close()
        conn.close()

        # 处理数据，添加中文映射
        trades = []
        for pos in positions:
            # 使用新的解析函数
            close_reason_code, close_reason_cn = parse_close_reason(pos['close_reason'])
            entry_reason_code, entry_reason_cn = parse_entry_reason(
                pos['entry_reason'],
                pos['entry_signal_type']
            )

            # 计算实际持仓时长（分钟）
            holding_minutes = 0
            if pos['open_time'] and pos['close_time']:
                delta = pos['close_time'] - pos['open_time']
                holding_minutes = int(delta.total_seconds() / 60)

            # 计算ROI (realized_pnl / margin * 100%)
            realized_pnl = float(pos['realized_pnl'] or 0)
            margin = float(pos['margin'] or 0)
            pnl_pct = (realized_pnl / margin * 100) if margin > 0 else 0

            trades.append({
                "id": pos['id'],
                "symbol": pos['symbol'],
                "position_side": pos['position_side'],
                "position_side_cn": "做多" if pos['position_side'] == 'LONG' else "做空",
                "leverage": pos['leverage'],
                "quantity": float(pos['quantity']),
                "entry_price": float(pos['entry_price']),
                "close_price": float(pos['close_price']) if pos['close_price'] else None,
                "realized_pnl": realized_pnl,
                "pnl_pct": pnl_pct,
                "holding_minutes": holding_minutes,
                "entry_reason_code": entry_reason_code,
                "entry_reason_cn": entry_reason_cn,
                "close_reason_code": close_reason_code,
                "close_reason_cn": close_reason_cn,
                "close_reason_detail": pos['close_reason'],
                "open_time": pos['open_time'].isoformat() if pos['open_time'] else None,
                "close_time": pos['close_time'].isoformat() if pos['close_time'] else None
            })

        # 计算分页信息
        total_pages = (total_count + page_size - 1) // page_size

        return {
            "success": True,
            "data": {
                "trades": trades,
                "count": len(trades),
                "total_count": total_count,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "filter": filter_type,
                "sort_by": sort_by
            }
        }

    except Exception as e:
        logger.error(f"获取成交列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cancelled")
async def get_cancelled_orders(
    hours: int = Query(default=24, ge=1, le=168, description="统计时间范围（小时）"),
    account_id: int = Query(default=2, description="账户ID"),
    page: int = Query(default=1, ge=1, description="页码"),
    page_size: int = Query(default=100, ge=10, le=200, description="每页数量")
):
    """
    获取24H取消订单列表及原因分析（分页）

    返回:
    - 取消总数
    - 各取消原因统计
    - 取消订单详情列表（分页）
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        time_threshold = datetime.now() - timedelta(hours=hours)

        # 获取总数
        cursor.execute("""
            SELECT COUNT(*) as total
            FROM futures_orders
            WHERE account_id = %s AND status = 'CANCELLED' AND created_at >= %s
        """, (account_id, time_threshold))
        total_cancelled = cursor.fetchone()['total']

        # 获取所有取消订单用于统计原因分布
        cursor.execute("""
            SELECT cancellation_reason, notes
            FROM futures_orders
            WHERE account_id = %s AND status = 'CANCELLED' AND created_at >= %s
        """, (account_id, time_threshold))
        all_reasons = cursor.fetchall()

        # 统计取消原因分布
        reason_stats = {}
        for row in all_reasons:
            reason_code, reason_cn = parse_cancel_reason(row['cancellation_reason'], row.get('notes'))
            if reason_code not in reason_stats:
                reason_stats[reason_code] = {
                    "code": reason_code,
                    "name_cn": reason_cn,
                    "count": 0
                }
            reason_stats[reason_code]["count"] += 1

        # 分页查询取消订单详情
        offset = (page - 1) * page_size
        cursor.execute("""
            SELECT
                id, order_id, symbol, side, order_type, leverage,
                price, quantity, margin, cancellation_reason, notes,
                created_at, canceled_at
            FROM futures_orders
            WHERE account_id = %s AND status = 'CANCELLED' AND created_at >= %s
            ORDER BY canceled_at DESC
            LIMIT %s OFFSET %s
        """, (account_id, time_threshold, page_size, offset))

        orders = cursor.fetchall()

        cursor.close()
        conn.close()

        # 处理订单列表
        cancelled_list = []
        for order in orders:
            reason_code, reason_cn = parse_cancel_reason(order['cancellation_reason'], order.get('notes'))

            cancelled_list.append({
                "id": order['id'],
                "order_id": order['order_id'],
                "symbol": order['symbol'],
                "side": order['side'],
                "side_cn": "做多" if "LONG" in order['side'] else "做空",
                "order_type": order['order_type'],
                "leverage": order['leverage'],
                "price": float(order['price']) if order['price'] else None,
                "quantity": float(order['quantity']) if order['quantity'] else None,
                "margin": float(order['margin']) if order['margin'] else None,
                "cancel_reason_code": reason_code,
                "cancel_reason_cn": reason_cn,
                "cancel_reason_detail": order['cancellation_reason'],
                "notes": order['notes'],
                "created_at": order['created_at'].isoformat() if order['created_at'] else None,
                "canceled_at": order['canceled_at'].isoformat() if order['canceled_at'] else None
            })

        # 计算占比
        reason_distribution = []
        for code, stats in sorted(reason_stats.items(), key=lambda x: x[1]['count'], reverse=True):
            stats["percentage"] = round(stats["count"] / total_cancelled * 100, 1) if total_cancelled > 0 else 0
            reason_distribution.append(stats)

        # 计算分页信息
        total_pages = (total_cancelled + page_size - 1) // page_size

        return {
            "success": True,
            "data": {
                "total_cancelled": total_cancelled,
                "reason_distribution": reason_distribution,
                "cancelled_orders": cancelled_list,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages
            }
        }

    except Exception as e:
        logger.error(f"获取取消订单分析失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/analysis")
async def get_reason_analysis(
    hours: int = Query(default=24, ge=1, le=168, description="统计时间范围（小时）"),
    account_id: int = Query(default=2, description="账户ID")
):
    """
    获取开仓/平仓原因分析统计

    返回:
    - 开仓信号类型分布及各类型胜率
    - 平仓原因分布及各原因平均盈亏
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        time_threshold = datetime.now() - timedelta(hours=hours)

        # 获取已平仓持仓
        cursor.execute("""
            SELECT
                entry_reason, entry_signal_type, notes as close_reason,
                realized_pnl, position_side
            FROM futures_positions
            WHERE account_id = %s AND status = 'CLOSED' AND close_time >= %s
        """, (account_id, time_threshold))

        positions = cursor.fetchall()

        cursor.close()
        conn.close()

        # 统计开仓原因
        entry_stats = {}
        # 统计平仓原因
        close_stats = {}
        # 统计方向
        direction_stats = {
            'LONG': {'count': 0, 'wins': 0, 'total_pnl': 0},
            'SHORT': {'count': 0, 'wins': 0, 'total_pnl': 0}
        }

        for pos in positions:
            pnl = float(pos['realized_pnl'] or 0)
            is_profit = pnl > 0

            # 开仓原因统计（使用新的解析函数,区分多空方向）
            entry_code, entry_cn = parse_entry_reason(pos['entry_reason'], pos['entry_signal_type'])
            side = pos['position_side']
            side_cn = "做多" if side == 'LONG' else "做空"

            # 创建组合键: code_LONG 或 code_SHORT
            entry_key = f"{entry_code}_{side}"
            entry_display = f"{entry_cn}({side_cn})"

            if entry_key not in entry_stats:
                entry_stats[entry_key] = {
                    "code": entry_key,
                    "name_cn": entry_display,
                    "count": 0,
                    "wins": 0,
                    "losses": 0,
                    "total_pnl": 0
                }
            entry_stats[entry_key]["count"] += 1
            entry_stats[entry_key]["total_pnl"] += pnl
            if is_profit:
                entry_stats[entry_key]["wins"] += 1
            else:
                entry_stats[entry_key]["losses"] += 1

            # 平仓原因统计（使用新的解析函数）
            close_code, close_cn = parse_close_reason(pos['close_reason'])
            if close_code not in close_stats:
                close_stats[close_code] = {
                    "code": close_code,
                    "name_cn": close_cn,
                    "count": 0,
                    "total_pnl": 0,
                    "pnl_list": []
                }
            close_stats[close_code]["count"] += 1
            close_stats[close_code]["total_pnl"] += pnl
            close_stats[close_code]["pnl_list"].append(pnl)

            # 方向统计
            side = pos['position_side']
            if side in direction_stats:
                direction_stats[side]['count'] += 1
                direction_stats[side]['total_pnl'] += pnl
                if is_profit:
                    direction_stats[side]['wins'] += 1

        # 计算开仓原因胜率
        entry_analysis = []
        for code, stats in sorted(entry_stats.items(), key=lambda x: x[1]['count'], reverse=True):
            win_rate = (stats['wins'] / stats['count'] * 100) if stats['count'] > 0 else 0
            entry_analysis.append({
                "code": stats['code'],
                "name_cn": stats['name_cn'],
                "count": stats['count'],
                "wins": stats['wins'],
                "losses": stats['losses'],
                "win_rate": round(win_rate, 1),
                "total_pnl": round(stats['total_pnl'], 2),
                "avg_pnl": round(stats['total_pnl'] / stats['count'], 2) if stats['count'] > 0 else 0
            })

        # 计算平仓原因平均盈亏
        close_analysis = []
        for code, stats in sorted(close_stats.items(), key=lambda x: x[1]['count'], reverse=True):
            avg_pnl = stats['total_pnl'] / stats['count'] if stats['count'] > 0 else 0
            close_analysis.append({
                "code": stats['code'],
                "name_cn": stats['name_cn'],
                "count": stats['count'],
                "total_pnl": round(stats['total_pnl'], 2),
                "avg_pnl": round(avg_pnl, 2)
            })

        # 计算方向胜率
        direction_analysis = []
        for side, stats in direction_stats.items():
            win_rate = (stats['wins'] / stats['count'] * 100) if stats['count'] > 0 else 0
            direction_analysis.append({
                "side": side,
                "side_cn": "做多" if side == 'LONG' else "做空",
                "count": stats['count'],
                "wins": stats['wins'],
                "win_rate": round(win_rate, 1),
                "total_pnl": round(stats['total_pnl'], 2)
            })

        return {
            "success": True,
            "data": {
                "entry_analysis": entry_analysis,
                "close_analysis": close_analysis,
                "direction_analysis": direction_analysis
            }
        }

    except Exception as e:
        logger.error(f"获取原因分析失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/suggestions")
async def get_strategy_suggestions(
    hours: int = Query(default=24, ge=1, le=168, description="统计时间范围（小时）"),
    account_id: int = Query(default=2, description="账户ID")
):
    """
    获取策略优化建议

    基于24H数据自动生成优化建议
    """
    try:
        # 先获取分析数据
        conn = get_db_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        time_threshold = datetime.now() - timedelta(hours=hours)

        # 获取已平仓持仓
        cursor.execute("""
            SELECT
                entry_reason, entry_signal_type, notes as close_reason,
                realized_pnl, unrealized_pnl_pct, position_side,
                stop_loss_pct, take_profit_pct, max_profit_pct
            FROM futures_positions
            WHERE account_id = %s AND status = 'CLOSED' AND close_time >= %s
        """, (account_id, time_threshold))
        positions = cursor.fetchall()

        # 获取取消订单
        cursor.execute("""
            SELECT cancellation_reason, COUNT(*) as count
            FROM futures_orders
            WHERE account_id = %s AND status = 'CANCELLED' AND created_at >= %s
            GROUP BY cancellation_reason
        """, (account_id, time_threshold))
        cancel_stats = cursor.fetchall()

        # 获取总订单数
        cursor.execute("""
            SELECT COUNT(*) as total
            FROM futures_orders
            WHERE account_id = %s AND created_at >= %s
        """, (account_id, time_threshold))
        total_orders = cursor.fetchone()['total']

        cursor.close()
        conn.close()

        suggestions = []

        # 分析止损触发情况
        stop_loss_count = 0
        trailing_stop_count = 0
        max_tp_count = 0
        trailing_tp_count = 0
        cross_reversal_count = 0
        five_m_sl_count = 0

        long_stats = {'count': 0, 'wins': 0}
        short_stats = {'count': 0, 'wins': 0}

        trailing_tp_drawdowns = []

        for pos in positions:
            close_code, _ = parse_close_reason(pos['close_reason'])
            pnl = float(pos['realized_pnl'] or 0)

            if close_code == 'hard_stop_loss':
                stop_loss_count += 1
            elif close_code == 'trailing_stop_loss':
                trailing_stop_count += 1
            elif close_code == 'max_take_profit':
                max_tp_count += 1
            elif close_code == 'trailing_take_profit':
                trailing_tp_count += 1
                # 计算回撤幅度
                max_profit = float(pos['max_profit_pct'] or 0)
                final_pnl_pct = float(pos['unrealized_pnl_pct'] or 0)
                if max_profit > 0:
                    drawdown = max_profit - final_pnl_pct
                    trailing_tp_drawdowns.append(drawdown)
            elif close_code in ['death_cross_reversal', 'golden_cross_reversal']:
                cross_reversal_count += 1
            elif close_code in ['5m_death_cross_sl', '5m_golden_cross_sl']:
                five_m_sl_count += 1

            # 方向统计
            if pos['position_side'] == 'LONG':
                long_stats['count'] += 1
                if pnl > 0:
                    long_stats['wins'] += 1
            else:
                short_stats['count'] += 1
                if pnl > 0:
                    short_stats['wins'] += 1

        total_positions = len(positions)

        # 生成建议

        # 1. 止损建议
        if total_positions > 0 and stop_loss_count / total_positions > 0.3:
            suggestions.append({
                "type": "warning",
                "category": "止损",
                "message": f"硬止损触发过多（{stop_loss_count}次，占比{round(stop_loss_count/total_positions*100)}%），建议适当放宽止损幅度或优化入场时机"
            })

        # 2. 5M止损建议
        if five_m_sl_count > 0:
            suggestions.append({
                "type": "info",
                "category": "5M止损",
                "message": f"5分钟信号止损触发{five_m_sl_count}次，该功能可及时避免更大亏损"
            })

        # 3. 移动止盈回撤建议
        if trailing_tp_drawdowns:
            avg_drawdown = sum(trailing_tp_drawdowns) / len(trailing_tp_drawdowns)
            if avg_drawdown > 1.5:
                suggestions.append({
                    "type": "warning",
                    "category": "移动止盈",
                    "message": f"移动止盈激活后平均回撤{round(avg_drawdown, 1)}%，建议调整回撤阈值"
                })

        # 4. 取消订单建议
        total_cancelled = sum(s['count'] for s in cancel_stats)
        if total_orders > 0 and total_cancelled / total_orders > 0.3:
            # 找出主要取消原因
            main_reason = max(cancel_stats, key=lambda x: x['count']) if cancel_stats else None
            if main_reason:
                _, reason_cn = parse_cancel_reason(main_reason['cancellation_reason'])
                suggestions.append({
                    "type": "warning",
                    "category": "订单取消",
                    "message": f"订单取消率较高（{round(total_cancelled/total_orders*100)}%），主要原因：{reason_cn}（{main_reason['count']}次）"
                })

        # 5. 方向胜率建议
        if long_stats['count'] >= 3 and short_stats['count'] >= 3:
            long_wr = long_stats['wins'] / long_stats['count'] * 100
            short_wr = short_stats['wins'] / short_stats['count'] * 100

            if abs(long_wr - short_wr) > 20:
                if long_wr > short_wr:
                    suggestions.append({
                        "type": "info",
                        "category": "方向分析",
                        "message": f"做多胜率（{round(long_wr)}%）明显高于做空（{round(short_wr)}%），当前市场偏多头"
                    })
                else:
                    suggestions.append({
                        "type": "info",
                        "category": "方向分析",
                        "message": f"做空胜率（{round(short_wr)}%）明显高于做多（{round(long_wr)}%），当前市场偏空头"
                    })

        # 6. 趋势反转平仓建议
        if cross_reversal_count > 0 and total_positions > 0:
            reversal_ratio = cross_reversal_count / total_positions
            if reversal_ratio > 0.2:
                suggestions.append({
                    "type": "info",
                    "category": "趋势反转",
                    "message": f"交叉反转平仓占比{round(reversal_ratio*100)}%（{cross_reversal_count}次），趋势切换频繁"
                })

        # 7. 如果没有足够数据
        if total_positions < 3:
            suggestions.append({
                "type": "info",
                "category": "数据不足",
                "message": f"24小时内仅有{total_positions}笔已平仓交易，建议积累更多数据后再做分析"
            })

        # 8. 胜率建议
        if total_positions >= 5:
            total_wins = long_stats['wins'] + short_stats['wins']
            win_rate = total_wins / total_positions * 100
            if win_rate < 40:
                suggestions.append({
                    "type": "danger",
                    "category": "胜率",
                    "message": f"整体胜率偏低（{round(win_rate)}%），建议优化入场条件或止盈止损策略"
                })
            elif win_rate > 60:
                suggestions.append({
                    "type": "success",
                    "category": "胜率",
                    "message": f"整体胜率良好（{round(win_rate)}%），策略表现稳定"
                })

        return {
            "success": True,
            "data": {
                "suggestions": suggestions,
                "stats_summary": {
                    "total_positions": total_positions,
                    "stop_loss_count": stop_loss_count,
                    "trailing_stop_count": trailing_stop_count,
                    "five_m_sl_count": five_m_sl_count,
                    "max_tp_count": max_tp_count,
                    "trailing_tp_count": trailing_tp_count,
                    "cross_reversal_count": cross_reversal_count,
                    "total_cancelled": total_cancelled,
                    "total_orders": total_orders
                }
            }
        }

    except Exception as e:
        logger.error(f"获取策略建议失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/signal-analysis")
async def get_signal_analysis(
    hours: int = Query(24, description="时间范围(小时)"),
    account_id: int = Query(2, description="账户ID: 1=实盘, 2=模拟")
):
    """
    信号分析API - 获取最新的每日复盘信号分析数据

    返回各个信号类型的详细表现分析，包括:
    - 交易笔数、胜率、平均盈亏
    - 最佳/最差交易
    - 捕获的大行情机会数
    - 做多/做空笔数
    - 平均持仓时长
    - 评分和评级
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        # 查询最新的信号分析数据（从daily_review_signal_analysis表）
        cursor.execute("""
            SELECT
                review_date,
                signal_type,
                total_trades,
                win_trades,
                loss_trades,
                win_rate,
                avg_pnl,
                best_trade,
                worst_trade,
                long_trades,
                short_trades,
                avg_holding_minutes,
                captured_opportunities,
                rating,
                score
            FROM daily_review_signal_analysis
            WHERE review_date >= DATE_SUB(CURDATE(), INTERVAL 1 DAY)
            ORDER BY review_date DESC, score DESC
            LIMIT 50
        """)

        signal_rows = cursor.fetchall()

        # 如果没有每日复盘数据，返回空结果
        if not signal_rows:
            return {
                "success": True,
                "data": {
                    "signal_stats": {},
                    "total_signals": 0,
                    "summary": {
                        "best_signal": None,
                        "worst_signal": None
                    }
                }
            }

        # 组织信号统计数据
        signal_stats = {}
        for row in signal_rows:
            signal_type = row['signal_type']
            signal_stats[signal_type] = {
                'total_trades': row['total_trades'],
                'win_trades': row['win_trades'],
                'loss_trades': row['loss_trades'],
                'win_rate': float(row['win_rate']) if row['win_rate'] else 0,
                'avg_pnl': float(row['avg_pnl']) if row['avg_pnl'] else 0,
                'best_trade': float(row['best_trade']) if row['best_trade'] else 0,
                'worst_trade': float(row['worst_trade']) if row['worst_trade'] else 0,
                'long_trades': row['long_trades'],
                'short_trades': row['short_trades'],
                'avg_holding_minutes': float(row['avg_holding_minutes']) if row['avg_holding_minutes'] else 0,
                'captured_opportunities': row['captured_opportunities'],
                'rating': row['rating'],
                'score': float(row['score']) if row['score'] is not None else 0.0
            }

        # 找出最佳和最差信号（score 可能为 NULL，用 0 兜底）
        best_signal = max(signal_stats.items(), key=lambda x: x[1]['score'] or 0)[0] if signal_stats else None
        worst_signal = min(signal_stats.items(), key=lambda x: x[1]['score'] or 0)[0] if signal_stats else None

        cursor.close()
        conn.close()

        return {
            "success": True,
            "data": {
                "signal_stats": signal_stats,
                "total_signals": len(signal_stats),
                "summary": {
                    "best_signal": best_signal,
                    "worst_signal": worst_signal
                }
            }
        }

    except Exception as e:
        logger.error(f"获取信号分析失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/opportunity-analysis")
async def get_opportunity_analysis(
    hours: int = Query(24, description="时间范围(小时)"),
    account_id: int = Query(2, description="账户ID: 1=实盘, 2=模拟")
):
    """
    机会分析API - 获取最新的每日复盘机会分析数据

    返回不同维度下的交易机会捕获情况，包括:
    - 按时间周期(5m/15m/1h)的捕获统计
    - 错过原因分析
    - 交易对表现排名
    - 总体统计摘要
    - 当前信号评分对比
    - 已捕获和错过的信号分析
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        # 查询最新的机会数据（从daily_review_opportunities表）
        cursor.execute("""
            SELECT
                timeframe,
                move_type,
                captured,
                symbol,
                miss_reason,
                price_change_pct
            FROM daily_review_opportunities
            WHERE review_date >= DATE_SUB(CURDATE(), INTERVAL 1 DAY)
            ORDER BY review_date DESC
            LIMIT 1000
        """)

        opportunity_rows = cursor.fetchall()

        # 如果没有数据，返回空结果
        if not opportunity_rows:
            return {
                "success": True,
                "data": {
                    "timeframe_analysis": {},
                    "miss_reasons": {},
                    "symbol_analysis": {
                        "all_symbols": {},
                        "best_symbols": [],
                        "worst_symbols": []
                    },
                    "summary": {
                        "total_opportunities": 0,
                        "best_timeframe": None,
                        "worst_timeframe": None,
                        "main_miss_reason": None
                    }
                }
            }

        # 1. 按时间周期统计
        timeframe_analysis = {}
        for tf in ['5m', '15m', '1h']:
            tf_opps = [o for o in opportunity_rows if o['timeframe'] == tf]
            captured = [o for o in tf_opps if o['captured']]
            missed = [o for o in tf_opps if not o['captured']]

            pumps = [o for o in tf_opps if o['move_type'] == 'pump']
            dumps = [o for o in tf_opps if o['move_type'] == 'dump']

            captured_pumps = [o for o in pumps if o['captured']]
            captured_dumps = [o for o in dumps if o['captured']]

            timeframe_analysis[tf] = {
                'total_opportunities': len(tf_opps),
                'captured': len(captured),
                'missed': len(missed),
                'capture_rate': len(captured) / len(tf_opps) * 100 if tf_opps else 0,
                'pumps': {
                    'total': len(pumps),
                    'captured': len(captured_pumps),
                    'rate': len(captured_pumps) / len(pumps) * 100 if pumps else 0
                },
                'dumps': {
                    'total': len(dumps),
                    'captured': len(captured_dumps),
                    'rate': len(captured_dumps) / len(dumps) * 100 if dumps else 0
                }
            }

        # 2. 错过原因统计
        miss_reasons = {}
        for opp in opportunity_rows:
            if not opp['captured'] and opp['miss_reason']:
                reason = opp['miss_reason']
                if reason not in miss_reasons:
                    miss_reasons[reason] = {
                        'count': 0,
                        'total_pct_change': 0,
                        'examples': []
                    }
                miss_reasons[reason]['count'] += 1
                miss_reasons[reason]['total_pct_change'] += abs(float(opp['price_change_pct']))

                if len(miss_reasons[reason]['examples']) < 3:
                    miss_reasons[reason]['examples'].append({
                        'symbol': opp['symbol'],
                        'change': abs(float(opp['price_change_pct'])),
                        'type': opp['move_type']
                    })

        # 计算平均错失幅度
        for reason, data in miss_reasons.items():
            data['avg_missed_change'] = data['total_pct_change'] / data['count']

        # 3. 按交易对统计
        symbol_analysis = {}
        for opp in opportunity_rows:
            symbol = opp['symbol']
            if symbol not in symbol_analysis:
                symbol_analysis[symbol] = {
                    'total_opportunities': 0,
                    'captured': 0,
                    'missed': 0
                }

            symbol_analysis[symbol]['total_opportunities'] += 1
            if opp['captured']:
                symbol_analysis[symbol]['captured'] += 1
            else:
                symbol_analysis[symbol]['missed'] += 1

        # 计算捕获率并排序
        for symbol, stats in symbol_analysis.items():
            stats['capture_rate'] = stats['captured'] / stats['total_opportunities'] * 100 if stats['total_opportunities'] > 0 else 0

        sorted_symbols = sorted(symbol_analysis.items(), key=lambda x: x[1]['capture_rate'], reverse=True)

        # 4. 总结
        best_timeframe = max(timeframe_analysis.items(), key=lambda x: x[1]['capture_rate'])[0] if timeframe_analysis else None
        worst_timeframe = min(timeframe_analysis.items(), key=lambda x: x[1]['capture_rate'])[0] if timeframe_analysis else None
        main_miss_reason = max(miss_reasons.items(), key=lambda x: x[1]['count'])[0] if miss_reasons else None

        cursor.close()
        conn.close()

        return {
            "success": True,
            "data": {
                "timeframe_analysis": timeframe_analysis,
                "miss_reasons": miss_reasons,
                "symbol_analysis": {
                    "all_symbols": symbol_analysis,
                    "best_symbols": sorted_symbols[:5],
                    "worst_symbols": sorted_symbols[-5:] if len(sorted_symbols) >= 5 else []
                },
                "summary": {
                    "total_opportunities": len(opportunity_rows),
                    "best_timeframe": best_timeframe,
                    "worst_timeframe": worst_timeframe,
                    "main_miss_reason": main_miss_reason
                }
            }
        }

    except Exception as e:
        logger.error(f"获取买入机会分析失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/kline-signal-analysis")
async def get_kline_signal_analysis(
    hours: int = Query(24, description="时间范围(小时)")
):
    """
    K线信号分析API - 获取最新的K线强度 + 信号捕捉分析
    
    返回数据包括:
    - 总体统计（捕获率、机会数、错过数）
    - Top强力信号（1H/15M/5M K线强度）
    - 错过的高质量机会
    - 历史趋势
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        # 获取最新的分析报告
        cursor.execute("""
            SELECT 
                id,
                analysis_time,
                total_analyzed,
                has_position,
                should_trade,
                missed_opportunities,
                wrong_direction,
                correct_captures,
                capture_rate,
                report_json,
                created_at
            FROM signal_analysis_reports
            ORDER BY analysis_time DESC
            LIMIT 1
        """)

        latest_report = cursor.fetchone()

        if not latest_report:
            return {
                "success": True,
                "data": {
                    "has_data": False,
                    "message": "暂无信号分析数据，请等待首次分析完成"
                }
            }

        # 解析JSON数据
        import json
        report_data = json.loads(latest_report['report_json']) if latest_report.get('report_json') else {}

        # 获取历史趋势（最近7次）
        cursor.execute("""
            SELECT 
                analysis_time,
                total_analyzed,
                should_trade,
                has_position,
                missed_opportunities,
                capture_rate
            FROM signal_analysis_reports
            ORDER BY analysis_time DESC
            LIMIT 7
        """)

        history = cursor.fetchall()

        # 处理Top机会数据
        top_opportunities = report_data.get('top_opportunities', [])[:15]
        
        # 格式化Top机会
        formatted_opportunities = []
        for opp in top_opportunities:
            s1h = opp.get('strength_1h', {})
            s15m = opp.get('strength_15m', {})
            s5m = opp.get('strength_5m', {})
            sig = opp.get('signal_status', {})
            
            # 判断多空倾向
            net_power = s1h.get('net_power', 0)
            bull_pct = s1h.get('bull_pct', 50)
            
            if net_power >= 3:
                trend = '强多'
            elif net_power <= -3:
                trend = '强空'
            elif bull_pct > 55:
                trend = '偏多'
            elif bull_pct < 45:
                trend = '偏空'
            else:
                trend = '震荡'
            
            # 判断捕捉状态
            has_pos = sig.get('has_position', False)
            if has_pos:
                position = sig.get('position', {})
                status = f"已捕捉({position.get('position_side', 'N/A')})"
                status_type = 'captured'
            else:
                status = "错过"
                status_type = 'missed'
            
            formatted_opportunities.append({
                'symbol': opp.get('symbol', 'N/A'),
                'trend': trend,
                'status': status,
                'status_type': status_type,
                'kline_1h': {
                    'bull_pct': s1h.get('bull_pct', 0),
                    'bull': s1h.get('bull', 0),
                    'total': s1h.get('total', 0),
                    'strong_bull': s1h.get('strong_bull', 0),
                    'strong_bear': s1h.get('strong_bear', 0),
                    'net_power': s1h.get('net_power', 0)
                },
                'kline_15m': {
                    'bull_pct': s15m.get('bull_pct', 0),
                    'bull': s15m.get('bull', 0),
                    'total': s15m.get('total', 0),
                    'strong_bull': s15m.get('strong_bull', 0),
                    'strong_bear': s15m.get('strong_bear', 0),
                    'net_power': s15m.get('net_power', 0)
                },
                'kline_5m': {
                    'bull_pct': s5m.get('bull_pct', 0),
                    'bull': s5m.get('bull', 0),
                    'total': s5m.get('total', 0),
                    'strong_bull': s5m.get('strong_bull', 0),
                    'strong_bear': s5m.get('strong_bear', 0),
                    'net_power': s5m.get('net_power', 0)
                }
            })

        # 处理错过机会数据
        missed_opportunities = report_data.get('missed_opportunities', [])[:10]
        
        formatted_missed = []
        for missed in missed_opportunities:
            formatted_missed.append({
                'symbol': missed.get('symbol', 'N/A'),
                'side': missed.get('side', 'N/A'),
                'reason': missed.get('reason', ''),
                'net_power_1h': missed.get('net_power_1h', 0),
                'net_power_15m': missed.get('net_power_15m', 0),
                'net_power_5m': missed.get('net_power_5m', 0),
                'possible_reasons': missed.get('possible_reasons', [])
            })

        # 处理历史趋势
        history_data = []
        for h in history:
            history_data.append({
                'time': h['analysis_time'].strftime('%m-%d %H:%M'),
                'total_analyzed': h['total_analyzed'],
                'should_trade': h['should_trade'],
                'has_position': h['has_position'],
                'missed_opportunities': h['missed_opportunities'],
                'capture_rate': float(h['capture_rate'])
            })

        cursor.close()
        conn.close()

        return {
            "success": True,
            "data": {
                "has_data": True,
                "analysis_time": latest_report['analysis_time'].strftime('%Y-%m-%d %H:%M:%S'),
                "summary": {
                    "total_analyzed": latest_report['total_analyzed'],
                    "has_position": latest_report['has_position'],
                    "should_trade": latest_report['should_trade'],
                    "missed_opportunities": latest_report['missed_opportunities'],
                    "wrong_direction": latest_report['wrong_direction'],
                    "correct_captures": latest_report['correct_captures'],
                    "capture_rate": float(latest_report['capture_rate'])
                },
                "top_opportunities": formatted_opportunities,
                "missed_opportunities": formatted_missed,
                "history": history_data
            }
        }

    except Exception as e:
        logger.error(f"获取K线信号分析失败: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
@router.get("/realtime-opportunity-analysis")
async def get_realtime_opportunity_analysis(
    account_id: int = Query(2, description="账户ID: 1=实盘, 2=模拟")
):
    """
    实时机会分析API - 展示当前信号评分和持仓对比

    返回数据包括:
    - 当前所有交易对的信号评分
    - 已开仓的信号（捕获到的机会）
    - 未开仓的强信号（错过的机会）
    - 错过原因分析（黑名单、评分不够、资金不足等）
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        # 初始化信号分析服务
        signal_service = SignalAnalysisService(db_config)

        # 1. 获取监控列表中的所有交易对（从K线数据表获取最近活跃的交易对）
        cursor.execute("""
            SELECT DISTINCT symbol
            FROM kline_data
            WHERE timeframe = '1h'
            AND timestamp >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
            ORDER BY symbol
        """)
        monitored_symbols = [row['symbol'] for row in cursor.fetchall()]

        if not monitored_symbols:
            return {
                "success": True,
                "data": {
                    "has_data": False,
                    "message": "暂无监控交易对"
                }
            }

        # 2. 获取当前所有持仓
        cursor.execute("""
            SELECT
                symbol,
                position_side,
                margin,
                quantity,
                entry_price,
                unrealized_pnl,
                created_at,
                entry_reason
            FROM futures_positions
            WHERE account_id = %s
            AND status IN ('open', 'building')
        """, (account_id,))

        current_positions = cursor.fetchall()
        position_symbols = {pos['symbol']: pos for pos in current_positions}

        # 3. 获取交易黑名单 (从 trading_symbol_rating)
        cursor.execute("""
            SELECT symbol, level_change_reason, rating_level, margin_multiplier, created_at
            FROM trading_symbol_rating
            WHERE rating_level >= 1
            ORDER BY rating_level DESC
        """)
        blacklist = {
            row['symbol']: {
                'reason': row['level_change_reason'],
                'level': row['rating_level'],
                'margin_multiplier': row['margin_multiplier']
            }
            for row in cursor.fetchall()
        }

        # 4. 获取账户余额
        cursor.execute("""
            SELECT current_balance
            FROM futures_trading_accounts
            WHERE id = %s
        """, (account_id,))
        account_balance = cursor.fetchone()
        available_balance = float(account_balance['current_balance']) if account_balance else 0

        # 5. 分析每个交易对的信号强度
        all_signals = []
        captured_signals = []
        missed_opportunities = []

        for symbol in monitored_symbols:
            # 分析K线强度
            strength_1h = signal_service.analyze_kline_strength(symbol, '1h', 24)
            strength_15m = signal_service.analyze_kline_strength(symbol, '15m', 24)
            strength_5m = signal_service.analyze_kline_strength(symbol, '5m', 24)

            if not all([strength_1h, strength_15m, strength_5m]):
                continue

            # 计算综合信号强度
            net_power_1h = strength_1h['net_power']
            net_power_15m = strength_15m['net_power']
            net_power_5m = strength_5m['net_power']

            # 判断信号方向和强度
            signal_direction = None
            signal_strength = 0
            signal_quality = "弱"

            # 强多信号：1H和15M都看多
            if net_power_1h >= 3 and net_power_15m >= 2:
                signal_direction = 'LONG'
                signal_strength = abs(net_power_1h) + abs(net_power_15m) * 0.5
                if net_power_1h >= 5 and net_power_15m >= 3:
                    signal_quality = "强"
                else:
                    signal_quality = "中"

            # 强空信号：1H和15M都看空
            elif net_power_1h <= -3 and net_power_15m <= -2:
                signal_direction = 'SHORT'
                signal_strength = abs(net_power_1h) + abs(net_power_15m) * 0.5
                if net_power_1h <= -5 and net_power_15m <= -3:
                    signal_quality = "强"
                else:
                    signal_quality = "中"

            signal_data = {
                'symbol': symbol,
                'signal_direction': signal_direction,
                'signal_strength': signal_strength,
                'signal_quality': signal_quality,
                'net_power_1h': net_power_1h,
                'net_power_15m': net_power_15m,
                'net_power_5m': net_power_5m,
                'kline_1h': strength_1h,
                'kline_15m': strength_15m,
                'kline_5m': strength_5m,
                'has_position': symbol in position_symbols,
                'position_info': position_symbols.get(symbol),
                'in_blacklist': symbol in blacklist,
                'blacklist_reason': blacklist.get(symbol)
            }

            all_signals.append(signal_data)

            # 6. 判断是否捕获或错过
            if signal_direction:  # 有明确信号
                if symbol in position_symbols:
                    # 已开仓
                    pos = position_symbols[symbol]
                    is_correct_direction = (
                        (signal_direction == 'LONG' and pos['position_side'] == 'LONG') or
                        (signal_direction == 'SHORT' and pos['position_side'] == 'SHORT')
                    )

                    captured_signals.append({
                        **signal_data,
                        'captured': True,
                        'correct_direction': is_correct_direction,
                        'status': '✅ 正确捕获' if is_correct_direction else '⚠️ 方向错误'
                    })
                else:
                    # 未开仓，分析原因
                    miss_reasons = []

                    if symbol in blacklist:
                        bl_info = blacklist[symbol]
                        miss_reasons.append(f'黑名单Level{bl_info["level"]}: {bl_info["reason"]}')

                    if signal_quality == "弱":
                        miss_reasons.append('信号强度不足')
                    elif signal_quality == "中" and signal_strength < 8:
                        miss_reasons.append('评分未达开仓阈值')

                    if available_balance < 100:
                        miss_reasons.append('资金不足')

                    if not miss_reasons:
                        miss_reasons.append('未产生开仓信号或系统未识别')

                    # 只记录强信号和中信号
                    if signal_quality in ["强", "中"]:
                        missed_opportunities.append({
                            **signal_data,
                            'captured': False,
                            'miss_reasons': miss_reasons,
                            'main_reason': miss_reasons[0]
                        })

        # 7. 按信号强度排序
        all_signals.sort(key=lambda x: abs(x['signal_strength']), reverse=True)
        captured_signals.sort(key=lambda x: abs(x['signal_strength']), reverse=True)
        missed_opportunities.sort(key=lambda x: abs(x['signal_strength']), reverse=True)

        # 8. 统计错过原因
        miss_reason_stats = {}
        for missed in missed_opportunities:
            for reason in missed['miss_reasons']:
                if reason not in miss_reason_stats:
                    miss_reason_stats[reason] = {
                        'count': 0,
                        'examples': []
                    }
                miss_reason_stats[reason]['count'] += 1
                if len(miss_reason_stats[reason]['examples']) < 3:
                    miss_reason_stats[reason]['examples'].append({
                        'symbol': missed['symbol'],
                        'direction': missed['signal_direction'],
                        'strength': round(missed['signal_strength'], 1),
                        'quality': missed['signal_quality']
                    })

        # 9. 返回数据
        cursor.close()
        conn.close()

        return {
            "success": True,
            "data": {
                "has_data": True,
                "analysis_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "summary": {
                    "total_monitored": len(monitored_symbols),
                    "total_signals": len([s for s in all_signals if s['signal_direction']]),
                    "strong_signals": len([s for s in all_signals if s['signal_quality'] == '强']),
                    "captured": len(captured_signals),
                    "correct_captures": len([s for s in captured_signals if s.get('correct_direction', False)]),
                    "wrong_direction": len([s for s in captured_signals if not s.get('correct_direction', True)]),
                    "missed": len(missed_opportunities),
                    "capture_rate": round(len(captured_signals) / len([s for s in all_signals if s['signal_direction']]) * 100, 1) if [s for s in all_signals if s['signal_direction']] else 0,
                    "available_balance": available_balance,
                    "blacklist_count": len(blacklist)
                },
                "all_signals": all_signals[:30],  # 前30个信号
                "captured_signals": captured_signals,
                "missed_opportunities": missed_opportunities[:20],  # 前20个错过的机会
                "miss_reason_stats": miss_reason_stats
            }
        }

    except Exception as e:
        logger.error(f"获取实时机会分析失败: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get('/daily-pnl')
async def get_daily_pnl_stats(
    month: str = Query(..., description="月份，格式: YYYY-MM"),
    margin_type: str = Query('usdt', description="合约类型: usdt=U本位, coin=币本位")
):
    """
    获取每日盈亏统计

    按月度统计每日的盈亏情况，包括:
    - 月度总览统计
    - 每日盈亏明细
    - 盈亏趋势图数据
    """
    try:
        # 验证月份格式
        try:
            year, month_num = month.split('-')
            year = int(year)
            month_num = int(month_num)
            if month_num < 1 or month_num > 12:
                raise ValueError("月份必须在1-12之间")
        except:
            raise HTTPException(status_code=400, detail="月份格式错误，应为 YYYY-MM")

        # 计算月份的开始和结束日期
        from datetime import date
        import calendar

        month_start = date(year, month_num, 1)
        last_day = calendar.monthrange(year, month_num)[1]
        month_end = date(year, month_num, last_day)

        # 确定account_id
        account_id = 2 if margin_type == 'usdt' else 3

        conn = pymysql.connect(**db_config, cursorclass=pymysql.cursors.DictCursor)
        cursor = conn.cursor()

        # 查询每日盈亏数据
        cursor.execute("""
            SELECT
                DATE(close_time) as trade_date,
                COUNT(*) as total_trades,
                SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as profit_trades,
                SUM(CASE WHEN realized_pnl <= 0 THEN 1 ELSE 0 END) as loss_trades,
                SUM(realized_pnl) as total_pnl,
                SUM(CASE WHEN realized_pnl > 0 THEN realized_pnl ELSE 0 END) as profit_amount,
                SUM(CASE WHEN realized_pnl <= 0 THEN realized_pnl ELSE 0 END) as loss_amount,
                SUM(margin) as total_margin,
                AVG(unrealized_pnl_pct) as avg_pnl_pct
            FROM futures_positions
            WHERE status = 'closed'
            AND account_id = %s
            AND DATE(close_time) >= %s
            AND DATE(close_time) <= %s
            GROUP BY DATE(close_time)
            ORDER BY trade_date ASC
        """, (account_id, month_start, month_end))

        daily_records = cursor.fetchall()

        # 构建每日数据
        daily_data = []
        total_pnl = 0
        total_trades = 0
        profit_days = 0
        max_daily_pnl = 0
        max_daily_pnl_date = None

        for record in daily_records:
            trade_date = record['trade_date']
            total_trades_day = record['total_trades']
            profit_trades_day = record['profit_trades']
            loss_trades_day = record['loss_trades']
            pnl = float(record['total_pnl']) if record['total_pnl'] else 0
            profit_amt = float(record['profit_amount']) if record['profit_amount'] else 0
            loss_amt = float(record['loss_amount']) if record['loss_amount'] else 0
            total_margin_day = float(record['total_margin']) if record['total_margin'] else 0

            # 计算胜率
            win_rate = (profit_trades_day / total_trades_day * 100) if total_trades_day > 0 else 0

            # 计算盈亏比
            profit_loss_ratio = (profit_amt / abs(loss_amt)) if loss_amt != 0 else 0

            # 计算ROI
            roi = (pnl / total_margin_day * 100) if total_margin_day > 0 else 0

            daily_data.append({
                'date': trade_date.strftime('%Y-%m-%d'),
                'total_trades': total_trades_day,
                'profit_trades': profit_trades_day,
                'loss_trades': loss_trades_day,
                'win_rate': round(win_rate, 2),
                'total_pnl': round(pnl, 2),
                'profit_amount': round(profit_amt, 2),
                'loss_amount': round(loss_amt, 2),
                'profit_loss_ratio': round(profit_loss_ratio, 2),
                'roi': round(roi, 2)
            })

            # 累计统计
            total_pnl += pnl
            total_trades += total_trades_day
            if pnl > 0:
                profit_days += 1

            # 记录最大单日盈亏
            if abs(pnl) > abs(max_daily_pnl):
                max_daily_pnl = pnl
                max_daily_pnl_date = trade_date.strftime('%Y-%m-%d')

        cursor.close()
        conn.close()

        # 计算月度统计
        total_days = len(daily_data)
        avg_daily_pnl = (total_pnl / total_days) if total_days > 0 else 0

        summary = {
            'total_pnl': round(total_pnl, 2),
            'total_trades': total_trades,
            'profit_days': profit_days,
            'loss_days': total_days - profit_days,
            'total_days': total_days,
            'avg_daily_pnl': round(avg_daily_pnl, 2),
            'max_daily_pnl': round(max_daily_pnl, 2),
            'max_daily_pnl_date': max_daily_pnl_date or '-',
            'month': f"{year}年{month_num}月"
        }

        return {
            "success": True,
            "data": {
                "summary": summary,
                "daily_data": daily_data
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取每日盈亏统计失败: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
