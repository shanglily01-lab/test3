#!/usr/bin/env python3
"""
波动率计算器
用于在开仓时一次性计算合适的止损止盈百分比

特点:
1. 基于最近24小时的历史K线数据
2. 区分多空方向的不同风险
3. 计算结果在开仓时固定,持仓期间不变
4. 使用缓存避免重复计算
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, Tuple, Optional
import statistics
import mysql.connector

logger = logging.getLogger(__name__)

# 数据库配置（统一从环境变量读，不再硬编码）
import os as _os
DB_CONFIG = {
    'host':     _os.getenv('DB_HOST', 'localhost'),
    'port':     int(_os.getenv('DB_PORT', '3306')),
    'user':     _os.getenv('DB_USER', ''),
    'password': _os.getenv('DB_PASSWORD', ''),
    'database': _os.getenv('DB_NAME', ''),
}


class VolatilityCalculator:
    """波动率计算器"""

    def __init__(self):
        self.cache = {}  # 缓存计算结果
        self.cache_ttl = 3600  # 缓存1小时(避免频繁查询K线)

    def get_sl_tp_for_position(
        self,
        symbol: str,
        position_side: str,
        entry_score: int = 50,
        signal_components: list = None
    ) -> Tuple[float, float, str]:
        """
        获取开仓时应该使用的止损止盈百分比

        参数:
            symbol: 交易对 (如 'BTC/USDT')
            position_side: 持仓方向 'LONG' 或 'SHORT'
            entry_score: 入场评分 (用于调整止损宽度)
            signal_components: 信号组件列表

        返回:
            (止损百分比, 止盈百分比, 计算原因)

        示例:
            >>> calc = VolatilityCalculator()
            >>> sl, tp, reason = calc.get_sl_tp_for_position('AXS/USDT', 'SHORT', 75)
            >>> print(f"SL: {sl}%, TP: {tp}%")
            SL: 4.0%, TP: 1.8%
        """
        signal_components = signal_components or []

        # 1. 获取波动率数据(使用缓存)
        volatility = self._get_volatility_cached(symbol)

        if not volatility:
            # 没有历史数据,使用保守的固定值
            logger.warning(f"{symbol} 无历史波动数据,使用固定值")
            return self._get_default_sl_tp(position_side, "无历史数据")

        # 2. 根据方向计算基础止损止盈
        if position_side == 'LONG':
            # 多单: 风险是向下,收益是向上
            base_sl = max(
                volatility['downside_p75'] * 1.3,  # 覆盖75%向下波动+30%
                volatility['avg_downside'] * 1.5
            )
            base_tp = min(
                volatility['upside_p75'] * 0.8,    # 目标75%向上波动的80%
                volatility['avg_upside'] * 2.0
            )
        else:  # SHORT
            # 空单: 风险是向上,收益是向下
            base_sl = max(
                volatility['upside_p75'] * 1.3,
                volatility['avg_upside'] * 1.5
            )
            base_tp = min(
                volatility['downside_p75'] * 0.8,
                volatility['avg_downside'] * 2.0
            )

        # 3. 根据入场评分调整(低分需要更大止损空间)
        score_multiplier = 1.0
        if entry_score < 35:
            score_multiplier = 1.3  # 低分+30%止损空间
        elif entry_score < 40:
            score_multiplier = 1.2
        elif entry_score > 60:
            score_multiplier = 0.9  # 高分可以适当收紧

        adjusted_sl = base_sl * score_multiplier

        # 4. 特殊信号调整
        special_adjustments = []

        if 'volatility_high' in signal_components:
            adjusted_sl *= 1.3  # 高波动信号+30%
            special_adjustments.append('高波动+30%')

        # 检查方向性风险
        directional_bias = volatility['avg_upside'] - volatility['avg_downside']
        if position_side == 'SHORT' and directional_bias > 0.5:
            # 空单遇到向上偏好的币种,增加止损
            adjusted_sl *= 1.2
            special_adjustments.append('向上偏好+20%')
        elif position_side == 'LONG' and directional_bias < -0.5:
            # 多单遇到向下偏好的币种,增加止损
            adjusted_sl *= 1.2
            special_adjustments.append('向下偏好+20%')

        # 5. 应用安全边界
        final_sl = max(adjusted_sl, 2.5)  # 最小2.5% (5x杠杆下ROI损失12.5%)
        final_sl = min(final_sl, 15.0)    # 最大15%(避免过于宽松)

        final_tp = max(base_tp, 5.0)      # 最小5% (5x杠杆下ROI 25%)
        final_tp = min(final_tp, 15.0)    # 最大15% (5x杠杆下ROI 75%)

        # 6. 确保盈亏比不会太差 (盈亏比 = 止损:止盈 = 风险:收益)
        risk_reward = final_sl / final_tp if final_tp > 0 else 0
        if risk_reward > 0.67:  # 盈亏比高于1:1.5太差 (止损不能超过止盈的67%)
            final_tp = final_sl * 2.0  # 至少保证1:2盈亏比
            special_adjustments.append('盈亏比调整至1:2')

        # 7. 生成计算原因
        reason_parts = [
            f"基于24H数据: 向上{volatility['avg_upside']:.1f}% 向下{volatility['avg_downside']:.1f}%",
            f"评分{entry_score}分"
        ]

        if special_adjustments:
            reason_parts.append(', '.join(special_adjustments))

        reason_parts.append(f"盈亏比1:{(1/risk_reward if risk_reward > 0 else 0):.2f}")
        reason = ' | '.join(reason_parts)

        logger.info(f"{symbol} {position_side} - SL:{final_sl:.2f}% TP:{final_tp:.2f}% - {reason}")

        return round(final_sl, 2), round(final_tp, 2), reason

    def _get_volatility_cached(self, symbol: str) -> Optional[Dict]:
        """获取波动率数据(带缓存)"""
        cache_key = f"{symbol}_volatility"

        # 检查缓存
        if cache_key in self.cache:
            cached_time, cached_data = self.cache[cache_key]
            if (datetime.now() - cached_time).total_seconds() < self.cache_ttl:
                return cached_data

        # 计算新数据
        volatility = self._calculate_volatility(symbol)

        if volatility:
            self.cache[cache_key] = (datetime.now(), volatility)

        return volatility

    def _calculate_volatility(self, symbol: str) -> Optional[Dict]:
        """计算交易对的方向性波动率"""
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor(dictionary=True)

            # 查询最近24小时的1小时K线
            # 原因: 持仓时间4-6小时,用24小时数据既贴近当前市场,又有足够样本
            cursor.execute("""
                SELECT
                    open_price, high_price, low_price, close_price
                FROM kline_data
                WHERE symbol = %s
                AND timeframe = '1h'
                AND timestamp >= DATE_SUB(NOW(), INTERVAL 24 HOUR)
                ORDER BY timestamp ASC
            """, (symbol,))

            klines = cursor.fetchall()
            cursor.close()

            if not klines or len(klines) < 12:
                logger.warning(f"{symbol} K线数据不足: {len(klines) if klines else 0}根")
                return None

            # 计算方向性波动
            upside_moves = []    # 向上波动
            downside_moves = []  # 向下波动

            for k in klines:
                open_p = float(k['open_price'])
                high_p = float(k['high_price'])
                low_p = float(k['low_price'])

                upside_pct = (high_p - open_p) / open_p * 100
                downside_pct = (open_p - low_p) / open_p * 100

                upside_moves.append(upside_pct)
                downside_moves.append(downside_pct)

            # 统计指标
            return {
                'avg_upside': statistics.mean(upside_moves),
                'avg_downside': statistics.mean(downside_moves),
                'upside_p75': statistics.quantiles(upside_moves, n=4)[2],
                'downside_p75': statistics.quantiles(downside_moves, n=4)[2],
                'max_upside': max(upside_moves),
                'max_downside': max(downside_moves),
                'kline_count': len(klines)
            }

        except Exception as e:
            logger.error(f"计算 {symbol} 波动率失败: {e}", exc_info=True)
            return None

    def _get_default_sl_tp(self, position_side: str, reason: str) -> Tuple[float, float, str]:
        """返回默认的止损止盈值"""
        # 保守的固定值
        default_sl = 3.0  # 3%止损(比原来的2%更宽松)
        default_tp = 6.0  # 6%止盈(保持1:2盈亏比)

        full_reason = f"使用默认值 ({reason}) | 盈亏比1:2.0"

        return default_sl, default_tp, full_reason

    def clear_cache(self):
        """清空缓存"""
        self.cache.clear()
        logger.info("波动率缓存已清空")

    def get_cache_stats(self) -> Dict:
        """获取缓存统计"""
        now = datetime.now()
        valid_count = sum(
            1 for cached_time, _ in self.cache.values()
            if (now - cached_time).total_seconds() < self.cache_ttl
        )

        return {
            'total_cached': len(self.cache),
            'valid_cached': valid_count,
            'ttl_seconds': self.cache_ttl
        }


# 全局单例
_calculator_instance = None

def get_volatility_calculator() -> VolatilityCalculator:
    """获取波动率计算器单例"""
    global _calculator_instance
    if _calculator_instance is None:
        _calculator_instance = VolatilityCalculator()
    return _calculator_instance
