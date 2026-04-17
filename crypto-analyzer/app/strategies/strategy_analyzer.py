"""
基于策略配置的投资分析器
根据用户的个人投资策略，自定义分析权重和规则
"""
import logging
from typing import Dict, List, Optional
from datetime import datetime

from app.strategies.strategy_config import (
    InvestmentStrategy,
    get_active_strategy,
    get_strategy_manager
)

logger = logging.getLogger(__name__)


class StrategyBasedAnalyzer:
    """基于策略的分析器"""

    def __init__(self, strategy: InvestmentStrategy = None):
        """
        初始化分析器

        Args:
            strategy: 投资策略，默认使用当前激活策略
        """
        if strategy is None:
            strategy = get_active_strategy()

        self.strategy = strategy
        logger.info(f"初始化策略分析器: {strategy.name}")

    def analyze_symbol(self, symbol: str, dimension_scores: Dict) -> Dict:
        """
        基于策略分析单个币种

        Args:
            symbol: 币种符号
            dimension_scores: 各维度得分 {
                'technical': 75.0,
                'hyperliquid': 80.0,
                'news': 60.0,
                'funding_rate': 70.0,
                'ethereum': 65.0
            }

        Returns:
            分析结果字典
        """
        try:
            # 1. 计算加权综合得分
            total_score = self._calculate_weighted_score(dimension_scores)

            # 2. 判断信号强度
            signal_strength = self._evaluate_signal_strength(total_score, dimension_scores)

            # 3. 生成操作建议
            recommendation = self._generate_recommendation(
                symbol, total_score, signal_strength, dimension_scores
            )

            # 4. 计算风险收益比
            risk_reward = self._calculate_risk_reward(total_score)

            # 5. 生成详细原因
            reasons = self._generate_reasons(dimension_scores)

            return {
                'symbol': symbol,
                'strategy_name': self.strategy.name,
                'total_score': round(total_score, 2),
                'signal_strength': signal_strength,
                'recommendation': recommendation,
                'risk_reward_ratio': risk_reward,
                'reasons': reasons,
                'dimension_scores': dimension_scores,
                'timestamp': datetime.now().isoformat()
            }

        except Exception as e:
            logger.error(f"分析 {symbol} 失败: {e}")
            return None

    def _calculate_weighted_score(self, dimension_scores: Dict) -> float:
        """
        计算加权综合得分

        Args:
            dimension_scores: 各维度得分

        Returns:
            加权总分 (0-100)
        """
        weights = self.strategy.dimension_weights

        technical_score = dimension_scores.get('technical', 0) * (weights.technical / 100)
        hyperliquid_score = dimension_scores.get('hyperliquid', 0) * (weights.hyperliquid / 100)
        news_score = dimension_scores.get('news', 0) * (weights.news / 100)
        funding_score = dimension_scores.get('funding_rate', 0) * (weights.funding_rate / 100)
        ethereum_score = dimension_scores.get('ethereum', 0) * (weights.ethereum / 100)

        total = (
            technical_score +
            hyperliquid_score +
            news_score +
            funding_score +
            ethereum_score
        )

        return min(100.0, max(0.0, total))

    def _evaluate_signal_strength(self, total_score: float, dimension_scores: Dict) -> str:
        """
        评估信号强度

        Args:
            total_score: 综合得分
            dimension_scores: 各维度得分

        Returns:
            信号强度: "极强", "强", "中等", "弱", "极弱"
        """
        # 检查是否达到最小信号强度
        if total_score < self.strategy.risk_profile.min_signal_strength:
            return "弱"

        # 统计有多少维度达到良好水平(>=60)
        dimensions_good = sum(1 for score in dimension_scores.values() if score >= 60)

        # 根据综合得分和维度一致性判断
        if total_score >= 85 and dimensions_good >= 4:
            return "极强"
        elif total_score >= 75 and dimensions_good >= 3:
            return "强"
        elif total_score >= 60 and dimensions_good >= 2:
            return "中等"
        elif total_score >= 50:
            return "弱"
        else:
            return "极弱"

    def _generate_recommendation(
        self,
        symbol: str,
        total_score: float,
        signal_strength: str,
        dimension_scores: Dict
    ) -> Dict:
        """
        生成操作建议

        Args:
            symbol: 币种符号
            total_score: 综合得分
            signal_strength: 信号强度
            dimension_scores: 各维度得分

        Returns:
            操作建议字典
        """
        rules = self.strategy.trading_rules
        risk = self.strategy.risk_profile

        # 基础建议
        if total_score >= 75:
            action = "强烈买入"
            position_size = risk.max_position_size
        elif total_score >= 60:
            action = "买入"
            position_size = risk.max_position_size * 0.7
        elif total_score >= 50:
            action = "小仓位买入"
            position_size = risk.max_position_size * 0.4
        elif total_score >= 40:
            action = "观望"
            position_size = 0
        elif total_score >= 30:
            action = "考虑卖出"
            position_size = 0
        else:
            action = "卖出"
            position_size = 0

        # 检查是否允许做空
        if total_score < 30 and risk.allow_short:
            action = "考虑做空"

        # 计算目标价格区间（基于当前得分）
        # 这里简化处理，实际应该基于技术分析
        score_ratio = total_score / 100.0
        entry_optimal = f"当前价格适中" if 50 <= total_score <= 70 else \
                       f"等待回调" if total_score > 70 else \
                       f"避免入场"

        return {
            'action': action,
            'position_size_pct': round(position_size, 2),
            'stop_loss_pct': risk.stop_loss,
            'take_profit_pct': risk.take_profit,
            'max_leverage': risk.max_leverage,
            'entry_strategy': entry_optimal,
            'confidence': signal_strength
        }

    def _calculate_risk_reward(self, total_score: float) -> str:
        """
        计算风险收益比

        Args:
            total_score: 综合得分

        Returns:
            风险收益比字符串
        """
        risk = self.strategy.risk_profile

        # 基于策略配置计算
        reward = risk.take_profit
        loss = risk.stop_loss

        ratio = reward / loss if loss > 0 else 0

        return f"1:{ratio:.1f}"

    def _generate_reasons(self, dimension_scores: Dict) -> List[str]:
        """
        生成决策原因

        Args:
            dimension_scores: 各维度得分

        Returns:
            原因列表
        """
        reasons = []
        weights = self.strategy.dimension_weights

        # 按权重排序维度
        sorted_dimensions = sorted(
            dimension_scores.items(),
            key=lambda x: getattr(weights, x[0], 0),
            reverse=True
        )

        dimension_names = {
            'technical': '技术指标',
            'hyperliquid': 'Hyperliquid聪明钱',
            'news': '新闻情绪',
            'funding_rate': '资金费率',
            'ethereum': '以太坊链上数据'
        }

        for dim_key, score in sorted_dimensions:
            weight = getattr(weights, dim_key, 0)
            name = dimension_names.get(dim_key, dim_key)

            if score >= 75:
                reasons.append(f"✅ {name}表现优秀 ({score:.0f}分, 权重{weight}%)")
            elif score >= 60:
                reasons.append(f"🟢 {name}表现良好 ({score:.0f}分, 权重{weight}%)")
            elif score >= 40:
                reasons.append(f"🟡 {name}表现一般 ({score:.0f}分, 权重{weight}%)")
            else:
                reasons.append(f"🔴 {name}表现较差 ({score:.0f}分, 权重{weight}%)")

        return reasons

    def compare_strategies(self, symbol: str, dimension_scores: Dict) -> Dict:
        """
        比较不同策略的分析结果

        Args:
            symbol: 币种符号
            dimension_scores: 各维度得分

        Returns:
            各策略对比结果
        """
        manager = get_strategy_manager()
        all_strategies = manager.list_strategies()

        results = {}

        for strategy_name in all_strategies:
            strategy = manager.load_strategy(strategy_name)
            if strategy:
                analyzer = StrategyBasedAnalyzer(strategy)
                result = analyzer.analyze_symbol(symbol, dimension_scores)
                results[strategy_name] = result

        return results

    def get_strategy_info(self) -> Dict:
        """获取当前策略信息"""
        return {
            'name': self.strategy.name,
            'description': self.strategy.description,
            'risk_level': self.strategy.risk_profile.level,
            'dimension_weights': {
                'technical': self.strategy.dimension_weights.technical,
                'hyperliquid': self.strategy.dimension_weights.hyperliquid,
                'news': self.strategy.dimension_weights.news,
                'funding_rate': self.strategy.dimension_weights.funding_rate,
                'ethereum': self.strategy.dimension_weights.ethereum
            },
            'risk_controls': {
                'max_position_size': self.strategy.risk_profile.max_position_size,
                'stop_loss': self.strategy.risk_profile.stop_loss,
                'take_profit': self.strategy.risk_profile.take_profit,
                'min_signal_strength': self.strategy.risk_profile.min_signal_strength,
                'allow_short': self.strategy.risk_profile.allow_short,
                'max_leverage': self.strategy.risk_profile.max_leverage
            }
        }


def analyze_with_strategy(symbol: str, dimension_scores: Dict, strategy_name: str = None) -> Dict:
    """
    使用指定策略分析

    Args:
        symbol: 币种符号
        dimension_scores: 各维度得分
        strategy_name: 策略名称，默认使用当前激活策略

    Returns:
        分析结果
    """
    if strategy_name:
        manager = get_strategy_manager()
        strategy = manager.load_strategy(strategy_name)
        if not strategy:
            logger.error(f"策略不存在: {strategy_name}")
            return None
        analyzer = StrategyBasedAnalyzer(strategy)
    else:
        analyzer = StrategyBasedAnalyzer()

    return analyzer.analyze_symbol(symbol, dimension_scores)


if __name__ == "__main__":
    # 测试代码
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))

    logging.basicConfig(level=logging.INFO)

    print("=" * 80)
    print("基于策略的投资分析器测试")
    print("=" * 80)

    # 模拟维度得分
    test_scores = {
        'technical': 75.0,
        'hyperliquid': 85.0,
        'news': 60.0,
        'funding_rate': 70.0,
        'ethereum': 65.0
    }

    symbol = "BTC/USDT"

    # 测试不同策略的分析结果
    manager = get_strategy_manager()
    strategies = ['conservative', 'balanced', 'aggressive']

    for strategy_name in strategies:
        print(f"\n{'=' * 80}")
        print(f"策略: {strategy_name.upper()}")
        print("=" * 80)

        strategy = manager.load_strategy(strategy_name)
        analyzer = StrategyBasedAnalyzer(strategy)

        result = analyzer.analyze_symbol(symbol, test_scores)

        if result:
            print(f"\n币种: {result['symbol']}")
            print(f"综合得分: {result['total_score']}/100")
            print(f"信号强度: {result['signal_strength']}")
            print(f"\n操作建议:")
            rec = result['recommendation']
            print(f"  - 操作: {rec['action']}")
            print(f"  - 建议仓位: {rec['position_size_pct']}%")
            print(f"  - 止损: {rec['stop_loss_pct']}%")
            print(f"  - 止盈: {rec['take_profit_pct']}%")
            print(f"  - 最大杠杆: {rec['max_leverage']}x")
            print(f"  - 风险收益比: {result['risk_reward_ratio']}")
            print(f"  - 入场策略: {rec['entry_strategy']}")
            print(f"\n决策原因:")
            for reason in result['reasons']:
                print(f"  {reason}")

    # 测试策略对比
    print(f"\n{'=' * 80}")
    print("策略对比")
    print("=" * 80)

    analyzer = StrategyBasedAnalyzer()
    comparison = analyzer.compare_strategies(symbol, test_scores)

    print(f"\n{'策略':<15} {'得分':<10} {'操作':<15} {'仓位':<10} {'信号强度':<10}")
    print("-" * 80)
    for strat_name, result in comparison.items():
        if result:
            rec = result['recommendation']
            print(f"{strat_name:<15} "
                  f"{result['total_score']:<10.1f} "
                  f"{rec['action']:<15} "
                  f"{rec['position_size_pct']:<10.1f}% "
                  f"{result['signal_strength']:<10}")

    print("\n✨ 测试完成！")
