#!/usr/bin/env python3
"""
自动合约交易服务
Automatic Futures Trading Service

自动根据投资建议开仓，专注于 BTC, ETH, SOL, BNB
Automatically opens positions based on investment recommendations
"""

import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import yaml
import pymysql
from decimal import Decimal
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from loguru import logger

from app.trading.futures_trading_engine import FuturesTradingEngine
from app.trading.binance_futures_engine import BinanceFuturesEngine


class AutoFuturesTrader:
    """自动合约交易服务"""

    def __init__(self, config_path: str = None):
        """
        初始化自动交易服务

        Args:
            config_path: 配置文件路径
        """
        if config_path is None:
            config_path = Path(__file__).parent.parent.parent / 'config.yaml'

        # 加载配置（支持环境变量）
        from app.utils.config_loader import load_config
        self.config = load_config(Path(config_path))

        self.db_config = self.config['database']['mysql']

        # 初始化Telegram通知服务
        from app.services.trade_notifier import init_trade_notifier
        trade_notifier = init_trade_notifier(self.config)

        # 初始化实盘引擎
        live_engine = None
        try:
            live_engine = BinanceFuturesEngine(self.db_config, trade_notifier=trade_notifier)
            logger.info("✅ AutoFuturesTrader: 实盘引擎已初始化")
        except Exception as e:
            logger.warning(f"⚠️ AutoFuturesTrader: 实盘引擎初始化失败: {e}")

        # 初始化模拟盘引擎，传入live_engine以便平仓同步
        self.engine = FuturesTradingEngine(self.db_config, trade_notifier=trade_notifier, live_engine=live_engine)

        # 交易配置
        self.account_id = 2  # 默认合约账户

        # 仅交易这4个币种
        self.target_symbols = ['BTC/USDT', 'ETH/USDT', 'SOL/USDT', 'BNB/USDT']

        # 最小置信度要求
        self.min_confidence = 75  # 置信度 >= 75% 才开仓

        # 杠杆配置（根据建议强度）
        self.leverage_map = {
            '强烈买入': 10,
            '买入': 5,
            '持有': 0,  # 不操作
            '卖出': 5,
            '强烈卖出': 10
        }

        # 仓位大小配置（币数量）
        self.position_size_map = {
            'BTC/USDT': Decimal('0.01'),   # 0.01 BTC
            'ETH/USDT': Decimal('0.1'),    # 0.1 ETH
            'SOL/USDT': Decimal('1.0'),    # 1.0 SOL
            'BNB/USDT': Decimal('0.5')     # 0.5 BNB
        }

        # 止盈止损配置（根据置信度调整）
        self.stop_loss_take_profit_map = {
            'high_confidence': {  # >= 85%
                'stop_loss_pct': Decimal('5'),
                'take_profit_pct': Decimal('20')
            },
            'medium_confidence': {  # >= 75%
                'stop_loss_pct': Decimal('5'),
                'take_profit_pct': Decimal('15')
            },
            'low_confidence': {  # < 75% (不会开仓)
                'stop_loss_pct': Decimal('5'),
                'take_profit_pct': Decimal('10')
            }
        }

        logger.info("AutoFuturesTrader initialized")
        logger.info(f"Target symbols: {self.target_symbols}")
        logger.info(f"Min confidence: {self.min_confidence}%")

    def get_latest_recommendations(self) -> List[Dict]:
        """
        获取最新的投资建议（1小时内）

        Returns:
            投资建议列表
        """
        connection = pymysql.connect(**self.db_config)
        cursor = connection.cursor(pymysql.cursors.DictCursor)

        sql = """
        SELECT
            symbol,
            recommendation,
            confidence,
            reasoning,
            updated_at
        FROM investment_recommendations
        WHERE symbol IN ({})
        AND updated_at >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
        ORDER BY symbol ASC
        """.format(','.join(['%s'] * len(self.target_symbols)))

        cursor.execute(sql, self.target_symbols)
        recommendations = cursor.fetchall()
        cursor.close()
        connection.close()

        return recommendations

    def check_existing_position(self, symbol: str) -> Optional[Dict]:
        """
        检查是否已有该币种的持仓

        Args:
            symbol: 交易对

        Returns:
            持仓信息，如果没有则返回None
        """
        positions = self.engine.get_open_positions(self.account_id)
        for pos in positions:
            if pos['symbol'] == symbol:
                return pos
        return None

    def should_open_position(self, recommendation: Dict) -> Tuple[bool, str]:
        """
        判断是否应该开仓

        Args:
            recommendation: 投资建议

        Returns:
            (是否开仓, 原因)
        """
        symbol = recommendation['symbol']
        rec_type = recommendation['recommendation']
        confidence = float(recommendation['confidence'])

        # 检查1: 置信度是否达标
        if confidence < self.min_confidence:
            return False, f"Confidence {confidence:.1f}% < {self.min_confidence}%"

        # 检查2: 是否为"持有"建议
        if rec_type == '持有':
            return False, "Recommendation is HOLD"

        # 检查3: 是否已有持仓
        existing = self.check_existing_position(symbol)
        if existing:
            return False, f"Position already exists (ID: {existing['position_id']})"

        return True, "Ready to open"

    def calculate_stop_loss_take_profit(self, confidence: float) -> Tuple[Decimal, Decimal]:
        """
        根据置信度计算止盈止损

        Args:
            confidence: 置信度

        Returns:
            (止损百分比, 止盈百分比)
        """
        if confidence >= 85:
            config = self.stop_loss_take_profit_map['high_confidence']
        elif confidence >= 75:
            config = self.stop_loss_take_profit_map['medium_confidence']
        else:
            config = self.stop_loss_take_profit_map['low_confidence']

        return config['stop_loss_pct'], config['take_profit_pct']

    def open_position_from_recommendation(self, recommendation: Dict) -> Dict:
        """
        根据投资建议开仓

        Args:
            recommendation: 投资建议

        Returns:
            开仓结果
        """
        symbol = recommendation['symbol']
        rec_type = recommendation['recommendation']
        confidence = float(recommendation['confidence'])

        # 确定开仓方向和杠杆
        if rec_type in ['强烈买入', '买入']:
            position_side = 'LONG'
            leverage = self.leverage_map[rec_type]
        elif rec_type in ['强烈卖出', '卖出']:
            position_side = 'SHORT'
            leverage = self.leverage_map[rec_type]
        else:
            return {
                'success': False,
                'message': f'Invalid recommendation type: {rec_type}'
            }

        # 获取仓位大小
        quantity = self.position_size_map.get(symbol, Decimal('0.01'))

        # 计算止盈止损
        stop_loss_pct, take_profit_pct = self.calculate_stop_loss_take_profit(confidence)

        # 开仓
        logger.info(f"🚀 Opening {position_side} position for {symbol}")
        logger.info(f"   Recommendation: {rec_type}, Confidence: {confidence:.1f}%")
        logger.info(f"   Quantity: {quantity}, Leverage: {leverage}x")
        logger.info(f"   Stop-loss: {stop_loss_pct}%, Take-profit: {take_profit_pct}%")

        # 构建开仓信号类型和原因
        entry_signal_type = f"RECOMMENDATION_{rec_type}"  # e.g., RECOMMENDATION_强烈买入
        entry_reason = recommendation.get('reasoning', '') or f"{rec_type} (置信度: {confidence:.1f}%)"

        result = self.engine.open_position(
            account_id=self.account_id,
            symbol=symbol,
            position_side=position_side,
            quantity=quantity,
            leverage=leverage,
            stop_loss_pct=stop_loss_pct,
            take_profit_pct=take_profit_pct,
            source='auto_signal',
            entry_signal_type=entry_signal_type,
            entry_reason=entry_reason
        )

        if result['success']:
            logger.info(f"✅ Position opened successfully!")
            logger.info(f"   Position ID: {result['position_id']}")
            logger.info(f"   Entry price: {result['entry_price']:.2f}")
            logger.info(f"   Margin: {result['margin']:.2f} USDT")
            logger.info(f"   Liquidation: {result['liquidation_price']:.2f}")
            logger.info(f"   Stop-loss: {result['stop_loss_price']:.2f}")
            logger.info(f"   Take-profit: {result['take_profit_price']:.2f}")
        else:
            logger.error(f"❌ Failed to open position: {result['message']}")

        return result

    def run_auto_trading_cycle(self) -> Dict:
        """
        执行一次自动交易周期

        Returns:
            交易结果统计
        """
        logger.info("=" * 70)
        logger.info(f"🤖 Auto-Trading Cycle Started - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 70)

        # 获取最新建议
        recommendations = self.get_latest_recommendations()

        if not recommendations:
            logger.warning("⚠️  No recent recommendations found (last 1 hour)")
            return {
                'processed': 0,
                'opened': 0,
                'skipped': 0,
                'failed': 0,
                'details': []
            }

        logger.info(f"📊 Found {len(recommendations)} recommendations")

        # 统计结果
        results = {
            'processed': 0,
            'opened': 0,
            'skipped': 0,
            'failed': 0,
            'details': []
        }

        # 处理每个建议
        for rec in recommendations:
            results['processed'] += 1

            symbol = rec['symbol']
            rec_type = rec['recommendation']
            confidence = float(rec['confidence'])

            logger.info(f"\n📌 Processing {symbol}:")
            logger.info(f"   Recommendation: {rec_type}")
            logger.info(f"   Confidence: {confidence:.1f}%")

            detail = {
                'symbol': symbol,
                'recommendation': rec_type,
                'confidence': confidence,
                'timestamp': datetime.now().isoformat()
            }

            # 判断是否开仓
            should_open, reason = self.should_open_position(rec)

            if not should_open:
                logger.info(f"   ⏭️  Skipped: {reason}")
                detail['status'] = 'skipped'
                detail['reason'] = reason
                results['skipped'] += 1
                results['details'].append(detail)
                continue

            # 开仓
            try:
                result = self.open_position_from_recommendation(rec)

                if result['success']:
                    detail['status'] = 'opened'
                    detail['position_id'] = result['position_id']
                    detail['entry_price'] = result['entry_price']
                    detail['margin'] = result['margin']
                    detail['leverage'] = result.get('leverage', 1)
                    results['opened'] += 1
                else:
                    detail['status'] = 'failed'
                    detail['error'] = result['message']
                    results['failed'] += 1

            except Exception as e:
                logger.error(f"   ❌ Exception: {e}", exc_info=True)
                detail['status'] = 'failed'
                detail['error'] = str(e)
                results['failed'] += 1

            results['details'].append(detail)

        # 输出总结
        logger.info("\n" + "=" * 70)
        logger.info("📈 Auto-Trading Cycle Summary:")
        logger.info(f"   Total processed: {results['processed']}")
        logger.info(f"   ✅ Opened: {results['opened']}")
        logger.info(f"   ⏭️  Skipped: {results['skipped']}")
        logger.info(f"   ❌ Failed: {results['failed']}")
        logger.info("=" * 70)

        return results

    def get_account_summary(self) -> Dict:
        """
        获取账户摘要

        Returns:
            账户信息
        """
        connection = pymysql.connect(**self.db_config)
        cursor = connection.cursor(pymysql.cursors.DictCursor)

        sql = """
        SELECT
            current_balance,
            frozen_balance,
            unrealized_pnl,
            realized_pnl,
            total_equity,
            total_trades,
            win_rate
        FROM futures_trading_accounts
        WHERE id = %s
        """

        cursor.execute(sql, (self.account_id,))
        account = cursor.fetchone()
        cursor.close()
        connection.close()

        if account:
            for key, value in account.items():
                if isinstance(value, Decimal):
                    account[key] = float(value)

        return account

    def close(self):
        """关闭资源"""
        if hasattr(self, 'engine'):
            self.engine.close()


def main():
    """主函数 - 用于测试"""
    logger.info("🤖 Auto Futures Trader - Test Mode")

    # 创建自动交易服务
    trader = AutoFuturesTrader()

    # 显示账户信息
    account = trader.get_account_summary()
    logger.info(f"\n💰 Account Summary:")
    logger.info(f"   Balance: {account['current_balance']:.2f} USDT")
    logger.info(f"   Available: {account['current_balance'] - account['frozen_balance']:.2f} USDT")
    logger.info(f"   Unrealized PnL: {account['unrealized_pnl']:.2f} USDT")
    logger.info(f"   Total Equity: {account['total_equity']:.2f} USDT")

    # 执行一次交易周期
    results = trader.run_auto_trading_cycle()

    # 再次显示账户信息
    account = trader.get_account_summary()
    logger.info(f"\n💰 Account Summary After Trading:")
    logger.info(f"   Balance: {account['current_balance']:.2f} USDT")
    logger.info(f"   Available: {account['current_balance'] - account['frozen_balance']:.2f} USDT")
    logger.info(f"   Unrealized PnL: {account['unrealized_pnl']:.2f} USDT")

    # 关闭
    trader.close()


if __name__ == '__main__':
    main()
