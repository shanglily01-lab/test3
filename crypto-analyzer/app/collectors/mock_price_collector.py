"""
模拟价格数据采集器
用于无法访问真实交易所API时的演示模式
生成模拟的实时价格和K线数据
"""

import asyncio
import random
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from loguru import logger
import pandas as pd
import numpy as np


class MockPriceCollector:
    """模拟价格数据采集器"""

    def __init__(self, exchange_id: str = "mock", config: dict = None):
        """
        初始化模拟采集器

        Args:
            exchange_id: 模拟交易所ID
            config: 配置字典
        """
        self.exchange_id = exchange_id
        self.config = config or {}

        # 初始价格 (模拟真实市场价格)
        self.base_prices = {
            'BTC/USDT': 45000.0,
            'ETH/USDT': 2500.0,
            'BNB/USDT': 320.0,
            'SOL/USDT': 105.0,
            'ADA/USDT': 0.52,
            'XRP/USDT': 0.58,
            'DOGE/USDT': 0.085,
            'MATIC/USDT': 0.92,
            'DOT/USDT': 7.25,
            'AVAX/USDT': 38.50,
        }

        # 当前价格 (会随机波动)
        self.current_prices = self.base_prices.copy()

        # 记录开盘价
        self.open_prices = self.base_prices.copy()

        logger.info(f"✅ 初始化模拟采集器 ({exchange_id}) - 演示模式")

    def _simulate_price_change(self, current_price: float, volatility: float = 0.002) -> float:
        """
        模拟价格变化

        Args:
            current_price: 当前价格
            volatility: 波动率 (默认0.2%)

        Returns:
            新价格
        """
        # 随机涨跌 (-volatility% 到 +volatility%)
        change_pct = random.uniform(-volatility, volatility)
        new_price = current_price * (1 + change_pct)
        return round(new_price, 2)

    async def fetch_ticker(self, symbol: str) -> Optional[Dict]:
        """
        获取模拟实时价格

        Args:
            symbol: 交易对，如 'BTC/USDT'

        Returns:
            价格数据字典
        """
        try:
            # 模拟网络延迟
            await asyncio.sleep(random.uniform(0.1, 0.3))

            if symbol not in self.current_prices:
                logger.warning(f"不支持的交易对: {symbol}")
                return None

            # 更新价格 (随机波动)
            old_price = self.current_prices[symbol]
            new_price = self._simulate_price_change(old_price)
            self.current_prices[symbol] = new_price

            # 计算24h变化
            change_24h = ((new_price - self.open_prices[symbol]) / self.open_prices[symbol]) * 100

            # 生成高低价
            high = new_price * (1 + random.uniform(0, 0.015))
            low = new_price * (1 - random.uniform(0, 0.015))

            # 生成成交量
            base_volume = random.uniform(10000, 50000)

            return {
                'exchange': self.exchange_id,
                'symbol': symbol,
                'timestamp': datetime.now(),
                'price': new_price,
                'open': self.open_prices[symbol],
                'high': high,
                'low': low,
                'close': new_price,
                'volume': base_volume,
                'quote_volume': base_volume * new_price,
                'bid': new_price * 0.9999,
                'ask': new_price * 1.0001,
                'change_24h': change_24h,
            }

        except Exception as e:
            logger.error(f"模拟获取 {symbol} 价格失败: {e}")
            return None

    async def fetch_best_price(self, symbol: str) -> Optional[Dict]:
        """
        获取最优价格（兼容MultiExchangeCollector接口）

        Args:
            symbol: 交易对，如 'BTC/USDT'

        Returns:
            价格数据字典
        """
        # 直接使用fetch_ticker，因为模拟器只有一个"交易所"
        return await self.fetch_ticker(symbol)

    async def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str = '1h',
        limit: int = 100,
        since: Optional[int] = None
    ) -> Optional[pd.DataFrame]:
        """
        获取模拟K线数据 (OHLCV)

        Args:
            symbol: 交易对
            timeframe: 时间周期
            limit: 获取数量
            since: 起始时间戳(毫秒)

        Returns:
            DataFrame包含 [timestamp, open, high, low, close, volume]
        """
        try:
            await asyncio.sleep(random.uniform(0.2, 0.5))

            if symbol not in self.base_prices:
                return None

            # 解析时间周期
            timeframe_minutes = {
                '1m': 1, '5m': 5, '15m': 15, '30m': 30,
                '1h': 60, '4h': 240, '1d': 1440
            }.get(timeframe, 60)

            # 生成历史K线数据
            data = []
            base_price = self.base_prices[symbol]

            # 起始时间
            if since:
                start_time = datetime.fromtimestamp(since / 1000)
            else:
                start_time = datetime.now() - timedelta(minutes=timeframe_minutes * limit)

            for i in range(limit):
                timestamp = start_time + timedelta(minutes=timeframe_minutes * i)

                # 生成OHLC (带随机趋势)
                trend = np.sin(i / 20) * 0.02  # 模拟波动趋势
                open_price = base_price * (1 + trend + random.uniform(-0.01, 0.01))
                close_price = open_price * (1 + random.uniform(-0.02, 0.02))
                high_price = max(open_price, close_price) * (1 + random.uniform(0, 0.01))
                low_price = min(open_price, close_price) * (1 - random.uniform(0, 0.01))
                volume = random.uniform(100, 1000)

                data.append([
                    timestamp,
                    round(open_price, 2),
                    round(high_price, 2),
                    round(low_price, 2),
                    round(close_price, 2),
                    round(volume, 2)
                ])

            df = pd.DataFrame(
                data,
                columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
            )

            df['symbol'] = symbol
            df['exchange'] = self.exchange_id
            df['timeframe'] = timeframe

            return df

        except Exception as e:
            logger.error(f"模拟获取 {symbol} K线数据失败: {e}")
            return None

    async def fetch_order_book(self, symbol: str, limit: int = 20) -> Optional[Dict]:
        """
        获取模拟订单簿

        Args:
            symbol: 交易对
            limit: 深度

        Returns:
            订单簿数据
        """
        try:
            await asyncio.sleep(random.uniform(0.1, 0.2))

            if symbol not in self.current_prices:
                return None

            current_price = self.current_prices[symbol]

            # 生成买单 (bid)
            bids = []
            for i in range(limit):
                price = current_price * (1 - (i + 1) * 0.0001)
                amount = random.uniform(0.1, 10)
                bids.append([price, amount])

            # 生成卖单 (ask)
            asks = []
            for i in range(limit):
                price = current_price * (1 + (i + 1) * 0.0001)
                amount = random.uniform(0.1, 10)
                asks.append([price, amount])

            return {
                'exchange': self.exchange_id,
                'symbol': symbol,
                'timestamp': datetime.now(),
                'bids': bids,
                'asks': asks,
                'bid_volume': sum(bid[1] for bid in bids),
                'ask_volume': sum(ask[1] for ask in asks),
            }

        except Exception as e:
            logger.error(f"模拟获取 {symbol} 订单簿失败: {e}")
            return None

    async def fetch_trades(self, symbol: str, limit: int = 50) -> Optional[List[Dict]]:
        """
        获取模拟最近成交记录

        Args:
            symbol: 交易对
            limit: 数量

        Returns:
            成交记录列表
        """
        try:
            await asyncio.sleep(random.uniform(0.1, 0.2))

            if symbol not in self.current_prices:
                return None

            current_price = self.current_prices[symbol]
            trades = []

            for i in range(limit):
                # 随机价格变动
                price = current_price * (1 + random.uniform(-0.001, 0.001))
                amount = random.uniform(0.01, 5)
                side = random.choice(['buy', 'sell'])

                trades.append({
                    'exchange': self.exchange_id,
                    'symbol': symbol,
                    'timestamp': datetime.now() - timedelta(seconds=i * 2),
                    'price': round(price, 2),
                    'amount': round(amount, 4),
                    'side': side,
                    'cost': round(price * amount, 2)
                })

            return trades

        except Exception as e:
            logger.error(f"模拟获取 {symbol} 成交记录失败: {e}")
            return None


# 使用示例
async def test_mock_collector():
    """测试模拟采集器"""

    collector = MockPriceCollector('binance_mock')

    print("\n=== 测试模拟价格采集器 ===\n")

    # 测试1: 获取实时价格
    print("1. 获取BTC实时价格:")
    ticker = await collector.fetch_ticker('BTC/USDT')
    if ticker:
        print(f"   价格: ${ticker['price']:,.2f}")
        print(f"   24h变化: {ticker['change_24h']:+.2f}%")
        print(f"   成交量: {ticker['volume']:,.2f}")

    # 测试2: 获取K线数据
    print("\n2. 获取1小时K线数据:")
    ohlcv = await collector.fetch_ohlcv('BTC/USDT', '1h', limit=10)
    if ohlcv is not None:
        print(f"   获取到 {len(ohlcv)} 条K线")
        print(ohlcv[['timestamp', 'close']].tail())

    # 测试3: 持续模拟价格变化
    print("\n3. 模拟价格实时变化 (5次):")
    for i in range(5):
        ticker = await collector.fetch_ticker('BTC/USDT')
        if ticker:
            print(f"   [{i+1}] ${ticker['price']:,.2f} ({ticker['change_24h']:+.2f}%)")
        await asyncio.sleep(1)

    print("\n✅ 测试完成!")


if __name__ == '__main__':
    asyncio.run(test_mock_collector())
