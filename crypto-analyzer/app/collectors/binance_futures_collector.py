"""
币安合约数据采集器
支持采集永续合约的实时价格、K线数据、资金费率、持仓量等信息
"""

import asyncio
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from loguru import logger
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from binance.client import Client


class BinanceFuturesCollector:
    """币安合约数据采集器"""

    def __init__(self, config: dict = None):
        """
        初始化合约数据采集器

        Args:
            config: 配置字典，包含API密钥等（可选，公开接口不需要）
        """
        self.config = config or {}
        self.exchange_id = 'binance_futures'

        # API端点
        self.base_url = "https://fapi.binance.com"

        # 获取API密钥（可选）
        api_key = self.config.get('api_key', '').strip()
        api_secret = self.config.get('api_secret', '').strip()

        # 初始化币安客户端（捕获连接错误）
        try:
            if api_key and api_secret:
                self.client = Client(api_key, api_secret)
                logger.info("初始化币安合约采集器 (使用API密钥)")
            else:
                self.client = Client("", "")
                logger.info("初始化币安合约采集器 (公开接口模式)")
        except (ConnectionError, requests.exceptions.ConnectionError, Exception) as e:
            logger.warning(f"币安合约采集器初始化失败（连接错误）: {e}")
            logger.debug(f"错误类型: {type(e).__name__}, 错误详情: {str(e)}")
            self.client = None
            logger.info("币安合约采集器将在首次使用时重试连接")

        # 创建带重试的 requests session
        self.session = self._create_session()
    
    def _ensure_client(self):
        """
        确保客户端已初始化，如果未初始化则尝试重新初始化
        
        Returns:
            bool: 客户端是否可用
        """
        if self.client is not None:
            return True
        
        # 尝试重新初始化客户端
        try:
            api_key = self.config.get('api_key', '').strip()
            api_secret = self.config.get('api_secret', '').strip()
            
            if api_key and api_secret:
                self.client = Client(api_key, api_secret)
                logger.info("币安合约采集器客户端重新初始化成功 (使用API密钥)")
            else:
                self.client = Client("", "")
                logger.info("币安合约采集器客户端重新初始化成功 (公开接口模式)")
            return True
        except Exception as e:
            logger.debug(f"币安合约采集器客户端重新初始化失败: {e}")
            return False

    def _create_session(self):
        """创建带重试机制的 requests session"""
        session = requests.Session()

        # 配置重试策略
        retry_strategy = Retry(
            total=3,  # 最多重试3次
            backoff_factor=1,  # 重试间隔：1s, 2s, 4s
            status_forcelist=[429, 500, 502, 503, 504],  # 这些状态码会重试
            allowed_methods=["GET", "POST"]  # 允许重试的HTTP方法
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # 设置超时
        session.timeout = 10  # 10秒超时

        return session

    async def _request_with_retry(self, url: str, params: dict = None, max_retries: int = 3) -> Optional[dict]:
        """
        带重试的异步请求

        Args:
            url: 请求URL
            params: 请求参数
            max_retries: 最大重试次数

        Returns:
            JSON响应数据或None
        """
        for attempt in range(max_retries):
            try:
                response = await asyncio.to_thread(
                    self.session.get,
                    url,
                    params=params,
                    timeout=10
                )

                if response.status_code == 200:
                    return response.json()
                else:
                    logger.warning(f"请求失败 (状态码: {response.status_code}), 重试 {attempt + 1}/{max_retries}")

            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout,
                    requests.exceptions.ChunkedEncodingError,
                    ConnectionAbortedError) as e:
                logger.warning(f"网络错误: {type(e).__name__}, 重试 {attempt + 1}/{max_retries}")

                if attempt < max_retries - 1:
                    # 指数退避
                    wait_time = 2 ** attempt
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"请求失败，已达最大重试次数: {url}")
                    return None

            except Exception as e:
                logger.error(f"请求异常: {e}")
                return None

        return None

    async def fetch_futures_ticker(self, symbol: str) -> Optional[Dict]:
        """
        获取合约实时价格

        Args:
            symbol: 交易对，如 'BTC/USDT' 或 'BTCUSDT'

        Returns:
            合约价格数据字典
        """
        try:
            # 转换交易对格式: BTC/USDT -> BTCUSDT
            binance_symbol = symbol.replace('/', '')

            # 使用公开API获取ticker
            url = f"{self.base_url}/fapi/v1/ticker/24hr"
            params = {'symbol': binance_symbol}

            response = await asyncio.to_thread(requests.get, url, params=params)

            if response.status_code == 200:
                ticker = response.json()

                return {
                    'exchange': self.exchange_id,
                    'symbol': symbol,
                    'timestamp': datetime.fromtimestamp(ticker['closeTime'] / 1000),
                    'price': float(ticker['lastPrice']),
                    'open': float(ticker['openPrice']),
                    'high': float(ticker['highPrice']),
                    'low': float(ticker['lowPrice']),
                    'close': float(ticker['lastPrice']),
                    'volume': float(ticker['volume']),  # 合约张数
                    'quote_volume': float(ticker['quoteVolume']),  # USDT成交额
                    'price_change': float(ticker['priceChange']),
                    'price_change_percent': float(ticker['priceChangePercent']),
                    'weighted_avg_price': float(ticker['weightedAvgPrice']),
                    'last_qty': float(ticker['lastQty']),
                    'count': int(ticker['count'])  # 成交笔数
                }
            else:
                logger.error(f"获取合约ticker失败: HTTP {response.status_code}")
                return None

        except Exception as e:
            logger.error(f"获取 {symbol} 合约ticker失败: {e}")
            return None

    async def fetch_futures_klines(
        self,
        symbol: str,
        timeframe: str = '1m',
        limit: int = 100
    ) -> Optional[pd.DataFrame]:
        """
        获取合约K线数据

        Args:
            symbol: 交易对
            timeframe: 时间周期 (1m, 5m, 15m, 1h, 4h, 1d等)
            limit: 获取数量（最大1500）

        Returns:
            DataFrame包含 [timestamp, open, high, low, close, volume]
        """
        try:
            # 转换交易对格式
            binance_symbol = symbol.replace('/', '')

            # 使用公开API获取K线
            url = f"{self.base_url}/fapi/v1/klines"
            params = {
                'symbol': binance_symbol,
                'interval': timeframe,
                'limit': min(limit, 1500)  # 币安限制最大1500
            }

            response = await asyncio.to_thread(requests.get, url, params=params)

            if response.status_code != 200:
                logger.error(f"获取合约K线失败: HTTP {response.status_code}")
                return None

            klines = response.json()

            if not klines:
                return None

            # 转换为DataFrame
            df = pd.DataFrame(klines, columns=[
                'open_time', 'open', 'high', 'low', 'close', 'volume',
                'close_time', 'quote_volume', 'trades', 'taker_buy_volume',
                'taker_buy_quote_volume', 'ignore'
            ])

            # 选择需要的列并转换类型
            df = df[['open_time', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trades']].copy()
            df['timestamp'] = pd.to_datetime(df['open_time'], unit='ms')
            df['open'] = df['open'].astype(float)
            df['high'] = df['high'].astype(float)
            df['low'] = df['low'].astype(float)
            df['close'] = df['close'].astype(float)
            df['volume'] = df['volume'].astype(float)
            df['quote_volume'] = df['quote_volume'].astype(float)
            df['trades'] = df['trades'].astype(int)

            # 添加元数据
            df['symbol'] = symbol
            df['exchange'] = self.exchange_id
            df['timeframe'] = timeframe

            return df

        except Exception as e:
            error_msg = str(e)
            # 如果是无效交易对错误，提供更友好的提示
            if 'Invalid symbol' in error_msg or '-1121' in error_msg or 'HTTP 400' in error_msg:
                logger.error(f"获取 {symbol} 合约K线失败: 交易对格式错误或不存在 (尝试的格式: {binance_symbol})")
                logger.debug(f"提示: 币安合约API需要格式如 'BTCUSDT'，请确认交易对 {symbol} 在币安合约市场是否存在")
            else:
                logger.error(f"获取 {symbol} 合约K线失败: {e}")
            return None

    async def fetch_funding_rate(self, symbol: str) -> Optional[Dict]:
        """
        获取永续合约资金费率

        Args:
            symbol: 交易对

        Returns:
            资金费率数据字典
        """
        try:
            # 转换交易对格式
            binance_symbol = symbol.replace('/', '')

            # 使用带重试的请求
            url = f"{self.base_url}/fapi/v1/premiumIndex"
            params = {'symbol': binance_symbol}

            data = await self._request_with_retry(url, params)

            if data:
                return {
                    'exchange': self.exchange_id,
                    'symbol': symbol,
                    'funding_rate': float(data.get('lastFundingRate', 0)),
                    'funding_time': int(data.get('time', 0)),
                    'timestamp': datetime.fromtimestamp(int(data.get('time', 0)) / 1000) if data.get('time') else datetime.now(),
                    'mark_price': float(data.get('markPrice', 0)),
                    'index_price': float(data.get('indexPrice', 0)),
                    'next_funding_time': int(data.get('nextFundingTime', 0)),
                    'interest_rate': float(data.get('interestRate', 0))
                }
            else:
                return None

        except Exception as e:
            logger.error(f"获取 {symbol} 资金费率失败: {e}")
            return None

    async def fetch_open_interest(self, symbol: str) -> Optional[Dict]:
        """
        获取持仓量

        Args:
            symbol: 交易对

        Returns:
            持仓量数据字典
        """
        try:
            # 转换交易对格式
            binance_symbol = symbol.replace('/', '')

            # 使用公开API获取持仓量
            url = f"{self.base_url}/fapi/v1/openInterest"
            params = {'symbol': binance_symbol}

            response = await asyncio.to_thread(requests.get, url, params=params)

            if response.status_code == 200:
                data = response.json()

                return {
                    'exchange': self.exchange_id,
                    'symbol': symbol,
                    'open_interest': float(data.get('openInterest', 0)),
                    'timestamp': datetime.fromtimestamp(int(data.get('time', 0)) / 1000) if data.get('time') else datetime.now(),
                }
            else:
                logger.error(f"获取持仓量失败: HTTP {response.status_code}")
                return None

        except Exception as e:
            logger.error(f"获取 {symbol} 持仓量失败: {e}")
            return None

    async def fetch_long_short_ratio(self, symbol: str, period: str = '5m') -> Optional[Dict]:
        """
        获取多空比率（全局账户数）

        Args:
            symbol: 交易对
            period: 时间周期 (5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d)

        Returns:
            多空比率数据字典
        """
        try:
            # 转换交易对格式
            binance_symbol = symbol.replace('/', '')

            # 使用公开API获取多空比
            url = f"{self.base_url}/futures/data/globalLongShortAccountRatio"
            params = {
                'symbol': binance_symbol,
                'period': period,
                'limit': 1
            }

            response = await asyncio.to_thread(requests.get, url, params=params)

            if response.status_code == 200:
                data = response.json()

                if data and len(data) > 0:
                    latest = data[0]

                    return {
                        'exchange': self.exchange_id,
                        'symbol': symbol,
                        'long_account': float(latest.get('longAccount', 0)),
                        'short_account': float(latest.get('shortAccount', 0)),
                        'long_short_ratio': float(latest.get('longShortRatio', 0)),
                        'timestamp': datetime.fromtimestamp(int(latest.get('timestamp', 0)) / 1000) if latest.get('timestamp') else datetime.now(),
                    }
                else:
                    return None
            else:
                logger.error(f"获取多空比失败: HTTP {response.status_code}")
                return None

        except Exception as e:
            logger.error(f"获取 {symbol} 多空比失败: {e}")
            return None

    async def fetch_long_short_position_ratio(self, symbol: str, period: str = '5m') -> Optional[Dict]:
        """
        获取多空持仓量比率（Top 20%大户的持仓量）

        Args:
            symbol: 交易对
            period: 时间周期 (5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d)

        Returns:
            持仓量比率数据字典
        """
        try:
            # 转换交易对格式
            binance_symbol = symbol.replace('/', '')

            # 使用公开API获取持仓量比
            url = f"{self.base_url}/futures/data/topLongShortPositionRatio"
            params = {
                'symbol': binance_symbol,
                'period': period,
                'limit': 1
            }

            response = await asyncio.to_thread(requests.get, url, params=params)

            if response.status_code == 200:
                data = response.json()

                if data and len(data) > 0:
                    latest = data[0]

                    return {
                        'exchange': self.exchange_id,
                        'symbol': symbol,
                        'long_position': float(latest.get('longAccount', 0)),
                        'short_position': float(latest.get('shortAccount', 0)),
                        'long_short_position_ratio': float(latest.get('longShortRatio', 0)),
                        'timestamp': datetime.fromtimestamp(int(latest.get('timestamp', 0)) / 1000) if latest.get('timestamp') else datetime.now(),
                    }
                else:
                    return None
            else:
                logger.error(f"获取持仓量比失败: HTTP {response.status_code}")
                return None

        except Exception as e:
            logger.error(f"获取 {symbol} 持仓量比失败: {e}")
            return None

    async def fetch_all_data(self, symbol: str, timeframe: str = '1m') -> Dict:
        """
        获取所有合约数据（一次性采集）

        Args:
            symbol: 交易对
            timeframe: K线时间周期

        Returns:
            包含所有数据的字典
        """
        try:
            # 并发获取所有数据（包括账户数比和持仓量比）
            ticker_task = self.fetch_futures_ticker(symbol)
            klines_task = self.fetch_futures_klines(symbol, timeframe, limit=1)
            funding_task = self.fetch_funding_rate(symbol)
            oi_task = self.fetch_open_interest(symbol)
            ls_account_task = self.fetch_long_short_ratio(symbol, period='5m')
            ls_position_task = self.fetch_long_short_position_ratio(symbol, period='5m')

            ticker, klines, funding, oi, ls_account, ls_position = await asyncio.gather(
                ticker_task,
                klines_task,
                funding_task,
                oi_task,
                ls_account_task,
                ls_position_task,
                return_exceptions=True
            )

            result = {
                'symbol': symbol,
                'timestamp': datetime.now(),
                'ticker': ticker if not isinstance(ticker, Exception) else None,
                'kline': klines.iloc[-1].to_dict() if klines is not None and not isinstance(klines, Exception) and len(klines) > 0 else None,
                'funding_rate': funding if not isinstance(funding, Exception) else None,
                'open_interest': oi if not isinstance(oi, Exception) else None,
                'long_short_account_ratio': ls_account if not isinstance(ls_account, Exception) else None,  # 账户数比
                'long_short_position_ratio': ls_position if not isinstance(ls_position, Exception) else None,  # 持仓量比
            }

            return result

        except Exception as e:
            logger.error(f"获取 {symbol} 所有合约数据失败: {e}")
            return {}

    async def get_all_futures_symbols(self) -> List[str]:
        """
        获取所有USDT永续合约交易对

        Returns:
            交易对列表
        """
        try:
            url = f"{self.base_url}/fapi/v1/exchangeInfo"
            response = await asyncio.to_thread(requests.get, url)

            if response.status_code == 200:
                data = response.json()
                symbols = []

                for symbol_info in data.get('symbols', []):
                    # 只选择USDT永续合约
                    if (symbol_info.get('contractType') == 'PERPETUAL' and
                        symbol_info.get('quoteAsset') == 'USDT' and
                        symbol_info.get('status') == 'TRADING'):

                        symbol = symbol_info['symbol']
                        # 转换格式: BTCUSDT -> BTC/USDT
                        base = symbol_info['baseAsset']
                        formatted = f"{base}/USDT"
                        symbols.append(formatted)

                logger.info(f"获取到 {len(symbols)} 个USDT永续合约")
                return symbols
            else:
                logger.error(f"获取合约列表失败: HTTP {response.status_code}")
                return []

        except Exception as e:
            logger.error(f"获取合约列表失败: {e}")
            return []


# 测试代码
async def main():
    """测试合约数据采集器"""

    collector = BinanceFuturesCollector()

    # 测试1: 获取合约ticker
    print("\n=== 测试1: 获取BTC合约ticker ===")
    ticker = await collector.fetch_futures_ticker('BTC/USDT')
    if ticker:
        print(f"交易所: {ticker['exchange']}")
        print(f"价格: ${ticker['price']:,.2f}")
        print(f"24h涨跌: {ticker['price_change_percent']:.2f}%")
        print(f"成交量: {ticker['volume']:,.0f} 张")
        print(f"成交额: ${ticker['quote_volume']:,.0f}")

    # 测试2: 获取K线数据
    print("\n=== 测试2: 获取1分钟K线 ===")
    klines = await collector.fetch_futures_klines('BTC/USDT', '1m', limit=5)
    if klines is not None:
        print(f"获取到 {len(klines)} 条K线")
        print(klines[['timestamp', 'open', 'high', 'low', 'close', 'volume']])

    # 测试3: 获取资金费率
    print("\n=== 测试3: 获取资金费率 ===")
    funding = await collector.fetch_funding_rate('BTC/USDT')
    if funding:
        funding_rate_pct = funding['funding_rate'] * 100
        print(f"当前资金费率: {funding_rate_pct:.4f}%")
        print(f"标记价格: ${funding['mark_price']:,.2f}")
        print(f"指数价格: ${funding['index_price']:,.2f}")
        next_time = datetime.fromtimestamp(funding['next_funding_time'] / 1000)
        print(f"下次结算时间: {next_time}")

    # 测试4: 获取持仓量
    print("\n=== 测试4: 获取持仓量 ===")
    oi = await collector.fetch_open_interest('BTC/USDT')
    if oi:
        print(f"持仓量: {oi['open_interest']:,.0f} 张")

    # 测试5: 获取多空比
    print("\n=== 测试5: 获取多空比 ===")
    ls_ratio = await collector.fetch_long_short_ratio('BTC/USDT')
    if ls_ratio:
        print(f"做多账户比例: {ls_ratio['long_account']:.2%}")
        print(f"做空账户比例: {ls_ratio['short_account']:.2%}")
        print(f"多空比: {ls_ratio['long_short_ratio']:.2f}")

    # 测试6: 获取所有数据
    print("\n=== 测试6: 一次性获取所有数据 ===")
    all_data = await collector.fetch_all_data('ETH/USDT', '1m')
    if all_data.get('ticker'):
        print(f"✓ Ticker数据: ${all_data['ticker']['price']:,.2f}")
    if all_data.get('funding_rate'):
        print(f"✓ 资金费率: {all_data['funding_rate']['funding_rate']*100:.4f}%")
    if all_data.get('open_interest'):
        print(f"✓ 持仓量: {all_data['open_interest']['open_interest']:,.0f}")
    if all_data.get('long_short_ratio'):
        print(f"✓ 多空比: {all_data['long_short_ratio']['long_short_ratio']:.2f}")

    # 测试7: 获取所有合约列表
    print("\n=== 测试7: 获取所有USDT永续合约 ===")
    symbols = await collector.get_all_futures_symbols()
    print(f"总共 {len(symbols)} 个合约")
    print(f"前10个: {symbols[:10]}")

    print("\n✓ 所有测试完成！")


if __name__ == '__main__':
    asyncio.run(main())
