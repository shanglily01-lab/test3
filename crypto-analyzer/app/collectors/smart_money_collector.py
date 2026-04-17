"""
聪明钱地址监控采集器
支持Ethereum, BSC等多链监控
使用Etherscan/BscScan API追踪大户交易
"""

import asyncio
import aiohttp
import sys
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from loguru import logger
from decimal import Decimal

# Windows系统特殊处理 - 修复"信号灯超时"错误
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


class SmartMoneyCollector:
    """聪明钱数据采集器"""

    def __init__(self, config: dict):
        """
        初始化聪明钱采集器

        Args:
            config: 配置字典,包含API密钥和监控地址
        """
        self.config = config
        self.smart_money_config = config.get('smart_money', {})

        # API配置
        self.etherscan_api_key = self.smart_money_config.get('etherscan_api_key', '')
        self.bscscan_api_key = self.smart_money_config.get('bscscan_api_key', '')

        # 代理配置
        self.proxy = self.smart_money_config.get('proxy', None)
        if self.proxy and self.proxy.strip() == '':
            self.proxy = None

        # API端点 - 使用V2版本
        self.etherscan_api = 'https://api.etherscan.io/v2/api'
        self.bscscan_api = 'https://api.bscscan.com/v2/api'  # BSC也升级到V2

        # 监控地址列表
        self.monitored_addresses = self.smart_money_config.get('addresses', [])

        # 最小交易金额阈值(USD)
        self.min_transaction_usd = self.smart_money_config.get('min_transaction_usd', 100000)

        logger.info(f"聪明钱采集器初始化完成 - 监控地址数: {len(self.monitored_addresses)}")
        if self.proxy:
            logger.info(f"使用代理: {self.proxy}")

    def _create_session_with_proxy(self, timeout_seconds: int = 30):
        """
        创建带代理的HTTP会话，针对Windows环境优化

        Args:
            timeout_seconds: 超时时间（秒）

        Returns:
            配置好的ClientSession
        """
        timeout = aiohttp.ClientTimeout(total=timeout_seconds, connect=10, sock_read=15)

        # 创建连接器，针对Windows + 代理优化
        connector = aiohttp.TCPConnector(
            ssl=False,  # 禁用SSL验证（通过代理时）
            limit=10,   # 限制并发连接数
            force_close=True,  # 每次请求后关闭连接，避免连接池问题
            enable_cleanup_closed=True  # 启用清理已关闭的连接
        )

        return aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            trust_env=True  # 信任环境变量中的代理设置
        )

    async def fetch_address_transactions(
        self,
        address: str,
        blockchain: str = 'ethereum',
        start_block: int = 0,
        limit: int = 100
    ) -> List[Dict]:
        """
        获取地址的交易历史

        Args:
            address: 区块链地址
            blockchain: 区块链网络(ethereum, bsc)
            start_block: 起始区块号
            limit: 返回数量限制

        Returns:
            交易列表
        """
        try:
            if blockchain == 'ethereum':
                api_url = self.etherscan_api
                api_key = self.etherscan_api_key
            elif blockchain == 'bsc':
                api_url = self.bscscan_api
                api_key = self.bscscan_api_key
            else:
                logger.error(f"不支持的区块链: {blockchain}")
                return []

            if not api_key:
                logger.warning(f"{blockchain} API密钥未配置")
                return []

            # 获取普通交易
            params = {
                'chainid': 1,  # 以太坊主网
                'module': 'account',
                'action': 'txlist',
                'address': address,
                'startblock': start_block,
                'endblock': 99999999,
                'page': 1,
                'offset': limit,
                'sort': 'desc',
                'apikey': api_key
            }

            # BSC使用chainid 56
            if blockchain == 'bsc':
                params['chainid'] = 56

            # 使用优化的会话配置
            async with self._create_session_with_proxy() as session:
                try:
                    async with session.get(api_url, params=params, proxy=self.proxy) as response:
                        if response.status == 200:
                            data = await response.json()

                            if data.get('status') == '1' and data.get('message') == 'OK':
                                transactions = data.get('result', [])
                                logger.info(f"获取 {address[:10]}... 交易记录: {len(transactions)} 条")
                                return transactions
                            else:
                                logger.warning(f"API返回错误: {data.get('message')}")
                                return []
                        else:
                            logger.error(f"请求失败: HTTP {response.status}")
                            return []
                except (asyncio.CancelledError, asyncio.TimeoutError) as e:
                    logger.warning(f"获取交易请求被取消或超时: {type(e).__name__}")
                    return []

        except asyncio.CancelledError:
            logger.warning(f"获取交易任务被取消")
            return []
        except Exception as e:
            logger.error(f"获取地址交易失败: {e}")
            return []

    async def fetch_erc20_transfers(
        self,
        address: str,
        blockchain: str = 'ethereum',
        contract_address: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict]:
        """
        获取ERC20代币转账记录

        Args:
            address: 钱包地址
            blockchain: 区块链网络
            contract_address: 代币合约地址(可选,不指定则获取所有)
            limit: 返回数量限制

        Returns:
            代币转账列表
        """
        try:
            if blockchain == 'ethereum':
                api_url = self.etherscan_api
                api_key = self.etherscan_api_key
            elif blockchain == 'bsc':
                api_url = self.bscscan_api
                api_key = self.bscscan_api_key
            else:
                return []

            if not api_key:
                return []

            params = {
                'chainid': 1,  # 以太坊主网
                'module': 'account',
                'action': 'tokentx',
                'address': address,
                'page': 1,
                'offset': limit,
                'sort': 'desc',
                'apikey': api_key
            }

            # BSC使用chainid 56
            if blockchain == 'bsc':
                params['chainid'] = 56

            # 如果指定了代币合约,添加过滤
            if contract_address:
                params['contractaddress'] = contract_address

            # 使用优化的会话配置
            async with self._create_session_with_proxy() as session:
                try:
                    async with session.get(api_url, params=params, proxy=self.proxy) as response:
                        if response.status == 200:
                            data = await response.json()

                            if data.get('status') == '1':
                                transfers = data.get('result', [])
                                logger.info(f"获取 {address[:10]}... ERC20转账: {len(transfers)} 条")
                                return transfers
                            else:
                                return []
                        else:
                            return []
                except (asyncio.CancelledError, asyncio.TimeoutError) as e:
                    logger.warning(f"获取ERC20转账请求被取消或超时: {type(e).__name__}")
                    return []

        except asyncio.CancelledError:
            logger.warning(f"获取ERC20转账任务被取消")
            return []
        except Exception as e:
            logger.error(f"获取ERC20转账失败: {e}")
            return []

    async def analyze_transaction(
        self,
        tx: Dict,
        address: str,
        blockchain: str
    ) -> Optional[Dict]:
        """
        分析单笔交易,提取关键信息

        Args:
            tx: 交易原始数据
            address: 监控的地址
            blockchain: 区块链网络

        Returns:
            结构化的交易数据
        """
        try:
            # 判断是ERC20转账还是ETH转账
            is_erc20 = 'tokenSymbol' in tx

            if is_erc20:
                # ERC20代币转账
                token_symbol = tx.get('tokenSymbol', 'UNKNOWN')
                token_name = tx.get('tokenName', '')
                token_address = tx.get('contractAddress', '')
                decimals = int(tx.get('tokenDecimal', 18))

                # 计算代币数量
                value = Decimal(tx.get('value', 0))
                amount = value / (10 ** decimals)

            else:
                # ETH/BNB转账
                token_symbol = 'ETH' if blockchain == 'ethereum' else 'BNB'
                token_name = 'Ethereum' if blockchain == 'ethereum' else 'Binance Coin'
                token_address = '0x0000000000000000000000000000000000000000'  # Native token

                value = Decimal(tx.get('value', 0))
                amount = value / Decimal(10 ** 18)  # Wei to ETH/BNB

            from_addr = tx.get('from', '').lower()
            to_addr = tx.get('to', '').lower()
            address_lower = address.lower()

            # 判断买入/卖出
            if from_addr == address_lower:
                action = 'sell'  # 地址发出 = 卖出
            elif to_addr == address_lower:
                action = 'buy'   # 地址接收 = 买入
            else:
                action = 'transfer'

            # 时间戳转换
            block_timestamp = int(tx.get('timeStamp', 0))
            timestamp = datetime.fromtimestamp(block_timestamp)

            # Gas信息
            gas_used = int(tx.get('gasUsed', 0))
            gas_price = int(tx.get('gasPrice', 0))
            tx_fee = (gas_used * gas_price) / (10 ** 18)

            transaction_data = {
                'tx_hash': tx.get('hash', ''),
                'address': address,
                'blockchain': blockchain,
                'token_address': token_address,
                'token_symbol': token_symbol,
                'token_name': token_name,
                'action': action,
                'amount': float(amount),
                'amount_usd': None,  # 需要后续通过价格API计算
                'price_usd': None,
                'from_address': from_addr,
                'to_address': to_addr,
                'block_number': int(tx.get('blockNumber', 0)),
                'block_timestamp': block_timestamp,
                'timestamp': timestamp,
                'gas_used': gas_used,
                'gas_price': gas_price,
                'transaction_fee': float(tx_fee),
                'is_large_transaction': False,  # 后续根据USD价值判断
                'is_first_buy': False,
                'signal_strength': 'weak'
            }

            return transaction_data

        except Exception as e:
            logger.error(f"分析交易失败: {e}")
            return None

    async def get_token_price(self, token_symbol: str, blockchain: str = 'ethereum') -> Optional[float]:
        """
        获取代币当前价格(USD)

        使用CoinGecko API获取价格

        Args:
            token_symbol: 代币符号
            blockchain: 区块链网络

        Returns:
            价格(USD)或None
        """
        try:
            # CoinGecko API不需要密钥
            api_url = 'https://api.coingecko.com/api/v3/simple/price'

            # 代币ID映射
            token_id_map = {
                'ETH': 'ethereum',
                'BNB': 'binancecoin',
                'BTC': 'bitcoin',
                'USDT': 'tether',
                'USDC': 'usd-coin',
                'DAI': 'dai',
                'WETH': 'ethereum',
                'WBNB': 'binancecoin'
            }

            token_id = token_id_map.get(token_symbol.upper(), token_symbol.lower())

            params = {
                'ids': token_id,
                'vs_currencies': 'usd'
            }

            # 设置超时和SSL配置
            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(api_url, params=params, proxy=self.proxy, ssl=False) as response:
                    if response.status == 200:
                        data = await response.json()
                        price = data.get(token_id, {}).get('usd')
                        return float(price) if price else None
                    else:
                        return None

        except Exception as e:
            logger.error(f"获取代币价格失败 {token_symbol}: {e}")
            return None

    async def enrich_transaction_with_price(self, tx_data: Dict) -> Dict:
        """
        为交易数据补充价格和USD金额

        Args:
            tx_data: 交易数据

        Returns:
            补充后的交易数据
        """
        try:
            token_symbol = tx_data.get('token_symbol')
            amount = tx_data.get('amount', 0)

            # 获取代币价格
            price = await self.get_token_price(token_symbol, tx_data.get('blockchain'))

            if price:
                tx_data['price_usd'] = price
                tx_data['amount_usd'] = amount * price

                # 判断是否大额交易
                if tx_data['amount_usd'] >= self.min_transaction_usd:
                    tx_data['is_large_transaction'] = True
                    tx_data['signal_strength'] = 'strong'
                elif tx_data['amount_usd'] >= self.min_transaction_usd / 2:
                    tx_data['signal_strength'] = 'medium'

            return tx_data

        except Exception as e:
            logger.error(f"补充交易价格失败: {e}")
            return tx_data

    async def monitor_address(
        self,
        address: str,
        blockchain: str = 'ethereum',
        hours: int = 24
    ) -> List[Dict]:
        """
        监控单个地址的最近活动

        Args:
            address: 钱包地址
            blockchain: 区块链网络
            hours: 监控时间范围(小时)

        Returns:
            交易列表
        """
        try:
            logger.info(f"开始监控地址: {address[:10]}... ({blockchain})")

            # 获取ERC20转账(代币交易更重要)
            erc20_transfers = await self.fetch_erc20_transfers(address, blockchain, limit=50)

            logger.info(f"API返回 {len(erc20_transfers)} 笔ERC20转账")

            # 过滤最近N小时的交易
            cutoff_time = datetime.now() - timedelta(hours=hours)
            recent_transactions = []

            logger.info(f"时间过滤: 只保留 {cutoff_time.strftime('%Y-%m-%d %H:%M:%S')} 之后的交易")

            for tx in erc20_transfers:
                tx_time = datetime.fromtimestamp(int(tx.get('timeStamp', 0)))

                logger.debug(f"交易时间: {tx_time.strftime('%Y-%m-%d %H:%M:%S')}, 代币: {tx.get('tokenSymbol', 'UNKNOWN')}")

                if tx_time >= cutoff_time:
                    # 分析交易
                    tx_data = await self.analyze_transaction(tx, address, blockchain)

                    if tx_data:
                        # 补充价格信息
                        tx_data = await self.enrich_transaction_with_price(tx_data)
                        recent_transactions.append(tx_data)

                        # 小延迟,避免API限流
                        await asyncio.sleep(0.2)

            logger.info(f"地址 {address[:10]}... 最近{hours}小时交易: {len(recent_transactions)} 笔")
            return recent_transactions

        except Exception as e:
            logger.error(f"监控地址失败 {address}: {e}")
            return []

    async def monitor_all_addresses(self, hours: int = 24) -> Dict[str, List[Dict]]:
        """
        监控所有配置的聪明钱地址

        Args:
            hours: 监控时间范围(小时)

        Returns:
            {address: [transactions]}
        """
        results = {}

        for addr_config in self.monitored_addresses:
            address = addr_config.get('address')
            blockchain = addr_config.get('blockchain', 'ethereum')

            if not address:
                continue

            try:
                transactions = await self.monitor_address(address, blockchain, hours)
                results[address] = transactions

                # 延迟避免API限流
                await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"监控地址 {address} 失败: {e}")
                results[address] = []

        return results

    def generate_signal(self, transactions: List[Dict], token_symbol: str) -> Optional[Dict]:
        """
        基于交易活动生成投资信号

        Args:
            transactions: 交易列表
            token_symbol: 代币符号

        Returns:
            信号数据或None
        """
        try:
            if not transactions:
                return None

            # 过滤特定代币的交易
            token_txs = [tx for tx in transactions if tx.get('token_symbol') == token_symbol]

            if not token_txs:
                return None

            # 统计买卖情况
            buy_txs = [tx for tx in token_txs if tx.get('action') == 'buy']
            sell_txs = [tx for tx in token_txs if tx.get('action') == 'sell']

            total_buy_usd = sum(tx.get('amount_usd', 0) for tx in buy_txs)
            total_sell_usd = sum(tx.get('amount_usd', 0) for tx in sell_txs)
            net_flow = total_buy_usd - total_sell_usd

            # 参与地址数(去重)
            addresses = set(tx.get('address') for tx in token_txs)

            # 判断信号类型
            if net_flow > 0:
                if len(buy_txs) >= 3:  # 多个地址买入
                    signal_type = 'ACCUMULATION'  # 积累
                else:
                    signal_type = 'BUY'
            elif net_flow < 0:
                if len(sell_txs) >= 3:
                    signal_type = 'DISTRIBUTION'  # 分发
                else:
                    signal_type = 'SELL'
            else:
                return None  # 无明显信号

            # 计算信号强度
            if abs(net_flow) > 1000000:  # >$1M
                signal_strength = 'STRONG'
                confidence = 90
            elif abs(net_flow) > 500000:  # >$500K
                signal_strength = 'MEDIUM'
                confidence = 70
            else:
                signal_strength = 'WEAK'
                confidence = 50

            # 构建信号
            signal = {
                'token_symbol': token_symbol,
                'signal_type': signal_type,
                'signal_strength': signal_strength,
                'confidence_score': confidence,
                'smart_money_count': len(addresses),
                'total_buy_amount_usd': total_buy_usd,
                'total_sell_amount_usd': total_sell_usd,
                'net_flow_usd': net_flow,
                'transaction_count': len(token_txs),
                'signal_start_time': min(tx['timestamp'] for tx in token_txs),
                'signal_end_time': max(tx['timestamp'] for tx in token_txs),
                'timestamp': datetime.now(),
                'related_tx_hashes': ','.join(tx['tx_hash'] for tx in token_txs[:10]),  # 最多10个
                'top_addresses': ','.join(list(addresses)[:5]),  # 前5个地址
                'is_active': True,
                'is_verified': False
            }

            logger.info(f"生成信号: {token_symbol} - {signal_type} ({signal_strength}), 净流入: ${net_flow:,.2f}")
            return signal

        except Exception as e:
            logger.error(f"生成信号失败: {e}")
            return None
