"""
Hyperliquid 数据采集器
监控 Hyperliquid DEX 上的大户交易和聪明钱地址
支持：仓位追踪、大额交易、PnL分析、清算数据
"""

import asyncio
import aiohttp
import sys
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from loguru import logger
from decimal import Decimal

# Windows系统特殊处理
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


class HyperliquidCollector:
    """Hyperliquid 数据采集器"""

    def __init__(self, config: dict):
        """
        初始化 Hyperliquid 采集器

        Args:
            config: 配置字典
        """
        self.config = config
        self.hyperliquid_config = config.get('hyperliquid', {})

        # API端点
        self.api_url = 'https://api.hyperliquid.xyz/info'  # 公开API，无需密钥

        # 代理配置
        self.proxy = config.get('smart_money', {}).get('proxy', None)
        if self.proxy and self.proxy.strip() == '':
            self.proxy = None

        # 监控地址列表
        self.monitored_addresses = self.hyperliquid_config.get('addresses') or []

        # 最小交易金额阈值(USD)
        self.min_trade_usd = self.hyperliquid_config.get('min_trade_usd', 50000)

        logger.info(f"Hyperliquid 采集器初始化完成 - 配置地址数: {len(self.monitored_addresses)}")
        logger.info(f"将从数据库动态加载更多监控地址")
        if self.proxy:
            logger.info(f"使用代理: {self.proxy}")

    async def fetch_user_state(self, address: str, max_retries: int = 3) -> Optional[Dict]:
        """
        获取用户当前状态（持仓、余额等）- 带重试机制

        Args:
            address: 用户地址
            max_retries: 最大重试次数

        Returns:
            用户状态数据
        """
        payload = {
            "type": "clearinghouseState",
            "user": address
        }

        for attempt in range(max_retries):
            try:
                timeout = aiohttp.ClientTimeout(total=20, connect=8)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.post(self.api_url, json=payload, proxy=self.proxy, ssl=False) as response:
                        if response.status == 200:
                            data = await response.json()
                            logger.debug(f"获取用户 {address[:10]}... 状态成功")
                            return data
                        elif response.status == 429:
                            # 速率限制，使用更长的延迟
                            logger.warning(f"获取用户状态失败: HTTP 429 速率限制 (尝试 {attempt + 1}/{max_retries})")
                            if attempt < max_retries - 1:
                                wait_time = 5 * (attempt + 1)
                                logger.info(f"等待 {wait_time} 秒后重试...")
                                await asyncio.sleep(wait_time)
                            continue
                        else:
                            logger.warning(f"获取用户状态失败: HTTP {response.status} (尝试 {attempt + 1}/{max_retries})")
                            if attempt < max_retries - 1:
                                await asyncio.sleep(2 ** attempt)  # 指数退避
                            continue

            except asyncio.TimeoutError:
                logger.warning(f"获取用户状态超时 (尝试 {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                continue
            except asyncio.CancelledError:
                logger.warning(f"请求被取消，跳过地址 {address[:10]}...")
                return None
            except Exception as e:
                logger.warning(f"获取用户状态异常: {e} (尝试 {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                continue

        logger.error(f"获取用户 {address[:10]}... 状态失败，已重试 {max_retries} 次")
        return None

    async def fetch_user_fills(self, address: str, limit: int = 100, max_retries: int = 3) -> List[Dict]:
        """
        获取用户的成交记录（最近的交易）- 带重试机制

        Args:
            address: 用户地址
            limit: 返回数量
            max_retries: 最大重试次数

        Returns:
            成交记录列表
        """
        payload = {
            "type": "userFills",
            "user": address
        }

        for attempt in range(max_retries):
            try:
                timeout = aiohttp.ClientTimeout(total=20, connect=8)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.post(self.api_url, json=payload, proxy=self.proxy, ssl=False) as response:
                        if response.status == 200:
                            fills = await response.json()
                            logger.info(f"获取 {address[:10]}... 成交记录: {len(fills)} 笔")
                            return fills[:limit] if fills else []
                        elif response.status == 429:
                            # 速率限制，使用更长的延迟
                            logger.warning(f"获取成交记录失败: HTTP 429 速率限制 (尝试 {attempt + 1}/{max_retries})")
                            if attempt < max_retries - 1:
                                wait_time = 5 * (attempt + 1)  # 5秒、10秒、15秒
                                logger.info(f"等待 {wait_time} 秒后重试...")
                                await asyncio.sleep(wait_time)
                            continue
                        else:
                            logger.warning(f"获取成交记录失败: HTTP {response.status} (尝试 {attempt + 1}/{max_retries})")
                            if attempt < max_retries - 1:
                                await asyncio.sleep(2 ** attempt)
                            continue

            except asyncio.TimeoutError:
                logger.warning(f"获取成交记录超时 (尝试 {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                continue
            except asyncio.CancelledError:
                logger.warning(f"请求被取消，跳过地址 {address[:10]}... 的成交记录")
                return []
            except Exception as e:
                logger.warning(f"获取成交记录异常: {e} (尝试 {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt)
                continue

        logger.error(f"获取 {address[:10]}... 成交记录失败，已重试 {max_retries} 次")
        return []

    async def fetch_user_funding_history(self, address: str) -> List[Dict]:
        """
        获取用户的资金费率历史

        Args:
            address: 用户地址

        Returns:
            资金费率记录
        """
        try:
            payload = {
                "type": "userFunding",
                "user": address
            }

            timeout = aiohttp.ClientTimeout(total=30, connect=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(self.api_url, json=payload, proxy=self.proxy, ssl=False) as response:
                    if response.status == 200:
                        funding = await response.json()
                        return funding if funding else []
                    else:
                        return []

        except Exception as e:
            logger.error(f"获取资金费率历史异常: {e}")
            return []

    async def fetch_leaderboard(self, period: str = "day") -> List[Dict]:
        """
        获取PnL排行榜（发现聪明钱地址）

        Args:
            period: 周期 (day, week, month, allTime)

        Returns:
            排行榜数据
        """
        try:
            # Hyperliquid 排行榜使用独立的 stats-data API
            # URL: https://stats-data.hyperliquid.xyz/Mainnet/leaderboard
            # 方法: GET (不是 POST!)
            leaderboard_url = 'https://stats-data.hyperliquid.xyz/Mainnet/leaderboard'

            # 增加超时时间，改善网络兼容性
            timeout = aiohttp.ClientTimeout(total=60, connect=20, sock_read=30)

            # 创建连接器，允许更灵活的 DNS 解析
            connector = aiohttp.TCPConnector(
                ssl=False,
                force_close=True,  # 每次请求后关闭连接
                limit=10,
                ttl_dns_cache=300
            )

            async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
                logger.debug(f"正在请求 Hyperliquid 排行榜: {leaderboard_url}")

                async with session.get(
                    leaderboard_url,
                    proxy=self.proxy,
                    ssl=False,
                    allow_redirects=True
                ) as response:
                    if response.status == 200:
                        data = await response.json()

                        # 提取 leaderboardRows 数组（直接包含所有交易者）
                        leaderboard = data.get('leaderboardRows', [])

                        logger.info(f"✓ 获取排行榜: {len(leaderboard)} 个交易者")
                        return leaderboard if leaderboard else []
                    else:
                        # 记录详细的错误信息
                        error_text = await response.text()
                        logger.error(f"获取排行榜失败: HTTP {response.status}, 响应: {error_text}")
                        return []

        except asyncio.TimeoutError:
            logger.error(f"获取排行榜超时（网络连接慢或 API 不可达）")
            return []
        except asyncio.CancelledError:
            logger.warning(f"获取排行榜被取消（可能是程序关闭）")
            return []
        except aiohttp.ClientError as e:
            logger.error(f"获取排行榜网络错误: {e}")
            return []
        except Exception as e:
            logger.error(f"获取排行榜异常: {type(e).__name__}: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return []

    def analyze_fill(self, fill: Dict) -> Optional[Dict]:
        """
        分析单笔成交记录

        Args:
            fill: 成交数据

        Returns:
            分析后的交易数据
        """
        try:
            # 提取关键信息
            coin = fill.get('coin', '')
            side = fill.get('side', '')  # 'A' = Ask (卖出), 'B' = Bid (买入)
            px = float(fill.get('px', 0))  # 成交价格
            sz = float(fill.get('sz', 0))  # 成交数量
            time_ms = fill.get('time', 0)
            closed_pnl = float(fill.get('closedPnl', 0))  # 已实现盈亏

            # 计算交易金额 (USD)
            notional_usd = px * sz

            # 判断方向
            if side == 'B':
                action = 'LONG'  # 做多（买入）
                action_cn = '做多'
            elif side == 'A':
                action = 'SHORT'  # 做空（卖出）
                action_cn = '做空'
            else:
                action = 'UNKNOWN'
                action_cn = '未知'

            # 时间转换（时间戳转换为datetime）
            timestamp = datetime.fromtimestamp(time_ms / 1000)

            # 构建交易数据
            trade_data = {
                'coin': coin,
                'action': action,
                'action_cn': action_cn,
                'side': side,
                'price': px,
                'size': sz,
                'notional_usd': notional_usd,
                'closed_pnl': closed_pnl,
                'timestamp': timestamp,
                'time_ms': time_ms,
                'is_large_trade': notional_usd >= self.min_trade_usd,
                'raw_data': fill
            }

            return trade_data

        except Exception as e:
            logger.error(f"分析成交记录失败: {e}")
            return None

    async def monitor_address(
        self,
        address: str,
        hours: int = 24
    ) -> Dict:
        """
        监控单个地址的交易活动

        Args:
            address: 用户地址
            hours: 时间范围（小时）

        Returns:
            地址数据（状态、交易、统计）
        """
        try:
            logger.info(f"开始监控 Hyperliquid 地址: {address[:10]}...")

            # 1. 获取用户状态（当前持仓）
            user_state = await self.fetch_user_state(address)

            # 2. 获取成交记录
            fills = await self.fetch_user_fills(address, limit=100)

            # 3. 过滤时间范围
            cutoff_time = datetime.now() - timedelta(hours=hours)
            recent_trades = []

            for fill in fills:
                trade_data = self.analyze_fill(fill)
                if trade_data and trade_data['timestamp'] >= cutoff_time:
                    recent_trades.append(trade_data)

            # 4. 统计分析
            long_trades = [t for t in recent_trades if t['action'] == 'LONG']
            short_trades = [t for t in recent_trades if t['action'] == 'SHORT']
            large_trades = [t for t in recent_trades if t['is_large_trade']]

            total_long_usd = sum(t['notional_usd'] for t in long_trades)
            total_short_usd = sum(t['notional_usd'] for t in short_trades)
            total_pnl = sum(t['closed_pnl'] for t in recent_trades)

            # 5. 提取当前持仓
            positions = []
            if user_state and 'assetPositions' in user_state:
                # 获取账户保证金信息（用于计算杠杆）
                margin_summary = user_state.get('marginSummary', {})
                account_value = float(margin_summary.get('accountValue', 0)) if margin_summary else 0
                
                for pos in user_state['assetPositions']:
                    position = pos.get('position', {})
                    coin = position.get('coin', '')
                    szi = float(position.get('szi', 0))  # 持仓数量（带符号，正=多，负=空）
                    entry_px = float(position.get('entryPx', 0))
                    unrealized_pnl = float(position.get('unrealizedPnl', 0))
                    
                    # 计算名义价值
                    notional_usd = abs(szi) * entry_px
                    
                    # 计算杠杆倍数：杠杆 = 名义价值 / 保证金
                    # Hyperliquid API 可能不直接提供每个持仓的杠杆，需要从账户级别计算
                    leverage = 1.0  # 默认值
                    if notional_usd > 0:
                        # 尝试从 position 中获取 margin 信息
                        margin_used = float(position.get('marginUsed', 0))
                        if margin_used > 0:
                            leverage = notional_usd / margin_used
                        else:
                            # 如果没有 marginUsed，尝试从 accountValue 和总持仓价值估算
                            # 这是一个近似值，可能不够准确
                            total_notional = sum(abs(float(p.get('position', {}).get('szi', 0)) * float(p.get('position', {}).get('entryPx', 0))) 
                                                for p in user_state.get('assetPositions', []))
                            if total_notional > 0 and account_value > 0:
                                # 估算：假设所有持仓使用相同的杠杆比例
                                estimated_leverage = total_notional / account_value
                                leverage = max(1.0, min(estimated_leverage, 50.0))  # 限制在1-50倍之间

                    if szi != 0:  # 只记录非零持仓
                        positions.append({
                            'coin': coin,
                            'size': abs(szi),
                            'side': 'LONG' if szi > 0 else 'SHORT',
                            'entry_price': entry_px,
                            'unrealized_pnl': unrealized_pnl,
                            'notional_usd': notional_usd,
                            'leverage': round(leverage, 2)  # 保留2位小数
                        })

            # 6. 构建返回数据
            result = {
                'address': address,
                'timestamp': datetime.now(),
                'hours': hours,
                'positions': positions,
                'recent_trades': recent_trades,
                'statistics': {
                    'total_trades': len(recent_trades),
                    'long_trades': len(long_trades),
                    'short_trades': len(short_trades),
                    'large_trades': len(large_trades),
                    'total_long_usd': total_long_usd,
                    'total_short_usd': total_short_usd,
                    'net_flow_usd': total_long_usd - total_short_usd,
                    'total_pnl': total_pnl,
                    'active_positions': len(positions)
                }
            }

            logger.info(f"地址 {address[:10]}... 最近{hours}小时: {len(recent_trades)} 笔交易, {len(positions)} 个持仓")

            return result

        except Exception as e:
            logger.error(f"监控地址失败: {e}")
            return {
                'address': address,
                'timestamp': datetime.now(),
                'error': str(e),
                'positions': [],
                'recent_trades': [],
                'statistics': {}
            }

    async def monitor_all_addresses(
        self,
        hours: int = 24,
        priority: str = 'all',
        hyperliquid_db=None
    ) -> Dict[str, Dict]:
        """
        监控 Hyperliquid 聪明钱地址 (支持从数据库加载)

        Args:
            hours: 时间范围（小时）
            priority: 优先级过滤
                - 'high': 高优先级 (PnL>10K, ROI>50%, 7天内活跃, 限200个)
                - 'medium': 中优先级 (PnL>5K, ROI>30%, 30天内活跃, 限500个)
                - 'low': 低优先级 (全部活跃钱包)
                - 'all': 所有钱包 (等同于low)
                - 'config': 只监控配置文件中的地址
            hyperliquid_db: Hyperliquid数据库实例 (如果不提供则从配置文件读取地址)

        Returns:
            {address: result}
        """
        results = {}
        addresses_to_monitor = []

        # 根据优先级获取地址列表
        if priority == 'config' or hyperliquid_db is None:
            # 只使用配置文件中的地址 (向后兼容)
            addresses_to_monitor = [
                addr.get('address')
                for addr in self.monitored_addresses
                if addr.get('address')
            ]
            logger.info(f"从配置文件加载 {len(addresses_to_monitor)} 个地址")

        else:
            # 从数据库获取地址
            try:
                if priority == 'high':
                    # 高优先级: PnL>10K, ROI>50%, 7天内活跃, 限200个
                    db_wallets = hyperliquid_db.get_monitored_wallets_by_priority(
                        min_pnl=10000,
                        min_roi=50,
                        days_active=7,
                        limit=200
                    )
                    logger.info(f"从数据库加载 {len(db_wallets)} 个高优先级地址 (PnL>10K, ROI>50%, 7天内活跃)")

                elif priority == 'medium':
                    # 中优先级: PnL>5K, ROI>30%, 30天内活跃, 限500个
                    db_wallets = hyperliquid_db.get_monitored_wallets_by_priority(
                        min_pnl=5000,
                        min_roi=30,
                        days_active=30,
                        limit=500
                    )
                    logger.info(f"从数据库加载 {len(db_wallets)} 个中优先级地址 (PnL>5K, ROI>30%, 30天内活跃)")

                else:
                    # 低优先级/全部: 所有活跃钱包
                    db_wallets = hyperliquid_db.get_monitored_wallets(active_only=True)
                    logger.info(f"从数据库加载 {len(db_wallets)} 个活跃钱包 (全量)")

                # 提取地址
                addresses_to_monitor = [wallet['address'] for wallet in db_wallets]

            except Exception as e:
                logger.error(f"从数据库加载地址失败: {e}, 回退到配置文件")
                addresses_to_monitor = [
                    addr.get('address')
                    for addr in self.monitored_addresses
                    if addr.get('address')
                ]

        if not addresses_to_monitor:
            logger.warning("没有找到需要监控的地址")
            return results

        # 监控所有地址
        logger.info(f"开始监控 {len(addresses_to_monitor)} 个地址, 回溯 {hours} 小时")

        for i, address in enumerate(addresses_to_monitor, 1):
            if not address:
                continue

            try:
                result = await self.monitor_address(address, hours)
                results[address] = result

                if i % 50 == 0:
                    logger.info(f"  进度: {i}/{len(addresses_to_monitor)} 个地址已监控")

                # 延迟避免API限流
                await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"监控地址 {address[:10]}... 失败: {e}")
                results[address] = {
                    'address': address,
                    'error': str(e),
                    'positions': [],
                    'recent_trades': []
                }

        logger.info(f"监控完成: {len(results)} 个地址")
        return results

    def generate_signal(self, address_data: Dict, coin: str) -> Optional[Dict]:
        """
        基于地址活动生成交易信号

        Args:
            address_data: 地址监控数据
            coin: 币种符号

        Returns:
            信号数据
        """
        try:
            trades = address_data.get('recent_trades', [])
            positions = address_data.get('positions', [])

            # 过滤特定币种的交易
            coin_trades = [t for t in trades if t['coin'] == coin]

            if not coin_trades:
                return None

            # 统计
            long_trades = [t for t in coin_trades if t['action'] == 'LONG']
            short_trades = [t for t in coin_trades if t['action'] == 'SHORT']

            total_long = sum(t['notional_usd'] for t in long_trades)
            total_short = sum(t['notional_usd'] for t in short_trades)
            net_flow = total_long - total_short
            total_pnl = sum(t['closed_pnl'] for t in coin_trades)

            # 检查当前持仓
            current_position = next((p for p in positions if p['coin'] == coin), None)

            # 判断信号类型
            if net_flow > 0:
                if len(long_trades) >= 3 or (current_position and current_position['side'] == 'LONG'):
                    signal_type = 'ACCUMULATION_LONG'  # 积累做多
                else:
                    signal_type = 'LONG'
            elif net_flow < 0:
                if len(short_trades) >= 3 or (current_position and current_position['side'] == 'SHORT'):
                    signal_type = 'ACCUMULATION_SHORT'  # 积累做空
                else:
                    signal_type = 'SHORT'
            else:
                return None

            # 计算信号强度
            if abs(net_flow) > 500000:  # >$500K
                signal_strength = 'STRONG'
                confidence = 85
            elif abs(net_flow) > 100000:  # >$100K
                signal_strength = 'MEDIUM'
                confidence = 70
            else:
                signal_strength = 'WEAK'
                confidence = 50

            # PnL影响置信度
            if total_pnl > 0:
                confidence += 10  # 盈利交易者更可信
            elif total_pnl < 0:
                confidence -= 10

            confidence = max(0, min(100, confidence))

            # 构建信号
            signal = {
                'source': 'hyperliquid',
                'coin': coin,
                'signal_type': signal_type,
                'signal_strength': signal_strength,
                'confidence_score': confidence,
                'trader_address': address_data.get('address'),
                'total_long_usd': total_long,
                'total_short_usd': total_short,
                'net_flow_usd': net_flow,
                'trade_count': len(coin_trades),
                'total_pnl': total_pnl,
                'current_position': current_position,
                'timestamp': datetime.now()
            }

            logger.info(f"生成信号: {coin} - {signal_type} ({signal_strength}), 净流入: ${net_flow:,.2f}, PnL: ${total_pnl:,.2f}")
            return signal

        except Exception as e:
            logger.error(f"生成信号失败: {e}")
            return None

    async def discover_smart_traders(self, period: str = "week", min_pnl: float = 10000) -> List[Dict]:
        """
        从排行榜发现聪明交易者

        Args:
            period: 周期 (day, week, month, allTime)
            min_pnl: 最低PnL要求

        Returns:
            聪明交易者列表
        """
        try:
            logger.info(f"发现 {period} 排行榜上的聪明交易者...")

            leaderboard = await self.fetch_leaderboard(period)

            smart_traders = []
            for entry in leaderboard:
                account_value = float(entry.get('accountValue', 0))
                user = entry.get('ethAddress', '')

                # 从 windowPerformances 中提取指定周期的数据
                # windowPerformances 格式: [["day", {...}], ["week", {...}], ["month", {...}], ["allTime", {...}]]
                window_performances = entry.get('windowPerformances', [])

                # 找到指定周期的数据
                period_data = None
                for window in window_performances:
                    if len(window) == 2 and window[0] == period:
                        period_data = window[1]
                        break

                if not period_data:
                    continue

                pnl = float(period_data.get('pnl', 0))
                roi_decimal = float(period_data.get('roi', 0))  # API返回的是小数形式，例如 0.01 = 1%
                vlm = float(period_data.get('vlm', 0))  # 交易量

                # 筛选条件
                if pnl >= min_pnl and account_value > 0:
                    trader = {
                        'address': user,
                        'pnl': pnl,
                        'account_value': account_value,
                        'roi': roi_decimal * 100,  # 转换为百分比
                        'volume': vlm,
                        'period': period,
                        'discovered_at': datetime.now()
                    }

                    smart_traders.append(trader)

            logger.info(f"发现 {len(smart_traders)} 个符合条件的聪明交易者")
            return smart_traders

        except Exception as e:
            logger.error(f"发现聪明交易者失败: {e}")
            return []

    async def fetch_top_smart_money_trades_24h(
        self,
        top_n: int = 100,
        min_trade_usd: float = 50000,
        hours: int = 24
    ) -> List[Dict]:
        """
        抓取前N名聪明钱在24小时内的所有交易

        Args:
            top_n: 取排行榜前N名（默认100）
            min_trade_usd: 最小交易金额阈值（USD）
            hours: 时间窗口（小时）

        Returns:
            交易列表
        """
        try:
            logger.info(f"开始抓取前 {top_n} 名聪明钱的 {hours}h 交易数据（单笔≥${min_trade_usd:,.0f}）")

            # 1. 从排行榜获取聪明交易者（使用week榜单，更稳定）
            smart_traders = await self.discover_smart_traders(period="week", min_pnl=10000)

            if not smart_traders:
                logger.warning("未能获取聪明交易者列表")
                return []

            # 2. 取前N名，按PnL排序
            top_traders = sorted(smart_traders, key=lambda x: x['pnl'], reverse=True)[:top_n]
            logger.info(f"筛选出前 {len(top_traders)} 名交易者")

            # 3. 批量抓取这些地址的成交记录
            all_trades = []
            cutoff_time = datetime.now() - timedelta(hours=hours)

            total_fills = 0
            total_analyzed = 0
            total_time_filtered = 0
            total_amount_filtered = 0

            for i, trader in enumerate(top_traders, 1):
                address = trader['address']

                try:
                    # 抓取成交记录（限制100笔，API限制）
                    fills = await self.fetch_user_fills(address, limit=100)
                    total_fills += len(fills)

                    if not fills:
                        continue

                    # 过滤：时间窗口 + 最小金额
                    for fill in fills:
                        fill_data = self.analyze_fill(fill)

                        if not fill_data:
                            continue

                        total_analyzed += 1

                        # 时间过滤
                        trade_time = fill_data.get('timestamp')
                        if not trade_time or trade_time < cutoff_time:
                            total_time_filtered += 1
                            continue

                        # 金额过滤
                        notional_usd = fill_data.get('notional_usd', 0)
                        if notional_usd < min_trade_usd:
                            total_amount_filtered += 1
                            continue

                        # 补充交易者信息
                        fill_data['trader_pnl'] = trader['pnl']
                        fill_data['trader_roi'] = trader['roi']
                        fill_data['trader_account_value'] = trader['account_value']

                        all_trades.append(fill_data)

                    # 每10个地址输出一次进度
                    if i % 10 == 0:
                        logger.info(f"  进度: {i}/{len(top_traders)}, 已收集 {len(all_trades)} 笔交易")

                    # API限流控制：每个地址间隔1.5秒（避免429错误）
                    await asyncio.sleep(1.5)

                except asyncio.CancelledError:
                    logger.warning(f"任务被取消，停止抓取")
                    break
                except Exception as e:
                    logger.warning(f"抓取地址 {address[:10]}... 失败: {e}")
                    continue

            # 打印详细统计信息
            logger.info(f"\n{'='*60}")
            logger.info(f"统计信息汇总:")
            logger.info(f"  - 总共获取的 fills 数量: {total_fills}")
            logger.info(f"  - 成功解析的交易数: {total_analyzed}")
            logger.info(f"  - 时间范围内的交易: {total_fills - total_time_filtered}")
            logger.info(f"  - 符合金额要求的交易(≥${min_trade_usd:,.0f}): {len(all_trades)}")
            logger.info(f"{'='*60}\n")

            # 4. 按时间倒序排序（过滤掉 timestamp 为 None 的记录）
            all_trades = [t for t in all_trades if t.get('timestamp') is not None]
            all_trades.sort(key=lambda x: x['timestamp'], reverse=True)

            logger.info(f"✓ 完成！共收集 {len(all_trades)} 笔聪明钱交易（24h内，≥${min_trade_usd:,.0f}）")
            return all_trades

        except asyncio.CancelledError:
            logger.warning("抓取任务被取消")
            return []
        except Exception as e:
            logger.error(f"抓取聪明钱交易失败: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return []
