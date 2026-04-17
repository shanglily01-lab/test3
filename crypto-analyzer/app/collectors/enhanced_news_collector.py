"""
增强版新闻采集器
支持多渠道采集：SEC官方、Twitter大V、Telegram等
"""

import asyncio
import aiohttp
import feedparser
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from loguru import logger
import re


class SECNewsCollector:
    """SEC (美国证券交易委员会) 新闻采集器"""

    BASE_URL = "https://www.sec.gov"

    # SEC 加密货币相关 RSS feeds
    RSS_FEEDS = {
        'sec_news': 'https://www.sec.gov/news/pressreleases.rss',
        'sec_litigation': 'https://www.sec.gov/litigation/litreleases.rss',
        'sec_admin': 'https://www.sec.gov/litigation/admin.rss',
    }

    # 加密货币相关关键词
    CRYPTO_KEYWORDS = [
        'crypto', 'cryptocurrency', 'bitcoin', 'ethereum', 'digital asset',
        'blockchain', 'ico', 'initial coin offering', 'defi',
        'binance', 'coinbase', 'ripple', 'sec', 'securities'
    ]

    def __init__(self, config: dict = None):
        self.config = config or {}
        # SEC 要求设置 User-Agent
        self.user_agent = 'Crypto Analyzer (your-email@example.com)'

    async def collect(self, symbols: List[str] = None) -> List[Dict]:
        """采集 SEC 新闻"""
        news_list = []

        for source_name, feed_url in self.RSS_FEEDS.items():
            try:
                # feedparser 在 executor 中运行
                loop = asyncio.get_event_loop()
                feed = await loop.run_in_executor(None, feedparser.parse, feed_url)

                for entry in feed.entries[:20]:  # 每个源取前20条
                    title = entry.get('title', '')
                    summary = entry.get('summary', '')

                    # 过滤加密货币相关新闻
                    if self._is_crypto_related(title + ' ' + summary):
                        news = {
                            'id': f"sec_{hash(entry.link)}",
                            'title': title,
                            'url': entry.get('link', ''),
                            'source': 'SEC',
                            'source_type': source_name,
                            'published_at': entry.get('published', ''),
                            'symbols': self._detect_symbols(title + ' ' + summary, symbols),
                            'sentiment': self._analyze_sentiment(title + ' ' + summary),
                            'description': summary[:300],
                            'data_source': 'sec',
                            'importance': 'high',  # SEC新闻重要性高
                            'category': self._categorize(title + ' ' + summary)
                        }
                        news_list.append(news)

            except Exception as e:
                logger.error(f"SEC 采集失败 {source_name}: {e}")

        logger.info(f"SEC 采集到 {len(news_list)} 条新闻")
        return news_list

    def _is_crypto_related(self, text: str) -> bool:
        """判断是否与加密货币相关"""
        text_lower = text.lower()
        return any(keyword in text_lower for keyword in self.CRYPTO_KEYWORDS)

    def _detect_symbols(self, text: str, target_symbols: List[str] = None) -> List[str]:
        """检测币种"""
        text_lower = text.lower()
        detected = []

        symbol_keywords = {
            'BTC': ['bitcoin', 'btc'],
            'ETH': ['ethereum', 'eth'],
            'XRP': ['ripple', 'xrp'],
            'BNB': ['binance', 'bnb'],
            'SOL': ['solana', 'sol'],
            'ADA': ['cardano', 'ada'],
        }

        for symbol, keywords in symbol_keywords.items():
            if target_symbols and symbol not in target_symbols:
                continue
            for keyword in keywords:
                if keyword in text_lower:
                    detected.append(symbol)
                    break

        return detected

    def _analyze_sentiment(self, text: str) -> str:
        """分析情绪 (SEC 新闻特点)"""
        text_lower = text.lower()

        # 负面关键词 (监管、诉讼等)
        negative_keywords = [
            'fraud', 'lawsuit', 'sue', 'charge', 'violation',
            'penalty', 'fine', 'illegal', 'unregistered',
            'misleading', 'investigation', 'enforcement'
        ]

        # 正面关键词 (批准、许可等)
        positive_keywords = [
            'approve', 'approval', 'permit', 'license',
            'settle', 'cleared', 'greenlight', 'authorize'
        ]

        negative_count = sum(1 for kw in negative_keywords if kw in text_lower)
        positive_count = sum(1 for kw in positive_keywords if kw in text_lower)

        if negative_count > positive_count:
            return 'negative'
        elif positive_count > negative_count:
            return 'positive'
        else:
            return 'neutral'

    def _categorize(self, text: str) -> str:
        """分类 SEC 新闻"""
        text_lower = text.lower()

        if any(kw in text_lower for kw in ['lawsuit', 'sue', 'charge']):
            return 'litigation'
        elif any(kw in text_lower for kw in ['approve', 'etf', 'filing']):
            return 'regulatory'
        elif any(kw in text_lower for kw in ['fraud', 'scam']):
            return 'fraud'
        else:
            return 'general'


class TwitterVIPCollector:
    """Twitter 大V 采集器 (Musk, Trump, Vitalik 等)"""

    # 知名加密货币相关推特账号
    VIP_ACCOUNTS = {
        'elonmusk': {
            'name': 'Elon Musk',
            'importance': 'critical',  # Dogecoin、BTC 影响巨大
            'related_tokens': ['DOGE', 'BTC']
        },
        'VitalikButerin': {
            'name': 'Vitalik Buterin',
            'importance': 'critical',  # ETH 创始人
            'related_tokens': ['ETH']
        },
        'realDonaldTrump': {
            'name': 'Donald Trump',
            'importance': 'high',
            'related_tokens': ['BTC', 'TRUMP']  # Trump coin
        },
        'cz_binance': {
            'name': 'CZ (Changpeng Zhao)',
            'importance': 'critical',  # Binance 创始人
            'related_tokens': ['BNB', 'BTC', 'ETH']
        },
        'SBF_FTX': {
            'name': 'Sam Bankman-Fried',
            'importance': 'high',
            'related_tokens': ['FTT', 'SOL']
        },
        'brian_armstrong': {
            'name': 'Brian Armstrong',
            'importance': 'high',  # Coinbase CEO
            'related_tokens': ['BTC', 'ETH', 'COIN']
        },
        'michael_saylor': {
            'name': 'Michael Saylor',
            'importance': 'high',  # MicroStrategy CEO, BTC 大佬
            'related_tokens': ['BTC']
        },
        'APompliano': {
            'name': 'Anthony Pompliano',
            'importance': 'medium',  # BTC 倡导者
            'related_tokens': ['BTC']
        },
        'DocumentingBTC': {
            'name': 'Documenting Bitcoin',
            'importance': 'medium',
            'related_tokens': ['BTC']
        },
        'WuBlockchain': {
            'name': 'Wu Blockchain',
            'importance': 'medium',
            'related_tokens': ['BTC', 'ETH']
        }
    }

    def __init__(self, config: dict = None):
        self.config = config or {}
        # Twitter API v2 credentials
        self.bearer_token = self.config.get('twitter', {}).get('bearer_token', '')
        self.api_key = self.config.get('twitter', {}).get('api_key', '')
        self.api_secret = self.config.get('twitter', {}).get('api_secret', '')
        # 代理配置（从 twitter 配置中读取优先，否则从 smart_money 读取）
        self.proxy = self.config.get('twitter', {}).get('proxy') or \
                     self.config.get('smart_money', {}).get('proxy') or None

    async def collect(self, symbols: List[str] = None) -> List[Dict]:
        """
        采集 Twitter 大V 推文

        注意：需要 Twitter API v2 权限
        免费版：每月 50 万条推文读取
        基础版：$100/月，1000 万条推文读取
        """

        if not self.bearer_token:
            logger.warning("Twitter API 未配置，跳过")
            return []

        news_list = []

        try:
            # 配置代理
            connector = None
            if self.proxy:
                connector = aiohttp.TCPConnector()

            async with aiohttp.ClientSession(connector=connector) as session:
                headers = {
                    'Authorization': f'Bearer {self.bearer_token}',
                    'User-Agent': 'CryptoAnalyzer/1.0'
                }

                # 逐个采集大V的推文
                for username, account_info in self.VIP_ACCOUNTS.items():
                    try:
                        tweets = await self._fetch_user_tweets(session, username, headers, self.proxy)

                        for tweet in tweets:
                            # 过滤加密货币相关推文
                            if self._is_crypto_related(tweet.get('text', '')):
                                news = {
                                    'id': f"tw_{tweet['id']}",
                                    'title': tweet.get('text', '')[:100],
                                    'content': tweet.get('text', ''),
                                    'url': f"https://twitter.com/{username}/status/{tweet['id']}",
                                    'source': f"Twitter - {account_info['name']}",
                                    'source_account': username,
                                    'published_at': tweet.get('created_at', ''),
                                    'symbols': self._detect_symbols_in_tweet(tweet.get('text', ''), symbols),
                                    'sentiment': self._analyze_tweet_sentiment(tweet.get('text', '')),
                                    'data_source': 'twitter',
                                    'importance': account_info['importance'],
                                    'metrics': {
                                        'likes': tweet.get('public_metrics', {}).get('like_count', 0),
                                        'retweets': tweet.get('public_metrics', {}).get('retweet_count', 0),
                                        'replies': tweet.get('public_metrics', {}).get('reply_count', 0),
                                    },
                                    'related_tokens': account_info['related_tokens']
                                }
                                news_list.append(news)

                        # 避免频率限制
                        await asyncio.sleep(1)

                    except Exception as e:
                        logger.error(f"采集 {username} 失败: {e}")

        except Exception as e:
            logger.error(f"Twitter 采集失败: {e}")

        logger.info(f"Twitter 采集到 {len(news_list)} 条推文")
        return news_list

    async def _fetch_user_tweets(
        self,
        session: aiohttp.ClientSession,
        username: str,
        headers: dict,
        proxy: str = None,
        max_results: int = 10
    ) -> List[Dict]:
        """获取用户推文 (Twitter API v2)"""

        try:
            # 1. 先获取用户 ID
            user_url = f"https://api.twitter.com/2/users/by/username/{username}"
            async with session.get(user_url, headers=headers, proxy=proxy or None) as response:
                if response.status != 200:
                    logger.error(f"获取用户 {username} ID 失败: {response.status}")
                    return []

                user_data = await response.json()
                user_id = user_data.get('data', {}).get('id')

                if not user_id:
                    return []

            # 2. 获取用户推文
            tweets_url = f"https://api.twitter.com/2/users/{user_id}/tweets"
            params = {
                'max_results': max_results,
                'tweet.fields': 'created_at,public_metrics,entities',
                'exclude': 'retweets,replies'  # 只要原创推文
            }

            async with session.get(tweets_url, headers=headers, params=params, proxy=proxy or None) as response:
                if response.status != 200:
                    logger.error(f"获取 {username} 推文失败: {response.status}")
                    return []

                data = await response.json()
                return data.get('data', [])

        except Exception as e:
            logger.error(f"获取 {username} 推文异常: {e}")
            return []

    def _is_crypto_related(self, text: str) -> bool:
        """判断推文是否与加密货币相关"""
        text_lower = text.lower()

        crypto_keywords = [
            'bitcoin', 'btc', 'ethereum', 'eth', 'crypto',
            'doge', 'dogecoin', 'blockchain', '$',
            'satoshi', 'hodl', 'defi', 'nft', 'web3'
        ]

        return any(kw in text_lower for kw in crypto_keywords)

    def _detect_symbols_in_tweet(self, text: str, target_symbols: List[str] = None) -> List[str]:
        """从推文中检测币种 (包括 $ 标签)"""
        detected = []

        # 检测 $BTC, $ETH 等标签
        cashtags = re.findall(r'\$([A-Z]{2,5})\b', text.upper())
        detected.extend(cashtags)

        # 检测文字描述
        text_lower = text.lower()
        symbol_keywords = {
            'BTC': ['bitcoin', 'btc'],
            'ETH': ['ethereum', 'eth'],
            'DOGE': ['dogecoin', 'doge'],
            'SOL': ['solana', 'sol'],
            'BNB': ['binance', 'bnb'],
        }

        for symbol, keywords in symbol_keywords.items():
            for keyword in keywords:
                if keyword in text_lower and symbol not in detected:
                    detected.append(symbol)
                    break

        # 过滤目标币种
        if target_symbols:
            detected = [s for s in detected if s in target_symbols]

        return list(set(detected))  # 去重

    def _analyze_tweet_sentiment(self, text: str) -> str:
        """分析推文情绪"""
        text_lower = text.lower()

        positive_keywords = [
            'bullish', 'moon', 'rocket', '🚀', 'pump',
            'buy', 'long', 'support', 'breakout', 'ath'
        ]

        negative_keywords = [
            'bearish', 'dump', 'crash', 'scam', 'rug',
            'sell', 'short', 'resistance', 'drop', 'fall'
        ]

        pos_count = sum(1 for kw in positive_keywords if kw in text_lower)
        neg_count = sum(1 for kw in negative_keywords if kw in text_lower)

        if pos_count > neg_count:
            return 'positive'
        elif neg_count > pos_count:
            return 'negative'
        else:
            return 'neutral'


class CoinGeckoNewsCollector:
    """CoinGecko 新闻采集器 - 使用 Trending 和 Events"""

    BASE_URL = "https://api.coingecko.com/api/v3"

    def __init__(self, config: dict = None):
        self.config = config or {}
        self.api_key = self.config.get('coingecko', {}).get('api_key', '')
        # 从 coingecko 配置中读取代理（优先），否则从 smart_money 读取
        self.proxy = self.config.get('coingecko', {}).get('proxy') or \
                     self.config.get('smart_money', {}).get('proxy') or None

    async def collect(self, symbols: List[str] = None) -> List[Dict]:
        """
        采集 CoinGecko 趋势币种和重要事件

        由于 status_updates API 已废弃，改用:
        1. /search/trending - 热门币种
        2. 币种详情中的描述信息
        """
        news_list = []

        try:
            # 配置代理
            connector = None
            if self.proxy:
                connector = aiohttp.TCPConnector()

            async with aiohttp.ClientSession(connector=connector) as session:
                headers = {}
                if self.api_key:
                    headers['x-cg-pro-api-key'] = self.api_key

                # 1. 获取热门趋势币种
                trending_url = f"{self.BASE_URL}/search/trending"

                async with session.get(trending_url, headers=headers, proxy=self.proxy or None) as response:
                    if response.status == 200:
                        data = await response.json()
                        coins = data.get('coins', [])

                        for item in coins[:10]:  # 取前10个热门币
                            coin = item.get('item', {})
                            symbol = coin.get('symbol', '').upper()

                            # 过滤目标币种
                            if symbols and symbol not in symbols:
                                continue

                            news = {
                                'id': f"cg_trending_{coin.get('id', '')}",
                                'title': f"🔥 {coin.get('name', '')} ({symbol}) - Trending #{item.get('score', 0) + 1}",
                                'content': f"{coin.get('name')} is currently trending on CoinGecko. Market Cap Rank: #{coin.get('market_cap_rank', 'N/A')}",
                                'url': f"https://www.coingecko.com/en/coins/{coin.get('id', '')}",
                                'source': 'CoinGecko Trending',
                                'published_at': datetime.now().isoformat(),
                                'symbols': [symbol],
                                'sentiment': 'positive',  # 热门通常是正面的
                                'data_source': 'coingecko',
                                'importance': 'medium',
                                'category': 'trending',
                                'metadata': {
                                    'market_cap_rank': coin.get('market_cap_rank'),
                                    'thumb': coin.get('thumb', ''),
                                    'price_btc': coin.get('price_btc', 0)
                                }
                            }
                            news_list.append(news)

                    elif response.status == 429:
                        logger.warning("CoinGecko API 频率限制，跳过本次采集")
                    else:
                        logger.error(f"CoinGecko Trending API 错误: {response.status}")

                # 2. 获取市场动态 (Top gainers/losers)
                markets_url = f"{self.BASE_URL}/coins/markets"
                params = {
                    'vs_currency': 'usd',
                    'order': 'percent_change_24h_desc',  # 24h涨幅排序
                    'per_page': 5,
                    'page': 1,
                    'sparkline': 'false'
                }

                async with session.get(markets_url, params=params, headers=headers, proxy=self.proxy or None) as response:
                    if response.status == 200:
                        gainers = await response.json()

                        for coin in gainers:
                            symbol = coin.get('symbol', '').upper()

                            # 过滤目标币种
                            if symbols and symbol not in symbols:
                                continue

                            change_24h = coin.get('price_change_percentage_24h', 0)

                            if abs(change_24h) > 10:  # 只关注涨跌超过10%的
                                sentiment = 'positive' if change_24h > 0 else 'negative'
                                emoji = '🚀' if change_24h > 0 else '📉'

                                news = {
                                    'id': f"cg_mover_{coin.get('id', '')}_{datetime.now().strftime('%Y%m%d')}",
                                    'title': f"{emoji} {coin.get('name', '')} ({symbol}) {change_24h:+.1f}% in 24h",
                                    'content': f"{coin.get('name')} price is ${coin.get('current_price', 0):,.2f}, changed {change_24h:+.2f}% in the last 24 hours. Market Cap: ${coin.get('market_cap', 0):,.0f}",
                                    'url': f"https://www.coingecko.com/en/coins/{coin.get('id', '')}",
                                    'source': 'CoinGecko Markets',
                                    'published_at': datetime.now().isoformat(),
                                    'symbols': [symbol],
                                    'sentiment': sentiment,
                                    'data_source': 'coingecko',
                                    'importance': 'high' if abs(change_24h) > 20 else 'medium',
                                    'category': 'market_mover',
                                    'metadata': {
                                        'current_price': coin.get('current_price', 0),
                                        'price_change_24h': change_24h,
                                        'market_cap': coin.get('market_cap', 0),
                                        'volume_24h': coin.get('total_volume', 0)
                                    }
                                }
                                news_list.append(news)

        except Exception as e:
            logger.error(f"CoinGecko 采集失败: {e}")
            import traceback
            logger.error(traceback.format_exc())

        logger.info(f"CoinGecko 采集到 {len(news_list)} 条更新")
        return news_list


class EnhancedNewsAggregator:
    """增强版新闻聚合器"""

    def __init__(self, config: dict):
        self.config = config
        self.collectors = []

        # 初始化所有采集器
        news_config = config.get('news', {})

        # SEC 新闻
        if news_config.get('sec', {}).get('enabled', True):
            self.collectors.append(SECNewsCollector(news_config))
            logger.info("✓ 启用 SEC 新闻采集器")

        # Twitter 大V
        if news_config.get('twitter', {}).get('enabled', False):
            self.collectors.append(TwitterVIPCollector(news_config))
            logger.info("✓ 启用 Twitter 大V 采集器")

        # CoinGecko
        if news_config.get('coingecko', {}).get('enabled', True):
            self.collectors.append(CoinGeckoNewsCollector(news_config))
            logger.info("✓ 启用 CoinGecko 采集器")

        logger.info(f"增强版新闻聚合器初始化完成，共 {len(self.collectors)} 个采集器")

    async def collect_all(self, symbols: List[str] = None) -> List[Dict]:
        """并发采集所有数据源"""
        all_news = []

        # 并发采集
        tasks = [collector.collect(symbols) for collector in self.collectors]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Exception):
                logger.error(f"采集器错误: {result}")
            else:
                all_news.extend(result)

        # 去重
        unique_news = self._deduplicate(all_news)

        # 按重要性和时间排序
        unique_news.sort(
            key=lambda x: (
                {'critical': 3, 'high': 2, 'medium': 1}.get(x.get('importance', 'medium'), 0),
                x.get('published_at', '')
            ),
            reverse=True
        )

        logger.info(f"增强版采集器采集到 {len(unique_news)} 条去重后的新闻")
        return unique_news

    def _deduplicate(self, news_list: List[Dict]) -> List[Dict]:
        """根据 URL 和标题去重"""
        seen = set()
        unique_news = []

        for news in news_list:
            # 使用 URL 或标题的哈希作为唯一标识
            key = news.get('url') or hash(news.get('title', ''))
            if key not in seen:
                seen.add(key)
                unique_news.append(news)

        return unique_news


# 使用示例
async def main():
    """测试增强版新闻采集器"""

    config = {
        'news': {
            'sec': {
                'enabled': True
            },
            'twitter': {
                'enabled': True,
                'bearer_token': 'your_twitter_bearer_token_here'
            },
            'coingecko': {
                'enabled': True,
                'api_key': ''  # 可选
            }
        }
    }

    aggregator = EnhancedNewsAggregator(config)

    print("\n=== 采集加密货币新闻 ===")
    all_news = await aggregator.collect_all(['BTC', 'ETH', 'DOGE'])

    print(f"\n总共采集到 {len(all_news)} 条新闻\n")

    for i, news in enumerate(all_news[:10], 1):
        print(f"{i}. 【{news.get('source', 'Unknown')}】 {news.get('title', '')[:80]}")
        print(f"   情绪: {news.get('sentiment', 'neutral')} | 重要性: {news.get('importance', 'medium')}")
        print(f"   币种: {', '.join(news.get('symbols', []))}")
        print(f"   URL: {news.get('url', 'N/A')}")
        print()


if __name__ == '__main__':
    asyncio.run(main())
