"""
增强版Dashboard API - 优化版本
使用缓存表，大幅提升性能
"""

import asyncio
import logging
from typing import Dict, List
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.exc import OperationalError, TimeoutError as SQLTimeoutError
import pymysql

from app.database.db_service import DatabaseService

logger = logging.getLogger(__name__)


class EnhancedDashboardCached:
    """增强版仪表盘数据服务（使用缓存）"""

    def __init__(self, config: dict, price_collector=None):
        """
        初始化

        Args:
            config: 系统配置
            price_collector: 价格采集器（可选，用于实时价格获取）
        """
        self.config = config
        self.db_service = DatabaseService(config.get('database', {}))
        self.price_collector = price_collector

    async def get_dashboard_data(self, symbols: List[str] = None) -> Dict:
        """
        获取完整的仪表盘数据（从缓存表读取，性能极高）

        Args:
            symbols: 币种列表,如 ['BTC/USDT', 'ETH/USDT']

        Returns:
            仪表盘数据字典
        """
        if symbols is None:
            symbols = self.config.get('symbols', ['BTC/USDT', 'ETH/USDT'])

        # logger.info(f"📊 从缓存获取Dashboard数据 - {len(symbols)} 个币种")  # 减少日志输出
        start_time = datetime.now()

        # 并行读取缓存表
        tasks = [
            self._get_prices_from_cache(symbols),
            self._get_recommendations_from_cache(symbols),
            self._get_news_from_db(limit=20),
            self._get_hyperliquid_from_cache(),
            self._get_system_stats(),
            self._get_futures_from_cache(symbols),  # 合约数据
        ]

        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"Dashboard数据获取异常: {e}")
            import traceback
            logger.error(traceback.format_exc())
            # 如果gather失败，返回空数据
            results = [[], [], [], {}, {}, []]

        prices, recommendations, news, hyperliquid, stats, futures = results

        # 处理异常
        if isinstance(prices, Exception):
            logger.error(f"获取价格失败: {prices}")
            import traceback
            logger.error(traceback.format_exc())
            prices = []
        if isinstance(recommendations, Exception):
            logger.error(f"获取建议失败: {recommendations}")
            import traceback
            logger.error(traceback.format_exc())
            recommendations = []
        if isinstance(news, Exception):
            logger.error(f"获取新闻失败: {news}")
            import traceback
            logger.error(traceback.format_exc())
            news = []
        if isinstance(hyperliquid, Exception):
            logger.error(f"获取Hyperliquid数据失败: {hyperliquid}")
            import traceback
            logger.error(traceback.format_exc())
            hyperliquid = {}
        if isinstance(stats, Exception):
            logger.error(f"获取统计失败: {stats}")
            import traceback
            logger.error(traceback.format_exc())
            stats = {}
        if isinstance(futures, Exception):
            logger.error(f"获取合约数据失败: {futures}")
            import traceback
            logger.error(traceback.format_exc())
            futures = []

        # 将 prices 合并到 futures（避免在 _get_futures_from_cache 内部重复查价格）
        if isinstance(prices, list) and isinstance(futures, list) and prices:
            prices_map = {p.get('full_symbol', ''): p for p in prices}
            for item in futures:
                sym = item.get('full_symbol', '')
                if sym in prices_map:
                    pi = prices_map[sym]
                    item['price']           = pi.get('price', 0)
                    item['current_price']   = pi.get('price', 0)
                    item['change_24h']      = pi.get('change_24h', 0)
                    item['price_change_24h']= pi.get('change_24h', 0)
                    item['volume_24h']      = pi.get('volume_24h', 0)

        # news_24h 直接从已获取的新闻列表计算，无需额外 DB 查询
        if isinstance(stats, dict):
            stats['news_24h'] = len(news) if isinstance(news, list) else 0

        # 统计信号
        signal_stats = self._calculate_signal_stats(recommendations)

        elapsed = (datetime.now() - start_time).total_seconds()
        # logger.info(f"✅ Dashboard数据获取完成，耗时: {elapsed:.3f}秒（从缓存）")  # 减少日志输出

        # 确保所有数据都是可序列化的
        try:
            # 确保stats是字典
            if not isinstance(stats, dict):
                stats = {}
            if not isinstance(signal_stats, dict):
                signal_stats = {}
            
            return {
                'success': True,
                'data': {
                    'prices': prices or [],
                    'recommendations': recommendations or [],
                    'news': news or [],
                    'hyperliquid': hyperliquid or {},
                    'futures': futures or [],  # 合约数据
                    'stats': {
                        **stats,
                        **signal_stats
                    },
                    'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'from_cache': True  # 标记数据来源于缓存
                }
            }
        except Exception as e:
            logger.error(f"构建Dashboard响应失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            # 返回最小有效响应
            return {
                'success': False,
                'data': {
                    'prices': [],
                    'recommendations': [],
                    'news': [],
                    'hyperliquid': {},
                    'futures': [],
                    'stats': {},
                    'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                },
                'error': str(e)
            }

    async def _get_prices_from_cache(self, symbols: List[str]) -> List[Dict]:
        """
        实时价格来自 kline_data (5m K线)；change_24h 与24h前K线对比计算；
        high/low/volume/trend 来自 price_stats_24h。
        全部 DB 操作放入线程池，事件循环不阻塞。
        """
        def _query():
            prices = []
            session = self.db_service.get_session()
            try:
                placeholders = ','.join([f':s{i}' for i in range(len(symbols))])
                params = {f's{i}': sym for i, sym in enumerate(symbols)}

                cur_rows = session.execute(text(f"""
                    SELECT kd.symbol, kd.close_price, kd.open_time
                    FROM kline_data kd
                    INNER JOIN (
                        SELECT symbol, MAX(open_time) AS max_ot
                        FROM kline_data
                        WHERE symbol IN ({placeholders}) AND timeframe = '5m'
                        GROUP BY symbol
                    ) t ON kd.symbol = t.symbol AND kd.open_time = t.max_ot AND kd.timeframe = '5m'
                """), params).fetchall()

                ago_rows = session.execute(text(f"""
                    SELECT kd.symbol, kd.close_price AS price_ago
                    FROM kline_data kd
                    INNER JOIN (
                        SELECT symbol, MAX(open_time) AS max_ot
                        FROM kline_data
                        WHERE symbol IN ({placeholders}) AND timeframe = '5m'
                          AND open_time <= (UNIX_TIMESTAMP() * 1000 - 86400000)
                        GROUP BY symbol
                    ) t ON kd.symbol = t.symbol AND kd.open_time = t.max_ot AND kd.timeframe = '5m'
                """), params).fetchall()

                stats_rows = session.execute(text(f"""
                    SELECT symbol, high_24h, low_24h, volume_24h, quote_volume_24h, trend
                    FROM price_stats_24h
                    WHERE symbol IN ({placeholders})
                """), params).fetchall()

                cur_map   = {dict(r._mapping if hasattr(r,'_mapping') else r)['symbol']: dict(r._mapping if hasattr(r,'_mapping') else r) for r in cur_rows}
                ago_map   = {dict(r._mapping if hasattr(r,'_mapping') else r)['symbol']: dict(r._mapping if hasattr(r,'_mapping') else r) for r in ago_rows}
                stats_map = {dict(r._mapping if hasattr(r,'_mapping') else r)['symbol']: dict(r._mapping if hasattr(r,'_mapping') else r) for r in stats_rows}

                for sym in symbols:
                    cur = cur_map.get(sym)
                    if not cur:
                        continue
                    ago = ago_map.get(sym, {})
                    st  = stats_map.get(sym, {})

                    current_price = float(cur['close_price'])
                    price_ago = float(ago['price_ago']) if ago.get('price_ago') else None
                    change_24h = ((current_price - price_ago) / price_ago * 100
                                  if price_ago and price_ago > 0
                                  else float(st.get('change_24h') or 0))

                    ot = cur.get('open_time')
                    ts_str = (datetime.utcfromtimestamp(ot / 1000).strftime('%Y-%m-%d %H:%M:%S') if ot else '')

                    prices.append({
                        'symbol':          sym.replace('/USDT', ''),
                        'full_symbol':     sym,
                        'price':           current_price,
                        'change_24h':      round(change_24h, 4),
                        'volume_24h':      float(st['volume_24h'])       if st.get('volume_24h')       else 0,
                        'quote_volume_24h':float(st['quote_volume_24h'])  if st.get('quote_volume_24h')  else 0,
                        'high_24h':        float(st['high_24h'])          if st.get('high_24h')          else 0,
                        'low_24h':         float(st['low_24h'])           if st.get('low_24h')           else 0,
                        'trend':           st.get('trend', 'sideways'),
                        'timestamp':       ts_str,
                    })

                prices.sort(key=lambda x: x['change_24h'], reverse=True)
                return prices
            finally:
                session.close()

        try:
            return await asyncio.to_thread(_query)
        except Exception as e:
            logger.error(f"读取价格失败: {e}")
            return []

    async def _get_recommendations_from_cache(self, symbols: List[str]) -> List[Dict]:
        """
        从投资建议缓存表读取推荐数据；资金费率合并在同一线程查询。
        全部 DB 操作放入线程池，事件循环不阻塞。
        """
        def _query():
            session = self.db_service.get_session()
            try:
                placeholders = ','.join([f':symbol{i}' for i in range(len(symbols))])
                params = {f'symbol{i}': sym for i, sym in enumerate(symbols)}

                results = session.execute(text(f"""
                    SELECT symbol, total_score, technical_score, news_score,
                           funding_score, hyperliquid_score, ethereum_score,
                           `signal`, confidence, current_price, entry_price,
                           stop_loss, take_profit, risk_level, risk_factors, reasons,
                           has_technical, has_news, has_funding, has_hyperliquid,
                           has_ethereum, data_completeness, updated_at
                    FROM investment_recommendations_cache
                    WHERE symbol IN ({placeholders})
                    ORDER BY confidence DESC
                """), params).fetchall()

                # 资金费率合并在同一线程，避免额外往返
                fr_rows = session.execute(text(f"""
                    SELECT symbol, current_rate, current_rate_pct, trend, market_sentiment
                    FROM funding_rate_stats
                    WHERE symbol IN ({placeholders})
                """), params).fetchall()
                funding_rates = {}
                for row in fr_rows:
                    rd = dict(row._mapping) if hasattr(row, '_mapping') else dict(row)
                    funding_rates[rd['symbol']] = {
                        'funding_rate':     float(rd['current_rate']),
                        'funding_rate_pct': float(rd['current_rate_pct']),
                        'trend':            rd['trend'],
                        'market_sentiment': rd['market_sentiment'],
                    }
                return results, funding_rates
            finally:
                session.close()

        try:
            db_results, funding_rates = await asyncio.to_thread(_query)
        except Exception as e:
            logger.error(f"获取建议失败: {e}")
            return []

        import json
        recommendations = []
        cached_symbols = set()

        for row in db_results:
            row_dict = dict(row._mapping) if hasattr(row, '_mapping') else dict(row)
            symbol = row_dict['symbol']
            cached_symbols.add(symbol)

            signal_time = ''
            if row_dict.get('updated_at'):
                signal_time = (row_dict['updated_at'].strftime('%Y-%m-%d %H:%M:%S')
                               if isinstance(row_dict['updated_at'], datetime)
                               else str(row_dict['updated_at']))

            recommendations.append({
                'symbol':           symbol.replace('/USDT', ''),
                'full_symbol':      symbol,
                'signal':           row_dict['signal'],
                'confidence':       float(row_dict['confidence']) if row_dict['confidence'] else 0,
                'current_price':    float(row_dict['current_price']) if row_dict['current_price'] else 0,
                'entry_price':      float(row_dict['entry_price']) if row_dict['entry_price'] else 0,
                'stop_loss':        float(row_dict['stop_loss']) if row_dict['stop_loss'] else 0,
                'take_profit':      float(row_dict['take_profit']) if row_dict['take_profit'] else 0,
                'reasons':          json.loads(row_dict['reasons']) if row_dict['reasons'] else [],
                'risk_level':       row_dict['risk_level'] or 'MEDIUM',
                'risk_factors':     json.loads(row_dict['risk_factors']) if row_dict['risk_factors'] else [],
                'scores': {
                    'total':      float(row_dict['total_score']) if row_dict['total_score'] else 50,
                    'technical':  float(row_dict['technical_score']) if row_dict['technical_score'] else 50,
                    'news':       float(row_dict['news_score']) if row_dict['news_score'] else 50,
                    'funding':    float(row_dict['funding_score']) if row_dict['funding_score'] else 50,
                    'hyperliquid':float(row_dict['hyperliquid_score']) if row_dict['hyperliquid_score'] else 50,
                    'ethereum':   float(row_dict['ethereum_score']) if row_dict['ethereum_score'] else 50,
                    'etf':        float(row_dict['etf_score']) if row_dict.get('etf_score') else 50,
                },
                'data_sources': {
                    'technical':   bool(row_dict['has_technical']),
                    'news':        bool(row_dict['has_news']),
                    'funding':     bool(row_dict['has_funding']),
                    'hyperliquid': bool(row_dict['has_hyperliquid']),
                    'ethereum':    bool(row_dict['has_ethereum']),
                    'etf':         bool(row_dict.get('has_etf')),
                },
                'data_completeness': float(row_dict['data_completeness']) if row_dict['data_completeness'] else 0,
                'funding_rate':      funding_rates.get(symbol),
                'signal_time':       signal_time,
            })

        # 为没有缓存数据的交易对返回默认值
        for symbol in symbols:
            if symbol not in cached_symbols:
                recommendations.append({
                    'symbol': symbol.replace('/USDT', ''), 'full_symbol': symbol,
                    'signal': '持有', 'confidence': 0, 'current_price': 0,
                    'entry_price': 0, 'stop_loss': 0, 'take_profit': 0,
                    'reasons': ['数据不足，无法生成投资建议'], 'risk_level': 'UNKNOWN',
                    'risk_factors': ['缺少价格数据'],
                    'scores': {'total':50,'technical':50,'news':50,'funding':50,'hyperliquid':50,'ethereum':50,'etf':50},
                    'data_sources': {'technical':False,'news':False,'funding':False,'hyperliquid':False,'ethereum':False,'etf':False},
                    'data_completeness': 0, 'funding_rate': None,
                })

        return recommendations

    async def _get_funding_rates_batch(self, symbols: List[str]) -> Dict:
        """批量获取资金费率（线程池执行）"""
        def _query():
            session = self.db_service.get_session()
            try:
                placeholders = ','.join([f':symbol{i}' for i in range(len(symbols))])
                params = {f'symbol{i}': sym for i, sym in enumerate(symbols)}
                rows = session.execute(text(f"""
                    SELECT symbol, current_rate, current_rate_pct, trend, market_sentiment
                    FROM funding_rate_stats WHERE symbol IN ({placeholders})
                """), params).fetchall()
                return {dict(r._mapping if hasattr(r,'_mapping') else r)['symbol']: {
                    'funding_rate':     float(dict(r._mapping if hasattr(r,'_mapping') else r)['current_rate']),
                    'funding_rate_pct': float(dict(r._mapping if hasattr(r,'_mapping') else r)['current_rate_pct']),
                    'trend':            dict(r._mapping if hasattr(r,'_mapping') else r)['trend'],
                    'market_sentiment': dict(r._mapping if hasattr(r,'_mapping') else r)['market_sentiment'],
                } for r in rows}
            finally:
                session.close()
        try:
            return await asyncio.to_thread(_query)
        except Exception as e:
            logger.warning(f"批量获取资金费率失败: {e}")
            return {}

    async def _get_news_from_db(self, limit: int = 20) -> List[Dict]:
        """获取最新新闻（线程池执行，不阻塞事件循环）"""
        try:
            news_list = await asyncio.to_thread(self.db_service.get_recent_news, 24, None, limit)

            result = []
            for news in news_list:
                # 处理发布时间
                if hasattr(news, 'published_datetime') and news.published_datetime:
                    published_at = news.published_datetime.strftime('%Y-%m-%d %H:%M')
                elif hasattr(news, 'published_at') and news.published_at:
                    published_at = news.published_at if isinstance(news.published_at, str) else str(news.published_at)
                else:
                    published_at = 'N/A'

                # 处理采集时间
                if hasattr(news, 'collected_at') and news.collected_at:
                    if isinstance(news.collected_at, datetime):
                        collected_at = news.collected_at.strftime('%Y-%m-%d %H:%M')
                    else:
                        collected_at = str(news.collected_at)
                elif hasattr(news, 'created_at') and news.created_at:
                    if isinstance(news.created_at, datetime):
                        collected_at = news.created_at.strftime('%Y-%m-%d %H:%M')
                    else:
                        collected_at = str(news.created_at)
                else:
                    collected_at = 'N/A'

                result.append({
                    'title': news.title or 'No Title',
                    'source': news.source or 'Unknown',
                    'url': news.url or '',
                    'symbols': news.symbols or '',
                    'sentiment': news.sentiment or 'neutral',
                    'sentiment_score': float(news.sentiment_score) if news.sentiment_score else 0.5,
                    'published_at': published_at,
                    'collected_at': collected_at
                })

            logger.debug(f"✅ 读取 {len(result)} 条新闻")
            return result

        except Exception as e:
            logger.error(f"获取新闻失败: {e}")
            return []

    async def _get_hyperliquid_from_cache(self) -> Dict:
        """从预计算缓存表读取聪明钱数据（线程池执行，不阻塞事件循环）"""
        _empty = {'monitored_wallets': 0, 'active_wallets': 0, 'total_volume_24h': 0,
                  'recent_trades': [], 'top_coins': []}

        def _query():
            from app.services.hyperliquid_token_mapper import get_token_mapper
            token_mapper = get_token_mapper()
            session = self.db_service.get_session()
            try:
                summary_row = session.execute(text(
                    "SELECT monitored_count, active_wallets_24h, total_volume_24h FROM dashboard_hl_summary WHERE id = 1"
                )).fetchone()

                top_coins_rows = session.execute(text("""
                    SELECT symbol AS coin, net_flow, total_volume
                    FROM hyperliquid_symbol_aggregation
                    WHERE period = '24h' ORDER BY ABS(net_flow) DESC LIMIT 20
                """)).fetchall()

                trades_rows = session.execute(text("""
                    SELECT address, wallet_label, coin, side, price, size,
                           notional_usd, closed_pnl, leverage, trade_time
                    FROM dashboard_hl_recent_trades ORDER BY trade_time DESC
                """)).fetchall()

                monitored_count      = int(summary_row[0]) if summary_row else 0
                active_wallets_count = int(summary_row[1]) if summary_row else 0
                total_volume_24h     = float(summary_row[2]) if summary_row else 0.0

                recent_trades = []
                for t in trades_rows:
                    td = dict(t._mapping) if hasattr(t, '_mapping') else dict(t)
                    wallet_label = td.get('wallet_label') or ''
                    if not wallet_label or wallet_label == 'None':
                        wallet_label = (td.get('address') or 'Unknown')[:10] + '...'
                    recent_trades.append({
                        'wallet_label': wallet_label,
                        'coin':         token_mapper.format_symbol(td['coin']),
                        'coin_raw':     td['coin'],
                        'side':         td['side'],
                        'size':         float(td.get('size', 0)),
                        'leverage':     float(td.get('leverage', 1)),
                        'notional_usd': float(td.get('notional_usd', 0)),
                        'price':        float(td.get('price', 0)),
                        'closed_pnl':   float(td.get('closed_pnl', 0)),
                        'trade_time':   td['trade_time'].strftime('%Y-%m-%d %H:%M') if td.get('trade_time') else '',
                    })

                top_coins = []
                for r in top_coins_rows:
                    rd = dict(r._mapping) if hasattr(r, '_mapping') else dict(r)
                    net_flow = float(rd.get('net_flow', 0))
                    top_coins.append({'coin': rd['coin'], 'net_flow': net_flow,
                                      'direction': 'bullish' if net_flow > 0 else 'bearish'})

                return {
                    'monitored_wallets': monitored_count,
                    'active_wallets':    active_wallets_count,
                    'total_volume_24h':  total_volume_24h,
                    'recent_trades':     recent_trades,
                    'top_coins':         top_coins,
                }
            finally:
                session.close()

        try:
            return await asyncio.to_thread(_query)
        except Exception as e:
            logger.error(f"从缓存读取Hyperliquid数据失败: {e}")
            return _empty

    async def _get_system_stats(self) -> Dict:
        """
        获取系统统计（无 DB 查询，news_24h 由 get_dashboard_data 从已获取的新闻列表填充）
        """
        return {
            'total_symbols': len(self.config.get('symbols', [])),
            'news_24h': 0,  # 由 get_dashboard_data 在 gather 完成后用 len(news) 填充
        }

    def _calculate_signal_stats(self, recommendations: List[Dict]) -> Dict:
        """统计信号分布"""
        strong_buy_count = sum(
            1 for r in recommendations
            if r['signal'] == 'STRONG_BUY'
        )
        
        buy_count = sum(
            1 for r in recommendations
            if r['signal'] == 'BUY'
        )
        
        strong_sell_count = sum(
            1 for r in recommendations
            if r['signal'] == 'STRONG_SELL'
        )
        
        sell_count = sum(
            1 for r in recommendations
            if r['signal'] == 'SELL'
        )

        bullish_count = strong_buy_count + buy_count
        bearish_count = strong_sell_count + sell_count

        hold_count = sum(
            1 for r in recommendations
            if r['signal'] == 'HOLD'
        )

        return {
            'strong_buy_count': strong_buy_count,
            'buy_count': buy_count,
            'strong_sell_count': strong_sell_count,
            'sell_count': sell_count,
            'bullish_count': bullish_count,
            'bearish_count': bearish_count,
            'hold_count': hold_count,
            'total_count': len(recommendations)
        }

    async def _get_futures_from_cache(self, symbols: List[str]) -> List[Dict]:
        """
        从资金费率缓存表读取合约数据 + 批量补充持仓量和多空比（3条SQL）。
        全部 DB 操作放入线程池，事件循环不阻塞。
        """
        def _query():
            session = self.db_service.get_session()
            try:
                placeholders = ','.join([f':symbol{i}' for i in range(len(symbols))])
                params = {f'symbol{i}': sym for i, sym in enumerate(symbols)}

                results = session.execute(text(f"""
                    SELECT symbol, current_rate, current_rate_pct, trend, market_sentiment
                    FROM funding_rate_stats WHERE symbol IN ({placeholders})
                """), params).fetchall()

                futures_data = []
                for row in results:
                    rd = dict(row._mapping) if hasattr(row, '_mapping') else dict(row)
                    symbol = rd['symbol']
                    futures_data.append({
                        'symbol':           symbol.replace('/USDT', ''),
                        'full_symbol':      symbol,
                        'open_interest':    0,
                        'long_short_ratio': 0,
                        'funding_rate':     float(rd['current_rate']) if rd.get('current_rate') else 0,
                        'funding_rate_pct': float(rd['current_rate_pct']) if rd.get('current_rate_pct') else 0,
                        'trend':            rd.get('trend', 'neutral'),
                        'market_sentiment': rd.get('market_sentiment', 'normal'),
                    })

                if not futures_data:
                    return futures_data

                all_syms, sym_map = [], {}
                for sym in symbols:
                    all_syms.append(sym);          sym_map[sym] = sym
                    ns = sym.replace('/', '');      all_syms.append(ns); sym_map[ns] = sym

                ph2 = ','.join([f':s{i}' for i in range(len(all_syms))])
                p2  = {f's{i}': s for i, s in enumerate(all_syms)}

                oi_rows = session.execute(text(f"""
                    SELECT f.symbol, f.open_interest
                    FROM futures_open_interest f
                    JOIN (SELECT symbol, MAX(timestamp) AS max_ts FROM futures_open_interest
                          WHERE symbol IN ({ph2}) AND exchange = 'binance_futures' GROUP BY symbol) t
                    ON f.symbol = t.symbol AND f.timestamp = t.max_ts
                    WHERE f.exchange = 'binance_futures'
                """), p2).fetchall()

                lsr_rows = session.execute(text(f"""
                    SELECT f.symbol, f.long_account, f.short_account, f.long_short_ratio,
                           f.long_position, f.short_position, f.long_short_position_ratio
                    FROM futures_long_short_ratio f
                    JOIN (SELECT symbol, MAX(timestamp) AS max_ts FROM futures_long_short_ratio
                          WHERE symbol IN ({ph2}) GROUP BY symbol) t
                    ON f.symbol = t.symbol AND f.timestamp = t.max_ts
                """), p2).fetchall()

                oi_map  = {sym_map[dict(r._mapping if hasattr(r,'_mapping') else r)['symbol']]:
                           float(dict(r._mapping if hasattr(r,'_mapping') else r).get('open_interest') or 0)
                           for r in oi_rows if sym_map.get(dict(r._mapping if hasattr(r,'_mapping') else r)['symbol'])}
                lsr_map = {sym_map[dict(r._mapping if hasattr(r,'_mapping') else r)['symbol']]:
                           dict(r._mapping if hasattr(r,'_mapping') else r)
                           for r in lsr_rows if sym_map.get(dict(r._mapping if hasattr(r,'_mapping') else r)['symbol'])}

                for item in futures_data:
                    sym = item['full_symbol']
                    item['open_interest'] = oi_map.get(sym, 0)
                    if sym in lsr_map:
                        rd = lsr_map[sym]
                        item['long_short_account_ratio']  = float(rd.get('long_short_ratio') or 0)
                        item['long_account']              = float(rd.get('long_account') or 0)
                        item['short_account']             = float(rd.get('short_account') or 0)
                        item['long_short_position_ratio'] = float(rd.get('long_short_position_ratio') or 0)
                        item['long_position']             = float(rd.get('long_position') or 0)
                        item['short_position']            = float(rd.get('short_position') or 0)
                    else:
                        item['long_short_account_ratio']  = 0
                        item['long_short_position_ratio'] = 0

                return futures_data
            finally:
                session.close()

        try:
            return await asyncio.to_thread(_query)
        except Exception as e:
            logger.error(f"从缓存读取合约数据失败: {e}")
            return []



# 导入timedelta
from datetime import timedelta
