"""
数据库服务类
负责数据的存储和查询
"""

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus
from typing import List, Dict, Optional
from datetime import datetime, timezone
from loguru import logger

from .models import Base, PriceData, KlineData, TradeData, OrderBookData, NewsData, FundingRateData, FuturesOpenInterest, FuturesLongShortRatio, SmartMoneyAddress, SmartMoneyTransaction, SmartMoneySignal, StrategyTradeRecord, StrategyTestRecord


class DatabaseService:
    """数据库服务类"""
    
    # 类级别的标记，用于避免重复打印连接日志
    _connection_logged = False
    _tables_created_logged = False

    def __init__(self, config: dict):
        """
        初始化数据库连接

        Args:
            config: 数据库配置字典
        """
        self.config = config
        self.engine = None
        self.SessionLocal = None
        self._init_database()

    def _init_database(self):
        """初始化数据库连接（带重试机制）"""
        max_retries = 3
        retry_delay = 2  # 秒
        
        for attempt in range(max_retries):
            try:
                db_type = self.config.get('type', 'mysql')

                if db_type == 'mysql':
                    mysql_config = self.config.get('mysql', {})
                    host = mysql_config.get('host', 'localhost')
                    port = mysql_config.get('port', 3306)
                    user = mysql_config.get('user', 'root')
                    password = mysql_config.get('password', '')
                    database = mysql_config.get('database', 'binance-data')

                    # URL编码密码以处理特殊字符
                    password_encoded = quote_plus(password)

                    # 创建连接字符串
                    db_uri = f"mysql+pymysql://{user}:{password_encoded}@{host}:{port}/{database}?charset=utf8mb4"

                    self.engine = create_engine(
                        db_uri,
                        pool_size=20,  # 增加连接池大小
                        max_overflow=30,  # 增加溢出连接数
                        pool_pre_ping=True,  # 自动检测连接是否有效
                        pool_recycle=3600,  # 1小时后回收连接，避免长时间连接失效
                        echo=False,  # 设为True可以看到SQL语句
                        connect_args={
                            'connect_timeout': 10,
                            'read_timeout': 60,  # 增加读取超时时间到60秒，处理复杂查询
                            'write_timeout': 30
                        }
                    )

                    # 只在首次连接时打印日志，避免重复打印
                    if not DatabaseService._connection_logged:
                        logger.info(f"MySQL数据库连接成功: {host}:{port}/{database}")
                        DatabaseService._connection_logged = True
                    else:
                        logger.debug(f"MySQL数据库连接池已创建: {host}:{port}/{database}")

                elif db_type == 'sqlite':
                    sqlite_path = self.config.get('sqlite', {}).get('path', './data/crypto.db')
                    db_uri = f"sqlite:///{sqlite_path}"
                    self.engine = create_engine(db_uri, echo=False)
                    
                    # 只在首次连接时打印日志
                    if not DatabaseService._connection_logged:
                        logger.info(f"SQLite数据库连接成功: {sqlite_path}")
                        DatabaseService._connection_logged = True
                    else:
                        logger.debug(f"SQLite数据库连接已创建: {sqlite_path}")

                else:
                    raise ValueError(f"不支持的数据库类型: {db_type}")

                # 创建会话工厂
                self.SessionLocal = sessionmaker(bind=self.engine)

                # 创建所有表
                Base.metadata.create_all(self.engine)
                
                # 只在首次创建表时打印日志
                if not hasattr(DatabaseService, '_tables_created_logged'):
                    logger.info("数据库表创建/检查完成")
                    DatabaseService._tables_created_logged = True
                else:
                    logger.debug("数据库表检查完成")
                
                # 成功，退出重试循环
                return
                
            except Exception as e:
                error_msg = str(e)
                is_connection_error = 'Lost connection' in error_msg or 'OperationalError' in str(type(e).__name__)
                
                if attempt < max_retries - 1 and is_connection_error:
                    logger.warning(f"数据库连接失败（尝试 {attempt + 1}/{max_retries}）: {e}，{retry_delay}秒后重试...")
                    import time
                    time.sleep(retry_delay)
                    retry_delay *= 2  # 指数退避
                else:
                    logger.error(f"数据库初始化失败（已重试 {attempt + 1} 次）: {e}")
                    raise

    def get_session(self) -> Session:
        """获取数据库会话"""
        return self.SessionLocal()

    def save_price_data(self, price_data: Dict) -> bool:
        """
        保存实时价格数据

        Args:
            price_data: 价格数据字典

        Returns:
            是否保存成功
        """
        session = self.get_session()
        try:
            price_record = PriceData(
                symbol=price_data.get('symbol'),
                exchange=price_data.get('exchange', 'binance'),
                timestamp=price_data.get('timestamp', datetime.now()),
                price=price_data.get('price'),
                open_price=price_data.get('open'),
                high_price=price_data.get('high'),
                low_price=price_data.get('low'),
                close_price=price_data.get('close'),
                volume=price_data.get('volume'),
                quote_volume=price_data.get('quote_volume'),
                bid_price=price_data.get('bid'),
                ask_price=price_data.get('ask'),
                change_24h=price_data.get('change_24h')
            )

            session.add(price_record)
            session.commit()
            logger.debug(f"保存价格数据成功: {price_data.get('symbol')} - ${price_data.get('price')}")
            return True

        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"保存价格数据失败: {e}")
            return False
        finally:
            session.close()

    def save_kline_data(self, kline_data: Dict) -> bool:
        """
        保存K线数据

        Args:
            kline_data: K线数据字典

        Returns:
            是否保存成功
        """
        session = self.get_session()
        try:
            kline_record = KlineData(
                symbol=kline_data.get('symbol'),
                exchange=kline_data.get('exchange', 'binance'),
                timeframe=kline_data.get('timeframe', '5m'),
                open_time=kline_data.get('open_time'),
                close_time=kline_data.get('close_time'),
                timestamp=kline_data.get('timestamp'),
                open_price=kline_data.get('open'),
                high_price=kline_data.get('high'),
                low_price=kline_data.get('low'),
                close_price=kline_data.get('close'),
                volume=kline_data.get('volume'),
                quote_volume=kline_data.get('quote_volume'),
                number_of_trades=kline_data.get('trades'),
                taker_buy_base_volume=kline_data.get('taker_buy_base'),
                taker_buy_quote_volume=kline_data.get('taker_buy_quote')
            )

            session.add(kline_record)
            session.commit()
            logger.debug(f"保存K线数据成功: {kline_data.get('symbol')} - {kline_data.get('timeframe')}")
            return True

        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"保存K线数据失败: {e}")
            return False
        finally:
            session.close()

    def save_kline_batch(self, klines: List[Dict]) -> int:
        """
        批量保存K线数据

        Args:
            klines: K线数据列表

        Returns:
            成功保存的数量
        """
        if not klines:
            return 0

        session = self.get_session()
        success_count = 0

        try:
            for kline_data in klines:
                kline_record = KlineData(
                    symbol=kline_data.get('symbol'),
                    exchange=kline_data.get('exchange', 'binance'),
                    timeframe=kline_data.get('timeframe', '5m'),
                    open_time=kline_data.get('open_time'),
                    close_time=kline_data.get('close_time'),
                    timestamp=kline_data.get('timestamp'),
                    open_price=kline_data.get('open'),
                    high_price=kline_data.get('high'),
                    low_price=kline_data.get('low'),
                    close_price=kline_data.get('close'),
                    volume=kline_data.get('volume'),
                    quote_volume=kline_data.get('quote_volume'),
                    number_of_trades=kline_data.get('trades'),
                    taker_buy_base_volume=kline_data.get('taker_buy_base'),
                    taker_buy_quote_volume=kline_data.get('taker_buy_quote')
                )
                session.add(kline_record)

            session.commit()
            success_count = len(klines)
            logger.info(f"批量保存K线数据成功: {success_count} 条")

        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"批量保存K线数据失败: {e}")
        finally:
            session.close()

        return success_count

    def save_trade_data(self, trade_data: Dict) -> bool:
        """
        保存交易数据

        Args:
            trade_data: 交易数据字典

        Returns:
            是否保存成功
        """
        session = self.get_session()
        try:
            trade_record = TradeData(
                trade_id=trade_data.get('trade_id'),
                symbol=trade_data.get('symbol'),
                exchange=trade_data.get('exchange', 'binance'),
                price=trade_data.get('price'),
                quantity=trade_data.get('quantity') or trade_data.get('amount'),
                quote_quantity=trade_data.get('quote_quantity') or trade_data.get('cost'),
                trade_time=trade_data.get('trade_time') or int(trade_data.get('timestamp').timestamp() * 1000),
                timestamp=trade_data.get('timestamp'),
                is_buyer_maker=trade_data.get('is_buyer_maker'),
                is_best_match=trade_data.get('is_best_match'),
                side=trade_data.get('side')
            )

            session.add(trade_record)
            session.commit()
            logger.debug(f"保存交易数据成功: {trade_data.get('symbol')}")
            return True

        except SQLAlchemyError as e:
            session.rollback()
            if 'Duplicate entry' in str(e):
                logger.debug(f"交易数据已存在,跳过: {trade_data.get('trade_id')}")
            else:
                logger.error(f"保存交易数据失败: {e}")
            return False
        finally:
            session.close()

    def save_trades_batch(self, trades: List[Dict]) -> int:
        """
        批量保存交易数据

        Args:
            trades: 交易数据列表

        Returns:
            成功保存的数量
        """
        if not trades:
            return 0

        session = self.get_session()
        success_count = 0

        for trade_data in trades:
            try:
                trade_record = TradeData(
                    trade_id=trade_data.get('trade_id'),
                    symbol=trade_data.get('symbol'),
                    exchange=trade_data.get('exchange', 'binance'),
                    price=trade_data.get('price'),
                    quantity=trade_data.get('quantity') or trade_data.get('amount'),
                    quote_quantity=trade_data.get('quote_quantity') or trade_data.get('cost'),
                    trade_time=trade_data.get('trade_time') or int(trade_data.get('timestamp').timestamp() * 1000),
                    timestamp=trade_data.get('timestamp'),
                    is_buyer_maker=trade_data.get('is_buyer_maker'),
                    is_best_match=trade_data.get('is_best_match'),
                    side=trade_data.get('side')
                )

                session.add(trade_record)
                session.commit()
                success_count += 1

            except SQLAlchemyError as e:
                session.rollback()
                if 'Duplicate entry' not in str(e):
                    logger.error(f"保存交易数据失败: {e}")

        session.close()
        logger.info(f"批量保存交易数据: 成功 {success_count}/{len(trades)} 条")
        return success_count

    def save_orderbook_data(self, orderbook_data: Dict) -> bool:
        """
        保存订单簿数据

        Args:
            orderbook_data: 订单簿数据字典

        Returns:
            是否保存成功
        """
        session = self.get_session()
        try:
            bids = orderbook_data.get('bids', [])
            asks = orderbook_data.get('asks', [])

            best_bid = bids[0][0] if bids else None
            best_ask = asks[0][0] if asks else None
            spread = (best_ask - best_bid) if (best_bid and best_ask) else None

            orderbook_record = OrderBookData(
                symbol=orderbook_data.get('symbol'),
                exchange=orderbook_data.get('exchange', 'binance'),
                timestamp=orderbook_data.get('timestamp', datetime.now()),
                best_bid=best_bid,
                best_ask=best_ask,
                spread=spread,
                bid_volume=orderbook_data.get('bid_volume'),
                ask_volume=orderbook_data.get('ask_volume')
            )

            session.add(orderbook_record)
            session.commit()
            logger.debug(f"保存订单簿数据成功: {orderbook_data.get('symbol')}")
            return True

        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"保存订单簿数据失败: {e}")
            return False
        finally:
            session.close()

    def save_news_data(self, news_data: Dict) -> bool:
        """
        保存新闻数据

        Args:
            news_data: 新闻数据字典

        Returns:
            是否保存成功
        """
        session = self.get_session()
        try:
            # 解析发布时间
            published_datetime = self._parse_news_datetime(news_data.get('published_at', ''))

            # 处理symbols列表
            symbols_list = news_data.get('symbols', [])
            symbols_str = ','.join(symbols_list) if symbols_list else ''

            # 提取votes数据
            votes = news_data.get('votes', {})

            news_record = NewsData(
                news_id=news_data.get('id'),
                title=news_data.get('title', '')[:500],  # 限制长度
                url=news_data.get('url', ''),
                source=news_data.get('source', ''),
                description=news_data.get('description', '')[:2000],
                published_at=news_data.get('published_at', ''),
                published_datetime=published_datetime,
                symbols=symbols_str,
                sentiment=news_data.get('sentiment', 'neutral'),
                sentiment_score=news_data.get('sentiment_score'),
                votes_positive=votes.get('positive', 0) if isinstance(votes, dict) else 0,
                votes_negative=votes.get('negative', 0) if isinstance(votes, dict) else 0,
                votes_important=votes.get('important', 0) if isinstance(votes, dict) else 0,
                data_source=news_data.get('data_source', 'unknown')
            )

            session.add(news_record)
            session.commit()
            logger.debug(f"保存新闻成功: {news_data.get('title', '')[:50]}")
            return True

        except SQLAlchemyError as e:
            session.rollback()
            if 'Duplicate entry' in str(e):
                logger.debug(f"新闻已存在,跳过: {news_data.get('url', '')}")
            else:
                logger.error(f"保存新闻数据失败: {e}")
            return False
        finally:
            session.close()

    def save_news_batch(self, news_list: List[Dict]) -> int:
        """
        批量保存新闻数据

        Args:
            news_list: 新闻数据列表

        Returns:
            成功保存的数量
        """
        if not news_list:
            return 0

        session = self.get_session()
        success_count = 0

        for news_data in news_list:
            try:
                # 解析发布时间
                published_datetime = self._parse_news_datetime(news_data.get('published_at', ''))

                # 处理symbols列表
                symbols_list = news_data.get('symbols', [])
                symbols_str = ','.join(symbols_list) if symbols_list else ''

                # 提取votes数据
                votes = news_data.get('votes', {})

                news_record = NewsData(
                    news_id=news_data.get('id'),
                    title=news_data.get('title', '')[:500],
                    url=news_data.get('url', ''),
                    source=news_data.get('source', ''),
                    description=news_data.get('description', '')[:2000],
                    published_at=news_data.get('published_at', ''),
                    published_datetime=published_datetime,
                    symbols=symbols_str,
                    sentiment=news_data.get('sentiment', 'neutral'),
                    sentiment_score=news_data.get('sentiment_score'),
                    votes_positive=votes.get('positive', 0) if isinstance(votes, dict) else 0,
                    votes_negative=votes.get('negative', 0) if isinstance(votes, dict) else 0,
                    votes_important=votes.get('important', 0) if isinstance(votes, dict) else 0,
                    data_source=news_data.get('data_source', 'unknown')
                )

                session.add(news_record)
                session.commit()
                success_count += 1

            except SQLAlchemyError as e:
                session.rollback()
                if 'Duplicate entry' not in str(e):
                    logger.error(f"保存新闻失败: {e}")

        session.close()
        logger.info(f"批量保存新闻: 成功 {success_count}/{len(news_list)} 条")
        return success_count

    def save_funding_rate_data(self, funding_data: Dict) -> bool:
        """
        保存资金费率数据

        Args:
            funding_data: 资金费率数据字典

        Returns:
            是否保存成功
        """
        session = self.get_session()
        try:
            funding_record = FundingRateData(
                symbol=funding_data.get('symbol'),
                exchange=funding_data.get('exchange', 'binance'),
                funding_rate=funding_data.get('funding_rate'),
                funding_time=funding_data.get('funding_time'),
                timestamp=funding_data.get('timestamp'),
                mark_price=funding_data.get('mark_price'),
                index_price=funding_data.get('index_price'),
                next_funding_time=funding_data.get('next_funding_time')
            )

            session.add(funding_record)
            session.commit()
            logger.debug(f"保存资金费率成功: {funding_data.get('symbol')} - {funding_data.get('funding_rate')}")
            return True

        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"保存资金费率数据失败: {e}")
            return False
        finally:
            session.close()

    def save_open_interest_data(self, oi_data: Dict) -> bool:
        """
        保存持仓量数据

        Args:
            oi_data: 持仓量数据字典

        Returns:
            是否保存成功
        """
        session = self.get_session()
        try:
            oi_record = FuturesOpenInterest(
                symbol=oi_data.get('symbol'),
                exchange=oi_data.get('exchange', 'binance_futures'),
                open_interest=oi_data.get('open_interest'),
                open_interest_value=oi_data.get('open_interest_value'),
                timestamp=oi_data.get('timestamp')
            )

            session.add(oi_record)
            session.commit()
            logger.debug(f"保存持仓量成功: {oi_data.get('symbol')} - {oi_data.get('open_interest')}")
            return True

        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"保存持仓量数据失败: {e}")
            return False
        finally:
            session.close()

    def save_long_short_ratio_data(self, ls_data: Dict) -> bool:
        """
        保存多空比数据

        Args:
            ls_data: 多空比数据字典

        Returns:
            是否保存成功
        """
        session = self.get_session()
        try:
            # 如果关键字段为None，使用默认值0或跳过保存
            long_account = ls_data.get('long_account')
            short_account = ls_data.get('short_account')
            long_short_ratio = ls_data.get('long_short_ratio')
            
            # 如果所有账户数相关字段都为None，但有持仓量数据，则使用持仓量数据计算或跳过账户数字段
            if long_account is None and short_account is None and long_short_ratio is None:
                # 如果有持仓量数据，可以跳过保存账户数数据，或者使用0作为默认值
                # 这里使用0作为默认值，因为数据库字段不允许NULL
                long_account = 0.0
                short_account = 0.0
                long_short_ratio = 0.0
            else:
                # 确保不为None
                long_account = long_account if long_account is not None else 0.0
                short_account = short_account if short_account is not None else 0.0
                long_short_ratio = long_short_ratio if long_short_ratio is not None else 0.0
            
            ls_record = FuturesLongShortRatio(
                symbol=ls_data.get('symbol'),
                exchange=ls_data.get('exchange', 'binance_futures'),
                period=ls_data.get('period', '5m'),
                long_account=long_account,
                short_account=short_account,
                long_short_ratio=long_short_ratio,
                long_position=ls_data.get('long_position'),  # 持仓量比 - 新增
                short_position=ls_data.get('short_position'),  # 持仓量比 - 新增
                long_short_position_ratio=ls_data.get('long_short_position_ratio'),  # 持仓量比 - 新增
                timestamp=ls_data.get('timestamp')
            )

            session.add(ls_record)
            session.commit()
            logger.debug(f"保存多空比成功: {ls_data.get('symbol')} - {ls_data.get('long_short_ratio')}")
            return True

        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"保存多空比数据失败: {e}")
            return False
        finally:
            session.close()

    def save_funding_rate_batch(self, funding_list: List[Dict]) -> int:
        """
        批量保存资金费率数据

        Args:
            funding_list: 资金费率数据列表

        Returns:
            成功保存的数量
        """
        if not funding_list:
            return 0

        session = self.get_session()
        success_count = 0

        try:
            for funding_data in funding_list:
                funding_record = FundingRateData(
                    symbol=funding_data.get('symbol'),
                    exchange=funding_data.get('exchange', 'binance'),
                    funding_rate=funding_data.get('funding_rate'),
                    funding_time=funding_data.get('funding_time'),
                    timestamp=funding_data.get('timestamp'),
                    mark_price=funding_data.get('mark_price'),
                    index_price=funding_data.get('index_price'),
                    next_funding_time=funding_data.get('next_funding_time')
                )
                session.add(funding_record)

            session.commit()
            success_count = len(funding_list)
            logger.info(f"批量保存资金费率数据成功: {success_count} 条")

        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"批量保存资金费率数据失败: {e}")
        finally:
            session.close()

        return success_count

    def _parse_news_datetime(self, date_str: str) -> datetime:
        """
        解析新闻发布时间（统一转换为UTC时间的naive datetime）
        
        注意：数据库存储的是UTC时间的naive datetime，以便统一时区处理
        """
        if not date_str:
            # 如果没有时间字符串，使用当前UTC时间
            return datetime.now(timezone.utc).replace(tzinfo=None)

        try:
            # ISO格式
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            # 如果有时区信息，转换为UTC时间
            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone.utc)
            # 转换为naive datetime（UTC时间）
            dt = dt.replace(tzinfo=None)
            return dt
        except:
            try:
                # RSS格式
                from email.utils import parsedate_to_datetime
                dt = parsedate_to_datetime(date_str)
                # 如果有时区信息，转换为UTC时间
                if dt.tzinfo is not None:
                    dt = dt.astimezone(timezone.utc)
                # 转换为naive datetime（UTC时间）
                dt = dt.replace(tzinfo=None)
                return dt
            except:
                # 解析失败，使用当前UTC时间
                return datetime.now(timezone.utc).replace(tzinfo=None)

    def cleanup_old_data(self, days: int = 90):
        """
        清理旧数据

        Args:
            days: 保留天数
        """
        session = self.get_session()
        try:
            from datetime import timedelta
            cutoff_date = datetime.now() - timedelta(days=days)

            # 清理价格数据
            deleted = session.query(PriceData).filter(PriceData.timestamp < cutoff_date).delete()
            logger.info(f"清理 {deleted} 条旧价格数据")

            # 清理新闻数据
            deleted_news = session.query(NewsData).filter(NewsData.published_datetime < cutoff_date).delete()
            logger.info(f"清理 {deleted_news} 条旧新闻数据")

            session.commit()

        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"清理旧数据失败: {e}")
        finally:
            session.close()

    def close(self):
        """关闭数据库连接"""
        if self.engine:
            self.engine.dispose()
            logger.info("数据库连接已关闭")

    # ==================== 聪明钱监控相关方法 ====================

    def save_smart_money_address(self, address_data: Dict) -> bool:
        """
        保存聪明钱地址

        Args:
            address_data: 地址数据字典

        Returns:
            是否保存成功
        """
        session = self.get_session()
        try:
            # 检查地址是否已存在
            existing = session.query(SmartMoneyAddress).filter(
                SmartMoneyAddress.address == address_data.get('address')
            ).first()

            if existing:
                # 更新现有地址信息
                existing.label = address_data.get('label', existing.label)
                existing.address_type = address_data.get('address_type', existing.address_type)
                existing.total_value_usd = address_data.get('total_value_usd', existing.total_value_usd)
                existing.win_rate = address_data.get('win_rate', existing.win_rate)
                existing.total_trades = address_data.get('total_trades', existing.total_trades)
                existing.profitable_trades = address_data.get('profitable_trades', existing.profitable_trades)
                existing.is_active = address_data.get('is_active', existing.is_active)
                existing.last_active = address_data.get('last_active', existing.last_active)
                existing.updated_at = datetime.now()
                logger.debug(f"更新聪明钱地址: {address_data.get('address')[:10]}...")
            else:
                # 创建新地址
                address_record = SmartMoneyAddress(
                    address=address_data.get('address'),
                    blockchain=address_data.get('blockchain', 'ethereum'),
                    label=address_data.get('label'),
                    address_type=address_data.get('address_type', 'whale'),
                    total_value_usd=address_data.get('total_value_usd'),
                    win_rate=address_data.get('win_rate'),
                    total_trades=address_data.get('total_trades', 0),
                    profitable_trades=address_data.get('profitable_trades', 0),
                    is_active=address_data.get('is_active', True),
                    first_seen=address_data.get('first_seen', datetime.now()),
                    last_active=address_data.get('last_active', datetime.now()),
                    data_source=address_data.get('data_source', 'manual')
                )
                session.add(address_record)
                logger.debug(f"新增聪明钱地址: {address_data.get('address')[:10]}...")

            session.commit()
            return True

        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"保存聪明钱地址失败: {e}")
            return False
        finally:
            session.close()

    def save_smart_money_transaction(self, tx_data: Dict) -> bool:
        """
        保存聪明钱交易记录

        Args:
            tx_data: 交易数据字典

        Returns:
            是否保存成功
        """
        session = self.get_session()
        try:
            tx_record = SmartMoneyTransaction(
                tx_hash=tx_data.get('tx_hash'),
                address=tx_data.get('address'),
                blockchain=tx_data.get('blockchain', 'ethereum'),
                token_address=tx_data.get('token_address'),
                token_symbol=tx_data.get('token_symbol'),
                token_name=tx_data.get('token_name'),
                action=tx_data.get('action'),
                amount=tx_data.get('amount'),
                amount_usd=tx_data.get('amount_usd'),
                price_usd=tx_data.get('price_usd'),
                from_address=tx_data.get('from_address'),
                to_address=tx_data.get('to_address'),
                dex_name=tx_data.get('dex_name'),
                contract_address=tx_data.get('contract_address'),
                block_number=tx_data.get('block_number'),
                block_timestamp=tx_data.get('block_timestamp'),
                timestamp=tx_data.get('timestamp'),
                gas_used=tx_data.get('gas_used'),
                gas_price=tx_data.get('gas_price'),
                transaction_fee=tx_data.get('transaction_fee'),
                is_large_transaction=tx_data.get('is_large_transaction', False),
                is_first_buy=tx_data.get('is_first_buy', False),
                signal_strength=tx_data.get('signal_strength', 'weak')
            )

            session.add(tx_record)
            session.commit()

            # 安全格式化日志（处理 None 值）
            amount_usd = tx_data.get('amount_usd') or 0
            logger.debug(f"保存聪明钱交易: {tx_data.get('token_symbol')} {tx_data.get('action')} ${amount_usd:,.0f}")
            return True

        except SQLAlchemyError as e:
            session.rollback()
            if 'Duplicate entry' in str(e):
                logger.debug(f"交易已存在,跳过: {tx_data.get('tx_hash')}")
            else:
                logger.error(f"保存聪明钱交易失败: {e}")
            return False
        finally:
            session.close()

    def save_smart_money_transactions_batch(self, txs: List[Dict]) -> int:
        """
        批量保存聪明钱交易

        Args:
            txs: 交易数据列表

        Returns:
            成功保存的数量
        """
        if not txs:
            return 0

        success_count = 0
        for tx_data in txs:
            if self.save_smart_money_transaction(tx_data):
                success_count += 1

        logger.info(f"批量保存聪明钱交易: 成功 {success_count}/{len(txs)} 条")
        return success_count

    def save_smart_money_signal(self, signal_data: Dict) -> bool:
        """
        保存聪明钱信号

        Args:
            signal_data: 信号数据字典

        Returns:
            是否保存成功
        """
        session = self.get_session()
        try:
            signal_record = SmartMoneySignal(
                token_symbol=signal_data.get('token_symbol'),
                token_address=signal_data.get('token_address'),
                blockchain=signal_data.get('blockchain', 'ethereum'),
                signal_type=signal_data.get('signal_type'),
                signal_strength=signal_data.get('signal_strength'),
                confidence_score=signal_data.get('confidence_score'),
                smart_money_count=signal_data.get('smart_money_count', 0),
                total_buy_amount_usd=signal_data.get('total_buy_amount_usd', 0),
                total_sell_amount_usd=signal_data.get('total_sell_amount_usd', 0),
                net_flow_usd=signal_data.get('net_flow_usd'),
                transaction_count=signal_data.get('transaction_count', 0),
                price_before=signal_data.get('price_before'),
                price_current=signal_data.get('price_current'),
                price_change_pct=signal_data.get('price_change_pct'),
                signal_start_time=signal_data.get('signal_start_time'),
                signal_end_time=signal_data.get('signal_end_time'),
                timestamp=signal_data.get('timestamp', datetime.now()),
                related_tx_hashes=signal_data.get('related_tx_hashes'),
                top_addresses=signal_data.get('top_addresses'),
                is_active=signal_data.get('is_active', True),
                is_verified=signal_data.get('is_verified', False)
            )

            session.add(signal_record)
            session.commit()
            logger.info(f"保存聪明钱信号: {signal_data.get('token_symbol')} - {signal_data.get('signal_type')} ({signal_data.get('signal_strength')})")
            return True

        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"保存聪明钱信号失败: {e}")
            return False
        finally:
            session.close()

    def get_smart_money_addresses(self, blockchain: str = None, active_only: bool = True) -> List[Dict]:
        """
        获取监控的聪明钱地址列表

        Args:
            blockchain: 区块链网络(可选)
            active_only: 只返回活跃地址

        Returns:
            地址列表
        """
        session = self.get_session()
        try:
            query = session.query(SmartMoneyAddress)

            if active_only:
                query = query.filter(SmartMoneyAddress.is_active == True)

            if blockchain:
                query = query.filter(SmartMoneyAddress.blockchain == blockchain)

            addresses = query.all()

            return [{
                'address': addr.address,
                'blockchain': addr.blockchain,
                'label': addr.label,
                'address_type': addr.address_type,
                'total_value_usd': float(addr.total_value_usd) if addr.total_value_usd else 0,
                'win_rate': float(addr.win_rate) if addr.win_rate else 0,
                'total_trades': addr.total_trades,
                'profitable_trades': addr.profitable_trades,
                'last_active': addr.last_active.strftime('%Y-%m-%d %H:%M:%S') if addr.last_active else None
            } for addr in addresses]

        except Exception as e:
            logger.error(f"获取聪明钱地址失败: {e}")
            return []
        finally:
            session.close()

    def get_recent_smart_money_transactions(
        self,
        token_symbol: str = None,
        hours: int = 24,
        action: str = None,
        limit: int = 100
    ) -> List[Dict]:
        """
        获取最近的聪明钱交易

        Args:
            token_symbol: 代币符号(可选)
            hours: 时间范围(小时)
            action: 交易类型 buy/sell(可选)
            limit: 返回数量限制

        Returns:
            交易列表
        """
        session = self.get_session()
        try:
            from datetime import timedelta
            from sqlalchemy import desc

            cutoff_time = datetime.now() - timedelta(hours=hours)

            query = session.query(SmartMoneyTransaction).filter(
                SmartMoneyTransaction.timestamp >= cutoff_time
            )

            if token_symbol:
                query = query.filter(SmartMoneyTransaction.token_symbol == token_symbol)

            if action:
                query = query.filter(SmartMoneyTransaction.action == action)

            transactions = query.order_by(desc(SmartMoneyTransaction.timestamp)).limit(limit).all()

            return [{
                'tx_hash': tx.tx_hash,
                'address': tx.address,
                'blockchain': tx.blockchain,
                'token_symbol': tx.token_symbol,
                'token_name': tx.token_name,
                'action': tx.action,
                'amount': float(tx.amount),
                'amount_usd': float(tx.amount_usd) if tx.amount_usd else 0,
                'price_usd': float(tx.price_usd) if tx.price_usd else 0,
                'timestamp': tx.timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'is_large_transaction': tx.is_large_transaction,
                'signal_strength': tx.signal_strength
            } for tx in transactions]

        except Exception as e:
            logger.error(f"获取聪明钱交易失败: {e}")
            return []
        finally:
            session.close()

    def get_active_smart_money_signals(self, limit: int = 10) -> List[Dict]:
        """
        获取活跃的聪明钱信号

        Args:
            limit: 返回数量限制

        Returns:
            信号列表
        """
        session = self.get_session()
        try:
            from sqlalchemy import desc

            signals = (
                session.query(SmartMoneySignal)
                .filter(SmartMoneySignal.is_active == True)
                .order_by(desc(SmartMoneySignal.confidence_score))
                .order_by(desc(SmartMoneySignal.timestamp))
                .limit(limit)
                .all()
            )

            return [{
                'token_symbol': sig.token_symbol,
                'blockchain': sig.blockchain,
                'signal_type': sig.signal_type,
                'signal_strength': sig.signal_strength,
                'confidence_score': float(sig.confidence_score),
                'smart_money_count': sig.smart_money_count,
                'total_buy_amount_usd': float(sig.total_buy_amount_usd) if sig.total_buy_amount_usd else 0,
                'total_sell_amount_usd': float(sig.total_sell_amount_usd) if sig.total_sell_amount_usd else 0,
                'net_flow_usd': float(sig.net_flow_usd) if sig.net_flow_usd else 0,
                'transaction_count': sig.transaction_count,
                'timestamp': sig.timestamp.strftime('%Y-%m-%d %H:%M:%S')
            } for sig in signals]

        except Exception as e:
            logger.error(f"获取聪明钱信号失败: {e}")
            return []
        finally:
            session.close()

    def get_smart_money_signal_by_token(self, token_symbol: str) -> Optional[Dict]:
        """
        获取指定代币的最新聪明钱信号

        Args:
            token_symbol: 代币符号

        Returns:
            信号数据或None
        """
        session = self.get_session()
        try:
            from sqlalchemy import desc

            signal = (
                session.query(SmartMoneySignal)
                .filter(SmartMoneySignal.token_symbol == token_symbol)
                .filter(SmartMoneySignal.is_active == True)
                .order_by(desc(SmartMoneySignal.timestamp))
                .first()
            )

            if not signal:
                return None

            return {
                'token_symbol': signal.token_symbol,
                'blockchain': signal.blockchain,
                'signal_type': signal.signal_type,
                'signal_strength': signal.signal_strength,
                'confidence_score': float(signal.confidence_score),
                'smart_money_count': signal.smart_money_count,
                'total_buy_amount_usd': float(signal.total_buy_amount_usd) if signal.total_buy_amount_usd else 0,
                'total_sell_amount_usd': float(signal.total_sell_amount_usd) if signal.total_sell_amount_usd else 0,
                'net_flow_usd': float(signal.net_flow_usd) if signal.net_flow_usd else 0,
                'transaction_count': signal.transaction_count,
                'price_change_pct': float(signal.price_change_pct) if signal.price_change_pct else 0,
                'timestamp': signal.timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'top_addresses': signal.top_addresses
            }

        except Exception as e:
            logger.error(f"获取代币聪明钱信号失败: {e}")
            return None
        finally:
            session.close()

    # ==================== 策略交易记录相关方法 ====================

    def save_strategy_trade_record(self, trade_data: Dict) -> bool:
        """
        保存策略交易记录

        Args:
            trade_data: 交易数据字典，包含以下字段：
                - strategy_id: 策略ID
                - strategy_name: 策略名称
                - account_id: 账户ID
                - symbol: 交易对
                - action: 交易动作 (BUY/SELL/CLOSE)
                - direction: 方向 (long/short)
                - position_side: 持仓方向 (LONG/SHORT)
                - entry_price: 开仓价格
                - exit_price: 平仓价格
                - quantity: 数量
                - leverage: 杠杆倍数
                - margin: 保证金
                - total_value: 总价值
                - fee: 手续费
                - realized_pnl: 已实现盈亏
                - position_id: 持仓ID
                - order_id: 订单ID
                - signal_id: 信号ID
                - reason: 交易原因
                - trade_time: 交易时间

        Returns:
            是否保存成功
        """
        session = self.get_session()
        try:
            trade_record = StrategyTradeRecord(
                strategy_id=trade_data.get('strategy_id'),
                strategy_name=trade_data.get('strategy_name'),
                account_id=trade_data.get('account_id'),
                symbol=trade_data.get('symbol'),
                action=trade_data.get('action'),
                direction=trade_data.get('direction'),
                position_side=trade_data.get('position_side'),
                entry_price=trade_data.get('entry_price'),
                exit_price=trade_data.get('exit_price'),
                quantity=trade_data.get('quantity'),
                leverage=trade_data.get('leverage', 1),
                margin=trade_data.get('margin'),
                total_value=trade_data.get('total_value'),
                fee=trade_data.get('fee'),
                realized_pnl=trade_data.get('realized_pnl'),
                position_id=trade_data.get('position_id'),
                order_id=trade_data.get('order_id'),
                signal_id=trade_data.get('signal_id'),
                reason=trade_data.get('reason'),
                trade_time=trade_data.get('trade_time', datetime.now())
            )

            session.add(trade_record)
            session.commit()
            logger.info(f"保存策略交易记录成功: {trade_data.get('strategy_name')} - {trade_data.get('symbol')} {trade_data.get('action')}")
            return True

        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"保存策略交易记录失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
        except Exception as e:
            session.rollback()
            logger.error(f"保存策略交易记录时发生未知错误: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
        finally:
            session.close()
    
    def save_strategy_test_record(self, trade_data: Dict) -> bool:
        """
        保存策略测试交易记录

        Args:
            trade_data: 交易数据字典，包含以下字段：
                - strategy_id: 策略ID
                - strategy_name: 策略名称
                - account_id: 账户ID
                - symbol: 交易对
                - action: 交易动作 (BUY/SELL/CLOSE)
                - direction: 方向 (long/short)
                - position_side: 持仓方向 (LONG/SHORT)
                - entry_price: 开仓价格
                - exit_price: 平仓价格
                - quantity: 数量
                - leverage: 杠杆倍数
                - margin: 保证金
                - total_value: 总价值
                - fee: 手续费
                - realized_pnl: 已实现盈亏
                - position_id: 持仓ID
                - order_id: 订单ID
                - signal_id: 信号ID
                - reason: 交易原因
                - trade_time: 交易时间

        Returns:
            是否保存成功
        """
        session = self.get_session()
        try:
            test_record = StrategyTestRecord(
                strategy_id=trade_data.get('strategy_id'),
                strategy_name=trade_data.get('strategy_name'),
                account_id=trade_data.get('account_id'),
                symbol=trade_data.get('symbol'),
                action=trade_data.get('action'),
                direction=trade_data.get('direction'),
                position_side=trade_data.get('position_side'),
                entry_price=trade_data.get('entry_price'),
                exit_price=trade_data.get('exit_price'),
                quantity=trade_data.get('quantity'),
                leverage=trade_data.get('leverage', 1),
                margin=trade_data.get('margin'),
                total_value=trade_data.get('total_value'),
                fee=trade_data.get('fee'),
                realized_pnl=trade_data.get('realized_pnl'),
                position_id=trade_data.get('position_id'),
                order_id=trade_data.get('order_id'),
                signal_id=trade_data.get('signal_id'),
                reason=trade_data.get('reason'),
                trade_time=trade_data.get('trade_time', datetime.now())
            )

            session.add(test_record)
            session.commit()
            logger.info(f"保存策略测试交易记录成功到 strategy_test_records 表: {trade_data.get('strategy_name')} - {trade_data.get('symbol')} {trade_data.get('action')}")
            return True

        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"保存策略测试交易记录失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
        except Exception as e:
            session.rollback()
            logger.error(f"保存策略测试交易记录时发生未知错误: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
        finally:
            session.close()
    
    def save_strategy_capital_record(self, capital_data: Dict) -> bool:
        """
        保存策略资金管理记录
        
        Args:
            capital_data: 资金数据字典，包含以下字段：
                - strategy_id: 策略ID
                - strategy_name: 策略名称
                - account_id: 账户ID
                - symbol: 交易对
                - trade_record_id: 关联的交易记录ID（可选）
                - position_id: 关联的持仓ID（可选）
                - order_id: 关联的订单ID（可选）
                - change_type: 资金变化类型 (FROZEN/UNFROZEN/REALIZED_PNL/FEE/DEPOSIT/WITHDRAW)
                - action: 交易动作 (BUY/SELL/CLOSE)
                - direction: 方向 (long/short)
                - amount_change: 金额变化（正数表示增加，负数表示减少）
                - balance_before: 变化前余额
                - balance_after: 变化后余额
                - frozen_before: 变化前冻结金额
                - frozen_after: 变化后冻结金额
                - available_before: 变化前可用余额
                - available_after: 变化后可用余额
                - entry_price: 开仓价格（可选）
                - exit_price: 平仓价格（可选）
                - quantity: 数量（可选）
                - leverage: 杠杆倍数（可选）
                - margin: 保证金（可选）
                - realized_pnl: 已实现盈亏（可选）
                - fee: 手续费（可选）
                - reason: 资金变化原因
                - description: 详细描述（可选）
                - change_time: 资金变化时间
        
        Returns:
            bool: 保存成功返回True，失败返回False
        """
        import pymysql
        
        try:
            # 从配置中获取MySQL连接参数
            mysql_config = self.config.get('mysql', {})
            connection = pymysql.connect(
                host=mysql_config.get('host', 'localhost'),
                port=mysql_config.get('port', 3306),
                user=mysql_config.get('user', 'root'),
                password=mysql_config.get('password', ''),
                database=mysql_config.get('database', 'binance-data'),
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor
            )
            cursor = connection.cursor()
            
            try:
                insert_sql = """
                    INSERT INTO strategy_capital_management (
                        strategy_id, strategy_name, account_id, symbol,
                        trade_record_id, position_id, order_id,
                        change_type, action, direction,
                        amount_change, balance_before, balance_after,
                        frozen_before, frozen_after, available_before, available_after,
                        entry_price, exit_price, quantity, leverage, margin,
                        realized_pnl, fee, reason, description, change_time
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s
                    )
                """
                
                cursor.execute(insert_sql, (
                    capital_data.get('strategy_id'),
                    capital_data.get('strategy_name'),
                    capital_data.get('account_id'),
                    capital_data.get('symbol'),
                    capital_data.get('trade_record_id'),
                    capital_data.get('position_id'),
                    capital_data.get('order_id'),
                    capital_data.get('change_type'),
                    capital_data.get('action'),
                    capital_data.get('direction'),
                    capital_data.get('amount_change'),
                    capital_data.get('balance_before'),
                    capital_data.get('balance_after'),
                    capital_data.get('frozen_before'),
                    capital_data.get('frozen_after'),
                    capital_data.get('available_before'),
                    capital_data.get('available_after'),
                    capital_data.get('entry_price'),
                    capital_data.get('exit_price'),
                    capital_data.get('quantity'),
                    capital_data.get('leverage'),
                    capital_data.get('margin'),
                    capital_data.get('realized_pnl'),
                    capital_data.get('fee'),
                    capital_data.get('reason'),
                    capital_data.get('description'),
                    capital_data.get('change_time', datetime.now())
                ))
                
                connection.commit()
                logger.info(f"保存策略资金管理记录成功: {capital_data.get('strategy_name')} - {capital_data.get('symbol')} {capital_data.get('change_type')} {capital_data.get('amount_change')}")
                return True
                
            except Exception as e:
                connection.rollback()
                logger.error(f"保存策略资金管理记录失败: {e}")
                import traceback
                logger.error(traceback.format_exc())
                return False
            finally:
                cursor.close()
                connection.close()
                
        except Exception as e:
            logger.error(f"保存策略资金管理记录时发生未知错误: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    # ==================== 数据查询方法 ====================

    def get_latest_kline(self, symbol: str, timeframe: str = '1h'):
        """
        获取最新的单条K线数据（返回对象）

        Args:
            symbol: 交易对符号
            timeframe: 时间周期

        Returns:
            KlineData对象或None
        """
        session = self.get_session()
        try:
            from sqlalchemy import desc

            kline = (
                session.query(KlineData)
                .filter(KlineData.symbol == symbol)
                .filter(KlineData.timeframe == timeframe)
                .order_by(desc(KlineData.open_time))
                .first()
            )

            return kline

        except Exception as e:
            logger.error(f"获取K线数据失败: {e}")
            return None
        finally:
            session.close()

    def get_latest_klines(self, symbol: str, timeframe: str = '1h', limit: int = 100):
        """
        获取最新的多条K线数据列表（返回对象列表）

        Args:
            symbol: 交易对符号
            timeframe: 时间周期
            limit: 返回数量

        Returns:
            KlineData对象列表
        """
        session = self.get_session()
        try:
            from sqlalchemy import desc

            klines = (
                session.query(KlineData)
                .filter(KlineData.symbol == symbol)
                .filter(KlineData.timeframe == timeframe)
                .order_by(desc(KlineData.open_time))
                .limit(limit)
                .all()
            )

            return klines

        except Exception as e:
            logger.error(f"获取K线数据列表失败: {e}")
            return []
        finally:
            session.close()

    def get_klines(self, symbol: str, timeframe: str = '1h', start_time: datetime = None, limit: int = 100):
        """
        获取指定时间范围的K线数据（返回对象列表）

        Args:
            symbol: 交易对符号
            timeframe: 时间周期
            start_time: 起始时间
            limit: 返回数量

        Returns:
            KlineData对象列表
        """
        session = self.get_session()
        try:
            from sqlalchemy import desc

            query = (
                session.query(KlineData)
                .filter(KlineData.symbol == symbol)
                .filter(KlineData.timeframe == timeframe)
            )

            if start_time:
                query = query.filter(KlineData.timestamp >= start_time)

            klines = query.order_by(desc(KlineData.open_time)).limit(limit).all()

            return klines

        except Exception as e:
            logger.error(f"获取K线数据列表失败: {e}")
            return []
        finally:
            session.close()

    def get_kline_at_time(self, symbol: str, timeframe: str, target_time: datetime):
        """
        获取指定时间的K线数据（返回对象）

        Args:
            symbol: 交易对符号
            timeframe: 时间周期
            target_time: 目标时间

        Returns:
            KlineData对象或None
        """
        session = self.get_session()
        try:
            from sqlalchemy import desc

            # 查找最接近目标时间的K线
            kline = (
                session.query(KlineData)
                .filter(KlineData.symbol == symbol)
                .filter(KlineData.timeframe == timeframe)
                .filter(KlineData.timestamp <= target_time)
                .order_by(desc(KlineData.timestamp))
                .first()
            )

            return kline

        except Exception as e:
            logger.error(f"获取指定时间K线失败: {e}")
            return None
        finally:
            session.close()

    def get_recent_news(self, hours: int = 24, symbols: str = None, limit: int = 50):
        """
        获取最近的新闻（返回对象列表，使用UTC时间）

        Args:
            hours: 时间范围(小时)
            symbols: 币种符号(可选)
            limit: 返回数量

        Returns:
            NewsData对象列表
        """
        session = self.get_session()
        try:
            from datetime import timedelta
            from sqlalchemy import desc, or_

            # 使用UTC时间计算24小时范围
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)
            # 转换为naive datetime以便与数据库中的时间比较（数据库存储的是UTC时间的naive datetime）
            cutoff_time = cutoff_time.replace(tzinfo=None)

            query = session.query(NewsData).filter(
                NewsData.published_datetime >= cutoff_time
            )

            if symbols:
                # 支持多个币种查询
                symbol_list = symbols.split(',') if ',' in symbols else [symbols]
                filters = [NewsData.symbols.like(f'%{s}%') for s in symbol_list]
                query = query.filter(or_(*filters))

            news_list = query.order_by(desc(NewsData.published_datetime)).limit(limit).all()

            return news_list

        except Exception as e:
            logger.error(f"获取新闻数据失败: {e}")
            return []
        finally:
            session.close()

    def get_latest_funding_rate(self, symbol: str):
        """
        获取最新的资金费率（返回对象）

        Args:
            symbol: 交易对符号

        Returns:
            FundingRateData对象或None
        """
        session = self.get_session()
        try:
            from sqlalchemy import desc

            funding = (
                session.query(FundingRateData)
                .filter(FundingRateData.symbol == symbol)
                .order_by(desc(FundingRateData.funding_time))
                .first()
            )

            return funding

        except Exception as e:
            logger.error(f"获取资金费率失败: {e}")
            return None
        finally:
            session.close()

    def get_latest_price(self, symbol: str):
        """
        获取最新价格（返回对象）

        Args:
            symbol: 交易对符号

        Returns:
            PriceData对象或None
        """
        session = self.get_session()
        try:
            from sqlalchemy import desc

            price = (
                session.query(PriceData)
                .filter(PriceData.symbol == symbol)
                .order_by(desc(PriceData.timestamp))
                .first()
            )

            return price

        except Exception as e:
            logger.error(f"获取价格数据失败: {e}")
            return None
        finally:
            session.close()

    def get_all_latest_prices(self) -> List[Dict]:
        """
        获取所有币种的最新价格（用于价格缓存服务）

        Returns:
            价格数据列表 [{"symbol": "BTC/USDT", "price": 50000, ...}, ...]
        """
        session = self.get_session()
        try:
            from sqlalchemy import desc, func

            # 获取每个币种的最新价格（使用子查询）
            subquery = (
                session.query(
                    PriceData.symbol,
                    func.max(PriceData.timestamp).label('max_timestamp')
                )
                .group_by(PriceData.symbol)
                .subquery()
            )

            prices = (
                session.query(PriceData)
                .join(
                    subquery,
                    (PriceData.symbol == subquery.c.symbol) &
                    (PriceData.timestamp == subquery.c.max_timestamp)
                )
                .all()
            )

            result = []
            for price in prices:
                price_value = float(price.price) if price.price else 0
                result.append({
                    'symbol': price.symbol,
                    'price': price_value,
                    'bid': float(price.bid_price) if hasattr(price, 'bid_price') and price.bid_price else price_value,
                    'ask': float(price.ask_price) if hasattr(price, 'ask_price') and price.ask_price else price_value,
                    'timestamp': price.timestamp
                })

            logger.debug(f"获取所有最新价格：{len(result)} 个币种")
            return result

        except Exception as e:
            logger.error(f"获取所有价格数据失败: {e}")
            return []
        finally:
            session.close()

    def get_latest_futures_data(self, symbol: str):
        """
        获取最新的合约数据（包括持仓量和多空比）

        Args:
            symbol: 交易对符号 (支持 BTC/USDT 或 BTCUSDT 格式)

        Returns:
            dict: 包含持仓量和多空比的字典
        """
        session = self.get_session()
        try:
            from sqlalchemy import desc, or_

            # 尝试两种格式: BTC/USDT 和 BTCUSDT
            symbol_no_slash = symbol.replace('/', '')

            # 获取最新的持仓量数据
            open_interest = (
                session.query(FuturesOpenInterest)
                .filter(
                    or_(
                        FuturesOpenInterest.symbol == symbol,
                        FuturesOpenInterest.symbol == symbol_no_slash
                    )
                )
                .filter(FuturesOpenInterest.exchange == 'binance_futures')
                .order_by(desc(FuturesOpenInterest.timestamp))
                .first()
            )

            # 获取最新的多空比数据
            long_short_ratio = (
                session.query(FuturesLongShortRatio)
                .filter(
                    or_(
                        FuturesLongShortRatio.symbol == symbol,
                        FuturesLongShortRatio.symbol == symbol_no_slash
                    )
                )
                .order_by(desc(FuturesLongShortRatio.timestamp))
                .first()
            )

            result = {
                'symbol': symbol,
                'open_interest': None,
                'long_short_ratio': None,
                'timestamp': None
            }

            if open_interest:
                result['open_interest'] = float(open_interest.open_interest)
                result['timestamp'] = open_interest.timestamp

            if long_short_ratio:
                # 账户数比
                result['long_short_ratio'] = {
                    'long_account': long_short_ratio.long_account,
                    'short_account': long_short_ratio.short_account,
                    'ratio': long_short_ratio.long_short_ratio
                }
                # 持仓量比（新增）
                if long_short_ratio.long_short_position_ratio:
                    result['long_short_position_ratio'] = {
                        'long_position': long_short_ratio.long_position,
                        'short_position': long_short_ratio.short_position,
                        'ratio': long_short_ratio.long_short_position_ratio
                    }
                if not result['timestamp']:
                    result['timestamp'] = long_short_ratio.timestamp

            return result

        except Exception as e:
            logger.error(f"获取合约数据失败: {e}")
            return None
        finally:
            session.close()

    def get_etf_summary(self, asset_type: str, days: int = 7):
        """
        获取ETF资金流向汇总数据

        Args:
            asset_type: 资产类型 ('BTC' 或 'ETH')
            days: 获取天数

        Returns:
            dict: ETF汇总数据
        """
        session = self.get_session()
        try:
            from sqlalchemy import text, desc
            from datetime import datetime, timedelta

            # 获取最近N天的每日汇总数据
            start_date = datetime.now() - timedelta(days=days)

            sql = text("""
                SELECT
                    trade_date,
                    total_net_inflow,
                    total_gross_inflow,
                    total_gross_outflow,
                    total_aum,
                    total_holdings,
                    etf_count,
                    inflow_count,
                    outflow_count,
                    top_inflow_ticker,
                    top_inflow_amount,
                    top_outflow_ticker,
                    top_outflow_amount
                FROM crypto_etf_daily_summary
                WHERE asset_type = :asset_type
                AND trade_date >= :start_date
                ORDER BY trade_date DESC
                LIMIT :days
            """)

            results = session.execute(sql, {
                "asset_type": asset_type,
                "start_date": start_date.date(),
                "days": days
            }).fetchall()

            if not results:
                return None

            # 最新一天的数据
            latest = results[0]

            # 计算趋势（最近3天平均流入）
            recent_inflows = [float(r[1]) if r[1] else 0 for r in results[:3]]
            avg_3day_inflow = sum(recent_inflows) / len(recent_inflows) if recent_inflows else 0

            # 计算周累计
            weekly_total = sum(float(r[1]) if r[1] else 0 for r in results)

            # 判断趋势
            if avg_3day_inflow > 100000000:  # 1亿美元
                trend = 'strong_inflow'
            elif avg_3day_inflow > 0:
                trend = 'inflow'
            elif avg_3day_inflow < -100000000:
                trend = 'strong_outflow'
            elif avg_3day_inflow < 0:
                trend = 'outflow'
            else:
                trend = 'neutral'

            result = {
                'asset_type': asset_type,
                'latest_date': latest[0],
                'latest_net_inflow': float(latest[1]) if latest[1] else 0,
                'latest_gross_inflow': float(latest[2]) if latest[2] else 0,
                'latest_gross_outflow': float(latest[3]) if latest[3] else 0,
                'total_aum': float(latest[4]) if latest[4] else 0,
                'total_holdings': float(latest[5]) if latest[5] else 0,
                'etf_count': int(latest[6]) if latest[6] else 0,
                'inflow_count': int(latest[7]) if latest[7] else 0,
                'outflow_count': int(latest[8]) if latest[8] else 0,
                'top_inflow_ticker': latest[9],
                'top_inflow_amount': float(latest[10]) if latest[10] else 0,
                'top_outflow_ticker': latest[11],
                'top_outflow_amount': float(latest[12]) if latest[12] else 0,
                'avg_3day_inflow': avg_3day_inflow,
                'weekly_total_inflow': weekly_total,
                'trend': trend,
                'days_data': len(results)
            }

            return result

        except Exception as e:
            logger.error(f"获取ETF汇总数据失败: {e}")
            import traceback
            traceback.print_exc()
            return None
        finally:
            session.close()
