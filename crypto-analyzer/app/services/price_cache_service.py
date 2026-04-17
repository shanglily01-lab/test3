"""
轻量级价格缓存服务
专为 Paper Trading 提供快速、无阻塞的价格查询
"""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, Optional
from decimal import Decimal
from loguru import logger
import threading


class PriceCacheService:
    """
    内存价格缓存服务
    - 从数据库定期更新价格（后台线程）
    - 提供快速的内存查询
    - 避免频繁数据库查询导致阻塞
    """

    def __init__(self, db_config: dict, update_interval: int = 5):
        """
        Args:
            db_config: 数据库配置
            update_interval: 更新间隔（秒）
        """
        self.db_config = db_config
        self.update_interval = update_interval

        # 价格缓存: {symbol: {"price": Decimal, "timestamp": datetime}}
        self._cache: Dict[str, Dict] = {}
        self._lock = threading.Lock()

        # 后台更新线程
        self._update_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._running = False

        logger.info(f"✅ 价格缓存服务初始化完成（更新间隔: {update_interval}秒）")

    def start(self):
        """启动后台更新线程"""
        if self._running:
            logger.warning("价格缓存服务已在运行")
            return

        self._running = True
        self._stop_event.clear()

        self._update_thread = threading.Thread(
            target=self._update_loop,
            name="PriceCacheUpdater",
            daemon=True
        )
        self._update_thread.start()
        logger.info("🚀 价格缓存后台更新线程已启动")

    def stop(self):
        """停止后台更新线程"""
        if not self._running:
            return

        self._running = False
        self._stop_event.set()

        if self._update_thread:
            self._update_thread.join(timeout=5)

        logger.info("👋 价格缓存服务已停止")

    def _update_loop(self):
        """后台更新循环"""
        # 移除启动日志，仅在失败时打印

        while not self._stop_event.is_set():
            try:
                self._update_prices_from_db()
            except Exception as e:
                logger.error(f"更新价格缓存失败: {e}")

            # 等待下次更新
            self._stop_event.wait(self.update_interval)

    def _update_prices_from_db(self):
        """从数据库更新价格（带重试机制）"""
        max_retries = 3
        retry_delay = 1  # 秒
        
        for attempt in range(max_retries):
            try:
                from app.database.db_service import DatabaseService

                db_service = DatabaseService(self.db_config)

                # 获取所有最新价格
                prices = db_service.get_all_latest_prices()

                if not prices:
                    logger.warning("数据库中没有价格数据")
                    return

                # 更新缓存
                with self._lock:
                    for price_data in prices:
                        symbol = price_data.get('symbol')
                        price = price_data.get('price')

                        if symbol and price:
                            self._cache[symbol] = {
                                'price': Decimal(str(price)),
                                'timestamp': datetime.now(),
                                'bid': Decimal(str(price_data.get('bid', price))),
                                'ask': Decimal(str(price_data.get('ask', price))),
                            }

                    # 移除成功时的日志，仅在失败时打印
                
                # 成功，退出重试循环
                return

            except Exception as e:
                error_msg = str(e)
                is_connection_error = 'Lost connection' in error_msg or 'OperationalError' in str(type(e).__name__) or '2013' in error_msg
                
                if attempt < max_retries - 1 and is_connection_error:
                    logger.debug(f"从数据库更新价格失败（尝试 {attempt + 1}/{max_retries}）: {e}，{retry_delay}秒后重试...")
                    import time
                    time.sleep(retry_delay)
                    retry_delay *= 2  # 指数退避
                else:
                    # 最后一次尝试失败，记录错误但不抛出异常（避免影响主程序）
                    logger.error(f"从数据库更新价格失败（已重试 {attempt + 1} 次）: {e}")
                    return  # 静默失败，使用缓存数据

    def get_price(self, symbol: str) -> Decimal:
        """
        获取币种价格（内存查询，极快）

        Args:
            symbol: 交易对，如 BTC/USDT

        Returns:
            价格（Decimal），如果没有则返回 0
        """
        with self._lock:
            cache_entry = self._cache.get(symbol)

            if cache_entry:
                return cache_entry['price']

            # 没有缓存数据
            logger.warning(f"⚠️  {symbol} 价格缓存未命中，请确保数据采集器正在运行")
            return Decimal('0')

    def get_price_detail(self, symbol: str) -> Optional[Dict]:
        """
        获取详细价格信息

        Args:
            symbol: 交易对

        Returns:
            价格详情字典或 None
        """
        with self._lock:
            cache_entry = self._cache.get(symbol)

            if cache_entry:
                return {
                    'symbol': symbol,
                    'price': cache_entry['price'],
                    'bid': cache_entry.get('bid', cache_entry['price']),
                    'ask': cache_entry.get('ask', cache_entry['price']),
                    'timestamp': cache_entry['timestamp']
                }

            return None

    def get_all_prices(self) -> Dict[str, Decimal]:
        """
        获取所有缓存的价格

        Returns:
            {symbol: price} 字典
        """
        with self._lock:
            return {
                symbol: data['price']
                for symbol, data in self._cache.items()
            }

    def is_cache_fresh(self, symbol: str, max_age_seconds: int = 60) -> bool:
        """
        检查缓存是否新鲜

        Args:
            symbol: 交易对
            max_age_seconds: 最大缓存年龄（秒）

        Returns:
            True 表示缓存新鲜
        """
        with self._lock:
            cache_entry = self._cache.get(symbol)

            if not cache_entry:
                return False

            age = (datetime.now() - cache_entry['timestamp']).total_seconds()
            return age <= max_age_seconds

    def get_cache_age(self, symbol: str) -> Optional[float]:
        """
        获取缓存年龄（秒）

        Args:
            symbol: 交易对

        Returns:
            缓存年龄（秒）或 None
        """
        with self._lock:
            cache_entry = self._cache.get(symbol)

            if cache_entry:
                return (datetime.now() - cache_entry['timestamp']).total_seconds()

            return None

    def manual_update(self):
        """手动触发更新（同步）"""
        self._update_prices_from_db()
        logger.info("手动更新价格缓存完成")


# 全局单例实例（延迟初始化）
_global_cache_service: Optional[PriceCacheService] = None


def get_global_price_cache() -> Optional[PriceCacheService]:
    """获取全局价格缓存服务实例"""
    return _global_cache_service


def init_global_price_cache(db_config: dict, update_interval: int = 5):
    """初始化全局价格缓存服务"""
    global _global_cache_service

    if _global_cache_service is not None:
        logger.warning("全局价格缓存服务已初始化")
        return _global_cache_service

    _global_cache_service = PriceCacheService(db_config, update_interval)
    _global_cache_service.start()

    logger.info("🌍 全局价格缓存服务已初始化并启动")
    return _global_cache_service


def stop_global_price_cache():
    """停止全局价格缓存服务"""
    global _global_cache_service

    if _global_cache_service:
        _global_cache_service.stop()
        _global_cache_service = None
        logger.info("🌍 全局价格缓存服务已停止")
