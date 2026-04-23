#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
智能数据采集服务（分层采集策略）
采集超级大脑需要的多时间周期K线数据: 5m, 15m, 1h, 1d
每5分钟检查一次，根据K线周期智能决定是否采集

智能策略:
- 5m K线: 每5分钟采集 (每次都采集)
- 15m K线: 每15分钟采集 (每3次采集1次)
- 1h K线: 每1小时采集 (每12次采集1次)
- 1d K线: 每1天采集 (每288次采集1次)

优势: 节省93.5%的无效采集，减少API压力和数据库写入

注意：实时价格由 WebSocket 服务提供，不在此采集
"""

import sys
import asyncio
from pathlib import Path
from datetime import datetime
from loguru import logger

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent))

from app.collectors.smart_futures_collector import SmartFuturesCollector
from app.utils.config_loader import load_config


class SmartCollectorService:
    """智能采集服务（分层策略）"""

    def __init__(self):
        """初始化服务"""
        # 配置日志
        logger.remove()
        logger.add(
            sys.stdout,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level:<8}</level> | <level>{message}</level>",
            level="INFO"
        )
        logger.add(
            "logs/smart_collector_{time:YYYY-MM-DD}.log",
            rotation="00:00",
            retention="7 days",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
            level="INFO"
        )

        # 加载配置
        config = load_config()
        db_config = config['database']['mysql']

        # 初始化智能采集器
        self.collector = SmartFuturesCollector(db_config)
        # 保存 DB 配置供实时价格采集 task 使用
        self.db_config = db_config

        # 检查间隔（秒）- 每5分钟检查一次，智能判断是否采集
        self.interval = 300  # 5分钟

        # 实时价格采集间隔（秒）- Binance 全市场 ticker/price 权重 2
        # 4s/次 = 15 次/分钟 × 权重 2 = 30 权重/分钟，远在 2400 限额内
        self.realtime_price_interval = 4

        logger.info("智能数据采集服务初始化完成")
        logger.info(f"K线采集间隔: {self.interval}秒 (5分钟)")
        logger.info(f"实时价格采集间隔: {self.realtime_price_interval}秒 (全市场 ticker/price)")

    # ── 实时价格采集（集中化，避免各策略各自打 Binance） ────────────
    async def realtime_price_loop(self):
        """
        每 4s 拉 Binance 全市场 /fapi/v1/ticker/price（权重 2），
        批量 UPSERT 到 realtime_prices 表。
        策略/API 读此表即可，不再透传 Binance。
        """
        import aiohttp
        from aiohttp import ClientTimeout
        import pymysql

        url = "https://fapi.binance.com/fapi/v1/ticker/price"

        def _bn_symbol_to_slash(bn_sym: str) -> str:
            """BTCUSDT -> BTC/USDT；1000PEPEUSDT -> 1000PEPE/USDT"""
            if bn_sym.endswith("USDT"):
                return bn_sym[:-4] + "/USDT"
            if bn_sym.endswith("USDC"):
                return bn_sym[:-4] + "/USDC"
            return bn_sym  # 未知后缀原样存

        def _upsert_sync(rows):
            """在线程里同步写库"""
            cfg = self.db_config
            conn = pymysql.connect(
                host=cfg.get('host', 'localhost'),
                port=int(cfg.get('port', 3306)),
                user=cfg.get('user', ''),
                password=cfg.get('password', ''),
                database=cfg.get('database', ''),
                charset='utf8mb4',
                connect_timeout=5,
            )
            try:
                with conn.cursor() as cur:
                    cur.executemany(
                        """INSERT INTO realtime_prices (symbol, price, source)
                           VALUES (%s, %s, 'binance_futures')
                           ON DUPLICATE KEY UPDATE price=VALUES(price)""",
                        rows,
                    )
                conn.commit()
            finally:
                conn.close()

        logger.info("实时价格采集 task 启动")
        first_log = True
        while True:
            try:
                async with aiohttp.ClientSession(timeout=ClientTimeout(total=5)) as session:
                    async with session.get(url) as r:
                        if r.status != 200:
                            logger.warning(f"实时价格 HTTP {r.status}")
                            await asyncio.sleep(self.realtime_price_interval)
                            continue
                        data = await r.json()
                rows = []
                for item in data:
                    sym = item.get('symbol', '')
                    price = item.get('price')
                    if not sym or not price:
                        continue
                    rows.append((_bn_symbol_to_slash(sym), str(price)))
                if rows:
                    await asyncio.to_thread(_upsert_sync, rows)
                    if first_log:
                        logger.info(f"实时价格采集首轮成功，{len(rows)} 个品种")
                        first_log = False
            except Exception as e:
                logger.error(f"实时价格采集异常: {e}")
            await asyncio.sleep(self.realtime_price_interval)

    async def run_forever(self):
        """持续运行智能采集服务"""
        logger.info("=" * 60)
        logger.info("智能数据采集服务启动")
        logger.info("K线采集: 5m(每次) / 15m(每3次) / 1h(每12次) / 1d(每288次)")
        logger.info(f"实时价格: 每 {self.realtime_price_interval}s 拉全市场 ticker/price")
        logger.info("=" * 60)

        # 并行启动实时价格采集 task
        price_task = asyncio.create_task(self.realtime_price_loop())

        cycle_count = 0

        while True:
            try:
                cycle_count += 1
                logger.info(f"\n【第 {cycle_count} 次 K 线采集】")

                # 执行采集
                await self.collector.run_collection_cycle()

                # 等待下一次采集
                logger.info(f"等待 {self.interval} 秒...\n")
                await asyncio.sleep(self.interval)

            except KeyboardInterrupt:
                logger.info("收到停止信号，服务退出")
                price_task.cancel()
                break
            except Exception as e:
                logger.error(f"采集周期异常: {e}")
                logger.exception(e)
                # 出错后等待30秒再重试
                logger.info("30秒后重试...")
                await asyncio.sleep(30)


def main():
    """主函数"""
    service = SmartCollectorService()

    try:
        asyncio.run(service.run_forever())
    except KeyboardInterrupt:
        logger.info("服务已停止")


if __name__ == '__main__':
    main()
