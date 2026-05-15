#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fast_collector_service - 统一数据采集进程
====================================================
合并自 (2026-05-15):
  - 原 fast_collector_service: K 线分层采集 (SmartFuturesCollector)
  - 原 whale_data_collector: funding rate / 24h stats / OI / LSR

主循环每 60s 一次轻量 tick, 按时间戳分别管理:
  K 线 (asyncio):
    - 5m / 15m / 1h / 4h / 1d, 由 SmartFuturesCollector 内部 should_collect_interval 智能跳过
    - 每个 tick 都调一次 run_collection_cycle, 内部分层判断
  whale (asyncio.to_thread 桥接, 同步 requests + pymysql):
    - funding + 24h stats: 每 10 分钟
    - OI 历史 + LSR 多空比: 每 120 分钟 (依赖 funding 返回的 top N 列表)
    - 旧数据 cleanup: 每 60 分钟

实时价格 由 FastAPI 主进程内的 data_sync_center (每 10s WebSocket/REST) 维护,
不在本进程范围.

watchdog (app/main.py:751) 每 5 分钟检查本进程 PID, 崩溃自动重启.
"""

import sys
import asyncio
import time
from pathlib import Path
from datetime import datetime
from loguru import logger

# 添加项目路径
sys.path.insert(0, str(Path(__file__).parent))

from app.collectors.smart_futures_collector import SmartFuturesCollector
from app.collectors.whale_data_lib import WhaleDataCollector
from app.utils.config_loader import load_config


# ── 周期常量 ─────────────────────────────────────────────────────────
TICK_SECS         = 60            # 主循环 tick (轻量, 内部按时间戳跳过)
FUNDING_INTERVAL  = 10 * 60       # whale funding + 24h: 10 min
OI_LS_INTERVAL    = 120 * 60      # whale OI + LSR:      120 min
CLEANUP_INTERVAL  = 60 * 60       # whale cleanup:       60 min


class FastCollectorService:
    """统一数据采集服务: K 线 + whale 数据."""

    def __init__(self):
        logger.remove()
        logger.add(
            sys.stdout,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level:<8}</level> | <level>{message}</level>",
            level="INFO",
        )
        logger.add(
            "logs/fast_collector_{time:YYYY-MM-DD}.log",
            rotation="00:00",
            retention="7 days",
            format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
            level="INFO",
        )

        config = load_config()
        db_config = config["database"]["mysql"]

        # K 线: SmartFuturesCollector (asyncio + aiohttp)
        self.kline = SmartFuturesCollector(db_config)
        # whale: WhaleDataCollector (sync requests + pymysql, asyncio.to_thread 桥接)
        self.whale = WhaleDataCollector(db_config)

        # whale 周期时间戳
        self._last_funding  = 0.0
        self._last_oi_ls    = 0.0
        self._last_cleanup  = 0.0
        # whale funding 返回的 top N 列表, 给 OI/LSR 用
        self._whale_top_syms: list = []

        logger.info("FastCollectorService 初始化完成")
        logger.info(f"主循环 tick: {TICK_SECS}s")
        logger.info(f"K线采集: 每 tick 调 run_collection_cycle (内部 5m/15m/1h/4h/1d 智能分层)")
        logger.info(f"whale funding+24h: 每 {FUNDING_INTERVAL // 60} 分钟")
        logger.info(f"whale OI+LSR:      每 {OI_LS_INTERVAL // 60} 分钟")
        logger.info(f"whale cleanup:     每 {CLEANUP_INTERVAL // 60} 分钟")
        logger.info("实时价格由 FastAPI 主进程 data_sync_center 维护, 不在本进程")

    # ── K 线 ────────────────────────────────────────────────────────
    async def _tick_kline(self):
        try:
            await self.kline.run_collection_cycle()
        except Exception as e:
            logger.error(f"K 线采集异常: {e}")
            logger.exception(e)

    # ── whale ───────────────────────────────────────────────────────
    async def _tick_whale_funding(self):
        """funding + 24h_stats, 同步代码走 to_thread 桥接."""
        try:
            # to_thread 把 sync 函数扔后台线程, 不阻塞 asyncio 主循环
            top_syms = await asyncio.to_thread(self.whale.run_funding_cycle)
            if top_syms:
                self._whale_top_syms = top_syms
            else:
                # 被封禁或无数据时不推进时钟, 下一 tick 再试
                logger.warning("whale funding 周期返回空, 不推进时钟 (下次 tick 重试)")
                return False
            return True
        except Exception as e:
            logger.error(f"whale funding 异常: {e}")
            logger.exception(e)
            return False

    async def _tick_whale_oi_lsr(self):
        if not self._whale_top_syms:
            logger.warning("whale OI/LSR 跳过: top_syms 列表为空")
            return False
        try:
            await asyncio.to_thread(self.whale.run_oi_lsr_cycle, self._whale_top_syms)
            return True
        except Exception as e:
            logger.error(f"whale OI/LSR 异常: {e}")
            logger.exception(e)
            return False

    async def _tick_whale_cleanup(self):
        try:
            await asyncio.to_thread(self.whale.run_cleanup)
            return True
        except Exception as e:
            logger.error(f"whale cleanup 异常: {e}")
            return False

    # ── 主循环 ──────────────────────────────────────────────────────
    async def run_forever(self):
        logger.info("=" * 60)
        logger.info("FastCollectorService 启动 (K 线 + whale 合并)")
        logger.info("=" * 60)

        tick_count = 0
        while True:
            try:
                tick_count += 1
                now = time.time()

                # K 线: 每 tick 调一次, 由 should_collect_interval 智能跳过
                await self._tick_kline()

                # whale funding + 24h_stats: 每 FUNDING_INTERVAL 秒
                if now - self._last_funding >= FUNDING_INTERVAL:
                    ok = await self._tick_whale_funding()
                    if ok:
                        self._last_funding = now

                # whale OI + LSR: 每 OI_LS_INTERVAL 秒 (必须有 top_syms)
                if now - self._last_oi_ls >= OI_LS_INTERVAL and self._whale_top_syms:
                    if await self._tick_whale_oi_lsr():
                        self._last_oi_ls = now

                # whale cleanup: 每 CLEANUP_INTERVAL 秒
                if now - self._last_cleanup >= CLEANUP_INTERVAL:
                    if await self._tick_whale_cleanup():
                        self._last_cleanup = now

                # 封禁窗口内拉长休眠
                ban_rem = self.whale.ban_remaining_s()
                if ban_rem > 0:
                    sleep_s = min(max(60.0, ban_rem + 5.0), 600.0)
                    logger.warning(f"Binance 封禁窗口内, 本轮休眠 {sleep_s:.0f}s 后再 tick")
                    await asyncio.sleep(sleep_s)
                else:
                    await asyncio.sleep(TICK_SECS)

            except KeyboardInterrupt:
                logger.info("收到停止信号, 服务退出")
                break
            except Exception as e:
                logger.error(f"主循环异常: {e}")
                logger.exception(e)
                await asyncio.sleep(30)


def main():
    service = FastCollectorService()
    try:
        asyncio.run(service.run_forever())
    except KeyboardInterrupt:
        logger.info("服务已停止")


if __name__ == "__main__":
    main()
