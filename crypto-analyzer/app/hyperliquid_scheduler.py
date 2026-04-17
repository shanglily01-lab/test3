"""
Hyperliquid 聪明钱包监控调度器
独立运行，避免阻塞主数据采集调度器

监控策略：
- 高优先级钱包: 每5分钟监控 (PnL>10K, ROI>50%, 7天内活跃, 限200个)
- 中优先级钱包: 每1小时监控 (PnL>5K, ROI>30%, 30天内活跃, 限500个)
- 全量扫描: 每6小时监控所有活跃钱包 (8000+个)
"""

import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import asyncio
import schedule
import time
import yaml
from datetime import datetime
from loguru import logger
from typing import Dict

from app.collectors.hyperliquid_collector import HyperliquidCollector
from app.database.db_service import DatabaseService


class HyperliquidScheduler:
    """Hyperliquid 聪明钱包监控调度器"""

    def __init__(self, config_path: str = 'config.yaml'):
        """
        初始化调度器

        Args:
            config_path: 配置文件路径
        """
        # 加载配置（支持环境变量）
        from app.utils.config_loader import load_config
        self.config = load_config(Path(config_path))

        # 初始化数据库服务
        logger.info("初始化数据库服务...")
        db_config = self.config.get('database', {})
        self.db_service = DatabaseService(db_config)

        # 初始化 Hyperliquid 采集器
        logger.info("初始化 Hyperliquid 采集器...")
        hyperliquid_config = self.config.get('hyperliquid', {})
        if hyperliquid_config.get('enabled', False):
            self.hyperliquid_collector = HyperliquidCollector(hyperliquid_config)
            logger.info("  ✓ Hyperliquid 采集器已启用")
        else:
            self.hyperliquid_collector = None
            logger.warning("  ⊗ Hyperliquid 采集器未启用")

        # 任务统计
        self.task_stats = {
            'hyperliquid_high': {'count': 0, 'last_run': None, 'last_error': None},
            'hyperliquid_medium': {'count': 0, 'last_run': None, 'last_error': None},
            'hyperliquid_all': {'count': 0, 'last_run': None, 'last_error': None},
        }

        logger.info("Hyperliquid 调度器初始化完成")

    async def monitor_hyperliquid_wallets(self, priority: str = 'all'):
        """
        监控 Hyperliquid 聪明钱包的资金动态

        Args:
            priority: 监控优先级 (high, medium, low, all, config)
        """
        if not self.hyperliquid_collector:
            logger.warning("Hyperliquid 采集器未启用，跳过监控")
            return

        task_name = f'hyperliquid_{priority}'
        try:
            logger.info(f"[{datetime.now().strftime('%H:%M:%S')}] 开始监控 Hyperliquid 聪明钱包 (优先级: {priority})...")

            from app.database.hyperliquid_db import HyperliquidDB

            with HyperliquidDB() as db:
                # 使用分级监控逻辑
                results = await self.hyperliquid_collector.monitor_all_addresses(
                    hours=168,  # 回溯7天（7*24=168小时）
                    priority=priority,
                    hyperliquid_db=db
                )

                if not results:
                    logger.info("  ⊗ 暂无监控钱包或未发现交易")
                    return

                monitored_wallets = list(results.keys())
                logger.info(f"  本次监控: {len(monitored_wallets)} 个地址")

                total_trades = 0
                total_positions = 0
                wallet_updates = []

                for address, result in results.items():
                    try:
                        # 保存交易记录
                        recent_trades = result.get('recent_trades', [])
                        for trade in recent_trades:
                            trade_data = {
                                'coin': trade['coin'],
                                'side': trade['action'],  # LONG/SHORT
                                'action': 'TRADE',
                                'price': trade['price'],
                                'size': trade['size'],
                                'notional_usd': trade['notional_usd'],
                                'closed_pnl': trade['closed_pnl'],
                                'trade_time': trade['timestamp'],
                                'raw_data': trade.get('raw_data', {})
                            }
                            db.save_wallet_trade(address, trade_data)
                            total_trades += 1

                        # 保存持仓快照
                        positions = result.get('positions', [])
                        snapshot_time = datetime.now()
                        for pos in positions:
                            position_data = {
                                'coin': pos['coin'],
                                'side': pos['side'],
                                'size': pos['size'],
                                'entry_price': pos['entry_price'],
                                'mark_price': pos.get('mark_price', pos['entry_price']),
                                'notional_usd': pos['notional_usd'],
                                'unrealized_pnl': pos['unrealized_pnl'],
                                'leverage': pos.get('leverage', 1),
                                'raw_data': {}
                            }
                            db.save_wallet_position(address, position_data, snapshot_time)
                            total_positions += 1

                        # 更新检查时间（需要先获取trader_id）
                        trader_id = db.get_or_create_trader(address)
                        last_trade_time = recent_trades[0]['timestamp'] if recent_trades else None
                        db.update_wallet_check_time(trader_id, last_trade_time)

                        # 记录有活动的钱包
                        if recent_trades or positions:
                            stats = result.get('statistics', {})
                            wallet_updates.append({
                                'address': address[:10] + '...',
                                'trades': len(recent_trades),
                                'positions': len(positions),
                                'net_flow': stats.get('net_flow_usd', 0),
                                'total_pnl': stats.get('total_pnl', 0)
                            })

                        # 延迟避免API限流
                        await asyncio.sleep(2)

                    except Exception as e:
                        logger.error(f"  监控钱包 {address[:10]}... 失败: {e}")

                # 汇总报告
                logger.info(f"  ✓ 监控完成: 检查 {len(monitored_wallets)} 个钱包, "
                          f"新交易 {total_trades} 笔, 持仓 {total_positions} 个")

                # 显示有活动的钱包
                if wallet_updates:
                    logger.info(f"  活跃钱包 ({len(wallet_updates)} 个):")
                    for w in wallet_updates[:5]:
                        pnl_str = f"PnL: ${w['total_pnl']:,.0f}" if w['total_pnl'] != 0 else ""
                        flow_str = f"净流: ${w['net_flow']:,.0f}" if w['net_flow'] != 0 else ""
                        logger.info(f"    • {w['address']}: {w['trades']}笔交易, {w['positions']}个持仓 {pnl_str} {flow_str}")

            # 更新统计
            self.task_stats[task_name]['count'] += 1
            self.task_stats[task_name]['last_run'] = datetime.now()

        except Exception as e:
            logger.error(f"Hyperliquid 钱包监控任务失败: {e}")
            self.task_stats[task_name]['last_error'] = str(e)
            import traceback
            logger.error(traceback.format_exc())

    def cleanup_old_data(self, retain_days: int = 30):
        """
        清理超过 retain_days 天的历史数据，释放磁盘空间。

        清理对象：
          - hyperliquid_wallet_trades   (按 trade_time)
          - hyperliquid_wallet_positions (按 snapshot_time)
          - hyperliquid_performance_snapshots (按 snapshot_date)
        """
        from app.database.hyperliquid_db import HyperliquidDB
        import pymysql, os

        logger.info(f"开始清理 Hyperliquid 历史数据（保留最近 {retain_days} 天）...")

        tables = [
            ("hyperliquid_wallet_trades",        "trade_time"),
            ("hyperliquid_wallet_positions",      "snapshot_time"),
            ("hyperliquid_performance_snapshots", "snapshot_date"),
        ]

        try:
            conn = pymysql.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                port=int(os.getenv('DB_PORT', 3306)),
                user=os.getenv('DB_USER', 'root'),
                password=os.getenv('DB_PASSWORD', ''),
                database=os.getenv('DB_NAME', 'binance-data'),
                charset='utf8mb4',
                autocommit=False,
            )
            cur = conn.cursor()
            total_deleted = 0

            for table, col in tables:
                try:
                    cur.execute(
                        f"DELETE FROM `{table}` WHERE `{col}` < NOW() - INTERVAL %s DAY",
                        (retain_days,)
                    )
                    deleted = cur.rowcount
                    conn.commit()
                    total_deleted += deleted
                    logger.info(f"  {table}: 删除 {deleted} 条旧记录")
                except Exception as e:
                    conn.rollback()
                    logger.warning(f"  {table} 清理失败: {e}")

            cur.close()
            conn.close()
            logger.info(f"清理完成，共删除 {total_deleted} 条记录")

        except Exception as e:
            logger.error(f"Hyperliquid 数据清理任务异常: {e}")

    def schedule_tasks(self):
        """设置所有定时任务"""
        logger.info("设置 Hyperliquid 监控任务...")

        if not self.hyperliquid_collector:
            logger.warning("Hyperliquid 采集器未启用，无法设置监控任务")
            return

        # 高优先级钱包: 每5分钟监控 (PnL>10K, ROI>50%, 7天内活跃, 限200个)
        schedule.every(5).minutes.do(
            lambda: asyncio.run(self.monitor_hyperliquid_wallets(priority='high'))
        )
        logger.info("  ✓ Hyperliquid 高优先级钱包 (200个) - 每 5 分钟")

        # 中优先级钱包: 每1小时监控 (PnL>5K, ROI>30%, 30天内活跃, 限500个)
        schedule.every(1).hours.do(
            lambda: asyncio.run(self.monitor_hyperliquid_wallets(priority='medium'))
        )
        logger.info("  ✓ Hyperliquid 中优先级钱包 (500个) - 每 1 小时")

        # 全量扫描: 每6小时监控所有活跃钱包
        schedule.every(6).hours.do(
            lambda: asyncio.run(self.monitor_hyperliquid_wallets(priority='all'))
        )
        logger.info("  ✓ Hyperliquid 全量扫描 (8000+个) - 每 6 小时")

        # 历史数据清理: 每天 03:00 执行，只保留 30 天
        schedule.every().day.at("03:00").do(self.cleanup_old_data)
        logger.info("  ✓ 历史数据清理 (保留30天) - 每天 03:00")

    def print_status(self):
        """打印任务状态"""
        logger.info("\n" + "=" * 80)
        logger.info("Hyperliquid 监控任务状态")
        logger.info("=" * 80)
        for task_name, stats in self.task_stats.items():
            status = "✅" if stats['last_error'] is None else "❌"
            last_run = stats['last_run'].strftime('%Y-%m-%d %H:%M:%S') if stats['last_run'] else "从未运行"
            logger.info(f"{status} {task_name}: 执行 {stats['count']} 次, 最后运行: {last_run}")
            if stats['last_error']:
                logger.error(f"   错误: {stats['last_error']}")
        logger.info("=" * 80 + "\n")

    def start(self):
        """启动调度器"""
        logger.info("\n" + "=" * 80)
        logger.info("Hyperliquid 聪明钱包监控调度器启动")
        logger.info("=" * 80)
        logger.info("=" * 80 + "\n")

        # 设置定时任务
        self.schedule_tasks()

        # 首次执行一次高优先级监控
        logger.info("执行首次监控（高优先级）...")
        asyncio.run(self.monitor_hyperliquid_wallets(priority='high'))

        # 定期打印状态 (每小时)
        schedule.every(1).hours.do(self.print_status)

        logger.info("\nHyperliquid 调度器已启动，按 Ctrl+C 停止\n")

        # 保持运行
        try:
            while True:
                schedule.run_pending()
                time.sleep(1)

        except KeyboardInterrupt:
            logger.info("\n\n收到停止信号，正在关闭...")
            self.stop()

    def stop(self):
        """停止调度器"""
        logger.info("关闭数据库连接...")
        self.db_service.close()
        logger.info("Hyperliquid 调度器已停止")


def main():
    """主函数"""
    # 配置日志
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)

    logger.remove()  # 移除默认处理器

    # 添加控制台输出
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level="INFO",
        colorize=True
    )

    # 添加文件输出
    logger.add(
        log_dir / "hyperliquid_scheduler_{time:YYYY-MM-DD}.log",
        rotation="00:00",
        retention="30 days",
        level="INFO",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
        encoding="utf-8"
    )

    # 创建并启动调度器
    scheduler = HyperliquidScheduler(config_path='config.yaml')
    scheduler.start()


if __name__ == '__main__':
    main()

