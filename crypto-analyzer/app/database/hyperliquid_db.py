#!/usr/bin/env python3
"""
Hyperliquid 聪明钱数据库管理
"""

try:
    import pymysql as mysql_module
    USE_PYMYSQL = True
except ImportError:
    try:
        import mysql.connector as mysql_module
        USE_PYMYSQL = False
    except ImportError:
        raise ImportError("请安装 MySQL 客户端: pip install pymysql 或 pip install mysql-connector-python")

from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Tuple
import yaml


class HyperliquidDB:
    """Hyperliquid 数据库管理类"""

    def __init__(self, config_path='config.yaml'):
        """
        初始化数据库连接

        Args:
            config_path: 配置文件路径
        """
        # 加载配置（支持环境变量）
        from app.utils.config_loader import load_config
        config = load_config()

        db_config = config.get('database', {}).get('mysql', {})

        if USE_PYMYSQL:
            # 使用 PyMySQL
            self.conn = mysql_module.connect(
                host=db_config.get('host', 'localhost'),
                port=db_config.get('port', 3306),
                user=db_config.get('user', 'root'),
                password=db_config.get('password', ''),
                database=db_config.get('database', 'binance-data'),
                charset='utf8mb4',
                cursorclass=mysql_module.cursors.DictCursor
            )
            self.cursor = self.conn.cursor()
        else:
            # 使用 mysql.connector
            self.conn = mysql_module.connect(
                host=db_config.get('host', 'localhost'),
                port=db_config.get('port', 3306),
                user=db_config.get('user', 'root'),
                password=db_config.get('password', ''),
                database=db_config.get('database', 'binance-data'),
                charset='utf8mb4'
            )
            self.cursor = self.conn.cursor(dictionary=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """关闭数据库连接"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def init_tables(self):
        """初始化数据库表"""
        schema_file = 'app/database/hyperliquid_schema.sql'

        try:
            with open(schema_file, 'r', encoding='utf-8') as f:
                sql_content = f.read()

            # 分割并执行SQL语句
            statements = [s.strip() for s in sql_content.split(';') if s.strip()]

            for statement in statements:
                # 跳过注释
                if statement.startswith('--') or not statement:
                    continue

                try:
                    self.cursor.execute(statement)
                    self.conn.commit()
                except Exception as e:
                    print(f"执行SQL时出错: {e}")
                    print(f"SQL: {statement[:100]}...")

            print("✅ 数据库表初始化成功")

        except FileNotFoundError:
            print(f"❌ 找不到schema文件: {schema_file}")
        except Exception as e:
            print(f"❌ 初始化失败: {e}")

    def get_or_create_trader(self, address: str, display_name: str = None) -> int:
        """
        获取或创建交易者记录

        Args:
            address: 钱包地址
            display_name: 显示名称

        Returns:
            trader_id
        """
        # 检查是否存在
        self.cursor.execute(
            "SELECT id FROM hyperliquid_traders WHERE address = %s",
            (address,)
        )
        result = self.cursor.fetchone()

        if result:
            trader_id = result['id']

            # 更新显示名称（如果提供）
            if display_name:
                self.cursor.execute(
                    """UPDATE hyperliquid_traders
                       SET display_name = %s, last_updated = %s
                       WHERE id = %s""",
                    (display_name, datetime.now(), trader_id)
                )
                self.conn.commit()

            return trader_id
        else:
            # 创建新记录
            now = datetime.now()
            self.cursor.execute(
                """INSERT INTO hyperliquid_traders
                   (address, display_name, first_seen, last_updated)
                   VALUES (%s, %s, %s, %s)""",
                (address, display_name, now, now)
            )
            self.conn.commit()
            return self.cursor.lastrowid

    def save_weekly_performance(self, address: str, display_name: str,
                                 week_start: date, week_end: date,
                                 pnl: float, roi: float, volume: float,
                                 account_value: float, pnl_rank: int = None):
        """
        保存周度表现数据

        Args:
            address: 钱包地址
            display_name: 显示名称
            week_start: 周开始日期
            week_end: 周结束日期
            pnl: 盈亏
            roi: ROI (百分比形式，如 15.5 表示 15.5%)
            volume: 交易量
            account_value: 账户价值
            pnl_rank: PnL排名
        """
        trader_id = self.get_or_create_trader(address, display_name)

        # 检查是否已存在该周的记录
        self.cursor.execute(
            """SELECT id FROM hyperliquid_weekly_performance
               WHERE trader_id = %s AND week_start = %s""",
            (trader_id, week_start)
        )
        existing = self.cursor.fetchone()

        now = datetime.now()

        if existing:
            # 更新现有记录
            self.cursor.execute(
                """UPDATE hyperliquid_weekly_performance
                   SET pnl = %s, roi = %s, volume = %s, account_value = %s,
                       pnl_rank = %s, recorded_at = %s
                   WHERE id = %s""",
                (pnl, roi, volume, account_value, pnl_rank, now, existing['id'])
            )
        else:
            # 插入新记录
            self.cursor.execute(
                """INSERT INTO hyperliquid_weekly_performance
                   (trader_id, address, week_start, week_end, pnl, roi, volume,
                    account_value, pnl_rank, recorded_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (trader_id, address, week_start, week_end, pnl, roi, volume,
                 account_value, pnl_rank, now)
            )

        self.conn.commit()

    def save_performance_snapshot(self, address: str, display_name: str,
                                   snapshot_date: date, performance_data: Dict):
        """
        保存表现快照

        Args:
            address: 钱包地址
            display_name: 显示名称
            snapshot_date: 快照日期
            performance_data: 表现数据字典，包含各周期的 pnl, roi, volume
        """
        trader_id = self.get_or_create_trader(address, display_name)

        # 检查是否已存在该日的快照
        self.cursor.execute(
            """SELECT id FROM hyperliquid_performance_snapshots
               WHERE trader_id = %s AND snapshot_date = %s""",
            (trader_id, snapshot_date)
        )
        existing = self.cursor.fetchone()

        now = datetime.now()

        if existing:
            # 更新现有快照
            self.cursor.execute(
                """UPDATE hyperliquid_performance_snapshots
                   SET day_pnl = %s, day_roi = %s, day_volume = %s,
                       week_pnl = %s, week_roi = %s, week_volume = %s,
                       month_pnl = %s, month_roi = %s, month_volume = %s,
                       alltime_pnl = %s, alltime_roi = %s, alltime_volume = %s,
                       account_value = %s, recorded_at = %s
                   WHERE id = %s""",
                (
                    performance_data.get('day_pnl', 0),
                    performance_data.get('day_roi', 0),
                    performance_data.get('day_volume', 0),
                    performance_data.get('week_pnl', 0),
                    performance_data.get('week_roi', 0),
                    performance_data.get('week_volume', 0),
                    performance_data.get('month_pnl', 0),
                    performance_data.get('month_roi', 0),
                    performance_data.get('month_volume', 0),
                    performance_data.get('alltime_pnl', 0),
                    performance_data.get('alltime_roi', 0),
                    performance_data.get('alltime_volume', 0),
                    performance_data.get('account_value', 0),
                    now,
                    existing['id']
                )
            )
        else:
            # 插入新快照
            self.cursor.execute(
                """INSERT INTO hyperliquid_performance_snapshots
                   (trader_id, address, snapshot_date,
                    day_pnl, day_roi, day_volume,
                    week_pnl, week_roi, week_volume,
                    month_pnl, month_roi, month_volume,
                    alltime_pnl, alltime_roi, alltime_volume,
                    account_value, recorded_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    trader_id, address, snapshot_date,
                    performance_data.get('day_pnl', 0),
                    performance_data.get('day_roi', 0),
                    performance_data.get('day_volume', 0),
                    performance_data.get('week_pnl', 0),
                    performance_data.get('week_roi', 0),
                    performance_data.get('week_volume', 0),
                    performance_data.get('month_pnl', 0),
                    performance_data.get('month_roi', 0),
                    performance_data.get('month_volume', 0),
                    performance_data.get('alltime_pnl', 0),
                    performance_data.get('alltime_roi', 0),
                    performance_data.get('alltime_volume', 0),
                    performance_data.get('account_value', 0),
                    now
                )
            )

        self.conn.commit()

    def get_weekly_leaderboard(self, week_start: date = None, limit: int = 100) -> List[Dict]:
        """
        获取周度排行榜

        Args:
            week_start: 周开始日期，None表示最近一周
            limit: 返回数量

        Returns:
            排行榜列表
        """
        if week_start is None:
            # 获取最近一周
            self.cursor.execute(
                "SELECT MAX(week_start) as max_week FROM hyperliquid_weekly_performance"
            )
            result = self.cursor.fetchone()
            if result and result['max_week']:
                week_start = result['max_week']
            else:
                return []

        self.cursor.execute(
            """SELECT t.address, t.display_name,
                      wp.week_start, wp.week_end,
                      wp.pnl, wp.roi, wp.volume, wp.account_value,
                      wp.pnl_rank, wp.recorded_at
               FROM hyperliquid_weekly_performance wp
               JOIN hyperliquid_traders t ON wp.trader_id = t.id
               WHERE wp.week_start = %s
               ORDER BY wp.pnl DESC
               LIMIT %s""",
            (week_start, limit)
        )

        return self.cursor.fetchall()

    def get_trader_history(self, address: str, days: int = 30) -> List[Dict]:
        """
        获取交易者历史表现

        Args:
            address: 钱包地址
            days: 查询天数

        Returns:
            历史记录列表
        """
        cutoff_date = date.today() - timedelta(days=days)

        self.cursor.execute(
            """SELECT ps.snapshot_date,
                      ps.day_pnl, ps.day_roi,
                      ps.week_pnl, ps.week_roi,
                      ps.month_pnl, ps.month_roi,
                      ps.alltime_pnl, ps.alltime_roi,
                      ps.account_value
               FROM hyperliquid_performance_snapshots ps
               JOIN hyperliquid_traders t ON ps.trader_id = t.id
               WHERE t.address = %s AND ps.snapshot_date >= %s
               ORDER BY ps.snapshot_date DESC""",
            (address, cutoff_date)
        )

        return self.cursor.fetchall()

    def get_top_traders_by_week(self, limit: int = 20) -> List[Dict]:
        """
        获取周度表现最佳的交易者（最近一周）

        Args:
            limit: 返回数量

        Returns:
            交易者列表
        """
        return self.get_weekly_leaderboard(limit=limit)

    # ==================== 钱包监控功能 ====================

    def add_monitored_wallet(self, address: str, label: str = None,
                             monitor_type: str = 'manual',
                             pnl: float = 0, roi: float = 0,
                             account_value: float = 0) -> int:
        """
        添加监控钱包

        Args:
            address: 钱包地址
            label: 标签
            monitor_type: 监控类型 (auto/manual)
            pnl: 发现时PnL
            roi: 发现时ROI
            account_value: 发现时账户价值

        Returns:
            monitor_id
        """
        trader_id = self.get_or_create_trader(address, label)

        # 检查是否已存在
        self.cursor.execute(
            "SELECT id FROM hyperliquid_monitored_wallets WHERE trader_id = %s",
            (trader_id,)
        )
        existing = self.cursor.fetchone()

        now = datetime.now()

        if existing:
            # 如果已存在，更新为监控状态
            self.cursor.execute(
                """UPDATE hyperliquid_monitored_wallets
                   SET is_monitoring = TRUE, updated_at = %s, label = %s
                   WHERE id = %s""",
                (now, label, existing['id'])
            )
            self.conn.commit()
            return existing['id']
        else:
            # 创建新监控记录
            self.cursor.execute(
                """INSERT INTO hyperliquid_monitored_wallets
                   (trader_id, address, label, monitor_type, is_monitoring,
                    discovered_pnl, discovered_roi, discovered_account_value,
                    discovered_at, created_at, updated_at)
                   VALUES (%s, %s, %s, %s, TRUE, %s, %s, %s, %s, %s, %s)""",
                (trader_id, address, label, monitor_type, pnl, roi,
                 account_value, now, now, now)
            )
            self.conn.commit()
            return self.cursor.lastrowid

    def get_monitored_wallets(self, active_only: bool = True) -> List[Dict]:
        """
        获取监控钱包列表

        Args:
            active_only: 只返回活跃监控

        Returns:
            钱包列表
        """
        query = """
            SELECT mw.*, t.display_name
            FROM hyperliquid_monitored_wallets mw
            JOIN hyperliquid_traders t ON mw.trader_id = t.id
        """

        if active_only:
            query += " WHERE mw.is_monitoring = TRUE"

        query += " ORDER BY mw.last_check_at ASC"

        self.cursor.execute(query)
        return self.cursor.fetchall()

    def get_monitored_wallets_by_priority(
        self,
        min_pnl: float = 0,
        min_roi: float = 0,
        days_active: int = 30,
        limit: int = 200
    ) -> List[Dict]:
        """
        按优先级获取监控钱包

        排序规则:
        1. 最近交易时间 (越近越好)
        2. PnL (越高越好)
        3. ROI (越高越好)
        4. 账户价值 (越大越好)

        Args:
            min_pnl: 最低PnL阈值 (USD)
            min_roi: 最低ROI阈值 (百分比)
            days_active: 最近N天内有交易
            limit: 返回数量限制

        Returns:
            钱包列表
        """
        # 计算截止日期
        from datetime import timedelta
        cutoff_date = datetime.now() - timedelta(days=days_active)

        query = """
            SELECT mw.*, t.display_name
            FROM hyperliquid_monitored_wallets mw
            JOIN hyperliquid_traders t ON mw.trader_id = t.id
            WHERE mw.is_monitoring = TRUE
              AND mw.discovered_pnl >= %s
              AND mw.discovered_roi >= %s
              AND (mw.last_trade_at >= %s OR mw.last_trade_at IS NULL)
            ORDER BY
              mw.last_trade_at DESC,
              mw.discovered_pnl DESC,
              mw.discovered_roi DESC,
              mw.discovered_account_value DESC
            LIMIT %s
        """

        self.cursor.execute(query, (min_pnl, min_roi, cutoff_date, limit))
        return self.cursor.fetchall()

    def update_wallet_check_time(self, trader_id: int, last_trade_time: datetime = None):
        """
        更新钱包检查时间

        Args:
            trader_id: 交易者ID
            last_trade_time: 最后交易时间
        """
        now = datetime.now()

        if last_trade_time:
            self.cursor.execute(
                """UPDATE hyperliquid_monitored_wallets
                   SET last_check_at = %s, last_trade_at = %s,
                       check_count = check_count + 1, updated_at = %s
                   WHERE trader_id = %s""",
                (now, last_trade_time, now, trader_id)
            )
        else:
            self.cursor.execute(
                """UPDATE hyperliquid_monitored_wallets
                   SET last_check_at = %s, check_count = check_count + 1,
                       updated_at = %s
                   WHERE trader_id = %s""",
                (now, now, trader_id)
            )

        self.conn.commit()

    def save_wallet_trade(self, address: str, trade_data: Dict):
        """
        保存钱包交易记录（自动去重）

        Args:
            address: 钱包地址
            trade_data: 交易数据
        """
        trader_id = self.get_or_create_trader(address)

        import json

        # 检查是否已存在相同的交易（基于唯一键：address, coin, side, trade_time, notional_usd）
        trade_time = trade_data.get('trade_time')
        notional_usd = trade_data.get('notional_usd', 0)
        
        if trade_time:
            # 将trade_time转换为datetime（如果还不是）
            if isinstance(trade_time, str):
                try:
                    trade_time = datetime.fromisoformat(trade_time.replace('Z', '+00:00'))
                except:
                    try:
                        trade_time = datetime.strptime(trade_time, '%Y-%m-%d %H:%M:%S')
                    except:
                        trade_time = None
            
            if trade_time:
                # 检查是否存在相同的交易（基于唯一键定义：address, coin, side, trade_time, notional_usd）
                # 注意：notional_usd 在唯一键中使用 ROUND(notional_usd, 2)，所以我们也使用相同的精度
                self.cursor.execute(
                    """SELECT id FROM hyperliquid_wallet_trades
                       WHERE address = %s AND coin = %s AND side = %s
                       AND trade_time = %s
                       AND ROUND(notional_usd, 2) = ROUND(%s, 2)
                       LIMIT 1""",
                    (address, trade_data.get('coin', ''), trade_data.get('side', ''), trade_time, notional_usd)
                )
                existing = self.cursor.fetchone()
                
                if existing:
                    # 交易已存在，跳过
                    return

        # 插入新交易记录
        try:
            self.cursor.execute(
                """INSERT INTO hyperliquid_wallet_trades
                   (trader_id, address, coin, side, action, price, size,
                    notional_usd, closed_pnl, trade_time, detected_at, raw_data)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    trader_id,
                    address,
                    trade_data.get('coin', ''),
                    trade_data.get('side', ''),
                    trade_data.get('action', 'UNKNOWN'),
                    trade_data.get('price', 0),
                    trade_data.get('size', 0),
                    trade_data.get('notional_usd', 0),
                    trade_data.get('closed_pnl', 0),
                    trade_time if trade_time else trade_data.get('trade_time'),
                    datetime.now(),
                    json.dumps(trade_data.get('raw_data', {}))
                )
            )
            self.conn.commit()
        except Exception as e:
            # 如果是重复键错误，忽略（可能是在检查后到插入前有并发插入）
            if 'Duplicate entry' in str(e) or '1062' in str(e):
                self.conn.rollback()
                return
            else:
                raise

    def save_wallet_position(self, address: str, position_data: Dict, snapshot_time: datetime):
        """
        保存钱包持仓快照

        Args:
            address: 钱包地址
            position_data: 持仓数据
            snapshot_time: 快照时间
        """
        trader_id = self.get_or_create_trader(address)

        import json

        # 检查是否已存在相同时间的快照
        self.cursor.execute(
            """SELECT id FROM hyperliquid_wallet_positions
               WHERE trader_id = %s AND coin = %s AND snapshot_time = %s""",
            (trader_id, position_data.get('coin', ''), snapshot_time)
        )
        existing = self.cursor.fetchone()

        if existing:
            # 更新现有记录
            self.cursor.execute(
                """UPDATE hyperliquid_wallet_positions
                   SET side = %s, size = %s, entry_price = %s, mark_price = %s,
                       notional_usd = %s, unrealized_pnl = %s, leverage = %s, raw_data = %s
                   WHERE id = %s""",
                (
                    position_data.get('side', ''),
                    position_data.get('size', 0),
                    position_data.get('entry_price', 0),
                    position_data.get('mark_price', 0),
                    position_data.get('notional_usd', 0),
                    position_data.get('unrealized_pnl', 0),
                    position_data.get('leverage', 1),
                    json.dumps(position_data.get('raw_data', {})),
                    existing['id']
                )
            )
        else:
            # 插入新记录
            self.cursor.execute(
                """INSERT INTO hyperliquid_wallet_positions
                   (trader_id, address, snapshot_time, coin, side, size,
                    entry_price, mark_price, notional_usd, unrealized_pnl,
                    leverage, raw_data)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    trader_id,
                    address,
                    snapshot_time,
                    position_data.get('coin', ''),
                    position_data.get('side', ''),
                    position_data.get('size', 0),
                    position_data.get('entry_price', 0),
                    position_data.get('mark_price', 0),
                    position_data.get('notional_usd', 0),
                    position_data.get('unrealized_pnl', 0),
                    position_data.get('leverage', 1),
                    json.dumps(position_data.get('raw_data', {}))
                )
            )

        self.conn.commit()

    def get_wallet_recent_trades(self, address: str, hours: int = 24, limit: int = 50) -> List[Dict]:
        """
        获取钱包最近交易

        Args:
            address: 钱包地址
            hours: 时间范围
            limit: 返回数量

        Returns:
            交易列表
        """
        cutoff_time = datetime.now() - timedelta(hours=hours)

        self.cursor.execute(
            """SELECT * FROM hyperliquid_wallet_trades
               WHERE address = %s AND trade_time >= %s
               ORDER BY trade_time DESC
               LIMIT %s""",
            (address, cutoff_time, limit)
        )

        return self.cursor.fetchall()

    def get_wallet_positions(self, address: str, latest_only: bool = True) -> List[Dict]:
        """
        获取钱包持仓

        Args:
            address: 钱包地址
            latest_only: 只返回最新持仓

        Returns:
            持仓列表
        """
        if latest_only:
            query = """
                SELECT * FROM v_hyperliquid_latest_positions
                WHERE address = %s
                ORDER BY snapshot_time DESC
            """
            self.cursor.execute(query, (address,))
        else:
            query = """
                SELECT * FROM hyperliquid_wallet_positions
                WHERE address = %s
                ORDER BY snapshot_time DESC
            """
            self.cursor.execute(query, (address,))

        return self.cursor.fetchall()

    def stop_monitoring_wallet(self, address: str):
        """
        停止监控钱包

        Args:
            address: 钱包地址
        """
        now = datetime.now()

        self.cursor.execute(
            """UPDATE hyperliquid_monitored_wallets
               SET is_monitoring = FALSE, updated_at = %s
               WHERE address = %s""",
            (now, address)
        )
        self.conn.commit()

    def remove_monitored_wallet(self, address: str):
        """
        移除监控钱包（删除记录）

        Args:
            address: 钱包地址
        """
        # 先获取 trader_id
        self.cursor.execute(
            "SELECT trader_id FROM hyperliquid_monitored_wallets WHERE address = %s",
            (address,)
        )
        result = self.cursor.fetchone()

        if result:
            self.cursor.execute(
                "DELETE FROM hyperliquid_monitored_wallets WHERE address = %s",
                (address,)
            )
            self.conn.commit()
