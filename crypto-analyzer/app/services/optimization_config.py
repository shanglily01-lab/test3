#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
优化配置管理器 - 自我优化的参数配置系统
支持所有4个优化问题的参数读取和自动调整
"""

from typing import Dict, Optional, Any, List
from datetime import datetime, timedelta
from loguru import logger
import pymysql
from decimal import Decimal


class OptimizationConfig:
    """优化配置管理器 - 支持自我优化的参数配置"""

    def __init__(self, db_config: dict):
        """
        初始化配置管理器

        Args:
            db_config: 数据库配置
        """
        self.db_config = db_config
        self.connection = None

        # 参数缓存 (减少数据库查询)
        self._param_cache = {}
        self._cache_time = None
        self._cache_ttl = 300  # 缓存5分钟

        logger.info("✅ 优化配置管理器已初始化")

    def _get_connection(self):
        """获取数据库连接"""
        if self.connection is None or not self.connection.open:
            self.connection = pymysql.connect(
                **self.db_config,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=True,
            )
        else:
            try:
                self.connection.ping(reconnect=True)
            except:
                self.connection = pymysql.connect(
                    **self.db_config,
                    charset='utf8mb4',
                    cursorclass=pymysql.cursors.DictCursor,
                    autocommit=True,
                )
        return self.connection

    def _refresh_cache(self):
        """刷新参数缓存"""
        conn = self._get_connection()
        cursor = conn.cursor()

        # 1. 从 adaptive_params 读取优化参数
        cursor.execute("SELECT param_key, param_value FROM adaptive_params")
        rows = cursor.fetchall()
        self._param_cache = {row['param_key']: float(row['param_value']) for row in rows}

        # 2. 从 system_settings 读取系统配置（allow_long, allow_short 等）
        cursor.execute("""
            SELECT setting_key, setting_value
            FROM system_settings
            WHERE setting_key IN ('allow_long', 'allow_short')
        """)
        system_rows = cursor.fetchall()
        for row in system_rows:
            self._param_cache[row['setting_key']] = float(row['setting_value'])

        self._cache_time = datetime.now()

        cursor.close()
        logger.debug(f"刷新配置缓存: {len(self._param_cache)} 个参数")

    def get_param(self, key: str, default: Any = None) -> Any:
        """
        获取参数值 (带缓存)

        Args:
            key: 参数键
            default: 默认值

        Returns:
            参数值
        """
        # 检查缓存是否过期
        if self._cache_time is None or (datetime.now() - self._cache_time).total_seconds() > self._cache_ttl:
            self._refresh_cache()

        return self._param_cache.get(key, default)

    def set_param(self, key: str, value: Any, updated_by: str = 'auto_optimizer'):
        """
        设置参数值

        Args:
            key: 参数键
            value: 参数值
            updated_by: 更新来源
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # allow_long 和 allow_short 保存到 system_settings 表
            if key in ('allow_long', 'allow_short'):
                cursor.execute("""
                    INSERT INTO system_settings (setting_key, setting_value, description, updated_by, updated_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON DUPLICATE KEY UPDATE
                        setting_value = VALUES(setting_value),
                        updated_by = VALUES(updated_by),
                        updated_at = NOW()
                """, (key, str(value),
                      f"是否允许{'做多' if key == 'allow_long' else '做空'} (1=允许, 0=禁止)",
                      updated_by))
            else:
                # 其他优化参数保存到 adaptive_params 表
                cursor.execute("""
                    UPDATE adaptive_params
                    SET param_value = %s, updated_by = %s, updated_at = NOW()
                    WHERE param_key = %s
                """, (value, updated_by, key))

            conn.commit()

            # 更新缓存
            self._param_cache[key] = float(value)

            logger.info(f"✅ 参数已更新: {key} = {value} (by {updated_by})")

        except Exception as e:
            conn.rollback()
            logger.error(f"❌ 设置参数失败: {key} = {value}, error: {e}")
            raise
        finally:
            cursor.close()

    # ============================================================
    # 问题1: 动态超时 + 分阶段超时
    # ============================================================

    def get_timeout_by_score(self, entry_score: int) -> int:
        """
        根据入场评分获取超时时间(分钟)

        Args:
            entry_score: 入场评分

        Returns:
            超时时间(分钟)
        """
        # 统一设置为180分钟（3小时）
        return 180

    def adjust_timeout_by_pnl(self, base_timeout_minutes: int, pnl_pct: float) -> int:
        """
        根据盈亏调整超时时间

        Args:
            base_timeout_minutes: 基础超时时间
            pnl_pct: 盈亏百分比 (0.01 = 1%)

        Returns:
            调整后的超时时间(分钟)
        """
        profit_threshold = self.get_param('timeout_profit_extend_threshold', 0.01)
        profit_multiplier = self.get_param('timeout_profit_extend_multiplier', 1.5)
        loss_threshold = self.get_param('timeout_loss_reduce_threshold', 0.005)
        loss_multiplier = self.get_param('timeout_loss_reduce_multiplier', 0.7)

        # 盈利>1% -> 延长50%
        if pnl_pct > profit_threshold:
            return int(base_timeout_minutes * profit_multiplier)

        # 亏损>0.5% -> 缩短30%
        if pnl_pct < -loss_threshold:
            return int(base_timeout_minutes * loss_multiplier)

        return base_timeout_minutes

    def get_staged_timeout_thresholds(self) -> Dict[int, float]:
        """
        获取分阶段超时阈值

        Returns:
            {小时: 亏损阈值}字典
            例如: {1: -0.02, 2: -0.015, 3: -0.01, 4: -0.005}
        """
        return {
            1: self.get_param('staged_timeout_1h_threshold', -0.02),
            2: self.get_param('staged_timeout_2h_threshold', -0.015),
            3: self.get_param('staged_timeout_3h_threshold', -0.01),
            4: self.get_param('staged_timeout_4h_threshold', -0.005)
        }

    # ============================================================
    # 问题2: 黑名单3级制度
    # ============================================================

    def get_blacklist_config(self, level: int) -> Dict[str, Any]:
        """
        获取黑名单等级配置

        Args:
            level: 黑名单等级 (0=白名单, 1/2/3=黑名单)

        Returns:
            配置字典: {
                'margin_multiplier': 保证金倍数,
                'reversal_threshold': 反转阈值(分)
            }
        """
        if level == 0:
            return {
                'margin_multiplier': self.get_param('whitelist_margin_multiplier', 1.0),
                'reversal_threshold': self.get_param('whitelist_reversal_threshold', 30)
            }
        elif level == 1:
            return {
                'margin_multiplier': self.get_param('blacklist_level1_margin_multiplier', 0.25),
                'reversal_threshold': self.get_param('blacklist_level1_reversal_threshold', 30)
            }
        elif level == 2:
            return {
                'margin_multiplier': self.get_param('blacklist_level2_margin_multiplier', 0.125),
                'reversal_threshold': self.get_param('blacklist_level2_reversal_threshold', 30)
            }
        else:  # level 3
            return {
                'margin_multiplier': 0,  # 永久禁止
                'reversal_threshold': float('inf')
            }

    def get_blacklist_trigger_config(self, level: int) -> Dict[str, Any]:
        """
        获取触发黑名单等级的条件配置

        Args:
            level: 黑名单等级 (1/2/3)

        Returns:
            配置字典: {
                'stop_loss_count': hard_stop_loss次数,
                'loss_amount': 亏损金额(USDT)
            }
        """
        if level == 1:
            return {
                'stop_loss_count': int(self.get_param('blacklist_level1_trigger_stop_loss_count', 3)),
                'loss_amount': self.get_param('blacklist_level1_trigger_loss_amount', 100)
            }
        elif level == 2:
            return {
                'stop_loss_count': int(self.get_param('blacklist_level2_trigger_stop_loss_count', 5)),
                'loss_amount': self.get_param('blacklist_level2_trigger_loss_amount', 200)
            }
        elif level == 3:
            return {
                'stop_loss_count': int(self.get_param('blacklist_level3_trigger_stop_loss_count', 8)),
                'loss_amount': self.get_param('blacklist_level3_trigger_loss_amount', 400)
            }
        else:
            return {'stop_loss_count': 0, 'loss_amount': 0}

    def get_blacklist_upgrade_config(self) -> Dict[str, Any]:
        """
        获取黑名单升级(降级)配置

        Returns:
            配置字典: {
                'profit_amount': 盈利金额(USDT),
                'win_rate': 胜率,
                'observation_days': 观察天数
            }
        """
        return {
            'profit_amount': self.get_param('blacklist_upgrade_profit_amount', 50),
            'win_rate': self.get_param('blacklist_upgrade_win_rate', 0.6),
            'observation_days': int(self.get_param('blacklist_upgrade_observation_days', 7))
        }

    # ============================================================
    # 问题3: 15M K线动态止盈
    # ============================================================

    def get_take_profit_config(self) -> Dict[str, Any]:
        """
        获取止盈配置

        Returns:
            配置字典: {
                'candle_count': 分析K线根数,
                'select_count': 选择K线根数,
                'fixed_coefficient': 固定止盈系数,
                'trailing_coefficient': 移动止盈激活系数,
                'update_interval_minutes': 更新间隔(分钟)
            }
        """
        return {
            'candle_count': int(self.get_param('tp_candle_count', 20)),
            'select_count': int(self.get_param('tp_select_count', 10)),
            'fixed_coefficient': self.get_param('tp_fixed_coefficient', 0.7),
            'trailing_coefficient': self.get_param('tp_trailing_coefficient', 0.5),
            'update_interval_minutes': int(self.get_param('tp_update_interval_minutes', 60))
        }

    # ============================================================
    # 交易方向控制
    # ============================================================

    def _read_system_setting(self, key: str) -> Optional[str]:
        """
        读取系统配置项——每次使用独立的新连接，避免共享连接缓存/旧事务快照污染。

        Returns:
            setting_value 字符串，未找到时返回 None
        """
        conn = None
        try:
            conn = pymysql.connect(
                **self.db_config,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=True,
            )
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT setting_value FROM system_settings WHERE setting_key = %s",
                    (key,)
                )
                row = cur.fetchone()
            return row['setting_value'] if row else None
        except Exception as e:
            logger.warning(f"读取系统配置 {key} 失败: {e}")
            return None
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    def is_long_allowed(self) -> bool:
        """
        检查是否允许做多（独立连接查数据库，实时生效）

        Returns:
            True=允许做多, False=禁止做多
        """
        val = self._read_system_setting('allow_long')
        if val is None:
            logger.warning("读取allow_long失败或未配置, 默认允许")
            return True
        return val in ('1', '1.0', 'true', 'True')

    def is_short_allowed(self) -> bool:
        """
        检查是否允许做空（独立连接查数据库，实时生效）

        Returns:
            True=允许做空, False=禁止做空
        """
        val = self._read_system_setting('allow_short')
        if val is None:
            logger.warning("读取allow_short失败或未配置, 默认允许")
            return True
        return val in ('1', '1.0', 'true', 'True')

    def is_direction_allowed(self, direction: str) -> bool:
        """
        检查指定方向是否允许交易

        Args:
            direction: 交易方向 'LONG' 或 'SHORT'

        Returns:
            True=允许, False=禁止
        """
        if direction == 'LONG':
            return self.is_long_allowed()
        elif direction == 'SHORT':
            return self.is_short_allowed()
        else:
            logger.warning(f"未知交易方向: {direction}")
            return False

    # ============================================================
    # 交易对评级管理
    # ============================================================

    def get_symbol_rating(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        获取交易对评级信息

        Args:
            symbol: 交易对符号

        Returns:
            评级信息字典,不存在返回None
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM trading_symbol_rating
            WHERE symbol = %s
        """, (symbol,))

        result = cursor.fetchone()
        cursor.close()

        return result

    def get_all_symbol_ratings(self) -> List[Dict[str, Any]]:
        """
        获取所有交易对的评级信息

        Returns:
            评级信息列表
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM trading_symbol_rating
            ORDER BY rating_level DESC, updated_at DESC
        """)

        results = cursor.fetchall()
        cursor.close()

        return results if results else []

    def get_symbol_rating_level(self, symbol: str) -> int:
        """
        获取交易对评级等级

        Args:
            symbol: 交易对符号

        Returns:
            评级等级 (0=白名单, 1/2/3=黑名单)
        """
        rating = self.get_symbol_rating(symbol)
        if rating is None:
            return 0  # 默认白名单
        return rating['rating_level']

    def update_symbol_rating(self, symbol: str, new_level: int, reason: str,
                            hard_stop_loss_count: int = 0,
                            total_loss_amount: float = 0,
                            total_profit_amount: float = 0,
                            win_rate: float = 0,
                            total_trades: int = 0):
        """
        更新交易对评级

        Args:
            symbol: 交易对符号
            new_level: 新评级等级
            reason: 评级变更原因
            hard_stop_loss_count: hard_stop_loss次数
            total_loss_amount: 总亏损金额
            total_profit_amount: 总盈利金额
            win_rate: 胜率
            total_trades: 总交易次数
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # 获取旧评级
            old_rating = self.get_symbol_rating(symbol)
            old_level = old_rating['rating_level'] if old_rating else 0

            # 获取统计日期范围
            observation_days = self.get_blacklist_upgrade_config()['observation_days']
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=observation_days)

            # 计算margin_multiplier和score_bonus
            if new_level == 3:
                margin_multiplier = 0.0
                score_bonus = 999
            elif new_level == 2:
                margin_multiplier = 0.125  # 50/400
                score_bonus = 10
            elif new_level == 1:
                margin_multiplier = 0.25  # 100/400
                score_bonus = 5
            else:
                margin_multiplier = 1.0
                score_bonus = 0

            # 更新或插入
            cursor.execute("""
                INSERT INTO trading_symbol_rating (
                    symbol, rating_level, margin_multiplier, score_bonus,
                    hard_stop_loss_count, total_loss_amount, total_profit_amount,
                    win_rate, total_trades, previous_level, level_changed_at,
                    level_change_reason, stats_start_date, stats_end_date
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s, %s
                )
                ON DUPLICATE KEY UPDATE
                    rating_level = VALUES(rating_level),
                    margin_multiplier = VALUES(margin_multiplier),
                    score_bonus = VALUES(score_bonus),
                    hard_stop_loss_count = VALUES(hard_stop_loss_count),
                    total_loss_amount = VALUES(total_loss_amount),
                    total_profit_amount = VALUES(total_profit_amount),
                    win_rate = VALUES(win_rate),
                    total_trades = VALUES(total_trades),
                    previous_level = %s,
                    level_changed_at = NOW(),
                    level_change_reason = VALUES(level_change_reason),
                    stats_start_date = VALUES(stats_start_date),
                    stats_end_date = VALUES(stats_end_date)
            """, (symbol, new_level, margin_multiplier, score_bonus,
                  hard_stop_loss_count, total_loss_amount, total_profit_amount,
                  win_rate, total_trades, old_level, reason, start_date, end_date,
                  old_level))

            conn.commit()

            logger.info(f"✅ 更新交易对评级: {symbol} {old_level}→{new_level} ({reason})")

            # 记录优化日志
            self._log_optimization('blacklist',
                                 f'upgrade_rating' if new_level < old_level else 'downgrade_rating',
                                 symbol,
                                 f'Level {old_level}',
                                 f'Level {new_level}',
                                 reason)

        except Exception as e:
            conn.rollback()
            logger.error(f"❌ 更新交易对评级失败: {symbol}, error: {e}")
            raise
        finally:
            cursor.close()

    # ============================================================
    # 波动率配置管理
    # ============================================================

    def get_symbol_volatility_profile(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        获取交易对波动率配置

        Args:
            symbol: 交易对符号

        Returns:
            波动率配置字典,不存在返回None
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT * FROM symbol_volatility_profile
            WHERE symbol = %s
        """, (symbol,))

        result = cursor.fetchone()
        cursor.close()

        return result

    def update_symbol_volatility_profile(self, symbol: str, direction: str,
                                        avg_range_pct: float,
                                        candles_analyzed: int):
        """
        更新交易对波动率配置

        Args:
            symbol: 交易对符号
            direction: 方向 (LONG/SHORT)
            avg_range_pct: 平均波动百分比
            candles_analyzed: 分析的K线数量
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # 获取止盈系数
            tp_config = self.get_take_profit_config()
            fixed_coef = tp_config['fixed_coefficient']
            trailing_coef = tp_config['trailing_coefficient']

            # 计算止盈参数
            fixed_tp_pct = avg_range_pct * fixed_coef
            trailing_activation_pct = avg_range_pct * trailing_coef

            if direction == 'LONG':
                cursor.execute("""
                    INSERT INTO symbol_volatility_profile (
                        symbol, long_avg_bullish_range_pct, long_fixed_tp_pct,
                        long_trailing_activation_pct, long_candles_analyzed
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        long_avg_bullish_range_pct = VALUES(long_avg_bullish_range_pct),
                        long_fixed_tp_pct = VALUES(long_fixed_tp_pct),
                        long_trailing_activation_pct = VALUES(long_trailing_activation_pct),
                        long_candles_analyzed = VALUES(long_candles_analyzed)
                """, (symbol, avg_range_pct, fixed_tp_pct, trailing_activation_pct, candles_analyzed))

            else:  # SHORT
                cursor.execute("""
                    INSERT INTO symbol_volatility_profile (
                        symbol, short_avg_bearish_range_pct, short_fixed_tp_pct,
                        short_trailing_activation_pct, short_candles_analyzed
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        short_avg_bearish_range_pct = VALUES(short_avg_bearish_range_pct),
                        short_fixed_tp_pct = VALUES(short_fixed_tp_pct),
                        short_trailing_activation_pct = VALUES(short_trailing_activation_pct),
                        short_candles_analyzed = VALUES(short_candles_analyzed)
                """, (symbol, avg_range_pct, fixed_tp_pct, trailing_activation_pct, candles_analyzed))

            conn.commit()

            logger.info(f"✅ 更新波动率配置: {symbol} {direction} avg={avg_range_pct:.4f}%, "
                       f"fixed_tp={fixed_tp_pct:.4f}%, trailing={trailing_activation_pct:.4f}%")

            # 记录优化日志
            self._log_optimization('take_profit', 'update_tp', symbol,
                                 None,
                                 f'{direction} avg={avg_range_pct:.4f}% -> fixed_tp={fixed_tp_pct:.4f}%',
                                 f'Analyzed {candles_analyzed} candles')

        except Exception as e:
            conn.rollback()
            logger.error(f"❌ 更新波动率配置失败: {symbol} {direction}, error: {e}")
            raise
        finally:
            cursor.close()

    # ============================================================
    # 优化日志
    # ============================================================

    def _log_optimization(self, log_type: str, action: str, symbol: Optional[str],
                         old_value: Optional[str], new_value: Optional[str],
                         reason: Optional[str]):
        """
        记录优化日志

        Args:
            log_type: 日志类型
            action: 操作
            symbol: 交易对
            old_value: 旧值
            new_value: 新值
            reason: 原因
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("""
                INSERT INTO optimization_logs (
                    log_type, action, symbol, old_value, new_value, reason
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (log_type, action, symbol, old_value, new_value, reason))

            conn.commit()

        except Exception as e:
            logger.error(f"❌ 记录优化日志失败: {e}")
        finally:
            cursor.close()


# 测试代码
if __name__ == '__main__':
    import os as _os
    db_config = {
        'host':     _os.getenv('DB_HOST', 'localhost'),
        'port':     int(_os.getenv('DB_PORT', '3306')),
        'user':     _os.getenv('DB_USER', ''),
        'password': _os.getenv('DB_PASSWORD', ''),
        'database': _os.getenv('DB_NAME', ''),
    }

    config = OptimizationConfig(db_config)

    # 测试问题1: 动态超时
    print("\n=== 问题1: 动态超时 ===")
    print(f"评分40超时: {config.get_timeout_by_score(40)}分钟")
    print(f"评分35超时: {config.get_timeout_by_score(35)}分钟")
    print(f"评分30超时: {config.get_timeout_by_score(30)}分钟")
    print(f"评分25超时: {config.get_timeout_by_score(25)}分钟")
    print(f"盈利1.5%调整: {config.adjust_timeout_by_pnl(240, 0.015)}分钟")
    print(f"亏损0.8%调整: {config.adjust_timeout_by_pnl(240, -0.008)}分钟")
    print(f"分阶段阈值: {config.get_staged_timeout_thresholds()}")

    # 测试问题2: 黑名单3级制度
    print("\n=== 问题2: 黑名单3级制度 ===")
    for level in [0, 1, 2, 3]:
        cfg = config.get_blacklist_config(level)
        print(f"Level {level}: margin={cfg['margin_multiplier']}, reversal={cfg['reversal_threshold']}")
        if level > 0:
            trigger = config.get_blacklist_trigger_config(level)
            print(f"  触发条件: stop_loss_count={trigger['stop_loss_count']}, loss_amount={trigger['loss_amount']}")
    upgrade_cfg = config.get_blacklist_upgrade_config()
    print(f"升级配置: profit={upgrade_cfg['profit_amount']}, win_rate={upgrade_cfg['win_rate']}, "
          f"days={upgrade_cfg['observation_days']}")

    # 测试问题3: 止盈配置
    print("\n=== 问题3: 15M K线动态止盈 ===")
    tp_cfg = config.get_take_profit_config()
    print(f"分析{tp_cfg['candle_count']}根, 选择{tp_cfg['select_count']}根")
    print(f"固定系数: {tp_cfg['fixed_coefficient']}, 移动系数: {tp_cfg['trailing_coefficient']}")
    print(f"更新间隔: {tp_cfg['update_interval_minutes']}分钟")
