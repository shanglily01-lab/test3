#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
智能自动交易服务 - 生产环境版本
直接在服务器后台运行
"""

import time
import sys
import os
import asyncio
import urllib.request
import json as _json
from datetime import datetime, time as dt_time, timezone, timedelta
from decimal import Decimal
from loguru import logger
import pymysql
from dotenv import load_dotenv

# 导入 WebSocket 价格服务
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from app.services.binance_ws_price import get_ws_price_service, BinanceWSPriceService
from app.services.adaptive_optimizer import AdaptiveOptimizer
from app.services.optimization_config import OptimizationConfig
from app.services.symbol_rating_manager import SymbolRatingManager
from app.services.volatility_profile_updater import VolatilityProfileUpdater
from app.services.smart_exit_optimizer import SmartExitOptimizer
from app.services.smart_entry_executor import SmartEntryExecutor
from app.services.big4_trend_detector import Big4TrendDetector
from app.services.breakout_signal_booster import BreakoutSignalBooster
from app.services.signal_blacklist_checker import SignalBlacklistChecker
from app.services.signal_score_v2_service import SignalScoreV2Service
from app.strategies.range_market_detector import RangeMarketDetector
from app.strategies.bollinger_mean_reversion import BollingerMeanReversionStrategy
from app.strategies.mode_switcher import TradingModeSwitcher
from app.services.big4_regime_monitor import Big4RegimeMonitor

# 加载环境变量
load_dotenv()

# 配置日志
logger.remove()
logger.add(
    sys.stderr,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:8} | {message}",
    level="INFO"
)
logger.add(
    "logs/smart_trader_{time:YYYY-MM-DD}.log",
    rotation="00:00",
    retention="30 days",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level:8} | {message}",
    level="INFO"
)


class SmartDecisionBrain:
    """智能决策大脑 - 内嵌版本"""

    def __init__(self, db_config: dict):
        self.db_config = db_config
        self.connection = None

        # 从config.yaml加载配置
        self._load_config()

        self.threshold = 65  # 开仓阈值提高至65：减少低质量信号，提高胜率
        self.max_threshold = 130  # 评分上限：拒绝高分追涨杀跌信号（130分以上往往是过热信号）

        # 初始化信号黑名单检查器（动态加载，5分钟缓存）
        self.blacklist_checker = SignalBlacklistChecker(db_config, cache_minutes=5)

        # V2评分服务已在_load_config()中初始化

    def _reload_blacklist(self):
        """重新加载黑名单和白名单（每5分钟运行）"""
        try:
            import yaml

            # 重新加载config.yaml中的交易对列表
            with open('config.yaml', 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                all_symbols = set(config.get('symbols', []))

            conn = self._get_connection()
            cursor = conn.cursor()

            # 重新加载黑名单标识（rating_level 1-2，用于小仓位）
            cursor.execute("""
                SELECT symbol FROM trading_symbol_rating
                WHERE rating_level >= 1 AND rating_level < 3
                ORDER BY rating_level DESC, updated_at DESC
            """)
            blacklist_rows = cursor.fetchall()
            old_blacklist = set(self.blacklist) if hasattr(self, 'blacklist') else set()
            new_blacklist = set([row['symbol'] for row in blacklist_rows]) if blacklist_rows else set()

            # 扫描池 = config.yaml 的所有交易对（不过滤）
            old_whitelist = set(self.whitelist) if hasattr(self, 'whitelist') else set()
            new_whitelist = all_symbols

            cursor.close()

            # 记录黑名单变化
            blacklist_added = new_blacklist - old_blacklist
            blacklist_removed = old_blacklist - new_blacklist

            if blacklist_added:
                logger.info(f"[BLACKLIST-UPDATE] ➕ 新增黑名单: {', '.join(sorted(blacklist_added))}")
            if blacklist_removed:
                logger.info(f"[BLACKLIST-UPDATE] ➖ 移除黑名单: {', '.join(sorted(blacklist_removed))}")

            # 记录扫描池变化（config.yaml变化）
            whitelist_added = new_whitelist - old_whitelist
            whitelist_removed = old_whitelist - new_whitelist

            if whitelist_added:
                logger.info(f"[WHITELIST-UPDATE] ➕ 新增扫描池: {', '.join(sorted(whitelist_added))}")
            if whitelist_removed:
                logger.info(f"[WHITELIST-UPDATE] ➖ 移除扫描池: {', '.join(sorted(whitelist_removed))}")

            self.blacklist = list(new_blacklist)
            self.whitelist = list(new_whitelist)

            return len(blacklist_added) > 0 or len(blacklist_removed) > 0 or len(whitelist_added) > 0 or len(whitelist_removed) > 0
        except Exception as e:
            logger.error(f"[BLACKLIST-RELOAD-ERROR] 重新加载黑白名单失败: {e}")
            return False

    def _load_config(self):
        """从数据库加载黑名单和自适应参数,从config.yaml加载交易对列表"""
        try:
            import yaml

            # 1. 从config.yaml加载交易对列表
            with open('config.yaml', 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                all_symbols = config.get('symbols', [])

            # 2. 从数据库加载黑名单标识（rating_level 1-2级，用于小仓位）
            # rating_level: 0=白名单, 1=黑名单1级, 2=黑名单2级, 3=黑名单3级(永久禁止，不扫描)
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT symbol FROM trading_symbol_rating
                WHERE rating_level >= 1 AND rating_level < 3
                ORDER BY rating_level DESC, updated_at DESC
            """)
            blacklist_rows = cursor.fetchall()
            self.blacklist = [row['symbol'] for row in blacklist_rows] if blacklist_rows else []

            # 3. 从数据库加载自适应参数
            cursor.execute("""
                SELECT param_key, param_value
                FROM adaptive_params
                WHERE param_type = 'long'
            """)
            long_params = {row['param_key']: float(row['param_value']) for row in cursor.fetchall()}

            cursor.execute("""
                SELECT param_key, param_value
                FROM adaptive_params
                WHERE param_type = 'short'
            """)
            short_params = {row['param_key']: float(row['param_value']) for row in cursor.fetchall()}

            cursor.close()

            # 4. 构建自适应参数字典
            self.adaptive_long = {
                'stop_loss_pct': long_params.get('long_stop_loss_pct', 0.03),
                'take_profit_pct': long_params.get('long_take_profit_pct', 0.02),
                'min_holding_minutes': long_params.get('long_min_holding_minutes', 60),
                'position_size_multiplier': long_params.get('long_position_size_multiplier', 1.0)
            }

            self.adaptive_short = {
                'stop_loss_pct': short_params.get('short_stop_loss_pct', 0.03),
                'take_profit_pct': short_params.get('short_take_profit_pct', 0.02),
                'min_holding_minutes': short_params.get('short_min_holding_minutes', 60),
                'position_size_multiplier': short_params.get('short_position_size_multiplier', 1.0)
            }

            # 5. 从数据库加载信号黑名单
            self.signal_blacklist = {}
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT signal_type, position_side
                    FROM signal_blacklist
                    WHERE is_active = TRUE
                """)
                signal_blacklist_rows = cursor.fetchall()
                for row in signal_blacklist_rows:
                    key = f"{row['signal_type']}_{row['position_side']}"
                    self.signal_blacklist[key] = True
                cursor.close()
            except:
                # 如果表不存在，使用空字典
                self.signal_blacklist = {}

            # 6. 扫描池 = config.yaml 中的所有交易对
            self.whitelist = all_symbols

            logger.info(f"✅ 从数据库加载配置:")
            logger.info(f"   总交易对: {len(all_symbols)}")
            logger.info(f"   数据库黑名单: {len(self.blacklist)} 个 (使用100U小仓位)")
            logger.info(f"   可交易: {len(self.whitelist)} 个")
            logger.info(f"   📊 自适应参数 (从数据库):")
            logger.info(f"      LONG止损: {self.adaptive_long['stop_loss_pct']*100:.1f}%, 止盈: {self.adaptive_long['take_profit_pct']*100:.1f}%, 最小持仓: {self.adaptive_long['min_holding_minutes']:.0f}分钟, 仓位倍数: {self.adaptive_long['position_size_multiplier']:.1f}")
            logger.info(f"      SHORT止损: {self.adaptive_short['stop_loss_pct']*100:.1f}%, 止盈: {self.adaptive_short['take_profit_pct']*100:.1f}%, 最小持仓: {self.adaptive_short['min_holding_minutes']:.0f}分钟, 仓位倍数: {self.adaptive_short['position_size_multiplier']:.1f}")

            if self.blacklist:
                logger.info(f"   ⚠️  黑名单交易对(小仓位): {', '.join(self.blacklist)}")

            if self.signal_blacklist:
                logger.info(f"   🚫 禁用信号: {len(self.signal_blacklist)} 个")

            # 7. 从数据库加载评分权重（含 is_active=FALSE 的禁用信号，权重置0）
            self.scoring_weights = {}
            self._disabled_signals = set()
            try:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT signal_component, weight_long, weight_short, is_active
                    FROM signal_scoring_weights
                    WHERE strategy_type = 'default'
                """)
                weight_rows = cursor.fetchall()
                active_count = 0
                disabled_count = 0
                for row in weight_rows:
                    if row['is_active']:
                        self.scoring_weights[row['signal_component']] = {
                            'long': float(row['weight_long']),
                            'short': float(row['weight_short'])
                        }
                        active_count += 1
                    else:
                        # 禁用信号: 权重置0，并记录到禁用集合
                        self.scoring_weights[row['signal_component']] = {'long': 0.0, 'short': 0.0}
                        self._disabled_signals.add(row['signal_component'])
                        disabled_count += 1
                cursor.close()

                if self.scoring_weights:
                    logger.info(f"   Signal weights: loaded {active_count} active, {disabled_count} disabled from DB")
            except:
                # 如果表不存在，使用默认权重（硬编码）
                self.scoring_weights = {
                    'position_low': {'long': 20, 'short': 0},
                    'position_mid': {'long': 5, 'short': 5},
                    'position_high': {'long': 0, 'short': 20},
                    'momentum_down_3pct': {'long': 0, 'short': 10},       # 震荡市优化: 从15降到10,需要更多信号配合
                    'momentum_up_3pct': {'long': 10, 'short': 0},         # 震荡市优化: 从15降到10,避免追涨杀跌
                    'trend_1h_bull': {'long': 20, 'short': 0},
                    'trend_1h_bear': {'long': 0, 'short': 20},
                    'volatility_high': {'long': 10, 'short': 10},
                    'consecutive_bull': {'long': 15, 'short': 0},
                    'consecutive_bear': {'long': 0, 'short': 15},
                    'volume_power_bull': {'long': 25, 'short': 0},        # 1H+15M量能多头
                    'volume_power_bear': {'long': 0, 'short': 25},        # 1H+15M量能空头
                    'volume_power_1h_bull': {'long': 15, 'short': 0},     # 仅1H量能多头
                    'volume_power_1h_bear': {'long': 0, 'short': 15},     # 仅1H量能空头
                    'breakout_long': {'long': 20, 'short': 0},            # 高位突破追涨
                    'breakdown_short': {'long': 0, 'short': 20}           # 低位破位追空
                    # 已移除: ema_bull, ema_bear (Big4市场趋势判断已足够)
                }
                logger.info(f"   📊 评分权重: 使用默认权重")

            # V2评分过滤服务（协同确认）
            resonance_config = config.get('signals', {}).get('resonance_filter', {})
            self.score_v2_service = SignalScoreV2Service(
                db_config=self.db_config,
                score_config=resonance_config
            )
            logger.info(f"   ✅ V2评分过滤服务已初始化")

        except Exception as e:
            import traceback
            logger.error(f"❌ 读取数据库配置失败，使用默认14个交易对")
            logger.error(f"   错误类型: {type(e).__name__}")
            logger.error(f"   错误信息: {e}")
            logger.error(f"   当前工作目录: {os.getcwd()}")
            logger.error(f"   详细堆栈:\n{traceback.format_exc()}")
            self.whitelist = [
                'BCH/USDT', 'LDO/USDT', 'ENA/USDT', 'WIF/USDT', 'TAO/USDT',
                'DASH/USDT', 'ETC/USDT', 'VIRTUAL/USDT', 'NEAR/USDT',
                'AAVE/USDT', 'SUI/USDT', 'UNI/USDT', 'ADA/USDT', 'SOL/USDT'
            ]
            self.blacklist = []
            self.adaptive_long = {'stop_loss_pct': 0.03, 'take_profit_pct': 0.02, 'min_holding_minutes': 60, 'position_size_multiplier': 1.0}
            self.adaptive_short = {'stop_loss_pct': 0.03, 'take_profit_pct': 0.02, 'min_holding_minutes': 60, 'position_size_multiplier': 1.0}
            # 🔥 修复: 初始化signal_blacklist
            self.signal_blacklist = {}
            # 🔥 修复: 初始化scoring_weights
            self.scoring_weights = {
                'position_low': {'long': 20, 'short': 0},
                'position_mid': {'long': 5, 'short': 5},
                'position_high': {'long': 0, 'short': 20},
                'momentum_down_3pct': {'long': 0, 'short': 10},
                'momentum_up_3pct': {'long': 10, 'short': 0},
                'trend_1h_bull': {'long': 20, 'short': 0},
                'trend_1h_bear': {'long': 0, 'short': 20},
                'volatility_high': {'long': 10, 'short': 10},
                'consecutive_bull': {'long': 15, 'short': 0},
                'consecutive_bear': {'long': 0, 'short': 15},
                'volume_power_bull': {'long': 25, 'short': 0},
                'volume_power_bear': {'long': 0, 'short': 25},
                'volume_power_1h_bull': {'long': 15, 'short': 0},
                'volume_power_1h_bear': {'long': 0, 'short': 15},
                'breakout_long': {'long': 20, 'short': 0},
                'breakdown_short': {'long': 0, 'short': 20}
            }
            # 🔥 修复: 初始化score_v2_service（异常情况下也需要）
            try:
                self.score_v2_service = SignalScoreV2Service(
                    db_config=self.db_config,
                    score_config={'enabled': True, 'min_symbol_score': 15}
                )
                logger.info(f"   ✅ V2评分过滤服务已初始化（降级模式）")
            except Exception as v2_error:
                logger.error(f"   ❌ V2评分过滤服务初始化失败: {v2_error}")
                self.score_v2_service = None

    def reload_config(self):
        """重新加载配置 - 供外部调用"""
        logger.info("🔄 重新加载配置文件...")
        self._load_config()
        # 同时强制重新加载信号黑名单
        if hasattr(self, 'blacklist_checker'):
            self.blacklist_checker.force_reload()
        return len(self.whitelist)

    def _get_connection(self):
        if self.connection is None or not self.connection.open:
            self.connection = pymysql.connect(
                **self.db_config,
                charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor,
                connect_timeout=10,  # 🔥 连接超时10秒
                read_timeout=30,     # 🔥 读取超时30秒
                write_timeout=30     # 🔥 写入超时30秒
            )
            # 🔥 设置InnoDB锁等待超时为5秒
            with self.connection.cursor() as cursor:
                cursor.execute("SET SESSION innodb_lock_wait_timeout = 5")
        else:
            try:
                self.connection.ping(reconnect=True)
            except:
                self.connection = pymysql.connect(
                    **self.db_config,
                    charset='utf8mb4',
                    cursorclass=pymysql.cursors.DictCursor,
                    connect_timeout=10,  # 🔥 连接超时10秒
                    read_timeout=30,     # 🔥 读取超时30秒
                    write_timeout=30     # 🔥 写入超时30秒
                )
                # 🔥 设置InnoDB锁等待超时为5秒
                with self.connection.cursor() as cursor:
                    cursor.execute("SET SESSION innodb_lock_wait_timeout = 5")
        return self.connection

    def _get_hl_whale_signal(self, symbol: str) -> tuple:
        """
        查询 Hyperliquid 鲸鱼资金流向信号
        Returns: (signal, long_short_ratio, net_flow, total_volume)
          signal: 'STRONG_BULLISH'|'BULLISH'|'STRONG_BEARISH'|'BEARISH'|'NEUTRAL'
        """
        try:
            # 将 Binance 格式 (BTC/USDT) 转换为 HL 格式 (BTC)
            hl_coin = symbol.replace('/USDT', '').replace('1000', 'k')
            # 特殊映射
            hl_map = {'1000PEPE': 'kPEPE', '1000BONK': 'kBONK', '1000SHIB': 'kSHIB', '1000FLOKI': 'kFLOKI'}
            hl_coin = hl_map.get(symbol.replace('/USDT', ''), hl_coin)

            conn = self._get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT net_flow, long_short_ratio, total_volume, hyperliquid_signal, updated_at
                    FROM hyperliquid_symbol_aggregation
                    WHERE symbol = %s AND period = '24h'
                    LIMIT 1
                """, (hl_coin,))
                row = cursor.fetchone()

            if not row:
                return ('NEUTRAL', 1.0, 0.0, 0.0)

            # 数据超过2小时不用
            from datetime import datetime, timedelta
            if row['updated_at'] and (datetime.now() - row['updated_at']) > timedelta(hours=2):
                return ('NEUTRAL', 1.0, 0.0, 0.0)

            net_flow = float(row['net_flow'] or 0)
            ls_ratio = float(row['long_short_ratio'] or 1.0)
            total_vol = float(row['total_volume'] or 0)
            hl_sig = row['hyperliquid_signal'] or 'NEUTRAL'

            # 使用 HL 预计算信号 + L/S 比率双重判断
            # L/S 相对阈值（基于总量）- 解决绝对值阈值对小币种不适用的问题
            if hl_sig in ('STRONG_BULLISH', 'BULLISH'):
                signal = hl_sig
            elif hl_sig in ('STRONG_BEARISH', 'BEARISH'):
                signal = hl_sig
            elif total_vol > 30000:
                # 用 L/S 比率作为补充信号
                if ls_ratio >= 3.0:
                    signal = 'STRONG_BULLISH'
                elif ls_ratio >= 2.0:
                    signal = 'BULLISH'
                elif ls_ratio <= 0.33:
                    signal = 'STRONG_BEARISH'
                elif ls_ratio <= 0.5:
                    signal = 'BEARISH'
                else:
                    signal = 'NEUTRAL'
            else:
                signal = 'NEUTRAL'

            return (signal, ls_ratio, net_flow, total_vol)
        except Exception as e:
            logger.debug(f"HL whale signal query failed for {symbol}: {e}")
            return ('NEUTRAL', 1.0, 0.0, 0.0)

    def _get_mf_confluence_signal(self, symbol: str) -> str:
        """
        多时间框架共振信号 (Multi-timeframe confluence)
        数据来源: technical_signals_cache (每小时更新)
        逻辑: 1H[24h] + 15M[4h] 同时偏向多头/空头 → 共振确认
        Returns: 'BULLISH'|'BEARISH'|'NEUTRAL'
        """
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT timeframe, window_label, bullish_pct, bearish_pct
                    FROM technical_signals_cache
                    WHERE symbol = %s AND timeframe IN ('1h', '15m')
                      AND window_label IN ('24h', '4h')
                      AND updated_at >= NOW() - INTERVAL 2 HOUR
                """, (symbol,))
                rows = cursor.fetchall()

            # 提取各时间框架数据
            data = {}
            for r in rows:
                key = f"{r['timeframe']}_{r['window_label']}"
                data[key] = {'bull': float(r['bullish_pct']), 'bear': float(r['bearish_pct'])}

            h1_24h = data.get('1h_24h', {})
            m15_4h = data.get('15m_4h', {})
            if not h1_24h or not m15_4h:
                return 'NEUTRAL'

            bull_1h = h1_24h.get('bull', 50)
            bear_1h = h1_24h.get('bear', 50)
            bull_15m = m15_4h.get('bull', 50)
            bear_15m = m15_4h.get('bear', 50)

            # 极强多头共振: 1H超70%多 AND 15M超65%多 (先检查强信号)
            if bull_1h > 70 and bull_15m > 65:
                return 'STRONG_BULLISH'
            # 极强空头共振: 1H超70%空 AND 15M超65%空
            elif bear_1h > 70 and bear_15m > 65:
                return 'STRONG_BEARISH'
            # 多头共振: 1H超60%多 AND 15M超55%多
            elif bull_1h > 60 and bull_15m > 55:
                return 'BULLISH'
            # 空头共振: 1H超60%空 AND 15M超55%空
            elif bear_1h > 60 and bear_15m > 55:
                return 'BEARISH'
            return 'NEUTRAL'
        except Exception as e:
            logger.debug(f"MF confluence signal failed for {symbol}: {e}")
            return 'NEUTRAL'

    def _detect_volume_climax(self, klines_1h: list) -> str:
        """
        量价高潮信号 (Volume Climax)
        原理: 连续下跌 + 成交量递增 → 恐慌性抛售/强制平仓高潮 → 反弹 LONG
              连续上涨 + 成交量递增 → 多头高潮出货 → 回调 SHORT
        条件: 最近4根1H K线中连续3根同向 + 每根量比前根大 + 总量 > 平均1.5x

        Returns: 'BULLISH_CLIMAX'|'BEARISH_CLIMAX'|'NONE'
        """
        if len(klines_1h) < 6:
            return 'NONE'

        recent = klines_1h[-5:]  # 最近5根
        avg_vol = sum(k['volume'] for k in klines_1h[-24:]) / 24

        # 检查最近3根是否连续下跌且量递增
        bear_run = []
        bull_run = []
        for k in recent[-3:]:
            is_red = k['close'] < k['open']
            is_green = k['close'] > k['open']
            bear_run.append(is_red)
            bull_run.append(is_green)

        bear_consec = all(bear_run)
        bull_consec = all(bull_run)

        if not bear_consec and not bull_consec:
            return 'NONE'

        vols = [k['volume'] for k in recent[-3:]]

        # 量能必须递增（每根 > 前根 的 90%）
        vol_increasing = all(vols[i] >= vols[i-1] * 0.90 for i in range(1, len(vols)))
        # 最后一根量 > 1.5x 平均量（确认是高潮量）
        vol_spike = vols[-1] > avg_vol * 1.5

        if bear_consec and vol_increasing and vol_spike:
            return 'BULLISH_CLIMAX'  # 空头高潮 → 多头反弹机会
        elif bull_consec and vol_increasing and vol_spike:
            return 'BEARISH_CLIMAX'  # 多头高潮 → 空头回调机会
        return 'NONE'

    def _detect_taker_buy_signal(self, klines_1h: list) -> str:
        """
        主动买入比例信号 (Taker Buy Ratio)
        原理: taker_buy_volume/total_volume 反映主动买方 vs 主动卖方的力量对比
          - 吸筹信号: 前3根低买盘(< 0.40) → 最近2根买盘突增(> 0.62)
            → 大资金趁跌承接，均值回归LONG
          - 持续施压: 连续4根买盘极低(< 0.36) → 空方主导，SHORT
        需要至少6根K线，且每根都有taker_buy_base_volume数据
        Returns: 'BULLISH_ABSORPTION'|'BEARISH_PRESSURE'|'NONE'
        """
        if len(klines_1h) < 6:
            return 'NONE'
        recent = klines_1h[-6:]
        buy_ratios = []
        for k in recent:
            vol = float(k.get('volume', 0) or 0)
            tbv = k.get('taker_buy_base_volume')
            if vol > 0 and tbv is not None:
                buy_ratios.append(float(tbv) / vol)
            else:
                return 'NONE'  # 数据不完整则跳过
        # 吸筹: 前3根低买盘 + 最近2根买盘突增
        prior_low = all(r < 0.42 for r in buy_ratios[:3])
        recent_surge = all(r > 0.60 for r in buy_ratios[-2:])
        if prior_low and recent_surge:
            return 'BULLISH_ABSORPTION'
        # 持续施压: 最近4根买盘持续极低
        persistent_pressure = all(r < 0.36 for r in buy_ratios[-4:])
        if persistent_pressure:
            return 'BEARISH_PRESSURE'
        return 'NONE'

    def _calc_ema(self, closes: list, period: int) -> list:
        """指数移动平均(EMA)计算"""
        if len(closes) < period:
            return []
        k = 2.0 / (period + 1)
        ema = sum(closes[:period]) / period
        result = [ema]
        for price in closes[period:]:
            ema = price * k + ema * (1 - k)
            result.append(ema)
        return result

    def _detect_macd_crossover(self, klines_1h: list) -> str:
        """
        MACD柱状图零轴交叉信号 (MACD Histogram Crossover)
        原理: MACD柱状图 = MACD线(EMA12-EMA26) - 信号线(EMA9)
          - 柱状图从负转正 (零轴上穿): 动量由空转多 → LONG
          - 柱状图从正转负 (零轴下穿): 动量由多转空 → SHORT
        这是动量切换的早期信号，比价格方向更超前
        需要: 至少40根1H K线 (26+9+5 冗余)
        Returns: 'BULLISH_CROSS'|'BEARISH_CROSS'|'NONE'
        """
        if len(klines_1h) < 40:
            return 'NONE'
        closes = [float(k['close']) for k in klines_1h[-40:]]
        ema12 = self._calc_ema(closes, 12)
        ema26 = self._calc_ema(closes, 26)
        # ema12有29个元素，ema26有15个元素
        # ema26[i]对应closes[25+i]，ema12[14+i]对应closes[25+i]
        if len(ema26) < 2:
            return 'NONE'
        macd = [ema12[14 + i] - ema26[i] for i in range(len(ema26))]
        if len(macd) < 11:
            return 'NONE'
        signal = self._calc_ema(macd, 9)
        if len(signal) < 2:
            return 'NONE'
        offset = len(macd) - len(signal)
        histogram = [macd[offset + i] - signal[i] for i in range(len(signal))]
        if len(histogram) < 2:
            return 'NONE'
        prev_hist = histogram[-2]
        curr_hist = histogram[-1]
        if prev_hist < 0 and curr_hist > 0:
            return 'BULLISH_CROSS'
        elif prev_hist > 0 and curr_hist < 0:
            return 'BEARISH_CROSS'
        return 'NONE'

    def _calc_rsi(self, closes: list, period: int = 14) -> list:
        """
        Wilder RSI计算
        Returns: RSI值列表，长度 = len(closes) - period - 1
        每个元素对应 closes[period + i] 位置的RSI值 (i=0,1,...)
        """
        if len(closes) <= period + 1:
            return []
        deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
        gains = [max(0.0, d) for d in deltas]
        losses = [max(0.0, -d) for d in deltas]
        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period
        rsi_vals = []
        for i in range(period, len(deltas)):
            if avg_loss == 0.0:
                rsi_vals.append(100.0)
            else:
                rs = avg_gain / avg_loss
                rsi_vals.append(100.0 - 100.0 / (1.0 + rs))
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        return rsi_vals

    def _detect_rsi_divergence(self, klines_1h: list) -> str:
        """
        RSI背离信号 (RSI Divergence)
        原理:
          - 多头背离(Bullish): 价格创新低但RSI未创新低 → 卖压衰竭 → LONG信号
          - 空头背离(Bearish): 价格创新高但RSI未创新高 → 买力衰竭 → SHORT信号
        条件:
          - 需要至少35根1H K线（14根初始RSI + 21根检测窗口）
          - 窗口: 前10根 vs 后10根（最新K线排除，避免未完成蜡烛干扰）
          - 背离确认: 价格变化幅度 > 0.5%，RSI反向变化 > 3点
        Returns: 'BULLISH_DIVERGENCE'|'BEARISH_DIVERGENCE'|'NONE'
        """
        if len(klines_1h) < 35:
            return 'NONE'
        closes = [k['close'] for k in klines_1h[-35:]]
        rsi_values = self._calc_rsi(closes, period=14)
        # rsi_values有20个元素，对应closes[14:34]
        if len(rsi_values) < 20:
            return 'NONE'
        # early窗口: closes[14:24] <-> rsi_values[0:10]
        # recent窗口: closes[24:34] <-> rsi_values[10:20]（排除closes[34]未完成K线）
        early_closes = closes[14:24]
        recent_closes = closes[24:34]
        early_rsi = rsi_values[:10]
        recent_rsi = rsi_values[10:]
        # 多头背离: 找两段各自的最低价和对应RSI
        early_min_idx = early_closes.index(min(early_closes))
        recent_min_idx = recent_closes.index(min(recent_closes))
        early_low_price = early_closes[early_min_idx]
        recent_low_price = recent_closes[recent_min_idx]
        early_low_rsi = early_rsi[early_min_idx]
        recent_low_rsi = recent_rsi[recent_min_idx]
        if recent_low_price < early_low_price * 0.995 and recent_low_rsi > early_low_rsi + 3.0:
            return 'BULLISH_DIVERGENCE'
        # 空头背离: 找两段各自的最高价和对应RSI
        early_max_idx = early_closes.index(max(early_closes))
        recent_max_idx = recent_closes.index(max(recent_closes))
        early_high_price = early_closes[early_max_idx]
        recent_high_price = recent_closes[recent_max_idx]
        early_high_rsi = early_rsi[early_max_idx]
        recent_high_rsi = recent_rsi[recent_max_idx]
        if recent_high_price > early_high_price * 1.005 and recent_high_rsi < early_high_rsi - 3.0:
            return 'BEARISH_DIVERGENCE'
        return 'NONE'

    def _get_rsi_level_signal(self, klines_1h: list) -> str:
        """
        RSI绝对位置信号 (RSI Level)
        原理: 基于最新K线的RSI绝对水平判断超卖/超买
          - 极度超卖 RSI < 15: 历史均值回归 → STRONG_LONG
          - 超卖 RSI 15-25: 低位反弹概率增加 → LONG
          - 极度超买 RSI > 85: 高位回落概率增加 → STRONG_SHORT
          - 超买 RSI 75-85: 获利了结压力 → SHORT
        与RSI背离信号互补：背离看动量变化, 位置信号看绝对水平
        Returns: 'STRONG_LONG'|'LONG'|'STRONG_SHORT'|'SHORT'|'NEUTRAL'
        """
        if len(klines_1h) < 20:
            return 'NEUTRAL'
        closes = [float(k['close']) for k in klines_1h[-20:]]
        rsi_values = self._calc_rsi(closes, period=14)
        if not rsi_values:
            return 'NEUTRAL'
        current_rsi = rsi_values[-1]
        if current_rsi < 15:
            return 'STRONG_LONG'
        elif current_rsi < 25:
            return 'LONG'
        elif current_rsi > 85:
            return 'STRONG_SHORT'
        elif current_rsi > 75:
            return 'SHORT'
        return 'NEUTRAL'

    def _get_kdj_signal(self, symbol: str) -> str:
        """
        KDJ指标信号 (来自 technical_indicators_cache)
        KDJ的J值比RSI更敏感: J = 3K - 2D
        J < 0  : 极度超卖（超越正常边界，恐慌性抛售）→ STRONG_LONG
        J 0-20 : 超卖区域 → LONG
        J > 100: 极度超买（超越正常边界，疯狂追涨）→ STRONG_SHORT
        J 80-100: 超买区域 → SHORT
        数据来源: technical_indicators_cache (每小时更新, timeframe='1h')
        Returns: 'STRONG_LONG'|'LONG'|'STRONG_SHORT'|'SHORT'|'NEUTRAL'
        """
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT kdj_k, kdj_d, kdj_j, updated_at
                    FROM technical_indicators_cache
                    WHERE symbol = %s AND timeframe = '1h'
                    ORDER BY updated_at DESC
                    LIMIT 1
                """, (symbol,))
                row = cursor.fetchone()
            if not row or row['kdj_j'] is None:
                return 'NEUTRAL'
            from datetime import datetime, timedelta
            if row['updated_at'] and (datetime.now() - row['updated_at']) > timedelta(hours=3):
                return 'NEUTRAL'
            j = float(row['kdj_j'])
            if j < 0:
                return 'STRONG_LONG'
            elif j < 20:
                return 'LONG'
            elif j > 100:
                return 'STRONG_SHORT'
            elif j > 80:
                return 'SHORT'
            return 'NEUTRAL'
        except Exception as e:
            logger.debug(f"KDJ signal failed for {symbol}: {e}")
            return 'NEUTRAL'

    def _get_bb_signal(self, symbol: str, current_price: float) -> str:
        """
        Bollinger Band位置信号 (来自 technical_indicators_cache)
        价格突破下轨: 超卖区域, 反转做多机会
        价格突破上轨: 超买区域, 反转做空机会
        数据来源: technical_indicators_cache (1h, 每小时更新)
        Returns: 'BELOW_LOWER'|'NEAR_LOWER'|'ABOVE_UPPER'|'NEAR_UPPER'|'NEUTRAL'
        """
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT bb_upper, bb_lower, updated_at
                    FROM technical_indicators_cache
                    WHERE symbol = %s AND timeframe = '1h'
                    ORDER BY updated_at DESC
                    LIMIT 1
                """, (symbol,))
                row = cursor.fetchone()
            if not row or row['bb_upper'] is None or row['bb_lower'] is None:
                return 'NEUTRAL'
            from datetime import datetime, timedelta
            if row['updated_at'] and (datetime.now() - row['updated_at']) > timedelta(hours=3):
                return 'NEUTRAL'
            bb_upper = float(row['bb_upper'])
            bb_lower = float(row['bb_lower'])
            band_width = bb_upper - bb_lower
            if band_width <= 0:
                return 'NEUTRAL'
            bb_pct = (current_price - bb_lower) / band_width
            if bb_pct < 0:
                return 'BELOW_LOWER'
            elif bb_pct < 0.10:
                return 'NEAR_LOWER'
            elif bb_pct > 1.0:
                return 'ABOVE_UPPER'
            elif bb_pct > 0.90:
                return 'NEAR_UPPER'
            return 'NEUTRAL'
        except Exception as e:
            logger.debug(f"BB signal failed for {symbol}: {e}")
            return 'NEUTRAL'

    def _get_stoch_rsi_signal(self, klines_1h: list) -> str:
        """
        Stochastic RSI 超买超卖信号 (Section 37, 来自 klines_1h)
        StochRSI 是 RSI 的归一化版本，比单纯 RSI 更灵敏，能更早捕捉超买超卖转折。

        计算流程:
          1. 从 klines_1h 计算 14 周期 RSI (需要 15 根 K 线)
          2. 取最近 14 个 RSI 值，计算各自的 StochRSI:
             stoch = (rsi - min(rsi_14)) / (max(rsi_14) - min(rsi_14))
          3. %K = 当前 StochRSI（或 3 期 SMA 平滑）
          4. %K < 0.20 → 超卖区 → OVERSOLD (LONG 信号)
             %K > 0.80 → 超买区 → OVERBOUGHT (SHORT 信号)

        需要至少 28 根 K 线 (14 for RSI + 14 for stoch window)

        Returns: 'STRONG_OVERSOLD'|'OVERSOLD'|'STRONG_OVERBOUGHT'|'OVERBOUGHT'|'NEUTRAL'
        """
        try:
            if len(klines_1h) < 28:
                return 'NEUTRAL'
            closes = [float(k['close']) for k in klines_1h]

            # Step 1: 计算 14 周期 RSI 序列
            rsi_period = 14
            rsi_values = []
            for i in range(rsi_period, len(closes)):
                window = closes[i - rsi_period: i + 1]
                gains = [max(0.0, window[j] - window[j-1]) for j in range(1, len(window))]
                losses = [max(0.0, window[j-1] - window[j]) for j in range(1, len(window))]
                avg_gain = sum(gains) / rsi_period
                avg_loss = sum(losses) / rsi_period
                if avg_loss == 0:
                    rsi_values.append(100.0)
                else:
                    rs = avg_gain / avg_loss
                    rsi_values.append(100.0 - 100.0 / (1.0 + rs))

            # Step 2: 计算最近 14 个 RSI 的 StochRSI
            stoch_period = 14
            if len(rsi_values) < stoch_period:
                return 'NEUTRAL'
            recent_rsi = rsi_values[-stoch_period:]
            rsi_min = min(recent_rsi)
            rsi_max = max(recent_rsi)
            if rsi_max == rsi_min:
                return 'NEUTRAL'
            stoch_k = (recent_rsi[-1] - rsi_min) / (rsi_max - rsi_min)

            # Step 3: 判断信号
            if stoch_k < 0.10:
                return 'STRONG_OVERSOLD'     # 极度超卖
            if stoch_k < 0.20:
                return 'OVERSOLD'            # 超卖
            if stoch_k > 0.90:
                return 'STRONG_OVERBOUGHT'   # 极度超买
            if stoch_k > 0.80:
                return 'OVERBOUGHT'          # 超买
            return 'NEUTRAL'
        except Exception as e:
            logger.debug(f"StochRSI signal failed: {e}")
            return 'NEUTRAL'

    def _get_close_chain_signal(self, klines_1h: list) -> str:
        """
        连续收盘方向信号 (Section 41, 来自 klines_1h)
        最近 5 根 1H K 线中，如果收盘价连续 4 次向同一方向移动，
        说明趋势动量强且持续，具有较强的方向性。

        BEAR_CHAIN: 连续4次收盘价下行 (closes[i] < closes[i-1] for i in last 4 pairs)
        BULL_CHAIN: 连续4次收盘价上行

        选择性约 10-15%（随机情况下，4连跌/涨概率≈(0.5^4)×16 ≈ 12.5%）
        需要与其他方向性信号配合，否则反转时可能已经过了最佳入场点

        Returns: 'BEAR_CHAIN'|'BULL_CHAIN'|'NEUTRAL'
        """
        try:
            if len(klines_1h) < 6:
                return 'NEUTRAL'
            closes = [float(k['close']) for k in klines_1h[-6:]]
            # 最后5根K线，形成4对相邻比较
            all_down = all(closes[i] < closes[i-1] for i in range(1, 5))
            all_up   = all(closes[i] > closes[i-1] for i in range(1, 5))
            if all_down:
                return 'BEAR_CHAIN'
            if all_up:
                return 'BULL_CHAIN'
            return 'NEUTRAL'
        except Exception as e:
            logger.debug(f"Close chain signal failed: {e}")
            return 'NEUTRAL'

    def _get_bb_squeeze_signal(self, klines_1h: list) -> str:
        """
        S43: BB 压缩后释放信号 (Bollinger Band Squeeze Release)
        原理：BB宽度（(upper-lower)/middle）相对最近20根K线均值收窄>=50%（压缩）
              最近1根K线BB宽度 >= 压缩期均值 * 1.3（释放），且有方向（价格在均线哪侧）

        Returns: 'BULL_SQUEEZE_RELEASE'|'BEAR_SQUEEZE_RELEASE'|'NEUTRAL'
        """
        try:
            if len(klines_1h) < 30:
                return 'NEUTRAL'
            period = 20
            # 计算每根K线的BB宽度
            bb_widths = []
            for i in range(period, len(klines_1h)):
                window = [float(k['close']) for k in klines_1h[i - period: i + 1]]
                mid = sum(window) / len(window)
                std = (sum((x - mid) ** 2 for x in window) / len(window)) ** 0.5
                width = (2 * std) / mid if mid > 0 else 0.0
                bb_widths.append(width)

            if len(bb_widths) < 10:
                return 'NEUTRAL'

            # 最近3根宽度均值作为"当前状态"，前7根均值作为"基准"
            recent_widths = bb_widths[-3:]
            base_widths = bb_widths[-10:-3]
            recent_avg = sum(recent_widths) / len(recent_widths)
            base_avg = sum(base_widths) / len(base_widths)
            if base_avg <= 0:
                return 'NEUTRAL'

            squeeze_ratio = recent_avg / base_avg
            # 触发条件：曾经压缩（ratio<0.6），且最新一根开始扩张
            latest_width = bb_widths[-1]
            prev_widths = bb_widths[-5:-1]
            min_recent = min(prev_widths) if prev_widths else base_avg

            if min_recent < base_avg * 0.6 and latest_width > min_recent * 1.3:
                # 释放方向：价格在中线哪侧
                current_price = float(klines_1h[-1]['close'])
                window_close = [float(k['close']) for k in klines_1h[-period:]]
                mid_now = sum(window_close) / len(window_close)
                if current_price > mid_now:
                    return 'BULL_SQUEEZE_RELEASE'
                else:
                    return 'BEAR_SQUEEZE_RELEASE'
            return 'NEUTRAL'
        except Exception as e:
            logger.debug(f"BB squeeze signal failed: {e}")
            return 'NEUTRAL'

    def _get_ema_distance_signal(self, symbol: str, current_price: float) -> str:
        """
        S44: EMA 距离拉伸信号
        价格相对短期 EMA（ema_short，通常20期）的偏离度。
        偏离过大意味着短期超涨/超跌，均值回归概率高。
        使用 technical_indicators_cache 中的 ema_short。

        Returns: 'OVER_EXTENDED_BULL'|'OVER_EXTENDED_BEAR'|'NEUTRAL'
        """
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT ema_short, updated_at
                    FROM technical_indicators_cache
                    WHERE symbol = %s
                    ORDER BY updated_at DESC LIMIT 1
                """, (symbol,))
                row = cursor.fetchone()
            if not row or not row['ema_short']:
                return 'NEUTRAL'
            if row['updated_at'] and (datetime.now() - row['updated_at']).total_seconds() > 7200:
                return 'NEUTRAL'
            ema = float(row['ema_short'])
            if ema <= 0 or current_price <= 0:
                return 'NEUTRAL'
            deviation_pct = (current_price - ema) / ema * 100.0
            # 超过 7% 偏离视为过度拉伸
            if deviation_pct > 7.0:
                return 'OVER_EXTENDED_BULL'   # 价格远高于EMA，空头回归
            if deviation_pct < -7.0:
                return 'OVER_EXTENDED_BEAR'   # 价格远低于EMA，多头回归
            return 'NEUTRAL'
        except Exception as e:
            logger.debug(f"EMA distance signal failed for {symbol}: {e}")
            return 'NEUTRAL'

    def _get_order_flow_spike_signal(self, klines_15m: list) -> str:
        """
        S45: 订单流突刺信号 (5M taker_buy 聚合 → 15M维度)
        使用 klines_15m 的 taker_buy_base_volume / volume 比值，检测最近一根K线是否有
        异常方向性资金涌入（taker_buy比率相比最近12根K线的均值突增/突减）。

        逻辑：taker_buy_ratio = taker_buy_base_volume / volume
          ratio > avg + 1.5*std 且 ratio > 0.65  → BUY_SPIKE（主动买盘突刺）
          ratio < avg - 1.5*std 且 ratio < 0.35  → SELL_SPIKE（主动卖盘突刺）

        Returns: 'BUY_SPIKE'|'SELL_SPIKE'|'NEUTRAL'
        """
        try:
            if len(klines_15m) < 13:
                return 'NEUTRAL'
            ratios = []
            for k in klines_15m:
                vol = float(k.get('volume') or 0)
                tbv = k.get('taker_buy_base_volume')
                if tbv is None or vol <= 0:
                    continue
                ratios.append(float(tbv) / vol)
            if len(ratios) < 6:
                return 'NEUTRAL'
            # 用倒数第二根作为"当前"（最新一根可能未完成）
            historical = ratios[:-1]
            current_ratio = ratios[-2] if len(ratios) >= 2 else ratios[-1]
            avg = sum(historical) / len(historical)
            variance = sum((r - avg) ** 2 for r in historical) / len(historical)
            std = variance ** 0.5
            if std == 0:
                return 'NEUTRAL'
            if current_ratio > avg + 1.5 * std and current_ratio > 0.65:
                return 'BUY_SPIKE'
            if current_ratio < avg - 1.5 * std and current_ratio < 0.35:
                return 'SELL_SPIKE'
            return 'NEUTRAL'
        except Exception as e:
            logger.debug(f"Order flow spike signal failed: {e}")
            return 'NEUTRAL'

    def _get_vwap_deviation_signal(self, klines_1h: list) -> str:
        """
        24H VWAP 偏离信号 (Section 39, 来自 klines_1h)
        VWAP (Volume-Weighted Average Price) = sum(typical_price × volume) / sum(volume)
        typical_price = (high + low + close) / 3

        价格相对 24H VWAP 的偏离度反映了超买/超卖程度:
          偏离 > +5% → 价格明显超出 VWAP → 空头收缩机会 (SHORT 信号)
          偏离 < -5% → 价格明显低于 VWAP → 多头回归机会 (LONG 信号)
          偏离 +3~5% → 轻度超买 → SHORT 辅助
          偏离 -3~-5% → 轻度超卖 → LONG 辅助

        Returns: 'STRONG_ABOVE'|'ABOVE'|'STRONG_BELOW'|'BELOW'|'NEUTRAL'
        """
        try:
            if len(klines_1h) < 24:
                return 'NEUTRAL'
            # 使用最近 24 根 1H K 线计算 VWAP
            recent = klines_1h[-24:]
            total_vol = sum(float(k['volume']) for k in recent)
            if total_vol <= 0:
                return 'NEUTRAL'
            vwap = sum(
                (float(k['high']) + float(k['low']) + float(k['close'])) / 3.0 * float(k['volume'])
                for k in recent
            ) / total_vol
            current_price = float(klines_1h[-1]['close'])
            deviation_pct = (current_price - vwap) / vwap * 100.0
            if deviation_pct > 5.0:
                return 'STRONG_ABOVE'   # 明显超买 → SHORT
            if deviation_pct > 3.0:
                return 'ABOVE'          # 轻度超买 → SHORT (辅助)
            if deviation_pct < -5.0:
                return 'STRONG_BELOW'   # 明显超卖 → LONG
            if deviation_pct < -3.0:
                return 'BELOW'          # 轻度超卖 → LONG (辅助)
            return 'NEUTRAL'
        except Exception as e:
            logger.debug(f"VWAP deviation signal failed: {e}")
            return 'NEUTRAL'

    def _get_mtf_candle_resonance_signal(self, klines_1h: list, klines_15m: list) -> str:
        """
        多周期K线反转共振信号 (Section 38)
        1H 出现反转形态（Hammer/Shooting Star/吞噬），同时 15M 同向确认反转已经启动。
        两个时间框架共振 → 信号可靠性比单一周期高。

        判断流程:
          1. 检测 1H 当前 K 线是否为 Hammer / Shooting Star / 多头吞噬 / 空头吞噬
          2. 检测最近 3 根 15M K 线中同向确认数（阳/阴线）
          3. 同向确认 >= 2 根 → 输出 BULL/BEAR 共振信号

        Returns: 'BULL_RESONANCE'|'BEAR_RESONANCE'|'NEUTRAL'
        """
        try:
            if len(klines_1h) < 5 or len(klines_15m) < 6:
                return 'NEUTRAL'

            # Step 1: 检测 1H 反转形态方向
            h1_dir = 'NEUTRAL'
            last = klines_1h[-1]
            h = float(last['high']); l = float(last['low'])
            o = float(last['open']); c = float(last['close'])
            candle_range = h - l
            if candle_range > 0:
                body = abs(c - o)
                upper_wick = h - max(c, o)
                lower_wick = min(c, o) - l
                prev3 = klines_1h[-4:-1]
                bearish_prev = sum(1 for k in prev3 if float(k['close']) < float(k['open']))
                bullish_prev = sum(1 for k in prev3 if float(k['close']) > float(k['open']))
                # Hammer → 潜在 BULL
                if (lower_wick >= 2 * body
                        and lower_wick >= 1.5 * (upper_wick + 1e-9)
                        and body <= candle_range * 0.40
                        and bearish_prev >= 2):
                    h1_dir = 'BULL'
                # Shooting Star → 潜在 BEAR
                elif (upper_wick >= 2 * body
                        and upper_wick >= 1.5 * (lower_wick + 1e-9)
                        and body <= candle_range * 0.40
                        and bullish_prev >= 2):
                    h1_dir = 'BEAR'
                # 多头吞噬 → BULL
                elif len(klines_1h) >= 3:
                    prev = klines_1h[-2]
                    prev_o = float(prev['open']); prev_c = float(prev['close'])
                    if (c > o and prev_c < prev_o  # 当前阳线, 前根阴线
                            and o < prev_c and c > prev_o  # 完全吞噬
                            and body > abs(prev_c - prev_o) * 1.1  # 大10%以上
                            and bearish_prev >= 2):
                        h1_dir = 'BULL'
                    elif (c < o and prev_c > prev_o  # 当前阴线, 前根阳线
                            and o > prev_c and c < prev_o  # 完全吞噬
                            and body > abs(prev_c - prev_o) * 1.1
                            and bullish_prev >= 2):
                        h1_dir = 'BEAR'

            if h1_dir == 'NEUTRAL':
                return 'NEUTRAL'

            # Step 2: 用最近 3 根 15M K 线确认同向
            recent_15m = klines_15m[-3:]
            if h1_dir == 'BULL':
                bull_confirm = sum(1 for k in recent_15m if float(k['close']) > float(k['open']))
                if bull_confirm >= 2:
                    return 'BULL_RESONANCE'
            elif h1_dir == 'BEAR':
                bear_confirm = sum(1 for k in recent_15m if float(k['close']) < float(k['open']))
                if bear_confirm >= 2:
                    return 'BEAR_RESONANCE'
            return 'NEUTRAL'
        except Exception as e:
            logger.debug(f"MTF candle resonance signal failed: {e}")
            return 'NEUTRAL'

    def _get_range_breakout_signal(self, klines_1h: list) -> str:
        """
        价格区间突破+量能确认信号 (Section 36, 来自 klines_1h)
        突破近期 N 小时高低点，且成交量显著放大，排除假突破

        BULL_BREAKOUT: 当前收盘价 > 12H高点*1.005 AND 当前量 > 均量*1.5
        BEAR_BREAKOUT: 当前收盘价 < 12H低点*0.995 AND 当前量 > 均量*1.5

        - 与 position_24h_high/low 的区别: 后者是"处于区间边缘", 本信号是"已突破区间"
        - 量能放大确认: 过滤低量假突破，真实突破需要资金介入

        Returns: 'BULL_BREAKOUT'|'BEAR_BREAKOUT'|'NEUTRAL'
        """
        try:
            if len(klines_1h) < 14:
                return 'NEUTRAL'
            recent = klines_1h[-13:-1]   # 前12根（排除当前K线）
            curr = klines_1h[-1]
            range_high = max(k['high'] for k in recent)
            range_low = min(k['low'] for k in recent)
            avg_vol = sum(k['volume'] for k in recent) / len(recent)
            curr_vol = float(curr['volume'])
            curr_close = float(curr['close'])
            if avg_vol <= 0:
                return 'NEUTRAL'
            vol_ratio = curr_vol / avg_vol
            # 价格突破上轨 + 量能放大 → 多头突破
            if curr_close > range_high * 1.005 and vol_ratio >= 1.5:
                return 'BULL_BREAKOUT'
            # 价格跌破下轨 + 量能放大 → 空头突破
            if curr_close < range_low * 0.995 and vol_ratio >= 1.5:
                return 'BEAR_BREAKOUT'
            return 'NEUTRAL'
        except Exception as e:
            logger.debug(f"Range breakout signal failed: {e}")
            return 'NEUTRAL'

    def _get_kdj_mtf_signal(self, symbol: str) -> str:
        """
        KDJ_J 多周期共振信号 (Section 34)
        单独的1h KDJ在Section 9中已处理; 本信号要求1h AND 15m同时处于极端区域
        以提高信号可靠性（两个时间框架共同确认）

        J < 0  (极度超卖) 在1h, 且15m KDJ_J < 20 → EXTREME_BULL
        J < 10 在1h AND J < 15 在15m → OVERSOLD_MTF
        J > 100 (极度超买) 在1h, 且15m KDJ_J > 80 → EXTREME_BEAR
        J > 90  在1h AND J > 85 在15m → OVERBOUGHT_MTF

        数据来源: technical_indicators_cache (timeframe='1h'/'15m', 每小时更新)
        Returns: 'EXTREME_BULL'|'OVERSOLD_MTF'|'EXTREME_BEAR'|'OVERBOUGHT_MTF'|'NEUTRAL'
        """
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT timeframe, kdj_j, updated_at
                    FROM technical_indicators_cache
                    WHERE symbol = %s AND timeframe IN ('1h', '15m')
                    AND updated_at >= DATE_SUB(NOW(), INTERVAL 3 HOUR)
                    ORDER BY timeframe, updated_at DESC
                """, (symbol,))
                rows = cursor.fetchall()
            if not rows:
                return 'NEUTRAL'
            j_by_tf = {}
            for row in rows:
                tf = row['timeframe']
                if tf not in j_by_tf and row['kdj_j'] is not None:
                    j_by_tf[tf] = float(row['kdj_j'])
            j_1h = j_by_tf.get('1h')
            j_15m = j_by_tf.get('15m')
            if j_1h is None or j_15m is None:
                return 'NEUTRAL'
            # Extreme oversold: 1h J < 0 AND 15m J < 20
            if j_1h < 0 and j_15m < 20:
                return 'EXTREME_BULL'
            # Oversold MTF: 1h J < 10 AND 15m J < 15
            if j_1h < 10 and j_15m < 15:
                return 'OVERSOLD_MTF'
            # Extreme overbought: 1h J > 100 AND 15m J > 80
            if j_1h > 100 and j_15m > 80:
                return 'EXTREME_BEAR'
            # Overbought MTF: 1h J > 90 AND 15m J > 85
            if j_1h > 90 and j_15m > 85:
                return 'OVERBOUGHT_MTF'
            return 'NEUTRAL'
        except Exception as e:
            logger.debug(f"KDJ MTF signal failed for {symbol}: {e}")
            return 'NEUTRAL'

    def _get_macd_mtf_signal(self, symbol: str) -> str:
        """
        MACD 多周期方向共振信号 (Section 35)
        与Section 19(MACD零轴交叉)不同, 本信号持续检测1h和4h MACD柱状图方向
        两个时间框架方向一致代表更可靠的趋势确认（非交叉事件, 而是持续方向）

        1h histogram > 0 AND 4h histogram > 0 → BULL (双周期多头动量一致)
        1h histogram < 0 AND 4h histogram < 0 → BEAR (双周期空头动量一致)
        histogram > 0 意味着MACD在信号线上方, 动量为多头方向

        数据来源: technical_indicators_cache (timeframe='1h'/'4h', 每小时更新)
        Returns: 'BULL'|'BEAR'|'NEUTRAL'
        """
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT timeframe, macd_histogram, updated_at
                    FROM technical_indicators_cache
                    WHERE symbol = %s AND timeframe IN ('1h', '4h')
                    AND updated_at >= DATE_SUB(NOW(), INTERVAL 5 HOUR)
                    ORDER BY timeframe, updated_at DESC
                """, (symbol,))
                rows = cursor.fetchall()
            if not rows:
                return 'NEUTRAL'
            hist_by_tf = {}
            for row in rows:
                tf = row['timeframe']
                if tf not in hist_by_tf and row['macd_histogram'] is not None:
                    hist_by_tf[tf] = float(row['macd_histogram'])
            h_1h = hist_by_tf.get('1h')
            h_4h = hist_by_tf.get('4h')
            if h_1h is None or h_4h is None:
                return 'NEUTRAL'
            if h_1h > 0 and h_4h > 0:
                return 'BULL'
            if h_1h < 0 and h_4h < 0:
                return 'BEAR'
            return 'NEUTRAL'
        except Exception as e:
            logger.debug(f"MACD MTF signal failed for {symbol}: {e}")
            return 'NEUTRAL'

    def _get_vol_strength_signal(self, symbol: str) -> str:
        """
        成交量强度不对称信号 (来自 technical_signals_cache)
        即使多头K线数量更多, 若空头单根K线均量 > 多头3倍, 说明空头力量压倒性更强
        数据来源: technical_signals_cache (24h/1h窗口, 每小时更新)
        Returns: 'BULL_DOMINANCE'|'BEAR_DOMINANCE'|'NEUTRAL'
        """
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT avg_bullish_strength, avg_bearish_strength, bullish_pct, updated_at
                    FROM technical_signals_cache
                    WHERE symbol = %s AND window_label = '24h' AND timeframe = '1h'
                    ORDER BY updated_at DESC
                    LIMIT 1
                """, (symbol,))
                row = cursor.fetchone()
            if not row:
                return 'NEUTRAL'
            from datetime import datetime, timedelta
            if row['updated_at'] and (datetime.now() - row['updated_at']) > timedelta(hours=3):
                return 'NEUTRAL'
            bull_str = float(row['avg_bullish_strength'] or 0)
            bear_str = float(row['avg_bearish_strength'] or 0)
            bull_pct = float(row['bullish_pct'] or 50)
            if bull_str <= 0 or bear_str <= 0:
                return 'NEUTRAL'
            bear_ratio = bear_str / bull_str
            bull_ratio = bull_str / bear_str
            # 空头单根均量 >= 3倍多头, 且非明显多头市场 -> 空头力量主导
            if bear_ratio >= 3.0 and bull_pct < 60:
                return 'BEAR_DOMINANCE'
            # 多头单根均量 >= 3倍空头, 且非明显空头市场 -> 多头力量主导
            if bull_ratio >= 3.0 and bull_pct > 40:
                return 'BULL_DOMINANCE'
            return 'NEUTRAL'
        except Exception as e:
            logger.debug(f"Vol strength signal failed for {symbol}: {e}")
            return 'NEUTRAL'

    def _get_vol_4h_signal(self, symbol: str) -> str:
        """
        4H短周期成交量强度信号 (来自 technical_signals_cache 4h/15m窗口)
        比24h/1h更敏感, 反映近4小时内的多空力量对比
        Returns: 'BEAR_DOMINANCE'|'BULL_DOMINANCE'|'NEUTRAL'
        """
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT avg_bullish_strength, avg_bearish_strength, bullish_pct, updated_at
                    FROM technical_signals_cache
                    WHERE symbol = %s AND window_label = '4h' AND timeframe = '15m'
                    ORDER BY updated_at DESC
                    LIMIT 1
                """, (symbol,))
                row = cursor.fetchone()
            if not row:
                return 'NEUTRAL'
            from datetime import datetime, timedelta
            if row['updated_at'] and (datetime.now() - row['updated_at']) > timedelta(hours=3):
                return 'NEUTRAL'
            bull_str = float(row['avg_bullish_strength'] or 0)
            bear_str = float(row['avg_bearish_strength'] or 0)
            bull_pct = float(row['bullish_pct'] or 50)
            if bull_str <= 0 or bear_str <= 0:
                return 'NEUTRAL'
            bear_ratio = bear_str / bull_str
            bull_ratio = bull_str / bear_str
            # 空头4H均量 >= 2.5倍多头 (比24h信号阈值略低, 因为4H样本量更少)
            if bear_ratio >= 2.5 and bull_pct < 65:
                return 'BEAR_DOMINANCE'
            if bull_ratio >= 2.5 and bull_pct > 35:
                return 'BULL_DOMINANCE'
            return 'NEUTRAL'
        except Exception as e:
            logger.debug(f"Vol 4h signal failed for {symbol}: {e}")
            return 'NEUTRAL'

    def _get_micro_trend_signal(self, symbol: str) -> str:
        """
        1H微观趋势动量信号 (Section 40, 来自 technical_signals_cache 1h/5m窗口)
        使用最近 1H 的 5M 数据（12根K线）判断极短期多空力量。
        比 Section 25（4H/15M）时效性更强，适合捕捉近 1 小时内的方向性资金动向。

        条件:
          bear_ratio >= 2.0 AND bullish_pct < 45  → BEAR_DOMINANCE → micro_trend_bear
          bull_ratio >= 2.0 AND bullish_pct > 55  → BULL_DOMINANCE → micro_trend_bull

        阈值选 2.0x（比 Section 25 的 2.5x 低，因 1H/5M 样本量更少、噪声更大）

        Returns: 'BEAR_DOMINANCE'|'BULL_DOMINANCE'|'NEUTRAL'
        """
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT avg_bullish_strength, avg_bearish_strength, bullish_pct, updated_at
                    FROM technical_signals_cache
                    WHERE symbol = %s AND window_label = '1h' AND timeframe = '5m'
                    ORDER BY updated_at DESC
                    LIMIT 1
                """, (symbol,))
                row = cursor.fetchone()
            if not row:
                return 'NEUTRAL'
            from datetime import datetime, timedelta
            if row['updated_at'] and (datetime.now() - row['updated_at']) > timedelta(hours=2):
                return 'NEUTRAL'
            bull_str = float(row['avg_bullish_strength'] or 0)
            bear_str = float(row['avg_bearish_strength'] or 0)
            bull_pct = float(row['bullish_pct'] or 50)
            if bull_str <= 0 or bear_str <= 0:
                return 'NEUTRAL'
            bear_ratio = bear_str / bull_str
            bull_ratio = bull_str / bear_str
            if bear_ratio >= 2.0 and bull_pct < 45:
                return 'BEAR_DOMINANCE'
            if bull_ratio >= 2.0 and bull_pct > 55:
                return 'BULL_DOMINANCE'
            return 'NEUTRAL'
        except Exception as e:
            logger.debug(f"Micro trend signal failed for {symbol}: {e}")
            return 'NEUTRAL'

    # -----------------------------------------------------------------------
    # OI 数据采集 + 信号
    # -----------------------------------------------------------------------

    def _collect_oi_for_symbol(self, symbol: str) -> bool:
        """
        采集单个交易对当前 Open Interest，写入 open_interest_history。
        Binance FAPI: GET /fapi/v1/openInterest?symbol=BTCUSDT
        symbol 格式: BTC/USDT -> BTCUSDT
        """
        try:
            binance_symbol = symbol.replace('/', '')
            url = f"https://fapi.binance.com/fapi/v1/openInterest?symbol={binance_symbol}"
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = _json.loads(resp.read())
            oi_value = float(data.get('openInterest', 0))
            if oi_value <= 0:
                return False

            conn = self._get_connection()
            with conn.cursor() as cursor:
                # 计算15分钟变化率（和上一条比）
                cursor.execute("""
                    SELECT open_interest FROM open_interest_history
                    WHERE symbol = %s
                    ORDER BY collected_at DESC LIMIT 1
                """, (symbol,))
                prev = cursor.fetchone()
                oi_change_15m = None
                if prev and float(prev['open_interest']) > 0:
                    oi_change_15m = round((oi_value - float(prev['open_interest'])) / float(prev['open_interest']) * 100, 4)

                cursor.execute("""
                    INSERT INTO open_interest_history (symbol, open_interest, oi_change_15m, collected_at)
                    VALUES (%s, %s, %s, NOW())
                """, (symbol, oi_value, oi_change_15m))
            conn.commit()
            return True
        except Exception as e:
            logger.debug(f"OI collect failed for {symbol}: {e}")
            return False

    def _get_oi_signal(self, symbol: str) -> str:
        """
        S42: Open Interest 变化率信号
        查最近1小时内的OI记录，计算累计变化率。
        OI大幅增加 + 价格上涨 = 多头加仓（看涨）
        OI大幅增加 + 价格下跌 = 空头加仓（看空）
        OI大幅减少 = 平仓，趋势可能反转

        Returns: 'LONG_BUILD'|'SHORT_BUILD'|'LIQUIDATION_BULL'|'LIQUIDATION_BEAR'|'NEUTRAL'
        """
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT open_interest, oi_change_15m, collected_at
                    FROM open_interest_history
                    WHERE symbol = %s
                      AND collected_at >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
                    ORDER BY collected_at ASC
                """, (symbol,))
                rows = cursor.fetchall()
            if len(rows) < 2:
                return 'NEUTRAL'

            first_oi = float(rows[0]['open_interest'])
            last_oi = float(rows[-1]['open_interest'])
            if first_oi <= 0:
                return 'NEUTRAL'
            total_change_pct = (last_oi - first_oi) / first_oi * 100

            if total_change_pct >= 3.0:
                return 'OI_SURGE'      # OI大幅增加，结合价格判断
            if total_change_pct <= -3.0:
                return 'OI_DROP'       # OI大幅减少，持仓平仓
            return 'NEUTRAL'
        except Exception as e:
            logger.debug(f"OI signal failed for {symbol}: {e}")
            return 'NEUTRAL'

    def _run_oi_collection_round(self, symbols: list):
        """采集所有交易对的OI，每15分钟调用一次（从主循环调度）"""
        ok = 0
        for sym in symbols:
            if self._collect_oi_for_symbol(sym):
                ok += 1
        logger.debug(f"OI collection: {ok}/{len(symbols)} symbols updated")

    def _get_momentum_accel_signal(self, klines_1h: list) -> str:
        """
        价格动量加速信号 (来自 klines_1h)
        对比最近2H vs 前2H的价格变化率, 判断动量是否在加速
        Returns: 'ACCEL_DOWN'|'ACCEL_UP'|'DECEL_DOWN'|'NEUTRAL'
        - ACCEL_DOWN: 最近2H跌幅 > 前2H跌幅 * 1.5 → 空头加速
        - ACCEL_UP:   最近2H涨幅 > 前2H涨幅 * 1.5 → 多头加速
        - DECEL_DOWN: 前2H有较大跌幅, 最近2H明显减速 → 潜在反弹
        """
        try:
            if len(klines_1h) < 5:
                return 'NEUTRAL'
            # 最近2根K线的价格变化
            recent_close = klines_1h[-1]['close']
            two_h_ago_close = klines_1h[-3]['close']  # 2H前
            four_h_ago_close = klines_1h[-5]['close']  # 4H前
            if two_h_ago_close <= 0 or four_h_ago_close <= 0:
                return 'NEUTRAL'
            recent_chg = (recent_close - two_h_ago_close) / two_h_ago_close * 100
            prior_chg = (two_h_ago_close - four_h_ago_close) / four_h_ago_close * 100
            # 两段变化量都太小则忽略
            if abs(recent_chg) < 0.3 and abs(prior_chg) < 0.3:
                return 'NEUTRAL'
            # 空头加速: 两段都在跌, 且最近跌幅更大
            if recent_chg < -0.5 and prior_chg < -0.3 and recent_chg < prior_chg * 1.5:
                return 'ACCEL_DOWN'
            # 多头加速: 两段都在涨, 且最近涨幅更大
            if recent_chg > 0.5 and prior_chg > 0.3 and recent_chg > prior_chg * 1.5:
                return 'ACCEL_UP'
            # 空头减速: 前段有明显下跌, 最近明显减速 → 可能反弹
            if prior_chg < -1.0 and recent_chg > prior_chg * 0.3:
                return 'DECEL_DOWN'
            return 'NEUTRAL'
        except Exception as e:
            logger.debug(f"Momentum accel signal failed: {e}")
            return 'NEUTRAL'

    def _get_candle_quality_signal(self, klines_1h: list) -> str:
        """
        K线实体质量信号: 最近6根1H蜡烛实体占比 + 方向
        高实体占比(>0.65)说明趋势方向明确, 无大影线表示无阻力
        Returns: 'STRONG_BEAR'|'STRONG_BULL'|'NEUTRAL'
        """
        try:
            if len(klines_1h) < 6:
                return 'NEUTRAL'
            recent = klines_1h[-6:]
            bear_quality = []
            bull_quality = []
            for k in recent:
                high = k.get('high', 0)
                low = k.get('low', 0)
                open_p = k.get('open', 0)
                close = k.get('close', 0)
                total_range = high - low
                if total_range <= 0:
                    continue
                body = abs(close - open_p)
                body_ratio = body / total_range
                if close < open_p:  # 阴线
                    bear_quality.append(body_ratio)
                else:  # 阳线
                    bull_quality.append(body_ratio)
            # 需要至少4根同向K线且平均实体占比高
            if len(bear_quality) >= 4:
                avg_bear_quality = sum(bear_quality) / len(bear_quality)
                if avg_bear_quality >= 0.55:
                    return 'STRONG_BEAR'
            if len(bull_quality) >= 4:
                avg_bull_quality = sum(bull_quality) / len(bull_quality)
                if avg_bull_quality >= 0.55:
                    return 'STRONG_BULL'
            return 'NEUTRAL'
        except Exception as e:
            logger.debug(f"Candle quality signal failed: {e}")
            return 'NEUTRAL'

    def _get_funding_trend_signal(self, symbol: str) -> str:
        """
        资金费率趋势信号 (来自 funding_rate_stats 的 trend 列)
        基于费率趋势和市场情绪，识别多头过热或空头拥挤机会。
        与 Section 12 的 _get_funding_rate_signal (极端值检测) 互补，
        本方法利用 trend/market_sentiment 字段，门槛更低。
        Returns:
          'OVERHEATED'       - 多头过热(正费率>0.05%), SHORT候选
          'BULLISH'          - 轻度多头偏向(正费率>0.02%), SHORT候选
          'STRONGLY_BEARISH' - 空头过度拥挤(负费率<-0.05%), LONG候选(挤空)
          'BEARISH'          - 轻度空头偏向(负费率<-0.03%), LONG候选
          'NEUTRAL'          - 无明显信号
        """
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT current_rate_pct, trend, updated_at
                    FROM funding_rate_stats
                    WHERE symbol = %s
                    LIMIT 1
                """, (symbol,))
                row = cursor.fetchone()
            if not row:
                return 'NEUTRAL'
            from datetime import datetime, timedelta
            if row.get('updated_at') and (datetime.now() - row['updated_at']) > timedelta(hours=4):
                return 'NEUTRAL'
            rate = float(row.get('current_rate_pct') or 0)
            trend = row.get('trend', 'neutral') or 'neutral'
            # 多头过热: 正费率高, 做多成本大, 潜在空头机会
            if trend in ('strongly_bullish',) and rate >= 0.05:
                return 'OVERHEATED'
            if trend in ('bullish', 'strongly_bullish') and rate >= 0.02:
                return 'BULLISH'
            # 空头过度: 负费率深, 做空成本大, 潜在挤空机会
            if trend in ('strongly_bearish',) and rate <= -0.05:
                return 'STRONGLY_BEARISH'
            if trend in ('bearish', 'strongly_bearish') and rate <= -0.03:
                return 'BEARISH'
            return 'NEUTRAL'
        except Exception as e:
            logger.debug(f"Funding trend signal failed for {symbol}: {e}")
            return 'NEUTRAL'

    def _get_adx_signal(self, klines_1h: list) -> str:
        """
        ADX趋势强度信号 (从 klines_1h 实时计算, 14周期)
        ADX衡量趋势强度, +DI/-DI判断方向。
        Returns:
          'STRONG_BEAR'  - ADX>25 且 -DI>+DI 且 -DI>25 (强下降趋势)
          'STRONG_BULL'  - ADX>25 且 +DI>-DI 且 +DI>25 (强上升趋势)
          'WEAK_TREND'   - ADX<20 (震荡市，趋势信号不可靠)
          'NEUTRAL'      - 其他
        """
        try:
            if len(klines_1h) < 20:
                return 'NEUTRAL'
            period = 14
            needed = period * 2 + 1
            klines = klines_1h[-min(needed, len(klines_1h)):]
            if len(klines) < period + 1:
                return 'NEUTRAL'
            tr_list, plus_dm, minus_dm = [], [], []
            for i in range(1, len(klines)):
                h = klines[i]['high']
                l = klines[i]['low']
                prev_c = klines[i-1]['close']
                prev_h = klines[i-1]['high']
                prev_l = klines[i-1]['low']
                tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
                tr_list.append(tr)
                up_move = h - prev_h
                down_move = prev_l - l
                pdm = up_move if up_move > down_move and up_move > 0 else 0
                ndm = down_move if down_move > up_move and down_move > 0 else 0
                plus_dm.append(pdm)
                minus_dm.append(ndm)
            # Wilder smoothing
            def wilder_smooth(data, n):
                if len(data) < n:
                    return []
                result = [sum(data[:n])]
                for i in range(n, len(data)):
                    result.append(result[-1] - result[-1] / n + data[i])
                return result
            atr_s = wilder_smooth(tr_list, period)
            pdi_s = wilder_smooth(plus_dm, period)
            ndi_s = wilder_smooth(minus_dm, period)
            if not atr_s:
                return 'NEUTRAL'
            dx_list = []
            for i in range(len(atr_s)):
                if atr_s[i] <= 0:
                    continue
                pdi = 100 * pdi_s[i] / atr_s[i]
                ndi = 100 * ndi_s[i] / atr_s[i]
                di_sum = pdi + ndi
                if di_sum <= 0:
                    continue
                dx = 100 * abs(pdi - ndi) / di_sum
                dx_list.append((dx, pdi, ndi))
            if len(dx_list) < period:
                return 'NEUTRAL'
            adx = sum(d[0] for d in dx_list[-period:]) / period
            last_pdi = dx_list[-1][1]
            last_ndi = dx_list[-1][2]
            if adx > 25 and last_ndi > last_pdi and last_ndi > 25:
                return 'STRONG_BEAR'
            if adx > 25 and last_pdi > last_ndi and last_pdi > 25:
                return 'STRONG_BULL'
            if adx < 20:
                return 'WEAK_TREND'
            return 'NEUTRAL'
        except Exception as e:
            logger.debug(f"ADX signal failed: {e}")
            return 'NEUTRAL'

    def _get_volume_divergence_signal(self, klines_1h: list) -> str:
        """
        量价背离信号 (来自 klines_1h)
        价格创新低但成交量萎缩 → 空头动能减弱 → 潜在LONG
        价格创新高但成交量萎缩 → 多头动能减弱 → 潜在SHORT
        Returns: 'VOL_DIVERGE_BULL'|'VOL_DIVERGE_BEAR'|'NEUTRAL'
        """
        try:
            if len(klines_1h) < 10:
                return 'NEUTRAL'
            recent = klines_1h[-6:]
            earlier = klines_1h[-10:-4]
            recent_close_min = min(k['close'] for k in recent)
            earlier_close_min = min(k['close'] for k in earlier)
            recent_close_max = max(k['close'] for k in recent)
            earlier_close_max = max(k['close'] for k in earlier)
            recent_vol_avg = sum(k['volume'] for k in recent) / len(recent)
            earlier_vol_avg = sum(k['volume'] for k in earlier) / len(earlier)
            if earlier_vol_avg <= 0:
                return 'NEUTRAL'
            vol_ratio = recent_vol_avg / earlier_vol_avg
            # 价格创新低但量能萎缩超过20%: 空头乏力, LONG机会
            if recent_close_min < earlier_close_min * 0.995 and vol_ratio < 0.80:
                return 'VOL_DIVERGE_BULL'
            # 价格创新高但量能萎缩超过20%: 多头乏力, SHORT机会
            if recent_close_max > earlier_close_max * 1.005 and vol_ratio < 0.80:
                return 'VOL_DIVERGE_BEAR'
            return 'NEUTRAL'
        except Exception as e:
            logger.debug(f"Volume divergence signal failed: {e}")
            return 'NEUTRAL'

    def _get_candle_reversal_signal(self, klines_1h: list) -> str:
        """
        K线反转形态信号 (来自 klines_1h)
        Hammer(锤线): 下影线长 >= 2倍实体 + 前3根偏空 → 空头衰竭, LONG机会
        Shooting Star(射击之星): 上影线长 >= 2倍实体 + 前3根偏多 → 多头衰竭, SHORT机会
        适用于已有主方向信号的辅助确认, 权重较低(辅助性)
        Returns: 'HAMMER'|'SHOOTING_STAR'|'NEUTRAL'
        """
        try:
            if len(klines_1h) < 5:
                return 'NEUTRAL'
            last = klines_1h[-1]
            h = float(last['high'])
            l = float(last['low'])
            o = float(last['open'])
            c = float(last['close'])
            candle_range = h - l
            if candle_range <= 0:
                return 'NEUTRAL'
            body = abs(c - o)
            upper_wick = h - max(c, o)
            lower_wick = min(c, o) - l
            # Hammer: 下影线 >= 2×实体, 下影线 >= 1.5×上影线, 实体不超过总range的40%
            if (lower_wick >= 2 * body
                    and lower_wick >= 1.5 * (upper_wick + 1e-9)
                    and body <= candle_range * 0.40):
                # 前3根至少2根是阴线 (之前处于下跌趋势才有反转意义)
                prev3 = klines_1h[-4:-1]
                bearish_prev = sum(1 for k in prev3 if k['close'] < k['open'])
                if bearish_prev >= 2:
                    return 'HAMMER'
            # Shooting Star: 上影线 >= 2×实体, 上影线 >= 1.5×下影线, 实体不超过总range的40%
            if (upper_wick >= 2 * body
                    and upper_wick >= 1.5 * (lower_wick + 1e-9)
                    and body <= candle_range * 0.40):
                # 前3根至少2根是阳线 (之前处于上涨趋势才有反转意义)
                prev3 = klines_1h[-4:-1]
                bullish_prev = sum(1 for k in prev3 if k['close'] > k['open'])
                if bullish_prev >= 2:
                    return 'SHOOTING_STAR'
            return 'NEUTRAL'
        except Exception as e:
            logger.debug(f"Candle reversal signal failed: {e}")
            return 'NEUTRAL'

    def _get_price_structure_signal(self, klines_1h: list) -> str:
        """
        价格结构信号 (来自 klines_1h)
        连续更高的低点 (Higher Lows): 下跌趋势内多头防守逐步上移, 空头压力减弱 → LONG
        连续更低的高点 (Lower Highs): 上涨趋势内空头反扑逐步下移, 多头推力减弱 → SHORT
        使用 Low 和 High 价格, 不依赖收盘方向
        Returns: 'HIGHER_LOWS'|'LOWER_HIGHS'|'NEUTRAL'
        """
        try:
            if len(klines_1h) < 6:
                return 'NEUTRAL'
            recent = klines_1h[-5:]  # 最近5根K线
            lows = [float(k['low']) for k in recent]
            highs = [float(k['high']) for k in recent]
            # 连续更高的低点: 至少4对连续满足条件 (5根K线中4对相邻对)
            higher_lows = sum(1 for i in range(1, len(lows)) if lows[i] > lows[i-1])
            lower_highs = sum(1 for i in range(1, len(highs)) if highs[i] < highs[i-1])
            if higher_lows >= 4:
                return 'HIGHER_LOWS'
            if lower_highs >= 4:
                return 'LOWER_HIGHS'
            return 'NEUTRAL'
        except Exception as e:
            logger.debug(f"Price structure signal failed: {e}")
            return 'NEUTRAL'

    def _get_engulfing_signal(self, klines_1h: list) -> str:
        """
        吞噬形态信号 (Engulfing Pattern, 来自 klines_1h)
        多头吞噬: 当前阳线实体完全包住前一根阴线实体, 且前3根偏空 → 强力反转信号 LONG
        空头吞噬: 当前阴线实体完全包住前一根阳线实体, 且前3根偏多 → 强力反转信号 SHORT
        比锤线/射击之星更可靠的反转确认形态
        Returns: 'BULL_ENGULF'|'BEAR_ENGULF'|'NEUTRAL'
        """
        try:
            if len(klines_1h) < 5:
                return 'NEUTRAL'
            curr = klines_1h[-1]
            prev = klines_1h[-2]
            curr_body_low = min(float(curr['close']), float(curr['open']))
            curr_body_high = max(float(curr['close']), float(curr['open']))
            prev_body_low = min(float(prev['close']), float(prev['open']))
            prev_body_high = max(float(prev['close']), float(prev['open']))
            prev_body_size = prev_body_high - prev_body_low
            curr_body_size = curr_body_high - curr_body_low
            # 实体太小则无意义 (doji不算)
            if prev_body_size < 1e-9 or curr_body_size < 1e-9:
                return 'NEUTRAL'
            prev3 = klines_1h[-4:-1]
            # 多头吞噬: 当前为阳线, 实体完全包住前根阴线实体
            if (float(curr['close']) > float(curr['open'])  # 当前阳线
                    and float(prev['close']) < float(prev['open'])  # 前根阴线
                    and curr_body_low <= prev_body_low
                    and curr_body_high >= prev_body_high
                    and curr_body_size >= prev_body_size * 1.1):  # 当前实体至少比前根大10%
                bearish_prev = sum(1 for k in prev3 if k['close'] < k['open'])
                if bearish_prev >= 2:
                    return 'BULL_ENGULF'
            # 空头吞噬: 当前为阴线, 实体完全包住前根阳线实体
            if (float(curr['close']) < float(curr['open'])  # 当前阴线
                    and float(prev['close']) > float(prev['open'])  # 前根阳线
                    and curr_body_low <= prev_body_low
                    and curr_body_high >= prev_body_high
                    and curr_body_size >= prev_body_size * 1.1):  # 当前实体至少比前根大10%
                bullish_prev = sum(1 for k in prev3 if k['close'] > k['open'])
                if bullish_prev >= 2:
                    return 'BEAR_ENGULF'
            return 'NEUTRAL'
        except Exception as e:
            logger.debug(f"Engulfing signal failed: {e}")
            return 'NEUTRAL'

    def _get_rsi_mtf_signal(self, symbol: str) -> str:
        """
        多周期RSI共振信号 (来自 technical_indicators_cache)
        1h和15m RSI同时超卖/超买, 共振确认, 信号强度更高
        数据来源: technical_indicators_cache (每小时更新)
        Returns: 'STRONG_LONG'|'STRONG_SHORT'|'LONG'|'SHORT'|'NEUTRAL'
        """
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT timeframe, rsi_value, updated_at
                    FROM technical_indicators_cache
                    WHERE symbol = %s AND timeframe IN ('1h', '15m')
                    AND rsi_value IS NOT NULL
                    ORDER BY updated_at DESC
                    LIMIT 4
                """, (symbol,))
                rows = cursor.fetchall()
            if not rows:
                return 'NEUTRAL'
            from datetime import datetime, timedelta
            rsi_map = {}
            for r in rows:
                tf = r['timeframe']
                if tf not in rsi_map:
                    if r['updated_at'] and (datetime.now() - r['updated_at']) <= timedelta(hours=3):
                        rsi_map[tf] = float(r['rsi_value'])
            rsi_1h  = rsi_map.get('1h')
            rsi_15m = rsi_map.get('15m')
            if rsi_1h is None or rsi_15m is None:
                return 'NEUTRAL'
            # 双周期同时超卖: 强多头共振
            if rsi_1h < 25 and rsi_15m < 25:
                return 'STRONG_LONG'
            # 双周期同时超买: 强空头共振
            if rsi_1h > 75 and rsi_15m > 75:
                return 'STRONG_SHORT'
            # 单周期超卖
            if rsi_1h < 30 and rsi_15m < 35:
                return 'LONG'
            if rsi_1h < 35 and rsi_15m < 30:
                return 'LONG'
            # 单周期超买
            if rsi_1h > 70 and rsi_15m > 65:
                return 'SHORT'
            if rsi_1h > 65 and rsi_15m > 70:
                return 'SHORT'
            return 'NEUTRAL'
        except Exception as e:
            logger.debug(f"RSI MTF signal failed for {symbol}: {e}")
            return 'NEUTRAL'

    def _get_relative_strength_signal(self, symbol: str) -> str:
        """
        相对强弱信号 (vs BTC)
        币种24H涨跌幅与BTC比较: 弱于BTC→空头; 强于BTC→多头
        极端弱势（跌幅远超BTC）: STRONG_SHORT
        极端强势（涨幅远超BTC）: STRONG_LONG
        数据来源: price_stats_24h (每15分钟更新)
        Returns: 'VERY_WEAK'|'WEAK'|'STRONG'|'VERY_STRONG'|'NEUTRAL'
        """
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT p.change_24h as coin_change,
                           b.change_24h as btc_change
                    FROM price_stats_24h p
                    JOIN price_stats_24h b ON b.symbol = 'BTC/USDT'
                    WHERE p.symbol = %s
                    AND p.updated_at > DATE_SUB(NOW(), INTERVAL 2 HOUR)
                    LIMIT 1
                """, (symbol,))
                row = cursor.fetchone()
            if not row or row['btc_change'] is None or row['coin_change'] is None:
                return 'NEUTRAL'
            diff = float(row['coin_change']) - float(row['btc_change'])
            if diff <= -10:
                return 'VERY_WEAK'
            elif diff <= -5:
                return 'WEAK'
            elif diff >= 10:
                return 'VERY_STRONG'
            elif diff >= 5:
                return 'STRONG'
            return 'NEUTRAL'
        except Exception as e:
            logger.debug(f"RS signal failed for {symbol}: {e}")
            return 'NEUTRAL'

    def _get_ema_mtf_signal(self, symbol: str) -> str:
        """
        多周期EMA趋势共振信号 (来自 technical_indicators_cache)
        15m + 1h + 4h EMA全部同向: 强趋势共振信号
        三周期全部向上: 极少见,强多头 (牛市中逆势强势股)
        三周期全部向下: 熊市中普遍,中等空头确认
        数据来源: technical_indicators_cache (每小时更新)
        Returns: 'TRIPLE_BULL'|'TRIPLE_BEAR'|'PARTIAL_BULL'|'PARTIAL_BEAR'|'NEUTRAL'
        """
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT timeframe, ema_trend, updated_at
                    FROM technical_indicators_cache
                    WHERE symbol = %s AND timeframe IN ('15m', '1h', '4h')
                    AND updated_at > DATE_SUB(NOW(), INTERVAL 3 HOUR)
                    ORDER BY updated_at DESC
                    LIMIT 6
                """, (symbol,))
                rows = cursor.fetchall()
            if not rows:
                return 'NEUTRAL'
            ema_map = {}
            for r in rows:
                tf = r['timeframe']
                if tf not in ema_map and r['ema_trend']:
                    ema_map[tf] = r['ema_trend']
            t15m = ema_map.get('15m')
            t1h  = ema_map.get('1h')
            t4h  = ema_map.get('4h')
            if None in (t15m, t1h, t4h):
                return 'NEUTRAL'
            if t15m == 'up' and t1h == 'up' and t4h == 'up':
                return 'TRIPLE_BULL'
            if t15m == 'down' and t1h == 'down' and t4h == 'down':
                return 'TRIPLE_BEAR'
            # 两个周期对齐
            if t1h == 'up' and t4h == 'up':
                return 'PARTIAL_BULL'
            if t1h == 'down' and t4h == 'down':
                return 'PARTIAL_BEAR'
            return 'NEUTRAL'
        except Exception as e:
            logger.debug(f"EMA MTF signal failed for {symbol}: {e}")
            return 'NEUTRAL'

    def _get_funding_rate_signal(self, symbol: str) -> tuple:
        """
        读取资金费率，返回信号方向及强度
        Returns: (signal, rate_pct)
          signal: 'STRONG_LONG'|'LONG'|'STRONG_SHORT'|'SHORT'|'NEUTRAL'
          rate_pct: 当前费率（百分比，如 -0.22）
        - 负费率极端 → 空头挤爆预警 → LONG 机会
        - 正费率极端 → 多头过热预警 → SHORT 机会
        """
        try:
            conn = self._get_connection()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT current_rate_pct, updated_at
                    FROM funding_rate_stats
                    WHERE symbol = %s
                    LIMIT 1
                """, (symbol,))
                row = cursor.fetchone()

            if not row or row['current_rate_pct'] is None:
                return ('NEUTRAL', 0.0)

            from datetime import datetime, timedelta
            if row['updated_at'] and (datetime.now() - row['updated_at']) > timedelta(hours=2):
                return ('NEUTRAL', 0.0)

            rate_pct = float(row['current_rate_pct'])
            # 极端负费率（空头在付钱）→ LONG 做多机会
            if rate_pct <= -0.15:
                return ('STRONG_LONG', rate_pct)
            elif rate_pct <= -0.08:
                return ('LONG', rate_pct)
            # 极端正费率（多头在付钱）→ SHORT 做空机会
            elif rate_pct >= 0.15:
                return ('STRONG_SHORT', rate_pct)
            elif rate_pct >= 0.08:
                return ('SHORT', rate_pct)
            else:
                return ('NEUTRAL', rate_pct)
        except Exception as e:
            logger.debug(f"Funding rate signal query failed for {symbol}: {e}")
            return ('NEUTRAL', 0.0)

    def check_anti_fomo_filter(self, symbol: str, current_price: float, side: str) -> tuple:
        """
        防追高/防杀跌过滤器（V5.2重新启用 - 2026-03-22）

        做多防追高: 不在24H区间80%以上位置开多
        做空防杀跌: 不在24H区间20%以下位置开空

        返回: (是否通过, 原因)
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # 检查24H价格位置
            cursor.execute("""
                SELECT high_24h, low_24h, change_24h
                FROM price_stats_24h
                WHERE symbol = %s
            """, (symbol,))

            stats_24h = cursor.fetchone()
            cursor.close()

            if not stats_24h:
                return True, "无24H数据,跳过过滤"

            high_24h = float(stats_24h['high_24h'])
            low_24h = float(stats_24h['low_24h'])
            change_24h = float(stats_24h['change_24h'] or 0)

            # 计算价格在24H区间的位置百分比
            if high_24h > low_24h:
                position_pct = (current_price - low_24h) / (high_24h - low_24h) * 100
            else:
                position_pct = 50  # 无波动时默认中间位置

            # 做多防追高: 不在高于80%位置开多
            if side == 'LONG' and position_pct > 80:
                return False, f"防追高-价格位于24H区间{position_pct:.1f}%位置,距最高仅{(high_24h-current_price)/current_price*100:.2f}%"

            # 做空防杀跌: 不在低于20%位置开空
            if side == 'SHORT' and position_pct < 20:
                return False, f"防杀跌-价格位于24H区间{position_pct:.1f}%位置,距最低仅{(current_price-low_24h)/current_price*100:.2f}%"

            # 额外检查: 24H大涨且在高位 → 更严格
            if side == 'LONG' and change_24h > 15 and position_pct > 70:
                return False, f"防追高-24H涨{change_24h:+.2f}%且位于{position_pct:.1f}%高位"

            # 额外检查: 24H大跌且在低位 → 更严格
            if side == 'SHORT' and change_24h < -15 and position_pct < 30:
                return False, f"防杀跌-24H跌{change_24h:+.2f}%且位于{position_pct:.1f}%低位"

            return True, f"位置{position_pct:.1f}%,24H{change_24h:+.2f}%"

        except Exception as e:
            logger.error(f"防追高检查失败 {symbol}: {e}")
            return True, "检查失败,放行"

    def load_klines(self, symbol: str, timeframe: str, limit: int = 100):
        conn = self._get_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        query = """
            SELECT open_price as open, high_price as high,
                   low_price as low, close_price as close,
                   volume, taker_buy_base_volume
            FROM kline_data
            WHERE symbol = %s AND timeframe = %s AND exchange = 'binance_futures'
            AND open_time >= UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL 60 DAY)) * 1000
            ORDER BY open_time DESC LIMIT %s
        """
        cursor.execute(query, (symbol, timeframe, limit))
        klines = list(cursor.fetchall())
        cursor.close()

        klines.reverse()
        for k in klines:
            k['open'] = float(k['open'])
            k['high'] = float(k['high'])
            k['low'] = float(k['low'])
            k['close'] = float(k['close'])
            k['volume'] = float(k['volume'])
            if k.get('taker_buy_base_volume') is not None:
                k['taker_buy_base_volume'] = float(k['taker_buy_base_volume'])

        return klines

    def analyze(self, symbol: str, big4_result: dict = None):
        """分析并决策 - 支持做多和做空 (主要使用1小时K线)

        Args:
            symbol: 交易对
            big4_result: Big4趋势结果 (由SmartTraderService传入)
        """
        if symbol not in self.whitelist:
            return None

        try:
            klines_1d = self.load_klines(symbol, '1d', 50)
            klines_1h = self.load_klines(symbol, '1h', 100)
            klines_15m = self.load_klines(symbol, '15m', 96)  # 24小时的15分钟K线

            if len(klines_1d) < 30 or len(klines_1h) < 72 or len(klines_15m) < 48:  # 至少需要72小时(3天)数据
                return None

            current = klines_1h[-1]['close']

            # 分别计算做多和做空得分
            long_score = 0
            short_score = 0

            # 记录信号组成 (用于后续性能分析)
            signal_components = {}

            # ========== 1小时K线分析 (主要) ==========

            # 1. 位置评分 - 使用100小时(4天+)高低点（修复：原72H在强趋势中误判，改为用全部100H数据）
            _pos_candles = klines_1h[-100:]  # 最多100H，避免强趋势持续压低/抬高position_pct
            high_100h = max(k['high'] for k in _pos_candles)
            low_100h = min(k['low'] for k in _pos_candles)

            if high_100h == low_100h:
                position_pct = 50
            else:
                position_pct = (current - low_100h) / (high_100h - low_100h) * 100

            # 提前计算1H量能（在位置判断之前）
            volumes_1h = [k['volume'] for k in klines_1h[-24:]]
            avg_volume_1h = sum(volumes_1h) / len(volumes_1h) if volumes_1h else 1

            strong_bull_1h = 0  # 有力量的阳线
            strong_bear_1h = 0  # 有力量的阴线

            for k in klines_1h[-24:]:
                is_bull = k['close'] > k['open']
                is_high_volume = k['volume'] > avg_volume_1h * 1.5  # 成交量 > 1.5倍平均量（修复：原1.2倍噪声过多）

                if is_bull and is_high_volume:
                    strong_bull_1h += 1
                elif not is_bull and is_high_volume:
                    strong_bear_1h += 1

            net_power_1h = strong_bull_1h - strong_bear_1h

            # 低位做多，高位做空 (但要检查量能,避免在破位时做多)
            if position_pct < 30:
                # 检查是否有强空头量能 (破位信号)
                # 如果有强空头量能,不做多 (避免破位时抄底)
                if net_power_1h > -2:  # 没有强空头量能,可以考虑做多
                    weight = self.scoring_weights.get('position_low', {'long': 20, 'short': 0})
                    long_score += weight['long']
                    if weight['long'] > 0:
                        signal_components['position_low'] = weight['long']
            elif position_pct > 70:
                # 🔥 修复：当Big4强力看多时（牛市），高位是正常状态，不产生做空信号
                # 避免牛市中position_high持续触发空头与V2多头冲突导致全部信号被拒绝
                big4_bullish = (big4_result and
                                big4_result.get('overall_signal') in ('BULLISH', 'STRONG_BULLISH') and
                                big4_result.get('signal_strength', 0) >= 50)
                if not big4_bullish:
                    weight = self.scoring_weights.get('position_high', {'long': 0, 'short': 20})
                    short_score += weight['short']
                    if weight['short'] > 0:
                        signal_components['position_high'] = weight['short']
            else:
                # 修复：position_mid不再双向加分（无方向意义），改为只加给当前量能主导方向
                # 如果双方量能相当（差值<2），则不加分（中性行情，无倾向）
                weight = self.scoring_weights.get('position_mid', {'long': 5, 'short': 5})
                if net_power_1h >= 2:  # 量能偏多，加给多头
                    long_score += weight['long']
                    if weight['long'] > 0:
                        signal_components['position_mid'] = weight['long']
                elif net_power_1h <= -2:  # 量能偏空，加给空头
                    short_score += weight['short']
                    if weight['short'] > 0:
                        signal_components['position_mid'] = weight['short']
                # 否则不加分（净力量不明显，中性不提供有效信息）

            # 2. 短期动量 - 最近24小时涨幅
            gain_24h = (current - klines_1h[-24]['close']) / klines_1h[-24]['close'] * 100
            if gain_24h < -3:  # 24小时跌超过3% - 看跌信号,应该做空
                weight = self.scoring_weights.get('momentum_down_3pct', {'long': 0, 'short': 15})  # 修复: 下跌应该增加SHORT评分
                short_score += weight['short']  # 修复: 改为增加short_score
                if weight['short'] > 0:
                    signal_components['momentum_down_3pct'] = weight['short']
            elif gain_24h > 3:  # 24小时涨超过3% - 看涨信号,应该做多
                weight = self.scoring_weights.get('momentum_up_3pct', {'long': 15, 'short': 0})  # 修复: 上涨应该增加LONG评分
                long_score += weight['long']  # 修复: 改为增加long_score
                if weight['long'] > 0:
                    signal_components['momentum_up_3pct'] = weight['long']

            # 3. 1小时趋势评分 - 最近24根K线(1天)
            bullish_1h = sum(1 for k in klines_1h[-24:] if k['close'] > k['open'])
            bearish_1h = 24 - bullish_1h

            if bullish_1h >= 15:  # 阳线>=15根(62.5%) — 牛市顺势，不需过严
                weight = self.scoring_weights.get('trend_1h_bull', {'long': 20, 'short': 0})
                long_score += weight['long']
                if weight['long'] > 0:
                    signal_components['trend_1h_bull'] = weight['long']
            elif bearish_1h >= 14:  # 阴线>=14根(58.3%)，明确空头趋势
                weight = self.scoring_weights.get('trend_1h_bear', {'long': 0, 'short': 20})
                short_score += weight['short']
                if weight['short'] > 0:
                    signal_components['trend_1h_bear'] = weight['short']

            # 4. 波动率评分 - 最近24小时
            recent_24h = klines_1h[-24:]
            volatility = (max(k['high'] for k in recent_24h) - min(k['low'] for k in recent_24h)) / current * 100

            # 高波动率更适合交易
            if volatility > 5:  # 波动超过5%
                weight = self.scoring_weights.get('volatility_high', {'long': 10, 'short': 10})
                if long_score > short_score:
                    long_score += weight['long']
                    if weight['long'] > 0:
                        signal_components['volatility_high'] = weight['long']
                else:
                    short_score += weight['short']
                    if weight['short'] > 0:
                        signal_components['volatility_high'] = weight['short']

            # 5. 连续趋势强化信号 - 最近10根1小时K线
            recent_10h = klines_1h[-10:]
            bullish_10h = sum(1 for k in recent_10h if k['close'] > k['open'])
            bearish_10h = 10 - bullish_10h

            # 计算最近10小时涨跌幅
            gain_10h = (current - recent_10h[0]['close']) / recent_10h[0]['close'] * 100

            # 连续阳线且上涨幅度适中(不在顶部) - 强做多信号
            if bullish_10h >= 7 and gain_10h < 5 and position_pct < 70:
                weight = self.scoring_weights.get('consecutive_bull', {'long': 15, 'short': 0})
                long_score += weight['long']
                if weight['long'] > 0:
                    signal_components['consecutive_bull'] = weight['long']

            # 连续阴线 - 趋势空头信号（去除position_pct>30限制：低位连阴=趋势延续，非反弹保护）
            elif bearish_10h >= 7 and gain_10h > -10:
                weight = self.scoring_weights.get('consecutive_bear', {'long': 0, 'short': 15})
                short_score += weight['short']
                if weight['short'] > 0:
                    signal_components['consecutive_bear'] = weight['short']

            # ========== 量能加权K线分析 (核心趋势判断) ==========

            # 6. 1小时K线量能分析已在前面计算（提前用于位置判断）

            # 7. 15分钟K线量能分析 - 最近24根(6小时)
            volumes_15m = [k['volume'] for k in klines_15m[-24:]]
            avg_volume_15m = sum(volumes_15m) / len(volumes_15m) if volumes_15m else 1

            strong_bull_15m = 0
            strong_bear_15m = 0

            for k in klines_15m[-24:]:
                is_bull = k['close'] > k['open']
                is_high_volume = k['volume'] > avg_volume_15m * 1.5  # 修复：与1H统一使用1.5倍

                if is_bull and is_high_volume:
                    strong_bull_15m += 1
                elif not is_bull and is_high_volume:
                    strong_bear_15m += 1

            net_power_15m = strong_bull_15m - strong_bear_15m

            # 量能多头信号: 1H和15M都显示强力多头
            if net_power_1h >= 2 and net_power_15m >= 2:
                weight = self.scoring_weights.get('volume_power_bull', {'long': 25, 'short': 0})
                long_score += weight['long']
                if weight['long'] > 0:
                    signal_components['volume_power_bull'] = weight['long']
                    logger.info(f"{symbol} 量能多头强势: 1H净力量={net_power_1h}, 15M净力量={net_power_15m}")

            # 量能空头信号: 1H和15M都显示强力空头
            elif net_power_1h <= -2 and net_power_15m <= -2:
                weight = self.scoring_weights.get('volume_power_bear', {'long': 0, 'short': 25})
                short_score += weight['short']
                if weight['short'] > 0:
                    signal_components['volume_power_bear'] = weight['short']
                    logger.info(f"{symbol} 量能空头强势: 1H净力量={net_power_1h}, 15M净力量={net_power_15m}")

            # 单一时间框架量能信号 (辅助)
            elif net_power_1h >= 3:  # 仅1H强力多头
                weight = self.scoring_weights.get('volume_power_1h_bull', {'long': 15, 'short': 0})
                long_score += weight['long']
                if weight['long'] > 0:
                    signal_components['volume_power_1h_bull'] = weight['long']
            elif net_power_1h <= -3:  # 仅1H强力空头
                weight = self.scoring_weights.get('volume_power_1h_bear', {'long': 0, 'short': 15})
                short_score += weight['short']
                if weight['short'] > 0:
                    signal_components['volume_power_1h_bear'] = weight['short']

            # 11. 24H位置评分（仿币本位短窗口，对当日走势更敏感）
            high_24h_pos = max(k['high'] for k in klines_1h[-24:])
            low_24h_pos  = min(k['low']  for k in klines_1h[-24:])
            if high_24h_pos != low_24h_pos:
                position_24h_pct = (current - low_24h_pos) / (high_24h_pos - low_24h_pos) * 100
            else:
                position_24h_pct = 50.0

            if position_24h_pct < 30:
                weight = self.scoring_weights.get('position_24h_low', {'long': 12, 'short': 0})
                if weight['long'] > 0 and net_power_1h > -2:  # 有强空头量能时不抄底
                    long_score += weight['long']
                    signal_components['position_24h_low'] = weight['long']
            elif position_24h_pct > 70:
                _big4_strong_bull_24h = (big4_result and
                    big4_result.get('overall_signal') in ('BULLISH', 'STRONG_BULLISH') and
                    big4_result.get('signal_strength', 0) >= 50)
                if not _big4_strong_bull_24h:
                    weight = self.scoring_weights.get('position_24h_high', {'long': 0, 'short': 12})
                    if weight['short'] > 0:
                        short_score += weight['short']
                        signal_components['position_24h_high'] = weight['short']

            # 12. 量能信号（1.2×阈值，仿币本位，更敏感，作为1.5×的补充层）
            strong_bull_1h_12x = sum(
                1 for k in klines_1h[-24:]
                if k['close'] > k['open'] and k['volume'] > avg_volume_1h * 1.2
            )
            strong_bear_1h_12x = sum(
                1 for k in klines_1h[-24:]
                if k['close'] < k['open'] and k['volume'] > avg_volume_1h * 1.2
            )
            net_power_1h_12x = strong_bull_1h_12x - strong_bear_1h_12x

            strong_bull_15m_12x = sum(
                1 for k in klines_15m[-24:]
                if k['close'] > k['open'] and k['volume'] > avg_volume_15m * 1.2
            )
            strong_bear_15m_12x = sum(
                1 for k in klines_15m[-24:]
                if k['close'] < k['open'] and k['volume'] > avg_volume_15m * 1.2
            )
            net_power_15m_12x = strong_bull_15m_12x - strong_bear_15m_12x

            if net_power_1h_12x >= 2 and net_power_15m_12x >= 2:
                weight = self.scoring_weights.get('volume_power_12x_bull', {'long': 15, 'short': 0})
                if weight['long'] > 0:
                    long_score += weight['long']
                    signal_components['volume_power_12x_bull'] = weight['long']
            elif net_power_1h_12x <= -2 and net_power_15m_12x <= -2:
                weight = self.scoring_weights.get('volume_power_12x_bear', {'long': 0, 'short': 15})
                if weight['short'] > 0:
                    short_score += weight['short']
                    signal_components['volume_power_12x_bear'] = weight['short']

            # 8. 高位突破追涨信号: position_high + 双重多头量能 + Big4非强空
            # 重启条件：原版无Big4过滤导致熊市追涨；现加 STRONG_BEARISH 拦截
            if position_pct > 70 and net_power_1h >= 2 and net_power_15m >= 2:
                big4_is_strong_bear = (big4_result and
                                       big4_result.get('overall_signal') == 'STRONG_BEARISH' and
                                       big4_result.get('signal_strength', 0) >= 50)
                if not big4_is_strong_bear:
                    weight = self.scoring_weights.get('breakout_long', {'long': 0, 'short': 0})
                    long_score += weight['long']
                    if weight['long'] > 0:
                        signal_components['breakout_long'] = weight['long']
                        logger.info(f"{symbol} 高位突破: position={position_pct:.1f}%, 1H净力量={net_power_1h}, 15M净力量={net_power_15m}")

            # 9. 破位追空信号: position_low + 强力量能空头 → 可以做空
            # 历史数据验证: 643笔订单, 55.8%胜率, $5736盈利 (最赚钱的信号之一)
            # 触发条件: 价格低位 + 1H和15M双重空头量能确认（修复：原条件逻辑冗余，实为只检查1H）
            if position_pct < 30 and net_power_1h <= -2 and net_power_15m <= -2:
                weight = self.scoring_weights.get('breakdown_short', {'long': 0, 'short': 20})
                short_score += weight['short']
                if weight['short'] > 0:
                    signal_components['breakdown_short'] = weight['short']
                    logger.info(f"{symbol} 破位追空: position={position_pct:.1f}%, 1H净力量={net_power_1h}, 15M净力量={net_power_15m}")

            # 10. Big4强力多头趋势延续信号
            # 当BTC/ETH/BNB/SOL全部强力看多（STRONG_BULLISH），且自身有上涨动量，
            # 给予趋势延续加分（顺势操作，防止牛市反复错过入场）
            if (big4_result and big4_result.get('overall_signal') == 'STRONG_BULLISH' and
                    big4_result.get('signal_strength', 0) >= 50 and
                    gain_24h > 2 and position_pct > 50):
                weight = self.scoring_weights.get('big4_strong_bull_cont', {'long': 15, 'short': 0})
                long_score += weight['long']
                if weight['long'] > 0:
                    signal_components['big4_strong_bull_cont'] = weight['long']
                    logger.info(f"{symbol} Big4强力牛市延续: signal_strength={big4_result.get('signal_strength',0):.0f}, gain_24h={gain_24h:.2f}%")

            # 11. 量价高潮信号 (volume_climax)
            # 原理: 连续阴线+量能递增 = 多杀多高潮，价格超卖，空头动能衰竭 → LONG
            #       连续阳线+量能递增 = 多头高潮，价格超买，买盘枯竭  → SHORT
            vc_signal = self._detect_volume_climax(klines_1h)
            if vc_signal == 'BULLISH_CLIMAX':
                weight = self.scoring_weights.get('volume_climax_bull', {'long': 20, 'short': 0})
                if weight['long'] > 0:
                    long_score += weight['long']
                    signal_components['volume_climax_bull'] = weight['long']
                    logger.info(f"{symbol} 空头量价高潮(LONG信号): 连续阴线+量递增")
            elif vc_signal == 'BEARISH_CLIMAX':
                weight = self.scoring_weights.get('volume_climax_bear', {'long': 0, 'short': 20})
                if weight['short'] > 0:
                    short_score += weight['short']
                    signal_components['volume_climax_bear'] = weight['short']
                    logger.info(f"{symbol} 多头量价高潮(SHORT信号): 连续阳线+量递增")

            # 12. Hyperliquid 鲸鱼资金流向信号 (whale_flow)
            # 数据来源: hyperliquid_symbol_aggregation (每小时更新)
            # 原理: Hyperliquid 上聪明钱的多空比和净流向 → 领先指标
            hl_signal, hl_ls_ratio, hl_net_flow, hl_vol = self._get_hl_whale_signal(symbol)
            if hl_signal == 'STRONG_BULLISH':
                weight = self.scoring_weights.get('whale_flow_long', {'long': 25, 'short': 0})
                if weight['long'] > 0:
                    long_score += weight['long']
                    signal_components['whale_flow_long'] = weight['long']
                    logger.info(f"{symbol} HL鲸鱼做多: signal={hl_signal} L/S={hl_ls_ratio:.2f} net={hl_net_flow:+.0f} vol={hl_vol:.0f}")
            elif hl_signal == 'BULLISH':
                weight = self.scoring_weights.get('whale_flow_long', {'long': 25, 'short': 0})
                pts = max(1, int(weight['long'] * 0.72))  # 72% weight for regular BULLISH
                if pts > 0:
                    long_score += pts
                    signal_components['whale_flow_long'] = pts
                    logger.info(f"{symbol} HL鲸鱼做多(弱): signal={hl_signal} L/S={hl_ls_ratio:.2f} net={hl_net_flow:+.0f} vol={hl_vol:.0f}")
            elif hl_signal == 'STRONG_BEARISH':
                weight = self.scoring_weights.get('whale_flow_short', {'long': 0, 'short': 25})
                if weight['short'] > 0:
                    short_score += weight['short']
                    signal_components['whale_flow_short'] = weight['short']
                    logger.info(f"{symbol} HL鲸鱼做空: signal={hl_signal} L/S={hl_ls_ratio:.2f} net={hl_net_flow:+.0f} vol={hl_vol:.0f}")
            elif hl_signal == 'BEARISH':
                weight = self.scoring_weights.get('whale_flow_short', {'long': 0, 'short': 25})
                pts = max(1, int(weight['short'] * 0.72))
                if pts > 0:
                    short_score += pts
                    signal_components['whale_flow_short'] = pts
                    logger.info(f"{symbol} HL鲸鱼做空(弱): signal={hl_signal} L/S={hl_ls_ratio:.2f} net={hl_net_flow:+.0f} vol={hl_vol:.0f}")

            # 12. 资金费率极端信号 (funding_rate_extreme)
            # 原理: 费率极负 = 空头在付钱 = 空头过拥挤 = LONG 反弹机会
            #       费率极正 = 多头在付钱 = 多头过拥挤 = SHORT 反转机会
            # 注: 与防追高/多空禁令过滤层独立；提供补充确认分
            fr_signal, fr_rate = self._get_funding_rate_signal(symbol)
            if fr_signal == 'STRONG_LONG':
                weight = self.scoring_weights.get('funding_rate_extreme_long', {'long': 22, 'short': 0})
                if weight['long'] > 0:
                    long_score += weight['long']
                    signal_components['funding_rate_extreme_long'] = weight['long']
                    logger.info(f"{symbol} 资金费率极负LONG: {fr_rate:.3f}%")
            elif fr_signal == 'LONG':
                weight = self.scoring_weights.get('funding_rate_extreme_long', {'long': 22, 'short': 0})
                pts = max(1, int(weight['long'] * 0.64))
                if pts > 0:
                    long_score += pts
                    signal_components['funding_rate_extreme_long'] = pts
                    logger.info(f"{symbol} 资金费率偏负LONG: {fr_rate:.3f}%")
            elif fr_signal == 'STRONG_SHORT':
                weight = self.scoring_weights.get('funding_rate_extreme_short', {'long': 0, 'short': 22})
                if weight['short'] > 0:
                    short_score += weight['short']
                    signal_components['funding_rate_extreme_short'] = weight['short']
                    logger.info(f"{symbol} 资金费率极正SHORT: {fr_rate:.3f}%")
            elif fr_signal == 'SHORT':
                weight = self.scoring_weights.get('funding_rate_extreme_short', {'long': 0, 'short': 22})
                pts = max(1, int(weight['short'] * 0.64))
                if pts > 0:
                    short_score += pts
                    signal_components['funding_rate_extreme_short'] = pts
                    logger.info(f"{symbol} 资金费率偏正SHORT: {fr_rate:.3f}%")

            # 14. 多时间框架共振信号 (mf_confluence)
            # 数据: technical_signals_cache (每小时更新，基于5m/15m/1h K线)
            # 原理: 1H[24h] + 15M[4h] 同方向 → 趋势可信度更高
            mf_signal = self._get_mf_confluence_signal(symbol)
            if mf_signal in ('BULLISH', 'STRONG_BULLISH'):
                weight = self.scoring_weights.get('mf_confluence_bull', {'long': 15, 'short': 0})
                pts = weight['long'] if mf_signal == 'STRONG_BULLISH' else max(1, int(weight['long'] * 0.8))
                if pts > 0:
                    long_score += pts
                    signal_components['mf_confluence_bull'] = pts
                    logger.debug(f"{symbol} 多时框共振多头: {mf_signal}")
            elif mf_signal in ('BEARISH', 'STRONG_BEARISH'):
                weight = self.scoring_weights.get('mf_confluence_bear', {'long': 0, 'short': 15})
                pts = weight['short'] if mf_signal == 'STRONG_BEARISH' else max(1, int(weight['short'] * 0.8))
                if pts > 0:
                    short_score += pts
                    signal_components['mf_confluence_bear'] = pts
                    logger.debug(f"{symbol} 多时框共振空头: {mf_signal}")

            # 15. RSI背离信号 (rsi_divergence)
            # 原理: 价格和RSI方向背离说明动量衰竭，预示趋势反转
            # 多头背离: 价格创新低但RSI底部抬高 → 卖压耗尽 → LONG
            # 空头背离: 价格创新高但RSI顶部降低 → 买力枯竭 → SHORT
            rsi_div_signal = self._detect_rsi_divergence(klines_1h)
            if rsi_div_signal == 'BULLISH_DIVERGENCE':
                weight = self.scoring_weights.get('rsi_divergence_bull', {'long': 18, 'short': 0})
                pts = weight['long']
                if pts > 0:
                    long_score += pts
                    signal_components['rsi_divergence_bull'] = pts
                    logger.debug(f"{symbol} RSI多头背离: 价格新低但RSI底部抬高")
            elif rsi_div_signal == 'BEARISH_DIVERGENCE':
                weight = self.scoring_weights.get('rsi_divergence_bear', {'long': 0, 'short': 18})
                pts = weight['short']
                if pts > 0:
                    short_score += pts
                    signal_components['rsi_divergence_bear'] = pts
                    logger.debug(f"{symbol} RSI空头背离: 价格新高但RSI顶部降低")

            # 16. RSI绝对位置信号 (rsi_level) - 超卖/超买
            # 原理: RSI极端水平代表价格偏离均衡的程度，历史均值回归倾向
            # 与RSI背离互补：背离=动量变化，位置=绝对水平
            rsi_level_signal = self._get_rsi_level_signal(klines_1h)
            if rsi_level_signal in ('STRONG_LONG', 'LONG'):
                weight = self.scoring_weights.get('rsi_level_bull', {'long': 20, 'short': 0})
                pts = weight['long'] if rsi_level_signal == 'STRONG_LONG' else max(1, int(weight['long'] * 0.7))
                if pts > 0:
                    long_score += pts
                    signal_components['rsi_level_bull'] = pts
                    logger.debug(f"{symbol} RSI超卖: {rsi_level_signal}")
            elif rsi_level_signal in ('STRONG_SHORT', 'SHORT'):
                weight = self.scoring_weights.get('rsi_level_bear', {'long': 0, 'short': 20})
                pts = weight['short'] if rsi_level_signal == 'STRONG_SHORT' else max(1, int(weight['short'] * 0.7))
                if pts > 0:
                    short_score += pts
                    signal_components['rsi_level_bear'] = pts
                    logger.debug(f"{symbol} RSI超买: {rsi_level_signal}")

            # 17. KDJ指标信号 (kdj) - 比RSI更敏感的超买超卖信号
            # 原理: KDJ J值 = 3K - 2D，可超越0-100边界，极端J值表示极度背离
            # J<0 → 价格超越均衡下限（恐慌性抛售），均值回归更确定 → LONG
            # J>100 → 价格超越均衡上限（贪婪追涨），回调更确定 → SHORT
            # 数据源: technical_indicators_cache (1H，每小时更新)
            kdj_signal = self._get_kdj_signal(symbol)
            if kdj_signal in ('STRONG_LONG', 'LONG'):
                weight = self.scoring_weights.get('kdj_bull', {'long': 22, 'short': 0})
                pts = weight['long'] if kdj_signal == 'STRONG_LONG' else max(1, int(weight['long'] * 0.65))
                if pts > 0:
                    long_score += pts
                    signal_components['kdj_bull'] = pts
                    logger.debug(f"{symbol} KDJ超卖: {kdj_signal}")
            elif kdj_signal in ('STRONG_SHORT', 'SHORT'):
                weight = self.scoring_weights.get('kdj_bear', {'long': 0, 'short': 22})
                pts = weight['short'] if kdj_signal == 'STRONG_SHORT' else max(1, int(weight['short'] * 0.65))
                if pts > 0:
                    short_score += pts
                    signal_components['kdj_bear'] = pts
                    logger.debug(f"{symbol} KDJ超买: {kdj_signal}")

            # 18. 主动买入比例信号 (taker_buy)
            # 原理: 主动买入量/总量反映市场参与者的攻击性方向
            # 吸筹: 前3根低买盘→最近2根买盘突增 = 大资金承接 → LONG
            # 施压: 连续4根极低买盘 = 空方持续主导 → SHORT
            taker_signal = self._detect_taker_buy_signal(klines_1h)
            if taker_signal == 'BULLISH_ABSORPTION':
                weight = self.scoring_weights.get('taker_buy_bull', {'long': 20, 'short': 0})
                pts = weight['long']
                if pts > 0:
                    long_score += pts
                    signal_components['taker_buy_bull'] = pts
                    logger.debug(f"{symbol} 主动买盘吸筹: 前低后高买盘比")
            elif taker_signal == 'BEARISH_PRESSURE':
                weight = self.scoring_weights.get('taker_buy_bear', {'long': 0, 'short': 20})
                pts = weight['short']
                if pts > 0:
                    short_score += pts
                    signal_components['taker_buy_bear'] = pts
                    logger.debug(f"{symbol} 主动卖盘施压: 持续低买盘比")

            # 19. MACD柱状图零轴交叉信号 (macd_cross)
            # 原理: MACD柱状图从负转正/正转负代表动量切换，是方向变化的领先指标
            # 全市场同时只有少数(3-10个)币种出现交叉，信号选择性极高
            macd_cross_signal = self._detect_macd_crossover(klines_1h)
            if macd_cross_signal == 'BULLISH_CROSS':
                weight = self.scoring_weights.get('macd_cross_bull', {'long': 25, 'short': 0})
                pts = weight['long']
                if pts > 0:
                    long_score += pts
                    signal_components['macd_cross_bull'] = pts
                    logger.debug(f"{symbol} MACD多头交叉: 柱状图由负转正")
            elif macd_cross_signal == 'BEARISH_CROSS':
                weight = self.scoring_weights.get('macd_cross_bear', {'long': 0, 'short': 25})
                pts = weight['short']
                if pts > 0:
                    short_score += pts
                    signal_components['macd_cross_bear'] = pts
                    logger.debug(f"{symbol} MACD空头交叉: 柱状图由正转负")

            # 20. Bollinger Band位置信号 (bb_band)
            # 原理: 价格突破BB下轨代表极度超卖（可能反弹）; 突破上轨代表极度超买（可能回调）
            # 数据来源: technical_indicators_cache (1h, 每小时更新)
            bb_signal = self._get_bb_signal(symbol, current)
            if bb_signal == 'BELOW_LOWER':
                weight = self.scoring_weights.get('bb_below_lower', {'long': 20, 'short': 0})
                pts = weight['long']
                if pts > 0:
                    long_score += pts
                    signal_components['bb_below_lower'] = pts
                    logger.debug(f"{symbol} BB下轨突破: 价格低于BB下轨,超卖区域")
            elif bb_signal == 'NEAR_LOWER':
                weight = self.scoring_weights.get('bb_near_lower', {'long': 10, 'short': 0})
                pts = weight['long']
                if pts > 0:
                    long_score += pts
                    signal_components['bb_near_lower'] = pts
                    logger.debug(f"{symbol} BB下轨附近: 价格临近BB下轨")
            elif bb_signal == 'ABOVE_UPPER':
                weight = self.scoring_weights.get('bb_above_upper', {'long': 0, 'short': 20})
                pts = weight['short']
                if pts > 0:
                    short_score += pts
                    signal_components['bb_above_upper'] = pts
                    logger.debug(f"{symbol} BB上轨突破: 价格高于BB上轨,超买区域")
            elif bb_signal == 'NEAR_UPPER':
                weight = self.scoring_weights.get('bb_near_upper', {'long': 0, 'short': 10})
                pts = weight['short']
                if pts > 0:
                    short_score += pts
                    signal_components['bb_near_upper'] = pts
                    logger.debug(f"{symbol} BB上轨附近: 价格临近BB上轨")

            # 21. 成交量强度不对称信号 (vol_strength)
            # 原理: 即使阳线数量多, 若空头单根均量>>多头, 说明实际力量偏空
            # 数据来源: technical_signals_cache (24h/1h, 每小时更新)
            vol_str_signal = self._get_vol_strength_signal(symbol)
            if vol_str_signal == 'BEAR_DOMINANCE':
                weight = self.scoring_weights.get('vol_strength_bear', {'long': 0, 'short': 15})
                pts = weight['short']
                if pts > 0:
                    short_score += pts
                    signal_components['vol_strength_bear'] = pts
                    logger.debug(f"{symbol} 成交量空头主导: 空头单根均量是多头的3倍以上")
            elif vol_str_signal == 'BULL_DOMINANCE':
                weight = self.scoring_weights.get('vol_strength_bull', {'long': 15, 'short': 0})
                pts = weight['long']
                if pts > 0:
                    long_score += pts
                    signal_components['vol_strength_bull'] = pts
                    logger.debug(f"{symbol} 成交量多头主导: 多头单根均量是空头的3倍以上")

            # 22. 多周期RSI共振信号 (rsi_mtf)
            # 原理: 1h和15m RSI同时超卖/超买, 比单周期信号更可靠
            # 数据来源: technical_indicators_cache (高精度计算, 每小时更新)
            rsi_mtf_signal = self._get_rsi_mtf_signal(symbol)
            if rsi_mtf_signal == 'STRONG_LONG':
                weight = self.scoring_weights.get('rsi_mtf_strong_bull', {'long': 25, 'short': 0})
                pts = weight['long']
                if pts > 0:
                    long_score += pts
                    signal_components['rsi_mtf_strong_bull'] = pts
                    logger.debug(f"{symbol} RSI双周期超卖共振: 1h+15m RSI均<25")
            elif rsi_mtf_signal == 'STRONG_SHORT':
                weight = self.scoring_weights.get('rsi_mtf_strong_bear', {'long': 0, 'short': 25})
                pts = weight['short']
                if pts > 0:
                    short_score += pts
                    signal_components['rsi_mtf_strong_bear'] = pts
                    logger.debug(f"{symbol} RSI双周期超买共振: 1h+15m RSI均>75")
            elif rsi_mtf_signal == 'LONG':
                weight = self.scoring_weights.get('rsi_mtf_bull', {'long': 15, 'short': 0})
                pts = weight['long']
                if pts > 0:
                    long_score += pts
                    signal_components['rsi_mtf_bull'] = pts
                    logger.debug(f"{symbol} RSI双周期超卖: 1h+15m RSI接近超卖区")
            elif rsi_mtf_signal == 'SHORT':
                weight = self.scoring_weights.get('rsi_mtf_bear', {'long': 0, 'short': 15})
                pts = weight['short']
                if pts > 0:
                    short_score += pts
                    signal_components['rsi_mtf_bear'] = pts
                    logger.debug(f"{symbol} RSI双周期超买: 1h+15m RSI接近超买区")

            # 23. 相对强弱信号 (rs_vs_btc)
            # 原理: 强于BTC的币种在牛市涨更多; 弱于BTC的币种在熊市跌更多
            # 极端弱势(跌幅远超BTC): 空头强信号; 极端强势: 多头强信号
            # 数据来源: price_stats_24h (每15分钟更新)
            rs_signal = self._get_relative_strength_signal(symbol)
            if rs_signal == 'VERY_WEAK':
                weight = self.scoring_weights.get('rs_very_weak', {'long': 0, 'short': 20})
                pts = weight['short']
                if pts > 0:
                    short_score += pts
                    signal_components['rs_very_weak'] = pts
                    logger.debug(f"{symbol} 极端弱势: 24H跌幅比BTC低10%以上")
            elif rs_signal == 'WEAK':
                weight = self.scoring_weights.get('rs_weak', {'long': 0, 'short': 10})
                pts = weight['short']
                if pts > 0:
                    short_score += pts
                    signal_components['rs_weak'] = pts
                    logger.debug(f"{symbol} 相对弱势: 24H跌幅比BTC低5-10%")
            elif rs_signal == 'VERY_STRONG':
                weight = self.scoring_weights.get('rs_very_strong', {'long': 20, 'short': 0})
                pts = weight['long']
                if pts > 0:
                    long_score += pts
                    signal_components['rs_very_strong'] = pts
                    logger.debug(f"{symbol} 极端强势: 24H涨幅比BTC高10%以上")
            elif rs_signal == 'STRONG':
                weight = self.scoring_weights.get('rs_strong', {'long': 10, 'short': 0})
                pts = weight['long']
                if pts > 0:
                    long_score += pts
                    signal_components['rs_strong'] = pts
                    logger.debug(f"{symbol} 相对强势: 24H涨幅比BTC高5-10%")

            # 24. 多周期EMA趋势共振 (ema_mtf)
            # 原理: 15m+1h+4h三个周期EMA同向代表强趋势确认,反转风险低
            # Triple BULL (全部向上): 极少见,熊市中的强势逆市多头
            # Triple BEAR (全部向下): 当前熊市中常见,但当其他信号共振时意义更强
            # 数据来源: technical_indicators_cache (每小时更新)
            ema_mtf_signal = self._get_ema_mtf_signal(symbol)
            if ema_mtf_signal == 'TRIPLE_BULL':
                weight = self.scoring_weights.get('ema_triple_bull', {'long': 25, 'short': 0})
                pts = weight['long']
                if pts > 0:
                    long_score += pts
                    signal_components['ema_triple_bull'] = pts
                    logger.debug(f"{symbol} EMA三周期多头共振: 15m+1h+4h全部向上")
            elif ema_mtf_signal == 'TRIPLE_BEAR':
                weight = self.scoring_weights.get('ema_triple_bear', {'long': 0, 'short': 15})
                pts = weight['short']
                if pts > 0:
                    short_score += pts
                    signal_components['ema_triple_bear'] = pts
                    logger.debug(f"{symbol} EMA三周期空头共振: 15m+1h+4h全部向下")
            elif ema_mtf_signal == 'PARTIAL_BULL':
                weight = self.scoring_weights.get('ema_partial_bull', {'long': 10, 'short': 0})
                pts = weight['long']
                if pts > 0:
                    long_score += pts
                    signal_components['ema_partial_bull'] = pts
                    logger.debug(f"{symbol} EMA双周期多头对齐: 1h+4h向上")
            elif ema_mtf_signal == 'PARTIAL_BEAR':
                weight = self.scoring_weights.get('ema_partial_bear', {'long': 0, 'short': 10})
                pts = weight['short']
                if pts > 0:
                    short_score += pts
                    signal_components['ema_partial_bear'] = pts
                    logger.debug(f"{symbol} EMA双周期空头对齐: 1h+4h向下")

            # 25. 4H短周期成交量强度信号 (vol_4h)
            # 原理: 4h/15m窗口比24h/1h更敏感, 捕捉近4小时的空多力量变化
            # 数据来源: technical_signals_cache (4h/15m, 每小时更新)
            vol_4h_signal = self._get_vol_4h_signal(symbol)
            if vol_4h_signal == 'BEAR_DOMINANCE':
                weight = self.scoring_weights.get('vol_4h_bear', {'long': 0, 'short': 12})
                pts = weight['short']
                if pts > 0:
                    short_score += pts
                    signal_components['vol_4h_bear'] = pts
                    logger.debug(f"{symbol} 4H空头量能主导: 近4H空头单根均量是多头2.5倍以上")
            elif vol_4h_signal == 'BULL_DOMINANCE':
                weight = self.scoring_weights.get('vol_4h_bull', {'long': 12, 'short': 0})
                pts = weight['long']
                if pts > 0:
                    long_score += pts
                    signal_components['vol_4h_bull'] = pts
                    logger.debug(f"{symbol} 4H多头量能主导: 近4H多头单根均量是空头2.5倍以上")

            # 26. 价格动量加速信号 (momentum_accel)
            # 原理: 对比最近2H vs 前2H价格变化率，判断趋势是否在加速或减速
            # 数据来源: klines_1h (实时K线)
            ma_signal = self._get_momentum_accel_signal(klines_1h)
            if ma_signal == 'ACCEL_DOWN':
                weight = self.scoring_weights.get('momentum_accel_bear', {'long': 0, 'short': 15})
                pts = weight['short']
                if pts > 0:
                    short_score += pts
                    signal_components['momentum_accel_bear'] = pts
                    logger.debug(f"{symbol} 空头动量加速: 最近2H跌幅>前2H * 1.5倍")
            elif ma_signal == 'ACCEL_UP':
                weight = self.scoring_weights.get('momentum_accel_bull', {'long': 15, 'short': 0})
                pts = weight['long']
                if pts > 0:
                    long_score += pts
                    signal_components['momentum_accel_bull'] = pts
                    logger.debug(f"{symbol} 多头动量加速: 最近2H涨幅>前2H * 1.5倍")
            elif ma_signal == 'DECEL_DOWN':
                weight = self.scoring_weights.get('momentum_decel_bull', {'long': 10, 'short': 0})
                pts = weight['long']
                if pts > 0:
                    long_score += pts
                    signal_components['momentum_decel_bull'] = pts
                    logger.debug(f"{symbol} 空头减速反弹: 前2H大跌但最近2H明显减速")

            # 27. K线实体质量信号 (candle_quality)
            # 原理: 最近6根1H蜡烛中4根以上同向且实体占比>=62%, 说明趋势方向明确无犹豫
            # 数据来源: klines_1h (实时K线)
            cq_signal = self._get_candle_quality_signal(klines_1h)
            if cq_signal == 'STRONG_BEAR':
                weight = self.scoring_weights.get('candle_quality_bear', {'long': 0, 'short': 12})
                pts = weight['short']
                if pts > 0:
                    short_score += pts
                    signal_components['candle_quality_bear'] = pts
                    logger.debug(f"{symbol} K线空头质量高: 6根中4+根阴线实体占比>=62%")
            elif cq_signal == 'STRONG_BULL':
                weight = self.scoring_weights.get('candle_quality_bull', {'long': 12, 'short': 0})
                pts = weight['long']
                if pts > 0:
                    long_score += pts
                    signal_components['candle_quality_bull'] = pts
                    logger.debug(f"{symbol} K线多头质量高: 6根中4+根阳线实体占比>=62%")

            # 28. 资金费率趋势信号 (funding_trend, 与Section12的极端值信号互补)
            # 正费率高=多头过热=SHORT机会; 负费率深=空头拥挤=LONG机会(挤空)
            fr_signal = self._get_funding_trend_signal(symbol)
            if fr_signal == 'OVERHEATED':
                weight = self.scoring_weights.get('funding_overheated', {'long': 0, 'short': 12})
                pts = weight['short']
                if pts > 0:
                    short_score += pts
                    signal_components['funding_overheated'] = pts
                    logger.debug(f"{symbol} 资金费率多头过热: SHORT+{pts}pt")
            elif fr_signal == 'BULLISH':
                weight = self.scoring_weights.get('funding_bullish', {'long': 0, 'short': 8})
                pts = weight['short']
                if pts > 0:
                    short_score += pts
                    signal_components['funding_bullish'] = pts
                    logger.debug(f"{symbol} 资金费率轻度多头: SHORT+{pts}pt")
            elif fr_signal == 'STRONGLY_BEARISH':
                weight = self.scoring_weights.get('funding_strongly_bearish', {'long': 10, 'short': 0})
                pts = weight['long']
                if pts > 0:
                    long_score += pts
                    signal_components['funding_strongly_bearish'] = pts
                    logger.debug(f"{symbol} 资金费率空头拥挤: LONG+{pts}pt (挤空)")
            elif fr_signal == 'BEARISH':
                weight = self.scoring_weights.get('funding_bearish', {'long': 6, 'short': 0})
                pts = weight['long']
                if pts > 0:
                    long_score += pts
                    signal_components['funding_bearish'] = pts
                    logger.debug(f"{symbol} 资金费率轻度空头: LONG+{pts}pt")

            # 29. ADX趋势强度信号 (从klines_1h计算)
            # ADX>25 + 方向性: 确认有效趋势; WEAK_TREND(<20): 趋势信号需谨慎
            adx_signal = self._get_adx_signal(klines_1h)
            if adx_signal == 'STRONG_BEAR':
                weight = self.scoring_weights.get('adx_strong_bear', {'long': 0, 'short': 10})
                pts = weight['short']
                if pts > 0:
                    short_score += pts
                    signal_components['adx_strong_bear'] = pts
                    logger.debug(f"{symbol} ADX强下降趋势: SHORT+{pts}pt")
            elif adx_signal == 'STRONG_BULL':
                weight = self.scoring_weights.get('adx_strong_bull', {'long': 10, 'short': 0})
                pts = weight['long']
                if pts > 0:
                    long_score += pts
                    signal_components['adx_strong_bull'] = pts
                    logger.debug(f"{symbol} ADX强上升趋势: LONG+{pts}pt")

            # 30. 量价背离信号 (从klines_1h计算)
            # 价格创新低但量能萎缩 → 空头乏力 → LONG; 价格创新高但量能萎缩 → 多头乏力 → SHORT
            vd_signal = self._get_volume_divergence_signal(klines_1h)
            if vd_signal == 'VOL_DIVERGE_BULL':
                weight = self.scoring_weights.get('vol_diverge_bull', {'long': 12, 'short': 0})
                pts = weight['long']
                if pts > 0:
                    long_score += pts
                    signal_components['vol_diverge_bull'] = pts
                    logger.debug(f"{symbol} 量价背离(多): 价格新低但量能萎缩, LONG+{pts}pt")
            elif vd_signal == 'VOL_DIVERGE_BEAR':
                weight = self.scoring_weights.get('vol_diverge_bear', {'long': 0, 'short': 12})
                pts = weight['short']
                if pts > 0:
                    short_score += pts
                    signal_components['vol_diverge_bear'] = pts
                    logger.debug(f"{symbol} 量价背离(空): 价格新高但量能萎缩, SHORT+{pts}pt")

            # 31. K线反转形态信号 (candle_reversal)
            # Hammer(锤线): 前3根偏空 + 最后一根下影线>=2倍实体 → 空头衰竭, LONG
            # Shooting Star(射击之星): 前3根偏多 + 最后一根上影线>=2倍实体 → 多头衰竭, SHORT
            cr_signal = self._get_candle_reversal_signal(klines_1h)
            if cr_signal == 'HAMMER':
                weight = self.scoring_weights.get('candle_reversal_bull', {'long': 10, 'short': 0})
                pts = weight['long']
                if pts > 0:
                    long_score += pts
                    signal_components['candle_reversal_bull'] = pts
                    logger.debug(f"{symbol} 锤线反转形态: 空头衰竭信号, LONG+{pts}pt")
            elif cr_signal == 'SHOOTING_STAR':
                weight = self.scoring_weights.get('candle_reversal_bear', {'long': 0, 'short': 10})
                pts = weight['short']
                if pts > 0:
                    short_score += pts
                    signal_components['candle_reversal_bear'] = pts
                    logger.debug(f"{symbol} 射击之星反转形态: 多头衰竭信号, SHORT+{pts}pt")

            # 32. 吞噬形态信号 (engulfing) - 比锤线更强的反转确认
            # 多头吞噬: 阳线实体完全包住前根阴线 + 前3根偏空 → 空头被彻底覆盖, LONG
            # 空头吞噬: 阴线实体完全包住前根阳线 + 前3根偏多 → 多头被彻底覆盖, SHORT
            eng_signal = self._get_engulfing_signal(klines_1h)
            if eng_signal == 'BULL_ENGULF':
                weight = self.scoring_weights.get('engulfing_bull', {'long': 15, 'short': 0})
                pts = weight['long']
                if pts > 0:
                    long_score += pts
                    signal_components['engulfing_bull'] = pts
                    logger.debug(f"{symbol} 多头吞噬形态: 阳线完全覆盖前阴线, LONG+{pts}pt")
            elif eng_signal == 'BEAR_ENGULF':
                weight = self.scoring_weights.get('engulfing_bear', {'long': 0, 'short': 15})
                pts = weight['short']
                if pts > 0:
                    short_score += pts
                    signal_components['engulfing_bear'] = pts
                    logger.debug(f"{symbol} 空头吞噬形态: 阴线完全覆盖前阳线, SHORT+{pts}pt")

            # 33. 价格结构信号 (price_structure)
            # 连续更高低点(Higher Lows): 买方支撑在上移, 下跌趋势内的多头机会
            # 连续更低高点(Lower Highs): 卖方压力在下移, 上涨趋势内的空头机会
            ps_signal = self._get_price_structure_signal(klines_1h)
            if ps_signal == 'HIGHER_LOWS':
                weight = self.scoring_weights.get('higher_lows', {'long': 12, 'short': 0})
                pts = weight['long']
                if pts > 0:
                    long_score += pts
                    signal_components['higher_lows'] = pts
                    logger.debug(f"{symbol} 连续更高低点: 多头支撑上移, LONG+{pts}pt")
            elif ps_signal == 'LOWER_HIGHS':
                weight = self.scoring_weights.get('lower_highs', {'long': 0, 'short': 12})
                pts = weight['short']
                if pts > 0:
                    short_score += pts
                    signal_components['lower_highs'] = pts
                    logger.debug(f"{symbol} 连续更低高点: 空头压制下移, SHORT+{pts}pt")

            # 34. KDJ_J 多周期共振信号 (kdj_mtf)
            # 与Section 9不同, 要求1h AND 15m同时满足极端区域条件
            # EXTREME_BULL: 1h J<0 AND 15m J<20 (两周期均极度超卖)
            # OVERSOLD_MTF: 1h J<10 AND 15m J<15 (两周期均在超卖区)
            # EXTREME_BEAR: 1h J>100 AND 15m J>80 (两周期均极度超买)
            # OVERBOUGHT_MTF: 1h J>90 AND 15m J>85
            kdj_mtf_signal = self._get_kdj_mtf_signal(symbol)
            if kdj_mtf_signal in ('EXTREME_BULL', 'OVERSOLD_MTF'):
                sig_name = 'kdj_j_mtf_bull'
                weight = self.scoring_weights.get(sig_name, {'long': 15, 'short': 0})
                pts = weight['long'] if kdj_mtf_signal == 'EXTREME_BULL' else max(1, int(weight['long'] * 0.7))
                if pts > 0:
                    long_score += pts
                    signal_components[sig_name] = pts
                    logger.debug(f"{symbol} KDJ_J多周期超卖共振: {kdj_mtf_signal}, LONG+{pts}pt")
            elif kdj_mtf_signal in ('EXTREME_BEAR', 'OVERBOUGHT_MTF'):
                sig_name = 'kdj_j_mtf_bear'
                weight = self.scoring_weights.get(sig_name, {'long': 0, 'short': 15})
                pts = weight['short'] if kdj_mtf_signal == 'EXTREME_BEAR' else max(1, int(weight['short'] * 0.7))
                if pts > 0:
                    short_score += pts
                    signal_components[sig_name] = pts
                    logger.debug(f"{symbol} KDJ_J多周期超买共振: {kdj_mtf_signal}, SHORT+{pts}pt")

            # 35. MACD 多周期方向共振信号 (macd_mtf)
            # 与Section 19(零轴交叉事件)不同: 持续检测1h和4h histogram方向
            # 两个周期histogram同为正 → 多头动量共振(BULL); 同为负 → 空头动量共振(BEAR)
            macd_mtf_signal = self._get_macd_mtf_signal(symbol)
            if macd_mtf_signal == 'BULL':
                weight = self.scoring_weights.get('macd_hist_align_bull', {'long': 10, 'short': 0})
                pts = weight['long']
                if pts > 0:
                    long_score += pts
                    signal_components['macd_hist_align_bull'] = pts
                    logger.debug(f"{symbol} MACD 1H+4H直方图同为正: 多头动量共振, LONG+{pts}pt")
            elif macd_mtf_signal == 'BEAR':
                weight = self.scoring_weights.get('macd_hist_align_bear', {'long': 0, 'short': 10})
                pts = weight['short']
                if pts > 0:
                    short_score += pts
                    signal_components['macd_hist_align_bear'] = pts
                    logger.debug(f"{symbol} MACD 1H+4H直方图同为负: 空头动量共振, SHORT+{pts}pt")

            # 36. 价格区间突破+量能确认信号 (range_breakout)
            # 突破近12H最高/最低价 + 成交量放大1.5x → 过滤假突破，确认真实方向性资金介入
            rb_signal = self._get_range_breakout_signal(klines_1h)
            if rb_signal == 'BULL_BREAKOUT':
                weight = self.scoring_weights.get('range_breakout_bull', {'long': 15, 'short': 0})
                pts = weight['long']
                if pts > 0:
                    long_score += pts
                    signal_components['range_breakout_bull'] = pts
                    logger.debug(f"{symbol} 价格突破12H高点+量能放大: 多头确认突破, LONG+{pts}pt")
            elif rb_signal == 'BEAR_BREAKOUT':
                weight = self.scoring_weights.get('range_breakout_bear', {'long': 0, 'short': 15})
                pts = weight['short']
                if pts > 0:
                    short_score += pts
                    signal_components['range_breakout_bear'] = pts
                    logger.debug(f"{symbol} 价格跌破12H低点+量能放大: 空头确认突破, SHORT+{pts}pt")

            # 37. Stochastic RSI 超买超卖信号 (stoch_rsi)
            # 基于1H K线实时计算，比单纯RSI更灵敏，能更早捕捉超买超卖转折
            stoch_rsi_signal = self._get_stoch_rsi_signal(klines_1h)
            if stoch_rsi_signal in ('STRONG_OVERSOLD', 'OVERSOLD'):
                sig_name = 'stoch_rsi_bull'
                weight = self.scoring_weights.get(sig_name, {'long': 12, 'short': 0})
                pts = weight['long'] if stoch_rsi_signal == 'STRONG_OVERSOLD' else max(1, int(weight['long'] * 0.7))
                if pts > 0:
                    long_score += pts
                    signal_components[sig_name] = pts
                    logger.debug(f"{symbol} StochRSI超卖区间 ({stoch_rsi_signal}): 潜在反弹, LONG+{pts}pt")
            elif stoch_rsi_signal in ('STRONG_OVERBOUGHT', 'OVERBOUGHT'):
                sig_name = 'stoch_rsi_bear'
                weight = self.scoring_weights.get(sig_name, {'long': 0, 'short': 12})
                pts = weight['short'] if stoch_rsi_signal == 'STRONG_OVERBOUGHT' else max(1, int(weight['short'] * 0.7))
                if pts > 0:
                    short_score += pts
                    signal_components[sig_name] = pts
                    logger.debug(f"{symbol} StochRSI超买区间 ({stoch_rsi_signal}): 潜在回落, SHORT+{pts}pt")

            # 38. 多周期K线反转共振信号 (mtf_candle_resonance)
            # 1H反转形态（Hammer/Shooting Star/吞噬）+ 15M同向确认 → 共振强化
            mtf_candle_signal = self._get_mtf_candle_resonance_signal(klines_1h, klines_15m)
            if mtf_candle_signal == 'BULL_RESONANCE':
                weight = self.scoring_weights.get('mtf_candle_bull', {'long': 15, 'short': 0})
                pts = weight['long']
                if pts > 0:
                    long_score += pts
                    signal_components['mtf_candle_bull'] = pts
                    logger.debug(f"{symbol} 1H反转形态+15M同向确认: 多周期共振, LONG+{pts}pt")
            elif mtf_candle_signal == 'BEAR_RESONANCE':
                weight = self.scoring_weights.get('mtf_candle_bear', {'long': 0, 'short': 15})
                pts = weight['short']
                if pts > 0:
                    short_score += pts
                    signal_components['mtf_candle_bear'] = pts
                    logger.debug(f"{symbol} 1H反转形态+15M同向确认: 多周期共振, SHORT+{pts}pt")

            # 41. 连续收盘方向信号 (close_chain)
            # 最近5根K线连续4次同向收盘 → 强动量信号
            close_chain_signal = self._get_close_chain_signal(klines_1h)
            if close_chain_signal == 'BULL_CHAIN':
                weight = self.scoring_weights.get('close_chain_bull', {'long': 12, 'short': 0})
                pts = weight['long']
                if pts > 0:
                    long_score += pts
                    signal_components['close_chain_bull'] = pts
                    logger.debug(f"{symbol} 连续4根1H阳线收盘: 强多头趋势, LONG+{pts}pt")
            elif close_chain_signal == 'BEAR_CHAIN':
                weight = self.scoring_weights.get('close_chain_bear', {'long': 0, 'short': 12})
                pts = weight['short']
                if pts > 0:
                    short_score += pts
                    signal_components['close_chain_bear'] = pts
                    logger.debug(f"{symbol} 连续4根1H阴线收盘: 强空头趋势, SHORT+{pts}pt")

            # 40. 1H微观趋势动量信号 (micro_trend)
            # 使用 1H/5M 窗口（12根5M K线）的多空力量对比，捕捉最近1小时的方向性资金动向
            micro_signal = self._get_micro_trend_signal(symbol)
            if micro_signal == 'BULL_DOMINANCE':
                weight = self.scoring_weights.get('micro_trend_bull', {'long': 10, 'short': 0})
                pts = weight['long']
                if pts > 0:
                    long_score += pts
                    signal_components['micro_trend_bull'] = pts
                    logger.debug(f"{symbol} 1H/5M微观趋势: 空/多均量比>=2.0x, 多头动量, LONG+{pts}pt")
            elif micro_signal == 'BEAR_DOMINANCE':
                weight = self.scoring_weights.get('micro_trend_bear', {'long': 0, 'short': 10})
                pts = weight['short']
                if pts > 0:
                    short_score += pts
                    signal_components['micro_trend_bear'] = pts
                    logger.debug(f"{symbol} 1H/5M微观趋势: 空/多均量比>=2.0x, 空头动量, SHORT+{pts}pt")

            # 39. 24H VWAP 偏离信号 (vwap_deviation)
            # VWAP 偏离 > +5% → 价格超出近24H均价 → SHORT；偏离 < -5% → 价格低于均价 → LONG
            vwap_signal = self._get_vwap_deviation_signal(klines_1h)
            if vwap_signal in ('STRONG_BELOW', 'BELOW'):
                sig_name = 'vwap_bull'
                weight = self.scoring_weights.get(sig_name, {'long': 12, 'short': 0})
                pts = weight['long'] if vwap_signal == 'STRONG_BELOW' else max(1, int(weight['long'] * 0.6))
                if pts > 0:
                    long_score += pts
                    signal_components[sig_name] = pts
                    logger.debug(f"{symbol} 价格低于24H VWAP ({vwap_signal}): 潜在均值回归, LONG+{pts}pt")
            elif vwap_signal in ('STRONG_ABOVE', 'ABOVE'):
                sig_name = 'vwap_bear'
                weight = self.scoring_weights.get(sig_name, {'long': 0, 'short': 12})
                pts = weight['short'] if vwap_signal == 'STRONG_ABOVE' else max(1, int(weight['short'] * 0.6))
                if pts > 0:
                    short_score += pts
                    signal_components[sig_name] = pts
                    logger.debug(f"{symbol} 价格高于24H VWAP ({vwap_signal}): 潜在均值回归, SHORT+{pts}pt")

            # 42. Open Interest 变化率信号 (oi_surge)
            oi_signal = self._get_oi_signal(symbol)
            if oi_signal == 'OI_SURGE':
                # OI 暴增 + 价格位置决定方向
                current_close = float(klines_1h[-1]['close']) if klines_1h else 0
                vwap_ref = self._get_vwap_deviation_signal(klines_1h) if klines_1h else 'NEUTRAL'
                if vwap_ref in ('STRONG_BELOW', 'BELOW'):  # 价格偏低 + OI增 = 空头建仓后可能逼空
                    sig_name = 'oi_surge_bull'
                    weight = self.scoring_weights.get(sig_name, {'long': 10, 'short': 0})
                    pts = weight['long']
                    if pts > 0:
                        long_score += pts
                        signal_components[sig_name] = pts
                elif vwap_ref in ('STRONG_ABOVE', 'ABOVE'):  # 价格偏高 + OI增 = 多头追涨
                    sig_name = 'oi_surge_bear'
                    weight = self.scoring_weights.get(sig_name, {'long': 0, 'short': 10})
                    pts = weight['short']
                    if pts > 0:
                        short_score += pts
                        signal_components[sig_name] = pts
            elif oi_signal == 'OI_DROP':
                # OI 暴减 = 大量持仓平仓，趋势反转风险
                sig_name = 'oi_drop_reversal'
                weight = self.scoring_weights.get(sig_name, {'long': 0, 'short': 8})
                pts = weight['short']
                if pts > 0:
                    short_score += pts
                    signal_components[sig_name] = pts

            # 43. BB 压缩后释放信号 (bb_squeeze)
            bb_squeeze_signal = self._get_bb_squeeze_signal(klines_1h)
            if bb_squeeze_signal == 'BULL_SQUEEZE_RELEASE':
                sig_name = 'bb_squeeze_bull'
                weight = self.scoring_weights.get(sig_name, {'long': 14, 'short': 0})
                pts = weight['long']
                if pts > 0:
                    long_score += pts
                    signal_components[sig_name] = pts
                    logger.debug(f"{symbol} BB压缩后向上释放, LONG+{pts}pt")
            elif bb_squeeze_signal == 'BEAR_SQUEEZE_RELEASE':
                sig_name = 'bb_squeeze_bear'
                weight = self.scoring_weights.get(sig_name, {'long': 0, 'short': 14})
                pts = weight['short']
                if pts > 0:
                    short_score += pts
                    signal_components[sig_name] = pts
                    logger.debug(f"{symbol} BB压缩后向下释放, SHORT+{pts}pt")

            # 44. EMA 距离拉伸信号 (ema_distance)
            current_px = float(klines_1h[-1]['close']) if klines_1h else 0.0
            ema_dist_signal = self._get_ema_distance_signal(symbol, current_px) if current_px > 0 else 'NEUTRAL'
            if ema_dist_signal == 'OVER_EXTENDED_BEAR':
                sig_name = 'ema_dist_bull'
                weight = self.scoring_weights.get(sig_name, {'long': 10, 'short': 0})
                pts = weight['long']
                if pts > 0:
                    long_score += pts
                    signal_components[sig_name] = pts
                    logger.debug(f"{symbol} 价格过度低于EMA, 均值回归LONG+{pts}pt")
            elif ema_dist_signal == 'OVER_EXTENDED_BULL':
                sig_name = 'ema_dist_bear'
                weight = self.scoring_weights.get(sig_name, {'long': 0, 'short': 10})
                pts = weight['short']
                if pts > 0:
                    short_score += pts
                    signal_components[sig_name] = pts
                    logger.debug(f"{symbol} 价格过度高于EMA, 均值回归SHORT+{pts}pt")

            # 45. 订单流突刺信号 (order_flow_spike)
            of_spike = self._get_order_flow_spike_signal(klines_15m) if klines_15m else 'NEUTRAL'
            if of_spike == 'BUY_SPIKE':
                sig_name = 'order_flow_bull'
                weight = self.scoring_weights.get(sig_name, {'long': 12, 'short': 0})
                pts = weight['long']
                if pts > 0:
                    long_score += pts
                    signal_components[sig_name] = pts
                    logger.debug(f"{symbol} 15M taker_buy突刺, LONG+{pts}pt")
            elif of_spike == 'SELL_SPIKE':
                sig_name = 'order_flow_bear'
                weight = self.scoring_weights.get(sig_name, {'long': 0, 'short': 12})
                pts = weight['short']
                if pts > 0:
                    short_score += pts
                    signal_components[sig_name] = pts
                    logger.debug(f"{symbol} 15M taker_sell突刺, SHORT+{pts}pt")

            # ========== 移除EMA评分 (已有Big4市场趋势判断) ==========
            # 此注释保留历史说明: 旧版EMA评分已移除,新版多周期EMA共振(ema_mtf)已加入

            # ========== 移除1D信号 (4小时持仓不需要1D趋势) ==========
            # 已移除: trend_1d_bull, trend_1d_bear

            # V1评分计算完成，稍后与V2一起打印

            # 读取 Big4 整体信号
            # STRONG_BULLISH/STRONG_BEARISH: 单币>=11阳/阴+0.5% 且强权重>60%
            # BULLISH/BEARISH            : 单币>=9阳/阴+0.5%  且总权重>60%
            # NEUTRAL                    : 不满足上述条件
            _b4_signal = big4_result.get('overall_signal', 'NEUTRAL') if big4_result else 'NEUTRAL'

            return None

        except Exception as e:
            logger.error(f"{symbol} 分析失败: {e}")
            return None

    def scan_all(self, big4_result: dict = None):
        """扫描所有币种

        Args:
            big4_result: Big4趋势结果 (由SmartTraderService传入，仅用于日志显示)
        """
        # 时间封锁：下午1点~5点不开新仓（市场恐慌抛压窗口）
        _now_hour = datetime.now().hour + datetime.now().minute / 60
        if 13.0 <= _now_hour < 17.0:
            logger.info(f"[BLACKOUT] 当前 {datetime.now().strftime('%H:%M')}，13:00~17:00 封锁开仓，跳过扫描")
            return []

        # 每次扫描前重新加载黑名单,确保运行时添加的黑名单立即生效
        self._reload_blacklist()

        # K线数据新鲜度检查：防止采集服务崩溃时基于过时数据开仓
        try:
            _stale_conn = self._get_connection()
            # commit() 重置 REPEATABLE READ 快照，确保读到最新数据
            try:
                _stale_conn.commit()
            except Exception:
                pass
            _stale_cur = _stale_conn.cursor()
            _stale_cur.execute(
                "SELECT MAX(open_time) FROM kline_data WHERE timeframe='1h' AND exchange='binance_futures'"
            )
            _stale_row = _stale_cur.fetchone()
            _stale_cur.close()
            # DictCursor returns dict — use column alias to access value
            _stale_val = _stale_row.get('MAX(open_time)') if isinstance(_stale_row, dict) else (_stale_row[0] if _stale_row else None)
            if _stale_row and _stale_val:
                _age_hours = (time.time() * 1000 - _stale_val) / 3600000
                if _age_hours > 2:
                    logger.error(
                        f"[STALE_DATA] ⚠️ K线数据已 {_age_hours:.1f}h 未更新！"
                        f"采集服务可能已崩溃，跳过本轮扫描以避免基于过时数据开仓。"
                    )
                    return []
        except Exception as _e:
            logger.warning(f"检查K线新鲜度失败（放行）: {_e}")

        # 📈 ADX市场状态检测（BTC 4H图，每轮扫描更新一次）
        try:
            _adx_conn = self._get_connection()
            _adx_cur = _adx_conn.cursor()
            _adx_cur.execute(
                "SELECT high_price, low_price, close_price FROM kline_data "
                "WHERE symbol='BTC/USDT' AND timeframe='4h' AND exchange='binance_futures' "
                "ORDER BY open_time DESC LIMIT 30"
            )
            _adx_rows = list(reversed(_adx_cur.fetchall()))
            _adx_cur.close()
            if len(_adx_rows) >= 16:
                # DictCursor returns dicts — use column names
                _highs  = [float(r['high_price'])  for r in _adx_rows]
                _lows   = [float(r['low_price'])   for r in _adx_rows]
                _closes = [float(r['close_price']) for r in _adx_rows]
                _tr_list, _plus_dm_list, _minus_dm_list = [], [], []
                for _i in range(1, len(_closes)):
                    _tr = max(_highs[_i]-_lows[_i], abs(_highs[_i]-_closes[_i-1]), abs(_lows[_i]-_closes[_i-1]))
                    _tr_list.append(_tr)
                    _pdm = max(0, _highs[_i]-_highs[_i-1]) if _highs[_i]-_highs[_i-1] > _lows[_i-1]-_lows[_i] else 0
                    _mdm = max(0, _lows[_i-1]-_lows[_i]) if _lows[_i-1]-_lows[_i] > _highs[_i]-_highs[_i-1] else 0
                    _plus_dm_list.append(_pdm)
                    _minus_dm_list.append(_mdm)
                _p = 14
                _atr = sum(_tr_list[-_p:]) / _p
                if _atr > 0:
                    _pdi = sum(_plus_dm_list[-_p:]) / _p / _atr * 100
                    _mdi = sum(_minus_dm_list[-_p:]) / _p / _atr * 100
                    _ds  = _pdi + _mdi
                    self.market_adx = abs(_pdi - _mdi) / _ds * 100 if _ds > 0 else 25.0
                else:
                    self.market_adx = 25.0
            else:
                self.market_adx = 25.0
            _adx_label = "震荡市⚠️(阈值+10)" if self.market_adx < 20 else ("弱趋势" if self.market_adx < 30 else "趋势市")
            logger.info(f"📈 [ADX] BTC 4H ADX={self.market_adx:.1f} → {_adx_label}")
        except Exception as _adx_e:
            logger.warning(f"[ADX] 计算失败，默认25: {_adx_e}")
            self.market_adx = 25.0

        logger.info(f"\n{'='*100}")
        logger.info(f"🔍 开始扫描 {len(self.whitelist)} 个交易对 | 开仓阈值: {self.threshold}分")

        # 显示Big4状态
        big4_signal = 'NEUTRAL'
        big4_strength = 0
        if big4_result:
            big4_signal = big4_result.get('overall_signal', 'NEUTRAL')
            big4_strength = big4_result.get('signal_strength', 0)
            logger.info(f"📊 Big4市场趋势: {big4_signal} (强度: {big4_strength:.1f})")

        logger.info(f"{'='*100}")

        opportunities = []

        for symbol in self.whitelist:
            result = self.analyze(symbol, big4_result=big4_result)
            if result:
                opportunities.append(result)

        # 🧠 从众效应防御：Big4 NEUTRAL + ≥80%同向 + ≥5个信号时，提高阈值
        # 前提条件：仅在 Big4 为 NEUTRAL 时生效
        # Big4 明确看多/空时，同向信号多是正常现象，不应惩罚
        # Big4 NEUTRAL 时大量同向信号才是情绪化追涨/杀跌的特征
        if len(opportunities) >= 5 and big4_signal == 'NEUTRAL':
            long_count = sum(1 for o in opportunities if o['side'] == 'LONG')
            short_count = len(opportunities) - long_count
            dominant_pct = max(long_count, short_count) / len(opportunities)
            if dominant_pct >= 0.8:
                dominant_side = 'LONG' if long_count > short_count else 'SHORT'
                before_count = len(opportunities)
                if dominant_side == 'LONG':
                    # 多头羊群 = FOMO追涨，提高LONG门槛压制低质量信号
                    herding_threshold = self.threshold + 5
                    opportunities = [
                        o for o in opportunities
                        if o['side'] != 'LONG' or o['score'] >= herding_threshold
                    ]
                    logger.warning(
                        f"🧠 [HERDING-LONG] {dominant_pct*100:.0f}%偏多({long_count}多/{short_count}空)"
                        f"，FOMO追涨，LONG门槛+5至{herding_threshold}分: {before_count}→{len(opportunities)}个"
                    )
                else:
                    # 空头羊群 = 恐慌是趋势力量的体现，顺势放行所有SHORT，不设惩罚
                    logger.info(
                        f"🧠 [HERDING-SHORT] {dominant_pct*100:.0f}%偏空({long_count}多/{short_count}空)"
                        f"，恐慌=趋势确认，顺势放行全部{before_count}个SHORT信号"
                    )

        # 🛑 崩后企稳检测：Big4刚从BEARISH转为NEUTRAL，禁止继续追空
        # 场景：大跌后市场已企稳，但信号组件（阴线/量能）具有滞后性，仍显示空头偏多
        # 历史证明：此场景中HIGH-SCORE SHORT胜率仅17.4%（3/16，亏-131U）
        if big4_signal == 'NEUTRAL' and any(o['side'] == 'SHORT' for o in opportunities):
            try:
                _pc_cur = self._get_connection().cursor()
                _pc_cur.execute("""
                    SELECT
                        SUM(CASE WHEN overall_signal IN ('BEARISH','STRONG_BEARISH') THEN 1 ELSE 0 END) as bearish_cnt,
                        COUNT(*) as total_cnt
                    FROM big4_trend_history
                    WHERE created_at >= DATE_SUB(NOW(), INTERVAL 4 HOUR)
                """)
                _pc_row = _pc_cur.fetchone()
                _pc_cur.close()
                if _pc_row and _pc_row['total_cnt']:
                    _bearish_ratio = (_pc_row['bearish_cnt'] or 0) / _pc_row['total_cnt']
                    if _bearish_ratio >= 0.5:
                        _short_before = sum(1 for o in opportunities if o['side'] == 'SHORT')
                        opportunities = [o for o in opportunities if o['side'] != 'SHORT']
                        logger.warning(
                            f"🛑 [POST-CRASH] 近4H BEARISH占比{_bearish_ratio*100:.0f}%"
                            f"，Big4已转NEUTRAL → 崩后企稳，禁止做空"
                            f"，过滤{_short_before}个SHORT信号"
                        )
            except Exception as _e:
                logger.warning(f"崩后企稳检测失败（放行）: {_e}")

        logger.info(f"{'='*100}")
        logger.info(f"✅ 扫描完成 | 合格信号: {len(opportunities)} 个 | Big4状态: {big4_signal}(强度{big4_strength:.0f})")
        logger.info(f"{'='*100}\n")

        return opportunities

    def _validate_signal_direction(self, signal_components: dict, side: str) -> tuple:
        """
        验证信号方向一致性,防止矛盾信号

        Args:
            signal_components: 信号组件字典
            side: 交易方向 (LONG/SHORT)

        Returns:
            (is_valid, reason) - 是否有效,原因描述
        """
        if not signal_components:
            return True, "无信号组件"

        # 定义空头信号（不应该出现在做多信号中）- 已移除1D信号
        bearish_signals = {
            'breakdown_short', 'volume_power_bear', 'volume_power_1h_bear',
            'trend_1h_bear', 'momentum_down_3pct', 'consecutive_bear',
            'position_24h_high', 'volume_power_12x_bear',
        }

        # 定义多头信号（不应该出现在做空信号中）- 已移除1D和EMA信号
        bullish_signals = {
            'breakout_long', 'volume_power_bull', 'volume_power_1h_bull',
            'trend_1h_bull', 'momentum_up_3pct', 'consecutive_bull',
            'position_24h_low', 'volume_power_12x_bull',
        }

        signal_set = set(signal_components.keys())

        if side == 'LONG':
            conflicts = bearish_signals & signal_set
            if conflicts:
                # 特殊情况：低位下跌3%可能是超跌反弹机会,允许做多
                if conflicts == {'momentum_down_3pct'} and 'position_low' in signal_set:
                    return True, "超跌反弹允许"
                return False, f"做多但包含空头信号: {', '.join(conflicts)}"

        elif side == 'SHORT':
            conflicts = bullish_signals & signal_set
            if conflicts:
                # 特殊情况：高位上涨3%可能是超涨回调机会,允许做空
                if conflicts == {'momentum_up_3pct'} and 'position_high' in signal_set:
                    return True, "超涨回调允许"
                return False, f"做空但包含多头信号: {', '.join(conflicts)}"

        return True, "信号方向一致"


class SmartTraderService:
    """智能交易服务"""

    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', '3306')),
            'user': os.getenv('DB_USER', 'root'),
            'password': os.getenv('DB_PASSWORD', ''),
            'database': os.getenv('DB_NAME', 'binance-data')
        }

        self.account_id = 2
        self.position_size_usdt = 400  # 默认仓位
        self.blacklist_position_size_usdt = 100  # 黑名单交易对使用小仓位
        self.connection = None  # 必须在 _load_max_positions() 之前初始化
        self.max_positions = self._load_max_positions()
        self.leverage = 10
        self.scan_interval = 60  # Reduced to 60s for higher trade frequency

        self.brain = SmartDecisionBrain(self.db_config)
        self._load_trading_mode_flags_from_db()
        self.running = True
        self.event_loop = None  # 事件循环引用，在async_main中设置
        self._pending_entry_count = 0  # 正在后台采样中（尚未写入DB）的任务数

        # WebSocket 价格服务
        self.ws_service: BinanceWSPriceService = get_ws_price_service()

        # 自适应优化器
        self.optimizer = AdaptiveOptimizer(self.db_config)
        self.last_optimization_date = None  # 记录上次优化日期

        # 优化配置管理器 (支持自我优化的参数配置)
        self.opt_config = OptimizationConfig(self.db_config)

        # 交易对评级管理器 (3级黑名单制度)
        self.rating_manager = SymbolRatingManager(self.db_config)

        # 波动率配置更新器 (15M K线动态止盈)
        self.volatility_updater = VolatilityProfileUpdater(self.db_config)

        # 市场状态监控器（操纵子原理：自动感知市场状态，切换allow_long/allow_short）
        self.regime_monitor = Big4RegimeMonitor(self.db_config)

        # 加载智能平仓配置
        import yaml
        with open('config.yaml', 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            self.smart_exit_config = config.get('signals', {}).get('smart_exit', {'enabled': False})
            # 最大持仓时间：优先从数据库读取，不再参考config.yaml
            try:
                _db_mh = self.opt_config._read_system_setting('max_hold_hours')
                _db_hours = max(3, min(8, int(_db_mh))) if _db_mh else 4
            except Exception:
                _db_hours = 4  # DB读取失败默认4小时
            self.max_hold_minutes = _db_hours * 60
            logger.info(f"最大持仓时间(DB): {self.max_hold_minutes}分钟 ({_db_hours}小时)")

            # 🔥 从数据库读取系统配置（优先级高于config.yaml）
            from app.services.system_settings_loader import get_big4_filter_enabled

            # Big4过滤器配置
            big4_enabled_from_db = get_big4_filter_enabled()
            self.big4_filter_config = {'enabled': big4_enabled_from_db}
            self.brain.big4_filter_enabled = big4_enabled_from_db  # 同步到brain，使analyze()内动态阈值感知过滤器状态
            logger.info(f"从数据库加载Big4过滤器配置: {'启用' if big4_enabled_from_db else '禁用'}")

        # 初始化 api_key_service 全局单例（供 SmartExitOptimizer._close_live_positions_on_exchange 使用）
        try:
            from app.services.api_key_service import init_api_key_service
            init_api_key_service(self.db_config)
            logger.info("api_key_service 全局单例已初始化")
        except Exception as _aks_e:
            logger.error(f"api_key_service 初始化失败: {_aks_e}")

        # 初始化智能平仓优化器
        if self.smart_exit_config.get('enabled'):
            self.smart_exit_optimizer = SmartExitOptimizer(
                db_config=self.db_config,
                live_engine=self,
                price_service=self.ws_service
            )
            logger.info("✅ 智能平仓优化器已启动")
        else:
            self.smart_exit_optimizer = None
            logger.info("⚠️ 智能平仓优化器未启用")

        # 初始化BTC动量跟随策略
        from app.services.btc_momentum_trader import BTCMomentumTrader
        self.btc_momentum_trader = BTCMomentumTrader(
            db_config=self.db_config,
            ws_price_service=self.ws_service
        )
        logger.info("✅ BTC动量跟随策略已初始化")

        # 初始化价格采样建仓执行器（V1策略：15分钟价格采样找最优点，一次性开仓）
        self.smart_entry_executor = SmartEntryExecutor(
            db_config=self.db_config,
            live_engine=self,
            price_service=self.ws_service,
            account_id=self.account_id
        )
        logger.info("✅ 价格采样建仓执行器已启动 (V1: 15分钟价格采样，一次性开仓)")


        # 初始化Big4趋势检测器 (四大天王: BTC/ETH/BNB/SOL)
        self.big4_detector = Big4TrendDetector()
        self.big4_symbols = ['BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'SOL/USDT']

        # ========== 破位信号加权系统 ==========
        self.breakout_booster = BreakoutSignalBooster(expiry_hours=4)
        logger.info("✅ 破位信号加权系统已初始化 (4小时有效期)")

        # ========== 震荡市交易策略模块 ==========
        self.range_detector = RangeMarketDetector(self.db_config)
        self.bollinger_strategy = BollingerMeanReversionStrategy(self.db_config)
        self.mode_switcher = TradingModeSwitcher(self.db_config)
        logger.info("✅ 震荡市交易策略模块已初始化")

        logger.info("🔱 Big4趋势检测器已启动 (实时检测模式)")

        # Telegram 通知（熔断/告警直接调用 self.telegram_notifier.send_message(...)）
        from app.services.trade_notifier import TradeNotifier as _TradeNotifier
        self.telegram_notifier = _TradeNotifier({
            'notifications': {
                'telegram': {
                    'enabled': bool(os.getenv('TELEGRAM_BOT_TOKEN')),
                    'bot_token': os.getenv('TELEGRAM_BOT_TOKEN', ''),
                    'chat_id': os.getenv('TELEGRAM_CHAT_ID', ''),
                    'notify_events': ['all']
                }
            }
        })

        logger.info("=" * 60)
        logger.info("智能自动交易服务已启动")
        logger.info(f"账户ID: {self.account_id}")
        logger.info(f"仓位: 正常${self.position_size_usdt} / 黑名单${self.blacklist_position_size_usdt} | 杠杆: {self.leverage}x | 最大持仓: {self.max_positions}")
        logger.info(f"白名单: {len(self.brain.whitelist)}个币种 | 黑名单: {len(self.brain.blacklist)}个币种 | 扫描间隔: {self.scan_interval}秒")
        logger.info("🧠 自适应优化器已启用 (每日凌晨2点自动运行)")
        logger.info("🔧 优化配置管理器已启用 (支持4大优化问题的自我配置)")
        logger.info("=" * 60)

    def _get_connection(self):
        if self.connection is None or not self.connection.open:
            self.connection = pymysql.connect(
                **self.db_config,
                autocommit=True,
                connect_timeout=10,  # 🔥 连接超时10秒
                read_timeout=30,     # 🔥 读取超时30秒
                write_timeout=30     # 🔥 写入超时30秒
            )
            # 🔥 设置InnoDB锁等待超时为5秒，防止死锁长时间阻塞
            with self.connection.cursor() as cursor:
                cursor.execute("SET SESSION innodb_lock_wait_timeout = 5")
        else:
            try:
                self.connection.ping(reconnect=True)
            except:
                self.connection = pymysql.connect(
                    **self.db_config,
                    autocommit=True,
                    connect_timeout=10,  # 🔥 连接超时10秒
                    read_timeout=30,     # 🔥 读取超时30秒
                    write_timeout=30     # 🔥 写入超时30秒
                )
                # 🔥 设置InnoDB锁等待超时为5秒
                with self.connection.cursor() as cursor:
                    cursor.execute("SET SESSION innodb_lock_wait_timeout = 5")
        return self.connection

    def _load_trading_mode_flags_from_db(self):
        """启动时从 DB 同步主策略复选框：仅 setting_value=1 时启用，缺省关（与 UI 勾选一致）。"""
        try:
            conn = pymysql.connect(
                **self.db_config,
                autocommit=True,
                connect_timeout=10,
                read_timeout=30,
                write_timeout=30,
            )
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT setting_key, setting_value FROM system_settings
                        WHERE setting_key IN ('signal_confirmation_enabled', 'trend_following_enabled')
                    """)
                    rows = {r[0]: r[1] for r in cur.fetchall()}
            finally:
                conn.close()
            new_sc = int(float(rows.get('signal_confirmation_enabled', '0'))) == 1
            new_tf = int(float(rows.get('trend_following_enabled', '0'))) == 1
            self.brain.signal_confirmation_enabled = new_sc
            self.brain.trend_following_enabled = new_tf
            logger.info(
                f"[TRADING-MODE] 主策略(启动): 信号确认={'ON' if new_sc else 'OFF'} "
                f"趋势跟随={'ON' if new_tf else 'OFF'}"
            )
        except Exception as e:
            logger.warning(f"[TRADING-MODE] 启动时读取主策略开关失败，保持默认关: {e}")

    def check_trading_enabled(self) -> bool:
        """
        检查交易是否启用（从system_settings表读取）

        Returns:
            bool: True=交易启用, False=交易停止
        """
        try:
            # 注意: _get_connection() 返回单例连接 self.connection，不应调用 conn.close()
            # 单例连接通过 ping(reconnect=True) 自动保活，无需每次重建
            conn = self._get_connection()
            cursor = conn.cursor(pymysql.cursors.DictCursor)

            # 从 system_settings 表读取 u_futures_trading_enabled
            cursor.execute("""
                SELECT setting_value
                FROM system_settings
                WHERE setting_key = 'u_futures_trading_enabled'
            """)

            result = cursor.fetchone()
            cursor.close()
            # ⚠️ 不调用 conn.close() — conn 是单例 self.connection，关闭会强迫每5秒重建TCP连接

            if result:
                # setting_value 可能是字符串 '1'/'0' 或布尔值
                value = result['setting_value']
                if isinstance(value, str):
                    enabled = value in ('1', 'true', 'True', 'yes')
                else:
                    enabled = bool(value)
                # 缓存上次成功读取的值，用于连接失败时的兜底
                self._last_trading_enabled = enabled
                self._last_trading_enabled_at = datetime.now()
                return enabled
            else:
                # 如果数据库中没有记录，默认禁止（安全策略：查不到开关就不开单）
                logger.warning(f"[TRADING-CONTROL] 未找到U本位交易控制设置(u_futures_trading_enabled), 默认禁止开单")
                return False

        except Exception as e:
            # 连接失败时，优先使用缓存值（60秒内有效），避免短暂DB断线就停止交易
            cached = getattr(self, '_last_trading_enabled', None)
            cached_at = getattr(self, '_last_trading_enabled_at', None)
            if cached is not None and cached_at and (datetime.now() - cached_at).total_seconds() < 60:
                logger.warning(f"[TRADING-CONTROL] 检查交易状态失败({e}), 使用缓存值: {'开启' if cached else '关闭'}")
                return cached
            # 缓存过期或无缓存，默认禁止
            logger.error(f"[TRADING-CONTROL] 检查交易状态失败: {e}, 缓存已过期，默认禁止开单")
            return False

    def _check_profit_and_auto_disable(self, profit_threshold=1000.0, window_hours=6, check_interval_hours=4) -> bool:
        """
        盈利熔断：统计窗口内总盈利超过阈值后自动禁止开仓

        逻辑：
        - 每 check_interval_hours 小时检测一次（默认4小时）
        - 检查最近 window_hours 小时已平仓PNL总和（默认6小时）
        - 若超过 profit_threshold（默认1000U），说明刚经历大行情，市场随时可能反转
        - 立即将 u_futures_trading_enabled 设为 0，由用户手动重新开启

        Returns:
            True = 已触发熔断（调用方应停止本轮开仓）
        """
        # 每 check_interval_hours 小时检测一次，避免每次扫描都查询
        last_check = getattr(self, '_profit_guard_last_check', None)
        if last_check and (datetime.now() - last_check).total_seconds() < check_interval_hours * 3600:
            return False
        self._profit_guard_last_check = datetime.now()

        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # 使用MySQL NOW()而非Python UTC时间，避免时区不一致导致窗口计算错误
            cursor.execute("""
                SELECT COALESCE(SUM(realized_pnl), 0)
                FROM futures_positions
                WHERE status = 'closed' AND account_id = %s
                  AND close_time >= DATE_SUB(NOW(), INTERVAL %s HOUR)
            """, (self.account_id, window_hours))
            pnl_6h = float(cursor.fetchone()[0])

            logger.info(f"[PROFIT-GUARD] 过去{window_hours}h盈利: {pnl_6h:+.2f}U | 熔断阈值: {profit_threshold}U")

            if pnl_6h >= profit_threshold:
                cursor.execute("""
                    UPDATE system_settings
                    SET setting_value = '0',
                        description = CONCAT('盈利熔断自动禁止: 过去6h盈利=', %s, 'U，请手动重新开启'),
                        updated_at = NOW()
                    WHERE setting_key = 'u_futures_trading_enabled'
                """, (round(pnl_6h, 1),))
                cursor.close()
                logger.warning(
                    f"[PROFIT-GUARD] 盈利熔断触发! 过去{window_hours}h盈利={pnl_6h:+.1f}U "
                    f"超过阈值{profit_threshold}U => u_futures_trading_enabled=0，请手动重新开启"
                )
                _last_notified = getattr(self, '_profit_guard_notified_at', None)
                _cooldown_ok = (_last_notified is None or
                                (datetime.now() - _last_notified).total_seconds() >= 300)
                if _cooldown_ok and hasattr(self, 'telegram_notifier') and self.telegram_notifier:
                    try:
                        self.telegram_notifier.send_message(
                            f"🔴 【U本位盈利熔断】已触发\n\n"
                            f"过去{window_hours}h盈利: {pnl_6h:+.1f}U\n"
                            f"阈值: {profit_threshold}U\n"
                            f"U本位交易已自动停止，请手动重新开启"
                        )
                        self._profit_guard_notified_at = datetime.now()
                    except Exception:
                        pass
                return True

            cursor.close()
            self._profit_guard_notified_at = None  # 条件解除，重置冷却
            return False

        except Exception as e:
            logger.error(f"[PROFIT-GUARD] 盈利熔断检查失败: {e}")
            return False

    def _check_loss_and_auto_disable(self, loss_threshold=2000.0, window_hours=3, check_interval_hours=3) -> bool:
        """
        亏损熔断：统计窗口内总亏损超过阈值后自动禁止开仓

        逻辑：
        - 每 check_interval_hours 小时检测一次（默认3小时）
        - 检查最近 window_hours 小时已平仓PNL总和（默认3小时）
        - 若亏损超过 loss_threshold（默认2000U），自动禁止开仓
        - 立即将 u_futures_trading_enabled 设为 0，由用户手动重新开启

        Returns:
            True = 已触发熔断（调用方应停止本轮开仓）
        """
        last_check = getattr(self, '_loss_guard_last_check', None)
        if last_check and (datetime.now() - last_check).total_seconds() < check_interval_hours * 3600:
            return False
        self._loss_guard_last_check = datetime.now()

        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # 使用MySQL NOW()而非Python UTC时间，避免时区不一致导致窗口计算错误
            cursor.execute("""
                SELECT COALESCE(SUM(realized_pnl), 0)
                FROM futures_positions
                WHERE status = 'closed' AND account_id = %s
                  AND close_time >= DATE_SUB(NOW(), INTERVAL %s HOUR)
            """, (self.account_id, window_hours))
            pnl = float(cursor.fetchone()[0])

            logger.info(f"[LOSS-GUARD] 过去{window_hours}h盈亏: {pnl:+.2f}U | 亏损熔断阈值: -{loss_threshold}U")

            if pnl <= -loss_threshold:
                cursor.execute("""
                    UPDATE system_settings
                    SET setting_value = '0',
                        description = CONCAT('亏损熔断自动禁止: 过去3h亏损=', %s, 'U，请手动重新开启'),
                        updated_at = NOW()
                    WHERE setting_key = 'u_futures_trading_enabled'
                """, (round(pnl, 1),))
                cursor.close()
                logger.warning(
                    f"[LOSS-GUARD] 亏损熔断触发! 过去{window_hours}h亏损={pnl:+.1f}U "
                    f"超过阈值-{loss_threshold}U => u_futures_trading_enabled=0，请手动重新开启"
                )
                _last_notified = getattr(self, '_loss_guard_notified_at', None)
                _cooldown_ok = (_last_notified is None or
                                (datetime.now() - _last_notified).total_seconds() >= 300)
                if _cooldown_ok and hasattr(self, 'telegram_notifier') and self.telegram_notifier:
                    try:
                        self.telegram_notifier.send_message(
                            f"🔴 【U本位亏损熔断】已触发\n\n"
                            f"过去{window_hours}h亏损: {pnl:+.1f}U\n"
                            f"阈值: -{loss_threshold}U\n"
                            f"U本位交易已自动停止，请手动重新开启"
                        )
                        self._loss_guard_notified_at = datetime.now()
                    except Exception:
                        pass
                return True

            cursor.close()
            self._loss_guard_notified_at = None  # 条件解除，重置冷却
            return False

        except Exception as e:
            logger.error(f"[LOSS-GUARD] 亏损熔断检查失败: {e}")
            return False

    def get_big4_result(self):
        """
        获取Big4趋势结果 (实时检测模式)

        每次调用都会实时检测市场趋势，确保信号的时效性
        """
        try:
            result = self.big4_detector.detect_market_trend()
            logger.debug(f"🔱 Big4趋势实时检测 | {result['overall_signal']} (强度: {result['signal_strength']:.0f})")

            # 更新破位信号加权系统
            # BULLISH=看涨→LONG, BEARISH=看跌→SHORT
            direction_map = {'BULLISH': 'LONG', 'BEARISH': 'SHORT', 'NEUTRAL': 'NEUTRAL'}
            direction = direction_map.get(result['overall_signal'], 'NEUTRAL')
            if direction != 'NEUTRAL':
                self.breakout_booster.update_big4_breakout(
                    direction,
                    result['signal_strength']
                )
                logger.debug(f"💥 破位系统已更新: {direction} 强度{result['signal_strength']:.0f}")

            return result
        except Exception as e:
            logger.error(f"❌ Big4趋势检测失败: {e}")
            # 检测失败时返回中性结果
            return {
                'overall_signal': 'NEUTRAL',
                'signal_strength': 0,
                'details': {},
                'timestamp': datetime.now()
            }

    def get_current_price(self, symbol: str):
        """获取当前价格 - 仅使用WebSocket实时价，无可用价格时返回None拒绝开仓"""
        try:
            if self.ws_service:
                ws_price = self.ws_service.get_price(symbol)
                if ws_price and ws_price > 0:
                    logger.debug(f"[PRICE] {symbol} 使用WebSocket实时价: {ws_price}")
                    return ws_price
            logger.warning(f"[PRICE] {symbol} WebSocket价格不可用，拒绝返回价格")
        except Exception as e:
            logger.error(f"[ERROR] 获取{symbol}价格失败: {e}")
        return None

    def get_open_positions_count(self):
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) FROM futures_positions
                WHERE status IN ('open', 'building') AND account_id = %s
            """, (self.account_id,))
            result = cursor.fetchone()
            cursor.close()
            # DictCursor returns dict, plain cursor returns tuple
            db_count = (list(result.values())[0] if isinstance(result, dict) else result[0]) if result else 0
            # 加上后台采样中尚未写入DB的任务数，防止超限
            return int(db_count) + self._pending_entry_count
        except Exception as e:
            logger.warning(f"[POS-COUNT] 读取持仓数失败，仅返回pending计数: {e}")
            return self._pending_entry_count

    def has_position(self, symbol: str, side: str = None):
        """
        检查是否有持仓
        symbol: 交易对
        side: 方向(LONG/SHORT), None表示检查任意方向
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            if side:
                # 检查特定方向的持仓（包括正在建仓的持仓）
                cursor.execute("""
                    SELECT COUNT(*) FROM futures_positions
                    WHERE symbol = %s AND position_side = %s AND status IN ('open', 'building') AND account_id = %s
                """, (symbol, side, self.account_id))
            else:
                # 检查任意方向的持仓（包括正在建仓的持仓）
                cursor.execute("""
                    SELECT COUNT(*) FROM futures_positions
                    WHERE symbol = %s AND status IN ('open', 'building') AND account_id = %s
                """, (symbol, self.account_id))

            result = cursor.fetchone()
            cursor.close()
            cnt = (list(result.values())[0] if isinstance(result, dict) else result[0]) if result else 0
            return int(cnt) > 0
        except:
            return False

    def count_positions(self, symbol: str, side: str = None):
        """
        统计持仓数量
        symbol: 交易对
        side: 方向(LONG/SHORT), None表示统计任意方向
        Returns: 持仓数量
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            if side:
                # 统计特定方向的持仓数量
                cursor.execute("""
                    SELECT COUNT(*) FROM futures_positions
                    WHERE symbol = %s AND position_side = %s AND status IN ('open', 'building') AND account_id = %s
                """, (symbol, side, self.account_id))
            else:
                # 统计任意方向的持仓数量
                cursor.execute("""
                    SELECT COUNT(*) FROM futures_positions
                    WHERE symbol = %s AND status IN ('open', 'building') AND account_id = %s
                """, (symbol, self.account_id))

            result = cursor.fetchone()
            cursor.close()
            cnt = (list(result.values())[0] if isinstance(result, dict) else result[0]) if result else 0
            return int(cnt)
        except:
            return 0

    def _get_account_available_balance(self) -> float:
        """从DB读取账户可用余额"""
        try:
            conn = self._get_connection()
            cur = conn.cursor(pymysql.cursors.DictCursor)
            cur.execute(
                "SELECT current_balance, frozen_balance FROM futures_trading_accounts WHERE id=%s",
                (self.account_id,)
            )
            row = cur.fetchone()
            cur.close()
            if row:
                return float(row['current_balance']) - float(row['frozen_balance'] or 0)
        except Exception as e:
            logger.warning(f"[BALANCE] 读取余额失败，使用默认值: {e}")
        return 10000.0  # 安全兜底

    def _get_margin_per_batch(self, symbol: str, score: float = 75) -> float:
        """
        动态仓位计算：基于账户余额 + 信号评分
        base_pct 从 system_settings.position_size_pct 读取（默认 3%）
        score 65→1.0x 倍率，score 120→1.5x 倍率
        rating_level 1 → 50%，level 2 → 25%，level 3 → 禁止
        """
        rating_level = self.opt_config.get_symbol_rating_level(symbol)
        if rating_level >= 3:
            return 0.0

        # 从 system_settings 读取 base percentage（默认 3%）
        try:
            conn = self._get_connection()
            cur = conn.cursor(pymysql.cursors.DictCursor)
            cur.execute(
                "SELECT setting_value FROM system_settings WHERE setting_key='position_size_pct'",
            )
            row = cur.fetchone()
            cur.close()
            base_pct = float(row['setting_value']) if row else 0.03
        except Exception:
            base_pct = 0.03

        available = self._get_account_available_balance()

        # score 倍率：score=65 → 1.0x，score=120 → 1.5x（线性插值，上限1.5）
        score_mult = 1.0 + min(0.5, max(0.0, (score - 65) / 110))

        size = available * base_pct * score_mult

        # 绝对上下限
        min_size = 300.0
        max_size = available * 0.06  # 单笔不超过可用余额 6%
        size = max(min_size, min(max_size, size))

        # 评级折减
        if rating_level == 1:
            size *= 0.50
        elif rating_level == 2:
            size *= 0.25

        logger.debug(
            f"[SIZING] {symbol} score={score:.0f} available={available:.0f} "
            f"base_pct={base_pct:.1%} mult={score_mult:.2f} size={size:.0f} level={rating_level}"
        )
        return round(size, 2)

    def validate_signal_timeframe(self, signal_components: dict, side: str, symbol: str) -> tuple:
        """
        验证信号组合的时间框架一致性

        Returns:
            (is_valid, reason) - 是否有效,原因描述
        """
        if not signal_components:
            return True, "无信号组件"

        # 提取趋势信号（trend_1d_bull/bear已移除，不再生成，无需检查）
        has_1h_bull = 'trend_1h_bull' in signal_components
        has_1h_bear = 'trend_1h_bear' in signal_components

        # 规则1: 做多时,1小时必须不能看跌
        if side == 'LONG' and has_1h_bear:
            return False, "时间框架冲突: 做多但1H看跌"

        # 规则2: 做空时,1小时必须不能看涨
        if side == 'SHORT' and has_1h_bull:
            return False, "时间框架冲突: 做空但1H看涨"

        return True, "时间框架一致"

    def _load_max_positions(self) -> int:
        """从 system_settings 读取 max_positions，失败时返回默认值 50"""
        try:
            conn = self._get_connection()
            cur = conn.cursor(pymysql.cursors.DictCursor)
            cur.execute("SELECT setting_value FROM system_settings WHERE setting_key='max_positions'")
            row = cur.fetchone()
            cur.close()
            if row:
                return int(float(row['setting_value']))
        except Exception as e:
            logger.warning(f"[MAX-POS] 读取max_positions失败，使用默认值50: {e}")
        return 50

    def _get_sl_tp_from_settings(self):
        """从 system_settings 读取止损/止盈比例，失败时返回默认值 2%/5%"""
        try:
            conn = self._get_connection()
            cur = conn.cursor(pymysql.cursors.DictCursor)
            cur.execute("SELECT setting_key, setting_value FROM system_settings WHERE setting_key IN ('stop_loss_pct','take_profit_pct')")
            rows = {r['setting_key']: r['setting_value'] for r in cur.fetchall()}
            cur.close()
            sl = float(rows.get('stop_loss_pct', 0.02))
            tp = float(rows.get('take_profit_pct', 0.03))
            return sl, tp
        except Exception as e:
            logger.warning(f"[SL/TP] 读取system_settings失败，使用默认值: {e}")
            return 0.02, 0.03

    def calculate_volatility_adjusted_stop_loss(self, signal_components: dict, base_stop_loss_pct: float) -> float:
        """
        根据波动率调整止损百分比

        Args:
            signal_components: 信号组件
            base_stop_loss_pct: 基础止损百分比(如0.02)

        Returns:
            调整后的止损百分比
        """
        # 检查是否包含高波动信号
        has_high_volatility = 'volatility_high' in signal_components

        if has_high_volatility:
            # 高波动环境: 扩大止损到1.5倍(2% -> 3%)
            adjusted_sl = base_stop_loss_pct * 1.5
            logger.info(f"[VOLATILITY_ADJUST] 高波动环境,止损从{base_stop_loss_pct*100:.1f}%扩大到{adjusted_sl*100:.1f}%")
            return adjusted_sl

        return base_stop_loss_pct

    def validate_position_high_signal(self, symbol: str, signal_components: dict, side: str) -> tuple:
        """
        缺陷2修复: 增强position_high信号验证

        position_high单独不足以确认顶部,需要额外确认:
        1. 更长周期的位置检查(7天而非3天)
        2. 涨幅是否已经放缓(连续上影线)
        3. 是否有momentum_up信号(避免加速上涨时做空)

        Returns:
            (is_valid, reason)
        """
        # 只检查包含position_high的做空信号
        if side != 'SHORT' or 'position_high' not in signal_components:
            return True, "不是position_high做空"

        try:
            # 检查1: 是否伴随momentum_up(涨势)信号
            # 如果价格还在上涨3%+,说明动能未衰竭,不适合做空
            has_momentum_up = 'momentum_up_3pct' in signal_components
            if has_momentum_up:
                return False, "position_high但伴随momentum_up_3pct,动能未衰竭"

            # 检查2: 加载最近的K线,检查是否有顶部特征
            klines_1h = self.brain.load_klines(symbol, '1h', 24)
            if len(klines_1h) < 10:
                return True, "K线数据不足,跳过验证"

            # 计算最近10根K线的上影线比例
            recent_10 = klines_1h[-10:]
            upper_shadow_count = 0
            for k in recent_10:
                body_high = max(k['open'], k['close'])
                upper_shadow = k['high'] - body_high
                body_size = abs(k['close'] - k['open'])

                # 上影线 > 实体的50% 认为是上影线K线
                if body_size > 0 and upper_shadow / body_size > 0.5:
                    upper_shadow_count += 1

            upper_shadow_ratio = upper_shadow_count / 10

            # 如果最近10根K线上影线<30%,说明买盘还很强,不适合做空
            if upper_shadow_ratio < 0.3:
                return False, f"position_high但上影线比例仅{upper_shadow_ratio*100:.0f}%,买盘未衰竭"

            # 缺陷4修复: 检查成交量是否萎缩(顶部特征)
            recent_5 = klines_1h[-5:]
            earlier_5 = klines_1h[-10:-5]

            recent_volume = sum([float(k.get('volume', 0)) for k in recent_5])
            earlier_volume = sum([float(k.get('volume', 0)) for k in earlier_5])

            if recent_volume > 0 and earlier_volume > 0:
                volume_ratio = recent_volume / earlier_volume

                # 如果最近5根K线成交量 > 之前5根的1.2倍,说明成交量在放大,不是顶部
                if volume_ratio > 1.2:
                    return False, f"position_high但成交量放大{volume_ratio:.2f}倍,非顶部特征"

                logger.info(f"[VOLUME_CHECK] {symbol} 成交量比例{volume_ratio:.2f},符合顶部萎缩特征")

            logger.info(f"[POSITION_HIGH_VALID] {symbol} 上影线{upper_shadow_ratio*100:.0f}%,顶部特征明显")
            return True, "position_high验证通过"

        except Exception as e:
            logger.warning(f"[POSITION_HIGH_CHECK] {symbol} 验证失败: {e},默认通过")
            return True, "验证异常,默认通过"

    def open_position(self, opp: dict):
        """开仓 - 支持做多和做空，支持分批建仓，使用 WebSocket 实时价格"""
        symbol = opp['symbol']
        side = opp['side']  # 'LONG' 或 'SHORT'
        strategy = opp.get('strategy', 'default')  # 获取策略类型

        # ========== 第零步：验证symbol格式 ==========
        # U本位服务只应该交易 /USDT 交易对
        if symbol.endswith('/USD') and not symbol.endswith('/USDT'):
            logger.error(f"[SYMBOL_ERROR] {symbol} 是币本位交易对(/USD),不应在U本位服务开仓,已拒绝")
            return False

        if not symbol.endswith('/USDT'):
            logger.error(f"[SYMBOL_ERROR] {symbol} 格式错误,U本位服务只支持/USDT交易对,已拒绝")
            return False

        # ========== 第一步：验证信号（无论用哪种开仓方式都要验证） ==========
        signal_components = opp.get('signal_components', {})

        # 缺陷1修复: 验证时间框架一致性
        is_valid, reason = self.validate_signal_timeframe(signal_components, side, symbol)
        if not is_valid:
            logger.warning(f"[SIGNAL_REJECT] {symbol} {side} - {reason}")
            return False

        # 缺陷2修复: position_high信号额外验证
        is_valid, reason = self.validate_position_high_signal(symbol, signal_components, side)
        if not is_valid:
            logger.warning(f"[SIGNAL_REJECT] {symbol} {side} - {reason}")
            return False

        # 新增验证: 检查是否在平仓后冷却期内(1小时)
        if self.check_recent_close(symbol, side, cooldown_minutes=15):
            logger.warning(f"[SIGNAL_REJECT] {symbol} {side} - 平仓后15分钟冷却期内")
            return False

        # 🧠 生物学优化: 动作电位不应期（开仓后2小时内不再同向开仓）
        # 例外: 紧急反弹信号(Big4触底)不受此限制，触底是强信号
        if opp.get('signal_type') != 'EMERGENCY_BOUNCE':
            if self.check_recent_open(symbol, side, cooldown_minutes=120):
                logger.warning(f"[REFRACTORY] {symbol} {side} - 开仓不应期内(2小时)，拒绝重复开仓")
                return False

        # 🧠 生物学优化: 突触习惯化（连续亏损提高开仓门槛）
        loss_multiplier = self.get_symbol_loss_multiplier(symbol, side)
        if loss_multiplier < 1.0:
            score = opp.get('score', 0)
            effective_threshold = round(60 / loss_multiplier)
            if score < effective_threshold:
                consecutive = round((1.0 - loss_multiplier) / 0.1) + 1
                logger.warning(
                    f"[HABITUATION] {symbol} {side} - 连续{consecutive}次亏损，"
                    f"有效阈值{effective_threshold}分，当前{score}分不足，拒绝开仓"
                )
                return False

        # 🧠 确认偏误防御：反向信号≥3个时拒绝开仓（说明市场本身存在分歧，非一致看涨/跌）
        counter_signals = opp.get('counter_signals', 0)
        if counter_signals >= 3:
            logger.warning(
                f"[CONFIRMATION_BIAS] {symbol} {side} - {counter_signals}个反向信号存在，"
                f"市场存在明显分歧，拒绝开仓（确认偏误防御）"
            )
            return False

        # 🧠 群体极化防御-资金费率：资金费率极端时拒绝同向开仓（机构已经完成布局，散户追涨正在被割）
        # LONG时资金费率>0.1%（散户过度看多，多头爆仓风险大）→ 拒绝
        # SHORT时资金费率<-0.1%（散户过度看空，空头爆仓风险大）→ 拒绝
        funding_rate_pct = self.get_funding_rate_pct(symbol)
        if funding_rate_pct is not None:
            if side == 'LONG' and funding_rate_pct > 0.1:
                logger.warning(
                    f"[FUNDING_VETO] {symbol} {side} - 资金费率{funding_rate_pct:.3f}%>0.1%，"
                    f"散户过度看多，拒绝追多（群体极化防御）"
                )
                return False
            elif side == 'SHORT' and funding_rate_pct < -0.1:
                logger.warning(
                    f"[FUNDING_VETO] {symbol} {side} - 资金费率{funding_rate_pct:.3f}%<-0.1%，"
                    f"散户过度看空，拒绝追空（群体极化防御）"
                )
                return False

        # 新增验证: 检查交易方向是否允许
        if not self.opt_config.is_direction_allowed(side):
            direction_name = "做多" if side == "LONG" else "做空"
            logger.warning(f"[SIGNAL_REJECT] {symbol} {side} - 系统已禁止{direction_name}")
            return False

        # 🔥 Top 30过滤：仅实盘开启时生效，模拟盘无限制
        try:
            conn_t30 = self._get_connection()
            cur_t30 = conn_t30.cursor()
            cur_t30.execute("SELECT setting_value FROM system_settings WHERE setting_key='live_trading_enabled'")
            row_t30 = cur_t30.fetchone()
            cur_t30.close()
            conn_t30.close()
            live_enabled = row_t30 and str(row_t30.get('setting_value', '0')) in ('1', 'true')
        except Exception:
            live_enabled = False
        if live_enabled and not self.is_symbol_in_top_performers(symbol):
            logger.warning(f"[SIGNAL_REJECT] {symbol} {side} - 实盘模式：不在盈利Top 30交易对中")
            return False

        # 🔥 V5.1优化: 移除防追高/防杀跌过滤
        # 原因: Big4触底检测已提供全局保护（禁止做空2小时）
        # 防杀跌过滤容易误杀破位追空信号，与Big4机制冲突
        # 移除日期: 2026-02-09

        # ========== 第二步：提前检查黑名单 ==========
        rating_level = self.opt_config.get_symbol_rating_level(symbol)
        if rating_level == 3:
            logger.warning(f"[BLACKLIST_LEVEL3] {symbol} 已被永久禁止交易")
            return False

        # ========== 第三步：价格采样建仓（15分钟采样找最优点，一次性开仓） ==========
        if self.smart_entry_executor and self.event_loop:
            try:
                # 准备信号字典
                signal = {
                    'symbol': symbol,
                    'direction': side,
                    'leverage': self.leverage,
                    'signal_time': datetime.now(),
                    'strategy_id': 'smart_trader_v1',
                    'trade_params': {
                        'entry_score': opp.get('score', 0),
                        'signal_components': opp.get('signal_components', {}),
                        'signal_combination_key': self._generate_signal_combination_key(opp.get('signal_components', {}))
                    }
                }

                # 在事件循环中创建异步任务（后台执行）
                self._pending_entry_count += 1

                async def _run_entry_and_release(sig):
                    try:
                        await self.smart_entry_executor.execute_entry(sig)
                    finally:
                        self._pending_entry_count = max(0, self._pending_entry_count - 1)

                asyncio.run_coroutine_threadsafe(
                    _run_entry_and_release(signal),
                    self.event_loop
                )

                logger.info(f"[V1-PRICE-SAMPLING] {symbol} {side} 价格采样建仓任务已启动 (15分钟采样，一次性开仓，pending={self._pending_entry_count})")
                logger.info(f"   📝 信号评分: {opp.get('score', 0)} | 信号组合: {signal['trade_params']['signal_combination_key']}")
                return True

            except Exception as e:
                logger.error(f"❌ [V1-PRICE-SAMPLING-ERROR] {symbol} 启动采样任务失败: {e}")
                import traceback
                logger.error(traceback.format_exc())
                return False  # 避免继续执行回退策略造成重复下单

        # ========== 回退策略：一次性直接开仓（执行器不可用时使用）==========
        try:

            # 优先从 WebSocket 获取实时价格
            current_price = self.ws_service.get_price(symbol)

            # 如果 WebSocket 价格不可用，回退到 Binance REST API 实时价
            if not current_price or current_price <= 0:
                logger.warning(f"[WS_FALLBACK] {symbol} WebSocket价格不可用，回退到Binance REST API")
                current_price = self.get_current_price(symbol)
                if not current_price:
                    logger.error(f"{symbol} 无法获取价格")
                    return False
                price_source = "REST"
            else:
                price_source = "WS"

            # 检查是否为反转开仓(使用原仓位保证金)
            is_reversal = 'reversal_from' in opp
            rating_level = 0  # 默认白名单
            is_hedge = False  # 默认非对冲
            adjusted_position_size = None  # 初始化变量,避免UnboundLocalError

            if is_reversal and 'original_margin' in opp:
                # 反转开仓: 使用原仓位相同的保证金
                adjusted_position_size = opp['original_margin']
                logger.info(f"[REVERSAL_MARGIN] {symbol} 反转开仓, 使用原仓位保证金: ${adjusted_position_size:.2f}")

                # 仍需获取自适应参数用于止损止盈
                if side == 'LONG':
                    adaptive_params = self.brain.adaptive_long
                else:  # SHORT
                    adaptive_params = self.brain.adaptive_short

                # 反转开仓也需要检查评级(用于日志显示)
                rating_level = self.opt_config.get_symbol_rating_level(symbol)

            if not is_reversal or 'original_margin' not in opp:
                # 正常开仓流程：动态仓位，基于余额 + 信号评分
                _entry_score_for_sizing = float(opp.get('score', 75))
                margin_per_batch = self._get_margin_per_batch(symbol, score=_entry_score_for_sizing)

                # Level 3 = 永久禁止
                if margin_per_batch == 0:
                    logger.warning(f"[BLACKLIST_LEVEL3] {symbol} 已被永久禁止交易 (Level{rating_level})")
                    return False

                # 低分非主流LONG仓位减半：score≤65 且非主流币 → 200U，降低踩雷风险
                _entry_score = opp.get('score', 100)
                _MAINSTREAM = {
                    'BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'SOL/USDT', 'XRP/USDT',
                    'ADA/USDT', 'DOGE/USDT', 'AVAX/USDT', 'DOT/USDT', 'LINK/USDT',
                    'UNI/USDT', 'LTC/USDT', 'ATOM/USDT', 'ETC/USDT', 'APT/USDT',
                    'SUI/USDT', 'OP/USDT', 'ARB/USDT', 'NEAR/USDT', 'INJ/USDT',
                }
                if side == 'LONG' and _entry_score <= 65 and symbol not in _MAINSTREAM and margin_per_batch > 200:
                    logger.info(
                        f"[LOW_SCORE_GUARD] {symbol} LONG score={_entry_score}≤65 非主流币，"
                        f"仓位 ${margin_per_batch:.0f} → $200"
                    )
                    margin_per_batch = 200.0

                # 记录评级信息
                rating_tag = f"[Level{rating_level}]" if rating_level > 0 else ""
                logger.info(f"{rating_tag} {symbol} 固定保证金: ${margin_per_batch:.2f}")

                # ========== 检查是否为震荡市策略 ==========
                mode_config = None
                if strategy == 'bollinger_mean_reversion':
                    try:
                        mode_config = self.mode_switcher.get_current_mode(self.account_id, 'usdt_futures')
                        if mode_config:
                            logger.info(f"[RANGE_MODE] {symbol} 使用震荡市交易参数")
                            # 震荡市模式使用固定保证金的60%
                            base_position_size = margin_per_batch * 0.6
                            logger.info(f"[RANGE_POSITION] {symbol} 震荡市仓位: ${base_position_size:.2f} (60%)")
                        else:
                            base_position_size = margin_per_batch
                    except Exception as e:
                        logger.error(f"[MODE_ERROR] 获取模式配置失败: {e}")
                        base_position_size = margin_per_batch
                else:
                    # 趋势模式: 使用完整固定保证金
                    base_position_size = margin_per_batch

                # 根据Big4市场信号动态调整仓位倍数 (震荡市策略不调整仓位)
                if strategy == 'bollinger_mean_reversion':
                    position_multiplier = 1.0
                    logger.info(f"[RANGE_MODE] {symbol} 震荡市策略不使用Big4仓位调整")
                else:
                    try:
                        big4_result = self.get_big4_result()
                        market_signal = big4_result.get('overall_signal', 'NEUTRAL')

                        # 根据市场信号决定仓位倍数
                        if market_signal == 'BULLISH' and side == 'LONG':
                            position_multiplier = 1.2  # 市场看多,做多加仓
                            logger.info(f"[BIG4-POSITION] {symbol} 市场看多,做多仓位 × 1.2")
                        elif market_signal == 'BEARISH' and side == 'SHORT':
                            position_multiplier = 1.2  # 市场看空,做空加仓
                            logger.info(f"[BIG4-POSITION] {symbol} 市场看空,做空仓位 × 1.2")
                        else:
                            position_multiplier = 1.0  # 其他情况正常仓位
                            if market_signal != 'NEUTRAL':
                                logger.info(f"[BIG4-POSITION] {symbol} 逆势信号,仓位 × 1.0 (市场{market_signal}, 开仓{side})")
                    except Exception as e:
                        logger.warning(f"[BIG4-POSITION] 获取市场信号失败,使用默认仓位倍数1.0: {e}")
                        position_multiplier = 1.0

                # 获取自适应参数
                if side == 'LONG':
                    adaptive_params = self.brain.adaptive_long
                else:  # SHORT
                    adaptive_params = self.brain.adaptive_short

                # 应用仓位倍数
                adjusted_position_size = base_position_size * position_multiplier

            quantity = adjusted_position_size * self.leverage / current_price
            notional_value = quantity * current_price
            margin = adjusted_position_size

            # ========== 根据策略类型确定止损止盈 ==========
            if strategy == 'bollinger_mean_reversion' and 'take_profit_price' in opp and 'stop_loss_price' in opp:
                # 震荡市策略: 使用策略提供的具体价格
                stop_loss = opp['stop_loss_price']
                take_profit = opp['take_profit_price']

                # 计算实际百分比用于日志
                if side == 'LONG':
                    stop_loss_pct = (current_price - stop_loss) / current_price
                    take_profit_pct = (take_profit - current_price) / current_price
                else:  # SHORT
                    stop_loss_pct = (stop_loss - current_price) / current_price
                    take_profit_pct = (current_price - take_profit) / current_price

                logger.info(f"[RANGE_TP_SL] {symbol} 使用布林带策略止盈止损: TP=${take_profit:.4f}({take_profit_pct*100:.2f}%), SL=${stop_loss:.4f}({stop_loss_pct*100:.2f}%)")

            else:
                # 从 system_settings 读取止损止盈比例
                stop_loss_pct, take_profit_pct = self._get_sl_tp_from_settings()

                if side == 'LONG':
                    stop_loss = current_price * (1 - stop_loss_pct)
                    take_profit = current_price * (1 + take_profit_pct)
                else:  # SHORT
                    stop_loss = current_price * (1 + stop_loss_pct)
                    take_profit = current_price * (1 - take_profit_pct)

            logger.info(f"[OPEN] {symbol} {side} | 价格: ${current_price:.4f} ({price_source}) | 数量: {quantity:.2f}")

            conn = self._get_connection()
            cursor = conn.cursor()

            # 准备信号组成数据
            import json
            signal_components = opp.get('signal_components', {})
            logger.info(f"[DEBUG] signal_components: {signal_components}, has key: {'signal_components' in opp}")
            signal_components_json = json.dumps(signal_components) if signal_components else None
            entry_score = opp.get('score', 0)

            # 生成信号组合键 (按字母顺序排序信号名称)
            if signal_components:
                sorted_signals = sorted(signal_components.keys())
                signal_combination_key = " + ".join(sorted_signals)
            else:
                # 如果是震荡市策略但缺少signal_components（兼容旧版本）
                if strategy == 'bollinger_mean_reversion':
                    signal_combination_key = "range_trading"
                else:
                    signal_combination_key = "unknown"

            # 检查是否为反转信号
            if is_reversal:
                signal_combination_key = f"REVERSAL_{opp.get('reversal_from', 'unknown')}"

            # 震荡市策略特殊标记（如果还没有RANGE前缀）
            if strategy == 'bollinger_mean_reversion' and not signal_combination_key.startswith('RANGE_'):
                signal_combination_key = f"RANGE_{signal_combination_key}"

            logger.info(f"[SIGNAL_COMBO] {symbol} {side} 信号组合: {signal_combination_key} (评分: {entry_score}) 策略: {strategy}")

            # Big4 信号记录
            if opp.get('big4_adjusted'):
                big4_signal = opp.get('big4_signal', 'NEUTRAL')
                big4_strength = opp.get('big4_strength', 0)
                logger.info(f"[BIG4-APPLIED] {symbol} Big4趋势: {big4_signal} (强度: {big4_strength})")

            # ========== 根据策略类型确定超时时间 ==========
            if strategy == 'bollinger_mean_reversion' and mode_config:
                # 震荡市策略: 使用range_max_hold_hours (默认4小时)
                range_max_hold_hours = int(mode_config.get('range_max_hold_hours', 4))  # 转换Decimal为int
                base_timeout_minutes = range_max_hold_hours * 60
                logger.info(f"[RANGE_TIMEOUT] {symbol} 震荡市最大持仓时间: {base_timeout_minutes}分钟")
            else:
                # 趋势模式: 实时从DB读取 max_hold_hours（无需重启即可生效，不参考config.yaml）
                _mh_val = self.opt_config._read_system_setting('max_hold_hours')
                _mh_hours = max(3, min(8, int(_mh_val))) if _mh_val else self.max_hold_minutes // 60
                base_timeout_minutes = _mh_hours * 60

            # 计算超时时间点 (UTC时间)
            timeout_at = datetime.now() + timedelta(minutes=base_timeout_minutes)

            # 准备entry_reason
            entry_reason = opp.get('reason', '')
            if strategy == 'bollinger_mean_reversion':
                entry_reason = f"[震荡市] {entry_reason}"

            # 策略模式：signal_confirm / trend_follow，fallback smart_trader（兼容旧路径）
            position_source = opp.get('strategy_mode', 'smart_trader')

            # 插入持仓记录 (包含动态超时字段和计划平仓时间)
            cursor.execute("""
                INSERT INTO futures_positions
                (account_id, symbol, position_side, quantity, entry_price,
                 leverage, notional_value, margin, open_time, stop_loss_price, take_profit_price,
                 entry_signal_type, entry_reason, entry_score, signal_components, max_hold_minutes, timeout_at,
                 planned_close_time, source, status, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s,
                        DATE_ADD(NOW(), INTERVAL %s MINUTE), %s, 'open', NOW(), NOW())
            """, (
                self.account_id, symbol, side, quantity, current_price, self.leverage,
                notional_value, margin, stop_loss, take_profit,
                signal_combination_key, entry_reason, entry_score, signal_components_json,
                base_timeout_minutes, timeout_at,
                base_timeout_minutes,  # planned_close_time = NOW() + max_hold_minutes
                position_source
            ))

            # 获取持仓ID
            position_id = cursor.lastrowid

            # 🔥 账户余额改为定时计算，避免并发更新死锁

            cursor.close()

            # 显示实际使用的止损止盈百分比
            sl_pct = f"-{stop_loss_pct*100:.1f}%" if side == 'LONG' else f"+{stop_loss_pct*100:.1f}%"
            tp_pct = f"+{take_profit_pct*100:.1f}%" if side == 'LONG' else f"-{take_profit_pct*100:.1f}%"

            # 显示评级和对冲标签
            if rating_level == 0:
                rating_tag = ""
            elif rating_level == 1:
                rating_tag = " [黑名单L1-25%]"
            elif rating_level == 2:
                rating_tag = " [黑名单L2-12.5%]"
            else:
                rating_tag = " [黑名单L3-禁止]"

            hedge_tag = " [对冲]" if is_hedge else ""

            # 格式化信号组合显示(显示各信号的分数)
            if signal_components:
                signal_details = ", ".join([f"{k}:{v}" for k, v in sorted(signal_components.items(), key=lambda x: x[1], reverse=True)])
            else:
                signal_details = "无"

            logger.info(
                f"[SUCCESS] {symbol} {side}开仓成功{rating_tag}{hedge_tag} | "
                f"信号: [{signal_combination_key}] | "
                f"止损: ${stop_loss:.4f} ({sl_pct}) | 止盈: ${take_profit:.4f} ({tp_pct}) | "
                f"仓位: ${margin:.0f} | 超时: {base_timeout_minutes}分钟"
            )
            logger.info(f"[SIGNAL_DETAIL] {symbol} 信号详情: {signal_details}")

            # 启动智能平仓监控（统一平仓入口）
            if self.smart_exit_optimizer and self.event_loop:
                try:
                    asyncio.run_coroutine_threadsafe(
                        self.smart_exit_optimizer.start_monitoring_position(position_id),
                        self.event_loop
                    )
                    logger.info(f"✅ 持仓{position_id}已加入智能平仓监控")
                except Exception as e:
                    logger.error(f"❌ 持仓{position_id}启动监控失败: {e}")

            return True

        except Exception as e:
            logger.error(f"[ERROR] {symbol} 开仓失败: {e}")
            return False

    def _generate_signal_combination_key(self, signal_components: dict) -> str:
        """生成信号组合键"""
        if signal_components:
            sorted_signals = sorted(signal_components.keys())
            return " + ".join(sorted_signals)
        else:
            return "unknown"

    def check_top_bottom(self, symbol: str, position_side: str, entry_price: float):
        """智能识别顶部和底部 - 使用1h K线更稳健的判断"""
        try:
            # 使用1小时K线分析（更稳健，减少假信号）
            klines_1h = self.brain.load_klines(symbol, '1h', 48)
            if len(klines_1h) < 24:
                return False, None

            current = klines_1h[-1]
            recent_24 = klines_1h[-24:]  # 最近24小时
            recent_12 = klines_1h[-12:]  # 最近12小时

            if position_side == 'LONG':
                # 做多持仓 - 寻找顶部信号

                # 1. 价格在最近12小时创新高后回落
                max_high = max(k['high'] for k in recent_12)
                max_high_idx = len(recent_12) - 1 - [k['high'] for k in reversed(recent_12)].index(max_high)
                is_peak = max_high_idx < 10  # 高点在前10根K线，现在回落

                # 2. 当前价格已经从高点回落（1h级别阈值提高到1.5%）
                current_price = current['close']
                pullback_pct = (max_high - current_price) / max_high * 100

                # 3. 最近4根1h K线趋势确认：至少3根收阴或长上影线
                recent_4 = klines_1h[-4:]
                bearish_count = sum(1 for k in recent_4 if k['close'] < k['open'])
                long_upper_shadow = sum(1 for k in recent_4 if (k['high'] - max(k['open'], k['close'])) > abs(k['close'] - k['open']) * 1.5)

                # 4. 成交量确认：最近3根K线成交量放大
                if len(recent_24) >= 24:
                    avg_volume_24h = sum(k['volume'] for k in recent_24[:21]) / 21
                    recent_3_volume = sum(k['volume'] for k in klines_1h[-3:]) / 3
                    volume_surge = recent_3_volume > avg_volume_24h * 1.2
                else:
                    volume_surge = True  # 数据不足时忽略成交量确认

                # 见顶判断条件（更严格）
                if is_peak and pullback_pct >= 1.5 and (bearish_count >= 3 or long_upper_shadow >= 2):
                    # 计算当前盈利
                    profit_pct = (current_price - entry_price) / entry_price * 100
                    return True, f"TOP_DETECTED(高点回落{pullback_pct:.1f}%,盈利{profit_pct:+.1f}%)"

            elif position_side == 'SHORT':
                # 做空持仓 - 寻找底部信号

                # 1. 价格在最近12小时创新低后反弹
                min_low = min(k['low'] for k in recent_12)
                min_low_idx = len(recent_12) - 1 - [k['low'] for k in reversed(recent_12)].index(min_low)
                is_bottom = min_low_idx < 10  # 低点在前10根K线，现在反弹

                # 2. 当前价格已经从低点反弹（1h级别阈值提高到1.5%）
                current_price = current['close']
                bounce_pct = (current_price - min_low) / min_low * 100

                # 3. 最近4根1h K线趋势确认：至少3根收阳或长下影线
                recent_4 = klines_1h[-4:]
                bullish_count = sum(1 for k in recent_4 if k['close'] > k['open'])
                long_lower_shadow = sum(1 for k in recent_4 if (min(k['open'], k['close']) - k['low']) > abs(k['close'] - k['open']) * 1.5)

                # 4. 成交量确认：最近3根K线成交量放大
                if len(recent_24) >= 24:
                    avg_volume_24h = sum(k['volume'] for k in recent_24[:21]) / 21
                    recent_3_volume = sum(k['volume'] for k in klines_1h[-3:]) / 3
                    volume_surge = recent_3_volume > avg_volume_24h * 1.2
                else:
                    volume_surge = True  # 数据不足时忽略成交量确认

                # 见底判断条件（更严格）
                if is_bottom and bounce_pct >= 1.5 and (bullish_count >= 3 or long_lower_shadow >= 2):
                    # 计算当前盈利
                    profit_pct = (entry_price - current_price) / entry_price * 100
                    return True, f"BOTTOM_DETECTED(低点反弹{bounce_pct:.1f}%,盈利{profit_pct:+.1f}%)"

            return False, None

        except Exception as e:
            logger.error(f"[ERROR] {symbol} 顶底识别失败: {e}")
            return False, None

    # ========== 以下方法已废弃，平仓逻辑已统一到SmartExitOptimizer ==========
    # check_stop_loss_take_profit() 和 close_old_positions() 已被移除
    # 所有平仓逻辑现在由 SmartExitOptimizer 统一处理


    def check_hedge_positions(self):
        """检查并处理对冲持仓 - 平掉亏损方向"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor(pymysql.cursors.DictCursor)  # 使用字典游标

            # 1. 找出所有存在对冲的交易对
            cursor.execute("""
                SELECT
                    symbol,
                    SUM(CASE WHEN position_side = 'LONG' THEN 1 ELSE 0 END) as long_count,
                    SUM(CASE WHEN position_side = 'SHORT' THEN 1 ELSE 0 END) as short_count
                FROM futures_positions
                WHERE status = 'open' AND account_id = %s
                GROUP BY symbol
                HAVING long_count > 0 AND short_count > 0
            """, (self.account_id,))

            hedge_pairs = cursor.fetchall()

            if not hedge_pairs:
                return

            logger.info(f"[HEDGE] 发现 {len(hedge_pairs)} 个对冲交易对")

            # 2. 处理每个对冲交易对
            for pair in hedge_pairs:
                symbol = pair['symbol']

                # 获取该交易对的所有持仓
                cursor.execute("""
                    SELECT id, position_side, entry_price, quantity, open_time
                    FROM futures_positions
                    WHERE symbol = %s AND status = 'open' AND account_id = %s
                    ORDER BY position_side, open_time
                """, (symbol, self.account_id))

                positions = cursor.fetchall()

                if len(positions) < 2:
                    continue

                # 获取当前价格
                current_price = self.get_current_price(symbol)
                if not current_price:
                    continue

                # 计算每个持仓的盈亏
                long_positions = []
                short_positions = []

                for pos in positions:
                    entry_price = float(pos['entry_price'])
                    quantity = float(pos['quantity'])

                    if pos['position_side'] == 'LONG':
                        pnl_pct = (current_price - entry_price) / entry_price * 100
                        realized_pnl = (current_price - entry_price) * quantity
                        long_positions.append({
                            'id': pos['id'],
                            'entry_price': entry_price,
                            'quantity': quantity,
                            'pnl_pct': pnl_pct,
                            'realized_pnl': realized_pnl,
                            'open_time': pos['open_time']
                        })
                    else:  # SHORT
                        pnl_pct = (entry_price - current_price) / entry_price * 100
                        realized_pnl = (entry_price - current_price) * quantity
                        short_positions.append({
                            'id': pos['id'],
                            'entry_price': entry_price,
                            'quantity': quantity,
                            'pnl_pct': pnl_pct,
                            'realized_pnl': realized_pnl,
                            'open_time': pos['open_time']
                        })

                # 策略1: 如果一方亏损>1%且另一方盈利,平掉亏损方
                for long_pos in long_positions:
                    for short_pos in short_positions:
                        # LONG亏损>1%, SHORT盈利 -> 平掉LONG
                        if long_pos['pnl_pct'] < -1 and short_pos['pnl_pct'] > 0:
                            logger.info(
                                f"[HEDGE_CLOSE] {symbol} LONG亏损{long_pos['pnl_pct']:.2f}% ({long_pos['realized_pnl']:+.2f} USDT), "
                                f"SHORT盈利{short_pos['pnl_pct']:.2f}% -> 平掉LONG"
                            )

                            # Get leverage and margin
                            cursor.execute("""
                                SELECT leverage, margin FROM futures_positions WHERE id = %s
                            """, (long_pos['id'],))
                            pos_detail = cursor.fetchone()
                            leverage = pos_detail['leverage'] if pos_detail else 1
                            margin = float(pos_detail['margin']) if pos_detail else 0.0
                            roi = (long_pos['realized_pnl'] / margin) * 100 if margin > 0 else 0

                            cursor.execute("""
                                UPDATE futures_positions
                                SET status = 'closed', mark_price = %s,
                                    realized_pnl = %s,
                                    close_time = NOW(), updated_at = NOW(),
                                    notes = CONCAT(IFNULL(notes, ''), '|hedge_loss_cut')
                                WHERE id = %s
                            """, (current_price, long_pos['realized_pnl'], long_pos['id']))

                            # Calculate values for orders and trades
                            import uuid
                            notional_value = current_price * long_pos['quantity']
                            fee = notional_value * 0.0004
                            order_id = f"HEDGE-{long_pos['id']}"
                            trade_id = str(uuid.uuid4())

                            # Create futures_orders record for close reason
                            cursor.execute("""
                                INSERT INTO futures_orders (
                                    account_id, order_id, position_id, symbol,
                                    side, order_type, leverage,
                                    price, quantity, executed_quantity,
                                    total_value, executed_value,
                                    fee, fee_rate, status,
                                    avg_fill_price, fill_time,
                                    realized_pnl, pnl_pct,
                                    order_source, notes
                                ) VALUES (
                                    %s, %s, %s, %s,
                                    %s, 'MARKET', %s,
                                    %s, %s, %s,
                                    %s, %s,
                                    %s, %s, 'FILLED',
                                    %s, %s,
                                    %s, %s,
                                    'smart_trader', %s
                                )
                            """, (
                                self.account_id, order_id, long_pos['id'], symbol,
                                'CLOSE_LONG', leverage,
                                current_price, long_pos['quantity'], long_pos['quantity'],
                                notional_value, notional_value,
                                fee, 0.0004,
                                current_price, datetime.now(),
                                long_pos['realized_pnl'], long_pos['pnl_pct'], '对冲止损平仓'
                            ))

                            # Create futures_trades record for frontend display
                            cursor.execute("""
                                INSERT INTO futures_trades (
                                    trade_id, position_id, account_id, symbol, side,
                                    price, quantity, notional_value, leverage, margin,
                                    fee, realized_pnl, pnl_pct, roi, entry_price,
                                    close_price, order_id, trade_time, created_at
                                ) VALUES (
                                    %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s
                                )
                            """, (
                                trade_id, long_pos['id'], self.account_id, symbol, 'CLOSE_LONG',
                                current_price, long_pos['quantity'], notional_value, leverage, margin,
                                fee, long_pos['realized_pnl'], long_pos['pnl_pct'], roi, long_pos['entry_price'],
                                current_price, f"HEDGE-{long_pos['id']}", datetime.now(), datetime.now()
                            ))

                            # 🔥 账户统计改为定时计算，避免并发更新死锁

                        # SHORT亏损>1%, LONG盈利 -> 平掉SHORT
                        elif short_pos['pnl_pct'] < -1 and long_pos['pnl_pct'] > 0:
                            logger.info(
                                f"[HEDGE_CLOSE] {symbol} SHORT亏损{short_pos['pnl_pct']:.2f}% ({short_pos['realized_pnl']:+.2f} USDT), "
                                f"LONG盈利{long_pos['pnl_pct']:.2f}% -> 平掉SHORT"
                            )

                            # Get leverage and margin
                            cursor.execute("""
                                SELECT leverage, margin FROM futures_positions WHERE id = %s
                            """, (short_pos['id'],))
                            pos_detail = cursor.fetchone()
                            leverage = pos_detail['leverage'] if pos_detail else 1
                            margin = float(pos_detail['margin']) if pos_detail else 0.0
                            roi = (short_pos['realized_pnl'] / margin) * 100 if margin > 0 else 0

                            cursor.execute("""
                                UPDATE futures_positions
                                SET status = 'closed', mark_price = %s,
                                    realized_pnl = %s,
                                    close_time = NOW(), updated_at = NOW(),
                                    notes = CONCAT(IFNULL(notes, ''), '|hedge_loss_cut')
                                WHERE id = %s
                            """, (current_price, short_pos['realized_pnl'], short_pos['id']))

                            # Calculate values for orders and trades
                            import uuid
                            notional_value = current_price * short_pos['quantity']
                            fee = notional_value * 0.0004
                            order_id = f"HEDGE-{short_pos['id']}"
                            trade_id = str(uuid.uuid4())

                            # Create futures_orders record for close reason
                            cursor.execute("""
                                INSERT INTO futures_orders (
                                    account_id, order_id, position_id, symbol,
                                    side, order_type, leverage,
                                    price, quantity, executed_quantity,
                                    total_value, executed_value,
                                    fee, fee_rate, status,
                                    avg_fill_price, fill_time,
                                    realized_pnl, pnl_pct,
                                    order_source, notes
                                ) VALUES (
                                    %s, %s, %s, %s,
                                    %s, 'MARKET', %s,
                                    %s, %s, %s,
                                    %s, %s,
                                    %s, %s, 'FILLED',
                                    %s, %s,
                                    %s, %s,
                                    'smart_trader', %s
                                )
                            """, (
                                self.account_id, order_id, short_pos['id'], symbol,
                                'CLOSE_SHORT', leverage,
                                current_price, short_pos['quantity'], short_pos['quantity'],
                                notional_value, notional_value,
                                fee, 0.0004,
                                current_price, datetime.now(),
                                short_pos['realized_pnl'], short_pos['pnl_pct']
                            ))

                            # Create futures_trades record for frontend display
                            cursor.execute("""
                                INSERT INTO futures_trades (
                                    trade_id, position_id, account_id, symbol, side,
                                    price, quantity, notional_value, leverage, margin,
                                    fee, realized_pnl, pnl_pct, roi, entry_price,
                                    close_price, order_id, trade_time, created_at
                                ) VALUES (
                                    %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s
                                )
                            """, (
                                trade_id, short_pos['id'], self.account_id, symbol, 'CLOSE_SHORT',
                                current_price, short_pos['quantity'], notional_value, leverage, margin,
                                fee, short_pos['realized_pnl'], short_pos['pnl_pct'], roi, short_pos['entry_price'],
                                current_price, order_id, datetime.now(), datetime.now()
                            ))

                            # 🔥 账户统计改为定时计算，避免并发更新死锁

            cursor.close()

        except Exception as e:
            logger.error(f"[ERROR] 检查对冲持仓失败: {e}")

    def get_position_score(self, symbol: str, side: str):
        """获取持仓的开仓得分"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor(pymysql.cursors.DictCursor)  # 使用字典游标

            cursor.execute("""
                SELECT entry_signal_type FROM futures_positions
                WHERE symbol = %s AND position_side = %s AND status = 'open' AND account_id = %s
                LIMIT 1
            """, (symbol, side, self.account_id))

            result = cursor.fetchone()
            cursor.close()

            if result and result['entry_signal_type']:
                # entry_signal_type 格式: SMART_BRAIN_30
                signal_type = result['entry_signal_type']
                if 'SMART_BRAIN_' in signal_type:
                    score = int(signal_type.split('_')[-1])
                    return score

            return 0
        except:
            return 0

    def check_recent_close(self, symbol: str, side: str, cooldown_minutes: int = 15):
        """
        检查指定交易对和方向是否在冷却期内(刚刚平仓)
        返回True表示在冷却期,不应该开仓
        默认冷却期15分钟,避免反复开平造成频繁交易
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT COUNT(*) FROM futures_positions
                WHERE symbol = %s AND position_side = %s AND status = 'closed'
                  AND account_id = %s
                  AND close_time >= DATE_SUB(NOW(), INTERVAL %s MINUTE)
            """, (symbol, side, self.account_id, cooldown_minutes))

            result = cursor.fetchone()
            cursor.close()

            # 如果最近X分钟内有平仓记录,返回True(冷却中)
            return result[0] > 0 if result else False
        except:
            return False

    def check_recent_open(self, symbol: str, side: str, cooldown_minutes: int = 120):
        """
        检查同交易对同方向是否在开仓不应期内（神经元动作电位不应期原理）
        开仓后N分钟内不允许同方向再次开仓，防止信号叠加风险
        返回True表示在不应期中，不应再次开仓
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) FROM futures_positions
                WHERE symbol = %s AND position_side = %s AND account_id = %s
                  AND created_at >= DATE_SUB(NOW(), INTERVAL %s MINUTE)
            """, (symbol, side, self.account_id, cooldown_minutes))
            result = cursor.fetchone()
            cursor.close()
            return result[0] > 0 if result else False
        except:
            return False

    def get_symbol_loss_multiplier(self, symbol: str, side: str) -> float:
        """
        获取symbol连败衰减系数（突触习惯化原理）
        连续亏损会提高下次开仓的有效评分阈值，防止在持续亏损的币种上反复入场

        连败次数 → 系数 → 有效阈值(60/系数)
        0~1次   → 1.0  → 60分（无惩罚）
        2次     → 0.9  → 67分
        3次     → 0.8  → 75分
        4次     → 0.7  → 86分
        5次+    → 0.6  → 100分

        一旦盈利，系数重置为1.0
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT profit_amount
                FROM futures_positions
                WHERE symbol = %s AND position_side = %s AND account_id = %s
                  AND status = 'closed'
                ORDER BY close_time DESC
                LIMIT 5
            """, (symbol, side, self.account_id))
            rows = cursor.fetchall()
            cursor.close()

            if not rows:
                return 1.0

            # 从最新一笔开始，统计连续亏损次数（遇到盈利即停止）
            consecutive_losses = 0
            for row in rows:
                profit = float(row[0]) if row[0] is not None else 0
                if profit < 0:
                    consecutive_losses += 1
                else:
                    break  # 盈利单出现，习惯化重置

            multipliers = {0: 1.0, 1: 1.0, 2: 0.9, 3: 0.8, 4: 0.7}
            return multipliers.get(consecutive_losses, 0.6)
        except Exception as e:
            logger.debug(f"[HABITUATION] {symbol} 获取连败系数失败: {e}")
            return 1.0

    def get_funding_rate_pct(self, symbol: str) -> float:
        """
        获取当前资金费率（百分比）
        返回 None 表示无数据（放行，不阻拦）
        """
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT current_rate_pct
                FROM funding_rate_stats
                WHERE symbol = %s
                ORDER BY updated_at DESC
                LIMIT 1
            """, (symbol,))
            row = cursor.fetchone()
            cursor.close()
            if row and row[0] is not None:
                return float(row[0])
            return None
        except Exception as e:
            logger.debug(f"[FUNDING_RATE] {symbol} 获取资金费率失败: {e}")
            return None
        finally:
            if conn:
                conn.close()

    def is_symbol_in_top_performers(self, symbol: str) -> bool:
        """
        检查交易对是否在盈利Top 50列表中
        返回True表示在列表中,允许开仓
        返回False表示不在列表中,拒绝开仓
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT COUNT(*) FROM top_performing_symbols
                WHERE symbol = %s
            """, (symbol,))

            result = cursor.fetchone()
            cursor.close()

            return result[0] > 0 if result else False
        except Exception as e:
            # 如果表不存在或查询失败，默认允许开仓（向后兼容）
            logger.warning(f"检查Top 50列表失败: {e}, 默认允许开仓")
            return True

    def close_position_by_side(self, symbol: str, side: str, reason: str = "reverse_signal", sync_live: bool = True):
        """关闭指定交易对和方向的持仓。sync_live=False时只更新模拟单DB，不同步实盘。"""
        try:
            current_price = self.get_current_price(symbol)
            if not current_price:
                return False

            conn = self._get_connection()
            cursor = conn.cursor(pymysql.cursors.DictCursor)  # 使用字典游标

            # 获取持仓信息用于日志和计算盈亏
            cursor.execute("""
                SELECT id, entry_price, quantity, leverage, margin FROM futures_positions
                WHERE symbol = %s AND position_side = %s AND status = 'open' AND account_id = %s
            """, (symbol, side, self.account_id))

            positions = cursor.fetchall()

            for pos in positions:
                entry_price = float(pos['entry_price'])
                quantity = float(pos['quantity'])
                leverage = pos['leverage'] if pos.get('leverage') else 1
                margin = float(pos['margin']) if pos.get('margin') else 0.0
                pnl_pct = (current_price - entry_price) / entry_price * 100

                # Calculate realized PnL
                if side == 'LONG':
                    realized_pnl = (current_price - entry_price) * quantity
                    pnl_pct = (current_price - entry_price) / entry_price * 100
                else:  # SHORT
                    realized_pnl = (entry_price - current_price) * quantity
                    pnl_pct = (entry_price - current_price) / entry_price * 100

                roi = (realized_pnl / margin) * 100 if margin > 0 else 0

                logger.info(
                    f"[REVERSE_CLOSE] {symbol} {side} | "
                    f"开仓: ${entry_price:.4f} | 平仓: ${current_price:.4f} | "
                    f"盈亏: {pnl_pct:+.2f}% ({realized_pnl:+.2f} USDT) | 原因: {reason}"
                )

                cursor.execute("""
                    UPDATE futures_positions
                    SET status = 'closed', mark_price = %s,
                        realized_pnl = %s,
                        close_time = NOW(), updated_at = NOW(),
                        notes = CONCAT(IFNULL(notes, ''), '|', %s)
                    WHERE id = %s
                """, (current_price, realized_pnl, reason, pos['id']))

                # Calculate values for orders and trades
                import uuid
                close_side = 'CLOSE_LONG' if side == 'LONG' else 'CLOSE_SHORT'
                notional_value = current_price * quantity
                fee = notional_value * 0.0004
                order_id = f"REVERSE-{pos['id']}"
                trade_id = str(uuid.uuid4())

                # Create futures_orders record for close reason
                cursor.execute("""
                    INSERT INTO futures_orders (
                        account_id, order_id, position_id, symbol,
                        side, order_type, leverage,
                        price, quantity, executed_quantity,
                        total_value, executed_value,
                        fee, fee_rate, status,
                        avg_fill_price, fill_time,
                        realized_pnl, pnl_pct,
                        order_source, notes
                    ) VALUES (
                        %s, %s, %s, %s,
                        %s, 'MARKET', %s,
                        %s, %s, %s,
                        %s, %s,
                        %s, %s, 'FILLED',
                        %s, %s,
                        %s, %s,
                        'smart_trader', %s
                    )
                """, (
                    self.account_id, order_id, pos['id'], symbol,
                    close_side, leverage,
                    current_price, quantity, quantity,
                    notional_value, notional_value,
                    fee, 0.0004,
                    current_price, datetime.now(),
                    realized_pnl, pnl_pct, reason
                ))

                # Create futures_trades record for frontend display
                cursor.execute("""
                    INSERT INTO futures_trades (
                        trade_id, position_id, account_id, symbol, side,
                        price, quantity, notional_value, leverage, margin,
                        fee, realized_pnl, pnl_pct, roi, entry_price,
                        close_price, order_id, trade_time, created_at
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s
                    )
                """, (
                    trade_id, pos['id'], self.account_id, symbol, close_side,
                    current_price, quantity, notional_value, leverage, margin,
                    fee, realized_pnl, pnl_pct, roi, entry_price,
                    current_price, order_id, datetime.now(), datetime.now()
                ))

                # 🔥 账户统计改为定时计算，避免并发更新死锁
                # 不再实时更新 futures_trading_accounts
                # 由 update_account_stats.py 每5分钟统一计算

            cursor.close()
            conn.close()

            # ========== 同步实盘平仓（仅逆向信号触发，SmartExitOptimizer不走此路径）==========
            if not sync_live:
                return True

            try:
                _c = self._get_connection()
                _cur = _c.cursor()
                _cur.execute("SELECT setting_value FROM system_settings WHERE setting_key='live_trading_enabled'")
                _r = _cur.fetchone()
                live_enabled = _r and str(_r.get('setting_value', '0')).lower() in ('1', 'true', 'yes')
                _cur.close(); _c.close()
            except Exception:
                live_enabled = False

            if live_enabled and positions:
                try:
                    from app.services.api_key_service import APIKeyService
                    from app.trading.binance_futures_engine import BinanceFuturesEngine
                    from decimal import Decimal as _Dec
                    svc = APIKeyService(self.db_config)
                    active_keys = svc.get_all_active_api_keys('binance')
                    for ak in active_keys:
                        try:
                            _engine = BinanceFuturesEngine(self.db_config, api_key=ak['api_key'], api_secret=ak['api_secret'])
                            # 优先从 live_futures_positions 取 quantity，直接下单，
                            # 避免 get_open_positions() 失败时误判"持仓不存在"
                            try:
                                _lq_c = self._get_connection()
                                _lq_cur = _lq_c.cursor(pymysql.cursors.DictCursor)
                                _lq_cur.execute(
                                    "SELECT quantity, entry_price FROM live_futures_positions "
                                    "WHERE account_id=%s AND symbol=%s AND position_side=%s AND status='OPEN' "
                                    "ORDER BY id DESC LIMIT 1",
                                    (ak['id'], symbol, side)
                                )
                                _lq_row = _lq_cur.fetchone()
                                _lq_cur.close(); _lq_c.close()
                            except Exception:
                                _lq_row = None

                            if _lq_row and _lq_row.get('quantity'):
                                _res = _engine.close_position_direct(
                                    symbol=symbol,
                                    position_side=side,
                                    quantity=_Dec(str(_lq_row['quantity'])),
                                    entry_price=_Dec(str(_lq_row['entry_price'])),
                                    reason=f'paper_sync_{reason}'
                                )
                            else:
                                # fallback：无 live 记录时仍用 close_position_by_symbol
                                _res = _engine.close_position_by_symbol(
                                    symbol=symbol, position_side=side,
                                    close_quantity=None, reason=f'paper_sync_{reason}'
                                )

                            if _res.get('success'):
                                logger.info(f"[同步实盘平仓] {symbol} {side} 账号[{ak['account_name']}] 平仓成功")
                                # 成功才更新 live_futures_positions
                                try:
                                    _lc = self._get_connection()
                                    _lcur = _lc.cursor()
                                    close_p = _res.get('close_price', current_price)
                                    live_pnl = _res.get('realized_pnl', 0)
                                    _lcur.execute("""
                                        UPDATE live_futures_positions
                                        SET status='CLOSED', close_time=NOW(),
                                            close_price=%s, close_reason=%s,
                                            realized_pnl=%s, mark_price=%s,
                                            notes=CONCAT(IFNULL(notes,''), '|reverse_sync_close:', %s)
                                        WHERE account_id=%s AND symbol=%s
                                          AND position_side=%s AND status='OPEN'
                                    """, (close_p, reason, live_pnl, close_p, reason, ak['id'], symbol, side))
                                    _lc.commit()
                                    _lcur.close(); _lc.close()
                                except Exception as _dbe:
                                    logger.error(f"[同步实盘平仓] 更新live_futures_positions失败: {_dbe}")
                        except Exception as _ex:
                            logger.error(f"[同步实盘平仓] 账号[{ak.get('account_name','')}] 异常: {_ex}")
                except Exception as sync_ex:
                    logger.error(f"[同步实盘平仓] 整体异常: {sync_ex}")
            # ========== 同步实盘平仓结束 ==========

            return True

        except Exception as e:
            logger.error(f"[ERROR] 关闭{symbol} {side}持仓失败: {e}")
            return False

    def reconcile_live_positions(self):
        """
        双向对账：
        1. DB OPEN 但交易所已平 -> 标为 CLOSED
        2. 交易所有持仓但 DB 没有 -> 调用 sync_positions_from_binance 补录
        每5分钟调用一次。
        """
        try:
            from app.services.api_key_service import APIKeyService
            from app.trading.binance_futures_engine import BinanceFuturesEngine
            svc = APIKeyService(self.db_config)
            active_keys = svc.get_all_active_api_keys('binance')
            if not active_keys:
                return

            for ak in active_keys:
                try:
                    engine = BinanceFuturesEngine(self.db_config, ak['api_key'], ak['api_secret'])

                    # --- 方向1：DB OPEN 但交易所已无持仓 → 标 CLOSED ---
                    exchange_positions = engine.get_open_positions()
                    exchange_set = {(p['symbol'], p['position_side']) for p in exchange_positions}

                    conn = self._get_connection()
                    cur = conn.cursor(pymysql.cursors.DictCursor)
                    cur.execute(
                        "SELECT id, symbol, position_side, paper_position_id "
                        "FROM live_futures_positions "
                        "WHERE account_id=%s AND status='OPEN'",
                        (ak['id'],)
                    )
                    db_opens = cur.fetchall()

                    closed_count = 0
                    # 建一个 symbol→exchange_price 映射，用于对账时回填 close_price
                    exchange_price_map = {p['symbol']: float(p.get('mark_price') or p.get('entry_price') or 0)
                                          for p in exchange_positions}

                    for row in db_opens:
                        key = (row['symbol'], row['position_side'])
                        if key not in exchange_set:
                            # 尝试从 WS 价格或 K 线取最新价
                            close_p = None
                            try:
                                close_p = self.get_current_price(row['symbol'])
                            except Exception:
                                pass
                            if not close_p:
                                close_p = exchange_price_map.get(row['symbol'])
                            # 计算已实现 PnL
                            live_pnl = 0.0
                            try:
                                if close_p and row.get('entry_price') and row.get('quantity'):
                                    ep = float(row['entry_price'])
                                    qty = float(row['quantity'])
                                    if row['position_side'] == 'LONG':
                                        live_pnl = (float(close_p) - ep) * qty
                                    else:
                                        live_pnl = (ep - float(close_p)) * qty
                            except Exception:
                                pass
                            cur.execute(
                                "UPDATE live_futures_positions SET status='CLOSED', close_time=NOW(), "
                                "close_price=%s, realized_pnl=%s, close_reason='reconcile_closed', "
                                "notes=CONCAT(IFNULL(notes,''), '|reconcile_closed') "
                                "WHERE id=%s",
                                (close_p, live_pnl, row['id'])
                            )
                            if row['paper_position_id']:
                                cur.execute(
                                    "UPDATE futures_positions SET status='closed', close_time=NOW(), "
                                    "notes=CONCAT(IFNULL(notes,''), '|reconcile_closed') "
                                    "WHERE id=%s AND status='open'",
                                    (row['paper_position_id'],)
                                )
                            closed_count += 1

                    conn.commit()
                    cur.close(); conn.close()

                    if closed_count > 0:
                        logger.info(f"[对账] 账号[{ak['account_name']}] 关闭 {closed_count} 个交易所已平仓的DB记录")

                    # --- 方向2：交易所有持仓但 DB 无记录 → 补录 ---
                    db_open_set = {(row['symbol'], row['position_side']) for row in db_opens}
                    missing = [(p['symbol'], p['position_side']) for p in exchange_positions
                               if (p['symbol'], p['position_side']) not in db_open_set]
                    if missing:
                        sync_result = engine.sync_positions_from_binance(account_id=ak['id'])
                        new_count = sync_result.get('new', 0)
                        if new_count > 0:
                            logger.info(f"[对账] 账号[{ak['account_name']}] 补录 {new_count} 个交易所持仓到DB")

                except Exception as e:
                    logger.error(f"[对账] 账号[{ak.get('account_name','')}] 对账失败: {e}")
        except Exception as e:
            logger.error(f"[对账] 整体异常: {e}")

    async def close_position(self, symbol: str, direction: str, position_size: float, reason: str = "smart_exit"):
        """
        异步平仓方法（供SmartExitOptimizer调用）
        只更新模拟单DB，不触发实盘同步。
        实盘同步由SmartExitOptimizer._close_live_positions_on_exchange()负责。
        """
        try:
            success = self.close_position_by_side(symbol, direction, reason, sync_live=False)
            if success:
                return {'success': True}
            else:
                return {'success': False, 'error': 'close_position_by_side returned False'}
        except Exception as e:
            logger.error(f"异步平仓失败: {symbol} {direction} | {e}")
            return {'success': False, 'error': str(e)}

    def run_adaptive_optimization(self):
        """运行自适应优化 - 每日定时任务"""
        try:
            logger.info("=" * 80)
            logger.info("🧠 开始运行自适应优化...")
            logger.info("=" * 80)

            # 生成24小时优化报告
            report = self.optimizer.generate_optimization_report(hours=24)

            # 打印报告
            self.optimizer.print_report(report)

            # 检查是否有高严重性问题
            high_severity_count = report['summary']['high_severity_issues']

            if high_severity_count > 0:
                logger.warning(f"🔴 发现 {high_severity_count} 个高严重性问题!")
                # TODO: 发送Telegram通知 (需要集成telegram bot)

            # 自动应用优化 (黑名单 + 参数调整)
            if report['blacklist_candidates'] or report['problematic_signals']:
                logger.info(f"📝 准备应用优化:")
                if report['blacklist_candidates']:
                    logger.info(f"   🚫 黑名单候选: {len(report['blacklist_candidates'])} 个")
                if report['problematic_signals']:
                    logger.info(f"   ⚙️  问题信号: {len(report['problematic_signals'])} 个")

                # 自动应用优化 (包括参数调整和权重调整)
                results = self.optimizer.apply_optimizations(report, auto_apply=True, apply_params=True, apply_weights=True)

                if results['blacklist_added']:
                    logger.info(f"✅ 自动添加 {len(results['blacklist_added'])} 个交易对到黑名单")
                    for item in results['blacklist_added']:
                        logger.info(f"   ➕ {item['symbol']} - {item['reason']}")

                if results['params_updated']:
                    logger.info(f"✅ 自动调整 {len(results['params_updated'])} 个参数")
                    for update in results['params_updated']:
                        logger.info(f"   📊 {update}")

                if results.get('weights_adjusted'):
                    logger.info(f"✅ 自动调整 {len(results['weights_adjusted'])} 个评分权重")

                # 重新加载配置以应用所有更新
                if results['blacklist_added'] or results['params_updated'] or results.get('weights_adjusted'):
                    whitelist_count = self.brain.reload_config()
                    logger.info(f"🔄 配置已重新加载，当前可交易: {whitelist_count} 个币种")

                if results['warnings']:
                    logger.warning("⚠️ 优化警告:")
                    for warning in results['warnings']:
                        logger.warning(f"   {warning}")
            else:
                logger.info("✅ 无需加入黑名单的交易对")

            logger.info("=" * 80)
            logger.info("🧠 自适应优化完成")
            logger.info("=" * 80)

        except Exception as e:
            logger.error(f"❌ 自适应优化失败: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def check_and_run_daily_optimization(self):
        """检查是否需要运行每日优化 (凌晨2点)"""
        try:
            now = datetime.now()
            current_date = now.date()

            # 检查是否是凌晨2点且今天还没运行过
            if now.hour == 2 and self.last_optimization_date != current_date:
                logger.info(f"⏰ 触发每日自适应优化 (时间: {now.strftime('%Y-%m-%d %H:%M:%S')})")

                # 1. 运行原有的自适应优化 (参数调整)
                self.run_adaptive_optimization()

                # 2. 问题2优化: 更新交易对评级
                logger.info("=" * 80)
                logger.info("🏆 开始更新交易对评级 (3级黑名单制度)")
                logger.info("=" * 80)
                rating_results = self.rating_manager.update_all_symbol_ratings()
                self.rating_manager.print_rating_report(rating_results)

                # 3. 问题4优化: 更新波动率配置 (15M K线动态止盈)
                logger.info("=" * 80)
                logger.info("📊 开始更新波动率配置 (15M K线动态止盈)")
                logger.info("=" * 80)
                volatility_results = self.volatility_updater.update_all_symbols_volatility(self.brain.whitelist)
                self.volatility_updater.print_volatility_report(volatility_results)

                # 4. 新增: 评估信号黑名单（动态升级/降级）
                logger.info("=" * 80)
                logger.info("🔍 开始评估信号黑名单（动态管理）")
                logger.info("=" * 80)
                try:
                    from app.services.signal_blacklist_reviewer import SignalBlacklistReviewer
                    reviewer = SignalBlacklistReviewer(self.db_config)
                    review_results = reviewer.review_all_blacklisted_signals()
                    reviewer.close()

                    # 打印评估结果摘要
                    if review_results['removed']:
                        logger.info(f"✅ 解除黑名单: {len(review_results['removed'])} 个信号")
                        for item in review_results['removed'][:5]:  # 只显示前5个
                            logger.info(f"   - {item['signal'][:50]} ({item['side']})")
                    if review_results['upgraded']:
                        logger.info(f"📈 降低等级: {len(review_results['upgraded'])} 个信号")
                    if review_results['downgraded']:
                        logger.warning(f"📉 提高等级: {len(review_results['downgraded'])} 个信号")

                    # 如果有信号被解除黑名单，重新加载配置
                    if review_results['removed'] or review_results['upgraded']:
                        logger.info("🔄 重新加载黑名单配置...")
                        self.brain.reload_blacklist()

                except Exception as e:
                    logger.error(f"❌ 信号黑名单评估失败: {e}")
                    import traceback
                    logger.error(traceback.format_exc())

                self.last_optimization_date = current_date

        except Exception as e:
            logger.error(f"检查每日优化失败: {e}")

    async def init_ws_service(self):
        """初始化 WebSocket 价格服务"""
        try:
            # 启动 WebSocket 服务并订阅所有白名单币种
            if not self.ws_service.is_running():
                logger.info(f"🚀 初始化 WebSocket 价格服务，订阅 {len(self.brain.whitelist)} 个币种")
                asyncio.create_task(self.ws_service.start(self.brain.whitelist))
                await asyncio.sleep(3)  # 等待连接建立

                # 检查连接状态
                if self.ws_service.is_running():
                    logger.info("✅ WebSocket 价格服务已启动")
                else:
                    logger.warning("⚠️ WebSocket 价格服务启动失败，将使用数据库价格")
        except Exception as e:
            logger.error(f"WebSocket 服务初始化失败: {e}，将使用数据库价格")

    async def _start_smart_exit_monitoring(self):
        """为所有已开仓的持仓启动统一智能平仓监控（包括普通持仓和分批建仓持仓）"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # 查询所有开仓持仓（不再区分是否分批建仓，统一由SmartExitOptimizer管理）
            cursor.execute("""
                SELECT id, symbol, position_side
                FROM futures_positions
                WHERE status = 'open'
                AND account_id = %s
            """, (self.account_id,))

            positions = cursor.fetchall()
            cursor.close()

            for pos in positions:
                position_id, symbol, side = pos
                await self.smart_exit_optimizer.start_monitoring_position(position_id)
                logger.info(f"✅ 启动智能平仓监控: 持仓{position_id} {symbol} {side}")

            logger.info(f"✅ 智能平仓监控已启动，统一监控 {len(positions)} 个持仓")

        except Exception as e:
            logger.error(f"❌ 启动智能平仓监控失败: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def _check_and_restart_smart_exit_optimizer(self):
        """SmartExitOptimizer健康检查 - 增量对账（不再全量重启，避免竞争条件）"""
        try:
            if not self.smart_exit_optimizer or not self.event_loop:
                logger.warning("⚠️ SmartExitOptimizer未初始化")
                return

            conn = self._get_connection()
            cursor = conn.cursor()

            # 获取DB中所有open持仓的ID（精确对比，不只是数量）
            cursor.execute("""
                SELECT id
                FROM futures_positions
                WHERE status = 'open'
                AND account_id = %s
            """, (self.account_id,))
            db_position_ids = {row[0] for row in cursor.fetchall()}

            # 检查超时未平仓持仓（真正的异常）
            cursor.execute("""
                SELECT COUNT(*) as count
                FROM futures_positions
                WHERE status = 'open'
                AND account_id = %s
                AND planned_close_time IS NOT NULL
                AND NOW() > planned_close_time
            """, (self.account_id,))
            timeout_count = cursor.fetchone()[0]
            cursor.close()

            monitoring_ids = set(self.smart_exit_optimizer.monitoring_tasks.keys())

            # 在DB中但未被监控的持仓 → 补充启动监控
            to_add = db_position_ids - monitoring_ids
            # 在监控中但DB已无记录的持仓 → 停止冗余监控（finally已处理大部分）
            to_remove = monitoring_ids - db_position_ids

            for pid in to_add:
                try:
                    asyncio.run_coroutine_threadsafe(
                        self.smart_exit_optimizer.start_monitoring_position(pid),
                        self.event_loop
                    )
                    logger.info(f"[HEALTH-CHECK] 补充监控: 持仓{pid}")
                except Exception as e:
                    logger.error(f"[HEALTH-CHECK] 补充监控失败: 持仓{pid} | {e}")

            for pid in to_remove:
                try:
                    asyncio.run_coroutine_threadsafe(
                        self.smart_exit_optimizer.stop_monitoring_position(pid),
                        self.event_loop
                    )
                    logger.info(f"[HEALTH-CHECK] 停止冗余监控: 持仓{pid}")
                except Exception as e:
                    logger.error(f"[HEALTH-CHECK] 停止冗余监控失败: 持仓{pid} | {e}")

            # 只有超时持仓才是真正异常，才发告警
            if timeout_count > 0:
                logger.error(f"❌ 发现{timeout_count}个超时未平仓持仓，SmartExitOptimizer可能异常")
                if hasattr(self, 'telegram_notifier') and self.telegram_notifier:
                    try:
                        self.telegram_notifier.send_message(
                            f"⚠️ 发现{timeout_count}个超时未平仓持仓\n\n"
                            f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                            f"请人工检查SmartExitOptimizer是否正常运行！"
                        )
                    except Exception as e:
                        logger.warning(f"发送Telegram告警失败: {e}")

            # 打印健康状态
            if to_add or to_remove:
                logger.info(
                    f"[HEALTH-CHECK] 对账完成: +{len(to_add)}补充 -{len(to_remove)}移除 "
                    f"| DB={len(db_position_ids)}, 监控={len(monitoring_ids)}"
                )
            elif datetime.now().minute % 10 == 0:
                logger.debug(
                    f"💓 SmartExitOptimizer健康: {len(monitoring_ids)}个持仓监控中"
                )

        except Exception as e:
            logger.error(f"SmartExitOptimizer健康检查失败: {e}")

    async def _restart_smart_exit_monitoring(self):
        """重启SmartExitOptimizer监控"""
        try:
            logger.info("========== 重启SmartExitOptimizer监控 ==========")

            # 1. 取消所有现有监控任务
            if self.smart_exit_optimizer and self.smart_exit_optimizer.monitoring_tasks:
                logger.info(f"取消 {len(self.smart_exit_optimizer.monitoring_tasks)} 个现有监控任务...")

                for position_id, task in list(self.smart_exit_optimizer.monitoring_tasks.items()):
                    try:
                        task.cancel()
                        logger.debug(f"  取消监控任务: 持仓{position_id}")
                    except Exception as e:
                        logger.warning(f"  取消任务失败: 持仓{position_id} | {e}")

                # 等待任务取消
                await asyncio.sleep(1)

                # 清空监控任务字典
                self.smart_exit_optimizer.monitoring_tasks.clear()
                logger.info("✅ 已清空所有监控任务")

            # 2. 重新启动所有持仓的监控
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT id, symbol, position_side, planned_close_time
                FROM futures_positions
                WHERE status = 'open'
                AND account_id = %s
                ORDER BY id ASC
            """, (self.account_id,))

            positions = cursor.fetchall()
            cursor.close()

            logger.info(f"发现 {len(positions)} 个开仓持仓需要监控")

            success_count = 0
            fail_count = 0

            for pos in positions:
                position_id, symbol, side, planned_close = pos
                try:
                    await self.smart_exit_optimizer.start_monitoring_position(position_id)

                    planned_str = planned_close.strftime('%H:%M') if planned_close else 'None'
                    logger.info(
                        f"✅ [{success_count+1}/{len(positions)}] 重启监控: "
                        f"持仓{position_id} {symbol} {side} | "
                        f"计划平仓: {planned_str}"
                    )
                    success_count += 1

                except Exception as e:
                    logger.error(f"❌ 重启监控失败: 持仓{position_id} {symbol} | {e}")
                    fail_count += 1

            logger.info(
                f"========== 监控重启完成: 成功{success_count}, 失败{fail_count} =========="
            )

            # 3. 发送完成通知
            if hasattr(self, 'telegram_notifier') and self.telegram_notifier:
                try:
                    self.telegram_notifier.send_message(
                        f"✅ SmartExitOptimizer重启完成\n\n"
                        f"成功: {success_count}个持仓\n"
                        f"失败: {fail_count}个持仓\n"
                        f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                except Exception as e:
                    logger.warning(f"发送Telegram通知失败: {e}")

        except Exception as e:
            logger.error(f"❌ 重启SmartExitOptimizer失败: {e}")
            import traceback
            logger.error(traceback.format_exc())

            # 发送失败告警
            if hasattr(self, 'telegram_notifier') and self.telegram_notifier:
                try:
                    self.telegram_notifier.send_message(
                        f"❌ SmartExitOptimizer重启失败\n\n"
                        f"错误: {str(e)}\n"
                        f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                        f"请手动检查服务状态"
                    )
                except Exception as e:
                    logger.warning(f"发送Telegram失败告警失败: {e}")

    def run(self):
        """主循环"""
        last_smart_exit_check = datetime.now()
        last_blacklist_reload = datetime.now()
        last_config_reload = datetime.now()
        last_regime_check = datetime.now()  # 市场状态机检测（每小时一次）
        last_reconcile = datetime.now()     # 实盘持仓对账（每5分钟）
        last_oi_collection = datetime.now() - timedelta(minutes=20)  # OI采集（每15分钟）

        while self.running:
            try:
                # 0. 检查是否需要运行每日自适应优化 (凌晨2点)
                self.check_and_run_daily_optimization()

                # 0.4. OI 数据采集（每15分钟）
                now = datetime.now()
                if (now - last_oi_collection).total_seconds() >= 900:
                    try:
                        self._run_oi_collection_round(self.symbols)
                    except Exception as e:
                        logger.debug(f"OI collection round failed: {e}")
                    last_oi_collection = now

                # 0.5. 定期重新加载黑名单 (每5分钟)
                if (now - last_blacklist_reload).total_seconds() >= 300:  # 5分钟
                    self.brain._reload_blacklist()
                    last_blacklist_reload = now

                # 0.55. 市场状态机检测（每小时一次，操纵子原理）
                # 根据Big4过去48H趋势分布自动更新allow_long/allow_short
                if (now - last_regime_check).total_seconds() >= 3600:
                    try:
                        self.regime_monitor.run_detection()
                    except Exception as e:
                        logger.warning(f"⚠️ [BIG4-REGIME] 市场状态检测失败: {e}")
                    last_regime_check = now

                # 0.6. 定期重新加载Big4配置 + 交易模式开关 (每5分钟检查数据库)
                if (now - last_config_reload).total_seconds() >= 300:
                    try:
                        from app.services.system_settings_loader import get_big4_filter_enabled
                        old_big4_enabled = self.big4_filter_config.get('enabled', True)
                        new_big4_enabled = get_big4_filter_enabled()

                        if old_big4_enabled != new_big4_enabled:
                            self.big4_filter_config = {'enabled': new_big4_enabled}
                            self.brain.big4_filter_enabled = new_big4_enabled
                            logger.info(f"[BIG4-CONFIG-UPDATE] Big4过滤器配置已更新: {'启用' if new_big4_enabled else '禁用'}")
                    except Exception as e:
                        logger.warning(f"[CONFIG-RELOAD] 重新加载Big4配置失败: {e}")

                    try:
                        _tm_conn = self._get_connection()
                        _tm_cur = _tm_conn.cursor()
                        _tm_cur.execute("""
                            SELECT setting_key, setting_value FROM system_settings
                            WHERE setting_key IN ('signal_confirmation_enabled', 'trend_following_enabled', 'max_positions')
                        """)
                        _tm_rows = {r[0]: r[1] for r in _tm_cur.fetchall()}
                        _tm_cur.close(); _tm_conn.close()
                        new_sc = int(float(_tm_rows.get('signal_confirmation_enabled', '0'))) == 1
                        new_tf = int(float(_tm_rows.get('trend_following_enabled', '0'))) == 1
                        if new_sc != self.brain.signal_confirmation_enabled or new_tf != self.brain.trend_following_enabled:
                            logger.info(f"[TRADING-MODE] 模式更新: 信号确认={'ON' if new_sc else 'OFF'} 趋势跟随={'ON' if new_tf else 'OFF'}")
                            self.brain.signal_confirmation_enabled = new_sc
                            self.brain.trend_following_enabled = new_tf
                        if 'max_positions' in _tm_rows:
                            new_mp = int(float(_tm_rows['max_positions']))
                            if new_mp != self.max_positions:
                                logger.info(f"[CONFIG-RELOAD] 最大持仓数更新: {self.max_positions} -> {new_mp}")
                                self.max_positions = new_mp
                    except Exception as e:
                        logger.warning(f"[CONFIG-RELOAD] 重新加载交易模式配置失败: {e}")

                    last_config_reload = now

                # 0.6. 实盘持仓对账（每5分钟，检测交易所已平但DB未更新的仓位）
                if (now - last_reconcile).total_seconds() >= 300:
                    try:
                        self.reconcile_live_positions()
                    except Exception as _re:
                        logger.warning(f"[对账] 异常: {_re}")
                    last_reconcile = now

                # 0.65. BTC动量跟随策略检测（每轮都跑，内部自带冷却控制）
                try:
                    self.btc_momentum_trader.check_and_execute()
                except Exception as _e:
                    logger.warning(f"[BTC动量] 检测异常: {_e}")

                # 0.7. 🔒 提前检查交易开关（最高优先级）
                # 如果U本位交易已关闭，直接跳过本轮所有扫描和开仓逻辑
                if not self.check_trading_enabled():
                    logger.info("[TRADING-DISABLED] ⏸️ U本位合约交易已停止，跳过本轮扫描")
                    time.sleep(self.scan_interval)
                    continue

                # 注意：止盈止损、超时检查已统一迁移到SmartExitOptimizer
                # 1. [已停用] 检查止盈止损 -> 由SmartExitOptimizer处理
                # self.check_stop_loss_take_profit()

                # 2. 检查对冲持仓(平掉亏损方向)
                self.check_hedge_positions()

                # 3. [已停用] 关闭超时持仓 -> 由SmartExitOptimizer处理
                # self.close_old_positions()

                # 3.5. SmartExitOptimizer健康检查和自动重启（每分钟检查）
                now = datetime.now()
                if (now - last_smart_exit_check).total_seconds() >= 60:
                    self._check_and_restart_smart_exit_optimizer()
                    last_smart_exit_check = now

                # 4. 检查持仓
                current_positions = self.get_open_positions_count()
                logger.info(f"[STATUS] 持仓: {current_positions}/{self.max_positions}")

                if current_positions >= self.max_positions:
                    logger.info("[SKIP] 已达最大持仓,跳过扫描")
                    time.sleep(self.scan_interval)
                    continue

                # 5. 🔥 强制只做趋势单,不再做震荡市场的单
                logger.info(f"[SCAN] 模式: TREND (只做趋势) | 扫描 {len(self.brain.whitelist)} 个币种...")

                # 获取Big4结果并扫描趋势信号
                big4_result = self.get_big4_result()

                # 🔥 震荡市检测：多空拉锯时禁止开仓，避免两边被磨损
                if big4_result and big4_result.get('is_choppy'):
                    current_signal = big4_result.get('overall_signal', 'NEUTRAL')
                    choppy_info = big4_result.get('choppy_market', {})
                    # 例外：出现强趋势信号时允许顺势开仓
                    if current_signal in ('STRONG_BULLISH', 'STRONG_BEARISH'):
                        logger.info(f"[CHOPPY-OVERRIDE] 检测到震荡市({choppy_info.get('reason', '')})，"
                                    f"但当前{current_signal}强信号，允许顺势开仓")
                    else:
                        logger.warning(f"[CHOPPY-MARKET] {choppy_info.get('reason', '震荡市')}，暂停本轮开仓")
                        time.sleep(self.scan_interval)
                        continue

                opportunities = self.brain.scan_all(big4_result=big4_result)
                logger.info(f"[TREND-SCAN] 趋势模式扫描完成, 找到 {len(opportunities)} 个机会")

                if not opportunities:
                    logger.info("[SCAN] 无交易机会")
                    time.sleep(self.scan_interval)
                    continue

                # 5.5. 盈利熔断检查：每4小时检测一次，过去6小时总盈利超1000U则自动禁止开仓
                if self._check_profit_and_auto_disable(profit_threshold=1000.0, window_hours=6, check_interval_hours=4):
                    logger.warning("[PROFIT-GUARD] 盈利熔断已触发，停止本轮开仓，请检查后手动重新开启交易")
                    time.sleep(self.scan_interval)
                    continue

                # 5.5.1 亏损熔断检查：每30分钟检测一次，过去3小时亏损超2000U则自动禁止开仓
                if self._check_loss_and_auto_disable(loss_threshold=2000.0, window_hours=3, check_interval_hours=0.5):
                    logger.warning("[LOSS-GUARD] 亏损熔断已触发，停止本轮开仓，请检查后手动重新开启交易")
                    time.sleep(self.scan_interval)
                    continue

                # 5.8. 🚀 反弹交易窗口检查 (优先于正常信号)
                # 逻辑: Big4触底 = 全市场信号，所有交易对都开多
                try:
                    conn_bounce = self._get_connection()
                    cursor = conn_bounce.cursor()

                    # 检查是否有Big4的活跃反弹窗口
                    BIG4 = ['BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'SOL/USDT']

                    cursor.execute("""
                        SELECT symbol, lower_shadow_pct, window_end, trigger_time
                        FROM bounce_window
                        WHERE account_id = 2
                        AND trading_type = 'usdt_futures'
                        AND symbol IN ('BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'SOL/USDT')
                        AND window_end > NOW()
                        ORDER BY trigger_time DESC
                        LIMIT 1
                    """)

                    big4_bounce = cursor.fetchone()

                    if big4_bounce:
                        # 🚀 Big4触底 = 全市场反弹信号!
                        window_end = big4_bounce['window_end']
                        remaining_minutes = (window_end - datetime.now()).total_seconds() / 60
                        trigger_symbol = big4_bounce['symbol']

                        logger.warning(f"🚀🚀🚀 [MARKET-BOUNCE] {trigger_symbol} 触发全市场反弹窗口! "
                                     f"下影{big4_bounce['lower_shadow_pct']:.1f}%, 剩余{remaining_minutes:.0f}分钟")

                        # 获取所有交易对
                        cursor.execute("""
                            SELECT DISTINCT symbol
                            FROM symbols
                            WHERE trading_type = 'usdt_futures'
                            AND is_active = TRUE
                        """)
                        all_symbols = [row['symbol'] for row in cursor.fetchall()]
                        logger.info(f"🚀 [MARKET-BOUNCE] 准备对 {len(all_symbols)} 个交易对开多")

                        opened_count = 0
                        for symbol in all_symbols:
                            if self.get_open_positions_count() >= self.max_positions:
                                logger.info(f"[BOUNCE-SKIP] 已达最大持仓 {self.max_positions}，停止反弹交易")
                                break

                            # 检查是否已有该币种的LONG仓位
                            if self.has_position(symbol, 'LONG'):
                                continue

                            # 检查是否有SHORT仓位
                            if self.has_position(symbol, 'SHORT'):
                                continue

                            # 检查最近是否平仓过LONG (冷却期)
                            if self.check_recent_close(symbol, 'LONG', cooldown_minutes=30):
                                continue

                            # 获取当前价格
                            try:
                                current_price = self.binance_api.get_current_price(symbol)
                            except Exception as e:
                                logger.error(f"[BOUNCE-ERROR] {symbol} 获取价格失败: {e}")
                                continue

                            # 🔥 激进开仓策略: 立即开仓
                            # Big4触底 = 市场信号，所有币跟涨
                            bounce_opp = {
                                'symbol': symbol,
                                'side': 'LONG',
                                'score': 100,
                                'strategy': 'emergency_bounce',
                                'reason': f"🚀市场反弹: {trigger_symbol}触底{big4_bounce['lower_shadow_pct']:.1f}%, 窗口{remaining_minutes:.0f}分钟",
                                'signal_type': 'EMERGENCY_BOUNCE',
                                'position_size_pct': 70,  # 🔥 激进仓位70%
                                'take_profit_pct': 8.0,   # 🔥 止盈8%（基于历史平均反弹12.6%）
                                'stop_loss_pct': 3.0,     # 🔥 止损3%
                                'trailing_stop_pct': 5.0, # 🔥 动态追踪：回撤5%平仓
                            }

                            # 开仓
                            try:
                                self.open_position(bounce_opp)
                                opened_count += 1
                                logger.info(f"✅ [BOUNCE-OPENED] {symbol} 反弹多单已开 ({opened_count}/{len(all_symbols)})")
                                time.sleep(1)  # 避免频率限制
                            except Exception as e:
                                logger.error(f"❌ [BOUNCE-ERROR] {symbol} 反弹开仓失败: {e}")

                        logger.warning(f"🚀 [MARKET-BOUNCE] 反弹交易完成: 共开仓 {opened_count} 个币种")

                    cursor.close()
                    conn_bounce.close()

                except Exception as e:
                    logger.error(f"[BOUNCE-CHECK-ERROR] 反弹窗口检查失败: {e}")

                # 6. 执行交易
                logger.info(f"[EXECUTE] 找到 {len(opportunities)} 个机会")

                # 输出所有机会的详细信息
                if opportunities:
                    logger.info(f"\n{'='*100}")
                    logger.info(f"🎯 开仓机会列表 (按评分排序)")
                    logger.info(f"{'='*100}")
                    logger.info(f"{'币种':<14} {'方向':<6} {'评分':<6} {'信号组成':<50}")
                    logger.info(f"{'-'*100}")

                    sorted_opps = sorted(opportunities, key=lambda x: x['score'], reverse=True)
                    for opp in sorted_opps:
                        signal_comps = ', '.join(opp.get('signal_components', {}).keys())
                        logger.info(f"{opp['symbol']:<14} {opp['side']:<6} {opp['score']:<6} {signal_comps:<50}")

                    logger.info(f"{'='*100}\n")

                # 读取 allow_long / allow_short 开关（每轮刷新）
                try:
                    _sw_conn = self._get_connection()
                    _sw_cur = _sw_conn.cursor()
                    _sw_cur.execute(
                        "SELECT setting_key, setting_value FROM system_settings "
                        "WHERE setting_key IN ('allow_long','allow_short')"
                    )
                    _sw_rows = {r[0]: r[1] for r in _sw_cur.fetchall()}
                    _sw_cur.close(); _sw_conn.close()
                    _allow_long  = str(_sw_rows.get('allow_long',  '1')) in ('1', 'true', 'True')
                    _allow_short = str(_sw_rows.get('allow_short', '1')) in ('1', 'true', 'True')
                except Exception:
                    _allow_long = True
                    _allow_short = True

                for opp in opportunities:
                    if self.get_open_positions_count() >= self.max_positions:
                        break

                    symbol = opp['symbol']
                    new_side = opp['side']
                    new_score = opp['score']
                    opposite_side = 'SHORT' if new_side == 'LONG' else 'LONG'

                    # allow_long / allow_short 开关检查
                    if new_side == 'LONG' and not _allow_long:
                        logger.debug(f"[SWITCH] {symbol} LONG 被 allow_long=0 拦截")
                        continue
                    if new_side == 'SHORT' and not _allow_short:
                        logger.debug(f"[SWITCH] {symbol} SHORT 被 allow_short=0 拦截")
                        continue

                    # 🔥 获取Big4状态（用于后续判断）
                    try:
                        big4_result = self.get_big4_result()
                    except Exception as e:
                        logger.error(f"[BIG4-ERROR] Big4检测失败: {e}")
                        big4_result = None

                    # Big4强度门槛检查（阈值已在 scan_all() 中处理，此处只拦截极弱信号）
                    if self.big4_filter_config.get('enabled', True):
                        if big4_result:
                            big4_signal = big4_result.get('overall_signal', 'NEUTRAL')
                            big4_strength = big4_result.get('signal_strength', 0)
                            logger.info(f"[BIG4] {symbol} Big4: {big4_signal}({big4_strength:.1f})")
                            # 强度 < 20 时市场方向完全不明确，禁止开仓
                            # 例外：NEUTRAL市场下高确信信号(score>=threshold)允许通过
                            _exec_threshold = self.brain.threshold
                            if big4_strength < 20:
                                if big4_signal == 'NEUTRAL' and new_score >= _exec_threshold:
                                    logger.info(f"[NEUTRAL-PASS] {symbol} Big4强度低({big4_strength:.1f})但NEUTRAL高确信({new_score}), 允许开仓")
                                else:
                                    logger.warning(f"[BIG4-WEAK] {symbol} Big4强度过低({big4_strength:.1f}<20), 禁止开仓")
                                    continue
                            # 执行层安全网：net_weighted_score 封死反向单
                            # 例外：NEUTRAL市场下高确信信号允许跨方向（Big4信号弱，单币信号强）
                            _exec_net_ws = big4_result.get('net_weighted_score', 0)
                            _neutral_high_conf = (big4_signal == 'NEUTRAL' and new_score >= _exec_threshold)
                            if _exec_net_ws > 0 and new_side == 'SHORT':
                                if _neutral_high_conf:
                                    logger.info(f"[DIR-PASS] {symbol} net_ws={_exec_net_ws:.1f}>0 但NEUTRAL高确信空头({new_score}), 放行")
                                else:
                                    logger.warning(f"[DIR-BLOCK] {symbol} net_ws={_exec_net_ws:.1f}>0, 封死空单")
                                    continue
                            if _exec_net_ws < 0 and new_side == 'LONG':
                                if _neutral_high_conf:
                                    logger.info(f"[DIR-PASS] {symbol} net_ws={_exec_net_ws:.1f}<0 但NEUTRAL高确信多头({new_score}), 放行")
                                else:
                                    logger.warning(f"[DIR-BLOCK] {symbol} net_ws={_exec_net_ws:.1f}<0, 封死多单")
                                    continue
                        else:
                            logger.warning(f"[BIG4-ERROR] {symbol} Big4数据不可用, 跳过开仓")
                            continue
                    else:
                        logger.debug(f"[BIG4-DISABLED] {symbol} Big4过滤已禁用")

                    # ========== 只接受趋势信号 ==========
                    signal_type = opp.get('signal_type', '')

                    # 🔥 只做趋势单,不再做震荡市单
                    # 紧急反弹信号(Big4触底)优先级最高
                    if signal_type == 'EMERGENCY_BOUNCE':
                        logger.warning(f"🚀 [EMERGENCY-BOUNCE] {symbol} 反弹信号")
                    elif 'TREND' in signal_type:
                        logger.info(f"[TREND-SIGNAL] {symbol} 趋势信号")
                    else:
                        # 非趋势信号,跳过
                        logger.debug(f"[SKIP-NON-TREND] {symbol} 非趋势信号,跳过 (类型: {signal_type[:40]})")
                        continue

                    # Big4 趋势检测 - 应用到所有币种（可配置禁用）
                    if self.big4_filter_config.get('enabled', True):
                        try:
                            # 如果是四大天王本身,使用该币种的专属信号
                            if symbol in self.big4_symbols:
                                symbol_detail = big4_result['details'].get(symbol, {})
                                symbol_signal = symbol_detail.get('signal', 'NEUTRAL')
                                signal_strength = symbol_detail.get('strength', 0)
                                logger.info(f"[BIG4-SELF] {symbol} 自身趋势: {symbol_signal} (强度: {signal_strength})")
                            else:
                                # 对其他币种,使用Big4整体趋势信号
                                symbol_signal = big4_result.get('overall_signal', 'NEUTRAL')
                                signal_strength = big4_result.get('signal_strength', 0)
                                logger.info(f"[BIG4-MARKET] {symbol} 市场整体趋势: {symbol_signal} (强度: {signal_strength:.1f})")

                            # ========== 破位否决检查 ==========
                            # Big4强度>=12时，完全禁止逆向开仓
                            should_skip, veto_reason = self.breakout_booster.should_skip_opposite_signal(
                                new_side,
                                new_score
                            )
                            if should_skip:
                                logger.warning(f"💥 [BREAKOUT-VETO] {symbol} {veto_reason}")
                                continue
                            # ========== 破位否决结束 ==========

                            # 📝 注意：Big4方向过滤和加分已在scan_all()中提前处理
                            # 这里只记录Big4状态信息，不再重复过滤和加分
                            logger.debug(f"[BIG4-INFO] {symbol} {new_side} | Big4: {symbol_signal}({signal_strength:.1f})")

                            # 更新机会的Big4状态信息 (用于后续记录)
                            opp['big4_adjusted'] = True
                            opp['big4_signal'] = symbol_signal
                            opp['big4_strength'] = signal_strength

                        except Exception as e:
                            logger.error(f"[BIG4-ERROR] {symbol} Big4检测失败: {e}")
                            # 失败不影响正常交易流程

                        # 🔥 紧急干预检查: 触底/触顶反转保护 (实时判断)
                        try:
                            emergency = big4_result.get('emergency_intervention', {})

                            # 🔥 新增: 实时检查市场恢复状态，绕过Big4检测器的15分钟缓存
                            should_block_long = emergency.get('block_long', False)
                            should_block_short = emergency.get('block_short', False)

                            # 如果有做空限制，实时检查是否已反弹3%+ (不依赖bottom_detected字段)
                            if should_block_short and new_side == 'SHORT':
                                # 快速检查: 查询最近4根1H K线，判断是否已反弹
                                try:
                                    conn_check = self._get_connection()
                                    cursor_check = conn_check.cursor(pymysql.cursors.DictCursor)

                                    # 检查Big4是否已完成3%反弹
                                    all_recovered = True
                                    for big4_symbol in ['BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'SOL/USDT']:
                                        cursor_check.execute("""
                                            SELECT low_price, close_price
                                            FROM kline_data
                                            WHERE symbol = %s
                                            AND timeframe = '1h'
                                            AND exchange = 'binance_futures'
                                            ORDER BY open_time DESC
                                            LIMIT 4
                                        """, (big4_symbol,))

                                        recent_klines = cursor_check.fetchall()
                                        if recent_klines:
                                            period_low = min([float(k['low_price']) for k in recent_klines])
                                            latest_close = float(recent_klines[0]['close_price'])
                                            recovery_pct = (latest_close - period_low) / period_low * 100

                                            if recovery_pct < 2.0:
                                                all_recovered = False
                                                break

                                    cursor_check.close()
                                    # ⚠️ 不调用 conn_check.close()，conn_check 是单例 self.connection

                                    # 如果所有Big4都已反弹2%+，解除禁止做空
                                    if all_recovered:
                                        should_block_short = False
                                        logger.info(f"✅ [SMART-RELEASE] {symbol} 市场已反弹2%+，解除做空限制")

                                except Exception as check_error:
                                    logger.error(f"[SMART-RELEASE-ERROR] {symbol} 实时检查失败: {check_error}")

                            # 如果有做多限制，实时检查是否已回调3%+ (不依赖top_detected字段)
                            if should_block_long and new_side == 'LONG':
                                try:
                                    conn_check = self._get_connection()
                                    cursor_check = conn_check.cursor(pymysql.cursors.DictCursor)

                                    all_cooled = True
                                    for big4_symbol in ['BTC/USDT', 'ETH/USDT', 'BNB/USDT', 'SOL/USDT']:
                                        cursor_check.execute("""
                                            SELECT high_price, close_price
                                            FROM kline_data
                                            WHERE symbol = %s
                                            AND timeframe = '1h'
                                            AND exchange = 'binance_futures'
                                            ORDER BY open_time DESC
                                            LIMIT 4
                                        """, (big4_symbol,))

                                        recent_klines = cursor_check.fetchall()
                                        if recent_klines:
                                            period_high = max([float(k['high_price']) for k in recent_klines])
                                            latest_close = float(recent_klines[0]['close_price'])
                                            cooldown_pct = (latest_close - period_high) / period_high * 100

                                            if cooldown_pct > -2.0:
                                                all_cooled = False
                                                break

                                    cursor_check.close()
                                    # ⚠️ 不调用 conn_check.close()，conn_check 是单例 self.connection

                                    if all_cooled:
                                        should_block_long = False
                                        logger.info(f"✅ [SMART-RELEASE] {symbol} 市场已回调2%+，解除做多限制")

                                except Exception as check_error:
                                    logger.error(f"[SMART-RELEASE-ERROR] {symbol} 实时检查失败: {check_error}")

                            # 执行最终的阻止判断：临时跳过本次信号（不修改系统设置，由emergency_intervention表管理到期）
                            if should_block_long and new_side == 'LONG':
                                logger.warning(f"🚨 [EMERGENCY-BLOCK] {symbol} 触顶反转风险,本次做多信号跳过 | {emergency.get('details', '')}")
                                continue
                            if should_block_short and new_side == 'SHORT':
                                logger.warning(f"🚨 [EMERGENCY-BLOCK] {symbol} 触底反弹风险,本次做空信号跳过 | {emergency.get('details', '')}")
                                continue

                        except Exception as e:
                            logger.error(f"[EMERGENCY-ERROR] {symbol} 紧急干预检查失败: {e}")
                            # 检查失败不影响正常交易

                    else:
                        # Big4过滤已禁用（测试模式）
                        logger.debug(f"[BIG4-DISABLED] {symbol} Big4过滤已禁用，直接使用原始信号 (测试模式)")

                    # 🔥 已移除"同方向只能1个持仓"的限制，支持分批建仓（多个独立持仓）
                    # 每批建仓都是独立的持仓记录，可以有多个同方向持仓
                    # if self.has_position(symbol, new_side):
                    #     logger.info(f"[SKIP] {symbol} {new_side}方向已有持仓")
                    #     continue

                    # 🔥 限制：同一交易对同方向只能有1个持仓
                    position_count = self.count_positions(symbol, new_side)
                    if position_count >= 1:
                        logger.info(f"[SKIP] {symbol} {new_side}方向已有{position_count}个持仓，达到上限(1)")
                        continue

                    # 检查是否刚刚平仓(15分钟冷却期)
                    if self.check_recent_close(symbol, new_side, cooldown_minutes=15):
                        logger.info(f"[SKIP] {symbol} {new_side}方向15分钟内刚平仓,冷却中")
                        continue

                    # 检查是否有反向持仓 - 如果有则跳过,不做对冲
                    if self.has_position(symbol, opposite_side):
                        logger.info(f"[SKIP] {symbol} 已有{opposite_side}持仓,跳过{new_side}信号(不做对冲)")
                        continue

                    # 🔥 防追高/防杀跌: 24H区间位置过滤
                    current_price_for_check = opp.get('current_price') or opp.get('price', 0)
                    if current_price_for_check > 0:
                        anti_fomo_pass, anti_fomo_reason = self.brain.check_anti_fomo_filter(
                            symbol, current_price_for_check, new_side
                        )
                        if not anti_fomo_pass:
                            logger.warning(f"🚫 [ANTI-FOMO] {symbol} {new_side} 跳过: {anti_fomo_reason}")
                            continue
                        else:
                            logger.debug(f"[ANTI-FOMO] {symbol} {new_side} 通过: {anti_fomo_reason}")

                    # 正常开仓
                    self.open_position(opp)
                    time.sleep(2)

                # 7. 等待
                logger.info(f"[WAIT] {self.scan_interval}秒后下一轮...")
                time.sleep(self.scan_interval)

            except KeyboardInterrupt:
                logger.info("[EXIT] 收到停止信号")
                self.running = False
                break
            except Exception as e:
                import traceback
                logger.error(f"[ERROR] 主循环异常: {e}\n{traceback.format_exc()}")
                time.sleep(60)

        logger.info("[STOP] 服务已停止")


async def async_main():
    """异步主函数"""
    service = SmartTraderService()

    # 保存事件循环引用，供分批建仓使用
    service.event_loop = asyncio.get_event_loop()

    # 初始化 WebSocket 服务
    await service.init_ws_service()

    # 初始化智能平仓监控
    if service.smart_exit_optimizer:
        await service._start_smart_exit_monitoring()

    # 🔥 启动账户统计定时更新任务（每5分钟）
    async def update_account_stats_task():
        """每5分钟更新一次账户统计"""
        from update_account_stats import update_account_statistics
        while True:
            try:
                await asyncio.sleep(300)  # 5分钟 = 300秒
                logger.info("🔄 定时更新账户统计...")
                await asyncio.get_event_loop().run_in_executor(None, update_account_statistics)
            except Exception as e:
                logger.error(f"❌ 账户统计更新失败: {e}")

    # 创建后台任务
    asyncio.create_task(update_account_stats_task())
    logger.info("✅ 账户统计定时更新任务已启动（每5分钟）")

    # 🔥 启动盈利Top 30交易对定时更新任务（每天凌晨2点）
    async def update_top_performers_task():
        """每天凌晨2点更新盈利Top 30交易对"""
        from update_top_performers import update_top_performing_symbols
        import time as time_module
        from datetime import datetime, time

        while True:
            try:
                # 计算距离下次凌晨2点的秒数
                now = datetime.now()
                target_time = datetime.combine(now.date(), time(2, 0))  # 今天凌晨2点
                if now >= target_time:
                    # 如果已经过了今天凌晨2点，目标改为明天凌晨2点
                    target_time = datetime.combine(now.date() + timedelta(days=1), time(2, 0))

                seconds_until_target = (target_time - now).total_seconds()
                logger.info(f"⏰ Top 30更新任务将在 {seconds_until_target/3600:.1f} 小时后执行（{target_time}）")

                # 等待到凌晨2点
                await asyncio.sleep(seconds_until_target)

                # 执行更新
                logger.info("🔄 开始更新盈利Top 30交易对...")
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    update_top_performing_symbols,
                    2,  # account_id=2 (U本位)
                    50  # top_n=50
                )
                logger.info("Top 50更新完成")

            except Exception as e:
                logger.error(f"❌ Top 30更新失败: {e}")
                # 失败后等待1小时再重试
                await asyncio.sleep(3600)

    # 创建后台任务
    asyncio.create_task(update_top_performers_task())
    logger.info("✅ Top 30定时更新任务已启动（每天凌晨2点）")

    # 在事件循环中运行同步的主循环
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, service.run)


if __name__ == '__main__':
    try:
        # 运行异步主函数
        asyncio.run(async_main())
    except KeyboardInterrupt:
        logger.info("服务已停止")
