"""
智能平仓优化器
基于实时价格监控的智能平仓策略（独立持仓，全部平仓）
"""
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Tuple
from decimal import Decimal
from loguru import logger
import mysql.connector
from mysql.connector import pooling
import aiohttp

from app.services.price_sampler import PriceSampler
from app.services.signal_analysis_service import SignalAnalysisService
from app.analyzers.kline_strength_scorer import KlineStrengthScorer
from app.services.api_key_service import get_api_key_service


class SmartExitOptimizer:
    """智能平仓优化器（基于实时价格监控 + K线强度衰减检测 + 全部平仓）"""

    def __init__(self, db_config: dict, live_engine, price_service, account_id=None):
        """
        初始化平仓优化器

        Args:
            db_config: 数据库配置
            live_engine: 交易引擎（用于执行平仓）
            price_service: 价格服务（WebSocket实时价格）
            account_id: 账户ID（可选，如果不提供则从live_engine获取或默认为2）
        """
        self.db_config = db_config
        self.live_engine = live_engine
        self.price_service = price_service
        # 优先使用传入的account_id，其次从live_engine获取，最后默认为2
        if account_id is not None:
            self.account_id = account_id
        else:
            self.account_id = getattr(live_engine, 'account_id', 2)

        # 数据库连接池（增加池大小以支持多个并发监控任务）
        # 每个监控任务每秒需要1个连接，预留32个连接支持32个并发持仓监控
        self.db_pool = pooling.MySQLConnectionPool(
            pool_name="exit_optimizer_pool",
            pool_size=32,
            **db_config
        )

        # 监控状态
        self.monitoring_tasks: Dict[str, asyncio.Task] = {}  # position_id -> task

        # 智能平仓计划
        self.exit_plans: Dict[int, Dict] = {}  # position_id -> exit_plan

        # === K线强度监控 ===
        self.signal_analyzer = SignalAnalysisService(db_config)
        self.kline_scorer = KlineStrengthScorer()
        self.enable_kline_monitoring = True  # 启用K线强度监控

        # K线强度检查间隔（15分钟）
        self.kline_check_interval = 900  # 秒
        self.last_kline_check: Dict[int, datetime] = {}  # position_id -> last_check_time

        # === 智能监控策略 K线缓冲区 (新增) ===
        self.kline_5m_buffer: Dict[int, List] = {}  # position_id -> 最近N根5M K线
        self.kline_15m_buffer: Dict[int, List] = {}  # position_id -> 最近N根15M K线
        self.last_5m_check: Dict[int, datetime] = {}  # position_id -> 上次检查5M的时间
        self.last_15m_check: Dict[int, datetime] = {}  # position_id -> 上次检查15M的时间

        # 价格采样器（用于150分钟后的最优价格评估）
        self.price_samples: Dict[int, List[float]] = {}  # position_id -> 价格采样列表

        # === HTTP Session 复用（性能优化）===
        self._http_session: Optional[aiohttp.ClientSession] = None

        # 分阶段超时 - 亏损持续时间追踪（生物学负反馈：需持续亏损才平仓，非瞬时亏损）
        # key: "position_id:hour_checkpoint"，value: 首次触发该档位亏损的时间
        self._loss_onset_times: Dict[str, datetime] = {}

        # 移动止盈追踪：记录每个持仓的历史峰值ROI
        self.peak_roi: Dict[int, float] = {}

    def _get_pool_connection(self):
        """从连接池获取连接，并设置InnoDB锁等待超时"""
        conn = self.db_pool.get_connection()
        cursor = conn.cursor()
        cursor.execute("SET SESSION innodb_lock_wait_timeout = 5")
        cursor.close()
        return conn

    async def start_monitoring_position(self, position_id: int):
        """
        开始监控持仓（从开仓完成后立即开始）

        Args:
            position_id: 持仓ID
        """
        if position_id in self.monitoring_tasks:
            logger.warning(f"持仓 {position_id} 已在监控中")
            return

        # 创建独立监控任务
        task = asyncio.create_task(self._monitor_position(position_id))
        self.monitoring_tasks[position_id] = task

        logger.info(f"✅ 开始监控持仓 {position_id}")

    async def stop_monitoring_position(self, position_id: int):
        """
        停止监控持仓

        Args:
            position_id: 持仓ID
        """
        if position_id in self.monitoring_tasks:
            self.monitoring_tasks[position_id].cancel()
            del self.monitoring_tasks[position_id]

            # 清理K线检查时间记录
            if position_id in self.last_kline_check:
                del self.last_kline_check[position_id]

            # 清理K线缓冲区
            if position_id in self.kline_5m_buffer:
                del self.kline_5m_buffer[position_id]
            if position_id in self.kline_15m_buffer:
                del self.kline_15m_buffer[position_id]
            if position_id in self.last_5m_check:
                del self.last_5m_check[position_id]
            if position_id in self.last_15m_check:
                del self.last_15m_check[position_id]

            # 清理价格采样
            if position_id in self.price_samples:
                del self.price_samples[position_id]

            # 清理移动止盈峰值记录
            if position_id in self.peak_roi:
                del self.peak_roi[position_id]

            logger.info(f"⏹️ 停止监控持仓 {position_id}")

    async def _monitor_position(self, position_id: int):
        """
        持仓监控主循环（实时价格监控）

        Args:
            position_id: 持仓ID
        """
        try:
            while True:
                # 获取持仓信息
                position = await self._get_position(position_id)

                if not position:
                    logger.info(f"持仓 {position_id} 不存在，停止监控")
                    break

                # 支持monitoring status='open'
                if position['status'] not in ('open', 'building'):
                    logger.info(f"持仓 {position_id} 已关闭 (status={position['status']})，停止监控")
                    break

                # 获取实时价格
                current_price = await self._get_realtime_price(position['symbol'])

                # 如果无法获取价格，跳过本次检查
                if current_price is None:
                    logger.warning(f"持仓{position_id} {position['symbol']} 无法获取价格，跳过本次平仓检查")
                    await asyncio.sleep(2)  # 等待2秒后重试
                    continue

                # 计算当前盈亏（如果avg_entry_price为空，使用entry_price作为备用）
                try:
                    profit_info = self._calculate_profit(position, current_price)
                except ValueError as ve:
                    # avg_entry_price 或 quantity 为空，可能是持仓刚创建或正在建仓中
                    logger.debug(f"持仓{position_id} 计算盈亏失败（可能正在建仓）: {ve}")
                    await asyncio.sleep(2)
                    continue

                # 更新最高盈利记录
                await self._update_max_profit(position_id, profit_info)

                # === 更新K线缓冲区和价格采样（用于智能监控）===
                await self._update_kline_buffers(position_id, position['symbol'])
                await self._update_price_samples(position_id, float(current_price))

                # 检查兜底平仓条件（超高盈利/巨额亏损）
                should_close, reason = await self._check_exit_conditions(
                    position, current_price, profit_info
                )

                if should_close:
                    logger.info(
                        f"🚨 触发兜底平仓: 持仓{position_id} {position['symbol']} "
                        f"{position['direction']} | {reason}"
                    )
                    await self._execute_close(position_id, current_price, reason)
                    break

                # === K线强度衰减检测 (新增 - 每15分钟检查一次) ===
                should_check_kline = await self._should_check_kline_strength(position_id)
                if should_check_kline and self.enable_kline_monitoring:
                    kline_exit_signal = await self._check_kline_strength_decay(
                        position, current_price, profit_info
                    )
                    if kline_exit_signal:
                        reason, ratio = kline_exit_signal
                        logger.info(
                            f"📊 K线强度衰减触发平仓: 持仓{position_id} {position['symbol']} | {reason}"
                        )
                        # 统一全部平仓，不再分批
                        await self._execute_close(position_id, current_price, reason)
                        break

                # 检查智能平仓
                exit_completed = await self._smart_exit(
                    position_id, position, current_price, profit_info
                )

                if exit_completed:
                    logger.info(f"✅ 智能平仓完成: 持仓{position_id}")
                    break

                await asyncio.sleep(1)  # 每秒检查一次（实时监控）

        except asyncio.CancelledError:
            logger.info(f"监控任务被取消: 持仓 {position_id}")
        except Exception as e:
            logger.error(f"监控持仓 {position_id} 异常: {type(e).__name__}: {e}", exc_info=True)
        finally:
            # 任务自然结束或异常结束时，从 monitoring_tasks 中移除自己
            # 避免健康检查因 db_count != monitoring_count 误触发重启
            if position_id in self.monitoring_tasks:
                del self.monitoring_tasks[position_id]
                logger.debug(f"监控任务自清理: 持仓 {position_id}")
            # 清理该持仓的亏损计时记录，防止内存泄漏
            keys_to_delete = [k for k in self._loss_onset_times if k.startswith(f"{position_id}:")]
            for k in keys_to_delete:
                del self._loss_onset_times[k]

    async def _get_position(self, position_id: int) -> Optional[Dict]:
        """
        获取持仓信息

        Args:
            position_id: 持仓ID

        Returns:
            持仓字典
        """
        conn = None
        cursor = None
        try:
            conn = self._get_pool_connection()
            cursor = conn.cursor(dictionary=True)

            cursor.execute("""
                SELECT
                    id, symbol, position_side as direction, status,
                    avg_entry_price, quantity as position_size,
                    entry_signal_time, open_time, planned_close_time,
                    close_extended, extended_close_time,
                    max_profit_pct, max_profit_price, max_profit_time,
                    stop_loss_price, take_profit_price, leverage,
                    margin, entry_price, max_hold_minutes, timeout_at, created_at,
                    source
                FROM futures_positions
                WHERE id = %s
            """, (position_id,))

            return cursor.fetchone()

        except Exception as e:
            logger.error(f"获取持仓信息失败: {e}")
            return None
        finally:
            if cursor:
                try: cursor.close()
                except Exception: pass
            if conn:
                try: conn.close()
                except Exception: pass

    async def _get_realtime_price(self, symbol: str) -> Decimal:
        """
        获取实时价格（多级降级策略）

        Args:
            symbol: 交易对

        Returns:
            当前价格
        """
        # 第1级: WebSocket价格
        try:
            price = self.price_service.get_price(symbol)
            if price and price > 0:
                return Decimal(str(price))
        except Exception as e:
            logger.warning(f"{symbol} WebSocket获取失败: {e}")

        # 第2级: REST API实时价格（异步，复用session）
        try:
            symbol_clean = symbol.replace('/', '').upper()

            # 根据交易对类型选择API
            if symbol.endswith('/USD'):
                # 币本位合约使用dapi
                api_url = 'https://dapi.binance.com/dapi/v1/ticker/price'
                symbol_for_api = symbol_clean + '_PERP'
            else:
                # U本位合约使用fapi
                api_url = 'https://fapi.binance.com/fapi/v1/ticker/price'
                symbol_for_api = symbol_clean

            session = await self._get_http_session()
            async with session.get(
                api_url,
                params={'symbol': symbol_for_api},
                timeout=aiohttp.ClientTimeout(total=3)
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    # 币本位API返回数组，U本位返回对象
                    if isinstance(data, list) and len(data) > 0:
                        rest_price = float(data[0]['price'])
                    else:
                        rest_price = float(data['price'])

                    if rest_price > 0:
                        logger.debug(f"{symbol} 降级到REST API价格: {rest_price}")
                        return Decimal(str(rest_price))
        except Exception as e:
            logger.warning(f"{symbol} REST API获取失败: {e}")

        # 第3级: 使用持仓的最后已知价格（entry_price或mark_price）作为最后保底
        # 绝对不能返回0，否则会误触发止盈止损
        logger.error(f"{symbol} WebSocket和REST API都失败，这不应该发生！请检查网络连接")
        return None  # 返回None表示无法获取价格，让调用方决定如何处理

    def _calculate_profit(self, position: Dict, current_price: Decimal) -> Dict:
        """
        计算当前盈亏信息

        Args:
            position: 持仓信息
            current_price: 当前价格

        Returns:
            {'profit_pct': float, 'profit_usdt': float, 'current_price': float}
        """
        # 验证必要字段
        entry_price_val = position.get('entry_price')
        position_size_val = position.get('position_size')

        # 直接使用 entry_price（不再使用avg_entry_price）
        if entry_price_val is None or entry_price_val == '':
            raise ValueError(f"持仓 {position.get('id')} entry_price 为空")

        if position_size_val is None or position_size_val == '' or float(position_size_val) == 0:
            raise ValueError(f"持仓 {position.get('id')} position_size 为空或为0")

        entry_price = Decimal(str(entry_price_val))
        position_size = Decimal(str(position_size_val))
        direction = position['direction']

        # 计算盈亏百分比
        if direction == 'LONG':
            profit_pct = float((current_price - entry_price) / entry_price * 100)
        else:  # SHORT
            profit_pct = float((entry_price - current_price) / entry_price * 100)

        # 计算盈亏金额（USDT）
        profit_usdt = float(position_size * entry_price * Decimal(str(profit_pct / 100)))

        return {
            'profit_pct': profit_pct,
            'profit_usdt': profit_usdt,
            'current_price': float(current_price)
        }

    async def _update_max_profit(self, position_id: int, profit_info: Dict):
        """
        更新最高盈利记录（原子操作，避免一键平仓时锁等待超时）
        """
        conn = None
        cursor = None
        try:
            conn = self._get_pool_connection()
            cursor = conn.cursor()

            # 短锁等待超时(2s)：平仓时行锁被占用则快速失败，不阻塞监控循环
            cursor.execute("SET innodb_lock_wait_timeout = 2")

            # 原子条件更新：仅在盈利更高且仓位仍开放时写入，无需先 SELECT
            cursor.execute("""
                UPDATE futures_positions
                SET
                    max_profit_pct   = %s,
                    max_profit_price = %s,
                    max_profit_time  = NOW()
                WHERE id = %s
                  AND status = 'open'
                  AND (max_profit_pct IS NULL OR max_profit_pct < %s)
            """, (
                profit_info['profit_pct'],
                profit_info['current_price'],
                position_id,
                profit_info['profit_pct']
            ))
            conn.commit()

        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                except Exception:
                    pass
            logger.debug(f"更新最高盈利跳过: 持仓{position_id} - {e}")
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass

    async def _check_exit_conditions(
        self,
        position: Dict,
        current_price: Decimal,
        profit_info: Dict
    ) -> tuple[bool, str]:
        """
        检查平仓条件（分层逻辑）

        Args:
            position: 持仓信息
            current_price: 当前价格
            profit_info: 盈亏信息

        Returns:
            (should_close: bool, reason: str)
        """
        profit_pct = profit_info['profit_pct']
        max_profit_pct = float(position['max_profit_pct']) if position['max_profit_pct'] else 0.0

        # 计算ROI（相对保证金的收益率）
        leverage = float(position.get('leverage', 1))
        roi_pct = profit_pct * leverage

        # 计算当前回撤（从最高点）
        drawback = max_profit_pct - profit_pct

        _source = position.get('source', '') or ''
        _SCHEDULED_PREFIXES = ('discovery_trader', 'top50_trader', 'alien_trader', 'market_trader', 'dimension_trader')
        _is_scheduled = any(_source.startswith(p) for p in _SCHEDULED_PREFIXES)

        # ========== 优先级最高：止损止盈检查（任何时候都检查） ==========

        # 检查止损价格
        stop_loss_price = position.get('stop_loss_price')
        if stop_loss_price and float(stop_loss_price) > 0:
            stop_loss_price = Decimal(str(stop_loss_price))
            direction = position['direction']

            if direction == 'LONG':
                # 多头：当前价格 <= 止损价
                if current_price <= stop_loss_price:
                    return True, f"止损(价格{current_price:.8f} <= 止损价{stop_loss_price:.8f}, 价格变化{profit_pct:.2f}%, ROI {roi_pct:.2f}%)"
            else:  # SHORT
                # 空头：当前价格 >= 止损价
                if current_price >= stop_loss_price:
                    return True, f"止损(价格{current_price:.8f} >= 止损价{stop_loss_price:.8f}, 价格变化{profit_pct:.2f}%, ROI {roi_pct:.2f}%)"

        # 检查止盈价格
        take_profit_price = position.get('take_profit_price')
        if take_profit_price and float(take_profit_price) > 0:
            take_profit_price = Decimal(str(take_profit_price))
            direction = position['direction']

            if direction == 'LONG':
                # 多头：当前价格 >= 止盈价
                if current_price >= take_profit_price:
                    return True, f"止盈(价格{current_price:.8f} >= 止盈价{take_profit_price:.8f}, 价格变化{profit_pct:.2f}%, ROI {roi_pct:.2f}%)"
            else:  # SHORT
                # 空头：当前价格 <= 止盈价
                if current_price <= take_profit_price:
                    return True, f"止盈(价格{current_price:.8f} <= 止盈价{take_profit_price:.8f}, 价格变化{profit_pct:.2f}%, ROI {roi_pct:.2f}%)"

        # discovery_trader / top50_trader / dimension_trader: skip smart-exit logic, only SL/TP and max_hold
        if _is_scheduled:
            _open_ref = position.get('created_at') or position.get('open_time')
            # 首选：planned_close_time / timeout_at（由开仓时策略 hold_h 换算的精确时间）
            _deadline = position.get('planned_close_time') or position.get('timeout_at')
            if _deadline:
                if datetime.now() >= _deadline:
                    hold_hours = (datetime.now() - _open_ref).total_seconds() / 3600 if _open_ref else 0
                    return True, f"max_hold到期 (planned_close到期, 持仓{hold_hours:.1f}h)"
                return False, ""
            # 次选：max_hold_minutes 列（策略参数写入的分钟数）
            max_hold_minutes = position.get('max_hold_minutes')
            if max_hold_minutes and max_hold_minutes > 0 and _open_ref:
                hold_minutes = (datetime.now() - _open_ref).total_seconds() / 60
                if hold_minutes >= max_hold_minutes:
                    hold_hours = hold_minutes / 60
                    return True, f"max_hold到期 ({hold_hours:.1f}h, max={max_hold_minutes/60:.0f}h)"
                return False, ""
            # 两者都缺失：数据异常，不强制平仓
            logger.warning(f"[MAX_HOLD] 仓位 {position.get('id')} source={_source} 缺少 planned_close_time 和 max_hold_minutes，跳过超时检查")
            return False, ""

        # ========== 智能监控逻辑（开仓30分钟后启动，每秒实时检查）==========
        position_id = position['id']
        position_side = position.get('position_side', position['direction'])
        entry_time = position.get('entry_signal_time') or position.get('open_time') or datetime.now()
        hold_minutes = (datetime.now() - entry_time).total_seconds() / 60

        MIN_HOLD_MINUTES = 60  # 60分钟最小持仓时间（原30分钟，延长以减少被假突破割肉）

        # 计算ROI（考虑杠杆后的真实收益率）
        leverage = float(position.get('leverage', 1))
        roi_pct = profit_pct * leverage

        # === 优先级1: 极端亏损兜底止损（无需等待60分钟）===
        # 改为基于ROI判断，ROI亏损≥10%立即止损
        if roi_pct <= -10.0:
            return True, f"极端亏损止损(ROI{roi_pct:.2f}%≤-10%, 价格变化{profit_pct:.2f}%)"

        # === 开仓60分钟后启动智能监控 ===
        if hold_minutes >= MIN_HOLD_MINUTES:

            # === 优先级2: 趋势反转止损（基于16根5M K线方向投票）===
            # LONG: ≥11/16逆向 → 趋势反转（多头市场短周期震荡多，需更高确信度）
            # SHORT: ≥9/16逆向 → 趋势反转（空单对反弹更敏感，保持原门槛）
            #        7~8根逆向 → 趋势不明朗 → 继续等待
            #        < 7根逆向 → 方向仍合理 → 不干预
            if profit_pct < 0:
                against_count = await self._count_against_direction_5m(position_id, position_side)
                reversal_threshold = 11
                if against_count >= reversal_threshold:
                    logger.info(
                        f"📊 持仓{position_id} {position_side} 趋势反转: "
                        f"{against_count}/16根5M K线逆向(阈值{reversal_threshold}), 价格{profit_pct:.2f}%, ROI{roi_pct:.2f}%"
                    )
                    return True, f"趋势反转({against_count}/16根K线逆向,价格{profit_pct:.2f}%,ROI{roi_pct:.2f}%)"
                elif against_count >= 7:
                    logger.debug(
                        f"持仓{position_id} {position_side} 趋势不明朗({against_count}/16逆向)，继续等待"
                    )

            # === 优先级3: 移动止盈止损 ===
            # 激活: ROI >= 6% 后开始追踪；从峰值回撤 >= 2.5% ROI 则平仓
            # （原0.5%阈值过紧，改为2.5%避免被插针误触发）
            TRAILING_ACTIVATE_ROI = 6.0   # 盈利6% ROI后开启追踪
            TRAILING_DRAWDOWN_PCT = 2.5   # 从峰值回撤2.5% ROI触发平仓

            # 更新峰值ROI
            if roi_pct > self.peak_roi.get(position_id, 0.0):
                self.peak_roi[position_id] = roi_pct

            peak = self.peak_roi.get(position_id, 0.0)
            if peak >= TRAILING_ACTIVATE_ROI:
                drawdown = peak - roi_pct
                if drawdown >= TRAILING_DRAWDOWN_PCT:
                    return True, f"移动止盈触发(峰值ROI{peak:.2f}%,当前ROI{roi_pct:.2f}%,回撤{drawdown:.2f}%)"

        # ========== 智能平仓逻辑（计划平仓前30分钟）==========
        planned_close_time = position['planned_close_time']

        # 如果没有设置计划平仓时间，只检查止损止盈，不执行智能平仓
        if planned_close_time is None:
            return False, ""

        now = datetime.now()
        monitoring_start_time = planned_close_time - timedelta(minutes=30)

        # 如果还未到监控时间，继续其他检查（不再直接返回）
        if now < monitoring_start_time:
            return False, ""

        # ========== 到达监控窗口，使用智能平仓 ==========
        # 注意：这里不再直接返回平仓决策
        # 而是在 _monitor_position 中调用 _smart_exit 处理平仓
        # 这个方法现在主要用于兜底逻辑

        # 兜底逻辑1: 超高盈利立即全部平仓（改为基于ROI）
        if roi_pct >= 25.0:
            return True, f"超高盈利全部平仓(ROI {roi_pct:.2f}%, 价格变化{profit_pct:.2f}%)"

        # 兜底逻辑2: 巨额亏损立即全部平仓（改为基于ROI）
        if roi_pct <= -10.0:
            return True, f"巨额亏损全部平仓(ROI {roi_pct:.2f}%, 价格变化{profit_pct:.2f}%)"

        # 默认：不平仓（由智能平仓处理）
        return False, ""

    async def _smart_exit(
        self,
        position_id: int,
        position: Dict,
        current_price: Decimal,
        profit_info: Dict
    ) -> bool:
        """
        智能平仓逻辑（计划平仓前30分钟）

        策略：
        1. T-30启动监控，T-20完成价格基线（10分钟采样）
        2. T-20到T+0寻找最佳价格，一次性平仓100%
        3. T+0（planned_close_time）必须强制执行

        时间窗口示例（planned_close_time = 11:46）:
        - 11:16 (T-30): 启动监控
        - 11:26 (T-20): 完成10分钟价格基线
        - 11:26-11:46: 20分钟寻找最佳平仓价格
        - 11:46 (T+0): 计划平仓时间，必须强制执行

        Args:
            position_id: 持仓ID
            position: 持仓信息
            current_price: 当前价格
            profit_info: 盈亏信息

        Returns:
            是否完成平仓
        """
        planned_close_time = position['planned_close_time']

        # 如果没有设置计划平仓时间，不执行智能平仓
        if planned_close_time is None:
            return False

        now = datetime.now()
        monitoring_start_time = planned_close_time - timedelta(minutes=30)

        # ========== 最高优先级：超时强制平仓 ==========
        if now >= planned_close_time:
            logger.warning(
                f"⚡ {position['symbol']} 已超过计划平仓时间，立即强制平仓! | "
                f"计划: {planned_close_time.strftime('%H:%M:%S')}, "
                f"当前: {now.strftime('%H:%M:%S')}"
            )
            # 获取当前价格
            current_price = await self._get_realtime_price(position['symbol'])
            await self._execute_close(position_id, current_price, "超时强制平仓")
            return True

        # 如果还未到监控时间，直接返回
        if now < monitoring_start_time:
            return False

        # 初始化平仓计划（第一次进入监控窗口）
        if position_id not in self.exit_plans:
            logger.info(
                f"🎯 {position['symbol']} 进入智能平仓窗口（30分钟） | "
                f"当前盈亏: {profit_info['profit_pct']:.2f}% | "
                f"计划平仓: {planned_close_time.strftime('%H:%M:%S')}"
            )

            # 启动价格基线采样器 (优化后: 10分钟采样窗口)
            sampler = PriceSampler(position['symbol'], self.price_service, window_seconds=600)
            sampling_task = asyncio.create_task(sampler.start_background_sampling())

            # 创建平仓计划
            exit_plan = {
                'symbol': position['symbol'],
                'direction': position['direction'],
                'entry_price': float(position['entry_price']),
                'total_quantity': float(position['position_size']),
                'monitoring_start_time': monitoring_start_time,
                'planned_close_time': planned_close_time,
                'sampler': sampler,
                'sampling_task': sampling_task,
                'baseline_built': False,
                'closed': False
            }

            self.exit_plans[position_id] = exit_plan

            # 优化后: 等待10分钟建立基线
            logger.info(f"📊 {position['symbol']} 等待10分钟建立平仓价格基线...")

        exit_plan = self.exit_plans[position_id]

        # 如果已经平仓，直接返回
        if exit_plan['closed']:
            return True

        sampler = exit_plan['sampler']

        # 等待基线建立
        if not exit_plan['baseline_built']:
            if sampler.initial_baseline_built:
                exit_plan['baseline_built'] = True
                baseline = sampler.get_current_baseline()
                logger.info(
                    f"✅ {position['symbol']} 平仓基线建立: "
                    f"范围 {baseline['min_price']:.6f} - {baseline['max_price']:.6f}"
                )
            else:
                # 基线还未建立，继续等待
                return False

        baseline = sampler.get_current_baseline()
        if not baseline:
            return False

        elapsed_minutes = (now - exit_plan['monitoring_start_time']).total_seconds() / 60

        # ========== 平仓判断（一次性100%）==========
        should_exit, reason = await self._should_exit_single(
            position, current_price, baseline, exit_plan['entry_price'],
            elapsed_minutes, planned_close_time
        )

        if should_exit:
            # 一次性平仓100%
            await self._execute_close(position_id, current_price, reason)
            exit_plan['closed'] = True

            logger.info(
                f"✅ 智能平仓完成: {position['symbol']} @ {current_price:.6f} | {reason}"
            )

            # 停止采样器
            sampler.stop_sampling()
            exit_plan['sampling_task'].cancel()

            # 清理平仓计划
            del self.exit_plans[position_id]

            return True  # 完成平仓

        return False  # 未完成平仓

    async def _should_exit_single(
        self,
        position: Dict,
        current_price: Decimal,
        baseline: Dict,
        entry_price: float,
        elapsed_minutes: float,
        planned_close_time: datetime
    ) -> tuple[bool, str]:
        """
        一次性平仓判断（100%）

        时间窗口: T-30 到 T+0 (30分钟)
        强制截止: T+0 (planned_close_time必须执行)

        策略：
        1. 寻找最佳价格立即平仓
        2. T+0（planned_close_time）必须强制执行

        Returns:
            (是否平仓, 原因)
        """
        direction = position['direction']
        now = datetime.now()

        # ========== 最高优先级：超时强制平仓（已到达planned_close_time）==========
        if now >= planned_close_time:
            return True, f"计划平仓时间已到，强制执行"

        if direction == 'LONG':
            # 使用 PriceSampler 的评分系统
            exit_plan = self.exit_plans[position['id']]
            sampler = exit_plan['sampler']
            evaluation = sampler.is_good_long_exit_price(current_price, entry_price)

            # ===== 智能优化器仅在亏损时介入（止损优化） =====
            # 盈利订单由正常止盈逻辑处理，不需要优化器提前平仓
            if evaluation['profit_pct'] < -1.0:
                # 亏损超过1%，启用止损优化

                # 条件1: 极佳卖点（评分 >= 95分）- 减少亏损
                if evaluation['score'] >= 95:
                    return True, f"止损优化-极佳卖点(评分{evaluation['score']}, 亏损{evaluation['profit_pct']:.2f}%): {evaluation['reason']}"

                # 条件2: 优秀卖点（评分 >= 85分）- 减少亏损
                if evaluation['score'] >= 85:
                    return True, f"止损优化-优秀卖点(评分{evaluation['score']}, 亏损{evaluation['profit_pct']:.2f}%)"

                # 条件3: 突破基线最高价（亏损时的反弹机会，减少损失）
                if float(current_price) >= baseline['max_price'] * 1.001:
                    return True, f"止损优化-突破基线最高价(亏损{evaluation['profit_pct']:.2f}%)"

                # 条件4: 强下跌趋势预警（亏损时趋势恶化，提前止损）
                if baseline['trend']['direction'] == 'down' and baseline['trend']['strength'] > 0.6:
                    return True, f"止损优化-强下跌趋势预警(亏损{evaluation['profit_pct']:.2f}%)"

            # 条件5: 时间压力（T-10分钟，无论盈亏都必须平仓）
            if elapsed_minutes >= 20 and evaluation['score'] >= 60:
                return True, f"接近截止(已{elapsed_minutes:.0f}分钟)，评分{evaluation['score']}"

        else:  # SHORT
            exit_plan = self.exit_plans[position['id']]
            sampler = exit_plan['sampler']
            evaluation = sampler.is_good_short_exit_price(current_price, entry_price)

            # ===== 智能优化器仅在亏损时介入（止损优化） =====
            # 盈利订单由正常止盈逻辑处理，不需要优化器提前平仓
            if evaluation['profit_pct'] < -1.0:
                # 亏损超过1%，启用止损优化

                # 条件1: 极佳买点（评分 >= 95分）- 减少亏损
                if evaluation['score'] >= 95:
                    return True, f"止损优化-极佳买点(评分{evaluation['score']}, 亏损{evaluation['profit_pct']:.2f}%): {evaluation['reason']}"

                # 条件2: 优秀买点（评分 >= 85分）- 减少亏损
                if evaluation['score'] >= 85:
                    return True, f"止损优化-优秀买点(评分{evaluation['score']}, 亏损{evaluation['profit_pct']:.2f}%)"

                # 条件3: 跌破基线最低价（亏损时的下探机会，减少损失）
                if float(current_price) <= baseline['min_price'] * 0.999:
                    return True, f"止损优化-跌破基线最低价(亏损{evaluation['profit_pct']:.2f}%)"

                # 条件4: 强上涨趋势预警（空单亏损时趋势恶化，提前止损）
                if baseline['trend']['direction'] == 'up' and baseline['trend']['strength'] > 0.6:
                    return True, f"止损优化-强上涨趋势预警(亏损{evaluation['profit_pct']:.2f}%)"

            # 条件5: 时间压力（T-10分钟，无论盈亏都必须平仓）
            if elapsed_minutes >= 20 and evaluation['score'] >= 60:
                return True, f"接近截止(已{elapsed_minutes:.0f}分钟)，评分{evaluation['score']}"

        return False, ""

    async def _execute_close(self, position_id: int, current_price: Decimal, reason: str):
        """
        执行平仓操作

        Args:
            position_id: 持仓ID
            current_price: 当前价格
            reason: 平仓原因
        """
        try:
            # 获取持仓信息
            position = await self._get_position(position_id)

            if not position:
                logger.error(f"持仓 {position_id} 不存在，无法平仓")
                return

            logger.info(
                f"🔴 执行平仓: 持仓{position_id} {position['symbol']} "
                f"{position['direction']} | 价格{current_price} | {reason}"
            )

            # 调用实盘引擎执行平仓
            close_result = await self.live_engine.close_position(
                symbol=position['symbol'],
                direction=position['direction'],
                position_size=float(position['position_size']),
                reason=reason
            )

            if close_result['success']:
                # 计算已实现盈亏
                # close_position_by_side already wrote the correct value to DB;
                # we compute here only so _update_position_closed has a fallback
                # (the SQL uses IF(... = 0, ...) so it won't overwrite a correct value).
                try:
                    ep = float(position.get('entry_price') or position.get('avg_entry_price') or 0)
                    qty = float(position.get('position_size') or 0)
                    cp = float(current_price)
                    if ep <= 0 or qty <= 0:
                        raise ValueError(f"invalid ep={ep} qty={qty}")
                    if position['direction'] == 'LONG':
                        realized_pnl = (cp - ep) * qty
                    else:
                        realized_pnl = (ep - cp) * qty
                except Exception as pnl_err:
                    logger.warning(f"本地PnL计算失败 (DB值将保留): {pnl_err}")
                    realized_pnl = 0.0

                # 同步平掉关联的实盘仓位（调交易所真实下单）
                await self._close_live_positions_on_exchange(
                    paper_position_id=position_id,
                    symbol=position['symbol'],
                    direction=position['direction'],
                    reason=reason
                )

                # 更新数据库状态
                await self._update_position_closed(
                    position_id,
                    float(current_price),
                    reason,
                    realized_pnl
                )

                logger.info(f"[INFO] 平仓成功: 持仓{position_id} pnl={realized_pnl:+.4f}")

                # 停止监控
                await self.stop_monitoring_position(position_id)
            else:
                logger.error(f"平仓失败: 持仓{position_id} | {close_result.get('error')}")

        except Exception as e:
            logger.error(f"执行平仓异常: {e}")

    async def _close_live_positions_on_exchange(
        self,
        paper_position_id: int,
        symbol: str,
        direction: str,
        reason: str
    ):
        """
        查找与模拟单关联的实盘仓位，调用 BinanceFuturesEngine 真实平仓
        """
        try:
            from app.trading.binance_futures_engine import BinanceFuturesEngine
            import pymysql
            import pymysql.cursors

            conn = pymysql.connect(
                **self.db_config, charset='utf8mb4',
                cursorclass=pymysql.cursors.DictCursor, autocommit=True
            )
            cur = conn.cursor()
            cur.execute(
                "SELECT lp.id, lp.account_id, lp.quantity, lp.entry_price "
                "FROM live_futures_positions lp "
                "WHERE lp.paper_position_id=%s AND lp.status='OPEN'",
                (paper_position_id,)
            )
            live_rows = cur.fetchall()
            cur.close(); conn.close()

            # 用 api_key_service 获取全部激活密钥（已解密）
            api_service = get_api_key_service()
            if not api_service:
                logger.warning("[实盘平仓] api_key_service 未初始化，跳过实盘平仓")
                return

            all_keys_list = api_service.get_all_active_api_keys()
            if not all_keys_list:
                logger.warning("[实盘平仓] 无有效API密钥，跳过实盘平仓")
                return

            if live_rows:
                # 有 paper_position_id 关联，按关联记录逐个平仓
                all_keys = {ak['id']: ak for ak in all_keys_list}
                for row in live_rows:
                    account_id = row['account_id']
                    key_info = all_keys.get(account_id)
                    if not key_info:
                        logger.warning(f"[实盘平仓] account_id={account_id} 无有效API密钥，跳过")
                        continue
                    try:
                        from decimal import Decimal as _Dec
                        engine = BinanceFuturesEngine(self.db_config, key_info['api_key'], key_info['api_secret'])
                        result = engine.close_position_direct(
                            symbol=symbol,
                            position_side=direction,
                            quantity=_Dec(str(row['quantity'])),
                            entry_price=_Dec(str(row['entry_price'])),
                            reason=reason
                        )
                        if result.get('success'):
                            logger.info(f"[实盘平仓] {key_info.get('account_name',account_id)} {symbol} {direction} 平仓成功")
                            # 交易所平仓成功，立即更新该条记录
                            try:
                                _uc = pymysql.connect(**self.db_config, autocommit=True)
                                _ucur = _uc.cursor()
                                close_p = result.get('close_price', 0)
                                live_pnl = result.get('realized_pnl', 0)
                                _ucur.execute("""
                                    UPDATE live_futures_positions
                                    SET status='CLOSED', close_time=NOW(),
                                        close_price=%s, close_reason=%s,
                                        realized_pnl=%s,
                                        notes=CONCAT(IFNULL(notes,''), '|exit_sync:', %s)
                                    WHERE id=%s AND status='OPEN'
                                """, (close_p, reason, live_pnl, reason, row['id']))
                                _ucur.close(); _uc.close()
                            except Exception as _dbe:
                                logger.error(f"[实盘平仓] 更新live_futures_positions失败 id={row['id']}: {_dbe}")
                        else:
                            logger.error(f"[实盘平仓] {key_info.get('account_name',account_id)} {symbol} {direction} 平仓失败: {result.get('error')}")
                    except Exception as e:
                        logger.error(f"[实盘平仓] account_id={account_id} 平仓异常: {e}")
            else:
                # fallback：无 paper_position_id 关联，对所有账号按 symbol+direction 平仓
                logger.info(f"[实盘平仓] 无 paper_position_id 关联，fallback 对所有账号平 {symbol} {direction}")
                for key_info in all_keys_list:
                    try:
                        engine = BinanceFuturesEngine(self.db_config, key_info['api_key'], key_info['api_secret'])
                        result = engine.close_position_by_symbol(symbol, direction, reason=reason)
                        if result.get('success'):
                            logger.info(f"[实盘平仓] {key_info.get('account_name','')} {symbol} {direction} 平仓成功")
                            # 按 account_id+symbol+side 更新
                            try:
                                _uc = pymysql.connect(**self.db_config, autocommit=True)
                                _ucur = _uc.cursor()
                                close_p = result.get('close_price', 0)
                                live_pnl = result.get('realized_pnl', 0)
                                _ucur.execute("""
                                    UPDATE live_futures_positions
                                    SET status='CLOSED', close_time=NOW(),
                                        close_price=%s, close_reason=%s,
                                        realized_pnl=%s,
                                        notes=CONCAT(IFNULL(notes,''), '|exit_sync_fallback:', %s)
                                    WHERE account_id=%s AND symbol=%s
                                      AND position_side=%s AND status='OPEN'
                                """, (close_p, reason, live_pnl, reason, key_info['id'], symbol, direction))
                                _ucur.close(); _uc.close()
                            except Exception as _dbe:
                                logger.error(f"[实盘平仓] fallback更新live_futures_positions失败: {_dbe}")
                        else:
                            err = result.get('error', '')
                            if '未找到' in err:
                                logger.debug(f"[实盘平仓] {key_info.get('account_name','')} {symbol} 交易所无此仓位，跳过")
                            else:
                                logger.error(f"[实盘平仓] {key_info.get('account_name','')} {symbol} 平仓失败: {err}")
                    except Exception as e:
                        logger.error(f"[实盘平仓] {key_info.get('account_name','')} {symbol} 平仓异常: {e}")

        except Exception as e:
            logger.error(f"[实盘平仓] _close_live_positions_on_exchange 异常: {e}")

    async def _update_position_closed(
        self,
        position_id: int,
        close_price: float,
        close_reason: str,
        realized_pnl: float = 0.0
    ):
        """
        更新持仓为已平仓状态

        Args:
            position_id: 持仓ID
            close_price: 平仓价格
            close_reason: 平仓原因
            realized_pnl: 已实现盈亏
        """
        conn = None
        cursor = None
        try:
            conn = self._get_pool_connection()
            cursor = conn.cursor()

            now = datetime.now()
            # Use IF(...) to protect the realized_pnl already set by close_position_by_side.
            # Only overwrite if the field is still NULL or exactly 0 (unset).
            cursor.execute("""
                UPDATE futures_positions
                SET
                    status = 'closed',
                    close_time = %s,
                    mark_price = %s,
                    realized_pnl = IF(realized_pnl IS NULL OR realized_pnl = 0, %s, realized_pnl),
                    notes = CONCAT(IFNULL(notes, ''), '|close_reason:', %s)
                WHERE id = %s
            """, (now, close_price, round(realized_pnl, 4), close_reason, position_id))
            # live_futures_positions 由 _close_live_positions_on_exchange 在交易所平仓成功后逐条更新
            # 不在此处无条件更新，避免交易所未平仓时 DB 被错误标为 CLOSED

            conn.commit()

        except Exception as e:
            logger.error(f"更新持仓状态失败: {e}")
        finally:
            if cursor:
                try: cursor.close()
                except Exception: pass
            if conn:
                try: conn.close()
                except Exception: pass

    # ==================== K线强度监控方法 (新增) ====================

    async def _should_check_kline_strength(self, position_id: int) -> bool:
        """
        判断是否需要检查K线强度（每15分钟检查一次）

        Args:
            position_id: 持仓ID

        Returns:
            是否需要检查
        """
        now = datetime.now()

        if position_id not in self.last_kline_check:
            # 首次检查
            self.last_kline_check[position_id] = now
            return True

        last_check = self.last_kline_check[position_id]
        elapsed = (now - last_check).total_seconds()

        if elapsed >= self.kline_check_interval:
            self.last_kline_check[position_id] = now
            return True

        return False

    async def _update_kline_buffers(self, position_id: int, symbol: str):
        """
        更新K线缓冲区（5M和15M）

        Args:
            position_id: 持仓ID
            symbol: 交易对
        """
        try:
            now = datetime.now()

            # === 更新5M K线缓冲区 ===
            if position_id not in self.kline_5m_buffer:
                # 首次初始化：获取最近20根5M K线（用于趋势统计）
                klines = await self._fetch_latest_kline(symbol, '5m', limit=20)
                if klines:
                    self.kline_5m_buffer[position_id] = klines
                    self.last_5m_check[position_id] = now
                    logger.debug(f"初始化5M K线缓冲区: 持仓{position_id}，获取{len(klines)}根K线")
            elif (now - self.last_5m_check.get(position_id, now)).total_seconds() >= 300:
                # 定期更新：每5分钟检查一次
                klines = await self._fetch_latest_kline(symbol, '5m', limit=1)
                if klines and len(klines) > 0:
                    latest_kline = klines[0]

                    # 检查是否是新K线（避免重复）
                    if len(self.kline_5m_buffer[position_id]) == 0 or \
                       latest_kline['close_time'] > self.kline_5m_buffer[position_id][-1]['close_time']:
                        self.kline_5m_buffer[position_id].append(latest_kline)
                        # 只保留最近20根（覆盖100分钟，足够趋势统计）
                        if len(self.kline_5m_buffer[position_id]) > 20:
                            self.kline_5m_buffer[position_id] = self.kline_5m_buffer[position_id][-20:]
                        logger.debug(f"更新5M K线: 持仓{position_id}，收盘时间{latest_kline['close_time']}")

                    self.last_5m_check[position_id] = now

            # === 更新15M K线缓冲区 ===
            if position_id not in self.kline_15m_buffer:
                # 首次初始化：获取最近3根15M K线
                klines = await self._fetch_latest_kline(symbol, '15m', limit=3)
                if klines:
                    self.kline_15m_buffer[position_id] = klines
                    self.last_15m_check[position_id] = now
                    logger.debug(f"初始化15M K线缓冲区: 持仓{position_id}，获取{len(klines)}根K线")
            elif (now - self.last_15m_check.get(position_id, now)).total_seconds() >= 900:
                # 定期更新：每15分钟检查一次
                klines = await self._fetch_latest_kline(symbol, '15m', limit=1)
                if klines and len(klines) > 0:
                    latest_kline = klines[0]

                    # 检查是否是新K线（避免重复）
                    if len(self.kline_15m_buffer[position_id]) == 0 or \
                       latest_kline['close_time'] > self.kline_15m_buffer[position_id][-1]['close_time']:
                        self.kline_15m_buffer[position_id].append(latest_kline)
                        # 只保留最近3根
                        if len(self.kline_15m_buffer[position_id]) > 3:
                            self.kline_15m_buffer[position_id] = self.kline_15m_buffer[position_id][-3:]
                        logger.debug(f"更新15M K线: 持仓{position_id}，收盘时间{latest_kline['close_time']}")

                    self.last_15m_check[position_id] = now

        except Exception as e:
            logger.error(f"更新K线缓冲区失败: {e}")

    async def _get_http_session(self):
        """获取或创建HTTP session（复用以提升性能）"""
        if self._http_session is None or self._http_session.closed:
            # 创建连接器，限制并发连接数避免过载
            connector = aiohttp.TCPConnector(limit=50, limit_per_host=10)
            self._http_session = aiohttp.ClientSession(connector=connector)
        return self._http_session

    async def _fetch_latest_kline(self, symbol: str, interval: str, limit: int = 1):
        """
        获取最新K线数据（异步，复用session）

        Args:
            symbol: 交易对
            interval: 时间间隔（5m/15m）
            limit: 获取K线数量（默认1根，初始化时可获取多根）

        Returns:
            K线字典列表 [{open, high, low, close, close_time, open_time}]
        """
        try:
            symbol_clean = symbol.replace('/', '').upper()

            # 根据交易对类型选择API
            if symbol.endswith('/USD'):
                api_url = 'https://dapi.binance.com/dapi/v1/klines'
                symbol_for_api = symbol_clean + '_PERP'
            else:
                api_url = 'https://fapi.binance.com/fapi/v1/klines'
                symbol_for_api = symbol_clean

            session = await self._get_http_session()
            async with session.get(
                api_url,
                params={'symbol': symbol_for_api, 'interval': interval, 'limit': limit},
                timeout=aiohttp.ClientTimeout(total=10)  # 增加超时时间到10秒
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    if data and len(data) > 0:
                        # 返回K线列表
                        klines = []
                        for kline in data:
                            klines.append({
                                'open': float(kline[1]),
                                'high': float(kline[2]),
                                'low': float(kline[3]),
                                'close': float(kline[4]),
                                'open_time': datetime.fromtimestamp(kline[0] / 1000),
                                'close_time': datetime.fromtimestamp(kline[6] / 1000)
                            })
                        return klines
                else:
                    # 记录非200状态码
                    logger.warning(f"获取{symbol} {interval} K线失败: HTTP {response.status}")
                    return None
        except asyncio.TimeoutError:
            logger.warning(f"获取{symbol} {interval} K线超时（10秒）")
            return None
        except Exception as e:
            logger.warning(f"获取{symbol} {interval} K线失败: {type(e).__name__}: {e}")
            return None

    async def _count_against_direction_5m(self, position_id: int, position_side: str) -> int:
        """
        统计最近16根5M K线中，与持仓方向相反的K线数量。

        用于趋势反转判断：
          ≥ 10/16（62.5%）逆向 → 趋势已反转，立即平仓
          8~9/16              → 趋势不明朗，继续等待
          < 8/16              → 方向未变，不干预

        Args:
            position_id: 持仓ID
            position_side: 持仓方向（LONG/SHORT）

        Returns:
            逆向K线根数，缓冲区不足16根时返回 -1（表示数据不足，不触发）
        """
        if position_id not in self.kline_5m_buffer:
            return -1
        buffer = self.kline_5m_buffer[position_id]
        if len(buffer) < 16:
            return -1  # 数据不足，不触发

        last_16 = buffer[-16:]
        against_count = 0
        for candle in last_16:
            if position_side == 'LONG' and candle['close'] < candle['open']:
                against_count += 1
            elif position_side == 'SHORT' and candle['close'] > candle['open']:
                against_count += 1
        return against_count

    async def _check_5m_no_improvement_single(self, position_id: int, position_side: str) -> bool:
        """
        检查最新1根5M K线是否无好转（快速止损）

        Args:
            position_id: 持仓ID
            position_side: 持仓方向（LONG/SHORT）

        Returns:
            是否无好转
        """
        if position_id not in self.kline_5m_buffer:
            return False

        buffer = self.kline_5m_buffer[position_id]
        if len(buffer) < 1:
            return False

        latest_candle = buffer[-1]

        # 判断最新K线是否朝不利方向发展
        if position_side == 'LONG':
            # 多仓: 期待阳线，如果是阴线则无好转
            if latest_candle['close'] < latest_candle['open']:
                logger.debug(f"持仓{position_id} LONG 5M阴线无好转: {latest_candle['open']:.6f} -> {latest_candle['close']:.6f}")
                return True
        else:  # SHORT
            # 空仓: 期待阴线，如果是阳线则无好转
            if latest_candle['close'] > latest_candle['open']:
                logger.debug(f"持仓{position_id} SHORT 5M阳线无好转: {latest_candle['open']:.6f} -> {latest_candle['close']:.6f}")
                return True

        return False

    async def _check_5m_no_improvement(self, position_id: int, position_side: str) -> bool:
        """
        检查连续2根5M K线是否无好转（谨慎止损）

        Args:
            position_id: 持仓ID
            position_side: 持仓方向（LONG/SHORT）

        Returns:
            是否无好转
        """
        if position_id not in self.kline_5m_buffer:
            return False

        buffer = self.kline_5m_buffer[position_id]
        if len(buffer) < 2:
            return False

        candle_1, candle_2 = buffer[-2:]

        # 判断是否持续恶化或无明显好转
        if position_side == 'LONG':
            # 多仓: 期待价格上涨，如果连续2根收盘价不涨则无好转
            if candle_2['close'] <= candle_1['close']:
                logger.debug(f"持仓{position_id} LONG 2根5M无好转: {candle_1['close']:.6f} -> {candle_2['close']:.6f}")
                return True  # 继续下跌或横盘
        else:  # SHORT
            # 空仓: 期待价格下跌，如果连续2根收盘价不跌则无好转
            if candle_2['close'] >= candle_1['close']:
                logger.debug(f"持仓{position_id} SHORT 2根5M无好转: {candle_1['close']:.6f} -> {candle_2['close']:.6f}")
                return True  # 继续上涨或横盘

        return False

    async def _check_15m_no_sustained_improvement(self, position_id: int, position_side: str) -> bool:
        """
        检查2根15M K线是否无持续好转

        Args:
            position_id: 持仓ID
            position_side: 持仓方向（LONG/SHORT）

        Returns:
            是否无持续好转
        """
        if position_id not in self.kline_15m_buffer:
            return False

        buffer = self.kline_15m_buffer[position_id]
        if len(buffer) < 2:
            return False

        candle_1, candle_2 = buffer[-2:]

        # 判断是否持续好转
        if position_side == 'LONG':
            # 第1根好转但第2根反转
            if candle_1['close'] > candle_1['open'] and candle_2['close'] < candle_1['close']:
                logger.debug(
                    f"持仓{position_id} LONG 15M无持续好转: "
                    f"K1 {candle_1['open']:.6f}->{candle_1['close']:.6f}, "
                    f"K2 {candle_2['open']:.6f}->{candle_2['close']:.6f}"
                )
                return True
        else:  # SHORT
            # 第1根好转但第2根反转
            if candle_1['close'] < candle_1['open'] and candle_2['close'] > candle_1['close']:
                logger.debug(
                    f"持仓{position_id} SHORT 15M无持续好转: "
                    f"K1 {candle_1['open']:.6f}->{candle_1['close']:.6f}, "
                    f"K2 {candle_2['open']:.6f}->{candle_2['close']:.6f}"
                )
                return True

        return False

    async def _update_price_samples(self, position_id: int, current_price: float):
        """
        更新价格采样（用于150分钟后的最优价格评估）

        Args:
            position_id: 持仓ID
            current_price: 当前价格
        """
        if position_id not in self.price_samples:
            self.price_samples[position_id] = []

        self.price_samples[position_id].append(current_price)

        # 只保留最近30分钟的数据（每秒1个，保留1800个）
        if len(self.price_samples[position_id]) > 1800:
            self.price_samples[position_id] = self.price_samples[position_id][-1800:]

    async def _find_optimal_exit_price(self, position_id: int, position_side: str, current_price: float, profit_pct: float) -> bool:
        """
        寻找最优平仓价格（150分钟后启动）

        Args:
            position_id: 持仓ID
            position_side: 持仓方向
            current_price: 当前价格
            profit_pct: 当前盈亏百分比

        Returns:
            是否找到最优价格
        """
        if position_id not in self.price_samples or len(self.price_samples[position_id]) < 600:
            # 数据不足（少于10分钟）
            return False

        recent_prices = self.price_samples[position_id][-1800:]  # 最近30分钟

        if profit_pct > 0:
            # 盈利场景: 寻找局部高点
            if position_side == 'LONG':
                # 做多: 当前价格是最近10分钟的最高点
                recent_10min = recent_prices[-600:]
                if current_price >= max(recent_10min):
                    logger.info(f"持仓{position_id} LONG 找到局部高点 ${current_price:.6f}，盈利{profit_pct:.2f}%")
                    return True
            else:  # SHORT
                # 做空: 当前价格是最近10分钟的最低点
                recent_10min = recent_prices[-600:]
                if current_price <= min(recent_10min):
                    logger.info(f"持仓{position_id} SHORT 找到局部低点 ${current_price:.6f}，盈利{profit_pct:.2f}%")
                    return True
        else:
            # 亏损场景: 寻找相对回升点
            if position_side == 'LONG':
                # 做多亏损: 价格反弹（相对回升）
                recent_10min = recent_prices[-600:]
                if current_price >= max(recent_10min[-120:]):  # 最近2分钟的高点
                    logger.info(f"持仓{position_id} LONG 找到相对回升点 ${current_price:.6f}，亏损{profit_pct:.2f}%")
                    return True
            else:  # SHORT
                # 做空亏损: 价格回落（相对回升）
                recent_10min = recent_prices[-600:]
                if current_price <= min(recent_10min[-120:]):  # 最近2分钟的低点
                    logger.info(f"持仓{position_id} SHORT 找到相对回落点 ${current_price:.6f}，亏损{profit_pct:.2f}%")
                    return True

        return False

    async def _check_top_bottom(self, symbol: str, position_side: str, entry_price: float, leverage: float = 1.0) -> tuple:
        """
        检查是否触发顶底识别

        Args:
            symbol: 交易对
            position_side: 持仓方向（LONG/SHORT）
            entry_price: 开仓价格
            leverage: 杠杆倍数（默认1倍）

        Returns:
            (is_top_bottom: bool, reason: str)
        """
        try:
            # 从live_engine获取当前价格
            current_price = self.live_engine.get_current_price(symbol)
            if not current_price:
                return False, ""

            # 计算当前盈亏比例
            if position_side == 'LONG':
                profit_pct = ((current_price - entry_price) / entry_price) * 100
            else:  # SHORT
                profit_pct = ((entry_price - current_price) / entry_price) * 100

            # 计算ROI（考虑杠杆）
            roi_pct = profit_pct * leverage

            # 获取1h和4h K线强度
            strength_1h = self.signal_analyzer.analyze_kline_strength(symbol, '1h', 24)
            strength_4h = self.signal_analyzer.analyze_kline_strength(symbol, '4h', 24)

            if not strength_1h or not strength_4h:
                return False, ""

            # 顶部识别（针对LONG持仓）- 改为基于ROI
            if position_side == 'LONG':
                # 条件1: 有盈利（ROI至少10%）
                has_profit = roi_pct >= 10.0

                # 条件2: 1h和4h都转为强烈看空
                strong_bearish_1h = strength_1h.get('net_power', 0) <= -5
                strong_bearish_4h = strength_4h.get('net_power', 0) <= -3

                if has_profit and strong_bearish_1h and strong_bearish_4h:
                    return True, f"顶部识别(ROI盈利{roi_pct:.1f}%+强烈看空,价格{profit_pct:.1f}%)"

            # 底部识别（针对SHORT持仓）- 改为基于ROI
            elif position_side == 'SHORT':
                # 条件1: 有盈利（ROI至少10%）
                has_profit = roi_pct >= 10.0

                # 条件2: 1h和4h都转为强烈看多
                strong_bullish_1h = strength_1h.get('net_power', 0) >= 5
                strong_bullish_4h = strength_4h.get('net_power', 0) >= 3

                if has_profit and strong_bullish_1h and strong_bullish_4h:
                    return True, f"底部识别(ROI盈利{roi_pct:.1f}%+强烈看多,价格{profit_pct:.1f}%)"

            return False, ""

        except Exception as e:
            logger.error(f"检查顶底识别失败: {e}")
            return False, ""

    async def _check_kline_strength_decay(
        self,
        position: Dict,
        current_price: float,
        profit_info: Dict
    ) -> Optional[Tuple[str, float]]:
        """
        统一平仓检查（止盈止损 + 超时 + K线强度衰减）

        优先级（从高到低）：
        1. 极端亏损兜底止损（ROI≤-10%）
        2. 固定止盈检查（兜底）
        3. 智能顶底识别（30分钟后）
        4. 最优价格评估（150分钟后）
        5. 动态超时检查
        6. 分阶段超时检查
        7. 绝对时间强制平仓
        8. K线强度衰减检查

        Args:
            position: 持仓信息
            current_price: 当前价格
            profit_info: 盈亏信息

        Returns:
            (平仓原因, 平仓比例) 或 None
        """
        try:
            position_id = position['id']
            symbol = position['symbol']
            direction = position['direction']
            position_side = position.get('position_side', direction)  # LONG/SHORT
            entry_price = float(position.get('entry_price', 0))
            entry_time = position.get('entry_signal_time') or position.get('open_time') or datetime.now()
            quantity = float(position.get('quantity', 0))

            # discovery_trader / top50_trader / dimension_trader 仓位：跳过智能平仓逻辑，只检查 max_hold 超时
            _source = position.get('source', '')
            _SCHED = ('discovery_trader', 'top50_trader', 'alien_trader', 'market_trader', 'dimension_trader')
            if any(_source.startswith(p) for p in _SCHED):
                _created_at = position.get('created_at') or position.get('open_time')
                if _created_at:
                    _hold_min = (datetime.now() - _created_at).total_seconds() / 60
                    _max_hold = position.get('max_hold_minutes') or 240
                    if _hold_min >= _max_hold:
                        return (f'max_hold到期({_hold_min/60:.1f}h)', 1.0)
                return None
            margin = float(position.get('margin', 0))
            leverage = float(position.get('leverage', 1))

            # 获取持仓时长（分钟）
            hold_minutes = (datetime.now() - entry_time).total_seconds() / 60
            hold_hours = hold_minutes / 60

            # ============================================================
            # === 优先级0: 最小持仓时间限制 (30分钟) ===
            # ============================================================
            # 开仓60分钟内只允许止损和止盈,不允许其他原因平仓
            MIN_HOLD_MINUTES = 60  # 60分钟最小持仓时间（原30分钟，延长以减少假突破割肉）

            # ============================================================
            # === 优先级1: 极端亏损兜底止损（风控底线，无需等待最小持仓时间） ===
            # ============================================================
            # 只在极端情况下立即止损，正常亏损由智能监控策略处理

            pnl_pct = profit_info.get('profit_pct', 0)
            roi_pct = pnl_pct * leverage  # 计算ROI（考虑杠杆）

            # 极端亏损立即止损（兜底保护）- 改为基于ROI
            if roi_pct <= -10.0:
                # ROI亏损>=10%，立即止损（防止继续扩大）
                logger.warning(
                    f"🛑 持仓{position_id} {symbol} {position_side} 触发极端亏损止损 | "
                    f"ROI亏损{roi_pct:.2f}% ≤ -10% (价格变化{pnl_pct:.2f}%)，立即止损"
                )
                return ('极端亏损止损(ROI≤-10%)', 1.0)

            # ============================================================
            # === 优先级2: 固定止盈检查（兜底） ===
            # ============================================================
            take_profit_price = position.get('take_profit_price')
            if take_profit_price and float(take_profit_price) > 0:
                if position_side == 'LONG':
                    if current_price >= float(take_profit_price):
                        pnl_pct = profit_info.get('profit_pct', 0)
                        logger.info(
                            f"✅ 持仓{position_id} {symbol} LONG触发固定止盈 | "
                            f"当前价${current_price:.6f} >= 止盈价${take_profit_price:.6f} | "
                            f"盈亏{pnl_pct:+.2f}%"
                        )
                        return ('固定止盈', 1.0)
                elif position_side == 'SHORT':
                    if current_price <= float(take_profit_price):
                        pnl_pct = profit_info.get('profit_pct', 0)
                        logger.info(
                            f"✅ 持仓{position_id} {symbol} SHORT触发固定止盈 | "
                            f"当前价${current_price:.6f} <= 止盈价${take_profit_price:.6f} | "
                            f"盈亏{pnl_pct:+.2f}%"
                        )
                        return ('固定止盈', 1.0)

            # ============================================================
            # === 在此之后的所有平仓检查都需要满足最小持仓时间(30分钟) ===
            # ============================================================
            # 开仓30分钟内不平仓(除了止损和止盈)
            if hold_minutes < MIN_HOLD_MINUTES:
                # 30分钟内只允许止损和止盈,不进行其他平仓检查
                return None

            # ============================================================
            # === 优先级4: 智能顶底识别 ===
            # ============================================================
            # 注: 已满足30分钟最小持仓时间,现在可以检查顶底
            is_top_bottom, tb_reason = await self._check_top_bottom(symbol, position_side, entry_price, leverage)
            if is_top_bottom:
                logger.info(
                    f"🔝 持仓{position_id} {symbol}触发顶底识别: {tb_reason} | "
                    f"持仓{hold_hours:.1f}小时"
                )
                return (tb_reason, 1.0)

            # ============================================================
            # === 优先级4.5: 最优价格评估（150分钟后启动）===
            # ============================================================
            # 接近3小时持仓时间（150分钟后），启动价格评估系统寻找最优平仓点
            if hold_minutes >= 150:
                pnl_pct = profit_info.get('profit_pct', 0)
                optimal_found = await self._find_optimal_exit_price(
                    position_id, position_side, float(current_price), pnl_pct
                )
                if optimal_found:
                    logger.info(
                        f"💎 持仓{position_id} {symbol} {position_side} 找到最优平仓价格 | "
                        f"持仓{hold_minutes:.0f}分钟 | 盈亏{pnl_pct:+.2f}%"
                    )
                    return ('最优价格评估', 1.0)

            # ============================================================
            # === 优先级5: 动态超时检查（基于timeout_at字段） ===
            # ============================================================
            timeout_at = position.get('timeout_at')
            if timeout_at:
                now_utc = datetime.now()
                if now_utc >= timeout_at:
                    max_hold_minutes = position.get('max_hold_minutes') or 240  # fallback 4小时
                    logger.warning(
                        f"⏰ 持仓{position_id} {symbol}触发动态超时 | "
                        f"超时阈值{max_hold_minutes}分钟"
                    )
                    return (f'动态超时({max_hold_minutes}min)', 1.0)

            # ============================================================
            # === 优先级5.5: 信号衰减检查（沉没成本防御）===
            # ============================================================
            # 原理：如果V2 K线评分方向已强力反转，应果断止损，而非因"已持这么久了"而继续持有
            # 触发条件：当前亏损 + V2方向与持仓方向相反 + V2强度为strong（非偶发波动）
            # Big4=BULLISH时LONG仓：要求ROI<-3%才触发（避免BULLISH行情中短暂回调被踢出）
            try:
                _decay_pnl_pct = profit_info.get('profit_pct', 0) / 100.0
                if _decay_pnl_pct < 0:  # 只在亏损时才触发（盈利时继续持有）
                    conn = self.db_pool.get_connection()
                    try:
                        cursor = conn.cursor()
                        # 查V2评分
                        cursor.execute(
                            "SELECT direction, strength_level FROM coin_kline_scores WHERE symbol = %s LIMIT 1",
                            (symbol,)
                        )
                        row = cursor.fetchone()
                        # 查Big4最新信号
                        cursor.execute(
                            "SELECT overall_signal FROM big4_trend_history ORDER BY created_at DESC LIMIT 1"
                        )
                        big4_row = cursor.fetchone()
                        cursor.close()
                    finally:
                        conn.close()
                    if row:
                        v2_direction = row[0]   # 'LONG' or 'SHORT' or 'NEUTRAL'
                        v2_strength = row[1]    # 'strong', 'moderate', 'weak'
                        # 方向反转 + 强度strong → 信号衰减，立即平仓
                        opposite_direction = ('SHORT' if position_side == 'LONG' else 'LONG')
                        if v2_direction == opposite_direction and v2_strength == 'strong':
                            # Big4=BULLISH时做多仓位：要求ROI<-3%才触发，避免短暂回调被踢出
                            big4_signal = big4_row[0] if big4_row else 'NEUTRAL'
                            if position_side == 'LONG' and big4_signal in ('BULLISH', 'STRONG_BULLISH'):
                                _decay_roi = _decay_pnl_pct * leverage
                                if _decay_roi > -3.0:
                                    logger.info(
                                        f"[SIGNAL_DECAY] {symbol} LONG Big4={big4_signal}，"
                                        f"V2反转SHORT但ROI={_decay_roi:.1f}%>-3%，跳过衰减平仓"
                                    )
                                    pass  # 不触发，继续持有
                                else:
                                    logger.warning(
                                        f"🧠 [SIGNAL_DECAY] 持仓{position_id} {symbol} {position_side} "
                                        f"亏损ROI{_decay_roi:.1f}%，V2方向已强力反转({v2_direction} strong)，"
                                        f"Big4={big4_signal}但亏损已达触发门槛，信号衰减平仓"
                                    )
                                    return (f'信号衰减(V2强力反转{v2_direction})', 1.0)
                            else:
                                logger.warning(
                                    f"🧠 [SIGNAL_DECAY] 持仓{position_id} {symbol} {position_side} "
                                    f"亏损{pnl_pct*100:.2f}%，V2方向已强力反转({v2_direction} strong)，"
                                    f"信号完全衰减，沉没成本防御触发平仓"
                                )
                                return (f'信号衰减(V2强力反转{v2_direction})', 1.0)
            except Exception as _e:
                logger.debug(f"[SIGNAL_DECAY] {symbol} 检查失败: {_e}")

            # ============================================================
            # === 优先级6: 分阶段超时检查（1h/2h/3h/4h不同亏损阈值） ===
            # ============================================================
            # 获取分阶段超时阈值配置
            # 针对上涨趋势优化: 放宽阈值,给持仓更多时间
            staged_thresholds = {
                1: -0.025,  # 1小时: -2.5% (放宽0.5%)
                2: -0.02,   # 2小时: -2.0% (放宽0.5%)
                3: -0.015,  # 3小时: -1.5% (放宽0.5%)
                4: -0.01    # 4小时: -1.0% (放宽0.5%)
            }

            # 尝试从配置中获取
            if hasattr(self.live_engine, 'opt_config'):
                config_thresholds = self.live_engine.opt_config.get_staged_timeout_thresholds()
                if config_thresholds:
                    staged_thresholds = config_thresholds

            pnl_pct = profit_info.get('profit_pct', 0) / 100.0  # 转换为小数

            for hour_checkpoint, loss_threshold in sorted(staged_thresholds.items()):
                if hold_hours >= hour_checkpoint:
                    onset_key = f"{position_id}:{hour_checkpoint}"
                    if pnl_pct < loss_threshold:
                        now_ts = datetime.now()
                        if onset_key not in self._loss_onset_times:
                            # 首次触发该档位亏损，开始计时，本轮不平仓
                            self._loss_onset_times[onset_key] = now_ts
                            logger.info(
                                f"⏳ 持仓{position_id} {symbol} [{hour_checkpoint}h档] "
                                f"亏损{pnl_pct*100:.2f}%开始计时，需持续30分钟才触发平仓"
                            )
                        else:
                            loss_duration_min = (now_ts - self._loss_onset_times[onset_key]).total_seconds() / 60
                            if loss_duration_min >= 30:
                                logger.warning(
                                    f"⏱️ 持仓{position_id} {symbol}触发分阶段超时 | "
                                    f"持仓{hold_hours:.1f}h >= {hour_checkpoint}h | "
                                    f"亏损{pnl_pct*100:.2f}%持续{loss_duration_min:.0f}分钟"
                                )
                                return (f'分阶段超时{hour_checkpoint}H(亏损{pnl_pct*100:.1f}%持续{loss_duration_min:.0f}分)', 1.0)
                            else:
                                logger.info(
                                    f"⏳ {symbol} [{hour_checkpoint}h档] 亏损持续{loss_duration_min:.0f}/30分钟，继续等待..."
                                )
                    else:
                        # 亏损恢复，清除该档位计时（习惯化重置）
                        if onset_key in self._loss_onset_times:
                            del self._loss_onset_times[onset_key]
                            logger.info(f"✅ {symbol} [{hour_checkpoint}h档] 亏损已恢复，重置计时器")

            # ============================================================
            # === 优先级7: 3小时绝对时间强制平仓 ===
            # ============================================================
            max_hold_minutes = position.get('max_hold_minutes') or 180
            if hold_minutes >= max_hold_minutes:
                hold_hours_cfg = max_hold_minutes / 60
                logger.warning(f"⏰ 持仓{position_id} {symbol}已持有{hold_hours:.1f}小时，触发{hold_hours_cfg:.0f}小时强制平仓")
                return (f'持仓时长到期({hold_hours_cfg:.0f}小时强制平仓)', 1.0)

            # ============================================================
            # === 优先级8: K线强度衰减检查（智能平仓） ===
            # ============================================================
            # 注意: 15M强力反转和亏损+反转已在优先级1处理(止损风控),这里不再重复检查

            # 获取K线强度
            strength_1h = self.signal_analyzer.analyze_kline_strength(symbol, '1h', 24)
            strength_15m = self.signal_analyzer.analyze_kline_strength(symbol, '15m', 24)
            strength_5m = self.signal_analyzer.analyze_kline_strength(symbol, '5m', 24)

            if not all([strength_1h, strength_15m, strength_5m]):
                return None

            # 计算当前K线强度评分
            current_kline = self.kline_scorer.calculate_strength_score(
                strength_1h, strength_15m, strength_5m
            )

            # === 亏损 + 强度反转（止损，全平） ===
            # 注意: 这个检查在1小时限制之后,所以不会过早触发
            # 改为基于ROI判断
            pnl_pct = profit_info['profit_pct']
            roi_pct = pnl_pct * leverage
            if roi_pct < -2.0:
                # ROI亏损>2%，检查K线方向是否反转
                if current_kline['direction'] != 'NEUTRAL' and current_kline['direction'] != direction:
                    logger.warning(
                        f"⚠️ 持仓{position_id} {symbol}ROI亏损{roi_pct:.2f}%>2%且K线方向反转 | "
                        f"当前方向{current_kline['direction']} vs 持仓{direction} (价格变化{pnl_pct:.2f}%)"
                    )
                    return ('亏损ROI>2%+方向反转', 1.0)

            # === 禁用盈利平仓，让利润奔跑 ===
            # 注: 盈利单不再平仓，由固定止盈8%或顶底识别触发全部平仓
            # 只有亏损单才平仓

            # 【已禁用】盈利平仓逻辑
            # 原因: 分批止盈导致平均盈利只有5.46U，应该让利润奔跑
            #
            # if current_stage == 0:
            #     if profit_info['profit_pct'] >= 2.0 and current_kline['total_score'] < 15:
            #         return ('盈利>=2%+强度大幅减弱', 0.5)
            #
            # 新策略: 盈利单不分批，等待固定止盈8%或移动止盈

            return None

        except Exception as e:
            logger.error(f"检查K线强度衰减失败: {e}")
            return None

