# -*- coding: utf-8 -*-
"""
实盘订单监控服务

核心职责：
- 监控限价单成交状态（PENDING → FILLED）
- 限价单成交后自动设置止损止盈订单
- 趋势转向时自动取消未成交限价单

架构说明：
- 实盘不负责策略判断（开仓/平仓条件、止损触发、智能止盈等）
- 所有策略判断由模拟盘完成
- 实盘仅同步执行模拟盘的操作（下单/平仓/撤单）
- 这样避免了重复检查，确保策略逻辑统一
"""

import asyncio
from datetime import datetime
from decimal import Decimal
from typing import Dict, Optional, List
import pymysql
import json
from loguru import logger

# 导入交易通知器
try:
    from app.services.trade_notifier import get_trade_notifier
except ImportError:
    get_trade_notifier = None


class LiveOrderMonitor:
    """
    实盘订单监控器

    职责：
    1. 监控限价单成交状态
    2. 成交后自动设置止损止盈订单

    注意：
    - 不负责策略判断（由模拟盘负责）
    - 不检查智能止盈/止损（由模拟盘负责）
    - 仅执行订单管理和风控单设置
    """

    def __init__(self, db_config: Dict, live_engine):
        """
        初始化监控器

        Args:
            db_config: 数据库配置
            live_engine: 实盘交易引擎实例 (BinanceFuturesEngine)
        """
        self.db_config = db_config
        self.live_engine = live_engine
        self.running = False
        self.task = None
        self.connection = None
        self.check_interval = 10  # 检查间隔（秒）

    def _get_connection(self):
        """获取数据库连接"""
        if self.connection is None or not self.connection.open:
            try:
                self.connection = pymysql.connect(
                    host=self.db_config.get('host', 'localhost'),
                    port=self.db_config.get('port', 3306),
                    user=self.db_config.get('user', 'root'),
                    password=self.db_config.get('password', ''),
                    database=self.db_config.get('database', 'binance-data'),
                    charset='utf8mb4',
                    cursorclass=pymysql.cursors.DictCursor,
                    autocommit=True
                )
            except Exception as e:
                logger.error(f"[实盘监控] 创建数据库连接失败: {e}")
                raise
        else:
            try:
                self.connection.ping(reconnect=True)
            except Exception:
                self.connection = pymysql.connect(
                    host=self.db_config.get('host', 'localhost'),
                    port=self.db_config.get('port', 3306),
                    user=self.db_config.get('user', 'root'),
                    password=self.db_config.get('password', ''),
                    database=self.db_config.get('database', 'binance-data'),
                    charset='utf8mb4',
                    cursorclass=pymysql.cursors.DictCursor,
                    autocommit=True
                )
        return self.connection

    def _calculate_ema(self, prices: List[float], period: int) -> List[float]:
        """
        计算EMA（指数移动平均）

        Args:
            prices: 价格列表
            period: EMA周期

        Returns:
            EMA值列表
        """
        if len(prices) < period:
            return []

        ema_values = []
        multiplier = 2 / (period + 1)

        # 初始EMA使用SMA
        sma = sum(prices[:period]) / period
        ema_values.append(sma)

        # 计算后续EMA
        for i in range(period, len(prices)):
            ema = prices[i] * multiplier + ema_values[-1] * (1 - multiplier)
            ema_values.append(ema)

        return ema_values

    def _check_trend_reversal(self, position: Dict) -> Optional[str]:
        """
        检查趋势是否已转向（出现反向EMA交叉信号）

        Args:
            position: 仓位信息

        Returns:
            取消原因（如果需要取消），否则返回 None
        """
        try:
            symbol = position['symbol']
            position_side = position['position_side']  # LONG 或 SHORT

            # 默认使用15分钟时间周期
            timeframe = '15m'

            # 查询最近的K线数据
            conn = self._get_connection()
            cursor = conn.cursor()

            cursor.execute(
                """SELECT close_price
                FROM kline_data
                WHERE symbol = %s AND timeframe = %s
                ORDER BY timestamp DESC
                LIMIT 50""",
                (symbol, timeframe)
            )
            klines = cursor.fetchall()

            if not klines or len(klines) < 30:
                return None  # K线数据不足，跳过检查

            # 将K线反转为正序（从旧到新）
            prices = [float(k['close_price']) for k in reversed(klines)]

            # 计算EMA9和EMA26
            ema9_values = self._calculate_ema(prices, 9)
            ema26_values = self._calculate_ema(prices, 26)

            if len(ema9_values) < 2 or len(ema26_values) < 2:
                return None

            # 取最后两个EMA值来判断交叉
            curr_ema9 = ema9_values[-1]
            prev_ema9 = ema9_values[-2]
            curr_ema26 = ema26_values[-1]
            prev_ema26 = ema26_values[-2]

            # 检测死叉（EMA9下穿EMA26）
            is_death_cross = (prev_ema9 >= prev_ema26 and curr_ema9 < curr_ema26) or \
                            (prev_ema9 > prev_ema26 and curr_ema9 <= curr_ema26)

            # 检测金叉（EMA9上穿EMA26）
            is_golden_cross = (prev_ema9 <= prev_ema26 and curr_ema9 > curr_ema26) or \
                             (prev_ema9 < prev_ema26 and curr_ema9 >= curr_ema26)

            # 做多限价单，出现死叉则取消
            if position_side == 'LONG' and is_death_cross:
                ema_diff_pct = abs((curr_ema9 - curr_ema26) / curr_ema26 * 100)
                return f"趋势转向(死叉): EMA9={curr_ema9:.4f} < EMA26={curr_ema26:.4f}, 差值={ema_diff_pct:.2f}%"

            # 做空限价单，出现金叉则取消
            if position_side == 'SHORT' and is_golden_cross:
                ema_diff_pct = abs((curr_ema9 - curr_ema26) / curr_ema26 * 100)
                return f"趋势转向(金叉): EMA9={curr_ema9:.4f} > EMA26={curr_ema26:.4f}, 差值={ema_diff_pct:.2f}%"

            return None

        except Exception as e:
            logger.error(f"[实盘监控] 检查趋势转向时出错: {e}")
            return None

    async def _cancel_binance_order(self, position: Dict, reason: str):
        """
        取消币安订单

        Args:
            position: 仓位信息
            reason: 取消原因
        """
        try:
            symbol = position['symbol']
            order_id = position['binance_order_id']

            # 调用交易引擎取消订单
            result = self.live_engine.cancel_order(symbol, order_id)

            if result.get('success'):
                logger.info(f"[实盘监控] ✓ 币安订单已取消: {symbol} #{order_id} - {reason}")

                # 更新数据库状态
                await self._update_position_canceled(
                    position,
                    'CANCELED',  # 使用简短的状态码
                    cancellation_reason=f'trend_reversal: {reason}'
                )

                # 发送Telegram通知
                self._send_order_cancel_notification(position, reason)
            else:
                logger.error(f"[实盘监控] ✗ 取消币安订单失败: {result.get('error', '未知错误')}")

        except Exception as e:
            logger.error(f"[实盘监控] 取消币安订单异常: {e}")

    def start(self):
        """启动监控"""
        if self.running:
            logger.warning("[实盘监控] 监控已在运行中")
            return

        self.running = True
        self.task = asyncio.create_task(self._monitor_loop())
        logger.info("[实盘监控] 订单监控服务已启动")

    def stop(self):
        """停止监控"""
        self.running = False
        if self.task:
            self.task.cancel()
        logger.info("[实盘监控] 订单监控服务已停止")

    async def _monitor_loop(self):
        """
        监控循环

        职责：
        - 监控限价单成交状态
        - 限价单成交后自动设置止损止盈

        注意：
        实盘不负责策略判断（开仓/平仓/止损触发等），所有策略判断由模拟盘完成。
        实盘仅同步执行模拟盘的操作（下单/平仓/撤单）。
        """
        while self.running:
            try:
                # 检查待成交的限价单（成交后设置止损止盈）
                await self._check_pending_orders()

                # ❌ 已禁用：实盘不做策略判断，智能止盈由模拟盘负责
                # await self._check_smart_exit_for_open_positions()
            except Exception as e:
                logger.error(f"[实盘监控] 监控循环出错: {e}")

            await asyncio.sleep(self.check_interval)

    async def _check_pending_orders(self):
        """检查待处理的限价单"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # 设置会话时区为 UTC+8
            cursor.execute("SET time_zone = '+08:00'")

            # 查询状态为 PENDING 的限价单
            # 数据流说明 (2026-04-25):
            #   - live_futures_positions.stop_loss_price / take_profit_price 经常 NULL
            #     (paper_limit_sync_service 在 sl_pct/tp_pct 计算失败时会传 None)
            #   - 真实 SL/TP 在 paper 主表 futures_positions, 由策略层维护 (含 trail-tp 更新)
            #   - 通过 binance_order_id 关联: live_futures_positions <-> futures_orders <-> futures_positions
            #   - COALESCE 优先用 live 表自身值, 缺失时回退到 paper 表值
            #
            # 时间过滤 (created_at >= NOW() - INTERVAL 30 MINUTE):
            #   - 跳过历史已被手动处理的 PENDING 单, 避免对幽灵单反复查询币安
            #   - 30 min 给新开仓的 LIMIT 单留够成交窗口 (典型限价超时 < 30 min)
            #
            # 去除原有 LEFT JOIN trading_strategies: 该表不存在, timeout_minutes 硬置 0
            cursor.execute("""
                SELECT p.id, p.account_id, p.binance_order_id, p.symbol, p.position_side, p.quantity,
                       COALESCE(p.stop_loss_price, fp.stop_loss_price)     AS stop_loss_price,
                       COALESCE(p.take_profit_price, fp.take_profit_price) AS take_profit_price,
                       p.leverage, p.entry_price,
                       p.strategy_id, p.created_at, p.source,
                       p.sl_order_id, p.tp_order_id,
                       0 AS timeout_minutes,
                       TIMESTAMPDIFF(SECOND, p.created_at, NOW()) as elapsed_seconds
                FROM live_futures_positions p
                LEFT JOIN futures_orders fo
                       ON fo.binance_order_id = p.binance_order_id
                      AND fo.live_sync_status = 'SYNCED'
                LEFT JOIN futures_positions fp
                       ON fp.id = fo.position_id
                WHERE p.status = 'PENDING'
                  AND p.binance_order_id IS NOT NULL
                  AND p.created_at >= NOW() - INTERVAL 30 MINUTE
            """)

            pending_positions = cursor.fetchall()

            if not pending_positions:
                return

            logger.debug(f"[实盘监控] 发现 {len(pending_positions)} 个待监控的限价单")

            for position in pending_positions:
                await self._check_order_status(position)

        except Exception as e:
            logger.error(f"[实盘监控] 检查待处理订单失败: {e}")

    async def _check_order_status(self, position: Dict):
        """检查单个订单的状态"""
        try:
            order_id = position['binance_order_id']
            symbol = position['symbol']
            binance_symbol = symbol.replace('/', '').upper()
            position_side = position['position_side']

            # 查询币安订单状态
            result = self.live_engine._request('GET', '/fapi/v1/order', {
                'symbol': binance_symbol,
                'orderId': order_id
            })

            if isinstance(result, dict) and result.get('success') == False:
                logger.warning(f"[实盘监控] 查询订单 {order_id} 失败: {result.get('error')}")
                return

            status = result.get('status', '')
            executed_qty = Decimal(str(result.get('executedQty', '0')))
            avg_price = Decimal(str(result.get('avgPrice', '0')))

            if status == 'FILLED' and executed_qty > 0:
                logger.info(f"[实盘监控] 限价单 {order_id} 已成交: {executed_qty} @ {avg_price}")

                # 更新数据库状态
                await self._update_position_filled(position, executed_qty, avg_price)

                # 设置止损止盈
                await self._place_sl_tp_orders(position, executed_qty)

            elif status == 'NEW':
                # 订单尚未成交

                # 1. 检查趋势是否转向
                trend_reversal_reason = self._check_trend_reversal(position)
                if trend_reversal_reason:
                    logger.info(f"[实盘监控] 📉 检测到趋势转向，准备取消限价单: {symbol} #{order_id}")
                    await self._cancel_binance_order(position, trend_reversal_reason)
                    return

                # 2. 限价单超时转市价 - 已禁用
                # 原因：模拟盘的 futures_limit_order_executor.py 已经处理限价单超时，
                # 并会同步到实盘开仓。如果这里也处理，会导致重复开仓。
                #
                # 注意：实盘限价单的超时取消由模拟盘的限价单超时逻辑触发同步取消。
                # 这里只需要处理：
                # - 限价单成交后设置止损止盈（上面的 FILLED 分支）
                # - 趋势转向时取消限价单（上面的 _check_trend_reversal 逻辑）
                pass

            elif status in ['CANCELED', 'EXPIRED', 'REJECTED']:
                # 订单已取消/过期/拒绝，更新数据库
                logger.info(f"[实盘监控] 限价单 {order_id} 状态: {status}")
                await self._update_position_canceled(position, status)

        except Exception as e:
            logger.error(f"[实盘监控] 检查订单状态失败: {e}")

    async def _update_position_filled(self, position: Dict, executed_qty: Decimal, avg_price: Decimal):
        """更新已成交的仓位"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            update_sql = """UPDATE live_futures_positions
                SET status = 'OPEN',
                    quantity = %s,
                    entry_price = %s,
                    updated_at = NOW()
                WHERE id = %s"""
            update_params = (float(executed_qty), float(avg_price), position['id'])

            cursor.execute(update_sql, update_params)
            conn.commit()  # 🔧 修复：添加 commit，确保数据库更新生效

            logger.info(f"[实盘监控] 仓位 {position['id']} 已更新为 OPEN")

        except Exception as e:
            logger.error(f"[实盘监控] 更新仓位状态失败: {e}")

    async def _update_position_canceled(self, position: Dict, status: str, cancellation_reason: str = None):
        """
        更新已取消的仓位

        Args:
            position: 仓位信息
            status: 状态（如 TIMEOUT_PRICE_DEVIATION, TREND_REVERSAL 等）
            cancellation_reason: 取消原因（strategy_signal/timeout/price_deviation/trend_reversal）
        """
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # 更新 live_futures_positions 表
            update_sql = """UPDATE live_futures_positions
                SET status = %s,
                    updated_at = NOW()
                WHERE id = %s"""
            update_params = (status, position['id'])

            cursor.execute(update_sql, update_params)

            # 同时更新 futures_orders 表的 cancellation_reason
            if cancellation_reason:
                # 查找对应的订单记录（通过 binance_order_id）
                order_id = position.get('binance_order_id')
                if order_id:
                    cursor.execute("""
                        UPDATE futures_orders
                        SET cancellation_reason = %s,
                            status = 'CANCELLED',
                            canceled_at = NOW()
                        WHERE binance_order_id = %s
                          AND status IN ('PENDING', 'NEW')
                    """, (cancellation_reason, order_id))

                    logger.info(f"[实盘监控] 订单 {order_id} 取消原因已更新: {cancellation_reason}")

            conn.commit()  # 🔧 修复：添加 commit
            logger.info(f"[实盘监控] 仓位 {position['id']} 已更新为 {status}")

        except Exception as e:
            logger.error(f"[实盘监控] 更新仓位状态失败: {e}")

    async def _handle_limit_order_timeout(self, position: Dict, order_id: str, elapsed_minutes: float):
        """
        处理限价单超时

        超时后的处理逻辑：
        - 价格偏离 ≤0.5%: 取消限价单，以市价重新开仓
        - 价格偏离 >0.5%: 取消限价单，不开仓（避免追高/杀低）

        Args:
            position: 仓位信息
            order_id: 币安订单ID
            elapsed_minutes: 已等待分钟数
        """
        try:
            symbol = position['symbol']
            binance_symbol = symbol.replace('/', '').upper()
            position_side = position['position_side']
            limit_price = Decimal(str(position.get('entry_price', 0)))

            # 获取当前价格
            current_price = self.live_engine.get_current_price(symbol)
            if current_price == 0:
                logger.warning(f"[实盘监控] 无法获取 {symbol} 当前价格，跳过超时处理")
                return

            current_price = Decimal(str(current_price))

            # 计算价格偏离
            # 做多：当前价高于限价太多（追高）
            # 做空：当前价低于限价太多（杀低）
            if position_side == 'LONG':
                deviation_pct = (current_price - limit_price) / limit_price * 100
            else:  # SHORT
                deviation_pct = (limit_price - current_price) / limit_price * 100

            max_deviation_pct = Decimal('0.5')  # 最大允许偏离 0.5%

            # 先取消币安上的限价单
            cancel_result = self.live_engine.cancel_order(symbol, order_id)
            if not cancel_result.get('success'):
                logger.error(f"[实盘监控] 取消限价单失败: {cancel_result.get('error')}")
                return

            if deviation_pct > max_deviation_pct:
                # 价格偏离过大，取消订单不开仓
                logger.info(f"[实盘监控] ⏰ 限价单超时取消: {symbol} {position_side} "
                           f"已等待 {elapsed_minutes:.1f} 分钟, "
                           f"价格偏离 {deviation_pct:.2f}% > {max_deviation_pct}%, "
                           f"限价={limit_price}, 当前={current_price}")

                # 更新数据库状态为超时取消
                await self._update_position_canceled(position, 'TIMEOUT_PRICE_DEVIATION')

                # 发送TG通知
                self._send_timeout_cancel_notification(position, deviation_pct, elapsed_minutes)

            else:
                # 价格偏离在可接受范围内，以市价重新开仓
                logger.info(f"[实盘监控] ⏰ 限价单超时转市价: {symbol} {position_side} "
                           f"已等待 {elapsed_minutes:.1f} 分钟, "
                           f"价格偏离 {deviation_pct:.2f}% ≤ {max_deviation_pct}%")

                # 以市价重新开仓
                await self._execute_market_order_after_timeout(position, current_price)

        except Exception as e:
            logger.error(f"[实盘监控] 处理限价单超时失败: {e}")

    async def _execute_market_order_after_timeout(self, position: Dict, current_price: Decimal):
        """
        限价单超时后以市价执行开仓

        Args:
            position: 原限价单仓位信息
            current_price: 当前价格
        """
        try:
            symbol = position['symbol']
            position_side = position['position_side']
            quantity = Decimal(str(position['quantity']))
            leverage = position.get('leverage', 1)
            stop_loss_price = position.get('stop_loss_price')
            take_profit_price = position.get('take_profit_price')
            strategy_id = position.get('strategy_id')
            account_id = position.get('account_id', 1)  # 默认账户ID为1
            source = position.get('source', 'timeout_convert')

            logger.info(f"[实盘监控] 📈 执行市价开仓: {symbol} {position_side} "
                       f"数量={quantity}, 杠杆={leverage}x")

            # 调用实盘引擎以市价开仓
            result = self.live_engine.open_position(
                account_id=account_id,
                symbol=symbol,
                position_side=position_side,  # 直接使用 'LONG' 或 'SHORT'
                quantity=quantity,
                leverage=leverage,
                limit_price=None,  # 市价单不需要限价
                stop_loss_price=Decimal(str(stop_loss_price)) if stop_loss_price else None,
                take_profit_price=Decimal(str(take_profit_price)) if take_profit_price else None,
                source=f"{source}_timeout_market",
                strategy_id=strategy_id
            )

            if result.get('success'):
                actual_price = result.get('entry_price', float(current_price))
                logger.info(f"[实盘监控] ✅ 市价开仓成功: {symbol} @ {actual_price}")

                # 删除原来的 PENDING 仓位记录（因为 open_position 会创建新记录）
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute("""
                    DELETE FROM live_futures_positions
                    WHERE id = %s AND status = 'PENDING'
                """, (position['id'],))
                logger.debug(f"[实盘监控] 已删除原 PENDING 仓位记录 #{position['id']}")

            else:
                logger.error(f"[实盘监控] ❌ 市价开仓失败: {result.get('error')}")
                # 更新原仓位状态为失败
                await self._update_position_canceled(position, 'TIMEOUT_MARKET_FAILED')

        except Exception as e:
            logger.error(f"[实盘监控] 市价开仓异常: {e}")
            await self._update_position_canceled(position, 'TIMEOUT_MARKET_ERROR')

    async def _place_sl_tp_orders(self, position: Dict, executed_qty: Decimal):
        """设置止损止盈订单"""
        symbol = position['symbol']
        position_side = position['position_side']
        position_id = position.get('id')
        stop_loss_price = position.get('stop_loss_price')
        take_profit_price = position.get('take_profit_price')

        # 检查是否已经设置过止损止盈（市价单在 open_position 时已设置）
        existing_sl_order_id = position.get('sl_order_id')
        existing_tp_order_id = position.get('tp_order_id')

        if existing_sl_order_id and existing_tp_order_id:
            logger.info(f"[实盘监控] {symbol} 止损止盈已设置 (SL={existing_sl_order_id}, TP={existing_tp_order_id})，跳过重复设置")
            return

        # 如果部分已设置，只设置缺失的
        if existing_sl_order_id:
            stop_loss_price = None  # 跳过止损设置
            logger.debug(f"[实盘监控] {symbol} 止损已存在，跳过止损设置")
        if existing_tp_order_id:
            take_profit_price = None  # 跳过止盈设置
            logger.debug(f"[实盘监控] {symbol} 止盈已存在，跳过止盈设置")

        if not stop_loss_price and not take_profit_price:
            return

        # 获取当前价格用于验证
        try:
            current_price = self.live_engine.get_current_price(symbol)
            if current_price == 0:
                logger.warning(f"[实盘监控] 无法获取 {symbol} 当前价格，跳过止损止盈设置")
                return
        except Exception as e:
            logger.error(f"[实盘监控] 获取价格失败: {e}")
            return

        # 设置止损
        if stop_loss_price:
            stop_loss_price = Decimal(str(stop_loss_price))
            # 验证止损价格是否合理
            # 做多：止损价必须低于当前价
            # 做空：止损价必须高于当前价
            is_valid = False
            if position_side == 'LONG' and stop_loss_price < current_price:
                is_valid = True
            elif position_side == 'SHORT' and stop_loss_price > current_price:
                is_valid = True

            if is_valid:
                try:
                    sl_result = self.live_engine._place_stop_loss(
                        symbol=symbol,
                        position_side=position_side,
                        quantity=executed_qty,
                        stop_price=stop_loss_price
                    )
                    if sl_result.get('success'):
                        sl_order_id = sl_result.get('order_id')
                        logger.info(f"[实盘监控] ✓ 止损单已设置: {symbol} @ {stop_loss_price}, 订单ID={sl_order_id}")

                        # 保存止损订单ID到数据库
                        try:
                            conn = self._get_connection()
                            cursor = conn.cursor()
                            cursor.execute("""
                                UPDATE live_futures_positions
                                SET sl_order_id = %s
                                WHERE id = %s
                            """, (sl_order_id, position['id']))
                            conn.commit()  # 🔧 修复：添加 commit
                            cursor.close()
                            logger.info(f"[实盘监控] ✓ 止损订单ID已保存: {sl_order_id}")
                        except Exception as db_err:
                            logger.error(f"[实盘监控] 保存止损订单ID失败: {db_err}")

                        # 发送Telegram通知
                        try:
                            notifier = get_trade_notifier() if get_trade_notifier else None
                            if notifier:
                                notifier.notify_stop_loss_set(
                                    symbol=symbol,
                                    direction=position_side,
                                    stop_price=float(stop_loss_price),
                                    quantity=float(executed_qty)
                                )
                        except Exception as notify_err:
                            logger.warning(f"[实盘监控] 发送止损通知失败: {notify_err}")
                    else:
                        logger.error(f"[实盘监控] ✗ 止损单设置失败: {sl_result.get('error')}")
                except Exception as e:
                    logger.error(f"[实盘监控] 设置止损单异常: {e}")
            else:
                logger.warning(f"[实盘监控] 止损价 {stop_loss_price} 无效 ({position_side} 当前价 {current_price})，跳过止损设置")

        # 设置止盈
        if take_profit_price:
            take_profit_price = Decimal(str(take_profit_price))
            # 验证止盈价格是否合理
            # 做多：止盈价必须高于当前价
            # 做空：止盈价必须低于当前价
            is_valid = False
            if position_side == 'LONG' and take_profit_price > current_price:
                is_valid = True
            elif position_side == 'SHORT' and take_profit_price < current_price:
                is_valid = True

            if is_valid:
                try:
                    tp_result = self.live_engine._place_take_profit(
                        symbol=symbol,
                        position_side=position_side,
                        quantity=executed_qty,
                        take_profit_price=take_profit_price
                    )
                    if tp_result.get('success'):
                        tp_order_id = tp_result.get('order_id')
                        logger.info(f"[实盘监控] ✓ 止盈单已设置: {symbol} @ {take_profit_price}, 订单ID={tp_order_id}")

                        # 保存止盈订单ID到数据库
                        try:
                            conn = self._get_connection()
                            cursor = conn.cursor()
                            cursor.execute("""
                                UPDATE live_futures_positions
                                SET tp_order_id = %s
                                WHERE id = %s
                            """, (tp_order_id, position['id']))
                            conn.commit()  # 🔧 修复：添加 commit
                            cursor.close()
                            logger.info(f"[实盘监控] ✓ 止盈订单ID已保存: {tp_order_id}")
                        except Exception as db_err:
                            logger.error(f"[实盘监控] 保存止盈订单ID失败: {db_err}")

                        # 发送Telegram通知
                        try:
                            notifier = get_trade_notifier() if get_trade_notifier else None
                            if notifier:
                                notifier.notify_take_profit_set(
                                    symbol=symbol,
                                    direction=position_side,
                                    take_profit_price=float(take_profit_price),
                                    quantity=float(executed_qty)
                                )
                        except Exception as notify_err:
                            logger.warning(f"[实盘监控] 发送止盈通知失败: {notify_err}")
                    else:
                        logger.error(f"[实盘监控] ✗ 止盈单设置失败: {tp_result.get('error')}")
                except Exception as e:
                    logger.error(f"[实盘监控] 设置止盈单异常: {e}")
            else:
                logger.warning(f"[实盘监控] 止盈价 {take_profit_price} 无效 ({position_side} 当前价 {current_price})，跳过止盈设置")

    def _send_order_cancel_notification(self, position: Dict, reason: str):
        """发送订单取消的Telegram通知"""
        try:
            from app.services.trade_notifier import get_trade_notifier
            notifier = get_trade_notifier()
            if not notifier:
                return

            symbol = position['symbol']
            position_side = position['position_side']
            direction_text = "做多" if position_side == 'LONG' else "做空"
            entry_price = position.get('entry_price', 0)
            quantity = position.get('quantity', 0)

            message = f"""
🚫 <b>【订单取消】{symbol}</b>

📌 方向: {direction_text}
💰 价格: {entry_price}
📊 数量: {quantity}
💡 原因: {reason}

⏰ {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}
"""

            notifier._send_telegram(message)
            logger.info(f"[实盘监控] ✅ 订单取消通知已发送: {symbol}")

        except Exception as e:
            logger.warning(f"[实盘监控] 发送订单取消通知失败: {e}")

    def _send_timeout_cancel_notification(self, position: Dict, deviation_pct: Decimal, elapsed_minutes: float):
        """发送限价单超时取消的Telegram通知"""
        try:
            from app.services.trade_notifier import get_trade_notifier
            notifier = get_trade_notifier()
            if not notifier:
                return

            symbol = position['symbol']
            position_side = position['position_side']
            direction_text = "做多" if position_side == 'LONG' else "做空"

            message = f"""
⚠️ <b>【限价单超时取消】{symbol}</b>

📌 方向: {direction_text}
⏱️ 等待时长: {elapsed_minutes:.1f} 分钟
📊 价格偏离: {deviation_pct:.2f}% (> 0.5%)
💡 原因: 价格偏离过大，避免追高/杀低

⏰ {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}
"""

            notifier._send_telegram(message)
            logger.info(f"[实盘监控] ✅ 超时取消通知已发送: {symbol}")

        except Exception as e:
            logger.warning(f"[实盘监控] 发送超时取消通知失败: {e}")

    # ==================== 冗余代码已移除 ====================
    # 实盘不负责策略判断，智能止盈/止损由模拟盘负责
    # 模拟盘通过 strategy_executor.py 执行智能出场策略后，
    # 会自动同步到实盘（通过 futures_trading_engine.close_position）
    # 因此实盘无需重复实现这些策略逻辑
    # =======================================================


# 全局监控实例
_live_order_monitor: Optional[LiveOrderMonitor] = None


def get_live_order_monitor() -> Optional[LiveOrderMonitor]:
    """获取全局监控实例"""
    return _live_order_monitor


def init_live_order_monitor(db_config: Dict, live_engine) -> LiveOrderMonitor:
    """初始化全局监控实例"""
    global _live_order_monitor
    _live_order_monitor = LiveOrderMonitor(db_config, live_engine)
    return _live_order_monitor
