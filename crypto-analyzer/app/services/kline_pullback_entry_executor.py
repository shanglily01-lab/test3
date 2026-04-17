"""
K线回调建仓执行器 V2 (一次性开仓版本)
基于K线形态回调确认实现最优入场时机

核心策略：
- 做多：等待1根反向阴线作为回调确认
- 做空：等待1根反向阳线作为反弹确认
- 单级确认：15M（0-30分钟），超时放弃
- 纪律严明：宁愿错过，不追涨杀跌
- 确认后立即一次性开仓100%，不分批
"""
import asyncio
import json
import pymysql
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple
from decimal import Decimal
from loguru import logger

# 实盘同步锁：与 smart_entry_executor 共享同一模块级字典，防止并发竞态
from app.services.smart_entry_executor import _live_sync_locks

from app.services.optimization_config import OptimizationConfig


class KlinePullbackEntryExecutor:
    """K线回调建仓执行器（一次性开仓）"""

    def __init__(self, db_config: dict, live_engine, price_service, account_id=None, brain=None, opt_config=None, max_hold_minutes: int = 180):
        """
        初始化执行器

        Args:
            db_config: 数据库配置
            live_engine: 交易引擎
            price_service: 价格服务（WebSocket）
            account_id: 账户ID
            brain: 智能大脑（用于获取自适应参数）
            opt_config: 优化配置（用于获取波动率配置）
            max_hold_minutes: 最大持仓时间（分钟），由 config.yaml signals.max_hold_hours 传入
        """
        self.db_config = db_config
        self.live_engine = live_engine
        self.price_service = price_service
        if account_id is not None:
            self.account_id = account_id
        else:
            self.account_id = getattr(live_engine, 'account_id', 2)

        # 获取brain和opt_config（用于止盈止损计算）
        self.brain = brain if brain else getattr(live_engine, 'brain', None)
        self.opt_config = opt_config if opt_config else getattr(live_engine, 'opt_config', None)

        # 如果仍没有opt_config，创建新实例
        if not self.opt_config:
            self.opt_config = OptimizationConfig(db_config)

        # 最大持仓时间（由外部配置传入，范围 180~480 分钟）
        self.max_hold_minutes = max(180, min(480, max_hold_minutes))

        # 时间窗口配置
        self.total_window_minutes = 30  # 总时间窗口30分钟
        self.primary_window_minutes = 30  # 第一阶段30分钟（15M）
        self.check_interval_seconds = 60  # 每60秒检查一次（K线更新频率）

    def _get_margin_amount(self, symbol: str) -> float:
        """
        根据交易对评级等级获取保证金金额

        Args:
            symbol: 交易对符号

        Returns:
            保证金金额(USDT)，如果是黑名单3级则返回0
        """
        rating_level = self.opt_config.get_symbol_rating_level(symbol)

        # 根据评级等级设置保证金
        if rating_level == 0:
            # 白名单/默认：400U
            return 400.0
        elif rating_level == 1:
            # 黑名单1级：100U
            return 100.0
        elif rating_level == 2:
            # 黑名单2级：50U
            return 50.0
        else:
            # 黑名单3级：不交易
            return 0.0

    def _calculate_stop_take_prices(self, symbol: str, direction: str, current_price: float, signal_components: dict) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
        """
        计算止盈止损价格和百分比

        Args:
            symbol: 交易对
            direction: 方向 LONG/SHORT
            current_price: 当前价格
            signal_components: 信号组成

        Returns:
            (止损价格, 止盈价格, 止损百分比, 止盈百分比)
        """
        SL_PCT, TP_PCT = self._get_sl_tp_from_settings()

        if direction == 'LONG':
            stop_loss_price = current_price * (1 - SL_PCT)
            take_profit_price = current_price * (1 + TP_PCT)
        else:  # SHORT
            stop_loss_price = current_price * (1 + SL_PCT)
            take_profit_price = current_price * (1 - TP_PCT)

        return stop_loss_price, take_profit_price, SL_PCT, TP_PCT

    def _get_sl_tp_from_settings(self):
        """从 system_settings 读取止损止盈比例，默认 2%/5%"""
        try:
            conn = pymysql.connect(**self.db_config, autocommit=True)
            cur = conn.cursor()
            cur.execute("SELECT setting_key, setting_value FROM system_settings WHERE setting_key IN ('stop_loss_pct','take_profit_pct')")
            rows = {r[0]: r[1] for r in cur.fetchall()}
            cur.close(); conn.close()
            sl = float(rows.get('stop_loss_pct', 0.02))
            tp = float(rows.get('take_profit_pct', 0.05))
            return sl, tp
        except Exception as e:
            logger.warning(f"[SL/TP] 读取system_settings失败，使用默认值: {e}")
            return 0.02, 0.05

    def _calculate_volatility_adjusted_stop_loss(self, signal_components: dict, base_stop_loss_pct: float) -> float:
        """波动率自适应止损"""
        if not signal_components:
            return base_stop_loss_pct

        # 如果包含破位信号，扩大止损
        if any(key.startswith('breakdown_') for key in signal_components.keys()):
            adjusted_pct = base_stop_loss_pct * 1.5
            logger.debug(f"[VOLATILITY_SL] 破位信号，止损扩大1.5倍: {adjusted_pct*100:.2f}%")
            return adjusted_pct

        return base_stop_loss_pct

    async def _get_current_price(self, symbol: str) -> Optional[float]:
        """获取当前价格（优先WebSocket，回退到数据库）"""
        # 优先从WebSocket获取
        if self.price_service:
            price = self.price_service.get_price(symbol)
            if price and price > 0:
                return float(price)

        # 回退到数据库
        conn = None
        try:
            conn = pymysql.connect(**self.db_config)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT close_price FROM kline_data
                WHERE symbol = %s AND timeframe = '5m'
                ORDER BY open_time DESC
                LIMIT 1
            """, (symbol,))
            result = cursor.fetchone()
            if result:
                return float(result[0])
        except Exception as e:
            logger.error(f"❌ 从数据库获取价格失败: {e}")
        finally:
            if conn:
                conn.close()

        return None

    async def execute_entry(self, signal: Dict) -> Dict:
        """
        执行K线回调建仓

        流程：
        1. 阶段1（0-30分钟）：监控15M K线，等待1根反向K线
        2. 检测到回调确认后，立即一次性开仓100%
        3. 30分钟截止，如果未触发则放弃

        Args:
            signal: 开仓信号 {
                'symbol': str,
                'direction': 'LONG'/'SHORT',
                'leverage': int,
                'signal_time': datetime,
                'trade_params': {...}
            }

        Returns:
            建仓结果 {'success': bool, 'position_id': int, 'price': float}
        """
        symbol = signal['symbol']
        direction = signal['direction']

        # 使用真实的信号触发时间
        signal_time = signal.get('signal_time', datetime.now())
        if isinstance(signal_time, str):
            signal_time = datetime.fromisoformat(signal_time)

        # 获取保证金金额
        margin = self._get_margin_amount(symbol)

        if margin == 0:
            rating_level = self.opt_config.get_symbol_rating_level(symbol)
            logger.warning(f"❌ {symbol} 为黑名单{rating_level}级，禁止交易")
            return {'success': False, 'reason': f'黑名单{rating_level}级禁止交易'}

        logger.info(f"🚀 {symbol} 开始K线回调建仓 V2（一次性开仓） | 方向: {direction}")
        logger.info(f"   信号时间: {signal_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"   策略: 等待1根反向K线确认 | 15M(0-30min) → 5M(30-60min)")
        logger.info(f"💰 保证金: {margin}U (评级等级: {self.opt_config.get_symbol_rating_level(symbol)})")

        # 确保symbol已订阅到WebSocket价格服务
        if self.price_service and hasattr(self.price_service, 'subscribe'):
            try:
                await self.price_service.subscribe([symbol])
                logger.debug(f"✅ {symbol} 已订阅到WebSocket价格服务")
            except Exception as e:
                logger.warning(f"⚠️ {symbol} WebSocket订阅失败: {e}，将使用数据库价格")

        try:
            # 检查信号是否已过期
            elapsed_seconds = (datetime.now() - signal_time).total_seconds()
            if elapsed_seconds >= self.total_window_minutes * 60:
                logger.warning(f"⚠️ {symbol} 信号已过期 | 已过: {elapsed_seconds/60:.1f}分钟")
                return {'success': False, 'error': f'信号已过期({elapsed_seconds/60:.0f}分钟)'}

            # 主循环：等待回调确认
            logger.info(f"🔄 {symbol} 进入监控循环，窗口时长: {self.total_window_minutes}分钟")
            phase = 'primary'
            fallback_logged = False

            while (datetime.now() - signal_time).total_seconds() < self.total_window_minutes * 60:
                elapsed_minutes = (datetime.now() - signal_time).total_seconds() / 60

                # 判断当前阶段
                if elapsed_minutes < self.primary_window_minutes:
                    timeframe = '15m'
                    phase = 'primary'
                else:
                    timeframe = '5m'
                    phase = 'fallback'
                    if not fallback_logged:
                        logger.info(f"⏰ {symbol} 30分钟后切换到5M精准监控")
                        fallback_logged = True

                # 检测回调确认
                pullback_confirmed, reason = await self._check_pullback_confirmation(
                    symbol, direction, timeframe, signal_time, phase
                )

                if pullback_confirmed:
                    # 检测到回调确认，立即开仓
                    logger.info(f"✅ {symbol} 回调确认触发: {reason}")
                    return await self._execute_single_entry(
                        symbol, direction, margin, signal, signal_time
                    )

                # 等待下一次检查
                await asyncio.sleep(self.check_interval_seconds)

            # 超时未触发
            logger.warning(f"⏱️ {symbol} 30分钟窗口结束，未检测到回调确认，放弃建仓")
            return {'success': False, 'error': '超时未触发回调确认'}

        except Exception as e:
            logger.error(f"❌ {symbol} 回调建仓执行出错: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {'success': False, 'error': str(e)}

    async def _check_pullback_confirmation(
        self, symbol: str, direction: str, timeframe: str,
        signal_time: datetime, phase: str
    ) -> Tuple[bool, str]:
        """
        检查是否出现回调确认（1根反向K线）

        Args:
            symbol: 交易对
            direction: 方向 LONG/SHORT
            timeframe: 时间框架 15m/5m
            signal_time: 信号时间
            phase: 当前阶段 primary/fallback

        Returns:
            (是否确认, 原因描述)
        """
        conn = None
        try:
            conn = pymysql.connect(**self.db_config, cursorclass=pymysql.cursors.DictCursor)
            cursor = conn.cursor()

            # 根据阶段确定检测基准时间
            if phase == 'primary':
                base_time = signal_time
            else:
                base_time = signal_time + timedelta(minutes=self.primary_window_minutes)

            # 获取基准时间后的最近2根K线
            # open_time是毫秒时间戳，需要转换
            base_timestamp = int(base_time.timestamp() * 1000)
            cursor.execute("""
                SELECT open_time, open_price, close_price
                FROM kline_data
                WHERE symbol = %s AND timeframe = %s
                AND open_time >= %s
                ORDER BY open_time DESC
                LIMIT 2
            """, (symbol, timeframe, base_timestamp))

            klines = cursor.fetchall()

            if len(klines) < 1:
                logger.debug(f"⚠️ {symbol} {timeframe.upper()} 数据不足，base_time={base_time}")
                return False, "数据不足"

            latest_kline = klines[0]
            is_green = latest_kline['close_price'] > latest_kline['open_price']  # 阳线
            is_red = latest_kline['close_price'] < latest_kline['open_price']    # 阴线

            # 调试日志：显示最新K线信息
            kline_type = "🟢阳线" if is_green else "🔴阴线" if is_red else "⚪️十字星"
            logger.debug(
                f"📊 {symbol} {timeframe.upper()} 最新K线: {kline_type} | "
                f"开:{latest_kline['open_price']:.6f} 收:{latest_kline['close_price']:.6f} | "
                f"时间:{latest_kline['open_time']}"
            )

            # 做多：等待阴线回调
            if direction == 'LONG' and is_red:
                return True, f"{timeframe.upper()}阴线回调确认"

            # 做空：等待阳线反弹
            if direction == 'SHORT' and is_green:
                return True, f"{timeframe.upper()}阳线反弹确认"

            return False, "等待反向K线"

        except Exception as e:
            logger.error(f"❌ 检查回调确认失败: {e}")
            return False, f"检查失败: {e}"
        finally:
            if conn:
                conn.close()

    async def _execute_single_entry(
        self, symbol: str, direction: str, margin: float,
        signal: Dict, signal_time: datetime
    ) -> Dict:
        """
        执行一次性开仓

        Args:
            symbol: 交易对
            direction: 方向
            margin: 保证金金额
            signal: 原始信号
            signal_time: 信号时间

        Returns:
            开仓结果
        """
        try:
            # 开仓前最后检查交易开关 + 方向开关（防止主循环已禁止但执行器任务仍在运行）
            try:
                conn_chk = pymysql.connect(**self.db_config, autocommit=True)
                cur_chk = conn_chk.cursor()
                direction_key = 'allow_long' if direction == 'LONG' else 'allow_short'
                cur_chk.execute(
                    "SELECT setting_key, setting_value FROM system_settings "
                    "WHERE setting_key IN ('u_futures_trading_enabled', %s)",
                    (direction_key,)
                )
                rows = {r[0]: r[1] for r in cur_chk.fetchall()}
                conn_chk.close()
                if rows.get('u_futures_trading_enabled', '1') not in ('1', 'true', 'True', 'yes'):
                    logger.warning(f"[TRADING-DISABLED] {symbol} 回调确认触发但交易已禁止，放弃开仓")
                    return {'success': False, 'reason': '交易开关已关闭'}
                direction_name = '做多' if direction == 'LONG' else '做空'
                if rows.get(direction_key, '1') not in ('1', 'true', 'True', 'yes'):
                    logger.warning(f"[DIRECTION-DISABLED] {symbol} {direction} 回调确认触发但系统已禁止{direction_name}，放弃开仓")
                    return {'success': False, 'reason': f'系统已禁止{direction_name}'}
            except Exception as chk_err:
                logger.warning(f"[TRADING-DISABLED] 检查交易开关失败: {chk_err}，默认禁止开单")
                return {'success': False, 'reason': f'检查交易开关异常: {chk_err}'}

            # 获取当前价格
            current_price = await self._get_current_price(symbol)
            if not current_price:
                logger.error(f"❌ {symbol} 无法获取当前价格")
                return {'success': False, 'error': '无法获取价格'}

            # 计算止盈止损
            signal_components = signal.get('trade_params', {}).get('signal_components', {})
            stop_loss_price, take_profit_price, stop_loss_pct, take_profit_pct = \
                self._calculate_stop_take_prices(symbol, direction, current_price, signal_components)

            # 计算仓位
            leverage = signal.get('leverage', 5)
            quantity = margin * leverage / current_price
            notional_value = quantity * current_price

            # 生成信号组合键
            if signal_components:
                sorted_signals = sorted(signal_components.keys())
                signal_combination_key = "TREND_" + " + ".join(sorted_signals)
            else:
                signal_combination_key = "TREND_unknown"

            # 计算超时和计划平仓时间（实时从DB读取 max_hold_hours，无需重启）
            _mh_val = self.opt_config._read_system_setting('max_hold_hours') if self.opt_config else None
            _mh_hours = max(3, min(8, int(_mh_val or self.max_hold_minutes // 60)))
            max_hold_minutes = _mh_hours * 60
            timeout_at = datetime.now() + timedelta(minutes=max_hold_minutes)
            planned_close_time = datetime.now() + timedelta(minutes=max_hold_minutes)

            # 准备数据
            entry_score = signal.get('trade_params', {}).get('entry_score', 0)
            entry_reason = f"V2回调确认 | 评分:{entry_score}"

            # 🔥 防重复开仓：插入前再次检查是否已有持仓
            conn = pymysql.connect(**self.db_config)
            try:
                cursor = conn.cursor()

                cursor.execute("""
                    SELECT COUNT(*) FROM futures_positions
                    WHERE symbol = %s AND position_side = %s
                    AND status = 'open' AND account_id = %s
                """, (symbol, direction, self.account_id))

                existing_count = cursor.fetchone()[0]
                if existing_count > 0:
                    logger.warning(f"⚠️ {symbol} {direction} 已有{existing_count}个持仓，放弃本次开仓（防重复）")
                    return {'success': False, 'reason': '已有持仓，防止重复开仓'}

                # 插入持仓记录
                cursor.execute("""
                    INSERT INTO futures_positions
                    (account_id, symbol, position_side, quantity, entry_price,
                     leverage, notional_value, margin, open_time, stop_loss_price, take_profit_price,
                     entry_signal_type, entry_reason, entry_score, signal_components, max_hold_minutes, timeout_at,
                     planned_close_time, source, status, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            'smart_trader', 'open', NOW(), NOW())
                """, (
                    self.account_id, symbol, direction, quantity, current_price, leverage,
                    notional_value, margin, stop_loss_price, take_profit_price,
                    signal_combination_key, entry_reason, entry_score,
                    json.dumps(signal_components) if signal_components else None,
                    max_hold_minutes, timeout_at, planned_close_time
                ))

                position_id = cursor.lastrowid

                # 冻结资金
                cursor.execute("""
                    UPDATE futures_trading_accounts
                    SET current_balance = current_balance - %s,
                        frozen_balance = frozen_balance + %s,
                        updated_at = NOW()
                    WHERE id = %s
                """, (margin, margin, self.account_id))

                conn.commit()
            finally:
                conn.close()

            logger.info(f"✅ {symbol} 一次性开仓完成 | 持仓ID:{position_id} | 价格:${current_price:.4f} | 保证金:{margin}U")

            # ========== 同步实盘开仓 ==========
            try:
                _c = pymysql.connect(**self.db_config, autocommit=True)
                _cur = _c.cursor()
                _cur.execute("SELECT setting_value FROM system_settings WHERE setting_key='live_trading_enabled'")
                _r = _cur.fetchone()
                live_trading_enabled = _r and str(_r[0]).lower() in ('1', 'true', 'yes')
                # 实盘同步必须是 TOP 30 交易对
                _cur.execute("SELECT COUNT(*) FROM top_performing_symbols WHERE symbol=%s", (symbol,))
                in_top30 = (_cur.fetchone() or [0])[0] > 0
                _cur.close(); _c.close()
            except Exception:
                live_trading_enabled = False
                in_top30 = False

            if not live_trading_enabled:
                pass  # 同步开关关闭
            elif not in_top30:
                logger.info(f"[同步实盘] {symbol} 不在TOP30列表，跳过实盘同步")
            else:
                try:
                    from app.services.api_key_service import APIKeyService
                    from app.trading.binance_futures_engine import BinanceFuturesEngine
                    svc = APIKeyService(self.db_config)
                    active_keys = svc.get_all_active_api_keys('binance')
                    for ak in active_keys:
                        try:
                            _engine = BinanceFuturesEngine(
                                self.db_config,
                                api_key=ak['api_key'],
                                api_secret=ak['api_secret']
                            )
                            _bal = _engine.get_account_balance()
                            if not _bal or not _bal.get('success'):
                                logger.warning(f"[同步实盘] 账号{ak['account_name']} 获取余额失败，跳过")
                                continue
                            _available = float(_bal.get('available', 0))
                            _max_margin = float(ak['max_position_value'])
                            _lev = int(ak['max_leverage'])
                            _margin = min(_max_margin, _available * 0.9)
                            if _margin < 5:
                                logger.warning(f"[同步实盘] 账号{ak['account_name']} 可用余额不足(margin={_margin:.2f}U)，跳过")
                                continue
                            # 实盘持仓数量上限（每账号最多5单，加锁防并发竞态）
                            _ak_id = ak['id']
                            if _ak_id not in _live_sync_locks:
                                _live_sync_locks[_ak_id] = asyncio.Lock()
                            async with _live_sync_locks[_ak_id]:
                                _cnt_c = pymysql.connect(**self.db_config, autocommit=True)
                                _cnt_cur = _cnt_c.cursor()
                                _cnt_cur.execute("SELECT COUNT(*) FROM live_futures_positions WHERE account_id=%s AND status='OPEN'", (_ak_id,))
                                _live_count = (_cnt_cur.fetchone() or [0])[0]
                                _cnt_cur.close(); _cnt_c.close()
                                if _live_count >= 5:
                                    logger.info(f"[同步实盘] 账号{ak['account_name']} 已有{_live_count}个实盘持仓，达上限(5)，跳过")
                                    continue
                                _qty = Decimal(str(_margin * _lev / current_price))
                            _result = _engine.open_position(
                                account_id=ak['id'],
                                symbol=symbol,
                                position_side=direction,
                                quantity=_qty,
                                leverage=_lev,
                                stop_loss_price=Decimal(str(stop_loss_price)) if stop_loss_price else None,
                                take_profit_price=Decimal(str(take_profit_price)) if take_profit_price else None,
                                source='smart_trader_sync',
                                paper_position_id=position_id
                            )
                            if _result.get('success'):
                                logger.info(f"[同步实盘] ✅ {symbol} {direction} 账号[{ak['account_name']}] 同步开仓成功 保证金={_margin:.2f}U 杠杆={_lev}x")
                                try:
                                    _notifier = getattr(self.live_engine, 'telegram_notifier', None)
                                    if _notifier:
                                        _notifier.notify_open_position(
                                            symbol=symbol, direction=direction,
                                            quantity=float(_qty), entry_price=current_price,
                                            leverage=_lev, margin=_margin,
                                            stop_loss_price=float(stop_loss_price) if stop_loss_price else None,
                                            take_profit_price=float(take_profit_price) if take_profit_price else None,
                                            strategy_name=f'实盘同步[{ak["account_name"]}]'
                                        )
                                except Exception: pass
                            else:
                                logger.error(f"[同步实盘] ❌ {symbol} {direction} 账号[{ak['account_name']}] 失败: {_result.get('error', _result.get('message', ''))}")
                        except Exception as _ex:
                            logger.error(f"[同步实盘] ❌ 账号[{ak.get('account_name','')}] 异常: {_ex}")
                except Exception as sync_ex:
                    logger.error(f"[同步实盘] 整体异常: {sync_ex}")
            # ========== 同步实盘开仓结束 ==========

            # 启动智能平仓监控
            if self.live_engine.smart_exit_optimizer:
                try:
                    asyncio.create_task(
                        self.live_engine.smart_exit_optimizer.start_monitoring_position(position_id)
                    )
                    logger.info(f"✅ 持仓{position_id}已加入智能平仓监控")
                except Exception as e:
                    logger.error(f"❌ 持仓{position_id}启动监控失败: {e}")

            return {
                'success': True,
                'position_id': position_id,
                'price': current_price,
                'margin': margin,
                'quantity': quantity
            }

        except Exception as e:
            logger.error(f"❌ {symbol} 一次性开仓失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {'success': False, 'error': str(e)}
