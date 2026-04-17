"""
模拟交易 API 接口
提供账户管理、下单、持仓查询等功能
"""

from fastapi import APIRouter, HTTPException, Depends, Body
from pydantic import BaseModel
from typing import Optional, List
from decimal import Decimal
from datetime import datetime
import yaml
from functools import lru_cache

from app.trading.paper_trading_engine import PaperTradingEngine
from app.services.price_cache_service import get_global_price_cache
from loguru import logger

router = APIRouter(prefix="/api/paper-trading", tags=["模拟交易"])

# WebSocket价格服务（全局单例，批量订阅实时价格）
_ws_price_service = None
_ws_initialized = False

def get_ws_price_service():
    """获取WebSocket价格服务（延迟初始化）"""
    global _ws_price_service, _ws_initialized
    if _ws_price_service is None:
        try:
            from app.services.binance_ws_price import BinanceWSPriceService
            _ws_price_service = BinanceWSPriceService(market_type='spot')
            logger.info("✅ WebSocket价格服务已创建")

            # 启动WebSocket并订阅交易对（异步任务）
            if not _ws_initialized:
                import asyncio
                import threading

                def start_ws_service():
                    """在后台线程中运行WebSocket服务"""
                    try:
                        config = get_config()
                        symbols = config.get('symbols', [])

                        # 创建新的事件循环
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)

                        # 订阅交易对并启动服务
                        loop.run_until_complete(_ws_price_service.subscribe(symbols))
                        loop.run_until_complete(_ws_price_service.start())

                        logger.info(f"✅ WebSocket已订阅 {len(symbols)} 个现货交易对")
                    except Exception as e:
                        logger.error(f"❌ WebSocket服务启动失败: {e}")

                # 在后台线程中启动
                ws_thread = threading.Thread(target=start_ws_service, daemon=True)
                ws_thread.start()
                _ws_initialized = True

        except Exception as e:
            logger.warning(f"⚠️ WebSocket价格服务初始化失败: {e}")
    return _ws_price_service

# ==================== 依赖注入：延迟初始化（修复阻塞问题）====================

@lru_cache()
def get_config():
    """缓存配置文件读取（支持环境变量）"""
    from app.utils.config_loader import load_config
    return load_config()

def get_db_config():
    """获取数据库配置"""
    config = get_config()
    return config.get('database', {}).get('mysql', {})

def get_engine():
    """获取 PaperTradingEngine 实例（集成缓存+WebSocket批量价格）"""
    db_config = get_db_config()
    price_cache = get_global_price_cache()
    ws_service = get_ws_price_service()  # WebSocket批量实时价格
    return PaperTradingEngine(db_config, price_cache_service=price_cache, ws_price_service=ws_service)


# ==================== 请求模型 ====================

class PlaceOrderRequest(BaseModel):
    """下单请求"""
    account_id: Optional[int] = None  # None表示使用默认账户
    symbol: str  # 交易对，如 BTC/USDT
    side: str  # BUY 或 SELL
    quantity: float  # 数量
    order_type: str = "MARKET"  # MARKET 或 LIMIT
    price: Optional[float] = None  # 限价单价格
    order_source: str = "manual"  # manual, signal, auto
    pending_order_id: Optional[str] = None  # 待成交订单ID（如果是从待成交订单触发的）


class CreateAccountRequest(BaseModel):
    """创建账户请求"""
    account_name: str
    initial_balance: float = 10000.0


class UpdateStopLossTakeProfitRequest(BaseModel):
    """更新止盈止损请求"""
    account_id: Optional[int] = None  # None表示使用默认账户
    symbol: str  # 交易对
    stop_loss_price: Optional[float] = None  # 止损价格（None表示清除）
    take_profit_price: Optional[float] = None  # 止盈价格（None表示清除）


class BatchPricesRequest(BaseModel):
    """批量获取价格请求"""
    symbols: List[str]  # 交易对列表
    force_refresh: bool = False  # 是否强制刷新


# ==================== API 接口 ====================

@router.get("/account")
async def get_account(account_id: Optional[int] = None, engine: PaperTradingEngine = Depends(get_engine)):

    """
    获取账户信息

    Args:
        account_id: 账户ID，不传则获取默认账户

    Returns:
        账户详细信息
    """
    try:
        summary = engine.get_account_summary(account_id or 1)
        if not summary:
            raise HTTPException(status_code=404, detail="账户不存在")

        account = summary['account']

        # 转换 Decimal 为 float
        return {
            "account": {
                "id": account['id'],
                "account_name": account['account_name'],
                "current_balance": float(account['current_balance']),
                "total_equity": float(account['total_equity']),
                "initial_balance": float(account['initial_balance']),
                "realized_pnl": float(account['realized_pnl']),
                "unrealized_pnl": float(account['unrealized_pnl']),
                "total_profit_loss": float(account['total_profit_loss']),
                "total_profit_loss_pct": float(account['total_profit_loss_pct']),
                "total_trades": account['total_trades'],
                "winning_trades": account['winning_trades'],
                "losing_trades": account['losing_trades'],
                "win_rate": float(account['win_rate']),
                "status": account['status']
            },
            "positions_count": len(summary['positions']),
            "recent_trades_count": len(summary['recent_trades'])
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取账户信息失败: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/account/summary")
async def get_account_summary(account_id: Optional[int] = None, engine: PaperTradingEngine = Depends(get_engine)):
    """
    获取账户完整摘要（包括持仓、订单、交易历史）

    Returns:
        完整账户摘要
    """
    try:
        summary = engine.get_account_summary(account_id or 1)
        if not summary:
            raise HTTPException(status_code=404, detail="账户不存在")

        # 转换数据
        account = summary['account']
        positions = []
        for pos in summary['positions']:
            positions.append({
                "symbol": pos['symbol'],
                "quantity": float(pos['quantity']),
                "available_quantity": float(pos['available_quantity']),
                "avg_entry_price": float(pos['avg_entry_price']),
                "current_price": float(pos['current_price']) if pos['current_price'] else 0,
                "market_value": float(pos['market_value']) if pos['market_value'] else 0,
                "unrealized_pnl": float(pos['unrealized_pnl']) if pos['unrealized_pnl'] else 0,
                "unrealized_pnl_pct": float(pos['unrealized_pnl_pct']) if pos['unrealized_pnl_pct'] else 0,
                "first_buy_time": pos['first_buy_time'].strftime('%Y-%m-%d %H:%M:%S') if pos['first_buy_time'] else None
            })

        recent_trades = []
        for trade in summary['recent_trades']:
            recent_trades.append({
                "trade_id": trade['trade_id'],
                "symbol": trade['symbol'],
                "side": trade['side'],
                "price": float(trade['price']),
                "quantity": float(trade['quantity']),
                "total_amount": float(trade['total_amount']),
                "fee": float(trade['fee']),
                "realized_pnl": float(trade['realized_pnl']) if trade['realized_pnl'] else None,
                "pnl_pct": float(trade['pnl_pct']) if trade['pnl_pct'] else None,
                "trade_time": trade['trade_time'].strftime('%Y-%m-%d %H:%M:%S')
            })

        return {
            "account": {
                "id": account['id'],
                "account_name": account['account_name'],
                "current_balance": float(account['current_balance']),
                "total_equity": float(account['total_equity']),
                "initial_balance": float(account['initial_balance']),
                "realized_pnl": float(account['realized_pnl']),
                "unrealized_pnl": float(account['unrealized_pnl']),
                "total_profit_loss": float(account['total_profit_loss']),
                "total_profit_loss_pct": float(account['total_profit_loss_pct']),
                "total_trades": account['total_trades'],
                "winning_trades": account['winning_trades'],
                "losing_trades": account['losing_trades'],
                "win_rate": float(account['win_rate'])
            },
            "positions": positions,
            "recent_trades": recent_trades
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取账户信息失败: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/account")
async def create_account(request: CreateAccountRequest, engine: PaperTradingEngine = Depends(get_engine)):
    """
    创建新账户

    Returns:
        新账户ID
    """
    try:
        account_id = engine.create_account(
            request.account_name,
            Decimal(str(request.initial_balance))
        )
        return {
            "success": True,
            "account_id": account_id,
            "message": f"账户 {request.account_name} 创建成功"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取账户信息失败: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/order")
async def place_order(request: PlaceOrderRequest, engine: PaperTradingEngine = Depends(get_engine)):
    """
    下单（买入/卖出）

    Returns:
        订单执行结果
    """
    try:
        account_id = request.account_id or 1

        success, message, order_id = engine.place_order(
            account_id=account_id,
            symbol=request.symbol,
            side=request.side.upper(),
            quantity=Decimal(str(request.quantity)),
            order_type=request.order_type.upper(),
            price=Decimal(str(request.price)) if request.price else None,
            order_source=request.order_source,
            pending_order_id=request.pending_order_id
        )

        if success:
            return {
                "success": True,
                "order_id": order_id,
                "message": message
            }
        else:
            raise HTTPException(status_code=400, detail=message)

    except HTTPException:
        raise
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取账户信息失败: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/positions")
async def get_positions(account_id: Optional[int] = None, engine: PaperTradingEngine = Depends(get_engine)):
    """
    获取持仓列表

    Returns:
        持仓列表
    """
    try:
        summary = engine.get_account_summary(account_id or 1)
        if not summary:
            raise HTTPException(status_code=404, detail="账户不存在")

        positions = []
        for pos in summary.get('positions', []):
            positions.append({
                "symbol": pos['symbol'],
                "quantity": float(pos['quantity']),
                "available_quantity": float(pos['available_quantity']),
                "avg_entry_price": float(pos['avg_entry_price']),
                "current_price": float(pos['current_price']) if pos['current_price'] else 0,
                "market_value": float(pos['market_value']) if pos['market_value'] else 0,
                "total_cost": float(pos['total_cost']),
                "unrealized_pnl": float(pos['unrealized_pnl']) if pos['unrealized_pnl'] else 0,
                "unrealized_pnl_pct": float(pos['unrealized_pnl_pct']) if pos['unrealized_pnl_pct'] else 0,
                "stop_loss_price": float(pos['stop_loss_price']) if pos.get('stop_loss_price') else None,
                "take_profit_price": float(pos['take_profit_price']) if pos.get('take_profit_price') else None,
                "first_buy_time": pos['first_buy_time'].strftime('%Y-%m-%d %H:%M:%S') if pos['first_buy_time'] else None,
                "last_update_time": pos['last_update_time'].strftime('%Y-%m-%d %H:%M:%S') if pos['last_update_time'] else None
            })

        return {
            "positions": positions,
            "total_count": len(positions)
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取账户信息失败: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trades")
async def get_trades(account_id: Optional[int] = None, limit: int = 100, engine: PaperTradingEngine = Depends(get_engine)):
    """
    获取交易历史（从数据库读取）

    Args:
        account_id: 账户ID
        limit: 返回数量限制

    Returns:
        交易历史列表
    """
    conn = engine._get_connection()
    try:
        with conn.cursor() as cursor:
            # 从数据库读取交易历史，关联订单表和持仓表获取交易类型和止盈止损信息
            cursor.execute(
                """SELECT 
                    t.*,
                    o.order_source,
                    COALESCE(
                        (SELECT p_open.stop_loss_price 
                         FROM paper_trading_positions p_open 
                         WHERE p_open.symbol = t.symbol 
                           AND p_open.account_id = t.account_id 
                           AND p_open.status = 'open' 
                         LIMIT 1),
                        (SELECT p_closed.stop_loss_price 
                         FROM paper_trading_positions p_closed 
                         WHERE p_closed.symbol = t.symbol 
                           AND p_closed.account_id = t.account_id 
                           AND p_closed.status = 'closed'
                           AND p_closed.last_update_time <= DATE_ADD(t.trade_time, INTERVAL 5 MINUTE)
                         ORDER BY p_closed.last_update_time DESC
                         LIMIT 1)
                    ) as stop_loss_price,
                    COALESCE(
                        (SELECT p_open.take_profit_price 
                         FROM paper_trading_positions p_open 
                         WHERE p_open.symbol = t.symbol 
                           AND p_open.account_id = t.account_id 
                           AND p_open.status = 'open' 
                         LIMIT 1),
                        (SELECT p_closed.take_profit_price 
                         FROM paper_trading_positions p_closed 
                         WHERE p_closed.symbol = t.symbol 
                           AND p_closed.account_id = t.account_id 
                           AND p_closed.status = 'closed'
                           AND p_closed.last_update_time <= DATE_ADD(t.trade_time, INTERVAL 5 MINUTE)
                         ORDER BY p_closed.last_update_time DESC
                         LIMIT 1)
                    ) as take_profit_price
                FROM paper_trading_trades t
                LEFT JOIN paper_trading_orders o ON t.order_id = o.order_id
                WHERE t.account_id = %s
                ORDER BY t.trade_time DESC
                LIMIT %s""",
                (account_id or 1, limit)
            )
            trades = cursor.fetchall()

            result = []
            for trade in trades:
                result.append({
                    "trade_id": trade['trade_id'],
                    "order_id": trade['order_id'],
                    "symbol": trade['symbol'],
                    "side": trade['side'],
                    "price": float(trade['price']),
                    "quantity": float(trade['quantity']),
                    "total_amount": float(trade['total_amount']),
                    "fee": float(trade['fee']),
                    "realized_pnl": float(trade['realized_pnl']) if trade['realized_pnl'] else None,
                    "pnl_pct": float(trade['pnl_pct']) if trade['pnl_pct'] else None,
                    "cost_price": float(trade['cost_price']) if trade['cost_price'] else None,
                    "trade_time": trade['trade_time'].strftime('%Y-%m-%d %H:%M:%S') if trade['trade_time'] else None,
                    "order_source": trade.get('order_source', 'manual'),
                    "stop_loss_price": float(trade['stop_loss_price']) if trade.get('stop_loss_price') else None,
                    "take_profit_price": float(trade['take_profit_price']) if trade.get('take_profit_price') else None
                })

            return {
                "success": True,
                "trades": result,
                "total_count": len(result)
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取账户信息失败: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@router.get("/closed-trades")
async def get_closed_trades(account_id: Optional[int] = None, limit: int = 100, engine: PaperTradingEngine = Depends(get_engine)):
    """
    获取已平仓的交易历史记录（仅返回有盈亏的SELL交易）

    Args:
        account_id: 账户ID
        limit: 返回数量限制

    Returns:
        已平仓交易历史列表
    """
    conn = engine._get_connection()
    try:
        with conn.cursor() as cursor:
            # 查询所有SELL交易（代表平仓）- 优化：简化查询，止盈止损信息从trades表直接获取
            cursor.execute(
                """SELECT
                    t.*,
                    o.order_source
                FROM paper_trading_trades t
                LEFT JOIN paper_trading_orders o ON t.order_id = o.order_id
                WHERE t.account_id = %s
                  AND t.side = 'SELL'
                  AND t.realized_pnl IS NOT NULL
                ORDER BY t.trade_time DESC
                LIMIT %s""",
                (account_id or 1, limit)
            )
            trades = cursor.fetchall()

            result = []
            for trade in trades:
                result.append({
                    "trade_id": trade['trade_id'],
                    "order_id": trade['order_id'],
                    "symbol": trade['symbol'],
                    "side": trade['side'],
                    "price": float(trade['price']),
                    "quantity": float(trade['quantity']),
                    "total_amount": float(trade['total_amount']),
                    "fee": float(trade['fee']),
                    "realized_pnl": float(trade['realized_pnl']) if trade['realized_pnl'] else 0,
                    "pnl_pct": float(trade['pnl_pct']) if trade['pnl_pct'] else 0,
                    "cost_price": float(trade['cost_price']) if trade['cost_price'] else None,
                    "trade_time": trade['trade_time'].strftime('%Y-%m-%d %H:%M:%S') if trade['trade_time'] else None,
                    "order_source": trade.get('order_source', 'manual'),
                    # 止损止盈信息暂时不返回（性能优化）
                    "stop_loss_price": None,
                    "take_profit_price": None
                })

            return {
                "success": True,
                "trades": result,
                "total_count": len(result)
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取已平仓交易历史失败: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@router.get("/orders")
async def get_orders(account_id: Optional[int] = None, limit: int = 100, status: Optional[str] = None, engine: PaperTradingEngine = Depends(get_engine)):
    """
    获取订单历史（从数据库读取）

    Args:
        account_id: 账户ID
        limit: 返回数量限制
        status: 订单状态过滤 (PENDING, FILLED, CANCELLED, REJECTED)

    Returns:
        订单历史列表
    """
    conn = engine._get_connection()
    try:
        with conn.cursor() as cursor:
            # 构建查询
            if status:
                cursor.execute(
                    """SELECT * FROM paper_trading_orders
                    WHERE account_id = %s AND status = %s
                    ORDER BY created_at DESC
                    LIMIT %s""",
                    (account_id or 1, status, limit)
                )
            else:
                cursor.execute(
                    """SELECT * FROM paper_trading_orders
                    WHERE account_id = %s
                    ORDER BY created_at DESC
                    LIMIT %s""",
                    (account_id or 1, limit)
                )
            orders = cursor.fetchall()

            result = []
            for order in orders:
                result.append({
                    "order_id": order['order_id'],
                    "symbol": order['symbol'],
                    "side": order['side'],
                    "order_type": order['order_type'],
                    "price": float(order['price']) if order['price'] else None,
                    "quantity": float(order['quantity']),
                    "executed_quantity": float(order['executed_quantity']),
                    "total_amount": float(order['total_amount']) if order['total_amount'] else None,
                    "executed_amount": float(order['executed_amount']) if order['executed_amount'] else None,
                    "fee": float(order['fee']),
                    "status": order['status'],
                    "avg_fill_price": float(order['avg_fill_price']) if order['avg_fill_price'] else None,
                    "fill_time": order['fill_time'].strftime('%Y-%m-%d %H:%M:%S') if order['fill_time'] else None,
                    "order_source": order['order_source'],
                    "created_at": order['created_at'].strftime('%Y-%m-%d %H:%M:%S') if order['created_at'] else None
                })

            return {
                "success": True,
                "orders": result,
                "total_count": len(result)
            }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取账户信息失败: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


@router.get("/price")
async def get_current_price(symbol: str, force_refresh: bool = False, engine: PaperTradingEngine = Depends(get_engine)):
    """
    获取当前市场价格

    Args:
        symbol: 交易对（查询参数，如 ?symbol=BTC/USDT）
        force_refresh: 是否强制从实时API获取（不使用缓存）
        engine: 自动注入的 Engine 实例

    Returns:
        当前价格
    """
    try:
        # 如果强制刷新，直接从实时API获取
        if force_refresh:
            import aiohttp
            from aiohttp import ClientTimeout
            
            price = None
            timeout = ClientTimeout(total=3)  # 3秒超时，更快响应
            
            # 尝试从Binance现货API获取
            try:
                symbol_clean = symbol.replace('/', '').upper()
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(
                        'https://api.binance.com/api/v3/ticker/price',
                        params={'symbol': symbol_clean}
                    ) as response:
                        if response.status == 200:
                            data = await response.json()
                            if data and 'price' in data:
                                price = float(data['price'])
                                logger.debug(f"从Binance实时API获取 {symbol} 价格: {price}")
            except Exception as e:
                logger.debug(f"Binance实时API获取失败: {e}")
            
            # 如果Binance失败，尝试从Gate.io获取
            if not price:
                try:
                    gate_symbol = symbol.replace('/', '_').upper()
                    async with aiohttp.ClientSession(timeout=timeout) as session:
                        async with session.get(
                            'https://api.gateio.ws/api/v4/spot/tickers',
                            params={'currency_pair': gate_symbol}
                        ) as response:
                            if response.status == 200:
                                data = await response.json()
                                if data and len(data) > 0 and 'last' in data[0]:
                                    price = float(data[0]['last'])
                                    logger.debug(f"从Gate.io实时API获取 {symbol} 价格: {price}")
                except Exception as e:
                    logger.debug(f"Gate.io实时API获取失败: {e}")
            
            if price and price > 0:
                return {
                    "symbol": symbol,
                    "price": price,
                    "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "source": "realtime_api"
                }
        
        # 默认使用引擎的价格获取（优先使用实时价格）
        # 即使没有 force_refresh，也尝试使用实时价格
        price = engine.get_current_price(symbol, use_realtime=True)

        if price == 0:
            raise HTTPException(
                status_code=404,
                detail=f"{symbol} 暂无价格数据，请确保数据采集器正在运行"
            )

        return {
            "symbol": symbol,
            "price": float(price),
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "source": "realtime_or_cache"
        }
    except HTTPException:
        raise
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取账户信息失败: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/prices/batch")
async def get_batch_prices(request: BatchPricesRequest, engine: PaperTradingEngine = Depends(get_engine)):
    """
    批量获取多个交易对的当前价格（避免大量并发请求）

    Args:
        request: 批量价格请求（包含symbols和force_refresh）

    Returns:
        价格字典
    """
    try:
        result = {}
        for symbol in request.symbols:
            try:
                # 使用缓存价格，避免大量API调用
                price = engine.get_current_price(symbol, use_realtime=False)
                result[symbol] = {
                    "price": float(price) if price else 0,
                    "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
            except Exception as e:
                logger.warning(f"获取{symbol}价格失败: {e}")
                result[symbol] = {
                    "price": 0,
                    "error": str(e)
                }
        return result
    except Exception as e:
        logger.error(f"批量获取价格失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/update-positions")
async def update_positions(account_id: Optional[int] = None, engine: PaperTradingEngine = Depends(get_engine)):
    """
    手动更新持仓市值和盈亏（包括止盈止损检测）

    Returns:
        更新结果
    """
    try:
        engine.update_positions_value(account_id or 1)
        return {
            "success": True,
            "message": "持仓市值已更新"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取账户信息失败: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/position/update-stop-loss-take-profit")
async def update_position_stop_loss_take_profit(
    request: UpdateStopLossTakeProfitRequest,
    engine: PaperTradingEngine = Depends(get_engine)
):
    """
    更新持仓的止盈止损

    Args:
        request: 更新请求

    Returns:
        更新结果
    """
    try:
        account_id = request.account_id or 1
        
        stop_loss = Decimal(str(request.stop_loss_price)) if request.stop_loss_price is not None else None
        take_profit = Decimal(str(request.take_profit_price)) if request.take_profit_price is not None else None
        
        success, message = engine.update_position_stop_loss_take_profit(
            account_id=account_id,
            symbol=request.symbol,
            stop_loss_price=stop_loss,
            take_profit_price=take_profit
        )
        
        if success:
            return {
                "success": True,
                "message": message
            }
        else:
            raise HTTPException(status_code=400, detail=message)
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"更新止盈止损失败: {str(e)}")


class CreatePendingOrderRequest(BaseModel):
    """创建待成交订单请求"""
    account_id: Optional[int] = None
    order_id: str
    symbol: str
    side: str
    quantity: float
    trigger_price: float
    order_source: str = "auto"
    stop_loss_price: Optional[float] = None
    take_profit_price: Optional[float] = None


@router.post("/pending-order")
async def create_pending_order(
    request: CreatePendingOrderRequest = Body(...),
    engine: PaperTradingEngine = Depends(get_engine)
):
    """
    创建待成交订单

    Returns:
        创建结果
    """
    try:
        account_id = request.account_id or 1
        success, message = engine.create_pending_order(
            account_id=account_id,
            order_id=request.order_id,
            symbol=request.symbol,
            side=request.side.upper(),
            quantity=Decimal(str(request.quantity)),
            trigger_price=Decimal(str(request.trigger_price)),
            order_source=request.order_source,
            stop_loss_price=Decimal(str(request.stop_loss_price)) if request.stop_loss_price else None,
            take_profit_price=Decimal(str(request.take_profit_price)) if request.take_profit_price else None
        )
        
        if success:
            return {
                "success": True,
                "message": message
            }
        else:
            raise HTTPException(status_code=400, detail=message)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"创建待成交订单失败: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/pending-orders")
async def get_pending_orders(
    account_id: Optional[int] = None,
    executed: bool = False,
    engine: PaperTradingEngine = Depends(get_engine)
):
    """
    获取待成交订单列表

    Args:
        account_id: 账户ID
        executed: 是否只获取已执行的订单

    Returns:
        待成交订单列表
    """
    try:
        orders = engine.get_pending_orders(account_id or 1, executed=executed)
        
        # 转换数据格式，确保所有字段都可以序列化
        result = []
        for order in orders:
            result.append({
                "order_id": order.get('order_id', ''),
                "symbol": order.get('symbol', ''),
                "side": order.get('side', ''),
                "quantity": float(order.get('quantity', 0)) if order.get('quantity') is not None else 0,
                "trigger_price": float(order.get('trigger_price', 0)) if order.get('trigger_price') is not None else 0,
                "frozen_amount": float(order.get('frozen_amount', 0)) if order.get('frozen_amount') is not None else 0,
                "frozen_quantity": float(order.get('frozen_quantity', 0)) if order.get('frozen_quantity') is not None else 0,
                "status": order.get('status', 'PENDING'),
                "executed": bool(order.get('executed', False)),
                "order_source": order.get('order_source', 'auto'),
                "stop_loss_price": float(order.get('stop_loss_price', 0)) if order.get('stop_loss_price') else None,
                "take_profit_price": float(order.get('take_profit_price', 0)) if order.get('take_profit_price') else None,
                "created_at": order.get('created_at').strftime('%Y-%m-%d %H:%M:%S') if order.get('created_at') else None,
                "executed_at": order.get('executed_at').strftime('%Y-%m-%d %H:%M:%S') if order.get('executed_at') else None
            })
        
        return {
            "success": True,
            "orders": result,
            "count": len(result)
        }
    except Exception as e:
        logger.error(f"获取待成交订单失败: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cancelled-orders")
async def get_cancelled_orders(
    account_id: Optional[int] = None,
    limit: int = 50,
    engine: PaperTradingEngine = Depends(get_engine)
):
    """
    获取已取消/已过期的订单列表

    Args:
        account_id: 账户ID
        limit: 返回的最大订单数

    Returns:
        已取消订单列表
    """
    try:
        orders = engine.get_cancelled_orders(account_id or 1, limit=limit)

        # 转换数据格式，确保所有字段都可以序列化
        result = []
        for order in orders:
            result.append({
                "order_id": order.get('order_id', ''),
                "symbol": order.get('symbol', ''),
                "side": order.get('side', ''),
                "quantity": float(order.get('quantity', 0)) if order.get('quantity') is not None else 0,
                "trigger_price": float(order.get('trigger_price', 0)) if order.get('trigger_price') is not None else 0,
                "frozen_amount": float(order.get('frozen_amount', 0)) if order.get('frozen_amount') is not None else 0,
                "status": order.get('status', 'CANCELLED'),
                "order_source": order.get('order_source', 'auto'),
                "stop_loss_price": float(order.get('stop_loss_price', 0)) if order.get('stop_loss_price') else None,
                "take_profit_price": float(order.get('take_profit_price', 0)) if order.get('take_profit_price') else None,
                "created_at": order.get('created_at').strftime('%Y-%m-%d %H:%M:%S') if order.get('created_at') else None,
                "updated_at": order.get('updated_at').strftime('%Y-%m-%d %H:%M:%S') if order.get('updated_at') else None
            })

        return {
            "success": True,
            "orders": result,
            "count": len(result)
        }
    except Exception as e:
        logger.error(f"获取已取消订单失败: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


class UpdatePendingOrderStopLossTakeProfitRequest(BaseModel):
    """更新待成交订单止盈止损请求"""
    order_id: str
    stop_loss_price: Optional[float] = None
    take_profit_price: Optional[float] = None


@router.put("/pending-order/stop-loss-take-profit")
async def update_pending_order_stop_loss_take_profit(
    request: UpdatePendingOrderStopLossTakeProfitRequest = Body(...),
    account_id: Optional[int] = None,
    engine: PaperTradingEngine = Depends(get_engine)
):
    """
    更新待成交订单的止盈止损价格

    Args:
        request: 更新请求
        account_id: 账户ID

    Returns:
        更新结果
    """
    try:
        conn = engine._get_connection()
        try:
            with conn.cursor() as cursor:
                # 检查订单是否存在
                cursor.execute(
                    """SELECT order_id FROM paper_trading_pending_orders 
                    WHERE order_id = %s AND account_id = %s AND executed = FALSE AND status != 'DELETED'""",
                    (request.order_id, account_id or 1)
                )
                if not cursor.fetchone():
                    raise HTTPException(status_code=404, detail="待成交订单不存在或已执行")

                # 更新止盈止损价格
                update_fields = []
                params = []
                
                if request.stop_loss_price is not None:
                    update_fields.append("stop_loss_price = %s")
                    params.append(Decimal(str(request.stop_loss_price)) if request.stop_loss_price > 0 else None)
                
                if request.take_profit_price is not None:
                    update_fields.append("take_profit_price = %s")
                    params.append(Decimal(str(request.take_profit_price)) if request.take_profit_price > 0 else None)
                
                if not update_fields:
                    raise HTTPException(status_code=400, detail="至少需要提供一个价格参数")

                params.extend([request.order_id, account_id or 1])
                cursor.execute(
                    f"""UPDATE paper_trading_pending_orders 
                    SET {', '.join(update_fields)}, updated_at = NOW()
                    WHERE order_id = %s AND account_id = %s""",
                    params
                )
                conn.commit()
                return {
                    "success": True,
                    "message": "止盈止损价格更新成功"
                }
        finally:
            conn.close()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新待成交订单止盈止损失败: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"更新止盈止损失败: {str(e)}")


@router.delete("/pending-order")
async def cancel_pending_order(
    order_id: str,
    account_id: Optional[int] = None,
    engine: PaperTradingEngine = Depends(get_engine)
):
    """
    撤销待成交订单

    Args:
        order_id: 订单ID（使用查询参数，支持包含斜杠等特殊字符）
        account_id: 账户ID

    Returns:
        撤销结果
    """
    try:
        # 使用查询参数可以正确处理包含斜杠的order_id
        logger.debug(f"收到撤单请求: order_id={order_id}, account_id={account_id}")
        success, message = engine.cancel_pending_order(account_id or 1, order_id)
        if success:
            return {
                "success": True,
                "message": message
            }
        else:
            raise HTTPException(status_code=400, detail=message)
    except HTTPException:
        raise
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取账户信息失败: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/symbols")
async def get_symbols():
    """
    获取可交易的币种列表（从配置文件读取）

    Returns:
        交易对列表
    """
    try:
        config = get_config()
        symbols = config.get('symbols', ['BTC/USDT', 'ETH/USDT'])
        return {
            "symbols": symbols,
            "total": len(symbols)
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取账户信息失败: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/ema-signals")
async def get_ema_signals(limit: int = 10):
    """
    获取最新的 EMA 买入信号

    Args:
        limit: 返回信号数量限制

    Returns:
        最新的 EMA 信号列表
    """
    try:
        from app.database.db_service import DatabaseService
        from sqlalchemy import text
        from datetime import datetime, timedelta

        db_config = get_db_config()
        db_service = DatabaseService({'type': 'mysql', 'mysql': db_config})
        session = db_service.get_session()

        try:
            # 读取信号文件（如果存在）
            import os
            signal_file = 'signals/ema_alerts.txt'
            signals = []

            if os.path.exists(signal_file):
                # 读取最近的信号
                with open(signal_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    # 倒序读取最近的信号
                    for line in reversed(lines[-limit*10:]):  # 读取更多行以确保有足够的唯一信号
                        if '买入信号' in line and '时间:' in line:
                            try:
                                # 解析信号信息
                                parts = line.split()
                                if len(parts) >= 3:
                                    symbol = parts[1]
                                    strength = '未知'

                                    if 'STRONG' in line or '强' in line:
                                        strength = 'strong'
                                    elif 'MEDIUM' in line or '中' in line:
                                        strength = 'medium'
                                    elif 'WEAK' in line or '弱' in line:
                                        strength = 'weak'

                                    # 检查是否已存在相同交易对的信号
                                    if not any(s['symbol'] == symbol for s in signals):
                                        signals.append({
                                            'symbol': symbol,
                                            'signal_strength': strength,
                                            'signal_type': 'BUY',
                                            'timeframe': '15m',
                                            'message': line.strip(),
                                            'timestamp': datetime.now().isoformat()
                                        })

                                        if len(signals) >= limit:
                                            break
                            except Exception as e:
                                continue

            # 如果文件中没有信号，返回空列表
            return {
                "success": True,
                "signals": signals,
                "count": len(signals),
                "message": "从信号文件读取成功" if signals else "暂无信号"
            }

        finally:
            session.close()

    except Exception as e:
        return {
            "success": False,
            "signals": [],
            "count": 0,
            "message": f"读取信号失败: {str(e)}"
        }


# ==================== 现货V2 (动态价格采样策略) API ====================

@router.get("/spot-v2/positions")
async def get_spot_v2_positions(account_id: Optional[int] = None, engine: PaperTradingEngine = Depends(get_engine)):
    """
    获取现货V2持仓列表 (兼容旧前端)
    现在统一使用 paper_trading_positions 表
    """
    try:
        summary = engine.get_account_summary(account_id or 1)
        if not summary:
            return {
                "positions": [],
                "total_count": 0
            }

        positions = []
        for pos in summary.get('positions', []):
            positions.append({
                "symbol": pos['symbol'],
                "quantity": float(pos['quantity']),
                "available_quantity": float(pos['available_quantity']),
                "avg_entry_price": float(pos['avg_entry_price']),
                "current_price": float(pos['current_price']) if pos['current_price'] else 0,
                "market_value": float(pos['market_value']) if pos['market_value'] else 0,
                "total_cost": float(pos['total_cost']),
                "unrealized_pnl": float(pos['unrealized_pnl']) if pos['unrealized_pnl'] else 0,
                "unrealized_pnl_pct": float(pos['unrealized_pnl_pct']) if pos['unrealized_pnl_pct'] else 0,
                "stop_loss_price": float(pos['stop_loss_price']) if pos.get('stop_loss_price') else None,
                "take_profit_price": float(pos['take_profit_price']) if pos.get('take_profit_price') else None,
                "first_buy_time": pos['first_buy_time'].strftime('%Y-%m-%d %H:%M:%S') if pos['first_buy_time'] else None,
                "last_update_time": pos['last_update_time'].strftime('%Y-%m-%d %H:%M:%S') if pos['last_update_time'] else None
            })

        return {
            "positions": positions,
            "total_count": len(positions)
        }
    except Exception as e:
        logger.error(f"获取现货V2持仓失败: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
