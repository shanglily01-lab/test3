"""
实盘合约交易API接口
提供币安实盘合约交易的HTTP接口（支持多账号、JWT认证）
"""

from fastapi import APIRouter, HTTPException, Query, Depends, Request
from pydantic import BaseModel, Field
from typing import Optional, List
from decimal import Decimal
from loguru import logger
import pymysql

from app.api.auth_api import get_current_user
from app.services.api_key_service import get_api_key_service

router = APIRouter(prefix="/api/live-trading", tags=["实盘交易"])


def resolve_api_key_id(request: Request, api_key_id: int = Query(0)) -> int:
    """从 query param 或 X-API-Key-ID header 中读取 api_key_id，header 优先"""
    if api_key_id == 0:
        header_val = request.headers.get('X-API-Key-ID', '0')
        try:
            api_key_id = int(header_val)
        except (ValueError, TypeError):
            pass
    return api_key_id

# 全局变量
_live_engine = None
_db_config = None
_engine_cache: dict = {}   # key: (user_id, api_key_id) -> BinanceFuturesEngine


def get_db_config():
    """获取数据库配置"""
    global _db_config
    if _db_config is None:
        try:
            from app.utils.config_loader import load_config
            config = load_config()
            _db_config = config.get('database', {}).get('mysql', {})
        except Exception as e:
            logger.error(f"加载数据库配置失败: {e}")
            _db_config = {}
    return _db_config


def get_live_engine():
    """获取默认实盘交易引擎实例（使用 config.yaml 中的 API Key，仅供价格查询等无需认证的接口使用）"""
    global _live_engine
    if _live_engine is None:
        try:
            from app.trading.binance_futures_engine import BinanceFuturesEngine
            db_config = get_db_config()
            _live_engine = BinanceFuturesEngine(db_config)
            logger.info("默认实盘交易引擎初始化成功")
        except Exception as e:
            logger.error(f"初始化实盘交易引擎失败: {e}")
            raise HTTPException(status_code=500, detail=f"初始化实盘交易引擎失败: {e}")
    return _live_engine


def get_user_engine(user_id: int, api_key_id: int):
    """
    根据用户ID和API密钥ID获取（或创建）对应的交易引擎实例。
    用户必须先在实盘页面添加自己的币安 API Key 才能使用。

    Args:
        user_id: 当前登录用户ID
        api_key_id: user_api_keys.id，0 表示自动选取用户的第一个活跃账号

    Returns:
        BinanceFuturesEngine 实例
    """
    from app.trading.binance_futures_engine import BinanceFuturesEngine

    service = get_api_key_service()
    if not service:
        raise HTTPException(status_code=500, detail="API密钥服务未初始化")

    if api_key_id == 0:
        keys = service.get_api_key(user_id, exchange='binance')
        if not keys:
            raise HTTPException(status_code=400, detail="未配置币安账号，请先在实盘页面添加币安 API Key")
        api_key_id = keys['id']
        key_info = keys
    else:
        key_info = service.get_api_key_by_id(user_id, api_key_id)
        if not key_info:
            raise HTTPException(status_code=400, detail="API密钥不存在或无权限访问")

    cache_key = (user_id, api_key_id)
    if cache_key not in _engine_cache:
        _engine_cache[cache_key] = BinanceFuturesEngine(
            get_db_config(),
            api_key=key_info['api_key'],
            api_secret=key_info['api_secret']
        )
        logger.info(f"[实盘] 用户 {user_id} 账号 '{key_info['account_name']}' 引擎已创建")

    try:
        service.update_last_used(api_key_id)
    except Exception:
        pass

    return _engine_cache[cache_key]


# ==================== 请求模型 ====================

class OpenPositionRequest(BaseModel):
    """开仓请求"""
    api_key_id: int = Field(default=0, description="API密钥ID，0表示自动选取")
    account_id: int = Field(default=1, description="账户ID（live_trading_accounts）")
    symbol: str = Field(..., description="交易对，如 BTC/USDT")
    position_side: str = Field(..., description="持仓方向: LONG 或 SHORT")
    quantity: Optional[float] = Field(default=None, gt=0, description="开仓数量（与quantity_pct二选一）")
    quantity_pct: Optional[float] = Field(default=None, gt=0, le=50, description="资金占比百分比（1-50%）")
    leverage: int = Field(default=5, ge=1, le=125, description="杠杆倍数")
    limit_price: Optional[float] = Field(default=None, description="限价（None为市价）")
    stop_loss_pct: Optional[float] = Field(default=None, description="止损百分比")
    take_profit_pct: Optional[float] = Field(default=None, description="止盈百分比")
    stop_loss_price: Optional[float] = Field(default=None, description="止损价格")
    take_profit_price: Optional[float] = Field(default=None, description="止盈价格")
    source: str = Field(default="manual", description="来源")
    strategy_id: Optional[int] = Field(default=None, description="策略ID")


class ClosePositionRequest(BaseModel):
    """平仓请求"""
    api_key_id: int = Field(default=0, description="API密钥ID，0表示自动选取")
    position_id: int = Field(..., description="持仓ID")
    close_quantity: Optional[float] = Field(default=None, description="平仓数量（None为全部）")
    reason: str = Field(default="manual", description="平仓原因")


class SetLeverageRequest(BaseModel):
    """设置杠杆请求"""
    api_key_id: int = Field(default=0, description="API密钥ID，0表示自动选取")
    symbol: str = Field(..., description="交易对")
    leverage: int = Field(..., ge=1, le=125, description="杠杆倍数")


class CancelOrderRequest(BaseModel):
    """取消订单请求"""
    api_key_id: int = Field(default=0, description="API密钥ID，0表示自动选取")
    symbol: str = Field(..., description="交易对")
    order_id: str = Field(..., description="订单ID")


class SetStopLossTakeProfitRequest(BaseModel):
    """设置止损止盈请求"""
    api_key_id: int = Field(default=0, description="API密钥ID，0表示自动选取")
    position_id: int = Field(..., description="持仓ID")
    stop_loss_price: Optional[float] = Field(default=None, description="止损价格")
    take_profit_price: Optional[float] = Field(default=None, description="止盈价格")


# ==================== API端点 ====================

@router.get("/my-accounts")
async def get_my_accounts(current_user: dict = Depends(get_current_user)):
    """
    获取当前登录用户的所有币安账号（不含密钥内容）
    """
    try:
        service = get_api_key_service()
        binance_keys = []
        if service:
            keys = service.get_user_api_keys(current_user['user_id'])
            binance_keys = [k for k in keys if k.get('exchange', '').lower() == 'binance']

        return {
            'success': True,
            'accounts': binance_keys,
            'count': len(binance_keys),
            'username': current_user.get('username', '')
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取账号列表失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/test-connection")
async def test_connection(
    api_key_id: int = Depends(resolve_api_key_id),
    current_user: dict = Depends(get_current_user)
):
    """
    测试币安API连接

    返回连接状态和账户余额
    """
    try:
        engine = get_user_engine(current_user['user_id'], api_key_id)
        result = engine.test_connection()

        if result.get('success'):
            return {
                "success": True,
                "message": "币安API连接正常",
                "data": {
                    "balance": result.get('balance', 0),
                    "available": result.get('available', 0),
                    "server_time": result.get('server_time')
                }
            }
        else:
            return {
                "success": False,
                "message": result.get('error', '连接失败'),
                "data": None
            }
    except Exception as e:
        logger.error(f"测试连接失败: {e}")
        return {
            "success": False,
            "message": str(e),
            "data": None
        }


@router.get("/account/balance")
async def get_account_balance(
    api_key_id: int = Depends(resolve_api_key_id),
    current_user: dict = Depends(get_current_user)
):
    """
    获取账户余额

    返回USDT余额信息
    """
    try:
        engine = get_user_engine(current_user['user_id'], api_key_id)
        result = engine.get_account_balance()

        if result.get('success'):
            return {
                "success": True,
                "data": {
                    "asset": result.get('asset', 'USDT'),
                    "balance": float(result.get('balance', 0)),
                    "available": float(result.get('available', 0)),
                    "unrealized_pnl": float(result.get('unrealized_pnl', 0))
                }
            }
        else:
            raise HTTPException(status_code=400, detail=result.get('error'))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取余额失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/account/info")
async def get_account_info(
    api_key_id: int = Depends(resolve_api_key_id),
    current_user: dict = Depends(get_current_user)
):
    """
    获取账户详细信息

    返回完整的账户信息
    """
    try:
        engine = get_user_engine(current_user['user_id'], api_key_id)
        result = engine.get_account_info()

        if result.get('success'):
            return {
                "success": True,
                "data": {
                    "total_margin_balance": float(result.get('total_margin_balance', 0)),
                    "available_balance": float(result.get('available_balance', 0)),
                    "total_unrealized_profit": float(result.get('total_unrealized_profit', 0)),
                    "total_wallet_balance": float(result.get('total_wallet_balance', 0))
                }
            }
        else:
            raise HTTPException(status_code=400, detail=result.get('error'))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取账户信息失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/price/{symbol:path}")
async def get_price(symbol: str):
    """
    获取当前价格

    Args:
        symbol: 交易对，如 BTCUSDT 或 BTC/USDT
    """
    try:
        # 统一格式
        if '/' not in symbol:
            if 'USDT' in symbol.upper():
                base = symbol.upper().replace('USDT', '')
                symbol = f"{base}/USDT"

        engine = get_live_engine()
        price = engine.get_current_price(symbol)

        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "price": float(price)
            }
        }
    except Exception as e:
        logger.error(f"获取价格失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/price")
async def get_price_by_query(symbol: str = Query(..., description="交易对，如 BTC/USDT")):
    """
    获取当前价格（查询参数版本）

    Args:
        symbol: 交易对，如 BTCUSDT 或 BTC/USDT
    """
    try:
        # 统一格式
        if '/' not in symbol:
            if 'USDT' in symbol.upper():
                base = symbol.upper().replace('USDT', '')
                symbol = f"{base}/USDT"

        engine = get_live_engine()
        price = engine.get_current_price(symbol)

        return {
            "success": True,
            "data": {
                "symbol": symbol,
                "price": float(price)
            }
        }
    except Exception as e:
        logger.error(f"获取价格失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/leverage")
async def set_leverage(
    request: SetLeverageRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    设置杠杆倍数

    设置指定交易对的杠杆
    """
    try:
        engine = get_user_engine(current_user['user_id'], request.api_key_id)
        result = engine.set_leverage(request.symbol, request.leverage)

        if result.get('success'):
            return {
                "success": True,
                "message": f"杠杆已设置为 {request.leverage}x",
                "data": result
            }
        else:
            raise HTTPException(status_code=400, detail=result.get('error'))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"设置杠杆失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/positions")
async def get_positions(
    api_key_id: int = Depends(resolve_api_key_id),
    current_user: dict = Depends(get_current_user)
):
    """
    获取当前持仓

    返回所有活跃持仓
    """
    try:
        engine = get_user_engine(current_user['user_id'], api_key_id)
        positions = engine.get_open_positions()

        # 转换Decimal为float
        for pos in positions:
            for key, value in pos.items():
                if isinstance(value, Decimal):
                    pos[key] = float(value)

        return {
            "success": True,
            "data": positions,
            "count": len(positions)
        }
    except Exception as e:
        logger.error(f"获取持仓失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/open")
async def open_position(
    request: OpenPositionRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    开仓

    执行实盘开仓操作

    支持两种方式指定数量：
    - quantity: 直接指定数量
    - quantity_pct: 按可用余额百分比计算（1-50%）

    注意：这是实盘交易，会使用真实资金！
    """
    try:
        engine = get_user_engine(current_user['user_id'], request.api_key_id)

        # 验证方向
        position_side = request.position_side.upper()
        if position_side not in ['LONG', 'SHORT']:
            raise HTTPException(status_code=400, detail="position_side 必须是 LONG 或 SHORT")

        # 验证必须提供 quantity 或 quantity_pct
        if request.quantity is None and request.quantity_pct is None:
            raise HTTPException(status_code=400, detail="必须提供 quantity 或 quantity_pct")

        # 如果使用百分比，需要计算实际数量
        quantity = request.quantity
        if request.quantity_pct is not None:
            # 获取账户可用余额
            balance_result = engine.get_account_balance()
            if not balance_result.get('success'):
                raise HTTPException(status_code=400, detail=f"获取账户余额失败: {balance_result.get('error')}")

            available_balance = Decimal(str(balance_result.get('available', 0)))

            # 获取当前价格
            price = request.limit_price
            if price is None:
                price = float(engine.get_current_price(request.symbol))

            if price <= 0:
                raise HTTPException(status_code=400, detail="无法获取有效价格")

            # 计算数量: margin = balance * pct% => positionValue = margin * leverage => quantity = positionValue / price
            margin_to_use = available_balance * Decimal(str(request.quantity_pct / 100))
            position_value = margin_to_use * Decimal(str(request.leverage))
            quantity = float(position_value / Decimal(str(price)))

            logger.info(f"[实盘API] 按百分比计算数量: {request.quantity_pct}% 余额={available_balance:.2f} "
                       f"保证金={margin_to_use:.2f} 数量={quantity:.6f}")

        logger.info(f"[实盘API] 收到开仓请求: {request.symbol} {position_side} "
                   f"{quantity} @ {request.limit_price or '市价'}")

        result = engine.open_position(
            account_id=request.account_id,
            symbol=request.symbol,
            position_side=position_side,
            quantity=Decimal(str(quantity)),
            leverage=request.leverage,
            limit_price=Decimal(str(request.limit_price)) if request.limit_price else None,
            stop_loss_pct=Decimal(str(request.stop_loss_pct)) if request.stop_loss_pct else None,
            take_profit_pct=Decimal(str(request.take_profit_pct)) if request.take_profit_pct else None,
            stop_loss_price=Decimal(str(request.stop_loss_price)) if request.stop_loss_price else None,
            take_profit_price=Decimal(str(request.take_profit_price)) if request.take_profit_price else None,
            source=request.source,
            strategy_id=request.strategy_id
        )

        if result.get('success'):
            return {
                "success": True,
                "message": result.get('message', '开仓成功'),
                "data": result
            }
        else:
            raise HTTPException(status_code=400, detail=result.get('error'))

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"开仓失败: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/close")
async def close_position(
    request: ClosePositionRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    平仓

    执行实盘平仓操作

    注意：这是实盘交易！
    """
    try:
        engine = get_user_engine(current_user['user_id'], request.api_key_id)

        logger.info(f"[实盘API] 收到平仓请求: position_id={request.position_id}, "
                   f"quantity={request.close_quantity}, reason={request.reason}")

        result = engine.close_position(
            position_id=request.position_id,
            close_quantity=Decimal(str(request.close_quantity)) if request.close_quantity else None,
            reason=request.reason
        )

        if result.get('success'):
            return {
                "success": True,
                "message": result.get('message', '平仓成功'),
                "data": result
            }
        else:
            raise HTTPException(status_code=400, detail=result.get('error'))

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"平仓失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class CloseBySymbolRequest(BaseModel):
    """通过交易对平仓请求"""
    api_key_id: int = Field(default=0, description="API密钥ID，0表示自动选取")
    symbol: str = Field(..., description="交易对，如 BTC/USDT")
    position_side: str = Field(..., description="持仓方向: LONG 或 SHORT")
    quantity: Optional[float] = Field(default=None, description="平仓数量（None为全部）")
    reason: str = Field(default="manual", description="平仓原因")


@router.post("/close-by-symbol")
async def close_position_by_symbol(
    request: CloseBySymbolRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    通过交易对和方向平仓

    直接向币安发送平仓订单，不依赖本地数据库

    注意：这是实盘交易！
    """
    try:
        engine = get_user_engine(current_user['user_id'], request.api_key_id)

        # 验证方向
        position_side = request.position_side.upper()
        if position_side not in ['LONG', 'SHORT']:
            raise HTTPException(status_code=400, detail="position_side 必须是 LONG 或 SHORT")

        logger.info(f"[实盘API] 收到按交易对平仓请求: {request.symbol} {position_side}")

        # 获取当前持仓
        positions = engine.get_open_positions()
        target_position = None

        for pos in positions:
            if pos['symbol'] == request.symbol and pos['position_side'] == position_side:
                target_position = pos
                break

        if not target_position:
            raise HTTPException(status_code=400, detail=f"未找到 {request.symbol} {position_side} 持仓")

        # 确定平仓数量
        close_quantity = request.quantity
        if close_quantity is None:
            close_quantity = float(target_position['quantity'])

        # 发送平仓订单
        binance_symbol = request.symbol.replace('/', '').upper()
        side = 'SELL' if position_side == 'LONG' else 'BUY'

        params = {
            'symbol': binance_symbol,
            'side': side,
            'positionSide': position_side,
            'type': 'MARKET',
            'quantity': str(close_quantity)
        }

        result = engine._request('POST', '/fapi/v1/order', params)

        if isinstance(result, dict) and result.get('success') == False:
            raise HTTPException(status_code=400, detail=result.get('error'))

        # 解析结果
        order_id = str(result.get('orderId', ''))
        executed_qty = Decimal(str(result.get('executedQty', '0')))
        avg_price = Decimal(str(result.get('avgPrice', '0')))

        if avg_price == 0:
            avg_price = engine.get_current_price(request.symbol)

        # 计算盈亏
        entry_price = Decimal(str(target_position['entry_price']))
        if position_side == 'LONG':
            pnl = (avg_price - entry_price) * executed_qty
        else:
            pnl = (entry_price - avg_price) * executed_qty

        logger.info(f"[实盘API] 平仓成功: {request.symbol} {executed_qty} @ {avg_price}, PnL={pnl:.2f}")

        return {
            "success": True,
            "message": f"平仓成功: PnL={pnl:.2f} USDT",
            "data": {
                "order_id": order_id,
                "symbol": request.symbol,
                "position_side": position_side,
                "close_quantity": float(executed_qty),
                "close_price": float(avg_price),
                "realized_pnl": float(pnl),
                "reason": request.reason
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"平仓失败: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


class CloseAllRequest(BaseModel):
    api_key_id: int = Field(default=0, description="API密钥ID，0表示自动选取")
    reason: str = Field(default="manual_close_all", description="平仓原因")


@router.post("/close-all")
async def close_all_positions(
    request: CloseAllRequest,
    current_user: dict = Depends(get_current_user)
):
    """一键平仓 — 关闭当前账号所有持仓"""
    try:
        engine = get_user_engine(current_user['user_id'], request.api_key_id)
        positions = engine.get_open_positions()
        if not positions:
            return {"success": True, "message": "当前无持仓", "closed": 0, "failed": 0}

        closed, failed, errors = 0, 0, []
        for pos in positions:
            pid = pos.get('id') or pos.get('position_id')
            try:
                result = engine.close_position(position_id=pid, reason=request.reason)
                if result.get('success'):
                    closed += 1
                else:
                    failed += 1
                    errors.append(f"{pos.get('symbol')}: {result.get('error')}")
            except Exception as ex:
                failed += 1
                errors.append(f"{pos.get('symbol')}: {ex}")

        logger.info(f"[实盘API] 一键平仓完成: closed={closed}, failed={failed}")
        return {
            "success": True,
            "message": f"平仓完成：成功 {closed} 笔，失败 {failed} 笔",
            "closed": closed,
            "failed": failed,
            "errors": errors
        }
    except Exception as e:
        logger.error(f"一键平仓失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/set-stop-loss-take-profit")
async def set_stop_loss_take_profit(
    request: SetStopLossTakeProfitRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    为已有持仓设置或修改止损止盈

    注意：
    1. 如果已有止损/止盈订单，会先取消旧订单再创建新订单
    2. 传入null可以只设置其中一个
    """
    try:
        engine = get_user_engine(current_user['user_id'], request.api_key_id)
        db_config = get_db_config()
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        # 1. 获取持仓信息
        cursor.execute(
            """SELECT * FROM live_futures_positions
            WHERE id = %s AND status = 'OPEN'""",
            (request.position_id,)
        )
        position = cursor.fetchone()

        if not position:
            raise HTTPException(status_code=404, detail=f"未找到持仓 ID={request.position_id}")

        symbol = position['symbol']
        position_side = position['position_side']
        quantity = Decimal(str(position['quantity']))
        entry_price = Decimal(str(position['entry_price']))

        logger.info(f"[实盘API] 设置止损止盈: {symbol} {position_side}, SL={request.stop_loss_price}, TP={request.take_profit_price}")

        # 2. 取消现有的止损止盈订单
        # 注意：从 2025-12-09 起，条件订单迁移到 Algo Service
        binance_symbol = symbol.replace('/', '')

        # 2.1 取消 Algo 条件单
        algo_orders = engine._request('GET', '/fapi/v1/openAlgoOrders', {'symbol': binance_symbol})

        if isinstance(algo_orders, dict) and algo_orders.get('orders'):
            for order in algo_orders['orders']:
                algo_id = order.get('algoId')
                if algo_id:
                    engine._request('DELETE', '/fapi/v1/algoOrder', {
                        'symbol': binance_symbol,
                        'algoId': algo_id
                    })
                    logger.info(f"[实盘API] 取消旧Algo订单: {algo_id}")
        elif isinstance(algo_orders, list):
            for order in algo_orders:
                algo_id = order.get('algoId')
                if algo_id:
                    engine._request('DELETE', '/fapi/v1/algoOrder', {
                        'symbol': binance_symbol,
                        'algoId': algo_id
                    })
                    logger.info(f"[实盘API] 取消旧Algo订单: {algo_id}")

        # 2.2 取消普通挂单
        open_orders = engine._request('GET', '/fapi/v1/openOrders', {'symbol': binance_symbol})

        if isinstance(open_orders, list):
            for order in open_orders:
                order_type = order.get('type', '')
                if order_type in ['LIMIT', 'STOP', 'TAKE_PROFIT']:
                    order_id = order.get('orderId')
                    engine._request('DELETE', '/fapi/v1/order', {
                        'symbol': binance_symbol,
                        'orderId': order_id
                    })
                    logger.info(f"[实盘API] 取消旧订单: {order_id} ({order_type})")

        # 3. 设置新的止损订单
        sl_order_id = None
        if request.stop_loss_price is not None:
            stop_loss_price = Decimal(str(request.stop_loss_price))

            # 验证止损价格
            sl_valid = False
            if position_side == 'LONG' and stop_loss_price < entry_price:
                sl_valid = True
            elif position_side == 'SHORT' and stop_loss_price > entry_price:
                sl_valid = True

            if sl_valid:
                sl_result = engine._place_stop_loss(symbol, position_side, quantity, stop_loss_price)
                if sl_result.get('success'):
                    sl_order_id = sl_result.get('order_id')
                    logger.info(f"[实盘API] 止损单已设置: {stop_loss_price}, 订单ID={sl_order_id}")
                else:
                    raise HTTPException(status_code=400, detail=f"止损单设置失败: {sl_result.get('error')}")
            else:
                raise HTTPException(status_code=400, detail=f"止损价格无效: {position_side} 持仓入场价 {entry_price}")

        # 4. 设置新的止盈订单
        tp_order_id = None
        if request.take_profit_price is not None:
            take_profit_price = Decimal(str(request.take_profit_price))

            # 验证止盈价格
            tp_valid = False
            if position_side == 'LONG' and take_profit_price > entry_price:
                tp_valid = True
            elif position_side == 'SHORT' and take_profit_price < entry_price:
                tp_valid = True

            if tp_valid:
                tp_result = engine._place_take_profit(symbol, position_side, quantity, take_profit_price)
                if tp_result.get('success'):
                    tp_order_id = tp_result.get('order_id')
                    logger.info(f"[实盘API] 止盈单已设置: {take_profit_price}, 订单ID={tp_order_id}")
                else:
                    raise HTTPException(status_code=400, detail=f"止盈单设置失败: {tp_result.get('error')}")
            else:
                raise HTTPException(status_code=400, detail=f"止盈价格无效: {position_side} 持仓入场价 {entry_price}")

        # 5. 更新数据库
        cursor.execute("""
            UPDATE live_futures_positions
            SET stop_loss_price = %s,
                take_profit_price = %s,
                sl_order_id = %s,
                tp_order_id = %s,
                updated_at = NOW()
            WHERE id = %s
        """, (
            request.stop_loss_price,
            request.take_profit_price,
            sl_order_id,
            tp_order_id,
            request.position_id
        ))
        conn.commit()

        cursor.close()
        conn.close()

        return {
            "success": True,
            "message": "止损止盈已设置",
            "data": {
                "position_id": request.position_id,
                "symbol": symbol,
                "stop_loss_price": request.stop_loss_price,
                "take_profit_price": request.take_profit_price,
                "sl_order_id": sl_order_id,
                "tp_order_id": tp_order_id
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"设置止损止盈失败: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/orders")
async def get_open_orders(
    symbol: Optional[str] = None,
    api_key_id: int = Depends(resolve_api_key_id),
    current_user: dict = Depends(get_current_user)
):
    """
    获取挂单

    返回所有未成交订单
    """
    try:
        engine = get_user_engine(current_user['user_id'], api_key_id)
        orders = engine.get_open_orders(symbol)

        return {
            "success": True,
            "data": orders,
            "count": len(orders)
        }
    except Exception as e:
        logger.error(f"获取挂单失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/order")
async def cancel_order(
    request: CancelOrderRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    取消订单
    """
    try:
        engine = get_user_engine(current_user['user_id'], request.api_key_id)
        result = engine.cancel_order(request.symbol, request.order_id)

        if result.get('success'):
            # 发送Telegram通知
            try:
                from app.services.trade_notifier import get_trade_notifier
                from datetime import datetime
                notifier = get_trade_notifier()
                if notifier:
                    message = f"""
🚫 <b>【订单取消】{request.symbol}</b>

📋 订单ID: {request.order_id}
💡 原因: 手动取消

⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
                    notifier._send_telegram(message)
            except Exception as notify_err:
                logger.warning(f"发送订单取消通知失败: {notify_err}")

            return {
                "success": True,
                "message": result.get('message', '订单已取消'),
                "data": result
            }
        else:
            raise HTTPException(status_code=400, detail=result.get('error'))

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"取消订单失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/orders/{symbol}")
async def cancel_all_orders(
    symbol: str,
    api_key_id: int = Depends(resolve_api_key_id),
    current_user: dict = Depends(get_current_user)
):
    """
    取消指定交易对的所有订单
    """
    try:
        engine = get_user_engine(current_user['user_id'], api_key_id)
        result = engine.cancel_all_orders(symbol)

        if result.get('success'):
            return {
                "success": True,
                "message": result.get('message'),
                "data": result
            }
        else:
            raise HTTPException(status_code=400, detail=result.get('error'))

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"取消订单失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 账户管理 ====================

@router.get("/accounts")
async def get_live_accounts():
    """
    获取实盘账户列表
    """
    try:
        db_config = get_db_config()
        connection = pymysql.connect(
            host=db_config.get('host', 'localhost'),
            port=db_config.get('port', 3306),
            user=db_config.get('user', 'root'),
            password=db_config.get('password', ''),
            database=db_config.get('database', 'binance-data'),
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT * FROM live_trading_accounts ORDER BY is_default DESC, id")
                accounts = cursor.fetchall()

                return {
                    "success": True,
                    "data": accounts,
                    "count": len(accounts)
                }
        finally:
            connection.close()

    except Exception as e:
        logger.error(f"获取账户列表失败: {e}")
        # 如果表不存在，返回空列表
        if "doesn't exist" in str(e):
            return {
                "success": True,
                "data": [],
                "count": 0,
                "message": "请先执行数据库迁移脚本创建表"
            }
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/accounts/sync")
async def sync_account_balance(
    api_key_id: int = Depends(resolve_api_key_id),
    account_id: int = Query(1, description="live_trading_accounts ID"),
    current_user: dict = Depends(get_current_user)
):
    """
    同步账户余额

    从币安获取最新余额并更新本地记录
    """
    try:
        engine = get_user_engine(current_user['user_id'], api_key_id)
        balance_result = engine.get_account_balance()

        if not balance_result.get('success'):
            raise HTTPException(status_code=400, detail=balance_result.get('error'))

        # 更新本地数据库
        db_config = get_db_config()
        connection = pymysql.connect(
            host=db_config.get('host', 'localhost'),
            port=db_config.get('port', 3306),
            user=db_config.get('user', 'root'),
            password=db_config.get('password', ''),
            database=db_config.get('database', 'binance-data'),
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True
        )

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """UPDATE live_trading_accounts
                    SET total_balance = %s,
                        available_balance = %s,
                        unrealized_pnl = %s,
                        last_sync_time = NOW()
                    WHERE id = %s""",
                    (float(balance_result.get('balance', 0)),
                     float(balance_result.get('available', 0)),
                     float(balance_result.get('unrealized_pnl', 0)),
                     account_id)
                )

                return {
                    "success": True,
                    "message": "账户余额已同步",
                    "data": {
                        "balance": float(balance_result.get('balance', 0)),
                        "available": float(balance_result.get('available', 0)),
                        "unrealized_pnl": float(balance_result.get('unrealized_pnl', 0))
                    }
                }
        finally:
            connection.close()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"同步账户余额失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 风控设置 ====================

@router.get("/risk-config/{account_id}")
async def get_risk_config(account_id: int):
    """
    获取风控配置
    """
    try:
        db_config = get_db_config()
        connection = pymysql.connect(
            host=db_config.get('host', 'localhost'),
            port=db_config.get('port', 3306),
            user=db_config.get('user', 'root'),
            password=db_config.get('password', ''),
            database=db_config.get('database', 'binance-data'),
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """SELECT max_position_value, max_daily_loss,
                              max_total_positions, max_leverage
                    FROM live_trading_accounts WHERE id = %s""",
                    (account_id,)
                )
                config = cursor.fetchone()

                if not config:
                    raise HTTPException(status_code=404, detail="账户不存在")

                return {
                    "success": True,
                    "data": config
                }
        finally:
            connection.close()

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取风控配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


class RiskConfigRequest(BaseModel):
    """风控配置请求"""
    max_position_value: float = Field(default=1000, description="单笔最大持仓价值")
    max_daily_loss: float = Field(default=100, description="日最大亏损")
    max_total_positions: int = Field(default=5, description="最大同时持仓数")
    max_leverage: int = Field(default=10, description="最大杠杆")


@router.put("/risk-config/{account_id}")
async def update_risk_config(account_id: int, request: RiskConfigRequest):
    """
    更新风控配置
    """
    try:
        db_config = get_db_config()
        connection = pymysql.connect(
            host=db_config.get('host', 'localhost'),
            port=db_config.get('port', 3306),
            user=db_config.get('user', 'root'),
            password=db_config.get('password', ''),
            database=db_config.get('database', 'binance-data'),
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True
        )

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    """UPDATE live_trading_accounts
                    SET max_position_value = %s,
                        max_daily_loss = %s,
                        max_total_positions = %s,
                        max_leverage = %s
                    WHERE id = %s""",
                    (request.max_position_value, request.max_daily_loss,
                     request.max_total_positions, request.max_leverage, account_id)
                )

                return {
                    "success": True,
                    "message": "风控配置已更新"
                }
        finally:
            connection.close()

    except Exception as e:
        logger.error(f"更新风控配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 币安同步 ====================

@router.post("/sync-from-binance")
async def sync_from_binance(
    api_key_id: int = Depends(resolve_api_key_id),
    account_id: int = Query(1, description="live_trading_accounts ID"),
    current_user: dict = Depends(get_current_user)
):
    """
    从币安同步持仓状态到本地数据库

    处理以下情况：
    1. 在币安APP手动平仓的订单 -> 更新状态为CLOSED
    2. 在币安APP撤销的限价单 -> 更新状态为CANCELED
    3. 在币安APP手动开的仓 -> 新增记录到数据库
    """
    try:
        engine = get_user_engine(current_user['user_id'], api_key_id)
        result = engine.sync_positions_from_binance(account_id)

        if result.get('success'):
            return {
                "success": True,
                "message": f"同步完成: 已平仓{result.get('closed', 0)}个, "
                          f"已取消{result.get('canceled', 0)}个, "
                          f"已成交{result.get('filled', 0)}个, "
                          f"新增{result.get('new', 0)}个",
                "data": result
            }
        else:
            raise HTTPException(status_code=400, detail=result.get('error'))

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"从币安同步失败: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/history")
async def get_position_history(
    days: int = Query(30, ge=1, le=365),
    api_key_id: int = Depends(resolve_api_key_id),
    current_user: dict = Depends(get_current_user)
):
    """获取近N天已平仓的历史订单（来自 live_futures_positions）"""
    from datetime import datetime
    service = get_api_key_service()
    if not service:
        raise HTTPException(status_code=500, detail="API密钥服务未初始化")

    if api_key_id > 0:
        key_info = service.get_api_key_by_id(current_user['user_id'], api_key_id)
    else:
        key_info = service.get_api_key(current_user['user_id'], exchange='binance')

    if not key_info:
        raise HTTPException(status_code=400, detail="未找到API密钥，请先添加币安账号")

    account_id = key_info['id']
    db_config = get_db_config()

    try:
        conn = pymysql.connect(
            **db_config, charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor, autocommit=True
        )
        cursor = conn.cursor()
        cursor.execute("""
            SELECT symbol, position_side, quantity, entry_price, close_price,
                   realized_pnl, leverage, source, notes,
                   open_time, close_time, close_reason, status
            FROM live_futures_positions
            WHERE account_id = %s
              AND status IN ('CLOSED', 'LIQUIDATED')
              AND open_time >= DATE_SUB(NOW(), INTERVAL %s DAY)
            ORDER BY close_time DESC
            LIMIT 200
        """, (account_id, days))
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"查询历史订单失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

    for row in rows:
        for k, v in row.items():
            if isinstance(v, Decimal):
                row[k] = float(v)
            elif isinstance(v, datetime):
                row[k] = v.isoformat()

        # realized_pnl 为 0 时（币安侧止损/止盈，未回写 DB），用价差 * 数量估算
        ep = row.get('entry_price') or 0
        cp = row.get('close_price') or 0
        qty = row.get('quantity') or 0
        rpnl = row.get('realized_pnl') or 0
        row['pnl_estimated'] = False
        if rpnl == 0 and ep > 0 and cp > 0 and qty > 0:
            side = (row.get('position_side') or '').upper()
            if side == 'LONG':
                row['realized_pnl'] = round((float(cp) - float(ep)) * float(qty), 4)
            elif side == 'SHORT':
                row['realized_pnl'] = round((float(ep) - float(cp)) * float(qty), 4)
            row['pnl_estimated'] = True

    return {"success": True, "data": rows, "count": len(rows)}
