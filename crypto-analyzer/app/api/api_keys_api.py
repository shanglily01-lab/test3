# -*- coding: utf-8 -*-
"""
API密钥管理接口
"""

from pathlib import Path
import pymysql
from dotenv import dotenv_values
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
from loguru import logger

from app.services.api_key_service import get_api_key_service

router = APIRouter(prefix="/api/api-keys", tags=["API密钥管理"])

# 单用户系统，固定 user_id
_USER_ID = 1

_ENV_PATH = Path(__file__).parent.parent.parent / '.env'
_env_cache: Optional[dict] = None


def _get_env() -> dict:
    global _env_cache
    if _env_cache is None:
        _env_cache = dotenv_values(_ENV_PATH)
    return _env_cache


def get_db_config() -> dict:
    env = _get_env()
    return {
        'host':     env.get('DB_HOST', 'localhost'),
        'port':     int(env.get('DB_PORT', 3306)),
        'user':     env.get('DB_USER', 'root'),
        'password': env.get('DB_PASSWORD', ''),
        'database': env.get('DB_NAME', ''),
    }


def _get_conn() -> pymysql.connections.Connection:
    cfg = get_db_config()
    return pymysql.connect(
        host=cfg['host'],
        port=cfg['port'],
        user=cfg['user'],
        password=cfg['password'],
        database=cfg['database'],
        cursorclass=pymysql.cursors.DictCursor,
    )


# ==================== 请求模型 ====================

class SaveAPIKeyRequest(BaseModel):
    """保存API密钥请求"""
    exchange: str = Field(default='binance', description='交易所')
    account_name: str = Field(..., min_length=1, max_length=100, description='账户名称')
    api_key: str = Field(..., min_length=10, description='API Key')
    api_secret: str = Field(..., min_length=10, description='API Secret')
    permissions: str = Field(default='spot,futures', description='权限')
    is_testnet: bool = Field(default=False, description='是否测试网')
    max_position_value: float = Field(default=1000.0, ge=0, description='最大持仓价值')
    max_daily_loss: float = Field(default=100.0, ge=0, description='最大日亏损')
    max_leverage: int = Field(default=10, ge=1, le=125, description='最大杠杆')
    margin_per_trade: float = Field(default=40.0, gt=0, description='每笔实盘保证金(USDT)，用于模拟盘->实盘同步')


class DeleteAPIKeyRequest(BaseModel):
    """删除API密钥请求"""
    api_key_id: int = Field(..., description='API密钥ID')


class UpdateRiskRequest(BaseModel):
    """更新风控参数"""
    api_key_id: int
    max_leverage: int = Field(ge=1, le=125)
    max_position_value: float = Field(ge=0)
    max_daily_loss: float = Field(ge=0)


class VerifyByIdRequest(BaseModel):
    """通过 ID 验证已保存的 API Key"""
    api_key_id: int = Field(..., description='API密钥ID')


class VerifyRawRequest(BaseModel):
    """直接验证原始 API 密钥（用于保存前测试）"""
    exchange: str = Field(default='binance', description='交易所')
    api_key: str = Field(..., min_length=10, description='API Key')
    api_secret: str = Field(..., min_length=10, description='API Secret')


# ==================== 接口 ====================

@router.get("/list")
async def list_api_keys():
    """
    获取当前用户的所有API密钥（不含密钥内容）
    """
    try:
        service = get_api_key_service()
        if not service:
            logger.error("API密钥服务未初始化")
            raise HTTPException(status_code=500, detail="API密钥服务未初始化，请检查服务器日志")

        keys = service.get_user_api_keys(_USER_ID)
        return {
            'success': True,
            'api_keys': keys
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取API密钥列表失败: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/save")
async def save_api_key(request: SaveAPIKeyRequest):
    """
    保存API密钥（新增或更新）
    """
    try:
        service = get_api_key_service()
        if not service:
            logger.error("API密钥服务未初始化")
            raise HTTPException(status_code=500, detail="API密钥服务未初始化，请检查服务器日志")

        result = service.save_api_key(
            user_id=_USER_ID,
            exchange=request.exchange,
            account_name=request.account_name,
            api_key=request.api_key,
            api_secret=request.api_secret,
            permissions=request.permissions,
            is_testnet=request.is_testnet,
            max_position_value=request.max_position_value,
            max_daily_loss=request.max_daily_loss,
            max_leverage=request.max_leverage,
            margin_per_trade=request.margin_per_trade,
        )

        if result['success']:
            return {
                'success': True,
                'message': 'API密钥保存成功',
                'api_key_id': result['api_key_id']
            }
        else:
            raise HTTPException(status_code=400, detail=result.get('error', '保存失败'))

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"保存API密钥失败: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/delete")
async def delete_api_key(request: DeleteAPIKeyRequest):
    """
    删除API密钥
    """
    service = get_api_key_service()
    if not service:
        raise HTTPException(status_code=500, detail="API密钥服务未初始化")

    try:
        result = service.delete_api_key(
            user_id=_USER_ID,
            api_key_id=request.api_key_id
        )

        if result['success']:
            return {'success': True, 'message': 'API密钥已删除'}
        else:
            raise HTTPException(status_code=400, detail=result.get('error', '删除失败'))

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除API密钥失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/verify")
async def verify_api_key(
    request: VerifyByIdRequest,
    exchange: str = 'binance'
):
    """
    验证已保存的 API 密钥是否有效（通过 api_key_id）
    """
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM user_api_keys WHERE id=%s AND user_id=%s AND status='active'",
            (request.api_key_id, _USER_ID)
        )
        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row:
            return {'success': False, 'error': '未找到对应的 API 密钥'}

        # 解密
        service = get_api_key_service()
        if service and hasattr(service, '_decrypt'):
            api_key_plain = service._decrypt(row['api_key'])
            api_secret_plain = service._decrypt(row['api_secret'])
        else:
            api_key_plain = row['api_key']
            api_secret_plain = row['api_secret']

        if row['exchange'] == 'binance':
            from app.trading.binance_futures_engine import BinanceFuturesEngine
            temp_engine = BinanceFuturesEngine(
                get_db_config(),
                api_key=api_key_plain,
                api_secret=api_secret_plain
            )
            balance = temp_engine.get_account_balance()
            if balance and balance.get('success'):
                return {
                    'success': True,
                    'balance': {
                        'balance': float(balance.get('balance', 0)),
                        'available': float(balance.get('available', 0))
                    }
                }
            else:
                return {'success': False, 'error': balance.get('error', '无法获取账户信息，请检查 API 权限')}
        else:
            return {'success': False, 'error': f'暂不支持 {row["exchange"]}'}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"验证API密钥失败: {e}")
        return {'success': False, 'error': str(e)}


@router.post("/verify-raw")
async def verify_api_key_raw(request: VerifyRawRequest):
    """
    验证原始 API Key / Secret 是否有效（不需要先保存，用于填写后即时测试）
    """
    try:
        if request.exchange == 'binance':
            from app.trading.binance_futures_engine import BinanceFuturesEngine
            temp_engine = BinanceFuturesEngine(
                get_db_config(),
                api_key=request.api_key,
                api_secret=request.api_secret
            )
            balance = temp_engine.get_account_balance()
            if balance and balance.get('success'):
                return {
                    'success': True,
                    'balance': {
                        'balance': float(balance.get('balance', 0)),
                        'available': float(balance.get('available', 0))
                    }
                }
            else:
                return {'success': False, 'error': balance.get('error', '无法获取账户信息，请检查 API 权限')}
        else:
            return {'success': False, 'error': f'暂不支持 {request.exchange}'}
    except Exception as e:
        logger.error(f"验证原始API密钥失败: {e}")
        return {'success': False, 'error': str(e)}


@router.get("/balance/{api_key_id}")
async def get_api_key_balance(api_key_id: int):
    """
    获取指定 API 密钥对应账户的实时余额
    """
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM user_api_keys WHERE id=%s AND status='active'",
            (api_key_id,)
        )
        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row:
            return {'success': False, 'error': '未找到对应的 API 密钥'}

        service = get_api_key_service()
        if service and hasattr(service, '_decrypt'):
            api_key_plain = service._decrypt(row['api_key'])
            api_secret_plain = service._decrypt(row['api_secret'])
        else:
            api_key_plain = row['api_key']
            api_secret_plain = row['api_secret']

        if row['exchange'] == 'binance':
            from app.trading.binance_futures_engine import BinanceFuturesEngine
            temp_engine = BinanceFuturesEngine(
                get_db_config(),
                api_key=api_key_plain,
                api_secret=api_secret_plain
            )
            bal = temp_engine.get_account_balance()
            if bal and bal.get('success'):
                wallet = float(bal.get('balance', 0))
                available = float(bal.get('available', 0))
                upnl = float(bal.get('unrealized_pnl', 0))
                equity = wallet + upnl
                used_margin = max(0, equity - available)
                return {
                    'success': True,
                    'data': {
                        'total_equity': equity,
                        'available_balance': available,
                        'used_margin': used_margin,
                        'unrealized_pnl': upnl,
                    }
                }
            else:
                return {'success': False, 'error': bal.get('error', '无法获取账户余额')}
        else:
            return {'success': False, 'error': f'暂不支持 {row["exchange"]}'}

    except Exception as e:
        logger.error(f"获取API密钥余额失败: {e}")
        return {'success': False, 'error': str(e)}


@router.get("/has-key")
async def has_api_key(exchange: str = 'binance'):
    """
    检查用户是否已配置API密钥
    """
    service = get_api_key_service()
    if not service:
        raise HTTPException(status_code=500, detail="API密钥服务未初始化")

    try:
        api_key = service.get_api_key(_USER_ID, exchange)
        return {
            'success': True,
            'has_key': api_key is not None,
            'exchange': exchange
        }
    except Exception as e:
        logger.error(f"检查API密钥失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/update-risk")
async def update_risk(request: UpdateRiskRequest):
    """更新指定API Key的风控参数（杠杆/持仓/日亏损限额）"""
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("SELECT id FROM user_api_keys WHERE id=%s AND user_id=%s",
                    (request.api_key_id, _USER_ID))
        if not cur.fetchone():
            raise HTTPException(status_code=403, detail="无权限或密钥不存在")
        cur.execute("""UPDATE user_api_keys
            SET max_leverage=%s, max_position_value=%s, max_daily_loss=%s, updated_at=NOW()
            WHERE id=%s""",
            (request.max_leverage, request.max_position_value, request.max_daily_loss, request.api_key_id))
        conn.commit(); cur.close(); conn.close()
        return {'success': True, 'message': '风控参数已更新'}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新风控参数失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))
