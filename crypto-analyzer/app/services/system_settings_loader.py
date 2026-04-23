"""
系统配置加载器
从数据库读取系统配置，避免Linux权限问题
"""
import pymysql
from loguru import logger
from typing import Dict, Any


def get_system_settings() -> Dict[str, Any]:
    """
    从数据库获取系统配置

    Returns:
        配置字典
    """
    try:
        # 直接读取数据库配置
        import os
        db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'user': os.getenv('DB_USER', ''),
            'password': os.getenv('DB_PASSWORD', ''),
            'database': os.getenv('DB_NAME', ''),
            'charset': 'utf8mb4'
        }

        conn = pymysql.connect(**db_config, cursorclass=pymysql.cursors.DictCursor)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT setting_key, setting_value
            FROM system_settings
        """)

        settings = cursor.fetchall()
        cursor.close()
        conn.close()

        # 转换为字典
        result = {}
        for setting in settings:
            key = setting['setting_key']
            value = setting['setting_value']

            # 转换boolean值
            if value.lower() in ['true', 'false']:
                result[key] = value.lower() == 'true'
            else:
                result[key] = value

        return result

    except Exception as e:
        logger.warning(f"⚠️  从数据库加载系统配置失败: {e}，使用默认配置")
        # 返回默认配置
        return {
            'batch_entry_strategy': 'kline_pullback',
            'big4_filter_enabled': True
        }


def get_batch_entry_strategy() -> str:
    """
    获取分批建仓策略

    Returns:
        'kline_pullback' (V2) or 'price_percentile' (V1)
    """
    settings = get_system_settings()
    return settings.get('batch_entry_strategy', 'kline_pullback')


def get_big4_filter_enabled() -> bool:
    """
    获取Big4过滤器状态

    Returns:
        True/False
    """
    settings = get_system_settings()
    return settings.get('big4_filter_enabled', True)


def get_disable_sl_tp_hold() -> bool:
    """
    获取"不设止盈/止损/持仓时间"总开关。
    开启后策略引擎 (live/whale/bigmid) 新开仓时不写 SL/TP/timeout,
    且进程内的硬止盈、移动止盈检查会跳过。仅影响新开仓,存量仓位不变。

    Returns:
        True=裸奔(不设SL/TP/持仓) / False=正常
    """
    settings = get_system_settings()
    val = settings.get('disable_sl_tp_hold', False)
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() in ('1', 'true', 'yes', 'on')


def get_close_sync_live_enabled() -> bool:
    """
    获取"平仓同步实盘"总开关。
    与 live_trading_enabled（开仓同步）独立：
    - live_trading_enabled        → paper 开仓时同步在实盘开同向仓
    - close_sync_live_enabled     → paper 平仓时同步平 Binance 对应仓位

    默认 False：paper 平仓不牵连实盘，必须显式勾选才开启。

    Returns:
        True=平仓时同步实盘 / False=不同步
    """
    settings = get_system_settings()
    val = settings.get('close_sync_live_enabled', False)
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() in ('1', 'true', 'yes', 'on')
