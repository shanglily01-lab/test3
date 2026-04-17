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
