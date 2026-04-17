# -*- coding: utf-8 -*-
"""
数据管理API
提供数据统计、查询和维护功能
"""

from fastapi import APIRouter, HTTPException, UploadFile, File, Form, BackgroundTasks
from fastapi.responses import FileResponse
from typing import Dict, List, Optional
from datetime import datetime, timedelta, date
from loguru import logger
import pymysql
from pymysql.cursors import DictCursor
import yaml
from pathlib import Path
import csv
import io
import asyncio
import pandas as pd
from app.services.data_collection_task_manager import task_manager, TaskStatus
import threading

router = APIRouter(prefix="/api/data-management", tags=["数据管理"])


def _iso(dt) -> Optional[str]:
    """将 datetime/str/None 统一转为 ISO 字符串，容忍 DB 返回 str 的情况。"""
    if dt is None:
        return None
    if hasattr(dt, 'isoformat'):
        return dt.isoformat()
    return str(dt)

# 数据库连接池（全局变量）
_db_pool = None
_db_pool_lock = threading.Lock()
_db_config = None

# collection-status缓存（数据采集情况不需要实时更新，缓存5分钟）
_collection_status_cache = None
_collection_status_cache_time = None
_collection_status_cache_lock = threading.Lock()
COLLECTION_STATUS_CACHE_TTL = 30   # 30秒内命中缓存（防并发），实际数据由存储过程每5分钟刷新

# statistics缓存（数据统计不需要实时更新，缓存5分钟）
_statistics_cache = None
_statistics_cache_time = None
_statistics_cache_lock = threading.Lock()
STATISTICS_CACHE_TTL = 300  # 缓存5分钟


def get_db_config():
    """获取数据库配置（缓存）"""
    global _db_config
    if _db_config is None:
        from app.utils.config_loader import load_config
        config = load_config()
        _db_config = config.get('database', {}).get('mysql', {})
    return _db_config


def get_db_connection(retry_count=3):
    """获取数据库连接（使用连接池，带重试机制）"""
    global _db_pool
    
    if _db_pool is None:
        with _db_pool_lock:
            # 双重检查，避免重复创建
            if _db_pool is None:
                db_config = get_db_config()
                try:
                    # 使用 pymysql 的连接池（通过自定义实现）
                    # 由于 pymysql 没有内置连接池，我们使用简单的连接复用
                    _db_pool = {
                        'config': db_config,
                        'connections': [],
                        'max_size': 20,  # 增加连接池大小
                        'lock': threading.Lock()
                    }
                    logger.info("✅ 数据管理API数据库连接池初始化成功")
                except Exception as e:
                    logger.error(f"❌ 数据库连接池初始化失败: {e}")
                    raise
    
    # 尝试从池中获取连接
    pool = _db_pool
    with pool['lock']:
        # 清理已关闭或失效的连接
        valid_connections = []
        for conn in pool['connections']:
            try:
                # 检查连接是否有效
                if conn.open:
                    # 尝试ping数据库以验证连接
                    conn.ping(reconnect=False)
                    valid_connections.append(conn)
                else:
                    try:
                        conn.close()
                    except:
                        pass
            except:
                # 连接已失效，关闭它
                try:
                    conn.close()
                except:
                    pass
        pool['connections'] = valid_connections
        
        # 如果有可用连接，直接返回
        if pool['connections']:
            conn = pool['connections'].pop()
            try:
                # 再次验证连接有效性
                conn.ping(reconnect=False)
                return conn
            except:
                # 连接失效，继续创建新连接
                try:
                    conn.close()
                except:
                    pass
        
        # 否则创建新连接
        if len(pool['connections']) < pool['max_size']:
            for attempt in range(retry_count):
                try:
                    conn = pymysql.connect(
                        host=pool['config'].get('host', 'localhost'),
                        port=pool['config'].get('port', 3306),
                        user=pool['config'].get('user', 'root'),
                        password=pool['config'].get('password', ''),
                        database=pool['config'].get('database', 'binance-data'),
                        charset='utf8mb4',
                        cursorclass=DictCursor,
                        connect_timeout=10,
                        read_timeout=120,
                        write_timeout=60,
                        autocommit=False
                    )
                    # 验证连接
                    conn.ping(reconnect=False)
                    return conn
                except Exception as e:
                    if attempt < retry_count - 1:
                        logger.warning(f"⚠️ 创建数据库连接失败（尝试 {attempt + 1}/{retry_count}）: {e}")
                        import time
                        time.sleep(0.5)  # 等待后重试
                    else:
                        logger.error(f"❌ 创建数据库连接失败（已重试 {retry_count} 次）: {e}")
                        raise
    
    # 如果池已满，创建临时连接（不放入池中）
    db_config = get_db_config()
    for attempt in range(retry_count):
        try:
            conn = pymysql.connect(
                host=db_config.get('host', 'localhost'),
                port=db_config.get('port', 3306),
                user=db_config.get('user', 'root'),
                password=db_config.get('password', ''),
                database=db_config.get('database', 'binance-data'),
                charset='utf8mb4',
                cursorclass=DictCursor,
                connect_timeout=10,
                read_timeout=120,
                write_timeout=60,
                autocommit=False
            )
            conn.ping(reconnect=False)
            return conn
        except Exception as e:
            if attempt < retry_count - 1:
                logger.warning(f"⚠️ 创建临时数据库连接失败（尝试 {attempt + 1}/{retry_count}）: {e}")
                import time
                time.sleep(0.5)
            else:
                logger.error(f"❌ 创建临时数据库连接失败（已重试 {retry_count} 次）: {e}")
                raise


def return_db_connection(conn):
    """归还数据库连接到池中"""
    global _db_pool
    if _db_pool and conn and conn.open:
        pool = _db_pool
        with pool['lock']:
            if len(pool['connections']) < pool['max_size']:
                pool['connections'].append(conn)
                return
    # 如果池已满或连接已关闭，直接关闭连接
    if conn:
        try:
            conn.close()
        except:
            pass


class DBConnection:
    """数据库连接上下文管理器"""
    def __init__(self):
        self.conn = None
    
    def __enter__(self):
        self.conn = get_db_connection()
        return self.conn
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            return_db_connection(self.conn)
        return False


def _update_config_file(config_path: Path, symbols: List[str], data_type: str, timeframe: str = None) -> bool:
    """
    更新config.yaml文件，添加新的交易对和时间周期
    
    Args:
        config_path: 配置文件路径
        symbols: 要添加的交易对列表
        data_type: 数据类型 ('price' 或 'kline')
        timeframe: 时间周期（仅kline类型需要）
    
    Returns:
        是否成功更新
    """
    try:
        # 读取现有配置
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        updated = False
        
        # 更新symbols列表
        if 'symbols' not in config:
            config['symbols'] = []
        
        existing_symbols = set(config['symbols'])
        new_symbols = []
        for symbol in symbols:
            symbol = symbol.strip()
            if symbol and symbol not in existing_symbols:
                config['symbols'].append(symbol)
                new_symbols.append(symbol)
                updated = True
        
        # 如果是K线数据，更新timeframes列表
        if data_type == 'kline' and timeframe:
            if 'collector' not in config:
                config['collector'] = {}
            if 'timeframes' not in config['collector']:
                config['collector']['timeframes'] = []
            
            existing_timeframes = set(config['collector']['timeframes'])
            if timeframe not in existing_timeframes:
                config['collector']['timeframes'].append(timeframe)
                updated = True
        
        # 如果有更新，保存配置文件
        if updated:
            with open(config_path, 'w', encoding='utf-8') as f:
                # 使用更好的格式选项保持YAML可读性
                yaml.dump(config, f, allow_unicode=True, default_flow_style=False, 
                         sort_keys=False, indent=2, width=120)
            logger.info(f"配置文件已更新: 添加了 {len(new_symbols)} 个新交易对" + 
                      (f"，时间周期 {timeframe}" if data_type == 'kline' and timeframe else ""))
        
        return updated
        
    except Exception as e:
        logger.error(f"更新配置文件失败: {e}")
        import traceback
        traceback.print_exc()
        return False


def _check_status_active(latest_time, threshold_seconds):
    """
    检查数据采集状态是否活跃
    
    判断逻辑：
    - 如果最新数据时间在阈值内，返回 'active'
    - 如果最新数据时间超过阈值但小于阈值的3倍，返回 'warning'（可能延迟）
    - 如果最新数据时间超过阈值的3倍，返回 'inactive'
    """
    try:
        if not latest_time:
            return 'inactive'
        
        # 转换为datetime对象
        if isinstance(latest_time, str):
            # 尝试解析ISO格式
            try:
                latest_time = datetime.fromisoformat(latest_time.replace('Z', '+00:00'))
            except:
                # 如果失败，尝试其他常见格式
                try:
                    # 尝试 MySQL datetime 格式: YYYY-MM-DD HH:MM:SS
                    latest_time = datetime.strptime(latest_time, '%Y-%m-%d %H:%M:%S')
                except:
                    try:
                        # 尝试带毫秒的格式
                        latest_time = datetime.strptime(latest_time, '%Y-%m-%d %H:%M:%S.%f')
                    except:
                        logger.error(f"无法解析时间格式: {latest_time}")
                        return 'inactive'
        elif not isinstance(latest_time, datetime):
            # 如果是其他类型（如MySQL的datetime对象），尝试转换
            if hasattr(latest_time, 'isoformat'):
                try:
                    latest_time = datetime.fromisoformat(latest_time.isoformat())
                except:
                    latest_time = datetime.strptime(str(latest_time), '%Y-%m-%d %H:%M:%S')
            else:
                try:
                    latest_time = datetime.strptime(str(latest_time), '%Y-%m-%d %H:%M:%S')
                except:
                    logger.error(f"无法转换时间类型: {type(latest_time)}, value: {latest_time}")
                    return 'inactive'
        
        # 处理时区：统一转换为无时区的本地时间进行比较
        if latest_time.tzinfo is not None:
            # 如果有时区信息，转换为本地时间
            latest_time = latest_time.replace(tzinfo=None)
        
        # 计算时间差（秒）
        now = datetime.now()
        time_diff = (now - latest_time).total_seconds()
        
        # 判断状态
        if time_diff < 0:
            # 未来时间，可能是时区问题，视为活跃
            return 'active'
        elif time_diff <= threshold_seconds:
            return 'active'
        elif time_diff <= threshold_seconds * 3:
            return 'warning'  # 延迟但可能还在运行
        else:
            return 'inactive'
            
    except Exception as e:
        logger.error(f"检查状态失败: {e}, latest_time={latest_time}, type={type(latest_time)}")
        import traceback
        traceback.print_exc()
        return 'inactive'


@router.get("/statistics")
async def get_data_statistics():
    """
    获取所有数据表的统计信息（存储过程缓存版：直接读 data_management_stats_cache 表）
    存储过程 update_data_management_stats_cache() 每5分钟刷新缓存
    """
    global _statistics_cache, _statistics_cache_time

    # Python层缓存（30秒），避免同一时间多次请求打到数据库
    with _statistics_cache_lock:
        if _statistics_cache is not None and _statistics_cache_time is not None:
            cache_age = (datetime.now() - _statistics_cache_time).total_seconds()
            if cache_age < 30:
                logger.debug(f"使用内存缓存数据统计 (缓存年龄: {cache_age:.0f}秒)")
                return _statistics_cache

    try:
        large_tables = {'price_data', 'kline_data', 'trade_data', 'smart_money_transactions'}

        # 静态元数据（label/description/category 由 Python 维护，不进入存储过程）
        tables = [
            # ---- 市场数据 ----
            {'name': 'price_data',                        'label': '实时价格',        'description': '交易所实时价格数据',          'is_binance': True,  'category': '市场数据'},
            {'name': 'kline_data',                        'label': 'K线数据',         'description': '多周期K线数据',               'is_binance': True,  'category': '市场数据'},
            {'name': 'orderbook_data',                    'label': '订单簿',          'description': '订单簿深度数据',              'is_binance': True,  'category': '市场数据'},
            {'name': 'trade_data',                        'label': '成交记录',        'description': '历史成交数据',                'is_binance': True,  'category': '市场数据'},
            {'name': 'price_stats_24h',                   'label': '24H价格统计',     'description': '24小时价格统计缓存',          'is_binance': False, 'category': '市场数据'},
            # ---- 合约数据 ----
            {'name': 'funding_rate_data',                 'label': '资金费率',        'description': '合约资金费率数据',            'is_binance': True,  'category': '合约数据'},
            {'name': 'funding_rate_stats',                'label': '资金费率统计',    'description': '资金费率统计汇总',            'is_binance': False, 'category': '合约数据'},
            {'name': 'futures_open_interest',             'label': '持仓量',          'description': '合约持仓量数据',              'is_binance': True,  'category': '合约数据'},
            {'name': 'futures_long_short_ratio',          'label': '多空比',          'description': '合约多空比数据',              'is_binance': True,  'category': '合约数据'},
            {'name': 'futures_liquidations',              'label': '清算数据',        'description': '合约清算记录',                'is_binance': True,  'category': '合约数据'},
            {'name': 'futures_klines',                    'label': '合约K线',         'description': '合约K线数据',                 'is_binance': True,  'category': '合约数据'},
            {'name': 'futures_funding_fees',              'label': '合约资金费',      'description': '合约已收资金费记录',          'is_binance': False, 'category': '合约数据'},
            # ---- U本位合约（模拟） ----
            {'name': 'futures_positions',                 'label': '合约持仓',        'description': 'U本位合约持仓记录',           'is_binance': False, 'category': 'U本位合约'},
            {'name': 'futures_orders',                    'label': '合约订单',        'description': 'U本位合约订单记录',           'is_binance': False, 'category': 'U本位合约'},
            {'name': 'futures_trades',                    'label': '合约成交',        'description': 'U本位合约成交记录',           'is_binance': False, 'category': 'U本位合约'},
            {'name': 'futures_trading_accounts',          'label': '合约账户',        'description': 'U本位合约账户信息',           'is_binance': False, 'category': 'U本位合约'},
            {'name': 'pending_positions',                 'label': '待开仓',          'description': '等待开仓的挂单记录',          'is_binance': False, 'category': 'U本位合约'},
            {'name': 'sentinel_orders',                   'label': '哨兵订单',        'description': '哨兵策略订单记录',            'is_binance': False, 'category': 'U本位合约'},
            # ---- 实盘合约 ----
            {'name': 'live_futures_positions',            'label': '实盘持仓',        'description': '实盘合约持仓记录',            'is_binance': False, 'category': '实盘合约'},
            {'name': 'live_futures_orders',               'label': '实盘订单',        'description': '实盘合约订单记录',            'is_binance': False, 'category': '实盘合约'},
            {'name': 'live_futures_trades',               'label': '实盘成交',        'description': '实盘合约成交记录',            'is_binance': False, 'category': '实盘合约'},
            {'name': 'live_trading_accounts',             'label': '实盘账户',        'description': '实盘合约账户信息',            'is_binance': False, 'category': '实盘合约'},
            {'name': 'live_trading_logs',                 'label': '实盘日志',        'description': '实盘交易日志',                'is_binance': False, 'category': '实盘合约'},
            {'name': 'user_api_keys',                     'label': 'API密钥',         'description': '用户Binance API密钥配置',     'is_binance': False, 'category': '实盘合约'},
            # ---- 现货交易 ----
            {'name': 'paper_trading_positions',           'label': '现货持仓',        'description': '现货持仓记录',                'is_binance': False, 'category': '现货交易'},
            {'name': 'paper_trading_orders',              'label': '现货订单',        'description': '现货订单记录',                'is_binance': False, 'category': '现货交易'},
            {'name': 'paper_trading_trades',              'label': '现货成交',        'description': '现货成交记录',                'is_binance': False, 'category': '现货交易'},
            {'name': 'paper_trading_accounts',            'label': '现货账户',        'description': '现货账户信息',                'is_binance': False, 'category': '现货交易'},
            {'name': 'paper_trading_balance_history',     'label': '现货余额历史',    'description': '现货账户余额历史',            'is_binance': False, 'category': '现货交易'},
            {'name': 'paper_trading_pending_orders',      'label': '现货挂单',        'description': '现货挂单记录',                'is_binance': False, 'category': '现货交易'},
            {'name': 'paper_trading_signal_executions',   'label': '现货信号执行',    'description': '现货信号执行记录',            'is_binance': False, 'category': '现货交易'},
            {'name': 'spot_positions',                    'label': '现货持仓V1',      'description': '旧版现货持仓记录',            'is_binance': False, 'category': '现货交易'},
            {'name': 'spot_positions_v2',                 'label': '现货持仓V2',      'description': '新版现货持仓记录',            'is_binance': False, 'category': '现货交易'},
            {'name': 'spot_batch_history',                'label': '现货批量历史',    'description': '现货批量操作历史',            'is_binance': False, 'category': '现货交易'},
            {'name': 'spot_capital_usage',                'label': '现货资金使用',    'description': '现货策略资金占用',            'is_binance': False, 'category': '现货交易'},
            {'name': 'spot_signals_history',              'label': '现货信号历史',    'description': '现货策略历史信号',            'is_binance': False, 'category': '现货交易'},
            {'name': 'spot_trading_logs',                 'label': '现货交易日志',    'description': '现货策略操作日志',            'is_binance': False, 'category': '现货交易'},
            # ---- 信号分析 ----
            {'name': 'ema_signals',                       'label': 'EMA信号',         'description': 'EMA技术指标信号',             'is_binance': False, 'category': '信号分析'},
            {'name': 'signal_blacklist',                  'label': '信号黑名单',      'description': '失败信号黑名单',              'is_binance': False, 'category': '信号分析'},
            {'name': 'signal_component_performance',      'label': '信号组件性能',    'description': '信号组件历史性能统计',        'is_binance': False, 'category': '信号分析'},
            {'name': 'signal_scoring_weights',            'label': '信号权重',        'description': '各策略信号评分权重',          'is_binance': False, 'category': '信号分析'},
            {'name': 'signal_position_multipliers',       'label': '信号仓位倍数',    'description': '信号仓位倍增配置',            'is_binance': False, 'category': '信号分析'},
            {'name': 'signal_threshold_overrides',        'label': '信号阈值覆盖',    'description': '信号评分阈值动态覆盖',        'is_binance': False, 'category': '信号分析'},
            {'name': 'signal_analysis_reports',           'label': '信号分析报告',    'description': '信号分析汇总报告',            'is_binance': False, 'category': '信号分析'},
            {'name': 'big4_trend_history',                'label': 'Big4趋势历史',    'description': 'BTC/ETH/BNB/SOL趋势记录',    'is_binance': False, 'category': '信号分析'},
            {'name': 'investment_recommendations',        'label': '投资建议',        'description': 'AI投资建议',                  'is_binance': False, 'category': '信号分析'},
            {'name': 'investment_recommendations_cache',  'label': '投资建议缓存',    'description': '投资建议缓存数据',            'is_binance': False, 'category': '信号分析'},
            {'name': 'trading_symbol_rating',             'label': '币种评级',        'description': '交易对评分与限制级别',        'is_binance': False, 'category': '信号分析'},
            {'name': 'technical_indicators_cache',        'label': '技术指标缓存',    'description': '技术指标计算缓存',            'is_binance': False, 'category': '信号分析'},
            # ---- 交易分析 ----
            {'name': 'daily_review_reports',              'label': '每日复盘',        'description': '每日交易复盘报告',            'is_binance': False, 'category': '交易分析'},
            {'name': 'daily_review_signal_analysis',      'label': '复盘信号分析',    'description': '每日信号组件明细',            'is_binance': False, 'category': '交易分析'},
            {'name': 'daily_review_opportunities',        'label': '复盘机会',        'description': '复盘发现的错失机会',          'is_binance': False, 'category': '交易分析'},
            {'name': 'retrospective_analysis',            'label': '回溯分析',        'description': '历史策略回溯分析结果',        'is_binance': False, 'category': '交易分析'},
            # ---- ETF数据 ----
            {'name': 'crypto_etf_flows',                  'label': 'ETF流向',         'description': '加密货币ETF资金流向',         'is_binance': False, 'category': 'ETF数据'},
            {'name': 'crypto_etf_products',               'label': 'ETF产品',         'description': 'ETF产品信息',                 'is_binance': False, 'category': 'ETF数据'},
            {'name': 'crypto_etf_events',                 'label': 'ETF事件',         'description': 'ETF重要事件',                 'is_binance': False, 'category': 'ETF数据'},
            {'name': 'crypto_etf_daily_summary',          'label': 'ETF日度汇总',     'description': 'ETF每日统计',                 'is_binance': False, 'category': 'ETF数据'},
            {'name': 'crypto_etf_sentiment',              'label': 'ETF情绪',         'description': 'ETF市场情绪数据',             'is_binance': False, 'category': 'ETF数据'},
            # ---- 企业金库 ----
            {'name': 'corporate_treasury_companies',      'label': '企业信息',        'description': '持有加密货币的企业',          'is_binance': False, 'category': '企业金库'},
            {'name': 'corporate_treasury_purchases',      'label': '企业买入',        'description': '企业加密资产买入记录',        'is_binance': False, 'category': '企业金库'},
            {'name': 'corporate_treasury_financing',      'label': '企业融资',        'description': '企业融资数据',                'is_binance': False, 'category': '企业金库'},
            {'name': 'corporate_treasury_stock_prices',   'label': '企业股价',        'description': '相关企业股票价格数据',        'is_binance': False, 'category': '企业金库'},
            {'name': 'corporate_treasury_summary',        'label': '企业汇总',        'description': '企业持仓汇总',                'is_binance': False, 'category': '企业金库'},
            # ---- Gas数据 ----
            {'name': 'blockchain_gas_daily',              'label': 'Gas日度',         'description': '区块链Gas每日统计',           'is_binance': False, 'category': 'Gas数据'},
            {'name': 'blockchain_gas_daily_summary',      'label': 'Gas汇总',         'description': 'Gas数据日度汇总',             'is_binance': False, 'category': 'Gas数据'},
            # ---- Hyperliquid ----
            {'name': 'hyperliquid_traders',               'label': 'HL交易员',        'description': 'Hyperliquid交易员信息',       'is_binance': False, 'category': 'Hyperliquid'},
            {'name': 'hyperliquid_monitored_wallets',     'label': 'HL监控钱包',      'description': 'HL重点监控钱包列表',          'is_binance': False, 'category': 'Hyperliquid'},
            {'name': 'hyperliquid_wallet_positions',      'label': 'HL持仓',          'description': 'HL钱包持仓',                  'is_binance': False, 'category': 'Hyperliquid'},
            {'name': 'hyperliquid_wallet_trades',         'label': 'HL交易',          'description': 'HL钱包交易',                  'is_binance': False, 'category': 'Hyperliquid'},
            {'name': 'hyperliquid_wallet_fund_changes',   'label': 'HL资金变动',      'description': 'HL钱包资金变动记录',          'is_binance': False, 'category': 'Hyperliquid'},
            {'name': 'hyperliquid_monthly_performance',   'label': 'HL月度',          'description': 'HL月度表现',                  'is_binance': False, 'category': 'Hyperliquid'},
            {'name': 'hyperliquid_weekly_performance',    'label': 'HL周度',          'description': 'HL周度表现',                  'is_binance': False, 'category': 'Hyperliquid'},
            {'name': 'hyperliquid_performance_snapshots', 'label': 'HL性能快照',      'description': 'HL交易员性能快照',            'is_binance': False, 'category': 'Hyperliquid'},
            {'name': 'hyperliquid_leaderboard_history',   'label': 'HL排行榜历史',    'description': 'HL排行榜历史记录',            'is_binance': False, 'category': 'Hyperliquid'},
            {'name': 'hyperliquid_symbol_aggregation',    'label': 'HL币种聚合',      'description': 'HL各币种聚合统计',            'is_binance': False, 'category': 'Hyperliquid'},
            # ---- 市场分析 ----
            {'name': 'market_regime',                     'label': '市场状态',        'description': '市场行情状态（牛/熊/震荡）',  'is_binance': False, 'category': '市场分析'},
            {'name': 'market_regime_changes',             'label': '市场状态变化',    'description': '市场状态切换记录',            'is_binance': False, 'category': '市场分析'},
            {'name': 'market_observations',               'label': '市场观察',        'description': '市场观察记录',                'is_binance': False, 'category': '市场分析'},
            {'name': 'news_data',                         'label': '新闻数据',        'description': '加密货币新闻',                'is_binance': False, 'category': '市场分析'},
            {'name': 'news_sentiment_aggregation',        'label': '新闻情绪聚合',    'description': '新闻情绪汇总统计',            'is_binance': False, 'category': '市场分析'},
            {'name': 'range_market_zones',                'label': '震荡区间',        'description': '震荡行情区间识别',            'is_binance': False, 'category': '市场分析'},
            {'name': 'range_trading_history',             'label': '震荡交易历史',    'description': '震荡策略历史记录',            'is_binance': False, 'category': '市场分析'},
            # ---- 智能追踪 ----
            {'name': 'smart_money_addresses',             'label': '聪明钱地址',      'description': '聪明钱钱包地址库',            'is_binance': False, 'category': '智能追踪'},
            {'name': 'smart_money_signals',               'label': '聪明钱信号',      'description': '聪明钱交易信号',              'is_binance': False, 'category': '智能追踪'},
            {'name': 'smart_money_transactions',          'label': '聪明钱交易',      'description': '聪明钱链上交易记录',          'is_binance': False, 'category': '智能追踪'},
            # ---- 策略管理 ----
            {'name': 'strategy_capital_management',       'label': '策略资金管理',    'description': '策略资金分配管理',            'is_binance': False, 'category': '策略管理'},
            {'name': 'strategy_execution_results',        'label': '策略执行结果',    'description': '策略执行汇总结果',            'is_binance': False, 'category': '策略管理'},
            {'name': 'strategy_execution_result_details', 'label': '策略执行明细',    'description': '策略执行逐笔明细',            'is_binance': False, 'category': '策略管理'},
            {'name': 'strategy_hits',                     'label': '策略命中',        'description': '策略信号命中记录',            'is_binance': False, 'category': '策略管理'},
            {'name': 'strategy_regime_params',            'label': '策略市场参数',    'description': '按市场状态的策略参数',        'is_binance': False, 'category': '策略管理'},
            {'name': 'strategy_test_records',             'label': '策略测试记录',    'description': '策略回测记录',                'is_binance': False, 'category': '策略管理'},
            {'name': 'strategy_test_results',             'label': '策略测试结果',    'description': '策略回测结果汇总',            'is_binance': False, 'category': '策略管理'},
            {'name': 'strategy_trade_records',            'label': '策略交易记录',    'description': '策略产生的交易记录',          'is_binance': False, 'category': '策略管理'},
            {'name': 'trading_strategies',                'label': '交易策略配置',    'description': '策略配置表',                  'is_binance': False, 'category': '策略管理'},
            # ---- 系统配置 ----
            {'name': 'adaptive_params',                   'label': '自适应参数',      'description': '策略自适应参数',              'is_binance': False, 'category': '系统配置'},
            {'name': 'symbol_volatility_profile',         'label': '波动率配置',      'description': '币种波动率画像',              'is_binance': False, 'category': '系统配置'},
            {'name': 'symbol_risk_params',                'label': '币种风控参数',    'description': '每个币种的风控阈值',          'is_binance': False, 'category': '系统配置'},
            {'name': 'parameter_adjustments',             'label': '参数调整',        'description': '系统参数调整历史',            'is_binance': False, 'category': '系统配置'},
            {'name': 'trading_control',                   'label': '交易控制',        'description': '交易开关控制',                'is_binance': False, 'category': '系统配置'},
            {'name': 'trading_cooldowns',                 'label': '交易冷却',        'description': '币种交易冷却时间',            'is_binance': False, 'category': '系统配置'},
            {'name': 'trading_mode_config',               'label': '交易模式配置',    'description': '交易模式相关配置',            'is_binance': False, 'category': '系统配置'},
            {'name': 'trading_mode_switch_log',           'label': '交易模式切换',    'description': '交易模式切换日志',            'is_binance': False, 'category': '系统配置'},
            # ---- 系统管理 ----
            {'name': 'users',                             'label': '用户表',          'description': '系统用户信息',                'is_binance': False, 'category': '系统管理'},
            {'name': 'login_logs',                        'label': '登录日志',        'description': '用户登录记录',                'is_binance': False, 'category': '系统管理'},
            {'name': 'refresh_tokens',                    'label': '刷新令牌',        'description': 'JWT刷新令牌',                 'is_binance': False, 'category': '系统管理'},
            {'name': 'system_status',                     'label': '系统状态',        'description': '系统运行状态记录',            'is_binance': False, 'category': '系统管理'},
            {'name': 'optimization_history',              'label': '优化历史',        'description': '参数优化历史',                'is_binance': False, 'category': '系统管理'},
            {'name': 'optimization_logs',                 'label': '优化日志',        'description': '参数优化过程日志',            'is_binance': False, 'category': '系统管理'},
        ]

        with DBConnection() as conn:
            cursor = conn.cursor()

            # 一条 SELECT 读取缓存表（存储过程每5分钟刷新）
            cursor.execute("SELECT table_name, row_count, size_mb, latest_time, oldest_time FROM data_management_stats_cache")
            cache_rows = {row['table_name']: row for row in cursor.fetchall()}

        def _fmt_time(dt):
            if dt is None:
                return None
            s = dt.isoformat() if hasattr(dt, 'isoformat') else str(dt)
            if not s.endswith(('+', '-', 'Z')) and 'T' in s:
                s += '+08:00'
            elif 'T' not in s and s:
                s = s + 'T00:00:00+08:00'
            return s

        statistics = []
        for table in tables:
            name = table['name']
            cached = cache_rows.get(name)
            if cached:
                statistics.append({
                    **table,
                    'exists': True,
                    'count': int(cached['row_count'] or 0),
                    'count_approx': name in large_tables,
                    'latest_time': _fmt_time(cached['latest_time']),
                    'oldest_time': _fmt_time(cached['oldest_time']),
                    'size_mb': float(cached['size_mb'] or 0),
                })
            else:
                statistics.append({
                    **table,
                    'exists': False,
                    'count': 0,
                    'latest_time': None,
                    'oldest_time': None,
                    'size_mb': 0,
                })

        total_count = sum(s['count'] for s in statistics)
        total_size  = sum(s['size_mb'] for s in statistics)

        result = {
            'success': True,
            'data': {
                'tables': statistics,
                'summary': {
                    'total_tables': len(statistics),
                    'total_records': total_count,
                    'total_size_mb': round(total_size, 2)
                }
            }
        }

        with _statistics_cache_lock:
            _statistics_cache = result
            _statistics_cache_time = datetime.now()
            logger.debug("数据统计已写入内存缓存")

        return result

    except Exception as e:
        logger.error(f"获取数据统计失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取数据统计失败: {str(e)}")


@router.get("/table/{table_name}/sample")
async def get_table_sample(table_name: str, limit: int = 10):
    """
    获取指定表的样本数据
    
    Args:
        table_name: 表名
        limit: 返回记录数限制
    """
    try:
        with DBConnection() as conn:
            cursor = conn.cursor()
            
            # 安全检查：只允许查询白名单中的表
            allowed_tables = [
                # 市场数据
                'price_data', 'kline_data', 'orderbook_data', 'trade_data',
                # 合约数据
                'funding_rate_data', 'futures_open_interest', 'futures_long_short_ratio', 'futures_liquidations',
                # U本位合约
                'futures_positions', 'futures_orders', 'futures_trades', 'futures_trading_accounts',
                # 实盘合约
                'live_futures_positions', 'live_futures_orders', 'live_futures_trades', 'live_trading_accounts', 'live_trading_logs',
                # 现货交易
                'paper_trading_positions', 'paper_trading_orders', 'paper_trading_trades', 'paper_trading_accounts',
                # 信号分析
                'ema_signals', 'signal_blacklist', 'signal_component_performance', 'investment_recommendations', 'trading_symbol_rating',
                # ETF数据
                'crypto_etf_flows', 'crypto_etf_products', 'crypto_etf_events', 'crypto_etf_daily_summary',
                # 企业金库
                'corporate_treasury_companies', 'corporate_treasury_purchases', 'corporate_treasury_financing', 'corporate_treasury_summary',
                # Gas数据
                'blockchain_gas_daily', 'blockchain_gas_daily_summary',
                # Hyperliquid
                'hyperliquid_traders', 'hyperliquid_wallet_positions', 'hyperliquid_wallet_trades', 'hyperliquid_monthly_performance',
                # 市场分析
                'market_regime', 'market_observations', 'news_data',
                # 系统配置
                'adaptive_params', 'trading_symbol_rating', 'symbol_volatility_profile', 'users'
        ]
        
        if table_name not in allowed_tables:
            raise HTTPException(status_code=400, detail=f"不允许查询表: {table_name}")
        
        # 检查表是否存在
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail=f"表 {table_name} 不存在")
        
        # 获取样本数据（尝试不同的排序字段）
        order_fields = ['id', 'timestamp', 'created_at', 'updated_at', 'open_time', 'trade_time', 'date']
        rows = []
        
        for field in order_fields:
            try:
                cursor.execute(f"SELECT * FROM {table_name} ORDER BY {field} DESC LIMIT %s", (limit,))
                rows = cursor.fetchall()
                if rows:
                    break
            except:
                continue
        
        # 如果所有排序字段都失败，直接查询
        if not rows:
            cursor.execute(f"SELECT * FROM {table_name} LIMIT %s", (limit,))
            rows = cursor.fetchall()
        
        # 转换数据格式
        sample_data = []
        for row in rows:
            row_dict = {}
            for key, value in row.items():
                if isinstance(value, datetime):
                    row_dict[key] = value.isoformat()
                elif isinstance(value, (int, float)):
                    row_dict[key] = value
                else:
                    row_dict[key] = str(value) if value is not None else None
            sample_data.append(row_dict)
            
            return {
                'success': True,
                'table_name': table_name,
                'count': len(sample_data),
                'data': sample_data
            }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取表 {table_name} 样本数据失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取样本数据失败: {str(e)}")


@router.delete("/table/{table_name}/cleanup")
async def cleanup_old_data(
    table_name: str,
    days: int = 30,
    confirm: bool = False
):
    """
    清理指定表的旧数据
    
    Args:
        table_name: 表名
        days: 保留最近N天的数据
        confirm: 确认删除（必须为True才能执行）
    """
    if not confirm:
        raise HTTPException(status_code=400, detail="必须设置 confirm=true 才能执行删除操作")
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 安全检查：只允许清理白名单中的表
        allowed_tables = [
            'price_data', 'kline_data', 'news_data', 'funding_rate_data',
            'futures_open_interest', 'futures_long_short_ratio',
            'smart_money_transactions', 'ema_signals'
        ]
        
        if table_name not in allowed_tables:
            raise HTTPException(status_code=400, detail=f"不允许清理表: {table_name}")
        
        # 检查表是否存在
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail=f"表 {table_name} 不存在")
        
        # 获取删除前的记录数
        cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
        before_count = cursor.fetchone()['count']
        
        # 尝试不同的时间字段
        time_fields = ['timestamp', 'created_at', 'updated_at', 'open_time', 'trade_time', 'date']
        deleted_count = 0
        
        for field in time_fields:
            try:
                cursor.execute(f"SELECT COUNT(*) as count FROM {table_name} WHERE {field} < DATE_SUB(NOW(), INTERVAL %s DAY)", (days,))
                old_count = cursor.fetchone()['count']
                
                if old_count > 0:
                    cursor.execute(f"DELETE FROM {table_name} WHERE {field} < DATE_SUB(NOW(), INTERVAL %s DAY)", (days,))
                    deleted_count = cursor.rowcount
                    conn.commit()
                    break
            except:
                continue
        
        # 获取删除后的记录数
        cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
        after_count = cursor.fetchone()['count']
        
        cursor.close()
        conn.close()
        
        return {
            'success': True,
            'table_name': table_name,
            'before_count': before_count,
            'deleted_count': deleted_count,
            'after_count': after_count,
            'message': f'成功清理 {deleted_count} 条超过 {days} 天的旧数据'
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"清理表 {table_name} 数据失败: {e}")
        if conn:
            conn.rollback()
        raise HTTPException(status_code=500, detail=f"清理数据失败: {str(e)}")


def _execute_query_with_retry(conn, query, params=None, retry_count=2):
    """执行查询，带重试机制"""
    for attempt in range(retry_count):
        try:
            # 检查连接有效性
            if not conn.open:
                raise pymysql.err.InterfaceError("Connection is closed")
            
            # 尝试ping以验证连接
            conn.ping(reconnect=False)
            
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            result = cursor.fetchall()
            cursor.close()
            return result
        except (pymysql.err.InterfaceError, pymysql.err.OperationalError) as e:
            if attempt < retry_count - 1:
                logger.warning(f"⚠️ 数据库连接失效，尝试重新连接（{attempt + 1}/{retry_count}）: {e}")
                # 关闭旧连接
                try:
                    conn.close()
                except:
                    pass
                # 重新获取连接
                conn = get_db_connection()
            else:
                raise
    return None


def _ensure_connection(conn):
    """确保连接有效，如果失效则重新获取"""
    try:
        if conn and conn.open:
            conn.ping(reconnect=False)
            return conn
    except:
        pass
    # 连接失效，重新获取
    try:
        if conn:
            conn.close()
    except:
        pass
    return get_db_connection()


@router.get("/collection-status")
async def get_collection_status():
    """
    获取各类数据的采集情况 — 直接读取 collection_status_cache 缓存表（存储过程每5分钟刷新）
    """
    global _collection_status_cache, _collection_status_cache_time

    # 30秒内命中缓存直接返回（防并发）
    with _collection_status_cache_lock:
        if _collection_status_cache is not None and _collection_status_cache_time is not None:
            cache_age = (datetime.now() - _collection_status_cache_time).total_seconds()
            if cache_age < COLLECTION_STATUS_CACHE_TTL:
                return _collection_status_cache

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM collection_status_cache")
        rows = {r['type_key']: r for r in cursor.fetchall()}
        cursor.close()
        return_db_connection(conn)
        conn = None

        collection_status = []

        # 1. 实时价格数据
        r = rows.get('price', {})
        collection_status.append({
            'type': '实时价格数据',
            'category': 'market_data',
            'icon': 'bi-graph-up',
            'description': '各交易所的实时价格数据',
            'count': r.get('total_count', 0),
            'latest_time': _iso(r.get('latest_time')),
            'oldest_time': _iso(r.get('oldest_time')),
            'symbol_count': r.get('symbol_count', 0),
            'exchange_count': r.get('exchange_count', 0),
            'status': _check_status_active(r['latest_time'], 600) if r.get('latest_time') else 'inactive'
        })

        # 2. K线数据（多时间周期状态判断）
        r = rows.get('kline', {})
        status = 'inactive'
        kline_thresholds = [
            ('kline_latest_1m',  600),
            ('kline_latest_5m',  1800),
            ('kline_latest_15m', 3600),
            ('kline_latest_1h',  10800),
            ('kline_latest_1d',  172800),
        ]
        for field, threshold in kline_thresholds:
            t = r.get(field)
            if t:
                check = _check_status_active(t, threshold)
                if check == 'active':
                    status = 'active'
                    break
                elif check == 'warning' and status == 'inactive':
                    status = 'warning'
        if status == 'inactive' and r.get('latest_time'):
            status = _check_status_active(r['latest_time'], 1800)
        collection_status.append({
            'type': 'K线数据',
            'category': 'market_data',
            'icon': 'bi-bar-chart',
            'description': '不同时间周期的K线数据',
            'count': r.get('total_count', 0),
            'latest_time': _iso(r.get('latest_time')),
            'oldest_time': _iso(r.get('oldest_time')),
            'symbol_count': r.get('symbol_count', 0),
            'timeframe_count': r.get('timeframe_count', 0),
            'status': status
        })

        # 3. 合约数据
        r = rows.get('futures', {})
        collection_status.append({
            'type': '合约数据',
            'category': 'futures_data',
            'icon': 'bi-graph-up-arrow',
            'description': '合约持仓量、资金费率、多空比等数据',
            'count': r.get('total_count', 0),
            'latest_time': _iso(r.get('latest_time')),
            'oldest_time': _iso(r.get('oldest_time')),
            'status': _check_status_active(r['latest_time'], 1200) if r.get('latest_time') else 'inactive'
        })

        # 4. 新闻数据
        r = rows.get('news', {})
        collection_status.append({
            'type': '新闻数据',
            'category': 'news_data',
            'icon': 'bi-newspaper',
            'description': '加密货币相关新闻',
            'count': r.get('total_count', 0),
            'latest_time': _iso(r.get('latest_time')),
            'oldest_time': _iso(r.get('oldest_time')),
            'source_count': r.get('source_count', 0),
            'status': _check_status_active(r['latest_time'], 3600) if r.get('latest_time') else 'inactive'
        })

        # 5. ETF数据（手动导入，30天阈值）
        r = rows.get('etf', {})
        etf_status = 'inactive'
        if r.get('total_count', 0) > 0:
            if r.get('latest_time'):
                time_diff = (datetime.now() - r['latest_time']).total_seconds()
                etf_status = 'active' if time_diff < 2592000 else 'warning'
            else:
                etf_status = 'active'
        collection_status.append({
            'type': 'ETF数据',
            'category': 'etf_data',
            'icon': 'bi-pie-chart',
            'description': '加密货币ETF资金流向数据（手动导入）',
            'count': r.get('total_count', 0),
            'latest_time': _iso(r.get('latest_time')),
            'oldest_time': _iso(r.get('oldest_time')),
            'etf_count': r.get('etf_count', 0),
            'status': etf_status
        })

        # 6. 企业金库数据（手动导入，30天阈值）
        r = rows.get('treasury', {})
        treasury_status = 'inactive'
        if r.get('total_count', 0) > 0:
            if r.get('latest_time'):
                time_diff = (datetime.now() - r['latest_time']).total_seconds()
                treasury_status = 'active' if time_diff < 2592000 else 'warning'
            else:
                treasury_status = 'active'
        collection_status.append({
            'type': '企业金库数据',
            'category': 'treasury_data',
            'icon': 'bi-building',
            'description': '企业持仓和融资记录数据（手动导入）',
            'count': r.get('total_count', 0),
            'latest_time': _iso(r.get('latest_time')),
            'oldest_time': _iso(r.get('oldest_time')),
            'company_count': r.get('company_count', 0),
            'status': treasury_status
        })

        # 7. Hyperliquid聪明钱
        r = rows.get('hyperliquid', {})
        collection_status.append({
            'type': 'Hyperliquid聪明钱',
            'category': 'smart_money',
            'icon': 'bi-lightning-charge',
            'description': 'Hyperliquid平台聪明钱交易数据',
            'count': r.get('total_count', 0),
            'latest_time': _iso(r.get('latest_time')),
            'oldest_time': _iso(r.get('oldest_time')),
            'wallet_count': r.get('wallet_count', 0),
            'trader_count': r.get('trader_count', 0),
            'monitored_count': r.get('monitored_count', 0),
            'coin_count': r.get('coin_count', 0),
            'status': _check_status_active(r['latest_time'], 86400) if r.get('latest_time') else 'inactive'
        })

        # 8. 链上聪明钱
        r = rows.get('smart_money', {})
        collection_status.append({
            'type': '链上聪明钱',
            'category': 'smart_money',
            'icon': 'bi-wallet2',
            'description': '链上聪明钱交易和信号数据',
            'count': r.get('total_count', 0),
            'latest_time': _iso(r.get('latest_time')),
            'oldest_time': _iso(r.get('oldest_time')),
            'wallet_count': r.get('wallet_count', 0),
            'address_count': r.get('address_count', 0),
            'token_count': r.get('token_count', 0),
            'blockchain_count': r.get('blockchain_count', 0),
            'signal_count': r.get('signal_count', 0),
            'latest_signal_time': _iso(r.get('latest_signal_time')),
            'status': _check_status_active(r['latest_time'], 86400) if r.get('latest_time') else 'inactive'
        })

        result = {'success': True, 'data': collection_status}

        with _collection_status_cache_lock:
            _collection_status_cache = result
            _collection_status_cache_time = datetime.now()

        return result

    except Exception as e:
        logger.error(f"获取数据采集情况失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取数据采集情况失败: {str(e)}")
    finally:
        if conn:
            return_db_connection(conn)


def _parse_date(date_str: str):
    """解析日期字符串，支持多种格式"""
    if not date_str:
        return None
    try:
        # 尝试多种日期格式
        for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d']:
            try:
                return datetime.strptime(str(date_str).strip(), fmt).date()
            except:
                continue
        return None
    except:
        return None

def _parse_number(num_str: str):
    """解析数字字符串，支持逗号、货币符号等"""
    if not num_str:
        return None
    try:
        # 移除逗号、货币符号、空格
        cleaned = str(num_str).replace(',', '').replace('$', '').replace(' ', '').strip()
        # 处理括号表示负数的情况
        is_negative = False
        if cleaned.startswith('(') and cleaned.endswith(')'):
            is_negative = True
            cleaned = cleaned[1:-1]
        if cleaned:
            value = float(cleaned)
            return -value if is_negative else value
        return None
    except:
        return None


@router.post("/import/etf")
async def import_etf_data(
    file: UploadFile = File(...),
    asset_type: str = Form("BTC")
):
    """
    导入ETF数据文件（CSV格式）
    
    支持多种字段名格式：
    - Date/date, Ticker/ticker
    - NetInflow/net_inflow, GrossInflow/gross_inflow, GrossOutflow/gross_outflow
    - AUM/aum, BTC_Holdings/BTCHoldings/btc_holdings, ETH_Holdings/ETHHoldings/eth_holdings
    - NAV/nav, Close/close_price, Volume/volume
    """
    try:
        if not file.filename.endswith('.csv'):
            raise HTTPException(status_code=400, detail="只支持CSV文件")
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 读取文件内容
        content = await file.read()
        text_content = content.decode('utf-8-sig')  # 处理BOM
        reader = csv.DictReader(io.StringIO(text_content))
        
        imported = 0
        errors = []
        
        for row_num, row in enumerate(reader, start=2):  # 从第2行开始（第1行是表头）
            try:
                # 支持多种字段名格式
                trade_date_str = row.get('Date') or row.get('date') or row.get('trade_date') or row.get('TradeDate')
                ticker = (row.get('Ticker') or row.get('ticker') or row.get('TICKER')).strip().upper() if (row.get('Ticker') or row.get('ticker') or row.get('TICKER')) else None
                
                if not ticker or not trade_date_str:
                    errors.append(f"第{row_num}行: 缺少必要字段 (Ticker/Date)")
                    continue
                
                # 解析日期（支持多种格式）
                trade_date = _parse_date(trade_date_str)
                if not trade_date:
                    errors.append(f"第{row_num}行: 日期格式错误 (支持 YYYY-MM-DD, MM/DD/YYYY 等)")
                    continue
                
                # 查找ETF产品（获取ID和资产类型）
                cursor.execute("SELECT id, asset_type FROM crypto_etf_products WHERE ticker = %s", (ticker,))
                etf_result = cursor.fetchone()
                
                if not etf_result:
                    # 尝试查找所有ETH ETF，提供更详细的错误信息
                    if asset_type.upper() == 'ETH':
                        cursor.execute("SELECT ticker FROM crypto_etf_products WHERE asset_type = 'ETH'")
                        eth_tickers = [r['ticker'] for r in cursor.fetchall()]
                        errors.append(f"第{row_num}行: 未找到ETF产品 '{ticker}'。可用的ETH ETF: {', '.join(eth_tickers)}")
                    else:
                        errors.append(f"第{row_num}行: 未找到ETF产品 '{ticker}'，请先在系统中添加该ETF")
                    logger.warning(f"ETF导入: 第{row_num}行，未找到ticker '{ticker}' (资产类型: {asset_type})")
                    continue
                
                etf_id = etf_result['id']
                db_asset_type = etf_result['asset_type']
                
                # 验证资产类型是否匹配
                if asset_type.upper() != db_asset_type.upper():
                    errors.append(f"第{row_num}行: ETF '{ticker}' 的资产类型是 {db_asset_type}，但导入时选择的是 {asset_type}")
                    logger.warning(f"ETF导入: 第{row_num}行，资产类型不匹配 - ticker: {ticker}, 数据库: {db_asset_type}, 表单: {asset_type}")
                    continue
                
                # 解析数值字段（支持多种字段名）
                net_inflow = _parse_number(row.get('NetInflow') or row.get('net_inflow') or row.get('Net_Inflow'))
                gross_inflow = _parse_number(row.get('GrossInflow') or row.get('gross_inflow') or row.get('Gross_Inflow'))
                gross_outflow = _parse_number(row.get('GrossOutflow') or row.get('gross_outflow') or row.get('Gross_Outflow'))
                aum = _parse_number(row.get('AUM') or row.get('aum'))
                
                # 解析持仓量（根据ETF资产类型）
                btc_holdings = None
                eth_holdings = None
                if db_asset_type == 'BTC':
                    btc_holdings = _parse_number(
                        row.get('BTC_Holdings') or row.get('BTCHoldings') or 
                        row.get('btc_holdings') or row.get('Holdings') or row.get('holdings')
                    )
                elif db_asset_type == 'ETH':
                    eth_holdings = _parse_number(
                        row.get('ETH_Holdings') or row.get('ETHHoldings') or 
                        row.get('eth_holdings') or row.get('Holdings') or row.get('holdings')
                    )
                
                nav = _parse_number(row.get('NAV') or row.get('nav'))
                close_price = _parse_number(row.get('Close') or row.get('close_price') or row.get('ClosePrice'))
                volume = _parse_number(row.get('Volume') or row.get('volume'))
                data_source = (row.get('data_source') or row.get('DataSource') or 'manual').strip()
                
                # 插入或更新数据
                cursor.execute("""
                    INSERT INTO crypto_etf_flows
                    (etf_id, ticker, trade_date, net_inflow, gross_inflow, gross_outflow,
                     aum, btc_holdings, eth_holdings, nav, close_price, volume, data_source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        net_inflow = VALUES(net_inflow),
                        gross_inflow = VALUES(gross_inflow),
                        gross_outflow = VALUES(gross_outflow),
                        aum = VALUES(aum),
                        btc_holdings = VALUES(btc_holdings),
                        eth_holdings = VALUES(eth_holdings),
                        nav = VALUES(nav),
                        close_price = VALUES(close_price),
                        volume = VALUES(volume),
                        data_source = VALUES(data_source),
                        updated_at = CURRENT_TIMESTAMP
                """, (
                    etf_id, ticker, trade_date, net_inflow, gross_inflow, gross_outflow,
                    aum, btc_holdings, eth_holdings, nav, close_price, volume, data_source
                ))
                
                imported += 1
                
            except Exception as e:
                error_msg = f"第{row_num}行: {str(e)}"
                errors.append(error_msg)
                logger.error(f"导入ETF数据第{row_num}行失败: {e}")
                logger.error(f"  行数据: {row}")
                import traceback
                logger.error(traceback.format_exc())
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # 如果没有任何导入成功，且没有错误信息，可能是文件格式问题
        if imported == 0 and len(errors) == 0:
            errors.append("文件格式可能不正确，请检查CSV文件是否包含正确的列（Date, Ticker等）")
        
        return {
            'success': imported > 0,  # 只有成功导入至少一条记录才算成功
            'imported': imported,
            'errors': errors,
            'error_count': len(errors),
            'message': f'成功导入 {imported} 条记录，失败 {len(errors)} 条' if imported > 0 or len(errors) > 0 else '未导入任何记录，请检查文件格式'
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"导入ETF数据失败: {e}")
        raise HTTPException(status_code=500, detail=f"导入ETF数据失败: {str(e)}")


def parse_bitcoin_treasuries_format(text: str):
    """
    解析 Bitcoin Treasuries 网站的复制格式
    
    参考 scripts/corporate_treasury/batch_import.py 的 parse_bitcoin_treasuries_format 函数
    
    示例格式：
    1
    Strategy
    🇺🇸	MSTR	640,808
    
    返回：[(公司名, 股票代码, BTC数量), ...]
    """
    companies = []
    lines = text.strip().split('\n')
    
    logger.debug(f"开始解析，共 {len(lines)} 行")
    
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        
        # 跳过注释行和空行
        if line.startswith('#') or not line:
            i += 1
            continue
        
        # 跳过排名数字
        if line.isdigit():
            i += 1
            continue
        
        # 如果是公司名（不包含制表符，且不是国旗行）
        # 注意：需要排除以各种国旗开头的行
        if '\t' not in line and line:
            # 检查是否是国旗行（可能包含各种国旗emoji）
            is_flag_line = False
            for flag in ['🇺🇸', '🇯🇵', '🇨🇦', '🇬🇧', '🇩🇪', '🇫🇷', '🇦🇺', '🇨🇭', '🇸🇬', '🇰🇷']:
                if line.startswith(flag):
                    is_flag_line = True
                    break
            
            if not is_flag_line:
                company_name = line
                
                # 下一行应该是国旗、股票代码和数量
                if i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    
                    # 解析格式：🇺🇸	MSTR	640,808
                    parts = next_line.split('\t')
                    
                    if len(parts) >= 3:
                        ticker = parts[1].strip()
                        btc_amount_str = parts[2].strip().replace(',', '')
                        
                        try:
                            btc_amount = float(btc_amount_str)
                            companies.append((company_name, ticker, btc_amount))
                            logger.debug(f"解析成功: {company_name} ({ticker}) - {btc_amount:,.0f}")
                        except ValueError as e:
                            logger.warning(f"跳过无效数量: {company_name} - {parts[2]} (错误: {e})")
                    else:
                        logger.warning(f"跳过格式不正确的行: {company_name} 的下一行格式错误: {next_line}")
                    
                    i += 2  # 跳过下一行
                    continue
        
        i += 1
    
    logger.info(f"解析完成，共解析到 {len(companies)} 家公司")
    return companies


@router.post("/import/corporate-treasury")
async def import_corporate_treasury_data(
    file: UploadFile = File(...),
    asset_type: str = Form("BTC"),
    data_date: str = Form(None)
):
    """
    导入企业金库数据文件
    
    支持两种格式：
    1. 文本格式（.txt）：Bitcoin Treasuries 网站格式，导入持仓数据到 corporate_treasury_purchases
    2. CSV格式（.csv）：融资数据，导入到 corporate_treasury_financing
    """
    try:
        conn = get_db_connection()
        # 使用字典游标，与 get_db_connection() 返回的 DictCursor 一致
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        
        # 读取文件内容
        content = await file.read()
        text_content = content.decode('utf-8')
        
        # 判断文件类型
        is_text_format = file.filename.endswith('.txt')
        
        imported = 0
        updated = 0
        skipped = 0
        errors = []
        
        if is_text_format:
            # 文本格式：Bitcoin Treasuries 格式，导入持仓数据
            # 解析数据日期
            purchase_date = None
            if data_date:
                try:
                    purchase_date = datetime.strptime(data_date, '%Y-%m-%d').date()
                except:
                    raise HTTPException(status_code=400, detail="数据日期格式错误 (应为 YYYY-MM-DD)")
            else:
                purchase_date = datetime.now().date()
            
            # 解析文本格式
            companies = parse_bitcoin_treasuries_format(text_content)
            
            logger.info(f"解析到 {len(companies)} 家公司")
            if companies:
                logger.info(f"前3家公司: {companies[:3]}")
            
            if not companies:
                raise HTTPException(status_code=400, detail="无法解析文本格式，请检查文件格式是否正确。确保文件格式为：排名数字、公司名、国旗+股票代码+持仓量（用制表符分隔）")
            
            # 导入持仓数据（与 bitcointreasuries.net 自动同步共用同一写入逻辑）
            from app.services.corporate_treasury_holdings import upsert_corporate_holdings_batch

            batch = upsert_corporate_holdings_batch(
                cursor, companies, purchase_date, asset_type, "manual"
            )
            imported = batch["imported"]
            updated = batch["updated"]
            skipped = batch["skipped"]
            errors.extend(batch["errors"])
            inserted_only = imported - updated

            # 构建详细的消息（imported = 成功写入条数，含新增+更新）
            total_processed = imported + skipped
            message_parts = []
            if inserted_only > 0:
                message_parts.append(f"新增 {inserted_only} 条")
            if updated > 0:
                message_parts.append(f"更新 {updated} 条")
            if skipped > 0:
                message_parts.append(f"跳过 {skipped} 条（已存在且持仓量相同）")
            if errors:
                message_parts.append(f"失败 {len(errors)} 条")
            
            message = f"共处理 {total_processed} 条记录：" + "，".join(message_parts) if message_parts else f"共处理 {total_processed} 条记录"
            
        else:
            # CSV格式：融资数据
            if not file.filename.endswith('.csv'):
                raise HTTPException(status_code=400, detail="只支持 .txt 或 .csv 文件")
            
            # 解析数据日期
            financing_date = None
            if data_date:
                try:
                    financing_date = datetime.strptime(data_date, '%Y-%m-%d').date()
                except:
                    raise HTTPException(status_code=400, detail="数据日期格式错误 (应为 YYYY-MM-DD)")
            else:
                financing_date = datetime.now().date()
            
            # 读取CSV内容
            text_content_bom = content.decode('utf-8-sig')  # 处理BOM
            reader = csv.DictReader(io.StringIO(text_content_bom))
            
            for row_num, row in enumerate(reader, start=2):
                try:
                    company_name = row.get('company_name', '').strip()
                    ticker = (row.get('ticker') or row.get('ticker_symbol') or '').strip().upper()
                    
                    if not company_name:
                        errors.append(f"第{row_num}行: 缺少公司名称")
                        continue
                    
                    # 查找或创建公司
                    cursor.execute("""
                        SELECT id FROM corporate_treasury_companies
                        WHERE company_name = %s OR ticker_symbol = %s
                        LIMIT 1
                    """, (company_name, ticker))
                    company_result = cursor.fetchone()
                    
                    if not company_result:
                        cursor.execute("""
                            INSERT INTO corporate_treasury_companies
                            (company_name, ticker_symbol, category, is_active)
                            VALUES (%s, %s, %s, 1)
                        """, (company_name, ticker, 'holding'))
                        company_id = cursor.lastrowid
                    else:
                        company_id = company_result['id']
                    
                    # 解析融资日期
                    row_financing_date = financing_date
                    financing_date_str = row.get('financing_date', '').strip()
                    if financing_date_str:
                        parsed_date = _parse_date(financing_date_str)
                        if parsed_date:
                            row_financing_date = parsed_date
                    
                    # 解析数值字段
                    financing_type = (row.get('financing_type') or 'equity').strip() or 'equity'
                    amount = _parse_number(row.get('amount'))
                    purpose = row.get('purpose', '').strip() or None
                    announcement_url = row.get('announcement_url', '').strip() or None
                    notes = row.get('notes', '').strip() or None
                    data_source = (row.get('data_source') or 'manual').strip()
                    
                    # 插入融资记录
                    cursor.execute("""
                        INSERT INTO corporate_treasury_financing
                        (company_id, financing_date, financing_type, amount, purpose, announcement_url, notes, data_source)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (company_id, row_financing_date, financing_type, amount, purpose, announcement_url, notes, data_source))
                    
                    imported += 1
                    
                except Exception as e:
                    errors.append(f"第{row_num}行: {str(e)}")
                    logger.error(f"导入企业金库融资数据第{row_num}行失败: {e}")
            
            message = f'成功导入 {imported} 条融资记录，失败 {len(errors)} 条'
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {
            'success': True,
            'imported': imported,
            'updated': updated if is_text_format else 0,
            'skipped': skipped if is_text_format else 0,
            'errors': errors,
            'error_count': len(errors),
            'message': message
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"导入企业金库数据失败: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"导入企业金库数据失败: {str(e)}")


@router.get("/template/etf")
async def download_etf_template(asset_type: str = "BTC"):
    """
    下载ETF数据导入模板CSV文件
    
    支持BTC和ETH两种资产类型
    
    Args:
        asset_type: 资产类型，BTC 或 ETH，默认为 BTC
    
    参考格式：
    - BTC: Date, Ticker, NetInflow, BTC_Holdings
    - ETH: Date, Ticker, NetInflow, ETH_Holdings
    """
    try:
        asset_type = asset_type.upper()
        if asset_type not in ['BTC', 'ETH']:
            asset_type = 'BTC'
        
        # 创建模板内容
        # NetInflow 是美元金额（不是百万美元），Holdings 是持仓量
        from datetime import date, timedelta
        yesterday = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        if asset_type == 'BTC':
            # BTC ETF 模板
            holdings_column = "BTC_Holdings"
            template_content = f"Date,Ticker,NetInflow,{holdings_column}\n"
            # 常见的BTC ETF tickers
            btc_tickers = ['IBIT', 'FBTC', 'BITB', 'ARKB', 'BTCO', 'EZBC', 'BRRR', 'HODL', 'BTCW', 'GBTC', 'DEFI']
            for ticker in btc_tickers:
                template_content += f"{yesterday},{ticker},0,0\n"
            filename = "etf_btc_import_template.csv"
        else:
            # ETH ETF 模板
            holdings_column = "ETH_Holdings"
            template_content = f"Date,Ticker,NetInflow,{holdings_column}\n"
            # 常见的ETH ETF tickers（来自数据库schema）
            eth_tickers = ['ETHA', 'FETH', 'ETHW', 'ETHV', 'QETH', 'EZET', 'CETH', 'ETHE', 'ETH']
            for ticker in eth_tickers:
                template_content += f"{yesterday},{ticker},0,0\n"
            filename = "etf_eth_import_template.csv"
        
        # 创建临时文件
        template_path = Path(__file__).parent.parent.parent / "templates" / filename
        template_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(template_path, 'w', encoding='utf-8-sig', newline='') as f:
            f.write(template_content)
        
        return FileResponse(
            path=str(template_path),
            filename=filename,
            media_type="text/csv"
        )
        
    except Exception as e:
        logger.error(f"生成ETF模板失败: {e}")
        raise HTTPException(status_code=500, detail=f"生成ETF模板失败: {str(e)}")


@router.get("/template/corporate-treasury")
async def download_corporate_treasury_template():
    """
    下载企业金库持仓数据导入模板（文本格式）
    
    参考 scripts/corporate_treasury/import_template.txt
    格式：从 Bitcoin Treasuries 网站复制的格式
    """
    try:
        # 创建模板内容（参考 scripts/corporate_treasury/import_template.txt）
        template_content = """# 企业金库批量导入模板
# 从 Bitcoin Treasuries 网站复制的格式示例
# 使用方法：复制以下内容，或从 https://bitcointreasuries.net/ 复制最新数据

1
Strategy
🇺🇸	MSTR	640,808
2
MARA Holdings, Inc.
🇺🇸	MARA	53,250
3
XXI
🇺🇸	CEP	43,514
4
Metaplanet Inc.
🇯🇵	MTPLF	30,823
5
Bitcoin Standard Treasury Company
🇺🇸	CEPO	30,021
6
Riot Platforms, Inc.
🇺🇸	RIOT	19,287
7
Tesla, Inc.
🇺🇸	TSLA	11,509
8
Coinbase Global, Inc.
🇺🇸	COIN	11,776
9
Block, Inc.
🇺🇸	SQ	8,692
10
Galaxy Digital Holdings Ltd
🇺🇸	GLXY	6,894
"""
        
        # 创建临时文件
        template_path = Path(__file__).parent.parent.parent / "templates" / "corporate_treasury_import_template.txt"
        template_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(template_path, 'w', encoding='utf-8', newline='') as f:
            f.write(template_content)
        
        return FileResponse(
            path=str(template_path),
            filename="corporate_treasury_import_template.txt",
            media_type="text/plain"
        )
        
    except Exception as e:
        logger.error(f"生成企业金库模板失败: {e}")
        raise HTTPException(status_code=500, detail=f"生成企业金库模板失败: {str(e)}")


@router.get("/template/corporate-treasury-financing")
async def download_corporate_treasury_financing_template():
    """
    下载企业金库融资数据导入模板CSV文件
    
    用于导入 corporate_treasury_financing 表
    """
    try:
        # 创建模板内容（融资数据CSV格式）
        template_content = "company_name,ticker,financing_date,financing_type,amount,purpose,announcement_url,notes,data_source\n"
        template_content += "MicroStrategy,MSTR,2025-01-27,equity,1000000,购买BTC,https://example.com/announcement1,融资用于购买BTC,manual\n"
        template_content += "Tesla,TSLA,2025-01-27,convertible_note,500000,购买BTC,https://example.com/announcement2,可转换债券,manual\n"
        template_content += "Block,SQ,2025-01-27,loan,300000,购买BTC,https://example.com/announcement3,贷款融资,manual\n"
        template_content += "Coinbase,COIN,2025-01-27,atm,200000,购买BTC,https://example.com/announcement4,ATM融资,manual\n"
        
        # 创建临时文件
        template_path = Path(__file__).parent.parent.parent / "templates" / "corporate_treasury_financing_import_template.csv"
        template_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(template_path, 'w', encoding='utf-8-sig', newline='') as f:
            f.write(template_content)
        
        return FileResponse(
            path=str(template_path),
            filename="corporate_treasury_financing_import_template.csv",
            media_type="text/csv"
        )
        
    except Exception as e:
        logger.error(f"生成企业金库融资模板失败: {e}")
        raise HTTPException(status_code=500, detail=f"生成企业金库融资模板失败: {str(e)}")


async def _execute_collection_task(task_id: str, request_data: Dict):
    """
    后台执行数据采集任务
    
    Args:
        task_id: 任务ID
        request_data: 采集请求数据
    """
    try:
        task_manager.set_task_status(task_id, TaskStatus.RUNNING)

        symbols = request_data.get('symbols', [])
        data_type = request_data.get('data_type', 'price')
        start_time_str = request_data.get('start_time')
        end_time_str = request_data.get('end_time')
        timeframes = request_data.get('timeframes', None)
        if not timeframes:
            timeframe = request_data.get('timeframe', '1h')
            timeframes = [timeframe] if data_type == 'kline' else []
        collect_futures = request_data.get('collect_futures', False)
        # 合约数据的时间周期，默认使用现货的时间周期
        futures_timeframes = request_data.get('futures_timeframes', None)
        if collect_futures and not futures_timeframes:
            futures_timeframes = timeframes  # 默认使用现货的时间周期
        save_to_config = request_data.get('save_to_config', False)
        
        # 解析时间（与 /collect 端点相同的逻辑）
        # 前端发送的时间可能是本地时间（UTC+8）或UTC时间
        # 对于Binance数据采集，需要将本地时间转换为UTC时间
        try:
            if '+08:00' in start_time_str:
                # 本地时间（UTC+8），需要转换为UTC时间
                start_time = datetime.fromisoformat(start_time_str)
                start_time = start_time.replace(tzinfo=None) - timedelta(hours=8)
            elif 'Z' in start_time_str or '+00:00' in start_time_str:
                # UTC时间
                start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
                if start_time.tzinfo:
                    start_time = start_time.replace(tzinfo=None)
            else:
                # 没有时区信息，假设是本地时间（UTC+8）
                start_time = datetime.fromisoformat(start_time_str)
                start_time = start_time - timedelta(hours=8)
            
            if '+08:00' in end_time_str:
                # 本地时间（UTC+8），需要转换为UTC时间
                end_time = datetime.fromisoformat(end_time_str)
                end_time = end_time.replace(tzinfo=None) - timedelta(hours=8)
            elif 'Z' in end_time_str or '+00:00' in end_time_str:
                # UTC时间
                end_time = datetime.fromisoformat(end_time_str.replace('Z', '+00:00'))
                if end_time.tzinfo:
                    end_time = end_time.replace(tzinfo=None)
            else:
                # 没有时区信息，假设是本地时间（UTC+8）
                end_time = datetime.fromisoformat(end_time_str)
                end_time = end_time - timedelta(hours=8)
        except Exception as e:
            logger.error(f"解析时间失败: {e}")
            raise
        
        # 计算总步骤数和预估数据量
        collect_price = data_type in ['price', 'both']
        collect_kline = data_type in ['kline', 'both']
        total_steps = len(symbols) * (
            (1 if collect_price else 0) +
            (len(timeframes) if collect_kline else 0) +
            (len(futures_timeframes) if collect_futures and futures_timeframes else 0)
        )
        
        # 估算总数据量（用于更准确的进度计算）
        time_delta = end_time - start_time
        days = time_delta.total_seconds() / 86400
        estimated_total_records = 0
        
        # 注意：历史数据采集时，我们是从API获取K线数据，频率是固定的
        # price_interval 配置只影响实时采集，不影响历史数据采集
        # 历史价格数据使用1m K线，所以是每分钟1条
        
        if collect_price:
            # 价格数据：历史采集使用1m K线数据，每分钟1条
            estimated_total_records += len(symbols) * int(days * 24 * 60)
        
        if collect_kline:
            # K线数据：根据时间周期估算（K线数据频率是固定的）
            timeframe_minutes = {
                '1m': 1, '5m': 5, '15m': 15, '30m': 30,
                '1h': 60, '4h': 240, '1d': 1440, '1w': 10080
            }
            for tf in timeframes:
                minutes = timeframe_minutes.get(tf, 60)
                estimated_total_records += len(symbols) * int(days * 24 * 60 / minutes)
        
        if collect_futures and futures_timeframes:
            # 合约K线数据：根据合约时间周期估算
            for tf in futures_timeframes:
                minutes = timeframe_minutes.get(tf, 60)
                estimated_total_records += len(symbols) * int(days * 24 * 60 / minutes)
        
        task = task_manager.get_task(task_id)
        if task:
            task.total_steps = total_steps
            # 存储预估数据量，用于进度计算
            task.estimated_total_records = estimated_total_records
        
        # 更新初始状态
        task_manager.update_task_progress(
            task_id,
            current_step=f"准备采集 {len(symbols)} 个交易对，预估 {estimated_total_records:,} 条数据...",
            progress=0
        )
        
        # 导入采集器
        from app.collectors.price_collector import MultiExchangeCollector
        from app.collectors.binance_futures_collector import BinanceFuturesCollector
        from app.collectors.gate_collector import GateCollector
        
        # 加载配置（支持环境变量）
        from app.utils.config_loader import load_config
        config = load_config()

        # 初始化采集器
        collector = MultiExchangeCollector(config)
        
        # 初始化合约采集器
        binance_futures_collector = None
        gate_collector = None
        if collect_futures:
            try:
                binance_config = config.get('exchanges', {}).get('binance', {})
                binance_futures_collector = BinanceFuturesCollector(binance_config)
            except Exception as e:
                logger.warning(f"Binance合约数据采集器初始化失败: {e}")
        
        # 初始化Gate.io采集器（用于HYPE/USDT）
        try:
            gate_config = config.get('exchanges', {}).get('gate', {})
            if gate_config.get('enabled', False):
                gate_collector = GateCollector(gate_config)
        except Exception as e:
            logger.warning(f"Gate.io采集器初始化失败: {e}")
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        total_saved = 0
        errors = []
        completed_steps = 0
        
        # 遍历每个交易对
        for symbol_idx, symbol in enumerate(symbols):
            try:
                symbol = symbol.strip().upper()  # 统一转换为大写
                if not symbol:
                    continue
                
                # 确保格式正确（移除多余空格，统一格式）
                symbol = symbol.replace(' ', '').replace('_', '/')  # 支持 BTC_USDT 格式
                if '/' not in symbol and symbol.endswith('USDT'):
                    # 如果已经是 BTCUSDT 格式，转换为 BTC/USDT
                    base = symbol[:-4]  # 移除 USDT
                    symbol = f"{base}/USDT"
                
                # 判断是否使用Gate.io采集（仅HYPE/USDT）
                use_gate = (symbol.upper() == 'HYPE/USDT')
                
                task_manager.update_task_progress(
                    task_id,
                    current_step=f"正在采集 {symbol}...",
                    progress=(symbol_idx / len(symbols)) * 100
                )
                
                if collect_price:
                    task_manager.update_task_progress(
                        task_id,
                        current_step=f"正在从API获取 {symbol} 价格数据..."
                    )
                    
                    if use_gate and gate_collector:
                        # HYPE/USDT 从Gate.io采集
                        days = int((end_time - start_time).total_seconds() / 86400) + 1
                        since = int(start_time.timestamp())
                        df = await gate_collector.fetch_ohlcv(
                            symbol=symbol,
                            timeframe='1m',
                            limit=1000,
                            since=since
                        )
                        # 如果数据不够，需要分批获取
                        if df is not None and len(df) > 0:
                            all_data = [df]
                            last_timestamp = df['timestamp'].iloc[-1]
                            current_since = int(last_timestamp.timestamp()) + 1
                            while current_since < int(end_time.timestamp()):
                                next_df = await gate_collector.fetch_ohlcv(
                                    symbol=symbol,
                                    timeframe='1m',
                                    limit=1000,
                                    since=current_since
                                )
                                if next_df is None or len(next_df) == 0:
                                    break
                                all_data.append(next_df)
                                last_timestamp = next_df['timestamp'].iloc[-1]
                                current_since = int(last_timestamp.timestamp()) + 1
                                if len(next_df) < 1000:
                                    break
                                await asyncio.sleep(0.5)
                            if len(all_data) > 1:
                                df = pd.concat(all_data, ignore_index=True)
                                df = df.drop_duplicates(subset=['timestamp'])
                                df = df.sort_values('timestamp').reset_index(drop=True)
                    else:
                        # 其他交易对从Binance采集
                        df = await collector.fetch_historical_data(
                            symbol=symbol,
                            timeframe='1m',
                            days=int((end_time - start_time).total_seconds() / 86400) + 1,
                            exchange='binance' if not use_gate else None
                        )
                    
                    if df is not None and len(df) > 0:
                        task_manager.update_task_progress(
                            task_id,
                            current_step=f"✓ 已获取 {symbol} 价格数据 {len(df)} 条（原始），正在过滤..."
                        )
                    
                    if df is not None and len(df) > 0:
                        df = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]
                        task_manager.update_task_progress(
                            task_id,
                            current_step=f"✓ 过滤后剩余 {len(df)} 条数据，正在保存..."
                        )
                        
                        if len(df) == 0:
                            task_manager.update_task_progress(
                                task_id,
                                current_step=f"⚠️ {symbol}: 过滤后无数据，可能时间范围不匹配"
                            )
                            errors.append(f"{symbol}: 价格数据时间范围不匹配")
                            continue
                        
                        saved_count = 0
                        total_rows = len(df)
                        update_interval = max(1, total_rows // 20)  # 每5%更新一次进度
                        
                        for idx, row_tuple in enumerate(df.iterrows()):
                            try:
                                _, row = row_tuple
                                created_at = datetime.now()
                                cursor.execute("""
                                    INSERT INTO price_data
                                    (symbol, exchange, timestamp, price, open_price, high_price, low_price, close_price, volume, quote_volume, bid_price, ask_price, change_24h, created_at)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    ON DUPLICATE KEY UPDATE
                                        price = VALUES(price),
                                        open_price = VALUES(open_price),
                                        high_price = VALUES(high_price),
                                        low_price = VALUES(low_price),
                                        close_price = VALUES(close_price),
                                        volume = VALUES(volume),
                                        quote_volume = VALUES(quote_volume),
                                        bid_price = VALUES(bid_price),
                                        ask_price = VALUES(ask_price),
                                        change_24h = VALUES(change_24h),
                                        created_at = VALUES(created_at)
                                """, (
                                    symbol, 'gate' if use_gate else 'binance', row['timestamp'],
                                    float(row['close']), float(row['open']),
                                    float(row['high']), float(row['low']),
                                    float(row['close']), float(row['volume']),
                                    float(row.get('quote_volume', 0)), 0, 0, 0, created_at
                                ))
                                if cursor.rowcount > 0:
                                    saved_count += 1
                                
                                # 实时更新进度（每保存一定数量后更新）
                                if saved_count % update_interval == 0 or saved_count == total_rows:
                                    total_saved_temp = total_saved + saved_count
                                    task = task_manager.get_task(task_id)
                                    if task and task.estimated_total_records > 0:
                                        # 基于实际保存的数据量计算进度
                                        progress = min(95, (total_saved_temp / task.estimated_total_records) * 100)
                                    else:
                                        # 回退到基于步骤的进度计算
                                        progress = min(95, (completed_steps / total_steps) * 100) if total_steps > 0 else 0
                                    
                                    task_manager.update_task_progress(
                                        task_id,
                                        current_step=f"正在保存 {symbol} 价格数据 ({saved_count}/{total_rows})...",
                                        total_saved=total_saved_temp,
                                        progress=progress
                                    )
                            except Exception as e:
                                logger.error(f"保存价格数据失败: {e}")
                                continue
                        
                        total_saved += saved_count
                        completed_steps += 1
                        task_manager.update_task_progress(
                            task_id,
                            completed_steps=completed_steps,
                            total_saved=total_saved,
                            current_step=f"✓ {symbol} 价格数据采集完成，保存 {saved_count} 条"
                        )
                    else:
                        errors.append(f"{symbol}: 未获取到价格数据")
                
                if collect_kline:
                    if not timeframes:
                        timeframes = ['1m', '5m', '15m', '1h', '1d']
                    
                    symbol_saved = 0
                    for timeframe in timeframes:
                        try:
                            task_manager.update_task_progress(
                                task_id,
                                current_step=f"正在从API获取 {symbol} {timeframe} K线数据..."
                            )
                            
                            if use_gate and gate_collector:
                                # HYPE/USDT 从Gate.io采集
                                days = int((end_time - start_time).total_seconds() / 86400) + 1
                                since = int(start_time.timestamp())
                                df = await gate_collector.fetch_ohlcv(
                                    symbol=symbol,
                                    timeframe=timeframe,
                                    limit=1000,
                                    since=since
                                )
                                # 如果数据不够，需要分批获取
                                if df is not None and len(df) > 0:
                                    all_data = [df]
                                    last_timestamp = df['timestamp'].iloc[-1]
                                    current_since = int(last_timestamp.timestamp()) + 1
                                    while current_since < int(end_time.timestamp()):
                                        next_df = await gate_collector.fetch_ohlcv(
                                            symbol=symbol,
                                            timeframe=timeframe,
                                            limit=1000,
                                            since=current_since
                                        )
                                        if next_df is None or len(next_df) == 0:
                                            break
                                        all_data.append(next_df)
                                        last_timestamp = next_df['timestamp'].iloc[-1]
                                        current_since = int(last_timestamp.timestamp()) + 1
                                        if len(next_df) < 1000:
                                            break
                                        await asyncio.sleep(0.5)
                                    if len(all_data) > 1:
                                        df = pd.concat(all_data, ignore_index=True)
                                        df = df.drop_duplicates(subset=['timestamp'])
                                        df = df.sort_values('timestamp').reset_index(drop=True)
                            else:
                                # 其他交易对从Binance采集
                                df = await collector.fetch_historical_data(
                                    symbol=symbol,
                                    timeframe=timeframe,
                                    days=int((end_time - start_time).total_seconds() / 86400) + 1,
                                    exchange='binance' if not use_gate else None
                                )
                            
                            if df is not None and len(df) > 0:
                                task_manager.update_task_progress(
                                    task_id,
                                    current_step=f"✓ 已获取 {symbol} {timeframe} K线数据 {len(df)} 条（原始），正在过滤..."
                                )
                            
                            if df is not None and len(df) > 0:
                                df = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]
                                task_manager.update_task_progress(
                                    task_id,
                                    current_step=f"✓ 过滤后剩余 {len(df)} 条数据，正在保存..."
                                )
                                
                                if len(df) == 0:
                                    task_manager.update_task_progress(
                                        task_id,
                                        current_step=f"⚠️ {symbol} {timeframe}: 过滤后无数据，可能时间范围不匹配"
                                    )
                                    errors.append(f"{symbol} {timeframe}: K线数据时间范围不匹配")
                                    continue
                                
                                timeframe_saved = 0
                                total_rows = len(df)
                                update_interval = max(1, total_rows // 20)  # 每5%更新一次进度
                                
                                for idx, row_tuple in enumerate(df.iterrows()):
                                    try:
                                        _, row = row_tuple
                                        timestamp = row['timestamp']
                                        if isinstance(timestamp, pd.Timestamp):
                                            timestamp_dt = timestamp.to_pydatetime()
                                            open_time_ms = int(timestamp.timestamp() * 1000)
                                        elif isinstance(timestamp, datetime):
                                            timestamp_dt = timestamp
                                            open_time_ms = int(timestamp.timestamp() * 1000)
                                        else:
                                            timestamp_dt = pd.to_datetime(timestamp).to_pydatetime()
                                            open_time_ms = int(pd.to_datetime(timestamp).timestamp() * 1000)
                                        
                                        timeframe_minutes = {
                                            '1m': 1, '5m': 5, '15m': 15, '30m': 30,
                                            '1h': 60, '4h': 240, '1d': 1440
                                        }.get(timeframe, 60)
                                        close_time_ms = open_time_ms + (timeframe_minutes * 60 * 1000) - 1
                                        created_at = datetime.now()
                                        
                                        cursor.execute("""
                                            INSERT INTO kline_data
                                            (symbol, exchange, timeframe, open_time, close_time, timestamp, open_price, high_price, low_price, close_price, volume, quote_volume, created_at)
                                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                            ON DUPLICATE KEY UPDATE
                                                open_price = VALUES(open_price),
                                                high_price = VALUES(high_price),
                                                low_price = VALUES(low_price),
                                                close_price = VALUES(close_price),
                                                volume = VALUES(volume),
                                                quote_volume = VALUES(quote_volume),
                                                created_at = VALUES(created_at)
                                        """, (
                                            symbol, 'gate' if use_gate else 'binance', timeframe, open_time_ms, close_time_ms,
                                            timestamp_dt, float(row['open']), float(row['high']),
                                            float(row['low']), float(row['close']), float(row['volume']),
                                            float(row.get('quote_volume', 0)), created_at
                                        ))
                                        if cursor.rowcount > 0:
                                            timeframe_saved += 1
                                        
                                        # 实时更新进度（每保存一定数量后更新）
                                        if timeframe_saved % update_interval == 0 or timeframe_saved == total_rows:
                                            total_saved_temp = total_saved + symbol_saved + timeframe_saved
                                            task = task_manager.get_task(task_id)
                                            if task and task.estimated_total_records > 0:
                                                # 基于实际保存的数据量计算进度
                                                progress = min(95, (total_saved_temp / task.estimated_total_records) * 100)
                                            else:
                                                # 回退到基于步骤的进度计算
                                                progress = min(95, (completed_steps / total_steps) * 100) if total_steps > 0 else 0
                                            
                                            task_manager.update_task_progress(
                                                task_id,
                                                current_step=f"正在保存 {symbol} {timeframe} K线数据 ({timeframe_saved}/{total_rows})...",
                                                total_saved=total_saved_temp,
                                                progress=progress
                                            )
                                    except Exception as e:
                                        logger.error(f"保存K线数据失败: {e}")
                                        continue
                                
                                symbol_saved += timeframe_saved
                                completed_steps += 1
                                task_manager.update_task_progress(
                                    task_id,
                                    completed_steps=completed_steps,
                                    total_saved=total_saved + symbol_saved,
                                    current_step=f"✓ {symbol} {timeframe} K线数据采集完成，保存 {timeframe_saved} 条"
                                )
                        except Exception as e:
                            error_msg = str(e)
                            # 如果是无效交易对，提供更详细的错误信息
                            if 'Invalid symbol' in error_msg or '-1121' in error_msg:
                                error_msg = f"{symbol} {timeframe}: 交易对不存在或格式错误（币安可能不支持此交易对）"
                            else:
                                error_msg = f"{symbol} {timeframe}: {error_msg}"
                            errors.append(error_msg)
                            logger.error(f"采集 {symbol} {timeframe} K线数据失败: {e}")
                    
                    total_saved += symbol_saved
                    if symbol_saved == 0:
                        errors.append(f"{symbol}: 所有周期均未获取到K线数据")
                
                # 采集合约数据
                if collect_futures and futures_timeframes:
                    if use_gate and gate_collector:
                        # HYPE/USDT 从Gate.io采集合约数据
                        try:
                            task_manager.update_task_progress(
                                task_id,
                                current_step=f"正在采集 {symbol} 合约数据（Gate.io）..."
                            )
                            futures_saved = 0

                            for timeframe in futures_timeframes:
                                try:
                                    task_manager.update_task_progress(
                                        task_id,
                                        current_step=f"正在从API获取 {symbol} 合约 {timeframe} K线数据（Gate.io）..."
                                    )
                                    
                                    df = await gate_collector.fetch_historical_futures_data(
                                        symbol=symbol,
                                        timeframe=timeframe,
                                        days=int((end_time - start_time).total_seconds() / 86400) + 1
                                    )
                                    
                                    if df is not None and len(df) > 0:
                                        task_manager.update_task_progress(
                                            task_id,
                                            current_step=f"✓ 已获取 {symbol} 合约 {timeframe} K线数据 {len(df)} 条（原始），正在过滤..."
                                        )
                                    
                                    if df is not None and len(df) > 0:
                                        df = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]
                                        task_manager.update_task_progress(
                                            task_id,
                                            current_step=f"✓ 过滤后剩余 {len(df)} 条数据，正在保存..."
                                        )
                                        
                                        if len(df) == 0:
                                            task_manager.update_task_progress(
                                                task_id,
                                                current_step=f"⚠️ {symbol} 合约 {timeframe}: 过滤后无数据，可能时间范围不匹配"
                                            )
                                            errors.append(f"{symbol} 合约 {timeframe}: K线数据时间范围不匹配")
                                            continue
                                        
                                        timeframe_saved = 0
                                        total_rows = len(df)
                                        update_interval = max(1, total_rows // 20)
                                        
                                        for idx, row_tuple in enumerate(df.iterrows()):
                                            try:
                                                _, row = row_tuple
                                                timestamp = row['timestamp']
                                                if isinstance(timestamp, pd.Timestamp):
                                                    timestamp_dt = timestamp.to_pydatetime()
                                                    open_time_ms = int(timestamp.timestamp() * 1000)
                                                elif isinstance(timestamp, datetime):
                                                    timestamp_dt = timestamp
                                                    open_time_ms = int(timestamp.timestamp() * 1000)
                                                else:
                                                    timestamp_dt = pd.to_datetime(timestamp).to_pydatetime()
                                                    open_time_ms = int(pd.to_datetime(timestamp).timestamp() * 1000)
                                                
                                                timeframe_minutes = {
                                                    '1m': 1, '5m': 5, '15m': 15, '30m': 30,
                                                    '1h': 60, '4h': 240, '1d': 1440
                                                }.get(timeframe, 60)
                                                close_time_ms = open_time_ms + (timeframe_minutes * 60 * 1000) - 1
                                                created_at = datetime.now()
                                                
                                                cursor.execute("""
                                                    INSERT INTO kline_data
                                                    (symbol, exchange, timeframe, open_time, close_time, timestamp, open_price, high_price, low_price, close_price, volume, quote_volume, created_at)
                                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                                    ON DUPLICATE KEY UPDATE
                                                        open_price = VALUES(open_price),
                                                        high_price = VALUES(high_price),
                                                        low_price = VALUES(low_price),
                                                        close_price = VALUES(close_price),
                                                        volume = VALUES(volume),
                                                        quote_volume = VALUES(quote_volume),
                                                        created_at = VALUES(created_at)
                                                """, (
                                                    symbol, 'gate_futures', timeframe, open_time_ms, close_time_ms,
                                                    timestamp_dt, float(row['open']), float(row['high']),
                                                    float(row['low']), float(row['close']), float(row['volume']),
                                                    float(row.get('quote_volume', 0)), created_at
                                                ))
                                                if cursor.rowcount > 0:
                                                    timeframe_saved += 1
                                                
                                                if timeframe_saved % update_interval == 0 or timeframe_saved == total_rows:
                                                    total_saved_temp = total_saved + futures_saved + timeframe_saved
                                                    task = task_manager.get_task(task_id)
                                                    if task and task.estimated_total_records > 0:
                                                        progress = min(95, (total_saved_temp / task.estimated_total_records) * 100)
                                                    else:
                                                        progress = min(95, (completed_steps / total_steps) * 100) if total_steps > 0 else 0
                                                    
                                                    task_manager.update_task_progress(
                                                        task_id,
                                                        current_step=f"正在保存 {symbol} 合约 {timeframe} K线数据 ({timeframe_saved}/{total_rows})...",
                                                        total_saved=total_saved_temp,
                                                        progress=progress
                                                    )
                                            except Exception as e:
                                                logger.error(f"保存合约K线数据失败: {e}")
                                                continue
                                        
                                        futures_saved += timeframe_saved
                                        completed_steps += 1
                                        task_manager.update_task_progress(
                                            task_id,
                                            completed_steps=completed_steps,
                                            total_saved=total_saved + futures_saved,
                                            current_step=f"✓ {symbol} 合约 {timeframe} K线数据采集完成，保存 {timeframe_saved} 条"
                                        )
                                except Exception as e:
                                    error_msg = f"{symbol} 合约 {timeframe}: {str(e)}"
                                    errors.append(error_msg)
                                    logger.error(f"采集 {symbol} 合约 {timeframe} K线数据失败: {e}")
                            
                            total_saved += futures_saved
                        except Exception as e:
                            error_msg = f"{symbol} 合约数据: {str(e)}"
                            errors.append(error_msg)
                            logger.error(f"采集 {symbol} 合约数据失败: {e}")
                    elif binance_futures_collector:
                        # 其他交易对从Binance采集合约数据
                        try:
                            task_manager.update_task_progress(
                                task_id,
                                current_step=f"正在采集 {symbol} 合约数据..."
                            )
                            futures_saved = 0

                            for timeframe in futures_timeframes:
                                try:
                                    task_manager.update_task_progress(
                                        task_id,
                                        current_step=f"正在从API获取 {symbol} 合约 {timeframe} K线数据..."
                                    )
                                    
                                    days = int((end_time - start_time).total_seconds() / 86400) + 1
                                    timeframe_minutes = {
                                        '1m': 1, '5m': 5, '15m': 15, '30m': 30,
                                        '1h': 60, '4h': 240, '1d': 1440
                                    }.get(timeframe, 60)
                                    klines_needed = int(days * 1440 / timeframe_minutes)
                                    limit = min(klines_needed, 1500)
                                    
                                    df = await binance_futures_collector.fetch_futures_klines(
                                        symbol=symbol,
                                        timeframe=timeframe,
                                        limit=limit
                                    )
                                    
                                    if df is not None and len(df) > 0:
                                        task_manager.update_task_progress(
                                            task_id,
                                            current_step=f"✓ 已获取 {symbol} 合约 {timeframe} K线数据 {len(df)} 条（原始），正在过滤..."
                                        )
                                    
                                    if df is not None and len(df) > 0:
                                        df = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]
                                        task_manager.update_task_progress(
                                            task_id,
                                            current_step=f"✓ 过滤后剩余 {len(df)} 条数据，正在保存..."
                                        )
                                        
                                        if len(df) == 0:
                                            task_manager.update_task_progress(
                                                task_id,
                                                current_step=f"⚠️ {symbol} 合约 {timeframe}: 过滤后无数据，可能时间范围不匹配"
                                            )
                                            errors.append(f"{symbol} 合约 {timeframe}: K线数据时间范围不匹配")
                                            continue
                                        
                                        timeframe_saved = 0
                                        total_rows = len(df)
                                        update_interval = max(1, total_rows // 20)  # 每5%更新一次进度
                                        
                                        for idx, row_tuple in enumerate(df.iterrows()):
                                            try:
                                                _, row = row_tuple
                                                timestamp = row['timestamp']
                                                if isinstance(timestamp, pd.Timestamp):
                                                    timestamp_dt = timestamp.to_pydatetime()
                                                    open_time_ms = int(timestamp.timestamp() * 1000)
                                                elif isinstance(timestamp, datetime):
                                                    timestamp_dt = timestamp
                                                    open_time_ms = int(timestamp.timestamp() * 1000)
                                                else:
                                                    timestamp_dt = pd.to_datetime(timestamp).to_pydatetime()
                                                    open_time_ms = int(pd.to_datetime(timestamp).timestamp() * 1000)
                                                
                                                timeframe_minutes = {
                                                    '1m': 1, '5m': 5, '15m': 15, '30m': 30,
                                                    '1h': 60, '4h': 240, '1d': 1440
                                                }.get(timeframe, 60)
                                                close_time_ms = open_time_ms + (timeframe_minutes * 60 * 1000) - 1
                                                created_at = datetime.now()
                                                
                                                cursor.execute("""
                                                    INSERT INTO kline_data
                                                    (symbol, exchange, timeframe, open_time, close_time, timestamp, open_price, high_price, low_price, close_price, volume, quote_volume, created_at)
                                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                                    ON DUPLICATE KEY UPDATE
                                                        open_price = VALUES(open_price),
                                                        high_price = VALUES(high_price),
                                                        low_price = VALUES(low_price),
                                                        close_price = VALUES(close_price),
                                                        volume = VALUES(volume),
                                                        quote_volume = VALUES(quote_volume),
                                                        created_at = VALUES(created_at)
                                                """, (
                                                    symbol, 'binance_futures', timeframe, open_time_ms, close_time_ms,
                                                    timestamp_dt, float(row['open']), float(row['high']),
                                                    float(row['low']), float(row['close']), float(row['volume']),
                                                    float(row.get('quote_volume', 0)), created_at
                                                ))
                                                if cursor.rowcount > 0:
                                                    timeframe_saved += 1
                                                
                                                # 实时更新进度（每保存一定数量后更新）
                                                if timeframe_saved % update_interval == 0 or timeframe_saved == total_rows:
                                                    total_saved_temp = total_saved + futures_saved + timeframe_saved
                                                    task = task_manager.get_task(task_id)
                                                    if task and task.estimated_total_records > 0:
                                                        # 基于实际保存的数据量计算进度
                                                        progress = min(95, (total_saved_temp / task.estimated_total_records) * 100)
                                                    else:
                                                        # 回退到基于步骤的进度计算
                                                        progress = min(95, (completed_steps / total_steps) * 100) if total_steps > 0 else 0
                                                    
                                                    task_manager.update_task_progress(
                                                        task_id,
                                                        current_step=f"正在保存 {symbol} 合约 {timeframe} K线数据 ({timeframe_saved}/{total_rows})...",
                                                        total_saved=total_saved_temp,
                                                        progress=progress
                                                    )
                                            except Exception as e:
                                                logger.error(f"保存合约K线数据失败: {e}")
                                                continue
                                    
                                    futures_saved += timeframe_saved
                                    completed_steps += 1
                                    task_manager.update_task_progress(
                                        task_id,
                                        completed_steps=completed_steps,
                                        total_saved=total_saved + futures_saved,
                                        current_step=f"✓ {symbol} 合约 {timeframe} K线数据采集完成，保存 {timeframe_saved} 条"
                                    )
                                    
                                    # 延迟避免API限流
                                    await asyncio.sleep(0.3)
                                    
                                except Exception as e:
                                    error_msg = str(e)
                                    # 如果是无效交易对，提供更详细的错误信息
                                    if 'Invalid symbol' in error_msg or '-1121' in error_msg or 'HTTP 400' in error_msg:
                                        error_msg = f"{symbol} 合约 {timeframe}: 交易对不存在或格式错误（币安合约可能不支持此交易对）"
                                    else:
                                        error_msg = f"{symbol} 合约 {timeframe}: {error_msg}"
                                    errors.append(error_msg)
                                    logger.error(f"采集 {symbol} 合约 {timeframe} K线数据失败: {e}")
                            
                            total_saved += futures_saved
                            if futures_saved == 0:
                                errors.append(f"{symbol}: 所有周期均未获取到合约数据")
                                
                        except Exception as e:
                            error_msg = f"{symbol} 合约数据: {str(e)}"
                            errors.append(error_msg)
                            logger.error(f"采集 {symbol} 合约数据失败: {e}")
                
            except Exception as e:
                error_msg = f"{symbol}: {str(e)}"
                errors.append(error_msg)
                logger.error(f"采集 {symbol} 数据失败: {e}")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # 更新配置文件
        config_updated = False
        if save_to_config:
            try:
                price_updated = _update_config_file(config_path, symbols, 'price', None)
                if price_updated:
                    config_updated = True
                if collect_kline and timeframes:
                    for tf in timeframes:
                        updated = _update_config_file(config_path, symbols, 'kline', tf)
                        if updated:
                            config_updated = True
            except Exception as e:
                logger.error(f"更新配置文件失败: {e}")
        
        result = {
            'success': True,
            'total_saved': total_saved,
            'errors': errors,
            'config_updated': config_updated,
            'collect_futures': collect_futures,
            'message': f'成功采集 {total_saved} 条数据' + (f'，{len(errors)} 个错误' if errors else '')
        }
        
        task_manager.set_task_status(task_id, TaskStatus.COMPLETED, result)
        task_manager.update_task_progress(task_id, progress=100.0, current_step="采集完成")
        
    except Exception as e:
        logger.error(f"数据采集任务执行失败: {e}")
        import traceback
        traceback.print_exc()
        task_manager.set_task_status(
            task_id,
            TaskStatus.FAILED,
            {'success': False, 'error': str(e)}
        )


@router.post("/collect")
async def collect_historical_data(request: Dict, background_tasks: BackgroundTasks):
    """
    采集历史数据
    
    Args:
        request: 包含以下字段的字典
            - symbols: 交易对列表，如 ["BTC/USDT", "ETH/USDT"]
            - data_type: 数据类型，'price'、'kline' 或 'both'（同时采集价格和K线数据）
            - start_time: 开始时间 (ISO格式字符串)
            - end_time: 结束时间 (ISO格式字符串)
            - timeframes: 时间周期列表（仅K线数据需要），如 ['1m', '5m', '1h']，默认 ['1m', '5m', '15m', '1h', '1d']
            - timeframe: 单个时间周期（向后兼容，如果timeframes不存在则使用此字段）
            - collect_futures: 是否采集合约数据 (bool)
            - save_to_config: 是否保存到配置文件 (bool)
    """
    try:
        symbols = request.get('symbols', [])
        data_type = request.get('data_type', 'price')
        start_time_str = request.get('start_time')
        end_time_str = request.get('end_time')
        # 支持多个时间周期，默认所有周期
        timeframes = request.get('timeframes', None)
        if not timeframes:
            # 向后兼容：如果只有单个timeframe，转换为列表
            timeframe = request.get('timeframe', '1h')
            timeframes = [timeframe] if data_type == 'kline' else []
        collect_futures = request.get('collect_futures', False)
        save_to_config = request.get('save_to_config', False)
        
        if not symbols:
            raise HTTPException(status_code=400, detail="交易对列表不能为空")
        
        if not start_time_str or not end_time_str:
            raise HTTPException(status_code=400, detail="开始时间和结束时间不能为空")
        
        # 解析时间
        try:
            start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
            end_time = datetime.fromisoformat(end_time_str.replace('Z', '+00:00'))
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"时间格式错误: {str(e)}")
        
        if start_time >= end_time:
            raise HTTPException(status_code=400, detail="结束时间必须晚于开始时间")
        
        # 创建后台任务
        task_id = task_manager.create_task(request)
        
        # 在后台执行采集任务
        background_tasks.add_task(_execute_collection_task, task_id, request)
        
        return {
            'success': True,
            'task_id': task_id,
            'message': '数据采集任务已提交，正在后台执行'
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"创建数据采集任务失败: {e}")
        raise HTTPException(status_code=500, detail=f"创建数据采集任务失败: {str(e)}")


@router.get("/collect/task/{task_id}")
async def get_collection_task_status(task_id: str):
    """获取数据采集任务状态"""
    try:
        task = task_manager.get_task(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="任务不存在")
        
        return task.to_dict()
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取任务状态失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取任务状态失败: {str(e)}")


@router.post("/collect-sync")
async def collect_historical_data_sync(request: Dict):
    """
    同步采集历史数据（保留向后兼容）
    
    Args:
        request: 包含以下字段的字典
            - symbols: 交易对列表，如 ["BTC/USDT", "ETH/USDT"]
            - data_type: 数据类型，'price'、'kline' 或 'both'（同时采集价格和K线数据）
            - start_time: 开始时间 (ISO格式字符串)
            - end_time: 结束时间 (ISO格式字符串)
            - timeframes: 时间周期列表（仅K线数据需要），如 ['1m', '5m', '1h']，默认 ['1m', '5m', '15m', '1h', '1d']
            - timeframe: 单个时间周期（向后兼容，如果timeframes不存在则使用此字段）
            - collect_futures: 是否采集合约数据 (bool)
            - save_to_config: 是否保存到配置文件 (bool)
    """
    try:
        symbols = request.get('symbols', [])
        data_type = request.get('data_type', 'price')
        start_time_str = request.get('start_time')
        end_time_str = request.get('end_time')
        # 支持多个时间周期，默认所有周期
        timeframes = request.get('timeframes', None)
        if not timeframes:
            # 向后兼容：如果只有单个timeframe，转换为列表
            timeframe = request.get('timeframe', '1h')
            timeframes = [timeframe] if data_type == 'kline' else []
        collect_futures = request.get('collect_futures', False)
        save_to_config = request.get('save_to_config', False)
        
        if not symbols:
            raise HTTPException(status_code=400, detail="交易对列表不能为空")
        
        if not start_time_str or not end_time_str:
            raise HTTPException(status_code=400, detail="开始时间和结束时间不能为空")
        
        # 解析时间
        try:
            start_time = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
            end_time = datetime.fromisoformat(end_time_str.replace('Z', '+00:00'))
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"时间格式错误: {str(e)}")
        
        if start_time >= end_time:
            raise HTTPException(status_code=400, detail="结束时间必须晚于开始时间")
        
        # 导入采集器
        from app.collectors.price_collector import MultiExchangeCollector
        import yaml
        from pathlib import Path
        
        # 加载配置（支持环境变量）
        from app.utils.config_loader import load_config
        config = load_config()

        # 初始化采集器
        collector = MultiExchangeCollector(config)
        
        # 初始化合约采集器（如果需要）
        futures_collector = None
        if collect_futures:
            try:
                from app.collectors.binance_futures_collector import BinanceFuturesCollector
                binance_config = config.get('exchanges', {}).get('binance', {})
                futures_collector = BinanceFuturesCollector(binance_config)
                logger.info("合约数据采集器初始化成功")
            except Exception as e:
                logger.warning(f"合约数据采集器初始化失败: {e}，将跳过合约数据采集")
                collect_futures = False
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        total_saved = 0
        errors = []
        
        # 遍历每个交易对
        for symbol in symbols:
            try:
                symbol = symbol.strip()
                if not symbol:
                    continue
                
                logger.info(f"开始采集 {symbol} 的{data_type}数据，时间范围: {start_time} - {end_time}")
                
                # 判断是否需要采集价格数据
                collect_price = data_type in ['price', 'both']
                # 判断是否需要采集K线数据
                collect_kline = data_type in ['kline', 'both']
                
                if collect_price:
                    # 采集价格数据 - 使用1分钟K线数据来获取历史价格
                    df = await collector.fetch_historical_data(
                        symbol=symbol,
                        timeframe='1m',
                        days=int((end_time - start_time).total_seconds() / 86400) + 1
                    )
                    
                    if df is not None and len(df) > 0:
                        # 过滤时间范围
                        df = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]
                        
                        saved_count = 0
                        for _, row in df.iterrows():
                            try:
                                # 获取当前时间作为created_at
                                created_at = datetime.now()
                                
                                # 从K线数据转换为价格数据格式
                                cursor.execute("""
                                    INSERT INTO price_data
                                    (symbol, exchange, timestamp, price, open_price, high_price, low_price, close_price, volume, quote_volume, bid_price, ask_price, change_24h, created_at)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    ON DUPLICATE KEY UPDATE
                                        price = VALUES(price),
                                        open_price = VALUES(open_price),
                                        high_price = VALUES(high_price),
                                        low_price = VALUES(low_price),
                                        close_price = VALUES(close_price),
                                        volume = VALUES(volume),
                                        quote_volume = VALUES(quote_volume),
                                        bid_price = VALUES(bid_price),
                                        ask_price = VALUES(ask_price),
                                        change_24h = VALUES(change_24h),
                                        created_at = VALUES(created_at)
                                """, (
                                    symbol,
                                    'binance',  # 默认交易所
                                    row['timestamp'],
                                    float(row['close']),  # 使用收盘价作为价格
                                    float(row['open']),
                                    float(row['high']),
                                    float(row['low']),
                                    float(row['close']),
                                    float(row['volume']),
                                    float(row.get('quote_volume', 0)),
                                    0,  # bid
                                    0,  # ask
                                    0,  # change_24h (历史数据无法计算)
                                    created_at
                                ))
                                if cursor.rowcount > 0:
                                    saved_count += 1
                            except Exception as e:
                                logger.error(f"保存价格数据失败: {e}")
                                continue
                        
                        total_saved += saved_count
                        logger.info(f"{symbol} 价格数据采集完成，保存 {saved_count} 条")
                    else:
                        errors.append(f"{symbol}: 未获取到价格数据")
                    
                if collect_kline:
                    # 采集K线数据 - 对所有时间周期进行采集
                    if not timeframes:
                        timeframes = ['1m', '5m', '15m', '1h', '1d']  # 默认所有周期
                    
                    symbol_saved = 0
                    for timeframe in timeframes:
                        try:
                            logger.info(f"  采集 {symbol} {timeframe} K线数据...")
                            
                            if use_gate and gate_collector:
                                # HYPE/USDT 从Gate.io采集
                                days = int((end_time - start_time).total_seconds() / 86400) + 1
                                since = int(start_time.timestamp())
                                df = await gate_collector.fetch_ohlcv(
                                    symbol=symbol,
                                    timeframe=timeframe,
                                    limit=1000,
                                    since=since
                                )
                                # 如果数据不够，需要分批获取
                                if df is not None and len(df) > 0:
                                    all_data = [df]
                                    last_timestamp = df['timestamp'].iloc[-1]
                                    current_since = int(last_timestamp.timestamp()) + 1
                                    while current_since < int(end_time.timestamp()):
                                        next_df = await gate_collector.fetch_ohlcv(
                                            symbol=symbol,
                                            timeframe=timeframe,
                                            limit=1000,
                                            since=current_since
                                        )
                                        if next_df is None or len(next_df) == 0:
                                            break
                                        all_data.append(next_df)
                                        last_timestamp = next_df['timestamp'].iloc[-1]
                                        current_since = int(last_timestamp.timestamp()) + 1
                                        if len(next_df) < 1000:
                                            break
                                        await asyncio.sleep(0.5)
                                    if len(all_data) > 1:
                                        df = pd.concat(all_data, ignore_index=True)
                                        df = df.drop_duplicates(subset=['timestamp'])
                                        df = df.sort_values('timestamp').reset_index(drop=True)
                            else:
                                # 其他交易对从Binance采集
                                df = await collector.fetch_historical_data(
                                    symbol=symbol,
                                    timeframe=timeframe,
                                    days=int((end_time - start_time).total_seconds() / 86400) + 1,
                                    exchange='binance' if not use_gate else None
                                )
                            
                            if df is not None and len(df) > 0:
                                # 过滤时间范围
                                df = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]
                                
                                if len(df) == 0:
                                    errors.append(f"{symbol} {timeframe}: K线数据时间范围不匹配")
                                    continue
                                
                                timeframe_saved = 0
                                for idx, row_tuple in enumerate(df.iterrows()):
                                    try:
                                        _, row = row_tuple
                                        # 计算时间戳（毫秒）
                                        timestamp = row['timestamp']
                                        # 确保timestamp是datetime类型
                                        if isinstance(timestamp, pd.Timestamp):
                                            timestamp_dt = timestamp.to_pydatetime()
                                            open_time_ms = int(timestamp.timestamp() * 1000)
                                        elif isinstance(timestamp, datetime):
                                            timestamp_dt = timestamp
                                            open_time_ms = int(timestamp.timestamp() * 1000)
                                        else:
                                            # 尝试转换
                                            timestamp_dt = pd.to_datetime(timestamp).to_pydatetime()
                                            open_time_ms = int(pd.to_datetime(timestamp).timestamp() * 1000)
                                        
                                        # 计算收盘时间（根据时间周期）
                                        timeframe_minutes = {
                                            '1m': 1, '5m': 5, '15m': 15, '30m': 30,
                                            '1h': 60, '4h': 240, '1d': 1440
                                        }.get(timeframe, 60)
                                        close_time_ms = open_time_ms + (timeframe_minutes * 60 * 1000) - 1
                                        
                                        # 获取当前时间作为created_at
                                        created_at = datetime.now()
                                        
                                        cursor.execute("""
                                            INSERT INTO kline_data
                                            (symbol, exchange, timeframe, open_time, close_time, timestamp, open_price, high_price, low_price, close_price, volume, quote_volume, created_at)
                                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                            ON DUPLICATE KEY UPDATE
                                                open_price = VALUES(open_price),
                                                high_price = VALUES(high_price),
                                                low_price = VALUES(low_price),
                                                close_price = VALUES(close_price),
                                                volume = VALUES(volume),
                                                quote_volume = VALUES(quote_volume),
                                                created_at = VALUES(created_at)
                                        """, (
                                            symbol,
                                            'gate' if use_gate else 'binance',
                                            timeframe,
                                            open_time_ms,
                                            close_time_ms,
                                            timestamp_dt,
                                            float(row['open']),
                                            float(row['high']),
                                            float(row['low']),
                                            float(row['close']),
                                            float(row['volume']),
                                            float(row.get('quote_volume', 0)),
                                            created_at
                                        ))
                                        if cursor.rowcount > 0:
                                            timeframe_saved += 1
                                    except Exception as e:
                                        logger.error(f"保存K线数据失败 ({timeframe}): {e}")
                                        continue
                                
                                symbol_saved += timeframe_saved
                                logger.info(f"  ✓ {symbol} {timeframe} K线数据采集完成，保存 {timeframe_saved} 条")
                            else:
                                logger.warning(f"  ⊗ {symbol} {timeframe}: 未获取到K线数据")
                        except Exception as e:
                            error_msg = f"{symbol} {timeframe}: {str(e)}"
                            errors.append(error_msg)
                            logger.error(f"采集 {symbol} {timeframe} K线数据失败: {e}")
                    
                    total_saved += symbol_saved
                    if symbol_saved > 0:
                        logger.info(f"{symbol} K线数据采集完成，共保存 {symbol_saved} 条（所有周期）")
                    else:
                        errors.append(f"{symbol}: 所有周期均未获取到K线数据")
                
                # 采集合约数据
                if collect_futures and futures_collector:
                    try:
                        logger.info(f"开始采集 {symbol} 的合约数据...")
                        futures_saved = 0
                        
                        # 对每个时间周期采集合约K线数据
                        for timeframe in timeframes:
                            try:
                                logger.info(f"  采集 {symbol} 合约 {timeframe} K线数据...")
                                
                                # 计算需要获取的数据量
                                days = int((end_time - start_time).total_seconds() / 86400) + 1
                                # 根据时间周期计算limit
                                timeframe_minutes = {
                                    '1m': 1, '5m': 5, '15m': 15, '30m': 30,
                                    '1h': 60, '4h': 240, '1d': 1440
                                }.get(timeframe, 60)
                                # 每个周期需要的K线数量
                                klines_needed = int(days * 1440 / timeframe_minutes)
                                limit = min(klines_needed, 1500)  # 币安限制最大1500
                                
                                # 获取合约K线数据
                                df = await futures_collector.fetch_futures_klines(
                                    symbol=symbol,
                                    timeframe=timeframe,
                                    limit=limit
                                )
                                
                                if df is not None and len(df) > 0:
                                    # 过滤时间范围
                                    df = df[(df['timestamp'] >= start_time) & (df['timestamp'] <= end_time)]
                                    
                                    timeframe_saved = 0
                                    for _, row in df.iterrows():
                                        try:
                                            # 计算时间戳（毫秒）
                                            timestamp = row['timestamp']
                                            if isinstance(timestamp, pd.Timestamp):
                                                timestamp_dt = timestamp.to_pydatetime()
                                                open_time_ms = int(timestamp.timestamp() * 1000)
                                            elif isinstance(timestamp, datetime):
                                                timestamp_dt = timestamp
                                                open_time_ms = int(timestamp.timestamp() * 1000)
                                            else:
                                                timestamp_dt = pd.to_datetime(timestamp).to_pydatetime()
                                                open_time_ms = int(pd.to_datetime(timestamp).timestamp() * 1000)
                                            
                                            # 计算收盘时间
                                            timeframe_minutes = {
                                                '1m': 1, '5m': 5, '15m': 15, '30m': 30,
                                                '1h': 60, '4h': 240, '1d': 1440
                                            }.get(timeframe, 60)
                                            close_time_ms = open_time_ms + (timeframe_minutes * 60 * 1000) - 1
                                            
                                            # 获取当前时间作为created_at
                                            created_at = datetime.now()
                                            
                                            # 保存合约K线数据
                                            cursor.execute("""
                                                INSERT INTO kline_data
                                                (symbol, exchange, timeframe, open_time, close_time, timestamp, open_price, high_price, low_price, close_price, volume, quote_volume, created_at)
                                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                                ON DUPLICATE KEY UPDATE
                                                    open_price = VALUES(open_price),
                                                    high_price = VALUES(high_price),
                                                    low_price = VALUES(low_price),
                                                    close_price = VALUES(close_price),
                                                    volume = VALUES(volume),
                                                    quote_volume = VALUES(quote_volume),
                                                    created_at = VALUES(created_at)
                                            """, (
                                                symbol,
                                                'binance_futures',
                                                timeframe,
                                                open_time_ms,
                                                close_time_ms,
                                                timestamp_dt,
                                                float(row['open']),
                                                float(row['high']),
                                                float(row['low']),
                                                float(row['close']),
                                                float(row['volume']),
                                                float(row.get('quote_volume', 0)),
                                                created_at
                                            ))
                                            timeframe_saved += 1
                                        except Exception as e:
                                            logger.error(f"保存合约K线数据失败 ({timeframe}): {e}")
                                            continue
                                    
                                    futures_saved += timeframe_saved
                                    logger.info(f"  ✓ {symbol} 合约 {timeframe} K线数据采集完成，保存 {timeframe_saved} 条")
                                else:
                                    logger.warning(f"  ⊗ {symbol} 合约 {timeframe}: 未获取到K线数据")
                                
                                # 延迟避免API限流
                                await asyncio.sleep(0.3)
                                
                            except Exception as e:
                                error_msg = f"{symbol} 合约 {timeframe}: {str(e)}"
                                errors.append(error_msg)
                                logger.error(f"采集 {symbol} 合约 {timeframe} K线数据失败: {e}")
                        
                        total_saved += futures_saved
                        if futures_saved > 0:
                            logger.info(f"{symbol} 合约数据采集完成，共保存 {futures_saved} 条（所有周期）")
                        else:
                            errors.append(f"{symbol}: 所有周期均未获取到合约数据")
                            
                    except Exception as e:
                        error_msg = f"{symbol} 合约数据: {str(e)}"
                        errors.append(error_msg)
                        logger.error(f"采集 {symbol} 合约数据失败: {e}")
                
            except Exception as e:
                error_msg = f"{symbol}: {str(e)}"
                errors.append(error_msg)
                logger.error(f"采集 {symbol} 数据失败: {e}")
                import traceback
                traceback.print_exc()
        
        conn.commit()
        cursor.close()
        conn.close()
        
        # 如果勾选了保存到配置文件，则更新config.yaml
        config_updated = False
        if save_to_config:
            try:
                # 更新交易对列表
                price_updated = _update_config_file(config_path, symbols, 'price', None)
                if price_updated:
                    config_updated = True
                
                # 对于K线数据，更新所有时间周期
                if collect_kline and timeframes:
                    for tf in timeframes:
                        updated = _update_config_file(config_path, symbols, 'kline', tf)
                        if updated:
                            config_updated = True
                
                if config_updated:
                    timeframe_str = ', '.join(timeframes) if collect_kline and timeframes else ''
                    logger.info(f"配置文件已更新: 添加了 {len(symbols)} 个交易对" + 
                              (f"，时间周期 {timeframe_str}" if timeframe_str else ""))
            except Exception as e:
                logger.error(f"更新配置文件失败: {e}")
        
        return {
            'success': True,
            'total_saved': total_saved,
            'errors': errors,
            'config_updated': config_updated,
            'collect_futures': collect_futures,
            'message': f'成功采集 {total_saved} 条数据' + (f'，{len(errors)} 个错误' if errors else '') + 
                      (f'，配置文件已更新' if config_updated else '')
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"数据采集失败: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"数据采集失败: {str(e)}")

