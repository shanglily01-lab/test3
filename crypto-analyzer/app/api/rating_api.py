#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
交易对评级管理API
提供前端界面查看和手动触发评级更新
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, List
from loguru import logger
import sys
import os
import math
from dotenv import load_dotenv

load_dotenv()

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from app.services.symbol_rating_manager import SymbolRatingManager
from app.services.optimization_config import OptimizationConfig
from app.services.securities_filter import is_security


def safe_float(value, default=0.0):
    """安全转换float,避免inf和nan"""
    try:
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return default
        return f
    except (ValueError, TypeError):
        return default

router = APIRouter()

# 数据库配置 — 统一使用本地库（由环境变量控制）
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', ''),
    'database': os.getenv('DB_NAME', 'binance-data')
}


class RatingUpdateRequest(BaseModel):
    """评级更新请求"""
    observation_days: Optional[int] = None  # 观察天数,默认从配置读取


class ManualRatingRequest(BaseModel):
    """手动设置评级请求"""
    symbol: str
    rating_level: int  # 0=白名单, 1=黑名单1级, 2=黑名单2级, 3=永久禁止
    reason: Optional[str] = "手动设置"


@router.get("/api/rating/config")
async def get_rating_config():
    """获取评级配置"""
    try:
        opt_config = OptimizationConfig(DB_CONFIG)

        # 升级配置
        upgrade_config = opt_config.get_blacklist_upgrade_config()

        # 触发配置
        trigger_configs = {}
        for level in [1, 2, 3]:
            trigger = opt_config.get_blacklist_trigger_config(level)
            blacklist_cfg = opt_config.get_blacklist_config(level)

            # Level 3的reversal_threshold是inf,需要特殊处理
            reversal_threshold = blacklist_cfg['reversal_threshold']
            if level == 3:
                reversal_threshold = 999999  # 用一个大数字代替inf

            trigger_configs[f"level{level}"] = {
                "trigger_stop_loss_count": trigger['stop_loss_count'],
                "trigger_loss_amount": trigger['loss_amount'],
                "margin_multiplier": safe_float(blacklist_cfg['margin_multiplier']),
                "reversal_threshold": safe_float(reversal_threshold)
            }

        return {
            "success": True,
            "upgrade_config": {
                "profit_amount": upgrade_config['profit_amount'],
                "win_rate": upgrade_config['win_rate'],
                "observation_days": upgrade_config['observation_days']
            },
            "trigger_configs": trigger_configs
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/rating/current")
async def get_current_ratings(trading_type: Optional[str] = None):
    """
    获取当前所有评级

    Args:
        trading_type: 可选的交易类型过滤 ('usdt_futures' 或 'coin_futures')
                     usdt_futures: 只返回USDT结尾的交易对
                     coin_futures: 只返回USD结尾的交易对
                     None: 返回所有交易对
    """
    try:
        opt_config = OptimizationConfig(DB_CONFIG)

        # 获取所有评级
        ratings = opt_config.get_all_symbol_ratings()

        # 按等级分组
        grouped = {
            "level0": [],  # 白名单
            "level1": [],  # 黑名单1级
            "level2": [],  # 黑名单2级
            "level3": []   # 黑名单3级(永久禁止)
        }

        for rating in ratings:
            symbol = rating['symbol']

            # 根据 trading_type 过滤
            if trading_type == 'usdt_futures':
                # U本位: 只显示USDT结尾的交易对
                if not symbol.endswith('USDT'):
                    continue
            elif trading_type == 'coin_futures':
                # 币本位: 只显示USD结尾的交易对(排除USDT)
                if not symbol.endswith('USD') or symbol.endswith('USDT'):
                    continue

            level = rating['rating_level']
            grouped[f"level{level}"].append({
                "symbol": symbol,
                "rating_level": level,
                "reason": rating.get('level_change_reason', '') or '',
                "hard_stop_loss_count": rating.get('hard_stop_loss_count', 0),
                "total_loss_amount": safe_float(rating.get('total_loss_amount', 0)),
                "total_profit_amount": safe_float(rating.get('total_profit_amount', 0)),
                "win_rate": safe_float(rating.get('win_rate', 0)),
                "total_trades": rating.get('total_trades', 0),
                "updated_at": rating.get('updated_at').isoformat() if rating.get('updated_at') else None
            })

        total_count = sum(len(grouped[f"level{i}"]) for i in range(4))

        return {
            "success": True,
            "ratings": grouped,
            "total_count": total_count
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/rating/update")
async def trigger_rating_update(request: RatingUpdateRequest):
    """手动触发评级更新"""
    try:
        rating_manager = SymbolRatingManager(DB_CONFIG)

        # 执行评级更新
        results = rating_manager.update_all_symbol_ratings(
            observation_days=request.observation_days
        )

        # 格式化结果
        formatted_results = {
            "total_symbols": results['total_symbols'],
            "upgraded": [
                {
                    "symbol": item['symbol'],
                    "old_level": item['old_level'],
                    "new_level": item['new_level'],
                    "reason": item['reason'],
                    "stats": item['stats']
                }
                for item in results['upgraded']
            ],
            "downgraded": [
                {
                    "symbol": item['symbol'],
                    "old_level": item['old_level'],
                    "new_level": item['new_level'],
                    "reason": item['reason'],
                    "stats": item['stats']
                }
                for item in results['downgraded']
            ],
            "unchanged": [
                {
                    "symbol": item['symbol'],
                    "level": item['level'],
                    "reason": item['reason']
                }
                for item in results['unchanged']
            ],
            "new_rated": [
                {
                    "symbol": item['symbol'],
                    "new_level": item['new_level'],
                    "reason": item['reason'],
                    "stats": item['stats']
                }
                for item in results['new_rated']
            ]
        }

        return {
            "success": True,
            "message": "评级更新完成",
            "results": formatted_results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/api/rating/set")
async def set_symbol_rating(request: ManualRatingRequest):
    """手动设置单个交易对的评级"""
    try:
        if request.rating_level not in (0, 1, 2, 3):
            raise HTTPException(status_code=400, detail="rating_level must be 0, 1, 2, or 3")
        opt_config = OptimizationConfig(DB_CONFIG)
        opt_config.update_symbol_rating(
            symbol=request.symbol.upper(),
            new_level=request.rating_level,
            reason=request.reason or "手动设置"
        )
        return {"success": True, "message": f"{request.symbol.upper()} 评级已设置为 {request.rating_level}"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/top50")
async def get_top50():
    """获取 TOP50 高胜率交易对列表及统计"""
    import pymysql
    try:
        conn = pymysql.connect(**DB_CONFIG, cursorclass=pymysql.cursors.DictCursor)
        cur = conn.cursor()
        # 多取一些, 出库后剔除证券类再截 50, 防止股票占满榜单
        cur.execute("""
            SELECT symbol, total_realized_pnl, total_trades, winning_trades, losing_trades,
                   win_rate, avg_pnl_per_trade, max_single_profit, max_single_loss,
                   profit_factor, rank_score, last_updated
            FROM top_performing_symbols
            ORDER BY rank_score DESC
            LIMIT 100
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        rows = [r for r in rows if not is_security(r['symbol'])][:50]

        data = []
        for r in rows:
            data.append({
                'symbol':             r['symbol'],
                'total_realized_pnl': float(r['total_realized_pnl'] or 0),
                'total_trades':       int(r['total_trades'] or 0),
                'winning_trades':     int(r['winning_trades'] or 0),
                'losing_trades':      int(r['losing_trades'] or 0),
                'win_rate':           float(r['win_rate'] or 0),
                'avg_pnl_per_trade':  float(r['avg_pnl_per_trade'] or 0),
                'max_single_profit':  float(r['max_single_profit'] or 0) if r['max_single_profit'] is not None else 0,
                'max_single_loss':    float(r['max_single_loss'] or 0) if r['max_single_loss'] is not None else 0,
                'profit_factor':      float(r['profit_factor'] or 0) if r['profit_factor'] is not None else 0,
                'rank_score':         int(r['rank_score'] or 0),
                'last_updated':       r['last_updated'].isoformat() if r['last_updated'] else None,
            })

        total_pnl = sum(r['total_realized_pnl'] for r in data)
        avg_wr = (sum(r['win_rate'] for r in data) / len(data)) if data else 0
        last_updated = data[0]['last_updated'] if data else None

        return {
            'success': True,
            'data': data,
            'stats': {
                'count':         len(data),
                'total_pnl':     round(total_pnl, 2),
                'avg_win_rate':  round(avg_wr, 2),
                'last_updated':  last_updated,
            }
        }
    except Exception as e:
        logger.error(f"获取TOP50失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/rating/symbol/{symbol}")
async def get_symbol_rating(symbol: str, days: int = 7):
    """获取单个交易对的评级和表现分析"""
    try:
        rating_manager = SymbolRatingManager(DB_CONFIG)
        opt_config = OptimizationConfig(DB_CONFIG)

        # 获取当前评级
        current_rating = opt_config.get_symbol_rating(symbol)

        # 分析表现
        stats = rating_manager.analyze_symbol_performance(symbol, days)

        # 计算潜在新评级
        current_level = current_rating['rating_level'] if current_rating else 0
        potential_level, reason = rating_manager.calculate_new_rating_level(stats, current_level)

        return {
            "success": True,
            "symbol": symbol,
            "current_rating": {
                "level": current_level,
                "reason": current_rating.get('level_change_reason', '无评级') if current_rating else "无评级",
                "updated_at": current_rating['updated_at'].isoformat() if current_rating and current_rating.get('updated_at') else None
            },
            "performance_stats": stats,
            "potential_change": {
                "would_change": potential_level != current_level,
                "new_level": potential_level,
                "reason": reason
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
