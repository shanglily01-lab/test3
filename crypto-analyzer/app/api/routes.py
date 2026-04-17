"""
API路由定义
"""

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Dict, Optional
from loguru import logger

from app.database.db_service import DatabaseService
from app.services.analysis_service import AnalysisService


router = APIRouter()

# 全局数据库服务
_db_service = None


def get_db_service():
    """获取数据库服务单例"""
    global _db_service
    if _db_service is None:
        from app.utils.config_loader import load_config
        config = load_config()
        _db_service = DatabaseService(config.get('database', {}))
    return _db_service


def get_db_session():
    """获取数据库会话"""
    db_service = get_db_service()
    session = db_service.get_session()
    try:
        yield session
    finally:
        session.close()


@router.get("/api")
async def api_info():
    """API信息"""
    return {
        "name": "Crypto Analyzer API",
        "version": "1.2.0",
        "status": "running",
        "endpoints": [
            "/api/dashboard",
            "/api/prices",
            "/api/analysis/{symbol}",
            "/api/kline/{symbol}",
            "/api/news",
            "/api/sentiment/{symbol}",
            "/api/funding-rate/{symbol}",
            "/api/smart-money/addresses",
            "/api/smart-money/transactions/{token_symbol}",
            "/api/smart-money/signals",
            "/api/smart-money/signals/{token_symbol}",
            "/api/smart-money/dashboard"
        ]
    }


# 注释掉这个端点，因为 main.py 中有更完整的实现（带缓存和enhanced_dashboard）
# @router.get("/api/dashboard")
# async def get_dashboard(session: Session = Depends(get_db_session)):
#     """
#     获取仪表盘数据
#     包含: 最新价格、投资建议、新闻
#     """
#     try:
#         analysis_service = AnalysisService(session)
#         data = analysis_service.get_dashboard_data()
#         return {"success": True, "data": data}
#     except Exception as e:
#         logger.error(f"获取仪表盘数据失败: {e}")
#         raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/prices")
async def get_prices(limit: int = 20, session: Session = Depends(get_db_session)):
    """获取最新价格列表"""
    try:
        analysis_service = AnalysisService(session)
        prices = analysis_service.get_latest_prices(limit=limit)
        return {"success": True, "data": prices}
    except Exception as e:
        logger.error(f"获取价格失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/analysis/{symbol}")
async def get_analysis(symbol: str, session: Session = Depends(get_db_session)):
    """
    获取指定币种的详细分析
    包含: 技术指标、投资建议、新闻情绪
    """
    try:
        analysis_service = AnalysisService(session)
        advice = analysis_service.generate_investment_advice(symbol)
        return {"success": True, "data": advice}
    except Exception as e:
        logger.error(f"获取分析失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/kline/{symbol}")
async def get_kline(
    symbol: str,
    timeframe: str = '1h',
    limit: int = 100,
    session: Session = Depends(get_db_session)
):
    """获取K线数据"""
    try:
        analysis_service = AnalysisService(session)
        df = analysis_service.get_kline_data(symbol, timeframe, limit)

        if df.empty:
            return {"success": True, "data": []}

        # 转换为JSON格式
        kline_data = df.to_dict('records')
        # 转换timestamp为字符串
        for item in kline_data:
            if 'timestamp' in item:
                item['timestamp'] = item['timestamp'].strftime('%Y-%m-%d %H:%M:%S')

        return {"success": True, "data": kline_data}
    except Exception as e:
        logger.error(f"获取K线数据失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/news")
async def get_news(
    symbol: str = None,
    hours: int = 24,
    limit: int = 50,
    session: Session = Depends(get_db_session)
):
    """获取新闻列表（使用UTC时间）"""
    try:
        from app.database.models import NewsData
        from sqlalchemy import desc
        from datetime import datetime, timedelta, timezone

        # 使用UTC时间计算24小时范围
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)
        # 转换为naive datetime以便与数据库中的时间比较（数据库存储的是UTC时间的naive datetime）
        cutoff_time = cutoff_time.replace(tzinfo=None)
        query = session.query(NewsData).filter(
            NewsData.published_datetime >= cutoff_time
        )

        if symbol:
            symbol_code = symbol.split('/')[0] if '/' in symbol else symbol
            query = query.filter(NewsData.symbols.like(f'%{symbol_code}%'))

        news_list = query.order_by(desc(NewsData.published_datetime)).limit(limit).all()

        data = [{
            'title': n.title,
            'source': n.source,
            'sentiment': n.sentiment,
            'symbols': n.symbols,
            'published_at': n.published_datetime.strftime('%Y-%m-%d %H:%M UTC') if n.published_datetime else '',
            'url': n.url,
            'description': n.description
        } for n in news_list]

        return {"success": True, "data": data}
    except Exception as e:
        logger.error(f"获取新闻失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/sentiment/{symbol}")
async def get_sentiment(
    symbol: str,
    hours: int = 24,
    session: Session = Depends(get_db_session)
):
    """获取新闻情绪分析"""
    try:
        analysis_service = AnalysisService(session)
        sentiment = analysis_service.get_news_sentiment(symbol, hours)
        return {"success": True, "data": sentiment}
    except Exception as e:
        logger.error(f"获取情绪分析失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/funding-rate/{symbol}")
async def get_funding_rate(
    symbol: str,
    session: Session = Depends(get_db_session)
):
    """获取资金费率数据"""
    try:
        analysis_service = AnalysisService(session)
        funding_rate = analysis_service.get_funding_rate(symbol)

        if not funding_rate:
            return {
                "success": False,
                "message": f"未找到 {symbol} 的资金费率数据"
            }

        return {"success": True, "data": funding_rate}
    except Exception as e:
        logger.error(f"获取资金费率失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== 聪明钱监控API ====================

@router.get("/api/smart-money/addresses")
async def get_smart_money_addresses(blockchain: str = None):
    """
    获取监控的聪明钱地址列表

    Args:
        blockchain: 区块链网络(可选): ethereum, bsc

    Returns:
        监控地址列表及统计信息
    """
    try:
        db_service = get_db_service()
        addresses = db_service.get_smart_money_addresses(blockchain=blockchain, active_only=True)

        return {
            "success": True,
            "data": {
                "addresses": addresses,
                "total_count": len(addresses)
            }
        }
    except Exception as e:
        logger.error(f"获取聪明钱地址失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/smart-money/transactions/{token_symbol}")
async def get_smart_money_transactions(
    token_symbol: str,
    hours: int = 24,
    action: str = None,
    limit: int = 100
):
    """
    获取指定代币的聪明钱交易记录

    Args:
        token_symbol: 代币符号(如 BTC, ETH)
        hours: 时间范围(小时)
        action: 交易类型(可选): buy, sell
        limit: 返回数量限制

    Returns:
        交易记录列表
    """
    try:
        db_service = get_db_service()
        transactions = db_service.get_recent_smart_money_transactions(
            token_symbol=token_symbol,
            hours=hours,
            action=action,
            limit=limit
        )

        # 统计买卖情况
        buy_count = sum(1 for tx in transactions if tx['action'] == 'buy')
        sell_count = sum(1 for tx in transactions if tx['action'] == 'sell')
        total_buy_usd = sum(tx['amount_usd'] for tx in transactions if tx['action'] == 'buy')
        total_sell_usd = sum(tx['amount_usd'] for tx in transactions if tx['action'] == 'sell')

        return {
            "success": True,
            "data": {
                "transactions": transactions,
                "statistics": {
                    "total_count": len(transactions),
                    "buy_count": buy_count,
                    "sell_count": sell_count,
                    "total_buy_usd": round(total_buy_usd, 2),
                    "total_sell_usd": round(total_sell_usd, 2),
                    "net_flow_usd": round(total_buy_usd - total_sell_usd, 2)
                }
            }
        }
    except Exception as e:
        logger.error(f"获取聪明钱交易失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/smart-money/signals")
async def get_smart_money_signals(limit: int = 10):
    """
    获取活跃的聪明钱信号列表

    Args:
        limit: 返回数量限制

    Returns:
        聪明钱信号列表,按置信度排序
    """
    try:
        db_service = get_db_service()
        signals = db_service.get_active_smart_money_signals(limit=limit)

        return {
            "success": True,
            "data": {
                "signals": signals,
                "total_count": len(signals)
            }
        }
    except Exception as e:
        logger.error(f"获取聪明钱信号失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/smart-money/signals/{token_symbol}")
async def get_token_smart_money_signal(token_symbol: str):
    """
    获取指定代币的最新聪明钱信号

    Args:
        token_symbol: 代币符号(如 BTC, ETH)

    Returns:
        聪明钱信号详情
    """
    try:
        db_service = get_db_service()
        signal = db_service.get_smart_money_signal_by_token(token_symbol)

        if not signal:
            return {
                "success": False,
                "message": f"未找到 {token_symbol} 的聪明钱信号"
            }

        return {"success": True, "data": signal}
    except Exception as e:
        logger.error(f"获取代币聪明钱信号失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/smart-money/dashboard")
async def get_smart_money_dashboard():
    """
    聪明钱监控仪表盘

    Returns:
        聪明钱活动概览,包括活跃信号、最近大额交易、热门代币等
    """
    try:
        db_service = get_db_service()

        # 获取活跃信号
        active_signals = db_service.get_active_smart_money_signals(limit=5)

        # 获取最近大额交易
        recent_transactions = db_service.get_recent_smart_money_transactions(
            hours=24,
            limit=20
        )
        large_transactions = [
            tx for tx in recent_transactions
            if tx.get('is_large_transaction', False)
        ][:10]

        # 获取监控地址数量
        addresses = db_service.get_smart_money_addresses(active_only=True)

        # 统计热门代币(最近24小时交易最多的)
        from collections import Counter
        token_counter = Counter(tx['token_symbol'] for tx in recent_transactions)
        top_tokens = [
            {"token": token, "transaction_count": count}
            for token, count in token_counter.most_common(10)
        ]

        # 统计买卖比例
        buy_count = sum(1 for tx in recent_transactions if tx['action'] == 'buy')
        sell_count = sum(1 for tx in recent_transactions if tx['action'] == 'sell')

        return {
            "success": True,
            "data": {
                "active_signals": active_signals,
                "large_transactions": large_transactions,
                "top_active_tokens": top_tokens,
                "statistics": {
                    "monitored_addresses_count": len(addresses),
                    "total_transactions_24h": len(recent_transactions),
                    "large_transactions_24h": len(large_transactions),
                    "buy_count_24h": buy_count,
                    "sell_count_24h": sell_count,
                    "buy_sell_ratio": round(buy_count / sell_count, 2) if sell_count > 0 else 0
                }
            }
        }
    except Exception as e:
        logger.error(f"获取聪明钱仪表盘失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/ema-signals")
async def get_ema_signals(
    limit: int = 20,
    signal_type: Optional[str] = None,
    days: int = 2,
    hours: Optional[int] = None,
    session: Session = Depends(get_db_session)
):
    """
    获取EMA信号列表

    Args:
        limit: 返回数量限制
        signal_type: 信号类型过滤 (BUY或SELL)
        days: 查询最近N天的信号 (默认2天)
        hours: 查询最近N小时的信号 (优先级高于days)

    Returns:
        EMA信号列表
    """
    try:
        # 构建查询 - 添加时间范围过滤
        if hours is not None:
            # 使用UTC时间确保时区一致性
            from datetime import datetime, timedelta, timezone
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)
            query = """
                SELECT
                    symbol, timeframe, signal_type, signal_strength,
                    timestamp, price, short_ema, long_ema,
                    ema_config, volume_ratio, volume_type, price_change_pct, ema_distance_pct,
                    created_at
                FROM ema_signals
                WHERE timestamp >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL :hours HOUR)
            """
            params = {'limit': limit, 'hours': hours}
        else:
            query = """
                SELECT
                    symbol, timeframe, signal_type, signal_strength,
                    timestamp, price, short_ema, long_ema,
                    ema_config, volume_ratio, volume_type, price_change_pct, ema_distance_pct,
                    created_at
                FROM ema_signals
                WHERE timestamp >= DATE_SUB(NOW(), INTERVAL :days DAY)
            """
            params = {'limit': limit, 'days': days}

        if signal_type:
            query += " AND signal_type = :signal_type"
            params['signal_type'] = signal_type.upper()

        query += " ORDER BY timestamp DESC LIMIT :limit"

        result = session.execute(text(query), params)
        signals = []
        
        # 获取当前时间（用于前端过滤，使用UTC时间）
        from datetime import datetime, timedelta, timezone
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours if hours is not None else (days * 24))
        # 转换为naive datetime以便与数据库中的时间比较
        cutoff_time = cutoff_time.replace(tzinfo=None)

        for row in result:
            volume_ratio = float(row.volume_ratio) if row.volume_ratio else 0.0
            # 确保时间戳正确转换
            timestamp = row.timestamp
            if timestamp and hasattr(timestamp, 'isoformat'):
                timestamp_str = timestamp.isoformat()
            elif timestamp:
                timestamp_str = str(timestamp)
            else:
                timestamp_str = None
                
            signals.append({
                'symbol': row.symbol,
                'timeframe': row.timeframe,
                'signal_type': row.signal_type,
                'signal_strength': row.signal_strength,
                'timestamp': timestamp_str,
                'price': float(row.price),
                'short_ema': float(row.short_ema),
                'long_ema': float(row.long_ema),
                'ema_config': row.ema_config,
                'volume_ratio': volume_ratio,
                'volume_type': row.volume_type if hasattr(row, 'volume_type') and row.volume_type else ('放量' if volume_ratio > 1 else '缩量'),  # 成交量类型
                'volume_multiple': volume_ratio,  # 添加 volume_multiple 字段，前端使用
                'price_change_pct': float(row.price_change_pct),
                'ema_distance_pct': float(row.ema_distance_pct),
                'created_at': row.created_at.isoformat() if row.created_at else None
            })

        return {
            "success": True,
            "data": signals,
            "count": len(signals)
        }

    except Exception as e:
        logger.error(f"获取EMA信号失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ==================== Dashboard 快照 API ====================

@router.get("/api/dashboard/snapshot")
async def get_dashboard_snapshot():
    """
    读取预计算的 Dashboard 快照（由调度器每5分钟更新一次）。
    响应时间通常 <10ms，无任何实时计算。
    """
    try:
        import pymysql
        import os
        import json

        conn = pymysql.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            user=os.getenv('DB_USER', ''),
            password=os.getenv('DB_PASSWORD', ''),
            database=os.getenv('DB_NAME', ''),
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=5
        )
        cursor = conn.cursor()
        cursor.execute("""
            SELECT snapshot_json, updated_at, compute_ms
            FROM dashboard_snapshot
            WHERE snapshot_key = 'main'
        """)
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if not row:
            raise HTTPException(status_code=503, detail="Snapshot not yet generated, please retry in 30 seconds")

        data = json.loads(row['snapshot_json'])
        data['updated_at'] = row['updated_at'].isoformat() if row['updated_at'] else None
        data['compute_ms'] = row['compute_ms']
        return {'success': True, 'data': data}

    except HTTPException:
        raise
    except Exception as e:
        # 表不存在（首次启动，调度任务尚未运行）→ 当作 503 而非 500
        err_str = str(e)
        if "doesn't exist" in err_str or "1146" in err_str:
            raise HTTPException(status_code=503, detail="Snapshot table not yet created, retry in 60 seconds")
        logger.error(f"读取Dashboard快照失败: {e}")
        raise HTTPException(status_code=500, detail=err_str)


# ==================== Hyperliquid聪明钱交易API ====================

@router.get("/api/hyperliquid/cached")
async def get_hyperliquid_cached(
    hours: int = 24,
    min_usd: float = 100000,
    limit: int = 30
):
    """
    从DB缓存读取Hyperliquid聪明钱数据（dashboard专用快速接口）
    统计来自 hyperliquid_symbol_aggregation，交易明细来自 hyperliquid_wallet_trades
    """
    try:
        import pymysql
        import os

        conn = pymysql.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            user=os.getenv('DB_USER', ''),
            password=os.getenv('DB_PASSWORD', ''),
            database=os.getenv('DB_NAME', ''),
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=10
        )
        cursor = conn.cursor()

        # 1. 聚合统计 from hyperliquid_symbol_aggregation (period='24h')
        cursor.execute("""
            SELECT
                COALESCE(SUM(total_trades), 0) AS total_count,
                COALESCE(SUM(long_trades), 0)  AS long_count,
                COALESCE(SUM(short_trades), 0) AS short_count,
                COALESCE(SUM(net_flow), 0)     AS net_flow_usd,
                COUNT(DISTINCT symbol)         AS unique_coins,
                MAX(updated_at)                AS last_updated
            FROM hyperliquid_symbol_aggregation
            WHERE period = '24h'
        """)
        agg = cursor.fetchone() or {}

        long_count  = int(agg.get('long_count')  or 0)
        short_count = int(agg.get('short_count') or 0)
        ls_ratio = round(long_count / short_count, 2) if short_count > 0 else 0

        # 2. 唯一钱包数 from wallet_trades (24h)
        cursor.execute("""
            SELECT COUNT(DISTINCT address) AS unique_wallets
            FROM hyperliquid_wallet_trades
            WHERE trade_time >= NOW() - INTERVAL %s HOUR
        """, (hours,))
        wallet_row = cursor.fetchone() or {}
        unique_wallets = int(wallet_row.get('unique_wallets') or 0)

        statistics = {
            'total_count':      int(agg.get('total_count') or 0),
            'long_count':       long_count,
            'short_count':      short_count,
            'net_flow_usd':     float(agg.get('net_flow_usd') or 0),
            'unique_wallets':   unique_wallets,
            'unique_coins':     int(agg.get('unique_coins') or 0),
            'long_short_ratio': ls_ratio,
        }

        # 3. 近期大额交易
        cursor.execute("""
            SELECT coin, side, price, size, notional_usd, closed_pnl, trade_time
            FROM hyperliquid_wallet_trades
            WHERE trade_time >= NOW() - INTERVAL %s HOUR
              AND notional_usd >= %s
            ORDER BY notional_usd DESC
            LIMIT %s
        """, (hours, min_usd, limit))
        trades = []
        for t in cursor.fetchall():
            trades.append({
                'coin':        t['coin'],
                'action':      t['side'],
                'side':        t['side'],
                'price':       float(t['price']),
                'size':        float(t['size']),
                'notional_usd': float(t['notional_usd']),
                'closed_pnl':  float(t['closed_pnl']),
                'timestamp':   t['trade_time'].isoformat() if t['trade_time'] else None,
            })

        cursor.close()
        conn.close()

        return {
            'success': True,
            'data': {
                'trades':     trades,
                'statistics': statistics
            }
        }

    except Exception as e:
        logger.error(f"获取Hyperliquid缓存数据失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/hyperliquid/trades")
async def get_hyperliquid_smart_money_trades(
    hours: int = 168,  # 默认7天（7*24=168小时）
    min_usd: float = 50000,
    limit: int = 200,
    coin: Optional[str] = None,
    side: Optional[str] = None
):
    """
    获取 Hyperliquid 前100名聪明钱在指定时间内的交易

    Args:
        hours: 时间窗口（小时），默认24
        min_usd: 最小交易金额（USD），默认50000
        limit: 返回数量限制，默认200
        coin: 币种过滤（可选），如 BTC, ETH
        side: 方向过滤（可选），LONG 或 SHORT

    Returns:
        交易列表及统计信息
    """
    try:
        from app.collectors.hyperliquid_collector import HyperliquidCollector
        from app.utils.config_loader import load_config
        import asyncio

        # 加载配置（支持环境变量）
        config = load_config()

        # 创建采集器
        collector = HyperliquidCollector(config)

        # 抓取数据（调用新方法）
        logger.info(f"API: 开始抓取 Hyperliquid 聪明钱交易（{hours}h, ≥${min_usd:,.0f}）")
        trades = await collector.fetch_top_smart_money_trades_24h(
            top_n=100,
            min_trade_usd=min_usd,
            hours=hours
        )

        # 过滤
        if coin:
            trades = [t for t in trades if t.get('coin', '').upper() == coin.upper()]

        if side:
            trades = [t for t in trades if t.get('side', '').upper() == side.upper()]

        # 限制数量
        trades = trades[:limit]

        # 统计
        long_trades = [t for t in trades if t.get('side') == 'LONG']
        short_trades = [t for t in trades if t.get('side') == 'SHORT']

        total_long_usd = sum(t.get('notional_usd', 0) for t in long_trades)
        total_short_usd = sum(t.get('notional_usd', 0) for t in short_trades)

        unique_wallets = len(set(t.get('address') for t in trades))
        unique_coins = len(set(t.get('coin') for t in trades))

        # 计算多空比
        long_short_ratio = len(long_trades) / len(short_trades) if len(short_trades) > 0 else 0

        logger.info(f"API: 返回 {len(trades)} 笔交易")

        return {
            "success": True,
            "data": {
                "trades": trades,
                "statistics": {
                    "total_count": len(trades),
                    "long_count": len(long_trades),
                    "short_count": len(short_trades),
                    "total_long_usd": round(total_long_usd, 2),
                    "total_short_usd": round(total_short_usd, 2),
                    "net_flow_usd": round(total_long_usd - total_short_usd, 2),
                    "unique_wallets": unique_wallets,
                    "unique_coins": unique_coins,
                    "long_short_ratio": round(long_short_ratio, 2),
                    "time_range_hours": hours,
                    "min_trade_usd": min_usd
                }
            }
        }

    except asyncio.CancelledError:
        logger.warning("API请求被取消")
        raise HTTPException(status_code=503, detail="请求被取消")
    except Exception as e:
        logger.error(f"获取 Hyperliquid 聪明钱交易失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
