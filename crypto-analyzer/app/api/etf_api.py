"""
ETF数据API
提供加密货币ETF持仓、资金流向等数据
"""

from fastapi import APIRouter, HTTPException
from typing import Optional
from datetime import datetime, timedelta
import mysql.connector
from mysql.connector import pooling
from pathlib import Path
from decimal import Decimal
from app.services.price_cache_service import get_global_price_cache
from app.utils.config_loader import load_config

router = APIRouter()

# 从 config.yaml 加载数据库配置
project_root = Path(__file__).parent.parent.parent
config_path = project_root / "config.yaml"
connection_pool = None
_init_failed = False


def get_db_connection():
    """获取数据库连接（延迟初始化连接池）"""
    global connection_pool, _init_failed

    if _init_failed:
        raise HTTPException(status_code=500, detail="数据库连接池初始化失败")

    if connection_pool is None:
        try:
            if not config_path.exists():
                _init_failed = True
                raise HTTPException(status_code=500, detail=f"config.yaml 不存在: {config_path}")

            # 使用 config_loader 加载配置，自动替换环境变量
            config = load_config(config_path)

            mysql_config = config.get('database', {}).get('mysql', {})

            db_config = {
                "host": mysql_config.get('host', 'localhost'),
                "port": mysql_config.get('port', 3306),
                "user": mysql_config.get('user', 'root'),
                "password": mysql_config.get('password', ''),
                "database": mysql_config.get('database', 'binance-data'),
                "pool_name": "etf_api_pool",
                "pool_size": 10,  # 增加连接池大小
                "pool_reset_session": True,
                "autocommit": True
            }

            connection_pool = pooling.MySQLConnectionPool(**db_config)
            print(f"✅ ETF API数据库连接池创建成功")

        except HTTPException:
            raise
        except mysql.connector.Error as e:
            _init_failed = True
            raise HTTPException(status_code=500, detail=f"MySQL连接失败: {str(e)}")
        except Exception as e:
            _init_failed = True
            raise HTTPException(status_code=500, detail=f"初始化失败: {str(e)}")

    try:
        conn = connection_pool.get_connection()
        return conn
    except mysql.connector.Error as e:
        raise HTTPException(status_code=500, detail=f"获取数据库连接失败: {str(e)}")


def decimal_to_float(obj):
    """将Decimal转换为float"""
    if isinstance(obj, Decimal):
        return float(obj)
    return obj


@router.get("/api/etf/summary")
async def get_etf_summary():
    """
    获取ETF数据总览

    返回:
    - 总览统计：ETF数量、BTC/ETH总持仓、总市值、24h资金流入
    - BTC ETF列表
    - ETH ETF列表
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        # 获取最新交易日期
        cursor.execute("""
            SELECT MAX(trade_date) as latest_date
            FROM crypto_etf_flows
        """)
        latest_date_result = cursor.fetchone()
        latest_date = latest_date_result['latest_date'] if latest_date_result else None

        if not latest_date:
            return {
                "success": True,
                "data": {
                    "summary": {
                        "total_etfs": 0,
                        "total_btc": 0,
                        "total_eth": 0,
                        "total_value_usd": 0,
                        "net_flow_24h": 0
                    },
                    "btc_etfs": [],
                    "eth_etfs": []
                },
                "message": "暂无ETF数据",
                "timestamp": datetime.now().isoformat()
            }

        # 获取BTC价格和ETH价格
        # 方案1: 从价格缓存服务获取（实时价格）
        btc_price = 0
        eth_price = 0

        price_cache = get_global_price_cache()
        if price_cache:
            btc_price_decimal = price_cache.get_price('BTC/USDT')
            eth_price_decimal = price_cache.get_price('ETH/USDT')

            if btc_price_decimal and btc_price_decimal > 0:
                btc_price = float(btc_price_decimal)
            if eth_price_decimal and eth_price_decimal > 0:
                eth_price = float(eth_price_decimal)

        # 方案2: 如果价格缓存没有数据，从price_data表获取
        if btc_price == 0 or eth_price == 0:
            try:
                cursor.execute("""
                    SELECT symbol, close_price
                    FROM price_data
                    WHERE symbol IN ('BTCUSDT', 'ETHUSDT')
                    ORDER BY timestamp DESC
                    LIMIT 2
                """)
                prices = cursor.fetchall()
                for p in prices:
                    if p['symbol'] == 'BTCUSDT' and btc_price == 0:
                        btc_price = float(p['close_price']) if p['close_price'] else 0
                    elif p['symbol'] == 'ETHUSDT' and eth_price == 0:
                        eth_price = float(p['close_price']) if p['close_price'] else 0
            except Exception as e:
                print(f"⚠️ 从price_data表获取价格失败: {e}")

        # 方案3: 如果还是没有数据，使用默认值
        if btc_price == 0:
            btc_price = 100000  # 默认BTC价格
        if eth_price == 0:
            eth_price = 3500    # 默认ETH价格

        # 获取最新一天的所有ETF数据
        cursor.execute("""
            SELECT
                p.ticker,
                p.full_name as name,
                p.provider,
                p.asset_type,
                f.btc_holdings,
                f.eth_holdings,
                f.net_inflow as flow_24h,
                f.aum,
                f.trade_date as updated_at
            FROM crypto_etf_flows f
            JOIN crypto_etf_products p ON f.etf_id = p.id
            WHERE f.trade_date = %s
              AND p.is_active = TRUE
            ORDER BY
                CASE
                    WHEN p.asset_type = 'BTC' THEN 1
                    WHEN p.asset_type = 'ETH' THEN 2
                    ELSE 3
                END,
                f.btc_holdings DESC,
                f.eth_holdings DESC
        """, (latest_date,))

        etfs = cursor.fetchall()

        # 分组并计算统计
        btc_etfs = []
        eth_etfs = []
        total_btc = 0
        total_eth = 0
        total_value_usd = 0
        net_flow_24h = 0

        for etf in etfs:
            etf_data = {
                'symbol': etf['ticker'],
                'name': etf['name'],
                'provider': etf['provider'],
                'btc_holdings': decimal_to_float(etf['btc_holdings']) or 0,
                'eth_holdings': decimal_to_float(etf['eth_holdings']) or 0,
                'market_value': decimal_to_float(etf['aum']) or 0,
                'flow_24h': decimal_to_float(etf['flow_24h']) or 0,
                'updated_at': etf['updated_at'].isoformat() if etf['updated_at'] else None
            }

            if etf['asset_type'] == 'BTC':
                btc_holdings = etf_data['btc_holdings']
                etf_data['market_value'] = btc_holdings * btc_price
                total_btc += btc_holdings
                btc_etfs.append(etf_data)
            elif etf['asset_type'] == 'ETH':
                eth_holdings = etf_data['eth_holdings']
                etf_data['market_value'] = eth_holdings * eth_price
                total_eth += eth_holdings
                eth_etfs.append(etf_data)

            net_flow_24h += etf_data['flow_24h']
            total_value_usd += etf_data['market_value']

        # 汇总统计
        summary = {
            'total_etfs': len(etfs),
            'total_btc': round(total_btc, 2),
            'total_eth': round(total_eth, 2),
            'total_value_usd': round(total_value_usd, 2),
            'net_flow_24h': round(net_flow_24h, 2),
            'btc_price': round(btc_price, 2),
            'eth_price': round(eth_price, 2),
            'latest_date': latest_date.isoformat() if latest_date else None
        }

        return {
            "success": True,
            "data": {
                "summary": summary,
                "btc_etfs": btc_etfs,
                "eth_etfs": eth_etfs
            },
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"获取ETF数据失败: {str(e)}")
    finally:
        cursor.close()
        conn.close()


@router.get("/api/etf/flows")
async def get_etf_flows(
    asset_type: Optional[str] = None,
    days: int = 30
):
    """
    获取ETF资金流向历史数据

    参数:
    - asset_type: 资产类型 (BTC/ETH)，不指定则返回全部
    - days: 历史天数，默认30天
    """
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        where_clause = "WHERE f.trade_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)"
        params = [days]

        if asset_type:
            where_clause += " AND p.asset_type = %s"
            params.append(asset_type.upper())

        query = f"""
            SELECT
                f.trade_date,
                p.asset_type,
                SUM(f.net_inflow) as daily_net_inflow,
                SUM(f.gross_inflow) as daily_gross_inflow,
                SUM(f.gross_outflow) as daily_gross_outflow,
                COUNT(DISTINCT f.etf_id) as etf_count
            FROM crypto_etf_flows f
            JOIN crypto_etf_products p ON f.etf_id = p.id
            {where_clause}
            GROUP BY f.trade_date, p.asset_type
            ORDER BY f.trade_date DESC, p.asset_type
        """

        cursor.execute(query, tuple(params))
        flows = cursor.fetchall()

        # 格式化数据
        formatted_flows = []
        for flow in flows:
            formatted_flows.append({
                'trade_date': flow['trade_date'].isoformat() if flow['trade_date'] else None,
                'asset_type': flow['asset_type'],
                'net_inflow': decimal_to_float(flow['daily_net_inflow']) or 0,
                'gross_inflow': decimal_to_float(flow['daily_gross_inflow']) or 0,
                'gross_outflow': decimal_to_float(flow['daily_gross_outflow']) or 0,
                'etf_count': flow['etf_count'] or 0
            })

        return {
            "success": True,
            "data": formatted_flows,
            "count": len(formatted_flows),
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取资金流向数据失败: {str(e)}")
    finally:
        cursor.close()
        conn.close()


@router.post("/api/etf/sync-farside-btc")
async def post_sync_farside_btc():
    """
    手动触发：从 https://farside.co.uk/btc/ 抓取 BTC 现货 ETF 日度净流入并写入数据库。
    与调度器每日任务相同逻辑；Farside 表内单位为 M$（百万美元），入库为美元。
    """
    try:
        from app.services.farside_etf_sync import sync_farside_btc_flows

        config = load_config(config_path)
        mysql_config = config.get("database", {}).get("mysql", {})
        if not mysql_config:
            raise HTTPException(status_code=500, detail="config.yaml 中未配置 database.mysql")

        fu = config.get("farside_etf", {}).get("btc_url", "https://farside.co.uk/btc/")
        result = sync_farside_btc_flows(mysql_config, page_url=fu)
        return {"success": True, "data": result}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Farside 同步失败: {str(e)}")


@router.post("/api/etf/sync-farside-eth")
async def post_sync_farside_eth():
    """
    手动触发：从 https://farside.co.uk/eth/ 抓取 ETH 现货 ETF 日度净流入并写入数据库。
    与调度器每日任务相同逻辑；Farside 表内单位为 M$（百万美元），入库为美元。
    """
    try:
        from app.services.farside_etf_sync import sync_farside_eth_flows

        config = load_config(config_path)
        mysql_config = config.get("database", {}).get("mysql", {})
        if not mysql_config:
            raise HTTPException(status_code=500, detail="config.yaml 中未配置 database.mysql")

        fu = config.get("farside_etf", {}).get("eth_url", "https://farside.co.uk/eth/")
        result = sync_farside_eth_flows(mysql_config, page_url=fu)
        return {"success": True, "data": result}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Farside ETH 同步失败: {str(e)}")
