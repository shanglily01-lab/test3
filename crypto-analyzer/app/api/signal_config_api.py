#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
信号配置管理 API
查询/启用/禁用 signal_scoring_weights 中的各信号
GET  /api/signal_config          - 查询所有信号权重及状态
POST /api/signal_config/toggle   - 批量切换 is_active
PUT  /api/signal_config/{signal} - 修改单个信号权重 + is_active
GET  /api/signal_config/review   - 读取最近N天信号表现（signal_performance_daily）
"""

import os
import pymysql
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional, List
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

router = APIRouter()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', ''),
    'database': os.getenv('DB_NAME', 'binance-data'),
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor,
}


def _get_conn():
    return pymysql.connect(**DB_CONFIG)


class SignalToggleRequest(BaseModel):
    signal_components: List[str]
    is_active: bool


class SignalUpdateRequest(BaseModel):
    weight_long: Optional[float] = None
    weight_short: Optional[float] = None
    is_active: Optional[bool] = None
    description: Optional[str] = None


# ---------------------------------------------------------------------------
# 查询所有信号权重
# ---------------------------------------------------------------------------

@router.get("/api/signal_config")
def list_signal_configs(active_only: bool = False):
    """列出所有信号权重配置"""
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            sql = """
                SELECT signal_component, strategy_type, weight_long, weight_short,
                       base_weight, is_active, description, performance_score,
                       last_adjusted, updated_at
                FROM signal_scoring_weights
                WHERE strategy_type = 'default'
            """
            if active_only:
                sql += " AND is_active = 1"
            sql += " ORDER BY signal_component"
            cur.execute(sql)
            rows = cur.fetchall()
        return {"total": len(rows), "signals": rows}
    except Exception as e:
        logger.error(f"list_signal_configs error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 批量切换启用/禁用
# ---------------------------------------------------------------------------

@router.post("/api/signal_config/toggle")
def toggle_signals(req: SignalToggleRequest):
    """
    批量启用或禁用信号
    Body: { "signal_components": ["stoch_rsi_bull", "bb_squeeze_bear"], "is_active": false }
    """
    if not req.signal_components:
        raise HTTPException(status_code=400, detail="signal_components cannot be empty")
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            placeholders = ','.join(['%s'] * len(req.signal_components))
            cur.execute(f"""
                UPDATE signal_scoring_weights
                SET is_active = %s, updated_at = NOW()
                WHERE signal_component IN ({placeholders})
                  AND strategy_type = 'default'
            """, [int(req.is_active)] + req.signal_components)
            affected = cur.rowcount
        conn.commit()
        action = "enabled" if req.is_active else "disabled"
        logger.info(f"[SIGNAL-CONFIG] {action} {affected} signals: {req.signal_components}")
        return {"success": True, "affected": affected, "action": action}
    except Exception as e:
        conn.rollback()
        logger.error(f"toggle_signals error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 修改单个信号
# ---------------------------------------------------------------------------

@router.put("/api/signal_config/{signal_component}")
def update_signal(signal_component: str, req: SignalUpdateRequest):
    """修改单个信号的权重和/或启用状态"""
    updates = {}
    if req.weight_long is not None:
        updates['weight_long'] = req.weight_long
    if req.weight_short is not None:
        updates['weight_short'] = req.weight_short
    if req.is_active is not None:
        updates['is_active'] = int(req.is_active)
    if req.description is not None:
        updates['description'] = req.description

    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            set_clause = ', '.join(f"{k} = %s" for k in updates)
            values = list(updates.values()) + [signal_component]
            cur.execute(f"""
                UPDATE signal_scoring_weights
                SET {set_clause}, updated_at = NOW()
                WHERE signal_component = %s AND strategy_type = 'default'
            """, values)
            affected = cur.rowcount
        conn.commit()
        if affected == 0:
            raise HTTPException(status_code=404, detail=f"Signal '{signal_component}' not found")
        logger.info(f"[SIGNAL-CONFIG] updated {signal_component}: {updates}")
        return {"success": True, "signal_component": signal_component, "updates": updates}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        logger.error(f"update_signal error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 信号表现查询
# ---------------------------------------------------------------------------

@router.get("/api/signal_config/review")
def get_signal_review(days: int = 7):
    """
    查询最近 N 天的信号表现汇总（来自 signal_performance_daily）
    若表不存在则返回空列表
    """
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            try:
                cur.execute("""
                    SELECT report_date, signal_component, trade_count, win_count,
                           loss_count, win_rate, total_pnl, avg_pnl,
                           avg_win_pnl, avg_loss_pnl
                    FROM signal_performance_daily
                    WHERE report_date >= DATE_SUB(CURDATE(), INTERVAL %s DAY)
                    ORDER BY report_date DESC, total_pnl ASC
                """, (days,))
                rows = cur.fetchall()
            except Exception:
                rows = []
        return {"days": days, "total": len(rows), "records": rows}
    except Exception as e:
        logger.error(f"get_signal_review error: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()
