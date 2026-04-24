#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
品种黑名单管理 API
（策略级别：list / live / whale / bigmid 都会读这张表的 is_active=1 记录，
 合并各自模块顶部的 BASE 黑名单，品种池刷新时生效）
"""

import os
from datetime import datetime
from typing import Optional, List

import pymysql
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from loguru import logger

router = APIRouter()


def _conn():
    return pymysql.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "3306")),
        user=os.getenv("DB_USER", ""),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", ""),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


class SymbolBlacklistCreate(BaseModel):
    symbol: str = Field(..., description="交易对，如 BTC/USDT")
    reason: Optional[str] = Field(None, description="拉黑原因")


@router.get("/api/symbol_blacklist")
async def list_blacklist(active_only: int = 1):
    """列出所有拉黑品种（默认只返回 is_active=1 的）"""
    try:
        conn = _conn()
        try:
            with conn.cursor() as c:
                if active_only:
                    c.execute(
                        "SELECT symbol, reason, created_by, is_active, "
                        "       created_at, updated_at "
                        "FROM symbol_blacklist WHERE is_active=1 ORDER BY updated_at DESC"
                    )
                else:
                    c.execute(
                        "SELECT symbol, reason, created_by, is_active, "
                        "       created_at, updated_at "
                        "FROM symbol_blacklist ORDER BY is_active DESC, updated_at DESC"
                    )
                rows = c.fetchall()
            # 时间字段转 ISO 字符串方便前端
            for r in rows:
                for k in ("created_at", "updated_at"):
                    if isinstance(r.get(k), datetime):
                        r[k] = r[k].isoformat()
            return {"success": True, "data": rows, "total": len(rows)}
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"list_blacklist 失败: {e}")
        raise HTTPException(500, str(e))


@router.post("/api/symbol_blacklist")
async def add_blacklist(req: SymbolBlacklistCreate):
    """添加或重新激活一个品种黑名单"""
    sym = req.symbol.strip().upper()
    if "/" not in sym or not sym.endswith(("/USDT", "/USDC", "/USD")):
        raise HTTPException(400, f"symbol 格式错误: {req.symbol}")
    reason = (req.reason or "").strip()[:200]
    try:
        conn = _conn()
        try:
            with conn.cursor() as c:
                c.execute(
                    "INSERT INTO symbol_blacklist (symbol, reason, created_by, is_active) "
                    "VALUES (%s, %s, 'manual', 1) "
                    "ON DUPLICATE KEY UPDATE "
                    "  is_active=1, reason=VALUES(reason), updated_at=NOW()",
                    (sym, reason or None),
                )
            logger.info(f"[symbol_blacklist] 加入 {sym} reason={reason!r}")
            return {"success": True, "symbol": sym, "reason": reason}
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"add_blacklist 失败: {e}")
        raise HTTPException(500, str(e))


@router.delete("/api/symbol_blacklist/{symbol:path}")
async def remove_blacklist(symbol: str):
    """从黑名单移除（软删：is_active=0；保留历史）"""
    sym = symbol.strip().upper()
    if "/" not in sym:
        # URL 可能 encode，前端传 BTCUSDT 需要自己补 /USDT
        raise HTTPException(400, f"symbol 格式错误: {symbol}")
    try:
        conn = _conn()
        try:
            with conn.cursor() as c:
                affected = c.execute(
                    "UPDATE symbol_blacklist SET is_active=0, updated_at=NOW() "
                    "WHERE symbol=%s AND is_active=1",
                    (sym,),
                )
            if affected:
                logger.info(f"[symbol_blacklist] 解除 {sym}")
                return {"success": True, "symbol": sym, "action": "deactivated"}
            return {"success": True, "symbol": sym, "action": "not_found_or_already_inactive"}
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"remove_blacklist 失败: {e}")
        raise HTTPException(500, str(e))
