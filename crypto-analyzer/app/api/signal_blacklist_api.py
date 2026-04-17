#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
信号黑名单管理 API
提供 CRUD 接口，供前端管理页面调用
"""

import os
import pymysql
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from loguru import logger

router = APIRouter()

DB_CONFIG = {
    'host':     os.getenv('DB_HOST', 'localhost'),
    'port':     int(os.getenv('DB_PORT', '3306')),
    'user':     os.getenv('DB_USER', ''),
    'password': os.getenv('DB_PASSWORD', ''),
    'database': os.getenv('DB_NAME', ''),
    'charset':  'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor,
}


def _get_conn():
    return pymysql.connect(**DB_CONFIG)


class SignalBlacklistCreate(BaseModel):
    signal_type: str
    position_side: str          # LONG / SHORT
    is_active: Optional[int] = 1
    notes: Optional[str] = None
    reason: Optional[str] = None


class SignalBlacklistUpdate(BaseModel):
    signal_type: Optional[str] = None
    position_side: Optional[str] = None
    is_active: Optional[int] = None
    notes: Optional[str] = None
    reason: Optional[str] = None


# ── 查询列表 ────────────────────────────────────────────────────────────────

@router.get("/api/signal_blacklist")
async def list_signal_blacklist(
    side: Optional[str] = None,      # LONG / SHORT
    is_active: Optional[int] = None,  # 1 / 0
    q: Optional[str] = None,          # 信号类型关键词搜索
    page: int = 1,
    page_size: int = 50,
):
    """获取信号黑名单列表（支持筛选/分页）"""
    try:
        conn = _get_conn()
        cur = conn.cursor()

        conditions = []
        params = []

        if side and side.upper() in ('LONG', 'SHORT'):
            conditions.append("position_side = %s")
            params.append(side.upper())

        if is_active is not None:
            conditions.append("is_active = %s")
            params.append(is_active)

        if q:
            conditions.append("signal_type LIKE %s")
            params.append(f"%{q}%")

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        # 统计总数
        cur.execute(f"SELECT COUNT(*) AS cnt FROM signal_blacklist {where}", params)
        total = cur.fetchone()['cnt']

        # 分页数据
        offset = (page - 1) * page_size
        cur.execute(
            f"""
            SELECT id, signal_type, position_side, is_active,
                   win_rate, total_loss, order_count,
                   created_at, updated_at, reason, notes
            FROM signal_blacklist
            {where}
            ORDER BY id DESC
            LIMIT %s OFFSET %s
            """,
            params + [page_size, offset],
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()

        # 序列化
        data = []
        for r in rows:
            data.append({
                'id':            r['id'],
                'signal_type':   r['signal_type'],
                'position_side': r['position_side'],
                'is_active':     int(r['is_active'] or 0),
                'win_rate':      float(r['win_rate'] or 0),
                'total_loss':    float(r['total_loss'] or 0),
                'order_count':   int(r['order_count'] or 0),
                'reason':        r['reason'] or '',
                'notes':         r['notes'] or '',
                'created_at':    r['created_at'].isoformat() if r['created_at'] else None,
                'updated_at':    r['updated_at'].isoformat() if r['updated_at'] else None,
            })

        # 汇总统计
        cur2 = _get_conn().cursor()
        cur2.execute("SELECT COUNT(*) AS total, SUM(is_active=1) AS active, SUM(is_active=0) AS inactive FROM signal_blacklist")
        stats_row = cur2.fetchone()
        cur2.connection.close()

        from datetime import datetime, timedelta, timezone
        yesterday = (datetime.now(tz=timezone.utc) - timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')
        cur3 = _get_conn().cursor()
        cur3.execute("SELECT COUNT(*) AS cnt FROM signal_blacklist WHERE created_at >= %s", (yesterday,))
        today_count = cur3.fetchone()['cnt']
        cur3.connection.close()

        return {
            'success': True,
            'data': data,
            'total': total,
            'page': page,
            'page_size': page_size,
            'stats': {
                'total':    int(stats_row['total'] or 0),
                'active':   int(stats_row['active'] or 0),
                'inactive': int(stats_row['inactive'] or 0),
                'today':    int(today_count),
            }
        }
    except Exception as e:
        logger.error(f"获取信号黑名单失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── 新增 ────────────────────────────────────────────────────────────────────

@router.post("/api/signal_blacklist")
async def create_signal_blacklist(body: SignalBlacklistCreate):
    """新增信号黑名单条目"""
    if body.position_side.upper() not in ('LONG', 'SHORT'):
        raise HTTPException(status_code=400, detail="position_side 必须是 LONG 或 SHORT")
    if not body.signal_type.strip():
        raise HTTPException(status_code=400, detail="signal_type 不能为空")
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO signal_blacklist (signal_type, position_side, is_active, notes, reason)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                body.signal_type.strip(),
                body.position_side.upper(),
                body.is_active,
                body.notes,
                body.reason or '手动添加',
            )
        )
        new_id = cur.lastrowid
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"信号黑名单新增: id={new_id} {body.signal_type} {body.position_side}")
        return {'success': True, 'id': new_id, 'message': '添加成功'}
    except pymysql.err.IntegrityError:
        raise HTTPException(status_code=409, detail="该信号+方向组合已存在")
    except Exception as e:
        logger.error(f"新增信号黑名单失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── 编辑 ────────────────────────────────────────────────────────────────────

@router.put("/api/signal_blacklist/{item_id}")
async def update_signal_blacklist(item_id: int, body: SignalBlacklistUpdate):
    """更新信号黑名单条目"""
    try:
        conn = _get_conn()
        cur = conn.cursor()

        # 确认存在
        cur.execute("SELECT id FROM signal_blacklist WHERE id = %s", (item_id,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="条目不存在")

        fields = []
        params = []
        if body.signal_type is not None:
            fields.append("signal_type = %s")
            params.append(body.signal_type.strip())
        if body.position_side is not None:
            if body.position_side.upper() not in ('LONG', 'SHORT'):
                raise HTTPException(status_code=400, detail="position_side 必须是 LONG 或 SHORT")
            fields.append("position_side = %s")
            params.append(body.position_side.upper())
        if body.is_active is not None:
            fields.append("is_active = %s")
            params.append(body.is_active)
        if body.notes is not None:
            fields.append("notes = %s")
            params.append(body.notes)
        if body.reason is not None:
            fields.append("reason = %s")
            params.append(body.reason)

        if not fields:
            raise HTTPException(status_code=400, detail="没有可更新的字段")

        params.append(item_id)
        cur.execute(f"UPDATE signal_blacklist SET {', '.join(fields)} WHERE id = %s", params)
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"信号黑名单更新: id={item_id}")
        return {'success': True, 'message': '更新成功'}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新信号黑名单失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── 启停切换 ─────────────────────────────────────────────────────────────────

@router.post("/api/signal_blacklist/{item_id}/toggle")
async def toggle_signal_blacklist(item_id: int):
    """切换信号黑名单启用/停用状态"""
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("SELECT id, is_active FROM signal_blacklist WHERE id = %s", (item_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="条目不存在")

        new_state = 0 if row['is_active'] else 1
        cur.execute("UPDATE signal_blacklist SET is_active = %s WHERE id = %s", (new_state, item_id))
        conn.commit()
        cur.close()
        conn.close()
        state_label = '已启用' if new_state else '已停用'
        logger.info(f"信号黑名单切换: id={item_id} -> {state_label}")
        return {'success': True, 'is_active': new_state, 'message': state_label}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"切换信号黑名单状态失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ── 删除 ────────────────────────────────────────────────────────────────────

@router.delete("/api/signal_blacklist/{item_id}")
async def delete_signal_blacklist(item_id: int):
    """删除信号黑名单条目"""
    try:
        conn = _get_conn()
        cur = conn.cursor()
        cur.execute("SELECT id FROM signal_blacklist WHERE id = %s", (item_id,))
        if not cur.fetchone():
            raise HTTPException(status_code=404, detail="条目不存在")
        cur.execute("DELETE FROM signal_blacklist WHERE id = %s", (item_id,))
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"信号黑名单删除: id={item_id}")
        return {'success': True, 'message': '删除成功'}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除信号黑名单失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))
