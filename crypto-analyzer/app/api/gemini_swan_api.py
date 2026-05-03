"""
Gemini 红黑天鹅榜 API.

数据源: 远程 dimesion gemini_swan_runs + gemini_swan_verdicts.
连接方式: 复用 app.services.gemini_swan_worker._load_remote_db_cfg
         (从 table_schemas.txt 头部读 host, 不硬编码 IP).

端点:
  GET  /api/gemini-swan/latest          最新一次 success run + verdicts
  GET  /api/gemini-swan/history?days=7  历史 runs 列表 (元数据)
  GET  /api/gemini-swan/symbol/{base}   单 base 近 7 天每次 verdict (如 KNC -> KNC/USDT)
  POST /api/gemini-swan/trigger         立即跑一次 (后台线程, 立即返回 {run_id, status:running})
  GET  /api/gemini-swan/status/{run_id} 查询单次跑的状态
"""
from __future__ import annotations

import json
import threading
from typing import Optional

import pymysql
from fastapi import APIRouter, HTTPException
from loguru import logger

from app.services.gemini_swan_worker import _load_remote_db_cfg, run_swan_round

router = APIRouter(prefix="/api/gemini-swan", tags=["Gemini Swan"])

# 排序优先级
LEVEL_ORDER = {"STRONG": 0, "MODERATE": 1, "WEAK": 2, "SKIP": 3}


def _conn():
    return pymysql.connect(**_load_remote_db_cfg())


def _row_to_verdict(row: dict) -> dict:
    triggers = row.get("triggers")
    universe_data = row.get("universe_data")
    try:
        triggers = json.loads(triggers) if isinstance(triggers, str) else (triggers or [])
    except (TypeError, ValueError):
        triggers = []
    try:
        universe_data = json.loads(universe_data) if isinstance(universe_data, str) else (universe_data or {})
    except (TypeError, ValueError):
        universe_data = {}
    return {
        "symbol": row["symbol"],
        "main_category": row["main_category"],
        "consistency_level": row["consistency_level"],
        "avg_confidence": float(row["avg_confidence"]) if row["avg_confidence"] is not None else 0.0,
        "rounds_total": row["rounds_total"],
        "universe_appearances": row["universe_appearances"],
        "black_count": row["black_count"],
        "red_count": row["red_count"],
        "skip_count": row["skip_count"],
        "catalyst": row.get("catalyst"),
        "data_signal": row.get("data_signal"),
        "risk_note": row.get("risk_note"),
        "triggers": triggers,
        "universe_data": universe_data,
    }


def _row_to_run(row: dict) -> dict:
    return {
        "run_id": row["id"],
        "asof_utc": row["asof_utc"].isoformat() if row.get("asof_utc") else None,
        "model": row["model"],
        "rounds": row["rounds"],
        "universe_size": row["universe_size"],
        "summary_zh": row.get("summary_zh") or "",
        "elapsed_s": float(row["elapsed_s"]) if row.get("elapsed_s") is not None else None,
        "status": row["status"],
        "error_msg": row.get("error_msg"),
        "triggered_by": row.get("triggered_by") or "scheduler",
        "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
    }


@router.get("/latest")
async def latest():
    """最新一次 success/partial run + 全部 verdicts (level 排序)."""
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT * FROM gemini_swan_runs
                    WHERE status IN ('success', 'partial')
                    ORDER BY id DESC LIMIT 1
                    """
                )
                run_row = cur.fetchone()
                if not run_row:
                    return {"run": None, "verdicts": [], "summary": {}}
                cur.execute(
                    """
                    SELECT * FROM gemini_swan_verdicts
                    WHERE run_id = %s
                      AND main_category IN ('black_swan', 'red_swan')
                    """,
                    (run_row["id"],),
                )
                verdicts = [_row_to_verdict(r) for r in cur.fetchall()]
    except Exception as e:
        logger.error(f"swan latest error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

    verdicts.sort(key=lambda v: (
        LEVEL_ORDER.get(v["consistency_level"], 9),
        -v["avg_confidence"],
    ))
    summary = {
        "black_strong": sum(1 for v in verdicts
                            if v["main_category"] == "black_swan" and v["consistency_level"] == "STRONG"),
        "black_moderate": sum(1 for v in verdicts
                              if v["main_category"] == "black_swan" and v["consistency_level"] == "MODERATE"),
        "black_weak": sum(1 for v in verdicts
                          if v["main_category"] == "black_swan" and v["consistency_level"] == "WEAK"),
        "red_strong": sum(1 for v in verdicts
                          if v["main_category"] == "red_swan" and v["consistency_level"] == "STRONG"),
        "red_moderate": sum(1 for v in verdicts
                            if v["main_category"] == "red_swan" and v["consistency_level"] == "MODERATE"),
        "red_weak": sum(1 for v in verdicts
                        if v["main_category"] == "red_swan" and v["consistency_level"] == "WEAK"),
    }
    return {
        "run": _row_to_run(run_row),
        "verdicts": verdicts,
        "summary": summary,
    }


@router.get("/history")
async def history(days: int = 7, limit: int = 50):
    """历史 runs 元数据."""
    days = max(1, min(30, days))
    limit = max(1, min(200, limit))
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT * FROM gemini_swan_runs
                    WHERE asof_utc >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s DAY)
                    ORDER BY id DESC LIMIT %s
                    """,
                    (days, limit),
                )
                rows = cur.fetchall()
    except Exception as e:
        logger.error(f"swan history error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    return {"runs": [_row_to_run(r) for r in rows]}


@router.get("/symbol/{base}")
async def symbol_history(base: str, days: int = 7):
    """单 base 近 N 天的判定轨迹 (如 KNC -> KNC/USDT)."""
    base = base.upper().strip()
    if "/" not in base:
        symbol = f"{base}/USDT"
    else:
        symbol = base
    days = max(1, min(30, days))
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT v.*, r.asof_utc, r.status
                    FROM gemini_swan_verdicts v
                    JOIN gemini_swan_runs r ON r.id = v.run_id
                    WHERE v.symbol = %s
                      AND r.asof_utc >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s DAY)
                      AND r.status IN ('success', 'partial')
                    ORDER BY r.asof_utc DESC
                    """,
                    (symbol, days),
                )
                rows = cur.fetchall()
    except Exception as e:
        logger.error(f"swan symbol error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    history_list = []
    for r in rows:
        v = _row_to_verdict(r)
        v["asof_utc"] = r["asof_utc"].isoformat() if r.get("asof_utc") else None
        v["run_status"] = r.get("status")
        history_list.append(v)
    return {"symbol": symbol, "history": history_list}


# 手动触发 — 用线程跑, 不阻塞 HTTP 请求
_trigger_lock = threading.Lock()
_trigger_state: dict = {"running": False, "last_run_id": None, "last_status": None}


def _trigger_worker_thread():
    try:
        rid = run_swan_round(triggered_by="manual")
        with _trigger_lock:
            _trigger_state["last_run_id"] = rid
            _trigger_state["last_status"] = "success" if rid else "failed"
    except Exception as e:
        logger.error(f"swan trigger 异常: {e}", exc_info=True)
        with _trigger_lock:
            _trigger_state["last_status"] = "failed"
    finally:
        with _trigger_lock:
            _trigger_state["running"] = False


@router.post("/trigger")
async def trigger():
    """立即触发一次 swan 跑 (后台线程). 已有跑中任务时拒绝."""
    with _trigger_lock:
        if _trigger_state["running"]:
            return {
                "accepted": False,
                "message": "已有任务在跑, 请等待完成",
                "state": dict(_trigger_state),
            }
        _trigger_state["running"] = True
    threading.Thread(target=_trigger_worker_thread, daemon=True,
                     name="GeminiSwanManual").start()
    return {"accepted": True, "message": "已在后台启动, 约 3 分钟完成"}


@router.get("/trigger/status")
async def trigger_status():
    """查询手动触发的当前状态."""
    with _trigger_lock:
        return dict(_trigger_state)


@router.get("/status/{run_id}")
async def run_status(run_id: int):
    """查询历史某次 run 的状态."""
    try:
        with _conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM gemini_swan_runs WHERE id = %s", (run_id,))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="run_id not found")
                cur.execute(
                    "SELECT COUNT(*) AS n FROM gemini_swan_verdicts WHERE run_id = %s",
                    (run_id,),
                )
                cnt = cur.fetchone()["n"]
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"swan status error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    return {"run": _row_to_run(row), "verdict_count": cnt}
