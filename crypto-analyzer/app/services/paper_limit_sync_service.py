# -*- coding: utf-8 -*-
"""
模拟盘开仓单 -> 实盘同步服务

职责：
- 每 10 秒扫描 futures_orders 中新成交的 OPEN_LONG/OPEN_SHORT 订单
  （live_sync_status IS NULL，LIMIT 或 MARKET 都同步；2026-04-24 从仅 LIMIT 扩展）
- 检查 system_settings.live_trading_enabled
- 用 user_api_keys.margin_per_trade / max_leverage 计算实盘数量
- 通过 BinanceFuturesEngine 在实盘开相同仓位，TP/SL 价格与模拟盘一致
- 成功：写 live_sync_status='SYNCED', live_position_id
  失败：写 live_sync_status='FAILED'（不重试，防止重复下单）
"""

from __future__ import annotations

import asyncio
import os
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional

import pymysql
import requests
from loguru import logger


def _db_cfg() -> Dict[str, Any]:
    return {
        "host":        os.getenv("DB_HOST", "localhost"),
        "port":        int(os.getenv("DB_PORT", "3306")),
        "user":        os.getenv("DB_USER", ""),
        "password":    os.getenv("DB_PASSWORD", ""),
        "database":    os.getenv("DB_NAME", ""),
        "charset":     "utf8mb4",
        "cursorclass": pymysql.cursors.DictCursor,
    }


class PaperLimitSyncService:
    """模拟盘限价单成交后自动同步到实盘。"""

    def __init__(
        self,
        interval_seconds: float = 10.0,
        api_base: str = "http://localhost:9021",
    ) -> None:
        self.interval = interval_seconds
        self.api_base = api_base.rstrip("/")
        self._task: Optional[asyncio.Task] = None
        self._stop = False

    def start(self) -> None:
        if self._task and not self._task.done():
            logger.info("[PaperSync] 已在运行，跳过重复启动")
            return
        self._stop = False
        self._task = asyncio.create_task(self._run())
        logger.info(f"[PaperSync] 启动 (interval={self.interval}s)")

    def stop(self) -> None:
        self._stop = True
        if self._task:
            self._task.cancel()

    async def _run(self) -> None:
        while not self._stop:
            try:
                await asyncio.to_thread(self._tick_once)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[PaperSync] tick 异常: {e}")
            await asyncio.sleep(self.interval)

    # ── 主逻辑 ────────────────────────────────────────────────────

    def _tick_once(self) -> None:
        try:
            conn = pymysql.connect(**_db_cfg())
        except Exception as e:
            logger.error(f"[PaperSync] 数据库连接失败: {e}")
            return

        try:
            with conn.cursor() as cur:
                if not self._is_live_enabled(cur):
                    return

                orders = self._fetch_pending_sync(cur)

            for order in orders:
                self._sync_one(conn, order)
        finally:
            conn.close()

    def _is_live_enabled(self, cur) -> bool:
        cur.execute(
            "SELECT setting_value FROM system_settings WHERE setting_key='live_trading_enabled' LIMIT 1"
        )
        row = cur.fetchone()
        if not row:
            return False
        return str(row["setting_value"]).strip() == "1"

    def _fetch_pending_sync(self, cur) -> List[Dict]:
        # side 过滤改为显式 OPEN_LONG/OPEN_SHORT，防止 CLOSE_* 单被误同步为开仓
        cur.execute(
            """
            SELECT
                fo.id, fo.account_id, fo.symbol, fo.side,
                fo.leverage, fo.quantity, fo.avg_fill_price,
                fo.stop_loss_price, fo.take_profit_price,
                fo.order_source, fo.position_id,
                fta.user_id
            FROM futures_orders fo
            JOIN futures_trading_accounts fta ON fta.id = fo.account_id
            WHERE fo.status = 'FILLED'
              AND fo.side IN ('OPEN_LONG', 'OPEN_SHORT')
              AND fo.live_sync_status IS NULL
              AND fo.fill_time >= NOW() - INTERVAL 2 HOUR
            ORDER BY fo.fill_time ASC
            LIMIT 20
            """
        )
        return cur.fetchall()

    def _sync_one(self, conn, order: Dict) -> None:
        order_id = order["id"]
        symbol = order["symbol"]
        user_id = order["user_id"]
        pos_side = "LONG" if "LONG" in str(order["side"]) else "SHORT"

        try:
            api_cfg = self._get_api_config(user_id)
            if api_cfg is None:
                logger.warning(f"[PaperSync] user_id={user_id} 无活跃 API key，跳过 order_id={order_id}")
                self._mark(conn, order_id, "FAILED", None)
                return

            live_account_id = self._get_live_account_id(user_id)
            if live_account_id is None:
                logger.warning(f"[PaperSync] user_id={user_id} 无实盘账户，跳过 order_id={order_id}")
                self._mark(conn, order_id, "FAILED", None)
                return

            margin = float(api_cfg["margin_per_trade"])
            leverage = int(api_cfg["max_leverage"])

            price = self._get_price(symbol)
            if price is None or price <= 0:
                logger.warning(f"[PaperSync] 获取 {symbol} 价格失败，跳过 order_id={order_id}")
                self._mark(conn, order_id, "FAILED", None)
                return

            quantity = Decimal(str(round(margin * leverage / price, 6)))

            from app.services.user_trading_engine_manager import get_engine_manager
            mgr = get_engine_manager()
            if mgr is None:
                logger.error(f"[PaperSync] engine_manager 未初始化，跳过 order_id={order_id}")
                self._mark(conn, order_id, "FAILED", None)
                return

            engine = mgr.get_engine(user_id)
            if engine is None:
                logger.warning(f"[PaperSync] user_id={user_id} 引擎为 None，跳过 order_id={order_id}")
                self._mark(conn, order_id, "FAILED", None)
                return

            # 将纸面 SL/TP 转为百分比，基于实盘实际成交价重算绝对价格
            # 避免纸面绝对价与实盘成交价偏差导致 SL/TP 验证失败
            paper_fill = float(order["avg_fill_price"] or 0)
            paper_sl = float(order["stop_loss_price"] or 0)
            paper_tp = float(order["take_profit_price"] or 0)

            sl_pct: Optional[Decimal] = None
            tp_pct: Optional[Decimal] = None

            if paper_fill > 0:
                if paper_sl > 0:
                    raw_sl = (paper_fill - paper_sl) / paper_fill * 100 if pos_side == "LONG" \
                        else (paper_sl - paper_fill) / paper_fill * 100
                    if raw_sl > 0:
                        sl_pct = Decimal(str(round(raw_sl, 4)))
                if paper_tp > 0:
                    raw_tp = (paper_tp - paper_fill) / paper_fill * 100 if pos_side == "LONG" \
                        else (paper_fill - paper_tp) / paper_fill * 100
                    if raw_tp > 0:
                        tp_pct = Decimal(str(round(raw_tp, 4)))

            if paper_fill <= 0 or sl_pct is None or tp_pct is None:
                logger.warning(
                    "[PaperSync] order_id=%s %s 无法计算SL/TP百分比 "
                    "fill=%.6f sl=%.6f tp=%.6f sl_pct=%s tp_pct=%s",
                    order_id, symbol, paper_fill, paper_sl, paper_tp, sl_pct, tp_pct,
                )

            result = engine.open_position(
                account_id=live_account_id,
                symbol=symbol,
                position_side=pos_side,
                quantity=quantity,
                leverage=leverage,
                stop_loss_pct=sl_pct,
                take_profit_pct=tp_pct,
                source=f"paper-limit-sync:{order.get('order_source', '')}",
                paper_position_id=order.get("position_id"),
            )

            if result and result.get("success"):
                live_pid = str(result.get("position_id") or result.get("id") or "")
                logger.info(
                    "[PaperSync] 同步成功 order_id=%s %s %s %s qty=%.4f lev=%dx live_pid=%s",
                    order_id, symbol, pos_side, price, float(quantity), leverage, live_pid,
                )
                self._mark(conn, order_id, "SYNCED", live_pid or None)
            else:
                err = (result or {}).get("message", "unknown")
                logger.error(f"[PaperSync] 实盘开仓失败 order_id={order_id} {symbol}: {err}")
                self._mark(conn, order_id, "FAILED", None)

        except Exception as e:
            logger.error(f"[PaperSync] 同步异常 order_id={order_id} {symbol}: {e}")
            self._mark(conn, order_id, "FAILED", None)

    # ── 辅助 ─────────────────────────────────────────────────────

    def _get_api_config(self, user_id: int) -> Optional[Dict]:
        try:
            conn = pymysql.connect(**_db_cfg())
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """SELECT margin_per_trade, max_leverage
                        FROM user_api_keys
                        WHERE user_id=%s AND status='active'
                        ORDER BY id ASC LIMIT 1""",
                        (user_id,),
                    )
                    row = cur.fetchone()
                    if not row:
                        return None
                    return {
                        "margin_per_trade": float(row["margin_per_trade"] or 40.0),
                        "max_leverage":     int(row["max_leverage"] or 5),
                    }
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"[PaperSync] 读取 api_config 失败 user_id={user_id}: {e}")
            return None

    def _get_live_account_id(self, user_id: int) -> Optional[int]:
        try:
            conn = pymysql.connect(**_db_cfg())
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT id FROM live_trading_accounts WHERE user_id=%s LIMIT 1",
                        (user_id,),
                    )
                    row = cur.fetchone()
                    return int(row["id"]) if row else None
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"[PaperSync] 读取 live_account_id 失败 user_id={user_id}: {e}")
            return None

    def _get_price(self, symbol: str) -> Optional[float]:
        try:
            r = requests.get(
                f"{self.api_base}/api/futures/price/{symbol}", timeout=5
            )
            r.raise_for_status()
            return float(r.json()["price"])
        except Exception as e:
            logger.warning(f"[PaperSync] 获取价格失败 {symbol}: {e}")
            return None

    def _mark(self, conn, order_id: int, status: str, live_position_id: Optional[str]) -> None:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """UPDATE futures_orders
                    SET live_sync_status=%s, live_synced_at=NOW(), live_position_id=%s
                    WHERE id=%s""",
                    (status, live_position_id, order_id),
                )
            conn.commit()
        except Exception as e:
            logger.error(f"[PaperSync] 更新同步状态失败 order_id={order_id}: {e}")


_service: Optional[PaperLimitSyncService] = None


def get_paper_limit_sync_service() -> Optional[PaperLimitSyncService]:
    return _service


def init_paper_limit_sync_service(
    interval_seconds: float = 10.0,
    api_base: str = "http://localhost:9021",
) -> PaperLimitSyncService:
    global _service
    _service = PaperLimitSyncService(interval_seconds=interval_seconds, api_base=api_base)
    return _service
