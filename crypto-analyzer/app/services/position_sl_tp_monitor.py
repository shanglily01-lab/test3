#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
持仓止盈止损监控服务（轻量版）

职责：
- 周期扫描 futures_positions 中所有 status='open' 的持仓（默认仅 dimension_trader 开的仓）
- 用 Binance WS 实时价（回退 REST ticker）与 DB 里存的 stop_loss_price / take_profit_price 比较
- 命中 SL/TP 立刻调用 FuturesTradingEngine.close_position(...) 平仓
- 不负责超时平仓（dimension_trader 自己 monitor_position 超时即平）

为什么需要这个：
- 原先有 smart_trader_service.py + smart_exit_optimizer.py 负责这事，但那两个模块在
  本轮清理里被删除。main.py 里对应启动代码被注释掉了，导致 SL/TP 虽然写进数据库但
  没人真正扫描价格触发平仓。
"""

from __future__ import annotations

import asyncio
import os
import time
from decimal import Decimal
from typing import Any, Dict, List, Optional

import pymysql
from loguru import logger


def _db_cfg() -> Dict[str, Any]:
    """全部从 .env 读，不带任何生产值默认。"""
    return {
        "host":     os.getenv("DB_HOST", "localhost"),
        "port":     int(os.getenv("DB_PORT", "3306")),
        "user":     os.getenv("DB_USER", ""),
        "password": os.getenv("DB_PASSWORD", ""),
        "database": os.getenv("DB_NAME", ""),
        "charset":  "utf8mb4",
        "cursorclass": pymysql.cursors.DictCursor,
    }


class PositionSLTPMonitor:
    """价格驱动的止盈止损监控循环。"""

    def __init__(
        self,
        engine,                      # FuturesTradingEngine 实例（已初始化）
        interval_seconds: float = 3.0,
        source_filter: str = "dimension_trader:%",  # 只看 dimension_trader 开的仓
        price_max_age_seconds: int = 30,
    ) -> None:
        self.engine = engine
        self.interval = float(interval_seconds)
        self.source_filter = source_filter
        self.price_max_age = int(price_max_age_seconds)
        self._task: Optional[asyncio.Task] = None
        self._stop = False
        # 仓位级 close 失败的短冷却，防止同一轮反复触发
        self._cooldown: Dict[int, float] = {}
        self._cooldown_seconds = 10.0

    def start(self) -> None:
        if self._task and not self._task.done():
            logger.info("[SL/TP Monitor] 已在运行，跳过重复启动")
            return
        self._stop = False
        self._task = asyncio.create_task(self._run())
        logger.info(
            f"[SL/TP Monitor] 启动 (interval={self.interval}s, "
            f"source_filter='{self.source_filter}')"
        )

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
                logger.error(f"[SL/TP Monitor] tick 异常: {e}")
            await asyncio.sleep(self.interval)

    def _tick_once(self) -> None:
        positions = self._fetch_open_positions()
        if not positions:
            return

        # 预取 WS 价格服务（惰性，异常不致命）
        try:
            from app.services.binance_ws_price import get_ws_price_service
            ws = get_ws_price_service("futures")
        except Exception:
            ws = None

        now = time.time()
        for pos in positions:
            pid = int(pos["id"])
            if self._cooldown.get(pid, 0) > now:
                continue

            symbol = pos["symbol"]
            side = pos["position_side"]  # 'LONG' / 'SHORT'
            sl = pos.get("stop_loss_price")
            tp = pos.get("take_profit_price")
            if sl is None and tp is None:
                continue

            price = self._get_live_price(ws, symbol)
            if price is None or price <= 0:
                continue

            trigger = self._check_trigger(side, price, sl, tp)
            if not trigger:
                continue

            reason, trigger_price = trigger
            logger.warning(
                f"[SL/TP Monitor] 触发平仓 pid={pid} {symbol} {side} "
                f"reason={reason} price={price:.6f} SL={sl} TP={tp}"
            )
            # 进冷却：无论成功失败，都在 cooldown 内不再尝试这个 pid，
            # 避免同一 pid 在 commit 与下一轮扫描之间被反复触发刷日志。
            self._cooldown[pid] = now + self._cooldown_seconds
            try:
                result = self.engine.close_position(
                    position_id=pid,
                    reason=reason,
                    close_price=Decimal(str(trigger_price)),
                )
            except Exception as e:
                logger.exception(f"[SL/TP Monitor] close_position 抛异常 pid={pid}: {e}")
                continue

            if not isinstance(result, dict):
                logger.error(f"[SL/TP Monitor] close_position 返回非 dict pid={pid}: {result!r}")
                continue

            if result.get("success"):
                if result.get("already_closed"):
                    logger.info(f"[SL/TP Monitor] pid={pid} 已在别处平仓，跳过")
                else:
                    logger.info(
                        f"[SL/TP Monitor] ✅ 平仓成功 pid={pid} {symbol} {side} "
                        f"realized_pnl={result.get('realized_pnl')} "
                        f"pnl_pct={result.get('pnl_pct')} "
                        f"exit_price={result.get('exit_price')}"
                    )
            else:
                # 失败放长冷却，防止疯狂刷屏
                self._cooldown[pid] = now + max(self._cooldown_seconds, 60.0)
                logger.error(
                    f"[SL/TP Monitor] ❌ 平仓失败 pid={pid} {symbol} {side} "
                    f"err={result.get('error')} msg={result.get('message')} reason={result.get('reason')}"
                )

    def _fetch_open_positions(self) -> List[Dict[str, Any]]:
        sql = (
            "SELECT id, symbol, position_side, entry_price, "
            "       stop_loss_price, take_profit_price, source "
            "FROM futures_positions "
            "WHERE status='open' "
            "  AND (source LIKE %s) "
            "  AND (stop_loss_price IS NOT NULL OR take_profit_price IS NOT NULL) "
            "LIMIT 500"
        )
        try:
            conn = pymysql.connect(**_db_cfg())
            try:
                with conn.cursor() as c:
                    c.execute(sql, (self.source_filter,))
                    rows = c.fetchall() or []
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"[SL/TP Monitor] 查询持仓失败: {e}")
            return []

        out: List[Dict[str, Any]] = []
        for r in rows:
            r["stop_loss_price"]   = float(r["stop_loss_price"])   if r.get("stop_loss_price")   is not None else None
            r["take_profit_price"] = float(r["take_profit_price"]) if r.get("take_profit_price") is not None else None
            out.append(r)
        return out

    def _get_live_price(self, ws, symbol: str) -> Optional[float]:
        if ws is not None:
            try:
                p = ws.get_price(symbol, max_age_seconds=self.price_max_age)
                if p is not None and p > 0:
                    return float(p)
            except Exception:
                pass
        # REST 兜底（一次调用拿全部价格开销大，这里单币）
        try:
            import requests
            sym = symbol.replace("/", "")
            r = requests.get(
                "https://fapi.binance.com/fapi/v1/ticker/price",
                params={"symbol": sym},
                timeout=3,
            )
            if r.status_code == 200:
                data = r.json()
                return float(data.get("price")) if data.get("price") else None
        except Exception:
            pass
        return None

    @staticmethod
    def _check_trigger(
        side: str,
        price: float,
        sl: Optional[float],
        tp: Optional[float],
    ) -> Optional[tuple[str, float]]:
        """返回 (reason, close_price) 或 None。"""
        side = (side or "").upper()
        if side == "LONG":
            if sl is not None and price <= sl:
                return ("stop_loss", sl)
            if tp is not None and price >= tp:
                return ("take_profit", tp)
        elif side == "SHORT":
            if sl is not None and price >= sl:
                return ("stop_loss", sl)
            if tp is not None and price <= tp:
                return ("take_profit", tp)
        return None


_monitor_instance: Optional[PositionSLTPMonitor] = None


def init_sl_tp_monitor(engine, **kwargs) -> PositionSLTPMonitor:
    global _monitor_instance
    if _monitor_instance is None:
        _monitor_instance = PositionSLTPMonitor(engine, **kwargs)
    return _monitor_instance


def get_sl_tp_monitor() -> Optional[PositionSLTPMonitor]:
    return _monitor_instance
