#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
持仓止盈止损监控服务（轻量版）

职责：
- 周期扫描 futures_positions 中所有 status='open' 的持仓
- 用 Binance WS 实时价（回退 REST ticker）与 DB 里存的 stop_loss_price / take_profit_price 比较
- 命中 SL/TP 立刻通过 HTTP API 平仓（避免共享 engine 连接线程安全问题）
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any, Dict, List, Optional

import pymysql
import requests
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


# 动态出场规则（与 strategy_live/whale/bigmid MID 同）
# monitor 扫描频率高（默认 1s）可以更及时抓到小币快速穿越
TRAIL_TP_TIERS = [
    (0.10, 0.03),  # peak ≥ 10% → 回落 3% 平
    (0.05, 0.02),  # peak ≥ 5%  → 回落 2% 平
    (0.03, 0.01),  # peak ≥ 3%  → 回落 1% 平
]
EARLY_SL_PCT             = 0.03   # 浮亏 ≥ 3% 早期止损
# peak ≥ 1.5% 启用保本守护（2026-04-24 从 3% 降低；补 peak 1-3% 的盲区）
BREAKEVEN_AFTER_PEAK_PCT = 0.015
BREAKEVEN_SL_PCT         = -0.005 # 保本线 -0.5%
# 入场保护期：开仓 N 分钟内 early-sl/breakeven 不触发（硬 SL 兜底）
# 2026-04-24：数据显示 38% early-sl 在 5m 内扎中（入场瞬间均值回归误杀）
ENTRY_GRACE_MIN          = 45


def _dynamic_trail_pullback(peak_pct: float) -> float:
    for threshold, pullback in TRAIL_TP_TIERS:
        if peak_pct >= threshold:
            return pullback
    return float('inf')


class PositionSLTPMonitor:
    """价格驱动的止盈止损监控循环。"""

    def __init__(
        self,
        engine=None,                 # 保留参数兼容旧调用，不再使用
        interval_seconds: float = 1.0,
        source_filter: str = "%",
        price_max_age_seconds: int = 30,
        api_base: str = "http://localhost:9021",
    ) -> None:
        self.interval = float(interval_seconds)
        self.source_filter = source_filter
        self.price_max_age = int(price_max_age_seconds)
        self.api_base = api_base.rstrip("/")
        self._task: Optional[asyncio.Task] = None
        self._stop = False
        self._cooldown: Dict[int, float] = {}
        self._cooldown_seconds = 10.0
        # peak_pnl_pct 内存映射：进程重启会丢，但一般持仓 <= 24h 影响可控
        self._peak_pnl_map: Dict[int, float] = {}
        # disable_sl_tp_hold 开关缓存，避免每秒查 DB
        self._disable_cache: tuple[float, bool] = (0.0, False)
        self._disable_cache_ttl = 10.0

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

        try:
            from app.services.binance_ws_price import get_ws_price_service
            ws = get_ws_price_service("futures")
        except Exception:
            ws = None

        # 全局裸奔开关（10s 缓存）
        disable_rules = self._is_disable_sl_tp_hold()

        # 清理已不在 open 列表的 peak 记录
        alive_pids = {int(p["id"]) for p in positions}
        self._peak_pnl_map = {k: v for k, v in self._peak_pnl_map.items() if k in alive_pids}

        now = time.time()
        for pos in positions:
            pid = int(pos["id"])
            if self._cooldown.get(pid, 0) > now:
                continue

            symbol = pos["symbol"]
            side = pos["position_side"]
            entry_price = float(pos.get("entry_price") or 0)
            sl = pos.get("stop_loss_price")
            tp = pos.get("take_profit_price")
            if sl is None and tp is None:
                continue
            if entry_price <= 0:
                continue

            price = self._get_live_price(ws, symbol)
            if price is None or price <= 0:
                continue

            # 计算浮盈浮亏百分比（价格维度）
            if side.upper() == "LONG":
                pnl_pct = (price - entry_price) / entry_price
            else:
                pnl_pct = (entry_price - price) / entry_price

            # 更新 peak
            prev_peak = self._peak_pnl_map.get(pid, 0.0)
            new_peak = max(prev_peak, pnl_pct)
            if new_peak != prev_peak:
                self._peak_pnl_map[pid] = new_peak

            reason: Optional[str] = None
            trigger_price = price

            # 1. 新规则（受 disable_sl_tp_hold 控制）
            if not disable_rules:
                # 入场保护期：开仓 ENTRY_GRACE_MIN 分钟内 early-sl/breakeven 不触发
                open_time = pos.get("open_time")
                in_grace = False
                if open_time:
                    import datetime as _dt
                    if isinstance(open_time, _dt.datetime):
                        age_s = time.time() - open_time.timestamp()
                        in_grace = age_s < ENTRY_GRACE_MIN * 60

                pullback_thresh = _dynamic_trail_pullback(new_peak)
                if (new_peak - pnl_pct) >= pullback_thresh:
                    reason = "trail-tp"
                elif not in_grace and new_peak >= BREAKEVEN_AFTER_PEAK_PCT and pnl_pct <= BREAKEVEN_SL_PCT:
                    reason = "breakeven-sl"
                elif not in_grace and pnl_pct <= -EARLY_SL_PCT:
                    reason = "early-sl"

            # 2. 原硬 SL/TP（兜底，永远生效）
            if not reason:
                trig = self._check_trigger(side, price, sl, tp)
                if trig:
                    reason, trigger_price = trig

            if not reason:
                continue

            logger.warning(
                f"[SL/TP Monitor] 触发平仓 pid={pid} {symbol} {side} "
                f"reason={reason} price={price:.6f} pnl={pnl_pct*100:+.2f}% "
                f"peak={new_peak*100:+.2f}% SL={sl} TP={tp}"
            )
            self._cooldown[pid] = now + self._cooldown_seconds
            self._peak_pnl_map.pop(pid, None)
            self._do_close(pid, symbol, side, reason, trigger_price, now)

    def _is_disable_sl_tp_hold(self) -> bool:
        """读 system_settings.disable_sl_tp_hold，10s 缓存避免每秒查 DB"""
        now = time.time()
        ts, val = self._disable_cache
        if (now - ts) < self._disable_cache_ttl:
            return val
        try:
            from app.services.system_settings_loader import get_disable_sl_tp_hold
            val = get_disable_sl_tp_hold()
        except Exception:
            val = False
        self._disable_cache = (now, val)
        return val

    def _do_close(self, pid: int, symbol: str, side: str, reason: str,
                  trigger_price: float, now: float) -> None:
        """通过 HTTP API 平仓，避免直接调用 engine 的共享连接导致线程冲突。"""
        try:
            resp = requests.post(
                f"{self.api_base}/api/futures/close/{pid}",
                json={"reason": reason, "close_price": trigger_price},
                timeout=10,
            )
            data = resp.json() if resp.content else {}
        except Exception as e:
            logger.exception(f"[SL/TP Monitor] HTTP 平仓请求异常 pid={pid}: {e}")
            self._cooldown[pid] = now + max(self._cooldown_seconds, 60.0)
            return

        if not resp.ok:
            self._cooldown[pid] = now + max(self._cooldown_seconds, 60.0)
            logger.error(
                f"[SL/TP Monitor] HTTP 平仓失败 pid={pid} status={resp.status_code} "
                f"body={resp.text[:200]}"
            )
            return

        inner = data.get("data") or data
        if inner.get("already_closed") or data.get("already_closed"):
            logger.info(f"[SL/TP Monitor] pid={pid} 已在别处平仓，跳过")
        else:
            logger.info(
                f"[SL/TP Monitor] 平仓成功 pid={pid} {symbol} {side} "
                f"realized_pnl={inner.get('realized_pnl')} "
                f"pnl_pct={inner.get('pnl_pct')} "
                f"exit_price={inner.get('exit_price') or inner.get('close_price')}"
            )

    def _fetch_open_positions(self) -> List[Dict[str, Any]]:
        sql = (
            "SELECT id, symbol, position_side, entry_price, "
            "       stop_loss_price, take_profit_price, source, open_time "
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
        # 1. 首选 WebSocket（零 REST 消耗）
        if ws is not None:
            try:
                p = ws.get_price(symbol, max_age_seconds=self.price_max_age)
                if p is not None and p > 0:
                    return float(p)
            except Exception:
                pass
        # 2. fallback 走 FastAPI /api/futures/price 端点
        #    该端点优先命中 L2 内存字典（每 5s 从 Binance 批量拉全市场一次）
        #    1s × N 仓位的 monitor 轮询在这里几乎全部命中内存，不直打 Binance
        #    ▸ 早期版本曾直接 requests.get fapi.binance.com —— 会让 monitor 提速到 1s 后快速打爆 IP 限额
        try:
            r = requests.get(
                f"{self.api_base}/api/futures/price/{symbol}",
                timeout=2,
            )
            if r.status_code == 200:
                data = r.json() or {}
                price = data.get("price")
                if price is not None and float(price) > 0:
                    return float(price)
        except Exception:
            pass
        return None

    @staticmethod
    def _check_trigger(
        side: str,
        price: float,
        sl: Optional[float],
        tp: Optional[float],
    ) -> Optional[tuple]:
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


def init_sl_tp_monitor(engine=None, **kwargs) -> PositionSLTPMonitor:
    global _monitor_instance
    if _monitor_instance is None:
        _monitor_instance = PositionSLTPMonitor(engine, **kwargs)
    return _monitor_instance


def get_sl_tp_monitor() -> Optional[PositionSLTPMonitor]:
    return _monitor_instance
