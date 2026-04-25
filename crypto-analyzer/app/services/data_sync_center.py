"""
数据同步中心 — Phase 1: 实时价格 (2026-04-25 抽离自 futures_api.py)

设计目标:
================================================================
  这个模块是**唯一**主动打 Binance 的地方. 所有 FastAPI 端点 / 策略进程 /
  paper_limit_sync / position_sl_tp_monitor 都通过 import 共享状态读取,
  零额外 Binance 请求 → 杜绝 IP 被封时的雪崩放大效应.

当前状态 (Phase 1):
  ✓ 实时价格 (Binance ticker REST 全市场, 每 10s, 权重 2)
  ✓ IP 封禁防御 (HTTP 418/429 解析 banned_until 精确退避)
  ✓ L1 进程缓存 + L2 内存字典共享读
  ✓ 策略进程通过 /api/futures/price 端点读 (不直接 import)

下一阶段 (Phase 2 / Phase 3):
  ○ Binance WebSocket 替代 REST (零 REST 请求, 币安官方推荐)
  ○ K 线统一抓取 (替代 fast_collector_service)
  ○ funding rate / LSR / OI 统一抓取
  ○ whale_data_collector 并入
  ○ Redis 共享缓存让多进程零拷贝读

为什么这层先做:
  IP 13.212.252.171 在 2026-04-25 被封 16h, 根因是 L3 fallback
  让 4 个 strategy 进程在 L2 字典 stale 时各自打 Binance 雪崩.
  抽出此模块 + 删 L3 = 强制 "统一一次拉数据" 的架构, 立刻止血.
"""
import asyncio as _asyncio
import re
import time as _time
from datetime import datetime as _dt
from typing import Dict, Optional, Tuple

import aiohttp
from aiohttp import ClientTimeout
from loguru import logger


# ═══════════════════ 共享内存状态 ═══════════════════
# L1: 进程内 3s 缓存 (热路径, 减少字典查询)
_PRICE_CACHE: Dict[str, Tuple[float, float]] = {}
_PRICE_CACHE_TTL: float = 3.0

# L2: 全市场内存字典, 后台 task 每 10s 从 Binance 批量拉取
# key 为带斜杠格式 (BTC/USDT), value (price, updated_at_epoch)
_REALTIME_PRICE_MAP: Dict[str, Tuple[float, float]] = {}
_REALTIME_PRICE_MAX_AGE_S: float = 24.0   # 超过 24s 视为过期 (10s 刷新 ×2)
_REALTIME_REFRESH_INTERVAL_S: float = 10.0

# L3 多源备份: Hyperliquid 永续 mid 价格 (单次 POST allMids 全市场)
# 当 Binance 封禁 / L2 过期时, 端点读这张表兜底.
# Hyperliquid 用 base symbol (BTC, ETH, ...), 永续都是 USD-margined,
# 这里存为 BTC/USDT 格式以便和 L2 同 key 复用.
# Gate.io 已废弃 (品种不全, 仅 HYPE/USDT 一个).
_HYPERLIQUID_PRICE_MAP: Dict[str, Tuple[float, float]] = {}
_HYPERLIQUID_MAX_AGE_S: float = 90.0
_HYPERLIQUID_REFRESH_INTERVAL_S: float = 30.0


# ═══════════════════ IP 封禁状态 ═══════════════════
# 策略 / 端点 / paper_sync 都通过 is_binance_banned() 查询;
# 命中时跳过任何 Binance 直查, 防止恶性循环加剧封禁.
_BANNED_UNTIL_TS: float = 0.0
_BAN_BACKOFF_S: float = 600.0  # 默认 10 min, 优先用 banned_until / Retry-After


def is_binance_banned() -> bool:
    return _time.time() < _BANNED_UNTIL_TS


def get_banned_until() -> float:
    return _BANNED_UNTIL_TS


_BAN_UNTIL_RE = re.compile(r'banned\s+until\s+(\d+)', re.IGNORECASE)


def _parse_binance_ban_message(body) -> Optional[float]:
    """从 Binance 错误响应解析 'banned until <ms_timestamp>'.
    支持 dict (parsed JSON) 或 str (raw body).
    返回 epoch seconds 或 None.
    """
    try:
        if isinstance(body, dict):
            msg = str(body.get('msg', ''))
        else:
            msg = str(body or '')
        m = _BAN_UNTIL_RE.search(msg)
        if m:
            return int(m.group(1)) / 1000.0
    except Exception:
        pass
    return None


def _set_banned(retry_after_s: Optional[float] = None,
                 reason: str = "unknown",
                 banned_until_ts: Optional[float] = None) -> None:
    """记录 IP 封禁状态.
    优先级: banned_until_ts (Binance error -1003 精确时间)
          > retry_after_s (Retry-After header)
          > _BAN_BACKOFF_S (默认 10 分钟)
    """
    global _BANNED_UNTIL_TS
    if banned_until_ts and banned_until_ts > _time.time():
        _BANNED_UNTIL_TS = banned_until_ts
    else:
        wait_s = max(retry_after_s or 0.0, _BAN_BACKOFF_S)
        _BANNED_UNTIL_TS = _time.time() + wait_s
    try:
        until_str = _dt.fromtimestamp(_BANNED_UNTIL_TS).strftime("%Y-%m-%d %H:%M:%S")
        wait_actual = _BANNED_UNTIL_TS - _time.time()
        logger.warning(
            f"[data-sync] Binance IP banned ({reason}), "
            f"暂停所有直连请求至 {until_str} (+{wait_actual:.0f}s)"
        )
    except Exception:
        pass


# ═══════════════════ 工具: symbol 格式互转 ═══════════════════
def _bn_symbol_to_slash(bn_sym: str) -> str:
    """BTCUSDT → BTC/USDT, BTCUSDC → BTC/USDC."""
    if bn_sym.endswith("USDT"):
        return bn_sym[:-4] + "/USDT"
    if bn_sym.endswith("USDC"):
        return bn_sym[:-4] + "/USDC"
    return bn_sym


def bn_clean_to_slash(symbol_clean: str, original: str) -> str:
    """BTCUSDT → BTC/USDT; 若原 symbol 带斜杠直接返回."""
    if '/' in original:
        return original
    if symbol_clean.endswith('USDT'):
        return symbol_clean[:-4] + '/USDT'
    if symbol_clean.endswith('USDC'):
        return symbol_clean[:-4] + '/USDC'
    return original


# ═══════════════════ L2 主任务: 拉 Binance 全市场 ═══════════════════
async def realtime_price_sync_loop():
    """后台 task: 每 10s 拉 Binance /fapi/v1/ticker/price 全市场, 写共享内存字典.

    这是整个系统**唯一**主动打 Binance ticker 的地方. 所有其他服务通过
    /api/futures/price HTTP 端点读 _REALTIME_PRICE_MAP, 零 DB IO.

    封禁防御: 收到 HTTP 418/429 时解析 body 里的 banned_until, 精确退避;
    封禁期间完全停止打 Binance.

    权重: 单次请求 2 weight, 每分钟 6 次 = 12/分; 远低于 IP 限额 6000/min.
    """
    url = "https://fapi.binance.com/fapi/v1/ticker/price"
    timeout = ClientTimeout(total=5)

    first_log = True
    consecutive_errors = 0
    while True:
        # 处于封禁期: 完全停止打 Binance, 等到解封才重试
        if is_binance_banned():
            wait = max(_BANNED_UNTIL_TS - _time.time(), 1.0)
            await _asyncio.sleep(min(wait, 30))
            continue

        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as r:
                    # HTTP 418 (IP banned) / 429 (rate limit) — 立即设封禁状态
                    if r.status in (418, 429):
                        # 优先解析 body 里的 "banned until <ts>" 精确时间
                        banned_until = None
                        try:
                            body = await r.json()
                            banned_until = _parse_binance_ban_message(body)
                        except Exception:
                            try:
                                txt = await r.text()
                                banned_until = _parse_binance_ban_message(txt)
                            except Exception:
                                pass
                        retry_after = r.headers.get('Retry-After')
                        retry_s = None
                        try:
                            if retry_after:
                                retry_s = float(retry_after)
                        except (ValueError, TypeError):
                            pass
                        _set_banned(retry_s, reason=f"L2 HTTP {r.status}",
                                     banned_until_ts=banned_until)
                        consecutive_errors += 1
                        continue
                    if r.status != 200:
                        logger.warning(f"[data-sync] L2 刷新 HTTP {r.status}")
                        consecutive_errors += 1
                        await _asyncio.sleep(
                            min(_REALTIME_REFRESH_INTERVAL_S * (1 + consecutive_errors), 60)
                        )
                        continue
                    data = await r.json()
            now_ts = _time.time()
            new_map = {}
            for item in data:
                bn_sym = item.get('symbol', '')
                price = item.get('price')
                if not bn_sym or not price:
                    continue
                try:
                    new_map[_bn_symbol_to_slash(bn_sym)] = (float(price), now_ts)
                except (ValueError, TypeError):
                    continue
            if new_map:
                # 原子替换
                _REALTIME_PRICE_MAP.clear()
                _REALTIME_PRICE_MAP.update(new_map)
                consecutive_errors = 0
                if first_log:
                    logger.info(f"[data-sync] L2 内存字典首次填充, {len(new_map)} 个品种")
                    first_log = False
        except Exception as e:
            consecutive_errors += 1
            logger.warning(f"[data-sync] L2 刷新异常 (连续 {consecutive_errors} 次): {e}")
        await _asyncio.sleep(
            _REALTIME_REFRESH_INTERVAL_S if consecutive_errors == 0
            else min(_REALTIME_REFRESH_INTERVAL_S * (1 + consecutive_errors), 60)
        )


# ═══════════════════ L3 多源备份: Hyperliquid ═══════════════════
async def hyperliquid_price_sync_loop():
    """后台 task: 每 30s POST Hyperliquid /info {type:allMids} 取全市场 mid 价.

    用途: Binance 封禁 / L2 过期时端点读 _HYPERLIQUID_PRICE_MAP 兜底.
    Hyperliquid 永续都是 USD-margined, 单次 POST 一次拿全市场, 覆盖广.
    （Gate.io 仅 HYPE/USDT 一个品种, 已废弃）

    返回结构: {"BTC": "95000.0", "ETH": "3500.0", ...}
    """
    url = "https://api.hyperliquid.xyz/info"
    body = {"type": "allMids"}
    timeout = ClientTimeout(total=8)

    first_log = True
    consecutive_errors = 0
    while True:
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, json=body) as r:
                    if r.status != 200:
                        consecutive_errors += 1
                        logger.warning(f"[data-sync] Hyperliquid HTTP {r.status}")
                        await _asyncio.sleep(
                            min(_HYPERLIQUID_REFRESH_INTERVAL_S * (1 + consecutive_errors), 120)
                        )
                        continue
                    data = await r.json()
            now_ts = _time.time()
            new_map = {}
            if isinstance(data, dict):
                for coin, price_str in data.items():
                    if not coin or not price_str:
                        continue
                    try:
                        # Hyperliquid 用 base symbol; 统一映射成 BTC/USDT 格式
                        new_map[f"{coin.upper()}/USDT"] = (float(price_str), now_ts)
                    except (ValueError, TypeError):
                        continue
            if new_map:
                _HYPERLIQUID_PRICE_MAP.clear()
                _HYPERLIQUID_PRICE_MAP.update(new_map)
                consecutive_errors = 0
                if first_log:
                    logger.info(f"[data-sync] L3 Hyperliquid 字典首次填充, {len(new_map)} 个品种")
                    first_log = False
        except Exception as e:
            consecutive_errors += 1
            logger.warning(f"[data-sync] Hyperliquid 刷新异常 (连续 {consecutive_errors} 次): {e}")
        await _asyncio.sleep(
            _HYPERLIQUID_REFRESH_INTERVAL_S if consecutive_errors == 0
            else min(_HYPERLIQUID_REFRESH_INTERVAL_S * (1 + consecutive_errors), 120)
        )


# ═══════════════════ 统一价格读 API ═══════════════════
def get_realtime_price(symbol_slash: str) -> Optional[Tuple[float, float, str]]:
    """统一价格读: 返回 (price, age_s, source) 或 None.

    优先级: L2 Binance (新鲜) > L3 Hyperliquid (新鲜) > L2 Binance (stale) > L3 Hyperliquid (stale).
    封禁期或 Binance 过期时优雅降级到 Hyperliquid, 永远不在此函数里发 HTTP.
    """
    now = _time.time()
    bn = _REALTIME_PRICE_MAP.get(symbol_slash)
    hl = _HYPERLIQUID_PRICE_MAP.get(symbol_slash)

    # 1) Binance 新鲜
    if bn:
        price, ts = bn
        age = now - ts
        if age < _REALTIME_PRICE_MAX_AGE_S:
            return (price, age, 'binance_memory')

    # 2) Hyperliquid 新鲜
    if hl:
        price, ts = hl
        age = now - ts
        if age < _HYPERLIQUID_MAX_AGE_S:
            return (price, age, 'hyperliquid_memory')

    # 3) Binance stale
    if bn:
        price, ts = bn
        return (price, now - ts, 'binance_memory_stale')

    # 4) Hyperliquid stale
    if hl:
        price, ts = hl
        return (price, now - ts, 'hyperliquid_memory_stale')

    return None


def get_l2_price_map_size() -> int:
    return len(_REALTIME_PRICE_MAP)


def get_l3_price_map_size() -> int:
    return len(_HYPERLIQUID_PRICE_MAP)


# ═══════════════════ Phase 2 / 3 待加 ═══════════════════
# 后续会在这个文件加:
#   async def kline_sync_loop():       # 替代 fast_collector_service
#   async def funding_sync_loop():     # funding_rates 表
#   async def lsr_oi_sync_loop():      # long_short_ratio + open_interest
#   async def whale_sync_loop():       # 整合 whale_data_collector
#   async def realtime_price_ws_loop(): # WebSocket 替代 REST (币安推荐)
#
# 目标架构: 所有这些 task 都在一个独立进程跑, 写 DB / 共享缓存.
# 策略进程零直连 Binance, 只读共享数据.
