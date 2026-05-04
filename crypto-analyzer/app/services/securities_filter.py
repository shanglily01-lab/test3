"""
证券类交易对过滤器.

红黑天鹅榜 / Top 榜单 等不允许把代币化股票/ETF 类交易对当成加密标的分析,
否则 Gemini 会用加密叙事 (资金费拥挤/链上解锁/赛道催化) 去解读股票, 论点污染.

名单 (2026-05-04 由 diag_find_securities_pairs.py 在远程数据库扫出, 经人工核对):
- 27 个 base 是币安期货上的代币化股 / ETF
- DASH, DIA 是同名加密项目 (Dash 老币, DIAdata 预言机), 不能误伤

修改名单:
- 新增证券类: 加进 EXCLUDED_STOCK_BASES
- 同名加密被误伤: 加进 SAFE_OVERRIDE
- 改完不需要重启进程: 各 worker 60s reload system_settings, 但本模块是常量, 进程内 import 一次
"""
from __future__ import annotations


EXCLUDED_STOCK_BASES: frozenset[str] = frozenset({
    "AAPL", "AMZN", "AVGO", "BABA", "C", "CL", "COIN", "CVX", "F", "GOOGL",
    "HOOD", "INTC", "META", "MSFT", "MSTR", "MU", "NVDA", "PLTR", "QQQ",
    "SPY", "T", "TSLA", "TSM",
})


SAFE_OVERRIDE: frozenset[str] = frozenset({
    "DASH",
    "DIA",
})


def base_of(symbol: str) -> str:
    s = symbol.upper().strip()
    if "/" in s:
        return s.split("/")[0]
    if s.endswith("USDT"):
        return s[:-4]
    return s


def is_security(symbol: str) -> bool:
    b = base_of(symbol)
    if b in SAFE_OVERRIDE:
        return False
    return b in EXCLUDED_STOCK_BASES
