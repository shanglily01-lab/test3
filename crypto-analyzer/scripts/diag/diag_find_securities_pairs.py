#!/usr/bin/env python3
"""
扫描 price_stats_24h 里的全部 symbol, 找出疑似"证券类"交易对.

用途:
  红黑天鹅榜 (gemini_swan_worker) 不允许把代币化股票/RWA 类交易对喂给 Gemini,
  因为这类标的的"涨跌驱动"和加密本身的链上叙事/资金费拥挤逻辑不兼容,
  让 Gemini 误判会污染榜单.

识别规则 (任一命中就标 suspect):
  1. base 完全等于已知美股/港股/ETF 大盘代码 (硬名单)
  2. base 以 X 结尾且去掉 X 后命中已知股票代码 (xStocks 风格: AAPLX/TSLAX/...)
  3. base 命中常见证券前缀关键字 (TSLA/AAPL/NVDA/...)

用法:
  cd crypto-analyzer
  python scripts/diag/diag_find_securities_pairs.py
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import pymysql

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass


# ----------------- 已知证券代码 (人工维护, 覆盖 Binance/友商可能上架的代币化股) -----------------
# 来源: 美股 SP500/NASDAQ100 + 加密相关的 MSTR/COIN/HOOD + 港股大盘 + 主流 ETF
KNOWN_STOCK_TICKERS = {
    # 七姐妹 + 大科技
    "AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "META", "NVDA", "TSLA", "NFLX",
    "ORCL", "ADBE", "CRM", "INTC", "AMD", "AVGO", "QCOM", "TXN", "IBM",
    "CSCO", "MU", "ARM", "PLTR", "SNOW", "DDOG", "NET", "CRWD", "ZS",
    # 半导体/AI 主线
    "ASML", "TSM", "SMCI", "ANET",
    # 金融
    "JPM", "BAC", "WFC", "GS", "MS", "C", "BLK", "SCHW", "V", "MA", "PYPL",
    "AXP", "COIN", "HOOD", "MSTR", "SQ", "BX",
    # 新能源/汽车
    "F", "GM", "RIVN", "LCID", "NIO", "XPEV", "LI", "BYD", "BYDDY",
    # 医药/医疗
    "PFE", "MRNA", "JNJ", "LLY", "UNH", "ABBV", "MRK", "BMY", "TMO", "ABT",
    "CVS", "WBA", "GILD", "AMGN", "REGN", "VRTX",
    # 消费/零售
    "WMT", "COST", "TGT", "HD", "LOW", "MCD", "SBUX", "NKE", "DIS", "KO",
    "PEP", "PG", "UL", "KHC", "CL",
    # 通信/媒体
    "T", "VZ", "CMCSA", "TMUS", "CHTR", "WBD", "PARA", "FOX", "FOXA",
    # 工业/能源
    "BA", "CAT", "DE", "MMM", "GE", "HON", "LMT", "RTX", "NOC", "GD",
    "XOM", "CVX", "COP", "OXY", "SLB", "EOG", "PSX", "MPC",
    # 中概股 (大概率被代币化)
    "BABA", "JD", "PDD", "BIDU", "TCEHY", "NTES", "BILI", "TME", "VIPS",
    # 其他高知名度
    "UBER", "LYFT", "ABNB", "DASH", "RBLX", "U",
    "SPOT", "PINS", "SNAP", "TWTR", "X",  # X 是 Twitter 重命名，但在加密里 X 也可能是项目代币 — 小心
    # 主流 ETF / 指数
    "SPY", "QQQ", "DIA", "IWM", "VOO", "VTI", "TLT", "HYG", "EEM", "FXI",
    "GLD", "SLV", "USO", "UNG", "VIX", "UVXY", "TQQQ", "SOXL", "ARKK",
    # 港股大盘
    "HSI", "TENCENT", "BIDU", "HKD",
    # 杠杆 ETF / 反向
    "TSLL", "TSLZ", "NVDX", "NVDY",
}

# 显式排除集合 (这些 base 在加密语境里有同名但 *不是* 证券, 误伤会很惨):
# - 用户后续如果要保护别的, 加这里
SAFE_OVERRIDE = {
    "X",     # 在加密里 X 可能是 ImmutableX 或新项目, 不能因为 Twitter 把它列证券
    "USD",   # 稳定币命名，已被 STABLECOINS 覆盖
    "BYD",   # BYDFI 等可能的加密混淆，需人工确认
}

# xStocks 后缀规则 (代币化股票常见命名): AAPLX, TSLAX, NVDAX, MSFTX...
XSTOCK_SUFFIX = "X"


# ----------------- DB -----------------
def load_db_cfg() -> dict:
    cfg = {"port": 3306, "user": "admin", "password": "Yintao@110",
           "database": "dimesion", "charset": "utf8mb4",
           "cursorclass": pymysql.cursors.DictCursor}
    env_host = os.getenv("DIMENSION_DB_HOST", "").strip()
    if env_host:
        cfg["host"] = env_host
        return cfg
    schemas = ROOT / "table_schemas.txt"
    head = schemas.read_text(encoding="utf-8").splitlines()[:15]
    for line in head:
        m = re.match(r"\s*host\s*[:=]\s*([\d\.]+)", line)
        if m:
            cfg["host"] = m.group(1)
            break
    if "host" not in cfg:
        raise RuntimeError("找不到 DB host")
    return cfg


def base_of(symbol: str) -> str:
    s = symbol.upper()
    if "/" in s:
        return s.split("/")[0]
    if s.endswith("USDT"):
        return s[:-4]
    return s


def classify(symbol: str) -> tuple[str, str]:
    """
    返回 (label, reason).
    label in {SECURITY, XSTOCK_LIKELY, SAFE_OVERRIDE, NORMAL}
    """
    b = base_of(symbol)
    if b in SAFE_OVERRIDE:
        return "SAFE_OVERRIDE", f"{b} 在加密里有同名但通常不是证券"
    if b in KNOWN_STOCK_TICKERS:
        return "SECURITY", f"硬名单命中: {b}"
    if (
        len(b) >= 4
        and b.endswith(XSTOCK_SUFFIX)
        and b[:-1] in KNOWN_STOCK_TICKERS
    ):
        return "XSTOCK_LIKELY", f"xStocks 风格: {b} = {b[:-1]} + 'X'"
    return "NORMAL", ""


def main():
    if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    cfg = load_db_cfg()
    with pymysql.connect(**cfg) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT symbol, current_price, change_24h, quote_volume_24h, updated_at
                FROM price_stats_24h
                WHERE updated_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 30 MINUTE)
                ORDER BY symbol
                """
            )
            rows = cur.fetchall()

    print(f"[scan] 远程 price_stats_24h 近 30 分钟内活跃 symbol 共 {len(rows)} 个\n")

    buckets = {"SECURITY": [], "XSTOCK_LIKELY": [], "SAFE_OVERRIDE": [], "NORMAL": []}
    for r in rows:
        sym = r["symbol"]
        label, reason = classify(sym)
        buckets[label].append((sym, reason, r))

    def fmt(row_pkg):
        sym, reason, r = row_pkg
        vol_m = (float(r["quote_volume_24h"] or 0)) / 1_000_000
        chg = float(r["change_24h"] or 0)
        return f"  {sym:<18} 24h={chg:+6.2f}%  vol={vol_m:>8.1f}M USDT   <- {reason}"

    print(f"=== SECURITY (硬名单, 共 {len(buckets['SECURITY'])} 个) ===")
    for pkg in buckets["SECURITY"]:
        print(fmt(pkg))
    print()

    print(f"=== XSTOCK_LIKELY (xStocks 风格 *X 后缀, 共 {len(buckets['XSTOCK_LIKELY'])} 个) ===")
    for pkg in buckets["XSTOCK_LIKELY"]:
        print(fmt(pkg))
    print()

    print(f"=== SAFE_OVERRIDE (同名但加密里通常不是证券, 共 {len(buckets['SAFE_OVERRIDE'])} 个) ===")
    for pkg in buckets["SAFE_OVERRIDE"]:
        print(fmt(pkg))
    print()

    suspects = [p[0] for p in buckets["SECURITY"] + buckets["XSTOCK_LIKELY"]]
    print(f"=== 总计疑似证券 symbol: {len(suspects)} ===")
    for s in suspects:
        print(f"  {s}")


if __name__ == "__main__":
    main()
