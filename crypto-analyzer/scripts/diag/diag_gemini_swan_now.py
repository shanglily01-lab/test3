#!/usr/bin/env python3
"""
基于当前市场异动数据 (24h 涨跌幅 + 资金费率极值) 抓 universe,
喂给 Gemini 标注哪些是「黑天鹅候选」/「红天鹅候选」并给具体催化剂.

数据源: 远程 dimesion (host 从 table_schemas.txt 头部读, 不硬编码)
表:
  - price_stats_24h    -> 24h 涨跌幅 / 成交额 (1 分钟更新)
  - funding_rate_stats -> 当前/7d 平均资金费率 (5 分钟更新)

universe 构成 (去重合并 ~30-40 个):
  - 24h 涨幅 top 12 (剔除大盘+稳定币, quote_volume >= 1000 万 USDT)
  - 24h 跌幅 top 12
  - 资金费率极正 top 10 (多头拥挤 -> 潜在黑天鹅 / 急跌洗盘)
  - 资金费率极负 top 10 (空头拥挤 -> 潜在红天鹅 / 空头挤压)

排除: BTC/ETH/BNB/SOL/XRP + USDT/USDC/DAI/FDUSD/BUSD/TUSD/USDE/USD1 稳定币

输出: logs/gemini_swan_now_<timestamp>.json (UTF-8)
依赖: pip install google-genai pymysql python-dotenv

用法:
  cd crypto-analyzer
  python scripts/diag/diag_gemini_swan_now.py
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.stderr.encoding != "utf-8":
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

import pymysql

# ------------------ 配置 ------------------
EXCLUDE_BASES = {"BTC", "ETH", "BNB", "SOL", "XRP"}
STABLECOINS = {"USDT", "USDC", "DAI", "FDUSD", "BUSD", "TUSD", "USDE", "USD1", "PYUSD"}
MIN_QUOTE_VOLUME = 10_000_000  # 1000 万 USDT
TOP_MOVER = 12
TOP_FUNDING = 10

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3-flash-preview")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
TIMEOUT_S = int(os.getenv("GEMINI_SWAN_TIMEOUT_S", "180"))
OUTPUT_DIR = ROOT / "logs"
OUTPUT_DIR.mkdir(exist_ok=True)


# ------------------ DB ------------------
def load_remote_db_cfg() -> dict:
    """从 table_schemas.txt 头部读 host (IP 会变, 不硬编码)."""
    path = ROOT / "table_schemas.txt"
    head = path.read_text(encoding="utf-8").splitlines()[:15]
    cfg = {"port": 3306, "user": "admin", "password": "Yintao@110",
           "database": "dimesion", "charset": "utf8mb4",
           "cursorclass": pymysql.cursors.DictCursor}
    for line in head:
        m = re.match(r"\s*host\s*[:=]\s*([\d\.]+)", line)
        if m:
            cfg["host"] = m.group(1)
            break
    if "host" not in cfg:
        raise RuntimeError("table_schemas.txt 头部没解析到 host IP")
    return cfg


def base_of(symbol: str) -> str:
    s = symbol.upper()
    if "/" in s:
        return s.split("/")[0]
    if s.endswith("USDT"):
        return s[:-4]
    return s


def is_excluded(symbol: str) -> bool:
    b = base_of(symbol)
    return b in EXCLUDE_BASES or b in STABLECOINS


# ------------------ universe 采集 ------------------
def fetch_movers_24h(cur, top_n: int):
    """24h 涨幅 / 跌幅 top, 已过滤成交额."""
    base_sql = """
        SELECT symbol, current_price, change_24h, quote_volume_24h, trend, updated_at
        FROM price_stats_24h
        WHERE quote_volume_24h >= %s
          AND change_24h IS NOT NULL
          AND updated_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 10 MINUTE)
        ORDER BY change_24h {order}
        LIMIT %s
    """
    # 取 top_n*3 留余地排除大盘
    cur.execute(base_sql.format(order="DESC"), (MIN_QUOTE_VOLUME, top_n * 3))
    gainers = [r for r in cur.fetchall() if not is_excluded(r["symbol"])][:top_n]
    cur.execute(base_sql.format(order="ASC"), (MIN_QUOTE_VOLUME, top_n * 3))
    losers = [r for r in cur.fetchall() if not is_excluded(r["symbol"])][:top_n]
    return gainers, losers


def fetch_extreme_funding(cur, top_n: int):
    """资金费率 极正 (多头拥挤) / 极负 (空头拥挤).

    用 funding_rate_data (新鲜, 与 price_stats_24h 同步), 不用 funding_rate_stats (已停更).
    取每个 symbol 最近 30 分钟内的最新一行 funding_rate 再做排序.
    """
    base_sql = """
        SELECT t.symbol AS symbol,
               t.funding_rate AS current_rate,
               NULL AS rate_avg_7d,
               t.timestamp AS updated_at
        FROM funding_rate_data t
        INNER JOIN (
            SELECT symbol, MAX(funding_time) AS max_ft
            FROM funding_rate_data
            WHERE timestamp >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 30 MINUTE)
            GROUP BY symbol
        ) latest ON t.symbol = latest.symbol AND t.funding_time = latest.max_ft
        ORDER BY t.funding_rate {order}
        LIMIT %s
    """
    cur.execute(base_sql.format(order="DESC"), (top_n * 3,))
    pos = [r for r in cur.fetchall() if not is_excluded(r["symbol"])][:top_n]
    cur.execute(base_sql.format(order="ASC"), (top_n * 3,))
    neg = [r for r in cur.fetchall() if not is_excluded(r["symbol"])][:top_n]
    return pos, neg


def merge_universe(gainers, losers, fund_pos, fund_neg) -> dict:
    """合并去重, 每个 symbol 收集所有触发原因."""
    uni: dict = {}
    def upsert(sym, **fields):
        sym = sym.upper()
        if sym not in uni:
            uni[sym] = {"symbol": sym, "triggers": [], "current_price": None,
                        "change_24h": None, "quote_volume_24h": None,
                        "current_rate": None, "rate_avg_7d": None}
        for k, v in fields.items():
            if k == "trigger":
                uni[sym]["triggers"].append(v)
            elif uni[sym].get(k) is None:
                uni[sym][k] = v

    for r in gainers:
        upsert(r["symbol"], trigger="24h_gainer",
               current_price=float(r["current_price"]) if r["current_price"] else None,
               change_24h=float(r["change_24h"]) if r["change_24h"] else None,
               quote_volume_24h=float(r["quote_volume_24h"]) if r["quote_volume_24h"] else None)
    for r in losers:
        upsert(r["symbol"], trigger="24h_loser",
               current_price=float(r["current_price"]) if r["current_price"] else None,
               change_24h=float(r["change_24h"]) if r["change_24h"] else None,
               quote_volume_24h=float(r["quote_volume_24h"]) if r["quote_volume_24h"] else None)
    for r in fund_pos:
        upsert(r["symbol"], trigger="funding_pos_extreme",
               current_rate=float(r["current_rate"]) if r["current_rate"] is not None else None,
               rate_avg_7d=float(r["rate_avg_7d"]) if r["rate_avg_7d"] is not None else None)
    for r in fund_neg:
        upsert(r["symbol"], trigger="funding_neg_extreme",
               current_rate=float(r["current_rate"]) if r["current_rate"] is not None else None,
               rate_avg_7d=float(r["rate_avg_7d"]) if r["rate_avg_7d"] is not None else None)
    return uni


# ------------------ Gemini ------------------
SWAN_PROMPT_TEMPLATE = """你是加密货币衍生品风险研究员。我会给你一组当前市场上有异动迹象的 USDT 永续合约,
**附带每个 symbol 的实时数据 (24h 涨跌幅, 资金费率, 触发原因)**。

请基于这些**实时数据 + 你对该币种基本面/赛道/历史叙事的认知**, 标注每个 symbol 在**未来 1-7 天**最可能的天鹅类型:

术语:
- 黑天鹅 (black_swan): 极端负向尾部 — 急跌、闪崩、连环爆仓、脱锚、信任危机、监管雷、暴雷传闻、解锁砸盘等
- 红天鹅 (red_swan):   极端正向尾部 — 暴涨、空头挤压、叙事爆发、利好兑现、生态催化、被收购/上线大所等
- skip:                数据不支持任何天鹅论点, 或 Gemini 完全不熟该币

判定要求:
1. **必须给具体催化剂** (catalyst) — 不能只说「高波动」「不确定性大」。
   合格示例: "近期解锁释放 X% 流通", "TVL 持续下行", "长期高正资金费 + 价格滞涨, 多头堆积",
            "空头资金费 -0.05%/8h 持续 3 天, 短期空头挤压风险", "AI 赛道 Q2 利好叙事 + 24h +35%".
   不合格示例: "波动大", "需要警惕", "看市场情绪".
2. **结合数据**: 资金费极正 + 24h 涨幅大 = 多头拥挤(black 倾向); 资金费极负 + 24h 跌幅大 = 反转可能(red 倾向).
3. **confidence**: 0.0-1.0. 自己不熟该币时给 ≤ 0.3 并 category=skip.

排除规则: 已排除 BTC/ETH/BNB/SOL/XRP 和稳定币, 你不需要再讨论它们.

# 当前 universe 数据 (来自远程数据库, UTC):
{universe_json}

**asof 时间** (由系统注入, 不要修改): {asof}

输出 **仅** 一个合法 JSON 对象, 不要 markdown 代码围栏 (asof_utc 字段不需要你填, 系统会覆盖):
{{
  "summary_zh": "整体市场氛围 1-2 句",
  "verdicts": [
    {{
      "symbol": "FOO/USDT",
      "category": "black_swan",
      "confidence": 0.65,
      "catalyst": "具体催化剂 (1-2 句)",
      "data_signal": "用上下文里哪个数据支持判断 (如 funding +0.08% + 24h +18%)",
      "risk_note": "反向风险一句"
    }}
  ]
}}
"""


def call_gemini(universe: dict) -> dict | None:
    if not GEMINI_API_KEY:
        print("ERROR: 请设置 GEMINI_API_KEY (.env 或环境变量)", file=sys.stderr)
        return None
    try:
        from google import genai
        from google.genai import types
    except ImportError:
        print("ERROR: pip install google-genai", file=sys.stderr)
        return None

    asof = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    universe_list = list(universe.values())
    universe_list.sort(key=lambda x: (x.get("change_24h") or 0), reverse=True)
    prompt = SWAN_PROMPT_TEMPLATE.format(
        universe_json=json.dumps(universe_list, ensure_ascii=False, indent=2),
        asof=asof,
    )

    client = genai.Client(api_key=GEMINI_API_KEY)
    cfg = types.GenerateContentConfig(
        response_mime_type="application/json",
        http_options=types.HttpOptions(timeout=TIMEOUT_S * 1000),
    )
    print(f"[gemini] model={GEMINI_MODEL} timeout={TIMEOUT_S}s "
          f"universe_size={len(universe_list)}", file=sys.stderr)
    t0 = time.time()
    try:
        resp = client.models.generate_content(
            model=GEMINI_MODEL, contents=prompt, config=cfg,
        )
    except Exception as e:
        print(f"ERROR: Gemini 调用失败: {e}", file=sys.stderr)
        return None
    text = (resp.text or "").strip()
    if text.startswith("```"):
        text = text.strip("`").lstrip("json").strip()
    print(f"[gemini] elapsed={time.time()-t0:.1f}s output_len={len(text)}", file=sys.stderr)
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        print(f"ERROR: JSON 解析失败: {e}\n--- raw ---\n{text[:2000]}", file=sys.stderr)
        return None


# ------------------ 主流程 ------------------
def main():
    cfg = load_remote_db_cfg()
    print(f"[db] connect dimesion @ {cfg['host']}", file=sys.stderr)
    conn = pymysql.connect(**cfg)
    try:
        with conn.cursor() as cur:
            gainers, losers = fetch_movers_24h(cur, TOP_MOVER)
            fund_pos, fund_neg = fetch_extreme_funding(cur, TOP_FUNDING)
    finally:
        conn.close()

    print(f"[universe] gainers={len(gainers)} losers={len(losers)} "
          f"fund_pos={len(fund_pos)} fund_neg={len(fund_neg)}", file=sys.stderr)
    universe = merge_universe(gainers, losers, fund_pos, fund_neg)
    print(f"[universe] merged_unique={len(universe)}", file=sys.stderr)

    if not universe:
        print("ERROR: universe 为空, 检查 price_stats_24h / funding_rate_stats 数据时效",
              file=sys.stderr)
        sys.exit(2)

    out = call_gemini(universe)
    if not out:
        sys.exit(3)

    out["asof_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    out["_meta"] = {
        "model": GEMINI_MODEL,
        "asof_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "universe_size": len(universe),
        "min_quote_volume_usdt": MIN_QUOTE_VOLUME,
        "excluded_bases": sorted(EXCLUDE_BASES | STABLECOINS),
        "raw_universe": list(universe.values()),
    }
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = OUTPUT_DIR / f"gemini_swan_now_{ts}.json"
    out_path.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[done] -> {out_path}", file=sys.stderr)

    # 摘要
    verdicts = out.get("verdicts") or []
    bs = [v for v in verdicts if str(v.get("category", "")).lower() == "black_swan"]
    rs = [v for v in verdicts if str(v.get("category", "")).lower() == "red_swan"]
    sk = [v for v in verdicts if str(v.get("category", "")).lower() == "skip"]
    print(f"[summary] black={len(bs)} red={len(rs)} skip={len(sk)}", file=sys.stderr)
    print(f"[summary] {out.get('summary_zh', '')}", file=sys.stderr)


if __name__ == "__main__":
    main()
