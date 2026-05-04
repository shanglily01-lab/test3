#!/usr/bin/env python3
"""
实盘盘面筛查：黑天鹅语境（极端弱势/暴跌） vs 红天鹅语境（极端强势/暴涨）。

数据来源（各 1 次 REST，与 whale_data_collector 一致）:
  - GET /fapi/v1/ticker/24hr   → 全市场 24h 涨跌、成交额
  - GET /fapi/v1/premiumIndex → 全市场 lastFundingRate

不等 DB、不下单。可选 --gemini 把候选摘要发给 Gemini 做中文盘面解读（非喊单）。

用法:
  cd crypto-analyzer
  python scripts/diag/diag_swan_screen_live.py
  python scripts/diag/diag_swan_screen_live.py --top 20 --min-quote-m USDT 3000000 --gemini
  python scripts/diag/diag_swan_screen_live.py --json > screen.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any

import requests

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

try:
    from dotenv import load_dotenv

    load_dotenv(os.path.join(ROOT, ".env"))
except ImportError:
    pass

from app.services.securities_filter import is_security

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

FAPI = "https://fapi.binance.com"
TIMEOUT = 15


def _std_sym(binance_sym: str) -> str:
    if binance_sym.endswith("USDT"):
        return f"{binance_sym[:-4]}/USDT"
    return binance_sym


def _get_json(url: str) -> Any:
    r = requests.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def fetch_merged() -> list[dict]:
    """合并 24hr ticker 与 premiumIndex 资金费率."""
    tickers = _get_json(f"{FAPI}/fapi/v1/ticker/24hr")
    prem = _get_json(f"{FAPI}/fapi/v1/premiumIndex")
    fund_map: dict[str, float] = {}
    if isinstance(prem, list):
        for p in prem:
            sym = p.get("symbol") or ""
            if sym.endswith("USDT"):
                fund_map[sym] = float(p.get("lastFundingRate") or 0)

    rows = []
    if not isinstance(tickers, list):
        return rows
    for d in tickers:
        sym = d.get("symbol") or ""
        if not sym.endswith("USDT"):
            continue
        try:
            chg = float(d.get("priceChangePercent", 0))
            qv = float(d.get("quoteVolume", 0))
            last = float(d.get("lastPrice", 0))
        except (TypeError, ValueError):
            continue
        rows.append(
            {
                "symbol": _std_sym(sym),
                "binance_symbol": sym,
                "last": last,
                "change_24h_pct": round(chg, 4),
                "quote_volume_24h": qv,
                "volume_24h": float(d.get("volume", 0) or 0),
                "high_24h": float(d.get("highPrice", 0) or 0),
                "low_24h": float(d.get("lowPrice", 0) or 0),
                "funding_rate": fund_map.get(sym),
            }
        )
    return rows


def screen(
    rows: list[dict],
    top_n: int,
    min_quote_vol: float,
) -> tuple[list[dict], list[dict]]:
    """返回 (黑天鹅侧: 跌幅榜前 N, 红天鹅侧: 涨幅榜前 N)，均已过滤最小成交额 + 证券类."""
    eligible = [
        r for r in rows
        if r["quote_volume_24h"] >= min_quote_vol and not is_security(r["symbol"])
    ]
    # 暴跌榜：涨幅升序（最负在前）
    black = sorted(eligible, key=lambda x: x["change_24h_pct"])[:top_n]
    # 暴涨榜：涨幅降序
    red = sorted(eligible, key=lambda x: x["change_24h_pct"], reverse=True)[:top_n]
    return black, red


def _gemini_interpret(black: list[dict], red: list[dict], model: str) -> str | None:
    api_key = os.getenv("GEMINI_API_KEY", "")
    if not api_key:
        print("WARN: 未设置 GEMINI_API_KEY，跳过 Gemini", file=sys.stderr)
        return None

    def fmt_rows(side: list[dict]) -> str:
        lines = []
        for r in side:
            fr = r.get("funding_rate")
            fr_s = f"{fr * 100:.4f}%" if fr is not None else "n/a"
            lines.append(
                f"  {r['symbol']:<16} 24h={r['change_24h_pct']:+.2f}%  "
                f"成交额U={r['quote_volume_24h']:.0f}  费率={fr_s}"
            )
        return "\n".join(lines)

    prompt = f"""你是加密货币盘面分析师。以下为当前 Binance USDT 永续合约中，
按 24 小时涨跌幅排序后的极端两端（已过滤低成交额），用于讨论「盘面语境」而非投资建议。

定义（本任务）：
- 「黑天鹅盘面侧」：24h 跌幅最靠前的一批合约（弱势/抛压/恐慌叙事）。
- 「红天鹅盘面侧」：24h 涨幅最靠前的一批合约（强势/FOMO/多头叙事）。

【黑天鹅盘面候选 — 24h 跌幅领先】
{fmt_rows(black)}

【红天鹅盘面候选 — 24h 涨幅领先】
{fmt_rows(red)}

请用中文输出 JSON（不要用 markdown 围栏）：
{{
  "tape_summary_zh": "2～4 句话概括当前极端分化是否在共振主流叙事（如 BTC 横盘山寨抽血等），语气克制",
  "black_side_zh": "对跌幅榜一侧的简短解读：是否与极高负费率/抛压一致，注意小币种流动性陷阱",
  "red_side_zh": "对涨幅榜一侧的简短解读：是否与极高正费率/多头拥挤相关，回撤风险",
  "disclaimer_zh": "仅为盘面描述，不构成开仓建议"
}}
"""
    try:
        from google import genai
        from google.genai import types

        client = genai.Client(api_key=api_key)
        config = types.GenerateContentConfig(
            response_mime_type="application/json",
            http_options=types.HttpOptions(timeout=90_000),
        )
        resp = client.models.generate_content(model=model, contents=prompt, config=config)
        text = (resp.text or "").strip()
        if text.startswith("```"):
            text = text.strip("`").lstrip("json").strip()
        return json.dumps(json.loads(text), ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"WARN: Gemini 失败: {e}", file=sys.stderr)
        return None


def main():
    ap = argparse.ArgumentParser(description="实盘黑天鹅/红天鹅盘面筛查（Binance 永续）")
    ap.add_argument("--top", type=int, default=15, help="每一侧取前 N 名（默认 15）")
    ap.add_argument(
        "--min-quote-m-usdt",
        type=float,
        default=3_000_000,
        metavar="M",
        help="最低 24h 成交额（USDT），默认 300 万，过滤冷门币",
    )
    ap.add_argument("--gemini", action="store_true", help="调用 Gemini 输出中文盘面解读（需 GEMINI_API_KEY）")
    ap.add_argument(
        "--model",
        default=os.getenv("GEMINI_MODEL", "gemini-3-flash-preview"),
        help="Gemini 模型名",
    )
    ap.add_argument("--json", action="store_true", help="整包结果打印为 JSON（便于管道）")
    args = ap.parse_args()

    print("正在请求 Binance FAPI（24hr + premiumIndex）…", file=sys.stderr)
    rows = fetch_merged()
    if not rows:
        print("ERROR: 无行情数据", file=sys.stderr)
        sys.exit(1)

    black, red = screen(rows, args.top, args.min_quote_m_usdt)

    out: dict[str, Any] = {
        "source": "binance_futures_usdt_perp",
        "filters": {
            "top_each_side": args.top,
            "min_quote_volume_usdt": args.min_quote_m_usdt,
        },
        "black_swan_tape": black,
        "red_swan_tape": red,
    }

    gem_text = None
    if args.gemini:
        gem_text = _gemini_interpret(black, red, args.model)
        if gem_text:
            out["gemini"] = json.loads(gem_text)

    if args.json:
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return

    print("=" * 72)
    print(f"黑天鹅盘面侧（24h 跌幅榜 Top {args.top}，成交额≥{args.min_quote_m_usdt:,.0f} USDT）")
    print("=" * 72)
    for r in black:
        fr = r.get("funding_rate")
        fr_s = f"{fr * 100:.4f}%" if fr is not None else "—"
        print(
            f"  {r['symbol']:<16} {r['change_24h_pct']:+8.2f}%  "
            f"U成交额={r['quote_volume_24h']:>14,.0f}  费率={fr_s}"
        )

    print()
    print("=" * 72)
    print(f"红天鹅盘面侧（24h 涨幅榜 Top {args.top}，成交额≥{args.min_quote_m_usdt:,.0f} USDT）")
    print("=" * 72)
    for r in red:
        fr = r.get("funding_rate")
        fr_s = f"{fr * 100:.4f}%" if fr is not None else "—"
        print(
            f"  {r['symbol']:<16} {r['change_24h_pct']:+8.2f}%  "
            f"U成交额={r['quote_volume_24h']:>14,.0f}  费率={fr_s}"
        )

    if args.gemini and gem_text:
        print()
        print("=" * 72)
        print("Gemini 盘面解读（实验性）")
        print("=" * 72)
        print(gem_text)


if __name__ == "__main__":
    main()
