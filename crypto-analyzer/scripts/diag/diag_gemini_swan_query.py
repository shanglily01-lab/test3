"""
Gemini 黑天鹅 / 红天鹅候选查询.

一次性查询脚本: 把 GEMINI_TOP30 (28 个大币种) 的简化市场数据打包发给 Gemini,
让它综合判断 7 天内最可能暴跌 (>20% drop) 和最可能暴涨 (>20% rally) 的候选.

只读 dimesion 库, 不下单, 不改 DB.
用法: python scripts/diag/diag_gemini_swan_query.py
"""
import sys
import os
import json
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from dotenv import load_dotenv
load_dotenv(ROOT / '.env')

import strategy_bigmid as sb


def _fetch_compact_data(cur, sym: str) -> dict:
    """拉每个 symbol 简化数据: 当前价 / 24h+7d 涨跌幅 / RSI / 成交量 / 7 天 daily K 概要."""
    daily = sb._fetch_klines(cur, sym, '1d', 8)  # 8 天 daily
    h1    = sb._fetch_klines(cur, sym, '1h', 168)  # 7 天 1h
    if len(daily) < 7 or len(h1) < 100:
        return None
    try:
        cur_p = sb.get_price(sym)
    except Exception:
        return None

    cur.execute("SELECT change_24h, volume_24h FROM price_stats_24h WHERE symbol=%s LIMIT 1", (sym,))
    r = cur.fetchone() or {}
    ch_24h = float(r.get('change_24h') or 0)
    vol_24h = float(r.get('volume_24h') or 0)

    # 7d 涨跌
    p_7d_ago = float(daily[0]['close_price']) if daily else cur_p
    ch_7d = (cur_p - p_7d_ago) / p_7d_ago * 100 if p_7d_ago else 0

    # RSI
    rsi_1h = sb._calc_rsi([float(b['close_price']) for b in h1], 14)
    rsi_d  = sb._calc_rsi([float(b['close_price']) for b in daily], 14) if len(daily) >= 15 else None

    # 7 天 daily K 概要 (open/close/high/low)
    daily_summary = []
    for b in daily[-7:]:
        daily_summary.append({
            't': datetime.fromtimestamp(b['open_time'] / 1000).strftime('%m-%d'),
            'o': round(float(b['open_price']), 6),
            'h': round(float(b['high_price']), 6),
            'l': round(float(b['low_price']), 6),
            'c': round(float(b['close_price']), 6),
            'v': round(float(b['volume'] or 0), 0),
        })

    return {
        'symbol':     sym,
        'cur_price':  round(cur_p, 6),
        'ch_24h_pct': round(ch_24h, 2),
        'ch_7d_pct':  round(ch_7d, 2),
        'rsi_1h':     rsi_1h,
        'rsi_daily':  rsi_d,
        'volume_24h': round(vol_24h, 0),
        'daily_7d':   daily_summary,
    }


def _build_swan_prompt(all_data: list) -> str:
    lines = [
        "You are an experienced crypto risk analyst. Below is recent market data for 28 top-cap cryptocurrencies.",
        "",
        "TASK:",
        "  Identify the most likely BLACK SWAN and RED SWAN candidates for the next 7 days.",
        "  - BLACK SWAN: coin most likely to crash >=20% within 7 days (look for: parabolic exhaustion, momentum divergence, breaking key support, RSI overbought + volume drying up, deteriorating funding/structure).",
        "  - RED SWAN: coin most likely to rally >=20% within 7 days (look for: deep oversold + volume capitulation, key support hold, accumulation patterns, RSI bullish divergence, prolonged consolidation breakout setup).",
        "  These are high-conviction tail-risk picks, not normal trading recommendations.",
        "",
        "DATA (each entry: symbol, current price, 24h %, 7d %, RSI 1h, RSI daily, 24h volume, last 7 daily K):",
        "",
    ]
    for d in all_data:
        lines.append(
            f"{d['symbol']:<14} cur={d['cur_price']} 24h={d['ch_24h_pct']:+.2f}% 7d={d['ch_7d_pct']:+.2f}% "
            f"RSI(1h)={d['rsi_1h']} RSI(d)={d['rsi_daily']} vol24h={d['volume_24h']:.0f}"
        )
        for k in d['daily_7d']:
            lines.append(f"  {k['t']}  O={k['o']:>12} H={k['h']:>12} L={k['l']:>12} C={k['c']:>12} V={k['v']:>14}")
        lines.append("")

    lines += [
        "OUTPUT (strict JSON, no markdown):",
        "{",
        '  "black_swans": [',
        '    {"symbol": "XXX/USDT", "reason": "<brief Chinese reason>", "confidence": <0-1>, "expected_drop_pct": <float>},',
        "    ...up to 3...",
        "  ],",
        '  "red_swans": [',
        '    {"symbol": "XXX/USDT", "reason": "<brief Chinese reason>", "confidence": <0-1>, "expected_rally_pct": <float>},',
        "    ...up to 3...",
        "  ],",
        '  "market_overall": "<brief Chinese overall market read, 1-2 sentences>"',
        "}",
        "",
        "If no clear candidate qualifies, return empty list. Don't pad to fill 3.",
    ]
    return "\n".join(lines)


def main():
    print(f"诊断: Gemini 黑天鹅/红天鹅候选 (top {len(sb.GEMINI_TOP30)})")
    print(f"模型: {sb.GEMINI_MODEL_NAME}")
    print("=" * 80)

    sb._load_bigmid_config()
    client = sb._init_gemini_client()
    if not client:
        print("ERROR: Gemini client 初始化失败")
        return

    # 拉数据
    print("\n[1/3] 拉取 28 个 symbol 的市场数据...")
    conn = sb._db_conn()
    cur = conn.cursor()
    all_data = []
    skipped = []
    try:
        for sym in sb.GEMINI_TOP30:
            d = _fetch_compact_data(cur, sym)
            if not d:
                skipped.append(sym)
                continue
            all_data.append(d)
    finally:
        cur.close()
        conn.close()
    print(f"  拉到 {len(all_data)} 个, 跳过 {len(skipped)}: {skipped}")

    if len(all_data) < 5:
        print("ERROR: 可用数据太少, 退出")
        return

    # 构造 prompt
    prompt = _build_swan_prompt(all_data)
    print(f"\n[2/3] Prompt 长度: {len(prompt)} 字符 (~{len(prompt)//4} tokens)")

    # 调 Gemini
    print(f"\n[3/3] 调 Gemini...")
    import time
    t0 = time.time()
    try:
        from google.genai import types
        config = types.GenerateContentConfig(
            response_mime_type='application/json',
            http_options=types.HttpOptions(timeout=60_000),  # 60s 超时, 数据量大
        )
        resp = client.models.generate_content(
            model=sb.GEMINI_MODEL_NAME,
            contents=prompt,
            config=config,
        )
        text = (resp.text or "").strip()
        if text.startswith("```"):
            text = text.strip("`").lstrip("json").strip()
        data = json.loads(text)
    except Exception as e:
        print(f"ERROR: Gemini 调用失败: {e}")
        return
    print(f"  耗时 {time.time()-t0:.1f}s")

    # 输出结果
    print("\n" + "=" * 80)
    print("Gemini 综合判断")
    print("=" * 80)

    overall = data.get('market_overall', '')
    if overall:
        print(f"\n[市场整体]: {overall}")

    print(f"\n[黑天鹅候选 (>=20% 下跌风险, 7 天内)]")
    bs = data.get('black_swans', []) or []
    if not bs:
        print("  (无明显候选)")
    for i, c in enumerate(bs, 1):
        print(f"  {i}. {c.get('symbol', '?'):<14} 预期跌 {c.get('expected_drop_pct', 0):.1f}%  conf={c.get('confidence', 0):.2f}")
        print(f"     reason: {c.get('reason', '')}")

    print(f"\n[红天鹅候选 (>=20% 暴涨潜力, 7 天内)]")
    rs = data.get('red_swans', []) or []
    if not rs:
        print("  (无明显候选)")
    for i, c in enumerate(rs, 1):
        print(f"  {i}. {c.get('symbol', '?'):<14} 预期涨 {c.get('expected_rally_pct', 0):.1f}%  conf={c.get('confidence', 0):.2f}")
        print(f"     reason: {c.get('reason', '')}")

    print("\n" + "=" * 80)
    print("注: Gemini 自己也承认会预测错, 这只是基于近期技术数据的概率推断, 不是建议直接下单.")


if __name__ == '__main__':
    main()
