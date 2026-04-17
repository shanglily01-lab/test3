# -*- coding: utf-8 -*-
"""
btc_gemini_regime.py
====================
BTC 大方向 Gemini 探测器。

每 15 分钟把最近 BTC 市场快照（1h 近 8 根 + 15m 近 8 根 + 衍生指标）打包发给
Gemini，让其判断当前是否处于"强多/强空/震荡"状态，并返回结构化结果：

    {
        "verdict":    "STRONG_LONG" | "STRONG_SHORT" | "NEUTRAL",
        "confidence": float in [0, 1],
        "reason":     "...",
        "raw":        原始文本,
        "ts":         unix timestamp,
    }

只被 dimension_trader.py 通过后台线程调用；本模块不依赖 dimension_trader。
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
from pathlib import Path

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ── UTF-8 输出（Windows 控制台）─────────────────────────────────────────────
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

ROOT           = Path(__file__).parent
LOG_DIR        = ROOT / "btc_gemini_logs"
LOG_DIR.mkdir(exist_ok=True)

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
GEMINI_MODEL   = os.getenv("GEMINI_MODEL", "gemini-2.5-flash").strip()

VALID_VERDICTS = {"STRONG_LONG", "STRONG_SHORT", "NEUTRAL"}


# ── Windows 代理自动检测 ───────────────────────────────────────────────────
def _detect_system_proxy() -> dict | None:
    if sys.platform != "win32":
        return None
    if os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy"):
        return None
    try:
        import subprocess
        r = subprocess.run(
            ["reg", "query",
             r"HKCU\Software\Microsoft\Windows\CurrentVersion\Internet Settings",
             "/v", "ProxyServer"],
            capture_output=True, text=True, timeout=3,
        )
        if r.returncode != 0:
            return None
        for line in r.stdout.splitlines():
            if "ProxyServer" in line:
                addr = line.strip().split()[-1]
                if addr and ":" in addr:
                    return {"http": f"http://{addr}", "https": f"http://{addr}"}
    except Exception:
        return None
    return None


_PROXIES = _detect_system_proxy()


# ── 快照构建 ───────────────────────────────────────────────────────────────
def _f(x, nd=2):
    try: return round(float(x), nd)
    except Exception: return x


def build_btc_snapshot(cs1h: list[dict], cs15m: list[dict],
                        cs5m: list[dict] | None = None,
                        hours_1h: int = 8, bars_15m: int = 8,
                        bars_5m: int = 12) -> dict:
    """把 BTC K 线切成 Gemini 友好的结构化快照。

    期望每根 K 线字段：t, open, high, low, close, vol, buy_vol

    5m 维度用于捕捉 15m 未闭合前的早期反转信号（例如 15m 尚震荡、
    5m 已连续单边）。
    """
    c1  = cs1h[-hours_1h:]   if len(cs1h)  >= hours_1h  else cs1h
    c15 = cs15m[-bars_15m:]  if len(cs15m) >= bars_15m else cs15m
    c5  = (cs5m or [])[-bars_5m:] if cs5m and len(cs5m) >= bars_5m else (cs5m or [])

    def summarize(window: list[dict], label: str) -> dict:
        if not window:
            return {"label": label, "n": 0}
        first_o = float(window[0]["open"])
        last_c  = float(window[-1]["close"])
        highs = [float(b["high"]) for b in window]
        lows  = [float(b["low"])  for b in window]
        vols  = [float(b["vol"])  for b in window]
        buys  = [float(b.get("buy_vol", 0)) for b in window]
        total_vol  = sum(vols)
        total_buy  = sum(buys)
        buy_ratio  = (total_buy / total_vol) if total_vol > 0 else 0.5
        rng_pct    = (max(highs) - min(lows)) / first_o * 100 if first_o else 0
        net_pct    = (last_c - first_o) / first_o * 100 if first_o else 0
        body_sum   = sum((float(b["close"]) - float(b["open"])) for b in window)
        body_pct   = body_sum / first_o * 100 if first_o else 0
        up_bars    = sum(1 for b in window if float(b["close"]) > float(b["open"]))
        return {
            "label":      label,
            "n":          len(window),
            "first_open": _f(first_o),
            "last_close": _f(last_c),
            "net_pct":    _f(net_pct, 3),
            "body_sum_pct": _f(body_pct, 3),
            "range_pct":  _f(rng_pct, 3),
            "up_bars":    up_bars,
            "down_bars":  len(window) - up_bars,
            "total_vol":  _f(total_vol, 1),
            "buy_ratio":  _f(buy_ratio, 3),
            "high":       _f(max(highs)),
            "low":        _f(min(lows)),
        }

    def bar_rows(window: list[dict]) -> list[dict]:
        rows = []
        for b in window:
            o = float(b["open"]); h = float(b["high"]); l = float(b["low"]); c = float(b["close"])
            v = float(b["vol"]); bv = float(b.get("buy_vol", 0))
            bratio = (bv / v) if v > 0 else 0.5
            rows.append({
                "t":    str(b.get("t")),
                "o":    _f(o),
                "h":    _f(h),
                "l":    _f(l),
                "c":    _f(c),
                "chg%": _f((c - o) / o * 100 if o else 0, 3),
                "vol":  _f(v, 1),
                "br":   _f(bratio, 3),
            })
        return rows

    latest_close = None
    if c5:         latest_close = float(c5[-1]["close"])
    elif cs15m:    latest_close = float(cs15m[-1]["close"])
    elif cs1h:     latest_close = float(cs1h[-1]["close"])

    snapshot = {
        "symbol":      "BTC/USDT",
        "now_price":   _f(latest_close) if latest_close is not None else None,
        "summary_1h":  summarize(c1,  "1h x{}".format(len(c1))),
        "summary_15m": summarize(c15, "15m x{}".format(len(c15))),
        "summary_5m":  summarize(c5,  "5m x{}".format(len(c5))),
        "bars_1h":     bar_rows(c1),
        "bars_15m":    bar_rows(c15),
        "bars_5m":     bar_rows(c5),
    }
    return snapshot


# ── Prompt / Gemini ────────────────────────────────────────────────────────
_PROMPT_TMPL = """\
你是资深加密量化交易员。下面是 BTC/USDT 最近的三周期市场快照。

数据维度（由慢到快）：
  - 1h  x{n1}  → 最近 {n1} 小时（方向锚）
  - 15m x{n15} → 最近 {m15} 分钟（中周期结构）
  - 5m  x{n5}  → 最近 {m5} 分钟（最敏感，用于捕捉 15m 未闭合的早期反转）

字段说明：o/h/l/c 开高低收，chg% 本根百分比变动，vol 成交量，br=买方占比
(taker_buy_base / vol)；br>0.55 偏买方主动，br<0.45 偏卖方主动。
summary_* 是该周期汇总：net_pct 净涨跌，range_pct 最高最低差，up_bars/down_bars
阳阴数，buy_ratio 加权买方占比。

请判断**当前瞬时**应当：

  - STRONG_SHORT → 市场强跌，禁止开 LONG（接多就是接刀）
  - STRONG_LONG  → 市场强涨，禁止开 SHORT（逆势必亏）
  - NEUTRAL      → 震荡或方向不够明确，双向都允许

判定原则（必须谨慎）：
  A. 至少两个周期方向一致（1h+15m 同向，或 15m+5m 同向）
  B. 买方占比（br/buy_ratio）要配合方向（涨趋势 br 偏高，跌趋势 br 偏低）
  C. 5m 单根急涨/急跌若与 15m 方向矛盾，通常是噪声，判 NEUTRAL
  D. 任何方向单周期突刺（只有 5m 同向，1h+15m 不配合）一律 NEUTRAL
  E. 无法清晰判断时 NEUTRAL，confidence ≤ 0.5

## 快照（JSON）

```json
{snapshot_json}
```

## 输出格式（严格 JSON，不要任何额外文本、解释、markdown 包装）

```json
{{
  "verdict":    "STRONG_LONG" | "STRONG_SHORT" | "NEUTRAL",
  "confidence": 0.0-1.0,
  "reason":     "一句话说明核心证据（含关键数字，至少引用 5m/15m/1h 中两个周期的 net_pct 或 buy_ratio）"
}}
```

只输出那一个 JSON 对象，不要输出别的东西。
"""


def _call_gemini(prompt: str, temperature: float = 0.2,
                 max_tokens: int = 4000, timeout: int = 60) -> str:
    if not GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY missing in .env")
    url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
           f"{GEMINI_MODEL}:generateContent")
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": temperature,
            "maxOutputTokens": max_tokens,
        },
    }
    resp = requests.post(
        url,
        headers={"x-goog-api-key": GEMINI_API_KEY, "Content-Type": "application/json"},
        json=payload,
        timeout=timeout,
        proxies=_PROXIES,
    )
    resp.raise_for_status()
    data = resp.json()
    parts = data["candidates"][0]["content"]["parts"]
    text = next((p["text"] for p in reversed(parts) if "text" in p), "")
    return text


def _extract_json_block(text: str) -> str | None:
    """从文本中抽出第一个平衡的 {...} 代码块（支持嵌套，忽略字符串内的括号）。"""
    if not text:
        return None
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = re.sub(r"^```[a-zA-Z]*\s*", "", stripped)
        stripped = re.sub(r"\s*```$", "", stripped)
    depth = 0
    start = -1
    in_str = False
    esc = False
    for i, ch in enumerate(stripped):
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
        elif ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and start >= 0:
                return stripped[start:i+1]
    return None


def _parse_verdict(text: str) -> dict:
    """从 Gemini 文本里提取结构化判定。"""
    if not text:
        return {"verdict": "NEUTRAL", "confidence": 0.0, "reason": "empty response"}
    block = _extract_json_block(text)
    if not block:
        return {"verdict": "NEUTRAL", "confidence": 0.0,
                "reason": f"no JSON in response: {text[:120]}"}
    try:
        obj = json.loads(block)
    except Exception:
        try:
            obj = json.loads(block.replace("'", '"'))
        except Exception as e:
            return {"verdict": "NEUTRAL", "confidence": 0.0,
                    "reason": f"JSON parse err: {e} | {block[:200]}"}
    v  = str(obj.get("verdict", "NEUTRAL")).upper().strip()
    if v not in VALID_VERDICTS:
        v = "NEUTRAL"
    try:    c = float(obj.get("confidence", 0.0))
    except: c = 0.0
    c = max(0.0, min(1.0, c))
    r  = str(obj.get("reason", "")).strip()[:500]
    return {"verdict": v, "confidence": c, "reason": r}


def ask_gemini_btc(cs1h: list[dict], cs15m: list[dict],
                   cs5m: list[dict] | None = None,
                   hours_1h: int = 8, bars_15m: int = 8,
                   bars_5m: int = 12) -> dict:
    """同步调用一次 Gemini，返回 {verdict, confidence, reason, raw, ts}。

    三周期数据（1h/15m/5m）一起喂给 Gemini 以捕捉早期反转信号。
    调用失败时返回 verdict='NEUTRAL'，reason 说明错误。
    """
    snapshot = build_btc_snapshot(cs1h, cs15m, cs5m,
                                   hours_1h=hours_1h,
                                   bars_15m=bars_15m,
                                   bars_5m=bars_5m)
    n1  = snapshot["summary_1h"].get("n", 0)
    n15 = snapshot["summary_15m"].get("n", 0)
    n5  = snapshot["summary_5m"].get("n", 0)
    prompt = _PROMPT_TMPL.format(
        n1=n1, n15=n15, n5=n5,
        m15=n15 * 15, m5=n5 * 5,
        snapshot_json=json.dumps(snapshot, ensure_ascii=False, indent=2),
    )
    ts = time.time()
    try:
        raw = _call_gemini(prompt)
    except Exception as e:
        return {"verdict": "NEUTRAL", "confidence": 0.0,
                "reason": f"gemini call failed: {e}",
                "raw": "", "ts": ts, "snapshot": snapshot}
    parsed = _parse_verdict(raw)
    parsed.update({"raw": raw, "ts": ts, "snapshot_summary": {
        "1h":  snapshot["summary_1h"],
        "15m": snapshot["summary_15m"],
        "5m":  snapshot["summary_5m"],
        "now_price": snapshot.get("now_price"),
    }})
    try:
        fname = LOG_DIR / time.strftime("btc_gemini_%Y%m%d.jsonl")
        with open(fname, "a", encoding="utf-8") as f:
            f.write(json.dumps({
                "ts": ts,
                "verdict": parsed["verdict"],
                "confidence": parsed["confidence"],
                "reason": parsed["reason"],
                "1h_summary":  snapshot["summary_1h"],
                "15m_summary": snapshot["summary_15m"],
                "5m_summary":  snapshot["summary_5m"],
                "now_price": snapshot.get("now_price"),
            }, ensure_ascii=False) + "\n")
    except Exception:
        pass
    return parsed


# ── CLI 自测 ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import pymysql
    conn = pymysql.connect(
        host=os.getenv("DB_HOST"), port=int(os.getenv("DB_PORT", 3306)),
        user=os.getenv("DB_USER"), password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"), charset="utf8mb4")

    def load(tf, n):
        with conn.cursor() as c:
            c.execute("""
                SELECT timestamp, open_price, high_price, low_price, close_price,
                       volume, taker_buy_base_volume
                FROM kline_data
                WHERE symbol='BTC/USDT' AND timeframe=%s
                  AND taker_buy_base_volume IS NOT NULL AND volume > 0
                ORDER BY timestamp DESC LIMIT %s
            """, (tf, n))
            rows = list(reversed(c.fetchall()))
        return [{"t": r[0], "open": float(r[1]), "high": float(r[2]),
                 "low": float(r[3]), "close": float(r[4]),
                 "vol": float(r[5]), "buy_vol": float(r[6])} for r in rows]

    c1  = load("1h",  10)
    c15 = load("15m", 10)
    c5  = load("5m",  14)
    conn.close()

    print(f"  loaded: 1h={len(c1)}  15m={len(c15)}  5m={len(c5)}")
    if _PROXIES:
        print(f"  proxy: {_PROXIES['https']}")
    r = ask_gemini_btc(c1, c15, c5)
    print()
    print(f"  verdict     = {r['verdict']}")
    print(f"  confidence  = {r['confidence']}")
    print(f"  reason      = {r['reason']}")
    print()
    print("  raw (len={}) ->".format(len(r.get("raw", ""))))
    print(r.get("raw", ""))
