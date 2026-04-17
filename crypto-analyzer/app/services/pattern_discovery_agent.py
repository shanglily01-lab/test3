#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PatternDiscoveryAgent

使用 Gemini 大模型，基于 MarketContextBuilder 生成的语义上下文，
发现人类未曾定义的市场预测维度和交叉信号假设。

使用 google-generativeai 旧 SDK（已验证可用）。
"""

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Fix Windows terminal UTF-8 display
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import google.generativeai as genai
from loguru import logger

from app.services.market_context_builder import MarketContextBuilder

# ─── 配置 ────────────────────────────────────────────────────────────────────

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL   = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

RESULTS_DIR = Path(__file__).parent.parent.parent / "discovery_results"


# ─── Gemini 调用层 ─────────────────────────────────────────────────────────────

def _init_gemini() -> genai.GenerativeModel:
    if not GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY not set in environment")
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel(
        model_name=GEMINI_MODEL,
        generation_config=genai.types.GenerationConfig(
            temperature=0.7,
            max_output_tokens=8192,
            response_mime_type="application/json",
        ),
    )
    logger.info(f"Gemini model ready: {GEMINI_MODEL}")
    return model


def _call_gemini(model: genai.GenerativeModel, prompt: str) -> str:
    """调用 Gemini，返回原始文本响应。失败时重试一次。"""
    for attempt in range(2):
        try:
            response = model.generate_content(prompt)
            return response.text
        except Exception as e:
            if attempt == 0:
                logger.warning(f"Gemini call failed (attempt 1): {e} — retrying in 5s")
                time.sleep(5)
            else:
                raise RuntimeError(f"Gemini call failed after 2 attempts: {e}") from e
    return ""  # unreachable


# ─── 响应解析 ─────────────────────────────────────────────────────────────────

def _repair_truncated_json(text: str) -> str:
    """
    Robustly repair a truncated JSON response from Gemini.
    Strategy: scan all occurrences of '\\n    }' (which closes a trade plan entry),
    try each one as a cutpoint, keep the last one that yields valid JSON.
    Falls back to simple bracket-balancing if that fails.
    """
    close_marker = "\n    }"
    suffix = "\n  ]\n}"

    # Strategy 1: find last position of close_marker that gives valid JSON
    last_valid_candidate = None
    pos = 0
    while True:
        idx = text.find(close_marker, pos)
        if idx == -1:
            break
        candidate = text[:idx + len(close_marker)].rstrip(",\n ") + suffix
        try:
            import json as _j
            _j.loads(candidate)
            last_valid_candidate = candidate
        except _j.JSONDecodeError:
            pass
        pos = idx + 1

    if last_valid_candidate is not None:
        return last_valid_candidate

    # Strategy 2: bracket-depth balance
    depth_brace = 0
    depth_bracket = 0
    for ch in text:
        if ch == "{":
            depth_brace += 1
        elif ch == "}":
            depth_brace -= 1
        elif ch == "[":
            depth_bracket += 1
        elif ch == "]":
            depth_bracket -= 1

    if depth_brace > 0 or depth_bracket > 0:
        last_clean = max(text.rfind('"win_rate_basis"'), text.rfind('"invalidation"'))
        if last_clean > 0:
            brace_end = text.find("\n    }", last_clean)
            if brace_end > 0:
                return text[:brace_end + 6].rstrip(",\n ") + suffix

    # Strategy 3: 截断发生在某个完整数组之后（如 confluence_signals），
    # 找最后一个 ] 然后补全对象/数组/根对象的关闭括号
    import json as _j3
    last_bracket = text.rfind("]")
    if last_bracket > 0:
        candidate = text[:last_bracket + 1].rstrip(",\n ") + "\n    }\n  ]\n}"
        try:
            _j3.loads(candidate)
            return candidate
        except _j3.JSONDecodeError:
            pass

    # Strategy 4: 截断发生在某个字段值的字符串中（mid-string truncation）。
    # 扫描字符流，找最后一个 brace_depth==2, bracket_depth==1 时的逗号
    # （即 trade_plans 数组中某个计划对象内最后一个完整字段末尾的逗号），
    # 从该位置截断并补全闭合括号。
    in_str = False
    esc = False
    bd = 0
    bkt = 0
    last_field_comma = -1

    for i, ch in enumerate(text):
        if esc:
            esc = False
            continue
        if ch == '\\' and in_str:
            esc = True
            continue
        if ch == '"':
            in_str = not in_str
            continue
        if in_str:
            continue
        if ch == '{':
            bd += 1
        elif ch == '}':
            bd -= 1
        elif ch == '[':
            bkt += 1
        elif ch == ']':
            bkt -= 1
        elif ch == ',' and bd == 2 and bkt == 1:
            last_field_comma = i

    if last_field_comma > 0:
        for closing in ("\n    }\n  ]\n}", "\n  ]\n}"):
            candidate = text[:last_field_comma] + closing
            try:
                import json as _j4
                _j4.loads(candidate)
                return candidate
            except _j4.JSONDecodeError:
                pass

    return text


def _extract_json(text: str) -> dict:
    """
    从 Gemini 响应中提取 JSON。
    支持：直接 JSON、```json 包裹、截断修复。
    """
    import re
    text = text.strip()

    # 去除代码块包裹
    code_match = re.search(r"```(?:json)?\s*([\s\S]+?)(?:```|$)", text)
    if code_match:
        text = code_match.group(1).strip()

    # 找 JSON 范围
    start = text.find("{")
    if start == -1:
        logger.warning("No JSON object found in response")
        return {"raw_response": text, "parse_error": "No JSON found"}

    json_text = text[start:]

    # 先尝试直接解析
    try:
        return json.loads(json_text)
    except json.JSONDecodeError:
        pass

    # 尝试截断到最后完整的 }
    end = json_text.rfind("}")
    if end != -1:
        try:
            return json.loads(json_text[:end+1])
        except json.JSONDecodeError:
            pass

    # 尝试修复截断
    repaired = _repair_truncated_json(json_text)
    try:
        return json.loads(repaired)
    except json.JSONDecodeError:
        pass

    logger.warning("Could not parse JSON from response — returning raw text wrapper")
    return {"raw_response": text, "parse_error": "JSON extraction failed"}


# ─── 结果展示 ─────────────────────────────────────────────────────────────────

def _pct(entry: float, target: float) -> str:
    """计算目标相对入场的百分比变化"""
    if entry <= 0:
        return "?"
    return f"{(target / entry - 1) * 100:+.2f}%"


def _display_trade_card(plan: dict, best: str) -> None:
    """打印单个交易计划卡片"""
    sym        = plan.get("symbol", "?")
    direction  = plan.get("direction", "?").upper()
    entry_zone = plan.get("entry_zone", {})
    entry_lo   = entry_zone.get("low", 0)
    entry_hi   = entry_zone.get("high", 0)
    entry_mid  = (entry_lo + entry_hi) / 2 if entry_lo and entry_hi else 0
    sl         = plan.get("stop_loss", 0)
    t1         = plan.get("target1", 0)
    t2         = plan.get("target2", 0)
    wr         = plan.get("win_rate_pct", "?")
    rr         = plan.get("risk_reward", "?")
    conf       = plan.get("confidence", "?")
    window     = plan.get("time_window", "")
    trigger    = plan.get("entry_trigger", "")
    invalid    = plan.get("invalidation", "")
    signals    = plan.get("confluence_signals", [])
    wr_basis   = plan.get("win_rate_basis", "")

    star = " [BEST CONVICTION]" if sym == best else ""

    if direction == "SKIP":
        print(f"  {sym:6s}  SKIP — {plan.get('invalidation', plan.get('win_rate_basis', ''))}")
        return

    dir_label = "LONG  ^" if direction == "LONG" else "SHORT v"

    print(f"")
    print(f"  +----------------------------------------------------------+")
    print(f"  | {sym:6s}  {dir_label}  WIN RATE: {wr}%   R/R: {rr}x   CONF: {conf}/10{star}")
    print(f"  +----------------------------------------------------------+")
    if entry_lo and entry_hi:
        print(f"  | Entry zone : ${entry_lo:>12,.2f} — ${entry_hi:>12,.2f}")
    if sl:
        sl_pct = _pct(entry_mid, sl) if entry_mid else "?"
        print(f"  | Stop loss  : ${sl:>12,.2f}  ({sl_pct} from entry mid)")
    if t1:
        t1_pct = _pct(entry_mid, t1) if entry_mid else "?"
        print(f"  | Target 1   : ${t1:>12,.2f}  ({t1_pct} — take 50%)")
    if t2:
        t2_pct = _pct(entry_mid, t2) if entry_mid else "?"
        print(f"  | Target 2   : ${t2:>12,.2f}  ({t2_pct} — trail rest)")
    if window:
        print(f"  | Window     : {window}")
    if trigger:
        print(f"  | Trigger    : {trigger}")
    if invalid:
        print(f"  | Invalidate : {invalid}")
    if signals:
        print(f"  | Signals    : {' | '.join(signals[:4])}")
    if wr_basis:
        # 截断长文本
        basis_short = wr_basis[:100] + ("..." if len(wr_basis) > 100 else "")
        print(f"  | WR basis   : {basis_short}")
    print(f"  +----------------------------------------------------------+")


def _display_results(result: dict, timestamp: str) -> None:
    """打印交易计划卡片（面向操盘手）"""
    sep = "=" * 66
    print(f"\n{sep}")
    print(f"  TRADING SIGNALS  |  {timestamp}")
    print(sep)

    # 市场状态
    regime  = result.get("market_regime", "")
    insight = result.get("cross_asset_insight", "")
    best    = result.get("highest_conviction_trade", "")

    if regime:
        print(f"  Market regime : {regime.upper()}")
    if best:
        print(f"  Best trade    : {best}")
    if insight:
        print(f"  Cross-asset   : {insight}")

    # 交易计划卡片
    plans = result.get("trade_plans", [])
    if plans:
        print(f"\n  --- TRADE PLANS ({len(plans)} symbols) ---")
        for plan in plans:
            _display_trade_card(plan, best)
    elif "raw_response" in result:
        # JSON 解析失败时打印原始响应
        print(f"\n  [RAW RESPONSE — JSON parse failed]")
        print(result["raw_response"][:2000])

    print(f"\n{sep}\n")


# ─── 结果持久化 ───────────────────────────────────────────────────────────────

def _save_results(result: dict, context: str, timestamp_str: str) -> Path:
    """保存发现结果到 discovery_results/ 目录"""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    safe_ts = timestamp_str.replace(":", "").replace(" ", "_").replace("/", "-")
    out_path = RESULTS_DIR / f"discovery_{safe_ts}.json"

    payload = {
        "timestamp":       timestamp_str,
        "model":           GEMINI_MODEL,
        "context_length":  len(context),
        "results":         result,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info(f"Results saved: {out_path}")
    return out_path


# ─── 主入口类 ─────────────────────────────────────────────────────────────────

class PatternDiscoveryAgent:
    """
    端到端发现流程：
      1. MarketContextBuilder 构建语义上下文
      2. Gemini 推理发现未知维度
      3. 解析 JSON 响应
      4. 打印 + 持久化结果
    """

    def __init__(self, symbols: list[str] = None):
        self.builder = MarketContextBuilder(symbols)
        self.model   = _init_gemini()

    def run(self, save: bool = True) -> dict:
        """
        执行完整发现流程。
        返回解析后的结果字典。
        """
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        logger.info(f"PatternDiscoveryAgent starting at {ts}")
        logger.info(f"Symbols: {self.builder.symbols}")

        # Step 1: 构建上下文
        logger.info("Step 1/3: Building market context...")
        t0 = time.time()
        try:
            full_prompt = self.builder.build_with_query()
        except Exception as e:
            logger.error(f"Context build failed: {e}")
            raise
        build_time = time.time() - t0
        logger.info(f"Context ready: {len(full_prompt)} chars in {build_time:.1f}s")

        # Step 2: Gemini 推理
        logger.info(f"Step 2/3: Calling Gemini ({GEMINI_MODEL})...")
        t1 = time.time()
        raw_response = _call_gemini(self.model, full_prompt)
        call_time = time.time() - t1
        logger.info(f"Gemini responded in {call_time:.1f}s ({len(raw_response)} chars)")

        # Step 3: 解析 + 展示
        logger.info("Step 3/3: Parsing response...")
        result = _extract_json(raw_response)
        _display_results(result, ts)

        # 持久化
        if save:
            path = _save_results(result, full_prompt, ts)
            print(f"Full results saved to: {path}\n")

        return result


# ─── 直接运行 ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    from dotenv import load_dotenv
    load_dotenv()

    agent = PatternDiscoveryAgent()
    try:
        agent.run()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)
