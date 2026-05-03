#!/usr/bin/env python3
"""
通过 Gemini 归纳「黑天鹅」与「红天鹅」语境下值得关注的合约/现货交易对。

说明:
  - 黑天鹅 (Black Swan): 尾部极端负向冲击 — 崩盘、连环爆仓、脱锚、信任危机等。
  - 红天鹅 (Red Swan): 与黑天鹅相对 framing — 极端正向尾部 /「暴涨」叙事（叙事驱动、高 beta、
    squeeze、主线赛道龙头等语境里常被讨论的标的；非承诺涨幅）。

依赖: pip install google-genai
环境: GEMINI_API_KEY 必填; GEMINI_MODEL 可选 (默认与 strategy_bigmid 一致).

用法:
  cd crypto-analyzer
  python scripts/diag/diag_gemini_swan_pairs.py

输出: 打印 Gemini 返回的 JSON（stderr 可配 DEBUG）。
"""

from __future__ import annotations

import json
import os
import sys

# 保证可从 scripts/diag 直接运行
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(ROOT, ".env"))
except ImportError:
    pass

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3-flash-preview")
TIMEOUT_S = int(os.getenv("GEMINI_SWAN_TIMEOUT_S", "120"))


SWAN_PROMPT = """你是加密货币衍生品与宏观风险研究员。请基于截至你知识截止日的公开市场结构常识，
列出适合讨论「黑天鹅」与「红天鹅」场景的 **USDT 本位永续/交割合约常见交易对**（也可用少量市值前列现货对），
**不要**宣称必然盈利或给出具体开仓指令。

术语（请在输出里用你自己的话简要复述）：
1) 黑天鹅交易语境：难以事前精确预测的极端负向尾部 — 恐慌崩盘、流动性枯竭、重大脱锚传闻、行业级信任危机等。
2) 红天鹅交易语境：与黑天鹅相对的 framing — 极端正向尾部 /「暴涨」叙事（常在叙事催化、风险偏好回升、
   主线赛道、高 beta 山寨、空头挤压等讨论中出现；强调的是上行尾部可能性与典型标的类型，不是喊单）。

任务：
- **黑天鹅侧** 与 **红天鹅侧** 各给出 **8～15** 个交易对字符串，格式与 Binance 类似：`BASE/USDT`（如 `BTC/USDT`）。
- 每个交易对应标注 `category`：`black_swan`（崩盘/避险/脱锚等语境）或 `red_swan`（暴涨/叙事/高弹性等语境）。
  若同一币种两边都可讨论，选最主要的一类并在 note 里一句话提另一面。
- 每个给出简短中文 `note`（1～2 句：该对在对应语境下的 **典型讨论角色**，如避险锚、稳定币相关、高 beta、赛道龙头等）。
- 明确列出主要 **风险**：杠杆、插针、流动性、相关性突变；红天鹅侧务必强调暴涨叙事同样可能反向剧烈回撤。

输出 **仅** 一个合法 JSON 对象，不要用 markdown 代码围栏，不要额外文字：
{
  "term_black_swan_zh": "你对本任务中黑天鹅语境的一句话定义",
  "term_red_swan_zh": "你对本任务中红天鹅语境的一句话定义",
  "pairs": [
    {
      "symbol": "BTC/USDT",
      "category": "black_swan",
      "note": "..."
    }
  ],
  "risk_warning_zh": "通用风险提示一段话",
  "disclaimer_zh": "历史与结构性讨论不构成投资建议"
}
"""


def _init_client():
    if not GEMINI_API_KEY:
        print("ERROR: 请设置环境变量 GEMINI_API_KEY", file=sys.stderr)
        return None
    try:
        from google import genai
        return genai.Client(api_key=GEMINI_API_KEY)
    except ImportError:
        print("ERROR: 请 pip install google-genai", file=sys.stderr)
        return None


def _call(client, prompt: str) -> dict | None:
    try:
        from google.genai import types
        config = types.GenerateContentConfig(
            response_mime_type="application/json",
            http_options=types.HttpOptions(timeout=TIMEOUT_S * 1000),
        )
        resp = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
            config=config,
        )
        text = (resp.text or "").strip()
        if text.startswith("```"):
            text = text.strip("`").lstrip("json").strip()
        return json.loads(text)
    except Exception as e:
        print(f"ERROR: Gemini 调用失败: {e}", file=sys.stderr)
        return None


def main():
    client = _init_client()
    if not client:
        sys.exit(1)
    print(f"模型: {GEMINI_MODEL}，超时 {TIMEOUT_S}s …", file=sys.stderr)
    data = _call(client, SWAN_PROMPT)
    if not data:
        sys.exit(2)
    # 轻量校验
    pairs = data.get("pairs") or []
    if not isinstance(pairs, list):
        print("WARN: pairs 非列表", file=sys.stderr)
    print(json.dumps(data, ensure_ascii=False, indent=2))

    # 人类可读摘要
    print("\n--- 摘要 ---", file=sys.stderr)
    print(data.get("term_black_swan_zh", ""), file=sys.stderr)
    print(data.get("term_red_swan_zh", ""), file=sys.stderr)
    bs = [p for p in pairs if str(p.get("category", "")).lower() == "black_swan"]
    rs = [p for p in pairs if str(p.get("category", "")).lower() == "red_swan"]
    print(f"黑天鹅侧重: {len(bs)} 个", file=sys.stderr)
    print(f"红天鹅侧重: {len(rs)} 个", file=sys.stderr)


if __name__ == "__main__":
    main()
