#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
strategy_explorer.py
====================
AI 驱动的策略探索循环。Gemini 探索人类未发现的市场微结构维度，
生成信号代码，经三阶段回测逐步验证。

  Stage 1: Big4 (BTC/ETH/BNB/SOL)         快速筛选
  Stage 2: 随机 10 个山寨币                扩展验证
  Stage 3: 全量 99 个山寨币                完整评估

用法:
  .venv/Scripts/python.exe strategy_explorer.py              # 单轮，Gemini 生成5策略
  .venv/Scripts/python.exe strategy_explorer.py --rounds 3   # 3轮共15策略
  .venv/Scripts/python.exe strategy_explorer.py --no-gemini  # 用内置测试策略跑流程
"""

import argparse
import json
import os
import random
import subprocess
import sys
import textwrap
from datetime import datetime
from pathlib import Path

import pymysql
import requests
from dotenv import load_dotenv

load_dotenv()

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ── 自动检测系统代理（Windows Clash/TUN 模式）─────────────────────────────────

def _detect_system_proxy() -> dict | None:
    """读取 Windows 注册表代理设置，返回 requests proxies dict 或 None。"""
    if sys.platform != "win32":
        return None
    # 优先使用环境变量中已有的代理
    if os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy"):
        return None  # requests 会自动读取
    try:
        result = subprocess.run(
            ["reg", "query",
             r"HKCU\Software\Microsoft\Windows\CurrentVersion\Internet Settings",
             "/v", "ProxyServer"],
            capture_output=True, text=True, timeout=3
        )
        if result.returncode != 0:
            return None
        for line in result.stdout.splitlines():
            if "ProxyServer" in line:
                parts = line.strip().split()
                proxy_addr = parts[-1]  # e.g. "127.0.0.1:7897"
                proxy_url = f"http://{proxy_addr}"
                return {"http": proxy_url, "https": proxy_url}
    except Exception:
        pass
    return None

_SYSTEM_PROXIES = _detect_system_proxy()

# ── 配置 ──────────────────────────────────────────────────────────────────────

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL   = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

HOLD_BARS = 3
SL_MIN = 0.005; SL_MAX = 0.020; SL_MULT = 1.5; TP_MULT = 2.5

# 三阶段过滤阈值（均在训练集上评估）
STAGE1_MIN_N = 5;  STAGE1_MIN_WR = 0.57   # Big4 快速筛
STAGE2_MIN_N = 15; STAGE2_MIN_WR = 0.58   # 10 symbols
STAGE3_MIN_N = 30; STAGE3_MIN_WR = 0.60   # all symbols

# 训练/测试拆分（按时间序列）
TRAIN_RATIO = 0.70   # 前70%为训练集，后30%为走时测试集
TEST_MIN_N  = 10     # 测试集最低样本数（低于此值标注为样本不足）

BIG4 = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT"]

ALT99 = [
    "ETH/USDT", "SOL/USDT", "ZEC/USDT", "XRP/USDT", "DOGE/USDT", "HYPE/USDT",
    "1000PEPE/USDT", "BNB/USDT", "TAO/USDT", "ENJ/USDT", "ADA/USDT", "ENA/USDT",
    "AVAX/USDT", "LINK/USDT", "SUI/USDT", "DOT/USDT", "WLD/USDT", "AAVE/USDT",
    "NEAR/USDT", "FIL/USDT", "LTC/USDT", "BCH/USDT", "UNI/USDT", "TRX/USDT",
    "1000SHIB/USDT", "PENGU/USDT", "FET/USDT", "CRV/USDT", "1000BONK/USDT",
    "APT/USDT", "WIF/USDT", "VIRTUAL/USDT", "LDO/USDT", "GALA/USDT", "TON/USDT",
    "HBAR/USDT", "NEIRO/USDT", "ARB/USDT", "ONDO/USDT", "XLM/USDT", "ALGO/USDT",
    "OP/USDT", "RENDER/USDT", "ETC/USDT", "JTO/USDT", "DRIFT/USDT", "ORDI/USDT",
    "TRU/USDT", "XMR/USDT", "CAKE/USDT", "ONT/USDT", "TIA/USDT", "DUSK/USDT",
    "AXS/USDT", "ICP/USDT", "ZRO/USDT", "POL/USDT", "CHZ/USDT", "ATOM/USDT",
    "SEI/USDT", "BLUR/USDT", "INJ/USDT", "BOME/USDT", "SAND/USDT", "ETHFI/USDT",
    "STRK/USDT", "CTSI/USDT", "PENDLE/USDT", "EDU/USDT", "JUP/USDT",
    "1000FLOKI/USDT", "COMP/USDT", "XTZ/USDT", "W/USDT", "LPT/USDT", "ARKM/USDT",
    "SNX/USDT", "1000LUNC/USDT", "SUPER/USDT", "XVG/USDT", "IMX/USDT",
    "1000SATS/USDT", "APE/USDT", "PYTH/USDT", "AR/USDT", "FLOW/USDT", "ROSE/USDT",
    "DYDX/USDT", "ID/USDT", "ENS/USDT", "VET/USDT", "AXL/USDT", "STG/USDT",
    "TRB/USDT", "CFX/USDT", "CHR/USDT", "THETA/USDT", "STX/USDT", "IOTA/USDT",
]

RESULTS_DIR = Path(__file__).parent / "explorer_results"
RESULTS_DIR.mkdir(exist_ok=True)

_DB_CFG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 3306)),
    "user":     os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "binance-data"),
    "charset":  "utf8mb4",
}


# ── 数据加载 ───────────────────────────────────────────────────────────────────

def load(symbol, timeframe, limit=2000):
    conn = pymysql.connect(**_DB_CFG)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT timestamp, open_price, high_price, low_price, close_price,
                   volume, taker_buy_base_volume
            FROM kline_data
            WHERE symbol=%s AND timeframe=%s
              AND taker_buy_base_volume IS NOT NULL AND volume > 0
            ORDER BY timestamp ASC LIMIT %s
        """, (symbol, timeframe, limit))
        rows = cur.fetchall()
    conn.close()
    return [{"t": r[0], "open": float(r[1]), "high": float(r[2]),
             "low": float(r[3]), "close": float(r[4]),
             "vol": float(r[5]), "buy_vol": float(r[6])} for r in rows]


# ── 信号辅助函数 ───────────────────────────────────────────────────────────────

def gradient(cs, n):
    if len(cs) < n: return 0.0
    return sum(c["close"] - c["open"] for c in cs[-n:]) / cs[-1]["close"]

def flux(cs, n):
    if len(cs) < n: return 0.5
    rs = [c["buy_vol"] / c["vol"] for c in cs[-n:] if c["vol"] > 0]
    return sum(rs) / len(rs) if rs else 0.5

def amplitude(cs, n):
    if len(cs) < n: return 0.0
    return sum(c["high"] - c["low"] for c in cs[-n:]) / n / cs[-1]["close"]

def sl_tp(cs):
    amp = amplitude(cs, 6)
    amp = max(SL_MIN, min(SL_MAX, amp))
    return amp * SL_MULT, amp * TP_MULT

def align4h(cs1h, cs4h, i):
    ts = cs1h[i]["t"]
    return [c for c in cs4h if c["t"] <= ts]


# ── 回测引擎 ───────────────────────────────────────────────────────────────────

def bt_1h(fn, candles, i_start: int = 22, i_end: int = None):
    stats = {"n": 0, "win": 0, "pnl": []}
    n = len(candles)
    if i_end is None: i_end = n - HOLD_BARS
    for i in range(i_start, i_end):
        try:
            sig = fn(candles[:i+1])
        except Exception:
            continue
        if sig not in ("SHORT", "LONG"):
            continue
        entry = candles[i]["close"]
        sl_pct, tp_pct = sl_tp(candles[:i+1])
        sl_abs = entry * sl_pct
        tp_abs = entry * tp_pct
        outcome = None
        for j in range(1, HOLD_BARS + 1):
            if i + j >= n: break
            nxt = candles[i + j]
            if sig == "SHORT":
                if nxt["high"] - entry >= sl_abs: outcome = -sl_pct; break
                if entry - nxt["low"]  >= tp_abs: outcome =  tp_pct; break
            else:
                if entry - nxt["low"]  >= sl_abs: outcome = -sl_pct; break
                if nxt["high"] - entry >= tp_abs: outcome =  tp_pct; break
        if outcome is None:
            lj = min(HOLD_BARS, n - i - 1)
            if lj > 0:
                close_lj = candles[i + lj]["close"]
                outcome = (entry - close_lj) / entry if sig == "SHORT" else (close_lj - entry) / entry
        if outcome is None: continue
        stats["n"] += 1
        stats["pnl"].append(outcome)
        if outcome > 0: stats["win"] += 1
    return stats


def bt_mtf(fn, cs1h, cs4h, i_start: int = 22, i_end: int = None):
    stats = {"n": 0, "win": 0, "pnl": []}
    n = len(cs1h)
    if i_end is None: i_end = n - HOLD_BARS
    for i in range(i_start, i_end):
        s4 = align4h(cs1h, cs4h, i)
        if len(s4) < 4: continue
        try:
            sig = fn(cs1h[:i+1], s4)
        except Exception:
            continue
        if sig not in ("SHORT", "LONG"):
            continue
        entry = cs1h[i]["close"]
        sl_pct, tp_pct = sl_tp(cs1h[:i+1])
        sl_abs = entry * sl_pct
        tp_abs = entry * tp_pct
        outcome = None
        for j in range(1, HOLD_BARS + 1):
            if i + j >= n: break
            nxt = cs1h[i + j]
            if sig == "SHORT":
                if nxt["high"] - entry >= sl_abs: outcome = -sl_pct; break
                if entry - nxt["low"]  >= tp_abs: outcome =  tp_pct; break
            else:
                if entry - nxt["low"]  >= sl_abs: outcome = -sl_pct; break
                if nxt["high"] - entry >= tp_abs: outcome =  tp_pct; break
        if outcome is None:
            lj = min(HOLD_BARS, n - i - 1)
            if lj > 0:
                close_lj = cs1h[i + lj]["close"]
                outcome = (entry - close_lj) / entry if sig == "SHORT" else (close_lj - entry) / entry
        if outcome is None: continue
        stats["n"] += 1
        stats["pnl"].append(outcome)
        if outcome > 0: stats["win"] += 1
    return stats


def run_strategy(strat, d1h, d4h, symbols, phase: str = "all"):
    """
    在给定 symbols 上运行策略，返回聚合统计 + 逐币统计。
    phase: "all" | "train" | "test"
      - train: 前 TRAIN_RATIO 的 bar 区间
      - test:  后 (1-TRAIN_RATIO) 的 bar 区间
    """
    mode = strat["mode"]
    fn   = strat["fn"]
    agg  = {"n": 0, "win": 0, "pnl": []}
    per  = {}

    for sym in symbols:
        cs1h = d1h.get(sym, [])
        if len(cs1h) < 30: continue

        n1h = len(cs1h)
        split_i = int(n1h * TRAIN_RATIO)
        if phase == "train":
            i_start, i_end = 22, split_i
        elif phase == "test":
            i_start, i_end = split_i, n1h - HOLD_BARS
        else:
            i_start, i_end = 22, None

        try:
            if mode == "1h":
                s = bt_1h(fn, cs1h, i_start, i_end)
            elif mode == "mtf_self":
                cs4h = d4h.get(sym, [])
                if len(cs4h) < 10: continue
                s = bt_mtf(fn, cs1h, cs4h, i_start, i_end)
            elif mode == "mtf_btc":
                if sym == "BTC/USDT": continue
                btc4h = d4h.get("BTC/USDT", [])
                if len(btc4h) < 10: continue
                s = bt_mtf(fn, cs1h, btc4h, i_start, i_end)
            else:
                continue
        except Exception:
            continue

        per[sym] = s
        agg["n"]   += s["n"]
        agg["win"] += s["win"]
        agg["pnl"] += s["pnl"]

    return agg, per


# ── Gemini 策略生成 ────────────────────────────────────────────────────────────

_KNOWN_STRATEGIES = """\
已知策略（禁止重复，但可以叠加新维度）:
1. D4b-FluxQuality  : 3连阳 + flux严格递减 + 最新flux<0.47 => SHORT
2. D1a-MTF-HDecay   : 自身4h宏观下行(<-0.4%) + 1h 3连阳 + 振幅&成交量双衰减 => SHORT
3. D3-AltLag        : BTC 4h下行(<-0.5%) + 山寨1h梯度>+0.3% + 山寨flux<0.55 => SHORT
4. AlienSell        : 4h gradient<-0.6% + 1h相位 ACCUMULATIVE/DRIVEN_UP => SHORT
5. AlienBuy         : 4h gradient>+0.6% + 1h相位 DISSIPATIVE/DRIVEN_DOWN => LONG
"""

_KNOWN_STRATEGIES_LONG = """\
已知做多策略（禁止重复）:
1. AlienBuy         : 4h gradient>+0.6% + 1h相位 DISSIPATIVE/DRIVEN_DOWN => LONG
2. E3-AltDipRecovery: BTC 4h强上行(>0.5%) + 山寨急跌+高振幅 + flux回升 => LONG
3. E2-MTFLong       : 4h宏观加速上行 + 1h回调但买压仍>0.52 => LONG
"""

_GEMINI_PROMPT_LONG_TMPL = """\
你是一个量化策略研究员，专门发现人类分析师尚未探索的市场微结构**做多信号**。

## 可用数据（每根K线字段）
open, high, low, close, vol（总成交量）, buy_vol（主动买量）

## 辅助函数（只能用这3个，禁止import）
- gradient(cs, n): 最近n根K线 sum(close-open) / current_close  [归一化方向动量]
- flux(cs, n): 最近n根K线 mean(buy_vol/vol)                    [买压比率 0-1，0.5中性]
- amplitude(cs, n): 最近n根K线 mean(high-low) / current_close  [归一化振幅]

## 做多信号的市场逻辑方向（给你灵感）
- 超卖反弹     : 价格急跌后买压快速回升（下影线密集、flux从低位回升）
- 吸筹确认     : 价格震荡但flux持续走强（价格不涨但买压在积累）
- BTC带动      : BTC强势但山寨还没跟上（滞后效应）
- 空头平仓     : 价格下跌但flux突然飙升（空头回补推动）
- 动能加速     : 4h宏观上行 + 1h短期回调是假摔（洗盘后继续）
- 低振幅突破前兆: 长期低振幅 + 买压积累 = 突破前蓄力
- 下影线吸收   : 大下影线（卖压被吸收）+ flux不弱

{known_strategies}

{prev_round_context}

## 任务
生成{num_strategies}个全新**做多**信号假设，要求：
- 必须只返回 "LONG"（不能返回SHORT）
- 市场微结构逻辑清晰（解释为什么之后价格会上涨）
- 探索上面列出的二阶效应或非线性关系，不要简单重复已知策略
- 代码简洁，只用辅助函数和基本Python运算（不要import）

## 输出格式（严格JSON数组，不要有任何其他文字）
[
  {{
    "name": "简短英文名",
    "mode": "1h",
    "hypothesis": "1-2句市场逻辑",
    "code": "def sig(cs1h, cs4h=None):\\n    ..."
  }}
]

## 函数规范
- 函数名必须是 sig，参数必须是 (cs1h, cs4h=None)
- mode=1h      : 只用 cs1h
- mode=mtf_self: 用 cs1h（目标币1h） + cs4h（同币4h）
- mode=mtf_btc : 用 cs1h（山寨1h） + cs4h（BTC的4h）
- 必须有 len 检查，不足时 return None
- 只能返回 "LONG" 或 None（禁止返回SHORT）

第{round_num}轮做多策略探索："""

_GEMINI_PROMPT_TMPL = """\
你是一个量化策略研究员，专门发现人类分析师尚未探索的市场微结构信号。

## 可用数据（每根K线字段）
open, high, low, close, vol（总成交量）, buy_vol（主动买量）

## 辅助函数（只能用这3个，禁止import）
- gradient(cs, n): 最近n根K线 sum(close-open) / current_close  [归一化方向动量]
- flux(cs, n): 最近n根K线 mean(buy_vol/vol)                    [买压比率 0-1，0.5中性]
- amplitude(cs, n): 最近n根K线 mean(high-low) / current_close  [归一化振幅]

## 可构建的派生维度（给你灵感）
- 梯度加速度   : gradient(cs, 3) vs gradient(cs, 6)  → 动量是否在减速
- flux趋势     : flux(cs, 3) vs flux(cs, 6)           → 买压是否在快速衰减
- 能量效率比   : abs(gradient) / amplitude             → 大振幅小方向 = 能量浪费
- K线实体占比  : (close-open) / (high-low)             → 越小越虚弱
- 上影线压力   : (high - max(open,close)) / (high-low) → 上方抛压强度
- 量价背离     : 成交量增加但梯度减小
- 买压加速衰减 : flux(cs, 2) vs flux(cs, 4) vs flux(cs, 6) 三段连续递减

{known_strategies}

{prev_round_context}

## 任务
生成{num_strategies}个全新信号假设，要求：
- 市场微结构逻辑清晰（解释为什么之后价格会按预期方向运动）
- 探索上面列出的二阶效应或非线性关系，不要简单重复已知策略的阈值
- 代码简洁，只用辅助函数和基本Python运算（不要import）

## 输出格式（严格JSON数组，不要有任何其他文字）
[
  {{
    "name": "简短英文名",
    "mode": "1h",
    "hypothesis": "1-2句市场逻辑",
    "code": "def sig(cs1h, cs4h=None):\\n    ..."
  }}
]

## 函数规范
- 函数名必须是 sig，参数必须是 (cs1h, cs4h=None)
- mode=1h      : 只用 cs1h
- mode=mtf_self: 用 cs1h（目标币1h） + cs4h（同币4h）
- mode=mtf_btc : 用 cs1h（山寨1h） + cs4h（BTC的4h）
- 必须有 len 检查，不足时 return None
- 只能返回 "SHORT", "LONG", 或 None

## 代码示例（展示可用的原始字段访问方式）
```python
# 访问最近3根K线实体占比
bodies = [(cs1h[-i]["close"] - cs1h[-i]["open"]) / (cs1h[-i]["high"] - cs1h[-i]["low"])
          if (cs1h[-i]["high"] - cs1h[-i]["low"]) > 0 else 0.5
          for i in range(1, 4)]

# 计算上影线比例（上影 / 整根范围）
upper_wicks = [(cs1h[-i]["high"] - max(cs1h[-i]["open"], cs1h[-i]["close"])) /
               (cs1h[-i]["high"] - cs1h[-i]["low"])
               if (cs1h[-i]["high"] - cs1h[-i]["low"]) > 0 else 0
               for i in range(1, 4)]
```

第{round_num}轮探索开始："""


def call_gemini(prompt: str) -> str:
    url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
           f"{GEMINI_MODEL}:generateContent")
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.95,
            "maxOutputTokens": 16384,
        },
    }
    resp = requests.post(
        url,
        headers={"x-goog-api-key": GEMINI_API_KEY, "Content-Type": "application/json"},
        json=payload,
        timeout=120,
        proxies=_SYSTEM_PROXIES,
    )
    resp.raise_for_status()
    data = resp.json()
    parts = data["candidates"][0]["content"]["parts"]
    # thinking 模式下第一个 part 是思考过程（无 text 或 thought=True），取最后一个 text part
    text = next((p["text"] for p in reversed(parts) if "text" in p), "")
    return text


def load_history() -> tuple[list, list]:
    """
    从所有历史结果文件加载已测试策略和近乎通过的策略。
    返回: (already_tried: list[dict], near_miss: list[dict])
    """
    already_tried = []
    near_miss = []
    for f in sorted(RESULTS_DIR.glob("explorer_*.json")):
        try:
            with open(f, encoding="utf-8") as fp:
                data = json.load(fp)
            if not isinstance(data, list):
                continue
            for r in data:
                if not isinstance(r, dict):
                    continue
                name = r.get("name", "")
                hyp  = r.get("hypothesis", "")
                if not name:
                    continue
                already_tried.append({"name": name, "hypothesis": hyp})
                # Stage4 近乎通过（train WR>=60% 但 test WR 在 54-59%）
                s3 = r.get("stage3") or {}
                s4 = r.get("stage4_test") or {}
                if s3.get("wr", 0) >= 0.60 and 0.54 <= s4.get("wr", 0) < 0.60:
                    near_miss.append({
                        "name": name, "hypothesis": hyp,
                        "mode": r.get("mode", ""),
                        "train_wr": s3.get("wr", 0),
                        "test_wr":  s4.get("wr", 0),
                        "code": r.get("code", ""),
                    })
        except Exception:
            pass
    return already_tried, near_miss


def generate_strategies(round_num: int, all_found: list, num: int = 5,
                        history: tuple[list, list] | None = None,
                        long_only: bool = False) -> list:
    already_tried, near_miss = history if history else ([], [])

    # 已知策略上下文（避免重复）
    prev_ctx = ""
    tried_this_run = [r["name"] for r in all_found]
    all_names = {r["name"] for r in already_tried} | set(tried_this_run)

    if already_tried or all_found:
        prev_ctx += "\n## 以下策略已经测试过（禁止重复，不要生成类似结构）\n"
        recent = list(already_tried)[-30:] + all_found[-10:]
        for r in recent:
            prev_ctx += f"- {r['name']}: {r.get('hypothesis', '')}\n"

    # 近乎通过的策略（让 Gemini 尝试变异，包含代码）
    if near_miss:
        prev_ctx += ("\n## 以下策略训练集良好但测试集略差（差<5%），请生成更稳健的变体\n"
                     "（可调整阈值、添加过滤条件、或换一种表达逻辑）\n")
        for r in near_miss[-3:]:
            prev_ctx += (f"\n### {r['name']} [{r['mode']}] "
                         f"train={r['train_wr']*100:.1f}% test={r['test_wr']*100:.1f}%\n"
                         f"假设: {r.get('hypothesis', '')}\n"
                         f"代码:\n```python\n{r.get('code', '')}\n```\n")

    if long_only:
        prompt = _GEMINI_PROMPT_LONG_TMPL.format(
            known_strategies=_KNOWN_STRATEGIES_LONG,
            prev_round_context=prev_ctx,
            num_strategies=num,
            round_num=round_num,
        )
    else:
        prompt = _GEMINI_PROMPT_TMPL.format(
            known_strategies=_KNOWN_STRATEGIES,
            prev_round_context=prev_ctx,
            num_strategies=num,
            round_num=round_num,
        )

    print(f"  [Gemini] Generating {num} strategy hypotheses (round {round_num})...")
    text = call_gemini(prompt)

    start = text.find("[")
    end   = text.rfind("]") + 1
    if start == -1 or end <= 0:
        raise ValueError(f"No JSON array in Gemini response:\n{text[:500]}")

    strategies = json.loads(text[start:end])
    print(f"  [Gemini] Received {len(strategies)} strategies.")
    return strategies


# ── 安全代码编译 ───────────────────────────────────────────────────────────────

_SAFE_BUILTINS = {
    "range": range, "len": len, "all": all, "any": any,
    "sum": sum, "min": min, "max": max, "abs": abs, "round": round,
    "list": list, "float": float, "int": int, "bool": bool,
    "enumerate": enumerate, "zip": zip,
}


def compile_strategy(code_str: str):
    ns = {
        "__builtins__": _SAFE_BUILTINS,
        "gradient":  gradient,
        "flux":      flux,
        "amplitude": amplitude,
    }
    exec(compile(code_str, "<strategy>", "exec"), ns)
    fn = ns.get("sig")
    if fn is None:
        raise ValueError("Code does not define a 'sig' function")
    return fn


# ── 主探索轮次 ─────────────────────────────────────────────────────────────────

def run_round(round_num: int, d1h: dict, d4h: dict,
              all_found: list, no_gemini: bool = False,
              history: tuple[list, list] | None = None,
              long_only: bool = False) -> list:
    sep = "=" * 80
    print(f"\n{sep}")
    print(f"  ROUND {round_num}  [{datetime.now().strftime('%Y-%m-%d %H:%M')}]")
    print(sep)

    # Stage 0: generate hypotheses
    if no_gemini:
        raw = _BUILTIN_STRATEGIES
    else:
        try:
            raw = generate_strategies(round_num, all_found, num=5,
                                      history=history, long_only=long_only)
        except Exception as e:
            print(f"  [ERROR] Gemini call failed: {e}")
            return []

    # Compile signal functions
    strategies = []
    print()
    for i, s in enumerate(raw):
        name = s.get("name", f"Strat-R{round_num}-{i+1}")
        mode = s.get("mode", "1h")
        hyp  = s.get("hypothesis", "")
        code = s.get("code", "")
        print(f"  [{i+1}/5] {name:30s} ({mode})")
        print(f"       {hyp}")
        try:
            fn = compile_strategy(code)
            strategies.append({"name": name, "mode": mode, "hypothesis": hyp,
                                "code": code, "fn": fn})
        except Exception as e:
            print(f"       COMPILE ERROR: {e}")

    if not strategies:
        print("  No compilable strategies. Skipping round.")
        return []

    train_pct = int(TRAIN_RATIO * 100)
    test_pct  = 100 - train_pct

    # LONG-only mode uses relaxed thresholds (signals are rarer in bearish dataset)
    s1_min_n  = STAGE1_MIN_N
    s1_min_wr = STAGE1_MIN_WR
    s2_min_n  = STAGE2_MIN_N
    s2_min_wr = 0.55 if long_only else STAGE2_MIN_WR
    s3_min_n  = STAGE3_MIN_N
    s3_min_wr = 0.57 if long_only else STAGE3_MIN_WR

    # ── Stage 1: Big4，训练集 ──────────────────────────────────────────────────
    print(f"\n  --- Stage 1 [train {train_pct}%]: Big4 ---  "
          f"(pass: n>={s1_min_n}, wr>={s1_min_wr*100:.0f}%)")
    stage1_pass = []
    for st in strategies:
        agg, _ = run_strategy(st, d1h, d4h, BIG4, phase="train")
        n  = agg["n"]
        wr = agg["win"] / n if n > 0 else 0.0
        ev = sum(agg["pnl"]) / n * 100 if n > 0 else 0.0
        ok = n >= s1_min_n and wr >= s1_min_wr
        flag = "PASS" if ok else "fail"
        print(f"  {flag:4s}  {st['name']:30s}  n={n:4d}  wr={wr*100:5.1f}%  ev={ev:+.2f}%")
        if ok:
            st["stage1"] = {"n": n, "wr": wr, "ev": ev}
            stage1_pass.append(st)

    if not stage1_pass:
        print("  No strategies passed Stage 1.")
        return []

    # ── Stage 2: 10 random alts，训练集 ───────────────────────────────────────
    candidates = [s for s in ALT99 if s not in set(BIG4)]
    sample10   = random.sample(candidates, min(10, len(candidates)))
    print(f"\n  --- Stage 2 [train {train_pct}%]: 10 alts ---  "
          f"(pass: n>={s2_min_n}, wr>={s2_min_wr*100:.0f}%)")
    print(f"  Sample: {', '.join(sample10)}")
    stage2_pass = []
    for st in stage1_pass:
        agg, _ = run_strategy(st, d1h, d4h, sample10, phase="train")
        n  = agg["n"]
        wr = agg["win"] / n if n > 0 else 0.0
        ev = sum(agg["pnl"]) / n * 100 if n > 0 else 0.0
        ok = n >= s2_min_n and wr >= s2_min_wr
        flag = "PASS" if ok else "fail"
        print(f"  {flag:4s}  {st['name']:30s}  n={n:4d}  wr={wr*100:5.1f}%  ev={ev:+.2f}%")
        if ok:
            st["stage2"] = {"n": n, "wr": wr, "ev": ev}
            stage2_pass.append(st)

    if not stage2_pass:
        print("  No strategies passed Stage 2.")
        return []

    # ── Stage 3: All alts，训练集 ──────────────────────────────────────────────
    print(f"\n  --- Stage 3 [train {train_pct}%]: All {len(ALT99)} alts ---  "
          f"(pass: n>={s3_min_n}, wr>={s3_min_wr*100:.0f}%)")
    stage3_pass = []
    for st in stage2_pass:
        agg, per_sym = run_strategy(st, d1h, d4h, ALT99, phase="train")
        n  = agg["n"]
        wr = agg["win"] / n if n > 0 else 0.0
        ev = sum(agg["pnl"]) / n * 100 if n > 0 else 0.0
        ok = n >= s3_min_n and wr >= s3_min_wr
        flag = "PASS" if ok else "border"
        print(f"  {flag:6s}  {st['name']:30s}  n={n:5d}  wr={wr*100:5.1f}%  ev={ev:+.2f}%")
        top_syms = sorted(
            [(sym, s["n"], s["win"] / s["n"] if s["n"] > 0 else 0.0)
             for sym, s in per_sym.items() if s["n"] >= 3],
            key=lambda x: -x[2]
        )
        if top_syms:
            top5 = [f"{sym}({wr2*100:.0f}%/{cnt})" for sym, cnt, wr2 in top_syms[:5]]
            print(f"         Top: {', '.join(top5)}")
        if ok:
            st["stage3"] = {"n": n, "wr": wr, "ev": ev}
            st["top_syms_train"] = top_syms
            stage3_pass.append(st)

    if not stage3_pass:
        print("  No strategies passed Stage 3.")
        return []

    # ── Stage 4: 走时测试集验证 ────────────────────────────────────────────────
    print(f"\n  --- Stage 4 [TEST {test_pct}%]: Walk-forward validation ---")
    discoveries = []
    for st in stage3_pass:
        agg_test, per_test = run_strategy(st, d1h, d4h, ALT99, phase="test")
        nt  = agg_test["n"]
        wrt = agg_test["win"] / nt if nt > 0 else 0.0
        evt = sum(agg_test["pnl"]) / nt * 100 if nt > 0 else 0.0

        s3 = st["stage3"]
        if nt < TEST_MIN_N:
            verdict = "LOW-N "
        elif wrt >= s3_min_wr:
            verdict = "PASS  "
        elif wrt >= s3_min_wr - 0.05:
            verdict = "border"
        else:
            verdict = "FAIL  "

        print(f"  {verdict}  {st['name']:30s}  "
              f"train: n={s3['n']:4d} wr={s3['wr']*100:5.1f}%  |  "
              f"TEST:  n={nt:4d} wr={wrt*100:5.1f}%  ev={evt:+.2f}%")

        # Per-symbol test breakdown
        top_test = sorted(
            [(sym, s["n"], s["win"] / s["n"] if s["n"] > 0 else 0.0)
             for sym, s in per_test.items() if s["n"] >= 3],
            key=lambda x: -x[2]
        )
        if top_test:
            top5t = [f"{sym}({wr2*100:.0f}%/{cnt})" for sym, cnt, wr2 in top_test[:5]]
            print(f"         Test top: {', '.join(top5t)}")

        discoveries.append({
            "round":      round_num,
            "name":       st["name"],
            "mode":       st["mode"],
            "hypothesis": st["hypothesis"],
            "code":       st["code"],
            "stage1":     st.get("stage1", {}),
            "stage2":     st.get("stage2", {}),
            "stage3":     st["stage3"],
            "stage4_test": {"n": nt, "wr": wrt, "ev": evt,
                            "verdict": verdict.strip()},
            "top_symbols_train": [(sym, round(wr2, 4), cnt)
                                  for sym, cnt, wr2 in st.get("top_syms_train", [])[:20]],
            "top_symbols_test":  [(sym, round(wr2, 4), cnt)
                                  for sym, cnt, wr2 in top_test[:20]],
            "ts": datetime.now().isoformat(),
        })

    return discoveries


# ── 内置测试策略（--no-gemini 跑流程验证）─────────────────────────────────────

_BUILTIN_STRATEGIES = [
    {
        "name": "BodyEfficiency-Decay",
        "mode": "1h",
        "hypothesis": "3连阳但K线实体效率递减（振幅增大但方向力减弱），散户接盘主力出货",
        "code": textwrap.dedent("""\
            def sig(cs1h, cs4h=None):
                if len(cs1h) < 6: return None
                if not all(cs1h[-i]["close"] > cs1h[-i]["open"] for i in range(1, 4)): return None
                def body(c):
                    rng = c["high"] - c["low"]
                    return (c["close"] - c["open"]) / rng if rng > 0 else 0.5
                b = [body(cs1h[-i]) for i in range(1, 4)]
                if b[2] > b[1] > b[0] and b[0] < 0.40:
                    return "SHORT"
                return None
        """),
    },
    {
        "name": "FluxAccelDown",
        "mode": "1h",
        "hypothesis": "买压快速衰减：短窗口flux比长窗口低超10ppt，且价格仍在上涨",
        "code": textwrap.dedent("""\
            def sig(cs1h, cs4h=None):
                if len(cs1h) < 8: return None
                f3 = flux(cs1h, 3)
                f6 = flux(cs1h, 6)
                g3 = gradient(cs1h, 3)
                if g3 > 0.002 and (f6 - f3) > 0.10 and f3 < 0.45:
                    return "SHORT"
                return None
        """),
    },
    {
        "name": "WastedEnergy",
        "mode": "1h",
        "hypothesis": "大振幅但净方向小（能量浪费），配合弱买压，多头力竭信号",
        "code": textwrap.dedent("""\
            def sig(cs1h, cs4h=None):
                if len(cs1h) < 8: return None
                g = gradient(cs1h, 6)
                a = amplitude(cs1h, 6)
                if a == 0: return None
                efficiency = abs(g) / a
                f = flux(cs1h, 3)
                g3 = gradient(cs1h, 3)
                if g3 > 0.001 and efficiency < 0.30 and f < 0.47:
                    return "SHORT"
                return None
        """),
    },
    {
        "name": "MacroMicroFluxDiv",
        "mode": "mtf_self",
        "hypothesis": "4h买压强但1h买压弱，跨周期flux背离，短期空头占优",
        "code": textwrap.dedent("""\
            def sig(cs1h, cs4h=None):
                if len(cs1h) < 6 or cs4h is None or len(cs4h) < 4: return None
                f1h = flux(cs1h, 3)
                f4h = flux(cs4h, 3)
                g4h = gradient(cs4h, 3)
                g1h = gradient(cs1h, 3)
                if f4h > 0.55 and f1h < 0.44 and g4h > 0.004 and g1h > 0.001:
                    return "SHORT"
                return None
        """),
    },
    {
        "name": "UpperWickExpansion",
        "mode": "1h",
        "hypothesis": "连续3根K线上影线占比递增（抛压越来越大）+ 买压已在0.5以下",
        "code": textwrap.dedent("""\
            def sig(cs1h, cs4h=None):
                if len(cs1h) < 6: return None
                def upper_wick(c):
                    rng = c["high"] - c["low"]
                    if rng == 0: return 0.0
                    return (c["high"] - max(c["open"], c["close"])) / rng
                uw = [upper_wick(cs1h[-i]) for i in range(1, 4)]
                f  = flux(cs1h, 3)
                g3 = gradient(cs1h, 3)
                if uw[2] < uw[1] < uw[0] and uw[0] > 0.25 and f < 0.50 and g3 > 0:
                    return "SHORT"
                return None
        """),
    },
]


# ── 入口 ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="AI Strategy Explorer")
    parser.add_argument("--rounds",     type=int, default=1)
    parser.add_argument("--no-gemini",  action="store_true",
                        help="Use built-in test strategies instead of Gemini")
    parser.add_argument("--long-only",  action="store_true",
                        help="Force Gemini to generate LONG-only strategies")
    args = parser.parse_args()

    sep = "=" * 80
    print(f"\n{sep}")
    print(f"  STRATEGY EXPLORER  [{datetime.now().strftime('%Y-%m-%d %H:%M')}]")
    mode_label = ('built-in' if args.no_gemini
                  else ('Gemini LONG-only' if args.long_only
                        else 'Gemini ' + GEMINI_MODEL))
    print(f"  Mode: {mode_label}")
    print(f"  Rounds: {args.rounds} x 5 strategies")
    s2_wr = 0.55 if args.long_only else STAGE2_MIN_WR
    s3_wr = 0.57 if args.long_only else STAGE3_MIN_WR
    print(f"  Stage1(Big4): n>={STAGE1_MIN_N}, wr>={STAGE1_MIN_WR*100:.0f}%  |  "
          f"Stage2(10): n>={STAGE2_MIN_N}, wr>={s2_wr*100:.0f}%  |  "
          f"Stage3(all): n>={STAGE3_MIN_N}, wr>={s3_wr*100:.0f}%")
    print(sep)

    # Load all data once
    print("\n  Loading DB data...")
    all_syms = list(set(["BTC/USDT"] + BIG4 + ALT99))
    d1h = {}; d4h = {}
    for s in all_syms:
        d1h[s] = load(s, "1h")
        d4h[s] = load(s, "4h")
    ok = sum(1 for v in d1h.values() if len(v) >= 100)
    print(f"  Loaded {len(d1h)} symbols. {ok} with >= 100 1h candles.")

    all_found = []
    history = load_history()
    already_tried, near_miss = history
    print(f"  History: {len(already_tried)} strategies tested, {len(near_miss)} near-misses")

    for rnd in range(1, args.rounds + 1):
        discoveries = run_round(rnd, d1h, d4h, all_found,
                                no_gemini=args.no_gemini, history=history,
                                long_only=args.long_only)
        all_found.extend(discoveries)

    # Save results
    if all_found:
        ts  = datetime.now().strftime("%Y%m%d_%H%M")
        out = RESULTS_DIR / f"explorer_{ts}.json"
        save_list = [{k: v for k, v in d.items() if k != "fn"}
                     for d in all_found]
        with open(out, "w", encoding="utf-8") as f:
            json.dump(save_list, f, ensure_ascii=False, indent=2)
        print(f"\n  Results saved: {out}")

    # Final summary
    print(f"\n{sep}")
    print(f"  EXPLORATION COMPLETE:  {len(all_found)} strategies reached Stage 4 (walk-forward)")
    full_pass = [d for d in all_found
                 if d.get("stage4_test", {}).get("verdict", "").startswith("PASS")]
    print(f"  Full validated (train>=60% AND test>=60%): {len(full_pass)}")
    print(sep)
    print(f"  {'Name':32s} {'Mode':10s}  "
          f"{'Train n':>8} {'Train WR':>9}  {'Test n':>7} {'Test WR':>8}  Verdict")
    print(f"  {'-'*80}")
    for d in all_found:
        s3 = d.get("stage3", {})
        s4 = d.get("stage4_test", {})
        v  = s4.get("verdict", "?")
        print(f"  {d['name']:32s} [{d['mode']:10s}]  "
              f"n={s3.get('n',0):5d}  {s3.get('wr',0)*100:5.1f}%  |  "
              f"n={s4.get('n',0):4d}  {s4.get('wr',0)*100:5.1f}%  {v}")
        if v.startswith("PASS"):
            print(f"    -> {d['hypothesis']}")
    if not all_found:
        print("  No strategies reached Stage 4 this run.")
    print(sep)


if __name__ == "__main__":
    main()
