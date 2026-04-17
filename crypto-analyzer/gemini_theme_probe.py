#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gemini_theme_probe.py
=====================
主题驱动的 "Gemini × 原语" 策略发现脚本。

工作流（一次运行 = 一个主题 = 一次部署）
  1. 读取已部署策略名（strategy_params 表）作为 Gemini 上下文 + 去重
  2. 组装 prompt：原语目录 + 已知策略 + 用户给定的主题/假设
  3. 调 Gemini 拿 N 个候选信号（JSON 数组，每条含 name/direction/hypothesis/code）
  4. 用 `primitives_gemini.exec_namespace` 编译候选为 Python 函数
  5. 调用 `auto_explore_alien5.validate_4stage` 跑 S1-S4 漏斗
  6. S4 PASS 的策略：
       - 写 `gemini_signals/<theme_slug>.py`（含 STRATEGIES 列表 + 源码）
       - 写 DB `strategy_params`，source='gemini_theme_probe'，SL=2%/TP=3%/hold=3h
       - 追加一行到 `gemini_theme_log.md`
  7. `dimension_trader.py` 下次热加载时通过 `_load_gemini_registry` 自动挂载，
     无需手动改代码

用法
  # 新主题，完整跑
  .venv/Scripts/python.exe gemini_theme_probe.py --theme "卖压衰竭买压接力" \
      --desc "sell_saturation 从高位快速回落 + order_flow_delta 翻正 + 4h 不强空"

  # 只想看 Gemini 怎么写，不跑回测 / 不部署
  .venv/Scripts/python.exe gemini_theme_probe.py --theme "xxx" --desc "..." --dry-run

  # 只看 Gemini 返回的 JSON，不编译不回测
  .venv/Scripts/python.exe gemini_theme_probe.py --theme "xxx" --desc "..." --preview-only

  # 指定候选数（默认 6）
  .venv/Scripts/python.exe gemini_theme_probe.py --theme "xxx" --desc "..." --n 8
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import textwrap
import time
from datetime import datetime
from pathlib import Path

import pymysql
import requests
from dotenv import load_dotenv

# 复用 auto_explore_alien5 的数据加载 / 回测漏斗 / deploy 基础设施
import auto_explore_alien5 as _alien5
from explored_filter import load_deployed_names
from primitives_gemini import PRIMITIVE_CATALOG, exec_namespace

load_dotenv()

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


# ── 常量 / 环境 ────────────────────────────────────────────────────────────────

ROOT          = Path(__file__).parent
SIGNALS_DIR   = ROOT / "gemini_signals"
THEME_LOG     = ROOT / "gemini_theme_log.md"
RESULTS_DIR   = ROOT / "gemini_results"
SIGNALS_DIR.mkdir(exist_ok=True)
RESULTS_DIR.mkdir(exist_ok=True)

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
GEMINI_MODEL   = os.getenv("GEMINI_MODEL",   "gemini-2.5-flash").strip()

_DB_CFG = dict(
    host=os.getenv("DB_HOST"),
    port=int(os.getenv("DB_PORT", "3306")),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    database=os.getenv("DB_NAME"),
    charset="utf8mb4",
)


# ── Windows 代理自动检测（复用 strategy_explorer.py 的做法）───────────────────

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
                parts = line.strip().split()
                addr = parts[-1]
                if addr and ":" in addr:
                    return {"http": f"http://{addr}", "https": f"http://{addr}"}
    except Exception:
        return None
    return None


_PROXIES = _detect_system_proxy()
if _PROXIES:
    print(f"  [proxy] detected system proxy -> {_PROXIES['https']}")


# ── Gemini 调用 ────────────────────────────────────────────────────────────────

def call_gemini(prompt: str, temperature: float = 0.9, max_tokens: int = 20000) -> str:
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
        timeout=180,
        proxies=_PROXIES,
    )
    resp.raise_for_status()
    data = resp.json()
    parts = data["candidates"][0]["content"]["parts"]
    text = next((p["text"] for p in reversed(parts) if "text" in p), "")
    return text


# ── Prompt 构造 ────────────────────────────────────────────────────────────────

_PROMPT_TMPL = """\
你是一位顶尖量化研究员，擅长从市场微结构和原语组合中发掘**人类尚未充分利用**
的 alpha 信号。你会用给定的原语函数库为一个具体主题假设产出 {n} 个**互不相同**
的信号函数候选（支持 LONG 与 SHORT 混合，取决于主题更适合哪边）。

## 本轮主题
**{theme}**

### 主题描述
{desc}
{feedback_block}
{catalog}

## 已部署策略（名字和它们的核心逻辑），**不要重名也不要照抄**
{existing_brief}

## 回测环境
- 时间框架：1h 入场判断，辅以 4h 宏观对齐
- 回测漏斗：S1(Big4)→S2(10 alts)→S3(86 alts 训练)→S4(86 alts 测试)
- PASS 门槛：S3 训练集胜率 ≥ 60%，S4 测试集胜率 ≥ 60%
- 固定 SL=2%/TP=3%/持仓=3 根 1h K 线

## 设计要求（强制）
1. 层次化：每个函数应至少有三层条件（宏观 4h 方向 + 中期形态 + 近期触发）
2. 不同候选应覆盖**不同参数组合或不同触发子条件**，避免同质化
3. 每层条件必须有明确市场假设，不得为调高胜率而"凑 AND"
4. 信号过于稀疏会通不过样本门槛：S3 需 n≥30，S4 需 n≥10
5. 函数名必须严格为 `sig(cs1h, cs4h)`，**禁止 import**，只用原语目录和安全构造

## 输出格式（严格 JSON 数组，前后不要有任何其他文字/markdown/注释）
[
  {{
    "name": "{theme_prefix}_L_v1",          // 必须唯一，格式 <主题Prefix>_<L|S>_<版本>
    "direction": "LONG",                    // "LONG" 或 "SHORT"
    "hypothesis": "1-2句市场逻辑",
    "code": "def sig(cs1h, cs4h):\\n    if len(cs1h) < 20 or len(cs4h) < 6: return None\\n    ...\\n    return 'LONG'"
  }},
  ...{n_minus_1} more ...
]
"""


def _slugify(s: str) -> str:
    s = re.sub(r"[^A-Za-z0-9_]+", "_", s.strip())
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "theme"


def _load_existing_brief(limit: int = 60) -> str:
    """按 source 分组列出 DB 里已部署的策略名（给 Gemini 看以免重名/撞主题）。"""
    try:
        conn = pymysql.connect(**_DB_CFG)
        with conn.cursor() as c:
            c.execute(
                "SELECT COALESCE(source,'(null)') src, strategy_name "
                "FROM strategy_params ORDER BY src, strategy_name"
            )
            rows = c.fetchall()
        conn.close()
    except Exception as e:
        return f"(DB read failed: {e})"
    bucket: dict[str, list[str]] = {}
    for src, nm in rows:
        bucket.setdefault(src, []).append(nm)
    lines = []
    for src, names in bucket.items():
        shown = names[:limit]
        more = f" (+{len(names) - len(shown)} more)" if len(names) > len(shown) else ""
        lines.append(f"- [{src} x {len(names)}] {', '.join(shown)}{more}")
    return "\n".join(lines) if lines else "(空)"


def _load_prev_run_feedback(theme_slug: str, max_candidates: int = 10) -> str:
    """读取 gemini_results 里最近一次同 slug 的 run，把每个候选的代码 + S1~S4
    实证指标组织成给 Gemini 的反馈段落，促使其产出"基于实证"的改进版。"""
    files = sorted(RESULTS_DIR.glob(f"{theme_slug}_*.json"))
    if not files:
        return ""
    try:
        data = json.loads(files[-1].read_text(encoding="utf-8"))
    except Exception:
        return ""
    cands = data.get("candidates", [])
    if not cands:
        return ""

    # 按 passed_stage 降序 + test_wr / s3_wr 降序，优先展示 "差一点就过" 的
    def _rank(c):
        s = c.get("stage_stats") or {}
        ps = s.get("passed_stage", 0)
        wr = (s.get("s4") or s.get("s3") or s.get("s2") or s.get("s1") or {}).get("wr", 0)
        n  = (s.get("s4") or s.get("s3") or s.get("s2") or s.get("s1") or {}).get("n", 0)
        return (-ps, -wr, -n)
    cands_sorted = sorted(cands, key=_rank)[:max_candidates]

    lines = [
        "\n## 上一轮实证反馈 (critical — 必须读懂并基于此做改进)",
        f"上一次同主题 run ({files[-1].name}) 给出了 {len(cands)} 个候选但全部未通过"
        " S4。请根据下方每个候选的 **实际回测指标** 诊断并改进。",
        "",
        "- **passed_stage**: 策略实际走完的阶段（0=连 S1 都没过）",
        "- **S1 Big4 n/wr**: Big4 训练集（4标的约5000根1h，门槛 n>=5, wr>=57%）",
        "- **S2 10alts n/wr**: 10 随机山寨训练集（门槛 n>=15, wr>=55%）",
        "- **S3 全山寨 n/wr**: 86alts 训练集（门槛 n>=30, wr>=57%）",
        "- **S4 测试集 n/wr**: 86alts 走时 30% 测试集（门槛 n>=10, wr>=57%）",
        "",
        "### 改进方向（请至少遵循 3 条）",
        "1. 对 **n 过少**（<30）的策略：放宽某些条件（去掉最稀有的 AND 分支）",
        "2. 对 **wr 接近但不到门槛**（差 1-3%）的策略：增加一个能过滤'弱信号'的附加条件",
        "3. 对 **n 很大但 wr 低**（n>100, wr 45-52%）的策略：信号太噪，增加时间/方向/位置过滤",
        "4. 换一个原语作为主触发（比如从 flux 换到 _order_flow_delta），避免同质",
        "5. 尝试反方向（若 LONG 全败，考虑改用 SHORT 版本的同结构）",
        "",
    ]
    for c in cands_sorted:
        s = c.get("stage_stats") or {}
        s1 = s.get("s1") or {}
        s2 = s.get("s2") or {}
        s3 = s.get("s3") or {}
        s4 = s.get("s4") or {}
        lines.append(f"#### {c['name']}  [{c['direction']}]  passed_stage={s.get('passed_stage', 0)}")
        lines.append(f"  hypothesis: {c.get('hypothesis','')}")
        lines.append(f"  S1 n={s1.get('n', '-'):>4}  wr={s1.get('wr', 0)*100:5.1f}%  ev={s1.get('ev', 0):+.2f}%"
                     + (f"  |  S2 n={s2.get('n','-'):>4} wr={s2.get('wr',0)*100:5.1f}%" if s2 else "")
                     + (f"  |  S3 n={s3.get('n','-'):>5} wr={s3.get('wr',0)*100:5.1f}%" if s3 else "")
                     + (f"  |  S4 n={s4.get('n','-'):>4} wr={s4.get('wr',0)*100:5.1f}%" if s4 else ""))
        # 代码：截断到 900 chars 保持 prompt 不过长
        code = (c.get("code") or "")[:900]
        lines.append("  code:")
        for ln in code.splitlines()[:30]:
            lines.append(f"    {ln}")
        lines.append("")
    return "\n".join(lines)


def build_prompt(theme: str, desc: str, n: int, theme_prefix: str,
                 theme_slug: str) -> str:
    return _PROMPT_TMPL.format(
        n=n,
        theme=theme,
        desc=desc.strip(),
        catalog=PRIMITIVE_CATALOG,
        existing_brief=_load_existing_brief(),
        theme_prefix=theme_prefix,
        n_minus_1=max(n - 1, 0),
        feedback_block=_load_prev_run_feedback(theme_slug),
    )


# ── 解析 + 编译 ────────────────────────────────────────────────────────────────

def extract_json_array(text: str) -> list:
    """从 Gemini 回复里提取 JSON 数组（容忍 ```json 代码块 / 多余前后缀）。"""
    t = text.strip()
    if "```" in t:
        m = re.search(r"```(?:json)?\s*(\[.*?\])\s*```", t, re.S)
        if m:
            t = m.group(1)
    start = t.find("[")
    end   = t.rfind("]") + 1
    if start == -1 or end <= 0:
        raise ValueError(f"No JSON array in Gemini response:\n{text[:400]}")
    return json.loads(t[start:end])


def compile_candidate(name: str, code: str):
    """Exec 编译 Gemini 生成的代码，返回 sig(cs1h, cs4h) 函数。"""
    ns = exec_namespace()
    exec(compile(code, f"<gemini:{name}>", "exec"), ns)
    fn = ns.get("sig")
    if fn is None:
        raise ValueError(f"{name}: code does not define sig(cs1h, cs4h)")
    return fn


# ── 四阶段漏斗（带每策略指标追踪）─────────────────────────────────────────────

def _run_with_tracking(strategies: list[dict], d1h: dict, d4h: dict) -> dict:
    """复刻 alien5.validate_4stage 但保存每策略在 S1-S4 的 n/wr/ev，便于 JSON 存档。

    返回 dict: {strategy_name: {"direction", "hypothesis", "code",
                                "s1"..."s4" 各自 {n,wr,ev} 或 None,
                                "passed_stage": 0-4, "passed": bool,
                                "test_wr", "test_n", "test_ev", "s3_wr", "s3_n"}}
    """
    import random
    stats: dict[str, dict] = {}
    for st in strategies:
        stats[st["name"]] = {
            "name":       st["name"],
            "direction":  st["direction"],
            "hypothesis": st.get("hypothesis", ""),
            "code":       st.get("code", ""),
            "s1": None, "s2": None, "s3": None, "s4": None,
            "passed_stage": 0,
            "passed": False,
        }

    def _metric(agg):
        n = agg["n"]
        wr = agg["win"] / n if n > 0 else 0.0
        ev = sum(agg["pnl"]) / n * 100 if n > 0 else 0.0
        return {"n": n, "wr": round(wr, 4), "ev": round(ev, 4)}

    # S1: Big4 train
    print(f"\n  --- S1 [Big4 train] ---")
    s1_pass = []
    for st in strategies:
        agg, _ = _alien5.run_strat(st["fn"], st["mode"], d1h, d4h,
                                   _alien5.BIG4, "train")
        m = _metric(agg)
        ok = m["n"] >= _alien5.STAGE1_MIN_N and m["wr"] >= _alien5.STAGE1_MIN_WR
        tag = "PASS" if ok else "----"
        print(f"  {tag}  {st['name']:42s}  n={m['n']:4d}  wr={m['wr']*100:5.1f}%  ev={m['ev']:+.2f}%")
        stats[st["name"]]["s1"] = m
        if ok:
            stats[st["name"]]["passed_stage"] = 1
            s1_pass.append(st)
    print(f"  S1: {len(s1_pass)}/{len(strategies)} passed")
    if not s1_pass:
        return stats

    # S2: 10 random alts train
    pool = [s for s in _alien5.ALT99 if s not in set(_alien5.BIG4)]
    sample10 = random.sample(pool, min(10, len(pool)))
    print(f"\n  --- S2 [10 alts train] ---  sample={sample10}")
    s2_pass = []
    for st in s1_pass:
        agg, _ = _alien5.run_strat(st["fn"], st["mode"], d1h, d4h,
                                   sample10, "train")
        m = _metric(agg)
        ok = m["n"] >= _alien5.STAGE2_MIN_N and m["wr"] >= _alien5.STAGE2_MIN_WR
        tag = "PASS" if ok else "----"
        print(f"  {tag}  {st['name']:42s}  n={m['n']:4d}  wr={m['wr']*100:5.1f}%  ev={m['ev']:+.2f}%")
        stats[st["name"]]["s2"] = m
        if ok:
            stats[st["name"]]["passed_stage"] = 2
            s2_pass.append(st)
    print(f"  S2: {len(s2_pass)}/{len(s1_pass)} passed")
    if not s2_pass:
        return stats

    # S3: All alts train
    print(f"\n  --- S3 [All {len(_alien5.ALT99)} alts train] ---")
    s3_pass = []
    for st in s2_pass:
        agg, _ = _alien5.run_strat(st["fn"], st["mode"], d1h, d4h,
                                   _alien5.ALT99, "train")
        m = _metric(agg)
        ok = m["n"] >= _alien5.STAGE3_MIN_N and m["wr"] >= _alien5.STAGE3_MIN_WR
        tag = "PASS" if ok else "----"
        print(f"  {tag}  {st['name']:42s}  n={m['n']:5d}  wr={m['wr']*100:5.1f}%  ev={m['ev']:+.2f}%")
        stats[st["name"]]["s3"] = m
        if ok:
            stats[st["name"]]["passed_stage"] = 3
            s3_pass.append(st)
    print(f"  S3: {len(s3_pass)}/{len(s2_pass)} passed")
    if not s3_pass:
        return stats

    # S4: All alts test (walk-forward 30%)
    print(f"\n  --- S4 [test 30% walk-forward] ---")
    for st in s3_pass:
        agg, _ = _alien5.run_strat(st["fn"], st["mode"], d1h, d4h,
                                   _alien5.ALT99, "test")
        m = _metric(agg)
        s3m = stats[st["name"]]["s3"]
        if   m["n"] < _alien5.TEST_MIN_N:             verdict = "LOW-N "
        elif m["wr"] >= _alien5.STAGE3_MIN_WR:        verdict = "PASS  "
        elif m["wr"] >= _alien5.STAGE3_MIN_WR - 0.05: verdict = "border"
        else:                                         verdict = "FAIL  "
        print(f"  {verdict}  {st['name']:42s}  "
              f"train={s3m['wr']*100:5.1f}%  |  "
              f"TEST n={m['n']:4d} wr={m['wr']*100:5.1f}%  ev={m['ev']:+.2f}%")
        stats[st["name"]]["s4"] = m
        if verdict.strip() == "PASS":
            stats[st["name"]]["passed_stage"] = 4
            stats[st["name"]]["passed"] = True

    # 回填方便 downstream 使用的扁平键
    for s in stats.values():
        if s.get("s3"):
            s["s3_wr"] = s["s3"]["wr"]; s["s3_n"] = s["s3"]["n"]
        if s.get("s4"):
            s["test_wr"] = s["s4"]["wr"]; s["test_n"] = s["s4"]["n"]; s["test_ev"] = s["s4"]["ev"]
    return stats


# ── 持久化：写 gemini_signals/*.py + DB ───────────────────────────────────────

def write_signals_module(theme: str, theme_slug: str, passed: list[dict]) -> Path:
    """把通过四阶段的候选的源码写入 gemini_signals/<slug>.py，
    模块顶部提供 STRATEGIES 列表（fn/direction/name/hypothesis），
    供 dimension_trader._load_gemini_registry 挂载。"""
    path = SIGNALS_DIR / f"{theme_slug}.py"

    lines = [
        "# -*- coding: utf-8 -*-",
        '"""',
        f"gemini_signals/{theme_slug}.py",
        f"=================={'=' * len(theme_slug)}",
        f"主题: {theme}",
        f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"由 gemini_theme_probe.py 自动生成，请不要手改本文件。",
        '"""',
        "from __future__ import annotations",
        "",
        "from primitives_gemini import exec_namespace",
        "",
        "",
        f'THEME_NAME   = {theme!r}',
        f'GENERATED_AT = {datetime.now().isoformat(timespec="seconds")!r}',
        "",
        "# 每个策略独立的 exec 命名空间，避免互相覆盖。",
        "",
    ]

    strat_entries = []
    for i, p in enumerate(passed):
        nm = p["name"]
        dr = p["direction"]
        hy = p.get("hypothesis", "")
        code = p["code"]
        var = f"_SIG_{i}"
        lines.append(f"# ── [{i}] {nm}  ({dr})  test_wr={p['test_wr']*100:.1f}% ─")
        lines.append(f"# hypothesis: {hy}")
        lines.append(f"_CODE_{i} = {code!r}")
        lines.append(f"_NS_{i} = exec_namespace()")
        lines.append(f"exec(compile(_CODE_{i}, '<gemini:{nm}>', 'exec'), _NS_{i})")
        lines.append(f"{var} = _NS_{i}['sig']")
        lines.append("")
        strat_entries.append(
            f'    {{"name": {nm!r}, "direction": {dr!r}, '
            f'"hypothesis": {hy!r}, "fn": {var}}},'
        )

    lines.append("STRATEGIES: list[dict] = [")
    lines.extend(strat_entries)
    lines.append("]")
    lines.append("")

    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def deploy_to_db(theme_slug: str, passed: list[dict]) -> None:
    """把 PASS 的策略登记到 strategy_params，source=f'gemini_{theme_slug}'，
    统一 SL=2%/TP=3%/hold=3h。"""
    if not passed:
        return
    source = f"gemini_{theme_slug}"[:60]
    conn = pymysql.connect(**_DB_CFG)
    try:
        with conn.cursor() as c:
            for p in passed:
                notes = (f"theme={theme_slug} | hyp={p.get('hypothesis','')}"
                         f" | s3_wr={p['s3_wr']*100:.1f}% s3_n={p['s3_n']}"
                         f" | test_wr={p['test_wr']*100:.1f}% test_n={p['test_n']}")[:500]
                c.execute("""
                    INSERT INTO strategy_params
                        (strategy_name, sl_pct, tp_pct, hold_h, signal_count,
                         backtest_wr, source, notes, created_at, updated_at)
                    VALUES (%s, 0.0200, 0.0300, 3, %s, %s, %s, %s, NOW(), NOW())
                    ON DUPLICATE KEY UPDATE
                        sl_pct=VALUES(sl_pct), tp_pct=VALUES(tp_pct),
                        hold_h=VALUES(hold_h), signal_count=VALUES(signal_count),
                        backtest_wr=VALUES(backtest_wr), source=VALUES(source),
                        notes=VALUES(notes), updated_at=NOW()
                """, (p["name"], int(p["test_n"]), float(p["test_wr"]), source, notes))
        conn.commit()
    finally:
        conn.close()


def append_theme_log(theme: str, theme_slug: str, summary: dict,
                     passed: list[dict], dry_run: bool) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    passed_names = [p["name"] for p in passed]
    tag = " (DRY-RUN)" if dry_run else ""
    if not THEME_LOG.exists():
        THEME_LOG.write_text("# Gemini Theme Probe Log\n\n", encoding="utf-8")
    with THEME_LOG.open("a", encoding="utf-8") as f:
        f.write(f"## {ts}  —  {theme}{tag}\n\n")
        f.write(f"- slug: `{theme_slug}`\n")
        f.write(f"- candidates: {summary['total']}  "
                f"S1: {summary['s1']}  S2: {summary['s2']}  "
                f"S3: {summary['s3']}  S4: {summary['passed']}\n")
        if passed_names:
            f.write(f"- deployed: {', '.join(passed_names)}\n")
        else:
            f.write("- deployed: (none)\n")
        for p in passed:
            f.write(
                f"  - `{p['name']}` ({p['direction']})  "
                f"train={p['s3_wr']*100:.1f}%  test={p['test_wr']*100:.1f}%  "
                f"test_n={p['test_n']}  ev={p['test_ev']:+.2f}%\n"
            )
        f.write("\n")


# ── 主流程 ────────────────────────────────────────────────────────────────────

def run_theme(theme: str, desc: str, n: int, dry_run: bool,
              preview_only: bool, theme_prefix: str | None = None,
              force: bool = False) -> None:
    slug = _slugify(theme.lower())[:40] or "theme"
    theme_prefix = theme_prefix or _slugify(theme.split()[0] if theme else "T")[:12]

    print(f"\n{'='*80}")
    print(f"  gemini_theme_probe  theme='{theme}'  slug='{slug}'  prefix='{theme_prefix}'")
    print(f"  n_candidates={n}  dry_run={dry_run}  preview_only={preview_only}  force={force}")
    print(f"  model={GEMINI_MODEL}")
    print(f"{'='*80}")

    # 1) 调 Gemini
    prompt = build_prompt(theme, desc, n, theme_prefix, slug)
    print(f"\n  [prompt] length={len(prompt)} chars")
    t0 = time.time()
    text = call_gemini(prompt)
    print(f"  [gemini] response {len(text)} chars in {time.time()-t0:.1f}s")

    if preview_only:
        print("\n  ── Gemini 原始返回 (preview-only) ──")
        print(text)
        return

    try:
        raw = extract_json_array(text)
    except Exception as e:
        print(f"  [ERROR] JSON parse failed: {e}")
        print("  Raw text head:")
        print(text[:1000])
        return
    print(f"  [parse] got {len(raw)} candidates")

    # 2) 去重 + 编译
    deployed = set() if force else load_deployed_names()
    strategies: list[dict] = []
    for i, s in enumerate(raw):
        nm = s.get("name") or f"{theme_prefix}_{i}"
        dr = (s.get("direction") or "").upper()
        if dr not in ("LONG", "SHORT"):
            print(f"  SKIP {nm}: bad direction={dr!r}")
            continue
        if nm in deployed:
            print(f"  SKIP {nm}: already in strategy_params (use --force to override)")
            continue
        code = s.get("code", "")
        if "def sig" not in code:
            print(f"  SKIP {nm}: no sig() in code")
            continue
        try:
            fn = compile_candidate(nm, code)
        except Exception as e:
            print(f"  SKIP {nm}: compile error: {e}")
            continue
        strategies.append({
            "name": nm,
            "direction": dr,
            "hypothesis": s.get("hypothesis", ""),
            "code": code,
            "fn": fn,
            "mode": "mtf_self",
            "theme": slug,
            "doc": s.get("hypothesis", ""),
        })
        print(f"  OK   {nm:40s}  ({dr})  {s.get('hypothesis','')[:50]}")

    if not strategies:
        print("  No compilable candidates. Abort.")
        return

    # 3) 跑四阶段
    print(f"\n  [load_data] loading candles for Big4 + ALT99 ...")
    symbols = list(set(_alien5.BIG4) | set(_alien5.ALT99))
    d1h, d4h = _alien5.load_data(symbols)
    if not d1h:
        print("  [ERROR] No candles loaded.")
        return

    t0 = time.time()
    per_stage_stats = _run_with_tracking(strategies, d1h, d4h)
    passed = [s for s in per_stage_stats.values() if s.get("passed")]
    print(f"\n  [validate_4stage] {len(passed)}/{len(strategies)} PASS  "
          f"(elapsed {time.time()-t0:.1f}s)")

    # 保存本轮全部候选 + 阶段指标到 gemini_results/
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_file = RESULTS_DIR / f"{slug}_{run_ts}.json"
    def _clean(d):
        """去掉 fn 这种不可序列化的值，只保留纯 JSON。"""
        if not isinstance(d, dict): return d
        return {k: v for k, v in d.items() if k != "fn"}
    run_file.write_text(json.dumps({
        "theme": theme,
        "slug":  slug,
        "desc":  desc,
        "model": GEMINI_MODEL,
        "n_requested": n,
        "timestamp":   datetime.now().isoformat(timespec="seconds"),
        "candidates":  [
            {
                "name": s["name"],
                "direction": s["direction"],
                "hypothesis": s["hypothesis"],
                "code": s["code"],
                "stage_stats": _clean(per_stage_stats.get(s["name"], {})),
            }
            for s in strategies
        ],
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"  [save ] run details -> {run_file.name}")

    # 4) 统计与展示
    n_s1 = sum(1 for v in per_stage_stats.values() if v.get("passed_stage", 0) >= 1)
    n_s2 = sum(1 for v in per_stage_stats.values() if v.get("passed_stage", 0) >= 2)
    n_s3 = sum(1 for v in per_stage_stats.values() if v.get("passed_stage", 0) >= 3)
    summary = {"total": len(strategies), "s1": n_s1, "s2": n_s2,
               "s3": n_s3, "passed": len(passed)}

    if not passed:
        print("\n  [RESULT] 0 strategies passed S4. Theme done (nothing deployed).")
        append_theme_log(theme, slug, summary, [], dry_run)
        return

    print("\n  ── PASS summary ──")
    for p in sorted(passed, key=lambda x: -x["test_wr"]):
        print(f"    [{p['direction']}] {p['name']:42s}  "
              f"train={p['s3_wr']*100:.1f}%  test={p['test_wr']*100:.1f}%  "
              f"test_n={p['test_n']}  ev={p['test_ev']:+.2f}%")

    # 5) 部署
    if dry_run:
        print("\n  [dry-run] skip writing files / DB. (use without --dry-run to deploy)")
        append_theme_log(theme, slug, summary, passed, dry_run=True)
        return

    # 保险：先把 direction/code 回填到 passed
    mod_path = write_signals_module(theme, slug, passed)
    print(f"\n  [write] signals module -> {mod_path}")

    deploy_to_db(slug, passed)
    print(f"  [DB   ] inserted/updated {len(passed)} rows with source='gemini_{slug}'")

    append_theme_log(theme, slug, summary, passed, dry_run=False)
    print(f"  [log  ] appended to {THEME_LOG.name}")
    print(
        "\n  DONE. dimension_trader 下次热加载 (每小时) 会自动挂载；"
        "也可立即重启使其生效。"
    )


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description="Gemini-driven theme probe for alpha signals",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""
          示例:
            python gemini_theme_probe.py --theme "SellExhaust-BuyTakeover" \\
              --desc "sell_saturation 从高位快速回落 + order_flow_delta 由负翻正 ..."

            python gemini_theme_probe.py --theme "MultiTF-FluxResonance" \\
              --desc "1h flux 加速且 4h flux 同步加速 -> 真买压" --n 8
        """),
    )
    ap.add_argument("--theme", required=True, help="主题名（中英文都可）")
    ap.add_argument("--desc",  required=True, help="主题假设 / 核心逻辑描述")
    ap.add_argument("--n",     type=int, default=6, help="候选数 (default 6)")
    ap.add_argument("--prefix", default=None, help="策略名前缀，默认取主题第一个词")
    ap.add_argument("--dry-run",      action="store_true", help="跑完回测但不写文件/DB")
    ap.add_argument("--preview-only", action="store_true", help="只打印 Gemini 原始返回")
    ap.add_argument("--force",        action="store_true", help="忽略已部署策略名的去重")
    args = ap.parse_args()

    run_theme(
        theme=args.theme,
        desc=args.desc,
        n=args.n,
        dry_run=args.dry_run,
        preview_only=args.preview_only,
        theme_prefix=args.prefix,
        force=args.force,
    )


if __name__ == "__main__":
    main()
