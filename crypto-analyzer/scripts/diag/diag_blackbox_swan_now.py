#!/usr/bin/env python3
"""
黑盒红黑天鹅探索 (本地脚本, 不落 DB).

与 diag_gemini_swan_now.py 的区别:
  现有 swan worker 是"白盒规则采样" - universe 由 24h 涨跌 + 资金费率极值规则筛出,
  Gemini 只在我们喂的 ~40 个里贴标签. 本质是分类器, 抓不到非价格驱动的天鹅.

  本脚本是"黑盒两阶段探索":
    阶段一: 完全不喂数据, 让 Gemini 凭叙事/赛道/事件认知, 自己列 top N 个怀疑名单
    阶段二: 直连 Binance fapi 拉这些 symbol 的实时数据 (24h ticker + lastFundingRate),
            让 Gemini 用数据收敛 (强化 / 反转 / 改 skip)

  数据源说明:
    脱离远程 dimesion (该库的 price_stats_24h / funding_rate_data 当前停更),
    每次脚本运行直接调 Binance fapi 公开接口拉一次, 内存走流程. 数据实时.
    需要本地能访问 fapi.binance.com (大陆可能要 VPN).

用法:
  cd crypto-analyzer
  python scripts/diag/diag_blackbox_swan_now.py                   # 跑完整两阶段
  python scripts/diag/diag_blackbox_swan_now.py --rounds-p1 1     # 阶段一只 1 轮
  python scripts/diag/diag_blackbox_swan_now.py --rounds-p2 1     # 阶段二只 1 轮
  python scripts/diag/diag_blackbox_swan_now.py --top-n 30        # 阶段一让 Gemini 列 30 个
  python scripts/diag/diag_blackbox_swan_now.py --dry-run         # 只跑阶段一不跑阶段二
  python scripts/diag/diag_blackbox_swan_now.py --no-llm          # 跳过 Gemini, 测白名单流程
  python scripts/diag/diag_blackbox_swan_now.py --no-db           # 不落 MySQL, 只 JSON

输出:
  logs/blackbox_swan_now_<timestamp>.json         - 完整原始数据 (raw_rounds 全部留底)
  本地 MySQL (DB_HOST/DB_NAME from .env)          - 两张表 (blackbox_swan_runs / verdicts)
    默认 localhost:3306/binance-data, root, 跨次累积. 跟生产 dimesion 完全隔离.

跨次查询示例 (mysql -u root -p binance-data):
  SELECT id, asof_utc, phase1_proposed, phase2_evaluated, status
    FROM blackbox_swan_runs ORDER BY id DESC LIMIT 10;

  SELECT symbol, COUNT(*) AS hits,
         SUM(main_category='black_swan') AS black,
         SUM(main_category='red_swan') AS red
    FROM blackbox_swan_verdicts
   WHERE consistency_level IN ('STRONG','MODERATE')
   GROUP BY symbol ORDER BY hits DESC;

依赖: pip install google-genai pymysql python-dotenv
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

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

from app.services.securities_filter import is_security

# ------------------ 配置 ------------------
EXCLUDE_BASES = {"BTC", "ETH", "BNB", "SOL", "XRP"}
STABLECOINS = {"USDT", "USDC", "DAI", "FDUSD", "BUSD", "TUSD", "USDE", "USD1", "PYUSD"}
MIN_QUOTE_VOLUME_PHASE2 = 1_000_000  # 阶段二死币门槛: 在 prompt 里告诉 Gemini 自己识别低流动性

# Gemini 知识里的旧名 / 俗称 -> Binance fapi 现行 base 的映射.
# 资源: Gemini 2025-2026 训练数据里项目还叫旧名, 但币安期货已改名/合并.
# 1000-prefix 缩放 (PEPE->1000PEPE 等) 不写死, 用 resolve_symbol 动态 fallback 覆盖.
SYMBOL_ALIASES = {
    "RNDR": "RENDER",   # Render Token 2024 改名
    "AGIX": "FET",      # ASI 联盟 (FET/AGIX/OCEAN) 合并到 FET ticker
    "OCEAN": "FET",     # 同上
    "ASI": "FET",       # ASI 联盟统称
    "MATIC": "POL",     # Polygon 2024 迁移 MATIC -> POL
}

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3-flash-preview")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
TIMEOUT_S = int(os.getenv("GEMINI_SWAN_TIMEOUT_S", "180"))
OUTPUT_DIR = ROOT / "logs"
OUTPUT_DIR.mkdir(exist_ok=True)


BINANCE_FAPI_BASE = os.getenv("BINANCE_FAPI_BASE", "https://fapi.binance.com")
FAPI_TIMEOUT_S = int(os.getenv("BINANCE_FAPI_TIMEOUT_S", "30"))


def normalize_symbol(s: str) -> str:
    """统一成 'XXX/USDT' 格式 (跟现有 swan worker 一致)."""
    s = (s or "").upper().strip()
    if not s:
        return ""
    if "/" in s:
        return s
    if s.endswith("USDT"):
        return f"{s[:-4]}/USDT"
    return f"{s}/USDT"


def base_of(symbol: str) -> str:
    s = symbol.upper()
    if "/" in s:
        return s.split("/")[0]
    if s.endswith("USDT"):
        return s[:-4]
    return s


def is_excluded(symbol: str) -> bool:
    b = base_of(symbol)
    if b in EXCLUDE_BASES or b in STABLECOINS:
        return True
    return is_security(symbol)


def resolve_symbol(sym: str, active: dict) -> Optional[str]:
    """把 Gemini 输出的 symbol 映射到 active universe 里的实际 key.

    顺序:
      1. 直接命中 (sym 已经是规范名)
      2. 静态 alias (RNDR -> RENDER, AGIX -> FET, ...)
      3. 1000x 缩放 fallback (PEPE -> 1000PEPE)
    找不到返回 None, 让 filter_and_enrich 当 filtered_out 处理.
    """
    if sym in active:
        return sym
    b = base_of(sym)
    if b in SYMBOL_ALIASES:
        alt = f"{SYMBOL_ALIASES[b]}/USDT"
        if alt in active:
            return alt
    alt_1000 = f"1000{b}/USDT"
    if alt_1000 in active:
        return alt_1000
    return None


# ------------------ Binance fapi 一次性采集 ------------------
def _fapi_get_json(path: str) -> list:
    url = f"{BINANCE_FAPI_BASE}{path}"
    req = urllib.request.Request(url, headers={"User-Agent": "blackbox-swan/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=FAPI_TIMEOUT_S) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.URLError as e:
        raise RuntimeError(
            f"Binance fapi 调用失败 ({url}): {e}. "
            f"检查网络 (大陆可能要 VPN) 或 BINANCE_FAPI_BASE 环境变量."
        ) from e
    return data


def fetch_binance_universe() -> dict:
    """直连 Binance fapi 一次性拉 USDT 永续的 24h ticker + lastFundingRate.

    返回 {NORMALIZED_SYMBOL: {current_price, change_24h, quote_volume_24h,
                              current_rate, price_updated_at, funding_updated_at}}.
    一次拉, 不入中间表 (黑盒探索结果会以 universe_data snapshot 形式
    存进 blackbox_swan_verdicts.universe_data 留底).

    过滤: 只保留 USDT 永续 (排除 USDC / BUSD / 币本位), 排除 5 大盘 + 稳定币 + 证券类.
    """
    print(f"[fapi] GET /fapi/v1/ticker/24hr", file=sys.stderr)
    t0 = time.time()
    tickers = _fapi_get_json("/fapi/v1/ticker/24hr")
    print(f"[fapi] ticker n={len(tickers)} elapsed={time.time()-t0:.1f}s", file=sys.stderr)

    print(f"[fapi] GET /fapi/v1/premiumIndex", file=sys.stderr)
    t0 = time.time()
    premiums = _fapi_get_json("/fapi/v1/premiumIndex")
    print(f"[fapi] premium n={len(premiums)} elapsed={time.time()-t0:.1f}s", file=sys.stderr)

    fund_by_raw_sym = {p.get("symbol"): p for p in premiums if p.get("symbol")}

    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    out: dict = {}
    for t in tickers:
        raw_sym = str(t.get("symbol", ""))
        if not raw_sym.endswith("USDT"):
            continue
        sym = normalize_symbol(raw_sym)
        if not sym or is_excluded(sym):
            continue
        try:
            last_price = float(t.get("lastPrice") or 0) or None
            change_pct = float(t.get("priceChangePercent") or 0)
            quote_vol = float(t.get("quoteVolume") or 0) or None
        except (TypeError, ValueError):
            continue
        fr_raw = fund_by_raw_sym.get(raw_sym, {}).get("lastFundingRate")
        try:
            funding_rate = float(fr_raw) if fr_raw not in (None, "") else None
        except (TypeError, ValueError):
            funding_rate = None
        out[sym] = {
            "symbol": sym,
            "current_price": last_price,
            "change_24h": change_pct,
            "quote_volume_24h": quote_vol,
            "current_rate": funding_rate,
            "price_updated_at": now_iso,
            "funding_updated_at": now_iso,
        }
    print(f"[fapi] universe (USDT perp, filtered) n={len(out)}", file=sys.stderr)
    return out


# ------------------ Gemini 调用 ------------------
PHASE1_PROMPT_TEMPLATE = """你是加密货币衍生品研究员. 今天是 {asof}.

任务: 完全凭你的认知 (赛道动向、解锁日历、即将到来的事件、监管动态、生态催化、
社交叙事、历史规律), 列出未来 1-7 天最可能出现极端尾部行情的 USDT 永续合约,
共 {top_n} 个.

严格要求:
- 排除 BTC/ETH/BNB/SOL/XRP 和稳定币 (USDT/USDC/DAI/FDUSD/BUSD/TUSD/USDE/USD1/PYUSD)
- 必须给具体催化剂, 不能写"波动大""不确定性高""需要警惕"这种废话
- 必须给催化剂的时间窗 (如"7 天内", "本周内", "Q2")
- 你的知识截止日期之后的事件你不一定知道, 要在 knowledge_cutoff_caveat 里如实说
- 不要看任何价格或资金费率数据, 这一步是纯叙事+赛道+事件判断
- {top_n} 个 symbol 不要重复
- symbol 用 'XXX/USDT' 或 'XXXUSDT' 任一种格式即可

合格催化剂示例:
- "X 项目 5 月 20 日团队代币解锁 12% 流通量, 历史解锁日跌幅显著"
- "AI 赛道叙事过去 1 周持续升温, X 作为该赛道头部代币尚未补涨"
- "X 链 TVL 持续下行 + 上次类似情况 30 天内跌 40%"
- "X 协议核心团队近期社交媒体异常沉默, 信任风险累积"

输出 **仅** 一个合法 JSON 对象, 不要 markdown 围栏:
{{
  "asof_assumed": "你假设的当前年月, 格式 YYYY-MM",
  "knowledge_cutoff_caveat": "你的知识截止日期是什么, 之后的事件你不一定知道的免责",
  "proposals": [
    {{
      "symbol": "FOO/USDT",
      "expected_swan": "black_swan",
      "narrative_catalyst": "具体催化剂 1-2 句, 含时间窗",
      "category_basis": "为什么定这个方向 (黑还是红)"
    }}
  ]
}}
"""


PHASE2_PROMPT_TEMPLATE = """你之前在叙事阶段提名了下面这批 USDT 永续合约作为红/黑天鹅候选.
现在我给你这些 symbol 的**实时市场数据** (24h 涨跌幅、最新一期 lastFundingRate、24h 成交额,
直接来自 Binance fapi).

# 当前 universe (你的提名 + 数据):
{candidates_json}

**asof 时间** (由系统注入): {asof}

任务: 用数据**收敛或反转**你之前的叙事提名, 输出最终 verdict.

判定规则:
1. **数据强烈支持提名** (如 red_swan 且 funding 极负+刚跌完, 或 black_swan 且 funding 极正+滞涨):
   保留 category, confidence 提到 >= 0.65, data_alignment="aligned".
2. **数据反向**:
   - 你说 red_swan 但已经涨了 30%+ (高位, 已兑现): 改 skip 或保留但 confidence <= 0.3.
   - 你说 black_swan 但已经跌了 30%+ (低位, 反转可能): 改 skip 或改 red_swan.
   data_alignment="conflicting".
3. **数据中性** (无明显异动): 保留方向但 confidence 0.3-0.5, data_alignment="neutral".
4. **死币过滤**: 24h 成交额 < 100 万 USDT 的, 强制 category=skip + reason="low_liquidity".
5. **你完全不熟这个币**: category=skip + confidence <= 0.3 + reason="unfamiliar".
6. 必须给 refined_catalyst, 不能照抄阶段一原文, 要结合数据重写.

排除规则: 已排除 BTC/ETH/BNB/SOL/XRP 和稳定币.

输出 **仅** 一个合法 JSON 对象, 不要 markdown 围栏:
{{
  "summary_zh": "整体看 1-2 句, 黑盒提名的 hit/miss 大致情况",
  "verdicts": [
    {{
      "symbol": "FOO/USDT",
      "category": "black_swan",
      "confidence": 0.65,
      "refined_catalyst": "叙事 + 数据综合后的催化剂 (1-2 句)",
      "data_alignment": "aligned",
      "risk_note": "反向风险一句"
    }}
  ]
}}
"""


def _gemini_client():
    if not GEMINI_API_KEY:
        print("ERROR: 请设置 GEMINI_API_KEY (.env 或环境变量)", file=sys.stderr)
        return None, None
    try:
        from google import genai
        from google.genai import types
    except ImportError:
        print("ERROR: pip install google-genai", file=sys.stderr)
        return None, None
    client = genai.Client(api_key=GEMINI_API_KEY)
    cfg = types.GenerateContentConfig(
        response_mime_type="application/json",
        http_options=types.HttpOptions(timeout=TIMEOUT_S * 1000),
    )
    return client, cfg


def _call_gemini_once(client, cfg, prompt: str, tag: str) -> Optional[dict]:
    t0 = time.time()
    try:
        resp = client.models.generate_content(
            model=GEMINI_MODEL, contents=prompt, config=cfg,
        )
    except Exception as e:
        print(f"ERROR [{tag}]: Gemini 调用失败: {e}", file=sys.stderr)
        return None
    text = (resp.text or "").strip()
    if text.startswith("```"):
        text = text.strip("`").lstrip("json").strip()
    print(f"[{tag}] elapsed={time.time()-t0:.1f}s output_len={len(text)}", file=sys.stderr)
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        print(f"ERROR [{tag}]: JSON 解析失败: {e}; raw[:500]={text[:500]}",
              file=sys.stderr)
        return None


# ------------------ 阶段一: 黑盒提名 ------------------
def run_phase1(client, cfg, rounds_n: int, top_n: int) -> dict:
    """跑 N 轮无数据提名, 返回 {raw_rounds: [...], proposals: {symbol: {...}}}."""
    raw_rounds = []
    proposed: dict = {}
    asof = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    for i in range(rounds_n):
        print(f"[p1] round {i+1}/{rounds_n} start", file=sys.stderr)
        prompt = PHASE1_PROMPT_TEMPLATE.format(asof=asof, top_n=top_n)
        out = _call_gemini_once(client, cfg, prompt, f"p1.{i+1}")
        raw_rounds.append(out)
        if not out:
            continue
        for p in out.get("proposals") or []:
            sym = normalize_symbol(str(p.get("symbol", "")))
            if not sym or is_excluded(sym):
                continue
            existing = proposed.get(sym)
            entry = {
                "symbol": sym,
                "expected_swans": [],
                "narrative_catalysts": [],
                "proposed_rounds": 0,
            }
            if existing:
                entry = existing
            entry["proposed_rounds"] += 1
            sw = str(p.get("expected_swan", "")).lower().strip()
            if sw in ("black_swan", "red_swan"):
                entry["expected_swans"].append(sw)
            nc = str(p.get("narrative_catalyst", "")).strip()
            if nc:
                entry["narrative_catalysts"].append(nc)
            proposed[sym] = entry
        if i < rounds_n - 1:
            time.sleep(2)  # 多轮之间稍微歇一下, 避免 rate limit
    print(f"[p1] done rounds={rounds_n} unique_proposals={len(proposed)}",
          file=sys.stderr)
    return {"raw_rounds": raw_rounds, "proposals": proposed}


# ------------------ 阶段二: 数据校验 + 收敛 ------------------
def filter_and_enrich(active: dict, proposals: dict) -> tuple[list, list]:
    """用 Binance fapi 一次采集的 active universe 当白名单过滤 + 直接拿数据.

    返回 (enriched_universe, filtered_out_rows).
    enriched_universe: 阶段二要喂 Gemini 的列表, 含 symbol+提名信息+实时数据.
    filtered_out_rows: 被拒的 [{symbol, reason}], 诊断黑盒拍脑袋率 (Gemini 编出的 symbol).
    """
    enriched = []
    filtered_out = []
    for sym, info in proposals.items():
        resolved = resolve_symbol(sym, active)
        if resolved is None:
            filtered_out.append({
                "symbol": sym,
                "reason": "not_in_binance_usdt_perp",
                "expected_swan_votes": info["expected_swans"],
                "narrative_catalyst": info["narrative_catalysts"][0]
                                      if info["narrative_catalysts"] else None,
                "proposed_rounds": info["proposed_rounds"],
            })
            continue
        u = active[resolved]
        enriched.append({
            "symbol": resolved,
            "alias_of": sym if resolved != sym else None,
            "proposed_rounds": info["proposed_rounds"],
            "expected_swan_votes": info["expected_swans"],
            "narrative_catalysts": info["narrative_catalysts"],
            "current_price": u.get("current_price"),
            "change_24h": u.get("change_24h"),
            "quote_volume_24h": u.get("quote_volume_24h"),
            "current_rate": u.get("current_rate"),
            "price_updated_at": u.get("price_updated_at"),
            "funding_updated_at": u.get("funding_updated_at"),
        })
    return enriched, filtered_out


def run_phase2(client, cfg, rounds_n: int, enriched: list) -> dict:
    """跑 M 轮数据收敛, 返回 {raw_rounds: [...], aggregated: [...]}."""
    raw_rounds = []
    asof = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    candidates_json = json.dumps(enriched, ensure_ascii=False, indent=2, default=str)

    for i in range(rounds_n):
        print(f"[p2] round {i+1}/{rounds_n} start (candidates={len(enriched)})",
              file=sys.stderr)
        prompt = PHASE2_PROMPT_TEMPLATE.format(
            candidates_json=candidates_json, asof=asof
        )
        out = _call_gemini_once(client, cfg, prompt, f"p2.{i+1}")
        raw_rounds.append(out)
        if i < rounds_n - 1:
            time.sleep(2)

    aggregated = _aggregate_phase2(raw_rounds, rounds_n, enriched)
    summaries = [r.get("summary_zh") for r in raw_rounds if r and r.get("summary_zh")]
    return {
        "raw_rounds": raw_rounds,
        "aggregated": aggregated,
        "summary_zh": summaries[-1] if summaries else "",
    }


def _aggregate_phase2(rounds: list, n_rounds: int, enriched: list) -> list:
    """每个 symbol 在 n_rounds 里的投票聚合, 给 STRONG/MODERATE/WEAK."""
    import math
    cat_count: dict = defaultdict(lambda: defaultdict(int))
    conf_sum: dict = defaultdict(lambda: defaultdict(float))
    last_catalyst: dict = {}
    last_alignment: dict = {}
    last_risk: dict = {}

    enriched_by_sym = {e["symbol"]: e for e in enriched}

    for r in rounds:
        if not r:
            continue
        for v in r.get("verdicts") or []:
            sym = normalize_symbol(str(v.get("symbol", "")))
            if not sym:
                continue
            cat = str(v.get("category", "")).lower().strip() or "skip"
            if cat not in ("black_swan", "red_swan", "skip"):
                cat = "skip"
            cat_count[sym][cat] += 1
            try:
                c = float(v.get("confidence") or 0.0)
            except (TypeError, ValueError):
                c = 0.0
            conf_sum[sym][cat] += c
            if v.get("refined_catalyst"):
                last_catalyst[sym] = str(v["refined_catalyst"])
            if v.get("data_alignment"):
                last_alignment[sym] = str(v["data_alignment"]).lower()
            if v.get("risk_note"):
                last_risk[sym] = str(v["risk_note"])

    strong_threshold = max(2, math.ceil(n_rounds * 0.7))
    moderate_threshold = max(2, math.ceil(n_rounds / 2))
    if n_rounds == 1:
        strong_threshold = 1
        moderate_threshold = 1

    out = []
    order = ["black_swan", "red_swan", "skip"]
    for sym, counts in cat_count.items():
        main_cat = max(order,
                       key=lambda c: (counts.get(c, 0),
                                      -order.index(c) if counts.get(c, 0) > 0 else -100))
        main_n = counts.get(main_cat, 0)
        avg_conf = (conf_sum[sym].get(main_cat, 0.0) / main_n) if main_n else 0.0
        if main_cat == "skip":
            level = "SKIP"
        elif main_n >= strong_threshold:
            level = "STRONG"
        elif main_n >= moderate_threshold:
            level = "MODERATE"
        else:
            level = "WEAK"
        e = enriched_by_sym.get(sym, {})
        out.append({
            "symbol": sym,
            "main_category": main_cat,
            "consistency_level": level,
            "avg_confidence": round(avg_conf, 3),
            "rounds_total": n_rounds,
            "black_count": counts.get("black_swan", 0),
            "red_count": counts.get("red_swan", 0),
            "skip_count": counts.get("skip", 0),
            "phase1_proposed_rounds": e.get("proposed_rounds"),
            "phase1_expected_swans": e.get("expected_swan_votes"),
            "refined_catalyst": last_catalyst.get(sym),
            "data_alignment": last_alignment.get(sym),
            "risk_note": last_risk.get(sym),
            "universe_data": {
                "current_price": e.get("current_price"),
                "change_24h": e.get("change_24h"),
                "quote_volume_24h": e.get("quote_volume_24h"),
                "current_rate": e.get("current_rate"),
                "alias_of": e.get("alias_of"),
            },
        })
    # 排序: 先 STRONG/MODERATE/WEAK/SKIP, 再 confidence
    rank = {"STRONG": 0, "MODERATE": 1, "WEAK": 2, "SKIP": 3}
    out.sort(key=lambda x: (rank.get(x["consistency_level"], 9), -x["avg_confidence"]))
    return out


# ------------------ 本地 MySQL 落库 ------------------
# 配置来源: .env 里的 DB_HOST / DB_PORT / DB_USER / DB_PASSWORD / DB_NAME
# 默认 localhost:3306/binance-data, 跟生产 dimesion (table_schemas.txt 里的远程库) 完全隔离.

MYSQL_SCHEMA_RUNS = """
CREATE TABLE IF NOT EXISTS `blackbox_swan_runs` (
  `id`              INT NOT NULL AUTO_INCREMENT,
  `asof_utc`        VARCHAR(40) NOT NULL,
  `model`           VARCHAR(64) NOT NULL,
  `rounds_p1`       INT NOT NULL,
  `rounds_p2`       INT NOT NULL,
  `top_n`           INT NOT NULL,
  `phase1_proposed` INT NOT NULL,
  `phase1_filtered_out` INT NOT NULL,
  `phase2_evaluated` INT NOT NULL,
  `summary_zh`      TEXT,
  `status`          VARCHAR(20) NOT NULL,
  `error_msg`       TEXT,
  `elapsed_s`       DECIMAL(10,2),
  `dry_run`         TINYINT(1) NOT NULL DEFAULT 0,
  `created_at`      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_runs_asof` (`asof_utc`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='black-box Gemini swan exploration runs (local)';
"""

MYSQL_SCHEMA_VERDICTS = """
CREATE TABLE IF NOT EXISTS `blackbox_swan_verdicts` (
  `id`              INT NOT NULL AUTO_INCREMENT,
  `run_id`          INT NOT NULL,
  `symbol`          VARCHAR(30) NOT NULL,
  `source_phase`    VARCHAR(30) NOT NULL COMMENT 'phase2_verdict / phase1_filtered',
  `main_category`   VARCHAR(20) DEFAULT NULL COMMENT 'black_swan / red_swan / skip / NULL',
  `consistency_level` VARCHAR(10) DEFAULT NULL COMMENT 'STRONG/MODERATE/WEAK/SKIP/NULL',
  `avg_confidence`  DECIMAL(4,3) DEFAULT NULL,
  `rounds_total`    INT DEFAULT NULL,
  `black_count`     INT DEFAULT NULL,
  `red_count`       INT DEFAULT NULL,
  `skip_count`      INT DEFAULT NULL,
  `phase1_proposed_rounds` INT DEFAULT NULL,
  `phase1_expected_swans`  TEXT COMMENT 'JSON array',
  `narrative_catalysts`    TEXT COMMENT 'JSON array (phase1 original)',
  `refined_catalyst`       TEXT,
  `data_alignment`         VARCHAR(20) DEFAULT NULL,
  `risk_note`              TEXT,
  `universe_data`          TEXT COMMENT 'JSON {price, change_24h, ...}',
  `filtered_out`           TINYINT(1) NOT NULL DEFAULT 0,
  `filtered_reason`        VARCHAR(64) DEFAULT NULL,
  `created_at`             DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_v_run` (`run_id`),
  KEY `idx_v_symbol` (`symbol`),
  KEY `idx_v_cat_level` (`main_category`, `consistency_level`),
  CONSTRAINT `fk_bbx_verdict_run` FOREIGN KEY (`run_id`)
    REFERENCES `blackbox_swan_runs` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='black-box Gemini swan verdicts (local)';
"""


def load_local_db_cfg(args) -> dict:
    """本地 MySQL: .env 优先, --db-* 参数可覆盖. 跟 dimesion 远程库无关."""
    cfg = {
        "host": args.db_host or os.getenv("DB_HOST", "localhost"),
        "port": int(args.db_port or os.getenv("DB_PORT", "3306")),
        "user": args.db_user or os.getenv("DB_USER", "root"),
        "password": args.db_password or os.getenv("DB_PASSWORD", ""),
        "database": args.db_name or os.getenv("DB_NAME", "binance-data"),
        "charset": "utf8mb4",
        "cursorclass": pymysql.cursors.DictCursor,
    }
    return cfg


def init_local_mysql(cfg: dict) -> pymysql.connections.Connection:
    """连本地 MySQL, 建表 (幂等). 库不存在则报错让用户先建."""
    try:
        conn = pymysql.connect(**cfg)
    except pymysql.err.OperationalError as e:
        # 1049 = Unknown database
        if e.args and e.args[0] == 1049:
            raise RuntimeError(
                f"本地数据库 `{cfg['database']}` 不存在. 先在 MySQL 里建好: "
                f"CREATE DATABASE `{cfg['database']}` CHARACTER SET utf8mb4 "
                f"COLLATE utf8mb4_unicode_ci;"
            ) from e
        raise
    with conn.cursor() as cur:
        cur.execute(MYSQL_SCHEMA_RUNS)
        cur.execute(MYSQL_SCHEMA_VERDICTS)
    conn.commit()
    return conn


def persist_to_mysql(
    cfg: dict,
    asof_iso: str,
    model: str,
    rounds_p1: int,
    rounds_p2: int,
    top_n: int,
    dry_run: bool,
    proposals: dict,
    filtered_out: list,
    enriched: list,
    p2_result: Optional[dict],
    elapsed_s: float,
    status: str,
    error_msg: Optional[str],
) -> int:
    """返回 run_id."""
    conn = init_local_mysql(cfg)
    try:
        with conn.cursor() as cur:
            phase2_evaluated = len(p2_result.get("aggregated") or []) if p2_result else 0
            summary_zh = (p2_result.get("summary_zh") if p2_result else "") or ""
            cur.execute(
                """
                INSERT INTO `blackbox_swan_runs`
                  (asof_utc, model, rounds_p1, rounds_p2, top_n,
                   phase1_proposed, phase1_filtered_out, phase2_evaluated,
                   summary_zh, status, error_msg, elapsed_s, dry_run)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    asof_iso, model, rounds_p1, rounds_p2, top_n,
                    len(proposals), len(filtered_out), phase2_evaluated,
                    summary_zh, status, error_msg, elapsed_s, 1 if dry_run else 0,
                ),
            )
            run_id = cur.lastrowid

            # narrative_catalysts 阶段二 verdict 没有 -> 从 enriched 里捞
            enriched_by_sym = {e["symbol"]: e for e in enriched}

            # 阶段二 verdict (主表)
            if p2_result:
                rows = []
                for v in p2_result.get("aggregated") or []:
                    e = enriched_by_sym.get(v["symbol"], {})
                    rows.append((
                        run_id, v["symbol"], "phase2_verdict",
                        v["main_category"], v["consistency_level"],
                        v["avg_confidence"], v["rounds_total"],
                        v["black_count"], v["red_count"], v["skip_count"],
                        v.get("phase1_proposed_rounds"),
                        json.dumps(v.get("phase1_expected_swans") or [], ensure_ascii=False),
                        json.dumps(e.get("narrative_catalysts") or [], ensure_ascii=False),
                        v.get("refined_catalyst"),
                        v.get("data_alignment"),
                        v.get("risk_note"),
                        json.dumps(v.get("universe_data") or {}, ensure_ascii=False),
                        0, None,
                    ))
                if rows:
                    cur.executemany(
                        """
                        INSERT INTO `blackbox_swan_verdicts`
                          (run_id, symbol, source_phase, main_category, consistency_level,
                           avg_confidence, rounds_total, black_count, red_count, skip_count,
                           phase1_proposed_rounds, phase1_expected_swans, narrative_catalysts,
                           refined_catalyst, data_alignment, risk_note, universe_data,
                           filtered_out, filtered_reason)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        rows,
                    )

            # 被白名单过滤掉的 (诊断 Gemini 拍脑袋率)
            f_rows = []
            for f in filtered_out:
                f_rows.append((
                    run_id, f["symbol"], "phase1_filtered",
                    None, None, None, None, None, None, None,
                    f.get("proposed_rounds"),
                    json.dumps(f.get("expected_swan_votes") or [], ensure_ascii=False),
                    json.dumps([f.get("narrative_catalyst")] if f.get("narrative_catalyst") else [],
                               ensure_ascii=False),
                    None, None, None, None,
                    1, f.get("reason"),
                ))
            if f_rows:
                cur.executemany(
                    """
                    INSERT INTO `blackbox_swan_verdicts`
                      (run_id, symbol, source_phase, main_category, consistency_level,
                       avg_confidence, rounds_total, black_count, red_count, skip_count,
                       phase1_proposed_rounds, phase1_expected_swans, narrative_catalysts,
                       refined_catalyst, data_alignment, risk_note, universe_data,
                       filtered_out, filtered_reason)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    f_rows,
                )
        conn.commit()
        return run_id
    finally:
        conn.close()


# ------------------ 主流程 ------------------
def main():
    parser = argparse.ArgumentParser(description="黑盒红黑天鹅探索 (本地, 不落 DB)")
    parser.add_argument("--rounds-p1", type=int, default=2,
                        help="阶段一无数据提名跑几轮 (默认 2)")
    parser.add_argument("--rounds-p2", type=int, default=2,
                        help="阶段二数据收敛跑几轮 (默认 2)")
    parser.add_argument("--top-n", type=int, default=20,
                        help="阶段一让 Gemini 列多少个候选 (默认 20)")
    parser.add_argument("--dry-run", action="store_true",
                        help="只跑阶段一 + 白名单过滤, 不跑阶段二 (省钱)")
    parser.add_argument("--no-llm", action="store_true",
                        help="不调 Gemini, 仅验证 DB + 白名单流程")
    parser.add_argument("--no-db", action="store_true",
                        help="不落本地 MySQL, 只写 JSON")
    parser.add_argument("--db-host", type=str, default=None,
                        help="本地 MySQL host (默认 .env DB_HOST 或 localhost)")
    parser.add_argument("--db-port", type=int, default=None,
                        help="本地 MySQL port (默认 .env DB_PORT 或 3306)")
    parser.add_argument("--db-user", type=str, default=None,
                        help="本地 MySQL user (默认 .env DB_USER 或 root)")
    parser.add_argument("--db-password", type=str, default=None,
                        help="本地 MySQL password (默认 .env DB_PASSWORD)")
    parser.add_argument("--db-name", type=str, default=None,
                        help="本地 MySQL database (默认 .env DB_NAME 或 binance-data)")
    args = parser.parse_args()
    t_start = time.time()

    if args.no_llm:
        # 仅测试 Binance fapi 采集流程 (不调 Gemini, 不写本地 MySQL)
        active = fetch_binance_universe()
        print(f"[no-llm] active_universe_size={len(active)}", file=sys.stderr)
        sample = list(active.items())[:10]
        for sym, u in sample:
            print(f"  {sym:18s} px={u['current_price']} ch24h={u['change_24h']} "
                  f"fr={u['current_rate']} vol={u['quote_volume_24h']}",
                  file=sys.stderr)
        return

    client, gcfg = _gemini_client()
    if not client:
        sys.exit(2)

    # 阶段一
    print(f"[main] phase1 rounds={args.rounds_p1} top_n={args.top_n}", file=sys.stderr)
    p1 = run_phase1(client, gcfg, args.rounds_p1, args.top_n)
    if not p1["proposals"]:
        print("ERROR: 阶段一无任何 proposal, 终止", file=sys.stderr)
        sys.exit(3)

    # Binance fapi 一次采集 + 白名单过滤
    active = fetch_binance_universe()
    enriched, filtered_out = filter_and_enrich(active, p1["proposals"])
    print(f"[main] phase1 -> phase2: enriched={len(enriched)} "
          f"filtered_out={len(filtered_out)}", file=sys.stderr)

    # 阶段二 (可选)
    p2_result = None
    if not args.dry_run and enriched:
        print(f"[main] phase2 rounds={args.rounds_p2}", file=sys.stderr)
        p2_result = run_phase2(client, gcfg, args.rounds_p2, enriched)

    # 落本地 JSON
    asof_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    out_payload = {
        "asof_utc": asof_iso,
        "_meta": {
            "model": GEMINI_MODEL,
            "rounds_p1": args.rounds_p1,
            "rounds_p2": args.rounds_p2 if not args.dry_run else 0,
            "top_n": args.top_n,
            "dry_run": args.dry_run,
            "data_source": "binance_fapi_realtime",
            "fapi_base": BINANCE_FAPI_BASE,
            "excluded_bases": sorted(EXCLUDE_BASES | STABLECOINS),
        },
        "phase1": {
            "raw_rounds": p1["raw_rounds"],
            "aggregated_proposals": [
                {
                    "symbol": s,
                    "proposed_rounds": v["proposed_rounds"],
                    "expected_swans": v["expected_swans"],
                    "narrative_catalysts": v["narrative_catalysts"],
                }
                for s, v in sorted(
                    p1["proposals"].items(),
                    key=lambda kv: -kv[1]["proposed_rounds"],
                )
            ],
        },
        "phase2": (
            {
                "enriched_universe": enriched,
                "filtered_out": filtered_out,
                "summary_zh": p2_result.get("summary_zh") if p2_result else "",
                "verdicts": p2_result.get("aggregated") if p2_result else [],
                "raw_rounds": p2_result.get("raw_rounds") if p2_result else [],
            }
            if p2_result is not None
            else {
                "enriched_universe": enriched,
                "filtered_out": filtered_out,
                "summary_zh": "",
                "verdicts": [],
                "raw_rounds": [],
                "_note": "dry-run, 阶段二未运行" if args.dry_run else "enriched 为空, 阶段二未运行",
            }
        ),
    }
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = OUTPUT_DIR / f"blackbox_swan_now_{ts}.json"
    out_path.write_text(json.dumps(out_payload, ensure_ascii=False, indent=2, default=str),
                        encoding="utf-8")
    print(f"[done] json -> {out_path}", file=sys.stderr)

    # 落本地 MySQL
    elapsed_s = round(time.time() - t_start, 2)
    if args.no_db:
        print("[done] --no-db 跳过本地 MySQL", file=sys.stderr)
    else:
        local_cfg = load_local_db_cfg(args)
        status = "success" if (args.dry_run or p2_result) else "partial"
        try:
            run_id = persist_to_mysql(
                cfg=local_cfg,
                asof_iso=asof_iso,
                model=GEMINI_MODEL,
                rounds_p1=args.rounds_p1,
                rounds_p2=0 if args.dry_run else args.rounds_p2,
                top_n=args.top_n,
                dry_run=args.dry_run,
                proposals=p1["proposals"],
                filtered_out=filtered_out,
                enriched=enriched,
                p2_result=p2_result,
                elapsed_s=elapsed_s,
                status=status,
                error_msg=None,
            )
            print(
                f"[done] mysql -> {local_cfg['host']}:{local_cfg['port']}/"
                f"{local_cfg['database']} run_id={run_id}",
                file=sys.stderr,
            )
        except Exception as e:
            print(f"ERROR: 本地 MySQL 写入失败: {e}", file=sys.stderr)

    # 控制台摘要
    print("", file=sys.stderr)
    print("===== 阶段一: 黑盒提名 (按被提名次数排序) =====", file=sys.stderr)
    sorted_p = sorted(p1["proposals"].items(), key=lambda kv: -kv[1]["proposed_rounds"])
    for sym, info in sorted_p[:30]:
        swans = ",".join(info["expected_swans"]) or "?"
        ncat = (info["narrative_catalysts"][0] if info["narrative_catalysts"] else "")[:80]
        in_active = "OK" if sym in {e["symbol"] for e in enriched} else "FILTERED"
        print(f"  [{in_active:8s}] {sym:18s} rounds={info['proposed_rounds']} "
              f"swan={swans:12s} catalyst={ncat}", file=sys.stderr)

    print("", file=sys.stderr)
    print(f"===== 白名单过滤: {len(filtered_out)} 个被剔 =====", file=sys.stderr)
    for f in filtered_out[:20]:
        print(f"  [{f['reason']}] {f['symbol']}", file=sys.stderr)

    if p2_result:
        print("", file=sys.stderr)
        print("===== 阶段二: 数据收敛后的最终 verdict =====", file=sys.stderr)
        for v in (p2_result["aggregated"] or [])[:30]:
            ud = v.get("universe_data") or {}
            print(
                f"  [{v['consistency_level']:8s}] {v['symbol']:18s} "
                f"{v['main_category']:11s} conf={v['avg_confidence']} "
                f"align={(v.get('data_alignment') or '?'):11s} "
                f"24h={ud.get('change_24h')} fund={ud.get('current_rate')}",
                file=sys.stderr,
            )
            if v.get("refined_catalyst"):
                print(f"           -> {v['refined_catalyst'][:120]}", file=sys.stderr)
        print(f"\nsummary: {p2_result.get('summary_zh', '')}", file=sys.stderr)


if __name__ == "__main__":
    main()
