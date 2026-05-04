"""
Gemini 红黑天鹅榜后台采集 worker.

每 2h 由 app/scheduler.py 在独立线程触发 run_swan_round(),
跑 N 轮 Gemini, 聚合一致性, 写入 gemini_swan_runs + gemini_swan_verdicts.

线程安全:
- 不调 FuturesTradingEngine, 纯只读 + 写两张新表, 无并发竞争.
- pymysql 连接每次新建, 不复用全局连接.

system_settings 控制 (60s 动态生效):
- gemini_swan_enabled  : '0' = worker 早返回不调 Gemini, '1' = 跑
- gemini_swan_rounds   : 1-5 轮 (默认 3)

数据源:
- price_stats_24h    -> 24h 涨跌幅 + 成交额 (1 分钟更新)
- funding_rate_data  -> 当前最新资金费率 (与 price_stats_24h 同步刷新)
"""
from __future__ import annotations

import json
import math
import os
import re
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pymysql
from loguru import logger

from app.services.securities_filter import is_security

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[2] / ".env")
except ImportError:
    pass


# ------------------ 配置常量 ------------------
EXCLUDE_BASES = {"BTC", "ETH", "BNB", "SOL", "XRP"}
STABLECOINS = {"USDT", "USDC", "DAI", "FDUSD", "BUSD", "TUSD", "USDE", "USD1", "PYUSD"}
MIN_QUOTE_VOLUME = 10_000_000  # 1000 万 USDT 24h 成交额下限
TOP_MOVER = 12                  # 24h 涨幅 / 跌幅 各取 top 12
TOP_FUNDING = 10                # 资金费率 极正 / 极负 各取 top 10
ROUND_INTERVAL_S = 60           # 多轮间隔
DEFAULT_ROUNDS = 3

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3-flash-preview")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_TIMEOUT_S = int(os.getenv("GEMINI_SWAN_TIMEOUT_S", "180"))


# ------------------ DB ------------------
def _load_remote_db_cfg() -> dict:
    """
    返回 dimesion DB 连接配置.

    host 解析优先级:
      1. 环境变量 DIMENSION_DB_HOST (服务器本地用 127.0.0.1 走回环, 避免公网 IP 抖动 + 安全组绕一圈)
      2. table_schemas.txt 头部 host: 行 (dev 机/外部访问用, IP 会变, 不硬编码到代码)
    """
    cfg = {"port": 3306, "user": "admin", "password": "Yintao@110",
           "database": "dimesion", "charset": "utf8mb4",
           "cursorclass": pymysql.cursors.DictCursor}

    env_host = os.getenv("DIMENSION_DB_HOST", "").strip()
    if env_host:
        cfg["host"] = env_host
        return cfg

    project_root = Path(__file__).resolve().parents[2]
    path = project_root / "table_schemas.txt"
    head = path.read_text(encoding="utf-8").splitlines()[:15]
    for line in head:
        m = re.match(r"\s*host\s*[:=]\s*([\d\.]+)", line)
        if m:
            cfg["host"] = m.group(1)
            break
    if "host" not in cfg:
        raise RuntimeError("DIMENSION_DB_HOST 未设, 且 table_schemas.txt 头部没解析到 host IP")
    return cfg


def _read_setting(cur, key: str, default: str) -> str:
    cur.execute(
        "SELECT setting_value FROM system_settings WHERE setting_key = %s LIMIT 1",
        (key,),
    )
    row = cur.fetchone()
    if not row:
        return default
    val = row.get("setting_value")
    return str(val) if val is not None else default


# ------------------ 工具 ------------------
def _base_of(symbol: str) -> str:
    s = symbol.upper()
    if "/" in s:
        return s.split("/")[0]
    if s.endswith("USDT"):
        return s[:-4]
    return s


def _is_excluded(symbol: str) -> bool:
    b = _base_of(symbol)
    if b in EXCLUDE_BASES or b in STABLECOINS:
        return True
    return is_security(symbol)


# ------------------ universe 采集 ------------------
def _fetch_movers_24h(cur, top_n: int):
    """24h 涨幅 / 跌幅 top, quote_volume_24h >= 1000 万 USDT."""
    base_sql = """
        SELECT symbol, current_price, change_24h, quote_volume_24h, trend, updated_at
        FROM price_stats_24h
        WHERE quote_volume_24h >= %s
          AND change_24h IS NOT NULL
          AND updated_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 10 MINUTE)
        ORDER BY change_24h {order}
        LIMIT %s
    """
    cur.execute(base_sql.format(order="DESC"), (MIN_QUOTE_VOLUME, top_n * 3))
    gainers = [r for r in cur.fetchall() if not _is_excluded(r["symbol"])][:top_n]
    cur.execute(base_sql.format(order="ASC"), (MIN_QUOTE_VOLUME, top_n * 3))
    losers = [r for r in cur.fetchall() if not _is_excluded(r["symbol"])][:top_n]
    return gainers, losers


def _fetch_extreme_funding(cur, top_n: int):
    """资金费率 极正 (多头拥挤) / 极负 (空头拥挤). 用 funding_rate_data (新鲜)."""
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
    pos = [r for r in cur.fetchall() if not _is_excluded(r["symbol"])][:top_n]
    cur.execute(base_sql.format(order="ASC"), (top_n * 3,))
    neg = [r for r in cur.fetchall() if not _is_excluded(r["symbol"])][:top_n]
    return pos, neg


def _merge_universe(gainers, losers, fund_pos, fund_neg) -> dict:
    uni: dict = {}

    def upsert(sym, **fields):
        sym = sym.upper()
        if sym not in uni:
            uni[sym] = {"symbol": sym, "triggers": [],
                        "current_price": None, "change_24h": None,
                        "quote_volume_24h": None,
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


# ------------------ Gemini 调用 ------------------
SWAN_PROMPT_TEMPLATE = """你是加密货币衍生品风险研究员. 我会给你一组当前市场上有异动迹象的 USDT 永续合约,
**附带每个 symbol 的实时数据 (24h 涨跌幅, 资金费率, 触发原因)**.

请基于这些**实时数据 + 你对该币种基本面/赛道/历史叙事的认知**, 标注每个 symbol 在**未来 1-7 天**最可能的天鹅类型:

术语:
- 黑天鹅 (black_swan): 极端负向尾部 - 急跌, 闪崩, 连环爆仓, 脱锚, 信任危机, 监管雷, 暴雷, 解锁砸盘等
- 红天鹅 (red_swan):   极端正向尾部 - 暴涨, 空头挤压, 叙事爆发, 利好兑现, 生态催化, 上线大所等
- skip:                数据不支持任何天鹅论点, 或你完全不熟该币

判定要求:
1. **必须给具体催化剂** (catalyst) - 不能只说 "高波动" "不确定性大".
2. **结合数据**: 资金费极正 + 24h 涨幅大 = 多头拥挤 (black 倾向);
                资金费极负 + 24h 跌幅大 = 反转可能 (red 倾向).
3. **confidence**: 0.0-1.0. 自己不熟该币时给 <= 0.3 并 category=skip.

排除规则: 已排除 BTC/ETH/BNB/SOL/XRP 和稳定币, 不需要再讨论.

**asof 时间** (由系统注入): {asof}

# 当前 universe 数据 (来自远程数据库, UTC):
{universe_json}

输出 **仅** 一个合法 JSON 对象, 不要 markdown 代码围栏:
{{
  "summary_zh": "整体市场氛围 1-2 句",
  "verdicts": [
    {{
      "symbol": "FOO/USDT",
      "category": "black_swan",
      "confidence": 0.65,
      "catalyst": "具体催化剂 1-2 句",
      "data_signal": "用上下文里哪个数据支持判断",
      "risk_note": "反向风险一句"
    }}
  ]
}}
"""


def _call_gemini(universe: dict) -> Optional[dict]:
    if not GEMINI_API_KEY:
        logger.error("GEMINI_API_KEY 未设置, swan worker 无法调用")
        return None
    try:
        from google import genai
        from google.genai import types
    except ImportError:
        logger.error("缺依赖, 请 pip install google-genai")
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
        http_options=types.HttpOptions(timeout=GEMINI_TIMEOUT_S * 1000),
    )
    t0 = time.time()
    try:
        resp = client.models.generate_content(
            model=GEMINI_MODEL, contents=prompt, config=cfg,
        )
    except Exception as e:
        logger.error(f"Gemini 调用失败: {e}")
        return None

    text = (resp.text or "").strip()
    if text.startswith("```"):
        text = text.strip("`").lstrip("json").strip()
    logger.info(f"swan gemini elapsed={time.time()-t0:.1f}s output_len={len(text)}")
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        logger.error(f"swan gemini JSON 解析失败: {e}; raw[:500]={text[:500]}")
        return None


# ------------------ 多轮聚合 ------------------
def _aggregate(rounds: list, n_rounds: int) -> dict:
    universe_count: dict = defaultdict(int)
    cat_count: dict = defaultdict(lambda: defaultdict(int))
    conf_sum: dict = defaultdict(lambda: defaultdict(float))
    last_catalyst: dict = defaultdict(list)
    last_signal: dict = defaultdict(list)
    last_risk: dict = defaultdict(list)
    triggers_seen: dict = defaultdict(set)
    last_universe_data: dict = {}
    summaries = []

    for r in rounds:
        if not r:
            continue
        if r.get("summary_zh"):
            summaries.append(r["summary_zh"])
        for sym, info in r["universe"].items():
            universe_count[sym] += 1
            for t in info.get("triggers", []):
                triggers_seen[sym].add(t)
            last_universe_data[sym] = {
                k: v for k, v in info.items() if k != "triggers"
            }
        for sym, v in r["verdicts"].items():
            cat = str(v.get("category", "")).lower().strip() or "skip"
            if cat not in ("black_swan", "red_swan", "skip"):
                cat = "skip"
            cat_count[sym][cat] += 1
            try:
                c = float(v.get("confidence") or 0.0)
            except (TypeError, ValueError):
                c = 0.0
            conf_sum[sym][cat] += c
            if v.get("catalyst"):
                last_catalyst[sym].append(str(v["catalyst"]))
            if v.get("data_signal"):
                last_signal[sym].append(str(v["data_signal"]))
            if v.get("risk_note"):
                last_risk[sym].append(str(v["risk_note"]))

    strong_threshold = max(2, math.ceil(n_rounds * 0.7))
    moderate_threshold = max(2, math.ceil(n_rounds / 2))
    if n_rounds == 1:
        strong_threshold = 1
        moderate_threshold = 1

    aggregated = []
    order = ["black_swan", "red_swan", "skip"]
    for sym, counts in cat_count.items():
        main_cat = max(order, key=lambda c: (counts.get(c, 0),
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
        aggregated.append({
            "symbol": sym,
            "main_category": main_cat,
            "consistency_level": level,
            "rounds_total": n_rounds,
            "universe_appearances": universe_count[sym],
            "black_count": counts.get("black_swan", 0),
            "red_count": counts.get("red_swan", 0),
            "skip_count": counts.get("skip", 0),
            "avg_confidence": round(avg_conf, 3),
            "catalyst": last_catalyst[sym][-1] if last_catalyst[sym] else None,
            "data_signal": last_signal[sym][-1] if last_signal[sym] else None,
            "risk_note": last_risk[sym][-1] if last_risk[sym] else None,
            "triggers": sorted(triggers_seen[sym]),
            "universe_data": last_universe_data.get(sym),
        })

    return {
        "summary_zh": summaries[-1] if summaries else "",
        "aggregated": aggregated,
    }


# ------------------ 单轮 ------------------
def _run_one_round(conn) -> Optional[dict]:
    with conn.cursor() as cur:
        gainers, losers = _fetch_movers_24h(cur, TOP_MOVER)
        fund_pos, fund_neg = _fetch_extreme_funding(cur, TOP_FUNDING)
    universe = _merge_universe(gainers, losers, fund_pos, fund_neg)
    logger.info(
        f"swan round universe_size={len(universe)} "
        f"(g={len(gainers)} l={len(losers)} fp={len(fund_pos)} fn={len(fund_neg)})"
    )
    if not universe:
        return None
    out = _call_gemini(universe)
    if not out:
        return None
    verdicts = out.get("verdicts") or []
    by_symbol = {}
    for v in verdicts:
        sym = str(v.get("symbol", "")).upper().strip()
        if sym:
            by_symbol[sym] = v
    return {
        "verdicts": by_symbol,
        "summary_zh": out.get("summary_zh", ""),
        "universe": {s: v for s, v in universe.items()},
    }


# ------------------ 入库 ------------------
def _persist(conn, asof_utc: datetime, rounds_done: int, universe_total: int,
             summary_zh: str, elapsed_s: float, status: str,
             error_msg: Optional[str], triggered_by: str,
             aggregated: list) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO gemini_swan_runs
              (asof_utc, model, rounds, universe_size, summary_zh,
               elapsed_s, status, error_msg, triggered_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (asof_utc, GEMINI_MODEL, rounds_done, universe_total,
             summary_zh, elapsed_s, status, error_msg, triggered_by),
        )
        run_id = cur.lastrowid

        if aggregated:
            rows = []
            for a in aggregated:
                rows.append((
                    run_id,
                    a["symbol"],
                    a["main_category"],
                    a["consistency_level"],
                    a["avg_confidence"],
                    a["rounds_total"],
                    a["universe_appearances"],
                    a["black_count"],
                    a["red_count"],
                    a["skip_count"],
                    a.get("catalyst"),
                    (a.get("data_signal") or "")[:255] or None,
                    (a.get("risk_note") or "")[:255] or None,
                    json.dumps(a.get("triggers") or [], ensure_ascii=False),
                    json.dumps(a.get("universe_data") or {}, ensure_ascii=False, default=str),
                ))
            cur.executemany(
                """
                INSERT INTO gemini_swan_verdicts
                  (run_id, symbol, main_category, consistency_level, avg_confidence,
                   rounds_total, universe_appearances, black_count, red_count, skip_count,
                   catalyst, data_signal, risk_note, triggers, universe_data)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                rows,
            )
    conn.commit()
    return run_id


# ------------------ 主入口 ------------------
def run_swan_round(force_rounds: Optional[int] = None,
                   triggered_by: str = "scheduler") -> Optional[int]:
    """跑一次 swan: N 轮 -> 聚合 -> 落库. 返回 run_id (失败返回 None)."""
    cfg = _load_remote_db_cfg()

    # 1. 读开关 + 轮数
    rounds_n = DEFAULT_ROUNDS
    enabled = True
    try:
        with pymysql.connect(**cfg) as probe:
            with probe.cursor() as cur:
                if _read_setting(cur, "gemini_swan_enabled", "1").strip() != "1":
                    enabled = False
                if force_rounds is None:
                    try:
                        rounds_n = int(_read_setting(cur, "gemini_swan_rounds",
                                                     str(DEFAULT_ROUNDS)))
                    except ValueError:
                        rounds_n = DEFAULT_ROUNDS
                    rounds_n = max(1, min(5, rounds_n))
                else:
                    rounds_n = max(1, min(5, force_rounds))
    except Exception as e:
        logger.error(f"swan worker 读 system_settings 失败: {e}")
        return None

    if not enabled:
        logger.info("swan worker 跳过: gemini_swan_enabled=0")
        return None

    asof = datetime.now(timezone.utc).replace(tzinfo=None)
    t_start = time.time()
    rounds: list = []
    error_msg: Optional[str] = None

    # 2. 跑 N 轮
    try:
        for i in range(rounds_n):
            logger.info(f"swan round {i+1}/{rounds_n} start (triggered_by={triggered_by})")
            t_round = time.time()
            try:
                with pymysql.connect(**cfg) as round_conn:
                    r = _run_one_round(round_conn)
                rounds.append(r)
            except Exception as e:
                logger.error(f"swan round {i+1} 异常: {e}", exc_info=True)
                rounds.append(None)
            if i < rounds_n - 1:
                wait = max(0, ROUND_INTERVAL_S - (time.time() - t_round))
                if wait > 0:
                    time.sleep(wait)
    except Exception as e:
        error_msg = str(e)
        logger.error(f"swan worker 跑轮异常: {e}", exc_info=True)

    # 3. 聚合
    valid = [r for r in rounds if r is not None]
    if not valid:
        status = "failed"
        agg_payload = {"summary_zh": "", "aggregated": []}
        error_msg = error_msg or "all rounds failed (universe empty or Gemini error)"
    elif len(valid) < rounds_n:
        status = "partial"
        agg_payload = _aggregate(rounds, rounds_n)
    else:
        status = "success"
        agg_payload = _aggregate(rounds, rounds_n)

    universe_total = len(set().union(*[set(r["universe"].keys()) for r in valid])) if valid else 0
    elapsed = round(time.time() - t_start, 2)

    # 4. 落库
    try:
        with pymysql.connect(**cfg) as conn:
            run_id = _persist(
                conn, asof, rounds_n, universe_total,
                agg_payload["summary_zh"], elapsed, status, error_msg,
                triggered_by, agg_payload["aggregated"],
            )
        logger.info(
            f"swan worker done run_id={run_id} status={status} "
            f"rounds={rounds_n} valid={len(valid)} symbols={len(agg_payload['aggregated'])} "
            f"elapsed={elapsed}s"
        )
        return run_id
    except Exception as e:
        logger.error(f"swan worker 落库失败: {e}", exc_info=True)
        return None


if __name__ == "__main__":
    import sys
    if sys.stdout.encoding != "utf-8":
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    rid = run_swan_round(triggered_by="manual")
    print(f"run_id={rid}")
