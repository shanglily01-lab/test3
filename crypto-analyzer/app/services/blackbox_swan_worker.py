"""
黑盒红黑天鹅探索 worker (本地 only, 不调远程 dimesion).

main.py lifespan 三个挂点:
  - 每 8h     触发 run_blackbox_swan_round() : 跑黑盒 + 落 verdict + (可选) 开 paper 单
  - 每 5min   触发 check_paper_closes()       : 扫 OPEN 仓位, SL/TP/hold 到期自动平仓
  - 每天 01:30 触发 run_hit_rate_check()      : 7d 回算 STRONG verdict 真实涨跌

四张本地表 (都在 binance-data 库, 跟远程 dimesion 完全隔离):
  - blackbox_swan_runs           每次跑的元数据
  - blackbox_swan_verdicts       phase2 verdict + filtered_out 名单
  - blackbox_swan_hit_rate       7d 回算结果
  - blackbox_swan_paper_trades   本地模拟成交 + 平仓 (不调 main 的 paper engine)

为什么不调 main 的 /api/futures/open:
  用户偏好 2026-05-18: 以后所有数据库只用本地, 远程 dimesion 不再写. main 的 paper engine
  写的是远程 futures_orders, 跟此偏好冲突. 因此 paper 改成本地模拟表 +
  本地 SL/TP/hold loop, 完全脱离 main.py.

开关 (.env, 改完下次触发生效):
  BLACKBOX_SWAN_ENABLED : 0 默认关 (worker 早返回不调 Gemini)
  BLACKBOX_PAPER_ENABLED: 0 默认关 (worker 跑完不开模拟仓)
  BLACKBOX_PAPER_REQUIRE_ALIGNED: 1 默认仅 aligned 才开 (排除 conflicting 接刀)
  BLACKBOX_PAPER_MARGIN_USDT / _LEVERAGE / _SL_PCT / _TP_PCT / _HOLD_MIN: 风控参数

实现复用:
  黑盒探索的纯函数 (fetch_binance_universe / run_phase1 / filter_and_enrich /
  run_phase2 / persist_to_mysql) 在 scripts/diag/diag_blackbox_swan_now.py
  worker import 复用, 不重复维护.
"""
from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from loguru import logger

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DIAG_DIR = PROJECT_ROOT / "scripts" / "diag"
if str(DIAG_DIR) not in sys.path:
    sys.path.insert(0, str(DIAG_DIR))

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass

import pymysql


# ------------------ 本地 MySQL 配置 (与 diag 脚本同源) ------------------
def _local_db_cfg() -> dict:
    return {
        "host": os.getenv("DB_HOST", "localhost"),
        "port": int(os.getenv("DB_PORT", "3306")),
        "user": os.getenv("DB_USER", "root"),
        "password": os.getenv("DB_PASSWORD", ""),
        "database": os.getenv("DB_NAME", "binance-data"),
        "charset": "utf8mb4",
        "cursorclass": pymysql.cursors.DictCursor,
    }


def _enabled() -> bool:
    return os.getenv("BLACKBOX_SWAN_ENABLED", "0").strip() == "1"


# ------------------ paper 模拟下单参数 (本地表, 不依赖远程 paper engine) ------------------
PAPER_MARGIN_USDT = float(os.getenv("BLACKBOX_PAPER_MARGIN_USDT", "500"))
PAPER_LEVERAGE = int(os.getenv("BLACKBOX_PAPER_LEVERAGE", "5"))
PAPER_SL_PCT = float(os.getenv("BLACKBOX_PAPER_SL_PCT", "0.03"))   # 3%
PAPER_TP_PCT = float(os.getenv("BLACKBOX_PAPER_TP_PCT", "0.08"))   # 8%
PAPER_HOLD_MIN = int(os.getenv("BLACKBOX_PAPER_HOLD_MIN", "360"))  # 6h
PAPER_REQUIRE_ALIGNED = os.getenv("BLACKBOX_PAPER_REQUIRE_ALIGNED", "1").strip() == "1"


def _paper_enabled() -> bool:
    return os.getenv("BLACKBOX_PAPER_ENABLED", "0").strip() == "1"


# ------------------ hit_rate 表 DDL (worker 自动建表) ------------------
HIT_RATE_SCHEMA = """
CREATE TABLE IF NOT EXISTS `blackbox_swan_hit_rate` (
  `id`              INT NOT NULL AUTO_INCREMENT,
  `verdict_id`      INT NOT NULL COMMENT 'FK -> blackbox_swan_verdicts.id',
  `run_id`          INT NOT NULL COMMENT 'FK -> blackbox_swan_runs.id (冗余, 方便查)',
  `symbol`          VARCHAR(30) NOT NULL,
  `main_category`   VARCHAR(20) NOT NULL COMMENT 'black_swan / red_swan',
  `consistency_level` VARCHAR(10) NOT NULL,
  `avg_confidence`  DECIMAL(4,3) NOT NULL,
  `verdict_at`      DATETIME NOT NULL COMMENT 'verdict 当时的 UTC 时间',
  `verdict_price`   DECIMAL(20,8) DEFAULT NULL COMMENT 'verdict 当时的 current_price',
  `check_at`        DATETIME NOT NULL COMMENT '本次回算时刻',
  `check_price`     DECIMAL(20,8) DEFAULT NULL COMMENT '回算时拉的最新价',
  `change_pct`      DECIMAL(8,3) DEFAULT NULL COMMENT '(check - verdict) / verdict * 100, %',
  `hit_or_miss`     VARCHAR(10) NOT NULL COMMENT 'hit / miss / unknown',
  `hit_threshold`   DECIMAL(5,2) NOT NULL COMMENT 'black/red 各自门槛 %, 默认 10.0',
  `lookback_days`   INT NOT NULL COMMENT '回算了 N 天前的 verdict',
  `created_at`      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_verdict_lookback` (`verdict_id`, `lookback_days`),
  KEY `idx_symbol_check` (`symbol`, `check_at`),
  KEY `idx_run_hit` (`run_id`, `hit_or_miss`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='blackbox swan STRONG verdict hit rate (7d 回算)';
"""


def _ensure_hit_rate_table(conn: pymysql.connections.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(HIT_RATE_SCHEMA)
    conn.commit()


# ------------------ paper_trades 表 DDL ------------------
PAPER_TRADES_SCHEMA = """
CREATE TABLE IF NOT EXISTS `blackbox_swan_paper_trades` (
  `id`              INT NOT NULL AUTO_INCREMENT,
  `run_id`          INT NOT NULL,
  `verdict_id`      INT NOT NULL COMMENT 'FK -> blackbox_swan_verdicts.id (UNIQUE: 同 verdict 不重复开仓)',
  `symbol`          VARCHAR(30) NOT NULL,
  `direction`       VARCHAR(10) NOT NULL COMMENT 'LONG / SHORT',
  `main_category`   VARCHAR(20) NOT NULL COMMENT 'black_swan / red_swan',
  `consistency_level` VARCHAR(10) NOT NULL,
  `avg_confidence`  DECIMAL(4,3) NOT NULL,
  `entry_price`     DECIMAL(20,8) NOT NULL,
  `entry_at`        DATETIME NOT NULL,
  `margin_usdt`     DECIMAL(20,8) NOT NULL,
  `leverage`        INT NOT NULL,
  `quantity`        DECIMAL(20,8) NOT NULL,
  `stop_loss_price`   DECIMAL(20,8) NOT NULL,
  `take_profit_price` DECIMAL(20,8) NOT NULL,
  `max_hold_minutes`  INT NOT NULL,
  `expire_at`       DATETIME NOT NULL COMMENT 'entry_at + max_hold_minutes',
  `status`          VARCHAR(20) NOT NULL DEFAULT 'OPEN' COMMENT 'OPEN/CLOSED_SL/CLOSED_TP/CLOSED_HOLD/CLOSED_MANUAL',
  `close_price`     DECIMAL(20,8) DEFAULT NULL,
  `close_at`        DATETIME DEFAULT NULL,
  `pnl_usdt`        DECIMAL(20,8) DEFAULT NULL COMMENT '(close-entry)*qty * (+1 LONG / -1 SHORT)',
  `pnl_pct`         DECIMAL(8,3) DEFAULT NULL COMMENT 'pnl_usdt / margin_usdt * 100',
  `refined_catalyst` TEXT,
  `risk_note`       TEXT,
  `source`          VARCHAR(60) NOT NULL DEFAULT 'blackbox:bbx-swan',
  `created_at`      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_verdict` (`verdict_id`),
  KEY `idx_status` (`status`),
  KEY `idx_symbol_status` (`symbol`, `status`),
  KEY `idx_run` (`run_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='blackbox swan paper trades (本地模拟成交, 不调远程 paper engine)';
"""


def _ensure_paper_trades_table(conn: pymysql.connections.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(PAPER_TRADES_SCHEMA)
    conn.commit()


# ------------------ paper 模拟成交 (本地表, 不调远程) ------------------
def _submit_paper_trades(run_id: int, verdicts: list, active: dict,
                         verdict_ids_by_symbol: dict) -> int:
    """对 STRONG (+ 默认 aligned) verdict 在本地 blackbox_swan_paper_trades 表写模拟开仓.

    不调任何远程 paper engine HTTP API, 不动 strategy_bigmid.
    SL/TP/hold 到期由 _check_paper_closes (5min 一次, 挂 main lifespan) 自动平.

    UNIQUE(verdict_id) 防止 worker 重跑同 run_id 重复开仓.

    返回写入成功数.
    """
    if not _paper_enabled():
        return 0
    if not verdicts:
        return 0

    # 过滤合格 verdict
    candidates = []
    for v in verdicts:
        if v.get("consistency_level") != "STRONG":
            continue
        cat = v.get("main_category")
        if cat not in ("black_swan", "red_swan"):
            continue
        if PAPER_REQUIRE_ALIGNED and v.get("data_alignment") != "aligned":
            logger.info(
                f"blackbox paper: 跳过 {v.get('symbol')} cat={cat} "
                f"align={v.get('data_alignment')} (REQUIRE_ALIGNED=1)"
            )
            continue
        candidates.append(v)

    if not candidates:
        logger.info(f"blackbox paper: 合格候选 0 个 (STRONG+aligned, run_id={run_id})")
        return 0

    logger.info(
        f"blackbox paper: 准备开 {len(candidates)} 笔本地模拟单 "
        f"(margin={PAPER_MARGIN_USDT} lev={PAPER_LEVERAGE} "
        f"sl={PAPER_SL_PCT*100:.1f}% tp={PAPER_TP_PCT*100:.1f}% hold={PAPER_HOLD_MIN}min)"
    )

    entry_at = datetime.now(timezone.utc).replace(tzinfo=None)
    cfg = _local_db_cfg()
    rows = []
    skipped_no_price = 0
    for v in candidates:
        sym = v.get("symbol")
        if not sym:
            continue
        verdict_id = verdict_ids_by_symbol.get(sym)
        if not verdict_id:
            logger.warning(f"blackbox paper: {sym} 找不到 verdict_id, 跳过")
            continue
        u = active.get(sym) or {}
        entry_p = u.get("current_price")
        if not entry_p:
            skipped_no_price += 1
            continue
        try:
            entry_p = float(entry_p)
            qty = round(PAPER_MARGIN_USDT * PAPER_LEVERAGE / entry_p, 6)
        except (TypeError, ValueError, ZeroDivisionError):
            logger.warning(f"blackbox paper: {sym} qty 计算失败 entry_p={entry_p}, 跳过")
            continue

        cat = v["main_category"]
        direction = "SHORT" if cat == "black_swan" else "LONG"
        if direction == "LONG":
            sl = round(entry_p * (1 - PAPER_SL_PCT), 8)
            tp = round(entry_p * (1 + PAPER_TP_PCT), 8)
        else:
            sl = round(entry_p * (1 + PAPER_SL_PCT), 8)
            tp = round(entry_p * (1 - PAPER_TP_PCT), 8)

        from datetime import timedelta
        expire_at = entry_at + timedelta(minutes=PAPER_HOLD_MIN)
        source = f"blackbox:bbx-swan-{'black' if cat == 'black_swan' else 'red'}"
        rows.append((
            run_id, verdict_id, sym, direction, cat,
            v.get("consistency_level"), v.get("avg_confidence"),
            entry_p, entry_at, PAPER_MARGIN_USDT, PAPER_LEVERAGE, qty,
            sl, tp, PAPER_HOLD_MIN, expire_at,
            v.get("refined_catalyst"), v.get("risk_note"), source,
        ))

    if not rows:
        logger.info(f"blackbox paper: 全部 candidates 被价格/verdict_id 过滤 skipped_no_price={skipped_no_price}")
        return 0

    inserted = 0
    try:
        with pymysql.connect(**cfg) as conn:
            _ensure_paper_trades_table(conn)
            with conn.cursor() as cur:
                for row in rows:
                    try:
                        cur.execute(
                            """
                            INSERT INTO blackbox_swan_paper_trades
                              (run_id, verdict_id, symbol, direction, main_category,
                               consistency_level, avg_confidence,
                               entry_price, entry_at, margin_usdt, leverage, quantity,
                               stop_loss_price, take_profit_price, max_hold_minutes, expire_at,
                               refined_catalyst, risk_note, source)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                    %s, %s, %s, %s, %s, %s, %s)
                            """,
                            row,
                        )
                        inserted += 1
                    except pymysql.err.IntegrityError as ie:
                        # UNIQUE(verdict_id) 重复 - worker 重跑同 verdict, 跳过
                        if ie.args and ie.args[0] == 1062:
                            logger.info(f"blackbox paper: verdict_id={row[1]} 已开过, 跳过")
                        else:
                            raise
            conn.commit()
    except Exception as e:
        logger.error(f"blackbox paper: 本地写入异常: {e}", exc_info=True)
        return 0

    logger.info(f"blackbox paper [run_id={run_id}] 本地开仓 {inserted}/{len(rows)} 笔")
    return inserted


# ------------------ paper 平仓 loop (5min 一次, 挂 main lifespan) ------------------
def check_paper_closes(triggered_by: str = "scheduler") -> int:
    """扫描所有 status='OPEN' 的 paper trade, 判断是否触发 SL/TP/hold 到期, 写平仓.

    不依赖远程, 当前价从 Binance fapi 一次拉. 返回本次平仓笔数.
    """
    cfg = _local_db_cfg()
    try:
        with pymysql.connect(**cfg) as conn:
            _ensure_paper_trades_table(conn)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, symbol, direction, entry_price, quantity, margin_usdt,
                           stop_loss_price, take_profit_price, expire_at
                    FROM blackbox_swan_paper_trades
                    WHERE status = 'OPEN'
                    """
                )
                opens = cur.fetchall()
    except Exception as e:
        logger.error(f"blackbox paper close: 读 OPEN 仓位失败: {e}")
        return 0

    if not opens:
        return 0

    # 拉一次 Binance fapi 当前价 (复用 diag 脚本)
    try:
        import diag_blackbox_swan_now as diag  # type: ignore
        active = diag.fetch_binance_universe()
    except Exception as e:
        logger.error(f"blackbox paper close: Binance fapi 拉取失败: {e}")
        return 0

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    closes = []
    for o in opens:
        sym = o["symbol"]
        u = active.get(sym) or {}
        cur_p = u.get("current_price")
        direction = o["direction"]
        entry_p = float(o["entry_price"])
        sl = float(o["stop_loss_price"])
        tp = float(o["take_profit_price"])

        status = None
        close_p = None
        # 1. 优先 SL/TP (即便到期, 也按触发价平)
        if cur_p is not None:
            cur_p = float(cur_p)
            if direction == "LONG":
                if cur_p <= sl:
                    status, close_p = "CLOSED_SL", sl
                elif cur_p >= tp:
                    status, close_p = "CLOSED_TP", tp
            else:  # SHORT
                if cur_p >= sl:
                    status, close_p = "CLOSED_SL", sl
                elif cur_p <= tp:
                    status, close_p = "CLOSED_TP", tp
        # 2. 否则看到期
        if status is None and o["expire_at"] and now >= o["expire_at"]:
            status, close_p = "CLOSED_HOLD", (cur_p if cur_p else entry_p)

        if status is None:
            continue

        qty = float(o["quantity"])
        margin = float(o["margin_usdt"])
        sign = 1.0 if direction == "LONG" else -1.0
        pnl_usdt = (close_p - entry_p) * qty * sign
        pnl_pct = (pnl_usdt / margin * 100.0) if margin else 0.0

        closes.append((
            status, close_p, now, round(pnl_usdt, 8), round(pnl_pct, 3), o["id"],
        ))

    if not closes:
        return 0

    try:
        with pymysql.connect(**cfg) as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    UPDATE blackbox_swan_paper_trades
                    SET status=%s, close_price=%s, close_at=%s, pnl_usdt=%s, pnl_pct=%s
                    WHERE id=%s AND status='OPEN'
                    """,
                    closes,
                )
            conn.commit()
    except Exception as e:
        logger.error(f"blackbox paper close: UPDATE 失败: {e}")
        return 0

    logger.info(f"blackbox paper close: 平仓 {len(closes)} 笔 (triggered_by={triggered_by})")
    for c in closes:
        logger.info(f"  -> trade_id={c[5]} status={c[0]} close={c[1]} pnl_usdt={c[3]} pnl_pct={c[4]}%")
    return len(closes)


# ------------------ 主入口 1: 跑一次黑盒 ------------------
def run_blackbox_swan_round(triggered_by: str = "scheduler") -> Optional[int]:
    """跑一次黑盒探索: phase1 (2 轮无数据提名) -> phase2 (2 轮数据收敛) -> 写本地 MySQL.

    返回 run_id (失败/关闭返回 None).
    """
    if not _enabled():
        logger.info("blackbox swan 跳过: BLACKBOX_SWAN_ENABLED=0 (默认关闭)")
        return None

    # 延迟 import diag 脚本里的纯函数 (避免主进程启动时就加载)
    try:
        import diag_blackbox_swan_now as diag  # type: ignore
    except ImportError as e:
        logger.error(f"blackbox swan: import diag_blackbox_swan_now 失败: {e}")
        return None

    cfg = _local_db_cfg()
    t_start = time.time()
    rounds_p1 = 2
    rounds_p2 = 2
    top_n = 20

    try:
        # Gemini client
        client, gcfg = diag._gemini_client()
        if not client:
            logger.error("blackbox swan: Gemini client 初始化失败")
            return None

        # 阶段一
        logger.info(f"blackbox swan: phase1 rounds={rounds_p1} top_n={top_n} (triggered_by={triggered_by})")
        p1 = diag.run_phase1(client, gcfg, rounds_p1, top_n)
        if not p1["proposals"]:
            logger.error("blackbox swan: phase1 无任何 proposal, 终止")
            return None

        # Binance fapi 一次采集 + 白名单过滤
        active = diag.fetch_binance_universe()
        enriched, filtered_out = diag.filter_and_enrich(active, p1["proposals"])
        logger.info(
            f"blackbox swan: phase1->phase2 enriched={len(enriched)} "
            f"filtered_out={len(filtered_out)}"
        )

        # 阶段二
        p2_result = None
        if enriched:
            logger.info(f"blackbox swan: phase2 rounds={rounds_p2}")
            p2_result = diag.run_phase2(client, gcfg, rounds_p2, enriched)

        # 落本地 MySQL
        elapsed = round(time.time() - t_start, 2)
        status = "success" if p2_result else "partial"
        asof_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
        run_id = diag.persist_to_mysql(
            cfg=cfg,
            asof_iso=asof_iso,
            model=diag.GEMINI_MODEL,
            rounds_p1=rounds_p1,
            rounds_p2=rounds_p2,
            top_n=top_n,
            dry_run=False,
            proposals=p1["proposals"],
            filtered_out=filtered_out,
            enriched=enriched,
            p2_result=p2_result,
            elapsed_s=elapsed,
            status=status,
            error_msg=None,
        )
        n_verdicts = len(p2_result.get("aggregated") or []) if p2_result else 0
        logger.info(
            f"blackbox swan 完成 run_id={run_id} status={status} "
            f"elapsed={elapsed}s proposed={len(p1['proposals'])} "
            f"filtered={len(filtered_out)} verdicts={n_verdicts}"
        )

        # paper 模拟成交 (本地表, BLACKBOX_PAPER_ENABLED=1 才生效, 默认关)
        if p2_result and run_id and _paper_enabled():
            try:
                # 拿 verdict_id 映射 (UNIQUE(verdict_id) 防重复开)
                verdict_ids_by_symbol = {}
                with pymysql.connect(**cfg) as vconn:
                    with vconn.cursor() as vcur:
                        vcur.execute(
                            """
                            SELECT id, symbol FROM blackbox_swan_verdicts
                            WHERE run_id = %s AND filtered_out = 0
                            """,
                            (run_id,),
                        )
                        for row in vcur.fetchall():
                            verdict_ids_by_symbol[row["symbol"]] = row["id"]

                _submit_paper_trades(
                    run_id=run_id,
                    verdicts=p2_result.get("aggregated") or [],
                    active=active,
                    verdict_ids_by_symbol=verdict_ids_by_symbol,
                )
            except Exception as e:
                logger.error(f"blackbox paper 模拟成交异常: {e}", exc_info=True)

        return run_id

    except Exception as e:
        logger.error(f"blackbox swan 跑轮异常: {e}", exc_info=True)
        return None


# ------------------ 主入口 2: hit rate 回算 ------------------
def run_hit_rate_check(
    triggered_by: str = "scheduler",
    lookback_days: int = 7,
    hit_threshold_pct: float = 10.0,
) -> int:
    """回算 N 天前的 STRONG verdict 准确率, 写 blackbox_swan_hit_rate 表.

    判定规则 (STRONG verdict only, skip 类不算):
      - black_swan + N天后跌幅 >= hit_threshold_pct: hit
      - red_swan + N天后涨幅 >= hit_threshold_pct: hit
      - 其他: miss
      - 拿不到当前价: unknown

    返回本次写入的行数.
    """
    # 不检查 enabled 开关 — hit rate 本身只是只读 + 落库, 不调 Gemini, 即便 worker 关了也能回算
    cfg = _local_db_cfg()

    try:
        import diag_blackbox_swan_now as diag  # type: ignore
    except ImportError as e:
        logger.error(f"blackbox hit rate: import diag 失败: {e}")
        return 0

    try:
        # 拉当前 Binance fapi 实时价 (一次拉全市场)
        active = diag.fetch_binance_universe()
    except Exception as e:
        logger.error(f"blackbox hit rate: Binance fapi 拉取失败: {e}", exc_info=True)
        return 0

    check_at = datetime.now(timezone.utc).replace(tzinfo=None)
    inserted = 0
    try:
        with pymysql.connect(**cfg) as conn:
            _ensure_hit_rate_table(conn)
            with conn.cursor() as cur:
                # 取 lookback_days 前 ±6h 窗口内的 STRONG verdict (剧情: 每天 01:30 跑, 抓 7 天前 ±6h)
                cur.execute(
                    """
                    SELECT v.id AS verdict_id, v.run_id, v.symbol, v.main_category,
                           v.consistency_level, v.avg_confidence, v.universe_data,
                           r.asof_utc AS verdict_at
                    FROM blackbox_swan_verdicts v
                    JOIN blackbox_swan_runs r ON r.id = v.run_id
                    WHERE v.filtered_out = 0
                      AND v.consistency_level = 'STRONG'
                      AND v.main_category IN ('black_swan', 'red_swan')
                      AND v.id NOT IN (
                          SELECT verdict_id FROM blackbox_swan_hit_rate
                          WHERE lookback_days = %s
                      )
                      AND r.created_at BETWEEN
                          DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s DAY) - INTERVAL 6 HOUR
                          AND DATE_SUB(UTC_TIMESTAMP(), INTERVAL %s DAY) + INTERVAL 6 HOUR
                    """,
                    (lookback_days, lookback_days, lookback_days),
                )
                rows = cur.fetchall()
                logger.info(f"blackbox hit rate: 找到 {len(rows)} 个 STRONG verdict 待回算 (lookback={lookback_days}d)")

                payload = []
                for v in rows:
                    # 从 universe_data JSON 拿 verdict 当时的价格
                    verdict_price = None
                    try:
                        import json
                        ud = json.loads(v["universe_data"]) if v["universe_data"] else {}
                        verdict_price = ud.get("current_price")
                        if verdict_price is not None:
                            verdict_price = float(verdict_price)
                    except (TypeError, ValueError, KeyError):
                        verdict_price = None

                    # 当前价从 active universe 拿 (考虑 alias_of)
                    sym = v["symbol"]
                    current_price = None
                    if sym in active:
                        current_price = active[sym].get("current_price")

                    if verdict_price and current_price:
                        change_pct = (current_price - verdict_price) / verdict_price * 100.0
                    else:
                        change_pct = None

                    # 判定 hit_or_miss
                    if change_pct is None:
                        result = "unknown"
                    elif v["main_category"] == "black_swan":
                        result = "hit" if change_pct <= -hit_threshold_pct else "miss"
                    elif v["main_category"] == "red_swan":
                        result = "hit" if change_pct >= hit_threshold_pct else "miss"
                    else:
                        result = "unknown"

                    payload.append((
                        v["verdict_id"], v["run_id"], sym,
                        v["main_category"], v["consistency_level"], v["avg_confidence"],
                        v["verdict_at"], verdict_price,
                        check_at, current_price,
                        change_pct if change_pct is not None else None,
                        result, hit_threshold_pct, lookback_days,
                    ))

                if payload:
                    cur.executemany(
                        """
                        INSERT INTO blackbox_swan_hit_rate
                          (verdict_id, run_id, symbol, main_category, consistency_level,
                           avg_confidence, verdict_at, verdict_price, check_at, check_price,
                           change_pct, hit_or_miss, hit_threshold, lookback_days)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        payload,
                    )
                    inserted = len(payload)
            conn.commit()

        # 打印当前累计 hit rate
        with pymysql.connect(**cfg) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT main_category,
                           SUM(hit_or_miss='hit') AS hits,
                           SUM(hit_or_miss='miss') AS misses,
                           SUM(hit_or_miss='unknown') AS unknowns,
                           COUNT(*) AS total
                    FROM blackbox_swan_hit_rate
                    WHERE lookback_days = %s
                    GROUP BY main_category
                    """,
                    (lookback_days,),
                )
                for r in cur.fetchall():
                    cat = r["main_category"]
                    h, m, u, t = r["hits"], r["misses"], r["unknowns"], r["total"]
                    rate = (h / max(1, h + m)) * 100.0
                    logger.info(
                        f"blackbox hit rate [{cat}]: hits={h} misses={m} unknown={u} "
                        f"total={t} hit_rate={rate:.1f}% (lookback={lookback_days}d, threshold={hit_threshold_pct}%)"
                    )

    except Exception as e:
        logger.error(f"blackbox hit rate 回算异常: {e}", exc_info=True)
        return 0

    logger.info(f"blackbox hit rate 完成: 本次新增 {inserted} 行 (triggered_by={triggered_by})")
    return inserted


# ------------------ CLI ------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="黑盒红黑天鹅 worker (手动触发)")
    parser.add_argument(
        "action",
        choices=["run", "hit_rate", "check_closes"],
        help="run=跑一次黑盒探索(含 paper); hit_rate=回算; check_closes=扫描 OPEN 仓位 SL/TP/hold",
    )
    parser.add_argument("--lookback-days", type=int, default=7)
    parser.add_argument("--threshold", type=float, default=10.0)
    args = parser.parse_args()

    if args.action == "run":
        rid = run_blackbox_swan_round(triggered_by="manual")
        print(f"run_id={rid}")
    elif args.action == "hit_rate":
        n = run_hit_rate_check(triggered_by="manual",
                               lookback_days=args.lookback_days,
                               hit_threshold_pct=args.threshold)
        print(f"inserted={n}")
    else:
        n = check_paper_closes(triggered_by="manual")
        print(f"closed={n}")
