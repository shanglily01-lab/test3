"""
WhaleDataCollector - 庄家数据采集 (funding rate / OI / LSR) 类库
====================================================================
2026-05-15 从根目录 whale_data_collector.py 抽出, 由 fast_collector_service.py 合并进程调用.
代码风格与 SmartFuturesCollector 对齐 (类封装 + sync API).

5 个采集任务:
  1. collect_funding_rates  - /fapi/v1/premiumIndex 全市场, 1 次请求, weight ~41
  2. collect_24h_stats      - /fapi/v1/ticker/24hr 全市场, 写 price_stats_24h
                              + 返回 top N 品种列表 (按 quoteVolume)
  3. collect_oi_history     - /futures/data/openInterestHist 逐品种, 1h period x 12 根
  4. collect_ls_ratio       - /futures/data/globalLongShortAccountRatio 逐品种, 1h x 6 根
  5. cleanup_old_data       - DELETE 旧数据 (OI/LSR > 7 天, funding > 30 天)

IP 封禁解析:
  _record_binance_ban_from_body 解析 -1003 / 418 响应里的 "banned until <ms>",
  _binance_ban_remaining_s() 在封禁窗口内返回剩余秒数, 调用方据此跳过本周期.
  共享一个实例级状态 (self._ban_until_ms), 不污染全局.

频率参考 (调用方主循环管理):
  funding + 24h: 每 10 分钟 (FUNDING_INTERVAL = 600s)
  OI + LSR:      每 120 分钟 (OI_LS_INTERVAL = 7200s)
  cleanup:       每 60 分钟

调用方需提供:
  db_config: dict (host / port / user / password / database)
"""
import os
import re
import time
import logging
from datetime import datetime, timezone
from typing import Optional

import pymysql
import requests

log = logging.getLogger(__name__)


# ── 常量 ─────────────────────────────────────────────────────────────
FAPI = "https://fapi.binance.com"
REQUEST_TIMEOUT = 10
INTER_REQ_SLEEP = 0.5    # 逐品种请求间隔 (秒)
TOP_N_BY_VOLUME = 100    # OI/LSR 覆盖前 N 个品种 (by quoteVolume)


class WhaleDataCollector:
    """同步采集器, 由 fast_collector_service 用 asyncio.to_thread 调用."""

    def __init__(self, db_config: Optional[dict] = None):
        # db_config: 显式传入 (优先); 否则回退到 os.getenv (兼容旧风格)
        self.db_config = db_config or {
            "host":     os.getenv("DB_HOST"),
            "port":     int(os.getenv("DB_PORT", 3306)),
            "user":     os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD", ""),
            "db":       os.getenv("DB_NAME"),
        }
        # IP 封禁状态 (实例级)
        self._ban_until_ms: int = 0
        self._ban_hit_this_cycle: bool = False

    # ── DB ──────────────────────────────────────────────────────────
    def _get_db(self):
        return pymysql.connect(
            host=self.db_config.get("host"),
            port=int(self.db_config.get("port", 3306)),
            user=self.db_config.get("user"),
            password=self.db_config.get("password", ""),
            db=self.db_config.get("db") or self.db_config.get("database"),
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
        )

    # ── IP 封禁解析 ─────────────────────────────────────────────────
    def ban_remaining_s(self) -> float:
        """距离 Binance 解禁还有多少秒. 未封禁返回 0."""
        if not self._ban_until_ms:
            return 0.0
        now_ms = int(time.time() * 1000)
        if now_ms >= self._ban_until_ms:
            return 0.0
        return max(0.0, (self._ban_until_ms - now_ms) / 1000.0)

    def _record_ban(self, status: int, body: str) -> None:
        if status != 418 and "-1003" not in body and "Way too many requests" not in body:
            return
        self._ban_hit_this_cycle = True
        m = re.search(r"banned until (\d+)", body)
        if m:
            self._ban_until_ms = int(m.group(1))
            until_utc = datetime.fromtimestamp(self._ban_until_ms / 1000, tz=timezone.utc)
            log.error(
                "Binance IP 限速/封禁 (-1003), 请勿在封禁期内继续请求 REST; "
                "解禁时间(UTC): %s (约 %.0f 分钟后).",
                until_utc.strftime("%Y-%m-%d %H:%M:%S"),
                self.ban_remaining_s() / 60.0,
            )
        else:
            self._ban_until_ms = int((time.time() + 3600) * 1000)
            log.error("Binance 限速 (-1003), 未解析到解禁时间, 按 1 小时 backoff")

    def _get(self, url: str, params: dict = None):
        rem = self.ban_remaining_s()
        if rem > 0:
            log.warning("仍在 Binance 封禁窗口内 (剩余约 %.0f 秒), 跳过: %s", rem, url)
            return None
        try:
            r = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                self._ban_until_ms = 0
                return r.json()
            body = r.text[:500]
            log.warning("HTTP %d  %s  %s", r.status_code, url, body)
            self._record_ban(r.status_code, body)
        except Exception as e:
            log.warning("请求失败: %s  %s", url, e)
        return None

    # ── 工具 ────────────────────────────────────────────────────────
    @staticmethod
    def _binance_sym(sym: str) -> str:
        """BTC/USDT -> BTCUSDT"""
        return sym.replace("/", "")

    @staticmethod
    def _std_sym(binance_sym: str) -> str:
        """BTCUSDT -> BTC/USDT (假定 USDT 结尾)"""
        if binance_sym.endswith("USDT"):
            base = binance_sym[:-4]
            return f"{base}/USDT"
        return binance_sym

    # ── 1. 资金费率 ─────────────────────────────────────────────────
    def collect_funding_rates(self, conn) -> int:
        """premiumIndex 全市场单次请求. 返回新增条数."""
        data = self._get(f"{FAPI}/fapi/v1/premiumIndex")
        if not data:
            if self.ban_remaining_s() > 0:
                log.error("资金费率批量接口无返回 (当前 Binance 封禁中)")
            else:
                log.error("资金费率批量接口无返回")
            return 0

        rows = []
        for d in data:
            sym = d.get("symbol", "")
            if not sym.endswith("USDT"):
                continue
            funding_time = int(d.get("time", 0))
            if funding_time == 0:
                continue
            rows.append((
                self._std_sym(sym),
                "binance",
                float(d.get("lastFundingRate", 0)),
                funding_time,
                datetime.fromtimestamp(funding_time / 1000),
                float(d.get("markPrice", 0)),
                float(d.get("indexPrice", 0)),
                int(d.get("nextFundingTime", 0)),
            ))
        if not rows:
            return 0

        cur = conn.cursor()
        # funding_rate_data 没有 unique key, 拿 max(funding_time) 防重
        cur.execute(
            "SELECT symbol, MAX(funding_time) as mt FROM funding_rate_data GROUP BY symbol"
        )
        existing = {r["symbol"]: r["mt"] for r in cur.fetchall()}
        insert_rows = [r for r in rows if existing.get(r[0], 0) < r[3]]
        if insert_rows:
            cur.executemany(
                """
                INSERT INTO funding_rate_data
                    (symbol, exchange, funding_rate, funding_time, timestamp,
                     mark_price, index_price, next_funding_time)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                insert_rows,
            )
            conn.commit()
        log.info("资金费率更新: %d 个品种 (新增 %d 条)", len(rows), len(insert_rows))
        return len(insert_rows)

    # ── 2. 24h 统计 ─────────────────────────────────────────────────
    def collect_24h_stats(self, conn) -> list:
        """ticker/24hr 全市场, 写 price_stats_24h. 返回 top N by quoteVolume."""
        data = self._get(f"{FAPI}/fapi/v1/ticker/24hr")
        if not data:
            return []
        usdt = [d for d in data if d.get("symbol", "").endswith("USDT")]
        usdt.sort(key=lambda x: float(x.get("quoteVolume", 0)), reverse=True)

        rows = []
        for d in usdt:
            sym = self._std_sym(d["symbol"])
            curr = float(d.get("lastPrice", 0))
            p24  = float(d.get("openPrice", 0))
            chg  = float(d.get("priceChangePercent", 0))
            rows.append((
                sym, curr, p24, chg, abs(curr - p24),
                float(d.get("highPrice", 0)),
                float(d.get("lowPrice", 0)),
                min(float(d.get("volume", 0)), 9.99e11),
                min(float(d.get("quoteVolume", 0)), 9.99e15),
                int(d.get("count", 0)),
                datetime.now(),
            ))
        if rows:
            cur = conn.cursor()
            cur.executemany(
                """
                INSERT INTO price_stats_24h
                    (symbol, current_price, price_24h_ago,
                     change_24h, change_24h_abs, high_24h, low_24h,
                     volume_24h, quote_volume_24h, trades_count_24h, updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                    current_price   = VALUES(current_price),
                    change_24h      = VALUES(change_24h),
                    change_24h_abs  = VALUES(change_24h_abs),
                    high_24h        = VALUES(high_24h),
                    low_24h         = VALUES(low_24h),
                    volume_24h      = VALUES(volume_24h),
                    quote_volume_24h= VALUES(quote_volume_24h),
                    trades_count_24h= VALUES(trades_count_24h),
                    updated_at      = VALUES(updated_at)
                """,
                rows,
            )
            conn.commit()
            log.info("24h统计更新: %d 个品种", len(rows))
        return [self._std_sym(d["symbol"]) for d in usdt[:TOP_N_BY_VOLUME]]

    # ── 3. OI 历史 ──────────────────────────────────────────────────
    def collect_oi_history(self, conn, symbols: list) -> None:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT symbol, MAX(timestamp) as mt FROM futures_open_interest
            WHERE exchange = 'binance' GROUP BY symbol
            """
        )
        latest = {r["symbol"]: r["mt"] for r in cur.fetchall()}

        inserted_total = 0
        for sym in symbols:
            bsym = self._binance_sym(sym)
            data = self._get(
                f"{FAPI}/futures/data/openInterestHist",
                {"symbol": bsym, "period": "1h", "limit": 12},
            )
            time.sleep(INTER_REQ_SLEEP)
            if not data or not isinstance(data, list):
                continue
            last_dt = latest.get(sym)
            rows = []
            for d in data:
                ts = datetime.fromtimestamp(int(d["timestamp"]) / 1000)
                if last_dt and ts <= last_dt:
                    continue
                oi_val = float(d.get("sumOpenInterestValue", 0))
                oi_qty = float(d.get("sumOpenInterest", 0))
                rows.append((sym, "binance", oi_qty, oi_val, ts))
            if rows:
                cur.executemany(
                    """
                    INSERT INTO futures_open_interest
                        (symbol, exchange, open_interest, open_interest_value, timestamp)
                    VALUES (%s,%s,%s,%s,%s)
                    """,
                    rows,
                )
                inserted_total += len(rows)
        conn.commit()
        log.info("OI历史更新: %d 条 (覆盖 %d 个品种)", inserted_total, len(symbols))

    # ── 4. 多空比 ──────────────────────────────────────────────────
    def collect_ls_ratio(self, conn, symbols: list) -> None:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT symbol, MAX(timestamp) as mt FROM futures_long_short_ratio
            WHERE exchange = 'binance' GROUP BY symbol
            """
        )
        latest = {r["symbol"]: r["mt"] for r in cur.fetchall()}

        inserted_total = 0
        for sym in symbols:
            bsym = self._binance_sym(sym)
            data = self._get(
                f"{FAPI}/futures/data/globalLongShortAccountRatio",
                {"symbol": bsym, "period": "1h", "limit": 6},
            )
            time.sleep(INTER_REQ_SLEEP)
            if not data or not isinstance(data, list) or not data:
                continue
            last_dt = latest.get(sym)
            rows = []
            for d in data:
                ts = datetime.fromtimestamp(int(d["timestamp"]) / 1000)
                if last_dt and ts <= last_dt:
                    continue
                rows.append((
                    sym, "binance", "1h",
                    float(d.get("longAccount", 0)),
                    float(d.get("shortAccount", 0)),
                    0.0, 0.0,  # position data not in this endpoint
                    float(d.get("longShortRatio", 0)),
                    float(d.get("longShortRatio", 0)),
                    ts,
                ))
            if rows:
                cur.executemany(
                    """
                    INSERT INTO futures_long_short_ratio
                        (symbol, exchange, period,
                         long_account, short_account,
                         long_position, short_position,
                         long_short_position_ratio, long_short_ratio,
                         timestamp)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    rows,
                )
                inserted_total += len(rows)
        conn.commit()
        log.info("多空比更新: %d 条 (覆盖 %d 个品种)", inserted_total, len(symbols))

    # ── 5. 清理旧数据 ──────────────────────────────────────────────
    def cleanup_old_data(self, conn) -> None:
        cur = conn.cursor()
        cur.execute("DELETE FROM futures_open_interest   WHERE timestamp < NOW() - INTERVAL 7 DAY")
        cur.execute("DELETE FROM futures_long_short_ratio WHERE timestamp < NOW() - INTERVAL 7 DAY")
        cur.execute("DELETE FROM funding_rate_data        WHERE timestamp < NOW() - INTERVAL 30 DAY")
        conn.commit()
        log.info("旧数据清理完成")

    # ── 6. 组合周期任务 (调用方便) ─────────────────────────────────
    def run_funding_cycle(self) -> list:
        """跑一轮 funding + 24h_stats, 返回 top N symbols 列表 (供 OI/LSR 用).
        返回空列表 = 本轮被封禁或失败.
        """
        self._ban_hit_this_cycle = False
        conn = None
        try:
            conn = self._get_db()
            top_syms = self.collect_24h_stats(conn)
            if not self._ban_hit_this_cycle:
                self.collect_funding_rates(conn)
            else:
                log.warning("本周期已判定 Binance 封禁/限速, 跳过资金费率请求")
            return top_syms
        except Exception as e:
            log.error("run_funding_cycle 异常: %s", e, exc_info=True)
            return []
        finally:
            if conn:
                try: conn.close()
                except Exception: pass

    def run_oi_lsr_cycle(self, symbols: list) -> None:
        """跑一轮 OI + LSR (按传入 symbols)."""
        if not symbols:
            return
        conn = None
        try:
            conn = self._get_db()
            self.collect_oi_history(conn, symbols)
            self.collect_ls_ratio(conn, symbols)
        except Exception as e:
            log.error("run_oi_lsr_cycle 异常: %s", e, exc_info=True)
        finally:
            if conn:
                try: conn.close()
                except Exception: pass

    def run_cleanup(self) -> None:
        conn = None
        try:
            conn = self._get_db()
            self.cleanup_old_data(conn)
        except Exception as e:
            log.error("run_cleanup 异常: %s", e, exc_info=True)
        finally:
            if conn:
                try: conn.close()
                except Exception: pass
