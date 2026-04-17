#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sentiment_collector.py
=======================
每小时采集 Fear & Greed Index，写入 market_sentiment 表。
Fear & Greed Index 每日更新一次，但采集器按小时运行保证不漏。

用法:
  .venv/Scripts/python.exe collectors/sentiment_collector.py
"""

import os
import sys
import time
from datetime import datetime, date, timezone
from pathlib import Path

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).parent.parent))

import pymysql
import requests
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

_DB_CFG = {
    "host":      os.getenv("DB_HOST", "localhost"),
    "port":      int(os.getenv("DB_PORT", 3306)),
    "user":      os.getenv("DB_USER", "root"),
    "password":  os.getenv("DB_PASSWORD", ""),
    "database":  os.getenv("DB_NAME", "binance-data"),
    "charset":   "utf8mb4",
    "autocommit": True,
}

FNG_URL      = "https://api.alternative.me/fng/?limit=3&format=json"
INTERVAL_S   = 3600   # 每小时检查一次


def fetch_fng() -> list[dict]:
    """返回最近几天的 Fear & Greed 数据"""
    try:
        r = requests.get(FNG_URL, timeout=10)
        r.raise_for_status()
        return r.json().get("data", [])
    except Exception as e:
        logger.error(f"Fear & Greed fetch failed: {e}")
        return []


def upsert_fng(records: list[dict]) -> None:
    if not records:
        return
    conn = pymysql.connect(**_DB_CFG)
    with conn.cursor() as cur:
        for rec in records:
            ts = int(rec.get("timestamp", 0))
            rec_date = date.fromtimestamp(ts) if ts else date.today()
            val   = int(rec.get("value", 0))
            label = rec.get("value_classification", "")
            cur.execute(
                """INSERT INTO market_sentiment (fng_value, fng_label, recorded_date)
                   VALUES (%s, %s, %s)
                   ON DUPLICATE KEY UPDATE fng_value=%s, fng_label=%s""",
                (val, label, rec_date, val, label),
            )
    conn.close()
    latest = records[0]
    logger.info(
        f"Fear & Greed: {latest.get('value')} ({latest.get('value_classification')})"
    )


def run() -> None:
    logger.info("Sentiment collector started.")
    while True:
        records = fetch_fng()
        upsert_fng(records)
        time.sleep(INTERVAL_S)


if __name__ == "__main__":
    run()
