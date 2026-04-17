#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
服务看守进程 (Watchdog)

监控以下服务，进程死亡或数据停更时自动重启：
  - fast_collector_service.py   (K线采集)
  - smart_trader_service.py     (交易大脑)

健康判断规则：
  - 采集服务: 进程存活 AND 5m K线更新时间 < KLINE_MAX_AGE 秒
  - 交易服务: 进程存活
"""

import os
import sys
import subprocess
import time
from datetime import datetime
from pathlib import Path

import psutil
import pymysql
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

# ─── 配置 ────────────────────────────────────────────────────────────────────
BASE_DIR     = Path(__file__).parent
VENV_PYTHON  = str(BASE_DIR / '.venv' / 'Scripts' / 'python.exe')

DB_CONFIG = {
    'host':            os.getenv('DB_HOST', 'localhost'),
    'port':            int(os.getenv('DB_PORT', 3306)),
    'user':            os.getenv('DB_USER', 'root'),
    'password':        os.getenv('DB_PASSWORD', ''),
    'database':        os.getenv('DB_NAME', 'binance-data'),
    'connect_timeout': 5,
}

# 受监控的服务：名称 -> 启动脚本
SERVICES = {
    'collector':    'fast_collector_service.py',
    'smart_trader': 'smart_trader_service.py',
}

CHECK_INTERVAL_SECONDS = 120   # 每 2 分钟巡检一次
KLINE_MAX_AGE_SECONDS  = 15 * 60   # 5m K线超过 15 分钟未更新视为采集卡死
MIN_RESTART_GAP        = 90    # 同一服务两次重启之间最短间隔（秒），防抖动

# ─── 状态 ────────────────────────────────────────────────────────────────────
_last_restart: dict[str, float] = {}   # service -> timestamp
_restart_count: dict[str, int]  = {}   # service -> count


# ─── 日志 ────────────────────────────────────────────────────────────────────
def _setup_logger() -> None:
    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level:<8}</level> | {message}",
        level="INFO",
        colorize=True,
    )
    logger.add(
        str(BASE_DIR / "logs" / "watchdog_{time:YYYY-MM-DD}.log"),
        rotation="00:00",
        retention="14 days",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level:<8} | {message}",
        level="INFO",
        encoding="utf-8",
    )


# ─── 工具函数 ─────────────────────────────────────────────────────────────────
def find_service_pid(script_name: str) -> int | None:
    """在所有 Python 进程的命令行参数里找目标脚本，返回 PID 或 None。"""
    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            if 'python' not in (proc.info['name'] or '').lower():
                continue
            cmdline = proc.info['cmdline'] or []
            if any(script_name in str(arg) for arg in cmdline):
                return proc.info['pid']
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return None


def get_kline_age_seconds() -> float | None:
    """查询 5m K线最新 close_time 距今秒数，查询失败返回 None。"""
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cur  = conn.cursor()
        cur.execute("SELECT MAX(close_time) FROM kline_data WHERE timeframe='5m'")
        row = cur.fetchone()
        cur.close()
        conn.close()
        ts = row[0] if row else None
        if ts and ts > 1e12:
            return (time.time() * 1000 - ts) / 1000.0
    except Exception as exc:
        logger.warning(f"K线新鲜度查询失败: {exc}")
    return None


def start_service(name: str) -> int | None:
    """
    启动服务，写入当日日志文件。
    60 秒内重复重启同一服务会跳过（防止崩溃循环）。
    返回新进程 PID 或 None（被跳过时）。
    """
    now = time.time()
    since_last = now - _last_restart.get(name, 0)
    if since_last < MIN_RESTART_GAP:
        logger.warning(
            f"[{name}] 距上次重启仅 {since_last:.0f}s，等待冷却期后再试"
        )
        return None

    script    = SERVICES[name]
    date_str  = datetime.now().strftime('%Y%m%d')
    log_path  = BASE_DIR / 'logs' / f'{name}_{date_str}.log'

    flags = subprocess.CREATE_NO_WINDOW if sys.platform == 'win32' else 0

    with open(log_path, 'a', encoding='utf-8', errors='replace') as fout:
        proc = subprocess.Popen(
            [VENV_PYTHON, str(BASE_DIR / script)],
            cwd=str(BASE_DIR),
            stdout=fout,
            stderr=fout,
            creationflags=flags,
        )

    _last_restart[name] = now
    _restart_count[name] = _restart_count.get(name, 0) + 1
    logger.info(
        f"[{name}] 已重启 (第 {_restart_count[name]} 次)，PID={proc.pid}"
    )
    return proc.pid


def kill_and_restart(name: str, pid: int) -> None:
    """杀掉现有进程后重启（用于采集服务卡死但进程还在的场景）。"""
    logger.warning(f"[{name}] 强制终止 PID={pid} 并重启...")
    try:
        psutil.Process(pid).kill()
    except Exception as exc:
        logger.warning(f"[{name}] 终止 PID={pid} 失败（可能已退出）: {exc}")
    time.sleep(3)
    start_service(name)


# ─── 核心巡检 ─────────────────────────────────────────────────────────────────
def check_and_heal() -> None:
    now_str = datetime.now().strftime('%H:%M:%S')

    # ── 采集服务 ──────────────────────────────────────────────────────────────
    coll_pid = find_service_pid(SERVICES['collector'])

    if coll_pid:
        kline_age = get_kline_age_seconds()
        if kline_age is not None and kline_age > KLINE_MAX_AGE_SECONDS:
            age_min = kline_age / 60
            logger.error(
                f"[collector] 进程存活(PID={coll_pid}) 但 5m K线已 {age_min:.1f}min 未更新，"
                f"判定为卡死，强制重启"
            )
            kill_and_restart('collector', coll_pid)
        else:
            age_info = f"{kline_age / 60:.1f}min ago" if kline_age is not None else "db unavail"
            logger.info(f"[{now_str}] collector  OK  PID={coll_pid}  kline={age_info}")
    else:
        logger.error(f"[collector] 进程不存在，重启...")
        start_service('collector')

    # ── 交易服务 ──────────────────────────────────────────────────────────────
    trader_pid = find_service_pid(SERVICES['smart_trader'])

    if trader_pid:
        logger.info(f"[{now_str}] smart_trader OK  PID={trader_pid}")
    else:
        logger.error(f"[smart_trader] 进程不存在，重启...")
        start_service('smart_trader')


# ─── 主循环 ───────────────────────────────────────────────────────────────────
def main() -> None:
    _setup_logger()

    logger.info("=" * 60)
    logger.info(
        f"Watchdog 启动  check={CHECK_INTERVAL_SECONDS}s  "
        f"kline_threshold={KLINE_MAX_AGE_SECONDS // 60}min"
    )
    logger.info(f"监控服务: {list(SERVICES.keys())}")
    logger.info("=" * 60)

    while True:
        try:
            check_and_heal()
        except Exception as exc:
            logger.error(f"Watchdog 巡检异常: {exc}")
            import traceback
            logger.error(traceback.format_exc())
        time.sleep(CHECK_INTERVAL_SECONDS)


if __name__ == '__main__':
    main()
