#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
trade_scheduler.py
==================
每 12 小时自动运行一轮策略：
  1. discovery_trader.py  —— Big4 (BTC/ETH/BNB/SOL)
  2. market_trader.py     —— 全市场 USDT 永续，按黑名单分级保证金

用法:
  .venv/Scripts/python.exe trade_scheduler.py              # 立即运行第一轮，之后每12h一轮
  .venv/Scripts/python.exe trade_scheduler.py --no-first   # 跳过第一轮，12h后才运行
  .venv/Scripts/python.exe trade_scheduler.py --interval 6 # 自定义间隔（小时）
"""

import argparse
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

from loguru import logger

BASE_DIR    = Path(__file__).parent
VENV_PYTHON = BASE_DIR / ".venv/Scripts/python.exe"
LOG_DIR     = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

# discovery_trader 开仓阶段完成大约需要 5 分钟（Gemini + 开仓）
# 等待这段时间再启动 market_trader，避免 BTC/ETH 等重复抢仓
DISCOVERY_WARMUP_SECONDS = 300

# 每轮启动前先杀掉残留的同名旧进程，防止叠加
MANAGED_SCRIPTS = ["discovery_trader.py", "alien_trader.py", "market_trader.py"]


def _kill_old(script: str) -> None:
    """杀掉所有仍在运行的同名旧进程（排除自身）"""
    import psutil, os
    self_pid = os.getpid()
    killed = []
    for proc in psutil.process_iter(["pid", "cmdline"]):
        try:
            cmdline = proc.info.get("cmdline") or []
            if any(script in arg for arg in cmdline) and proc.pid != self_pid:
                proc.kill()
                killed.append(proc.pid)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    if killed:
        logger.info(f"Killed old {script} processes: {killed}")


def _launch(script: str, extra_args: list[str], log_tag: str) -> subprocess.Popen:
    ts = datetime.now().strftime("%Y%m%d_%H%M")
    log_path = LOG_DIR / f"{log_tag}_{ts}.log"
    cmd = [str(VENV_PYTHON), script] + extra_args
    log_f = open(log_path, "w", encoding="utf-8", errors="replace")
    proc = subprocess.Popen(
        cmd,
        stdout=log_f,
        stderr=subprocess.STDOUT,
        cwd=BASE_DIR,
    )
    logger.info(f"Launched {script} (PID {proc.pid}) -> {log_path}")
    return proc


def run_round(batch_size: int = 4) -> None:
    now = datetime.now()
    logger.info(f"=== Trade round starting: {now.strftime('%Y-%m-%d %H:%M')} ===")

    # 每轮启动前先清理旧进程
    for s in MANAGED_SCRIPTS:
        _kill_old(s)
    time.sleep(1)

    # Step 1: 启动 discovery_trader（Big4，Gemini AI信号）
    proc_discovery = _launch("discovery_trader.py", [], "discovery")

    # Step 1b: 启动 alien_trader（Big4，统计验证信号：64-68%胜率）
    # alien_trader 检查DB中已有仓位，不会与 discovery_trader 重复开仓
    proc_alien = _launch("alien_trader.py", [], "alien")

    # Step 2: 等待 discovery/alien 完成开仓阶段，再启动 market_trader
    logger.info(f"Waiting {DISCOVERY_WARMUP_SECONDS}s for discovery/alien traders to finish opening positions...")
    time.sleep(DISCOVERY_WARMUP_SECONDS)

    # Step 3: 启动 market_trader（全市场扫描，按黑名单分级保证金）
    proc_market = _launch("market_trader.py", ["--batch-size", str(batch_size)], "market")

    logger.info(
        f"All traders running. discovery PID={proc_discovery.pid}, "
        f"alien PID={proc_alien.pid}, market PID={proc_market.pid}. "
        f"They self-monitor and will close positions automatically."
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="12h trade scheduler")
    parser.add_argument("--no-first", action="store_true",
                        help="Skip the immediate first run, wait for first interval")
    parser.add_argument("--interval", type=float, default=4.0,
                        help="Interval between rounds in hours (default: 4)")
    parser.add_argument("--batch-size", type=int, default=4,
                        help="Batch size for market_trader (default: 4)")
    args = parser.parse_args()

    interval_secs = args.interval * 3600

    if not args.no_first:
        run_round(batch_size=args.batch_size)
    else:
        next_run = datetime.now() + timedelta(seconds=interval_secs)
        logger.info(f"--no-first: skipping immediate run. First round at {next_run.strftime('%Y-%m-%d %H:%M')}")

    while True:
        next_run = datetime.now() + timedelta(seconds=interval_secs)
        logger.info(f"Next round scheduled at {next_run.strftime('%Y-%m-%d %H:%M')} "
                    f"(in {args.interval:.0f}h)")
        time.sleep(interval_secs)
        run_round(batch_size=args.batch_size)


if __name__ == "__main__":
    main()
