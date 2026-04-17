#!/usr/bin/env bash
# =============================================================
# 全服务启动脚本 (test3 本地开发环境)
# 启动顺序：API -> 数据采集 -> 调度器 -> 超级大脑 -> Hyperliquid -> Watchdog
# Watchdog 负责监控 collector 和 smart_trader，进程死亡或K线停更时自动重启
# =============================================================

set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

VENV="$SCRIPT_DIR/.venv/Scripts/python"
LOGS="$SCRIPT_DIR/logs"
mkdir -p "$LOGS"

echo "[START] Cleaning stale processes..."
pkill -f "uvicorn app.main" 2>/dev/null || true
pkill -f "smart_trader_service" 2>/dev/null || true
pkill -f "fast_collector_service" 2>/dev/null || true
pkill -f "app/scheduler.py" 2>/dev/null || true
pkill -f "hyperliquid_scheduler" 2>/dev/null || true
pkill -f "watchdog.py" 2>/dev/null || true
sleep 2

echo "[START] 1/6 FastAPI server (port 9021)..."
nohup "$VENV" -m uvicorn app.main:app --host 0.0.0.0 --port 9021 --no-access-log \
    > "$LOGS/main_$(date +%Y%m%d).log" 2>&1 &
MAIN_PID=$!
echo "  PID=$MAIN_PID"
sleep 4

echo "[START] 2/6 Fast collector (K-line data)..."
nohup "$VENV" fast_collector_service.py \
    > "$LOGS/collector_$(date +%Y%m%d).log" 2>&1 &
echo "  PID=$!"
sleep 2

echo "[START] 3/6 Data scheduler (news/indicators/ETF)..."
nohup "$VENV" app/scheduler.py \
    > "$LOGS/scheduler_$(date +%Y%m%d).log" 2>&1 &
echo "  PID=$!"
sleep 2

echo "[START] 4/6 Smart trader service (U-margined brain)..."
nohup "$VENV" smart_trader_service.py \
    > "$LOGS/smart_trader_$(date +%Y%m%d).log" 2>&1 &
echo "  PID=$!"
sleep 2

echo "[START] 5/6 Hyperliquid scheduler..."
nohup "$VENV" app/hyperliquid_scheduler.py \
    > "$LOGS/hyperliquid_$(date +%Y%m%d).log" 2>&1 &
echo "  PID=$!"
sleep 2

echo "[START] 6/6 Watchdog (auto-restart monitor)..."
nohup "$VENV" watchdog.py \
    > "$LOGS/watchdog_$(date +%Y%m%d).log" 2>&1 &
echo "  PID=$!"

echo ""
echo "[DONE] All services started."
echo "  API:         http://localhost:9021"
echo "  Logs:        $LOGS/"
echo "  Watchdog:    checks every 120s, kline threshold=15min"
echo ""
echo "Process status:"
sleep 2
ps aux | grep -E "(uvicorn|smart_trader|fast_collector|scheduler|hyperliquid|watchdog)" | grep -v grep | awk '{print "  " $11 " PID=" $2}'
