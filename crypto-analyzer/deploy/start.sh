#!/usr/bin/env bash
# ============================================================
# 手动启动三件套 (不用 systemd 时使用)
#   1. FastAPI (uvicorn) - 端口 9021
#   2. Fast collector     - K线实时采集
#   3. Dimension trader   - 核心策略执行
# 日志输出到 logs/
# ============================================================
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$APP_DIR"

if [ ! -d .venv ]; then
  echo "❌ .venv 不存在，先跑 bash deploy/install.sh"
  exit 1
fi
PY="$APP_DIR/.venv/bin/python"
LOGS="$APP_DIR/logs"
mkdir -p "$LOGS"

echo "[stop] 清理旧进程..."
pkill -f "uvicorn app.main" 2>/dev/null || true
pkill -f "fast_collector_service" 2>/dev/null || true
pkill -f "dimension_trader.py" 2>/dev/null || true
sleep 2

DATE=$(date +%Y%m%d)

echo "[1/3] FastAPI server (port 9021)"
nohup "$PY" -m uvicorn app.main:app --host 0.0.0.0 --port 9021 --no-access-log \
  > "$LOGS/api_${DATE}.log" 2>&1 &
API_PID=$!
echo "  PID=$API_PID  log=$LOGS/api_${DATE}.log"
sleep 5

echo "[2/3] Fast collector (K-line)"
nohup "$PY" fast_collector_service.py \
  > "$LOGS/collector_${DATE}.log" 2>&1 &
COL_PID=$!
echo "  PID=$COL_PID  log=$LOGS/collector_${DATE}.log"
sleep 2

echo "[3/3] Dimension trader"
nohup "$PY" dimension_trader.py \
  > "$LOGS/dimension_${DATE}.log" 2>&1 &
DIM_PID=$!
echo "  PID=$DIM_PID  log=$LOGS/dimension_${DATE}.log"

echo ""
echo "✅ 全部启动。查看状态:  bash deploy/status.sh"
echo "   停止:              bash deploy/stop.sh"
echo "   实时日志(API):     tail -f $LOGS/api_${DATE}.log"
