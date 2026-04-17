#!/usr/bin/env bash
# ============================================================
# 手动启动三件套 (不用 systemd 时使用)
#   1. FastAPI (uvicorn) - 端口 $PORT (默认 9021, 可用 PORT=9022 bash deploy/start.sh 覆盖)
#   2. Fast collector     - K线实时采集
#   3. Dimension trader   - 核心策略执行
# 只会停掉/启动 "cwd == 本目录" 的进程，不干扰同机其它部署
# ============================================================
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_DIR_REAL="$(readlink -f "$APP_DIR")"
cd "$APP_DIR"

if [ ! -x "$APP_DIR/.venv/bin/python" ]; then
  echo "❌ $APP_DIR/.venv/bin/python 不存在，先跑 bash deploy/install.sh"
  exit 1
fi
PY="$APP_DIR/.venv/bin/python"
LOGS="$APP_DIR/logs"
PORT="${PORT:-9021}"
mkdir -p "$LOGS"

# 只杀 cwd 在本目录的同名进程（不干扰其它部署）
kill_own() {
  local pattern="$1"
  for pid in $(pgrep -f "$pattern" 2>/dev/null || true); do
    local cwd
    cwd="$(readlink -f "/proc/$pid/cwd" 2>/dev/null || echo '')"
    if [ "$cwd" = "$APP_DIR_REAL" ]; then
      echo "  stop pid=$pid ($pattern)"
      kill "$pid" 2>/dev/null || true
    fi
  done
}

echo "[stop-own] 清理本目录下的旧进程 (其它目录的部署不受影响)..."
kill_own "uvicorn app.main"
kill_own "fast_collector_service"
kill_own "dimension_trader.py"
sleep 2

DATE=$(date +%Y%m%d)

echo "[1/3] FastAPI server (port $PORT)"
nohup "$PY" -m uvicorn app.main:app --host 0.0.0.0 --port "$PORT" --no-access-log \
  > "$LOGS/api_${DATE}.log" 2>&1 &
API_PID=$!
echo "  PID=$API_PID  port=$PORT  log=$LOGS/api_${DATE}.log"
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
echo "✅ 全部启动 (本目录: $APP_DIR_REAL)"
echo "   状态:    bash deploy/status.sh"
echo "   停止:    bash deploy/stop.sh"
echo "   日志:    tail -f $LOGS/dimension_${DATE}.log"
