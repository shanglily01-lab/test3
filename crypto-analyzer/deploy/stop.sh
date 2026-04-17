#!/usr/bin/env bash
# 只停本目录下启动的服务，不干扰同机其它部署
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_DIR_REAL="$(readlink -f "$APP_DIR")"

kill_own() {
  local pattern="$1"
  local found=0
  for pid in $(pgrep -f "$pattern" 2>/dev/null || true); do
    local cwd
    cwd="$(readlink -f "/proc/$pid/cwd" 2>/dev/null || echo '')"
    if [ "$cwd" = "$APP_DIR_REAL" ]; then
      kill "$pid" 2>/dev/null && echo "  killed pid=$pid  ($pattern)" || true
      found=1
    fi
  done
  [ "$found" = "0" ] && echo "  (not running) $pattern"
}

echo "[stop] 本目录: $APP_DIR_REAL"
kill_own "uvicorn app.main"
kill_own "fast_collector_service"
kill_own "dimension_trader.py"
sleep 1
echo "✅ 已停止 (其它目录的进程未动)"
