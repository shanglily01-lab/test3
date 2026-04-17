#!/usr/bin/env bash
# 查看三件套运行状态 + 最近日志
set -euo pipefail
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOGS="$APP_DIR/logs"

echo "=========================================="
echo " 进程状态"
echo "=========================================="
for PAT in "uvicorn app.main" "fast_collector_service" "dimension_trader.py"; do
  PIDS=$(pgrep -af "$PAT" | grep -v grep | awk '{print $1}' | tr '\n' ',' | sed 's/,$//')
  if [ -n "$PIDS" ]; then
    printf "  [RUN]  %-28s  pid=%s\n" "$PAT" "$PIDS"
  else
    printf "  [DOWN] %-28s\n" "$PAT"
  fi
done

echo ""
echo "=========================================="
echo " 端口监听"
echo "=========================================="
ss -ltnp 2>/dev/null | grep -E ':9021|:8000' || echo "  9021/8000 都未监听"

echo ""
echo "=========================================="
echo " 最近 5 行 API 日志"
echo "=========================================="
LATEST_API=$(ls -t "$LOGS"/api_*.log 2>/dev/null | head -n 1 || true)
[ -n "$LATEST_API" ] && tail -n 5 "$LATEST_API" || echo "  (无日志)"

echo ""
echo "=========================================="
echo " 最近 5 行 dimension_trader 日志"
echo "=========================================="
LATEST_DIM=$(ls -t "$LOGS"/dimension_*.log 2>/dev/null | head -n 1 || true)
[ -n "$LATEST_DIM" ] && tail -n 5 "$LATEST_DIM" || echo "  (无日志)"
