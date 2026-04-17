#!/usr/bin/env bash
# 只显示属于本目录的进程状态（按 cwd 过滤），避免与同机其它部署混淆
set -euo pipefail
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_DIR_REAL="$(readlink -f "$APP_DIR")"
LOGS="$APP_DIR/logs"

echo "=========================================="
echo " 本部署目录: $APP_DIR_REAL"
echo "=========================================="

find_own() {
  local pattern="$1"
  local out=""
  for pid in $(pgrep -f "$pattern" 2>/dev/null || true); do
    local cwd
    cwd="$(readlink -f "/proc/$pid/cwd" 2>/dev/null || echo '')"
    if [ "$cwd" = "$APP_DIR_REAL" ]; then
      out="$out,$pid"
    fi
  done
  echo "${out#,}"
}

for PAT in "uvicorn app.main" "fast_collector_service" "dimension_trader.py"; do
  PIDS="$(find_own "$PAT")"
  if [ -n "$PIDS" ]; then
    printf "  [RUN]  %-28s  pid=%s\n" "$PAT" "$PIDS"
  else
    printf "  [DOWN] %-28s\n" "$PAT"
  fi
done

echo ""
echo "=========================================="
echo " 本目录端口监听 (9021/9022 等 902x)"
echo "=========================================="
# 只列跟本目录 python 有关的监听（需要 root 才能看 -p，普通用户看不到 pid 是正常的）
ss -ltn 2>/dev/null | awk 'NR==1 || $4 ~ /:(902[0-9]|8000)$/' || echo "  (无匹配端口)"

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
