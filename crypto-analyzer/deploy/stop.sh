#!/usr/bin/env bash
# 停止所有手动启动的服务
set -euo pipefail

echo "[stop] uvicorn / collector / dimension_trader..."
pkill -f "uvicorn app.main" 2>/dev/null && echo "  uvicorn  killed" || echo "  uvicorn  not running"
pkill -f "fast_collector_service" 2>/dev/null && echo "  collector killed" || echo "  collector not running"
pkill -f "dimension_trader.py" 2>/dev/null && echo "  dimension killed" || echo "  dimension not running"
sleep 1
echo "✅ 已停止"
