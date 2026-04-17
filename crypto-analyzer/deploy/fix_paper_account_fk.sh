#!/usr/bin/env bash
# 一键修复 futures_* 表错误指向 paper_trading_accounts 的外键约束
# 症状: dimension_trader 开的仓位, SL/TP 触发但无法平仓, 日志有
#       (1452, 'Cannot add or update a child row: a foreign key constraint fails ...')
# 用法: 在 crypto-analyzer/ 下执行  bash deploy/fix_paper_account_fk.sh

set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$APP_DIR"

# 从 .env 读数据库配置
if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env
  set +a
else
  echo "❌ $APP_DIR/.env 不存在"
  exit 1
fi

: "${DB_HOST:=127.0.0.1}"
: "${DB_PORT:=3306}"
: "${DB_USER:?.env 里缺 DB_USER}"
: "${DB_PASSWORD:?.env 里缺 DB_PASSWORD}"
: "${DB_NAME:?.env 里缺 DB_NAME}"

echo "---- 修复前: 指向 paper_trading_accounts 的外键 ----"
mysql -h"$DB_HOST" -P"$DB_PORT" -u"$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" \
  -e "SELECT TABLE_NAME, CONSTRAINT_NAME
        FROM information_schema.KEY_COLUMN_USAGE
       WHERE TABLE_SCHEMA = DATABASE()
         AND REFERENCED_TABLE_NAME = 'paper_trading_accounts';"

echo ""
echo "---- 执行 DROP ----"
mysql -h"$DB_HOST" -P"$DB_PORT" -u"$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" \
  < deploy/fix_paper_account_fk.sql

echo ""
echo "✅ 完成。若自检查询返回空集，FK 已全部清除。"
echo "   接下来可以直接让后端重试平仓，无需重启服务："
echo "     监控器会在下一轮 (最多 3s) 重新尝试。"
