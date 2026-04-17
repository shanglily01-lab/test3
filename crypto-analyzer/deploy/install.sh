#!/usr/bin/env bash
# ============================================================
# AWS Linux 一键安装脚本 (Amazon Linux 2023 / Ubuntu 22.04)
# 用法:
#   cd /opt && sudo git clone https://github.com/shanglily01-lab/test3.git crypto
#   cd /opt/crypto/crypto-analyzer
#   sudo bash deploy/install.sh
# ============================================================
set -euo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$APP_DIR"

echo "[1/6] detect OS"
if [ -f /etc/os-release ]; then . /etc/os-release; OS_ID="${ID:-}"; else OS_ID="unknown"; fi
echo "  OS = $OS_ID"

echo "[2/6] install system packages"
case "$OS_ID" in
  amzn|rhel|centos|fedora)
    sudo dnf install -y python3.11 python3.11-pip python3.11-devel gcc mysql git tmux jq \
      || sudo yum install -y python3.11 python3.11-pip python3.11-devel gcc mysql git tmux jq
    PY=python3.11
    ;;
  ubuntu|debian)
    sudo apt-get update
    sudo apt-get install -y python3.11 python3.11-venv python3.11-dev build-essential \
                            default-mysql-client git tmux jq
    PY=python3.11
    ;;
  *)
    echo "  unsupported distro, assuming python3 + pip available"
    PY=python3
    ;;
esac

echo "[3/6] create virtualenv at $APP_DIR/.venv"
if [ ! -d .venv ]; then
  $PY -m venv .venv
fi
# shellcheck disable=SC1091
source .venv/bin/activate
pip install --upgrade pip wheel

echo "[4/6] install python dependencies"
pip install -r requirements.txt

echo "[5/6] prepare directories"
mkdir -p logs btc_gemini_logs gemini_signals data
touch logs/.keep

echo "[6/6] check .env"
if [ ! -f .env ]; then
  cp deploy/.env.example .env
  echo ""
  echo "  ⚠️  .env 已从模板创建，请立即编辑填入真实密钥："
  echo "      nano $APP_DIR/.env"
  echo ""
else
  echo "  ✅ .env 已存在"
fi

echo ""
echo "============================================================"
echo "✅ install done. 接下来："
echo "   1. 编辑 .env 填入 DB/API 密钥"
echo "   2. 初始化数据库: mysql -h\$DB_HOST -u\$DB_USER -p\$DB_PASSWORD \$DB_NAME < binance-data.sql  (如已有库则跳过)"
echo "   3. 启动服务:"
echo "      a) 开发/手动: bash deploy/start.sh"
echo "      b) 生产/systemd: sudo bash deploy/install-systemd.sh"
echo "============================================================"
