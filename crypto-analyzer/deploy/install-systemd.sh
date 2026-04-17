#!/usr/bin/env bash
# ============================================================
# 把 3 个 systemd unit 安装到 /etc/systemd/system 并启用
# 需要 root 权限
# ============================================================
set -euo pipefail

if [ "$(id -u)" -ne 0 ]; then
  echo "❌ 请用 sudo 运行: sudo bash deploy/install-systemd.sh"
  exit 1
fi

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
UNIT_DIR="$APP_DIR/deploy/systemd"

echo "[1/4] 创建 crypto 用户 (如不存在)"
if ! id crypto >/dev/null 2>&1; then
  useradd -r -s /bin/bash -d "$APP_DIR" crypto
  echo "  已创建 user=crypto"
else
  echo "  user=crypto 已存在"
fi
chown -R crypto:crypto "$APP_DIR"

echo "[2/4] 复制 unit 文件到 /etc/systemd/system"
for svc in crypto-api crypto-collector dimension-trader; do
  cp "$UNIT_DIR/$svc.service" "/etc/systemd/system/$svc.service"
  echo "  installed: $svc.service"
done

echo "[3/4] 重新加载 systemd"
systemctl daemon-reload

echo "[4/4] enable + start"
for svc in crypto-api crypto-collector dimension-trader; do
  systemctl enable  "$svc.service"
  systemctl restart "$svc.service"
  sleep 2
  STATE=$(systemctl is-active "$svc.service" || true)
  printf "  %-25s state=%s\n" "$svc" "$STATE"
done

echo ""
echo "============================================================"
echo "✅ systemd 安装完成。常用命令："
echo "   状态:    sudo systemctl status crypto-api crypto-collector dimension-trader"
echo "   重启:    sudo systemctl restart dimension-trader"
echo "   日志:    sudo journalctl -u dimension-trader -f"
echo "   文件日志: tail -f $APP_DIR/logs/dimension.log"
echo "============================================================"
