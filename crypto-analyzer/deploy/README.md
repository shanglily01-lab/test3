# AWS Linux 部署指南

> 本目录包含把 `crypto-analyzer` 部署到 **AWS EC2 (Amazon Linux 2023 / Ubuntu 22.04)** 所需的全部脚本。
> 核心三件套：**FastAPI (Web UI + API)** · **Fast Collector (K线)** · **Dimension Trader (策略执行)**

---

## 0. EC2 推荐规格

| 项目         | 最低            | 推荐                 |
|--------------|-----------------|----------------------|
| 实例         | t3.small        | t3.medium / c6i.large|
| 内存         | 2 GB            | 4 GB+                |
| 磁盘         | 30 GB gp3       | 50 GB gp3            |
| OS           | Amazon Linux 2023 / Ubuntu 22.04 | 同左  |
| 安全组出站   | 443 → 全部 (调用 Binance/Gemini) | 同左 |
| 安全组入站   | 22 (限 IP), 9021 (限 IP 或经 Nginx) | 同左 |

数据库：**RDS MySQL 8** 或 EC2 本地 MySQL。如用本地：`sudo dnf install -y mariadb105-server` (AL2023) 或 `apt install mysql-server` (Ubuntu)。

---

## 1. 首次部署 (5 步)

```bash
# ① 建目录、拉代码
sudo mkdir -p /opt/crypto && sudo chown $USER:$USER /opt/crypto
cd /opt/crypto
git clone https://github.com/shanglily01-lab/test3.git .
cd crypto-analyzer

# ② 跑安装脚本（装 python3.11、venv、pip deps）
bash deploy/install.sh

# ③ 编辑 .env 填真实密钥
nano .env   # 至少填 DB_*/ BINANCE_*/ GEMINI_API_KEY / TELEGRAM_*

# ④ 初始化数据库（如果是全新库）
# ⚠️ binance-data.sql (约 650MB) 不在 git 里，需手动上传：
#    本地:  scp binance-data.sql ec2-user@<EC2-IP>:/tmp/
#    EC2:   mysql -h"$DB_HOST" -u"$DB_USER" -p"$DB_PASSWORD" -e "CREATE DATABASE \`binance-data\` CHARACTER SET utf8mb4;"
#    EC2:   mysql -h"$DB_HOST" -u"$DB_USER" -p"$DB_PASSWORD" binance-data < /tmp/binance-data.sql
# 或仅恢复 schema（无历史K线，从采集起）：自行用 mysqldump --no-data 导出 schema.sql

# ⑤ 启动 (二选一)
# (a) 手动前台测试
bash deploy/start.sh  &&  bash deploy/status.sh

# (b) 生产 systemd（开机自启 + 进程守护 + journald）
sudo bash deploy/install-systemd.sh
```

打开浏览器访问 `http://<EC2-公网IP>:9021` 即可看到 Web UI。
如需 HTTPS，前面加一层 Nginx + Let's Encrypt（见 §5）。

---

## 2. 日常运维

| 动作        | 手动模式                           | systemd 模式                                       |
|-------------|------------------------------------|----------------------------------------------------|
| 启动        | `bash deploy/start.sh`             | `sudo systemctl start crypto-api crypto-collector dimension-trader` |
| 停止        | `bash deploy/stop.sh`              | `sudo systemctl stop  dimension-trader crypto-collector crypto-api` |
| 重启某个    | `pkill -f dimension_trader.py && nohup ... &` | `sudo systemctl restart dimension-trader` |
| 状态        | `bash deploy/status.sh`            | `sudo systemctl status crypto-api crypto-collector dimension-trader` |
| 实时日志    | `tail -f logs/dimension_*.log`     | `sudo journalctl -u dimension-trader -f`           |

---

## 3. 更新代码

```bash
cd /opt/crypto
git pull
cd crypto-analyzer
source .venv/bin/activate
pip install -r requirements.txt          # 如依赖变更
sudo systemctl restart crypto-api crypto-collector dimension-trader
```

---

## 4. 目录约定

| 路径                          | 说明                                     |
|-------------------------------|------------------------------------------|
| `/opt/crypto/crypto-analyzer` | 代码根 (WorkingDirectory)                |
| `/opt/crypto/crypto-analyzer/.venv`     | Python 虚拟环境                |
| `/opt/crypto/crypto-analyzer/.env`      | 运行时密钥 (不在 git 里)       |
| `/opt/crypto/crypto-analyzer/logs/`     | 日志（按天/按服务滚动）        |
| `/opt/crypto/crypto-analyzer/gemini_signals/` | 动态策略（运行时生成）   |
| `/opt/crypto/crypto-analyzer/btc_gemini_logs/` | BTC 方向判断日志         |

---

## 5. (可选) Nginx + HTTPS

```nginx
# /etc/nginx/conf.d/crypto.conf
server {
  listen 80;
  server_name your.domain.com;
  return 301 https://$host$request_uri;
}
server {
  listen 443 ssl http2;
  server_name your.domain.com;
  ssl_certificate     /etc/letsencrypt/live/your.domain.com/fullchain.pem;
  ssl_certificate_key /etc/letsencrypt/live/your.domain.com/privkey.pem;

  location / {
    proxy_pass         http://127.0.0.1:9021;
    proxy_http_version 1.1;
    proxy_set_header   Host              $host;
    proxy_set_header   X-Real-IP         $remote_addr;
    proxy_set_header   X-Forwarded-For   $proxy_add_x_forwarded_for;
    proxy_set_header   X-Forwarded-Proto $scheme;
    # WebSocket (如前端用)
    proxy_set_header   Upgrade           $http_upgrade;
    proxy_set_header   Connection        "upgrade";
    proxy_read_timeout 300s;
  }
}
```

```bash
sudo dnf install -y nginx certbot python3-certbot-nginx   # 或 apt
sudo systemctl enable --now nginx
sudo certbot --nginx -d your.domain.com
```

---

## 6. 常见坑

1. **`pydantic_core` 导入错误**：确认 `pip install -r requirements.txt` 已跑过（本仓库已把 pydantic 升到 `>=2.9,<3`）。
2. **Binance API 401/403**：在 Binance 后台把 EC2 弹性 IP 加入白名单，只勾 Enable Futures。
3. **Telegram 不发消息**：`curl -s "https://api.telegram.org/bot$TELEGRAM_BOT_TOKEN/getMe"` 自检 token，检查 EC2 出站 443 未被安全组拦截。
4. **策略目录空**：`dimension_trader.py` 启动时会从数据库 `signal_strategies_registry` 生成 `gemini_signals/*.py`；首次部署需先跑一次 `btc_gemini_regime.py` 或等待 Gemini 生成。
5. **时间戳错乱**：EC2 默认 UTC，K 线全部 UTC，不要改系统时区。
