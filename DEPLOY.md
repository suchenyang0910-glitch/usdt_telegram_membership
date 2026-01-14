# 部署说明

## 方式 A：Docker Compose（推荐）

1）准备 `.env`（从 `.env.example` 复制）  
2）如使用 compose 自带 MySQL，把 `.env` 里的 `DB_HOST` 改为 `mysql`，并保持 `DB_USER/DB_PASS/DB_NAME` 与 `docker-compose.yml` 一致  
3）启动

建议做法（减少每次更新都要核对 `.env`）：
- 非敏感配置（频道/开关/阈值等）：放到 `config/app_config.json`（可选）
- 敏感配置（token/密码/session 等）：放到 `.env.secrets`（可选，优先级高于 `.env`）

仍建议放在 `.env` 的关键配置（更稳定且更直观）：
- `BOT_TOKEN/BOT_USERNAME`
- `PAID_CHANNEL_ID/HIGHLIGHT_CHANNEL_ID/FREE_CHANNEL_ID_1/2`
- 支付/钱包相关：`TRONGRID_API_KEY`、`PAYMENT_MODE`、`RECEIVE_ADDRESS`、`USDT_ADDRESS_POOL` 等
- 数据库相关：`DB_HOST/DB_PORT/DB_USER/DB_PASS/DB_NAME`、`MYSQL_*`（compose）

`config/app_config.json`：
- 从 [app_config.example.json](file:///d:/VScode/usdt_telegram_membership/config/app_config.example.json) 复制为 `config/app_config.json` 后修改

`.env.secrets`：
- 从 [\.env.secrets.example](file:///d:/VScode/usdt_telegram_membership/.env.secrets.example) 复制为 `.env.secrets` 后填写

```bash
cd deploy
docker compose up -d --build
```

日志：
- 容器日志：`docker compose logs -f app`
- 文件日志：挂载到项目 `logs/` 下（`logs/bot.log`、`logs/runtime.log`）

### 管理后台（可选）

`admin_web.py` 以独立服务运行，默认不随 compose 启动（使用 profile 进行控制），并默认只绑定到本机 `127.0.0.1:8080`。

启用方式：

```bash
cd deploy
docker compose --profile admin up -d --build
```

必需环境变量（在 `.env` 中配置）：
- `ADMIN_WEB_USER`
- `ADMIN_WEB_PASS`

## 方式 B：systemd（Linux）

1）服务器安装依赖：Python、ffmpeg、MySQL 客户端库（如需）  
2）代码放到：`/opt/pvbot/usdt_telegram_membership`  
3）创建虚拟环境并安装依赖（核心链路）

```bash
cd /opt/pvbot/usdt_telegram_membership
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

如要启用邀请海报，再安装：

```bash
pip install -r requirements-media.txt
```

4）放置 `.env` 到 `/opt/pvbot/usdt_telegram_membership/.env`  
5）安装 systemd 服务

```bash
sudo cp /opt/pvbot/usdt_telegram_membership/deploy/pvbot.service /etc/systemd/system/pvbot.service
sudo systemctl daemon-reload
sudo systemctl enable --now pvbot
sudo systemctl status pvbot
```

日志文件：
- `/opt/pvbot/usdt_telegram_membership/logs/runtime.log`
- `/opt/pvbot/usdt_telegram_membership/logs/bot.log`

### 管理后台（可选）

安装服务：

```bash
sudo cp /opt/pvbot/usdt_telegram_membership/deploy/pvadmin.service /etc/systemd/system/pvadmin.service
sudo systemctl daemon-reload
sudo systemctl enable --now pvadmin
sudo systemctl status pvadmin
```

注意：
- 需要在 `/opt/pvbot/usdt_telegram_membership/.env` 里设置 `ADMIN_WEB_USER/ADMIN_WEB_PASS`
- 如需只允许本机访问，将 `ADMIN_WEB_HOST` 设为 `127.0.0.1`

### watchdog（可选，推荐开启无人值守）

watchdog 会周期性检查服务是否“在跑”，并可选按心跳文件判断“是否卡死/不工作”，必要时自动重启并通知 Telegram。

安装并启动 timer：

```bash
sudo cp /opt/pvbot/usdt_telegram_membership/deploy/pvbot-watchdog.service /etc/systemd/system/pvbot-watchdog.service
sudo cp /opt/pvbot/usdt_telegram_membership/deploy/pvbot-watchdog.timer /etc/systemd/system/pvbot-watchdog.timer
sudo systemctl daemon-reload
sudo systemctl enable --now pvbot-watchdog.timer
sudo systemctl status pvbot-watchdog.timer
```

建议在 `/opt/pvbot/usdt_telegram_membership/.env` 增加（按实际服务名调整）：
- `WATCHDOG_ENABLE=1`
- `WATCHDOG_MODE=systemd`
- `WATCHDOG_SYSTEMD_UNITS=pvbot.service,pvadmin.service`
- `WATCHDOG_PROJECT_DIR=/opt/pvbot/usdt_telegram_membership`
- `WATCHDOG_HEARTBEAT_MAP=pvbot.service:tmp/heartbeat_app.json`
- `WATCHDOG_HEARTBEAT_MAX_AGE_SEC=300`

