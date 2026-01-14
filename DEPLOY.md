# 部署说明

## 方式 A：Docker Compose（推荐）

1）准备 `.env`（从 `.env.example` 复制）  
2）如使用 compose 自带 MySQL，把 `.env` 里的 `DB_HOST` 改为 `mysql`，并保持 `DB_USER/DB_PASS/DB_NAME` 与 `docker-compose.yml` 一致  
3）启动

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
2）代码放到：`/opt/pvbot`  
3）创建虚拟环境并安装依赖（核心链路）

```bash
cd /opt/pvbot
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

如要启用邀请海报，再安装：

```bash
pip install -r requirements-media.txt
```

4）放置 `.env` 到 `/opt/pvbot/.env`  
5）安装 systemd 服务

```bash
sudo cp /opt/pvbot/deploy/pvbot.service /etc/systemd/system/pvbot.service
sudo systemctl daemon-reload
sudo systemctl enable --now pvbot
sudo systemctl status pvbot
```

日志文件：
- `/opt/pvbot/logs/runtime.log`
- `/opt/pvbot/logs/bot.log`

### 管理后台（可选）

安装服务：

```bash
sudo cp /opt/pvbot/deploy/pvadmin.service /etc/systemd/system/pvadmin.service
sudo systemctl daemon-reload
sudo systemctl enable --now pvadmin
sudo systemctl status pvadmin
```

注意：
- 需要在 `/opt/pvbot/.env` 里设置 `ADMIN_WEB_USER/ADMIN_WEB_PASS`
- 如需只允许本机访问，将 `ADMIN_WEB_HOST` 设为 `127.0.0.1`

