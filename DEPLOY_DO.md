# DigitalOcean 部署（Docker Compose + GitHub 自动同步）

## 1）创建 Droplet
- 系统：Ubuntu 22.04/24.04
- 规格：2vCPU / 2GB 内存起（剪辑+Bot 建议 2GB+）
- Authentication：选择 SSH Key（不要用密码登录）

## 2）登录服务器并安装 Docker

```bash
sudo apt-get update -y
sudo apt-get install -y ca-certificates curl gnupg git

sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo $VERSION_CODENAME) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt-get update -y
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

sudo usermod -aG docker $USER
newgrp docker
docker version
docker compose version
```

## 3）准备项目目录并拉取代码

```bash
mkdir -p /opt/pvbot/usdt_telegram_membership
cd /opt/pvbot/usdt_telegram_membership
git clone https://github.com/<你的账号>/<你的仓库>.git .
```

## 4）准备配置文件（不要提交敏感信息到 GitHub）

```bash
cp .env.example .env
```

推荐做法（减少每次更新都要核对 `.env`）：
- 非敏感配置：放到 `config/app_config.json`
- 敏感配置（token/密码/session）：放到 `.env.secrets`

```bash
cp config/app_config.example.json config/app_config.json
cp .env.secrets.example .env.secrets
```

建议配置：
- `BOT_TOKEN`、`BOT_USERNAME`
- `PAID_CHANNEL_ID`、`HIGHLIGHT_CHANNEL_ID`、`FREE_CHANNEL_ID_1/2`
- `ADMIN_USER_IDS`
- `TRONGRID_API_KEY`
- `MIN_TX_AGE_SEC`

使用 compose 自带 MySQL 时，把 `.env` 里的 `DB_HOST` 设置为：`mysql`

并确保数据库账号密码一致（两处要对齐）：
- 应用侧：`DB_USER/DB_PASS/DB_NAME`
- MySQL 容器侧：`MYSQL_USER/MYSQL_PASSWORD/MYSQL_DATABASE`

## 5）启动（第一次）

```bash
cd /opt/pvbot/usdt_telegram_membership/deploy
docker compose up -d --build
docker compose logs -f app
```

也可以用一键脚本：

```bash
bash /opt/pvbot/usdt_telegram_membership/deploy/ctl.sh up
bash /opt/pvbot/usdt_telegram_membership/deploy/ctl.sh logs app
```

## 6）健康检查与常用命令

```bash
cd /opt/pvbot/usdt_telegram_membership/deploy
docker compose ps
docker compose logs -f app
docker compose restart app
docker compose pull
docker compose up -d --build
```

一键更新代码并强制重建重启（推荐排障用）：

```bash
APP_DIR=/opt/pvbot/usdt_telegram_membership bash /opt/pvbot/usdt_telegram_membership/deploy/redeploy.sh
```

## 7）GitHub 自动同步到服务器（CI/CD）

### 7.1 服务器侧：准备 SSH 部署 Key

在服务器生成一个仅用于拉取代码的 key（推荐）：

```bash
ssh-keygen -t ed25519 -C "pvbot-deploy" -f /opt/pvbot/.ssh/deploy_key -N ""
cat /opt/pvbot/.ssh/deploy_key.pub
```

把输出的公钥加到 GitHub：
- 方式 A：加到仓库 Deploy keys（只读）
- 方式 B：加到你的 GitHub 账号 SSH keys（可读写，不推荐）

然后在服务器设置 git 使用该 key：

```bash
git config --global core.sshCommand "ssh -i /opt/pvbot/.ssh/deploy_key -o IdentitiesOnly=yes"
```

### 7.2 GitHub Actions：配置 Secrets

仓库 Settings → Secrets and variables → Actions → New repository secret：
- `DO_HOST`：服务器 IP
- `DO_USER`：服务器用户名（如 root 或 ubuntu）
- `DO_SSH_KEY`：你本地用于登录服务器的私钥内容（不是 deploy_key）
- `APP_DIR`：`/opt/pvbot/usdt_telegram_membership`
- 可选：`DO_SSH_PORT`：默认 22

### 7.3 push 后自动部署

当你 push 到 `main` 分支，Actions 会：
- SSH 登录服务器
- `git fetch && reset --hard origin/main`
- `docker compose up -d --build`

对应工作流文件见：`.github/workflows/deploy.yml`

