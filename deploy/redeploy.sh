set -euo pipefail

APP_DIR="${APP_DIR:-/opt/pvbot/usdt_telegram_membership}"
BRANCH="${BRANCH:-main}"
NO_CACHE="${NO_CACHE:-1}"
CLEAN="${CLEAN:-0}"

cd "$APP_DIR"
git fetch origin "$BRANCH"
git reset --hard "origin/$BRANCH"
git rev-parse --short HEAD

cd "$APP_DIR/deploy"
docker compose down
if [ "$CLEAN" = "1" ]; then
  docker builder prune -af || true
  docker system prune -af || true
fi
if [ "$NO_CACHE" = "1" ]; then
  docker compose build --no-cache app
fi
docker compose up -d --force-recreate
docker compose ps
