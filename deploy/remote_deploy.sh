set -e

APP_DIR="${APP_DIR:-/opt/pvbot}"
BRANCH="${BRANCH:-main}"

cd "$APP_DIR"

mkdir -p logs tmp

git fetch origin "$BRANCH"
git reset --hard "origin/$BRANCH"
git rev-parse --short HEAD

cd "$APP_DIR/deploy"
if [ "${NO_CACHE:-0}" = "1" ]; then
  docker compose build --no-cache
fi
docker compose up -d --build
docker compose ps
