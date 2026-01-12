set -e

APP_DIR="${APP_DIR:-/opt/pvbot}"
BRANCH="${BRANCH:-main}"

cd "$APP_DIR"

mkdir -p logs tmp

git fetch origin "$BRANCH"
git reset --hard "origin/$BRANCH"

cd "$APP_DIR/deploy"
docker compose up -d --build
docker compose ps

