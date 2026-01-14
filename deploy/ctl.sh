#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_DIR="$ROOT_DIR/deploy"
COMPOSE_FILE="$COMPOSE_DIR/docker-compose.yml"

if [[ ! -f "$COMPOSE_FILE" ]]; then
  echo "missing: $COMPOSE_FILE" >&2
  exit 2
fi

cd "$COMPOSE_DIR"

cmd="${1:-help}"
shift || true

case "$cmd" in
  up)
    docker compose up -d --build "$@"
    ;;
  down)
    docker compose down "$@"
    ;;
  ps)
    docker compose ps "$@"
    ;;
  logs)
    svc="${1:-app}"
    shift || true
    docker compose logs -f "$svc" "$@"
    ;;
  restart)
    svc="${1:-app}"
    shift || true
    docker compose restart "$svc" "$@"
    ;;
  admin-up)
    docker compose --profile admin up -d --build "$@"
    ;;
  admin-logs)
    docker compose logs -f admin "$@"
    ;;
  pull)
    docker compose pull "$@"
    ;;
  build)
    docker compose build "$@"
    ;;
  *)
    echo "usage: deploy/ctl.sh <cmd>"
    echo ""
    echo "cmd:"
    echo "  up              up -d --build"
    echo "  down            down"
    echo "  ps              ps"
    echo "  logs [svc]      logs -f (default: app)"
    echo "  restart [svc]   restart (default: app)"
    echo "  pull            pull"
    echo "  build           build"
    echo "  admin-up        --profile admin up -d --build"
    echo "  admin-logs      logs -f admin"
    exit 2
    ;;
esac

