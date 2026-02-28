#!/bin/bash
# Usage: ./dev.sh [start|stop]
# Manages the local dev server for conference-matcher

set -e

SERVICE_NAME="conference-matcher"
APP_DIR="$(cd "$(dirname "$0")" && pwd)"

case "${1:-start}" in
  start)
    # Stop any existing instance
    sprite-env services stop "$SERVICE_NAME" 2>/dev/null || true

    echo "Starting dev server..."
    sprite-env services create "$SERVICE_NAME" \
      --command "cd $APP_DIR && python app.py" \
      --port 8080 \
      2>/dev/null || true

    sprite-env services start "$SERVICE_NAME"
    echo "Dev server running. Access via Sprite's public URL on port 8080."
    ;;

  stop)
    echo "Stopping dev server..."
    sprite-env services stop "$SERVICE_NAME" 2>/dev/null || true
    echo "Dev server stopped."
    ;;

  *)
    echo "Usage: ./dev.sh [start|stop]"
    exit 1
    ;;
esac
