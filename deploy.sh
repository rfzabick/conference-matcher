#!/bin/bash
# Deploy conference-matcher to Fly.io
# Stops local dev server first, then deploys

set -e

APP_DIR="$(cd "$(dirname "$0")" && pwd)"

# Stop local dev server if running
sprite-env services stop conference-matcher 2>/dev/null || true

echo "Deploying to Fly.io..."
cd "$APP_DIR"
fly deploy
