#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "========================================================="
echo "🚀 SignalStack Challenger Trading Agent Fly.io Deployment"
echo "========================================================="

# 1. Check Fly CLI
if ! command -v flyctl &> /dev/null; then
    echo "❌ flyctl is not installed or not in PATH."
    exit 1
fi

echo "Checking Fly authentication..."
if ! flyctl auth whoami &> /dev/null; then
    echo "⚠️ Fly session expired or not authenticated. Please run:"
    echo "   flyctl auth login"
    exit 1
fi

# 2. Check or create volume
APP_NAME=$(grep '^app =' fly.toml | cut -d'"' -f2)
echo "App name: $APP_NAME"

echo "Checking if volume 'signalstack_challenger_data' exists..."
if ! flyctl volumes list -a "$APP_NAME" 2>/dev/null | grep -q "signalstack_challenger_data"; then
    echo "Creating persistent volume 'signalstack_challenger_data' (region: ord, size: 1GB)..."
    flyctl volumes create signalstack_challenger_data --app "$APP_NAME" --region ord --size 1 --yes
else
    echo "✅ Volume 'signalstack_challenger_data' already exists."
fi

# 3. Set secrets (load from .env if present)
if [ -f .env ]; then
    echo "Loading secrets from .env..."
    # shellcheck disable=SC1091
    set -a
    source .env
    set +a
fi

SECRETS_TO_SET=()
[ -n "$SIGNAL_SECRET" ] && SECRETS_TO_SET+=("SIGNAL_SECRET=$SIGNAL_SECRET")
[ -n "$SIGNALSTACK_WEBHOOK_URL" ] && SECRETS_TO_SET+=("SIGNALSTACK_WEBHOOK_URL=$SIGNALSTACK_WEBHOOK_URL")
[ -n "$NEON_DATABASE_URL" ] && SECRETS_TO_SET+=("NEON_DATABASE_URL=$NEON_DATABASE_URL")

if [ ${#SECRETS_TO_SET[@]} -gt 0 ]; then
    echo "Setting secrets on Fly..."
    flyctl secrets set "${SECRETS_TO_SET[@]}" --app "$APP_NAME"
else
    echo "ℹ️  No secrets passed via .env or environment variables."
    echo "   Ensure secrets are set in Fly via: flyctl secrets set KEY=VALUE --app $APP_NAME"
fi

# 4. Deploy
echo "Deploying to Fly.io..."
flyctl deploy --config fly.toml

echo "========================================================="
echo "✅ Deployment complete!"
echo "Dashboard: https://${APP_NAME}.fly.dev/"
echo "Health:    https://${APP_NAME}.fly.dev/health"
echo "Status:    https://${APP_NAME}.fly.dev/status"
echo "Account:   https://${APP_NAME}.fly.dev/account"
echo "========================================================="
