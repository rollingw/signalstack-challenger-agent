#!/usr/bin/env bash
set -e

export PATH="$HOME/.fly/bin:$PATH"

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
if ! flyctl auth whoami </dev/null &> /dev/null; then
    echo "⚠️ Fly session expired. Starting interactive login..."
    echo "   (Open the link below in your browser, approve, and paste the verification code if prompted)"
    flyctl auth login
fi

# 2. App name
APP_NAME=$(grep '^app =' fly.toml | cut -d'"' -f2)
echo "App name: $APP_NAME"

# 3. Optional secret sync (only when --sync-secrets is passed, to avoid double machine restarts)
if [[ "${1:-}" == "--sync-secrets" ]]; then
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
    fi
fi

# 4. Deploy using local Docker daemon (avoids remote builder WireGuard hangs)
echo "Deploying to Fly.io using local Docker build (--local-only)..."
flyctl deploy --config fly.toml --local-only

echo "========================================================="
echo "✅ Deployment complete!"
echo "Dashboard: https://${APP_NAME}.fly.dev/"
echo "Health:    https://${APP_NAME}.fly.dev/health"
echo "Status:    https://${APP_NAME}.fly.dev/status"
echo "Account:   https://${APP_NAME}.fly.dev/account"
echo "========================================================="
