#!/usr/bin/env bash
# ImmoScan - Script de deploiement VPS
# Execute par GitHub Actions via SSH apres un push sur master.
set -euo pipefail

APP_DIR="/opt/immoscan"
VENV_DIR="$APP_DIR/venv"
LOG_DIR="$APP_DIR/logs"

echo "=== ImmoScan Deploy $(date '+%Y-%m-%d %H:%M:%S') ==="

cd "$APP_DIR"

# 1. Pull latest code
echo "[1/5] Git pull..."
git fetch origin master
git reset --hard origin/master

# 2. Install/update dependencies
echo "[2/5] Install dependencies..."
source "$VENV_DIR/bin/activate"
pip install -q -r requirements.txt

# 3. Run database migrations (if any)
echo "[3/5] Database migrations..."
if [ -f "alembic.ini" ]; then
    alembic upgrade head 2>/dev/null || echo "  (pas de migration en attente)"
fi

# 4. Create log directory
mkdir -p "$LOG_DIR"

# 5. Restart systemd timers
echo "[4/5] Restart systemd timers..."
sudo systemctl daemon-reload
sudo systemctl restart immoscan-ventes.timer
sudo systemctl restart immoscan-loyers.timer
sudo systemctl enable immoscan-ventes.timer
sudo systemctl enable immoscan-loyers.timer

# 6. Verify
echo "[5/5] Status..."
systemctl is-active --quiet immoscan-ventes.timer && echo "  ventes.timer: active" || echo "  ventes.timer: FAILED"
systemctl is-active --quiet immoscan-loyers.timer && echo "  loyers.timer: active" || echo "  loyers.timer: FAILED"

echo "=== Deploy OK ==="
