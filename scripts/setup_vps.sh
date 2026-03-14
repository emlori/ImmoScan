#!/usr/bin/env bash
# ImmoScan - Setup initial du VPS (a lancer UNE SEULE FOIS en root)
set -euo pipefail

APP_DIR="/opt/immoscan"
DEPLOY_USER="deploy"
REPO_URL="https://github.com/emlori/ImmoScan.git"

echo "=== ImmoScan VPS Setup ==="

# 1. System packages
echo "[1/7] Installation des paquets systeme..."
apt-get update -qq
apt-get install -y -qq python3.11 python3.11-venv python3-pip \
    postgresql postgresql-contrib postgis \
    git curl

# 2. Create deploy user (if not exists)
echo "[2/7] Utilisateur deploy..."
if ! id "$DEPLOY_USER" &>/dev/null; then
    useradd -m -s /bin/bash "$DEPLOY_USER"
    echo "  Utilisateur '$DEPLOY_USER' cree"
fi

# Allow deploy user to restart immoscan services without password
cat > /etc/sudoers.d/immoscan <<'SUDOERS'
deploy ALL=(ALL) NOPASSWD: /bin/systemctl daemon-reload
deploy ALL=(ALL) NOPASSWD: /bin/systemctl restart immoscan-*
deploy ALL=(ALL) NOPASSWD: /bin/systemctl enable immoscan-*
deploy ALL=(ALL) NOPASSWD: /bin/systemctl stop immoscan-*
deploy ALL=(ALL) NOPASSWD: /bin/systemctl start immoscan-*
SUDOERS
chmod 440 /etc/sudoers.d/immoscan

# 3. Clone repo
echo "[3/7] Clone du repository..."
if [ -d "$APP_DIR" ]; then
    echo "  $APP_DIR existe deja, skip clone"
else
    git clone "$REPO_URL" "$APP_DIR"
    chown -R "$DEPLOY_USER:$DEPLOY_USER" "$APP_DIR"
fi

# 4. Setup virtualenv
echo "[4/7] Virtualenv Python..."
sudo -u "$DEPLOY_USER" python3.11 -m venv "$APP_DIR/venv"
sudo -u "$DEPLOY_USER" bash -c "
    source $APP_DIR/venv/bin/activate
    pip install -q -r $APP_DIR/requirements.txt
"

# 5. Setup PostgreSQL
echo "[5/7] PostgreSQL..."
sudo -u postgres psql -tc "SELECT 1 FROM pg_roles WHERE rolname='immoscan'" | grep -q 1 || \
    sudo -u postgres createuser immoscan
sudo -u postgres psql -tc "SELECT 1 FROM pg_database WHERE datname='immoscan'" | grep -q 1 || \
    sudo -u postgres createdb -O immoscan immoscan
sudo -u postgres psql -d immoscan -c "CREATE EXTENSION IF NOT EXISTS postgis;" 2>/dev/null

# 6. Install systemd units
echo "[6/7] Installation des services systemd..."
cp "$APP_DIR/deploy/immoscan-ventes.service" /etc/systemd/system/
cp "$APP_DIR/deploy/immoscan-ventes.timer"   /etc/systemd/system/
cp "$APP_DIR/deploy/immoscan-loyers.service" /etc/systemd/system/
cp "$APP_DIR/deploy/immoscan-loyers.timer"   /etc/systemd/system/
systemctl daemon-reload
systemctl enable immoscan-ventes.timer
systemctl enable immoscan-loyers.timer
systemctl start immoscan-ventes.timer
systemctl start immoscan-loyers.timer

# 7. Create directories
echo "[7/7] Repertoires..."
mkdir -p "$APP_DIR/logs" /backups/immoscan
chown -R "$DEPLOY_USER:$DEPLOY_USER" "$APP_DIR/logs" /backups/immoscan

echo ""
echo "=== Setup OK ==="
echo ""
echo "ACTIONS MANUELLES REQUISES :"
echo "  1. Creer le fichier $APP_DIR/.env (copier depuis .env.example)"
echo "  2. Configurer le mot de passe PostgreSQL pour 'immoscan'"
echo "  3. Lancer: python scripts/init_db.py"
echo "  4. Ajouter la cle SSH du deploy user dans GitHub Secrets"
echo "  5. Configurer les GitHub Secrets : VPS_HOST, VPS_USER, VPS_SSH_KEY, VPS_PORT"
echo ""
