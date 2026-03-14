# ImmoScan - Architecture de Deploiement

## Vue d'ensemble

```
Developer PC                   GitHub                         VPS (Production)
+-------------+     push     +----------------+    SSH      +------------------+
|  git push   | -----------> | GitHub Actions | --------->  | /opt/immoscan    |
|  master     |              |  1. Lint (ruff) |            |  git pull        |
+-------------+              |  2. Tests      |            |  pip install     |
                             |  3. Deploy SSH |            |  systemd restart |
                             +----------------+            +------------------+
                                                                    |
                                                           +--------+--------+
                                                           |                 |
                                                    systemd timers    PostgreSQL/PostGIS
                                                           |
                                              +------------+------------+
                                              |            |            |
                                         ventes.timer  loyers.timer  (cron optionnel)
                                         6x/jour       1x/jour 6h
                                         7,11,13,      scrape
                                         16,18,20h     locations
```

## Pipeline CI/CD (GitHub Actions)

### Trigger
- Push sur la branche `master`

### Jobs
1. **lint** : Verification du code avec `ruff`
2. **deploy** : Connexion SSH au VPS, pull du code, install des deps, restart des services

### Pourquoi pas de job `test` complet ?
Les tests d'integration necessitent PostgreSQL/PostGIS. On ne lance que le linting en CI.
Les tests complets se lancent en local avant le push.

## Architecture VPS

### Pas de Docker, pas d'Airflow
- **systemd timers** remplacent Airflow (plus leger pour un usage personnel)
- Deploiement direct dans `/opt/immoscan` avec virtualenv
- PostgreSQL natif sur le VPS

### Services systemd

| Service | Type | Planification | Script |
|---------|------|--------------|--------|
| `immoscan-ventes` | oneshot | 7h, 11h, 13h, 16h, 18h, 20h | `scripts/run_pipeline_live.py` |
| `immoscan-loyers` | oneshot | 6h | `scripts/run_loyers.py` |

### Arborescence VPS

```
/opt/immoscan/           # Clone du repo
  venv/                  # Virtualenv Python 3.11+
  .env                   # Variables d'environnement (PAS dans git)
  logs/                  # Logs des executions
/etc/systemd/system/
  immoscan-ventes.service
  immoscan-ventes.timer
  immoscan-loyers.service
  immoscan-loyers.timer
```

## Secrets GitHub requis

| Secret | Description | Exemple |
|--------|-------------|---------|
| `VPS_HOST` | IP ou hostname du VPS | `91.234.56.78` |
| `VPS_USER` | Utilisateur SSH | `deploy` |
| `VPS_SSH_KEY` | Cle privee SSH (ed25519) | `-----BEGIN OPENSSH...` |
| `VPS_PORT` | Port SSH | `22` |

## Setup initial du VPS (une seule fois)

```bash
# Sur le VPS, en tant que root :
bash <(curl -s https://raw.githubusercontent.com/emlori/ImmoScan/master/scripts/setup_vps.sh)
```

Ou manuellement :
```bash
sudo bash scripts/setup_vps.sh
```

## Deploiement manuel (si besoin)

```bash
ssh deploy@VPS_IP "cd /opt/immoscan && bash scripts/deploy.sh"
```

## Rollback

```bash
ssh deploy@VPS_IP "cd /opt/immoscan && git checkout HEAD~1 && bash scripts/deploy.sh"
```

## Logs et monitoring

```bash
# Voir les logs du dernier run ventes
journalctl -u immoscan-ventes.service --no-pager -n 50

# Voir les prochaines executions planifiees
systemctl list-timers immoscan-*

# Voir le statut
systemctl status immoscan-ventes.timer immoscan-loyers.timer
```
