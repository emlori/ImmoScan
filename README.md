# ImmoScan Besançon

Outil automatisé de détection et scoring d'opportunités d'investissement locatif à Besançon.

## Fonctionnalités

- **Scraping multi-sources** : LeBonCoin, PAP, SeLoger (via Scrapling avec StealthyFetcher anti-bot)
- **Scoring composite 0-100** : rentabilité brute, localisation, DPE, potentiel négo, risque vacance
- **Observatoire des loyers** : médianes par quartier/type/meublé, calcul automatique
- **4 scénarios de négociation** : prix affiché, -5%, -10%, -15%
- **Enrichissement IA** : analyse des annonces via Claude Haiku (signaux négo, red flags, état du bien)
- **Alertes Telegram** : notifications immédiates pour les meilleures opportunités + digest quotidien
- **Géocodage** : API Adresse data.gouv.fr + scoring proximité (tram, commerces, campus)
- **Orchestration Airflow** : DAGs automatisés (ventes 6x/jour, locations 1x/jour, digest, maintenance)

## Stack technique

| Composant | Technologie |
|-----------|------------|
| Langage | Python 3.11+ |
| Scraping | Scrapling (StealthyFetcher) |
| Base de données | PostgreSQL 15+ / PostGIS 3.x |
| ORM | SQLAlchemy 2.0 |
| Migrations | Alembic |
| Orchestration | Apache Airflow 2.8+ |
| IA | Claude API (Haiku) |
| Alertes | Telegram Bot API |
| Config | Pydantic Settings |

## Installation

```bash
# Cloner le repo
git clone https://github.com/emlori/ImmoScan.git
cd ImmoScan

# Créer l'environnement virtuel
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows

# Installer les dépendances
pip install -e ".[dev]"

# Installer les navigateurs Scrapling
scrapling install

# Configurer les variables d'environnement
cp .env.example .env
# Éditer .env avec vos credentials
```

## Configuration

```bash
# Initialiser la base de données (PostgreSQL + PostGIS requis)
python scripts/init_db.py

# Charger les données de référence des quartiers
python scripts/seed_quartiers.py
```

## Utilisation

```bash
# Lancer les tests
pytest tests/ -v

# Lancer Airflow en mode développement
airflow standalone
```

## Critères de recherche

| Critère | Valeur |
|---------|--------|
| Type | T2, T3 (appartements) |
| Prix | 120 000 – 160 000€ |
| DPE | A, B, C, D |
| Quartiers | Centre-Ville, Battant, Chablais |
| Objectif rentabilité brute | ≥ 8% |

## Architecture

```
src/
├── scrapers/       # Collecte des annonces (LeBonCoin, PAP, SeLoger)
├── parsers/        # Normalisation et déduplication
├── validation/     # Règles de validation à l'ingestion
├── scoring/        # Rentabilité, score composite, fiscalité indicative
├── geo/            # Géocodage et scoring localisation
├── enrichment/     # Enrichissement IA (Claude Haiku)
├── observatoire/   # Observatoire des loyers du marché
├── alerts/         # Alertes et formatting Telegram
├── monitoring/     # Health checks et métriques
└── db/             # Modèles SQLAlchemy et migrations Alembic
```

## Licence

MIT
