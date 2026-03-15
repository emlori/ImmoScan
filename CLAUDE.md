# ImmoScan Besançon

Outil automatisé de détection et scoring d'opportunités d'investissement locatif à Besançon.

## Objectif

Scraper les annonces immobilières (vente + location), scorer leur rentabilité, et alerter via Telegram quand une opportunité dépasse 8% de rentabilité brute.

## Stack

- **Python 3.11+** (virtualenv `venv/`)
- **Scrapling** avec StealthyFetcher (anti-bot, adaptive)
- **PostgreSQL 15+ / PostGIS 3.x**
- **Apache Airflow 2.8+** (orchestration)
- **Claude API Haiku** (enrichissement IA)
- **Telegram Bot API** (alertes)

## Structure du projet

```
immoscan/
├── CLAUDE.md                    # Ce fichier
├── README.md
├── pyproject.toml
├── .env.example
├── config/
│   ├── sources.yaml             # URLs, sélecteurs CSS, fréquences par source
│   ├── scoring.yaml             # Poids scoring, seuils alertes, décotes négo
│   └── quartiers.yaml           # Zones géo, scores attractivité, loyers fallback
├── src/
│   ├── __init__.py
│   ├── scrapers/
│   │   ├── __init__.py
│   │   ├── base.py              # Classe abstraite BaseScraper
│   │   ├── leboncoin.py         # Scraper LeBonCoin (vente + location)
│   │   ├── pap.py               # Scraper PAP (vente + location)
│   │   └── seloger.py           # Scraper SeLoger (location uniquement en v1)
│   ├── parsers/
│   │   ├── __init__.py
│   │   ├── normalizer.py        # Nettoyage, normalisation des données brutes
│   │   └── dedup.py             # Déduplication intra-source + inter-sources (matching flou)
│   ├── validation/
│   │   ├── __init__.py
│   │   └── validators.py        # Règles de validation à l'ingestion
│   ├── scoring/
│   │   ├── __init__.py
│   │   ├── rentabilite.py       # Calcul rentabilité brute (4 scénarios négo)
│   │   ├── composite.py         # Score composite 0-100 (pondéré)
│   │   └── fiscal.py            # Estimation indicative LMNP vs nu (simplifié)
│   ├── geo/
│   │   ├── __init__.py
│   │   ├── geocoder.py          # API Adresse data.gouv.fr + cache local
│   │   └── scoring_geo.py       # Score localisation (tram, commerces, campus)
│   ├── enrichment/
│   │   ├── __init__.py
│   │   └── claude_enricher.py   # Appel Claude Haiku, prompt structuré, parsing JSON
│   ├── observatoire/
│   │   ├── __init__.py
│   │   └── loyers.py            # Calcul médianes par segment, warm-up, saisonnalité
│   ├── alerts/
│   │   ├── __init__.py
│   │   ├── telegram_bot.py      # Envoi alertes + digest + alertes système
│   │   └── formatter.py         # Formatage Markdown des alertes
│   ├── monitoring/
│   │   ├── __init__.py
│   │   └── health.py            # Métriques scraping, alertes techniques
│   └── db/
│       ├── __init__.py
│       ├── models.py            # SQLAlchemy models
│       ├── connection.py        # Engine PostgreSQL + session management
│       └── migrations/          # Alembic migrations
├── dags/
│   ├── dag_ventes.py            # DAG Airflow : 6x/jour à 7h,11h,13h,16h,18h,20h
│   ├── dag_loyers.py            # DAG Airflow : 1x/jour à 6h
│   ├── dag_digest.py            # DAG Airflow : 1x/jour à 21h
│   └── dag_maintenance.py       # DAG Airflow : 1x/semaine dimanche 3h
├── tests/
│   ├── fixtures/                # Snapshots HTML archivés par source
│   ├── test_parsers.py
│   ├── test_validators.py
│   ├── test_scoring.py
│   ├── test_dedup.py
│   └── test_integration.py
└── scripts/
    ├── init_db.py               # Création tables + extensions PostGIS
    ├── seed_quartiers.py        # Import polygones quartiers + données initiales
    └── backup.sh                # pg_dump automatique
```

## Critères de recherche (ventes)

| Critère | Valeur |
|---------|--------|
| Type | Studio, T1, T2, T3 (appartements) |
| Prix | 30 000 – 160 000€ |
| Surface min Studio/T1 | ≥ 9 m² |
| Surface min T2 | ≥ 20 m² |
| Surface min T3 | ≥ 35 m² |
| DPE | A, B, C, D uniquement |
| Quartiers | Centre-Ville, Battant, Chablais, Rivotte, Grette - Butte, Montrapon, Saint-Claude - Torcols |
| Objectif renta brute | ≥ 8% |
| Marge négo max | 15% |

## Créneaux de scraping

- **Ventes** : 7h, 11h, 13h, 16h, 18h, 20h, 22h (7x/jour)
- **Locations** (Observatoire) : 6h (1x/jour)

## Scoring composite (0-100)

| Critère | Poids | Détail |
|---------|-------|--------|
| Rentabilité brute | 40% | Proportionnel, bonus au-dessus de 8% |
| Localisation | 25% | Quartier + proximité tram/commerces/campus |
| DPE | 15% | A=100, B=85, C=65, D=40 |
| Potentiel négo | 10% | Signaux texte + évolution prix (baisse détectée) |
| Risque vacance | 10% | Quartier + type + saisonnalité Observatoire |

4 scénarios de négociation calculés systématiquement : 0%, -5%, -10%, -15%.

## Niveaux d'alerte Telegram

| Niveau | Condition | Action |
|--------|-----------|--------|
| 🟢 TOP | Score ≥ 80 ou renta ≥ 8% prix affiché | Alerte immédiate |
| 🟡 BON | Score 60-79 ou renta ≥ 8% après négo | Digest toutes les 2h |
| 🔴 VEILLE | Score < 60 | Stocké, pas d'alerte |
| 📉 BAISSE | Baisse de prix détectée | Alerte spécifique |

Digest quotidien à 21h : top 3 du jour, baisses prix, stats pipeline, stats Observatoire.

## Modèle de données

### Tables PostgreSQL + PostGIS

```sql
-- Extension requise
CREATE EXTENSION IF NOT EXISTS postgis;

-- Annonces de vente
CREATE TABLE annonces (
    id SERIAL PRIMARY KEY,
    url_source TEXT NOT NULL UNIQUE,
    source VARCHAR(20) NOT NULL,  -- 'leboncoin', 'pap'
    hash_dedup VARCHAR(64) NOT NULL,
    source_ids JSONB DEFAULT '[]',  -- doublons multi-source détectés
    prix INTEGER NOT NULL,
    surface_m2 FLOAT NOT NULL,
    nb_pieces INTEGER NOT NULL,
    dpe CHAR(1),  -- nullable si non renseigné
    etage INTEGER,
    adresse_brute TEXT,
    quartier VARCHAR(50),
    coordonnees GEOMETRY(POINT, 4326),
    charges_copro FLOAT,
    description_texte TEXT,
    photos_urls JSONB DEFAULT '[]',
    date_publication TIMESTAMP,
    date_scrape TIMESTAMP NOT NULL DEFAULT NOW(),
    date_modification TIMESTAMP,
    historique_prix JSONB DEFAULT '[]',  -- [{date, prix}]
    completude_score FLOAT,
    statut VARCHAR(20) DEFAULT 'nouveau',  -- nouveau/vu/alerté/archivé
    created_at TIMESTAMP DEFAULT NOW()
);

-- Scores
CREATE TABLE scores (
    id SERIAL PRIMARY KEY,
    annonce_id INTEGER REFERENCES annonces(id) ON DELETE CASCADE,
    score_global FLOAT,
    renta_brute_affiche FLOAT,
    renta_brute_nego_5 FLOAT,
    renta_brute_nego_10 FLOAT,
    renta_brute_nego_15 FLOAT,
    score_localisation FLOAT,
    score_dpe FLOAT,
    score_negociation FLOAT,
    score_vacance FLOAT,
    regime_indicatif VARCHAR(10),  -- 'lmnp' ou 'nu'
    loyer_estime FLOAT,
    fiabilite_loyer VARCHAR(20),  -- 'preliminaire' ou 'fiable'
    date_calcul TIMESTAMP DEFAULT NOW()
);

-- Enrichissement IA
CREATE TABLE enrichissement_ia (
    id SERIAL PRIMARY KEY,
    annonce_id INTEGER REFERENCES annonces(id) ON DELETE CASCADE,
    signaux_nego JSONB DEFAULT '[]',
    etat_bien VARCHAR(30),
    equipements JSONB DEFAULT '[]',
    red_flags JSONB DEFAULT '[]',
    info_copro JSONB,
    resume_ia TEXT,
    date_analyse TIMESTAMP DEFAULT NOW()
);

-- Quartiers (référence géographique)
CREATE TABLE quartiers (
    id SERIAL PRIMARY KEY,
    nom VARCHAR(50) NOT NULL,
    polygone GEOMETRY(POLYGON, 4326),
    score_attractivite FLOAT,
    profil_locataire TEXT,
    tension_locative FLOAT
);

-- Observatoire : annonces de location
CREATE TABLE loyers_marche (
    id SERIAL PRIMARY KEY,
    url_source TEXT NOT NULL UNIQUE,
    source VARCHAR(20) NOT NULL,  -- 'leboncoin', 'pap', 'seloger'
    hash_dedup VARCHAR(64) NOT NULL,
    loyer_cc FLOAT NOT NULL,
    loyer_hc FLOAT,
    surface_m2 FLOAT NOT NULL,
    nb_pieces INTEGER NOT NULL,
    meuble BOOLEAN,
    quartier VARCHAR(50),
    coordonnees GEOMETRY(POINT, 4326),
    dpe CHAR(1),
    date_publication TIMESTAMP,
    date_scrape TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Observatoire : médianes calculées par segment
CREATE TABLE loyers_reference (
    id SERIAL PRIMARY KEY,
    quartier VARCHAR(50) NOT NULL,
    type_bien VARCHAR(5) NOT NULL,  -- 'T2', 'T3'
    meuble BOOLEAN NOT NULL,
    loyer_median FLOAT,
    loyer_q1 FLOAT,
    loyer_q3 FLOAT,
    nb_annonces INTEGER,
    fiabilite VARCHAR(20),  -- 'preliminaire' (<5) ou 'fiable' (>=5)
    loyer_m2_median FLOAT,
    date_calcul TIMESTAMP DEFAULT NOW(),
    UNIQUE(quartier, type_bien, meuble)
);

-- Alertes envoyées
CREATE TABLE alertes_log (
    id SERIAL PRIMARY KEY,
    annonce_id INTEGER REFERENCES annonces(id),
    canal VARCHAR(20) DEFAULT 'telegram',
    niveau VARCHAR(20),  -- 'top', 'bon', 'baisse_prix'
    date_envoi TIMESTAMP DEFAULT NOW()
);

-- Monitoring scraping
CREATE TABLE scraping_log (
    id SERIAL PRIMARY KEY,
    source VARCHAR(20) NOT NULL,
    type_scrape VARCHAR(10) NOT NULL,  -- 'vente' ou 'location'
    date_exec TIMESTAMP DEFAULT NOW(),
    nb_annonces_scrapees INTEGER DEFAULT 0,
    nb_nouvelles INTEGER DEFAULT 0,
    nb_erreurs INTEGER DEFAULT 0,
    duree_sec FLOAT,
    proxy_utilise VARCHAR(100),
    erreur_detail TEXT
);

-- Annonces rejetées par la validation
CREATE TABLE validation_log (
    id SERIAL PRIMARY KEY,
    url_source TEXT,
    source VARCHAR(20),
    raison_rejet TEXT,
    donnees_brutes JSONB,
    date_rejet TIMESTAMP DEFAULT NOW()
);
```

### Index recommandés

```sql
CREATE INDEX idx_annonces_statut ON annonces(statut);
CREATE INDEX idx_annonces_quartier ON annonces(quartier);
CREATE INDEX idx_annonces_source ON annonces(source);
CREATE INDEX idx_annonces_prix ON annonces(prix);
CREATE INDEX idx_annonces_geo ON annonces USING GIST(coordonnees);
CREATE INDEX idx_loyers_segment ON loyers_reference(quartier, type_bien, meuble);
CREATE INDEX idx_loyers_marche_source ON loyers_marche(source);
CREATE INDEX idx_scraping_log_date ON scraping_log(date_exec);
```

### Politique de rétention

- Annonces actives : indéfiniment
- Annonces archivées : supprimées après 6 mois
- loyers_marche : 12 mois glissants
- scraping_log : 90 jours
- validation_log : 30 jours

## Règles de validation (ingestion)

| Champ | Règle | Action si invalide |
|-------|-------|-------------------|
| prix | entier > 0, dans [10 000 – 500 000€] | rejet + validation_log |
| surface | float > 0, dans [10 – 300 m²] | rejet + validation_log |
| nb_pieces | entier dans [1 – 10] | rejet + validation_log |
| dpe | char dans [A-G] ou null | accepter avec null |
| url | URL valide, unique en base | rejet si doublon |
| adresse | non vide, contient "Besançon" ou CP 25 | rejet + validation_log |
| loyer (locations) | float > 0, dans [200 – 3 000€] | rejet + validation_log |

Détection anomalies : prix/m² > 2σ de la médiane quartier, surface incohérente vs nb_pieces, annonces pro déguisées.

## Déduplication

- **Intra-source** : hash sur URL canonique
- **Inter-sources** : matching flou sur (adresse normalisée + surface ±2m² + prix ±5%). Doublons liés via `source_ids` JSONB.

## Observatoire Loyers

- Sources : LeBonCoin + PAP + SeLoger (locations), 1x/jour à 6h
- Médiane par segment : quartier × type (T2/T3) × meublé/nu
- Intervalle Q1-Q3, pondération temporelle (décroissance exponentielle)
- Seuil minimum : 5 annonces/segment → fiabilité "fiable", sinon "préliminaire"
- Fallback : si segment < 5 annonces, remonter au quartier global
- Exclusion outliers : loyers hors [Q1 - 1.5×IQR, Q3 + 1.5×IQR]
- Warm-up : ~2-3 semaines, bascule auto après ~200 annonces

## Enrichissement IA (Claude Haiku)

- Appel uniquement sur annonces passant les filtres obligatoires
- Prompt structuré → réponse JSON avec schéma fixe
- Extractions : signaux_nego, etat_bien, equipements, red_flags, info_copro, resume
- Plafond hard : 300 appels/jour max
- Retry avec backoff sur 429/500

Schéma de réponse attendu :

```json
{
  "signaux_nego": ["urgent", "prix à débattre"],
  "etat_bien": "bon_etat",
  "equipements": ["parking", "cave", "double_vitrage"],
  "red_flags": [],
  "info_copro": {"nb_lots": 12, "charges_annuelles": 1200},
  "resume": "T3 lumineux en bon état, parking inclus, copro saine."
}
```

## Fiscalité

Estimation **indicative uniquement** (consultation spécialiste prévue) :
- LMNP micro-BIC : abattement 50%
- Location nue micro-foncier : abattement 30%
- Pas de simulation régime réel dans le code.

## DAGs Airflow

### dag_ventes (6x/jour : 7h, 11h, 13h, 16h, 18h, 20h)

```
scrape_leboncoin ──┐
                   ├──> validate ──> parse_normalize ──> geocode ──> compute_scores ──> enrich_ia ──> send_alerts
scrape_pap ────────┘
```

Chaque tâche en **soft fail** (trigger_rule='all_done'). Si une source échoue, le pipeline continue.

### dag_loyers (1x/jour : 6h)

```
scrape_lbc_location ───┐
scrape_pap_location ───┼──> validate_loyers ──> parse_normalize_loyers ──> compute_medianes
scrape_seloger_loc ────┘
```

### dag_digest (1x/jour : 21h)

Génère et envoie le digest Telegram : top 3, baisses prix, stats pipeline, stats Observatoire.

### dag_maintenance (1x/semaine : dimanche 3h)

Purge données expirées (rétention), VACUUM PostgreSQL, vérification espace disque, test backup.

## Monitoring & alertes système

Alertes techniques envoyées sur le même canal Telegram :

| Événement | Seuil | Action |
|-----------|-------|--------|
| Source indisponible | 3 échecs consécutifs | alerte immédiate |
| Taux parsing en chute | < 50% succès | alerte (changement DOM probable) |
| Zéro nouvelle annonce | 24h sans nouvelle | alerte |
| Espace disque | < 20% restant | alerte |
| Budget API Claude | plafond 300/jour atteint | alerte + pause enrichissement |
| Backup échoué | erreur pg_dump | alerte immédiate |

## Conformité juridique

- Usage **strictement personnel**, non commercial
- Respect robots.txt vérifié au démarrage de chaque DAG
- Délai 2-5 secondes entre requêtes
- Aucun stockage de données personnelles (noms, téléphones, emails des annonceurs)
- Possibilité d'arrêt par source via config (sources.yaml : `enabled: false`)
- Annonces supprimées de la source → archivées 30j puis purgées 6 mois

## Sauvegardes

- pg_dump quotidien à 3h → `/backups/immoscan/`
- Rétention : 7 journaliers + 4 hebdomadaires
- Test restauration mensuel

## Conventions de code

- Python : type hints partout, docstrings Google style
- Formatter : black + isort
- Linter : ruff
- Tests : pytest, fixtures HTML dans `tests/fixtures/`
- Logging : module `logging` standard, niveau INFO par défaut, DEBUG en dev
- Config : pydantic Settings pour validation des .env
- ORM : SQLAlchemy 2.0 style (mapped_column)
- Migrations : Alembic

## Variables d'environnement (.env)

```env
# PostgreSQL
DATABASE_URL=postgresql://immoscan:password@localhost:5432/immoscan

# Claude API
ANTHROPIC_API_KEY=sk-ant-...
ANTHROPIC_MODEL=claude-haiku-4-5-20251001
ANTHROPIC_MAX_DAILY_CALLS=300

# Telegram
TELEGRAM_BOT_TOKEN=123456:ABC-DEF...
TELEGRAM_CHAT_ID=-100123456789

# Proxies
PROXY_POOL_URL=http://proxy-provider-api/...
PROXY_ENABLED=true

# Scraping
SCRAPING_DELAY_MIN=2
SCRAPING_DELAY_MAX=5
SCRAPLING_ADAPTIVE=true
```

## Commandes utiles

```bash
# Setup
python -m venv venv && source venv/bin/activate
pip install -e ".[dev]"
scrapling install

# Base de données
python scripts/init_db.py
python scripts/seed_quartiers.py

# Tests
pytest tests/ -v
pytest tests/test_parsers.py -v  # parsers uniquement

# Airflow (dev)
airflow standalone  # lance webserver + scheduler

# Backup manuel
bash scripts/backup.sh
```
