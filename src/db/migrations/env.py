"""Alembic environment configuration for ImmoScan.

Configure la connexion a la base de donnees et importe les modeles
SQLAlchemy pour que les migrations automatiques fonctionnent.
"""

from __future__ import annotations

import sys
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import engine_from_config, pool

# Ajouter la racine du projet au sys.path pour les imports src.*
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Importer tous les modeles pour que Base.metadata contienne toutes les tables.
# Cet import est necessaire meme s'il semble inutilise : il enregistre
# les modeles dans le registre de metadonnees de Base.
from src.db.models import (  # noqa: F401, E402
    AlerteLog,
    Annonce,
    Base,
    EnrichissementIA,
    LoyerMarche,
    LoyerReference,
    Quartier,
    Score,
    ScrapingLog,
    ValidationLog,
)

# Configuration Alembic
config = context.config

# Configurer le logging depuis alembic.ini
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Metadonnees cibles pour les migrations automatiques
target_metadata = Base.metadata


def get_database_url() -> str:
    """Retourne l'URL de connexion a la base de donnees.

    Tente de charger l'URL depuis les settings pydantic. En cas d'echec
    (import ou configuration manquante), utilise la variable d'environnement
    directement ou une valeur par defaut.

    Returns:
        URL de connexion PostgreSQL.
    """
    try:
        from src.config import get_settings

        settings = get_settings()
        return settings.database.database_url
    except Exception:
        import os

        return os.getenv(
            "DATABASE_URL",
            "postgresql://immoscan:password@localhost:5432/immoscan",
        )


def run_migrations_offline() -> None:
    """Execute les migrations en mode 'offline'.

    Genere le SQL sans connexion a la base de donnees.
    Utile pour generer des scripts SQL a executer manuellement.
    """
    url = get_database_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
        compare_server_default=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Execute les migrations en mode 'online'.

    Cree une connexion a la base de donnees et execute les migrations
    dans une transaction.
    """
    # Injecter l'URL dans la configuration Alembic
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = get_database_url()

    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            compare_server_default=True,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
