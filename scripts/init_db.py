"""Initialisation de la base de donnees ImmoScan.

Cree l'extension PostGIS, toutes les tables definies dans les modeles
SQLAlchemy, et les index recommandes.

Usage:
    python scripts/init_db.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from sqlalchemy import text

# Ajouter le repertoire racine au path pour les imports
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.db.connection import get_engine
from src.db.models import Base

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def create_postgis_extension(engine) -> None:
    """Cree l'extension PostGIS si elle n'existe pas.

    Args:
        engine: Moteur SQLAlchemy connecte a PostgreSQL.
    """
    with engine.connect() as conn:
        conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
        conn.commit()
    logger.info("Extension PostGIS verifiee/creee.")


def create_tables(engine) -> None:
    """Cree toutes les tables definies dans les modeles.

    Args:
        engine: Moteur SQLAlchemy connecte a PostgreSQL.
    """
    Base.metadata.create_all(bind=engine)
    logger.info("Tables creees avec succes.")

    # Lister les tables creees
    for table_name in Base.metadata.tables:
        logger.info("  - Table: %s", table_name)


def create_additional_indexes(engine) -> None:
    """Cree les index supplementaires recommandes.

    Les index definis dans __table_args__ des modeles sont crees
    automatiquement par create_all(). Cette fonction cree les index
    GIST PostGIS et tout index supplementaire non declaratif.

    Args:
        engine: Moteur SQLAlchemy connecte a PostgreSQL.
    """
    additional_indexes = [
        (
            "idx_annonces_geo",
            "CREATE INDEX IF NOT EXISTS idx_annonces_geo "
            "ON annonces USING GIST(coordonnees)",
        ),
    ]

    with engine.connect() as conn:
        for index_name, ddl in additional_indexes:
            try:
                conn.execute(text(ddl))
                logger.info("Index cree: %s", index_name)
            except Exception:
                logger.warning(
                    "Index %s existe deja ou erreur de creation.", index_name
                )
        conn.commit()

    logger.info("Index supplementaires verifies/crees.")


def main() -> None:
    """Point d'entree principal pour l'initialisation de la base de donnees."""
    logger.info("=== Initialisation de la base de donnees ImmoScan ===")

    try:
        engine = get_engine()

        # 1. Extension PostGIS
        logger.info("Etape 1/3 : Creation de l'extension PostGIS...")
        create_postgis_extension(engine)

        # 2. Creation des tables
        logger.info("Etape 2/3 : Creation des tables...")
        create_tables(engine)

        # 3. Index supplementaires
        logger.info("Etape 3/3 : Creation des index supplementaires...")
        create_additional_indexes(engine)

        logger.info("=== Base de donnees initialisee avec succes ===")

    except Exception:
        logger.exception("Erreur lors de l'initialisation de la base de donnees.")
        sys.exit(1)

    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
