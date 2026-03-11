"""Import des quartiers de reference dans la base de donnees.

Charge les quartiers depuis config/quartiers.yaml, cree des polygones
approximatifs pour chaque zone et insere les donnees dans la table quartiers.

Les polygones sont des approximations basees sur les centres des quartiers
de Besancon, suffisantes pour le geocoding et le scoring.

Usage:
    python scripts/seed_quartiers.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

from geoalchemy2 import WKTElement
from sqlalchemy import select

# Ajouter le repertoire racine au path pour les imports
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.config import get_settings
from src.db.connection import get_session
from src.db.models import Quartier

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Polygones approximatifs des quartiers de Besancon (WGS84)
# Chaque polygone est defini par 5 points (rectangle ferme)
# bases sur les centres de chaque quartier avec un rayon d'environ 500m.
QUARTIER_POLYGONS: dict[str, str] = {
    "Centre-Ville": (
        "POLYGON(("
        "6.0181 47.2338, "
        "6.0301 47.2338, "
        "6.0301 47.2418, "
        "6.0181 47.2418, "
        "6.0181 47.2338"
        "))"
    ),
    "Battant": (
        "POLYGON(("
        "6.0110 47.2370, "
        "6.0250 47.2370, "
        "6.0250 47.2450, "
        "6.0110 47.2450, "
        "6.0110 47.2370"
        "))"
    ),
    "Chablais": (
        "POLYGON(("
        "6.0240 47.2310, "
        "6.0380 47.2310, "
        "6.0380 47.2390, "
        "6.0240 47.2390, "
        "6.0240 47.2310"
        "))"
    ),
}


def seed_quartiers() -> None:
    """Insere les quartiers de reference dans la base de donnees.

    Charge la configuration depuis quartiers.yaml, associe les polygones
    approximatifs, et insere ou met a jour les quartiers en base.
    """
    settings = get_settings()
    quartiers_config = settings.load_quartiers()
    quartiers_data = quartiers_config.get("quartiers", {})

    if not quartiers_data:
        logger.error("Aucun quartier trouve dans config/quartiers.yaml.")
        sys.exit(1)

    with get_session() as session:
        inserted = 0
        skipped = 0

        for key, config in quartiers_data.items():
            nom = config["nom"]

            # Verifier si le quartier existe deja
            existing = session.execute(
                select(Quartier).where(Quartier.nom == nom)
            ).scalar_one_or_none()

            if existing is not None:
                logger.info("Quartier '%s' existe deja (id=%d), ignore.", nom, existing.id)
                skipped += 1
                continue

            # Recuperer le polygone approximatif
            polygon_wkt = QUARTIER_POLYGONS.get(nom)
            polygone = WKTElement(polygon_wkt, srid=4326) if polygon_wkt else None

            quartier = Quartier(
                nom=nom,
                polygone=polygone,
                score_attractivite=config.get("score_attractivite"),
                profil_locataire=config.get("profil_locataire"),
                tension_locative=config.get("tension_locative"),
            )
            session.add(quartier)
            inserted += 1
            logger.info(
                "Quartier '%s' insere (attractivite=%.0f, tension=%.2f).",
                nom,
                config.get("score_attractivite", 0),
                config.get("tension_locative", 0),
            )

        logger.info(
            "Seed termine : %d inseres, %d ignores (deja existants).",
            inserted,
            skipped,
        )


def main() -> None:
    """Point d'entree principal pour le seeding des quartiers."""
    logger.info("=== Seeding des quartiers ImmoScan ===")

    try:
        seed_quartiers()
        logger.info("=== Seeding termine avec succes ===")
    except Exception:
        logger.exception("Erreur lors du seeding des quartiers.")
        sys.exit(1)


if __name__ == "__main__":
    main()
