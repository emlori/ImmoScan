"""DAG Airflow pour l'observatoire des loyers ImmoScan.

Scrape les annonces de location (LeBonCoin, PAP, SeLoger), valide,
normalise et calcule les medianes de loyers par segment
(quartier x type_bien x meuble).

Planification : 1x/jour a 6h.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------
# Configuration du DAG
# ------------------------------------------------------------------

DEFAULT_ARGS: dict[str, Any] = {
    "owner": "immoscan",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

SOURCES_LOCATION: list[str] = ["leboncoin", "pap", "seloger"]

# Segments de l'observatoire a calculer
QUARTIERS: list[str] = ["Centre-Ville", "Battant", "Chablais"]
TYPES_BIEN: list[str] = ["T2", "T3"]
MEUBLE_OPTIONS: list[bool] = [True, False]


# ------------------------------------------------------------------
# Fonctions des taches
# ------------------------------------------------------------------


def scrape_location_source(source_name: str, **context: Any) -> list[dict[str, Any]]:
    """Scrape les annonces de location depuis une source donnee.

    Args:
        source_name: Nom de la source ('leboncoin', 'pap' ou 'seloger').
        **context: Contexte Airflow avec TaskInstance pour XCom.

    Returns:
        Liste de dictionnaires contenant les annonces de location brutes.
    """
    start_time = time.time()
    results: list[dict[str, Any]] = []

    try:
        if source_name == "leboncoin":
            from src.scrapers.leboncoin import LeBonCoinScraper
            scraper = LeBonCoinScraper()
        elif source_name == "pap":
            from src.scrapers.pap import PAPScraper
            scraper = PAPScraper()
        elif source_name == "seloger":
            from src.scrapers.seloger import SeLogerScraper
            scraper = SeLogerScraper()
        else:
            logger.error("Source inconnue: %s", source_name)
            return results

        results = scraper.scrape(scrape_type="location")
        stats = scraper.get_scraping_stats()

        logger.info(
            "Scraping location %s termine: %d annonces, %d erreurs, %.1fs",
            source_name,
            stats.get("nb_annonces_scrapees", 0),
            stats.get("nb_erreurs", 0),
            time.time() - start_time,
        )

        _log_scraping_stats(source_name, "location", stats, time.time() - start_time)

    except Exception:
        logger.error(
            "Erreur lors du scraping location de %s",
            source_name,
            exc_info=True,
        )
        _log_scraping_error(source_name, "location", time.time() - start_time)

    ti = context.get("ti")
    if ti is not None:
        ti.xcom_push(key=f"raw_location_{source_name}", value=results)

    return results


def validate_loyers(**context: Any) -> list[dict[str, Any]]:
    """Valide les annonces de location brutes.

    Applique les regles de validation specifiques aux locations
    (loyer dans [200-3000], surface, nb_pieces, adresse Besancon).

    Args:
        **context: Contexte Airflow avec TaskInstance pour XCom.

    Returns:
        Liste des annonces de location validees.
    """
    from src.validation.validators import AnnonceValidator

    ti = context.get("ti")
    validator = AnnonceValidator()
    validated: list[dict[str, Any]] = []

    all_raw: list[dict[str, Any]] = []
    for source_name in SOURCES_LOCATION:
        raw_data = ti.xcom_pull(
            key=f"raw_location_{source_name}",
            task_ids=f"scrape_{source_name}_location",
        )
        if raw_data:
            all_raw.extend(raw_data)

    logger.info("Validation de %d annonces de location brutes", len(all_raw))

    for annonce in all_raw:
        is_valid, reasons = validator.validate_location(annonce)
        if is_valid:
            validated.append(annonce)
        else:
            _log_validation_reject(annonce, reasons)

    logger.info(
        "Validation locations terminee: %d/%d validees",
        len(validated),
        len(all_raw),
    )

    ti.xcom_push(key="validated_loyers", value=validated)
    return validated


def parse_normalize_loyers(**context: Any) -> list[dict[str, Any]]:
    """Normalise les annonces de location validees.

    Applique la normalisation specifique aux locations (loyer, surface,
    detection meuble, adresse).

    Args:
        **context: Contexte Airflow avec TaskInstance pour XCom.

    Returns:
        Liste des annonces de location normalisees.
    """
    from src.parsers.normalizer import AnnonceNormalizer

    ti = context.get("ti")
    validated = ti.xcom_pull(key="validated_loyers", task_ids="validate_loyers")

    if not validated:
        logger.warning("Aucune annonce de location validee a normaliser")
        ti.xcom_push(key="normalized_loyers", value=[])
        return []

    normalizer = AnnonceNormalizer()
    normalized: list[dict[str, Any]] = []

    for annonce in validated:
        try:
            result = normalizer.normalize_location(annonce)
            normalized.append(result)
        except Exception:
            logger.error(
                "Erreur normalisation location pour %s",
                annonce.get("url_source", "?"),
                exc_info=True,
            )

    logger.info(
        "Normalisation locations terminee: %d/%d annonces",
        len(normalized),
        len(validated),
    )

    ti.xcom_push(key="normalized_loyers", value=normalized)
    return normalized


def compute_medianes(**context: Any) -> dict[str, Any]:
    """Calcule les medianes de loyer par segment.

    Calcule les loyers medians, Q1, Q3 et loyer/m2 pour chaque
    combinaison quartier x type_bien x meuble. Met a jour la table
    loyers_reference en base.

    Args:
        **context: Contexte Airflow avec TaskInstance pour XCom.

    Returns:
        Dictionnaire avec les statistiques de l'observatoire.
    """
    from src.observatoire.loyers import ObservatoireLoyers

    ti = context.get("ti")
    normalized = ti.xcom_pull(key="normalized_loyers", task_ids="parse_normalize_loyers")

    if not normalized:
        logger.warning("Aucune annonce de location normalisee pour le calcul des medianes")
        return {"segments_calcules": 0, "total_annonces": 0}

    observatoire = ObservatoireLoyers()
    segments_calcules = 0
    references: list[dict[str, Any]] = []

    for quartier in QUARTIERS:
        for type_bien in TYPES_BIEN:
            for meuble in MEUBLE_OPTIONS:
                try:
                    mediane = observatoire.compute_medianes(
                        loyers=normalized,
                        quartier=quartier,
                        type_bien=type_bien,
                        meuble=meuble,
                    )

                    if mediane.get("nb_annonces", 0) > 0:
                        references.append(mediane)
                        segments_calcules += 1

                        logger.info(
                            "Mediane calculee: %s/%s/meuble=%s -> %s EUR "
                            "(n=%d, fiabilite=%s)",
                            quartier,
                            type_bien,
                            meuble,
                            mediane.get("loyer_median"),
                            mediane.get("nb_annonces", 0),
                            mediane.get("fiabilite"),
                        )

                except Exception:
                    logger.error(
                        "Erreur calcul mediane pour %s/%s/meuble=%s",
                        quartier,
                        type_bien,
                        meuble,
                        exc_info=True,
                    )

    # Sauvegarder les references en base
    _save_loyer_references(references)

    stats = {
        "segments_calcules": segments_calcules,
        "total_annonces": len(normalized),
        "references": references,
    }

    logger.info(
        "Calcul medianes termine: %d segments, %d annonces",
        segments_calcules,
        len(normalized),
    )

    return stats


# ------------------------------------------------------------------
# Fonctions utilitaires internes
# ------------------------------------------------------------------


def _log_scraping_stats(
    source: str,
    type_scrape: str,
    stats: dict[str, Any],
    duree_sec: float,
) -> None:
    """Enregistre les statistiques de scraping en base.

    Args:
        source: Nom de la source.
        type_scrape: Type de scraping.
        stats: Statistiques du scraper.
        duree_sec: Duree en secondes.
    """
    try:
        from src.db.connection import get_session
        from src.db.models import ScrapingLog

        with get_session() as session:
            log = ScrapingLog(
                source=source,
                type_scrape=type_scrape,
                nb_annonces_scrapees=stats.get("nb_annonces_scrapees", 0),
                nb_erreurs=stats.get("nb_erreurs", 0),
                duree_sec=duree_sec,
            )
            session.add(log)
    except Exception:
        logger.error(
            "Erreur enregistrement stats scraping pour %s",
            source,
            exc_info=True,
        )


def _log_scraping_error(
    source: str,
    type_scrape: str,
    duree_sec: float,
) -> None:
    """Enregistre une erreur de scraping en base.

    Args:
        source: Nom de la source.
        type_scrape: Type de scraping.
        duree_sec: Duree avant echec.
    """
    try:
        from src.db.connection import get_session
        from src.db.models import ScrapingLog

        with get_session() as session:
            log = ScrapingLog(
                source=source,
                type_scrape=type_scrape,
                nb_annonces_scrapees=0,
                nb_erreurs=1,
                duree_sec=duree_sec,
                erreur_detail="Erreur fatale lors du scraping location",
            )
            session.add(log)
    except Exception:
        logger.error(
            "Erreur enregistrement erreur scraping pour %s",
            source,
            exc_info=True,
        )


def _log_validation_reject(
    annonce: dict[str, Any],
    reasons: list[str],
) -> None:
    """Enregistre un rejet de validation en base.

    Args:
        annonce: Donnees brutes rejetees.
        reasons: Raisons du rejet.
    """
    try:
        from src.db.connection import get_session
        from src.db.models import ValidationLog

        with get_session() as session:
            log = ValidationLog(
                url_source=annonce.get("url_source"),
                source=annonce.get("source"),
                raison_rejet="; ".join(reasons),
                donnees_brutes=annonce,
            )
            session.add(log)
    except Exception:
        logger.debug(
            "Erreur enregistrement rejet validation location",
            exc_info=True,
        )


def _save_loyer_references(references: list[dict[str, Any]]) -> None:
    """Sauvegarde ou met a jour les references de loyer en base.

    Args:
        references: Liste de dictionnaires LoyerReference a sauvegarder.
    """
    try:
        from src.db.connection import get_session
        from src.db.models import LoyerReference

        with get_session() as session:
            for ref in references:
                # Upsert : chercher si le segment existe deja
                existing = (
                    session.query(LoyerReference)
                    .filter_by(
                        quartier=ref["quartier"],
                        type_bien=ref["type_bien"],
                        meuble=ref["meuble"],
                    )
                    .first()
                )

                if existing:
                    existing.loyer_median = ref.get("loyer_median")
                    existing.loyer_q1 = ref.get("loyer_q1")
                    existing.loyer_q3 = ref.get("loyer_q3")
                    existing.loyer_m2_median = ref.get("loyer_m2_median")
                    existing.nb_annonces = ref.get("nb_annonces")
                    existing.fiabilite = ref.get("fiabilite")
                else:
                    new_ref = LoyerReference(
                        quartier=ref["quartier"],
                        type_bien=ref["type_bien"],
                        meuble=ref["meuble"],
                        loyer_median=ref.get("loyer_median"),
                        loyer_q1=ref.get("loyer_q1"),
                        loyer_q3=ref.get("loyer_q3"),
                        loyer_m2_median=ref.get("loyer_m2_median"),
                        nb_annonces=ref.get("nb_annonces"),
                        fiabilite=ref.get("fiabilite"),
                    )
                    session.add(new_ref)

        logger.info("References de loyer sauvegardees: %d segments", len(references))

    except Exception:
        logger.error(
            "Erreur sauvegarde references loyer",
            exc_info=True,
        )


# ------------------------------------------------------------------
# Definition du DAG
# ------------------------------------------------------------------

with DAG(
    dag_id="immoscan_loyers",
    default_args=DEFAULT_ARGS,
    description="Observatoire des loyers - scraping locations et calcul des medianes",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["immoscan", "loyers", "observatoire"],
) as dag:

    # Taches de scraping location (paralleles)
    scrape_lbc = PythonOperator(
        task_id="scrape_leboncoin_location",
        python_callable=scrape_location_source,
        op_kwargs={"source_name": "leboncoin"},
    )

    scrape_pap = PythonOperator(
        task_id="scrape_pap_location",
        python_callable=scrape_location_source,
        op_kwargs={"source_name": "pap"},
    )

    scrape_seloger = PythonOperator(
        task_id="scrape_seloger_location",
        python_callable=scrape_location_source,
        op_kwargs={"source_name": "seloger"},
    )

    # Validation
    validate = PythonOperator(
        task_id="validate_loyers",
        python_callable=validate_loyers,
        trigger_rule="all_done",
    )

    # Normalisation
    normalize = PythonOperator(
        task_id="parse_normalize_loyers",
        python_callable=parse_normalize_loyers,
        trigger_rule="all_done",
    )

    # Calcul des medianes
    medianes = PythonOperator(
        task_id="compute_medianes",
        python_callable=compute_medianes,
        trigger_rule="all_done",
    )

    # Dependances :
    # scrape_lbc_location ───┐
    # scrape_pap_location ───┼──> validate_loyers ──> parse_normalize_loyers ──> compute_medianes
    # scrape_seloger_loc ────┘
    [scrape_lbc, scrape_pap, scrape_seloger] >> validate >> normalize >> medianes
