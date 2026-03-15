"""DAG Airflow pour le pipeline de ventes ImmoScan.

Orchestre le scraping des annonces de vente (LeBonCoin),
la validation, normalisation, deduplication, geocodage, scoring,
enrichissement IA et envoi d'alertes Telegram.

Planification : 7x/jour a 7h, 11h, 13h, 16h, 18h, 20h, 22h.
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

SOURCES_VENTE: list[str] = ["leboncoin"]


# ------------------------------------------------------------------
# Fonctions des taches
# ------------------------------------------------------------------


def scrape_source(source_name: str, **context: Any) -> list[dict[str, Any]]:
    """Scrape les annonces de vente depuis une source donnee.

    Instancie le scraper correspondant a la source, execute le scraping
    de type 'vente', et stocke les resultats bruts dans XCom.

    Args:
        source_name: Nom de la source ('leboncoin' ou 'pap').
        **context: Contexte Airflow avec TaskInstance pour XCom.

    Returns:
        Liste de dictionnaires contenant les annonces brutes.
    """
    start_time = time.time()
    results: list[dict[str, Any]] = []

    try:
        if source_name == "leboncoin":
            from src.scrapers.leboncoin import LeBonCoinScraper
            scraper = LeBonCoinScraper()
        else:
            logger.error("Source inconnue: %s", source_name)
            return results

        results = scraper.scrape(scrape_type="vente")
        stats = scraper.get_scraping_stats()

        logger.info(
            "Scraping %s termine: %d annonces, %d erreurs, %.1fs",
            source_name,
            stats.get("nb_annonces_scrapees", 0),
            stats.get("nb_erreurs", 0),
            time.time() - start_time,
        )

        # Enregistrer les stats de scraping en base
        _log_scraping_stats(source_name, "vente", stats, time.time() - start_time)

    except Exception:
        logger.error(
            "Erreur lors du scraping de %s",
            source_name,
            exc_info=True,
        )
        _log_scraping_error(source_name, "vente", time.time() - start_time)

    # Stocker dans XCom
    ti = context.get("ti")
    if ti is not None:
        ti.xcom_push(key=f"raw_{source_name}", value=results)

    return results


def validate_listings(**context: Any) -> list[dict[str, Any]]:
    """Valide les annonces brutes en appliquant les regles de validation.

    Recupere les donnees brutes de toutes les sources depuis XCom,
    valide chaque annonce et enregistre les rejets dans validation_log.

    Args:
        **context: Contexte Airflow avec TaskInstance pour XCom.

    Returns:
        Liste des annonces validees.
    """
    from src.validation.validators import AnnonceValidator

    ti = context.get("ti")
    validator = AnnonceValidator()
    validated: list[dict[str, Any]] = []

    # Recuperer les donnees de toutes les sources
    all_raw: list[dict[str, Any]] = []
    for source_name in SOURCES_VENTE:
        raw_data = ti.xcom_pull(key=f"raw_{source_name}", task_ids=f"scrape_{source_name}")
        if raw_data:
            all_raw.extend(raw_data)

    logger.info("Validation de %d annonces brutes", len(all_raw))

    for annonce in all_raw:
        is_valid, reasons = validator.validate_vente(annonce)
        if is_valid:
            validated.append(annonce)
        else:
            _log_validation_reject(annonce, reasons)

    logger.info(
        "Validation terminee: %d/%d annonces valides",
        len(validated),
        len(all_raw),
    )

    ti.xcom_push(key="validated", value=validated)
    return validated


def parse_normalize(**context: Any) -> list[dict[str, Any]]:
    """Normalise les annonces validees.

    Applique la normalisation (nettoyage texte, standardisation adresses,
    conversion types) sur chaque annonce validee.

    Args:
        **context: Contexte Airflow avec TaskInstance pour XCom.

    Returns:
        Liste des annonces normalisees.
    """
    from src.parsers.normalizer import AnnonceNormalizer

    ti = context.get("ti")
    validated = ti.xcom_pull(key="validated", task_ids="validate")

    if not validated:
        logger.warning("Aucune annonce validee a normaliser")
        ti.xcom_push(key="normalized", value=[])
        return []

    normalizer = AnnonceNormalizer()
    normalized: list[dict[str, Any]] = []

    for annonce in validated:
        try:
            result = normalizer.normalize_vente(annonce)
            normalized.append(result)
        except Exception:
            logger.error(
                "Erreur normalisation pour %s",
                annonce.get("url_source", "?"),
                exc_info=True,
            )

    logger.info(
        "Normalisation terminee: %d/%d annonces",
        len(normalized),
        len(validated),
    )

    ti.xcom_push(key="normalized", value=normalized)
    return normalized


def dedup_listings(**context: Any) -> list[dict[str, Any]]:
    """Deduplique les annonces (intra-source et inter-sources).

    Applique la deduplication par hash URL (intra-source) et par
    matching flou sur adresse/surface/prix (inter-sources).

    Args:
        **context: Contexte Airflow avec TaskInstance pour XCom.

    Returns:
        Liste des annonces dedupliquees.
    """
    from src.parsers.dedup import Deduplicator

    ti = context.get("ti")
    normalized = ti.xcom_pull(key="normalized", task_ids="parse_normalize")

    if not normalized:
        logger.warning("Aucune annonce normalisee a dedupliquer")
        ti.xcom_push(key="deduped", value=[])
        return []

    dedup = Deduplicator()
    seen_hashes: set[str] = set()
    unique: list[dict[str, Any]] = []

    for annonce in normalized:
        # Deduplication intra-source par hash URL
        url = annonce.get("url_source", "")
        if not url:
            continue

        hash_val = dedup.compute_hash_intra(url)
        if hash_val in seen_hashes:
            logger.debug("Doublon intra-source detecte: %s", url)
            continue

        seen_hashes.add(hash_val)
        annonce["hash_dedup"] = hash_val

        # Deduplication inter-sources (matching flou)
        duplicates = dedup.find_duplicates_inter(annonce, unique)
        if duplicates:
            annonce["source_ids"] = duplicates
            logger.debug(
                "Doublon inter-source pour %s: %s", url, duplicates
            )

        unique.append(annonce)

    logger.info(
        "Deduplication terminee: %d/%d annonces uniques",
        len(unique),
        len(normalized),
    )

    ti.xcom_push(key="deduped", value=unique)
    return unique


def geocode_listings(**context: Any) -> list[dict[str, Any]]:
    """Geocode les adresses des annonces via l'API data.gouv.fr.

    Convertit les adresses brutes en coordonnees GPS (latitude, longitude)
    et identifie le quartier.

    Args:
        **context: Contexte Airflow avec TaskInstance pour XCom.

    Returns:
        Liste des annonces geocodees.
    """
    from src.geo.geocoder import Geocoder
    from src.geo.scoring_geo import GeoScorer

    ti = context.get("ti")
    deduped = ti.xcom_pull(key="deduped", task_ids="dedup")

    if not deduped:
        logger.warning("Aucune annonce a geocoder")
        ti.xcom_push(key="geocoded", value=[])
        return []

    geocoder = Geocoder()
    geo_scorer = GeoScorer()
    geocoded: list[dict[str, Any]] = []

    for annonce in deduped:
        adresse = annonce.get("adresse_brute", "")
        if not adresse:
            geocoded.append(annonce)
            continue

        try:
            geo_result = geocoder.geocode(adresse)
            if geo_result:
                annonce["latitude"] = geo_result["latitude"]
                annonce["longitude"] = geo_result["longitude"]
                annonce["coordonnees"] = (
                    geo_result["latitude"],
                    geo_result["longitude"],
                )

                # Identifier le quartier si non deja defini
                if not annonce.get("quartier"):
                    quartier = geo_scorer.identify_quartier(
                        (geo_result["latitude"], geo_result["longitude"])
                    )
                    if quartier:
                        annonce["quartier"] = quartier
            else:
                logger.debug("Geocodage echoue pour: %s", adresse)
        except Exception:
            logger.error(
                "Erreur geocodage pour %s",
                annonce.get("url_source", "?"),
                exc_info=True,
            )

        geocoded.append(annonce)

    logger.info("Geocodage termine: %d annonces traitees", len(geocoded))

    ti.xcom_push(key="geocoded", value=geocoded)
    return geocoded


def compute_scores(**context: Any) -> list[dict[str, Any]]:
    """Calcule les scores de rentabilite et composite pour chaque annonce.

    Execute le pipeline de scoring complet : rentabilite brute (4 scenarios),
    score de localisation, score composite pondere.

    Args:
        **context: Contexte Airflow avec TaskInstance pour XCom.

    Returns:
        Liste des annonces avec leurs scores.
    """
    from src.geo.scoring_geo import GeoScorer
    from src.observatoire.loyers import ObservatoireLoyers
    from src.scoring.composite import CompositeScorer
    from src.scoring.rentabilite import RentabiliteCalculator

    ti = context.get("ti")
    geocoded = ti.xcom_pull(key="geocoded", task_ids="geocode")

    if not geocoded:
        logger.warning("Aucune annonce a scorer")
        ti.xcom_push(key="scored", value=[])
        return []

    renta_calc = RentabiliteCalculator()
    composite_scorer = CompositeScorer()
    geo_scorer = GeoScorer()
    observatoire = ObservatoireLoyers()
    scored: list[dict[str, Any]] = []

    for annonce in geocoded:
        try:
            prix = annonce.get("prix")
            quartier = annonce.get("quartier")
            surface = annonce.get("surface_m2", 0)
            nb_pieces = annonce.get("nb_pieces", 2)

            if not prix or prix <= 0:
                scored.append(annonce)
                continue

            # Estimer le loyer via l'observatoire
            type_bien = f"T{nb_pieces}" if nb_pieces else "T2"
            loyer_estimate = observatoire.estimate_loyer(
                quartier=quartier or "Centre-Ville",
                type_bien=type_bien,
                meuble=False,
                surface=surface,
            )
            loyer_mensuel = loyer_estimate.get("loyer_estime", 0) or 0
            fiabilite_loyer = loyer_estimate.get("fiabilite", "preliminaire")

            # Calcul de rentabilite (4 scenarios)
            renta_data = renta_calc.calculate(
                prix=prix,
                loyer_mensuel=loyer_mensuel,
                charges_copro=annonce.get("charges_copro"),
            )

            # Score de localisation
            coords = annonce.get("coordonnees")
            geo_score = 50.0  # Valeur par defaut
            if coords and isinstance(coords, (tuple, list)) and len(coords) >= 2:
                geo_score = geo_scorer.score_localisation(
                    coordonnees=(coords[0], coords[1]),
                    quartier=quartier,
                )

            # Ajout tension locative pour le score de vacance
            annonce_scoring = dict(annonce)
            if quartier:
                tension = geo_scorer.get_quartier_tension(quartier)
                annonce_scoring["tension_locative"] = tension

            # Score composite
            score_data = composite_scorer.score(
                annonce_data=annonce_scoring,
                renta_data=renta_data,
                geo_score=geo_score,
            )

            # Enrichir l'annonce avec les scores
            annonce["renta_data"] = renta_data
            annonce["score_data"] = score_data
            annonce["loyer_estime"] = loyer_mensuel
            annonce["fiabilite_loyer"] = fiabilite_loyer

        except Exception:
            logger.error(
                "Erreur scoring pour %s",
                annonce.get("url_source", "?"),
                exc_info=True,
            )

        scored.append(annonce)

    logger.info("Scoring termine: %d annonces scorees", len(scored))

    ti.xcom_push(key="scored", value=scored)
    return scored


def enrich_ia(**context: Any) -> list[dict[str, Any]]:
    """Enrichit les annonces via l'API Claude Haiku.

    Appelle Claude pour extraire des signaux de negociation, l'etat du bien,
    les equipements, les red flags et un resume structure.
    Respecte le plafond de 300 appels/jour.

    Args:
        **context: Contexte Airflow avec TaskInstance pour XCom.

    Returns:
        Liste des annonces enrichies.
    """
    import os

    ti = context.get("ti")
    scored = ti.xcom_pull(key="scored", task_ids="compute_scores")

    if not scored:
        logger.warning("Aucune annonce a enrichir")
        ti.xcom_push(key="enriched", value=[])
        return []

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        logger.warning("ANTHROPIC_API_KEY non configuree, enrichissement IA ignore")
        ti.xcom_push(key="enriched", value=scored)
        return scored

    from src.enrichment.claude_enricher import ClaudeEnricher

    enricher = ClaudeEnricher(
        api_key=api_key,
        model=os.environ.get("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001"),
        max_daily_calls=int(os.environ.get("ANTHROPIC_MAX_DAILY_CALLS", "300")),
    )

    enriched: list[dict[str, Any]] = []

    for annonce in scored:
        # N'enrichir que les annonces avec un score minimum ou une description
        description = annonce.get("description_texte")
        if not description:
            enriched.append(annonce)
            continue

        try:
            result = enricher.enrich(annonce)
            if result is not None:
                annonce["enrichment"] = result
                logger.debug(
                    "Enrichissement reussi pour %s",
                    annonce.get("url_source", "?"),
                )
        except Exception:
            logger.error(
                "Erreur enrichissement pour %s",
                annonce.get("url_source", "?"),
                exc_info=True,
            )

        enriched.append(annonce)

    logger.info(
        "Enrichissement termine: %d annonces traitees (API calls: %d/%d)",
        len(enriched),
        enricher.daily_call_count,
        enricher.max_daily_calls,
    )

    ti.xcom_push(key="enriched", value=enriched)
    return enriched


def send_alerts(**context: Any) -> dict[str, int]:
    """Envoie les alertes Telegram en fonction du niveau d'alerte.

    Applique les regles d'alerte :
    - TOP (score >= 80 ou renta >= 8% prix affiche) : alerte immediate
    - BON (score 60-79 ou renta >= 8% apres nego) : digest toutes les 2h
    - VEILLE (score < 60) : stocke, pas d'alerte
    - BAISSE : baisse de prix detectee

    Args:
        **context: Contexte Airflow avec TaskInstance pour XCom.

    Returns:
        Dictionnaire avec les compteurs d'alertes envoyees par niveau.
    """

    ti = context.get("ti")
    enriched = ti.xcom_pull(key="enriched", task_ids="enrich_ia")

    if not enriched:
        logger.warning("Aucune annonce pour les alertes")
        return {"top": 0, "bon": 0, "veille": 0}

    alert_counts: dict[str, int] = {"top": 0, "bon": 0, "veille": 0}

    try:
        from src.alerts.formatter import AlertFormatter
        from src.alerts.telegram_bot import TelegramBot

        bot = TelegramBot()
        formatter = AlertFormatter()
    except Exception:
        logger.error("Impossible d'initialiser le bot Telegram", exc_info=True)
        return alert_counts

    for annonce in enriched:
        score_data = annonce.get("score_data", {})
        renta_data = annonce.get("renta_data", {})
        enrichment = annonce.get("enrichment")
        niveau = score_data.get("niveau_alerte", "veille")

        try:
            if niveau == "top":
                message = formatter.format_top_alert(
                    annonce, score_data, renta_data, enrichment
                )
                bot.send_alert_sync(message, "top")
                alert_counts["top"] += 1
                _log_alert(annonce, "top")

            elif niveau == "bon":
                alert_counts["bon"] += 1
                _log_alert(annonce, "bon")

            else:
                alert_counts["veille"] += 1

        except Exception:
            logger.error(
                "Erreur envoi alerte pour %s",
                annonce.get("url_source", "?"),
                exc_info=True,
            )

    logger.info(
        "Alertes envoyees: %d TOP, %d BON, %d VEILLE",
        alert_counts["top"],
        alert_counts["bon"],
        alert_counts["veille"],
    )

    return alert_counts


# ------------------------------------------------------------------
# Fonctions utilitaires internes
# ------------------------------------------------------------------


def _log_scraping_stats(
    source: str,
    type_scrape: str,
    stats: dict[str, Any],
    duree_sec: float,
) -> None:
    """Enregistre les statistiques de scraping dans la base de donnees.

    Args:
        source: Nom de la source.
        type_scrape: Type de scraping ('vente' ou 'location').
        stats: Statistiques du scraper.
        duree_sec: Duree du scraping en secondes.
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
    """Enregistre une erreur de scraping dans la base de donnees.

    Args:
        source: Nom de la source.
        type_scrape: Type de scraping.
        duree_sec: Duree avant l'echec.
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
                erreur_detail="Erreur fatale lors du scraping",
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
    """Enregistre un rejet de validation dans la base de donnees.

    Args:
        annonce: Donnees brutes de l'annonce rejetee.
        reasons: Liste des raisons de rejet.
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
            "Erreur enregistrement rejet validation",
            exc_info=True,
        )


def _log_alert(annonce: dict[str, Any], niveau: str) -> None:
    """Enregistre une alerte envoyee dans la base de donnees.

    Args:
        annonce: Donnees de l'annonce alertee.
        niveau: Niveau de l'alerte ('top', 'bon', 'baisse_prix').
    """
    try:
        from src.db.connection import get_session
        from src.db.models import AlerteLog

        with get_session() as session:
            log = AlerteLog(
                niveau=niveau,
                canal="telegram",
            )
            session.add(log)
    except Exception:
        logger.debug("Erreur enregistrement alerte", exc_info=True)


# ------------------------------------------------------------------
# Definition du DAG
# ------------------------------------------------------------------

with DAG(
    dag_id="immoscan_ventes",
    default_args=DEFAULT_ARGS,
    description="Pipeline de scraping et scoring des annonces de vente a Besancon",
    schedule="0 7,11,13,16,18,20,22 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["immoscan", "ventes", "scraping"],
) as dag:

    # Tache de scraping LeBonCoin
    scrape_leboncoin = PythonOperator(
        task_id="scrape_leboncoin",
        python_callable=scrape_source,
        op_kwargs={"source_name": "leboncoin"},
    )

    # Validation (soft fail : continue meme si le scraping echoue)
    validate = PythonOperator(
        task_id="validate",
        python_callable=validate_listings,
        trigger_rule="all_done",
    )

    # Normalisation
    normalize = PythonOperator(
        task_id="parse_normalize",
        python_callable=parse_normalize,
        trigger_rule="all_done",
    )

    # Deduplication
    dedup = PythonOperator(
        task_id="dedup",
        python_callable=dedup_listings,
        trigger_rule="all_done",
    )

    # Geocodage
    geocode = PythonOperator(
        task_id="geocode",
        python_callable=geocode_listings,
        trigger_rule="all_done",
    )

    # Scoring
    scores = PythonOperator(
        task_id="compute_scores",
        python_callable=compute_scores,
        trigger_rule="all_done",
    )

    # Enrichissement IA
    enrich = PythonOperator(
        task_id="enrich_ia",
        python_callable=enrich_ia,
        trigger_rule="all_done",
    )

    # Alertes Telegram
    alerts = PythonOperator(
        task_id="send_alerts",
        python_callable=send_alerts,
        trigger_rule="all_done",
    )

    # Dependances :
    # scrape_leboncoin ──> validate ──> parse_normalize ──> dedup ──> geocode ──> compute_scores ──> enrich_ia ──> send_alerts
    scrape_leboncoin >> validate >> normalize >> dedup >> geocode >> scores >> enrich >> alerts
