"""DAG Airflow pour le digest quotidien ImmoScan.

Genere et envoie un digest Telegram a 21h contenant :
- Top 3 annonces du jour
- Baisses de prix detectees
- Statistiques du pipeline
- Statistiques de l'observatoire des loyers

Planification : 1x/jour a 21h.
"""

from __future__ import annotations

import logging
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


# ------------------------------------------------------------------
# Fonctions des taches
# ------------------------------------------------------------------


def query_top_annonces(**context: Any) -> list[dict[str, Any]]:
    """Recupere les top 3 annonces du jour depuis la base de donnees.

    Selectionne les annonces avec les meilleurs scores composites
    parmi celles scrapees dans les dernieres 24 heures.

    Args:
        **context: Contexte Airflow avec TaskInstance pour XCom.

    Returns:
        Liste des top 3 annonces avec leurs scores et rentabilite.
    """
    top_annonces: list[dict[str, Any]] = []

    try:
        from src.db.connection import get_session
        from src.db.models import Annonce, Score

        cutoff = datetime.now() - timedelta(hours=24)

        with get_session() as session:
            results = (
                session.query(Annonce, Score)
                .join(Score, Score.annonce_id == Annonce.id)
                .filter(Annonce.date_scrape >= cutoff)
                .filter(Score.score_global.isnot(None))
                .order_by(Score.score_global.desc())
                .limit(3)
                .all()
            )

            for annonce, score in results:
                top_annonces.append({
                    "annonce": {
                        "prix": annonce.prix,
                        "surface_m2": annonce.surface_m2,
                        "nb_pieces": annonce.nb_pieces,
                        "dpe": annonce.dpe,
                        "adresse_brute": annonce.adresse_brute,
                        "quartier": annonce.quartier,
                        "url_source": annonce.url_source,
                    },
                    "score": {
                        "score_global": score.score_global,
                    },
                    "renta": {
                        "renta_brute": score.renta_brute_affiche,
                        "renta_brute_nego_5": score.renta_brute_nego_5,
                        "renta_brute_nego_10": score.renta_brute_nego_10,
                        "renta_brute_nego_15": score.renta_brute_nego_15,
                    },
                })

    except Exception:
        logger.error("Erreur requete top annonces", exc_info=True)

    logger.info("Top annonces du jour: %d trouvees", len(top_annonces))

    ti = context.get("ti")
    if ti is not None:
        ti.xcom_push(key="top_annonces", value=top_annonces)

    return top_annonces


def query_price_drops(**context: Any) -> list[dict[str, Any]]:
    """Detecte les baisses de prix dans les dernieres 24 heures.

    Analyse l'historique de prix des annonces pour identifier celles
    dont le prix a baisse depuis le dernier scraping.

    Args:
        **context: Contexte Airflow avec TaskInstance pour XCom.

    Returns:
        Liste des baisses de prix detectees.
    """
    baisses: list[dict[str, Any]] = []

    try:
        from src.db.connection import get_session
        from src.db.models import Annonce

        cutoff = datetime.now() - timedelta(hours=24)

        with get_session() as session:
            annonces = (
                session.query(Annonce)
                .filter(Annonce.date_modification >= cutoff)
                .filter(Annonce.historique_prix.isnot(None))
                .all()
            )

            for annonce in annonces:
                historique = annonce.historique_prix or []
                if len(historique) >= 2:
                    dernier = historique[-1]
                    avant_dernier = historique[-2]

                    prix_actuel = dernier.get("prix", 0)
                    prix_precedent = avant_dernier.get("prix", 0)

                    if prix_actuel < prix_precedent and prix_precedent > 0:
                        baisses.append({
                            "annonce": {
                                "prix": prix_actuel,
                                "surface_m2": annonce.surface_m2,
                                "nb_pieces": annonce.nb_pieces,
                                "adresse_brute": annonce.adresse_brute,
                                "quartier": annonce.quartier,
                                "url_source": annonce.url_source,
                            },
                            "ancien_prix": prix_precedent,
                            "nouveau_prix": prix_actuel,
                        })

    except Exception:
        logger.error("Erreur requete baisses de prix", exc_info=True)

    logger.info("Baisses de prix detectees: %d", len(baisses))

    ti = context.get("ti")
    if ti is not None:
        ti.xcom_push(key="price_drops", value=baisses)

    return baisses


def aggregate_pipeline_stats(**context: Any) -> dict[str, Any]:
    """Agrege les statistiques du pipeline de scraping du jour.

    Calcule le nombre total d'annonces scrapees, nouvelles, erreurs
    et les sources actives dans les dernieres 24 heures.

    Args:
        **context: Contexte Airflow avec TaskInstance pour XCom.

    Returns:
        Dictionnaire avec les statistiques agregees.
    """
    stats: dict[str, Any] = {
        "nb_scrapees": 0,
        "nb_nouvelles": 0,
        "nb_erreurs": 0,
        "sources": [],
    }

    try:
        from src.db.connection import get_session
        from src.db.models import ScrapingLog

        cutoff = datetime.now() - timedelta(hours=24)

        with get_session() as session:
            logs = (
                session.query(ScrapingLog)
                .filter(ScrapingLog.date_exec >= cutoff)
                .filter(ScrapingLog.type_scrape == "vente")
                .all()
            )

            sources_set: set[str] = set()
            for log in logs:
                stats["nb_scrapees"] += log.nb_annonces_scrapees
                stats["nb_nouvelles"] += log.nb_nouvelles
                stats["nb_erreurs"] += log.nb_erreurs
                sources_set.add(log.source)

            stats["sources"] = sorted(sources_set)

    except Exception:
        logger.error("Erreur agregation stats pipeline", exc_info=True)

    logger.info(
        "Stats pipeline: %d scrapees, %d nouvelles, %d erreurs, sources=%s",
        stats["nb_scrapees"],
        stats["nb_nouvelles"],
        stats["nb_erreurs"],
        stats["sources"],
    )

    ti = context.get("ti")
    if ti is not None:
        ti.xcom_push(key="pipeline_stats", value=stats)

    return stats


def aggregate_observatory_stats(**context: Any) -> dict[str, Any]:
    """Agrege les statistiques de l'observatoire des loyers.

    Calcule le nombre d'annonces de location, de segments couverts
    et la fiabilite globale de l'observatoire.

    Args:
        **context: Contexte Airflow avec TaskInstance pour XCom.

    Returns:
        Dictionnaire avec les statistiques de l'observatoire.
    """
    obs_stats: dict[str, Any] = {
        "nb_locations": 0,
        "segments_couverts": 0,
        "fiabilite": "N/A",
    }

    try:
        from src.db.connection import get_session
        from src.db.models import LoyerMarche, LoyerReference

        with get_session() as session:
            # Compter les annonces de location des 30 derniers jours
            cutoff = datetime.now() - timedelta(days=30)
            nb_locations = (
                session.query(LoyerMarche)
                .filter(LoyerMarche.date_scrape >= cutoff)
                .count()
            )
            obs_stats["nb_locations"] = nb_locations

            # Compter les segments avec des donnees
            references = session.query(LoyerReference).all()
            segments_with_data = sum(
                1 for ref in references
                if ref.nb_annonces is not None and ref.nb_annonces > 0
            )
            obs_stats["segments_couverts"] = segments_with_data

            # Fiabilite globale
            fiable_count = sum(
                1 for ref in references if ref.fiabilite == "fiable"
            )
            total_refs = len(references)
            if total_refs > 0:
                fiable_pct = fiable_count / total_refs * 100
                obs_stats["fiabilite"] = f"{fiable_pct:.0f}% fiable ({fiable_count}/{total_refs})"
            else:
                obs_stats["fiabilite"] = "Aucune reference"

    except Exception:
        logger.error("Erreur agregation stats observatoire", exc_info=True)

    logger.info(
        "Stats observatoire: %d locations, %d segments, fiabilite=%s",
        obs_stats["nb_locations"],
        obs_stats["segments_couverts"],
        obs_stats["fiabilite"],
    )

    ti = context.get("ti")
    if ti is not None:
        ti.xcom_push(key="observatory_stats", value=obs_stats)

    return obs_stats


def send_digest(**context: Any) -> bool:
    """Formate et envoie le digest quotidien via Telegram.

    Assemble les resultats des taches precedentes (top annonces,
    baisses de prix, stats pipeline, stats observatoire) et envoie
    le digest formate.

    Args:
        **context: Contexte Airflow avec TaskInstance pour XCom.

    Returns:
        True si le digest a ete envoye avec succes, False sinon.
    """
    ti = context.get("ti")

    # Recuperer les donnees des taches precedentes
    top_annonces = ti.xcom_pull(
        key="top_annonces",
        task_ids="query_top_annonces",
    ) or []

    price_drops = ti.xcom_pull(
        key="price_drops",
        task_ids="query_price_drops",
    ) or []

    pipeline_stats = ti.xcom_pull(
        key="pipeline_stats",
        task_ids="aggregate_pipeline_stats",
    ) or {"nb_scrapees": 0, "nb_nouvelles": 0, "nb_erreurs": 0, "sources": []}

    observatory_stats = ti.xcom_pull(
        key="observatory_stats",
        task_ids="aggregate_observatory_stats",
    ) or {"nb_locations": 0, "segments_couverts": 0, "fiabilite": "N/A"}

    try:
        from src.alerts.formatter import AlertFormatter
        from src.alerts.telegram_bot import TelegramBot

        formatter = AlertFormatter()
        bot = TelegramBot()

        digest_text = formatter.format_digest(
            top_annonces=top_annonces,
            baisses=price_drops,
            stats=pipeline_stats,
            obs_stats=observatory_stats,
        )

        import asyncio

        success = asyncio.run(bot.send_digest(digest_text))

        if success:
            logger.info("Digest quotidien envoye avec succes")
        else:
            logger.warning("Echec envoi du digest quotidien")

        return success

    except Exception:
        logger.error("Erreur envoi digest quotidien", exc_info=True)
        return False


# ------------------------------------------------------------------
# Definition du DAG
# ------------------------------------------------------------------

with DAG(
    dag_id="immoscan_digest",
    default_args=DEFAULT_ARGS,
    description="Digest quotidien - top annonces, baisses de prix, stats pipeline",
    schedule=None,  # Digest desactive
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["immoscan", "digest", "telegram"],
) as dag:

    query_top = PythonOperator(
        task_id="query_top_annonces",
        python_callable=query_top_annonces,
    )

    query_drops = PythonOperator(
        task_id="query_price_drops",
        python_callable=query_price_drops,
    )

    pipeline_stats = PythonOperator(
        task_id="aggregate_pipeline_stats",
        python_callable=aggregate_pipeline_stats,
    )

    obs_stats = PythonOperator(
        task_id="aggregate_observatory_stats",
        python_callable=aggregate_observatory_stats,
    )

    digest = PythonOperator(
        task_id="send_digest",
        python_callable=send_digest,
        trigger_rule="all_done",
    )

    # Les 4 requetes s'executent en parallele, puis le digest est envoye
    [query_top, query_drops, pipeline_stats, obs_stats] >> digest
