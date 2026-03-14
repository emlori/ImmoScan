"""DAG Airflow pour la maintenance hebdomadaire ImmoScan.

Effectue les operations de maintenance :
- Purge des donnees expirees (politique de retention)
- VACUUM des tables PostgreSQL
- Verification de sante (espace disque, sources)
- Verification du backup

Planification : 1x/semaine le dimanche a 3h.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path
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
    "retry_delay": timedelta(minutes=10),
}

# Politique de retention
RETENTION_ARCHIVED_MONTHS: int = 6
RETENTION_LOYERS_MONTHS: int = 12
RETENTION_SCRAPING_LOG_DAYS: int = 90
RETENTION_VALIDATION_LOG_DAYS: int = 30

BACKUP_DIR: str = "/backups/immoscan"


# ------------------------------------------------------------------
# Fonctions des taches
# ------------------------------------------------------------------


def purge_expired_data(**context: Any) -> dict[str, int]:
    """Purge les donnees expirees selon la politique de retention.

    Supprime :
    - Annonces archivees depuis plus de 6 mois
    - Donnees loyers_marche de plus de 12 mois
    - Logs de scraping de plus de 90 jours
    - Logs de validation de plus de 30 jours

    Args:
        **context: Contexte Airflow.

    Returns:
        Dictionnaire avec le nombre de lignes supprimees par table.
    """
    purge_counts: dict[str, int] = {
        "annonces_archivees": 0,
        "loyers_marche": 0,
        "scraping_log": 0,
        "validation_log": 0,
    }

    try:
        from sqlalchemy import text

        from src.db.connection import get_session

        now = datetime.now()

        with get_session() as session:
            # 1. Annonces archivees > 6 mois
            cutoff_archive = now - timedelta(days=RETENTION_ARCHIVED_MONTHS * 30)
            result = session.execute(
                text(
                    "DELETE FROM annonces "
                    "WHERE statut = 'archive' "
                    "AND date_modification < :cutoff"
                ),
                {"cutoff": cutoff_archive},
            )
            purge_counts["annonces_archivees"] = result.rowcount
            logger.info(
                "Purge annonces archivees: %d lignes supprimees (cutoff: %s)",
                result.rowcount,
                cutoff_archive.isoformat(),
            )

            # 2. Loyers marche > 12 mois
            cutoff_loyers = now - timedelta(days=RETENTION_LOYERS_MONTHS * 30)
            result = session.execute(
                text(
                    "DELETE FROM loyers_marche "
                    "WHERE date_scrape < :cutoff"
                ),
                {"cutoff": cutoff_loyers},
            )
            purge_counts["loyers_marche"] = result.rowcount
            logger.info(
                "Purge loyers_marche: %d lignes supprimees (cutoff: %s)",
                result.rowcount,
                cutoff_loyers.isoformat(),
            )

            # 3. Scraping logs > 90 jours
            cutoff_scraping = now - timedelta(days=RETENTION_SCRAPING_LOG_DAYS)
            result = session.execute(
                text(
                    "DELETE FROM scraping_log "
                    "WHERE date_exec < :cutoff"
                ),
                {"cutoff": cutoff_scraping},
            )
            purge_counts["scraping_log"] = result.rowcount
            logger.info(
                "Purge scraping_log: %d lignes supprimees (cutoff: %s)",
                result.rowcount,
                cutoff_scraping.isoformat(),
            )

            # 4. Validation logs > 30 jours
            cutoff_validation = now - timedelta(days=RETENTION_VALIDATION_LOG_DAYS)
            result = session.execute(
                text(
                    "DELETE FROM validation_log "
                    "WHERE date_rejet < :cutoff"
                ),
                {"cutoff": cutoff_validation},
            )
            purge_counts["validation_log"] = result.rowcount
            logger.info(
                "Purge validation_log: %d lignes supprimees (cutoff: %s)",
                result.rowcount,
                cutoff_validation.isoformat(),
            )

    except Exception:
        logger.error("Erreur lors de la purge des donnees expirees", exc_info=True)
        raise

    total = sum(purge_counts.values())
    logger.info("Purge terminee: %d lignes supprimees au total", total)

    ti = context.get("ti")
    if ti is not None:
        ti.xcom_push(key="purge_counts", value=purge_counts)

    return purge_counts


def vacuum_tables(**context: Any) -> list[str]:
    """Execute VACUUM ANALYZE sur les tables principales PostgreSQL.

    Libere l'espace disque occupe par les lignes supprimees et met a jour
    les statistiques des tables pour l'optimiseur de requetes.

    Args:
        **context: Contexte Airflow.

    Returns:
        Liste des tables ayant ete VACUUM-ees avec succes.
    """
    tables = [
        "annonces",
        "scores",
        "enrichissement_ia",
        "loyers_marche",
        "loyers_reference",
        "alertes_log",
        "scraping_log",
        "validation_log",
    ]

    vacuumed: list[str] = []

    try:
        from sqlalchemy import text

        from src.db.connection import get_engine

        engine = get_engine()

        # VACUUM necessite autocommit (pas dans une transaction)
        with engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as conn:
            for table in tables:
                try:
                    conn.execute(text(f"VACUUM ANALYZE {table}"))
                    vacuumed.append(table)
                    logger.info("VACUUM ANALYZE %s: OK", table)
                except Exception:
                    logger.error(
                        "Erreur VACUUM ANALYZE %s",
                        table,
                        exc_info=True,
                    )

    except Exception:
        logger.error("Erreur connexion pour VACUUM", exc_info=True)

    logger.info(
        "VACUUM termine: %d/%d tables traitees",
        len(vacuumed),
        len(tables),
    )

    return vacuumed


def health_check(**context: Any) -> dict[str, Any]:
    """Effectue les verifications de sante du systeme.

    Verifie l'espace disque, la sante des sources, le taux de parsing
    et le budget API.

    Args:
        **context: Contexte Airflow.

    Returns:
        Rapport de sante agrege.
    """
    from src.monitoring.health import HealthMonitor

    monitor = HealthMonitor()

    # Recuperer les logs de scraping recents
    scraping_logs: list[dict[str, Any]] = []
    try:
        from src.db.connection import get_session
        from src.db.models import ScrapingLog

        with get_session() as session:
            logs = (
                session.query(ScrapingLog)
                .order_by(ScrapingLog.date_exec.desc())
                .limit(100)
                .all()
            )
            scraping_logs = [
                {
                    "source": log.source,
                    "type_scrape": log.type_scrape,
                    "date_exec": log.date_exec.isoformat() if log.date_exec else None,
                    "nb_annonces_scrapees": log.nb_annonces_scrapees,
                    "nb_nouvelles": log.nb_nouvelles,
                    "nb_erreurs": log.nb_erreurs,
                    "erreur_detail": log.erreur_detail,
                }
                for log in logs
            ]
    except Exception:
        logger.error("Erreur recuperation logs scraping pour health check", exc_info=True)

    # Generer le rapport de sante
    report = monitor.generate_health_report(
        scraping_logs=scraping_logs,
        sources=["leboncoin", "pap", "seloger"],
    )

    # Envoyer une alerte systeme si le statut est critique
    if report.get("status") == "critical":
        _send_health_alert(report)

    logger.info("Health check: %s", report.get("summary", "N/A"))

    ti = context.get("ti")
    if ti is not None:
        ti.xcom_push(key="health_report", value=report)

    return report


def verify_backup(**context: Any) -> dict[str, Any]:
    """Verifie l'integrite du dernier backup PostgreSQL.

    Controle que le fichier de backup existe, n'est pas vide et est recent.

    Args:
        **context: Contexte Airflow.

    Returns:
        Dictionnaire avec le resultat de la verification.
    """
    result: dict[str, Any] = {
        "status": "unknown",
        "message": "",
        "backup_path": None,
        "backup_size_mb": 0,
        "backup_age_hours": None,
    }

    backup_dir = Path(BACKUP_DIR)

    if not backup_dir.exists():
        result["status"] = "critical"
        result["message"] = f"Repertoire de backup inexistant: {BACKUP_DIR}"
        logger.error(result["message"])
        _send_backup_alert(result)
        return result

    # Chercher le backup le plus recent
    backups = sorted(
        backup_dir.glob("immoscan_*.sql.gz"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )

    if not backups:
        # Chercher aussi les fichiers .sql non compresses
        backups = sorted(
            backup_dir.glob("immoscan_*.sql"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )

    if not backups:
        result["status"] = "critical"
        result["message"] = f"Aucun fichier de backup trouve dans {BACKUP_DIR}"
        logger.error(result["message"])
        _send_backup_alert(result)
        return result

    latest_backup = backups[0]
    stat = latest_backup.stat()
    size_mb = stat.st_size / (1024 * 1024)
    age_hours = (datetime.now().timestamp() - stat.st_mtime) / 3600

    result["backup_path"] = str(latest_backup)
    result["backup_size_mb"] = round(size_mb, 2)
    result["backup_age_hours"] = round(age_hours, 1)

    # Verifications
    if stat.st_size == 0:
        result["status"] = "critical"
        result["message"] = f"Backup vide: {latest_backup.name}"
        _send_backup_alert(result)
    elif age_hours > 48:
        result["status"] = "warning"
        result["message"] = (
            f"Backup ancien: {latest_backup.name} ({age_hours:.0f}h)"
        )
        _send_backup_alert(result)
    else:
        result["status"] = "ok"
        result["message"] = (
            f"Backup OK: {latest_backup.name} "
            f"({size_mb:.1f} Mo, {age_hours:.0f}h)"
        )

    logger.info("Verification backup: %s", result["message"])

    return result


# ------------------------------------------------------------------
# Fonctions utilitaires internes
# ------------------------------------------------------------------


def _send_health_alert(report: dict[str, Any]) -> None:
    """Envoie une alerte systeme pour un rapport de sante critique.

    Args:
        report: Rapport de sante genere par HealthMonitor.
    """
    try:
        import asyncio

        from src.alerts.telegram_bot import TelegramBot

        bot = TelegramBot()
        summary = report.get("summary", "Statut inconnu")

        # Construire le detail des checks critiques
        critical_checks: list[str] = []
        for name, check in report.get("checks", {}).items():
            if check.get("status") == "critical":
                critical_checks.append(f"- {name}: {check.get('message', '?')}")

        detail = f"{summary}\n" + "\n".join(critical_checks) if critical_checks else summary

        asyncio.run(bot.send_system_alert("health_check_critical", detail))
    except Exception:
        logger.error("Erreur envoi alerte sante", exc_info=True)


def _send_backup_alert(result: dict[str, Any]) -> None:
    """Envoie une alerte systeme pour un probleme de backup.

    Args:
        result: Resultat de la verification du backup.
    """
    try:
        import asyncio

        from src.alerts.telegram_bot import TelegramBot

        bot = TelegramBot()
        asyncio.run(
            bot.send_system_alert("backup_issue", result.get("message", "Probleme backup"))
        )
    except Exception:
        logger.error("Erreur envoi alerte backup", exc_info=True)


# ------------------------------------------------------------------
# Definition du DAG
# ------------------------------------------------------------------

with DAG(
    dag_id="immoscan_maintenance",
    default_args=DEFAULT_ARGS,
    description="Maintenance hebdomadaire - purge, vacuum, health check, backup",
    schedule="0 3 * * 0",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["immoscan", "maintenance"],
) as dag:

    purge = PythonOperator(
        task_id="purge_expired_data",
        python_callable=purge_expired_data,
    )

    vacuum = PythonOperator(
        task_id="vacuum_tables",
        python_callable=vacuum_tables,
        trigger_rule="all_done",
    )

    health = PythonOperator(
        task_id="health_check",
        python_callable=health_check,
        trigger_rule="all_done",
    )

    backup = PythonOperator(
        task_id="verify_backup",
        python_callable=verify_backup,
        trigger_rule="all_done",
    )

    # Purge d'abord, puis VACUUM, puis checks en parallele
    purge >> vacuum >> [health, backup]
