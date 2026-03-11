"""Monitoring de sante et alertes techniques pour ImmoScan.

Surveille la sante du pipeline de scraping, les taux de parsing,
la disponibilite des sources, le budget API et l'espace disque.
"""

from __future__ import annotations

import logging
import shutil
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)

# Seuils de monitoring par defaut
DEFAULT_CONSECUTIVE_FAILURES_THRESHOLD = 3
DEFAULT_PARSING_RATE_THRESHOLD = 0.50
DEFAULT_DISK_SPACE_THRESHOLD = 0.20
DEFAULT_API_BUDGET_WARNING_THRESHOLD = 0.80
DEFAULT_ZERO_LISTINGS_HOURS = 24


class HealthMonitor:
    """Moniteur de sante pour le pipeline ImmoScan.

    Effectue des verifications sur les differents composants du systeme
    et genere des rapports de sante agregos. Chaque verification retourne
    un dictionnaire avec le statut ('ok', 'warning', 'critical'), un message
    et des details.

    Attributes:
        consecutive_failures_threshold: Nombre d'echecs consecutifs avant alerte.
        parsing_rate_threshold: Taux de parsing minimum avant alerte (0-1).
        disk_space_threshold: Espace disque minimum avant alerte (0-1).
        api_budget_warning_threshold: Seuil de consommation API avant alerte (0-1).
        zero_listings_hours: Heures sans nouvelle annonce avant alerte.
    """

    def __init__(
        self,
        consecutive_failures_threshold: int = DEFAULT_CONSECUTIVE_FAILURES_THRESHOLD,
        parsing_rate_threshold: float = DEFAULT_PARSING_RATE_THRESHOLD,
        disk_space_threshold: float = DEFAULT_DISK_SPACE_THRESHOLD,
        api_budget_warning_threshold: float = DEFAULT_API_BUDGET_WARNING_THRESHOLD,
        zero_listings_hours: int = DEFAULT_ZERO_LISTINGS_HOURS,
    ) -> None:
        """Initialise le moniteur de sante.

        Args:
            consecutive_failures_threshold: Nombre d'echecs consecutifs pour declencher
                une alerte critique (defaut: 3).
            parsing_rate_threshold: Taux de succes de parsing en dessous duquel
                une alerte est declenchee (defaut: 0.50 = 50%).
            disk_space_threshold: Pourcentage d'espace disque libre en dessous
                duquel une alerte est declenchee (defaut: 0.20 = 20%).
            api_budget_warning_threshold: Pourcentage de consommation du budget
                API au-dessus duquel un avertissement est emis (defaut: 0.80 = 80%).
            zero_listings_hours: Nombre d'heures sans nouvelles annonces avant
                alerte (defaut: 24).
        """
        self.consecutive_failures_threshold = consecutive_failures_threshold
        self.parsing_rate_threshold = parsing_rate_threshold
        self.disk_space_threshold = disk_space_threshold
        self.api_budget_warning_threshold = api_budget_warning_threshold
        self.zero_listings_hours = zero_listings_hours

    def check_source_health(
        self, source: str, scraping_logs: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """Verifie la disponibilite d'une source de scraping.

        Analyse les logs de scraping pour detecter les echecs consecutifs.
        Une source est consideree indisponible apres N echecs consecutifs.

        Args:
            source: Nom de la source ('leboncoin', 'pap', 'seloger').
            scraping_logs: Liste de logs de scraping pour cette source,
                tries par date decroissante (plus recent en premier).
                Chaque log contient: source, nb_erreurs, nb_annonces_scrapees,
                date_exec, erreur_detail.

        Returns:
            Dictionnaire avec:
                - status: 'ok', 'warning' ou 'critical'.
                - message: Description du statut.
                - details: Dict avec consecutive_failures, last_success,
                  total_logs.
        """
        if not scraping_logs:
            return {
                "status": "warning",
                "message": f"Aucun log de scraping pour {source}.",
                "details": {
                    "consecutive_failures": 0,
                    "last_success": None,
                    "total_logs": 0,
                },
            }

        # Filtrer les logs de cette source
        source_logs = [
            log for log in scraping_logs
            if log.get("source") == source
        ]

        if not source_logs:
            return {
                "status": "warning",
                "message": f"Aucun log de scraping pour {source}.",
                "details": {
                    "consecutive_failures": 0,
                    "last_success": None,
                    "total_logs": 0,
                },
            }

        # Compter les echecs consecutifs (logs tries par date decroissante)
        consecutive_failures = 0
        last_success: str | None = None

        for log in source_logs:
            nb_erreurs = log.get("nb_erreurs", 0)
            nb_scrapees = log.get("nb_annonces_scrapees", 0)

            # Un log est considere en echec si erreurs > 0 ET aucune annonce scrapee
            if nb_erreurs > 0 and nb_scrapees == 0:
                consecutive_failures += 1
            else:
                last_success = log.get("date_exec")
                break

        # Determiner le statut
        if consecutive_failures >= self.consecutive_failures_threshold:
            status = "critical"
            message = (
                f"Source {source} indisponible: {consecutive_failures} "
                f"echecs consecutifs."
            )
        elif consecutive_failures > 0:
            status = "warning"
            message = (
                f"Source {source}: {consecutive_failures} echec(s) "
                f"recent(s)."
            )
        else:
            status = "ok"
            message = f"Source {source} operationnelle."

        return {
            "status": status,
            "message": message,
            "details": {
                "consecutive_failures": consecutive_failures,
                "last_success": last_success,
                "total_logs": len(source_logs),
            },
        }

    def check_parsing_rate(
        self, scraping_logs: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """Verifie le taux de succes du parsing.

        Calcule le ratio d'annonces parsees avec succes par rapport au total.
        Alerte si le taux tombe en dessous du seuil configure.

        Args:
            scraping_logs: Liste de logs de scraping recents.
                Chaque log contient: nb_annonces_scrapees, nb_erreurs.

        Returns:
            Dictionnaire avec:
                - status: 'ok', 'warning' ou 'critical'.
                - message: Description du taux de parsing.
                - details: Dict avec rate, total, successful, errors.
        """
        if not scraping_logs:
            return {
                "status": "warning",
                "message": "Aucun log de scraping disponible pour le calcul du taux.",
                "details": {
                    "rate": 0.0,
                    "total": 0,
                    "successful": 0,
                    "errors": 0,
                },
            }

        total_scrapees = 0
        total_erreurs = 0

        for log in scraping_logs:
            total_scrapees += log.get("nb_annonces_scrapees", 0)
            total_erreurs += log.get("nb_erreurs", 0)

        total = total_scrapees + total_erreurs
        if total == 0:
            return {
                "status": "warning",
                "message": "Aucune annonce traitee dans les logs recents.",
                "details": {
                    "rate": 0.0,
                    "total": 0,
                    "successful": 0,
                    "errors": total_erreurs,
                },
            }

        rate = total_scrapees / total

        if rate < self.parsing_rate_threshold:
            status = "critical"
            message = (
                f"Taux de parsing critique: {rate:.1%} "
                f"(seuil: {self.parsing_rate_threshold:.0%}). "
                f"Changement de DOM probable."
            )
        elif rate < self.parsing_rate_threshold + 0.15:
            status = "warning"
            message = (
                f"Taux de parsing en baisse: {rate:.1%} "
                f"(seuil: {self.parsing_rate_threshold:.0%})."
            )
        else:
            status = "ok"
            message = f"Taux de parsing normal: {rate:.1%}."

        return {
            "status": status,
            "message": message,
            "details": {
                "rate": round(rate, 4),
                "total": total,
                "successful": total_scrapees,
                "errors": total_erreurs,
            },
        }

    def check_new_listings(
        self,
        scraping_logs: list[dict[str, Any]],
        hours: int = 24,
    ) -> dict[str, Any]:
        """Verifie qu'il y a eu des nouvelles annonces dans les dernieres N heures.

        Alerte si aucune nouvelle annonce n'a ete detectee dans la periode specifiee.

        Args:
            scraping_logs: Liste de logs de scraping avec date_exec et nb_nouvelles.
            hours: Nombre d'heures a verifier (defaut: 24).

        Returns:
            Dictionnaire avec:
                - status: 'ok', 'warning' ou 'critical'.
                - message: Description du statut.
                - details: Dict avec total_new, hours_checked, logs_in_period.
        """
        if not scraping_logs:
            return {
                "status": "warning",
                "message": f"Aucun log de scraping disponible (periode: {hours}h).",
                "details": {
                    "total_new": 0,
                    "hours_checked": hours,
                    "logs_in_period": 0,
                },
            }

        cutoff = datetime.now() - timedelta(hours=hours)
        total_new = 0
        logs_in_period = 0

        for log in scraping_logs:
            date_exec = log.get("date_exec")
            if date_exec is None:
                continue

            # Gerer les dates en string ou datetime
            if isinstance(date_exec, str):
                try:
                    date_exec = datetime.fromisoformat(date_exec)
                except ValueError:
                    continue

            if date_exec >= cutoff:
                logs_in_period += 1
                total_new += log.get("nb_nouvelles", 0)

        if logs_in_period == 0:
            status = "critical"
            message = (
                f"Aucun scraping execute dans les {hours} dernieres heures."
            )
        elif total_new == 0:
            status = "critical"
            message = (
                f"Zero nouvelle annonce en {hours}h "
                f"({logs_in_period} executions)."
            )
        else:
            status = "ok"
            message = (
                f"{total_new} nouvelle(s) annonce(s) en {hours}h "
                f"({logs_in_period} executions)."
            )

        return {
            "status": status,
            "message": message,
            "details": {
                "total_new": total_new,
                "hours_checked": hours,
                "logs_in_period": logs_in_period,
            },
        }

    def check_api_budget(
        self,
        daily_calls: int,
        max_calls: int = 300,
    ) -> dict[str, Any]:
        """Verifie la consommation du budget API Claude.

        Emet un avertissement si le budget depasse le seuil configure,
        et une alerte critique si le plafond est atteint.

        Args:
            daily_calls: Nombre d'appels API effectues aujourd'hui.
            max_calls: Plafond journalier d'appels API (defaut: 300).

        Returns:
            Dictionnaire avec:
                - status: 'ok', 'warning' ou 'critical'.
                - message: Description de la consommation.
                - details: Dict avec daily_calls, max_calls, usage_pct,
                  remaining.
        """
        if max_calls <= 0:
            return {
                "status": "critical",
                "message": "Plafond API invalide (max_calls <= 0).",
                "details": {
                    "daily_calls": daily_calls,
                    "max_calls": max_calls,
                    "usage_pct": 0.0,
                    "remaining": 0,
                },
            }

        usage_pct = daily_calls / max_calls
        remaining = max(0, max_calls - daily_calls)

        if daily_calls >= max_calls:
            status = "critical"
            message = (
                f"Budget API Claude atteint: {daily_calls}/{max_calls} "
                f"appels. Enrichissement en pause."
            )
        elif usage_pct >= self.api_budget_warning_threshold:
            status = "warning"
            message = (
                f"Budget API Claude: {daily_calls}/{max_calls} "
                f"({usage_pct:.0%}). {remaining} appels restants."
            )
        else:
            status = "ok"
            message = (
                f"Budget API Claude: {daily_calls}/{max_calls} "
                f"({usage_pct:.0%}). {remaining} appels restants."
            )

        return {
            "status": status,
            "message": message,
            "details": {
                "daily_calls": daily_calls,
                "max_calls": max_calls,
                "usage_pct": round(usage_pct, 4),
                "remaining": remaining,
            },
        }

    def check_disk_space(self, path: str = "/") -> dict[str, Any]:
        """Verifie l'espace disque disponible.

        Alerte si l'espace libre tombe en dessous du seuil configure (20%).

        Args:
            path: Chemin du systeme de fichiers a verifier (defaut: '/').

        Returns:
            Dictionnaire avec:
                - status: 'ok', 'warning' ou 'critical'.
                - message: Description de l'espace disque.
                - details: Dict avec total_gb, used_gb, free_gb, free_pct.
        """
        try:
            usage = shutil.disk_usage(path)
        except OSError as e:
            return {
                "status": "critical",
                "message": f"Impossible de verifier l'espace disque: {e}",
                "details": {
                    "total_gb": 0.0,
                    "used_gb": 0.0,
                    "free_gb": 0.0,
                    "free_pct": 0.0,
                },
            }

        total_gb = round(usage.total / (1024**3), 2)
        used_gb = round(usage.used / (1024**3), 2)
        free_gb = round(usage.free / (1024**3), 2)
        free_pct = usage.free / usage.total if usage.total > 0 else 0.0

        if free_pct < self.disk_space_threshold:
            status = "critical"
            message = (
                f"Espace disque critique: {free_gb}Go libres "
                f"({free_pct:.0%} restant, seuil: {self.disk_space_threshold:.0%})."
            )
        elif free_pct < self.disk_space_threshold + 0.10:
            status = "warning"
            message = (
                f"Espace disque faible: {free_gb}Go libres "
                f"({free_pct:.0%} restant)."
            )
        else:
            status = "ok"
            message = f"Espace disque OK: {free_gb}Go libres ({free_pct:.0%} restant)."

        return {
            "status": status,
            "message": message,
            "details": {
                "total_gb": total_gb,
                "used_gb": used_gb,
                "free_gb": free_gb,
                "free_pct": round(free_pct, 4),
            },
        }

    def generate_health_report(
        self,
        scraping_logs: list[dict[str, Any]] | None = None,
        sources: list[str] | None = None,
        daily_api_calls: int = 0,
        max_api_calls: int = 300,
        disk_path: str = "/",
    ) -> dict[str, Any]:
        """Genere un rapport de sante agrege de tous les composants.

        Execute toutes les verifications et combine les resultats en un
        rapport unique avec un statut global.

        Args:
            scraping_logs: Logs de scraping pour toutes les sources.
            sources: Liste des sources a verifier (defaut: ['leboncoin', 'pap']).
            daily_api_calls: Nombre d'appels API effectues aujourd'hui.
            max_api_calls: Plafond journalier d'appels API.
            disk_path: Chemin du systeme de fichiers a verifier.

        Returns:
            Dictionnaire avec:
                - status: Statut global ('ok', 'warning', 'critical').
                - timestamp: Date/heure du rapport.
                - checks: Dictionnaire de tous les resultats de verification.
                - summary: Resume textuel du rapport.
        """
        if scraping_logs is None:
            scraping_logs = []
        if sources is None:
            sources = ["leboncoin", "pap"]

        checks: dict[str, dict[str, Any]] = {}

        # Verification de chaque source
        for source in sources:
            checks[f"source_{source}"] = self.check_source_health(
                source, scraping_logs
            )

        # Taux de parsing
        checks["parsing_rate"] = self.check_parsing_rate(scraping_logs)

        # Nouvelles annonces
        checks["new_listings"] = self.check_new_listings(
            scraping_logs, self.zero_listings_hours
        )

        # Budget API
        checks["api_budget"] = self.check_api_budget(
            daily_api_calls, max_api_calls
        )

        # Espace disque
        checks["disk_space"] = self.check_disk_space(disk_path)

        # Determiner le statut global
        all_statuses = [check["status"] for check in checks.values()]

        if "critical" in all_statuses:
            global_status = "critical"
        elif "warning" in all_statuses:
            global_status = "warning"
        else:
            global_status = "ok"

        # Compter les problemes
        nb_critical = all_statuses.count("critical")
        nb_warning = all_statuses.count("warning")
        nb_ok = all_statuses.count("ok")

        summary = (
            f"Sante globale: {global_status.upper()} - "
            f"{nb_ok} OK, {nb_warning} avertissement(s), "
            f"{nb_critical} critique(s)."
        )

        logger.info("Rapport de sante: %s", summary)

        return {
            "status": global_status,
            "timestamp": datetime.now().isoformat(),
            "checks": checks,
            "summary": summary,
        }
