"""Tests pour les alertes Telegram et le monitoring de sante.

Couvre les modules :
- src.alerts.formatter (AlertFormatter)
- src.alerts.telegram_bot (TelegramBot)
- src.monitoring.health (HealthMonitor)

Tous les tests fonctionnent sans acces reseau ou API Telegram (tout est mocke).
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.alerts.formatter import AlertFormatter
from src.alerts.telegram_bot import TelegramBot
from src.monitoring.health import HealthMonitor


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def formatter() -> AlertFormatter:
    """Retourne une instance d'AlertFormatter."""
    return AlertFormatter()


@pytest.fixture
def sample_annonce() -> dict[str, Any]:
    """Retourne une annonce de vente fictive pour les tests d'alerte."""
    return {
        "url_source": "https://www.leboncoin.fr/ventes_immobilieres/1234567890.htm",
        "source": "leboncoin",
        "prix": 145000,
        "surface_m2": 55.0,
        "nb_pieces": 3,
        "dpe": "C",
        "etage": 2,
        "adresse_brute": "12 rue de la Republique, 25000 Besancon",
        "quartier": "Centre-Ville",
        "description_texte": "Bel appartement T3 lumineux.",
    }


@pytest.fixture
def sample_score() -> dict[str, Any]:
    """Retourne des donnees de scoring fictives."""
    return {
        "score_global": 85.5,
        "score_rentabilite": 90.0,
        "score_localisation": 80.0,
        "score_dpe": 65.0,
        "score_negociation": 30.0,
        "score_vacance": 60.0,
        "niveau_alerte": "top",
    }


@pytest.fixture
def sample_renta() -> dict[str, Any]:
    """Retourne des donnees de rentabilite fictives."""
    return {
        "renta_brute": 8.5,
        "renta_brute_nego_5": 8.9,
        "renta_brute_nego_10": 9.4,
        "renta_brute_nego_15": 10.0,
        "loyer_estime": 580.0,
    }


@pytest.fixture
def sample_enrichment() -> dict[str, Any]:
    """Retourne des donnees d'enrichissement IA fictives."""
    return {
        "signaux_nego": ["urgent", "prix a debattre"],
        "etat_bien": "bon_etat",
        "equipements": ["parking", "cave", "double_vitrage"],
        "red_flags": [],
        "info_copro": {"nb_lots": 12, "charges_annuelles": 1200},
        "resume_ia": "T3 lumineux en bon etat, parking inclus, copro saine.",
    }


@pytest.fixture
def health_monitor() -> HealthMonitor:
    """Retourne une instance de HealthMonitor avec les seuils par defaut."""
    return HealthMonitor()


@pytest.fixture
def healthy_scraping_logs() -> list[dict[str, Any]]:
    """Retourne des logs de scraping sains (pas d'erreur)."""
    now = datetime.now()
    return [
        {
            "source": "leboncoin",
            "type_scrape": "vente",
            "date_exec": (now - timedelta(hours=1)).isoformat(),
            "nb_annonces_scrapees": 25,
            "nb_nouvelles": 3,
            "nb_erreurs": 0,
        },
        {
            "source": "leboncoin",
            "type_scrape": "vente",
            "date_exec": (now - timedelta(hours=4)).isoformat(),
            "nb_annonces_scrapees": 20,
            "nb_nouvelles": 2,
            "nb_erreurs": 0,
        },
        {
            "source": "pap",
            "type_scrape": "vente",
            "date_exec": (now - timedelta(hours=2)).isoformat(),
            "nb_annonces_scrapees": 15,
            "nb_nouvelles": 1,
            "nb_erreurs": 0,
        },
    ]


@pytest.fixture
def failing_scraping_logs() -> list[dict[str, Any]]:
    """Retourne des logs de scraping avec 3+ echecs consecutifs pour leboncoin."""
    now = datetime.now()
    return [
        {
            "source": "leboncoin",
            "type_scrape": "vente",
            "date_exec": (now - timedelta(hours=1)).isoformat(),
            "nb_annonces_scrapees": 0,
            "nb_nouvelles": 0,
            "nb_erreurs": 5,
            "erreur_detail": "Timeout",
        },
        {
            "source": "leboncoin",
            "type_scrape": "vente",
            "date_exec": (now - timedelta(hours=4)).isoformat(),
            "nb_annonces_scrapees": 0,
            "nb_nouvelles": 0,
            "nb_erreurs": 3,
            "erreur_detail": "403 Forbidden",
        },
        {
            "source": "leboncoin",
            "type_scrape": "vente",
            "date_exec": (now - timedelta(hours=8)).isoformat(),
            "nb_annonces_scrapees": 0,
            "nb_nouvelles": 0,
            "nb_erreurs": 2,
            "erreur_detail": "Connection refused",
        },
        {
            "source": "leboncoin",
            "type_scrape": "vente",
            "date_exec": (now - timedelta(hours=12)).isoformat(),
            "nb_annonces_scrapees": 18,
            "nb_nouvelles": 4,
            "nb_erreurs": 0,
        },
    ]


# ---------------------------------------------------------------------------
# Tests AlertFormatter
# ---------------------------------------------------------------------------


class TestAlertFormatterTopAlert:
    """Tests pour format_top_alert."""

    def test_top_alert_contains_all_fields(
        self,
        formatter: AlertFormatter,
        sample_annonce: dict[str, Any],
        sample_score: dict[str, Any],
        sample_renta: dict[str, Any],
    ) -> None:
        """Verifie que l'alerte TOP contient tous les champs requis."""
        result = formatter.format_top_alert(sample_annonce, sample_score, sample_renta)

        assert "TOP OPPORTUNITE" in result
        assert "12 rue de la Republique" in result
        assert "Centre-Ville" in result
        assert "145 000" in result
        assert "55.0" in result
        assert "T3" in result
        assert "DPE: C" in result
        assert "85.5" in result
        assert "8.5%" in result
        assert "leboncoin.fr" in result

    def test_top_alert_contains_nego_scenarios(
        self,
        formatter: AlertFormatter,
        sample_annonce: dict[str, Any],
        sample_score: dict[str, Any],
        sample_renta: dict[str, Any],
    ) -> None:
        """Verifie que l'alerte TOP contient les scenarios de negociation."""
        result = formatter.format_top_alert(sample_annonce, sample_score, sample_renta)

        assert "8.9%" in result
        assert "9.4%" in result
        assert "10.0%" in result
        assert "-5%" in result
        assert "-10%" in result
        assert "-15%" in result

    def test_top_alert_with_enrichment(
        self,
        formatter: AlertFormatter,
        sample_annonce: dict[str, Any],
        sample_score: dict[str, Any],
        sample_renta: dict[str, Any],
        sample_enrichment: dict[str, Any],
    ) -> None:
        """Verifie que le resume IA est inclus quand l'enrichissement est fourni."""
        result = formatter.format_top_alert(
            sample_annonce, sample_score, sample_renta, sample_enrichment
        )

        assert "T3 lumineux en bon etat" in result
        assert "parking inclus" in result

    def test_top_alert_without_enrichment(
        self,
        formatter: AlertFormatter,
        sample_annonce: dict[str, Any],
        sample_score: dict[str, Any],
        sample_renta: dict[str, Any],
    ) -> None:
        """Verifie que l'alerte fonctionne sans enrichissement IA."""
        result = formatter.format_top_alert(
            sample_annonce, sample_score, sample_renta, None
        )

        assert "TOP OPPORTUNITE" in result
        # Pas de resume IA dans le resultat
        assert "\U0001f916" not in result

    def test_top_alert_contains_url_link(
        self,
        formatter: AlertFormatter,
        sample_annonce: dict[str, Any],
        sample_score: dict[str, Any],
        sample_renta: dict[str, Any],
    ) -> None:
        """Verifie que le lien vers l'annonce est present."""
        result = formatter.format_top_alert(sample_annonce, sample_score, sample_renta)

        assert "[Voir l'annonce]" in result
        assert sample_annonce["url_source"] in result


class TestAlertFormatterBonAlert:
    """Tests pour format_bon_alert."""

    def test_bon_alert_contains_all_fields(
        self,
        formatter: AlertFormatter,
        sample_annonce: dict[str, Any],
        sample_score: dict[str, Any],
        sample_renta: dict[str, Any],
    ) -> None:
        """Verifie que l'alerte BON contient tous les champs requis."""
        result = formatter.format_bon_alert(sample_annonce, sample_score, sample_renta)

        assert "BON PLAN" in result
        assert "12 rue de la Republique" in result
        assert "Centre-Ville" in result
        assert "145 000" in result
        assert "55.0" in result
        assert "T3" in result
        assert "85.5" in result
        assert "8.5%" in result
        assert "leboncoin.fr" in result

    def test_bon_alert_has_bon_plan_header(
        self,
        formatter: AlertFormatter,
        sample_annonce: dict[str, Any],
        sample_score: dict[str, Any],
        sample_renta: dict[str, Any],
    ) -> None:
        """Verifie que l'en-tete est BON PLAN et non TOP OPPORTUNITE."""
        result = formatter.format_bon_alert(sample_annonce, sample_score, sample_renta)

        assert "BON PLAN" in result
        assert "TOP OPPORTUNITE" not in result


class TestAlertFormatterBaissePrix:
    """Tests pour format_baisse_prix."""

    def test_baisse_prix_with_correct_difference(
        self,
        formatter: AlertFormatter,
        sample_annonce: dict[str, Any],
    ) -> None:
        """Verifie le calcul et l'affichage de la difference de prix."""
        result = formatter.format_baisse_prix(sample_annonce, 150000, 140000)

        assert "BAISSE DE PRIX" in result
        assert "150 000" in result
        assert "140 000" in result
        assert "10 000" in result
        assert "6.7%" in result

    def test_baisse_prix_contains_annonce_info(
        self,
        formatter: AlertFormatter,
        sample_annonce: dict[str, Any],
    ) -> None:
        """Verifie que les informations de l'annonce sont presentes."""
        result = formatter.format_baisse_prix(sample_annonce, 150000, 140000)

        assert "Centre-Ville" in result
        assert "55.0" in result
        assert "T3" in result
        assert "[Voir l'annonce]" in result


class TestAlertFormatterDigest:
    """Tests pour format_digest."""

    def test_digest_includes_top_3(self, formatter: AlertFormatter) -> None:
        """Verifie que le digest inclut les top 3 annonces."""
        top_annonces = [
            {
                "annonce": {"quartier": "Centre-Ville", "prix": 140000, "url_source": "https://example.com/1"},
                "score": {"score_global": 90},
                "renta": {"renta_brute": 9.2},
            },
            {
                "annonce": {"quartier": "Battant", "prix": 130000, "url_source": "https://example.com/2"},
                "score": {"score_global": 85},
                "renta": {"renta_brute": 8.8},
            },
        ]
        stats = {"nb_scrapees": 50, "nb_nouvelles": 8, "nb_erreurs": 2, "sources": ["leboncoin", "pap"]}
        obs_stats = {"nb_locations": 30, "segments_couverts": 6, "fiabilite": "fiable"}

        result = formatter.format_digest(top_annonces, [], stats, obs_stats)

        assert "DIGEST QUOTIDIEN" in result
        assert "Centre-Ville" in result
        assert "Battant" in result
        assert "140 000" in result
        assert "130 000" in result
        assert "9.2%" in result
        assert "8.8%" in result

    def test_digest_includes_stats(self, formatter: AlertFormatter) -> None:
        """Verifie que le digest inclut les statistiques du pipeline."""
        stats = {"nb_scrapees": 50, "nb_nouvelles": 8, "nb_erreurs": 2, "sources": ["leboncoin", "pap"]}
        obs_stats = {"nb_locations": 30, "segments_couverts": 6, "fiabilite": "fiable"}

        result = formatter.format_digest([], [], stats, obs_stats)

        assert "50" in result
        assert "8" in result
        assert "2" in result
        assert "leboncoin" in result
        assert "pap" in result

    def test_digest_includes_observatory_stats(self, formatter: AlertFormatter) -> None:
        """Verifie que le digest inclut les statistiques de l'observatoire."""
        stats = {"nb_scrapees": 0, "nb_nouvelles": 0, "nb_erreurs": 0}
        obs_stats = {"nb_locations": 30, "segments_couverts": 6, "fiabilite": "fiable"}

        result = formatter.format_digest([], [], stats, obs_stats)

        assert "Observatoire loyers" in result
        assert "30" in result
        assert "6" in result
        assert "fiable" in result

    def test_digest_includes_baisses(self, formatter: AlertFormatter) -> None:
        """Verifie que le digest inclut les baisses de prix."""
        baisses = [
            {"annonce": {"quartier": "Centre-Ville"}, "ancien_prix": 150000, "nouveau_prix": 140000},
        ]
        stats = {"nb_scrapees": 0, "nb_nouvelles": 0, "nb_erreurs": 0}
        obs_stats = {"nb_locations": 0, "segments_couverts": 0, "fiabilite": "N/A"}

        result = formatter.format_digest([], baisses, stats, obs_stats)

        assert "Baisses de prix" in result
        assert "Centre-Ville" in result
        assert "150 000" in result
        assert "140 000" in result

    def test_empty_digest(self, formatter: AlertFormatter) -> None:
        """Verifie le digest quand il n'y a aucune annonce."""
        stats = {"nb_scrapees": 0, "nb_nouvelles": 0, "nb_erreurs": 0}
        obs_stats = {"nb_locations": 0, "segments_couverts": 0, "fiabilite": "N/A"}

        result = formatter.format_digest([], [], stats, obs_stats)

        assert "DIGEST QUOTIDIEN" in result
        assert "Aucune opportunite" in result
        assert "Aucune baisse" in result


class TestAlertFormatterSystemAlert:
    """Tests pour format_system_alert."""

    def test_system_alert_format(self, formatter: AlertFormatter) -> None:
        """Verifie le formatage d'une alerte systeme."""
        result = formatter.format_system_alert(
            "source_indisponible", "LeBonCoin: 3 echecs consecutifs"
        )

        assert "ALERTE SYSTEME" in result
        assert "source_indisponible" in result
        assert "LeBonCoin: 3 echecs consecutifs" in result
        assert "Date:" in result


class TestAlertFormatterHelpers:
    """Tests pour les methodes utilitaires de formatage."""

    def test_format_prix_standard(self, formatter: AlertFormatter) -> None:
        """Verifie le formatage standard d'un prix."""
        assert formatter._format_prix(145000) == "145 000\u20ac"

    def test_format_prix_small(self, formatter: AlertFormatter) -> None:
        """Verifie le formatage d'un petit montant."""
        assert formatter._format_prix(500) == "500\u20ac"

    def test_format_prix_large(self, formatter: AlertFormatter) -> None:
        """Verifie le formatage d'un montant eleve."""
        assert formatter._format_prix(1250000) == "1 250 000\u20ac"

    def test_format_prix_zero(self, formatter: AlertFormatter) -> None:
        """Verifie le formatage d'un prix nul."""
        assert formatter._format_prix(0) == "0\u20ac"

    def test_format_renta_one_decimal(self, formatter: AlertFormatter) -> None:
        """Verifie le formatage de la rentabilite a une decimale."""
        assert formatter._format_renta(8.234) == "8.2%"

    def test_format_renta_rounds_correctly(self, formatter: AlertFormatter) -> None:
        """Verifie l'arrondi de la rentabilite."""
        assert formatter._format_renta(8.96) == "9.0%"

    def test_format_renta_integer(self, formatter: AlertFormatter) -> None:
        """Verifie le formatage d'une rentabilite entiere."""
        assert formatter._format_renta(8.0) == "8.0%"


# ---------------------------------------------------------------------------
# Tests TelegramBot
# ---------------------------------------------------------------------------


class TestTelegramBotSendAlert:
    """Tests pour send_alert du TelegramBot."""

    @pytest.mark.asyncio
    async def test_send_alert_success(self) -> None:
        """Verifie l'envoi reussi d'un message Telegram."""
        with patch("src.alerts.telegram_bot.Bot") as MockBot:
            mock_bot_instance = AsyncMock()
            MockBot.return_value = mock_bot_instance
            mock_bot_instance.send_message = AsyncMock(return_value=True)

            bot = TelegramBot(bot_token="fake-token", chat_id="-100123")
            bot._bot = mock_bot_instance

            result = await bot.send_alert("Test message", "top")

            assert result is True
            mock_bot_instance.send_message.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_alert_failure_returns_false(self) -> None:
        """Verifie que l'envoi echoue proprement en cas d'erreur Telegram."""
        with patch("src.alerts.telegram_bot.Bot") as MockBot:
            from telegram.error import TelegramError

            mock_bot_instance = AsyncMock()
            MockBot.return_value = mock_bot_instance
            mock_bot_instance.send_message = AsyncMock(
                side_effect=TelegramError("API Error")
            )

            bot = TelegramBot(bot_token="fake-token", chat_id="-100123")
            bot._bot = mock_bot_instance

            result = await bot.send_alert("Test message", "top")

            assert result is False

    @pytest.mark.asyncio
    async def test_send_alert_unexpected_error_returns_false(self) -> None:
        """Verifie la gestion d'une exception inattendue."""
        with patch("src.alerts.telegram_bot.Bot") as MockBot:
            mock_bot_instance = AsyncMock()
            MockBot.return_value = mock_bot_instance
            mock_bot_instance.send_message = AsyncMock(
                side_effect=RuntimeError("Unexpected")
            )

            bot = TelegramBot(bot_token="fake-token", chat_id="-100123")
            bot._bot = mock_bot_instance

            result = await bot.send_alert("Test message", "top")

            assert result is False


class TestTelegramBotSendImmediate:
    """Tests pour send_immediate du TelegramBot."""

    @pytest.mark.asyncio
    async def test_send_immediate_calls_send_alert(
        self,
        sample_annonce: dict[str, Any],
        sample_score: dict[str, Any],
        sample_renta: dict[str, Any],
    ) -> None:
        """Verifie que send_immediate formate et envoie un message."""
        with patch("src.alerts.telegram_bot.Bot") as MockBot:
            mock_bot_instance = AsyncMock()
            MockBot.return_value = mock_bot_instance
            mock_bot_instance.send_message = AsyncMock(return_value=True)

            bot = TelegramBot(bot_token="fake-token", chat_id="-100123")
            bot._bot = mock_bot_instance

            result = await bot.send_immediate(
                sample_annonce, sample_score, sample_renta
            )

            assert result is True
            mock_bot_instance.send_message.assert_called_once()
            call_kwargs = mock_bot_instance.send_message.call_args
            sent_text = call_kwargs.kwargs.get("text", "") or call_kwargs[1].get("text", "")
            assert "TOP OPPORTUNITE" in sent_text or "TOP" in sent_text

    @pytest.mark.asyncio
    async def test_send_immediate_with_enrichment(
        self,
        sample_annonce: dict[str, Any],
        sample_score: dict[str, Any],
        sample_renta: dict[str, Any],
        sample_enrichment: dict[str, Any],
    ) -> None:
        """Verifie que send_immediate inclut les donnees d'enrichissement."""
        with patch("src.alerts.telegram_bot.Bot") as MockBot:
            mock_bot_instance = AsyncMock()
            MockBot.return_value = mock_bot_instance
            mock_bot_instance.send_message = AsyncMock(return_value=True)

            bot = TelegramBot(bot_token="fake-token", chat_id="-100123")
            bot._bot = mock_bot_instance

            result = await bot.send_immediate(
                sample_annonce, sample_score, sample_renta, sample_enrichment
            )

            assert result is True


class TestTelegramBotSendDigest:
    """Tests pour send_digest du TelegramBot."""

    @pytest.mark.asyncio
    async def test_send_digest_success(self) -> None:
        """Verifie l'envoi reussi du digest quotidien."""
        with patch("src.alerts.telegram_bot.Bot") as MockBot:
            mock_bot_instance = AsyncMock()
            MockBot.return_value = mock_bot_instance
            mock_bot_instance.send_message = AsyncMock(return_value=True)

            bot = TelegramBot(bot_token="fake-token", chat_id="-100123")
            bot._bot = mock_bot_instance

            result = await bot.send_digest("Digest content here")

            assert result is True


class TestTelegramBotSendSystemAlert:
    """Tests pour send_system_alert du TelegramBot."""

    @pytest.mark.asyncio
    async def test_send_system_alert_success(self) -> None:
        """Verifie l'envoi reussi d'une alerte systeme."""
        with patch("src.alerts.telegram_bot.Bot") as MockBot:
            mock_bot_instance = AsyncMock()
            MockBot.return_value = mock_bot_instance
            mock_bot_instance.send_message = AsyncMock(return_value=True)

            bot = TelegramBot(bot_token="fake-token", chat_id="-100123")
            bot._bot = mock_bot_instance

            result = await bot.send_system_alert(
                "source_indisponible", "LeBonCoin timeout"
            )

            assert result is True


class TestTelegramBotEscapeMarkdown:
    """Tests pour _escape_markdown."""

    def test_escape_special_characters(self) -> None:
        """Verifie l'echappement des caracteres speciaux MarkdownV2."""
        result = TelegramBot._escape_markdown("Price: 100.5!")

        assert "\\." in result
        assert "\\!" in result

    def test_escape_preserves_already_escaped(self) -> None:
        """Verifie que les caracteres deja echappes ne sont pas re-echappes."""
        result = TelegramBot._escape_markdown("Already \\. escaped")

        # Le '.' deja echappe ne doit pas devenir '\\..'
        assert "\\\\." not in result

    def test_escape_underscores(self) -> None:
        """Verifie l'echappement des underscores."""
        result = TelegramBot._escape_markdown("test_value")

        assert "\\_" in result

    def test_escape_empty_string(self) -> None:
        """Verifie le comportement avec une chaine vide."""
        result = TelegramBot._escape_markdown("")

        assert result == ""


# ---------------------------------------------------------------------------
# Tests HealthMonitor
# ---------------------------------------------------------------------------


class TestHealthMonitorSourceHealth:
    """Tests pour check_source_health."""

    def test_source_healthy(
        self,
        health_monitor: HealthMonitor,
        healthy_scraping_logs: list[dict[str, Any]],
    ) -> None:
        """Verifie le statut OK d'une source operationnelle."""
        result = health_monitor.check_source_health("leboncoin", healthy_scraping_logs)

        assert result["status"] == "ok"
        assert result["details"]["consecutive_failures"] == 0
        assert "operationnelle" in result["message"]

    def test_source_3_consecutive_failures(
        self,
        health_monitor: HealthMonitor,
        failing_scraping_logs: list[dict[str, Any]],
    ) -> None:
        """Verifie la detection de 3 echecs consecutifs."""
        result = health_monitor.check_source_health("leboncoin", failing_scraping_logs)

        assert result["status"] == "critical"
        assert result["details"]["consecutive_failures"] == 3
        assert "indisponible" in result["message"]
        assert result["details"]["last_success"] is not None

    def test_source_no_logs(self, health_monitor: HealthMonitor) -> None:
        """Verifie le comportement sans logs de scraping."""
        result = health_monitor.check_source_health("leboncoin", [])

        assert result["status"] == "warning"
        assert result["details"]["total_logs"] == 0

    def test_source_partial_failures(self, health_monitor: HealthMonitor) -> None:
        """Verifie le statut warning avec 1-2 echecs consecutifs."""
        now = datetime.now()
        logs = [
            {
                "source": "pap",
                "date_exec": (now - timedelta(hours=1)).isoformat(),
                "nb_annonces_scrapees": 0,
                "nb_nouvelles": 0,
                "nb_erreurs": 3,
            },
            {
                "source": "pap",
                "date_exec": (now - timedelta(hours=4)).isoformat(),
                "nb_annonces_scrapees": 10,
                "nb_nouvelles": 2,
                "nb_erreurs": 0,
            },
        ]

        result = health_monitor.check_source_health("pap", logs)

        assert result["status"] == "warning"
        assert result["details"]["consecutive_failures"] == 1


class TestHealthMonitorParsingRate:
    """Tests pour check_parsing_rate."""

    def test_parsing_rate_normal(
        self,
        health_monitor: HealthMonitor,
        healthy_scraping_logs: list[dict[str, Any]],
    ) -> None:
        """Verifie un taux de parsing normal (100%)."""
        result = health_monitor.check_parsing_rate(healthy_scraping_logs)

        assert result["status"] == "ok"
        assert result["details"]["rate"] == 1.0
        assert result["details"]["errors"] == 0

    def test_low_parsing_rate(self, health_monitor: HealthMonitor) -> None:
        """Verifie l'alerte sur un taux de parsing faible."""
        logs = [
            {"nb_annonces_scrapees": 3, "nb_erreurs": 10},
            {"nb_annonces_scrapees": 2, "nb_erreurs": 8},
        ]

        result = health_monitor.check_parsing_rate(logs)

        assert result["status"] == "critical"
        assert result["details"]["rate"] < 0.50
        assert "critique" in result["message"]

    def test_parsing_rate_no_logs(self, health_monitor: HealthMonitor) -> None:
        """Verifie le comportement sans logs."""
        result = health_monitor.check_parsing_rate([])

        assert result["status"] == "warning"
        assert result["details"]["total"] == 0


class TestHealthMonitorNewListings:
    """Tests pour check_new_listings."""

    def test_new_listings_found(
        self,
        health_monitor: HealthMonitor,
        healthy_scraping_logs: list[dict[str, Any]],
    ) -> None:
        """Verifie la detection de nouvelles annonces."""
        result = health_monitor.check_new_listings(healthy_scraping_logs, hours=24)

        assert result["status"] == "ok"
        assert result["details"]["total_new"] > 0

    def test_zero_new_listings(self, health_monitor: HealthMonitor) -> None:
        """Verifie l'alerte quand aucune nouvelle annonce en 24h."""
        now = datetime.now()
        logs = [
            {
                "source": "leboncoin",
                "date_exec": (now - timedelta(hours=2)).isoformat(),
                "nb_annonces_scrapees": 20,
                "nb_nouvelles": 0,
                "nb_erreurs": 0,
            },
            {
                "source": "pap",
                "date_exec": (now - timedelta(hours=4)).isoformat(),
                "nb_annonces_scrapees": 15,
                "nb_nouvelles": 0,
                "nb_erreurs": 0,
            },
        ]

        result = health_monitor.check_new_listings(logs, hours=24)

        assert result["status"] == "critical"
        assert result["details"]["total_new"] == 0
        assert "Zero" in result["message"]

    def test_no_scraping_in_period(self, health_monitor: HealthMonitor) -> None:
        """Verifie l'alerte quand aucun scraping dans la periode."""
        old_logs = [
            {
                "source": "leboncoin",
                "date_exec": (datetime.now() - timedelta(hours=48)).isoformat(),
                "nb_annonces_scrapees": 20,
                "nb_nouvelles": 5,
                "nb_erreurs": 0,
            },
        ]

        result = health_monitor.check_new_listings(old_logs, hours=24)

        assert result["status"] == "critical"
        assert result["details"]["logs_in_period"] == 0


class TestHealthMonitorApiBudget:
    """Tests pour check_api_budget."""

    def test_api_budget_ok(self, health_monitor: HealthMonitor) -> None:
        """Verifie le statut OK quand le budget est largement disponible."""
        result = health_monitor.check_api_budget(50, 300)

        assert result["status"] == "ok"
        assert result["details"]["remaining"] == 250
        assert result["details"]["usage_pct"] < 0.80

    def test_api_budget_warning(self, health_monitor: HealthMonitor) -> None:
        """Verifie l'avertissement quand le budget approche la limite."""
        result = health_monitor.check_api_budget(250, 300)

        assert result["status"] == "warning"
        assert result["details"]["remaining"] == 50

    def test_api_budget_critical(self, health_monitor: HealthMonitor) -> None:
        """Verifie l'alerte critique quand le budget est epuise."""
        result = health_monitor.check_api_budget(300, 300)

        assert result["status"] == "critical"
        assert result["details"]["remaining"] == 0
        assert "pause" in result["message"]

    def test_api_budget_exceeded(self, health_monitor: HealthMonitor) -> None:
        """Verifie l'alerte quand le budget est depasse."""
        result = health_monitor.check_api_budget(350, 300)

        assert result["status"] == "critical"
        assert result["details"]["remaining"] == 0


class TestHealthMonitorDiskSpace:
    """Tests pour check_disk_space."""

    def test_disk_space_ok(self, health_monitor: HealthMonitor) -> None:
        """Verifie le statut OK quand l'espace disque est suffisant."""
        # Mock shutil.disk_usage pour simuler 50% libre
        mock_usage = MagicMock()
        mock_usage.total = 100 * (1024**3)  # 100 Go
        mock_usage.used = 50 * (1024**3)    # 50 Go
        mock_usage.free = 50 * (1024**3)    # 50 Go

        with patch("src.monitoring.health.shutil.disk_usage", return_value=mock_usage):
            result = health_monitor.check_disk_space("/")

        assert result["status"] == "ok"
        assert result["details"]["free_pct"] == 0.5
        assert result["details"]["free_gb"] == 50.0

    def test_disk_space_critical(self, health_monitor: HealthMonitor) -> None:
        """Verifie l'alerte critique quand l'espace disque est faible."""
        # Mock shutil.disk_usage pour simuler 10% libre
        mock_usage = MagicMock()
        mock_usage.total = 100 * (1024**3)  # 100 Go
        mock_usage.used = 90 * (1024**3)    # 90 Go
        mock_usage.free = 10 * (1024**3)    # 10 Go

        with patch("src.monitoring.health.shutil.disk_usage", return_value=mock_usage):
            result = health_monitor.check_disk_space("/")

        assert result["status"] == "critical"
        assert result["details"]["free_pct"] == 0.1
        assert "critique" in result["message"]

    def test_disk_space_warning(self, health_monitor: HealthMonitor) -> None:
        """Verifie l'avertissement quand l'espace disque approche le seuil."""
        # Mock shutil.disk_usage pour simuler 25% libre (entre 20% et 30%)
        mock_usage = MagicMock()
        mock_usage.total = 100 * (1024**3)
        mock_usage.used = 75 * (1024**3)
        mock_usage.free = 25 * (1024**3)

        with patch("src.monitoring.health.shutil.disk_usage", return_value=mock_usage):
            result = health_monitor.check_disk_space("/")

        assert result["status"] == "warning"
        assert result["details"]["free_pct"] == 0.25

    def test_disk_space_os_error(self, health_monitor: HealthMonitor) -> None:
        """Verifie la gestion d'une erreur OS lors de la verification disque."""
        with patch(
            "src.monitoring.health.shutil.disk_usage",
            side_effect=OSError("Permission denied"),
        ):
            result = health_monitor.check_disk_space("/nonexistent")

        assert result["status"] == "critical"
        assert "Impossible" in result["message"]


class TestHealthMonitorReport:
    """Tests pour generate_health_report."""

    def test_health_report_all_ok(
        self,
        health_monitor: HealthMonitor,
        healthy_scraping_logs: list[dict[str, Any]],
    ) -> None:
        """Verifie un rapport de sante entierement vert."""
        mock_usage = MagicMock()
        mock_usage.total = 100 * (1024**3)
        mock_usage.used = 50 * (1024**3)
        mock_usage.free = 50 * (1024**3)

        with patch("src.monitoring.health.shutil.disk_usage", return_value=mock_usage):
            result = health_monitor.generate_health_report(
                scraping_logs=healthy_scraping_logs,
                sources=["leboncoin", "pap"],
                daily_api_calls=50,
                max_api_calls=300,
            )

        assert result["status"] == "ok"
        assert "checks" in result
        assert "timestamp" in result
        assert "summary" in result
        # Toutes les verifications doivent etre presentes
        assert "source_leboncoin" in result["checks"]
        assert "source_pap" in result["checks"]
        assert "parsing_rate" in result["checks"]
        assert "new_listings" in result["checks"]
        assert "api_budget" in result["checks"]
        assert "disk_space" in result["checks"]

    def test_health_report_with_critical(
        self,
        health_monitor: HealthMonitor,
        failing_scraping_logs: list[dict[str, Any]],
    ) -> None:
        """Verifie que le rapport global est 'critical' si un check est critique."""
        mock_usage = MagicMock()
        mock_usage.total = 100 * (1024**3)
        mock_usage.used = 50 * (1024**3)
        mock_usage.free = 50 * (1024**3)

        with patch("src.monitoring.health.shutil.disk_usage", return_value=mock_usage):
            result = health_monitor.generate_health_report(
                scraping_logs=failing_scraping_logs,
                sources=["leboncoin"],
                daily_api_calls=50,
                max_api_calls=300,
            )

        assert result["status"] == "critical"
        assert "critique" in result["summary"]

    def test_health_report_empty_logs(self, health_monitor: HealthMonitor) -> None:
        """Verifie le rapport avec des logs vides."""
        mock_usage = MagicMock()
        mock_usage.total = 100 * (1024**3)
        mock_usage.used = 50 * (1024**3)
        mock_usage.free = 50 * (1024**3)

        with patch("src.monitoring.health.shutil.disk_usage", return_value=mock_usage):
            result = health_monitor.generate_health_report(
                scraping_logs=[],
                sources=["leboncoin"],
                daily_api_calls=0,
                max_api_calls=300,
            )

        # Doit y avoir des warnings (pas de logs)
        assert result["status"] in ("warning", "critical")
        assert "checks" in result
