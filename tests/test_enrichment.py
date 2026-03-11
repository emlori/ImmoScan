"""Tests pour le module d'enrichissement IA Claude.

Couvre le module src.enrichment.claude_enricher (ClaudeEnricher).
Tous les tests fonctionnent sans acces reseau ni cle API
(client Anthropic entierement mocke).
"""

from __future__ import annotations

import json
from datetime import date
from typing import Any
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from src.enrichment.claude_enricher import (
    EXPECTED_KEYS,
    VALID_ETAT_BIEN,
    ClaudeEnricher,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_anthropic_client() -> MagicMock:
    """Retourne un client Anthropic entierement mocke.

    Returns:
        MagicMock simulant le client anthropic.Anthropic.
    """
    return MagicMock()


@pytest.fixture
def enricher(mock_anthropic_client: MagicMock) -> ClaudeEnricher:
    """Retourne un ClaudeEnricher avec client mocke.

    Args:
        mock_anthropic_client: Client mocke.

    Returns:
        Instance de ClaudeEnricher prete pour les tests.
    """
    return ClaudeEnricher(
        api_key="sk-test-fake-key",
        model="claude-haiku-4-5-20251001",
        max_daily_calls=300,
        max_retries=3,
        base_delay=0.0,  # Pas de delai en test
        client=mock_anthropic_client,
    )


@pytest.fixture
def sample_annonce() -> dict[str, Any]:
    """Retourne une annonce de vente fictive pour l'enrichissement.

    Returns:
        Dictionnaire representant une annonce type.
    """
    return {
        "description_texte": (
            "Vente urgente ! Bel appartement T3 lumineux au 2eme etage, "
            "parking inclus, cave, double vitrage. Copropriete de 12 lots, "
            "charges annuelles 1200 EUR. Prix a debattre."
        ),
        "prix": 140000,
        "surface_m2": 55.0,
        "nb_pieces": 3,
        "quartier": "Centre-Ville",
        "dpe": "C",
        "adresse_brute": "12 rue de la Republique, 25000 Besancon",
        "charges_copro": 100.0,
    }


@pytest.fixture
def valid_response_json() -> str:
    """Retourne une reponse JSON valide de Claude.

    Returns:
        Chaine JSON formatee correctement.
    """
    return json.dumps(
        {
            "signaux_nego": ["urgent", "prix a debattre"],
            "etat_bien": "bon_etat",
            "equipements": ["parking", "cave", "double_vitrage"],
            "red_flags": [],
            "info_copro": {"nb_lots": 12, "charges_annuelles": 1200},
            "resume": "T3 lumineux en bon etat, parking inclus, copro saine.",
        }
    )


def _make_api_response(text: str) -> MagicMock:
    """Cree un mock de reponse API Claude.

    Args:
        text: Texte de la reponse.

    Returns:
        MagicMock simulant une reponse anthropic.
    """
    content_block = MagicMock()
    content_block.text = text
    response = MagicMock()
    response.content = [content_block]
    return response


# ---------------------------------------------------------------------------
# Tests : construction du prompt
# ---------------------------------------------------------------------------


class TestBuildPrompt:
    """Tests pour la construction du prompt d'analyse."""

    def test_prompt_contains_description(
        self, enricher: ClaudeEnricher, sample_annonce: dict[str, Any]
    ) -> None:
        """Le prompt doit contenir la description de l'annonce."""
        prompt = enricher._build_prompt(sample_annonce)
        assert "Bel appartement T3 lumineux" in prompt

    def test_prompt_contains_prix(
        self, enricher: ClaudeEnricher, sample_annonce: dict[str, Any]
    ) -> None:
        """Le prompt doit contenir le prix."""
        prompt = enricher._build_prompt(sample_annonce)
        assert "140000" in prompt

    def test_prompt_contains_surface(
        self, enricher: ClaudeEnricher, sample_annonce: dict[str, Any]
    ) -> None:
        """Le prompt doit contenir la surface."""
        prompt = enricher._build_prompt(sample_annonce)
        assert "55.0" in prompt

    def test_prompt_contains_quartier(
        self, enricher: ClaudeEnricher, sample_annonce: dict[str, Any]
    ) -> None:
        """Le prompt doit contenir le quartier."""
        prompt = enricher._build_prompt(sample_annonce)
        assert "Centre-Ville" in prompt

    def test_prompt_contains_dpe(
        self, enricher: ClaudeEnricher, sample_annonce: dict[str, Any]
    ) -> None:
        """Le prompt doit contenir le DPE."""
        prompt = enricher._build_prompt(sample_annonce)
        assert "C" in prompt

    def test_prompt_contains_all_required_fields(
        self, enricher: ClaudeEnricher, sample_annonce: dict[str, Any]
    ) -> None:
        """Le prompt doit mentionner description, prix, surface et quartier."""
        prompt = enricher._build_prompt(sample_annonce)
        assert "Description" in prompt
        assert "Prix" in prompt
        assert "Surface" in prompt
        assert "Quartier" in prompt

    def test_prompt_handles_missing_fields(
        self, enricher: ClaudeEnricher
    ) -> None:
        """Le prompt gere les champs manquants avec des valeurs par defaut."""
        annonce = {"prix": 120000}
        prompt = enricher._build_prompt(annonce)
        assert "Non disponible" in prompt
        assert "120000" in prompt

    def test_prompt_requests_json_format(
        self, enricher: ClaudeEnricher, sample_annonce: dict[str, Any]
    ) -> None:
        """Le prompt doit demander une reponse en JSON."""
        prompt = enricher._build_prompt(sample_annonce)
        assert "JSON" in prompt


# ---------------------------------------------------------------------------
# Tests : parsing de la reponse
# ---------------------------------------------------------------------------


class TestParseResponse:
    """Tests pour le parsing de la reponse Claude."""

    def test_parse_valid_json(
        self, enricher: ClaudeEnricher, valid_response_json: str
    ) -> None:
        """Un JSON valide avec toutes les cles est correctement parse."""
        result = enricher._parse_response(valid_response_json)
        assert result is not None
        assert result["signaux_nego"] == ["urgent", "prix a debattre"]
        assert result["etat_bien"] == "bon_etat"
        assert result["equipements"] == ["parking", "cave", "double_vitrage"]
        assert result["red_flags"] == []
        assert result["info_copro"]["nb_lots"] == 12
        assert result["resume"] == "T3 lumineux en bon etat, parking inclus, copro saine."

    def test_parse_malformed_json_returns_none(
        self, enricher: ClaudeEnricher
    ) -> None:
        """Un JSON malforma retourne None."""
        result = enricher._parse_response("ceci n'est pas du JSON {{{")
        assert result is None

    def test_parse_json_with_missing_keys_returns_none(
        self, enricher: ClaudeEnricher
    ) -> None:
        """Un JSON avec des cles manquantes retourne None."""
        incomplete = json.dumps({"signaux_nego": [], "etat_bien": "bon_etat"})
        result = enricher._parse_response(incomplete)
        assert result is None

    def test_parse_json_wrapped_in_markdown(
        self, enricher: ClaudeEnricher, valid_response_json: str
    ) -> None:
        """Un JSON entoure de marqueurs markdown ```json est correctement parse."""
        wrapped = f"```json\n{valid_response_json}\n```"
        result = enricher._parse_response(wrapped)
        assert result is not None
        assert result["etat_bien"] == "bon_etat"

    def test_parse_invalid_etat_bien_defaults_to_inconnu(
        self, enricher: ClaudeEnricher
    ) -> None:
        """Un etat_bien invalide est normalise en 'inconnu'."""
        data = {
            "signaux_nego": [],
            "etat_bien": "valeur_invalide_totalement_fausse",
            "equipements": [],
            "red_flags": [],
            "info_copro": None,
            "resume": "Test.",
        }
        result = enricher._parse_response(json.dumps(data))
        assert result is not None
        assert result["etat_bien"] == "inconnu"

    def test_parse_normalizes_info_copro(
        self, enricher: ClaudeEnricher
    ) -> None:
        """info_copro est normalise avec nb_lots et charges_annuelles."""
        data = {
            "signaux_nego": [],
            "etat_bien": "bon_etat",
            "equipements": [],
            "red_flags": [],
            "info_copro": {"nb_lots": 8, "extra_field": True},
            "resume": "Test.",
        }
        result = enricher._parse_response(json.dumps(data))
        assert result is not None
        assert result["info_copro"]["nb_lots"] == 8
        assert result["info_copro"]["charges_annuelles"] is None
        assert "extra_field" not in result["info_copro"]

    def test_parse_null_info_copro(
        self, enricher: ClaudeEnricher
    ) -> None:
        """info_copro null est normalise en dict avec None."""
        data = {
            "signaux_nego": [],
            "etat_bien": "bon_etat",
            "equipements": [],
            "red_flags": [],
            "info_copro": None,
            "resume": "Test.",
        }
        result = enricher._parse_response(json.dumps(data))
        assert result is not None
        assert result["info_copro"] == {"nb_lots": None, "charges_annuelles": None}

    def test_parse_non_dict_returns_none(
        self, enricher: ClaudeEnricher
    ) -> None:
        """Une reponse qui est un tableau JSON retourne None."""
        result = enricher._parse_response(json.dumps([1, 2, 3]))
        assert result is None


# ---------------------------------------------------------------------------
# Tests : limite quotidienne
# ---------------------------------------------------------------------------


class TestDailyLimit:
    """Tests pour la gestion de la limite quotidienne d'appels."""

    def test_daily_limit_allows_calls_under_limit(
        self, enricher: ClaudeEnricher
    ) -> None:
        """Les appels sont autorises tant que la limite n'est pas atteinte."""
        assert enricher._check_daily_limit() is True

    def test_daily_limit_blocks_at_max(
        self, mock_anthropic_client: MagicMock
    ) -> None:
        """Les appels sont bloques quand la limite est atteinte."""
        enricher = ClaudeEnricher(
            api_key="sk-test",
            max_daily_calls=5,
            base_delay=0.0,
            client=mock_anthropic_client,
        )
        enricher._daily_call_count = 5
        assert enricher._check_daily_limit() is False

    def test_daily_limit_enforced_in_enrich(
        self,
        mock_anthropic_client: MagicMock,
        sample_annonce: dict[str, Any],
    ) -> None:
        """enrich() retourne None quand la limite quotidienne est atteinte."""
        enricher = ClaudeEnricher(
            api_key="sk-test",
            max_daily_calls=0,
            base_delay=0.0,
            client=mock_anthropic_client,
        )
        result = enricher.enrich(sample_annonce)
        assert result is None
        mock_anthropic_client.messages.create.assert_not_called()

    def test_daily_counter_resets(
        self, enricher: ClaudeEnricher
    ) -> None:
        """reset_daily_counter() remet le compteur a zero."""
        enricher._daily_call_count = 250
        enricher.reset_daily_counter()
        assert enricher.daily_call_count == 0

    def test_daily_counter_auto_reset_on_new_day(
        self, enricher: ClaudeEnricher
    ) -> None:
        """Le compteur se reinitialise automatiquement a une nouvelle date."""
        enricher._daily_call_count = 100
        enricher._daily_reset_date = date(2020, 1, 1)
        assert enricher._check_daily_limit() is True
        assert enricher.daily_call_count == 0


# ---------------------------------------------------------------------------
# Tests : retry et gestion d'erreurs
# ---------------------------------------------------------------------------


class TestRetryLogic:
    """Tests pour le retry avec backoff exponentiel."""

    def test_retry_on_rate_limit_429(
        self,
        enricher: ClaudeEnricher,
        mock_anthropic_client: MagicMock,
        sample_annonce: dict[str, Any],
        valid_response_json: str,
    ) -> None:
        """L'enrichissement reessaie apres une erreur 429."""
        import anthropic as anth

        error_429 = anth.RateLimitError.__new__(anth.RateLimitError)
        error_429.__init__(
            message="Rate limit exceeded",
            response=MagicMock(status_code=429),
            body=None,
        )

        mock_anthropic_client.messages.create.side_effect = [
            error_429,
            _make_api_response(valid_response_json),
        ]

        result = enricher.enrich(sample_annonce)
        assert result is not None
        assert result["etat_bien"] == "bon_etat"
        assert mock_anthropic_client.messages.create.call_count == 2

    def test_retry_on_server_error_500(
        self,
        enricher: ClaudeEnricher,
        mock_anthropic_client: MagicMock,
        sample_annonce: dict[str, Any],
        valid_response_json: str,
    ) -> None:
        """L'enrichissement reessaie apres une erreur 500."""
        import anthropic as anth

        error_500 = anth.InternalServerError.__new__(anth.InternalServerError)
        error_500.__init__(
            message="Internal server error",
            response=MagicMock(status_code=500),
            body=None,
        )

        mock_anthropic_client.messages.create.side_effect = [
            error_500,
            error_500,
            _make_api_response(valid_response_json),
        ]

        result = enricher.enrich(sample_annonce)
        assert result is not None
        assert mock_anthropic_client.messages.create.call_count == 3

    def test_persistent_failure_returns_none(
        self,
        enricher: ClaudeEnricher,
        mock_anthropic_client: MagicMock,
        sample_annonce: dict[str, Any],
    ) -> None:
        """Retourne None apres echec de toutes les tentatives."""
        import anthropic as anth

        error_500 = anth.InternalServerError.__new__(anth.InternalServerError)
        error_500.__init__(
            message="Internal server error",
            response=MagicMock(status_code=500),
            body=None,
        )

        mock_anthropic_client.messages.create.side_effect = error_500

        result = enricher.enrich(sample_annonce)
        assert result is None
        assert mock_anthropic_client.messages.create.call_count == 3

    def test_non_retryable_error_returns_none_immediately(
        self,
        enricher: ClaudeEnricher,
        mock_anthropic_client: MagicMock,
        sample_annonce: dict[str, Any],
    ) -> None:
        """Une erreur API non retryable retourne None immediatement."""
        import anthropic as anth

        error = anth.AuthenticationError.__new__(anth.AuthenticationError)
        error.__init__(
            message="Invalid API key",
            response=MagicMock(status_code=401),
            body=None,
        )

        mock_anthropic_client.messages.create.side_effect = error

        result = enricher.enrich(sample_annonce)
        assert result is None
        assert mock_anthropic_client.messages.create.call_count == 1


# ---------------------------------------------------------------------------
# Tests : enrichissement end-to-end
# ---------------------------------------------------------------------------


class TestEnrichEndToEnd:
    """Tests d'integration pour le flux complet d'enrichissement."""

    def test_successful_enrichment(
        self,
        enricher: ClaudeEnricher,
        mock_anthropic_client: MagicMock,
        sample_annonce: dict[str, Any],
        valid_response_json: str,
    ) -> None:
        """L'enrichissement complet retourne un resultat valide."""
        mock_anthropic_client.messages.create.return_value = _make_api_response(
            valid_response_json
        )

        result = enricher.enrich(sample_annonce)

        assert result is not None
        assert set(result.keys()) == EXPECTED_KEYS
        assert isinstance(result["signaux_nego"], list)
        assert isinstance(result["equipements"], list)
        assert isinstance(result["red_flags"], list)
        assert isinstance(result["info_copro"], dict)
        assert isinstance(result["resume"], str)
        assert result["etat_bien"] in VALID_ETAT_BIEN

    def test_enrichment_increments_daily_counter(
        self,
        enricher: ClaudeEnricher,
        mock_anthropic_client: MagicMock,
        sample_annonce: dict[str, Any],
        valid_response_json: str,
    ) -> None:
        """Chaque appel reussi incremente le compteur quotidien."""
        mock_anthropic_client.messages.create.return_value = _make_api_response(
            valid_response_json
        )

        assert enricher.daily_call_count == 0
        enricher.enrich(sample_annonce)
        assert enricher.daily_call_count == 1
        enricher.enrich(sample_annonce)
        assert enricher.daily_call_count == 2

    def test_enrichment_with_unparseable_response(
        self,
        enricher: ClaudeEnricher,
        mock_anthropic_client: MagicMock,
        sample_annonce: dict[str, Any],
    ) -> None:
        """L'enrichissement retourne None si la reponse est unparseable."""
        mock_anthropic_client.messages.create.return_value = _make_api_response(
            "Je ne peux pas analyser cette annonce car je suis un modele de langage."
        )

        result = enricher.enrich(sample_annonce)
        assert result is None

    def test_schema_validation_all_fields_present(
        self,
        enricher: ClaudeEnricher,
        mock_anthropic_client: MagicMock,
        sample_annonce: dict[str, Any],
        valid_response_json: str,
    ) -> None:
        """Le resultat contient exactement les cles attendues du schema."""
        mock_anthropic_client.messages.create.return_value = _make_api_response(
            valid_response_json
        )

        result = enricher.enrich(sample_annonce)
        assert result is not None
        assert set(result.keys()) == EXPECTED_KEYS

        # Verifier les types de chaque champ
        assert isinstance(result["signaux_nego"], list)
        assert isinstance(result["etat_bien"], str)
        assert isinstance(result["equipements"], list)
        assert isinstance(result["red_flags"], list)
        assert isinstance(result["info_copro"], dict)
        assert "nb_lots" in result["info_copro"]
        assert "charges_annuelles" in result["info_copro"]
        assert isinstance(result["resume"], str)
