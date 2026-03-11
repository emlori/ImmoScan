"""Tests unitaires pour le module de validation des annonces.

Verifie les regles de validation des ventes et locations, la detection
d'anomalies, et les fonctions utilitaires de validation.
"""

from __future__ import annotations

from typing import Any

import pytest

from src.validation.validators import AnnonceValidator


@pytest.fixture
def validator() -> AnnonceValidator:
    """Retourne une instance du validateur."""
    return AnnonceValidator()


@pytest.fixture
def valid_vente_data() -> dict[str, Any]:
    """Retourne des donnees valides pour une annonce de vente."""
    return {
        "url_source": "https://www.leboncoin.fr/ventes/123456.htm",
        "source": "leboncoin",
        "prix": 145000,
        "surface_m2": 55.0,
        "nb_pieces": 3,
        "dpe": "C",
        "adresse_brute": "12 Rue de la Republique, 25000 Besancon",
    }


@pytest.fixture
def valid_location_data() -> dict[str, Any]:
    """Retourne des donnees valides pour une annonce de location."""
    return {
        "url_source": "https://www.leboncoin.fr/locations/789.htm",
        "source": "leboncoin",
        "loyer_cc": 550.0,
        "surface_m2": 45.0,
        "nb_pieces": 2,
        "adresse_brute": "5 Rue des Granges, 25000 Besancon",
    }


class TestValidVente:
    """Tests pour la validation d'annonces de vente valides."""

    def test_valid_vente_passes(
        self, validator: AnnonceValidator, valid_vente_data: dict[str, Any]
    ) -> None:
        """Verifie qu'une annonce vente valide passe la validation."""
        is_valid, reasons = validator.validate_vente(valid_vente_data)
        assert is_valid is True
        assert reasons == []

    def test_valid_vente_without_dpe(
        self, validator: AnnonceValidator, valid_vente_data: dict[str, Any]
    ) -> None:
        """Verifie qu'une annonce sans DPE est acceptee (DPE optionnel)."""
        valid_vente_data["dpe"] = None
        is_valid, reasons = validator.validate_vente(valid_vente_data)
        assert is_valid is True
        assert reasons == []

    def test_valid_vente_min_bounds(self, validator: AnnonceValidator) -> None:
        """Verifie qu'une annonce aux limites basses est acceptee."""
        data = {
            "url_source": "https://www.pap.fr/annonce/1",
            "source": "pap",
            "prix": 10000,
            "surface_m2": 10.0,
            "nb_pieces": 1,
            "adresse_brute": "Besancon",
        }
        is_valid, reasons = validator.validate_vente(data)
        assert is_valid is True

    def test_valid_vente_max_bounds(self, validator: AnnonceValidator) -> None:
        """Verifie qu'une annonce aux limites hautes est acceptee."""
        data = {
            "url_source": "https://www.pap.fr/annonce/2",
            "source": "pap",
            "prix": 500000,
            "surface_m2": 300.0,
            "nb_pieces": 10,
            "dpe": "G",
            "adresse_brute": "25000 Besancon",
        }
        is_valid, reasons = validator.validate_vente(data)
        assert is_valid is True


class TestInvalidPrix:
    """Tests pour les prix invalides."""

    def test_prix_missing(
        self, validator: AnnonceValidator, valid_vente_data: dict[str, Any]
    ) -> None:
        """Verifie le rejet d'une annonce sans prix."""
        del valid_vente_data["prix"]
        is_valid, reasons = validator.validate_vente(valid_vente_data)
        assert is_valid is False
        assert any("prix" in r for r in reasons)

    def test_prix_none(
        self, validator: AnnonceValidator, valid_vente_data: dict[str, Any]
    ) -> None:
        """Verifie le rejet d'une annonce avec prix None."""
        valid_vente_data["prix"] = None
        is_valid, reasons = validator.validate_vente(valid_vente_data)
        assert is_valid is False
        assert any("prix" in r for r in reasons)

    def test_prix_negative(
        self, validator: AnnonceValidator, valid_vente_data: dict[str, Any]
    ) -> None:
        """Verifie le rejet d'un prix negatif."""
        valid_vente_data["prix"] = -50000
        is_valid, reasons = validator.validate_vente(valid_vente_data)
        assert is_valid is False
        assert any("negatif" in r or "nul" in r for r in reasons)

    def test_prix_zero(
        self, validator: AnnonceValidator, valid_vente_data: dict[str, Any]
    ) -> None:
        """Verifie le rejet d'un prix nul."""
        valid_vente_data["prix"] = 0
        is_valid, reasons = validator.validate_vente(valid_vente_data)
        assert is_valid is False
        assert any("negatif" in r or "nul" in r for r in reasons)

    def test_prix_too_low(
        self, validator: AnnonceValidator, valid_vente_data: dict[str, Any]
    ) -> None:
        """Verifie le rejet d'un prix en dessous du minimum."""
        valid_vente_data["prix"] = 5000
        is_valid, reasons = validator.validate_vente(valid_vente_data)
        assert is_valid is False
        assert any("trop bas" in r for r in reasons)

    def test_prix_too_high(
        self, validator: AnnonceValidator, valid_vente_data: dict[str, Any]
    ) -> None:
        """Verifie le rejet d'un prix au dessus du maximum."""
        valid_vente_data["prix"] = 600000
        is_valid, reasons = validator.validate_vente(valid_vente_data)
        assert is_valid is False
        assert any("trop eleve" in r for r in reasons)


class TestInvalidSurface:
    """Tests pour les surfaces invalides."""

    def test_surface_missing(
        self, validator: AnnonceValidator, valid_vente_data: dict[str, Any]
    ) -> None:
        """Verifie le rejet d'une annonce sans surface."""
        del valid_vente_data["surface_m2"]
        is_valid, reasons = validator.validate_vente(valid_vente_data)
        assert is_valid is False
        assert any("surface" in r for r in reasons)

    def test_surface_too_small(
        self, validator: AnnonceValidator, valid_vente_data: dict[str, Any]
    ) -> None:
        """Verifie le rejet d'une surface en dessous du minimum."""
        valid_vente_data["surface_m2"] = 5.0
        is_valid, reasons = validator.validate_vente(valid_vente_data)
        assert is_valid is False
        assert any("trop petite" in r for r in reasons)

    def test_surface_too_large(
        self, validator: AnnonceValidator, valid_vente_data: dict[str, Any]
    ) -> None:
        """Verifie le rejet d'une surface au dessus du maximum."""
        valid_vente_data["surface_m2"] = 400.0
        is_valid, reasons = validator.validate_vente(valid_vente_data)
        assert is_valid is False
        assert any("trop grande" in r for r in reasons)

    def test_surface_zero(
        self, validator: AnnonceValidator, valid_vente_data: dict[str, Any]
    ) -> None:
        """Verifie le rejet d'une surface nulle."""
        valid_vente_data["surface_m2"] = 0.0
        is_valid, reasons = validator.validate_vente(valid_vente_data)
        assert is_valid is False
        assert any("surface" in r for r in reasons)


class TestInvalidNbPieces:
    """Tests pour les nombres de pieces invalides."""

    def test_nb_pieces_zero(
        self, validator: AnnonceValidator, valid_vente_data: dict[str, Any]
    ) -> None:
        """Verifie le rejet de 0 pieces."""
        valid_vente_data["nb_pieces"] = 0
        is_valid, reasons = validator.validate_vente(valid_vente_data)
        assert is_valid is False
        assert any("nb_pieces" in r for r in reasons)

    def test_nb_pieces_too_many(
        self, validator: AnnonceValidator, valid_vente_data: dict[str, Any]
    ) -> None:
        """Verifie le rejet de plus de 10 pieces."""
        valid_vente_data["nb_pieces"] = 15
        is_valid, reasons = validator.validate_vente(valid_vente_data)
        assert is_valid is False
        assert any("nb_pieces" in r for r in reasons)

    def test_nb_pieces_missing(
        self, validator: AnnonceValidator, valid_vente_data: dict[str, Any]
    ) -> None:
        """Verifie le rejet d'une annonce sans nombre de pieces."""
        del valid_vente_data["nb_pieces"]
        is_valid, reasons = validator.validate_vente(valid_vente_data)
        assert is_valid is False
        assert any("nb_pieces" in r for r in reasons)


class TestDPEValidation:
    """Tests pour la validation du DPE."""

    def test_dpe_valid_a_to_g(self, validator: AnnonceValidator) -> None:
        """Verifie que les lettres A a G sont valides."""
        base_data = {
            "url_source": "https://example.com/dpe",
            "prix": 145000,
            "surface_m2": 55.0,
            "nb_pieces": 3,
            "adresse_brute": "Besancon",
        }
        for letter in "ABCDEFG":
            base_data["dpe"] = letter
            is_valid, reasons = validator.validate_vente(base_data)
            assert is_valid is True, f"DPE '{letter}' devrait etre valide"

    def test_dpe_none_accepted(
        self, validator: AnnonceValidator, valid_vente_data: dict[str, Any]
    ) -> None:
        """Verifie que DPE None est accepte."""
        valid_vente_data["dpe"] = None
        is_valid, reasons = validator.validate_vente(valid_vente_data)
        assert is_valid is True

    def test_dpe_invalid_letter(
        self, validator: AnnonceValidator, valid_vente_data: dict[str, Any]
    ) -> None:
        """Verifie le rejet d'un DPE avec lettre invalide."""
        valid_vente_data["dpe"] = "X"
        is_valid, reasons = validator.validate_vente(valid_vente_data)
        assert is_valid is False
        assert any("dpe" in r for r in reasons)

    def test_dpe_invalid_number(
        self, validator: AnnonceValidator, valid_vente_data: dict[str, Any]
    ) -> None:
        """Verifie le rejet d'un DPE numerique."""
        valid_vente_data["dpe"] = "3"
        is_valid, reasons = validator.validate_vente(valid_vente_data)
        assert is_valid is False
        assert any("dpe" in r for r in reasons)


class TestURLValidation:
    """Tests pour la validation d'URL."""

    def test_valid_http_url(self, validator: AnnonceValidator) -> None:
        """Verifie qu'une URL HTTP est valide."""
        assert validator._is_valid_url("http://www.example.com/page") is True

    def test_valid_https_url(self, validator: AnnonceValidator) -> None:
        """Verifie qu'une URL HTTPS est valide."""
        assert validator._is_valid_url("https://www.example.com/page") is True

    def test_invalid_url_no_scheme(self, validator: AnnonceValidator) -> None:
        """Verifie qu'une URL sans scheme est invalide."""
        assert validator._is_valid_url("www.example.com") is False

    def test_invalid_url_empty(self, validator: AnnonceValidator) -> None:
        """Verifie qu'une URL vide est invalide."""
        assert validator._is_valid_url("") is False

    def test_invalid_url_ftp(self, validator: AnnonceValidator) -> None:
        """Verifie qu'une URL FTP est invalide."""
        assert validator._is_valid_url("ftp://example.com/file") is False

    def test_url_missing_in_vente(
        self, validator: AnnonceValidator, valid_vente_data: dict[str, Any]
    ) -> None:
        """Verifie le rejet d'une annonce sans URL."""
        valid_vente_data["url_source"] = ""
        is_valid, reasons = validator.validate_vente(valid_vente_data)
        assert is_valid is False
        assert any("url" in r for r in reasons)


class TestAddressValidation:
    """Tests pour la validation d'adresse."""

    def test_besancon_in_address(self, validator: AnnonceValidator) -> None:
        """Verifie la detection de Besancon dans l'adresse."""
        assert validator._is_besancon_address("12 Rue de la Paix, Besancon") is True

    def test_besancon_accent_in_address(self, validator: AnnonceValidator) -> None:
        """Verifie la detection de Besançon (avec accent) dans l'adresse."""
        assert validator._is_besancon_address("12 Rue de la Paix, Besançon") is True

    def test_cp_25000(self, validator: AnnonceValidator) -> None:
        """Verifie la detection du code postal 25000."""
        assert validator._is_besancon_address("12 Rue de la Paix, 25000") is True

    def test_cp_25_prefix(self, validator: AnnonceValidator) -> None:
        """Verifie la detection d'un code postal 25xxx."""
        assert validator._is_besancon_address("12 Rue de la Paix, 25030") is True

    def test_non_besancon_address(self, validator: AnnonceValidator) -> None:
        """Verifie le rejet d'une adresse hors Besancon."""
        assert validator._is_besancon_address("12 Rue de la Paix, Paris") is False

    def test_empty_address(self, validator: AnnonceValidator) -> None:
        """Verifie le rejet d'une adresse vide."""
        assert validator._is_besancon_address("") is False

    def test_address_missing_in_vente(
        self, validator: AnnonceValidator, valid_vente_data: dict[str, Any]
    ) -> None:
        """Verifie le rejet d'une annonce sans adresse."""
        valid_vente_data["adresse_brute"] = ""
        is_valid, reasons = validator.validate_vente(valid_vente_data)
        assert is_valid is False
        assert any("adresse" in r for r in reasons)

    def test_address_non_besancon_in_vente(
        self, validator: AnnonceValidator, valid_vente_data: dict[str, Any]
    ) -> None:
        """Verifie le rejet d'une annonce avec adresse hors Besancon."""
        valid_vente_data["adresse_brute"] = "12 Rue de Paris, 75001 Paris"
        is_valid, reasons = validator.validate_vente(valid_vente_data)
        assert is_valid is False
        assert any("adresse" in r or "Besancon" in r for r in reasons)


class TestValidLocation:
    """Tests pour la validation des locations."""

    def test_valid_location_passes(
        self, validator: AnnonceValidator, valid_location_data: dict[str, Any]
    ) -> None:
        """Verifie qu'une location valide passe la validation."""
        is_valid, reasons = validator.validate_location(valid_location_data)
        assert is_valid is True
        assert reasons == []


class TestInvalidLoyer:
    """Tests pour les loyers invalides."""

    def test_loyer_missing(
        self, validator: AnnonceValidator, valid_location_data: dict[str, Any]
    ) -> None:
        """Verifie le rejet d'une location sans loyer."""
        del valid_location_data["loyer_cc"]
        is_valid, reasons = validator.validate_location(valid_location_data)
        assert is_valid is False
        assert any("loyer" in r for r in reasons)

    def test_loyer_too_low(
        self, validator: AnnonceValidator, valid_location_data: dict[str, Any]
    ) -> None:
        """Verifie le rejet d'un loyer en dessous du minimum."""
        valid_location_data["loyer_cc"] = 100.0
        is_valid, reasons = validator.validate_location(valid_location_data)
        assert is_valid is False
        assert any("trop bas" in r for r in reasons)

    def test_loyer_too_high(
        self, validator: AnnonceValidator, valid_location_data: dict[str, Any]
    ) -> None:
        """Verifie le rejet d'un loyer au dessus du maximum."""
        valid_location_data["loyer_cc"] = 5000.0
        is_valid, reasons = validator.validate_location(valid_location_data)
        assert is_valid is False
        assert any("trop eleve" in r for r in reasons)

    def test_loyer_zero(
        self, validator: AnnonceValidator, valid_location_data: dict[str, Any]
    ) -> None:
        """Verifie le rejet d'un loyer nul."""
        valid_location_data["loyer_cc"] = 0.0
        is_valid, reasons = validator.validate_location(valid_location_data)
        assert is_valid is False
        assert any("loyer" in r for r in reasons)

    def test_loyer_negative(
        self, validator: AnnonceValidator, valid_location_data: dict[str, Any]
    ) -> None:
        """Verifie le rejet d'un loyer negatif."""
        valid_location_data["loyer_cc"] = -200.0
        is_valid, reasons = validator.validate_location(valid_location_data)
        assert is_valid is False
        assert any("loyer" in r for r in reasons)


class TestAnomalyDetection:
    """Tests pour la detection d'anomalies."""

    def test_prix_m2_outlier(self, validator: AnnonceValidator) -> None:
        """Verifie la detection d'un prix/m2 aberrant."""
        data = {
            "prix": 300000,
            "surface_m2": 30.0,  # 10000€/m2
            "nb_pieces": 2,
        }
        quartier_stats = {
            "prix_m2_median": 2500.0,
            "prix_m2_std": 500.0,
        }
        anomalies = validator.detect_anomalies(data, quartier_stats)
        assert len(anomalies) >= 1
        assert any("prix/m2" in a for a in anomalies)

    def test_prix_m2_normal(self, validator: AnnonceValidator) -> None:
        """Verifie qu'un prix/m2 normal ne declenche pas d'anomalie."""
        data = {
            "prix": 125000,
            "surface_m2": 50.0,  # 2500€/m2
            "nb_pieces": 3,
        }
        quartier_stats = {
            "prix_m2_median": 2500.0,
            "prix_m2_std": 500.0,
        }
        anomalies = validator.detect_anomalies(data, quartier_stats)
        assert not any("prix/m2" in a for a in anomalies)

    def test_surface_incoherent_t2(self, validator: AnnonceValidator) -> None:
        """Verifie la detection d'une surface incoherente pour un T2."""
        data = {
            "prix": 100000,
            "surface_m2": 15.0,  # T2 de 15m2 = anormal
            "nb_pieces": 2,
        }
        anomalies = validator.detect_anomalies(data)
        assert len(anomalies) >= 1
        assert any("surface incoherente" in a for a in anomalies)

    def test_surface_incoherent_t3(self, validator: AnnonceValidator) -> None:
        """Verifie la detection d'une surface incoherente pour un T3."""
        data = {
            "prix": 100000,
            "surface_m2": 30.0,  # T3 de 30m2 = anormal
            "nb_pieces": 3,
        }
        anomalies = validator.detect_anomalies(data)
        assert len(anomalies) >= 1
        assert any("surface incoherente" in a for a in anomalies)

    def test_surface_coherent(self, validator: AnnonceValidator) -> None:
        """Verifie qu'une surface coherente ne declenche pas d'anomalie."""
        data = {
            "prix": 145000,
            "surface_m2": 55.0,
            "nb_pieces": 3,
        }
        anomalies = validator.detect_anomalies(data)
        assert not any("surface incoherente" in a for a in anomalies)

    def test_pro_disguised(self, validator: AnnonceValidator) -> None:
        """Verifie la detection d'une annonce pro deguisee."""
        data = {
            "prix": 145000,
            "surface_m2": 55.0,
            "nb_pieces": 3,
            "description_texte": (
                "Bel appartement T3. Honoraires agence 5%. "
                "Ref. 12345. Mandat exclusif."
            ),
        }
        anomalies = validator.detect_anomalies(data)
        assert len(anomalies) >= 1
        assert any("pro deguisee" in a for a in anomalies)

    def test_not_pro(self, validator: AnnonceValidator) -> None:
        """Verifie qu'une annonce normale ne declenche pas l'alerte pro."""
        data = {
            "prix": 145000,
            "surface_m2": 55.0,
            "nb_pieces": 3,
            "description_texte": "Bel appartement T3 lumineux, proche centre-ville.",
        }
        anomalies = validator.detect_anomalies(data)
        assert not any("pro deguisee" in a for a in anomalies)

    def test_no_anomalies_without_stats(self, validator: AnnonceValidator) -> None:
        """Verifie qu'aucune anomalie de prix n'est detectee sans stats quartier."""
        data = {
            "prix": 145000,
            "surface_m2": 55.0,
            "nb_pieces": 3,
        }
        anomalies = validator.detect_anomalies(data)
        assert not any("prix/m2" in a for a in anomalies)


class TestRejectionReasons:
    """Tests pour la qualite des raisons de rejet."""

    def test_multiple_rejections(self, validator: AnnonceValidator) -> None:
        """Verifie que plusieurs raisons de rejet sont retournees."""
        data = {
            "url_source": "",
            "prix": -1,
            "surface_m2": 0.0,
            "nb_pieces": 0,
            "adresse_brute": "",
        }
        is_valid, reasons = validator.validate_vente(data)
        assert is_valid is False
        # Au moins prix, surface, nb_pieces, url, adresse
        assert len(reasons) >= 4

    def test_rejection_reasons_are_strings(self, validator: AnnonceValidator) -> None:
        """Verifie que les raisons de rejet sont des chaines non vides."""
        data: dict[str, Any] = {}
        is_valid, reasons = validator.validate_vente(data)
        assert is_valid is False
        for reason in reasons:
            assert isinstance(reason, str)
            assert len(reason) > 0
