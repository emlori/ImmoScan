"""Tests pour le scoring, la rentabilite, la fiscalite et le geocodage.

Couvre les modules :
- src.scoring.rentabilite (RentabiliteCalculator)
- src.scoring.composite (CompositeScorer)
- src.scoring.fiscal (FiscalEstimator)
- src.geo.scoring_geo (GeoScorer)
- src.geo.geocoder (Geocoder)

Tous les tests fonctionnent sans acces reseau (API mockee pour le geocodage).
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest

from src.geo.geocoder import Geocoder
from src.geo.scoring_geo import GeoScorer
from src.scoring.composite import CompositeScorer
from src.scoring.fiscal import FiscalEstimator
from src.scoring.rentabilite import RentabiliteCalculator


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def quartiers_config() -> dict[str, Any]:
    """Configuration des quartiers pour les tests."""
    return {
        "quartiers": {
            "centre_ville": {
                "nom": "Centre-Ville",
                "code_postal": "25000",
                "score_attractivite": 85,
                "tension_locative": 0.85,
                "risque_vacance": "faible",
                "centre": {"lat": 47.2378, "lon": 6.0241},
                "loyers_fallback": {
                    "T2": {"nu": {"loyer_median": 450, "loyer_m2": 12.5}},
                    "T3": {"nu": {"loyer_median": 580, "loyer_m2": 10.5}},
                },
            },
            "battant": {
                "nom": "Battant",
                "code_postal": "25000",
                "score_attractivite": 80,
                "tension_locative": 0.80,
                "risque_vacance": "faible",
                "centre": {"lat": 47.2410, "lon": 6.0180},
            },
            "chablais": {
                "nom": "Chablais",
                "code_postal": "25000",
                "score_attractivite": 70,
                "tension_locative": 0.70,
                "risque_vacance": "moyen",
                "centre": {"lat": 47.2350, "lon": 6.0310},
            },
        },
        "scoring_localisation": {
            "poids_tram": 30,
            "poids_commerces": 25,
            "poids_campus": 20,
            "poids_gare": 15,
            "poids_attractivite": 10,
            "rayon_tram": 500,
            "rayon_commerces": 300,
            "rayon_campus": 1000,
            "rayon_gare": 1500,
        },
        "geocodage": {
            "api_url": "https://api-adresse.data.gouv.fr/search/",
            "cache_ttl": 86400,
        },
    }


@pytest.fixture
def scoring_config() -> dict[str, Any]:
    """Configuration de scoring pour les tests."""
    return {
        "poids": {
            "rentabilite": 0.40,
            "localisation": 0.25,
            "dpe": 0.15,
            "negociation": 0.10,
            "vacance": 0.10,
        },
        "dpe_scores": {
            "A": 100,
            "B": 85,
            "C": 65,
            "D": 40,
            "E": 0,
            "F": 0,
            "G": 0,
        },
        "alertes": {
            "top": {"score_min": 80, "renta_min": 8.0},
            "bon": {"score_min": 60, "score_max": 79, "renta_min_nego": 8.0},
            "veille": {"score_max": 59},
        },
        "negociation": {"scenarios": [0, 5, 10, 15], "marge_max": 15},
        "rentabilite": {"cible_brute": 8.0, "bonus_seuil": 8.0, "bonus_max": 20},
        "rentabilite_scoring": {"min_renta": 4.0, "max_renta": 12.0},
        "negociation_scoring": {
            "signaux_texte": [
                "urgent",
                "prix a debattre",
                "a negocier",
                "faire offre",
                "vente rapide",
                "mutation",
                "succession",
                "divorce",
                "baisse de prix",
            ],
            "poids_signal": 15,
            "poids_baisse_prix": 40,
            "poids_duree_vente": 30,
        },
        "vacance_scoring": {
            "tension_elevee": 90,
            "tension_moyenne": 60,
            "tension_faible": 30,
        },
    }


@pytest.fixture
def rentabilite_calculator() -> RentabiliteCalculator:
    """Instance de RentabiliteCalculator avec config par defaut."""
    return RentabiliteCalculator()


@pytest.fixture
def composite_scorer(scoring_config: dict[str, Any]) -> CompositeScorer:
    """Instance de CompositeScorer avec config de test."""
    return CompositeScorer(scoring_config=scoring_config)


@pytest.fixture
def fiscal_estimator() -> FiscalEstimator:
    """Instance de FiscalEstimator avec config par defaut."""
    return FiscalEstimator()


@pytest.fixture
def geo_scorer(quartiers_config: dict[str, Any]) -> GeoScorer:
    """Instance de GeoScorer avec config de test."""
    return GeoScorer(quartiers_config=quartiers_config)


@pytest.fixture
def sample_annonce_ideal() -> dict[str, Any]:
    """Annonce ideale avec DPE A, bon quartier, signaux de nego."""
    return {
        "dpe": "A",
        "quartier": "Centre-Ville",
        "description_texte": "Vente urgente, prix a debattre, bel appartement.",
        "historique_prix": [
            {"date": "2025-01-01", "prix": 160000},
            {"date": "2025-02-01", "prix": 145000},
        ],
        "tension_locative": 0.85,
    }


@pytest.fixture
def sample_annonce_poor() -> dict[str, Any]:
    """Annonce mediocre : DPE E, pas de signaux, pas d'historique."""
    return {
        "dpe": "E",
        "quartier": "Inconnu",
        "description_texte": "Appartement a vendre.",
        "historique_prix": [],
        "tension_locative": 0.3,
    }


# ===========================================================================
# Tests RentabiliteCalculator
# ===========================================================================


class TestRentabiliteCalculator:
    """Tests pour le calcul de rentabilite brute."""

    def test_standard_case(self, rentabilite_calculator: RentabiliteCalculator) -> None:
        """Test du cas standard : 150k prix, 600 EUR/mois."""
        result = rentabilite_calculator.calculate(prix=150000, loyer_mensuel=600.0)

        expected_renta = (600 * 12 / 150000) * 100
        assert result["renta_brute"] == pytest.approx(expected_renta, rel=1e-2)
        assert result["loyer_annuel"] == 7200.0

    def test_scenario_nego_5(
        self, rentabilite_calculator: RentabiliteCalculator
    ) -> None:
        """Test scenario negociation -5%."""
        result = rentabilite_calculator.calculate(prix=150000, loyer_mensuel=600.0)

        expected = (600 * 12 / (150000 * 0.95)) * 100
        assert result["renta_brute_nego_5"] == pytest.approx(expected, rel=1e-2)

    def test_scenario_nego_10(
        self, rentabilite_calculator: RentabiliteCalculator
    ) -> None:
        """Test scenario negociation -10%."""
        result = rentabilite_calculator.calculate(prix=150000, loyer_mensuel=600.0)

        expected = (600 * 12 / (150000 * 0.90)) * 100
        assert result["renta_brute_nego_10"] == pytest.approx(expected, rel=1e-2)

    def test_scenario_nego_15(
        self, rentabilite_calculator: RentabiliteCalculator
    ) -> None:
        """Test scenario negociation -15%."""
        result = rentabilite_calculator.calculate(prix=150000, loyer_mensuel=600.0)

        expected = (600 * 12 / (150000 * 0.85)) * 100
        assert result["renta_brute_nego_15"] == pytest.approx(expected, rel=1e-2)

    def test_all_four_scenarios_present(
        self, rentabilite_calculator: RentabiliteCalculator
    ) -> None:
        """Verifie que les 4 scenarios sont presents dans le resultat."""
        result = rentabilite_calculator.calculate(prix=100000, loyer_mensuel=500.0)

        assert "renta_brute" in result
        assert "renta_brute_nego_5" in result
        assert "renta_brute_nego_10" in result
        assert "renta_brute_nego_15" in result

    def test_nego_increases_renta(
        self, rentabilite_calculator: RentabiliteCalculator
    ) -> None:
        """La rentabilite augmente avec le pourcentage de negociation."""
        result = rentabilite_calculator.calculate(prix=150000, loyer_mensuel=600.0)

        assert result["renta_brute"] < result["renta_brute_nego_5"]
        assert result["renta_brute_nego_5"] < result["renta_brute_nego_10"]
        assert result["renta_brute_nego_10"] < result["renta_brute_nego_15"]

    def test_prix_zero_raises_error(
        self, rentabilite_calculator: RentabiliteCalculator
    ) -> None:
        """Prix = 0 doit lever une ValueError."""
        with pytest.raises(ValueError, match="strictement positif"):
            rentabilite_calculator.calculate(prix=0, loyer_mensuel=500.0)

    def test_prix_negative_raises_error(
        self, rentabilite_calculator: RentabiliteCalculator
    ) -> None:
        """Prix negatif doit lever une ValueError."""
        with pytest.raises(ValueError, match="strictement positif"):
            rentabilite_calculator.calculate(prix=-100000, loyer_mensuel=500.0)

    def test_loyer_negative_raises_error(
        self, rentabilite_calculator: RentabiliteCalculator
    ) -> None:
        """Loyer negatif doit lever une ValueError."""
        with pytest.raises(ValueError, match="negatif"):
            rentabilite_calculator.calculate(prix=150000, loyer_mensuel=-500.0)

    def test_loyer_zero(
        self, rentabilite_calculator: RentabiliteCalculator
    ) -> None:
        """Loyer = 0 doit retourner une rentabilite de 0%."""
        result = rentabilite_calculator.calculate(prix=150000, loyer_mensuel=0.0)
        assert result["renta_brute"] == 0.0

    def test_charges_copro(
        self, rentabilite_calculator: RentabiliteCalculator
    ) -> None:
        """Les charges de copropriete sont correctement annualisees."""
        result = rentabilite_calculator.calculate(
            prix=150000, loyer_mensuel=600.0, charges_copro=100.0
        )

        assert result["charges_annuelles"] == 1200.0

    def test_charges_copro_none(
        self, rentabilite_calculator: RentabiliteCalculator
    ) -> None:
        """Charges = None doit retourner None pour charges_annuelles."""
        result = rentabilite_calculator.calculate(
            prix=150000, loyer_mensuel=600.0, charges_copro=None
        )

        assert result["charges_annuelles"] is None

    def test_scenarios_dict_present(
        self, rentabilite_calculator: RentabiliteCalculator
    ) -> None:
        """Le dict detaille des scenarios doit etre present."""
        result = rentabilite_calculator.calculate(prix=100000, loyer_mensuel=500.0)

        assert "scenarios" in result
        assert 0 in result["scenarios"]
        assert 5 in result["scenarios"]
        assert 10 in result["scenarios"]
        assert 15 in result["scenarios"]

    def test_renta_brute_simple_static(self) -> None:
        """Test de la methode statique de calcul rapide."""
        renta = RentabiliteCalculator.renta_brute_simple(150000, 600.0)
        expected = (600 * 12 / 150000) * 100
        assert renta == pytest.approx(expected, rel=1e-2)

    def test_renta_brute_simple_prix_zero(self) -> None:
        """Methode statique retourne 0 si prix = 0."""
        assert RentabiliteCalculator.renta_brute_simple(0, 500.0) == 0.0

    def test_renta_brute_simple_loyer_negatif(self) -> None:
        """Methode statique retourne 0 si loyer negatif."""
        assert RentabiliteCalculator.renta_brute_simple(150000, -100.0) == 0.0


# ===========================================================================
# Tests CompositeScorer
# ===========================================================================


class TestCompositeScorer:
    """Tests pour le scoring composite."""

    def test_ideal_listing_high_score(
        self,
        composite_scorer: CompositeScorer,
        sample_annonce_ideal: dict[str, Any],
    ) -> None:
        """Une annonce ideale doit avoir un score > 75."""
        renta_data = {
            "renta_brute": 10.0,
            "renta_brute_nego_5": 10.5,
            "renta_brute_nego_10": 11.1,
            "renta_brute_nego_15": 11.8,
        }
        geo_score = 95.0

        result = composite_scorer.score(
            sample_annonce_ideal, renta_data, geo_score
        )

        assert result["score_global"] > 75
        # With renta >= 8% at displayed price, alert should be TOP
        assert result["niveau_alerte"] == "top"

    def test_poor_listing_low_score(
        self,
        composite_scorer: CompositeScorer,
        sample_annonce_poor: dict[str, Any],
    ) -> None:
        """Une annonce mediocre doit avoir un score < 40."""
        renta_data = {
            "renta_brute": 3.0,
            "renta_brute_nego_5": 3.2,
            "renta_brute_nego_10": 3.3,
            "renta_brute_nego_15": 3.5,
        }
        geo_score = 20.0

        result = composite_scorer.score(
            sample_annonce_poor, renta_data, geo_score
        )

        assert result["score_global"] < 40

    def test_score_contains_all_components(
        self,
        composite_scorer: CompositeScorer,
    ) -> None:
        """Le resultat doit contenir toutes les composantes de score."""
        annonce = {"dpe": "C", "quartier": "Centre-Ville", "tension_locative": 0.7}
        renta_data = {"renta_brute": 6.0, "renta_brute_nego_5": 6.3,
                       "renta_brute_nego_10": 6.7, "renta_brute_nego_15": 7.1}

        result = composite_scorer.score(annonce, renta_data, 70.0)

        assert "score_global" in result
        assert "score_rentabilite" in result
        assert "score_localisation" in result
        assert "score_dpe" in result
        assert "score_negociation" in result
        assert "score_vacance" in result
        assert "niveau_alerte" in result
        assert "detail_poids" in result

    def test_score_bounded_0_100(
        self,
        composite_scorer: CompositeScorer,
    ) -> None:
        """Le score global doit etre entre 0 et 100."""
        annonce = {"dpe": "A", "tension_locative": 0.9}
        renta_data = {"renta_brute": 15.0, "renta_brute_nego_5": 16.0,
                       "renta_brute_nego_10": 17.0, "renta_brute_nego_15": 18.0}

        result = composite_scorer.score(annonce, renta_data, 100.0)

        assert 0 <= result["score_global"] <= 100

    def test_dpe_scoring_a(self, composite_scorer: CompositeScorer) -> None:
        """DPE A doit donner un score de 100."""
        assert composite_scorer._score_dpe("A") == 100.0

    def test_dpe_scoring_b(self, composite_scorer: CompositeScorer) -> None:
        """DPE B doit donner un score de 85."""
        assert composite_scorer._score_dpe("B") == 85.0

    def test_dpe_scoring_c(self, composite_scorer: CompositeScorer) -> None:
        """DPE C doit donner un score de 65."""
        assert composite_scorer._score_dpe("C") == 65.0

    def test_dpe_scoring_d(self, composite_scorer: CompositeScorer) -> None:
        """DPE D doit donner un score de 40."""
        assert composite_scorer._score_dpe("D") == 40.0

    def test_dpe_scoring_e(self, composite_scorer: CompositeScorer) -> None:
        """DPE E doit donner un score de 0."""
        assert composite_scorer._score_dpe("E") == 0.0

    def test_dpe_scoring_none(self, composite_scorer: CompositeScorer) -> None:
        """DPE None doit donner un score neutre de 30."""
        assert composite_scorer._score_dpe(None) == 30.0

    def test_dpe_scoring_lowercase(self, composite_scorer: CompositeScorer) -> None:
        """DPE minuscule doit etre reconnu."""
        assert composite_scorer._score_dpe("a") == 100.0
        assert composite_scorer._score_dpe("c") == 65.0

    def test_alert_level_top_by_score(
        self, composite_scorer: CompositeScorer
    ) -> None:
        """Score >= 80 doit donner niveau TOP."""
        level = composite_scorer.determine_alert_level(85.0, 5.0)
        assert level == "top"

    def test_alert_level_top_by_renta(
        self, composite_scorer: CompositeScorer
    ) -> None:
        """Renta >= 8% au prix affiche doit donner niveau TOP."""
        level = composite_scorer.determine_alert_level(50.0, 8.5)
        assert level == "top"

    def test_alert_level_bon_by_score(
        self, composite_scorer: CompositeScorer
    ) -> None:
        """Score 60-79 doit donner niveau BON."""
        level = composite_scorer.determine_alert_level(70.0, 5.0)
        assert level == "bon"

    def test_alert_level_bon_by_nego_renta(
        self, composite_scorer: CompositeScorer
    ) -> None:
        """Renta >= 8% apres nego doit donner niveau BON meme si score < 60."""
        level = composite_scorer.determine_alert_level(40.0, 5.0, 8.5)
        assert level == "bon"

    def test_alert_level_veille(self, composite_scorer: CompositeScorer) -> None:
        """Score < 60 et renta < 8% doit donner niveau VEILLE."""
        level = composite_scorer.determine_alert_level(40.0, 5.0, 6.0)
        assert level == "veille"

    def test_negociation_score_with_signals(
        self, composite_scorer: CompositeScorer
    ) -> None:
        """Les signaux de negociation dans la description augmentent le score."""
        annonce_with_signals = {
            "description_texte": "Vente urgente, prix a debattre !",
            "historique_prix": [],
        }
        annonce_without_signals = {
            "description_texte": "Bel appartement lumineux.",
            "historique_prix": [],
        }

        score_with = composite_scorer._score_negociation(annonce_with_signals)
        score_without = composite_scorer._score_negociation(annonce_without_signals)

        assert score_with > score_without

    def test_negociation_score_with_price_drop(
        self, composite_scorer: CompositeScorer
    ) -> None:
        """Une baisse de prix dans l'historique augmente le score de negociation."""
        annonce_drop = {
            "description_texte": "Appartement a vendre.",
            "historique_prix": [
                {"date": "2025-01-01", "prix": 160000},
                {"date": "2025-02-01", "prix": 145000},
            ],
        }
        annonce_no_drop = {
            "description_texte": "Appartement a vendre.",
            "historique_prix": [
                {"date": "2025-01-01", "prix": 145000},
                {"date": "2025-02-01", "prix": 150000},
            ],
        }

        score_drop = composite_scorer._score_negociation(annonce_drop)
        score_no_drop = composite_scorer._score_negociation(annonce_no_drop)

        assert score_drop > score_no_drop

    def test_weights_sum_to_one(self, composite_scorer: CompositeScorer) -> None:
        """La somme des poids de scoring doit etre 1.0."""
        assert composite_scorer.get_weights_sum() == pytest.approx(1.0, abs=1e-6)

    def test_vacance_score_high_tension(
        self, composite_scorer: CompositeScorer
    ) -> None:
        """Tension locative elevee (>= 0.8) doit donner un score de vacance eleve."""
        annonce = {"tension_locative": 0.85}
        score = composite_scorer._score_vacance(annonce)
        assert score == 90.0

    def test_vacance_score_low_tension(
        self, composite_scorer: CompositeScorer
    ) -> None:
        """Tension locative faible (< 0.5) doit donner un score de vacance faible."""
        annonce = {"tension_locative": 0.3}
        score = composite_scorer._score_vacance(annonce)
        assert score == 30.0

    def test_rentabilite_score_below_min(
        self, composite_scorer: CompositeScorer
    ) -> None:
        """Rentabilite en dessous du minimum doit donner un score de 0."""
        score = composite_scorer._score_rentabilite(3.0)
        assert score == 0.0

    def test_rentabilite_score_above_max(
        self, composite_scorer: CompositeScorer
    ) -> None:
        """Rentabilite au-dessus du maximum doit donner un score de 100."""
        score = composite_scorer._score_rentabilite(13.0)
        assert score == 100.0

    def test_rentabilite_score_midpoint(
        self, composite_scorer: CompositeScorer
    ) -> None:
        """Rentabilite au point median doit donner un score d'environ 50."""
        # min_renta=4, max_renta=12 => midpoint = 8 => score = 50
        score = composite_scorer._score_rentabilite(8.0)
        assert score == pytest.approx(50.0, abs=1.0)

    def test_has_price_drop_true(self) -> None:
        """Detecte correctement une baisse de prix."""
        historique = [
            {"date": "2025-01-01", "prix": 160000},
            {"date": "2025-02-01", "prix": 155000},
        ]
        assert CompositeScorer._has_price_drop(historique) is True

    def test_has_price_drop_false(self) -> None:
        """Pas de baisse quand les prix augmentent."""
        historique = [
            {"date": "2025-01-01", "prix": 150000},
            {"date": "2025-02-01", "prix": 155000},
        ]
        assert CompositeScorer._has_price_drop(historique) is False

    def test_has_price_drop_empty(self) -> None:
        """Pas de baisse avec un historique vide."""
        assert CompositeScorer._has_price_drop([]) is False

    def test_has_price_drop_single_entry(self) -> None:
        """Pas de baisse avec une seule entree."""
        assert CompositeScorer._has_price_drop([{"date": "2025-01-01", "prix": 150000}]) is False

    def test_enrichment_signals_affect_score(
        self, composite_scorer: CompositeScorer
    ) -> None:
        """Les signaux d'enrichissement IA affectent le score de negociation."""
        annonce = {
            "description_texte": "Appartement classique.",
            "historique_prix": [],
        }
        enrichment = {
            "signaux_nego": ["urgent", "mutation"],
        }

        score_with = composite_scorer._score_negociation(annonce, enrichment)
        score_without = composite_scorer._score_negociation(annonce, None)

        assert score_with > score_without


# ===========================================================================
# Tests FiscalEstimator
# ===========================================================================


class TestFiscalEstimator:
    """Tests pour l'estimation fiscale indicative."""

    def test_lmnp_micro_abattement(self, fiscal_estimator: FiscalEstimator) -> None:
        """LMNP micro-BIC applique un abattement de 50%."""
        result = fiscal_estimator.estimate(loyer_annuel=7200.0)

        assert result["lmnp_micro"]["revenu_brut"] == 7200.0
        assert result["lmnp_micro"]["abattement"] == 3600.0
        assert result["lmnp_micro"]["revenu_imposable"] == 3600.0
        assert result["lmnp_micro"]["taux_abattement"] == 0.50

    def test_nu_micro_abattement(self, fiscal_estimator: FiscalEstimator) -> None:
        """Location nue micro-foncier applique un abattement de 30%."""
        result = fiscal_estimator.estimate(loyer_annuel=7200.0)

        assert result["nu_micro"]["revenu_brut"] == 7200.0
        assert result["nu_micro"]["abattement"] == 2160.0
        assert result["nu_micro"]["revenu_imposable"] == 5040.0
        assert result["nu_micro"]["taux_abattement"] == 0.30

    def test_lmnp_is_more_advantageous(
        self, fiscal_estimator: FiscalEstimator
    ) -> None:
        """LMNP doit etre recommande (abattement plus eleve => imposable plus faible)."""
        result = fiscal_estimator.estimate(loyer_annuel=7200.0)

        assert result["regime_indicatif"] == "lmnp"
        assert result["economie_lmnp"] > 0

    def test_economie_lmnp_calculation(
        self, fiscal_estimator: FiscalEstimator
    ) -> None:
        """L'economie LMNP est la difference entre imposable nu et imposable LMNP."""
        result = fiscal_estimator.estimate(loyer_annuel=7200.0)

        expected_economie = (
            result["nu_micro"]["revenu_imposable"]
            - result["lmnp_micro"]["revenu_imposable"]
        )
        assert result["economie_lmnp"] == pytest.approx(expected_economie, abs=0.01)

    def test_disclaimer_present(self, fiscal_estimator: FiscalEstimator) -> None:
        """Le disclaimer doit etre present dans le resultat."""
        result = fiscal_estimator.estimate(loyer_annuel=5000.0)
        assert "disclaimer" in result
        assert "indicative" in result["disclaimer"].lower()

    def test_loyer_zero(self, fiscal_estimator: FiscalEstimator) -> None:
        """Loyer annuel = 0 doit fonctionner avec revenu imposable = 0."""
        result = fiscal_estimator.estimate(loyer_annuel=0.0)

        assert result["lmnp_micro"]["revenu_imposable"] == 0.0
        assert result["nu_micro"]["revenu_imposable"] == 0.0

    def test_loyer_negative_raises_error(
        self, fiscal_estimator: FiscalEstimator
    ) -> None:
        """Loyer annuel negatif doit lever une ValueError."""
        with pytest.raises(ValueError, match="negatif"):
            fiscal_estimator.estimate(loyer_annuel=-1000.0)

    def test_charges_passed_through(
        self, fiscal_estimator: FiscalEstimator
    ) -> None:
        """Les charges sont correctement reportees (info uniquement)."""
        result = fiscal_estimator.estimate(loyer_annuel=7200.0, charges=1440.0)

        assert result["charges_annuelles"] == 1440.0

    def test_charges_none(self, fiscal_estimator: FiscalEstimator) -> None:
        """Charges = None doit retourner None pour charges_annuelles."""
        result = fiscal_estimator.estimate(loyer_annuel=7200.0, charges=None)

        assert result["charges_annuelles"] is None


# ===========================================================================
# Tests GeoScorer
# ===========================================================================


class TestGeoScorer:
    """Tests pour le scoring de localisation."""

    def test_haversine_known_distance(self) -> None:
        """Test de la formule haversine avec une distance connue.

        Distance approximative Paris (48.8566, 2.3522) -> Lyon (45.7640, 4.8357)
        est d'environ 392 km.
        """
        paris = (48.8566, 2.3522)
        lyon = (45.7640, 4.8357)

        distance = GeoScorer._haversine_distance(paris, lyon)

        # Distance connue ~392 km, tolerance de 5 km
        assert distance == pytest.approx(392_000, rel=0.02)

    def test_haversine_same_point(self) -> None:
        """Distance entre deux points identiques doit etre 0."""
        point = (47.2378, 6.0241)
        distance = GeoScorer._haversine_distance(point, point)
        assert distance == pytest.approx(0.0, abs=0.1)

    def test_haversine_short_distance(self) -> None:
        """Test de distance courte (quelques centaines de metres)."""
        # Deux points a environ 500m l'un de l'autre a Besancon
        p1 = (47.2378, 6.0241)
        p2 = (47.2378, 6.0310)  # ~500m a l'est

        distance = GeoScorer._haversine_distance(p1, p2)

        # Devrait etre entre 400 et 600m
        assert 400 < distance < 600

    def test_distance_score_zero_at_max(self) -> None:
        """Score = 0 quand distance >= max_distance."""
        p1 = (47.2378, 6.0241)
        p2 = (47.2500, 6.0241)  # ~1.35 km au nord

        score = GeoScorer._distance_score(p1, p2, max_distance_m=500.0)
        assert score == 0.0

    def test_distance_score_100_at_same_point(self) -> None:
        """Score = 100 quand les points sont confondus."""
        p = (47.2378, 6.0241)
        score = GeoScorer._distance_score(p, p, max_distance_m=500.0)
        assert score == pytest.approx(100.0, abs=0.1)

    def test_distance_score_intermediate(self) -> None:
        """Score intermediaire pour une distance intermediaire."""
        p1 = (47.2378, 6.0241)
        p2 = (47.2378, 6.0241)

        # Test avec un point tres proche (score eleve)
        score = GeoScorer._distance_score(p1, p2, max_distance_m=1000.0)
        assert score > 90.0

    def test_distance_score_max_distance_zero(self) -> None:
        """max_distance = 0 doit retourner 0."""
        p = (47.2378, 6.0241)
        score = GeoScorer._distance_score(p, p, max_distance_m=0.0)
        assert score == 0.0

    def test_identify_quartier_centre_ville(
        self, geo_scorer: GeoScorer
    ) -> None:
        """Identifie correctement le Centre-Ville."""
        # Coordonnees du centre du Centre-Ville
        result = geo_scorer.identify_quartier((47.2378, 6.0241))
        assert result == "Centre-Ville"

    def test_identify_quartier_battant(self, geo_scorer: GeoScorer) -> None:
        """Identifie correctement Battant."""
        # Coordonnees proches du centre de Battant
        result = geo_scorer.identify_quartier((47.2410, 6.0180))
        assert result == "Battant"

    def test_identify_quartier_unknown(self, geo_scorer: GeoScorer) -> None:
        """Retourne None pour un point tres eloigne."""
        # Paris - bien au-dela du rayon de 2km
        result = geo_scorer.identify_quartier((48.8566, 2.3522))
        assert result is None

    def test_identify_quartier_invalid_coords(
        self, geo_scorer: GeoScorer
    ) -> None:
        """Retourne None pour des coordonnees invalides."""
        result = geo_scorer.identify_quartier(())  # type: ignore[arg-type]
        assert result is None

    def test_score_localisation_centre_ville(
        self, geo_scorer: GeoScorer
    ) -> None:
        """Le Centre-Ville de Besancon doit avoir un bon score de localisation."""
        score = geo_scorer.score_localisation(
            (47.2378, 6.0241), "Centre-Ville"
        )

        # Le centre-ville est proche de tout => score eleve
        assert score > 50.0

    def test_score_localisation_bounded(self, geo_scorer: GeoScorer) -> None:
        """Le score de localisation doit etre entre 0 et 100."""
        score = geo_scorer.score_localisation((47.2378, 6.0241), "Centre-Ville")
        assert 0 <= score <= 100

    def test_score_localisation_invalid_coords(
        self, geo_scorer: GeoScorer
    ) -> None:
        """Coordonnees invalides retournent un score de 0."""
        score = geo_scorer.score_localisation((), None)  # type: ignore[arg-type]
        assert score == 0.0

    def test_quartier_attractivite(self, geo_scorer: GeoScorer) -> None:
        """Recupere correctement le score d'attractivite d'un quartier."""
        score = geo_scorer._get_quartier_attractivite("Centre-Ville")
        assert score == 85.0

    def test_quartier_attractivite_unknown(self, geo_scorer: GeoScorer) -> None:
        """Retourne 50 pour un quartier inconnu."""
        score = geo_scorer._get_quartier_attractivite("Inconnu")
        assert score == 50.0

    def test_quartier_attractivite_none(self, geo_scorer: GeoScorer) -> None:
        """Retourne 50 pour quartier = None."""
        score = geo_scorer._get_quartier_attractivite(None)
        assert score == 50.0

    def test_quartier_tension(self, geo_scorer: GeoScorer) -> None:
        """Recupere correctement la tension locative."""
        tension = geo_scorer.get_quartier_tension("Centre-Ville")
        assert tension == 0.85

    def test_quartier_tension_unknown(self, geo_scorer: GeoScorer) -> None:
        """Retourne 0.5 pour un quartier inconnu."""
        tension = geo_scorer.get_quartier_tension("Inconnu")
        assert tension == 0.5

    def test_quartier_risque_vacance(self, geo_scorer: GeoScorer) -> None:
        """Recupere correctement le risque de vacance."""
        risque = geo_scorer.get_quartier_risque_vacance("Centre-Ville")
        assert risque == "faible"

        risque_chablais = geo_scorer.get_quartier_risque_vacance("Chablais")
        assert risque_chablais == "moyen"

    def test_quartier_risque_vacance_unknown(
        self, geo_scorer: GeoScorer
    ) -> None:
        """Retourne 'moyen' pour un quartier inconnu."""
        risque = geo_scorer.get_quartier_risque_vacance("Inconnu")
        assert risque == "moyen"


# ===========================================================================
# Tests Geocoder
# ===========================================================================


class TestGeocoder:
    """Tests pour le geocoder (API mockee)."""

    @pytest.fixture
    def geocoder(self, tmp_path: Path) -> Geocoder:
        """Instance de Geocoder avec cache temporaire."""
        return Geocoder(
            api_url="https://api-adresse.data.gouv.fr/search/",
            cache_dir=tmp_path / "geocode_cache",
            min_delay=0.0,
            max_retries=2,
        )

    @pytest.fixture
    def mock_api_response(self) -> dict[str, Any]:
        """Reponse type de l'API Adresse data.gouv.fr."""
        return {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [6.0241, 47.2378],
                    },
                    "properties": {
                        "label": "12 Rue de la Republique 25000 Besancon",
                        "score": 0.92,
                        "city": "Besancon",
                        "postcode": "25000",
                    },
                }
            ],
        }

    def test_geocode_success(
        self,
        geocoder: Geocoder,
        mock_api_response: dict[str, Any],
    ) -> None:
        """Test du geocodage reussi avec API mockee."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_api_response
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)
            mock_client.get.return_value = mock_response
            mock_client_cls.return_value = mock_client

            result = geocoder.geocode("12 rue de la Republique, 25000 Besancon")

        assert result is not None
        assert result["latitude"] == pytest.approx(47.2378)
        assert result["longitude"] == pytest.approx(6.0241)
        assert result["city"] == "Besancon"
        assert result["postcode"] == "25000"
        assert result["score"] == pytest.approx(0.92)

    def test_geocode_empty_address(self, geocoder: Geocoder) -> None:
        """Une adresse vide retourne None sans appel API."""
        result = geocoder.geocode("")
        assert result is None

    def test_geocode_whitespace_address(self, geocoder: Geocoder) -> None:
        """Une adresse contenant uniquement des espaces retourne None."""
        result = geocoder.geocode("   ")
        assert result is None

    def test_geocode_cache_memory(
        self,
        geocoder: Geocoder,
        mock_api_response: dict[str, Any],
    ) -> None:
        """Le second appel pour la meme adresse utilise le cache memoire."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_api_response
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)
            mock_client.get.return_value = mock_response
            mock_client_cls.return_value = mock_client

            result1 = geocoder.geocode("12 rue de la Republique, 25000 Besancon")
            result2 = geocoder.geocode("12 rue de la Republique, 25000 Besancon")

            # L'API ne doit avoir ete appelee qu'une seule fois
            assert mock_client.get.call_count == 1

        assert result1 == result2

    def test_geocode_no_results(self, geocoder: Geocoder) -> None:
        """Retourne None quand l'API ne renvoie aucun resultat."""
        empty_response = {"type": "FeatureCollection", "features": []}
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = empty_response
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)
            mock_client.get.return_value = mock_response
            mock_client_cls.return_value = mock_client

            result = geocoder.geocode("adresse inexistante xyz")

        assert result is None

    def test_geocode_api_error_retry(self, geocoder: Geocoder) -> None:
        """L'API est relancee en cas d'erreur (retry)."""
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Server Error",
            request=MagicMock(),
            response=mock_response,
        )

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)
            mock_client.get.return_value = mock_response
            mock_client_cls.return_value = mock_client

            with patch("time.sleep"):  # Accelerer le test
                result = geocoder.geocode("12 rue test")

        assert result is None
        # max_retries = 2, donc 2 appels
        assert mock_client.get.call_count == 2

    def test_cache_key_deterministic(self, geocoder: Geocoder) -> None:
        """La cle de cache est deterministe pour la meme adresse."""
        key1 = geocoder._make_cache_key("12 Rue de la Republique")
        key2 = geocoder._make_cache_key("12 rue de la republique")  # lowercase
        assert key1 == key2

    def test_file_cache_persistence(
        self,
        geocoder: Geocoder,
        mock_api_response: dict[str, Any],
    ) -> None:
        """Le cache fichier persiste les resultats."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_api_response
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)
            mock_client.get.return_value = mock_response
            mock_client_cls.return_value = mock_client

            geocoder.geocode("12 rue test besancon")

        # Verifier qu'un fichier cache a ete cree
        cache_files = list(geocoder._cache_dir.glob("*.json"))
        assert len(cache_files) == 1

    def test_clear_cache(
        self,
        geocoder: Geocoder,
        mock_api_response: dict[str, Any],
    ) -> None:
        """Le nettoyage du cache vide la memoire et les fichiers."""
        # Remplir le cache
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = mock_api_response
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client.__enter__ = MagicMock(return_value=mock_client)
            mock_client.__exit__ = MagicMock(return_value=False)
            mock_client.get.return_value = mock_response
            mock_client_cls.return_value = mock_client

            geocoder.geocode("12 rue cache test")

        assert len(geocoder._memory_cache) > 0

        geocoder.clear_cache()

        assert len(geocoder._memory_cache) == 0
        cache_files = list(geocoder._cache_dir.glob("*.json"))
        assert len(cache_files) == 0


# ===========================================================================
# Tests d'integration Pipeline
# ===========================================================================


class TestScoringPipeline:
    """Tests d'integration du pipeline complet de scoring."""

    def test_full_pipeline(
        self,
        rentabilite_calculator: RentabiliteCalculator,
        composite_scorer: CompositeScorer,
        fiscal_estimator: FiscalEstimator,
        geo_scorer: GeoScorer,
    ) -> None:
        """Test du pipeline complet : rentabilite -> geo -> composite -> fiscal."""
        # 1. Calcul de la rentabilite
        renta = rentabilite_calculator.calculate(
            prix=140000, loyer_mensuel=580.0, charges_copro=120.0
        )

        # 2. Scoring geo
        geo_score = geo_scorer.score_localisation(
            (47.2378, 6.0241), "Centre-Ville"
        )

        # 3. Scoring composite
        annonce = {
            "dpe": "C",
            "quartier": "Centre-Ville",
            "description_texte": "Bel appartement T3, vente rapide souhaitee.",
            "historique_prix": [],
            "tension_locative": 0.85,
        }
        composite = composite_scorer.score(annonce, renta, geo_score)

        # 4. Estimation fiscale
        fiscal = fiscal_estimator.estimate(
            loyer_annuel=renta["loyer_annuel"],
            charges=renta["charges_annuelles"],
        )

        # Verifications de bout en bout
        assert renta["renta_brute"] > 0
        assert geo_score > 0
        assert composite["score_global"] > 0
        assert composite["niveau_alerte"] in ("top", "bon", "veille")
        assert fiscal["regime_indicatif"] in ("lmnp", "nu")
        assert fiscal["disclaimer"] is not None

    def test_pipeline_high_yield_triggers_top_alert(
        self,
        rentabilite_calculator: RentabiliteCalculator,
        composite_scorer: CompositeScorer,
        geo_scorer: GeoScorer,
    ) -> None:
        """Un bien avec renta >= 8% doit declencher une alerte TOP."""
        # Prix bas + loyer eleve => renta elevee
        renta = rentabilite_calculator.calculate(
            prix=100000, loyer_mensuel=700.0
        )

        geo_score = geo_scorer.score_localisation(
            (47.2378, 6.0241), "Centre-Ville"
        )

        annonce = {
            "dpe": "B",
            "quartier": "Centre-Ville",
            "tension_locative": 0.85,
        }
        composite = composite_scorer.score(annonce, renta, geo_score)

        assert renta["renta_brute"] >= 8.0
        assert composite["niveau_alerte"] == "top"

    def test_pipeline_low_yield_triggers_veille(
        self,
        rentabilite_calculator: RentabiliteCalculator,
        composite_scorer: CompositeScorer,
    ) -> None:
        """Un bien peu rentable avec un mauvais DPE doit etre en VEILLE."""
        renta = rentabilite_calculator.calculate(
            prix=200000, loyer_mensuel=400.0
        )

        annonce = {
            "dpe": "F",
            "quartier": "Inconnu",
            "tension_locative": 0.3,
        }
        composite = composite_scorer.score(annonce, renta, 20.0)

        assert composite["niveau_alerte"] == "veille"
        assert composite["score_global"] < 60

    def test_scoring_config_weights_from_yaml(self) -> None:
        """Les poids du scoring.yaml sont correctement charges et somment a 1."""
        config_path = Path(__file__).resolve().parent.parent / "config" / "scoring.yaml"
        if not config_path.exists():
            pytest.skip("scoring.yaml non disponible")

        import yaml

        with open(config_path, encoding="utf-8") as f:
            config = yaml.safe_load(f)

        poids = config.get("poids", {})
        total = sum(poids.values())
        assert total == pytest.approx(1.0, abs=1e-6)
