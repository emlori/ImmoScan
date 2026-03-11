"""Tests pour le module observatoire des loyers.

Couvre le module src.observatoire.loyers (ObservatoireLoyers).
Tous les tests fonctionnent sans acces reseau ni base de donnees.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from src.observatoire.loyers import ObservatoireLoyers


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def quartiers_config() -> dict[str, Any]:
    """Configuration des quartiers avec loyers fallback pour les tests.

    Returns:
        Dictionnaire de configuration des quartiers.
    """
    return {
        "quartiers": {
            "centre_ville": {
                "nom": "Centre-Ville",
                "code_postal": "25000",
                "score_attractivite": 85,
                "tension_locative": 0.85,
                "loyers_fallback": {
                    "T2": {
                        "meuble": {"loyer_median": 550, "loyer_m2": 15.0},
                        "nu": {"loyer_median": 450, "loyer_m2": 12.5},
                    },
                    "T3": {
                        "meuble": {"loyer_median": 700, "loyer_m2": 12.5},
                        "nu": {"loyer_median": 580, "loyer_m2": 10.5},
                    },
                },
            },
            "battant": {
                "nom": "Battant",
                "code_postal": "25000",
                "score_attractivite": 80,
                "tension_locative": 0.80,
                "loyers_fallback": {
                    "T2": {
                        "meuble": {"loyer_median": 520, "loyer_m2": 14.5},
                        "nu": {"loyer_median": 430, "loyer_m2": 12.0},
                    },
                    "T3": {
                        "meuble": {"loyer_median": 670, "loyer_m2": 12.0},
                        "nu": {"loyer_median": 560, "loyer_m2": 10.0},
                    },
                },
            },
        }
    }


@pytest.fixture
def observatoire(quartiers_config: dict[str, Any]) -> ObservatoireLoyers:
    """Retourne un ObservatoireLoyers configure pour les tests.

    Args:
        quartiers_config: Configuration des quartiers.

    Returns:
        Instance de ObservatoireLoyers.
    """
    return ObservatoireLoyers(
        quartiers_config=quartiers_config,
        half_life_days=30,
        min_fiable=5,
    )


@pytest.fixture
def now() -> datetime:
    """Retourne la date/heure courante avec timezone UTC.

    Returns:
        datetime.now(timezone.utc).
    """
    return datetime.now(timezone.utc)


def _make_loyer(
    loyer_cc: float,
    surface_m2: float,
    nb_pieces: int,
    quartier: str,
    meuble: bool,
    date_scrape: datetime | None = None,
) -> dict[str, Any]:
    """Cree un dict d'annonce de location pour les tests.

    Args:
        loyer_cc: Loyer charges comprises.
        surface_m2: Surface en metres carres.
        nb_pieces: Nombre de pieces.
        quartier: Nom du quartier.
        meuble: True pour meuble, False pour nu.
        date_scrape: Date de scraping (defaut : maintenant).

    Returns:
        Dictionnaire representant une annonce de location.
    """
    if date_scrape is None:
        date_scrape = datetime.now(timezone.utc)
    return {
        "loyer_cc": loyer_cc,
        "surface_m2": surface_m2,
        "nb_pieces": nb_pieces,
        "quartier": quartier,
        "meuble": meuble,
        "date_scrape": date_scrape,
    }


def _make_loyers_centre_t2_nu(
    n: int, base_loyer: float = 450.0, now: datetime | None = None
) -> list[dict[str, Any]]:
    """Genere n annonces T2 nu Centre-Ville avec des loyers varies.

    Args:
        n: Nombre d'annonces a generer.
        base_loyer: Loyer de base.
        now: Date de reference.

    Returns:
        Liste de dictionnaires d'annonces.
    """
    if now is None:
        now = datetime.now(timezone.utc)
    loyers = []
    for i in range(n):
        loyer = base_loyer + (i - n // 2) * 10
        date_scrape = now - timedelta(days=i * 2)
        loyers.append(
            _make_loyer(
                loyer_cc=loyer,
                surface_m2=40.0,
                nb_pieces=2,
                quartier="Centre-Ville",
                meuble=False,
                date_scrape=date_scrape,
            )
        )
    return loyers


# ---------------------------------------------------------------------------
# Tests : compute_medianes
# ---------------------------------------------------------------------------


class TestComputeMedianes:
    """Tests pour le calcul des medianes de loyer."""

    def test_medianes_with_sufficient_data(
        self, observatoire: ObservatoireLoyers, now: datetime
    ) -> None:
        """Avec >= 5 annonces, la fiabilite est 'fiable'."""
        loyers = _make_loyers_centre_t2_nu(8, now=now)
        result = observatoire.compute_medianes(loyers, "Centre-Ville", "T2", False)

        assert result["fiabilite"] == "fiable"
        assert result["nb_annonces"] == 8
        assert result["loyer_median"] is not None
        assert result["loyer_q1"] is not None
        assert result["loyer_q3"] is not None
        assert result["loyer_m2_median"] is not None
        assert result["quartier"] == "Centre-Ville"
        assert result["type_bien"] == "T2"
        assert result["meuble"] is False

    def test_medianes_with_insufficient_data(
        self, observatoire: ObservatoireLoyers, now: datetime
    ) -> None:
        """Avec < 5 annonces, la fiabilite est 'preliminaire'."""
        loyers = _make_loyers_centre_t2_nu(3, now=now)
        result = observatoire.compute_medianes(loyers, "Centre-Ville", "T2", False)

        assert result["fiabilite"] == "preliminaire"
        assert result["nb_annonces"] == 3
        assert result["loyer_median"] is not None

    def test_medianes_empty_input(
        self, observatoire: ObservatoireLoyers
    ) -> None:
        """Avec une liste vide, retourne des valeurs nulles."""
        result = observatoire.compute_medianes([], "Centre-Ville", "T2", False)

        assert result["nb_annonces"] == 0
        assert result["loyer_median"] is None
        assert result["fiabilite"] == "preliminaire"

    def test_medianes_no_matching_segment(
        self, observatoire: ObservatoireLoyers, now: datetime
    ) -> None:
        """Sans annonces correspondant au segment, retourne des valeurs nulles."""
        loyers = _make_loyers_centre_t2_nu(5, now=now)
        # Chercher Battant T3 meuble alors que les annonces sont Centre-Ville T2 nu
        result = observatoire.compute_medianes(loyers, "Battant", "T3", True)

        assert result["nb_annonces"] == 0
        assert result["loyer_median"] is None

    def test_medianes_returns_correct_quartile_order(
        self, observatoire: ObservatoireLoyers, now: datetime
    ) -> None:
        """Q1 <= mediane <= Q3."""
        loyers = _make_loyers_centre_t2_nu(10, now=now)
        result = observatoire.compute_medianes(loyers, "Centre-Ville", "T2", False)

        assert result["loyer_q1"] is not None
        assert result["loyer_q3"] is not None
        assert result["loyer_median"] is not None
        assert result["loyer_q1"] <= result["loyer_median"]
        assert result["loyer_median"] <= result["loyer_q3"]

    def test_medianes_loyer_m2_calculated(
        self, observatoire: ObservatoireLoyers, now: datetime
    ) -> None:
        """Le loyer au m2 median est calcule correctement."""
        loyers = [
            _make_loyer(500.0, 40.0, 2, "Centre-Ville", False, now),
            _make_loyer(520.0, 40.0, 2, "Centre-Ville", False, now),
            _make_loyer(480.0, 40.0, 2, "Centre-Ville", False, now),
            _make_loyer(510.0, 40.0, 2, "Centre-Ville", False, now),
            _make_loyer(490.0, 40.0, 2, "Centre-Ville", False, now),
        ]
        result = observatoire.compute_medianes(loyers, "Centre-Ville", "T2", False)

        assert result["loyer_m2_median"] is not None
        # loyer/m2 devrait etre environ 12-13 pour loyer ~500 / 40m2
        assert 10.0 <= result["loyer_m2_median"] <= 15.0


# ---------------------------------------------------------------------------
# Tests : suppression des outliers
# ---------------------------------------------------------------------------


class TestOutlierRemoval:
    """Tests pour la suppression d'outliers par IQR."""

    def test_outliers_removed(
        self, observatoire: ObservatoireLoyers
    ) -> None:
        """Les valeurs extremes sont supprimees."""
        values = [450, 460, 470, 480, 490, 500, 510, 520, 1500]
        clean = observatoire._remove_outliers(values)
        assert 1500 not in clean
        assert len(clean) < len(values)

    def test_no_outliers_all_kept(
        self, observatoire: ObservatoireLoyers
    ) -> None:
        """Sans outliers, toutes les valeurs sont conservees."""
        values = [450, 460, 470, 480, 490, 500, 510, 520]
        clean = observatoire._remove_outliers(values)
        assert len(clean) == len(values)

    def test_all_same_values(
        self, observatoire: ObservatoireLoyers
    ) -> None:
        """Quand toutes les valeurs sont identiques, aucune n'est supprimee."""
        values = [500.0, 500.0, 500.0, 500.0, 500.0]
        clean = observatoire._remove_outliers(values)
        assert len(clean) == 5

    def test_small_sample_no_removal(
        self, observatoire: ObservatoireLoyers
    ) -> None:
        """Avec moins de 4 valeurs, pas de suppression d'outliers."""
        values = [100, 500, 1000]
        clean = observatoire._remove_outliers(values)
        assert len(clean) == 3

    def test_outliers_both_sides(
        self, observatoire: ObservatoireLoyers
    ) -> None:
        """Les outliers sont supprimes des deux cotes."""
        values = [50, 450, 460, 470, 480, 490, 500, 510, 520, 2000]
        clean = observatoire._remove_outliers(values)
        assert 50 not in clean
        assert 2000 not in clean


# ---------------------------------------------------------------------------
# Tests : ponderation temporelle
# ---------------------------------------------------------------------------


class TestTemporalWeight:
    """Tests pour la ponderation temporelle exponentielle."""

    def test_recent_listing_high_weight(
        self, observatoire: ObservatoireLoyers
    ) -> None:
        """Une annonce recente a un poids proche de 1."""
        now = datetime.now(timezone.utc)
        weight = observatoire._temporal_weight(now, half_life_days=30)
        assert weight > 0.95

    def test_old_listing_low_weight(
        self, observatoire: ObservatoireLoyers
    ) -> None:
        """Une annonce de 90 jours a un poids faible."""
        old_date = datetime.now(timezone.utc) - timedelta(days=90)
        weight = observatoire._temporal_weight(old_date, half_life_days=30)
        # Apres 90 jours (3 demi-vies) : 2^(-3) = 0.125
        assert weight < 0.15

    def test_half_life_exact(
        self, observatoire: ObservatoireLoyers
    ) -> None:
        """A la demi-vie exacte, le poids est ~0.5."""
        half_life_date = datetime.now(timezone.utc) - timedelta(days=30)
        weight = observatoire._temporal_weight(half_life_date, half_life_days=30)
        assert abs(weight - 0.5) < 0.05

    def test_none_date_returns_neutral_weight(
        self, observatoire: ObservatoireLoyers
    ) -> None:
        """Une date None retourne un poids neutre de 0.5."""
        weight = observatoire._temporal_weight(None)
        assert weight == 0.5

    def test_string_date_parsed(
        self, observatoire: ObservatoireLoyers
    ) -> None:
        """Une date en format string ISO est correctement parsee."""
        now = datetime.now(timezone.utc)
        date_str = now.isoformat()
        weight = observatoire._temporal_weight(date_str, half_life_days=30)
        assert weight > 0.95


# ---------------------------------------------------------------------------
# Tests : mediane ponderee
# ---------------------------------------------------------------------------


class TestWeightedMedian:
    """Tests pour le calcul de la mediane ponderee."""

    def test_equal_weights(
        self, observatoire: ObservatoireLoyers
    ) -> None:
        """Avec des poids egaux, la mediane ponderee est la mediane classique."""
        values = [100, 200, 300, 400, 500]
        weights = [1.0, 1.0, 1.0, 1.0, 1.0]
        median = observatoire._weighted_median(values, weights)
        assert median == 300

    def test_skewed_weights(
        self, observatoire: ObservatoireLoyers
    ) -> None:
        """Avec des poids inegaux, la mediane se deplace vers les poids forts."""
        values = [100, 200, 300, 400, 500]
        # Poids forts sur les valeurs basses
        weights = [10.0, 10.0, 1.0, 1.0, 1.0]
        median = observatoire._weighted_median(values, weights)
        assert median <= 200

    def test_single_value(
        self, observatoire: ObservatoireLoyers
    ) -> None:
        """Avec une seule valeur, retourne cette valeur."""
        median = observatoire._weighted_median([500], [1.0])
        assert median == 500

    def test_empty_raises_error(
        self, observatoire: ObservatoireLoyers
    ) -> None:
        """Une liste vide leve une ValueError."""
        with pytest.raises(ValueError, match="vide"):
            observatoire._weighted_median([], [])

    def test_mismatched_lengths_raises_error(
        self, observatoire: ObservatoireLoyers
    ) -> None:
        """Des listes de tailles differentes levent une ValueError."""
        with pytest.raises(ValueError, match="meme taille"):
            observatoire._weighted_median([1, 2, 3], [1.0, 2.0])


# ---------------------------------------------------------------------------
# Tests : filtrage par segment
# ---------------------------------------------------------------------------


class TestSegmentFilter:
    """Tests pour le filtrage des annonces par segment."""

    def test_correct_filtering(
        self, observatoire: ObservatoireLoyers, now: datetime
    ) -> None:
        """Seules les annonces du segment exact sont retournees."""
        loyers = [
            _make_loyer(500, 40, 2, "Centre-Ville", False, now),
            _make_loyer(550, 45, 2, "Centre-Ville", True, now),  # meuble
            _make_loyer(600, 60, 3, "Centre-Ville", False, now),  # T3
            _make_loyer(480, 38, 2, "Battant", False, now),  # autre quartier
            _make_loyer(510, 42, 2, "Centre-Ville", False, now),
        ]
        filtered = observatoire._filter_segment(
            loyers, "Centre-Ville", "T2", False
        )
        assert len(filtered) == 2
        for a in filtered:
            assert a["quartier"] == "Centre-Ville"
            assert a["nb_pieces"] == 2
            assert a["meuble"] is False

    def test_filter_empty_result(
        self, observatoire: ObservatoireLoyers, now: datetime
    ) -> None:
        """Si aucune annonce ne correspond, retourne une liste vide."""
        loyers = [
            _make_loyer(500, 40, 2, "Centre-Ville", False, now),
        ]
        filtered = observatoire._filter_segment(
            loyers, "Battant", "T3", True
        )
        assert len(filtered) == 0


# ---------------------------------------------------------------------------
# Tests : estimation de loyer
# ---------------------------------------------------------------------------


class TestEstimateLoyer:
    """Tests pour l'estimation de loyer."""

    def test_estimate_with_references(
        self, observatoire: ObservatoireLoyers
    ) -> None:
        """Utilise les references de l'observatoire quand disponibles."""
        references = [
            {
                "quartier": "Centre-Ville",
                "type_bien": "T2",
                "meuble": False,
                "loyer_median": 460.0,
                "loyer_m2_median": 12.0,
                "nb_annonces": 10,
                "fiabilite": "fiable",
            }
        ]
        result = observatoire.estimate_loyer(
            "Centre-Ville", "T2", False, 40.0, references=references
        )

        assert result["source"] == "observatoire"
        assert result["fiabilite"] == "fiable"
        assert result["loyer_estime"] == 480.0  # 12.0 * 40

    def test_estimate_fallback_no_references(
        self, observatoire: ObservatoireLoyers
    ) -> None:
        """Utilise le fallback quand aucune reference n'est disponible."""
        result = observatoire.estimate_loyer(
            "Centre-Ville", "T2", False, 40.0, references=None
        )

        assert result["source"] == "fallback"
        assert result["fiabilite"] == "preliminaire"
        # loyer_m2 fallback Centre-Ville T2 nu = 12.5, surface 40m2
        assert result["loyer_estime"] == 500.0

    def test_estimate_fallback_insufficient_references(
        self, observatoire: ObservatoireLoyers
    ) -> None:
        """Utilise le fallback quand les references sont insuffisantes (< 5)."""
        references = [
            {
                "quartier": "Centre-Ville",
                "type_bien": "T2",
                "meuble": False,
                "loyer_median": 460.0,
                "loyer_m2_median": 12.0,
                "nb_annonces": 3,  # Insuffisant
                "fiabilite": "preliminaire",
            }
        ]
        result = observatoire.estimate_loyer(
            "Centre-Ville", "T2", False, 40.0, references=references
        )

        assert result["source"] == "fallback"
        assert result["fiabilite"] == "preliminaire"

    def test_estimate_scales_with_surface(
        self, observatoire: ObservatoireLoyers
    ) -> None:
        """L'estimation de loyer est proportionnelle a la surface."""
        references = [
            {
                "quartier": "Centre-Ville",
                "type_bien": "T2",
                "meuble": False,
                "loyer_median": 460.0,
                "loyer_m2_median": 12.0,
                "nb_annonces": 10,
                "fiabilite": "fiable",
            }
        ]
        result_40 = observatoire.estimate_loyer(
            "Centre-Ville", "T2", False, 40.0, references=references
        )
        result_50 = observatoire.estimate_loyer(
            "Centre-Ville", "T2", False, 50.0, references=references
        )

        assert result_50["loyer_estime"] > result_40["loyer_estime"]
        ratio = result_50["loyer_estime"] / result_40["loyer_estime"]
        assert abs(ratio - 50.0 / 40.0) < 0.01

    def test_estimate_unknown_quartier(
        self, observatoire: ObservatoireLoyers
    ) -> None:
        """Retourne indisponible pour un quartier inconnu."""
        result = observatoire.estimate_loyer(
            "Quartier Inconnu", "T2", False, 40.0
        )

        assert result["loyer_estime"] is None
        assert result["fiabilite"] == "indisponible"

    def test_estimate_meuble_vs_nu(
        self, observatoire: ObservatoireLoyers
    ) -> None:
        """Le loyer meuble est superieur au loyer nu (en fallback)."""
        result_nu = observatoire.estimate_loyer(
            "Centre-Ville", "T2", False, 40.0
        )
        result_meuble = observatoire.estimate_loyer(
            "Centre-Ville", "T2", True, 40.0
        )

        assert result_meuble["loyer_estime"] > result_nu["loyer_estime"]


# ---------------------------------------------------------------------------
# Tests : pipeline complet
# ---------------------------------------------------------------------------


class TestFullPipeline:
    """Tests d'integration pour le flux complet de l'observatoire."""

    def test_listings_to_medianes_to_estimate(
        self, observatoire: ObservatoireLoyers, now: datetime
    ) -> None:
        """Pipeline complet : annonces -> medianes -> estimation."""
        # Generer des annonces de location
        loyers = _make_loyers_centre_t2_nu(10, base_loyer=450.0, now=now)

        # Calculer les medianes
        medianes = observatoire.compute_medianes(
            loyers, "Centre-Ville", "T2", False
        )

        assert medianes["fiabilite"] == "fiable"
        assert medianes["nb_annonces"] == 10

        # Estimer un loyer
        result = observatoire.estimate_loyer(
            "Centre-Ville",
            "T2",
            False,
            42.0,
            references=[medianes],
        )

        assert result["source"] == "observatoire"
        assert result["fiabilite"] == "fiable"
        assert result["loyer_estime"] is not None
        assert result["loyer_estime"] > 0

    def test_empty_listings_falls_back(
        self, observatoire: ObservatoireLoyers
    ) -> None:
        """Avec zero annonces, l'estimation utilise le fallback."""
        medianes = observatoire.compute_medianes([], "Centre-Ville", "T2", False)
        assert medianes["nb_annonces"] == 0

        result = observatoire.estimate_loyer(
            "Centre-Ville",
            "T2",
            False,
            40.0,
            references=[medianes],
        )

        assert result["source"] == "fallback"
        assert result["fiabilite"] == "preliminaire"
        assert result["loyer_estime"] is not None
