"""Tests d'integration pour les DAGs Airflow et pipelines ImmoScan.

Teste le pipeline complet (scrape -> validate -> normalize -> dedup -> score -> alert)
avec des donnees mock, sans base de donnees ni acces reseau.

Tous les imports Airflow sont mockes pour permettre l'execution sans Airflow installe.
"""

from __future__ import annotations

import importlib
import sys
import types
from datetime import datetime, timedelta
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

# ------------------------------------------------------------------
# Mock Airflow avant tout import de DAGs
# ------------------------------------------------------------------


def _install_airflow_mocks() -> dict[str, types.ModuleType]:
    """Installe des modules Airflow mockes dans sys.modules.

    Permet d'importer les fichiers DAG sans Airflow installe.

    Returns:
        Dictionnaire des modules mockes crees.
    """
    mocks: dict[str, types.ModuleType] = {}

    # Module airflow principal
    airflow_mod = types.ModuleType("airflow")
    airflow_mod.DAG = type("DAG", (), {
        "__init__": lambda self, **kwargs: None,
        "__enter__": lambda self: self,
        "__exit__": lambda self, *args: None,
    })
    mocks["airflow"] = airflow_mod

    # airflow.operators
    operators_mod = types.ModuleType("airflow.operators")
    mocks["airflow.operators"] = operators_mod

    # airflow.operators.python
    python_mod = types.ModuleType("airflow.operators.python")

    class MockPythonOperator:
        """Mock PythonOperator qui stocke les parametres sans executer."""

        def __init__(self, **kwargs: Any) -> None:
            self.task_id: str = kwargs.get("task_id", "")
            self.python_callable = kwargs.get("python_callable")
            self.op_kwargs: dict[str, Any] = kwargs.get("op_kwargs", {})
            self.trigger_rule: str = kwargs.get("trigger_rule", "all_success")
            self._upstream_list: list[Any] = []
            self._downstream_list: list[Any] = []

        def __rshift__(self, other: Any) -> Any:
            """Operateur >> pour les dependances."""
            if isinstance(other, list):
                for item in other:
                    self._downstream_list.append(item)
                    if hasattr(item, "_upstream_list"):
                        item._upstream_list.append(self)
            elif hasattr(other, "_upstream_list"):
                self._downstream_list.append(other)
                other._upstream_list.append(self)
            return other

        def __rrshift__(self, other: Any) -> "MockPythonOperator":
            """Operateur >> inverse pour les listes."""
            if isinstance(other, list):
                for item in other:
                    if hasattr(item, "_downstream_list"):
                        item._downstream_list.append(self)
                    self._upstream_list.append(item)
            return self

    python_mod.PythonOperator = MockPythonOperator
    mocks["airflow.operators.python"] = python_mod

    # airflow.utils
    utils_mod = types.ModuleType("airflow.utils")
    mocks["airflow.utils"] = utils_mod

    # airflow.utils.dates
    dates_mod = types.ModuleType("airflow.utils.dates")
    dates_mod.days_ago = lambda n: datetime.now() - timedelta(days=n)
    mocks["airflow.utils.dates"] = dates_mod

    # Installer tous les mocks dans sys.modules
    for name, mod in mocks.items():
        sys.modules[name] = mod

    return mocks


# Installer les mocks Airflow AVANT tout import de DAGs
_airflow_mocks = _install_airflow_mocks()


# ------------------------------------------------------------------
# Classe XCom mock pour simuler les echanges entre taches
# ------------------------------------------------------------------


class MockTaskInstance:
    """Simule un TaskInstance Airflow avec support XCom."""

    def __init__(self) -> None:
        self._xcom_store: dict[str, Any] = {}

    def xcom_push(self, key: str, value: Any) -> None:
        """Stocke une valeur dans XCom.

        Args:
            key: Cle XCom.
            value: Valeur a stocker.
        """
        self._xcom_store[key] = value

    def xcom_pull(self, key: str, task_ids: str | None = None) -> Any:
        """Recupere une valeur depuis XCom.

        Args:
            key: Cle XCom.
            task_ids: ID de la tache source (ignore dans le mock).

        Returns:
            Valeur stockee ou None.
        """
        return self._xcom_store.get(key)


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------


@pytest.fixture
def mock_ti() -> MockTaskInstance:
    """Fournit un TaskInstance mock frais."""
    return MockTaskInstance()


@pytest.fixture
def sample_raw_vente_listings() -> list[dict[str, Any]]:
    """Fournit des annonces de vente brutes pour les tests.

    Returns:
        Liste de 3 annonces de vente valides avec des donnees realistes.
    """
    return [
        {
            "url_source": "https://www.leboncoin.fr/ventes_immobilieres/123456.htm",
            "source": "leboncoin",
            "prix": 145000,
            "surface_m2": 55.0,
            "nb_pieces": 3,
            "dpe": "C",
            "adresse_brute": "12 Rue de la Republique, 25000 Besancon",
            "quartier": "Centre-Ville",
            "description_texte": "Bel appartement T3 lumineux, double vitrage, "
                "proche tram. Prix a debattre suite mutation.",
            "photos_urls": ["https://img.leboncoin.fr/photo1.jpg"],
            "date_publication": "2026-03-09",
            "date_scrape": datetime.now().isoformat(),
        },
        {
            "url_source": "https://www.leboncoin.fr/ventes_immobilieres/789012.htm",
            "source": "leboncoin",
            "prix": 130000,
            "surface_m2": 42.0,
            "nb_pieces": 2,
            "dpe": "B",
            "adresse_brute": "5 Rue Battant, 25000 Besancon",
            "quartier": "Battant",
            "description_texte": "T2 renove avec gout, parquet, cave.",
            "photos_urls": ["https://img.leboncoin.fr/photo2.jpg"],
            "date_publication": "2026-03-08",
            "date_scrape": datetime.now().isoformat(),
        },
        {
            "url_source": "https://www.pap.fr/annonce/vente-appartement-besancon/345678",
            "source": "pap",
            "prix": 155000,
            "surface_m2": 60.0,
            "nb_pieces": 3,
            "dpe": "D",
            "adresse_brute": "8 Avenue de Chardonnet, 25000 Besancon",
            "quartier": "Chablais",
            "description_texte": "Grand T3 avec balcon et parking.",
            "photos_urls": [],
            "date_publication": "2026-03-07",
            "date_scrape": datetime.now().isoformat(),
        },
    ]


@pytest.fixture
def sample_invalid_listings() -> list[dict[str, Any]]:
    """Fournit des annonces invalides pour tester le rejet.

    Returns:
        Liste d'annonces avec differents types d'erreurs de validation.
    """
    return [
        {
            # Prix hors bornes
            "url_source": "https://www.leboncoin.fr/ventes/bad1.htm",
            "source": "leboncoin",
            "prix": 5000,
            "surface_m2": 55.0,
            "nb_pieces": 3,
            "adresse_brute": "Besancon",
        },
        {
            # Surface manquante
            "url_source": "https://www.leboncoin.fr/ventes/bad2.htm",
            "source": "leboncoin",
            "prix": 140000,
            "surface_m2": None,
            "nb_pieces": 3,
            "adresse_brute": "Besancon",
        },
        {
            # Adresse hors Besancon
            "url_source": "https://www.leboncoin.fr/ventes/bad3.htm",
            "source": "leboncoin",
            "prix": 140000,
            "surface_m2": 55.0,
            "nb_pieces": 3,
            "adresse_brute": "12 Rue de Paris, 75001 Paris",
        },
    ]


@pytest.fixture
def sample_raw_location_listings() -> list[dict[str, Any]]:
    """Fournit des annonces de location brutes pour les tests.

    Returns:
        Liste d'annonces de location valides.
    """
    return [
        {
            "url_source": "https://www.leboncoin.fr/locations/loc1.htm",
            "source": "leboncoin",
            "loyer_cc": 550.0,
            "surface_m2": 42.0,
            "nb_pieces": 2,
            "meuble": False,
            "adresse_brute": "Besancon Centre-Ville",
            "quartier": "Centre-Ville",
            "date_scrape": datetime.now().isoformat(),
        },
        {
            "url_source": "https://www.leboncoin.fr/locations/loc2.htm",
            "source": "leboncoin",
            "loyer_cc": 480.0,
            "surface_m2": 38.0,
            "nb_pieces": 2,
            "meuble": False,
            "adresse_brute": "Besancon Battant",
            "quartier": "Battant",
            "date_scrape": datetime.now().isoformat(),
        },
        {
            "url_source": "https://www.pap.fr/locations/loc3",
            "source": "pap",
            "loyer_cc": 700.0,
            "surface_m2": 58.0,
            "nb_pieces": 3,
            "meuble": True,
            "adresse_brute": "Besancon Centre",
            "quartier": "Centre-Ville",
            "date_scrape": datetime.now().isoformat(),
        },
    ]


# ------------------------------------------------------------------
# Tests du pipeline de vente complet
# ------------------------------------------------------------------


class TestFullVentePipeline:
    """Teste le pipeline complet de vente sans DB ni reseau."""

    def test_validate_valid_listings(
        self,
        sample_raw_vente_listings: list[dict[str, Any]],
    ) -> None:
        """Toutes les annonces valides passent la validation."""
        from src.validation.validators import AnnonceValidator

        validator = AnnonceValidator()
        for annonce in sample_raw_vente_listings:
            is_valid, reasons = validator.validate_vente(annonce)
            assert is_valid, f"Annonce rejetee a tort: {reasons}"

    def test_validate_rejects_invalid(
        self,
        sample_invalid_listings: list[dict[str, Any]],
    ) -> None:
        """Les annonces invalides sont correctement rejetees."""
        from src.validation.validators import AnnonceValidator

        validator = AnnonceValidator()
        for annonce in sample_invalid_listings:
            is_valid, reasons = validator.validate_vente(annonce)
            assert not is_valid, "Annonce invalide acceptee a tort"
            assert len(reasons) > 0

    def test_normalize_vente(
        self,
        sample_raw_vente_listings: list[dict[str, Any]],
    ) -> None:
        """La normalisation produit des donnees exploitables."""
        from src.parsers.normalizer import AnnonceNormalizer

        normalizer = AnnonceNormalizer()
        for raw in sample_raw_vente_listings:
            result = normalizer.normalize_vente(raw)
            assert result["prix"] is not None
            assert isinstance(result["prix"], int)
            assert result["surface_m2"] is not None
            assert isinstance(result["surface_m2"], float)
            assert result["url_source"] is not None
            assert result["completude_score"] >= 0.0

    def test_dedup_intra_source(
        self,
        sample_raw_vente_listings: list[dict[str, Any]],
    ) -> None:
        """La deduplication intra-source detecte les doublons par URL."""
        from src.parsers.dedup import Deduplicator

        dedup = Deduplicator()

        # Dupliquer la premiere annonce
        listings = list(sample_raw_vente_listings)
        duplicate = dict(listings[0])
        duplicate["prix"] = 142000  # Prix legerement different
        listings.append(duplicate)

        seen_hashes: set[str] = set()
        unique: list[dict[str, Any]] = []

        for annonce in listings:
            url = annonce.get("url_source", "")
            hash_val = dedup.compute_hash_intra(url)
            if hash_val not in seen_hashes:
                seen_hashes.add(hash_val)
                unique.append(annonce)

        # 3 uniques au lieu de 4 (le doublon est filtre)
        assert len(unique) == 3

    def test_dedup_inter_sources(self) -> None:
        """La deduplication inter-sources detecte les annonces similaires."""
        from src.parsers.dedup import Deduplicator

        dedup = Deduplicator()

        annonce_lbc = {
            "url_source": "https://www.leboncoin.fr/ventes/111.htm",
            "adresse_brute": "12 Rue de la Republique, 25000 Besancon",
            "surface_m2": 55.0,
            "prix": 145000,
        }

        annonce_pap = {
            "url_source": "https://www.pap.fr/annonce/222",
            "adresse_brute": "12 Rue de la Republique, Besancon",
            "surface_m2": 54.5,  # Surface proche (+/- 2m2)
            "prix": 143000,  # Prix proche (+/- 5%)
        }

        duplicates = dedup.find_duplicates_inter(annonce_pap, [annonce_lbc])
        assert len(duplicates) > 0

    def test_scoring_pipeline(
        self,
        sample_raw_vente_listings: list[dict[str, Any]],
    ) -> None:
        """Le pipeline de scoring produit des scores valides."""
        from src.scoring.composite import CompositeScorer
        from src.scoring.rentabilite import RentabiliteCalculator

        renta_calc = RentabiliteCalculator()
        composite_scorer = CompositeScorer(scoring_config={})

        for annonce in sample_raw_vente_listings:
            prix = annonce["prix"]
            loyer_estime = 550.0  # Estimation simplifiee

            renta = renta_calc.calculate(prix=prix, loyer_mensuel=loyer_estime)
            assert renta["renta_brute"] > 0
            assert "renta_brute_nego_5" in renta
            assert "renta_brute_nego_10" in renta
            assert "renta_brute_nego_15" in renta

            score = composite_scorer.score(
                annonce_data=annonce,
                renta_data=renta,
                geo_score=70.0,
            )
            assert 0 <= score["score_global"] <= 100
            assert score["niveau_alerte"] in ("top", "bon", "veille")

    def test_end_to_end_pipeline(
        self,
        mock_ti: MockTaskInstance,
        sample_raw_vente_listings: list[dict[str, Any]],
    ) -> None:
        """Teste le pipeline complet de bout en bout avec donnees mock."""
        from src.parsers.dedup import Deduplicator
        from src.parsers.normalizer import AnnonceNormalizer
        from src.scoring.composite import CompositeScorer
        from src.scoring.rentabilite import RentabiliteCalculator
        from src.validation.validators import AnnonceValidator

        raw_data = sample_raw_vente_listings

        # Etape 1 : Validation
        validator = AnnonceValidator()
        validated = []
        for annonce in raw_data:
            is_valid, _ = validator.validate_vente(annonce)
            if is_valid:
                validated.append(annonce)
        assert len(validated) == 3

        # Etape 2 : Normalisation
        normalizer = AnnonceNormalizer()
        normalized = [normalizer.normalize_vente(a) for a in validated]
        assert len(normalized) == 3

        # Etape 3 : Deduplication
        dedup = Deduplicator()
        seen: set[str] = set()
        unique = []
        for a in normalized:
            h = dedup.compute_hash_intra(a.get("url_source", ""))
            if h not in seen:
                seen.add(h)
                a["hash_dedup"] = h
                unique.append(a)
        assert len(unique) == 3

        # Etape 4 : Scoring (geocodage mock)
        renta_calc = RentabiliteCalculator()
        scorer = CompositeScorer(scoring_config={})
        scored = []
        for a in unique:
            prix = a.get("prix", 0) or 0
            if prix <= 0:
                continue
            renta = renta_calc.calculate(prix=prix, loyer_mensuel=550.0)
            score = scorer.score(
                annonce_data=a,
                renta_data=renta,
                geo_score=65.0,
            )
            a["renta_data"] = renta
            a["score_data"] = score
            scored.append(a)

        assert len(scored) == 3
        for a in scored:
            assert "renta_data" in a
            assert "score_data" in a
            assert 0 <= a["score_data"]["score_global"] <= 100


# ------------------------------------------------------------------
# Tests de structure des DAGs
# ------------------------------------------------------------------


class TestDAGStructure:
    """Teste la structure et les dependances des DAGs."""

    def test_dag_ventes_import(self) -> None:
        """Le DAG ventes s'importe sans erreur."""
        from dags import dag_ventes

        assert hasattr(dag_ventes, "dag")

    def test_dag_loyers_import(self) -> None:
        """Le DAG loyers s'importe sans erreur."""
        from dags import dag_loyers

        assert hasattr(dag_loyers, "dag")

    def test_dag_digest_import(self) -> None:
        """Le DAG digest s'importe sans erreur."""
        from dags import dag_digest

        assert hasattr(dag_digest, "dag")

    def test_dag_maintenance_import(self) -> None:
        """Le DAG maintenance s'importe sans erreur."""
        from dags import dag_maintenance

        assert hasattr(dag_maintenance, "dag")

    def test_dag_ventes_task_functions_exist(self) -> None:
        """Toutes les fonctions des taches du DAG ventes existent."""
        from dags import dag_ventes

        assert callable(dag_ventes.scrape_source)
        assert callable(dag_ventes.validate_listings)
        assert callable(dag_ventes.parse_normalize)
        assert callable(dag_ventes.dedup_listings)
        assert callable(dag_ventes.geocode_listings)
        assert callable(dag_ventes.compute_scores)
        assert callable(dag_ventes.enrich_ia)
        assert callable(dag_ventes.send_alerts)

    def test_dag_loyers_task_functions_exist(self) -> None:
        """Toutes les fonctions des taches du DAG loyers existent."""
        from dags import dag_loyers

        assert callable(dag_loyers.scrape_location_source)
        assert callable(dag_loyers.validate_loyers)
        assert callable(dag_loyers.parse_normalize_loyers)
        assert callable(dag_loyers.compute_medianes)

    def test_dag_digest_task_functions_exist(self) -> None:
        """Toutes les fonctions des taches du DAG digest existent."""
        from dags import dag_digest

        assert callable(dag_digest.query_top_annonces)
        assert callable(dag_digest.query_price_drops)
        assert callable(dag_digest.aggregate_pipeline_stats)
        assert callable(dag_digest.aggregate_observatory_stats)
        assert callable(dag_digest.send_digest)

    def test_dag_maintenance_task_functions_exist(self) -> None:
        """Toutes les fonctions des taches du DAG maintenance existent."""
        from dags import dag_maintenance

        assert callable(dag_maintenance.purge_expired_data)
        assert callable(dag_maintenance.vacuum_tables)
        assert callable(dag_maintenance.health_check)
        assert callable(dag_maintenance.verify_backup)

    def test_dag_ventes_sources(self) -> None:
        """Les sources du DAG ventes sont correctes."""
        from dags.dag_ventes import SOURCES_VENTE

        assert "leboncoin" in SOURCES_VENTE
        assert "pap" in SOURCES_VENTE


# ------------------------------------------------------------------
# Tests de soft fail (resilience)
# ------------------------------------------------------------------


class TestSoftFail:
    """Teste la resilience du pipeline quand une source echoue."""

    def test_validate_with_empty_source(self, mock_ti: MockTaskInstance) -> None:
        """Le pipeline continue meme si une source retourne une liste vide."""
        from dags.dag_ventes import validate_listings

        # Simuler : leboncoin retourne des donnees, pap retourne rien
        mock_ti.xcom_push(key="raw_leboncoin", value=[
            {
                "url_source": "https://www.leboncoin.fr/ventes/1.htm",
                "source": "leboncoin",
                "prix": 140000,
                "surface_m2": 50.0,
                "nb_pieces": 3,
                "adresse_brute": "Besancon",
            },
        ])
        mock_ti.xcom_push(key="raw_pap", value=[])

        # Patcher XCom pull pour mapper les task_ids
        original_pull = mock_ti.xcom_pull

        def patched_pull(key: str, task_ids: str | None = None) -> Any:
            return original_pull(key=key)

        mock_ti.xcom_pull = patched_pull

        with patch("dags.dag_ventes._log_validation_reject"):
            result = validate_listings(ti=mock_ti)

        # Le pipeline continue avec les donnees disponibles
        assert isinstance(result, list)
        assert len(result) == 1

    def test_validate_with_no_data(self, mock_ti: MockTaskInstance) -> None:
        """Le pipeline gere proprement le cas ou toutes les sources echouent."""
        from dags.dag_ventes import validate_listings

        mock_ti.xcom_push(key="raw_leboncoin", value=None)
        mock_ti.xcom_push(key="raw_pap", value=None)

        original_pull = mock_ti.xcom_pull

        def patched_pull(key: str, task_ids: str | None = None) -> Any:
            return original_pull(key=key)

        mock_ti.xcom_pull = patched_pull

        result = validate_listings(ti=mock_ti)
        assert result == []

    def test_normalize_with_empty_input(self, mock_ti: MockTaskInstance) -> None:
        """La normalisation gere proprement une liste vide."""
        from dags.dag_ventes import parse_normalize

        mock_ti.xcom_push(key="validated", value=[])

        original_pull = mock_ti.xcom_pull

        def patched_pull(key: str, task_ids: str | None = None) -> Any:
            return original_pull(key=key)

        mock_ti.xcom_pull = patched_pull

        result = parse_normalize(ti=mock_ti)
        assert result == []

    def test_dedup_with_empty_input(self, mock_ti: MockTaskInstance) -> None:
        """La deduplication gere proprement une liste vide."""
        from dags.dag_ventes import dedup_listings

        mock_ti.xcom_push(key="normalized", value=[])

        original_pull = mock_ti.xcom_pull

        def patched_pull(key: str, task_ids: str | None = None) -> Any:
            return original_pull(key=key)

        mock_ti.xcom_pull = patched_pull

        result = dedup_listings(ti=mock_ti)
        assert result == []

    def test_scoring_with_zero_prix(self) -> None:
        """Le scoring gere proprement un prix a zero."""
        from src.scoring.rentabilite import RentabiliteCalculator

        calc = RentabiliteCalculator()
        with pytest.raises(ValueError):
            calc.calculate(prix=0, loyer_mensuel=500)

    def test_enrichment_without_api_key(self, mock_ti: MockTaskInstance) -> None:
        """L'enrichissement est ignore si la cle API n'est pas configuree."""
        from dags.dag_ventes import enrich_ia

        mock_ti.xcom_push(key="scored", value=[{"url_source": "test", "prix": 140000}])

        original_pull = mock_ti.xcom_pull

        def patched_pull(key: str, task_ids: str | None = None) -> Any:
            return original_pull(key=key)

        mock_ti.xcom_pull = patched_pull

        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": ""}, clear=False):
            result = enrich_ia(ti=mock_ti)

        # Retourne les annonces non enrichies (pas de cle API)
        assert len(result) == 1


# ------------------------------------------------------------------
# Tests de la politique de retention
# ------------------------------------------------------------------


class TestRetentionPolicy:
    """Teste les calculs de la politique de retention."""

    def test_archive_retention_cutoff(self) -> None:
        """La date de coupure pour les annonces archivees est a 6 mois."""
        from dags.dag_maintenance import RETENTION_ARCHIVED_MONTHS

        assert RETENTION_ARCHIVED_MONTHS == 6
        cutoff = datetime.now() - timedelta(days=RETENTION_ARCHIVED_MONTHS * 30)
        assert cutoff < datetime.now()
        # 6 mois = environ 180 jours
        delta = datetime.now() - cutoff
        assert 175 <= delta.days <= 185

    def test_loyers_retention_cutoff(self) -> None:
        """La date de coupure pour les loyers est a 12 mois."""
        from dags.dag_maintenance import RETENTION_LOYERS_MONTHS

        assert RETENTION_LOYERS_MONTHS == 12
        cutoff = datetime.now() - timedelta(days=RETENTION_LOYERS_MONTHS * 30)
        delta = datetime.now() - cutoff
        assert 355 <= delta.days <= 365

    def test_scraping_log_retention(self) -> None:
        """La retention des logs de scraping est de 90 jours."""
        from dags.dag_maintenance import RETENTION_SCRAPING_LOG_DAYS

        assert RETENTION_SCRAPING_LOG_DAYS == 90

    def test_validation_log_retention(self) -> None:
        """La retention des logs de validation est de 30 jours."""
        from dags.dag_maintenance import RETENTION_VALIDATION_LOG_DAYS

        assert RETENTION_VALIDATION_LOG_DAYS == 30


# ------------------------------------------------------------------
# Tests du digest
# ------------------------------------------------------------------


class TestDigest:
    """Teste la generation du digest quotidien."""

    def test_format_digest_empty(self) -> None:
        """Le digest se genere correctement sans donnees."""
        from src.alerts.formatter import AlertFormatter

        formatter = AlertFormatter()
        digest = formatter.format_digest(
            top_annonces=[],
            baisses=[],
            stats={"nb_scrapees": 0, "nb_nouvelles": 0, "nb_erreurs": 0, "sources": []},
            obs_stats={"nb_locations": 0, "segments_couverts": 0, "fiabilite": "N/A"},
        )

        assert "DIGEST QUOTIDIEN" in digest
        assert "Aucune opportunite" in digest
        assert "Aucune baisse" in digest

    def test_format_digest_with_data(self) -> None:
        """Le digest se genere correctement avec des donnees."""
        from src.alerts.formatter import AlertFormatter

        formatter = AlertFormatter()

        top_annonces = [
            {
                "annonce": {
                    "prix": 145000,
                    "quartier": "Centre-Ville",
                    "url_source": "https://example.com/1",
                },
                "score": {"score_global": 85.0},
                "renta": {"renta_brute": 8.5},
            },
        ]

        baisses = [
            {
                "annonce": {"quartier": "Battant"},
                "ancien_prix": 150000,
                "nouveau_prix": 142000,
            },
        ]

        stats = {
            "nb_scrapees": 120,
            "nb_nouvelles": 15,
            "nb_erreurs": 2,
            "sources": ["leboncoin", "pap"],
        }

        obs_stats = {
            "nb_locations": 45,
            "segments_couverts": 8,
            "fiabilite": "67% fiable",
        }

        digest = formatter.format_digest(top_annonces, baisses, stats, obs_stats)

        assert "DIGEST QUOTIDIEN" in digest
        assert "Centre-Ville" in digest
        assert "Battant" in digest
        assert "120" in digest
        assert "15" in digest

    def test_format_top_alert(self) -> None:
        """Le formatage d'alerte TOP fonctionne correctement."""
        from src.alerts.formatter import AlertFormatter

        formatter = AlertFormatter()

        annonce = {
            "prix": 145000,
            "surface_m2": 55.0,
            "nb_pieces": 3,
            "dpe": "C",
            "adresse_brute": "12 Rue de la Republique, Besancon",
            "quartier": "Centre-Ville",
            "url_source": "https://example.com/1",
        }

        score = {"score_global": 85.5}
        renta = {
            "renta_brute": 8.5,
            "renta_brute_nego_5": 8.9,
            "renta_brute_nego_10": 9.4,
            "renta_brute_nego_15": 10.0,
        }

        message = formatter.format_top_alert(annonce, score, renta)
        assert "TOP OPPORTUNITE" in message
        assert "Centre-Ville" in message

    def test_format_baisse_prix(self) -> None:
        """Le formatage d'alerte baisse de prix fonctionne correctement."""
        from src.alerts.formatter import AlertFormatter

        formatter = AlertFormatter()

        annonce = {
            "adresse_brute": "5 Rue Battant",
            "quartier": "Battant",
            "surface_m2": 42.0,
            "nb_pieces": 2,
            "url_source": "https://example.com/2",
        }

        message = formatter.format_baisse_prix(annonce, 150000, 140000)
        assert "BAISSE DE PRIX" in message
        assert "Battant" in message


# ------------------------------------------------------------------
# Tests des taches de maintenance
# ------------------------------------------------------------------


class TestMaintenanceTasks:
    """Teste les fonctions de maintenance (sans DB)."""

    def test_backup_verification_missing_dir(self) -> None:
        """La verification de backup detecte un repertoire manquant."""
        from dags.dag_maintenance import verify_backup

        with patch("dags.dag_maintenance.BACKUP_DIR", "/nonexistent/path"):
            result = verify_backup(ti=MockTaskInstance())

        assert result["status"] == "critical"
        assert "inexistant" in result["message"]

    def test_health_check_monitor(self) -> None:
        """Le moniteur de sante genere un rapport valide."""
        from src.monitoring.health import HealthMonitor

        monitor = HealthMonitor()

        report = monitor.generate_health_report(
            scraping_logs=[],
            sources=["leboncoin", "pap"],
        )

        assert report["status"] in ("ok", "warning", "critical")
        assert "checks" in report
        assert "summary" in report
        assert "timestamp" in report

    def test_health_check_source_failure_detection(self) -> None:
        """Le moniteur detecte les echecs consecutifs d'une source."""
        from src.monitoring.health import HealthMonitor

        monitor = HealthMonitor()

        # Simuler 3 echecs consecutifs
        failing_logs = [
            {"source": "leboncoin", "nb_erreurs": 1, "nb_annonces_scrapees": 0},
            {"source": "leboncoin", "nb_erreurs": 1, "nb_annonces_scrapees": 0},
            {"source": "leboncoin", "nb_erreurs": 1, "nb_annonces_scrapees": 0},
        ]

        result = monitor.check_source_health("leboncoin", failing_logs)
        assert result["status"] == "critical"
        assert result["details"]["consecutive_failures"] >= 3

    def test_disk_space_check(self) -> None:
        """La verification d'espace disque retourne un resultat valide."""
        from src.monitoring.health import HealthMonitor

        monitor = HealthMonitor()
        result = monitor.check_disk_space("/")

        assert result["status"] in ("ok", "warning", "critical")
        assert "free_gb" in result["details"]
        assert result["details"]["free_gb"] >= 0

    def test_api_budget_check(self) -> None:
        """Le check budget API detecte les differents seuils."""
        from src.monitoring.health import HealthMonitor

        monitor = HealthMonitor()

        # Budget normal
        result = monitor.check_api_budget(100, 300)
        assert result["status"] == "ok"

        # Budget eleve (>80%)
        result = monitor.check_api_budget(250, 300)
        assert result["status"] == "warning"

        # Budget atteint
        result = monitor.check_api_budget(300, 300)
        assert result["status"] == "critical"

    def test_retention_constants_coherence(self) -> None:
        """Les constantes de retention sont coherentes avec CLAUDE.md."""
        from dags.dag_maintenance import (
            RETENTION_ARCHIVED_MONTHS,
            RETENTION_LOYERS_MONTHS,
            RETENTION_SCRAPING_LOG_DAYS,
            RETENTION_VALIDATION_LOG_DAYS,
        )

        # Conformite avec la specification CLAUDE.md
        assert RETENTION_ARCHIVED_MONTHS == 6
        assert RETENTION_LOYERS_MONTHS == 12
        assert RETENTION_SCRAPING_LOG_DAYS == 90
        assert RETENTION_VALIDATION_LOG_DAYS == 30

        # Ordres de grandeur coherents
        assert RETENTION_VALIDATION_LOG_DAYS < RETENTION_SCRAPING_LOG_DAYS
        assert RETENTION_SCRAPING_LOG_DAYS < RETENTION_ARCHIVED_MONTHS * 30
        assert RETENTION_ARCHIVED_MONTHS < RETENTION_LOYERS_MONTHS


# ------------------------------------------------------------------
# Tests de l'observatoire des loyers (pipeline location)
# ------------------------------------------------------------------


class TestObservatoirePipeline:
    """Teste le pipeline de l'observatoire des loyers."""

    def test_validate_location_listings(
        self,
        sample_raw_location_listings: list[dict[str, Any]],
    ) -> None:
        """Les annonces de location valides passent la validation."""
        from src.validation.validators import AnnonceValidator

        validator = AnnonceValidator()
        for annonce in sample_raw_location_listings:
            is_valid, reasons = validator.validate_location(annonce)
            assert is_valid, f"Annonce location rejetee: {reasons}"

    def test_normalize_location(
        self,
        sample_raw_location_listings: list[dict[str, Any]],
    ) -> None:
        """La normalisation des locations produit des donnees exploitables."""
        from src.parsers.normalizer import AnnonceNormalizer

        normalizer = AnnonceNormalizer()
        for raw in sample_raw_location_listings:
            result = normalizer.normalize_location(raw)
            assert result["loyer_cc"] is not None
            assert result["surface_m2"] is not None
            assert result["nb_pieces"] is not None
            assert result["completude_score"] >= 0.0

    def test_compute_medianes(
        self,
        sample_raw_location_listings: list[dict[str, Any]],
    ) -> None:
        """Le calcul de medianes produit des resultats pour les segments alimentes."""
        from src.observatoire.loyers import ObservatoireLoyers

        observatoire = ObservatoireLoyers(quartiers_config={"quartiers": {}})

        result = observatoire.compute_medianes(
            loyers=sample_raw_location_listings,
            quartier="Centre-Ville",
            type_bien="T2",
            meuble=False,
        )

        # Au moins une annonce correspond au segment
        assert result["quartier"] == "Centre-Ville"
        assert result["type_bien"] == "T2"
        assert result["meuble"] is False

    def test_estimate_loyer_fallback(self) -> None:
        """L'estimation de loyer utilise le fallback si pas de reference."""
        from src.observatoire.loyers import ObservatoireLoyers

        observatoire = ObservatoireLoyers(quartiers_config={"quartiers": {}})

        result = observatoire.estimate_loyer(
            quartier="Centre-Ville",
            type_bien="T2",
            meuble=False,
            surface=42.0,
            references=None,
        )

        # Sans reference ni fallback valide, le loyer est indisponible
        assert result["fiabilite"] in ("indisponible", "preliminaire")


# ------------------------------------------------------------------
# Tests d'alert levels
# ------------------------------------------------------------------


class TestAlertLevels:
    """Teste la classification des niveaux d'alerte."""

    def test_top_alert_by_score(self) -> None:
        """Un score >= 80 declenche une alerte TOP."""
        from src.scoring.composite import CompositeScorer

        scorer = CompositeScorer(scoring_config={})
        level = scorer.determine_alert_level(
            score=85.0, renta_brute=5.0, renta_brute_nego_best=6.0
        )
        assert level == "top"

    def test_top_alert_by_renta(self) -> None:
        """Une renta >= 8% au prix affiche declenche une alerte TOP."""
        from src.scoring.composite import CompositeScorer

        scorer = CompositeScorer(scoring_config={})
        level = scorer.determine_alert_level(
            score=50.0, renta_brute=8.5, renta_brute_nego_best=9.0
        )
        assert level == "top"

    def test_bon_alert_by_score(self) -> None:
        """Un score 60-79 declenche une alerte BON."""
        from src.scoring.composite import CompositeScorer

        scorer = CompositeScorer(scoring_config={})
        level = scorer.determine_alert_level(
            score=70.0, renta_brute=5.0, renta_brute_nego_best=6.0
        )
        assert level == "bon"

    def test_bon_alert_by_nego_renta(self) -> None:
        """Une renta >= 8% apres nego declenche une alerte BON."""
        from src.scoring.composite import CompositeScorer

        scorer = CompositeScorer(scoring_config={})
        level = scorer.determine_alert_level(
            score=50.0, renta_brute=6.0, renta_brute_nego_best=8.5
        )
        assert level == "bon"

    def test_veille_alert(self) -> None:
        """Un score < 60 et renta < 8% donne VEILLE."""
        from src.scoring.composite import CompositeScorer

        scorer = CompositeScorer(scoring_config={})
        level = scorer.determine_alert_level(
            score=40.0, renta_brute=5.0, renta_brute_nego_best=6.0
        )
        assert level == "veille"


# ------------------------------------------------------------------
# Tests des XCom dans le pipeline DAG
# ------------------------------------------------------------------


class TestXComFlow:
    """Teste le flux XCom entre les taches du pipeline."""

    def test_scrape_pushes_xcom(self, mock_ti: MockTaskInstance) -> None:
        """La tache scrape pousse les resultats dans XCom."""
        from dags.dag_ventes import scrape_source

        # Mock le scraper via le import path used inside the function
        with patch(
            "src.scrapers.leboncoin.LeBonCoinScraper",
            side_effect=Exception("mock"),
        ):
            scrape_source(source_name="leboncoin", ti=mock_ti)

        # Verifie que XCom a ete pousse (meme vide en cas d'erreur)
        result = mock_ti.xcom_pull(key="raw_leboncoin")
        assert isinstance(result, list)

    def test_validate_pulls_and_pushes_xcom(
        self,
        mock_ti: MockTaskInstance,
        sample_raw_vente_listings: list[dict[str, Any]],
    ) -> None:
        """La tache validate tire les donnees et pousse les resultats."""
        from dags.dag_ventes import validate_listings

        # Simuler les donnees de scraping
        mock_ti.xcom_push(key="raw_leboncoin", value=sample_raw_vente_listings[:2])
        mock_ti.xcom_push(key="raw_pap", value=sample_raw_vente_listings[2:])

        original_pull = mock_ti.xcom_pull

        def patched_pull(key: str, task_ids: str | None = None) -> Any:
            return original_pull(key=key)

        mock_ti.xcom_pull = patched_pull

        with patch("dags.dag_ventes._log_validation_reject"):
            result = validate_listings(ti=mock_ti)

        assert len(result) == 3
        assert mock_ti.xcom_pull(key="validated") is not None

    def test_normalize_pulls_and_pushes_xcom(
        self,
        mock_ti: MockTaskInstance,
        sample_raw_vente_listings: list[dict[str, Any]],
    ) -> None:
        """La tache normalize tire les donnees et pousse les resultats."""
        from dags.dag_ventes import parse_normalize

        mock_ti.xcom_push(key="validated", value=sample_raw_vente_listings)

        original_pull = mock_ti.xcom_pull

        def patched_pull(key: str, task_ids: str | None = None) -> Any:
            return original_pull(key=key)

        mock_ti.xcom_pull = patched_pull

        result = parse_normalize(ti=mock_ti)

        assert len(result) == 3
        assert mock_ti.xcom_pull(key="normalized") is not None
