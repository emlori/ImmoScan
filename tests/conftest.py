"""Fixtures pytest partagees pour les tests ImmoScan."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml


FIXTURES_DIR = Path(__file__).parent / "fixtures"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"


@pytest.fixture
def fixtures_dir() -> Path:
    """Retourne le chemin vers le dossier de fixtures HTML.

    Returns:
        Chemin absolu vers tests/fixtures/.
    """
    return FIXTURES_DIR


@pytest.fixture
def sample_annonce_vente() -> dict[str, Any]:
    """Retourne une annonce de vente fictive pour les tests.

    Returns:
        Dictionnaire representant une annonce de vente type.
    """
    return {
        "url_source": "https://www.leboncoin.fr/ventes_immobilieres/1234567890.htm",
        "source": "leboncoin",
        "prix": 140000,
        "surface_m2": 55.0,
        "nb_pieces": 3,
        "dpe": "C",
        "etage": 2,
        "adresse_brute": "12 rue de la Republique, 25000 Besancon",
        "quartier": "Centre-Ville",
        "description_texte": "Bel appartement T3 lumineux, proche centre-ville.",
        "photos_urls": [
            "https://img.leboncoin.fr/ad-image/photo1.jpg",
            "https://img.leboncoin.fr/ad-image/photo2.jpg",
        ],
        "charges_copro": 120.0,
    }


@pytest.fixture
def sample_annonce_location() -> dict[str, Any]:
    """Retourne une annonce de location fictive pour les tests.

    Returns:
        Dictionnaire representant une annonce de location type.
    """
    return {
        "url_source": "https://www.leboncoin.fr/locations/9876543210.htm",
        "source": "leboncoin",
        "loyer_cc": 550.0,
        "loyer_hc": 480.0,
        "surface_m2": 45.0,
        "nb_pieces": 2,
        "meuble": False,
        "quartier": "Battant",
        "dpe": "B",
    }


@pytest.fixture
def scoring_config() -> dict[str, Any]:
    """Charge la configuration de scoring depuis le fichier YAML.

    Returns:
        Dictionnaire avec la configuration de scoring.
    """
    config_path = CONFIG_DIR / "scoring.yaml"
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


@pytest.fixture
def quartiers_config() -> dict[str, Any]:
    """Charge la configuration des quartiers depuis le fichier YAML.

    Returns:
        Dictionnaire avec la configuration des quartiers.
    """
    config_path = CONFIG_DIR / "quartiers.yaml"
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


@pytest.fixture
def sources_config() -> dict[str, Any]:
    """Charge la configuration des sources depuis le fichier YAML.

    Returns:
        Dictionnaire avec la configuration des sources.
    """
    config_path = CONFIG_DIR / "sources.yaml"
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f)
