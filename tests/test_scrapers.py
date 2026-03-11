"""Tests unitaires pour la couche scraping ImmoScan.

Teste les classes BaseScraper, LeBonCoinScraper, PAPScraper et SeLogerScraper
sans acces reseau, en utilisant des mocks et des fixtures HTML.
"""

from __future__ import annotations

import hashlib
import time
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, PropertyMock, patch
from urllib.parse import urlparse

import pytest

from src.scrapers.base import BaseScraper, RobotsChecker
from src.scrapers.leboncoin import LeBonCoinScraper
from src.scrapers.pap import PAPScraper
from src.scrapers.seloger import SeLogerScraper

FIXTURES_DIR = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_response(
    html_path: Path | None = None,
    html_content: str | None = None,
    status: int = 200,
    url: str = "https://example.com",
) -> MagicMock:
    """Cree un objet Response mock compatible avec l'API Scrapling.

    Utilise scrapling.Selector pour un vrai parsing HTML, et enrobe
    le resultat dans un MagicMock qui expose .status, .url, .text,
    .css() et .css_first() (ce dernier n'existant pas nativement
    dans Selector, il est ajoute comme raccourci).

    Args:
        html_path: Chemin vers un fichier HTML fixture.
        html_content: Contenu HTML brut (prioritaire sur html_path).
        status: Code HTTP de la reponse.
        url: URL associee a la reponse.

    Returns:
        MagicMock imitant une reponse Scrapling avec methodes css/css_first.
    """
    if html_content is None and html_path is not None:
        html_content = html_path.read_text(encoding="utf-8")
    elif html_content is None:
        html_content = "<html><body></body></html>"

    from scrapling import Selector

    page = Selector(html_content)

    def css_first(selector: str) -> Any:
        """Retourne le premier element CSS matching ou None."""
        results = page.css(selector)
        if results and len(results) > 0:
            return results[0]
        return None

    # Patcher css_first sur les elements enfants aussi
    def patched_css(selector: str) -> Any:
        """CSS avec support css_first sur chaque element retourne."""
        results = page.css(selector)
        _patch_elements(results)
        return results

    mock_resp = MagicMock()
    mock_resp.status = status
    mock_resp.url = url
    mock_resp.text = html_content
    mock_resp.css = patched_css
    mock_resp.css_first = css_first
    return mock_resp


def _patch_elements(elements: Any) -> None:
    """Ajoute css_first aux elements Scrapling Selector qui n'ont pas cette methode.

    Modifie les elements in-place pour que card.css_first(sel) fonctionne.

    Args:
        elements: Liste d'elements Scrapling Selector.
    """
    if elements is None:
        return
    for el in elements:
        if not hasattr(el, "css_first") or not callable(getattr(el, "css_first", None)):
            def make_css_first(element: Any) -> Any:
                def css_first(selector: str) -> Any:
                    results = element.css(selector)
                    _patch_elements(results)
                    if results and len(results) > 0:
                        return results[0]
                    return None
                return css_first
            el.css_first = make_css_first(el)


def _make_mock_settings() -> MagicMock:
    """Cree un mock de Settings charge avec la vraie configuration sources.yaml.

    Returns:
        MagicMock de Settings avec load_sources() fonctionnel.
    """
    import yaml

    config_path = Path(__file__).resolve().parent.parent / "config" / "sources.yaml"
    with open(config_path, encoding="utf-8") as f:
        sources_config = yaml.safe_load(f)

    settings = MagicMock()
    settings.load_sources.return_value = sources_config
    settings.scraping.delay_min = 2
    settings.scraping.delay_max = 5
    return settings


def _make_mock_fetcher() -> MagicMock:
    """Cree un mock de StealthyFetcher.

    Returns:
        MagicMock de StealthyFetcher avec fetch() retournant un 200 par defaut.
    """
    fetcher = MagicMock()
    # Par defaut, robots.txt retourne un 200 vide
    robots_response = MagicMock()
    robots_response.status = 200
    robots_response.text = "User-agent: *\nAllow: /\n"
    fetcher.fetch.return_value = robots_response
    return fetcher


# ---------------------------------------------------------------------------
# Tests BaseScraper (abstrait)
# ---------------------------------------------------------------------------


class TestBaseScraper:
    """Tests pour la classe abstraite BaseScraper."""

    def test_cannot_instantiate_directly(self) -> None:
        """Verifie que BaseScraper ne peut pas etre instancie directement."""
        with pytest.raises(TypeError, match="abstract"):
            BaseScraper(source_name="leboncoin")  # type: ignore[abstract]

    def test_unknown_source_raises_value_error(self) -> None:
        """Verifie qu'une source inconnue leve ValueError."""

        class DummyScraper(BaseScraper):
            def _get_search_url(self, t: str) -> str:
                return ""

            def _parse_listing_page(self, r: Any, t: str) -> list:
                return []

            def _parse_detail_page(self, r: Any, t: str) -> dict:
                return {}

            def _get_next_page_url(self, r: Any, p: int) -> str | None:
                return None

        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        with pytest.raises(ValueError, match="introuvable"):
            DummyScraper(
                source_name="source_inexistante",
                settings=settings,
                fetcher=fetcher,
            )

    def test_disabled_source_raises_value_error(self) -> None:
        """Verifie qu'une source desactivee leve ValueError."""

        class DummyScraper(BaseScraper):
            def _get_search_url(self, t: str) -> str:
                return ""

            def _parse_listing_page(self, r: Any, t: str) -> list:
                return []

            def _parse_detail_page(self, r: Any, t: str) -> dict:
                return {}

            def _get_next_page_url(self, r: Any, p: int) -> str | None:
                return None

        settings = _make_mock_settings()
        # Desactiver leboncoin dans la config
        config = settings.load_sources()
        config["sources"]["leboncoin"]["enabled"] = False
        settings.load_sources.return_value = config

        fetcher = _make_mock_fetcher()
        with pytest.raises(ValueError, match="desactivee"):
            DummyScraper(
                source_name="leboncoin",
                settings=settings,
                fetcher=fetcher,
            )

    def test_generate_hash_dedup_consistency(self) -> None:
        """Verifie que le hash dedup est deterministe pour une meme URL."""
        url = "https://www.leboncoin.fr/ventes_immobilieres/1234567890.htm"
        hash1 = BaseScraper.generate_hash_dedup(url)
        hash2 = BaseScraper.generate_hash_dedup(url)
        assert hash1 == hash2
        assert len(hash1) == 64  # SHA256 hex digest

    def test_generate_hash_dedup_ignores_fragment(self) -> None:
        """Verifie que le hash dedup ignore les fragments d'URL."""
        url1 = "https://www.leboncoin.fr/ventes_immobilieres/123.htm"
        url2 = "https://www.leboncoin.fr/ventes_immobilieres/123.htm#section"
        assert BaseScraper.generate_hash_dedup(url1) == BaseScraper.generate_hash_dedup(url2)

    def test_generate_hash_dedup_ignores_query_params(self) -> None:
        """Verifie que le hash dedup ignore les parametres de requete."""
        url1 = "https://www.leboncoin.fr/ventes_immobilieres/123.htm"
        url2 = "https://www.leboncoin.fr/ventes_immobilieres/123.htm?utm_source=test"
        assert BaseScraper.generate_hash_dedup(url1) == BaseScraper.generate_hash_dedup(url2)

    def test_generate_hash_dedup_different_urls(self) -> None:
        """Verifie que deux URLs differentes produisent des hash differents."""
        url1 = "https://www.leboncoin.fr/ventes_immobilieres/111.htm"
        url2 = "https://www.leboncoin.fr/ventes_immobilieres/222.htm"
        assert BaseScraper.generate_hash_dedup(url1) != BaseScraper.generate_hash_dedup(url2)

    def test_generate_hash_dedup_is_sha256(self) -> None:
        """Verifie que le hash est bien un SHA256 de l'URL canonique."""
        url = "https://www.leboncoin.fr/ventes_immobilieres/123.htm"
        expected = hashlib.sha256(url.encode("utf-8")).hexdigest()
        assert BaseScraper.generate_hash_dedup(url) == expected


class TestRateLimiting:
    """Tests pour le rate limiting du BaseScraper."""

    def test_rate_limit_applies_delay(self) -> None:
        """Verifie que le rate limiting applique un delai entre les requetes."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = LeBonCoinScraper(settings=settings, fetcher=fetcher)

        # Simuler une premiere requete
        scraper._last_request_time = time.time()

        # Mesurer le temps avec un delai minimal
        scraper._delay_min = 0.1
        scraper._delay_max = 0.2

        start = time.time()
        scraper._rate_limit()
        elapsed = time.time() - start

        # Le delai doit etre entre 0 et 0.2s (une partie peut avoir deja expire)
        assert elapsed < 0.3

    def test_rate_limit_no_delay_first_request(self) -> None:
        """Verifie que la premiere requete n'a pas de delai."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = LeBonCoinScraper(settings=settings, fetcher=fetcher)

        # Pas de derniere requete
        assert scraper._last_request_time == 0.0

        start = time.time()
        scraper._rate_limit()
        elapsed = time.time() - start

        # Pas de delai pour la premiere requete
        assert elapsed < 0.1


class TestRobotsChecker:
    """Tests pour le verifieur robots.txt."""

    def test_parse_robots_allows_path(self) -> None:
        """Verifie que les chemins autorises sont acceptes."""
        checker = RobotsChecker("https://www.example.com")
        checker._parse_robots("User-agent: *\nDisallow: /admin/\nAllow: /\n")
        checker._checked = True

        assert checker.is_allowed("https://www.example.com/recherche?q=test")
        assert not checker.is_allowed("https://www.example.com/admin/config")

    def test_parse_robots_disallow(self) -> None:
        """Verifie que les chemins interdits sont bloques."""
        checker = RobotsChecker("https://www.example.com")
        checker._parse_robots(
            "User-agent: *\nDisallow: /private/\nDisallow: /api/\n"
        )
        checker._checked = True

        assert not checker.is_allowed("https://www.example.com/private/data")
        assert not checker.is_allowed("https://www.example.com/api/v1/list")
        assert checker.is_allowed("https://www.example.com/public/page")

    def test_unchecked_robots_allows_all(self) -> None:
        """Verifie que sans verification, tout est autorise."""
        checker = RobotsChecker("https://www.example.com")
        # _checked est False par defaut
        assert checker.is_allowed("https://www.example.com/anything")

    def test_check_success(self) -> None:
        """Verifie le chargement reussi de robots.txt."""
        fetcher = MagicMock()
        response = MagicMock()
        response.status = 200
        response.text = "User-agent: *\nAllow: /\n"
        fetcher.fetch.return_value = response

        checker = RobotsChecker("https://www.example.com")
        result = checker.check(fetcher)

        assert result is True
        assert checker._checked is True

    def test_check_failure(self) -> None:
        """Verifie la gestion d'un robots.txt inaccessible."""
        fetcher = MagicMock()
        response = MagicMock()
        response.status = 404
        fetcher.fetch.return_value = response

        checker = RobotsChecker("https://www.example.com")
        result = checker.check(fetcher)

        assert result is False
        assert checker._checked is True  # Marque quand meme comme verifie

    def test_check_connection_error(self) -> None:
        """Verifie la gestion d'une erreur de connexion lors du check."""
        fetcher = MagicMock()
        fetcher.fetch.side_effect = ConnectionError("Timeout")

        checker = RobotsChecker("https://www.example.com")
        result = checker.check(fetcher)

        assert result is False
        assert checker._checked is True


# ---------------------------------------------------------------------------
# Tests LeBonCoinScraper
# ---------------------------------------------------------------------------


class TestLeBonCoinScraper:
    """Tests pour le scraper LeBonCoin."""

    def test_init_success(self) -> None:
        """Verifie l'initialisation correcte du scraper."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = LeBonCoinScraper(settings=settings, fetcher=fetcher)

        assert scraper.source_name == "leboncoin"
        assert scraper._delay_min == 2
        assert scraper._delay_max == 5
        assert scraper._max_pages == 5

    def test_get_search_url_vente(self) -> None:
        """Verifie la construction de l'URL de recherche pour les ventes."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = LeBonCoinScraper(settings=settings, fetcher=fetcher)

        url = scraper._get_search_url("vente")
        assert "leboncoin.fr" in url
        assert "category=9" in url  # Ventes immobilieres
        assert "Besan" in url
        assert "120000-160000" in url

    def test_get_search_url_location(self) -> None:
        """Verifie la construction de l'URL de recherche pour les locations."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = LeBonCoinScraper(settings=settings, fetcher=fetcher)

        url = scraper._get_search_url("location")
        assert "leboncoin.fr" in url
        assert "category=10" in url  # Locations

    def test_get_search_url_unknown_type(self) -> None:
        """Verifie qu'un type inconnu retourne une chaine vide."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = LeBonCoinScraper(settings=settings, fetcher=fetcher)

        url = scraper._get_search_url("commercial")
        assert url == ""

    def test_parse_listing_page(self) -> None:
        """Verifie le parsing d'une page de listing LeBonCoin."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = LeBonCoinScraper(settings=settings, fetcher=fetcher)

        response = _make_mock_response(
            html_path=FIXTURES_DIR / "leboncoin_listing.html",
            url="https://www.leboncoin.fr/recherche?category=9",
        )

        listings = scraper._parse_listing_page(response, "vente")

        assert len(listings) == 3

        # Premiere annonce
        first = listings[0]
        assert "url_source" in first
        assert "2345678901" in first["url_source"]
        assert first["prix"] == 145000
        assert first["surface_m2"] == 62.0
        assert first["nb_pieces"] == 3
        assert "Centre-Ville" in first["adresse_brute"]

        # Deuxieme annonce
        second = listings[1]
        assert second["prix"] == 128000
        assert second["surface_m2"] == 42.0
        assert second["nb_pieces"] == 2

        # Troisieme annonce
        third = listings[2]
        assert third["prix"] == 155000
        assert third["surface_m2"] == 68.0

    def test_parse_detail_page(self) -> None:
        """Verifie le parsing d'une page de detail LeBonCoin."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = LeBonCoinScraper(settings=settings, fetcher=fetcher)

        response = _make_mock_response(
            html_path=FIXTURES_DIR / "leboncoin_detail.html",
            url="https://www.leboncoin.fr/ventes_immobilieres/2345678901.htm",
        )

        data = scraper._parse_detail_page(response, "vente")

        assert "description_texte" in data
        assert "lumineux" in data["description_texte"]
        assert data["dpe"] == "C"
        assert len(data.get("photos_urls", [])) == 3
        assert data.get("etage") == 3
        assert data.get("charges_copro") == 150.0
        assert data.get("date_publication") == "2026-03-08T10:30:00"

    def test_parse_listing_page_empty(self) -> None:
        """Verifie le parsing d'une page vide."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = LeBonCoinScraper(settings=settings, fetcher=fetcher)

        response = _make_mock_response(
            html_content="<html><body><div>Aucun resultat</div></body></html>"
        )
        listings = scraper._parse_listing_page(response, "vente")
        assert listings == []

    def test_add_page_param(self) -> None:
        """Verifie l'ajout du parametre de page a une URL."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = LeBonCoinScraper(settings=settings, fetcher=fetcher)

        url = "https://www.leboncoin.fr/recherche?category=9&locations=Besancon"
        paged = scraper._add_page_param(url, 3)

        assert "page=3" in paged
        assert "category=9" in paged

    def test_parse_price_various_formats(self) -> None:
        """Verifie le parsing de prix dans differents formats."""
        assert LeBonCoinScraper._parse_price("145 000 \u20ac") == 145000
        assert LeBonCoinScraper._parse_price("128000\u20ac") == 128000
        assert LeBonCoinScraper._parse_price("155 000 euros") == 155000
        assert LeBonCoinScraper._parse_price("") is None
        assert LeBonCoinScraper._parse_price("gratuit") is None

    def test_parse_surface_various_formats(self) -> None:
        """Verifie le parsing de surfaces dans differents formats."""
        assert LeBonCoinScraper._parse_surface("62 m\u00b2") == 62.0
        assert LeBonCoinScraper._parse_surface("42m2") == 42.0
        assert LeBonCoinScraper._parse_surface("55,5 m\u00b2") == 55.5
        assert LeBonCoinScraper._parse_surface("") is None

    def test_parse_rooms_various_formats(self) -> None:
        """Verifie le parsing du nombre de pieces."""
        assert LeBonCoinScraper._parse_rooms("3 pi\u00e8ces") == 3
        assert LeBonCoinScraper._parse_rooms("2p") == 2
        assert LeBonCoinScraper._parse_rooms("T3") == 3
        assert LeBonCoinScraper._parse_rooms("") is None

    def test_parse_dpe(self) -> None:
        """Verifie le parsing du DPE."""
        assert LeBonCoinScraper._parse_dpe("C") == "C"
        assert LeBonCoinScraper._parse_dpe("Classe c") == "C"
        assert LeBonCoinScraper._parse_dpe("DPE : B") == "B"
        assert LeBonCoinScraper._parse_dpe("") is None
        assert LeBonCoinScraper._parse_dpe("123") is None


# ---------------------------------------------------------------------------
# Tests PAPScraper
# ---------------------------------------------------------------------------


class TestPAPScraper:
    """Tests pour le scraper PAP."""

    def test_init_success(self) -> None:
        """Verifie l'initialisation correcte du scraper."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = PAPScraper(settings=settings, fetcher=fetcher)

        assert scraper.source_name == "pap"
        assert scraper._delay_min == 3
        assert scraper._delay_max == 6
        assert scraper._max_pages == 3

    def test_get_search_url_vente(self) -> None:
        """Verifie la construction de l'URL de recherche PAP pour les ventes."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = PAPScraper(settings=settings, fetcher=fetcher)

        url = scraper._get_search_url("vente")
        assert "pap.fr" in url
        assert "vente" in url
        assert "besancon" in url

    def test_get_search_url_location(self) -> None:
        """Verifie la construction de l'URL PAP pour les locations."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = PAPScraper(settings=settings, fetcher=fetcher)

        url = scraper._get_search_url("location")
        assert "pap.fr" in url
        assert "location" in url

    def test_parse_listing_page(self) -> None:
        """Verifie le parsing d'une page de listing PAP."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = PAPScraper(settings=settings, fetcher=fetcher)

        response = _make_mock_response(
            html_path=FIXTURES_DIR / "pap_listing.html",
            url="https://www.pap.fr/annonce/vente-appartement-besancon-25000",
        )

        listings = scraper._parse_listing_page(response, "vente")

        assert len(listings) == 3

        # Premiere annonce
        first = listings[0]
        assert "url_source" in first
        assert "r100001" in first["url_source"]
        assert first["prix"] == 139000
        assert first["surface_m2"] == 58.0
        assert first["nb_pieces"] == 3

        # Deuxieme annonce
        second = listings[1]
        assert second["prix"] == 125000
        assert second["nb_pieces"] == 2

    def test_parse_detail_page(self) -> None:
        """Verifie le parsing d'une page de detail PAP."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = PAPScraper(settings=settings, fetcher=fetcher)

        response = _make_mock_response(
            html_path=FIXTURES_DIR / "pap_detail.html",
            url="https://www.pap.fr/annonces/appartement-besancon-25000-r100001",
        )

        data = scraper._parse_detail_page(response, "vente")

        assert "description_texte" in data
        assert "58m" in data["description_texte"] or "T3" in data["description_texte"]
        assert data.get("dpe") == "B"
        assert len(data.get("photos_urls", [])) == 2
        assert data.get("etage") == 2
        assert data.get("charges_copro") == 95.0
        assert "date_publication" in data

    def test_parse_listing_page_empty(self) -> None:
        """Verifie le parsing d'une page vide PAP."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = PAPScraper(settings=settings, fetcher=fetcher)

        response = _make_mock_response(
            html_content="<html><body></body></html>"
        )
        listings = scraper._parse_listing_page(response, "vente")
        assert listings == []

    def test_parse_price_formats(self) -> None:
        """Verifie le parsing de prix PAP."""
        assert PAPScraper._parse_price("139 000 \u20ac") == 139000
        assert PAPScraper._parse_price("125000\u20ac") == 125000
        assert PAPScraper._parse_price("") is None

    def test_next_page_url_construction(self) -> None:
        """Verifie la construction de l'URL de page suivante PAP."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = PAPScraper(settings=settings, fetcher=fetcher)

        response = MagicMock()
        response.url = "https://www.pap.fr/annonce/vente-appartement-besancon-25000"
        response.css_first.return_value = None  # Pas de lien "suivant"

        next_url = scraper._get_next_page_url(response, 1)
        assert next_url is not None
        assert "-page-2" in next_url

    def test_next_page_url_max_pages(self) -> None:
        """Verifie l'arret a la page max PAP."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = PAPScraper(settings=settings, fetcher=fetcher)

        response = MagicMock()
        response.url = "https://www.pap.fr/annonce/vente"
        response.css_first.return_value = None

        # max_pages est 3 pour PAP
        next_url = scraper._get_next_page_url(response, 3)
        assert next_url is None


# ---------------------------------------------------------------------------
# Tests SeLogerScraper
# ---------------------------------------------------------------------------


class TestSeLogerScraper:
    """Tests pour le scraper SeLoger."""

    def test_init_success(self) -> None:
        """Verifie l'initialisation correcte du scraper."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = SeLogerScraper(settings=settings, fetcher=fetcher)

        assert scraper.source_name == "seloger"
        assert scraper._delay_min == 3
        assert scraper._delay_max == 7
        assert scraper._max_pages == 3

    def test_get_search_url_location(self) -> None:
        """Verifie la construction de l'URL SeLoger pour les locations."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = SeLogerScraper(settings=settings, fetcher=fetcher)

        url = scraper._get_search_url("location")
        assert "seloger.com" in url
        assert "projects=1" in url  # Location
        assert "250056" in url  # Code commune Besancon

    def test_get_search_url_vente_returns_empty(self) -> None:
        """Verifie que la vente retourne une chaine vide en v1."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = SeLogerScraper(settings=settings, fetcher=fetcher)

        url = scraper._get_search_url("vente")
        assert url == ""

    def test_only_location_type_supported(self) -> None:
        """Verifie que seul le type 'location' est supporte."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = SeLogerScraper(settings=settings, fetcher=fetcher)

        # Le scrape complet doit gerer gracieusement un type non supporte
        results = scraper.scrape("vente")
        assert results == []

    def test_parse_listing_page_empty(self) -> None:
        """Verifie le parsing d'une page vide SeLoger."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = SeLogerScraper(settings=settings, fetcher=fetcher)

        response = _make_mock_response(
            html_content="<html><body></body></html>"
        )
        listings = scraper._parse_listing_page(response, "location")
        assert listings == []

    def test_pagination_param(self) -> None:
        """Verifie le parametre de pagination SeLoger."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = SeLogerScraper(settings=settings, fetcher=fetcher)

        assert scraper._pagination_param == "LISTING-LISTpg"

    def test_add_page_param(self) -> None:
        """Verifie l'ajout du parametre de pagination SeLoger."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = SeLogerScraper(settings=settings, fetcher=fetcher)

        url = "https://www.seloger.com/list.htm?projects=1&types=1"
        paged = scraper._add_page_param(url, 2)

        assert "LISTING-LISTpg=2" in paged
        assert "projects=1" in paged

    def test_next_page_url_max_pages(self) -> None:
        """Verifie l'arret a la page max SeLoger."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = SeLogerScraper(settings=settings, fetcher=fetcher)

        response = MagicMock()
        response.url = "https://www.seloger.com/list.htm"
        response.css_first.return_value = None

        next_url = scraper._get_next_page_url(response, 3)
        assert next_url is None

    def test_parse_float_formats(self) -> None:
        """Verifie le parsing de nombres decimaux SeLoger."""
        assert SeLogerScraper._parse_float("550 \u20ac") == 550.0
        assert SeLogerScraper._parse_float("1 200,50 \u20ac") == 1200.50
        assert SeLogerScraper._parse_float("") is None

    def test_parse_dpe(self) -> None:
        """Verifie le parsing du DPE SeLoger."""
        assert SeLogerScraper._parse_dpe("Classe A") == "A"
        assert SeLogerScraper._parse_dpe("D") == "D"
        assert SeLogerScraper._parse_dpe("") is None


# ---------------------------------------------------------------------------
# Tests d'erreurs et robustesse
# ---------------------------------------------------------------------------


class TestErrorHandling:
    """Tests de gestion d'erreurs et de robustesse."""

    def test_fetch_page_connection_error(self) -> None:
        """Verifie la gestion d'une erreur de connexion lors du fetch."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = LeBonCoinScraper(settings=settings, fetcher=fetcher)

        # Configurer le fetcher pour lever une exception
        fetcher.fetch.side_effect = ConnectionError("Network unreachable")

        # Desactiver le rate limiting pour le test
        scraper._last_request_time = 0.0
        scraper._delay_min = 0
        scraper._delay_max = 0

        # Le global config definit les retries
        scraper.global_config["max_retries"] = 1
        scraper.global_config["retry_delay"] = 0

        result = scraper._fetch_page("https://www.leboncoin.fr/recherche")
        assert result is None
        assert scraper._nb_erreurs == 1

    def test_fetch_page_server_error(self) -> None:
        """Verifie la gestion d'une erreur serveur 500."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = LeBonCoinScraper(settings=settings, fetcher=fetcher)

        error_response = MagicMock()
        error_response.status = 500
        fetcher.fetch.return_value = error_response

        scraper._delay_min = 0
        scraper._delay_max = 0
        scraper.global_config["max_retries"] = 2
        scraper.global_config["retry_delay"] = 0

        result = scraper._fetch_page("https://www.leboncoin.fr/recherche")
        assert result is None
        assert scraper._nb_erreurs == 1

    def test_fetch_page_rate_limited(self) -> None:
        """Verifie la gestion du rate limiting HTTP 429."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = LeBonCoinScraper(settings=settings, fetcher=fetcher)

        # Premiere requete : 429, deuxieme : 200
        rate_limited = MagicMock()
        rate_limited.status = 429
        success = MagicMock()
        success.status = 200
        fetcher.fetch.side_effect = [rate_limited, success]

        scraper._delay_min = 0
        scraper._delay_max = 0
        scraper.global_config["max_retries"] = 2
        scraper.global_config["retry_delay"] = 0

        result = scraper._fetch_page("https://www.leboncoin.fr/recherche")
        assert result is not None
        assert result.status == 200

    def test_fetch_page_robots_blocked(self) -> None:
        """Verifie le blocage par robots.txt."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = LeBonCoinScraper(settings=settings, fetcher=fetcher)

        # Configurer robots.txt pour bloquer
        scraper.robots_checker._disallowed_paths = ["/recherche"]
        scraper.robots_checker._checked = True

        result = scraper._fetch_page("https://www.leboncoin.fr/recherche?q=test")
        assert result is None

    def test_scrape_unsupported_type(self) -> None:
        """Verifie que le scraping d'un type non supporte retourne une liste vide."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = LeBonCoinScraper(settings=settings, fetcher=fetcher)

        results = scraper.scrape("commercial")
        assert results == []

    def test_scraping_stats(self) -> None:
        """Verifie les statistiques de scraping."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = LeBonCoinScraper(settings=settings, fetcher=fetcher)

        scraper._nb_scrapees = 15
        scraper._nb_erreurs = 2

        stats = scraper.get_scraping_stats()
        assert stats["source"] == "leboncoin"
        assert stats["nb_annonces_scrapees"] == 15
        assert stats["nb_erreurs"] == 2

    def test_scrape_empty_listing_stops_pagination(self) -> None:
        """Verifie que la pagination s'arrete si une page est vide."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = LeBonCoinScraper(settings=settings, fetcher=fetcher)

        # Mock : la page de listing ne contient aucune annonce
        empty_response = _make_mock_response(
            html_content="<html><body></body></html>",
            url="https://www.leboncoin.fr/recherche",
        )
        fetcher.fetch.return_value = empty_response

        scraper._delay_min = 0
        scraper._delay_max = 0

        results = scraper.scrape("vente")
        assert results == []


# ---------------------------------------------------------------------------
# Tests d'integration du workflow scrape()
# ---------------------------------------------------------------------------


class TestScrapeWorkflow:
    """Tests du workflow complet de scraping."""

    def test_full_scrape_workflow_leboncoin(self) -> None:
        """Verifie le workflow complet de scraping LeBonCoin avec mocks."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = LeBonCoinScraper(settings=settings, fetcher=fetcher)

        # Desactiver les delais
        scraper._delay_min = 0
        scraper._delay_max = 0
        scraper._max_pages = 1

        # Mock robots.txt
        robots_resp = MagicMock()
        robots_resp.status = 200
        robots_resp.text = "User-agent: *\nAllow: /\n"

        # Mock listing page
        listing_resp = _make_mock_response(
            html_path=FIXTURES_DIR / "leboncoin_listing.html",
            url="https://www.leboncoin.fr/recherche?category=9",
        )

        # Mock detail pages
        detail_resp = _make_mock_response(
            html_path=FIXTURES_DIR / "leboncoin_detail.html",
            url="https://www.leboncoin.fr/ventes_immobilieres/2345678901.htm",
        )

        # Sequencer les appels fetch :
        # 1. robots.txt
        # 2. listing page
        # 3-5. detail pages (3 annonces)
        fetcher.fetch.side_effect = [
            robots_resp,
            listing_resp,
            detail_resp,
            detail_resp,
            detail_resp,
        ]

        results = scraper.scrape("vente")

        assert len(results) == 3
        for r in results:
            assert r["source"] == "leboncoin"
            assert "hash_dedup" in r
            assert len(r["hash_dedup"]) == 64
            assert "date_scrape" in r

    def test_full_scrape_workflow_pap(self) -> None:
        """Verifie le workflow complet de scraping PAP avec mocks."""
        settings = _make_mock_settings()
        fetcher = _make_mock_fetcher()
        scraper = PAPScraper(settings=settings, fetcher=fetcher)

        scraper._delay_min = 0
        scraper._delay_max = 0
        scraper._max_pages = 1

        robots_resp = MagicMock()
        robots_resp.status = 200
        robots_resp.text = "User-agent: *\nAllow: /\n"

        listing_resp = _make_mock_response(
            html_path=FIXTURES_DIR / "pap_listing.html",
            url="https://www.pap.fr/annonce/vente-appartement-besancon",
        )

        detail_resp = _make_mock_response(
            html_path=FIXTURES_DIR / "pap_detail.html",
            url="https://www.pap.fr/annonces/appartement-besancon-25000-r100001",
        )

        fetcher.fetch.side_effect = [
            robots_resp,
            listing_resp,
            detail_resp,
            detail_resp,
            detail_resp,
        ]

        results = scraper.scrape("vente")

        assert len(results) == 3
        for r in results:
            assert r["source"] == "pap"
            assert "hash_dedup" in r
