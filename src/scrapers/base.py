"""Classe abstraite de base pour tous les scrapers ImmoScan.

Fournit l'infrastructure commune : gestion du rate limiting, rotation de proxies,
verification de robots.txt, pagination, logging et integration avec ScrapingLog.

Chaque source (LeBonCoin, PAP, SeLoger) doit heriter de BaseScraper
et implementer les methodes abstraites.
"""

from __future__ import annotations

import hashlib
import logging
import random
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any
from urllib.parse import urljoin, urlparse

from src.config import Settings, get_settings

logger = logging.getLogger(__name__)


class RobotsChecker:
    """Verifie la conformite avec robots.txt pour une source donnee.

    Effectue une verification simple en chargeant le fichier robots.txt
    du site cible et en verifiant les regles Disallow.

    Attributes:
        base_url: URL de base du site a verifier.
        _disallowed_paths: Liste des chemins interdits par robots.txt.
        _checked: Indique si la verification a deja ete effectuee.
    """

    def __init__(self, base_url: str) -> None:
        """Initialise le checker robots.txt.

        Args:
            base_url: URL de base du site (ex: 'https://www.leboncoin.fr').
        """
        self.base_url = base_url
        self._disallowed_paths: list[str] = []
        self._checked: bool = False

    def check(self, fetcher: Any, proxy: dict[str, str] | str | None = None) -> bool:
        """Charge et parse le fichier robots.txt.

        Args:
            fetcher: Instance StealthyFetcher pour effectuer la requete.
            proxy: Proxy a utiliser pour la requete.

        Returns:
            True si le fichier a ete charge avec succes, False sinon.
        """
        robots_url = urljoin(self.base_url, "/robots.txt")
        try:
            fetch_kwargs: dict[str, Any] = {}
            if proxy:
                fetch_kwargs["proxy"] = proxy
            response = fetcher.fetch(robots_url, **fetch_kwargs)
            if response.status == 200:
                self._parse_robots(response.text)
                self._checked = True
                logger.info("robots.txt charge avec succes pour %s", self.base_url)
                return True
            logger.warning(
                "robots.txt non accessible pour %s (status %d)",
                self.base_url,
                response.status,
            )
        except Exception:
            logger.warning(
                "Erreur lors du chargement de robots.txt pour %s",
                self.base_url,
                exc_info=True,
            )
        self._checked = True
        return False

    def _parse_robots(self, content: str) -> None:
        """Parse le contenu de robots.txt et extrait les regles Disallow.

        Ne prend en compte que les regles pour User-agent: * (agent generique).

        Args:
            content: Contenu textuel du fichier robots.txt.
        """
        applies_to_all = False
        for line in content.splitlines():
            line = line.strip()
            if line.lower().startswith("user-agent:"):
                agent = line.split(":", 1)[1].strip()
                applies_to_all = agent == "*"
            elif applies_to_all and line.lower().startswith("disallow:"):
                path = line.split(":", 1)[1].strip()
                if path:
                    self._disallowed_paths.append(path)

    def is_allowed(self, url: str) -> bool:
        """Verifie si une URL est autorisee par robots.txt.

        Args:
            url: URL complete a verifier.

        Returns:
            True si l'URL est autorisee (ou si robots.txt n'a pas ete charge).
        """
        if not self._checked:
            return True
        parsed = urlparse(url)
        path = parsed.path
        for disallowed in self._disallowed_paths:
            if path.startswith(disallowed):
                logger.warning("URL bloquee par robots.txt : %s", url)
                return False
        return True


class BaseScraper(ABC):
    """Classe abstraite de base pour les scrapers d'annonces immobilieres.

    Fournit l'infrastructure commune a tous les scrapers :
    - Rate limiting configurable (delai 2-5s entre requetes)
    - Rotation de proxies
    - Verification de robots.txt au demarrage
    - Gestion de la pagination
    - Logging structure
    - Integration avec le modele ScrapingLog pour le monitoring

    Chaque sous-classe doit implementer les methodes abstraites pour adapter
    le scraping a la structure specifique de chaque source.

    Attributes:
        source_name: Nom de la source ('leboncoin', 'pap', 'seloger').
        source_config: Configuration specifique a la source depuis sources.yaml.
        global_config: Configuration globale du scraping.
        settings: Parametres de l'application.
        fetcher: Instance StealthyFetcher pour les requetes HTTP.
        robots_checker: Verifieur de conformite robots.txt.
    """

    def __init__(
        self,
        source_name: str,
        settings: Settings | None = None,
        fetcher: Any | None = None,
    ) -> None:
        """Initialise le scraper de base.

        Charge la configuration de la source depuis sources.yaml,
        initialise le fetcher Scrapling et verifie robots.txt.

        Args:
            source_name: Nom de la source ('leboncoin', 'pap', 'seloger').
            settings: Instance Settings (charge les parametres par defaut si None).
            fetcher: Instance StealthyFetcher (cree une nouvelle instance si None).
                     Accepte Any pour permettre l'injection de mocks en test.

        Raises:
            ValueError: Si la source n'est pas trouvee dans sources.yaml
                ou si elle est desactivee.
        """
        self.source_name = source_name
        self.settings = settings or get_settings()
        self._sources_config = self.settings.load_sources()

        # Charger la configuration specifique a la source
        sources = self._sources_config.get("sources", {})
        if source_name not in sources:
            raise ValueError(f"Source '{source_name}' introuvable dans sources.yaml")

        self.source_config: dict[str, Any] = sources[source_name]
        if not self.source_config.get("enabled", False):
            raise ValueError(f"Source '{source_name}' est desactivee dans sources.yaml")

        self.global_config: dict[str, Any] = self._sources_config.get("global", {})

        # Configuration du rate limiting
        rate_limit = self.source_config.get("rate_limit", {})
        self._delay_min: int = rate_limit.get(
            "delay_min", self.settings.scraping.delay_min
        )
        self._delay_max: int = rate_limit.get(
            "delay_max", self.settings.scraping.delay_max
        )

        # Pagination
        pagination = self.source_config.get("pagination", {})
        self._pagination_param: str = pagination.get("param", "page")
        self._max_pages: int = pagination.get("max_pages", 5)

        # Selecteurs CSS
        self.selectors: dict[str, str] = self.source_config.get("selectors", {})

        # Fetcher Scrapling (import paresseux pour eviter les dependances lourdes en test)
        if fetcher is not None:
            self.fetcher = fetcher
        else:
            try:
                from scrapling import StealthyFetcher

                self.fetcher = StealthyFetcher()
            except (ImportError, ModuleNotFoundError) as e:
                logger.warning(
                    "StealthyFetcher indisponible (%s), "
                    "les sous-classes doivent fournir leur propre fetching",
                    e,
                )
                self.fetcher = None

        # Configuration du proxy
        self._proxy: dict[str, str] | str | None = None
        if self.settings.proxy.enabled and self.settings.proxy.pool_url:
            self._proxy = self.settings.proxy.pool_url
            logger.info(
                "Proxy active pour '%s': %s",
                self.source_name,
                self._proxy.split("@")[-1] if "@" in self._proxy else "configure",
            )

        # robots.txt checker
        base_urls = self.source_config.get("base_urls", {})
        first_url = next(iter(base_urls.values()), "")
        parsed = urlparse(first_url)
        self._base_domain = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme else ""
        self.robots_checker = RobotsChecker(self._base_domain)

        # Horodatage de la derniere requete (pour rate limiting)
        self._last_request_time: float = 0.0

        # Compteurs pour le monitoring
        self._nb_scrapees: int = 0
        self._nb_erreurs: int = 0

        logger.info(
            "Scraper '%s' initialise (delay: %d-%ds, max_pages: %d)",
            self.source_name,
            self._delay_min,
            self._delay_max,
            self._max_pages,
        )

    def check_robots_txt(self) -> bool:
        """Verifie la conformite avec robots.txt au demarrage.

        Doit etre appele avant le debut du scraping. Si la configuration
        globale l'exige, charge et parse le fichier robots.txt.

        Returns:
            True si la verification a reussi ou si elle est desactivee.
        """
        if not self.global_config.get("respect_robots_txt", True):
            logger.info("Verification robots.txt desactivee pour '%s'", self.source_name)
            return True
        if self.fetcher is None:
            logger.warning(
                "Fetcher indisponible, robots.txt non verifie pour '%s'",
                self.source_name,
            )
            return True
        return self.robots_checker.check(self.fetcher, proxy=self._proxy)

    def _rate_limit(self) -> None:
        """Applique le delai de rate limiting entre deux requetes.

        Calcule un delai aleatoire entre delay_min et delay_max,
        et attend le temps restant depuis la derniere requete.
        """
        if self._last_request_time > 0:
            elapsed = time.time() - self._last_request_time
            target_delay = random.uniform(self._delay_min, self._delay_max)
            if elapsed < target_delay:
                wait_time = target_delay - elapsed
                logger.debug(
                    "Rate limiting: attente de %.1fs avant la prochaine requete",
                    wait_time,
                )
                time.sleep(wait_time)
        self._last_request_time = time.time()

    def _fetch_page(self, url: str) -> Any | None:
        """Effectue une requete HTTP avec rate limiting et gestion d'erreurs.

        Verifie la conformite robots.txt, applique le rate limiting,
        puis effectue la requete via StealthyFetcher.

        Args:
            url: URL a recuperer.

        Returns:
            Objet Response de Scrapling, ou None en cas d'erreur.
        """
        # Verification robots.txt
        if not self.robots_checker.is_allowed(url):
            logger.warning("URL bloquee par robots.txt, ignoree: %s", url)
            return None

        # Rate limiting
        self._rate_limit()

        max_retries = self.global_config.get("max_retries", 3)
        retry_delay = self.global_config.get("retry_delay", 10)

        for attempt in range(1, max_retries + 1):
            try:
                logger.info("Requete [%d/%d] : %s", attempt, max_retries, url)
                fetch_kwargs: dict[str, Any] = {}
                if self._proxy:
                    fetch_kwargs["proxy"] = self._proxy
                response = self.fetcher.fetch(url, **fetch_kwargs)

                if response.status == 200 and response.text:
                    logger.debug("Reponse OK (200) pour %s", url)
                    return response
                elif response.status == 200 and not response.text:
                    logger.warning(
                        "Reponse vide (200) pour %s, tentative curl_cffi...",
                        url,
                    )
                    fallback = self._fetch_with_curl_cffi(url)
                    if fallback is not None:
                        return fallback
                    self._nb_erreurs += 1
                    return None
                elif response.status == 403:
                    logger.warning(
                        "Bloque (403) par StealthyFetcher pour %s, "
                        "tentative curl_cffi...",
                        url,
                    )
                    fallback = self._fetch_with_curl_cffi(url)
                    if fallback is not None:
                        return fallback
                    self._nb_erreurs += 1
                    return None
                elif response.status == 429:
                    logger.warning(
                        "Rate limited (429) pour %s, attente %ds",
                        url,
                        retry_delay * attempt,
                    )
                    time.sleep(retry_delay * attempt)
                elif response.status >= 500:
                    logger.warning(
                        "Erreur serveur (%d) pour %s, retry %d/%d",
                        response.status,
                        url,
                        attempt,
                        max_retries,
                    )
                    time.sleep(retry_delay)
                else:
                    logger.error(
                        "Reponse inattendue (%d) pour %s",
                        response.status,
                        url,
                    )
                    self._nb_erreurs += 1
                    return None
            except Exception:
                logger.error(
                    "Erreur de connexion pour %s (tentative %d/%d)",
                    url,
                    attempt,
                    max_retries,
                    exc_info=True,
                )
                if attempt < max_retries:
                    time.sleep(retry_delay)

        logger.error("Echec apres %d tentatives pour %s", max_retries, url)
        self._nb_erreurs += 1
        return None

    @staticmethod
    def _patch_css_first(element: Any) -> Any:
        """Ajoute css_first() a un element Scrapling Selector si absent.

        Patche recursivement : la methode css() renverra aussi des elements patches.

        Args:
            element: Element Selector de Scrapling.

        Returns:
            L'element patche avec css_first().
        """
        if hasattr(element, "css_first"):
            return element

        original_css = element.css

        def _patched_css(selector: str) -> list[Any]:
            results = original_css(selector)
            return [BaseScraper._patch_css_first(r) for r in results]

        def _css_first(selector: str) -> Any | None:
            results = original_css(selector)
            if results:
                return BaseScraper._patch_css_first(results[0])
            return None

        element.css = _patched_css
        element.css_first = _css_first
        return element

    def _make_adaptor_response(
        self, html: str, url: str, status: int = 200
    ) -> Any:
        """Cree un objet reponse compatible Scrapling depuis du HTML brut.

        Utile pour construire une reponse uniforme quel que soit le mode
        de fetching (curl_cffi, Camoufox, etc.).

        Args:
            html: Contenu HTML de la page.
            url: URL d'origine de la page.
            status: Code HTTP de la reponse.

        Returns:
            Objet Adaptor avec .status, .text, .css(), .css_first(), .url.
        """
        from scrapling.parser import Adaptor

        adaptor = Adaptor(html, url=url)
        adaptor.status = status
        self._patch_css_first(adaptor)
        return adaptor

    def _fetch_with_curl_cffi(self, url: str) -> Any | None:
        """Fallback HTTP via curl_cffi avec impersonation TLS Chrome.

        Utilise quand StealthyFetcher est bloque (403) par des protections
        anti-bot comme DataDome. curl_cffi reproduit le fingerprint TLS
        d'un vrai navigateur Chrome.

        Args:
            url: URL a recuperer.

        Returns:
            Objet compatible avec l'API Scrapling Response, ou None en cas d'echec.
        """
        try:
            from curl_cffi import requests as curl_requests

            proxies = None
            if self._proxy:
                proxies = {"http": self._proxy, "https": self._proxy}

            response = curl_requests.get(
                url,
                impersonate="chrome",
                proxies=proxies,
                timeout=self.global_config.get("timeout", 30),
            )

            if response.status_code == 200 and response.text:
                logger.info(
                    "curl_cffi OK (200) pour %s (%d octets)",
                    url,
                    len(response.text),
                )
                return self._make_adaptor_response(response.text, str(response.url))

            logger.warning(
                "curl_cffi echec (%d) pour %s",
                response.status_code,
                url,
            )
        except ImportError:
            logger.error("curl_cffi non installe, fallback impossible")
        except Exception:
            logger.error(
                "Erreur curl_cffi pour %s", url, exc_info=True
            )
        return None

    @staticmethod
    def generate_hash_dedup(url: str) -> str:
        """Genere un hash SHA256 pour la deduplication intra-source.

        Utilise l'URL canonique (sans parametres de tracking) comme cle.

        Args:
            url: URL de l'annonce a hasher.

        Returns:
            Hash SHA256 hexadecimal de l'URL canonique.
        """
        # Normaliser l'URL : retirer le fragment et les params de tracking
        parsed = urlparse(url)
        canonical = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def scrape(self, scrape_type: str) -> list[dict[str, Any]]:
        """Methode principale de scraping pour un type donne.

        Orchestre le processus complet : verification robots.txt,
        iteration sur les pages de resultats, parsing des listings
        et des pages de detail.

        Args:
            scrape_type: Type de scraping ('vente' ou 'location').

        Returns:
            Liste de dictionnaires contenant les donnees brutes des annonces.
        """
        start_time = time.time()
        self._nb_scrapees = 0
        self._nb_erreurs = 0
        results: list[dict[str, Any]] = []

        logger.info(
            "Demarrage scraping '%s' type '%s'",
            self.source_name,
            scrape_type,
        )

        # Verification robots.txt au demarrage
        self.check_robots_txt()

        # Verifier que le type est supporte
        supported_types = self.source_config.get("types", [])
        if scrape_type not in supported_types:
            logger.error(
                "Type '%s' non supporte par la source '%s' (types: %s)",
                scrape_type,
                self.source_name,
                supported_types,
            )
            return results

        # Construction de l'URL de recherche initiale
        search_url = self._get_search_url(scrape_type)
        if not search_url:
            logger.error("URL de recherche vide pour '%s'", self.source_name)
            return results

        # Pagination
        current_url: str | None = search_url
        page = 1

        while current_url and page <= self._max_pages:
            logger.info(
                "Scraping page %d/%d : %s",
                page,
                self._max_pages,
                current_url,
            )

            response = self._fetch_page(current_url)
            if response is None:
                logger.warning("Page %d inaccessible, arret de la pagination", page)
                break

            # Parser la page de listing
            try:
                listings = self._parse_listing_page(response, scrape_type)
                logger.info(
                    "Page %d : %d annonces trouvees",
                    page,
                    len(listings),
                )
            except Exception:
                logger.error(
                    "Erreur parsing page listing %d",
                    page,
                    exc_info=True,
                )
                self._nb_erreurs += 1
                listings = []

            if not listings:
                logger.info("Aucune annonce trouvee sur la page %d, arret", page)
                break

            # Parser chaque annonce en detail
            for listing in listings:
                detail_url = listing.get("url_source")
                if not detail_url:
                    continue

                try:
                    if self._should_fetch_detail(listing):
                        detail_response = self._fetch_page(detail_url)
                        if detail_response is not None:
                            detail_data = self._parse_detail_page(
                                detail_response, scrape_type
                            )
                            merged = {**listing, **detail_data}
                        else:
                            self._nb_erreurs += 1
                            continue
                    else:
                        # Listing deja complet (ex: JSON), pas de fetch detail
                        merged = listing

                    merged["source"] = self.source_name
                    merged["hash_dedup"] = self.generate_hash_dedup(detail_url)
                    merged["date_scrape"] = datetime.now().isoformat()
                    results.append(merged)
                    self._nb_scrapees += 1
                except Exception:
                    logger.error(
                        "Erreur parsing detail pour %s",
                        detail_url,
                        exc_info=True,
                    )
                    self._nb_erreurs += 1

            # Page suivante
            current_url = self._get_next_page_url(response, page)
            page += 1

        duree = time.time() - start_time
        logger.info(
            "Scraping '%s' type '%s' termine : %d annonces, %d erreurs, %.1fs",
            self.source_name,
            scrape_type,
            self._nb_scrapees,
            self._nb_erreurs,
            duree,
        )

        return results

    def get_scraping_stats(self) -> dict[str, Any]:
        """Retourne les statistiques de la derniere session de scraping.

        Returns:
            Dictionnaire compatible avec le modele ScrapingLog.
        """
        return {
            "source": self.source_name,
            "nb_annonces_scrapees": self._nb_scrapees,
            "nb_erreurs": self._nb_erreurs,
        }

    @abstractmethod
    def _get_search_url(self, scrape_type: str) -> str:
        """Construit l'URL de recherche initiale pour un type donne.

        Args:
            scrape_type: Type de scraping ('vente' ou 'location').

        Returns:
            URL de recherche complete.
        """
        ...

    @abstractmethod
    def _parse_listing_page(
        self, response: Any, scrape_type: str
    ) -> list[dict[str, Any]]:
        """Parse une page de resultats de recherche.

        Extrait les informations de base de chaque annonce visible
        sur la page de listing (titre, prix, surface, lien, etc.).

        Args:
            response: Reponse Scrapling contenant la page HTML.
            scrape_type: Type de scraping ('vente' ou 'location').

        Returns:
            Liste de dictionnaires avec les donnees de base de chaque annonce.
        """
        ...

    @abstractmethod
    def _parse_detail_page(
        self, response: Any, scrape_type: str
    ) -> dict[str, Any]:
        """Parse une page de detail d'annonce.

        Extrait les informations detaillees : description complete,
        DPE, etage, charges, photos, date de publication, etc.

        Args:
            response: Reponse Scrapling contenant la page HTML de detail.
            scrape_type: Type de scraping ('vente' ou 'location').

        Returns:
            Dictionnaire avec les donnees detaillees de l'annonce.
        """
        ...

    def _should_fetch_detail(self, listing: dict[str, Any]) -> bool:
        """Determine si la page de detail doit etre fetche pour une annonce.

        Par defaut, retourne True. Les sous-classes peuvent overrider
        pour eviter les requetes inutiles quand les donnees sont deja
        completes (ex: extraction JSON).

        Args:
            listing: Donnees de l'annonce extraites du listing.

        Returns:
            True s'il faut fetcher la page de detail.
        """
        return True

    @abstractmethod
    def _get_next_page_url(self, response: Any, current_page: int) -> str | None:
        """Determine l'URL de la page suivante dans la pagination.

        Args:
            response: Reponse Scrapling de la page courante.
            current_page: Numero de la page courante (commence a 1).

        Returns:
            URL de la page suivante, ou None s'il n'y a plus de pages.
        """
        ...
