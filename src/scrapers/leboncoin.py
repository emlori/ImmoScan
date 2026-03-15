"""Scraper LeBonCoin multi-couches avec contournement DataDome.

Architecture de fetching par ordre de priorite :
- Couche 0 : API Finder LeBonCoin (POST api.leboncoin.fr/finder/search)
- Couche 1 : Session curl_cffi avec warm-up et headers realistes
- Couche 2 : Resolution du cookie DataDome via Camoufox + injection curl_cffi
- Couche 3 : Fallback Camoufox complet avec humanize

La Couche 0 (API) est privilegiee car elle retourne du JSON structure
directement, sans HTML a parser et sans protection DataDome sur les pages
de recherche. Les couches 1-3 servent de fallback si l'API est indisponible.
"""

from __future__ import annotations

import json
import logging
import random
import re
import time
from datetime import datetime
from enum import IntEnum
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from src.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)


class FetchLayer(IntEnum):
    """Couches de fetching par ordre de priorite (legere a lourde)."""

    FINDER_API = 0
    CURL_SESSION = 1
    DATADOME_RESOLVE = 2
    CAMOUFOX_FULL = 3


class LeBonCoinScraper(BaseScraper):
    """Scraper multi-couches pour LeBonCoin avec contournement DataDome.

    Architecture de fetching :
    0. API Finder (POST api.leboncoin.fr/finder/search) — JSON direct, pas de DataDome
    1. curl_cffi session avec warm-up (headers realistes, Sec-Fetch-*, Referer)
    2. Resolution cookie DataDome via Camoufox + reinjection en curl_cffi
    3. Fallback Camoufox complet avec humanize

    La couche 0 est la methode principale. Elle retourne des donnees JSON
    completes (prix, surface, DPE, coords GPS, photos, description, etc.)
    sans avoir besoin de parser du HTML ni de contourner DataDome.

    Attributes:
        source_name: Toujours 'leboncoin'.
    """

    # API Finder LeBonCoin
    _API_URL: str = "https://api.leboncoin.fr/finder/search"
    _API_KEY: str = "ba0c2dad52b3ec"
    _API_LIMIT: int = 35

    # Mapping des types de scrape vers les categories API LeBonCoin
    _CATEGORY_IDS: dict[str, str] = {
        "vente": "9",
        "location": "10",
    }

    # Versions d'impersonation curl_cffi a tester par ordre de preference
    _IMPERSONATE_VERSIONS: list[str] = ["chrome124", "chrome", "safari"]

    # Headers communs simulant une navigation Chrome reelle
    _COMMON_HEADERS: dict[str, str] = {
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,image/apng,*/*;q=0.8"
        ),
        "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "max-age=0",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

    # Headers pour les requetes API
    _API_HEADERS: dict[str, str] = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Accept-Language": "fr-FR,fr;q=0.9",
        "Origin": "https://www.leboncoin.fr",
        "Referer": "https://www.leboncoin.fr/",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
    }

    # Champs requis pour considerer un listing JSON comme complet
    _REQUIRED_DETAIL_FIELDS: set[str] = {
        "url_source",
        "titre",
        "prix",
        "surface_m2",
        "nb_pieces",
        "adresse_brute",
    }

    def __init__(self, **kwargs: Any) -> None:
        """Initialise le scraper LeBonCoin multi-couches.

        Args:
            **kwargs: Arguments transmis a BaseScraper (settings, fetcher).
        """
        super().__init__(source_name="leboncoin", **kwargs)

        # Etat de la session curl_cffi
        self._curl_session: Any | None = None
        self._session_warmed: bool = False
        self._datadome_cookie: str | None = None
        self._current_impersonate: str = self._IMPERSONATE_VERSIONS[0]
        self._active_layer: FetchLayer = FetchLayer.FINDER_API

        # Statistiques par couche de fetching
        self._layer_attempts: dict[FetchLayer, int] = {l: 0 for l in FetchLayer}
        self._layer_successes: dict[FetchLayer, int] = {l: 0 for l in FetchLayer}

        # Flag indiquant que le dernier listing a ete extrait via JSON
        self._last_listing_from_json: bool = False

    # ------------------------------------------------------------------
    # Couche 0 : API Finder LeBonCoin (methode principale)
    # ------------------------------------------------------------------

    def _build_api_payload(
        self, scrape_type: str, pivot: str | None = None
    ) -> dict[str, Any]:
        """Construit le payload JSON pour l'API Finder LeBonCoin.

        Args:
            scrape_type: 'vente' ou 'location'.
            pivot: Curseur de pagination (retourne par l'API dans la reponse precedente).

        Returns:
            Dictionnaire JSON pour le POST vers /finder/search.
        """
        category_id = self._CATEGORY_IDS.get(scrape_type, "9")

        filters: dict[str, Any] = {
            "category": {"id": category_id},
            "enums": {
                "real_estate_type": ["2"],  # Appartements
                "ad_type": ["offer"],
            },
            "location": {
                "locations": [
                    {
                        "city": "Besançon",
                        "zipcode": "25000",
                        "label": "Besançon (25000)",
                    }
                ],
            },
        }

        # Filtres de prix et pieces selon le type
        ranges: dict[str, dict[str, int]] = {
            "rooms": {"min": 1, "max": 3},
        }
        if scrape_type == "vente":
            ranges["price"] = {"min": 30000, "max": 160000}
        filters["ranges"] = ranges

        payload: dict[str, Any] = {
            "limit": self._API_LIMIT,
            "limit_alu": 0,
            "filters": filters,
            "sort_by": "time",
            "sort_order": "desc",
        }

        if pivot:
            payload["pivot"] = pivot

        return payload

    def _search_via_api(
        self, scrape_type: str
    ) -> list[dict[str, Any]] | None:
        """Execute une recherche complete via l'API Finder avec pagination.

        Retourne toutes les annonces disponibles en paginant automatiquement
        via le curseur pivot. Respecte le rate limiting entre les pages.

        Args:
            scrape_type: 'vente' ou 'location'.

        Returns:
            Liste de dictionnaires avec les donnees completes par annonce,
            ou None si l'API est indisponible.
        """
        self._layer_attempts[FetchLayer.FINDER_API] += 1

        session = self._get_curl_session()
        proxies = self._get_proxies()
        timeout = self.global_config.get("timeout", 30)

        # Warm-up necessaire pour etablir les cookies de session
        if not self._warm_up_session():
            logger.warning("Warm-up echec, API Finder potentiellement impactee")

        all_listings: list[dict[str, Any]] = []
        pivot: str | None = None
        page = 1

        while page <= self._max_pages:
            if page > 1:
                self._rate_limit()

            payload = self._build_api_payload(scrape_type, pivot)

            logger.info(
                "API Finder [page %d/%d] : %s (limit=%d)",
                page,
                self._max_pages,
                scrape_type,
                self._API_LIMIT,
            )

            try:
                resp = session.post(
                    self._API_URL,
                    json=payload,
                    proxies=proxies,
                    timeout=timeout,
                    headers={
                        **self._API_HEADERS,
                        "api_key": self._API_KEY,
                    },
                )

                if resp.status_code != 200:
                    logger.warning(
                        "API Finder echec (status %d) page %d",
                        resp.status_code,
                        page,
                    )
                    if page == 1:
                        return None
                    break

                data = resp.json()
                ads = data.get("ads", [])

                if not ads:
                    logger.info("API Finder : plus d'annonces a la page %d", page)
                    break

                # Parser chaque annonce
                for ad in ads:
                    try:
                        listing = self._parse_json_ad(ad, scrape_type)
                        if listing:
                            all_listings.append(listing)
                    except Exception:
                        logger.debug("Erreur parsing annonce API", exc_info=True)

                logger.info(
                    "API Finder page %d : %d annonces parsees (total API: %s)",
                    page,
                    len(ads),
                    data.get("total", "?"),
                )

                # Pagination
                pivot = data.get("pivot")
                max_api_pages = data.get("max_pages", self._max_pages)
                if not pivot or page >= min(self._max_pages, max_api_pages):
                    break

                page += 1

            except Exception:
                logger.error(
                    "Erreur API Finder page %d", page, exc_info=True
                )
                if page == 1:
                    return None
                break

        if all_listings:
            self._layer_successes[FetchLayer.FINDER_API] += 1
            logger.info(
                "API Finder OK : %d annonces totales pour '%s'",
                len(all_listings),
                scrape_type,
            )

        return all_listings if all_listings else None

    def scrape(self, scrape_type: str) -> list[dict[str, Any]]:
        """Scrape les annonces LeBonCoin via API Finder (prioritaire) ou HTML.

        Tente d'abord l'API Finder qui retourne du JSON directement.
        Si l'API echoue, bascule sur le scraping HTML classique
        (couches 1-3 avec __NEXT_DATA__ ou selecteurs CSS).

        Args:
            scrape_type: 'vente' ou 'location'.

        Returns:
            Liste de dictionnaires avec les donnees brutes des annonces.
        """
        start_time = time.time()
        self._nb_scrapees = 0
        self._nb_erreurs = 0

        logger.info(
            "Demarrage scraping '%s' type '%s'",
            self.source_name,
            scrape_type,
        )

        # Verification robots.txt
        self.check_robots_txt()

        # Verifier que le type est supporte
        supported_types = self.source_config.get("types", [])
        if scrape_type not in supported_types:
            logger.error(
                "Type '%s' non supporte par '%s' (types: %s)",
                scrape_type,
                self.source_name,
                supported_types,
            )
            return []

        # Couche 0 : API Finder (methode principale)
        logger.info("Tentative via API Finder (Couche 0)...")
        api_results = self._search_via_api(scrape_type)

        if api_results is not None:
            # Enrichir avec les metadonnees de scraping
            results: list[dict[str, Any]] = []
            for listing in api_results:
                url = listing.get("url_source", "")
                listing["source"] = self.source_name
                listing["hash_dedup"] = self.generate_hash_dedup(url)
                listing["date_scrape"] = datetime.now().isoformat()
                results.append(listing)
                self._nb_scrapees += 1

            duree = time.time() - start_time
            logger.info(
                "Scraping '%s' type '%s' (API Finder) termine : "
                "%d annonces, %.1fs",
                self.source_name,
                scrape_type,
                self._nb_scrapees,
                duree,
            )
            return results

        # Fallback : scraping HTML classique (couches 1-3)
        logger.warning(
            "API Finder indisponible, fallback scraping HTML (couches 1-3)..."
        )
        self._active_layer = FetchLayer.CURL_SESSION
        return super().scrape(scrape_type)

    # ------------------------------------------------------------------
    # Fetching multi-couches HTML (override de BaseScraper._fetch_page)
    # ------------------------------------------------------------------

    def _fetch_page(self, url: str) -> Any | None:
        """Fetche une page LeBonCoin via la strategie multi-couches.

        Essaie chaque couche dans l'ordre jusqu'a obtenir un 200
        avec du contenu exploitable. Bascule automatiquement vers
        la couche suivante en cas d'echec.

        Args:
            url: URL a recuperer.

        Returns:
            Objet Adaptor compatible Scrapling, ou None en cas d'echec.
        """
        if not self.robots_checker.is_allowed(url):
            logger.warning("URL bloquee par robots.txt: %s", url)
            return None

        self._rate_limit()

        for layer in FetchLayer:
            if layer < self._active_layer:
                continue

            self._layer_attempts[layer] += 1
            logger.info(
                "LeBonCoin fetch [Couche %d: %s] : %s",
                layer.value,
                layer.name,
                url,
            )

            response = self._try_layer(layer, url)
            if response is not None:
                self._layer_successes[layer] += 1
                return response

            logger.warning(
                "Couche %d (%s) echouee pour %s",
                layer.value,
                layer.name,
                url,
            )

        logger.error("Toutes les couches ont echoue pour %s", url)
        self._nb_erreurs += 1
        return None

    def _try_layer(self, layer: FetchLayer, url: str) -> Any | None:
        """Execute une tentative de fetch pour une couche donnee.

        Note: FINDER_API n'est pas gere ici car l'API est utilisee dans
        scrape() directement (elle retourne des listings, pas du HTML).

        Args:
            layer: Couche de fetching a utiliser.
            url: URL a recuperer.

        Returns:
            Objet Adaptor en cas de succes, None sinon.
        """
        try:
            if layer == FetchLayer.FINDER_API:
                return None  # API geree dans scrape()
            elif layer == FetchLayer.CURL_SESSION:
                return self._fetch_curl_session(url)
            elif layer == FetchLayer.DATADOME_RESOLVE:
                return self._fetch_datadome_resolve(url)
            elif layer == FetchLayer.CAMOUFOX_FULL:
                return self._fetch_camoufox(url)
        except Exception:
            logger.error(
                "Erreur couche %s pour %s",
                layer.name,
                url,
                exc_info=True,
            )
        return None

    # ------------------------------------------------------------------
    # Couche 1 : curl_cffi session avec warm-up
    # ------------------------------------------------------------------

    def _get_curl_session(self) -> Any:
        """Retourne ou cree la session curl_cffi persistante.

        Returns:
            Instance de curl_cffi.requests.Session.
        """
        if self._curl_session is None:
            from curl_cffi.requests import Session

            self._curl_session = Session(impersonate=self._current_impersonate)
        return self._curl_session

    def _get_proxies(self) -> dict[str, str] | None:
        """Retourne la configuration proxy au format curl_cffi.

        Returns:
            Dict {"http": url, "https": url} ou None si pas de proxy.
        """
        if self._proxy:
            proxy_str = (
                self._proxy
                if isinstance(self._proxy, str)
                else next(iter(self._proxy.values()))
            )
            return {"http": proxy_str, "https": proxy_str}
        return None

    @staticmethod
    def _parse_proxy_for_playwright(proxy_url: str) -> dict[str, str]:
        """Convertit une URL proxy en format Playwright/Camoufox.

        Args:
            proxy_url: URL au format http://user:pass@host:port.

        Returns:
            Dict compatible Playwright : {server, username?, password?}.
        """
        parsed = urlparse(proxy_url)
        result: dict[str, str] = {
            "server": f"{parsed.scheme}://{parsed.hostname}:{parsed.port}",
        }
        if parsed.username:
            result["username"] = parsed.username
        if parsed.password:
            result["password"] = parsed.password
        return result

    def _warm_up_session(self) -> bool:
        """Warm-up : visite la homepage pour generer les cookies initiaux.

        Simule une navigation naturelle vers la homepage de LeBonCoin,
        ce qui declenche la generation du cookie DataDome initial.

        Returns:
            True si le warm-up a reussi (200 sur la homepage).
        """
        if self._session_warmed:
            return True

        session = self._get_curl_session()
        proxies = self._get_proxies()
        timeout = self.global_config.get("timeout", 30)

        logger.info("Warm-up session LeBonCoin : visite homepage...")

        try:
            resp = session.get(
                "https://www.leboncoin.fr/",
                proxies=proxies,
                timeout=timeout,
                headers={
                    **self._COMMON_HEADERS,
                    "Sec-Fetch-Dest": "document",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "none",
                    "Sec-Fetch-User": "?1",
                },
            )

            if resp.status_code == 200:
                cookie_names = list(session.cookies.keys()) if session.cookies else []
                logger.info(
                    "Warm-up OK (200), cookies: %s",
                    ", ".join(cookie_names) if cookie_names else "aucun",
                )
                self._session_warmed = True
                # Delai naturel entre homepage et recherche
                time.sleep(1.5 + random.random() * 2)
                return True

            logger.warning("Warm-up echec (status %d)", resp.status_code)
        except Exception:
            logger.error("Erreur warm-up session", exc_info=True)

        return False

    def _fetch_curl_session(self, url: str) -> Any | None:
        """Couche 1: Fetch via curl_cffi session avec warm-up.

        Maintient une session HTTP persistante avec cookies,
        simule une navigation naturelle avec les headers Sec-Fetch-*.

        Args:
            url: URL a recuperer.

        Returns:
            Objet Adaptor en cas de succes, None sinon.
        """
        if not self._warm_up_session():
            return None

        session = self._get_curl_session()
        proxies = self._get_proxies()
        timeout = self.global_config.get("timeout", 30)

        # Navigation interne vs externe
        is_internal = "/recherche" in url or "/ad/" in url

        headers = {
            **self._COMMON_HEADERS,
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin" if is_internal else "none",
        }
        if is_internal:
            headers["Referer"] = "https://www.leboncoin.fr/"
        else:
            headers["Sec-Fetch-User"] = "?1"

        # Injecter le cookie DataDome si disponible
        if self._datadome_cookie:
            session.cookies.set("datadome", self._datadome_cookie, domain=".leboncoin.fr")

        resp = session.get(url, proxies=proxies, timeout=timeout, headers=headers)

        if resp.status_code == 200 and resp.text and len(resp.text) > 1000:
            if self._is_datadome_challenge(resp.text):
                logger.warning("DataDome challenge detecte dans la reponse curl_cffi")
                return None

            logger.info(
                "curl_cffi session OK (200, %d octets) pour %s",
                len(resp.text),
                url,
            )
            return self._make_adaptor_response(resp.text, str(resp.url))

        logger.warning(
            "curl_cffi session echec (status=%d, len=%d) pour %s",
            resp.status_code,
            len(resp.text) if resp.text else 0,
            url,
        )
        return None

    # ------------------------------------------------------------------
    # Couche 2 : Resolution DataDome via Camoufox
    # ------------------------------------------------------------------

    def _resolve_datadome_cookie(self) -> str | None:
        """Resout le challenge DataDome via Camoufox et retourne le cookie.

        Lance un navigateur Camoufox stealth, visite la homepage,
        attend la resolution du challenge JS DataDome, et capture
        le cookie genere.

        Returns:
            Valeur du cookie datadome, ou None si echec.
        """
        try:
            from camoufox.sync_api import Camoufox
        except ImportError:
            logger.error(
                "camoufox non installe, couche DATADOME_RESOLVE indisponible. "
                "Installer avec : pip install 'camoufox[geoip]'"
            )
            return None

        logger.info("Resolution du cookie DataDome via Camoufox...")

        proxy_config = None
        if self._proxy:
            proxy_str = (
                self._proxy
                if isinstance(self._proxy, str)
                else next(iter(self._proxy.values()))
            )
            proxy_config = self._parse_proxy_for_playwright(proxy_str)

        try:
            with Camoufox(humanize=True, os=["windows"]) as browser:
                context_kwargs: dict[str, Any] = {
                    "locale": "fr-FR",
                    "timezone_id": "Europe/Paris",
                }
                if proxy_config:
                    context_kwargs["proxy"] = proxy_config

                context = browser.new_context(**context_kwargs)
                page = context.new_page()

                page.goto("https://www.leboncoin.fr/", wait_until="networkidle")
                page.wait_for_timeout(3000)

                # Attendre la resolution potentielle du challenge
                page.wait_for_timeout(2000)

                cookies = context.cookies()
                for cookie in cookies:
                    if cookie["name"] == "datadome":
                        logger.info("Cookie DataDome capture avec succes")
                        context.close()
                        return cookie["value"]

                logger.warning("Aucun cookie DataDome trouve apres navigation")
                context.close()
        except Exception:
            logger.error("Erreur resolution DataDome via Camoufox", exc_info=True)

        return None

    def _fetch_datadome_resolve(self, url: str) -> Any | None:
        """Couche 2: Resout DataDome puis reutilise curl_cffi.

        Capture le cookie DataDome via Camoufox, puis l'injecte
        dans une session curl_cffi pour continuer les requetes
        de maniere legere.

        Args:
            url: URL a recuperer.

        Returns:
            Objet Adaptor en cas de succes, None sinon.
        """
        if not self._datadome_cookie:
            self._datadome_cookie = self._resolve_datadome_cookie()
            if not self._datadome_cookie:
                return None

        # Reinitialiser la session curl_cffi pour integrer le nouveau cookie
        self._curl_session = None
        self._session_warmed = False

        return self._fetch_curl_session(url)

    # ------------------------------------------------------------------
    # Couche 3 : Camoufox complet
    # ------------------------------------------------------------------

    def _fetch_camoufox(self, url: str) -> Any | None:
        """Couche 3: Fetch complet via Camoufox avec humanize.

        Utilise un vrai navigateur Firefox stealth pour contourner
        DataDome. Plus lent mais le plus resilient.

        Args:
            url: URL a recuperer.

        Returns:
            Objet Adaptor en cas de succes, None sinon.
        """
        try:
            from camoufox.sync_api import Camoufox
        except ImportError:
            logger.error(
                "camoufox non installe, couche CAMOUFOX_FULL indisponible. "
                "Installer avec : pip install 'camoufox[geoip]'"
            )
            return None

        logger.info("Fetch Camoufox complet pour %s", url)

        proxy_config = None
        if self._proxy:
            proxy_str = (
                self._proxy
                if isinstance(self._proxy, str)
                else next(iter(self._proxy.values()))
            )
            proxy_config = self._parse_proxy_for_playwright(proxy_str)

        try:
            with Camoufox(humanize=True, os=["windows"]) as browser:
                context_kwargs: dict[str, Any] = {
                    "locale": "fr-FR",
                    "timezone_id": "Europe/Paris",
                }
                if proxy_config:
                    context_kwargs["proxy"] = proxy_config

                context = browser.new_context(**context_kwargs)
                page = context.new_page()

                # Warm-up via homepage
                page.goto("https://www.leboncoin.fr/", wait_until="networkidle")
                page.wait_for_timeout(2000 + int(random.random() * 2000))

                # Navigation vers l'URL cible
                page.goto(url, wait_until="networkidle")
                page.wait_for_timeout(3000)

                html = page.content()

                if html and len(html) > 1000:
                    if self._is_datadome_challenge(html):
                        # Attendre plus longtemps : le challenge peut se resoudre
                        logger.info("DataDome challenge detecte, attente resolution...")
                        page.wait_for_timeout(5000)
                        html = page.content()
                        if self._is_datadome_challenge(html):
                            logger.warning("DataDome challenge non resolu par Camoufox")
                            context.close()
                            return None

                    # Capturer le cookie DataDome pour reutilisation future
                    cookies = context.cookies()
                    for cookie in cookies:
                        if cookie["name"] == "datadome":
                            self._datadome_cookie = cookie["value"]
                            logger.info("Cookie DataDome mis a jour depuis Camoufox")

                    logger.info(
                        "Camoufox OK (%d octets) pour %s",
                        len(html),
                        url,
                    )
                    context.close()
                    return self._make_adaptor_response(html, url)

                logger.warning("Camoufox: contenu insuffisant pour %s", url)
                context.close()
        except Exception:
            logger.error("Erreur Camoufox pour %s", url, exc_info=True)

        return None

    # ------------------------------------------------------------------
    # Detection DataDome
    # ------------------------------------------------------------------

    @staticmethod
    def _is_datadome_challenge(html: str) -> bool:
        """Detecte si le HTML est une page de challenge DataDome.

        Verifie la presence de plusieurs indicateurs simultanement
        pour eviter les faux positifs.

        Args:
            html: Contenu HTML a analyser.

        Returns:
            True si le HTML contient un challenge DataDome.
        """
        indicators = [
            "geo.captcha-delivery.com",
            "dd.js",
            "datadome",
            "captcha-delivery",
            "interstitial",
        ]
        html_lower = html.lower()
        matches = sum(1 for indicator in indicators if indicator in html_lower)
        return matches >= 2

    # ------------------------------------------------------------------
    # Extraction JSON (Next.js __NEXT_DATA__)
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_next_data(html: str) -> dict[str, Any] | None:
        """Extrait les donnees JSON embarquees dans __NEXT_DATA__.

        LeBonCoin est une application Next.js qui embarque les donnees
        de la page dans une balise script JSON.

        Args:
            html: Contenu HTML de la page.

        Returns:
            Dictionnaire JSON, ou None si absent ou invalide.
        """
        match = re.search(
            r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
            html,
            re.DOTALL,
        )
        if match:
            try:
                return json.loads(match.group(1))
            except json.JSONDecodeError:
                logger.debug("JSON __NEXT_DATA__ malforma")
        return None

    @staticmethod
    def _extract_ld_json(html: str) -> list[dict[str, Any]]:
        """Extrait les donnees JSON-LD (schema.org) de la page.

        Args:
            html: Contenu HTML de la page.

        Returns:
            Liste de dictionnaires JSON-LD trouves.
        """
        results: list[dict[str, Any]] = []
        for match in re.finditer(
            r'<script type="application/ld\+json">(.*?)</script>',
            html,
            re.DOTALL,
        ):
            try:
                data = json.loads(match.group(1))
                results.append(data)
            except json.JSONDecodeError:
                continue
        return results

    def _parse_listings_from_json(
        self, next_data: dict[str, Any], scrape_type: str
    ) -> list[dict[str, Any]]:
        """Parse les annonces depuis la structure JSON Next.js.

        Explore plusieurs chemins possibles dans la structure JSON
        car LeBonCoin peut modifier l'arborescence.

        Args:
            next_data: Donnees JSON extraites de __NEXT_DATA__.
            scrape_type: 'vente' ou 'location'.

        Returns:
            Liste de dictionnaires avec les donnees de chaque annonce.
        """
        listings: list[dict[str, Any]] = []
        props = next_data.get("props", {}).get("pageProps", {})

        # Chercher les annonces dans plusieurs chemins possibles
        ads: list[dict[str, Any]] = []
        for path in ["searchData.ads", "listingData.ads", "ads"]:
            obj = props
            for key in path.split("."):
                obj = obj.get(key, {}) if isinstance(obj, dict) else {}
            if isinstance(obj, list) and obj:
                ads = obj
                break

        if not ads:
            logger.debug("Aucune annonce trouvee dans __NEXT_DATA__")
            return listings

        logger.info("JSON Next.js : %d annonces trouvees", len(ads))

        for ad in ads:
            try:
                listing = self._parse_json_ad(ad, scrape_type)
                if listing:
                    listings.append(listing)
            except Exception:
                logger.debug("Erreur parsing annonce JSON", exc_info=True)

        return listings

    def _parse_json_ad(
        self, ad: dict[str, Any], scrape_type: str
    ) -> dict[str, Any] | None:
        """Parse une annonce individuelle depuis le JSON (API Finder ou __NEXT_DATA__).

        Extrait toutes les informations disponibles : prix, surface,
        DPE, GES, localisation, photos, description, charges, historique prix, etc.

        Args:
            ad: Dictionnaire representant une annonce dans le JSON.
            scrape_type: 'vente' ou 'location'.

        Returns:
            Dictionnaire avec les donnees extraites, ou None si invalide.
        """
        list_id = ad.get("list_id")
        if not list_id:
            return None

        # URL de l'annonce
        slug = ad.get("url", "")
        if slug and not slug.startswith("http"):
            url_source = f"https://www.leboncoin.fr{slug}"
        elif slug:
            url_source = slug
        else:
            category_slug = (
                "ventes_immobilieres" if scrape_type == "vente" else "locations"
            )
            url_source = (
                f"https://www.leboncoin.fr/ad/{category_slug}/{list_id}.htm"
            )

        data: dict[str, Any] = {
            "url_source": url_source,
            "titre": ad.get("subject", ""),
        }

        # Prix (peut etre un entier ou une liste)
        price = ad.get("price")
        if isinstance(price, list) and price:
            data["prix"] = int(price[0])
        elif isinstance(price, (int, float)):
            data["prix"] = int(price)

        # Attributs (surface, pieces, DPE, etage, etc.)
        attributes = {
            attr.get("key", ""): attr.get("value", "")
            for attr in ad.get("attributes", [])
            if attr.get("key")
        }

        # Surface
        square = attributes.get("square")
        if square:
            try:
                data["surface_m2"] = float(str(square).replace(",", "."))
            except (ValueError, TypeError):
                pass

        # Nombre de pieces
        rooms = attributes.get("rooms")
        if rooms:
            try:
                data["nb_pieces"] = int(rooms)
            except (ValueError, TypeError):
                pass

        # DPE
        dpe = attributes.get("energy_rate")
        if dpe and len(str(dpe)) == 1 and str(dpe).upper() in "ABCDEFG":
            data["dpe"] = str(dpe).upper()

        # GES (disponible via API)
        ges = attributes.get("ges")
        if ges and len(str(ges)) == 1 and str(ges).upper() in "ABCDEFG":
            data["ges"] = str(ges).upper()

        # Etage
        floor = attributes.get("floor_number")
        if floor:
            try:
                data["etage"] = int(floor)
            except (ValueError, TypeError):
                pass

        # Meuble (location)
        if scrape_type == "location":
            furnished = attributes.get("furnished")
            if furnished:
                data["meuble"] = str(furnished).lower() in ("1", "true", "oui")

        # Charges annuelles de copropriete (API: annual_charges)
        annual_charges = attributes.get("annual_charges")
        if annual_charges:
            try:
                data["charges_copro"] = float(str(annual_charges).replace(",", "."))
            except (ValueError, TypeError):
                pass

        # Charges incluses (ancien champ, fallback)
        if "charges_copro" not in data:
            charges = attributes.get("charges_included")
            if charges:
                try:
                    data["charges_copro"] = float(str(charges).replace(",", "."))
                except (ValueError, TypeError):
                    pass

        # Loyer (location)
        if scrape_type == "location" and data.get("prix"):
            data["loyer_cc"] = float(data["prix"])

        # Prix/m2 (disponible via API)
        ppm = attributes.get("price_per_square_meter")
        if ppm:
            try:
                data["prix_m2"] = int(ppm)
            except (ValueError, TypeError):
                pass

        # Historique prix : detecter si old_price existe (baisse de prix)
        old_price = attributes.get("old_price")
        if old_price and data.get("prix"):
            try:
                old_p = int(old_price)
                if old_p != data["prix"]:
                    data["historique_prix"] = [
                        {"prix": old_p, "date": None},
                        {"prix": data["prix"], "date": ad.get("index_date")},
                    ]
            except (ValueError, TypeError):
                pass

        # Etat du bien (API: global_condition)
        condition = attributes.get("global_condition")
        if condition:
            condition_map = {
                "1": "neuf",
                "2": "tres_bon_etat",
                "3": "bon_etat",
                "4": "a_rafraichir",
                "5": "a_renover",
            }
            data["etat_bien"] = condition_map.get(str(condition), str(condition))

        # Localisation
        location = ad.get("location", {})
        addr_parts = [
            location.get("address"),
            location.get("city"),
            location.get("zipcode"),
        ]
        data["adresse_brute"] = ", ".join(p for p in addr_parts if p) or location.get(
            "city", ""
        )

        # Quartier (disponible via API)
        district = location.get("district")
        if district:
            data["quartier"] = district

        # Coordonnees
        lat = location.get("lat")
        lng = location.get("lng")
        if lat and lng:
            data["latitude"] = lat
            data["longitude"] = lng

        # Description
        body = ad.get("body", "")
        if body:
            data["description_texte"] = body

        # Photos
        images = ad.get("images", {})
        urls = images.get("urls", [])
        if urls:
            data["photos_urls"] = urls

        # Date de publication
        first_pub = ad.get("first_publication_date")
        if first_pub:
            data["date_publication"] = first_pub

        return data

    # ------------------------------------------------------------------
    # Override des methodes abstraites
    # ------------------------------------------------------------------

    def _get_search_url(self, scrape_type: str) -> str:
        """Construit l'URL de recherche LeBonCoin.

        Args:
            scrape_type: 'vente' ou 'location'.

        Returns:
            URL de recherche LeBonCoin complete.
        """
        base_urls = self.source_config.get("base_urls", {})
        url = base_urls.get(scrape_type, "")
        if not url:
            logger.error(
                "Aucune URL configuree pour LeBonCoin type '%s'", scrape_type
            )
        return url

    def _parse_listing_page(
        self, response: Any, scrape_type: str
    ) -> list[dict[str, Any]]:
        """Parse une page de resultats LeBonCoin.

        Strategie de parsing :
        1. Tente l'extraction JSON depuis __NEXT_DATA__ (Next.js)
        2. Fallback sur les selecteurs CSS si JSON indisponible

        Args:
            response: Reponse Scrapling/Adaptor de la page de listing.
            scrape_type: 'vente' ou 'location'.

        Returns:
            Liste de dictionnaires avec les donnees de chaque annonce.
        """
        html = response.text if hasattr(response, "text") else ""

        # Strategie 1 : JSON __NEXT_DATA__
        if html:
            next_data = self._extract_next_data(html)
            if next_data:
                json_listings = self._parse_listings_from_json(next_data, scrape_type)
                if json_listings:
                    logger.info(
                        "Extraction JSON reussie : %d annonces",
                        len(json_listings),
                    )
                    self._last_listing_from_json = True
                    return json_listings
                logger.debug("JSON __NEXT_DATA__ present mais pas d'annonces extractibles")

        # Strategie 2 : CSS selectors (fallback)
        self._last_listing_from_json = False
        return self._parse_listing_page_css(response, scrape_type)

    def _parse_listing_page_css(
        self, response: Any, scrape_type: str
    ) -> list[dict[str, Any]]:
        """Parse une page de listing via selecteurs CSS (methode originale).

        Args:
            response: Reponse Scrapling de la page de listing.
            scrape_type: 'vente' ou 'location'.

        Returns:
            Liste de dictionnaires avec les donnees de base par annonce.
        """
        listings: list[dict[str, Any]] = []
        container_selector = self.selectors.get("listing_container", "")

        if not container_selector:
            logger.warning("Selecteur listing_container manquant pour LeBonCoin")
            return listings

        try:
            cards = response.css(container_selector)
        except Exception:
            logger.error("Erreur CSS sur le listing LeBonCoin", exc_info=True)
            return listings

        logger.debug("LeBonCoin CSS: %d cartes trouvees", len(cards))

        for card in cards:
            try:
                listing = self._parse_card(card, scrape_type)
                if listing:
                    listings.append(listing)
            except Exception:
                logger.debug("Erreur parsing carte CSS", exc_info=True)

        return listings

    def _parse_card(
        self, card: Any, scrape_type: str
    ) -> dict[str, Any] | None:
        """Parse une carte d'annonce individuelle depuis la page de listing.

        Args:
            card: Element HTML de la carte d'annonce.
            scrape_type: 'vente' ou 'location'.

        Returns:
            Dictionnaire avec les donnees extraites, ou None si echec.
        """
        data: dict[str, Any] = {}

        # Titre
        title_el = card.css_first(self.selectors.get("title", ""))
        if title_el:
            data["titre"] = title_el.text.strip()

        # Lien vers la page de detail
        link_el = card.css_first(self.selectors.get("link", ""))
        if link_el:
            href = link_el.attrib.get("href", "")
            if href and not href.startswith("http"):
                href = f"https://www.leboncoin.fr{href}"
            data["url_source"] = href
        else:
            return None

        # Prix
        price_el = card.css_first(self.selectors.get("price", ""))
        if price_el:
            data["prix"] = self._parse_price(price_el.text)

        # Surface
        surface_el = card.css_first(self.selectors.get("surface", ""))
        if surface_el:
            data["surface_m2"] = self._parse_surface(surface_el.text)

        # Nombre de pieces
        rooms_el = card.css_first(self.selectors.get("rooms", ""))
        if rooms_el:
            data["nb_pieces"] = self._parse_rooms(rooms_el.text)

        # Localisation
        location_el = card.css_first(self.selectors.get("location", ""))
        if location_el:
            data["adresse_brute"] = location_el.text.strip()

        return data

    def _should_fetch_detail(self, listing: dict[str, Any]) -> bool:
        """Determine si la page de detail doit etre fetche.

        Quand les donnees sont extraites via JSON (__NEXT_DATA__),
        le listing contient deja toutes les informations necessaires.
        Eviter les requetes inutiles reduit le risque de detection.

        Args:
            listing: Donnees de l'annonce extraites du listing.

        Returns:
            False si les donnees JSON sont completes, True sinon.
        """
        if not self._last_listing_from_json:
            return True

        # Verifier que les champs essentiels sont presents
        has_required = all(listing.get(f) for f in self._REQUIRED_DETAIL_FIELDS)
        if has_required:
            logger.debug(
                "Listing JSON complet, skip detail pour %s",
                listing.get("url_source", "?"),
            )
            return False

        return True

    def _parse_detail_page(
        self, response: Any, scrape_type: str
    ) -> dict[str, Any]:
        """Parse une page de detail d'annonce LeBonCoin.

        Tente d'abord l'extraction JSON (__NEXT_DATA__),
        puis fallback sur les selecteurs CSS.

        Args:
            response: Reponse Scrapling de la page de detail.
            scrape_type: 'vente' ou 'location'.

        Returns:
            Dictionnaire avec les donnees detaillees de l'annonce.
        """
        data: dict[str, Any] = {}
        html = response.text if hasattr(response, "text") else ""

        # Strategie 1 : JSON __NEXT_DATA__ pour la page de detail
        if html:
            next_data = self._extract_next_data(html)
            if next_data:
                props = next_data.get("props", {}).get("pageProps", {})
                ad = props.get("ad", {})
                if ad:
                    json_data = self._parse_json_ad(ad, scrape_type)
                    if json_data:
                        logger.debug("Detail JSON extrait avec succes")
                        return json_data

        # Strategie 2 : CSS selectors (fallback)
        return self._parse_detail_page_css(response, scrape_type)

    def _parse_detail_page_css(
        self, response: Any, scrape_type: str
    ) -> dict[str, Any]:
        """Parse une page de detail via selecteurs CSS (methode originale).

        Args:
            response: Reponse Scrapling de la page de detail.
            scrape_type: 'vente' ou 'location'.

        Returns:
            Dictionnaire avec les donnees detaillees de l'annonce.
        """
        data: dict[str, Any] = {}

        # Description
        desc_el = response.css_first(self.selectors.get("description", ""))
        if desc_el:
            data["description_texte"] = desc_el.text.strip()

        # DPE
        dpe_el = response.css_first(self.selectors.get("dpe", ""))
        if dpe_el:
            data["dpe"] = self._parse_dpe(dpe_el.text)

        # Photos
        photo_els = response.css(self.selectors.get("photos", ""))
        if photo_els:
            data["photos_urls"] = [
                img.attrib.get("src", "")
                for img in photo_els
                if img.attrib.get("src")
            ]

        # Criteres specifiques de la page de detail
        criteria_items = response.css("div[data-qa-id='criteria_item']")
        for item in criteria_items:
            label_el = item.css_first("div[data-qa-id='criteria_item_label']")
            value_el = item.css_first("div[data-qa-id='criteria_item_value']")
            if label_el and value_el:
                label = label_el.text.strip().lower()
                value = value_el.text.strip()
                self._extract_criterion(data, label, value, scrape_type)

        # Date de publication
        date_el = response.css_first("time[datetime]")
        if date_el:
            data["date_publication"] = date_el.attrib.get("datetime", "")

        return data

    def _extract_criterion(
        self,
        data: dict[str, Any],
        label: str,
        value: str,
        scrape_type: str,
    ) -> None:
        """Extrait un critere specifique de la page de detail LeBonCoin.

        Args:
            data: Dictionnaire de donnees a enrichir.
            label: Label du critere (en minuscules).
            value: Valeur du critere.
            scrape_type: 'vente' ou 'location'.
        """
        if "surface" in label:
            data.setdefault("surface_m2", self._parse_surface(value))
        elif "pièce" in label or "piece" in label:
            data.setdefault("nb_pieces", self._parse_rooms(value))
        elif "étage" in label or "etage" in label:
            data["etage"] = self._parse_integer(value)
        elif "charges" in label:
            data["charges_copro"] = self._parse_float(value)
        elif "ges" in label or "énergie" in label or "energie" in label:
            parsed_dpe = self._parse_dpe(value)
            if parsed_dpe:
                data.setdefault("dpe", parsed_dpe)
        elif "meublé" in label or "meuble" in label:
            if scrape_type == "location":
                data["meuble"] = value.lower() in ("oui", "meublé", "meuble")
        elif "loyer" in label and scrape_type == "location":
            data.setdefault("loyer_cc", self._parse_float(value))

    def _get_next_page_url(self, response: Any, current_page: int) -> str | None:
        """Determine l'URL de la page suivante LeBonCoin.

        Args:
            response: Reponse Scrapling de la page courante.
            current_page: Numero de la page courante.

        Returns:
            URL de la page suivante, ou None si derniere page.
        """
        next_page = current_page + 1
        if next_page > self._max_pages:
            return None

        current_url = str(response.url) if hasattr(response, "url") else ""
        if not current_url:
            return None

        return self._add_page_param(current_url, next_page)

    def _add_page_param(self, url: str, page: int) -> str:
        """Ajoute ou met a jour le parametre de page dans une URL.

        Args:
            url: URL a modifier.
            page: Numero de page a inserer.

        Returns:
            URL modifiee avec le parametre de page.
        """
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        params[self._pagination_param] = [str(page)]
        new_query = urlencode(params, doseq=True)
        return urlunparse(parsed._replace(query=new_query))

    # ------------------------------------------------------------------
    # Monitoring et statistiques
    # ------------------------------------------------------------------

    def get_scraping_stats(self) -> dict[str, Any]:
        """Retourne les statistiques avec details par couche de fetching.

        Returns:
            Dictionnaire enrichi avec les stats par couche.
        """
        stats = super().get_scraping_stats()
        stats["layer_stats"] = {
            layer.name: {
                "attempts": self._layer_attempts[layer],
                "successes": self._layer_successes[layer],
            }
            for layer in FetchLayer
        }
        stats["active_layer"] = self._active_layer.name
        stats["datadome_cookie_captured"] = self._datadome_cookie is not None
        return stats

    # ------------------------------------------------------------------
    # Methodes utilitaires de parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_price(text: str) -> int | None:
        """Extrait un prix entier depuis un texte.

        Gere les formats courants : '140 000 EUR', '140000EUR', '140.000 EUR'.

        Args:
            text: Texte contenant le prix.

        Returns:
            Prix en euros (entier), ou None si non parsable.
        """
        if not text:
            return None
        cleaned = re.sub(r"[^\d]", "", text)
        if cleaned:
            try:
                return int(cleaned)
            except ValueError:
                return None
        return None

    @staticmethod
    def _parse_surface(text: str) -> float | None:
        """Extrait une surface en m2 depuis un texte.

        Gere les formats : '55 m2', '55m2', '55,5 m2'.

        Args:
            text: Texte contenant la surface.

        Returns:
            Surface en metres carres (float), ou None si non parsable.
        """
        if not text:
            return None
        match = re.search(r"(\d+[.,]?\d*)\s*m", text)
        if match:
            value = match.group(1).replace(",", ".")
            try:
                return float(value)
            except ValueError:
                return None
        return None

    @staticmethod
    def _parse_rooms(text: str) -> int | None:
        """Extrait un nombre de pieces depuis un texte.

        Gere les formats : '3 pieces', 'T3', '3p'.

        Args:
            text: Texte contenant le nombre de pieces.

        Returns:
            Nombre de pieces (entier), ou None si non parsable.
        """
        if not text:
            return None
        match = re.search(r"(\d+)", text)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return None
        return None

    @staticmethod
    def _parse_dpe(text: str) -> str | None:
        """Extrait la classe DPE depuis un texte.

        Args:
            text: Texte contenant le DPE.

        Returns:
            Lettre DPE (A-G) en majuscule, ou None si non parsable.
        """
        if not text:
            return None
        text = text.strip()
        match = re.search(r"(?:^|\s|:)\s*([A-Ga-g])\s*$", text)
        if match:
            return match.group(1).upper()
        return None

    @staticmethod
    def _parse_integer(text: str) -> int | None:
        """Extrait un entier depuis un texte.

        Args:
            text: Texte contenant un nombre entier.

        Returns:
            Entier extrait, ou None si non parsable.
        """
        if not text:
            return None
        match = re.search(r"(\d+)", text)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return None
        return None

    @staticmethod
    def _parse_float(text: str) -> float | None:
        """Extrait un nombre decimal depuis un texte.

        Args:
            text: Texte contenant un nombre.

        Returns:
            Nombre extrait (float), ou None si non parsable.
        """
        if not text:
            return None
        cleaned = re.sub(r"[^\d.,]", "", text)
        cleaned = cleaned.replace(",", ".")
        parts = cleaned.rsplit(".", 1)
        if len(parts) == 2:
            cleaned = parts[0].replace(".", "") + "." + parts[1]
        else:
            cleaned = cleaned.replace(".", "")
        if cleaned:
            try:
                return float(cleaned)
            except ValueError:
                return None
        return None
