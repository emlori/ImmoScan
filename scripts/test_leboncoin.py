"""Script de test standalone pour le scraper LeBonCoin multi-couches.

Valide chaque couche de fetching individuellement et l'extraction JSON.
Usage : python scripts/test_leboncoin.py [--layer N] [--proxy URL]

Couches disponibles :
  0 = API Finder (methode principale, pas de DataDome)
  1 = curl_cffi homepage
  2 = curl_cffi session + warm-up vers /recherche
  3 = Resolution cookie DataDome via Camoufox
  4 = Camoufox fetch complet (recherche)
  5 = Integration complete (LeBonCoinScraper.scrape)

ATTENTION : Ce script effectue de vraies requetes vers leboncoin.fr.
Limiter les executions a quelques tests par jour.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import time
from pathlib import Path

# Ajouter la racine du projet au path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("test_leboncoin")

# URL de test (recherche appartements T2-T3 a Besancon)
TEST_URL = (
    "https://www.leboncoin.fr/recherche"
    "?category=9&locations=Besan%C3%A7on_25000"
    "&real_estate_type=2&price=120000-160000&rooms=2-3"
)
HOMEPAGE_URL = "https://www.leboncoin.fr/"

# API Finder
API_URL = "https://api.leboncoin.fr/finder/search"
API_KEY = "ba0c2dad52b3ec"


def _get_proxy() -> str | None:
    """Charge l'URL proxy depuis la configuration."""
    try:
        from src.config import get_settings

        settings = get_settings()
        if settings.proxy.enabled and settings.proxy.pool_url:
            return settings.proxy.pool_url
    except Exception:
        pass
    return None


def test_api_finder(proxy_url: str | None = None) -> bool:
    """Test 0 : API Finder LeBonCoin (methode principale)."""
    logger.info("=" * 60)
    logger.info("TEST 0 : API Finder LeBonCoin (POST /finder/search)")
    logger.info("=" * 60)

    try:
        from curl_cffi.requests import Session

        proxies = None
        if proxy_url:
            proxies = {"http": proxy_url, "https": proxy_url}

        session = Session(impersonate="chrome124")

        # Warm-up homepage (cookies)
        logger.info("Warm-up homepage...")
        resp_home = session.get(
            HOMEPAGE_URL,
            proxies=proxies,
            timeout=30,
            headers={
                "Accept-Language": "fr-FR,fr;q=0.9",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
            },
        )
        logger.info("Homepage: %d", resp_home.status_code)
        time.sleep(1)

        # Requete API Finder
        logger.info("Requete API Finder...")
        payload = {
            "limit": 5,
            "limit_alu": 0,
            "filters": {
                "category": {"id": "9"},
                "enums": {"real_estate_type": ["2"], "ad_type": ["offer"]},
                "location": {
                    "locations": [
                        {
                            "city": "Besançon",
                            "zipcode": "25000",
                            "label": "Besançon (25000)",
                        }
                    ]
                },
                "ranges": {
                    "price": {"min": 120000, "max": 160000},
                    "rooms": {"min": 2, "max": 3},
                },
            },
            "sort_by": "time",
            "sort_order": "desc",
        }

        resp = session.post(
            API_URL,
            json=payload,
            proxies=proxies,
            timeout=30,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Accept-Language": "fr-FR,fr;q=0.9",
                "Origin": "https://www.leboncoin.fr",
                "Referer": "https://www.leboncoin.fr/",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-site",
                "api_key": API_KEY,
            },
        )

        logger.info("API Status: %d", resp.status_code)
        logger.info("API Taille: %d octets", len(resp.text) if resp.text else 0)

        if resp.status_code == 200:
            data = resp.json()
            ads = data.get("ads", [])
            total = data.get("total", 0)
            max_pages = data.get("max_pages", 0)

            logger.info("Total annonces: %d (max_pages: %d)", total, max_pages)
            logger.info("Annonces retournees: %d", len(ads))

            if ads:
                ad = ads[0]
                attrs = {
                    a.get("key"): a.get("value")
                    for a in ad.get("attributes", [])
                }
                loc = ad.get("location", {})
                logger.info("--- Premiere annonce ---")
                logger.info("  Titre    : %s", ad.get("subject", "?"))
                logger.info("  Prix     : %s", ad.get("price", "?"))
                logger.info("  Surface  : %s m2", attrs.get("square", "?"))
                logger.info("  Pieces   : %s", attrs.get("rooms", "?"))
                logger.info("  DPE      : %s", attrs.get("energy_rate", "?"))
                logger.info("  GES      : %s", attrs.get("ges", "?"))
                logger.info("  Quartier : %s", loc.get("district", "?"))
                logger.info("  Coords   : %s, %s", loc.get("lat", "?"), loc.get("lng", "?"))
                logger.info(
                    "  Charges  : %s EUR/an", attrs.get("annual_charges", "?")
                )
                logger.info("  Etat     : %s", attrs.get("global_condition", "?"))
                logger.info("  URL      : %s", ad.get("url", "?"))
                logger.info("  Photos   : %d", ad.get("images", {}).get("nb_images", 0))

                # Verifier les champs critiques
                has_body = bool(ad.get("body"))
                has_coords = bool(loc.get("lat") and loc.get("lng"))
                has_price = bool(ad.get("price"))
                logger.info("  --- Completude ---")
                logger.info("  Description: %s", has_body)
                logger.info("  Coordonnees: %s", has_coords)
                logger.info("  Prix: %s", has_price)

            logger.info("RESULTAT: OK")
            return True

        logger.warning("RESULTAT: ECHEC (status %d)", resp.status_code)
        logger.info("Body: %s", resp.text[:500])
        return False

    except Exception as e:
        logger.error("RESULTAT: ERREUR - %s", e, exc_info=True)
        return False


def test_curl_cffi_homepage(proxy_url: str | None = None) -> bool:
    """Test 1 : curl_cffi peut atteindre la homepage."""
    logger.info("=" * 60)
    logger.info("TEST 1 : curl_cffi homepage (sans session)")
    logger.info("=" * 60)

    try:
        from curl_cffi import requests as curl_requests

        proxies = None
        if proxy_url:
            proxies = {"http": proxy_url, "https": proxy_url}

        resp = curl_requests.get(
            HOMEPAGE_URL,
            impersonate="chrome",
            proxies=proxies,
            timeout=30,
        )

        logger.info("Status: %d", resp.status_code)
        logger.info("Taille: %d octets", len(resp.text) if resp.text else 0)
        logger.info("Cookies: %s", dict(resp.cookies))

        if resp.status_code == 200 and resp.text:
            has_next_data = "__NEXT_DATA__" in resp.text
            logger.info("__NEXT_DATA__ present: %s", has_next_data)
            logger.info("RESULTAT: OK")
            return True

        logger.warning("RESULTAT: ECHEC (status %d)", resp.status_code)
        return False

    except Exception as e:
        logger.error("RESULTAT: ERREUR - %s", e)
        return False


def test_curl_cffi_session(proxy_url: str | None = None) -> bool:
    """Test 2 : curl_cffi session avec warm-up."""
    logger.info("=" * 60)
    logger.info("TEST 2 : curl_cffi session avec warm-up")
    logger.info("=" * 60)

    try:
        from curl_cffi.requests import Session

        proxies = None
        if proxy_url:
            proxies = {"http": proxy_url, "https": proxy_url}

        session = Session(impersonate="chrome124")

        # Etape 1 : Homepage
        logger.info("Etape 1 : Visite homepage...")
        resp_home = session.get(
            HOMEPAGE_URL,
            proxies=proxies,
            timeout=30,
            headers={
                "Accept-Language": "fr-FR,fr;q=0.9",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
                "Sec-Fetch-User": "?1",
            },
        )
        logger.info("Homepage status: %d", resp_home.status_code)
        logger.info("Cookies apres homepage: %s", list(session.cookies.keys()))

        if resp_home.status_code != 200:
            logger.warning("RESULTAT: ECHEC homepage")
            return False

        # Delai naturel
        logger.info("Attente 3s (simulation navigation)...")
        time.sleep(3)

        # Etape 2 : Page de recherche
        logger.info("Etape 2 : Page de recherche...")
        resp_search = session.get(
            TEST_URL,
            proxies=proxies,
            timeout=30,
            headers={
                "Accept-Language": "fr-FR,fr;q=0.9",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "same-origin",
                "Referer": HOMEPAGE_URL,
            },
        )
        logger.info("Recherche status: %d", resp_search.status_code)
        logger.info("Taille: %d octets", len(resp_search.text) if resp_search.text else 0)
        logger.info("Cookies apres recherche: %s", list(session.cookies.keys()))

        if resp_search.status_code == 200 and resp_search.text:
            # Verifier si DataDome challenge
            html = resp_search.text
            is_challenge = (
                html.count("captcha-delivery") > 0 and html.count("datadome") > 0
            )
            logger.info("DataDome challenge: %s", is_challenge)

            if not is_challenge and len(html) > 1000:
                # Tenter extraction JSON
                _test_json_extraction(html)
                logger.info("RESULTAT: OK")
                return True

        logger.warning("RESULTAT: ECHEC (bloque ou contenu vide)")
        return False

    except Exception as e:
        logger.error("RESULTAT: ERREUR - %s", e)
        return False


def test_datadome_cookie(proxy_url: str | None = None) -> bool:
    """Test 3 : Capture du cookie DataDome via Camoufox."""
    logger.info("=" * 60)
    logger.info("TEST 3 : Resolution DataDome via Camoufox")
    logger.info("=" * 60)

    try:
        from camoufox.sync_api import Camoufox
    except ImportError:
        logger.error("RESULTAT: SKIP - camoufox non installe")
        logger.info("Installer avec : pip install 'camoufox[geoip]'")
        return False

    try:
        proxy_config = None
        if proxy_url:
            from urllib.parse import urlparse

            parsed = urlparse(proxy_url)
            proxy_config = {
                "server": f"{parsed.scheme}://{parsed.hostname}:{parsed.port}",
            }
            if parsed.username:
                proxy_config["username"] = parsed.username
            if parsed.password:
                proxy_config["password"] = parsed.password

        logger.info("Lancement Camoufox...")
        with Camoufox(humanize=True, os=["windows"]) as browser:
            context_kwargs: dict = {
                "locale": "fr-FR",
                "timezone_id": "Europe/Paris",
            }
            if proxy_config:
                context_kwargs["proxy"] = proxy_config

            context = browser.new_context(**context_kwargs)
            page = context.new_page()

            logger.info("Navigation vers homepage...")
            page.goto(HOMEPAGE_URL, wait_until="networkidle")
            page.wait_for_timeout(3000)

            cookies = context.cookies()
            cookie_names = [c["name"] for c in cookies]
            logger.info("Cookies captures: %s", cookie_names)

            datadome_cookie = None
            for cookie in cookies:
                if cookie["name"] == "datadome":
                    datadome_cookie = cookie["value"]
                    logger.info("Cookie DataDome: %s...", datadome_cookie[:30])

            if datadome_cookie:
                logger.info("RESULTAT: OK - Cookie DataDome capture")
                context.close()
                return True

            logger.warning("RESULTAT: ECHEC - Pas de cookie DataDome")
            context.close()
            return False

    except Exception as e:
        logger.error("RESULTAT: ERREUR - %s", e)
        return False


def test_camoufox_full(proxy_url: str | None = None) -> bool:
    """Test 4 : Fetch complet via Camoufox."""
    logger.info("=" * 60)
    logger.info("TEST 4 : Camoufox fetch complet (recherche)")
    logger.info("=" * 60)

    try:
        from camoufox.sync_api import Camoufox
    except ImportError:
        logger.error("RESULTAT: SKIP - camoufox non installe")
        return False

    try:
        proxy_config = None
        if proxy_url:
            from urllib.parse import urlparse

            parsed = urlparse(proxy_url)
            proxy_config = {
                "server": f"{parsed.scheme}://{parsed.hostname}:{parsed.port}",
            }
            if parsed.username:
                proxy_config["username"] = parsed.username
            if parsed.password:
                proxy_config["password"] = parsed.password

        with Camoufox(humanize=True, os=["windows"]) as browser:
            context_kwargs: dict = {
                "locale": "fr-FR",
                "timezone_id": "Europe/Paris",
            }
            if proxy_config:
                context_kwargs["proxy"] = proxy_config

            context = browser.new_context(**context_kwargs)
            page = context.new_page()

            # Warm-up
            logger.info("Warm-up homepage...")
            page.goto(HOMEPAGE_URL, wait_until="networkidle")
            page.wait_for_timeout(3000)

            # Recherche
            logger.info("Navigation vers page de recherche...")
            page.goto(TEST_URL, wait_until="networkidle")
            page.wait_for_timeout(3000)

            html = page.content()
            logger.info("Taille HTML: %d octets", len(html) if html else 0)

            if html and len(html) > 1000:
                is_challenge = (
                    html.count("captcha-delivery") > 0 and html.count("datadome") > 0
                )
                logger.info("DataDome challenge: %s", is_challenge)

                if not is_challenge:
                    _test_json_extraction(html)
                    logger.info("RESULTAT: OK")
                    context.close()
                    return True

            logger.warning("RESULTAT: ECHEC")
            context.close()
            return False

    except Exception as e:
        logger.error("RESULTAT: ERREUR - %s", e)
        return False


def test_integration(proxy_url: str | None = None) -> bool:
    """Test 5 : Integration complete via LeBonCoinScraper.scrape()."""
    logger.info("=" * 60)
    logger.info("TEST 5 : Integration LeBonCoinScraper.scrape('vente')")
    logger.info("=" * 60)

    try:
        from src.scrapers.leboncoin import LeBonCoinScraper

        scraper = LeBonCoinScraper()
        results = scraper.scrape("vente")

        logger.info("Annonces scrapees: %d", len(results))

        if results:
            stats = scraper.get_scraping_stats()
            logger.info("Stats: %s", json.dumps(stats, indent=2, default=str))

            # Verifier la qualite des donnees
            r = results[0]
            logger.info("--- Premiere annonce ---")
            logger.info("  URL      : %s", r.get("url_source", "?"))
            logger.info("  Titre    : %s", r.get("titre", "?"))
            logger.info("  Prix     : %s", r.get("prix", "?"))
            logger.info("  Surface  : %s m2", r.get("surface_m2", "?"))
            logger.info("  Pieces   : %s", r.get("nb_pieces", "?"))
            logger.info("  DPE      : %s", r.get("dpe", "?"))
            logger.info("  Quartier : %s", r.get("quartier", "?"))
            logger.info("  Coords   : %s, %s", r.get("latitude", "?"), r.get("longitude", "?"))
            logger.info("  Etat     : %s", r.get("etat_bien", "?"))
            logger.info("  Hash     : %s", r.get("hash_dedup", "?")[:20])
            logger.info("  Source   : %s", r.get("source", "?"))

            # Validation des champs requis
            required = {"url_source", "titre", "prix", "surface_m2", "nb_pieces", "source", "hash_dedup"}
            complete = sum(1 for r in results if all(r.get(f) for f in required))
            logger.info(
                "Completude: %d/%d annonces avec tous les champs requis (%.0f%%)",
                complete,
                len(results),
                100 * complete / len(results) if results else 0,
            )

            logger.info("RESULTAT: OK")
            return True

        logger.warning("RESULTAT: ECHEC - aucune annonce")
        return False

    except Exception as e:
        logger.error("RESULTAT: ERREUR - %s", e, exc_info=True)
        return False


def _test_json_extraction(html: str) -> None:
    """Teste l'extraction JSON depuis le HTML recupere."""
    logger.info("--- Extraction JSON ---")

    # __NEXT_DATA__
    match = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
        html,
        re.DOTALL,
    )
    if match:
        try:
            data = json.loads(match.group(1))
            props = data.get("props", {}).get("pageProps", {})

            # Chercher les annonces
            ads = (
                props.get("searchData", {}).get("ads", [])
                or props.get("listingData", {}).get("ads", [])
                or props.get("ads", [])
            )

            logger.info("__NEXT_DATA__ : %d annonces trouvees", len(ads))

            if ads:
                ad = ads[0]
                logger.info("  Exemple : %s", ad.get("subject", "?"))
                logger.info("  Prix    : %s", ad.get("price", "?"))
                logger.info("  URL     : %s", ad.get("url", "?"))

                attrs = {
                    a.get("key"): a.get("value")
                    for a in ad.get("attributes", [])
                }
                logger.info("  Surface : %s m2", attrs.get("square", "?"))
                logger.info("  Pieces  : %s", attrs.get("rooms", "?"))
                logger.info("  DPE     : %s", attrs.get("energy_rate", "?"))
        except json.JSONDecodeError:
            logger.warning("__NEXT_DATA__ present mais JSON invalide")
    else:
        logger.info("__NEXT_DATA__ absent")

    # JSON-LD
    ld_matches = re.findall(
        r'<script type="application/ld\+json">(.*?)</script>',
        html,
        re.DOTALL,
    )
    logger.info("JSON-LD : %d blocs trouves", len(ld_matches))


def main() -> None:
    """Point d'entree du script de test."""
    parser = argparse.ArgumentParser(
        description="Test du scraper LeBonCoin multi-couches"
    )
    parser.add_argument(
        "--layer",
        type=int,
        choices=[0, 1, 2, 3, 4, 5],
        help=(
            "Tester une couche specifique "
            "(0=API, 1=homepage, 2=session, 3=datadome, 4=camoufox, 5=integration)"
        ),
    )
    parser.add_argument(
        "--proxy",
        type=str,
        help="URL du proxy (http://user:pass@host:port). Auto-detecte depuis .env si absent.",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Executer tous les tests (attention: ~60s, plusieurs requetes)",
    )
    args = parser.parse_args()

    # Auto-detection du proxy depuis .env si non fourni
    proxy = args.proxy or _get_proxy()
    if proxy:
        masked = proxy.split("@")[-1] if "@" in proxy else "configure"
        logger.info("Proxy: %s", masked)
    else:
        logger.info("Proxy: desactive")

    tests = {
        0: ("API Finder LeBonCoin", test_api_finder),
        1: ("curl_cffi homepage", test_curl_cffi_homepage),
        2: ("curl_cffi session + warm-up", test_curl_cffi_session),
        3: ("DataDome cookie Camoufox", test_datadome_cookie),
        4: ("Camoufox fetch complet", test_camoufox_full),
        5: ("Integration LeBonCoinScraper", test_integration),
    }

    if args.layer is not None:
        name, func = tests[args.layer]
        logger.info("Test cible : %s", name)
        func(proxy)
    elif args.all:
        results: dict[str, bool] = {}
        for num, (name, func) in tests.items():
            results[name] = func(proxy)
            if num < len(tests) - 1:
                logger.info("Attente 5s avant le test suivant...")
                time.sleep(5)

        logger.info("=" * 60)
        logger.info("RESUME")
        logger.info("=" * 60)
        for name, success in results.items():
            status = "OK" if success else "ECHEC"
            logger.info("  %-35s : %s", name, status)
    else:
        # Par defaut, tester l'API Finder (le plus leger et fiable)
        logger.info("Test par defaut : API Finder LeBonCoin")
        logger.info("Utiliser --all pour tous les tests, --layer N pour un test specifique")
        logger.info("")
        test_api_finder(proxy)


if __name__ == "__main__":
    main()
