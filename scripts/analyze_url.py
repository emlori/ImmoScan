#!/usr/bin/env python3
"""Analyse une annonce LeBonCoin a la demande.

Scrape une annonce specifique, la passe dans le pipeline complet
(validation, geocodage, scoring, enrichissement IA) et retourne
le message formate pret pour Telegram.

Usage: python scripts/analyze_url.py <url_leboncoin>
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("analyze")

LBC_URL_RE = re.compile(
    r"https?://(?:www\.)?leboncoin\.fr/(?:ad/)?\w+/(\d+)"
)


def extract_list_id(url: str) -> str | None:
    """Extrait le list_id depuis une URL LeBonCoin."""
    match = LBC_URL_RE.search(url)
    return match.group(1) if match else None


def fetch_single_listing(url: str) -> dict[str, Any] | None:
    """Scrape une annonce LeBonCoin individuelle via __NEXT_DATA__."""
    from src.scrapers.leboncoin import LeBonCoinScraper

    scraper = LeBonCoinScraper()

    # Warm-up session (cookies, impersonation)
    if not scraper._warm_up_session():
        logger.error("Echec warm-up session")
        return None

    session = scraper._get_curl_session()
    proxies = scraper._get_proxies()

    headers = {
        **scraper._COMMON_HEADERS,
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Referer": "https://www.leboncoin.fr/",
    }

    try:
        resp = session.get(url, proxies=proxies, timeout=30, headers=headers)
    except Exception as e:
        logger.error("Erreur requete: %s", e)
        return None

    if resp.status_code != 200:
        logger.error("HTTP %d pour %s", resp.status_code, url)
        return None

    # Extraire __NEXT_DATA__ JSON embarque dans la page
    match = re.search(
        r'<script\s+id="__NEXT_DATA__"[^>]*>(.*?)</script>',
        resp.text,
        re.DOTALL,
    )
    if not match:
        logger.error("__NEXT_DATA__ introuvable dans la page")
        return None

    try:
        next_data = json.loads(match.group(1))
    except json.JSONDecodeError as e:
        logger.error("JSON invalide dans __NEXT_DATA__: %s", e)
        return None

    ad_data = next_data.get("props", {}).get("pageProps", {}).get("ad")
    if not ad_data:
        logger.error("Donnees annonce absentes de __NEXT_DATA__")
        return None

    # Determiner le type (vente/location) depuis l'URL
    scrape_type = "location" if "/locations/" in url else "vente"
    result = scraper._parse_json_ad(ad_data, scrape_type)

    return result


def analyze(url: str) -> dict[str, Any] | None:
    """Pipeline complet d'analyse pour une annonce individuelle.

    Retourne le dict annonce enrichi avec renta_data, score_data,
    enrichment et formatted_message, ou None si echec.
    """
    # 1. Scraping
    logger.info("Scraping %s", url)
    annonce = fetch_single_listing(url)
    if not annonce:
        return None

    logger.info(
        "OK: %s EUR, %s m2, T%s, quartier=%s",
        annonce.get("prix"),
        annonce.get("surface_m2"),
        annonce.get("nb_pieces"),
        annonce.get("quartier"),
    )

    # 2. Validation (soft : on log mais on ne rejette pas)
    from src.validation.validators import AnnonceValidator

    validator = AnnonceValidator()
    is_valid, reasons = validator.validate_vente(annonce)
    if not is_valid:
        logger.warning("Validation: %s", "; ".join(reasons))

    # 3. Normalisation
    from src.parsers.normalizer import AnnonceNormalizer

    normalizer = AnnonceNormalizer()
    try:
        annonce = normalizer.normalize_vente(annonce)
    except Exception as e:
        logger.warning("Normalisation: %s", e)

    # 4. Geocodage
    from src.geo.geocoder import Geocoder
    from src.geo.scoring_geo import GeoScorer

    geocoder = Geocoder()
    geo_scorer = GeoScorer()

    if annonce.get("latitude") and annonce.get("longitude"):
        annonce["coordonnees"] = (annonce["latitude"], annonce["longitude"])
        if not annonce.get("quartier"):
            q = geo_scorer.identify_quartier(annonce["coordonnees"])
            if q:
                annonce["quartier"] = q
    else:
        adresse = annonce.get("adresse_brute", "")
        if adresse:
            try:
                geo = geocoder.geocode(adresse)
                if geo:
                    annonce["latitude"] = geo["latitude"]
                    annonce["longitude"] = geo["longitude"]
                    annonce["coordonnees"] = (geo["latitude"], geo["longitude"])
                    if not annonce.get("quartier"):
                        q = geo_scorer.identify_quartier(annonce["coordonnees"])
                        if q:
                            annonce["quartier"] = q
            except Exception as e:
                logger.warning("Geocodage: %s", e)

    # 5. Scoring
    from src.observatoire.loyers import ObservatoireLoyers
    from src.scoring.composite import CompositeScorer
    from src.scoring.rentabilite import RentabiliteCalculator

    renta_calc = RentabiliteCalculator()
    composite = CompositeScorer()
    obs = ObservatoireLoyers()

    prix = annonce.get("prix", 0)
    quartier = annonce.get("quartier", "Centre-Ville")
    nb = annonce.get("nb_pieces", 2)
    surface = annonce.get("surface_m2", 0)

    if prix and prix > 0:
        loyer_nu = obs.estimate_loyer(quartier, f"T{nb}", False, surface)
        loyer_meuble = obs.estimate_loyer(quartier, f"T{nb}", True, surface)
        loyer = loyer_nu.get("loyer_estime", 0) or 0
        loyer_haut = loyer_meuble.get("loyer_estime", 0) or 0
        fiab = loyer_nu.get("fiabilite", "preliminaire")

        renta = renta_calc.calculate(prix, loyer, annonce.get("charges_copro"))

        coords = annonce.get("coordonnees")
        geo_score = (
            geo_scorer.score_localisation(coords, quartier) if coords else 50.0
        )
        tension = geo_scorer.get_quartier_tension(quartier)

        a_s = dict(annonce)
        a_s["tension_locative"] = tension
        score = composite.score(a_s, renta, geo_score)

        annonce["renta_data"] = renta
        annonce["score_data"] = score
        annonce["loyer_estime"] = loyer
        annonce["loyer_estime_meuble"] = loyer_haut
        annonce["fiabilite_loyer"] = fiab

    # 6. Enrichissement IA
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if api_key and annonce.get("description_texte"):
        from src.enrichment.claude_enricher import ClaudeEnricher

        try:
            enricher = ClaudeEnricher(api_key=api_key, max_daily_calls=50)
            result = enricher.enrich(annonce)
            if result:
                annonce["enrichment"] = result
                logger.info("Enrichissement IA OK")
        except Exception as e:
            logger.warning("Enrichissement IA: %s", e)

    # 7. Formatage du message
    from src.alerts.formatter import AlertFormatter

    formatter = AlertFormatter()
    niveau = annonce.get("score_data", {}).get("niveau_alerte", "veille")

    if niveau == "top":
        msg = formatter.format_top_alert(
            annonce,
            annonce.get("score_data", {}),
            annonce.get("renta_data", {}),
            annonce.get("enrichment"),
        )
    elif niveau == "bon":
        msg = formatter.format_bon_alert(
            annonce,
            annonce.get("score_data", {}),
            annonce.get("renta_data", {}),
            annonce.get("enrichment"),
        )
    else:
        # VEILLE : afficher quand meme l'analyse complete
        msg = formatter.format_top_alert(
            annonce,
            annonce.get("score_data", {}),
            annonce.get("renta_data", {}),
            annonce.get("enrichment"),
        )
        msg = msg.replace(
            "\U0001f7e2 *TOP OPPORTUNITE*",
            "\U0001f534 *VEILLE*",
        )

    annonce["formatted_message"] = msg
    annonce["niveau"] = niveau
    return annonce


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/analyze_url.py <url_leboncoin>")
        sys.exit(1)

    url = sys.argv[1]
    t0 = time.time()
    result = analyze(url)

    if result:
        sd = result.get("score_data", {})
        rd = result.get("renta_data", {})
        print(f"\nScore: {sd.get('score_global', 0):.0f}/100")
        print(f"Renta brute: {rd.get('renta_brute', 0):.1f}%")
        print(f"Niveau: {result.get('niveau', '?').upper()}")
        print(f"Temps: {time.time() - t0:.1f}s")

        # Envoi Telegram
        from src.alerts.telegram_bot import TelegramBot

        bot = TelegramBot()
        esc = bot._escape_markdown(result["formatted_message"])
        if bot.send_alert_sync(esc, "top"):
            print("Message Telegram envoye!")
        else:
            print("Erreur envoi Telegram")
    else:
        print("Erreur: impossible d'analyser cette annonce")
        sys.exit(1)
