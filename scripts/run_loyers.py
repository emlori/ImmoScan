#!/usr/bin/env python3
"""Pipeline observatoire des loyers ImmoScan.

Scrape les annonces de location LeBonCoin, valide, normalise
et calcule les medianes de loyer par segment (quartier x type x meuble).

Planification : 1x/jour a 6h via systemd timer.
"""

from __future__ import annotations

import logging
import sys
import time
from pathlib import Path
from typing import Any

# Setup
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("loyers")

# Couleurs
G = "\033[92m"
R = "\033[91m"
Y = "\033[93m"
C = "\033[96m"
B = "\033[1m"
X = "\033[0m"

# Segments de l'observatoire
QUARTIERS = [
    "Centre-Ville", "Boucle", "Battant", "Chablais", "Rivotte",
    "Chaprais Cras", "Grette - Butte",
    "Montrapon - Fontaine Ecu - Montboucons - Montjoux",
    "Saint-Claude - Torcols - Chailluz",
    "Palente - Orchamps - Saragosse - Vaîtes",
    "Bregille - Prés de Vaux", "Rosemont - Saint-Ferjeux",
    "Clairs-Soleils - Vareilles", "Planoise - Châteaufarine", "Velotte",
]
TYPES_BIEN = ["T2", "T3"]
MEUBLE_OPTIONS = [True, False]


def step(n: int, name: str) -> None:
    print(f"\n{B}{C}[ETAPE {n}] {name}{X}")


def main() -> None:
    t0 = time.time()

    print(f"{B}{'='*60}{X}")
    print(f"{B}  IMMOSCAN - OBSERVATOIRE DES LOYERS{X}")
    print(f"{B}{'='*60}{X}")

    # ── ETAPE 1 : SCRAPING LOCATIONS ─────────────────────────────
    step(1, "SCRAPING LOCATIONS LEBONCOIN")

    from src.scrapers.leboncoin import LeBonCoinScraper

    scraper = LeBonCoinScraper()
    raw = scraper.scrape(scrape_type="location")
    stats = scraper.get_scraping_stats()
    print(f"  Annonces brutes: {len(raw)} en {time.time() - t0:.1f}s")
    print(
        f"  Stats: scrapees={stats.get('nb_annonces_scrapees', 0)}, "
        f"erreurs={stats.get('nb_erreurs', 0)}"
    )

    if not raw:
        print(f"{R}  Aucune annonce. Pipeline arretee.{X}")
        sys.exit(1)

    # ── ETAPE 2 : VALIDATION ─────────────────────────────────────
    step(2, "VALIDATION")

    from src.validation.validators import AnnonceValidator

    validator = AnnonceValidator()
    validated: list[dict[str, Any]] = []
    rejected = 0
    for a in raw:
        ok, reasons = validator.validate_location(a)
        if ok:
            validated.append(a)
        else:
            rejected += 1
    print(f"  Validees: {len(validated)}/{len(raw)} (rejets: {rejected})")

    if not validated:
        print(f"{R}  Aucune annonce valide.{X}")
        sys.exit(1)

    # ── ETAPE 3 : NORMALISATION ──────────────────────────────────
    step(3, "NORMALISATION")

    from src.parsers.normalizer import AnnonceNormalizer

    normalizer = AnnonceNormalizer()
    normalized: list[dict[str, Any]] = []
    for a in validated:
        try:
            normalized.append(normalizer.normalize_location(a))
        except Exception as e:
            logger.warning("Erreur norm location: %s", e)
    print(f"  Normalisees: {len(normalized)}/{len(validated)}")

    # ── ETAPE 4 : CALCUL DES MEDIANES ────────────────────────────
    step(4, "CALCUL DES MEDIANES")

    from src.observatoire.loyers import ObservatoireLoyers

    obs = ObservatoireLoyers()
    segments_ok = 0

    for quartier in QUARTIERS:
        for type_bien in TYPES_BIEN:
            for meuble in MEUBLE_OPTIONS:
                try:
                    mediane = obs.compute_medianes(
                        loyers=normalized,
                        quartier=quartier,
                        type_bien=type_bien,
                        meuble=meuble,
                    )
                    nb = mediane.get("nb_annonces", 0)
                    if nb > 0:
                        segments_ok += 1
                        meuble_str = "meuble" if meuble else "nu"
                        print(
                            f"  {G}{quartier}/{type_bien}/{meuble_str}{X}: "
                            f"median={mediane.get('loyer_median')} EUR "
                            f"(n={nb}, {mediane.get('fiabilite')})"
                        )
                except Exception as e:
                    logger.warning("Erreur mediane %s/%s: %s", quartier, type_bien, e)

    # ── RESUME ───────────────────────────────────────────────────
    total_time = time.time() - t0
    print(f"\n{B}{'='*60}{X}")
    print(f"{B}{G}  OBSERVATOIRE TERMINE en {total_time:.1f}s{X}")
    print(f"{B}  Locations scrapees: {len(raw)}{X}")
    print(f"{B}  Validees:           {len(validated)}{X}")
    print(f"{B}  Normalisees:        {len(normalized)}{X}")
    print(f"{B}  Segments calcules:  {segments_ok}{X}")
    print(f"{B}{'='*60}{X}")


if __name__ == "__main__":
    main()
