#!/usr/bin/env python3
"""Test de la pipeline complete ImmoScan.

Teste chaque etape de la pipeline de maniere isolee puis integree,
depuis l'acquisition des donnees LeBonCoin API jusqu'a l'envoi
d'alertes Telegram.

Etapes testees :
1. Acquisition : API LeBonCoin (Layer 0)
2. Validation : Regles metier
3. Normalisation : Nettoyage et standardisation
4. Deduplication : Hash + matching flou
5. Geocodage : API data.gouv.fr
6. Scoring : Rentabilite + composite
7. Enrichissement IA : Claude Haiku
8. Alertes : Formatage + envoi Telegram

Usage :
    python scripts/test_pipeline.py                 # Toutes les etapes
    python scripts/test_pipeline.py --step scrape   # Une etape specifique
    python scripts/test_pipeline.py --dry-run       # Sans appels externes
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

# Ajouter la racine du projet au path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Charger les variables d'environnement
from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")

# Configurer le logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("test_pipeline")

# Couleurs pour le terminal
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RESET = "\033[0m"
BOLD = "\033[1m"


def print_step(step: str, description: str) -> None:
    """Affiche un titre d'etape."""
    print(f"\n{BOLD}{CYAN}{'='*60}{RESET}")
    print(f"{BOLD}{CYAN}  ETAPE: {step} - {description}{RESET}")
    print(f"{BOLD}{CYAN}{'='*60}{RESET}\n")


def print_result(success: bool, message: str) -> None:
    """Affiche le resultat d'un test."""
    icon = f"{GREEN}PASS{RESET}" if success else f"{RED}FAIL{RESET}"
    print(f"  [{icon}] {message}")


def print_warning(message: str) -> None:
    """Affiche un avertissement."""
    print(f"  [{YELLOW}WARN{RESET}] {message}")


# ======================================================================
# DONNEES DE TEST (annonces fictives realistes pour Besancon)
# ======================================================================

MOCK_ANNONCES_VENTE: list[dict[str, Any]] = [
    {
        "url_source": "https://www.leboncoin.fr/ventes_immobilieres/1234567890.htm",
        "source": "leboncoin",
        "prix": 139000,
        "surface_m2": 62.0,
        "nb_pieces": 3,
        "dpe": "C",
        "adresse_brute": "12 rue de la Republique, 25000 Besancon",
        "quartier": "Centre-Ville",
        "description_texte": (
            "Vente rapide cause mutation. Bel appartement T3 lumineux de 62m2, "
            "situe au 3eme etage avec ascenseur. Double vitrage, parquet, cave. "
            "Copropriete de 24 lots, charges 85EUR/mois. Proche tram et commerces. "
            "Ideal investisseur."
        ),
        "charges_copro": 85,
        "etage": 3,
        "photos_urls": ["https://img.leboncoin.fr/photo1.jpg"],
        "date_publication": "2026-03-10",
    },
    {
        "url_source": "https://www.leboncoin.fr/ventes_immobilieres/9876543210.htm",
        "source": "leboncoin",
        "prix": 125000,
        "surface_m2": 45.0,
        "nb_pieces": 2,
        "dpe": "D",
        "adresse_brute": "5 rue Battant, 25000 Besancon",
        "quartier": "Battant",
        "description_texte": (
            "A saisir ! T2 refait a neuf, cuisine equipee, salle de bain moderne. "
            "Actuellement loue 480EUR/mois. Parking en sous-sol inclus. "
            "Immeuble bien entretenu."
        ),
        "charges_copro": 60,
        "etage": 2,
        "photos_urls": [],
        "date_publication": "2026-03-12",
    },
    {
        "url_source": "https://www.leboncoin.fr/ventes_immobilieres/5555555555.htm",
        "source": "leboncoin",
        "prix": 155000,
        "surface_m2": 72.0,
        "nb_pieces": 3,
        "dpe": "B",
        "adresse_brute": "8 avenue de Chardonnet, 25000 Besancon",
        "quartier": "Chablais",
        "description_texte": (
            "Grand T3 de 72m2 en parfait etat. Balcon, cave, place de parking. "
            "DPE B, faibles charges. Quartier calme et familial. "
            "Copropriete 16 lots."
        ),
        "charges_copro": 70,
        "etage": 1,
        "photos_urls": ["https://img.leboncoin.fr/photo2.jpg", "https://img.leboncoin.fr/photo3.jpg"],
        "date_publication": "2026-03-08",
    },
]

# Annonces invalides pour tester la validation
MOCK_ANNONCES_INVALIDES: list[dict[str, Any]] = [
    {
        "url_source": "https://www.leboncoin.fr/ventes_immobilieres/invalid1.htm",
        "source": "leboncoin",
        "prix": 5000,  # Prix trop bas
        "surface_m2": 62.0,
        "nb_pieces": 3,
        "adresse_brute": "12 rue X, 25000 Besancon",
    },
    {
        "url_source": "https://www.leboncoin.fr/ventes_immobilieres/invalid2.htm",
        "source": "leboncoin",
        "prix": 139000,
        "surface_m2": 5.0,  # Surface trop petite
        "nb_pieces": 3,
        "adresse_brute": "12 rue X, 25000 Besancon",
    },
    {
        "url_source": "https://www.leboncoin.fr/ventes_immobilieres/invalid3.htm",
        "source": "leboncoin",
        "prix": 139000,
        "surface_m2": 62.0,
        "nb_pieces": 3,
        "adresse_brute": "12 rue X, 75001 Paris",  # Pas Besancon
    },
]


# ======================================================================
# TESTS PAR ETAPE
# ======================================================================


def test_config() -> bool:
    """Teste le chargement de la configuration."""
    print_step("CONFIG", "Chargement de la configuration")
    success = True

    try:
        from src.config import Settings

        settings = Settings()
        print_result(True, "Settings instancie")

        # YAML files
        sources = settings.load_sources()
        print_result("leboncoin" in sources.get("sources", {}), "sources.yaml charge (leboncoin present)")
        print_result("pap" not in sources.get("sources", {}), "PAP supprime de sources.yaml")

        scoring = settings.load_scoring()
        print_result("poids" in scoring, "scoring.yaml charge")

        quartiers = settings.load_quartiers()
        print_result("quartiers" in quartiers, "quartiers.yaml charge")

    except Exception as e:
        print_result(False, f"Erreur config: {e}")
        success = False

    return success


def test_scrape_api(dry_run: bool = False) -> list[dict[str, Any]]:
    """Teste l'acquisition via l'API LeBonCoin."""
    print_step("SCRAPE", "Acquisition via API LeBonCoin")

    if dry_run:
        print_warning("Mode dry-run : utilisation des donnees mock")
        for a in MOCK_ANNONCES_VENTE:
            print_result(True, f"Mock: {a['prix']}EUR {a['surface_m2']}m2 T{a['nb_pieces']} {a['quartier']}")
        return list(MOCK_ANNONCES_VENTE)

    try:
        from src.scrapers.leboncoin import LeBonCoinScraper

        scraper = LeBonCoinScraper()
        print_result(True, "LeBonCoinScraper instancie")

        # Tester l'API Finder (Layer 0) directement
        results = scraper.scrape(scrape_type="vente")
        stats = scraper.get_scraping_stats()

        nb = len(results)
        print_result(nb > 0, f"API retourne {nb} annonces")
        print_result(
            stats.get("nb_erreurs", 0) == 0,
            f"Erreurs scraping: {stats.get('nb_erreurs', 0)}",
        )

        if results:
            sample = results[0]
            print_result(True, f"Exemple: {sample.get('prix')}EUR {sample.get('surface_m2')}m2")

        return results

    except Exception as e:
        print_result(False, f"Erreur scraping: {e}")
        print_warning("Fallback sur donnees mock")
        return list(MOCK_ANNONCES_VENTE)


def test_validation(annonces: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Teste la validation des annonces."""
    print_step("VALIDATION", "Regles de validation metier")

    from src.validation.validators import AnnonceValidator

    validator = AnnonceValidator()
    print_result(True, "AnnonceValidator instancie")

    # Tester les annonces valides
    validated: list[dict[str, Any]] = []
    for annonce in annonces:
        is_valid, reasons = validator.validate_vente(annonce)
        if is_valid:
            validated.append(annonce)
        else:
            print_warning(f"Rejetee: {annonce.get('url_source', '?')[:50]} -> {reasons}")

    print_result(
        len(validated) > 0,
        f"Validees: {len(validated)}/{len(annonces)}",
    )

    # Tester les rejets attendus
    nb_rejets = 0
    for annonce in MOCK_ANNONCES_INVALIDES:
        is_valid, reasons = validator.validate_vente(annonce)
        if not is_valid:
            nb_rejets += 1

    print_result(
        nb_rejets == len(MOCK_ANNONCES_INVALIDES),
        f"Rejets attendus: {nb_rejets}/{len(MOCK_ANNONCES_INVALIDES)}",
    )

    return validated


def test_normalisation(annonces: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Teste la normalisation des annonces."""
    print_step("NORMALISATION", "Nettoyage et standardisation")

    from src.parsers.normalizer import AnnonceNormalizer

    normalizer = AnnonceNormalizer()
    print_result(True, "AnnonceNormalizer instancie")

    normalized: list[dict[str, Any]] = []
    for annonce in annonces:
        try:
            result = normalizer.normalize_vente(annonce)
            normalized.append(result)
        except Exception as e:
            print_result(False, f"Erreur normalisation: {e}")

    print_result(
        len(normalized) == len(annonces),
        f"Normalisees: {len(normalized)}/{len(annonces)}",
    )

    if normalized:
        sample = normalized[0]
        print_result(
            sample.get("completude_score") is not None,
            f"Score completude: {sample.get('completude_score', 'N/A')}",
        )

    return normalized


def test_dedup(annonces: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Teste la deduplication."""
    print_step("DEDUP", "Deduplication intra-source + inter-sources")

    from src.parsers.dedup import Deduplicator

    dedup = Deduplicator()
    print_result(True, "Deduplicator instancie")

    seen_hashes: set[str] = set()
    unique: list[dict[str, Any]] = []

    for annonce in annonces:
        url = annonce.get("url_source", "")
        if not url:
            continue

        hash_val = dedup.compute_hash_intra(url)
        if hash_val in seen_hashes:
            print_warning(f"Doublon detecte: {url[:50]}")
            continue

        seen_hashes.add(hash_val)
        annonce["hash_dedup"] = hash_val
        unique.append(annonce)

    print_result(True, f"Uniques: {len(unique)}/{len(annonces)}")

    # Tester le doublon inter-source
    if len(unique) >= 2:
        duplicates = dedup.find_duplicates_inter(unique[0], unique[1:])
        print_result(True, f"Inter-source doublons pour annonce 1: {len(duplicates)} trouves")

    return unique


def test_geocodage(annonces: list[dict[str, Any]], dry_run: bool = False) -> list[dict[str, Any]]:
    """Teste le geocodage des adresses."""
    print_step("GEOCODAGE", "API Adresse data.gouv.fr + scoring geo")

    if dry_run:
        print_warning("Mode dry-run : ajout de coordonnees fictives")
        coords_map = {
            "Centre-Ville": (47.2378, 6.0241),
            "Battant": (47.2410, 6.0280),
            "Chablais": (47.2340, 6.0200),
        }
        for annonce in annonces:
            quartier = annonce.get("quartier", "Centre-Ville")
            lat, lon = coords_map.get(quartier, (47.2378, 6.0241))
            annonce["latitude"] = lat
            annonce["longitude"] = lon
            annonce["coordonnees"] = (lat, lon)
            print_result(True, f"Coords mock: {quartier} -> ({lat}, {lon})")
        return annonces

    from src.geo.geocoder import Geocoder
    from src.geo.scoring_geo import GeoScorer

    geocoder = Geocoder()
    geo_scorer = GeoScorer()
    print_result(True, "Geocoder + GeoScorer instancies")

    geocoded: list[dict[str, Any]] = []
    for annonce in annonces:
        adresse = annonce.get("adresse_brute", "")
        if not adresse:
            geocoded.append(annonce)
            continue

        try:
            geo_result = geocoder.geocode(adresse)
            if geo_result:
                annonce["latitude"] = geo_result["latitude"]
                annonce["longitude"] = geo_result["longitude"]
                annonce["coordonnees"] = (geo_result["latitude"], geo_result["longitude"])

                if not annonce.get("quartier"):
                    quartier = geo_scorer.identify_quartier(
                        (geo_result["latitude"], geo_result["longitude"])
                    )
                    if quartier:
                        annonce["quartier"] = quartier

                print_result(
                    True,
                    f"Geocode: {adresse[:40]}... -> ({geo_result['latitude']:.4f}, {geo_result['longitude']:.4f}) score={geo_result['score']:.2f}",
                )
            else:
                print_warning(f"Geocodage echoue: {adresse[:40]}...")
        except Exception as e:
            print_result(False, f"Erreur geocodage: {e}")

        geocoded.append(annonce)

    return geocoded


def test_scoring(annonces: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Teste le scoring (rentabilite + composite)."""
    print_step("SCORING", "Rentabilite brute (4 scenarios) + score composite")

    from src.geo.scoring_geo import GeoScorer
    from src.observatoire.loyers import ObservatoireLoyers
    from src.scoring.composite import CompositeScorer
    from src.scoring.fiscal import FiscalEstimator
    from src.scoring.rentabilite import RentabiliteCalculator

    renta_calc = RentabiliteCalculator()
    composite_scorer = CompositeScorer()
    geo_scorer = GeoScorer()
    observatoire = ObservatoireLoyers()
    fiscal = FiscalEstimator()
    print_result(True, "Tous les scorers instancies")

    scored: list[dict[str, Any]] = []
    for annonce in annonces:
        prix = annonce.get("prix", 0)
        quartier = annonce.get("quartier", "Centre-Ville")
        surface = annonce.get("surface_m2", 0)
        nb_pieces = annonce.get("nb_pieces", 2)

        if not prix or prix <= 0:
            scored.append(annonce)
            continue

        # Estimation loyer
        type_bien = f"T{nb_pieces}" if nb_pieces else "T2"
        loyer_estimate = observatoire.estimate_loyer(
            quartier=quartier,
            type_bien=type_bien,
            meuble=False,
            surface=surface,
        )
        loyer_mensuel = loyer_estimate.get("loyer_estime", 0) or 0
        fiabilite = loyer_estimate.get("fiabilite", "preliminaire")

        # Rentabilite
        renta_data = renta_calc.calculate(
            prix=prix,
            loyer_mensuel=loyer_mensuel,
            charges_copro=annonce.get("charges_copro"),
        )

        # Score geo
        coords = annonce.get("coordonnees")
        geo_score = 50.0
        if coords and isinstance(coords, (tuple, list)) and len(coords) >= 2:
            geo_score = geo_scorer.score_localisation(
                coordonnees=(coords[0], coords[1]),
                quartier=quartier,
            )

        # Tension locative
        annonce_scoring = dict(annonce)
        tension = geo_scorer.get_quartier_tension(quartier)
        annonce_scoring["tension_locative"] = tension

        # Score composite
        score_data = composite_scorer.score(
            annonce_data=annonce_scoring,
            renta_data=renta_data,
            geo_score=geo_score,
        )

        # Fiscal
        charges_annuelles = (annonce.get("charges_copro") or 0) * 12
        fiscal_data = fiscal.estimate(
            loyer_annuel=loyer_mensuel * 12,
            charges=charges_annuelles if charges_annuelles > 0 else None,
        )

        annonce["renta_data"] = renta_data
        annonce["score_data"] = score_data
        annonce["loyer_estime"] = loyer_mensuel
        annonce["fiabilite_loyer"] = fiabilite
        annonce["fiscal_data"] = fiscal_data

        renta_brute = renta_data.get("renta_brute", 0)
        score_global = score_data.get("score_global", 0)
        niveau = score_data.get("niveau_alerte", "veille")

        print_result(
            True,
            f"T{nb_pieces} {quartier} {prix}EUR: renta={renta_brute:.1f}% score={score_global:.0f}/100 [{niveau.upper()}] "
            f"(loyer={loyer_mensuel:.0f}EUR/{fiabilite}, geo={geo_score:.0f}, fiscal={fiscal_data.get('regime_indicatif', '?')})",
        )

        scored.append(annonce)

    return scored


def test_enrichment_ia(annonces: list[dict[str, Any]], dry_run: bool = False) -> list[dict[str, Any]]:
    """Teste l'enrichissement via Claude API."""
    print_step("ENRICHISSEMENT IA", "Claude Haiku - analyse structuree")

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")

    if dry_run or not api_key:
        if not api_key:
            print_warning("ANTHROPIC_API_KEY non configuree")
        print_warning("Mode dry-run : enrichissement mock")

        for annonce in annonces:
            annonce["enrichment"] = {
                "signaux_nego": ["vente rapide"] if "rapide" in annonce.get("description_texte", "") else [],
                "etat_bien": "bon_etat",
                "equipements": ["double_vitrage", "cave"],
                "red_flags": [],
                "info_copro": {"nb_lots": None, "charges_annuelles": None},
                "resume": f"Mock: T{annonce.get('nb_pieces', '?')} a {annonce.get('quartier', '?')}",
            }
            print_result(True, f"Mock enrichment pour {annonce.get('url_source', '?')[:50]}")

        return annonces

    from src.enrichment.claude_enricher import ClaudeEnricher

    enricher = ClaudeEnricher(
        api_key=api_key,
        model=os.environ.get("ANTHROPIC_MODEL", "claude-haiku-4-5-20251001"),
        max_daily_calls=10,  # Limite basse pour les tests
    )
    print_result(True, "ClaudeEnricher instancie")

    # Tester sur la premiere annonce uniquement pour economiser les appels
    annonce = annonces[0] if annonces else None
    if annonce and annonce.get("description_texte"):
        try:
            t0 = time.time()
            result = enricher.enrich(annonce)
            elapsed = time.time() - t0

            if result:
                annonce["enrichment"] = result
                print_result(True, f"Enrichissement reussi en {elapsed:.1f}s")
                print_result(True, f"  signaux_nego: {result.get('signaux_nego', [])}")
                print_result(True, f"  etat_bien: {result.get('etat_bien', '?')}")
                print_result(True, f"  equipements: {result.get('equipements', [])}")
                print_result(True, f"  red_flags: {result.get('red_flags', [])}")
                print_result(True, f"  resume: {result.get('resume', '?')[:80]}...")
            else:
                print_result(False, "Enrichissement retourne None")
        except Exception as e:
            print_result(False, f"Erreur enrichissement: {e}")

        print_result(True, f"Appels API: {enricher.daily_call_count}/{enricher.max_daily_calls}")

    # Les autres annonces gardent un enrichissement mock
    for annonce in annonces[1:]:
        if "enrichment" not in annonce:
            annonce["enrichment"] = None

    return annonces


def test_alertes(annonces: list[dict[str, Any]], dry_run: bool = False) -> dict[str, int]:
    """Teste le formatage et l'envoi d'alertes Telegram."""
    print_step("ALERTES", "Formatage Markdown + envoi Telegram")

    from src.alerts.formatter import AlertFormatter

    formatter = AlertFormatter()
    print_result(True, "AlertFormatter instancie")

    alert_counts: dict[str, int] = {"top": 0, "bon": 0, "veille": 0}

    for annonce in annonces:
        score_data = annonce.get("score_data", {})
        renta_data = annonce.get("renta_data", {})
        enrichment = annonce.get("enrichment")
        niveau = score_data.get("niveau_alerte", "veille")

        # Tester le formatage
        try:
            if niveau == "top":
                message = formatter.format_top_alert(annonce, score_data, renta_data, enrichment)
                alert_counts["top"] += 1
                print_result(True, f"TOP alert formatee ({len(message)} chars)")
            elif niveau == "bon":
                message = formatter.format_bon_alert(annonce, score_data, renta_data)
                alert_counts["bon"] += 1
                print_result(True, f"BON alert formatee ({len(message)} chars)")
            else:
                alert_counts["veille"] += 1
                print_result(True, "VEILLE: stockee sans alerte")
        except Exception as e:
            print_result(False, f"Erreur formatage: {e}")

    # Tester le formatage digest
    try:
        digest = formatter.format_digest(
            top_annonces=[
                {"annonce": a, "score": a.get("score_data", {}), "renta": a.get("renta_data", {})}
                for a in annonces[:3]
            ],
            baisses=[],
            stats={"nb_scrapees": len(annonces), "nb_nouvelles": len(annonces), "nb_erreurs": 0, "sources": ["leboncoin"]},
            obs_stats={"nb_locations": 0, "segments_couverts": 0, "fiabilite": "preliminaire"},
        )
        print_result(True, f"Digest formate ({len(digest)} chars)")
    except Exception as e:
        print_result(False, f"Erreur formatage digest: {e}")

    # Tester le formatage system alert
    try:
        sys_alert = formatter.format_system_alert("test_pipeline", "Test de la pipeline complete")
        print_result(True, f"System alert formatee ({len(sys_alert)} chars)")
    except Exception as e:
        print_result(False, f"Erreur formatage system alert: {e}")

    # Envoi reel Telegram si configure
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")

    if dry_run or not bot_token or not chat_id:
        if not bot_token:
            print_warning("TELEGRAM_BOT_TOKEN non configure")
        if not chat_id:
            print_warning("TELEGRAM_CHAT_ID non configure")
        print_warning("Mode dry-run : pas d'envoi Telegram")
    else:
        try:
            from src.alerts.telegram_bot import TelegramBot

            bot = TelegramBot(bot_token=bot_token, chat_id=chat_id)
            print_result(True, "TelegramBot instancie")

            # Envoyer une alerte de test
            test_message = formatter.format_system_alert(
                "test_pipeline",
                f"Pipeline ImmoScan testee avec succes.\n"
                f"Annonces: {len(annonces)}\n"
                f"TOP: {alert_counts['top']}, BON: {alert_counts['bon']}, VEILLE: {alert_counts['veille']}",
            )
            escaped = bot._escape_markdown(test_message)
            sent = bot.send_alert_sync(escaped, "system")
            print_result(sent, "Alerte de test envoyee sur Telegram")

            # Envoyer la meilleure annonce si TOP ou BON
            best_annonce = None
            best_score = 0
            for a in annonces:
                s = a.get("score_data", {}).get("score_global", 0)
                if s > best_score:
                    best_score = s
                    best_annonce = a

            if best_annonce and best_score > 0:
                best_msg = formatter.format_top_alert(
                    best_annonce,
                    best_annonce.get("score_data", {}),
                    best_annonce.get("renta_data", {}),
                    best_annonce.get("enrichment"),
                )
                escaped_best = bot._escape_markdown(best_msg)
                sent_best = bot.send_alert_sync(escaped_best, "top")
                print_result(sent_best, f"Meilleure annonce envoyee (score={best_score:.0f})")

        except Exception as e:
            print_result(False, f"Erreur Telegram: {e}")

    print(f"\n  Bilan alertes: TOP={alert_counts['top']}, BON={alert_counts['bon']}, VEILLE={alert_counts['veille']}")

    return alert_counts


# ======================================================================
# EXECUTION PRINCIPALE
# ======================================================================


def run_full_pipeline(dry_run: bool = False, step: str | None = None) -> None:
    """Execute la pipeline complete ou une etape specifique."""
    start_time = time.time()

    print(f"\n{BOLD}{'='*60}{RESET}")
    print(f"{BOLD}  IMMOSCAN - TEST PIPELINE COMPLETE{RESET}")
    print(f"{BOLD}  Mode: {'DRY-RUN' if dry_run else 'LIVE'}{RESET}")
    if step:
        print(f"{BOLD}  Etape: {step}{RESET}")
    print(f"{BOLD}{'='*60}{RESET}")

    # Etape 0: Config
    if not step or step == "config":
        test_config()

    # Etape 1: Scraping
    if not step or step in ("scrape", "all"):
        annonces = test_scrape_api(dry_run=dry_run)
    else:
        annonces = list(MOCK_ANNONCES_VENTE)

    if not annonces:
        print(f"\n{RED}Aucune annonce disponible. Pipeline arretee.{RESET}")
        return

    # Etape 2: Validation
    if not step or step in ("validate", "all"):
        annonces = test_validation(annonces)

    # Etape 3: Normalisation
    if not step or step in ("normalize", "all"):
        annonces = test_normalisation(annonces)

    # Etape 4: Dedup
    if not step or step in ("dedup", "all"):
        annonces = test_dedup(annonces)

    # Etape 5: Geocodage
    if not step or step in ("geocode", "all"):
        annonces = test_geocodage(annonces, dry_run=dry_run)

    # Etape 6: Scoring
    if not step or step in ("score", "all"):
        annonces = test_scoring(annonces)

    # Etape 7: Enrichissement IA
    if not step or step in ("enrich", "all"):
        annonces = test_enrichment_ia(annonces, dry_run=dry_run)

    # Etape 8: Alertes
    if not step or step in ("alert", "all"):
        test_alertes(annonces, dry_run=dry_run)

    # Resume
    elapsed = time.time() - start_time
    print(f"\n{BOLD}{'='*60}{RESET}")
    print(f"{BOLD}{GREEN}  PIPELINE TERMINEE en {elapsed:.1f}s{RESET}")
    print(f"{BOLD}  Annonces traitees: {len(annonces)}{RESET}")
    print(f"{BOLD}{'='*60}{RESET}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test pipeline ImmoScan")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Ne pas faire d'appels externes (API LeBonCoin, Claude, Telegram)",
    )
    parser.add_argument(
        "--step",
        choices=["config", "scrape", "validate", "normalize", "dedup", "geocode", "score", "enrich", "alert"],
        help="Tester une etape specifique",
    )
    args = parser.parse_args()

    run_full_pipeline(dry_run=args.dry_run, step=args.step)
