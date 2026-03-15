#!/usr/bin/env python3
"""Pipeline complete ImmoScan avec donnees reelles LeBonCoin.

Execute toutes les etapes :
1. Scraping API LeBonCoin (vente)
2. Validation
3. Normalisation
4. Deduplication
5. Geocodage (API data.gouv.fr + coords API LeBonCoin)
6. Scoring (rentabilite + composite + fiscal)
7. Enrichissement IA (Claude Haiku, top 3)
8. Alertes Telegram (TOP, BON, digest)
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
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
logger = logging.getLogger("pipeline")

# Couleurs
G = "\033[92m"
R = "\033[91m"
Y = "\033[93m"
C = "\033[96m"
B = "\033[1m"
X = "\033[0m"


def step(n: int, name: str) -> None:
    print(f"\n{B}{C}[ETAPE {n}] {name}{X}")


# ── Registre de deduplication des alertes ────────────────────────
ALERTES_REGISTRY_PATH = PROJECT_ROOT / "data" / "alertes_sent.json"
REGISTRY_RETENTION_DAYS = 60


def load_registry() -> dict[str, dict[str, Any]]:
    """Charge le registre des alertes deja envoyees depuis le fichier JSON."""
    if not ALERTES_REGISTRY_PATH.exists():
        return {}
    try:
        return json.loads(ALERTES_REGISTRY_PATH.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Registre alertes corrompu, reset: %s", e)
        return {}


def save_registry(registry: dict[str, dict[str, Any]]) -> None:
    """Sauvegarde le registre sur disque."""
    ALERTES_REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
    ALERTES_REGISTRY_PATH.write_text(
        json.dumps(registry, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def purge_registry(registry: dict[str, dict[str, Any]]) -> int:
    """Supprime les entrees de plus de REGISTRY_RETENTION_DAYS jours. Retourne le nombre purgees."""
    cutoff = (datetime.now() - timedelta(days=REGISTRY_RETENTION_DAYS)).isoformat()
    to_remove = [
        url for url, data in registry.items()
        if data.get("date_envoi", "") < cutoff
    ]
    for url in to_remove:
        del registry[url]
    return len(to_remove)


def check_dedup(
    registry: dict[str, dict[str, Any]],
    annonce: dict[str, Any],
    niveau: str,
) -> str:
    """Verifie si une annonce doit etre envoyee.

    Returns:
        'send' : nouvelle annonce, envoyer l'alerte.
        'skip' : deja alertee au meme prix.
        'baisse' : baisse de prix detectee, envoyer alerte baisse.
    """
    url = annonce.get("url_source", "")
    if not url:
        return "send"

    entry = registry.get(url)
    if entry is None:
        return "send"

    prix_actuel = annonce.get("prix", 0)
    prix_registre = entry.get("prix", 0)

    if prix_actuel < prix_registre:
        return "baisse"

    return "skip"


def register_alert(
    registry: dict[str, dict[str, Any]],
    annonce: dict[str, Any],
    niveau: str,
) -> None:
    """Enregistre une alerte envoyee dans le registre."""
    url = annonce.get("url_source", "")
    if not url:
        return
    now = datetime.now().isoformat(timespec="seconds")
    registry[url] = {
        "niveau": niveau,
        "prix": annonce.get("prix", 0),
        "date_envoi": registry.get(url, {}).get("date_envoi", now),
        "date_maj": now,
    }


def main() -> None:
    t0 = time.time()

    print(f"{B}{'='*60}{X}")
    print(f"{B}  IMMOSCAN - PIPELINE COMPLETE DONNEES REELLES{X}")
    print(f"{B}{'='*60}{X}")

    # ============================================================
    # ETAPE 1 : SCRAPING API LEBONCOIN
    # ============================================================
    step(1, "SCRAPING API LEBONCOIN (vente)")

    from src.scrapers.leboncoin import LeBonCoinScraper

    scraper = LeBonCoinScraper()
    raw_annonces = scraper.scrape(scrape_type="vente")
    stats = scraper.get_scraping_stats()
    elapsed = time.time() - t0
    print(f"  Annonces brutes: {len(raw_annonces)} en {elapsed:.1f}s")
    print(
        f"  Stats: scrapees={stats.get('nb_annonces_scrapees', 0)}, "
        f"erreurs={stats.get('nb_erreurs', 0)}"
    )

    if not raw_annonces:
        print(f"{R}  Aucune annonce. Pipeline arretee.{X}")
        sys.exit(1)

    # Echantillon
    for a in raw_annonces[:3]:
        print(
            f"  -> {a.get('prix', '?'):>7} EUR "
            f"{a.get('surface_m2', '?'):>5} m2 "
            f"T{a.get('nb_pieces', '?')} "
            f"DPE:{a.get('dpe', '?')} "
            f"{str(a.get('adresse_brute', '?'))[:40]}"
        )
    if len(raw_annonces) > 3:
        print(f"  ... et {len(raw_annonces) - 3} autres")

    # ============================================================
    # ETAPE 2 : VALIDATION
    # ============================================================
    step(2, "VALIDATION")

    from src.validation.validators import AnnonceValidator

    validator = AnnonceValidator()
    validated: list[dict[str, Any]] = []
    rejected = 0
    for a in raw_annonces:
        ok, reasons = validator.validate_vente(a)
        if ok:
            validated.append(a)
        else:
            rejected += 1
            if rejected <= 3:
                reason_text = reasons[0][:60] if reasons else "?"
                print(f"  {Y}Rejet: {reason_text}{X}")
    print(f"  Validees: {len(validated)}/{len(raw_annonces)} (rejets: {rejected})")

    if not validated:
        print(f"{R}  Aucune annonce valide. Pipeline arretee.{X}")
        sys.exit(1)

    # ============================================================
    # ETAPE 3 : NORMALISATION
    # ============================================================
    step(3, "NORMALISATION")

    from src.parsers.normalizer import AnnonceNormalizer

    normalizer = AnnonceNormalizer()
    normalized: list[dict[str, Any]] = []
    for a in validated:
        try:
            normalized.append(normalizer.normalize_vente(a))
        except Exception as e:
            print(f"  {Y}Erreur norm: {e}{X}")
    print(f"  Normalisees: {len(normalized)}/{len(validated)}")

    # ============================================================
    # ETAPE 4 : DEDUPLICATION
    # ============================================================
    step(4, "DEDUPLICATION")

    from src.parsers.dedup import Deduplicator

    dedup = Deduplicator()
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    dupes = 0
    for a in normalized:
        url = a.get("url_source", "")
        if not url:
            continue
        h = dedup.compute_hash_intra(url)
        if h in seen:
            dupes += 1
            continue
        seen.add(h)
        a["hash_dedup"] = h
        unique.append(a)
    print(f"  Uniques: {len(unique)} (doublons retires: {dupes})")

    # ============================================================
    # ETAPE 5 : GEOCODAGE
    # ============================================================
    step(5, "GEOCODAGE")

    from src.geo.geocoder import Geocoder
    from src.geo.scoring_geo import GeoScorer

    geocoder = Geocoder()
    geo_scorer = GeoScorer()
    geo_ok = 0
    geo_skip = 0
    for a in unique:
        # Si l'API a deja fourni des coords
        if a.get("latitude") and a.get("longitude"):
            a["coordonnees"] = (a["latitude"], a["longitude"])
            if not a.get("quartier"):
                q = geo_scorer.identify_quartier(a["coordonnees"])
                if q:
                    a["quartier"] = q
            geo_ok += 1
            continue

        adresse = a.get("adresse_brute", "")
        if not adresse:
            geo_skip += 1
            continue

        try:
            geo = geocoder.geocode(adresse)
            if geo:
                a["latitude"] = geo["latitude"]
                a["longitude"] = geo["longitude"]
                a["coordonnees"] = (geo["latitude"], geo["longitude"])
                if not a.get("quartier"):
                    q = geo_scorer.identify_quartier(a["coordonnees"])
                    if q:
                        a["quartier"] = q
                geo_ok += 1
            else:
                geo_skip += 1
        except Exception:
            geo_skip += 1
        time.sleep(0.15)

    print(f"  Geocodees: {geo_ok}/{len(unique)} (skip: {geo_skip})")

    # ============================================================
    # ETAPE 6 : SCORING
    # ============================================================
    step(6, "SCORING")

    from src.observatoire.loyers import ObservatoireLoyers
    from src.scoring.composite import CompositeScorer
    from src.scoring.rentabilite import RentabiliteCalculator

    renta_calc = RentabiliteCalculator()
    composite = CompositeScorer()
    obs = ObservatoireLoyers()

    top_annonces: list[dict[str, Any]] = []
    bon_annonces: list[dict[str, Any]] = []
    veille_annonces: list[dict[str, Any]] = []

    for a in unique:
        prix = a.get("prix", 0)
        if not prix or prix <= 0:
            continue

        quartier = a.get("quartier", "Centre-Ville")
        nb = a.get("nb_pieces", 2)
        surface = a.get("surface_m2", 0)

        loyer_nu = obs.estimate_loyer(quartier, f"T{nb}", False, surface)
        loyer_meuble = obs.estimate_loyer(quartier, f"T{nb}", True, surface)
        loyer = loyer_nu.get("loyer_estime", 0) or 0
        loyer_haut = loyer_meuble.get("loyer_estime", 0) or 0
        fiab = loyer_nu.get("fiabilite", "preliminaire")

        renta = renta_calc.calculate(prix, loyer, a.get("charges_copro"))

        coords = a.get("coordonnees")
        geo_score = (
            geo_scorer.score_localisation(coords, quartier)
            if coords
            else 50.0
        )
        tension = geo_scorer.get_quartier_tension(quartier)

        a_s = dict(a)
        a_s["tension_locative"] = tension
        score = composite.score(a_s, renta, geo_score)

        a["renta_data"] = renta
        a["score_data"] = score
        a["loyer_estime"] = loyer
        a["loyer_estime_meuble"] = loyer_haut
        a["fiabilite_loyer"] = fiab

        niveau = score.get("niveau_alerte", "veille")
        if niveau == "top":
            top_annonces.append(a)
        elif niveau == "bon":
            bon_annonces.append(a)
        else:
            veille_annonces.append(a)

    print(
        f"  TOP: {len(top_annonces)}, BON: {len(bon_annonces)}, VEILLE: {len(veille_annonces)}"
    )

    # Afficher les meilleures annonces
    all_scored = top_annonces + bon_annonces + veille_annonces
    all_scored.sort(
        key=lambda x: x.get("score_data", {}).get("score_global", 0),
        reverse=True,
    )
    print("  Top 5 par score:")
    for a in all_scored[:5]:
        sd = a.get("score_data", {})
        rd = a.get("renta_data", {})
        print(
            f"    {sd.get('score_global', 0):>3.0f}/100 "
            f"renta={rd.get('renta_brute', 0):.1f}% "
            f"{a.get('prix', 0):>7} EUR "
            f"{a.get('surface_m2', 0):>5} m2 "
            f"T{a.get('nb_pieces', '?')} "
            f"{str(a.get('quartier', '?'))[:15]} "
            f"[{sd.get('niveau_alerte', '?').upper()}]"
        )

    # ============================================================
    # ETAPE 7 : ENRICHISSEMENT IA (TOP + BON)
    # ============================================================
    to_enrich = top_annonces + bon_annonces
    step(7, f"ENRICHISSEMENT IA ({len(to_enrich)} annonces TOP+BON)")

    from src.enrichment.claude_enricher import ClaudeEnricher

    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        print(f"  {Y}ANTHROPIC_API_KEY manquante, skip enrichissement{X}")
    else:
        enricher = ClaudeEnricher(api_key=api_key, max_daily_calls=20)
        enriched_count = 0

        for a in to_enrich:
            desc = a.get("description_texte", "")
            if not desc:
                print(f"  {Y}Skip (pas de description){X}")
                continue

            try:
                result = enricher.enrich(a)
                if result:
                    a["enrichment"] = result
                    enriched_count += 1
                    print(
                        f"  {G}OK{X} signaux={result['signaux_nego']}, "
                        f"etat={result['etat_bien']}"
                    )
                    print(f"     resume: {result['resume'][:100]}...")
                else:
                    print(f"  {Y}Echec enrichissement{X}")
            except Exception as e:
                print(f"  {R}Erreur: {e}{X}")
            time.sleep(0.5)

        print(f"  Enrichies: {enriched_count}/{len(to_enrich)}")

    # ============================================================
    # ETAPE 8 : ALERTES TELEGRAM (avec deduplication)
    # ============================================================
    step(8, "ALERTES TELEGRAM")

    from src.alerts.formatter import AlertFormatter
    from src.alerts.telegram_bot import TelegramBot

    formatter = AlertFormatter()
    bot = TelegramBot()
    sent_count = 0
    skip_count = 0
    baisse_count = 0

    # Charger le registre de dedup
    registry = load_registry()
    purged = purge_registry(registry)
    if purged:
        print(f"  Registre: {purged} anciennes entrees purgees (>{REGISTRY_RETENTION_DAYS}j)")
    print(f"  Registre: {len(registry)} annonces deja alertees")

    def _send_alert(a: dict[str, Any], niveau: str) -> bool:
        """Envoie une alerte TOP ou BON avec dedup. Retourne True si envoyee."""
        nonlocal sent_count, skip_count, baisse_count

        action = check_dedup(registry, a, niveau)

        if action == "skip":
            skip_count += 1
            return False

        if action == "baisse":
            # Baisse de prix detectee
            entry = registry.get(a.get("url_source", ""), {})
            ancien_prix = entry.get("prix", 0)
            nouveau_prix = a.get("prix", 0)
            msg = formatter.format_baisse_prix(a, ancien_prix, nouveau_prix)
            esc = bot._escape_markdown(msg)
            if bot.send_alert_sync(esc, "baisse_prix"):
                register_alert(registry, a, "baisse_prix")
                baisse_count += 1
                pct = round((ancien_prix - nouveau_prix) / ancien_prix * 100)
                print(
                    f"  {G}BAISSE envoyee{X}: "
                    f"{ancien_prix} -> {nouveau_prix} EUR (-{pct}%) "
                    f"T{a.get('nb_pieces')} {a.get('quartier', '?')}"
                )
            time.sleep(0.5)
            return True

        # action == "send" : nouvelle annonce
        if niveau == "top":
            msg = formatter.format_top_alert(
                a, a.get("score_data", {}), a.get("renta_data", {}),
                a.get("enrichment"),
            )
        else:
            msg = formatter.format_bon_alert(
                a, a.get("score_data", {}), a.get("renta_data", {}),
                a.get("enrichment"),
            )
        esc = bot._escape_markdown(msg)
        if bot.send_alert_sync(esc, niveau):
            register_alert(registry, a, niveau)
            sent_count += 1
            print(
                f"  {G}{niveau.upper()} envoyee{X}: "
                f"{a.get('prix')} EUR T{a.get('nb_pieces')} "
                f"{a.get('quartier', '?')}"
            )
        time.sleep(0.5)
        return True

    # TOP alerts
    for a in top_annonces[:3]:
        _send_alert(a, "top")

    # BON alerts (max 3)
    for a in bon_annonces[:3]:
        _send_alert(a, "bon")

    # Meilleure annonce si aucun TOP/BON envoye
    if sent_count == 0 and baisse_count == 0 and all_scored:
        best = all_scored[0]
        action = check_dedup(registry, best, "top")
        if action != "skip":
            msg = formatter.format_top_alert(
                best, best.get("score_data", {}), best.get("renta_data", {}),
                best.get("enrichment"),
            )
            esc = bot._escape_markdown(msg)
            if bot.send_alert_sync(esc, "top"):
                register_alert(registry, best, "top")
                sent_count += 1
                sg = best["score_data"]["score_global"]
                print(f"  {G}Meilleure annonce envoyee{X}: score={sg:.0f}")
            time.sleep(0.5)

    # Digest (pas de dedup, c'est un resume)
    digest = formatter.format_digest(
        [
            {
                "annonce": a,
                "score": a.get("score_data", {}),
                "renta": a.get("renta_data", {}),
            }
            for a in all_scored[:3]
        ],
        [],
        {
            "nb_scrapees": len(raw_annonces),
            "nb_nouvelles": len(unique),
            "nb_erreurs": rejected,
            "sources": ["leboncoin"],
        },
        {
            "nb_locations": 0,
            "segments_couverts": 0,
            "fiabilite": "preliminaire",
        },
    )
    esc_dig = bot._escape_markdown(digest)
    if bot.send_alert_sync(esc_dig, "digest"):
        sent_count += 1
        print(f"  {G}Digest envoye{X}")

    # Sauvegarder le registre
    save_registry(registry)

    print(
        f"  Telegram: {sent_count} envoyees, {skip_count} skippees (dedup), "
        f"{baisse_count} baisses de prix"
    )

    # ============================================================
    # RESUME FINAL
    # ============================================================
    total_time = time.time() - t0
    print(f"\n{B}{'='*60}{X}")
    print(f"{B}{G}  PIPELINE TERMINEE en {total_time:.1f}s{X}")
    print(f"{B}  Scraping:       {len(raw_annonces)} annonces brutes{X}")
    print(f"{B}  Validation:     {len(validated)} valides / {rejected} rejetees{X}")
    print(f"{B}  Dedup:          {len(unique)} uniques{X}")
    print(f"{B}  Geocodage:      {geo_ok} geocodees{X}")
    print(
        f"{B}  Scoring:        TOP={len(top_annonces)} "
        f"BON={len(bon_annonces)} VEILLE={len(veille_annonces)}{X}"
    )
    print(
        f"{B}  Telegram:       {sent_count} envoyees, "
        f"{skip_count} skippees, {baisse_count} baisses{X}"
    )
    print(f"{B}{'='*60}{X}")


if __name__ == "__main__":
    main()
