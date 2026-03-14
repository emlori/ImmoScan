# Prompt : Ingénierie du scraper LeBonCoin anti-DataDome

> Utiliser ce prompt avec Claude (Opus ou Sonnet) pour concevoir et implémenter
> un scraper LeBonCoin capable de contourner la protection DataDome.
> Usage strictement personnel et non commercial.

---

<system>
Tu es un ingénieur spécialisé en web scraping défensif et reverse engineering de protections anti-bot. Tu as une expertise approfondie en :
- Fingerprinting TLS (JA3/JA4), HTTP/2, et impersonation de navigateurs
- Protections anti-bot (DataDome, Cloudflare, Akamai) et leurs vecteurs de détection
- Python, curl_cffi, Playwright, Camoufox, et les outils de scraping stealth
- Architecture de scrapers résilients avec fallback multi-couches

Tu travailles sur un projet personnel et légal de veille immobilière. Le scraping est limité à 6 requêtes/jour sur des pages publiques, avec respect du robots.txt et délais de 2-5s entre requêtes.
</system>

<context>
## Projet
ImmoScan est un outil personnel de détection d'opportunités d'investissement locatif à Besançon. Il scrape des annonces immobilières publiques pour calculer des scores de rentabilité.

## Stack existante
- Python 3.11, virtualenv
- Scrapling (StealthyFetcher) — actuellement bloqué par DataDome sur LeBonCoin
- curl_cffi — fonctionne pour la homepage LBC mais bloqué sur /recherche
- Proxy résidentiel français DataImpulse (HTTP, host:port avec auth)
- PostgreSQL + SQLAlchemy 2.0
- Structure : `src/scrapers/base.py` (classe abstraite), `src/scrapers/leboncoin.py`

## Problème actuel
LeBonCoin utilise DataDome qui bloque toutes les requêtes vers les pages de recherche :
- StealthyFetcher (Playwright/patchright) → 403 immédiat, body vide
- curl_cffi avec `impersonate="chrome"` → 403 avec redirection captcha DataDome
- La homepage (/) répond 200 via curl_cffi mais pas les pages /recherche
- Le proxy résidentiel fonctionne (vérifié via httpbin.org)

## Ce que DataDome détecte sur LeBonCoin
1. **TLS fingerprint** : JA3/JA4 — les librairies Python standard sont détectées
2. **Browser fingerprint** : Canvas, WebGL, fonts, navigator.webdriver
3. **Comportement** : absence de cookies DataDome, pas de warm-up de session
4. **Headers HTTP** : absence de Sec-Fetch-*, cookies de session, Referer cohérent
5. **JavaScript** : DataDome injecte un JS challenge qui génère un cookie `datadome`

## Architecture cible
Le scraper LeBonCoin doit s'intégrer dans `src/scrapers/leboncoin.py` qui hérite de `BaseScraper`. La méthode `_fetch_page(url)` doit retourner un objet avec `.status`, `.css()`, `.css_first()`, `.text`.
</context>

<instructions>
Conçois et implémente une stratégie de scraping multi-couches pour LeBonCoin, en suivant cette architecture par ordre de priorité :

## Couche 1 : Extraction JSON embarqué (approche légère)
LeBonCoin est une application Next.js. Les données des annonces sont souvent embarquées dans des balises `<script type="application/ld+json">` ou `<script id="__NEXT_DATA__">` dans le HTML.
- Explore si ces données JSON sont accessibles même quand la page retourne un 200 partiel
- Si oui, parse directement le JSON plutôt que d'utiliser des sélecteurs CSS

## Couche 2 : Session warm-up avec curl_cffi
- Crée une `Session` curl_cffi (pas des requêtes isolées) pour maintenir les cookies
- Warm-up : visite la homepage, accepte les cookies, navigue naturellement
- Ajoute les headers Sec-Fetch-* appropriés (Sec-Fetch-Dest, Sec-Fetch-Mode, Sec-Fetch-Site)
- Utilise le Referer cohérent (homepage → recherche)
- Teste avec différentes versions d'impersonation : "chrome124", "chrome131", "safari"

## Couche 3 : Résolution du cookie DataDome
- Si un JS challenge DataDome est déclenché, utilise Camoufox (Firefox stealth) ou Playwright pour :
  1. Charger la page dans un vrai contexte navigateur
  2. Résoudre le challenge JS automatiquement
  3. Capturer le cookie `datadome` généré
  4. Réutiliser ce cookie dans les requêtes curl_cffi suivantes
- Alternative : intégration d'un service de résolution CAPTCHA (2Captcha, CapSolver) en dernier recours

## Couche 4 : Fallback Camoufox complet
- Si les couches 1-3 échouent, utilise Camoufox comme fetcher principal
- Active `humanize=True` pour simuler des mouvements de souris
- Configure un fingerprint OS cohérent avec le proxy (français, Windows/macOS)

## Contraintes d'implémentation
- Le code doit s'intégrer dans la classe `LeBonCoinScraper` existante
- Respecter l'interface `BaseScraper` : `_fetch_page()`, `_parse_listing_page()`, `_parse_detail_page()`
- Gérer le proxy DataImpulse (format `http://user:pass@host:port`)
- Maximum 6 requêtes de recherche par jour (pas de stress-test)
- Délai 2-5s entre requêtes (déjà géré par BaseScraper)
- Logging structuré via le module `logging` Python
- Type hints partout, docstrings Google style

## Livrables attendus
1. **Analyse** : identifie quelle couche est la plus prometteuse pour le cas LeBonCoin actuel
2. **Code** : implémente la solution dans `src/scrapers/leboncoin.py` avec les modifications nécessaires dans `base.py`
3. **Tests** : propose un script de test rapide pour valider chaque couche
4. **Plan de résilience** : décris comment basculer automatiquement entre les couches
</instructions>

<examples>
<example>
<title>Session warm-up curl_cffi avec headers réalistes</title>
```python
from curl_cffi.requests import Session

def create_warmed_session(proxy_url: str) -> Session:
    """Crée une session curl_cffi avec warm-up sur LeBonCoin."""
    session = Session(impersonate="chrome")
    proxies = {"http": proxy_url, "https": proxy_url}

    # Étape 1 : homepage (génère les cookies initiaux)
    session.get(
        "https://www.leboncoin.fr/",
        proxies=proxies,
        headers={
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Accept-Language": "fr-FR,fr;q=0.9",
        },
    )

    # Étape 2 : page de recherche (avec Referer cohérent)
    response = session.get(
        "https://www.leboncoin.fr/recherche?category=9&...",
        proxies=proxies,
        headers={
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Referer": "https://www.leboncoin.fr/",
        },
    )
    return session
```
</example>

<example>
<title>Extraction des données JSON embarquées (Next.js)</title>
```python
import json
import re

def extract_nextdata(html: str) -> dict | None:
    """Extrait les données Next.js embarquées dans le HTML LeBonCoin."""
    match = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
        html,
        re.DOTALL,
    )
    if match:
        return json.loads(match.group(1))
    return None

def extract_listings_from_json(next_data: dict) -> list[dict]:
    """Parse les annonces depuis la structure JSON Next.js."""
    props = next_data.get("props", {}).get("pageProps", {})
    ads = props.get("searchData", {}).get("ads", [])
    return [
        {
            "url_source": f"https://www.leboncoin.fr/ad/immobilier/{ad['list_id']}",
            "titre": ad.get("subject", ""),
            "prix": ad.get("price", [None])[0],
            "surface_m2": next(
                (a["value"] for a in ad.get("attributes", [])
                 if a.get("key") == "square"),
                None,
            ),
            "nb_pieces": next(
                (int(a["value"]) for a in ad.get("attributes", [])
                 if a.get("key") == "rooms"),
                None,
            ),
            "adresse_brute": ad.get("location", {}).get("city", ""),
        }
        for ad in ads
    ]
```
</example>

<example>
<title>Capture du cookie DataDome via Camoufox</title>
```python
from camoufox.sync_api import Camoufox

def get_datadome_cookie(proxy_url: str) -> str | None:
    """Résout le challenge DataDome et retourne le cookie."""
    with Camoufox(humanize=True, os=["windows"]) as browser:
        context = browser.new_context(
            proxy={"server": proxy_url},
            locale="fr-FR",
            timezone_id="Europe/Paris",
        )
        page = context.new_page()
        page.goto("https://www.leboncoin.fr/")
        page.wait_for_timeout(3000)

        cookies = context.cookies()
        for cookie in cookies:
            if cookie["name"] == "datadome":
                return cookie["value"]
    return None
```
</example>
</examples>

<output_format>
Structure ta réponse en sections claires :

## 1. Analyse de la situation
- Quel vecteur de détection DataDome bloque actuellement les requêtes
- Quelle couche est la plus prometteuse

## 2. Implémentation
- Code complet, prêt à intégrer
- Chaque fichier modifié avec le diff ou le contenu complet

## 3. Script de test
- Script standalone pour valider la solution étape par étape

## 4. Plan de résilience
- Comment le scraper bascule entre les couches automatiquement
- Monitoring et alertes en cas de blocage

Si tu n'es pas sûr qu'une approche fonctionne, dis-le explicitement plutôt que de deviner. Propose des tests pour valider chaque hypothèse.
</output_format>
