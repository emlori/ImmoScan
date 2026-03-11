"""Geocodage des adresses via l'API Adresse data.gouv.fr.

Fournit un geocoder avec cache local (memoire + fichier) pour convertir
les adresses brutes en coordonnees GPS, en ciblant specifiquement Besancon.

Utilise httpx pour les appels HTTP avec gestion des retries et rate limiting.
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from pathlib import Path
from typing import Any

import httpx

from src.config import PROJECT_ROOT, get_settings

logger = logging.getLogger(__name__)

# Repertoire de cache fichier
CACHE_DIR = PROJECT_ROOT / ".cache" / "geocode"


class Geocoder:
    """Geocoder utilisant l'API Adresse data.gouv.fr avec cache.

    Convertit des adresses textuelles en coordonnees GPS en ciblant
    Besancon (25000). Implemente un cache a deux niveaux :
    - Cache memoire (dict) pour les appels repetes dans une meme session.
    - Cache fichier (.cache/geocode/) pour la persistance entre sessions.

    Attributes:
        api_url: URL de l'API de geocodage.
        cache_ttl: Duree de vie du cache en secondes.
        _memory_cache: Cache memoire interne.
        _last_request_time: Timestamp du dernier appel API.
        _min_delay: Delai minimum entre deux appels API (secondes).
        _max_retries: Nombre maximum de tentatives en cas d'erreur.
    """

    def __init__(
        self,
        api_url: str | None = None,
        cache_ttl: int = 86400,
        cache_dir: Path | None = None,
        min_delay: float = 0.1,
        max_retries: int = 3,
    ) -> None:
        """Initialise le geocoder.

        Args:
            api_url: URL de l'API de geocodage. Si None, charge depuis la config YAML.
            cache_ttl: Duree de vie du cache en secondes (defaut: 24h).
            cache_dir: Repertoire pour le cache fichier. Si None, utilise .cache/geocode/.
            min_delay: Delai minimum entre requetes API en secondes.
            max_retries: Nombre maximum de tentatives en cas d'erreur reseau.
        """
        if api_url is not None:
            self.api_url = api_url
        else:
            try:
                settings = get_settings()
                quartiers_config = settings.load_quartiers()
                self.api_url = quartiers_config.get("geocodage", {}).get(
                    "api_url", "https://api-adresse.data.gouv.fr/search/"
                )
            except (FileNotFoundError, Exception):
                self.api_url = "https://api-adresse.data.gouv.fr/search/"

        self.cache_ttl = cache_ttl
        self._cache_dir = cache_dir or CACHE_DIR
        self._memory_cache: dict[str, dict[str, Any]] = {}
        self._last_request_time: float = 0.0
        self._min_delay = min_delay
        self._max_retries = max_retries

        # Creer le repertoire de cache si necessaire
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    def geocode(self, address: str) -> dict[str, Any] | None:
        """Geocode une adresse en coordonnees GPS.

        Interroge l'API Adresse data.gouv.fr pour convertir une adresse textuelle
        en coordonnees GPS (latitude, longitude). Le resultat est mis en cache.

        Args:
            address: Adresse textuelle a geocoder (ex: "12 rue de la Republique, 25000 Besancon").

        Returns:
            Dictionnaire contenant:
                - latitude (float): Latitude WGS84.
                - longitude (float): Longitude WGS84.
                - label (str): Adresse formatee retournee par l'API.
                - score (float): Score de confiance du geocodage (0-1).
                - city (str): Nom de la ville.
                - postcode (str): Code postal.
            Retourne None si le geocodage echoue ou ne produit aucun resultat.
        """
        if not address or not address.strip():
            logger.warning("Adresse vide fournie au geocoder.")
            return None

        address_clean = address.strip()
        cache_key = self._make_cache_key(address_clean)

        # 1. Verifier le cache memoire
        cached = self._get_from_memory_cache(cache_key)
        if cached is not None:
            logger.debug("Geocodage depuis le cache memoire : %s", address_clean)
            return cached

        # 2. Verifier le cache fichier
        cached = self._get_from_file_cache(cache_key)
        if cached is not None:
            self._memory_cache[cache_key] = cached
            logger.debug("Geocodage depuis le cache fichier : %s", address_clean)
            return cached

        # 3. Appeler l'API
        result = self._call_api(address_clean)
        if result is not None:
            self._save_to_memory_cache(cache_key, result)
            self._save_to_file_cache(cache_key, result)
            logger.info("Geocodage reussi pour : %s", address_clean)
        else:
            logger.warning("Geocodage echoue pour : %s", address_clean)

        return result

    def _call_api(self, address: str) -> dict[str, Any] | None:
        """Appelle l'API de geocodage avec rate limiting et retries.

        Args:
            address: Adresse nettoyee a geocoder.

        Returns:
            Dictionnaire de resultat ou None en cas d'echec.
        """
        self._rate_limit()

        params: dict[str, str | int] = {
            "q": address,
            "limit": 1,
            "postcode": "25000",
        }

        for attempt in range(self._max_retries):
            try:
                with httpx.Client(timeout=10.0) as client:
                    response = client.get(self.api_url, params=params)

                self._last_request_time = time.monotonic()

                if response.status_code == 429:
                    wait_time = 2 ** attempt
                    logger.warning(
                        "Rate limit atteint (429), attente %ds avant retry %d/%d",
                        wait_time,
                        attempt + 1,
                        self._max_retries,
                    )
                    time.sleep(wait_time)
                    continue

                response.raise_for_status()
                data = response.json()

                return self._parse_response(data)

            except httpx.TimeoutException:
                logger.warning(
                    "Timeout lors du geocodage (tentative %d/%d)",
                    attempt + 1,
                    self._max_retries,
                )
                if attempt < self._max_retries - 1:
                    time.sleep(1.0 * (attempt + 1))

            except httpx.HTTPStatusError as e:
                logger.error(
                    "Erreur HTTP %d lors du geocodage (tentative %d/%d)",
                    e.response.status_code,
                    attempt + 1,
                    self._max_retries,
                )
                if attempt < self._max_retries - 1:
                    time.sleep(1.0 * (attempt + 1))

            except httpx.RequestError as e:
                logger.error(
                    "Erreur reseau lors du geocodage : %s (tentative %d/%d)",
                    str(e),
                    attempt + 1,
                    self._max_retries,
                )
                if attempt < self._max_retries - 1:
                    time.sleep(1.0 * (attempt + 1))

        return None

    def _parse_response(self, data: dict[str, Any]) -> dict[str, Any] | None:
        """Parse la reponse de l'API Adresse.

        Args:
            data: Reponse JSON de l'API.

        Returns:
            Dictionnaire avec les informations de geocodage ou None.
        """
        features = data.get("features", [])
        if not features:
            return None

        feature = features[0]
        properties = feature.get("properties", {})
        geometry = feature.get("geometry", {})
        coordinates = geometry.get("coordinates", [])

        if len(coordinates) < 2:
            return None

        return {
            "latitude": coordinates[1],
            "longitude": coordinates[0],
            "label": properties.get("label", ""),
            "score": properties.get("score", 0.0),
            "city": properties.get("city", ""),
            "postcode": properties.get("postcode", ""),
        }

    def _rate_limit(self) -> None:
        """Applique le rate limiting entre les appels API."""
        if self._last_request_time > 0:
            elapsed = time.monotonic() - self._last_request_time
            if elapsed < self._min_delay:
                time.sleep(self._min_delay - elapsed)

    @staticmethod
    def _make_cache_key(address: str) -> str:
        """Genere une cle de cache deterministe pour une adresse.

        Args:
            address: Adresse nettoyee.

        Returns:
            Hash SHA256 de l'adresse normalisee.
        """
        normalized = address.lower().strip()
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

    def _get_from_memory_cache(self, key: str) -> dict[str, Any] | None:
        """Recupere un resultat depuis le cache memoire.

        Args:
            key: Cle de cache.

        Returns:
            Resultat de geocodage ou None si absent/expire.
        """
        entry = self._memory_cache.get(key)
        if entry is None:
            return None

        timestamp = entry.get("_cached_at", 0)
        if time.time() - timestamp > self.cache_ttl:
            del self._memory_cache[key]
            return None

        # Retourner une copie sans le champ interne
        result = {k: v for k, v in entry.items() if k != "_cached_at"}
        return result

    def _save_to_memory_cache(self, key: str, result: dict[str, Any]) -> None:
        """Sauvegarde un resultat dans le cache memoire.

        Args:
            key: Cle de cache.
            result: Resultat de geocodage a cacher.
        """
        entry = {**result, "_cached_at": time.time()}
        self._memory_cache[key] = entry

    def _get_from_file_cache(self, key: str) -> dict[str, Any] | None:
        """Recupere un resultat depuis le cache fichier.

        Args:
            key: Cle de cache.

        Returns:
            Resultat de geocodage ou None si absent/expire.
        """
        cache_file = self._cache_dir / f"{key}.json"
        if not cache_file.exists():
            return None

        try:
            with open(cache_file, encoding="utf-8") as f:
                data = json.load(f)

            timestamp = data.get("_cached_at", 0)
            if time.time() - timestamp > self.cache_ttl:
                cache_file.unlink(missing_ok=True)
                return None

            result = {k: v for k, v in data.items() if k != "_cached_at"}
            return result

        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Erreur de lecture du cache fichier %s : %s", cache_file, e)
            return None

    def _save_to_file_cache(self, key: str, result: dict[str, Any]) -> None:
        """Sauvegarde un resultat dans le cache fichier.

        Args:
            key: Cle de cache.
            result: Resultat de geocodage a persister.
        """
        cache_file = self._cache_dir / f"{key}.json"
        try:
            data = {**result, "_cached_at": time.time()}
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except OSError as e:
            logger.warning("Erreur d'ecriture du cache fichier %s : %s", cache_file, e)

    def clear_cache(self) -> None:
        """Vide les caches memoire et fichier."""
        self._memory_cache.clear()
        if self._cache_dir.exists():
            for cache_file in self._cache_dir.glob("*.json"):
                cache_file.unlink(missing_ok=True)
        logger.info("Caches geocodage vides.")
