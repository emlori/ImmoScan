"""Deduplication des annonces immobilieres.

Fournit la deduplication intra-source (hash URL) et inter-sources
(matching flou sur adresse, surface et prix) conformement aux regles
definies dans CLAUDE.md.
"""

from __future__ import annotations

import hashlib
import logging
import re
from difflib import SequenceMatcher
from typing import Any
from urllib.parse import urlparse, urlunparse

logger = logging.getLogger(__name__)

# Tolerance pour le matching inter-sources
PRICE_TOLERANCE_PERCENT: float = 5.0
SURFACE_TOLERANCE_M2: float = 2.0
ADDRESS_SIMILARITY_THRESHOLD: float = 0.7

# Seuil global : au moins l'adresse ET (prix OU surface) doivent matcher
MIN_MATCHING_CRITERIA: int = 2


class Deduplicator:
    """Deduplication des annonces intra-source et inter-sources.

    La deduplication intra-source repose sur un hash SHA256 de l'URL canonique.
    La deduplication inter-sources repose sur un matching flou combinant :
    - Similarite d'adresse normalisee
    - Surface a +/- 2 m2
    - Prix a +/- 5%

    Example:
        >>> dedup = Deduplicator()
        >>> h = dedup.compute_hash_intra("https://www.leboncoin.fr/ventes/123.htm?foo=bar")
        >>> isinstance(h, str) and len(h) == 64
        True
    """

    def compute_hash_intra(self, url: str) -> str:
        """Calcule le hash SHA256 de l'URL canonique pour la deduplication intra-source.

        L'URL est canonicalisee (scheme minuscule, domaine minuscule,
        suppression des parametres de tracking, suppression du fragment)
        avant le hachage.

        Args:
            url: URL brute de l'annonce.

        Returns:
            Hash SHA256 hexadecimal de 64 caracteres.
        """
        canonical = self._canonicalize_url(url)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def find_duplicates_inter(
        self, annonce: dict[str, Any], existing: list[dict[str, Any]]
    ) -> list[str]:
        """Trouve les doublons inter-sources d'une annonce parmi les existantes.

        Le matching est base sur la combinaison de :
        - Similarite d'adresse normalisee >= 0.7
        - Surface dans un intervalle de +/- 2 m2
        - Prix dans un intervalle de +/- 5%

        Une annonce est consideree comme doublon si l'adresse matche ET
        au moins un des deux autres criteres (surface ou prix) matche.

        Args:
            annonce: Annonce a tester (dict avec cles adresse_brute, surface_m2, prix).
            existing: Liste d'annonces existantes en base.

        Returns:
            Liste des source_ids (url_source) des doublons trouves.
        """
        duplicates: list[str] = []

        annonce_addr = self._normalize_address_for_matching(
            annonce.get("adresse_brute", "")
        )
        annonce_surface = annonce.get("surface_m2")
        annonce_prix = annonce.get("prix")

        if not annonce_addr:
            return duplicates

        for existing_annonce in existing:
            score = 0

            # Critere 1 : Adresse
            existing_addr = self._normalize_address_for_matching(
                existing_annonce.get("adresse_brute", "")
            )
            if existing_addr and self._compute_similarity(annonce_addr, existing_addr) >= ADDRESS_SIMILARITY_THRESHOLD:
                score += 1
            else:
                # Si l'adresse ne matche pas, on passe au suivant
                continue

            # Critere 2 : Surface
            existing_surface = existing_annonce.get("surface_m2")
            if (
                annonce_surface is not None
                and existing_surface is not None
                and abs(float(annonce_surface) - float(existing_surface)) <= SURFACE_TOLERANCE_M2
            ):
                score += 1

            # Critere 3 : Prix
            existing_prix = existing_annonce.get("prix")
            if (
                annonce_prix is not None
                and existing_prix is not None
                and existing_prix > 0
            ):
                price_diff_pct = (
                    abs(float(annonce_prix) - float(existing_prix))
                    / float(existing_prix)
                    * 100
                )
                if price_diff_pct <= PRICE_TOLERANCE_PERCENT:
                    score += 1

            # L'adresse matche (score >= 1), il faut au moins un autre critere
            if score >= MIN_MATCHING_CRITERIA:
                source_id = existing_annonce.get("url_source", "")
                if source_id:
                    duplicates.append(source_id)
                    logger.debug(
                        "Doublon inter-source detecte : %s (score=%d)",
                        source_id,
                        score,
                    )

        return duplicates

    def merge_duplicates(
        self, primary: dict[str, Any], duplicates: list[dict[str, Any]]
    ) -> dict[str, Any]:
        """Fusionne les donnees d'un doublon avec l'annonce principale.

        La strategie est de garder la valeur la plus complete :
        - Pour chaque champ, la valeur non-None est privilegiee
        - Pour les listes (photos), on concatene et deduplique
        - Le champ source_ids est mis a jour avec les URLs des doublons

        Args:
            primary: Annonce principale (la plus ancienne ou la plus complete).
            duplicates: Liste des annonces doublons a fusionner.

        Returns:
            Annonce fusionnee avec les donnees les plus completes.
        """
        merged = dict(primary)

        # Collecter les source_ids
        source_ids: list[str] = list(merged.get("source_ids", []))
        for dup in duplicates:
            dup_url = dup.get("url_source", "")
            if dup_url and dup_url not in source_ids:
                source_ids.append(dup_url)
        merged["source_ids"] = source_ids

        # Fusionner les champs en privilegiant les valeurs non-None
        fields_to_merge = [
            "dpe",
            "etage",
            "adresse_brute",
            "quartier",
            "charges_copro",
            "description_texte",
            "date_publication",
            "completude_score",
        ]

        for field in fields_to_merge:
            if merged.get(field) is None:
                for dup in duplicates:
                    if dup.get(field) is not None:
                        merged[field] = dup[field]
                        break

        # Fusionner les photos (concatenation + deduplication)
        all_photos: list[str] = list(merged.get("photos_urls", []))
        for dup in duplicates:
            for photo in dup.get("photos_urls", []):
                if photo not in all_photos:
                    all_photos.append(photo)
        merged["photos_urls"] = all_photos

        # Garder la completude_score la plus elevee
        scores = [merged.get("completude_score") or 0.0]
        for dup in duplicates:
            score = dup.get("completude_score")
            if score is not None:
                scores.append(score)
        merged["completude_score"] = max(scores)

        return merged

    def _normalize_address_for_matching(self, address: str) -> str:
        """Normalise agressivement une adresse pour la comparaison.

        Supprime la ponctuation, les accents, les mots outils, et
        normalise les types de voie pour maximiser les chances de matching.

        Args:
            address: Adresse brute.

        Returns:
            Adresse normalisee pour la comparaison (minuscule, sans accents).
        """
        if not address:
            return ""

        addr = str(address).lower().strip()

        # Suppression des accents courants
        replacements = {
            "é": "e", "è": "e", "ê": "e", "ë": "e",
            "à": "a", "â": "a", "ä": "a",
            "ù": "u", "û": "u", "ü": "u",
            "î": "i", "ï": "i",
            "ô": "o", "ö": "o",
            "ç": "c",
        }
        for accented, plain in replacements.items():
            addr = addr.replace(accented, plain)

        # Normalisation des types de voie
        voie_map = {
            "avenue": "av",
            "boulevard": "bd",
            "place": "pl",
            "impasse": "imp",
            "allee": "al",
            "passage": "pass",
            "chemin": "ch",
            "route": "rte",
            "faubourg": "fg",
            "square": "sq",
            "cours": "crs",
        }
        for long_form, short in voie_map.items():
            addr = re.sub(r"\b" + long_form + r"\b", short, addr)

        # Normalisation de "rue" (garder tel quel, c'est courant)
        # Suppression des mots outils
        stop_words = {"de", "du", "des", "le", "la", "les", "l", "d", "en", "au", "aux"}
        parts = addr.split()
        parts = [p for p in parts if p not in stop_words]

        # Suppression de la ponctuation
        addr = " ".join(parts)
        addr = re.sub(r"[^\w\s]", "", addr)
        addr = re.sub(r"\s+", " ", addr).strip()

        return addr

    def _compute_similarity(self, a: str, b: str) -> float:
        """Calcule la similarite entre deux chaines avec SequenceMatcher.

        Args:
            a: Premiere chaine.
            b: Deuxieme chaine.

        Returns:
            Score de similarite entre 0.0 et 1.0.
        """
        if not a or not b:
            return 0.0
        return SequenceMatcher(None, a, b).ratio()

    def _canonicalize_url(self, url: str) -> str:
        """Canonicalise une URL pour le hachage.

        - Scheme et domaine en minuscules
        - Suppression du fragment
        - Suppression des parametres de tracking courants
        - Suppression du slash final sur le path

        Args:
            url: URL brute.

        Returns:
            URL canonicalisee.
        """
        url = url.strip()
        if not url:
            return ""

        parsed = urlparse(url)

        # Minuscule pour scheme et domaine
        scheme = parsed.scheme.lower()
        netloc = parsed.netloc.lower()
        path = parsed.path

        # Suppression du slash final (sauf si c'est la racine)
        if path and path != "/" and path.endswith("/"):
            path = path.rstrip("/")

        # Filtrage des parametres de tracking
        tracking_params = {
            "utm_source", "utm_medium", "utm_campaign", "utm_content",
            "utm_term", "fbclid", "gclid", "ref", "tracking",
        }
        if parsed.query:
            params = parsed.query.split("&")
            filtered = []
            for param in params:
                key = param.split("=")[0].lower()
                if key not in tracking_params:
                    filtered.append(param)
            query = "&".join(sorted(filtered))
        else:
            query = ""

        # Reconstruction sans fragment
        return urlunparse((scheme, netloc, path, parsed.params, query, ""))
