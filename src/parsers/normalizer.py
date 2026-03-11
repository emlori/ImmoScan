"""Normalisation des donnees brutes scrapees.

Nettoie, normalise et enrichit les donnees extraites depuis les differentes
sources (LeBonCoin, PAP, SeLoger) avant validation et stockage en base.
"""

from __future__ import annotations

import html
import logging
import re
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import urljoin, urlparse

logger = logging.getLogger(__name__)

# Abreviations d'adresses francaises : forme courte -> forme longue
_ADDRESS_ABBREVIATIONS: dict[str, str] = {
    "r.": "Rue",
    "r ": "Rue ",
    "av.": "Avenue",
    "av ": "Avenue ",
    "bd.": "Boulevard",
    "bd ": "Boulevard ",
    "bld.": "Boulevard",
    "bld ": "Boulevard ",
    "pl.": "Place",
    "pl ": "Place ",
    "imp.": "Impasse",
    "imp ": "Impasse ",
    "all.": "Allee",
    "all ": "Allee ",
    "crs.": "Cours",
    "crs ": "Cours ",
    "fg.": "Faubourg",
    "fg ": "Faubourg ",
    "sq.": "Square",
    "sq ": "Square ",
    "rte.": "Route",
    "rte ": "Route ",
    "chem.": "Chemin",
    "chem ": "Chemin ",
    "pass.": "Passage",
    "pass ": "Passage ",
}

# Mois francais pour le parsing de dates
_MOIS_FR: dict[str, int] = {
    "janvier": 1,
    "fevrier": 2,
    "février": 2,
    "mars": 3,
    "avril": 4,
    "mai": 5,
    "juin": 6,
    "juillet": 7,
    "aout": 8,
    "août": 8,
    "septembre": 9,
    "octobre": 10,
    "novembre": 11,
    "decembre": 12,
    "décembre": 12,
}

# Mots-cles indicateurs de logement meuble
_MEUBLE_KEYWORDS: list[str] = [
    "meublé",
    "meuble",
    "meublée",
    "meublee",
    "équipé",
    "equipe",
    "équipée",
    "equipee",
    "furnished",
    "tout équipé",
    "tout equipe",
    "cuisine équipée",
    "cuisine equipee",
    "entièrement meublé",
    "entierement meuble",
]

# Champs optionnels pour le calcul du score de completude (vente)
_OPTIONAL_FIELDS_VENTE: list[str] = [
    "dpe",
    "etage",
    "adresse_brute",
    "quartier",
    "charges_copro",
    "description_texte",
    "photos_urls",
    "date_publication",
]

# Champs optionnels pour le calcul du score de completude (location)
_OPTIONAL_FIELDS_LOCATION: list[str] = [
    "loyer_hc",
    "dpe",
    "meuble",
    "quartier",
    "adresse_brute",
    "description_texte",
    "date_publication",
]


class AnnonceNormalizer:
    """Normalisation des donnees brutes d'annonces immobilieres.

    Nettoie et standardise les champs extraits par les scrapers pour
    garantir la coherence des donnees avant validation et stockage.

    Example:
        >>> normalizer = AnnonceNormalizer()
        >>> raw = {"prix": "145 000 €", "surface_m2": "55,5 m²", "nb_pieces": "3"}
        >>> result = normalizer.normalize_vente(raw)
        >>> result["prix"]
        145000
    """

    def normalize_vente(self, raw_data: dict[str, Any]) -> dict[str, Any]:
        """Normalise les donnees brutes d'une annonce de vente.

        Args:
            raw_data: Dictionnaire contenant les donnees brutes scrapees.

        Returns:
            Dictionnaire avec les donnees nettoyees et normalisees.
        """
        result: dict[str, Any] = {}

        # Champs obligatoires
        result["url_source"] = str(raw_data.get("url_source", "")).strip()
        result["source"] = str(raw_data.get("source", "")).strip().lower()
        result["prix"] = self._clean_price(raw_data.get("prix"))
        result["surface_m2"] = self._clean_surface(raw_data.get("surface_m2"))
        result["nb_pieces"] = self._clean_nb_pieces(raw_data.get("nb_pieces"))

        # Champs optionnels
        result["dpe"] = self._clean_dpe(raw_data.get("dpe"))
        result["etage"] = self._clean_etage(raw_data.get("etage"))
        result["adresse_brute"] = self._normalize_adresse(
            raw_data.get("adresse_brute")
        )
        result["quartier"] = (
            str(raw_data.get("quartier", "")).strip() or None
        )
        result["charges_copro"] = self._clean_float(raw_data.get("charges_copro"))
        result["description_texte"] = self._clean_text(
            raw_data.get("description_texte")
        )
        result["photos_urls"] = self._normalize_photos(
            raw_data.get("photos_urls"), raw_data.get("url_source", "")
        )
        result["date_publication"] = self._parse_date(
            raw_data.get("date_publication")
        )

        # Score de completude
        result["completude_score"] = self._compute_completude(
            result, _OPTIONAL_FIELDS_VENTE
        )

        return result

    def normalize_location(self, raw_data: dict[str, Any]) -> dict[str, Any]:
        """Normalise les donnees brutes d'une annonce de location.

        Args:
            raw_data: Dictionnaire contenant les donnees brutes scrapees.

        Returns:
            Dictionnaire avec les donnees nettoyees et normalisees.
        """
        result: dict[str, Any] = {}

        # Champs obligatoires
        result["url_source"] = str(raw_data.get("url_source", "")).strip()
        result["source"] = str(raw_data.get("source", "")).strip().lower()
        result["loyer_cc"] = self._clean_float(raw_data.get("loyer_cc"))
        result["surface_m2"] = self._clean_surface(raw_data.get("surface_m2"))
        result["nb_pieces"] = self._clean_nb_pieces(raw_data.get("nb_pieces"))

        # Champs optionnels
        result["loyer_hc"] = self._clean_float(raw_data.get("loyer_hc"))
        result["dpe"] = self._clean_dpe(raw_data.get("dpe"))
        result["quartier"] = (
            str(raw_data.get("quartier", "")).strip() or None
        )
        result["adresse_brute"] = self._normalize_adresse(
            raw_data.get("adresse_brute")
        )
        result["description_texte"] = self._clean_text(
            raw_data.get("description_texte")
        )
        result["date_publication"] = self._parse_date(
            raw_data.get("date_publication")
        )

        # Detection meuble
        result["meuble"] = self._detect_meuble(raw_data, result)

        # Score de completude
        result["completude_score"] = self._compute_completude(
            result, _OPTIONAL_FIELDS_LOCATION
        )

        return result

    def _clean_text(self, text: Any) -> str | None:
        """Nettoie un texte brut : supprime HTML, normalise les espaces.

        Args:
            text: Texte brut potentiellement contenant du HTML.

        Returns:
            Texte nettoye ou None si vide.
        """
        if text is None:
            return None
        text = str(text)

        # Suppression des balises HTML
        text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"<[^>]+>", " ", text)

        # Decodage des entites HTML
        text = html.unescape(text)

        # Normalisation des espaces
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = text.strip()

        return text if text else None

    def _normalize_adresse(self, adresse: Any) -> str | None:
        """Standardise le format d'une adresse.

        Capitalise chaque mot, expande les abreviations de voie,
        et assure la presence de Besancon si possible.

        Args:
            adresse: Adresse brute.

        Returns:
            Adresse normalisee ou None si vide.
        """
        if adresse is None:
            return None
        adresse = str(adresse).strip()
        if not adresse:
            return None

        # Suppression des sauts de ligne et espaces multiples
        adresse = re.sub(r"\s+", " ", adresse).strip()

        # Passage en minuscule pour le traitement, puis capitalisation
        adresse_lower = adresse.lower()

        # Expansion des abreviations de voie
        for abbrev, full in _ADDRESS_ABBREVIATIONS.items():
            # Chercher l'abreviation au debut du texte ou apres un numero
            pattern = re.compile(
                r"(?<=\d\s)" + re.escape(abbrev) + r"|^" + re.escape(abbrev),
                re.IGNORECASE,
            )
            adresse_lower = pattern.sub(full.lower(), adresse_lower)

        # Capitalisation de chaque mot significatif
        parts = adresse_lower.split()
        minor_words = {"de", "du", "des", "le", "la", "les", "l", "d", "en"}
        capitalized_parts: list[str] = []
        for i, part in enumerate(parts):
            # Toujours capitaliser le premier mot et les mots non mineurs
            if i == 0 or part not in minor_words:
                capitalized_parts.append(part.capitalize())
            else:
                capitalized_parts.append(part)
        adresse = " ".join(capitalized_parts)

        return adresse if adresse else None

    def _parse_date(self, date_str: Any) -> datetime | None:
        """Parse une date dans differents formats francais.

        Formats supportes :
        - "10 mars 2026" / "10 Mars 2026"
        - "10/03/2026" / "10-03-2026"
        - "2026-03-10" (ISO)
        - "2026-03-10T14:30:00" (ISO avec heure)
        - "Aujourd'hui" / "aujourd'hui"
        - "Hier" / "hier"
        - Objet datetime (passe tel quel)

        Args:
            date_str: Chaine de date a parser ou objet datetime.

        Returns:
            Objet datetime ou None si le parsing echoue.
        """
        if date_str is None:
            return None
        if isinstance(date_str, datetime):
            return date_str

        date_str = str(date_str).strip()
        if not date_str:
            return None

        # Mots-cles relatifs
        date_lower = date_str.lower()
        now = datetime.now()
        if date_lower in ("aujourd'hui", "aujourd hui", "aujourdhui"):
            return now.replace(hour=0, minute=0, second=0, microsecond=0)
        if date_lower == "hier":
            yesterday = now - timedelta(days=1)
            return yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

        # Format ISO : 2026-03-10 ou 2026-03-10T14:30:00
        iso_match = re.match(
            r"(\d{4})-(\d{2})-(\d{2})(?:T(\d{2}):(\d{2}):(\d{2}))?",
            date_str,
        )
        if iso_match:
            year = int(iso_match.group(1))
            month = int(iso_match.group(2))
            day = int(iso_match.group(3))
            hour = int(iso_match.group(4) or 0)
            minute = int(iso_match.group(5) or 0)
            second = int(iso_match.group(6) or 0)
            try:
                return datetime(year, month, day, hour, minute, second)
            except ValueError:
                return None

        # Format DD/MM/YYYY ou DD-MM-YYYY
        slash_match = re.match(
            r"(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})", date_str
        )
        if slash_match:
            day = int(slash_match.group(1))
            month = int(slash_match.group(2))
            year = int(slash_match.group(3))
            try:
                return datetime(year, month, day)
            except ValueError:
                return None

        # Format francais : "10 mars 2026"
        fr_match = re.match(
            r"(\d{1,2})\s+(\w+)\s+(\d{4})", date_str, re.IGNORECASE
        )
        if fr_match:
            day = int(fr_match.group(1))
            month_str = fr_match.group(2).lower()
            year = int(fr_match.group(3))
            month = _MOIS_FR.get(month_str)
            if month:
                try:
                    return datetime(year, month, day)
                except ValueError:
                    return None

        logger.debug("Impossible de parser la date : %s", date_str)
        return None

    def _clean_price(self, prix: Any) -> int | None:
        """Nettoie et convertit un prix en entier.

        Gere les formats : "145 000 €", "145000€", "145,000", "145000".

        Args:
            prix: Valeur brute du prix.

        Returns:
            Prix en entier ou None si la conversion echoue.
        """
        if prix is None:
            return None
        if isinstance(prix, (int, float)):
            return int(prix)
        prix_str = str(prix)
        # Supprimer tout sauf les chiffres
        cleaned = re.sub(r"[^\d]", "", prix_str)
        if not cleaned:
            return None
        try:
            return int(cleaned)
        except ValueError:
            return None

    def _clean_surface(self, surface: Any) -> float | None:
        """Nettoie et convertit une surface en float.

        Gere les formats : "42 m²", "42.5m²", "42,5 m2", "42.5".

        Args:
            surface: Valeur brute de la surface.

        Returns:
            Surface en float ou None si la conversion echoue.
        """
        if surface is None:
            return None
        if isinstance(surface, (int, float)):
            return float(surface)
        surface_str = str(surface)
        # Supprimer "m²", "m2", "m" en fin de chaine et les espaces
        surface_str = re.sub(r"\s*m[²2]?\s*$", "", surface_str, flags=re.IGNORECASE)
        surface_str = surface_str.strip()
        # Remplacer la virgule par un point
        surface_str = surface_str.replace(",", ".")
        # Supprimer les espaces internes (ex: "42 .5")
        surface_str = surface_str.replace(" ", "")
        if not surface_str:
            return None
        try:
            return float(surface_str)
        except ValueError:
            return None

    def _clean_nb_pieces(self, nb_pieces: Any) -> int | None:
        """Extrait le nombre de pieces en entier.

        Gere les formats : "3", "T3", "3 pieces", "3 pièces", 3.

        Args:
            nb_pieces: Valeur brute du nombre de pieces.

        Returns:
            Nombre de pieces en entier ou None si la conversion echoue.
        """
        if nb_pieces is None:
            return None
        if isinstance(nb_pieces, int):
            return nb_pieces
        if isinstance(nb_pieces, float):
            return int(nb_pieces)
        nb_str = str(nb_pieces).strip()
        # Extraire le premier nombre
        match = re.search(r"(\d+)", nb_str)
        if match:
            return int(match.group(1))
        return None

    def _clean_dpe(self, dpe: Any) -> str | None:
        """Normalise le DPE en lettre majuscule A-G.

        Args:
            dpe: Valeur brute du DPE.

        Returns:
            Lettre DPE (A-G) ou None si invalide/absent.
        """
        if dpe is None:
            return None
        dpe_str = str(dpe).strip().upper()
        if len(dpe_str) == 1 and dpe_str in "ABCDEFG":
            return dpe_str
        # Tenter d'extraire la premiere lettre A-G
        match = re.search(r"[A-G]", dpe_str)
        if match:
            return match.group(0)
        return None

    def _clean_etage(self, etage: Any) -> int | None:
        """Nettoie et convertit un etage en entier.

        Gere les formats : "3ème étage", "RDC", "1er", "2", "rez-de-chaussée".

        Args:
            etage: Valeur brute de l'etage.

        Returns:
            Numero d'etage (0 pour RDC) ou None si absent.
        """
        if etage is None:
            return None
        if isinstance(etage, int):
            return etage
        if isinstance(etage, float):
            return int(etage)
        etage_str = str(etage).strip().lower()
        if not etage_str:
            return None

        # Rez-de-chaussee
        if etage_str in ("rdc", "rez-de-chaussee", "rez-de-chaussée", "rez de chaussee", "rez de chaussée"):
            return 0

        # Extraire le premier nombre
        match = re.search(r"(\d+)", etage_str)
        if match:
            return int(match.group(1))
        return None

    def _clean_float(self, value: Any) -> float | None:
        """Convertit une valeur en float.

        Gere les formats avec virgule, espaces, symbole euro.

        Args:
            value: Valeur brute.

        Returns:
            Valeur en float ou None.
        """
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        val_str = str(value).strip()
        # Supprimer symboles monetaires et espaces
        val_str = re.sub(r"[€$\s]", "", val_str)
        val_str = val_str.replace(",", ".")
        if not val_str:
            return None
        try:
            return float(val_str)
        except ValueError:
            return None

    def _normalize_photos(
        self, photos: Any, base_url: str = ""
    ) -> list[str]:
        """Normalise les URLs de photos en URLs absolues.

        Args:
            photos: Liste brute d'URLs (ou None).
            base_url: URL de base pour les chemins relatifs.

        Returns:
            Liste d'URLs absolues nettoyees.
        """
        if not photos:
            return []
        if isinstance(photos, str):
            photos = [photos]
        if not isinstance(photos, list):
            return []

        result: list[str] = []
        for photo in photos:
            photo_str = str(photo).strip()
            if not photo_str:
                continue
            # Si c'est un chemin relatif, construire l'URL absolue
            parsed = urlparse(photo_str)
            if not parsed.scheme:
                if base_url:
                    photo_str = urljoin(base_url, photo_str)
                else:
                    continue
            result.append(photo_str)
        return result

    def _detect_meuble(
        self, raw_data: dict[str, Any], normalized: dict[str, Any]
    ) -> bool | None:
        """Detecte si un logement est meuble a partir des donnees.

        Cherche d'abord un champ explicite 'meuble', puis analyse
        le texte de description pour des mots-cles indicateurs.

        Args:
            raw_data: Donnees brutes originales.
            normalized: Donnees normalisees (contient description nettoyee).

        Returns:
            True si meuble, False si non-meuble, None si indetermine.
        """
        # Champ explicite
        meuble_explicit = raw_data.get("meuble")
        if isinstance(meuble_explicit, bool):
            return meuble_explicit
        if isinstance(meuble_explicit, str):
            lower = meuble_explicit.strip().lower()
            if lower in ("true", "oui", "1", "meublé", "meuble", "meublée", "meublee"):
                return True
            if lower in ("false", "non", "0", "vide", "nu", "nue"):
                return False

        # Detection dans la description
        description = normalized.get("description_texte") or ""
        description_lower = description.lower()
        for keyword in _MEUBLE_KEYWORDS:
            if keyword in description_lower:
                return True

        # Detection dans le titre si disponible
        titre = str(raw_data.get("titre", "")).lower()
        for keyword in _MEUBLE_KEYWORDS:
            if keyword in titre:
                return True

        return None

    def _compute_completude(
        self, data: dict[str, Any], optional_fields: list[str]
    ) -> float:
        """Calcule le score de completude (0.0 - 1.0).

        Le score represente la proportion de champs optionnels remplis.

        Args:
            data: Donnees normalisees.
            optional_fields: Liste des noms de champs optionnels.

        Returns:
            Score de completude entre 0.0 et 1.0.
        """
        if not optional_fields:
            return 1.0

        filled = 0
        for field in optional_fields:
            value = data.get(field)
            if value is not None:
                # Les listes vides ne comptent pas comme remplies
                if isinstance(value, list) and len(value) == 0:
                    continue
                # Les chaines vides ne comptent pas
                if isinstance(value, str) and not value.strip():
                    continue
                filled += 1

        return round(filled / len(optional_fields), 2)
