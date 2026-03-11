"""Regles de validation des annonces a l'ingestion.

Valide les donnees normalisees avant insertion en base de donnees,
conformement aux regles definies dans CLAUDE.md. Les annonces invalides
sont rejetees et tracees dans la table validation_log.
"""

from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# Bornes de validation pour les ventes
PRIX_MIN: int = 10_000
PRIX_MAX: int = 500_000
SURFACE_MIN: float = 10.0
SURFACE_MAX: float = 300.0
NB_PIECES_MIN: int = 1
NB_PIECES_MAX: int = 10
DPE_VALIDES: set[str] = {"A", "B", "C", "D", "E", "F", "G"}

# Bornes de validation pour les locations
LOYER_MIN: float = 200.0
LOYER_MAX: float = 3_000.0

# Codes postaux de Besancon et environs proches
CODES_POSTAUX_BESANCON: set[str] = {"25000", "25030", "25040", "25050"}

# Seuils de coherence surface/nb_pieces
SURFACE_COHERENCE: dict[int, float] = {
    1: 9.0,    # Studio minimum legal
    2: 20.0,   # T2 minimum raisonnable
    3: 35.0,   # T3 minimum raisonnable
    4: 50.0,   # T4 minimum raisonnable
    5: 65.0,   # T5 minimum raisonnable
}

# Mots-cles indiquant une annonce professionnelle deguisee
PRO_KEYWORDS: list[str] = [
    "agence",
    "cabinet",
    "groupe immobilier",
    "mandataire",
    "réseau",
    "reseau",
    "frais d'agence",
    "frais d agence",
    "honoraires",
    "exclusivité",
    "exclusivite",
    "mandat",
    "réf.",
    "ref.",
    "référence",
    "reference",
    "n° mandat",
]


class AnnonceValidator:
    """Validation des donnees normalisees d'annonces immobilieres.

    Applique les regles de validation definies dans la specification
    et detecte les anomalies statistiques.

    Example:
        >>> validator = AnnonceValidator()
        >>> data = {"prix": 145000, "surface_m2": 55.0, "nb_pieces": 3,
        ...         "url_source": "https://example.com/1", "adresse_brute": "Besancon"}
        >>> is_valid, reasons = validator.validate_vente(data)
        >>> is_valid
        True
    """

    def validate_vente(self, data: dict[str, Any]) -> tuple[bool, list[str]]:
        """Valide une annonce de vente normalisee.

        Regles :
        - prix : entier > 0, dans [10_000 - 500_000]
        - surface : float > 0, dans [10 - 300]
        - nb_pieces : entier dans [1 - 10]
        - dpe : char dans [A-G] ou None (accepte avec None)
        - url : URL valide, non vide
        - adresse : non vide, contient "Besançon"/"Besancon" ou CP 25xxx

        Args:
            data: Dictionnaire contenant les donnees normalisees de l'annonce.

        Returns:
            Tuple (is_valid, rejection_reasons). Si is_valid est True,
            rejection_reasons est une liste vide.
        """
        reasons: list[str] = []

        # Validation du prix
        prix = data.get("prix")
        if prix is None:
            reasons.append("prix manquant")
        elif not isinstance(prix, (int, float)):
            reasons.append(f"prix invalide (type {type(prix).__name__})")
        else:
            prix_int = int(prix)
            if prix_int <= 0:
                reasons.append(f"prix negatif ou nul ({prix_int})")
            elif prix_int < PRIX_MIN:
                reasons.append(f"prix trop bas ({prix_int} < {PRIX_MIN})")
            elif prix_int > PRIX_MAX:
                reasons.append(f"prix trop eleve ({prix_int} > {PRIX_MAX})")

        # Validation de la surface
        surface = data.get("surface_m2")
        if surface is None:
            reasons.append("surface manquante")
        elif not isinstance(surface, (int, float)):
            reasons.append(f"surface invalide (type {type(surface).__name__})")
        else:
            surface_f = float(surface)
            if surface_f <= 0:
                reasons.append(f"surface negative ou nulle ({surface_f})")
            elif surface_f < SURFACE_MIN:
                reasons.append(f"surface trop petite ({surface_f} < {SURFACE_MIN})")
            elif surface_f > SURFACE_MAX:
                reasons.append(f"surface trop grande ({surface_f} > {SURFACE_MAX})")

        # Validation du nombre de pieces
        nb_pieces = data.get("nb_pieces")
        if nb_pieces is None:
            reasons.append("nb_pieces manquant")
        elif not isinstance(nb_pieces, (int, float)):
            reasons.append(f"nb_pieces invalide (type {type(nb_pieces).__name__})")
        else:
            nb_int = int(nb_pieces)
            if nb_int < NB_PIECES_MIN:
                reasons.append(f"nb_pieces trop petit ({nb_int} < {NB_PIECES_MIN})")
            elif nb_int > NB_PIECES_MAX:
                reasons.append(f"nb_pieces trop grand ({nb_int} > {NB_PIECES_MAX})")

        # Validation du DPE (optionnel, mais si present doit etre A-G)
        dpe = data.get("dpe")
        if dpe is not None and dpe not in DPE_VALIDES:
            reasons.append(f"dpe invalide ('{dpe}' hors A-G)")

        # Validation de l'URL
        url = data.get("url_source")
        if not url or not isinstance(url, str) or not url.strip():
            reasons.append("url_source manquante ou vide")
        elif not self._is_valid_url(url):
            reasons.append(f"url_source invalide ({url})")

        # Validation de l'adresse
        adresse = data.get("adresse_brute")
        if not adresse or not isinstance(adresse, str) or not adresse.strip():
            reasons.append("adresse manquante ou vide")
        elif not self._is_besancon_address(adresse):
            reasons.append(
                f"adresse hors Besancon ('{adresse}' ne contient pas "
                "'Besancon'/'Besançon' ni CP 25xxx)"
            )

        is_valid = len(reasons) == 0
        if not is_valid:
            logger.info(
                "Annonce vente rejetee (%s) : %s",
                data.get("url_source", "URL inconnue"),
                "; ".join(reasons),
            )

        return is_valid, reasons

    def validate_location(self, data: dict[str, Any]) -> tuple[bool, list[str]]:
        """Valide une annonce de location normalisee.

        Regles :
        - loyer_cc : float > 0, dans [200 - 3_000]
        - surface : float > 0, dans [10 - 300]
        - nb_pieces : entier dans [1 - 10]
        - url : URL valide, non vide
        - adresse : non vide, contient "Besançon"/"Besancon" ou CP 25xxx

        Args:
            data: Dictionnaire contenant les donnees normalisees de la location.

        Returns:
            Tuple (is_valid, rejection_reasons).
        """
        reasons: list[str] = []

        # Validation du loyer
        loyer = data.get("loyer_cc")
        if loyer is None:
            reasons.append("loyer_cc manquant")
        elif not isinstance(loyer, (int, float)):
            reasons.append(f"loyer_cc invalide (type {type(loyer).__name__})")
        else:
            loyer_f = float(loyer)
            if loyer_f <= 0:
                reasons.append(f"loyer negatif ou nul ({loyer_f})")
            elif loyer_f < LOYER_MIN:
                reasons.append(f"loyer trop bas ({loyer_f} < {LOYER_MIN})")
            elif loyer_f > LOYER_MAX:
                reasons.append(f"loyer trop eleve ({loyer_f} > {LOYER_MAX})")

        # Validation de la surface
        surface = data.get("surface_m2")
        if surface is None:
            reasons.append("surface manquante")
        elif not isinstance(surface, (int, float)):
            reasons.append(f"surface invalide (type {type(surface).__name__})")
        else:
            surface_f = float(surface)
            if surface_f <= 0:
                reasons.append(f"surface negative ou nulle ({surface_f})")
            elif surface_f < SURFACE_MIN:
                reasons.append(f"surface trop petite ({surface_f} < {SURFACE_MIN})")
            elif surface_f > SURFACE_MAX:
                reasons.append(f"surface trop grande ({surface_f} > {SURFACE_MAX})")

        # Validation du nombre de pieces
        nb_pieces = data.get("nb_pieces")
        if nb_pieces is None:
            reasons.append("nb_pieces manquant")
        elif not isinstance(nb_pieces, (int, float)):
            reasons.append(f"nb_pieces invalide (type {type(nb_pieces).__name__})")
        else:
            nb_int = int(nb_pieces)
            if nb_int < NB_PIECES_MIN:
                reasons.append(f"nb_pieces trop petit ({nb_int} < {NB_PIECES_MIN})")
            elif nb_int > NB_PIECES_MAX:
                reasons.append(f"nb_pieces trop grand ({nb_int} > {NB_PIECES_MAX})")

        # Validation de l'URL
        url = data.get("url_source")
        if not url or not isinstance(url, str) or not url.strip():
            reasons.append("url_source manquante ou vide")
        elif not self._is_valid_url(url):
            reasons.append(f"url_source invalide ({url})")

        # Validation de l'adresse
        adresse = data.get("adresse_brute")
        if not adresse or not isinstance(adresse, str) or not adresse.strip():
            reasons.append("adresse manquante ou vide")
        elif not self._is_besancon_address(adresse):
            reasons.append(
                f"adresse hors Besancon ('{adresse}' ne contient pas "
                "'Besancon'/'Besançon' ni CP 25xxx)"
            )

        is_valid = len(reasons) == 0
        if not is_valid:
            logger.info(
                "Annonce location rejetee (%s) : %s",
                data.get("url_source", "URL inconnue"),
                "; ".join(reasons),
            )

        return is_valid, reasons

    def detect_anomalies(
        self, data: dict[str, Any], quartier_stats: dict[str, Any] | None = None
    ) -> list[str]:
        """Detecte les anomalies dans une annonce validee.

        Anomalies detectees :
        - Prix/m2 > 2 sigma de la mediane du quartier
        - Surface incoherente vs nombre de pieces
        - Signaux d'annonce professionnelle deguisee en particulier

        Args:
            data: Donnees normalisees de l'annonce.
            quartier_stats: Statistiques du quartier (optionnel).
                Attendu : {"prix_m2_median": float, "prix_m2_std": float}

        Returns:
            Liste de descriptions d'anomalies detectees (vide si aucune).
        """
        anomalies: list[str] = []

        prix = data.get("prix")
        surface = data.get("surface_m2")
        nb_pieces = data.get("nb_pieces")

        # Anomalie 1 : prix/m2 aberrant par rapport au quartier
        if (
            quartier_stats
            and prix is not None
            and surface is not None
            and surface > 0
        ):
            prix_m2 = float(prix) / float(surface)
            median = quartier_stats.get("prix_m2_median")
            std = quartier_stats.get("prix_m2_std")
            if median is not None and std is not None and std > 0:
                if abs(prix_m2 - median) > 2 * std:
                    anomalies.append(
                        f"prix/m2 aberrant ({prix_m2:.0f} vs mediane "
                        f"{median:.0f} +/- 2x{std:.0f})"
                    )

        # Anomalie 2 : surface incoherente avec le nombre de pieces
        if nb_pieces is not None and surface is not None:
            nb_int = int(nb_pieces)
            surface_f = float(surface)
            min_surface = SURFACE_COHERENCE.get(nb_int)
            if min_surface is not None and surface_f < min_surface:
                anomalies.append(
                    f"surface incoherente (T{nb_int} avec {surface_f}m2, "
                    f"minimum attendu {min_surface}m2)"
                )

        # Anomalie 3 : annonce pro deguisee
        description = str(data.get("description_texte", "")).lower()
        pro_signals: list[str] = []
        for keyword in PRO_KEYWORDS:
            if keyword in description:
                pro_signals.append(keyword)
        if len(pro_signals) >= 2:
            anomalies.append(
                f"annonce pro deguisee (signaux : {', '.join(pro_signals)})"
            )

        if anomalies:
            logger.debug(
                "Anomalies detectees pour %s : %s",
                data.get("url_source", "URL inconnue"),
                "; ".join(anomalies),
            )

        return anomalies

    def _is_valid_url(self, url: str) -> bool:
        """Verifie qu'une URL a un format valide.

        Args:
            url: URL a verifier.

        Returns:
            True si l'URL est valide (scheme http/https et domaine present).
        """
        try:
            parsed = urlparse(url.strip())
            return (
                parsed.scheme in ("http", "https")
                and bool(parsed.netloc)
                and "." in parsed.netloc
            )
        except Exception:
            return False

    def _is_besancon_address(self, address: str) -> bool:
        """Verifie qu'une adresse est localisee a Besancon.

        Accepte les adresses contenant "Besançon", "Besancon" (insensible
        a la casse), ou un code postal 25xxx.

        Args:
            address: Adresse a verifier.

        Returns:
            True si l'adresse est a Besancon ou dans le departement 25.
        """
        if not address:
            return False
        addr_lower = address.lower()

        # Verification du nom de ville
        if "besançon" in addr_lower or "besancon" in addr_lower:
            return True

        # Verification du code postal (25xxx)
        if re.search(r"\b25\d{3}\b", address):
            return True

        return False
