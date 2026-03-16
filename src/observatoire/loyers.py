"""Observatoire des loyers - calcul des medianes par segment.

Calcule les loyers medians par segment (quartier x type_bien x meuble)
a partir des annonces de location observees sur le marche. Implemente
la detection d'outliers par IQR, la ponderation temporelle exponentielle,
et un mecanisme de fallback sur les loyers de reference du fichier
quartiers.yaml.
"""

from __future__ import annotations

import logging
import math
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

# Surface typique par type de bien (m²) — sert de reference
# pour l'ajustement marginal de surface
_SURFACE_TYPIQUE: dict[str, float] = {
    "T1": 25.0,
    "T2": 45.0,
    "T3": 65.0,
    "T4": 85.0,
    "T5": 105.0,
}

# Seuil de deviation de surface (%) au-dela duquel on ajuste
_SURFACE_DEVIATION_SEUIL = 0.20  # 20%
# Plafond de l'ajustement (%)
_SURFACE_AJUSTEMENT_MAX = 0.15  # ±15%

# Chemin vers la config des quartiers (fallback)
_CONFIG_DIR = Path(__file__).resolve().parent.parent.parent / "config"


class ObservatoireLoyers:
    """Observatoire des loyers pour Besancon.

    Calcule les medianes de loyer par segment (quartier x type x meuble)
    a partir des annonces de location scrapees. Permet d'estimer le loyer
    d'un bien a partir des references calculees ou de valeurs fallback.

    Attributes:
        quartiers_config: Configuration des quartiers (loyers fallback).
        half_life_days: Demi-vie pour la ponderation temporelle (jours).
        min_fiable: Nombre minimum d'annonces pour la fiabilite "fiable".
    """

    def __init__(
        self,
        quartiers_config: dict[str, Any] | None = None,
        half_life_days: int = 30,
        min_fiable: int = 5,
    ) -> None:
        """Initialise l'observatoire des loyers.

        Args:
            quartiers_config: Configuration des quartiers avec loyers fallback.
                Si None, charge depuis config/quartiers.yaml.
            half_life_days: Demi-vie pour la ponderation temporelle (jours).
            min_fiable: Nombre minimum d'annonces pour la fiabilite "fiable".
        """
        self.half_life_days = half_life_days
        self.min_fiable = min_fiable

        if quartiers_config is not None:
            self.quartiers_config = quartiers_config
        else:
            self.quartiers_config = self._load_quartiers_config()

        logger.info(
            "ObservatoireLoyers initialise (half_life=%d jours, min_fiable=%d)",
            self.half_life_days,
            self.min_fiable,
        )

    def compute_medianes(
        self,
        loyers: list[dict[str, Any]],
        quartier: str,
        type_bien: str,
        meuble: bool,
    ) -> dict[str, Any]:
        """Calcule les medianes de loyer pour un segment donne.

        Filtre les annonces correspondant au segment, supprime les outliers
        par la methode IQR, applique une ponderation temporelle exponentielle,
        et calcule les statistiques de reference.

        Args:
            loyers: Liste d'annonces de location (dictionnaires avec
                loyer_cc, surface_m2, nb_pieces, quartier, meuble, date_scrape).
            quartier: Nom du quartier cible.
            type_bien: Type de bien ('T2', 'T3').
            meuble: True pour meuble, False pour nu.

        Returns:
            Dictionnaire compatible LoyerReference avec les champs :
            quartier, type_bien, meuble, loyer_median, loyer_q1, loyer_q3,
            loyer_m2_median, nb_annonces, fiabilite.
        """
        filtered = self._filter_segment(loyers, quartier, type_bien, meuble)

        result: dict[str, Any] = {
            "quartier": quartier,
            "type_bien": type_bien,
            "meuble": meuble,
            "loyer_median": None,
            "loyer_q1": None,
            "loyer_q3": None,
            "loyer_m2_median": None,
            "nb_annonces": 0,
            "fiabilite": "preliminaire",
        }

        if not filtered:
            logger.info(
                "Aucune annonce pour le segment %s/%s/meuble=%s",
                quartier,
                type_bien,
                meuble,
            )
            return result

        # Extraire les loyers CC
        loyer_values = [a["loyer_cc"] for a in filtered]

        # Supprimer les outliers par IQR
        clean_values = self._remove_outliers(loyer_values)

        if not clean_values:
            logger.warning(
                "Toutes les valeurs sont des outliers pour %s/%s/meuble=%s",
                quartier,
                type_bien,
                meuble,
            )
            return result

        # Reconstruire la liste filtree (garder les annonces dont le loyer
        # est dans clean_values)
        clean_set = set()
        remaining = list(clean_values)
        clean_filtered: list[dict[str, Any]] = []
        for annonce in filtered:
            if annonce["loyer_cc"] in remaining:
                remaining.remove(annonce["loyer_cc"])
                clean_filtered.append(annonce)

        if not clean_filtered:
            clean_filtered = filtered  # Fallback : garder tout

        # Calculer les poids temporels
        now = datetime.now(UTC)
        weights: list[float] = []
        values: list[float] = []
        loyer_m2_values: list[float] = []
        loyer_m2_weights: list[float] = []

        for annonce in clean_filtered:
            date_scrape = annonce.get("date_scrape")
            weight = self._temporal_weight(date_scrape, self.half_life_days)
            loyer_cc = annonce["loyer_cc"]
            surface = annonce.get("surface_m2", 0)

            values.append(loyer_cc)
            weights.append(weight)

            if surface and surface > 0:
                loyer_m2_values.append(loyer_cc / surface)
                loyer_m2_weights.append(weight)

        nb_annonces = len(values)

        # Medianes ponderees
        loyer_median = self._weighted_median(values, weights)

        # Quartiles (non ponderes, sur les valeurs nettoyees)
        sorted_values = sorted(clean_values)
        n = len(sorted_values)
        loyer_q1 = self._percentile(sorted_values, 25)
        loyer_q3 = self._percentile(sorted_values, 75)

        # Mediane loyer/m2
        loyer_m2_median: float | None = None
        if loyer_m2_values:
            loyer_m2_median = self._weighted_median(
                loyer_m2_values, loyer_m2_weights
            )

        # Fiabilite
        fiabilite = "fiable" if nb_annonces >= self.min_fiable else "preliminaire"

        result.update(
            {
                "loyer_median": round(loyer_median, 2),
                "loyer_q1": round(loyer_q1, 2),
                "loyer_q3": round(loyer_q3, 2),
                "loyer_m2_median": (
                    round(loyer_m2_median, 2) if loyer_m2_median is not None else None
                ),
                "nb_annonces": nb_annonces,
                "fiabilite": fiabilite,
            }
        )

        logger.info(
            "Medianes calculees pour %s/%s/meuble=%s : median=%.2f, "
            "Q1=%.2f, Q3=%.2f, n=%d, fiabilite=%s",
            quartier,
            type_bien,
            meuble,
            loyer_median,
            loyer_q1,
            loyer_q3,
            nb_annonces,
            fiabilite,
        )

        return result

    def estimate_loyer(
        self,
        quartier: str,
        type_bien: str,
        meuble: bool,
        surface: float,
        references: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Estime le loyer pour un bien donne.

        Utilise les references calculees par l'observatoire si disponibles
        et fiables, sinon se rabat sur les loyers fallback de quartiers.yaml.

        Args:
            quartier: Nom du quartier.
            type_bien: Type de bien ('T2', 'T3').
            meuble: True pour meuble, False pour nu.
            surface: Surface du bien en metres carres.
            references: Liste de references de loyers calculees
                (dictionnaires LoyerReference). Si None, utilise le fallback.

        Returns:
            Dictionnaire avec : loyer_estime, fiabilite, source.
        """
        # Chercher une reference correspondante
        if references:
            for ref in references:
                if (
                    ref.get("quartier") == quartier
                    and ref.get("type_bien") == type_bien
                    and ref.get("meuble") == meuble
                    and ref.get("nb_annonces", 0) >= self.min_fiable
                    and ref.get("loyer_median") is not None
                ):
                    loyer_base = ref["loyer_median"]
                    loyer_estime = self._ajuster_surface(
                        loyer_base, type_bien, surface
                    )
                    return {
                        "loyer_estime": round(loyer_estime, 2),
                        "fiabilite": "fiable",
                        "source": "observatoire",
                    }

        # Fallback sur quartiers.yaml
        return self._fallback_loyer(quartier, type_bien, meuble, surface)

    def _filter_segment(
        self,
        loyers: list[dict[str, Any]],
        quartier: str,
        type_bien: str,
        meuble: bool,
    ) -> list[dict[str, Any]]:
        """Filtre les annonces correspondant a un segment exact.

        Args:
            loyers: Liste complete des annonces de location.
            quartier: Nom du quartier.
            type_bien: Type de bien ('T2', 'T3').
            meuble: True pour meuble, False pour nu.

        Returns:
            Liste filtree d'annonces correspondant au segment.
        """
        # Determiner le nombre de pieces selon le type
        nb_pieces_map = {"T1": 1, "T2": 2, "T3": 3, "T4": 4, "T5": 5}
        nb_pieces_cible = nb_pieces_map.get(type_bien)

        filtered: list[dict[str, Any]] = []
        for annonce in loyers:
            if annonce.get("quartier") != quartier:
                continue
            if nb_pieces_cible is not None and annonce.get("nb_pieces") != nb_pieces_cible:
                continue
            if annonce.get("meuble") != meuble:
                continue
            if annonce.get("loyer_cc") is None:
                continue
            filtered.append(annonce)

        return filtered

    def _remove_outliers(self, values: list[float]) -> list[float]:
        """Supprime les outliers par la methode IQR.

        Un outlier est defini comme une valeur en dehors de
        [Q1 - 1.5 * IQR, Q3 + 1.5 * IQR].

        Args:
            values: Liste de valeurs numeriques.

        Returns:
            Liste nettoyee sans les outliers.
        """
        if len(values) < 4:
            return list(values)

        sorted_vals = sorted(values)
        q1 = self._percentile(sorted_vals, 25)
        q3 = self._percentile(sorted_vals, 75)
        iqr = q3 - q1

        if iqr == 0:
            # Toutes les valeurs sont identiques ou tres proches
            return list(values)

        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        clean = [v for v in values if lower_bound <= v <= upper_bound]
        removed = len(values) - len(clean)
        if removed > 0:
            logger.debug(
                "Outliers supprimes : %d/%d (bornes [%.2f, %.2f])",
                removed,
                len(values),
                lower_bound,
                upper_bound,
            )

        return clean

    def _temporal_weight(
        self,
        date_scrape: datetime | str | None,
        half_life_days: int = 30,
    ) -> float:
        """Calcule le poids temporel par decroissance exponentielle.

        Les annonces recentes pesent plus que les anciennes.
        Le poids est 1.0 pour une annonce du jour et diminue de moitie
        tous les half_life_days jours.

        Args:
            date_scrape: Date de scraping de l'annonce.
            half_life_days: Nombre de jours pour que le poids soit divise par 2.

        Returns:
            Poids temporel entre 0 et 1.
        """
        if date_scrape is None:
            return 0.5  # Poids neutre si pas de date

        now = datetime.now(UTC)

        if isinstance(date_scrape, str):
            try:
                date_scrape = datetime.fromisoformat(date_scrape)
            except ValueError:
                return 0.5

        # Ajouter timezone si naive
        if date_scrape.tzinfo is None:
            date_scrape = date_scrape.replace(tzinfo=UTC)

        delta_days = (now - date_scrape).total_seconds() / 86400.0

        if delta_days < 0:
            delta_days = 0

        # Decroissance exponentielle : w = 2^(-t/half_life)
        weight = math.pow(2, -delta_days / half_life_days)

        return weight

    def _weighted_median(
        self,
        values: list[float],
        weights: list[float],
    ) -> float:
        """Calcule la mediane ponderee.

        Trie les valeurs par ordre croissant, puis trouve la valeur
        ou le poids cumule atteint 50% du poids total.

        Args:
            values: Liste de valeurs numeriques.
            weights: Liste de poids correspondants.

        Returns:
            Mediane ponderee.

        Raises:
            ValueError: Si les listes sont vides ou de tailles differentes.
        """
        if not values:
            raise ValueError("La liste de valeurs ne peut pas etre vide.")

        if len(values) != len(weights):
            raise ValueError(
                "Les listes de valeurs et de poids doivent avoir la meme taille."
            )

        # Si un seul element, retourner directement
        if len(values) == 1:
            return values[0]

        # Trier par valeur croissante
        paired = sorted(zip(values, weights), key=lambda x: x[0])
        sorted_values = [v for v, _ in paired]
        sorted_weights = [w for _, w in paired]

        total_weight = sum(sorted_weights)
        if total_weight == 0:
            # Poids tous nuls : retourner la mediane simple
            mid = len(sorted_values) // 2
            return sorted_values[mid]

        cumulative = 0.0
        half = total_weight / 2.0

        for i, (val, w) in enumerate(zip(sorted_values, sorted_weights)):
            cumulative += w
            if cumulative >= half:
                return val

        # Fallback : dernier element
        return sorted_values[-1]

    def _fallback_loyer(
        self,
        quartier: str,
        type_bien: str,
        meuble: bool,
        surface: float,
    ) -> dict[str, Any]:
        """Retourne une estimation de loyer basee sur les fallback de quartiers.yaml.

        Args:
            quartier: Nom du quartier.
            type_bien: Type de bien ('T2', 'T3').
            meuble: True pour meuble, False pour nu.
            surface: Surface du bien en metres carres.

        Returns:
            Dictionnaire avec : loyer_estime, fiabilite, source.
        """
        quartiers = self.quartiers_config.get("quartiers", {})

        # Chercher le quartier par nom
        quartier_data: dict[str, Any] | None = None
        for _, qdata in quartiers.items():
            if qdata.get("nom") == quartier:
                quartier_data = qdata
                break

        if quartier_data is None:
            logger.warning(
                "Quartier '%s' introuvable dans la configuration.", quartier
            )
            return {
                "loyer_estime": None,
                "fiabilite": "indisponible",
                "source": "fallback",
            }

        fallback = quartier_data.get("loyers_fallback", {})
        type_data = fallback.get(type_bien, {})
        meuble_key = "meuble" if meuble else "nu"
        segment_data = type_data.get(meuble_key, {})

        # Priorite 1 : loyer_median (base par type/nb_pieces)
        loyer_median = segment_data.get("loyer_median")
        if loyer_median is not None:
            loyer_estime = self._ajuster_surface(
                loyer_median, type_bien, surface
            )
            return {
                "loyer_estime": round(loyer_estime, 2),
                "fiabilite": "preliminaire",
                "source": "fallback",
            }

        # Priorite 2 : loyer_m2 (ancien mode, compatibilite)
        loyer_m2 = segment_data.get("loyer_m2")
        if loyer_m2 is not None:
            loyer_estime = loyer_m2 * surface
            return {
                "loyer_estime": round(loyer_estime, 2),
                "fiabilite": "preliminaire",
                "source": "fallback",
            }

        logger.warning(
            "Loyer fallback indisponible pour %s/%s/%s",
            quartier,
            type_bien,
            meuble_key,
        )
        return {
            "loyer_estime": None,
            "fiabilite": "indisponible",
            "source": "fallback",
        }

    @staticmethod
    def _ajuster_surface(
        loyer_base: float,
        type_bien: str,
        surface: float,
    ) -> float:
        """Ajuste marginalement le loyer median selon la surface.

        Le loyer de base (median du segment par type/nb_pieces) est ajuste
        uniquement si la surface devie de plus de 20% de la surface typique
        pour ce type. L'ajustement est plafonne a +-15%.

        Logique : un T2 de 40m2 ou 50m2 se loue quasiment au meme prix.
        Seuls les ecarts importants (ex: T2 de 60m2) justifient un ajustement.

        Args:
            loyer_base: Loyer median du segment (EUR).
            type_bien: Type de bien ('T1', 'T2', 'T3', etc.).
            surface: Surface reelle du bien (m2).

        Returns:
            Loyer ajuste (EUR).
        """
        surface_ref = _SURFACE_TYPIQUE.get(type_bien)
        if surface_ref is None or surface <= 0:
            return loyer_base

        ecart_relatif = (surface - surface_ref) / surface_ref

        # Pas d'ajustement si l'ecart est dans la zone neutre (±20%)
        if abs(ecart_relatif) <= _SURFACE_DEVIATION_SEUIL:
            return loyer_base

        # Ajustement = partie excedentaire au-dela du seuil, plafonne
        if ecart_relatif > 0:
            ajustement = ecart_relatif - _SURFACE_DEVIATION_SEUIL
        else:
            ajustement = ecart_relatif + _SURFACE_DEVIATION_SEUIL

        ajustement = max(-_SURFACE_AJUSTEMENT_MAX, min(_SURFACE_AJUSTEMENT_MAX, ajustement))

        return loyer_base * (1.0 + ajustement)

    @staticmethod
    def _percentile(sorted_values: list[float], percentile: float) -> float:
        """Calcule un percentile sur une liste triee.

        Utilise l'interpolation lineaire entre les deux valeurs encadrantes.

        Args:
            sorted_values: Liste de valeurs triees par ordre croissant.
            percentile: Percentile souhaite (0-100).

        Returns:
            Valeur du percentile.
        """
        n = len(sorted_values)
        if n == 0:
            raise ValueError("La liste ne peut pas etre vide.")
        if n == 1:
            return sorted_values[0]

        k = (percentile / 100.0) * (n - 1)
        f = math.floor(k)
        c = math.ceil(k)

        if f == c:
            return sorted_values[int(k)]

        lower = sorted_values[int(f)]
        upper = sorted_values[int(c)]
        return lower + (upper - lower) * (k - f)

    @staticmethod
    def _load_quartiers_config() -> dict[str, Any]:
        """Charge la configuration des quartiers depuis le fichier YAML.

        Returns:
            Dictionnaire avec la configuration des quartiers.
        """
        filepath = _CONFIG_DIR / "quartiers.yaml"
        if not filepath.exists():
            logger.warning("Fichier quartiers.yaml introuvable : %s", filepath)
            return {"quartiers": {}}

        with open(filepath, encoding="utf-8") as f:
            return yaml.safe_load(f)
