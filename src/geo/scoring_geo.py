"""Scoring de localisation pour les annonces immobilieres a Besancon.

Calcule un score de localisation 0-100 en fonction de la proximite
aux transports (tram), commerces, campus/universite et de l'attractivite
du quartier. Utilise les coordonnees GPS et la config quartiers.yaml.
"""

from __future__ import annotations

import logging
import math
from typing import Any

from src.config import get_settings

logger = logging.getLogger(__name__)

# Points d'interet de Besancon : arrets de tram (ligne T1/T2, coordonnees WGS84)
TRAM_STOPS: list[tuple[float, float]] = [
    (47.2470, 6.0220),  # Gare Viotte
    (47.2440, 6.0200),  # Resistance
    (47.2410, 6.0190),  # Revolution
    (47.2378, 6.0241),  # Centre-Ville / Grande Rue
    (47.2350, 6.0270),  # Granvelle
    (47.2330, 6.0310),  # Chamars
    (47.2300, 6.0290),  # 8 Septembre
    (47.2270, 6.0340),  # Micropolis
    (47.2500, 6.0150),  # Minjoz (Hopital)
    (47.2530, 6.0050),  # Hauts du Chazal
    (47.2410, 6.0280),  # Flore
    (47.2390, 6.0350),  # Battant
    (47.2340, 6.0200),  # Canot
    (47.2480, 6.0300),  # Palente
    (47.2320, 6.0380),  # Saint-Jacques
]

# Zones commerciales principales
COMMERCIAL_AREAS: list[tuple[float, float]] = [
    (47.2378, 6.0241),  # Centre-Ville / rue pietonne Grande Rue
    (47.2400, 6.0200),  # Place de la Revolution
    (47.2410, 6.0180),  # Battant / quartier commercial
    (47.2340, 6.0270),  # Granvelle / commerces
    (47.2460, 6.0220),  # Gare Viotte / commerces
]

# Campus et universites
CAMPUS_LOCATIONS: list[tuple[float, float]] = [
    (47.2470, 6.0140),  # Campus de la Bouloie (UFR Sciences)
    (47.2530, 6.0060),  # Campus Hauts du Chazal (UFR Sante)
    (47.2400, 6.0270),  # Centre-ville universitaire (UFR Lettres / SLHS)
    (47.2380, 6.0300),  # IUT Besancon (centre)
]

# Gare principale
GARE_LOCATIONS: list[tuple[float, float]] = [
    (47.2470, 6.0220),  # Gare de Besancon Viotte
    (47.2760, 5.9550),  # Gare Besancon Franche-Comte TGV (Auxon)
]


class GeoScorer:
    """Calcule le score de localisation d'un bien immobilier a Besancon.

    Prend en compte la proximite aux arrets de tram, aux commerces, aux campus
    universitaires, a la gare, et l'attractivite globale du quartier pour
    produire un score de 0 a 100.

    Attributes:
        quartiers_config: Configuration des quartiers depuis quartiers.yaml.
        scoring_config: Poids et rayons pour le scoring de localisation.
    """

    def __init__(
        self,
        quartiers_config: dict[str, Any] | None = None,
    ) -> None:
        """Initialise le scorer de localisation.

        Args:
            quartiers_config: Configuration des quartiers. Si None, charge depuis
                le fichier quartiers.yaml.
        """
        if quartiers_config is not None:
            self.quartiers_config = quartiers_config
        else:
            try:
                settings = get_settings()
                self.quartiers_config = settings.load_quartiers()
            except (FileNotFoundError, Exception):
                self.quartiers_config = {}

        # Charger les parametres de scoring de localisation
        scoring_loc = self.quartiers_config.get("scoring_localisation", {})
        self._poids_tram = scoring_loc.get("poids_tram", 30) / 100.0
        self._poids_commerces = scoring_loc.get("poids_commerces", 25) / 100.0
        self._poids_campus = scoring_loc.get("poids_campus", 20) / 100.0
        self._poids_gare = scoring_loc.get("poids_gare", 15) / 100.0
        self._poids_attractivite = scoring_loc.get("poids_attractivite", 10) / 100.0

        self._rayon_tram = scoring_loc.get("rayon_tram", 500)
        self._rayon_commerces = scoring_loc.get("rayon_commerces", 300)
        self._rayon_campus = scoring_loc.get("rayon_campus", 1000)
        self._rayon_gare = scoring_loc.get("rayon_gare", 1500)

    def score_localisation(
        self,
        coordonnees: tuple[float, float],
        quartier: str | None = None,
    ) -> float:
        """Calcule le score de localisation d'un bien.

        Combine la proximite aux POI (tram, commerces, campus, gare) et
        le score d'attractivite du quartier pour donner un score global 0-100.

        Args:
            coordonnees: Tuple (latitude, longitude) en WGS84.
            quartier: Nom du quartier (optionnel, identifie automatiquement si None).

        Returns:
            Score de localisation entre 0 et 100.
        """
        if not coordonnees or len(coordonnees) < 2:
            logger.warning("Coordonnees invalides pour le scoring de localisation.")
            return 0.0

        lat, lon = coordonnees

        # Identifier le quartier si non fourni
        if quartier is None:
            quartier = self.identify_quartier(coordonnees)

        # Score de proximite au tram (meilleur arret le plus proche)
        score_tram = self._best_proximity_score(
            (lat, lon), TRAM_STOPS, self._rayon_tram
        )

        # Score de proximite aux commerces
        score_commerces = self._best_proximity_score(
            (lat, lon), COMMERCIAL_AREAS, self._rayon_commerces
        )

        # Score de proximite aux campus
        score_campus = self._best_proximity_score(
            (lat, lon), CAMPUS_LOCATIONS, self._rayon_campus
        )

        # Score de proximite a la gare
        score_gare = self._best_proximity_score(
            (lat, lon), GARE_LOCATIONS, self._rayon_gare
        )

        # Score d'attractivite du quartier
        score_attractivite = self._get_quartier_attractivite(quartier)

        # Score composite pondere
        score = (
            score_tram * self._poids_tram
            + score_commerces * self._poids_commerces
            + score_campus * self._poids_campus
            + score_gare * self._poids_gare
            + score_attractivite * self._poids_attractivite
        )

        # Normaliser sur 0-100 (les poids divisent par 100 => deja en 0-100 si total poids = 1)
        score_final = max(0.0, min(100.0, score))

        logger.debug(
            "Score localisation pour (%f, %f) quartier=%s : "
            "tram=%.1f, commerces=%.1f, campus=%.1f, gare=%.1f, "
            "attractivite=%.1f => total=%.1f",
            lat,
            lon,
            quartier,
            score_tram,
            score_commerces,
            score_campus,
            score_gare,
            score_attractivite,
            score_final,
        )

        return round(score_final, 2)

    def identify_quartier(
        self,
        coordonnees: tuple[float, float],
    ) -> str | None:
        """Identifie le quartier d'un point GPS par distance au centre de chaque quartier.

        Utilise la distance haversine pour trouver le quartier dont le centre
        est le plus proche du point donne, dans un rayon maximum de 2 km.

        Args:
            coordonnees: Tuple (latitude, longitude) en WGS84.

        Returns:
            Nom du quartier identifie, ou None si aucun quartier ne correspond.
        """
        if not coordonnees or len(coordonnees) < 2:
            return None

        quartiers = self.quartiers_config.get("quartiers", {})
        if not quartiers:
            return None

        best_quartier: str | None = None
        best_distance = float("inf")
        max_distance = 2000.0  # 2 km max

        for _key, config in quartiers.items():
            centre = config.get("centre", {})
            centre_lat = centre.get("lat")
            centre_lon = centre.get("lon")

            if centre_lat is None or centre_lon is None:
                continue

            distance = self._haversine_distance(
                coordonnees, (centre_lat, centre_lon)
            )

            if distance < best_distance and distance <= max_distance:
                best_distance = distance
                best_quartier = config.get("nom")

        return best_quartier

    def _best_proximity_score(
        self,
        coord: tuple[float, float],
        poi_list: list[tuple[float, float]],
        max_distance_m: float,
    ) -> float:
        """Calcule le meilleur score de proximite parmi une liste de POI.

        Retourne le score le plus eleve (POI le plus proche).

        Args:
            coord: Coordonnees du bien (latitude, longitude).
            poi_list: Liste de coordonnees des POI.
            max_distance_m: Distance maximale en metres pour un score > 0.

        Returns:
            Score de proximite entre 0 et 100.
        """
        if not poi_list:
            return 0.0

        best_score = 0.0
        for poi in poi_list:
            score = self._distance_score(coord, poi, max_distance_m)
            if score > best_score:
                best_score = score

        return best_score

    @staticmethod
    def _distance_score(
        coord1: tuple[float, float],
        coord2: tuple[float, float],
        max_distance_m: float,
    ) -> float:
        """Calcule un score 0-100 base sur la distance entre deux points.

        Score de 100 si les points sont confondus, decroissant lineairement
        jusqu'a 0 a la distance maximale.

        Args:
            coord1: Premier point (latitude, longitude).
            coord2: Second point (latitude, longitude).
            max_distance_m: Distance maximale en metres (score = 0 au-dela).

        Returns:
            Score entre 0 et 100.
        """
        if max_distance_m <= 0:
            return 0.0

        distance = GeoScorer._haversine_distance(coord1, coord2)

        if distance >= max_distance_m:
            return 0.0

        return 100.0 * (1.0 - distance / max_distance_m)

    @staticmethod
    def _haversine_distance(
        coord1: tuple[float, float],
        coord2: tuple[float, float],
    ) -> float:
        """Calcule la distance en metres entre deux points GPS (formule haversine).

        Args:
            coord1: Premier point (latitude, longitude) en degres.
            coord2: Second point (latitude, longitude) en degres.

        Returns:
            Distance en metres entre les deux points.
        """
        earth_radius_m = 6_371_000.0  # Rayon moyen de la Terre en metres

        lat1, lon1 = math.radians(coord1[0]), math.radians(coord1[1])
        lat2, lon2 = math.radians(coord2[0]), math.radians(coord2[1])

        dlat = lat2 - lat1
        dlon = lon2 - lon1

        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        )
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        return earth_radius_m * c

    def _get_quartier_attractivite(self, quartier: str | None) -> float:
        """Recupere le score d'attractivite d'un quartier.

        Args:
            quartier: Nom du quartier.

        Returns:
            Score d'attractivite entre 0 et 100 (defaut 50 si quartier inconnu).
        """
        if quartier is None:
            return 50.0

        quartiers = self.quartiers_config.get("quartiers", {})

        for _key, config in quartiers.items():
            if config.get("nom") == quartier:
                return float(config.get("score_attractivite", 50.0))

        return 50.0

    def get_quartier_tension(self, quartier: str | None) -> float:
        """Recupere la tension locative d'un quartier.

        Args:
            quartier: Nom du quartier.

        Returns:
            Tension locative entre 0 et 1 (defaut 0.5 si quartier inconnu).
        """
        if quartier is None:
            return 0.5

        quartiers = self.quartiers_config.get("quartiers", {})

        for _key, config in quartiers.items():
            if config.get("nom") == quartier:
                return float(config.get("tension_locative", 0.5))

        return 0.5

    def get_quartier_risque_vacance(self, quartier: str | None) -> str:
        """Recupere le risque de vacance locative d'un quartier.

        Args:
            quartier: Nom du quartier.

        Returns:
            Risque de vacance ('faible', 'moyen' ou 'eleve'). Defaut 'moyen'.
        """
        if quartier is None:
            return "moyen"

        quartiers = self.quartiers_config.get("quartiers", {})

        for _key, config in quartiers.items():
            if config.get("nom") == quartier:
                return config.get("risque_vacance", "moyen")

        return "moyen"
