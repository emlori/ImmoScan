"""Scoring composite 0-100 pour les annonces immobilieres.

Combine les scores de rentabilite, localisation, DPE, potentiel de negociation
et risque de vacance locative avec les poids definis dans scoring.yaml.
"""

from __future__ import annotations

import logging
from typing import Any

from src.config import get_settings

logger = logging.getLogger(__name__)

# Scores DPE par defaut (surcharges par scoring.yaml si disponible)
DEFAULT_DPE_SCORES: dict[str, float] = {
    "A": 100.0,
    "B": 85.0,
    "C": 65.0,
    "D": 40.0,
    "E": 20.0,
    "F": 10.0,
    "G": 0.0,
}

# Signaux textuels indiquant un potentiel de negociation
DEFAULT_SIGNAUX_NEGO: list[str] = [
    "urgent",
    "prix a debattre",
    "a negocier",
    "faire offre",
    "vente rapide",
    "mutation",
    "succession",
    "divorce",
    "baisse de prix",
]


class CompositeScorer:
    """Calcule le score composite 0-100 d'une annonce immobiliere.

    Combine 5 criteres ponderes :
    - Rentabilite brute (40%) : proportionnel, bonus au-dessus de 8%.
    - Localisation (25%) : score geo direct.
    - DPE (15%) : mapping lettre -> score fixe.
    - Potentiel negociation (10%) : signaux texte + baisses de prix.
    - Risque vacance (10%) : base sur la tension locative du quartier.

    Attributes:
        scoring_config: Configuration de scoring chargee depuis scoring.yaml.
        poids: Dictionnaire des poids par critere.
        dpe_scores: Mapping DPE lettre -> score.
        signaux_nego: Mots-cles indiquant un potentiel de negociation.
    """

    def __init__(
        self,
        scoring_config: dict[str, Any] | None = None,
    ) -> None:
        """Initialise le scorer composite.

        Args:
            scoring_config: Configuration de scoring. Si None, charge depuis scoring.yaml.
        """
        if scoring_config is not None:
            self.scoring_config = scoring_config
        else:
            try:
                settings = get_settings()
                self.scoring_config = settings.load_scoring()
            except (FileNotFoundError, Exception):
                self.scoring_config = {}

        # Charger les poids
        poids = self.scoring_config.get("poids", {})
        self.poids: dict[str, float] = {
            "rentabilite": poids.get("rentabilite", 0.40),
            "localisation": poids.get("localisation", 0.25),
            "dpe": poids.get("dpe", 0.15),
            "negociation": poids.get("negociation", 0.10),
            "vacance": poids.get("vacance", 0.10),
        }

        # Charger les scores DPE
        dpe_cfg = self.scoring_config.get("dpe_scores", {})
        self.dpe_scores: dict[str, float] = {}
        for grade, score in DEFAULT_DPE_SCORES.items():
            self.dpe_scores[grade] = float(dpe_cfg.get(grade, score))

        # Charger les signaux de negociation
        nego_cfg = self.scoring_config.get("negociation_scoring", {})
        self.signaux_nego: list[str] = nego_cfg.get(
            "signaux_texte", DEFAULT_SIGNAUX_NEGO
        )
        self._poids_signal = nego_cfg.get("poids_signal", 15)
        self._poids_baisse_prix = nego_cfg.get("poids_baisse_prix", 40)
        self._poids_duree_vente = nego_cfg.get("poids_duree_vente", 30)

        # Charger les parametres de rentabilite scoring
        renta_cfg = self.scoring_config.get("rentabilite_scoring", {})
        self._min_renta = renta_cfg.get("min_renta", 4.0)
        self._max_renta = renta_cfg.get("max_renta", 12.0)

        renta_bonus_cfg = self.scoring_config.get("rentabilite", {})
        self._bonus_seuil = renta_bonus_cfg.get("bonus_seuil", 8.0)
        self._bonus_max = renta_bonus_cfg.get("bonus_max", 20)

        # Charger les parametres de vacance
        vacance_cfg = self.scoring_config.get("vacance_scoring", {})
        self._vacance_tension_elevee = vacance_cfg.get("tension_elevee", 90)
        self._vacance_tension_moyenne = vacance_cfg.get("tension_moyenne", 60)
        self._vacance_tension_faible = vacance_cfg.get("tension_faible", 30)

        # Charger les seuils d'alerte
        alertes_cfg = self.scoring_config.get("alertes", {})
        self._top_score_min = alertes_cfg.get("top", {}).get("score_min", 80)
        self._top_renta_min = alertes_cfg.get("top", {}).get("renta_min", 8.0)
        self._bon_score_min = alertes_cfg.get("bon", {}).get("score_min", 60)
        self._bon_renta_min_nego = alertes_cfg.get("bon", {}).get(
            "renta_min_nego", 8.0
        )

    def score(
        self,
        annonce_data: dict[str, Any],
        renta_data: dict[str, Any],
        geo_score: float,
        enrichment: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Calcule le score composite d'une annonce.

        Args:
            annonce_data: Donnees de l'annonce contenant au minimum :
                - dpe (str | None) : classe energetique.
                - description_texte (str | None) : texte de l'annonce.
                - historique_prix (list | None) : historique des prix.
                - quartier (str | None) : quartier du bien.
                Optionnel :
                - tension_locative (float) : tension locative 0-1.
                - date_publication (str | None) : date de publication.
            renta_data: Resultats du calcul de rentabilite (de RentabiliteCalculator).
            geo_score: Score de localisation 0-100 (de GeoScorer).
            enrichment: Donnees d'enrichissement IA (optionnel), avec :
                - signaux_nego (list[str]) : signaux de negociation detectes.

        Returns:
            Dictionnaire contenant :
                - score_global (float) : score composite 0-100.
                - score_rentabilite (float) : composante rentabilite 0-100.
                - score_localisation (float) : composante localisation 0-100.
                - score_dpe (float) : composante DPE 0-100.
                - score_negociation (float) : composante negociation 0-100.
                - score_vacance (float) : composante vacance 0-100.
                - niveau_alerte (str) : 'top', 'bon' ou 'veille'.
                - detail_poids (dict) : contribution de chaque critere au score final.
        """
        # 1. Score de rentabilite
        renta_brute = renta_data.get("renta_brute", 0.0)
        score_rentabilite = self._score_rentabilite(renta_brute)

        # 2. Score de localisation (utilise directement)
        score_localisation = max(0.0, min(100.0, geo_score))

        # 3. Score DPE
        dpe = annonce_data.get("dpe")
        score_dpe = self._score_dpe(dpe)

        # 4. Score de negociation
        score_negociation = self._score_negociation(
            annonce_data, enrichment
        )

        # 5. Score de vacance locative
        score_vacance = self._score_vacance(annonce_data)

        # Score composite pondere
        score_global = (
            score_rentabilite * self.poids["rentabilite"]
            + score_localisation * self.poids["localisation"]
            + score_dpe * self.poids["dpe"]
            + score_negociation * self.poids["negociation"]
            + score_vacance * self.poids["vacance"]
        )

        score_global = max(0.0, min(100.0, round(score_global, 2)))

        # Determiner le niveau d'alerte
        renta_brute_nego_best = max(
            renta_data.get("renta_brute", 0.0),
            renta_data.get("renta_brute_nego_5", 0.0),
            renta_data.get("renta_brute_nego_10", 0.0),
            renta_data.get("renta_brute_nego_15", 0.0),
        )
        niveau_alerte = self.determine_alert_level(
            score_global, renta_brute, renta_brute_nego_best
        )

        result: dict[str, Any] = {
            "score_global": score_global,
            "score_rentabilite": round(score_rentabilite, 2),
            "score_localisation": round(score_localisation, 2),
            "score_dpe": round(score_dpe, 2),
            "score_negociation": round(score_negociation, 2),
            "score_vacance": round(score_vacance, 2),
            "niveau_alerte": niveau_alerte,
            "detail_poids": {
                "rentabilite": round(
                    score_rentabilite * self.poids["rentabilite"], 2
                ),
                "localisation": round(
                    score_localisation * self.poids["localisation"], 2
                ),
                "dpe": round(score_dpe * self.poids["dpe"], 2),
                "negociation": round(
                    score_negociation * self.poids["negociation"], 2
                ),
                "vacance": round(score_vacance * self.poids["vacance"], 2),
            },
        }

        logger.debug(
            "Score composite : global=%.2f, renta=%.2f, loc=%.2f, "
            "dpe=%.2f, nego=%.2f, vacance=%.2f => alerte=%s",
            score_global,
            score_rentabilite,
            score_localisation,
            score_dpe,
            score_negociation,
            score_vacance,
            niveau_alerte,
        )

        return result

    def _score_rentabilite(self, renta_brute: float) -> float:
        """Convertit la rentabilite brute en score 0-100.

        Mapping lineaire entre min_renta (score=0) et max_renta (score=100),
        avec un bonus au-dessus du seuil bonus.

        Args:
            renta_brute: Rentabilite brute en pourcentage.

        Returns:
            Score de rentabilite entre 0 et 100.
        """
        if renta_brute <= self._min_renta:
            return 0.0

        if renta_brute >= self._max_renta:
            return 100.0

        # Mapping lineaire
        score = (
            (renta_brute - self._min_renta)
            / (self._max_renta - self._min_renta)
            * 100.0
        )

        return min(100.0, max(0.0, score))

    def _score_dpe(self, dpe: str | None) -> float:
        """Convertit une classe DPE en score 0-100.

        Args:
            dpe: Classe DPE (A-G) ou None.

        Returns:
            Score DPE. Retourne 30.0 si le DPE est inconnu ou None (neutre).
        """
        if dpe is None:
            return 30.0

        dpe_upper = dpe.upper().strip()
        return self.dpe_scores.get(dpe_upper, 30.0)

    def _score_negociation(
        self,
        annonce_data: dict[str, Any],
        enrichment: dict[str, Any] | None = None,
    ) -> float:
        """Calcule le score de potentiel de negociation.

        Prend en compte :
        - Signaux textuels dans la description (mots-cles).
        - Baisses de prix dans l'historique.
        - Signaux detectes par l'enrichissement IA.

        Args:
            annonce_data: Donnees de l'annonce.
            enrichment: Donnees d'enrichissement IA (optionnel).

        Returns:
            Score de negociation entre 0 et 100.
        """
        score = 0.0

        # Signaux textuels dans la description
        description = annonce_data.get("description_texte", "") or ""
        description_lower = description.lower()

        signaux_trouves = 0
        for signal in self.signaux_nego:
            if signal.lower() in description_lower:
                signaux_trouves += 1

        # Maximum 2 signaux pris en compte
        signaux_trouves = min(signaux_trouves, 2)
        score += signaux_trouves * self._poids_signal

        # Baisses de prix dans l'historique
        historique = annonce_data.get("historique_prix", []) or []
        if self._has_price_drop(historique):
            score += self._poids_baisse_prix

        # Signaux d'enrichissement IA
        if enrichment is not None:
            signaux_ia = enrichment.get("signaux_nego", []) or []
            signaux_ia_count = min(len(signaux_ia), 2)
            # Eviter le double comptage : prendre le max entre texte et IA
            signaux_total = max(signaux_trouves, signaux_ia_count)
            score = signaux_total * self._poids_signal
            if self._has_price_drop(historique):
                score += self._poids_baisse_prix

        return min(100.0, max(0.0, score))

    @staticmethod
    def _has_price_drop(historique: list[dict[str, Any]]) -> bool:
        """Detecte une baisse de prix dans l'historique.

        Args:
            historique: Liste de dicts {date, prix} triee par date.

        Returns:
            True si au moins une baisse de prix est detectee.
        """
        if not historique or len(historique) < 2:
            return False

        for i in range(1, len(historique)):
            prix_precedent = historique[i - 1].get("prix", 0)
            prix_actuel = historique[i].get("prix", 0)
            if prix_actuel < prix_precedent:
                return True

        return False

    def _score_vacance(self, annonce_data: dict[str, Any]) -> float:
        """Calcule le score de risque de vacance locative.

        Un score eleve indique un faible risque de vacance (situation favorable).

        Args:
            annonce_data: Donnees de l'annonce contenant :
                - tension_locative (float, 0-1) : tension du marche locatif.
                - quartier (str) : nom du quartier.

        Returns:
            Score de vacance entre 0 et 100 (100 = tres faible risque).
        """
        # Utiliser la tension locative directement si fournie
        tension = annonce_data.get("tension_locative")

        if tension is not None:
            tension = float(tension)
            if tension >= 0.8:
                return float(self._vacance_tension_elevee)
            elif tension >= 0.5:
                return float(self._vacance_tension_moyenne)
            else:
                return float(self._vacance_tension_faible)

        # Fallback : score par defaut (tension moyenne)
        return float(self._vacance_tension_moyenne)

    def determine_alert_level(
        self,
        score: float,
        renta_brute: float,
        renta_brute_nego_best: float | None = None,
    ) -> str:
        """Determine le niveau d'alerte pour une annonce.

        Applique les seuils definis dans scoring.yaml :
        - TOP : score >= 80 OU renta >= 8% au prix affiche.
        - BON : score 60-79 OU renta >= 8% apres negociation.
        - VEILLE : score < 60.

        Args:
            score: Score composite 0-100.
            renta_brute: Rentabilite brute au prix affiche (%).
            renta_brute_nego_best: Meilleure rentabilite apres negociation (%).

        Returns:
            Niveau d'alerte : 'top', 'bon' ou 'veille'.
        """
        # TOP : score >= 80 OU renta >= 8% au prix affiche
        if score >= self._top_score_min or renta_brute >= self._top_renta_min:
            return "top"

        # BON : score 60-79 OU renta >= 8% apres negociation
        if score >= self._bon_score_min:
            return "bon"

        if renta_brute_nego_best is not None:
            if renta_brute_nego_best >= self._bon_renta_min_nego:
                return "bon"

        # VEILLE : score < 60
        return "veille"

    def get_weights_sum(self) -> float:
        """Retourne la somme des poids du scoring composite.

        Returns:
            Somme des poids (devrait etre 1.0).
        """
        return sum(self.poids.values())
