"""Calcul de rentabilite brute pour les investissements locatifs.

Calcule la rentabilite brute pour 4 scenarios de negociation
(prix affiche, -5%, -10%, -15%) conformement a la strategie ImmoScan.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class RentabiliteCalculator:
    """Calculateur de rentabilite brute pour investissements locatifs.

    Calcule la rentabilite brute annuelle selon la formule :
        renta_brute = (loyer_mensuel * 12 / prix_achat) * 100

    Fournit systematiquement 4 scenarios de negociation :
    0% (prix affiche), -5%, -10%, -15%.

    Attributes:
        scenarios_nego: Liste des pourcentages de decote a calculer.
    """

    def __init__(
        self,
        scenarios_nego: list[int] | None = None,
    ) -> None:
        """Initialise le calculateur de rentabilite.

        Args:
            scenarios_nego: Liste des pourcentages de decote a appliquer.
                Defaut: [0, 5, 10, 15].
        """
        self.scenarios_nego = scenarios_nego or [0, 5, 10, 15]

    def calculate(
        self,
        prix: int,
        loyer_mensuel: float,
        charges_copro: float | None = None,
    ) -> dict[str, Any]:
        """Calcule la rentabilite brute pour les 4 scenarios de negociation.

        Args:
            prix: Prix d'achat en euros (doit etre > 0).
            loyer_mensuel: Loyer mensuel estime en euros (doit etre >= 0).
            charges_copro: Charges de copropriete mensuelles en euros (optionnel).

        Returns:
            Dictionnaire contenant:
                - renta_brute (float): Rentabilite brute au prix affiche (%).
                - renta_brute_nego_5 (float): Rentabilite brute avec -5% (%).
                - renta_brute_nego_10 (float): Rentabilite brute avec -10% (%).
                - renta_brute_nego_15 (float): Rentabilite brute avec -15% (%).
                - loyer_annuel (float): Loyer annuel en euros.
                - charges_annuelles (float | None): Charges annuelles si fournies.
                - prix_original (int): Prix d'achat original.
                - loyer_mensuel (float): Loyer mensuel utilise.
                - scenarios (dict): Detail par scenario {pourcentage: {prix, renta}}.

        Raises:
            ValueError: Si le prix est <= 0 ou le loyer est negatif.
        """
        # Validation des entrees
        if prix <= 0:
            raise ValueError(f"Le prix doit etre strictement positif, recu : {prix}")

        if loyer_mensuel < 0:
            raise ValueError(
                f"Le loyer mensuel ne peut pas etre negatif, recu : {loyer_mensuel}"
            )

        loyer_annuel = loyer_mensuel * 12
        charges_annuelles = charges_copro * 12 if charges_copro is not None else None

        # Calculer la rentabilite pour chaque scenario
        scenarios: dict[int, dict[str, float]] = {}
        renta_results: dict[str, float] = {}

        for pct in self.scenarios_nego:
            factor = 1.0 - pct / 100.0
            prix_negocie = prix * factor

            if prix_negocie <= 0:
                renta = 0.0
            else:
                renta = (loyer_annuel / prix_negocie) * 100.0

            renta = round(renta, 2)

            scenarios[pct] = {
                "prix_negocie": round(prix_negocie, 2),
                "renta_brute": renta,
            }

            # Mapper vers les noms de champs standards
            if pct == 0:
                renta_results["renta_brute"] = renta
            else:
                renta_results[f"renta_brute_nego_{pct}"] = renta

        result: dict[str, Any] = {
            **renta_results,
            "loyer_annuel": round(loyer_annuel, 2),
            "charges_annuelles": (
                round(charges_annuelles, 2) if charges_annuelles is not None else None
            ),
            "prix_original": prix,
            "loyer_mensuel": loyer_mensuel,
            "scenarios": scenarios,
        }

        logger.debug(
            "Rentabilite calculee pour prix=%d, loyer=%f : brute=%.2f%%",
            prix,
            loyer_mensuel,
            result.get("renta_brute", 0.0),
        )

        return result

    @staticmethod
    def renta_brute_simple(prix: int, loyer_mensuel: float) -> float:
        """Calcul rapide de la rentabilite brute sans negociation.

        Methode utilitaire pour un calcul simple et rapide.

        Args:
            prix: Prix d'achat en euros.
            loyer_mensuel: Loyer mensuel en euros.

        Returns:
            Rentabilite brute en pourcentage. Retourne 0.0 si le prix est <= 0.
        """
        if prix <= 0:
            return 0.0
        if loyer_mensuel < 0:
            return 0.0
        return round((loyer_mensuel * 12 / prix) * 100.0, 2)
