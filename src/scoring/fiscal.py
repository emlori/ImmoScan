"""Estimation fiscale indicative pour investissements locatifs.

Fournit une estimation simplifiee du regime fiscal le plus avantageux
entre LMNP micro-BIC et location nue micro-foncier.

IMPORTANT : Estimation indicative uniquement, non contractuelle.
Consultation d'un specialiste recommandee pour toute decision.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# Taux d'abattement forfaitaire par regime
ABATTEMENT_LMNP_MICRO_BIC = 0.50  # 50% pour LMNP micro-BIC
ABATTEMENT_NU_MICRO_FONCIER = 0.30  # 30% pour location nue micro-foncier

DISCLAIMER = "Estimation indicative uniquement - consultation specialiste recommandee"


class FiscalEstimator:
    """Estimateur fiscal simplifie LMNP vs Location nue.

    Compare les deux regimes micro simplifies :
    - LMNP micro-BIC : abattement forfaitaire de 50% sur les revenus.
    - Location nue micro-foncier : abattement forfaitaire de 30% sur les revenus.

    Le regime reel n'est pas simule dans cette version.

    Attributes:
        abattement_lmnp: Taux d'abattement LMNP micro-BIC (0.50).
        abattement_nu: Taux d'abattement location nue micro-foncier (0.30).
    """

    def __init__(
        self,
        abattement_lmnp: float = ABATTEMENT_LMNP_MICRO_BIC,
        abattement_nu: float = ABATTEMENT_NU_MICRO_FONCIER,
    ) -> None:
        """Initialise l'estimateur fiscal.

        Args:
            abattement_lmnp: Taux d'abattement LMNP micro-BIC (defaut 0.50).
            abattement_nu: Taux d'abattement location nue micro-foncier (defaut 0.30).
        """
        self.abattement_lmnp = abattement_lmnp
        self.abattement_nu = abattement_nu

    def estimate(
        self,
        loyer_annuel: float,
        charges: float | None = None,
    ) -> dict[str, Any]:
        """Estime le resultat fiscal pour les deux regimes micro.

        Args:
            loyer_annuel: Revenus locatifs annuels bruts en euros.
            charges: Charges deductibles annuelles en euros (optionnel,
                utilise uniquement a titre informatif, non deduit en micro).

        Returns:
            Dictionnaire contenant :
                - lmnp_micro (dict) : detail du regime LMNP micro-BIC.
                    - revenu_brut (float) : loyer annuel brut.
                    - abattement (float) : montant de l'abattement.
                    - revenu_imposable (float) : revenu apres abattement.
                    - taux_abattement (float) : taux applique (0.50).
                - nu_micro (dict) : detail du regime location nue micro-foncier.
                    - revenu_brut (float) : loyer annuel brut.
                    - abattement (float) : montant de l'abattement.
                    - revenu_imposable (float) : revenu apres abattement.
                    - taux_abattement (float) : taux applique (0.30).
                - regime_indicatif (str) : 'lmnp' ou 'nu' (le plus avantageux).
                - economie_lmnp (float) : difference de revenu imposable
                    (economie en passant par LMNP vs nu).
                - charges_annuelles (float | None) : charges fournies.
                - disclaimer (str) : avertissement legal.

        Raises:
            ValueError: Si le loyer annuel est negatif.
        """
        if loyer_annuel < 0:
            raise ValueError(
                f"Le loyer annuel ne peut pas etre negatif, recu : {loyer_annuel}"
            )

        # LMNP micro-BIC : abattement 50%
        abattement_lmnp_montant = loyer_annuel * self.abattement_lmnp
        revenu_imposable_lmnp = loyer_annuel - abattement_lmnp_montant

        lmnp_micro: dict[str, float] = {
            "revenu_brut": round(loyer_annuel, 2),
            "abattement": round(abattement_lmnp_montant, 2),
            "revenu_imposable": round(revenu_imposable_lmnp, 2),
            "taux_abattement": self.abattement_lmnp,
        }

        # Location nue micro-foncier : abattement 30%
        abattement_nu_montant = loyer_annuel * self.abattement_nu
        revenu_imposable_nu = loyer_annuel - abattement_nu_montant

        nu_micro: dict[str, float] = {
            "revenu_brut": round(loyer_annuel, 2),
            "abattement": round(abattement_nu_montant, 2),
            "revenu_imposable": round(revenu_imposable_nu, 2),
            "taux_abattement": self.abattement_nu,
        }

        # Determiner le regime le plus avantageux
        # LMNP est plus avantageux car l'abattement est plus eleve (50% > 30%)
        # donc le revenu imposable est plus faible
        if revenu_imposable_lmnp <= revenu_imposable_nu:
            regime_indicatif = "lmnp"
        else:
            regime_indicatif = "nu"

        economie_lmnp = round(revenu_imposable_nu - revenu_imposable_lmnp, 2)

        result: dict[str, Any] = {
            "lmnp_micro": lmnp_micro,
            "nu_micro": nu_micro,
            "regime_indicatif": regime_indicatif,
            "economie_lmnp": economie_lmnp,
            "charges_annuelles": round(charges, 2) if charges is not None else None,
            "disclaimer": DISCLAIMER,
        }

        logger.debug(
            "Estimation fiscale : loyer_annuel=%.2f, LMNP_imposable=%.2f, "
            "nu_imposable=%.2f => regime=%s",
            loyer_annuel,
            revenu_imposable_lmnp,
            revenu_imposable_nu,
            regime_indicatif,
        )

        return result
