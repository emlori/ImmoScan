"""Enrichissement IA des annonces via l'API Claude Haiku.

Analyse les descriptions d'annonces immobilieres pour en extraire
des signaux de negociation, l'etat du bien, les equipements,
les red flags et un resume structure.

Respecte un plafond de 300 appels/jour et implemente un retry
avec backoff exponentiel sur les erreurs 429/500.

Suit les best practices Anthropic :
- System prompt pour le role et les regles
- Few-shot examples avec XML tags
- Prefilling de la reponse assistant pour forcer le JSON
"""

from __future__ import annotations

import json
import logging
import time
from datetime import date
from pathlib import Path
from typing import Any

import anthropic

logger = logging.getLogger(__name__)

# Repertoire des prompts
_PROMPTS_DIR = Path(__file__).resolve().parent.parent.parent / "prompts"

# Schema de reponse attendu de Claude
EXPECTED_KEYS = {
    "signaux_nego",
    "etat_bien",
    "equipements",
    "red_flags",
    "info_copro",
    "estimation_travaux",
    "scenarios_location",
    "prix_m2_marche",
    "resume",
}

# Valeurs autorisees pour etat_bien
VALID_ETAT_BIEN = {
    "neuf",
    "tres_bon_etat",
    "bon_etat",
    "correct",
    "a_rafraichir",
    "travaux_importants",
    "a_renover",
    "inconnu",
}


def _load_prompt_file(filename: str) -> str:
    """Charge un fichier prompt depuis le dossier prompts/.

    Args:
        filename: Nom du fichier prompt.

    Returns:
        Contenu du fichier prompt.
    """
    filepath = _PROMPTS_DIR / filename
    if not filepath.exists():
        logger.warning("Fichier prompt introuvable : %s", filepath)
        return ""
    with open(filepath, encoding="utf-8") as f:
        return f.read()


class ClaudeEnricher:
    """Enrichissement d'annonces immobilieres via Claude Haiku.

    Analyse la description textuelle d'une annonce pour en extraire
    des informations structurees : signaux de negociation, etat du bien,
    equipements, red flags, informations de copropriete et resume.

    Utilise les best practices Anthropic :
    - System prompt separe pour le role et les contraintes
    - Few-shot examples avec balises XML pour calibrer les reponses
    - Prefilling du message assistant pour forcer la sortie JSON

    Attributes:
        api_key: Cle API Anthropic.
        model: Nom du modele Claude a utiliser.
        max_daily_calls: Nombre maximum d'appels API par jour.
        max_retries: Nombre maximum de tentatives en cas d'erreur.
        base_delay: Delai de base pour le backoff exponentiel (secondes).
    """

    def __init__(
        self,
        api_key: str,
        model: str = "claude-haiku-4-5-20251001",
        max_daily_calls: int = 300,
        max_retries: int = 3,
        base_delay: float = 1.0,
        client: anthropic.Anthropic | None = None,
    ) -> None:
        """Initialise le module d'enrichissement Claude.

        Args:
            api_key: Cle API Anthropic.
            model: Nom du modele Claude a utiliser.
            max_daily_calls: Nombre maximum d'appels API par jour.
            max_retries: Nombre maximum de tentatives sur erreur 429/500.
            base_delay: Delai de base pour le backoff exponentiel (secondes).
            client: Client Anthropic pre-configure (pour les tests).
        """
        self.api_key = api_key
        self.model = model
        self.max_daily_calls = max_daily_calls
        self.max_retries = max_retries
        self.base_delay = base_delay

        self._daily_call_count: int = 0
        self._daily_reset_date: date = date.today()

        if client is not None:
            self._client = client
        else:
            self._client = anthropic.Anthropic(api_key=api_key)

        # Charger les prompts depuis les fichiers
        self._system_prompt = _load_prompt_file("enrichment_system.md")
        self._examples_prompt = _load_prompt_file("enrichment_examples.md")

        logger.info(
            "ClaudeEnricher initialise (modele=%s, max_daily=%d)",
            self.model,
            self.max_daily_calls,
        )

    def enrich(self, annonce_data: dict[str, Any]) -> dict[str, Any] | None:
        """Enrichit une annonce en appelant Claude Haiku.

        Construit un prompt structure a partir des donnees de l'annonce,
        appelle l'API Claude avec system prompt + few-shot + prefilling,
        puis parse et valide la reponse JSON.

        Args:
            annonce_data: Dictionnaire contenant les donnees de l'annonce.
                Champs attendus : description_texte, prix, surface_m2,
                nb_pieces, quartier, dpe, adresse_brute, charges_copro.

        Returns:
            Dictionnaire structure avec les champs : signaux_nego,
            etat_bien, equipements, red_flags, info_copro, resume.
            None en cas d'echec (limite atteinte, erreur API persistante,
            reponse non parseable).
        """
        if not self._check_daily_limit():
            logger.warning(
                "Limite quotidienne atteinte (%d/%d). Enrichissement ignore.",
                self._daily_call_count,
                self.max_daily_calls,
            )
            return None

        user_message = self._build_user_message(annonce_data)
        system_prompt = self._build_system_prompt()

        for attempt in range(self.max_retries):
            try:
                response = self._client.messages.create(
                    model=self.model,
                    max_tokens=1024,
                    system=system_prompt,
                    messages=[
                        {"role": "user", "content": user_message},
                        # Prefilling : force Claude a commencer par "{"
                        {"role": "assistant", "content": "{"},
                    ],
                )

                self._daily_call_count += 1
                response_text = "{" + response.content[0].text
                result = self._parse_response(response_text)

                if result is not None:
                    logger.info(
                        "Enrichissement reussi (appel %d/%d du jour)",
                        self._daily_call_count,
                        self.max_daily_calls,
                    )
                    return result

                logger.warning(
                    "Reponse Claude non parseable (tentative %d/%d)",
                    attempt + 1,
                    self.max_retries,
                )

            except anthropic.RateLimitError as exc:
                delay = self.base_delay * (2**attempt)
                logger.warning(
                    "Rate limit 429 (tentative %d/%d), attente %.1fs : %s",
                    attempt + 1,
                    self.max_retries,
                    delay,
                    exc,
                )
                if attempt < self.max_retries - 1:
                    time.sleep(delay)

            except anthropic.InternalServerError as exc:
                delay = self.base_delay * (2**attempt)
                logger.warning(
                    "Erreur serveur 500 (tentative %d/%d), attente %.1fs : %s",
                    attempt + 1,
                    self.max_retries,
                    delay,
                    exc,
                )
                if attempt < self.max_retries - 1:
                    time.sleep(delay)

            except anthropic.APIError as exc:
                logger.error(
                    "Erreur API non recuperable : %s",
                    exc,
                )
                return None

            except Exception as exc:
                logger.error(
                    "Erreur inattendue lors de l'enrichissement : %s",
                    exc,
                    exc_info=True,
                )
                return None

        logger.error(
            "Echec de l'enrichissement apres %d tentatives.",
            self.max_retries,
        )
        return None

    def _build_system_prompt(self) -> str:
        """Construit le system prompt avec role, regles et exemples.

        Returns:
            System prompt complet pour Claude.
        """
        parts = []

        if self._system_prompt:
            parts.append(self._system_prompt)
        else:
            # Fallback inline si le fichier prompt n'existe pas
            parts.append(
                "Tu es un analyste expert en investissement locatif "
                "specialise sur Besancon. Analyse les annonces immobilieres "
                "et retourne UNIQUEMENT un objet JSON valide."
            )

        if self._examples_prompt:
            parts.append(self._examples_prompt)

        return "\n\n".join(parts)

    def _build_user_message(self, annonce_data: dict[str, Any]) -> str:
        """Construit le message utilisateur avec les donnees de l'annonce.

        Utilise des balises XML pour structurer clairement les donnees
        d'entree conformement aux best practices Anthropic.

        Args:
            annonce_data: Dictionnaire contenant les donnees de l'annonce.

        Returns:
            Message utilisateur formate avec balises XML.
        """
        description = annonce_data.get("description_texte", "Non disponible")
        prix = annonce_data.get("prix", "Non renseigne")
        surface = annonce_data.get("surface_m2", "Non renseigne")
        nb_pieces = annonce_data.get("nb_pieces", "Non renseigne")
        quartier = annonce_data.get("quartier", "Non renseigne")
        dpe = annonce_data.get("dpe", "Non renseigne")
        adresse = annonce_data.get("adresse_brute", "Non renseignee")
        charges = annonce_data.get("charges_copro", "Non renseigne")

        return f"""Analyse cette annonce et retourne ton analyse JSON.

<annonce>
- Description : {description}
- Prix : {prix} EUR
- Surface : {surface} m2
- Nombre de pieces : {nb_pieces}
- Quartier : {quartier}
- DPE : {dpe}
- Adresse : {adresse}
- Charges copropriete : {charges} EUR/mois
</annonce>"""

    def _parse_response(self, response_text: str) -> dict[str, Any] | None:
        """Parse et valide la reponse JSON de Claude.

        Extrait le JSON de la reponse textuelle, verifie que toutes
        les cles attendues sont presentes et normalise les types.

        Args:
            response_text: Texte brut de la reponse Claude.

        Returns:
            Dictionnaire valide avec le schema attendu, ou None
            si le parsing ou la validation echoue.
        """
        try:
            # Tenter d'extraire le JSON directement
            text = response_text.strip()

            # Enlever les eventuels marqueurs de code markdown
            if text.startswith("```json"):
                text = text[7:]
            elif text.startswith("```"):
                text = text[3:]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()

            parsed = json.loads(text)
        except (json.JSONDecodeError, ValueError) as exc:
            logger.warning("Impossible de parser le JSON : %s", exc)
            return None

        if not isinstance(parsed, dict):
            logger.warning("La reponse n'est pas un dictionnaire JSON.")
            return None

        # Verifier que toutes les cles attendues sont presentes
        missing_keys = EXPECTED_KEYS - set(parsed.keys())
        if missing_keys:
            logger.warning("Cles manquantes dans la reponse : %s", missing_keys)
            return None

        # Normaliser et valider les types
        result: dict[str, Any] = {}

        # signaux_nego : liste de chaines
        signaux = parsed.get("signaux_nego", [])
        result["signaux_nego"] = (
            list(signaux) if isinstance(signaux, list) else []
        )

        # etat_bien : chaine
        etat = parsed.get("etat_bien", "inconnu")
        if isinstance(etat, str) and etat in VALID_ETAT_BIEN:
            result["etat_bien"] = etat
        else:
            result["etat_bien"] = "inconnu"

        # equipements : liste de chaines
        equip = parsed.get("equipements", [])
        result["equipements"] = list(equip) if isinstance(equip, list) else []

        # red_flags : liste de chaines
        flags = parsed.get("red_flags", [])
        result["red_flags"] = list(flags) if isinstance(flags, list) else []

        # info_copro : dict ou None
        copro = parsed.get("info_copro")
        if isinstance(copro, dict):
            nb_lots = copro.get("nb_lots")
            charges_copro = copro.get("charges_annuelles_copro")
            charges_lot = copro.get("charges_annuelles_lot")

            # Compatibilite : ancien champ charges_annuelles
            old_charges = copro.get("charges_annuelles")
            if old_charges is not None and charges_lot is None and charges_copro is None:
                charges_lot = old_charges

            # Deduire charges_lot si on a le total + nb_lots
            if charges_lot is None and charges_copro and nb_lots and nb_lots > 0:
                charges_lot = round(charges_copro / nb_lots, 2)

            # Deduire charges_copro si on a le lot + nb_lots
            if charges_copro is None and charges_lot and nb_lots and nb_lots > 0:
                charges_copro = round(charges_lot * nb_lots, 2)

            result["info_copro"] = {
                "nb_lots": nb_lots,
                "charges_annuelles_copro": charges_copro,
                "charges_annuelles_lot": charges_lot,
            }
        else:
            result["info_copro"] = {
                "nb_lots": None,
                "charges_annuelles_copro": None,
                "charges_annuelles_lot": None,
            }

        # estimation_travaux : dict
        travaux = parsed.get("estimation_travaux")
        if isinstance(travaux, dict):
            result["estimation_travaux"] = {
                "necessaire": bool(travaux.get("necessaire", False)),
                "description": travaux.get("description"),
                "budget_bas": travaux.get("budget_bas"),
                "budget_haut": travaux.get("budget_haut"),
            }
        else:
            result["estimation_travaux"] = {
                "necessaire": False,
                "description": None,
                "budget_bas": None,
                "budget_haut": None,
            }

        # scenarios_location : dict
        scenarios = parsed.get("scenarios_location")
        if isinstance(scenarios, dict):
            standard = scenarios.get("standard", {})
            coloc = scenarios.get("colocation", {})
            result["scenarios_location"] = {
                "standard": {
                    "loyer_nu": standard.get("loyer_nu") if isinstance(standard, dict) else None,
                    "loyer_meuble": standard.get("loyer_meuble") if isinstance(standard, dict) else None,
                },
                "colocation": {
                    "nb_chambres": coloc.get("nb_chambres") if isinstance(coloc, dict) else None,
                    "loyer_par_chambre": coloc.get("loyer_par_chambre") if isinstance(coloc, dict) else None,
                    "loyer_total": coloc.get("loyer_total") if isinstance(coloc, dict) else None,
                },
            }
        else:
            result["scenarios_location"] = {
                "standard": {"loyer_nu": None, "loyer_meuble": None},
                "colocation": {"nb_chambres": None, "loyer_par_chambre": None, "loyer_total": None},
            }

        # prix_m2_marche : dict
        prix_m2 = parsed.get("prix_m2_marche")
        if isinstance(prix_m2, dict):
            result["prix_m2_marche"] = {
                "fourchette_basse": prix_m2.get("fourchette_basse"),
                "fourchette_haute": prix_m2.get("fourchette_haute"),
            }
        else:
            result["prix_m2_marche"] = {
                "fourchette_basse": None,
                "fourchette_haute": None,
            }

        # resume : chaine
        resume = parsed.get("resume", "")
        result["resume"] = str(resume) if resume else ""

        return result

    def _check_daily_limit(self) -> bool:
        """Verifie si la limite quotidienne d'appels est atteinte.

        Reinitialise automatiquement le compteur si la date a change.

        Returns:
            True si un appel supplementaire est autorise, False sinon.
        """
        today = date.today()
        if today != self._daily_reset_date:
            self.reset_daily_counter()

        return self._daily_call_count < self.max_daily_calls

    def reset_daily_counter(self) -> None:
        """Reinitialise le compteur d'appels quotidiens.

        Appele automatiquement a minuit ou manuellement pour les tests.
        """
        self._daily_call_count = 0
        self._daily_reset_date = date.today()
        logger.info("Compteur quotidien reinitialise.")

    @property
    def daily_call_count(self) -> int:
        """Nombre d'appels API effectues aujourd'hui.

        Returns:
            Nombre d'appels effectues depuis la derniere reinitialisation.
        """
        return self._daily_call_count
