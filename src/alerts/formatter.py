"""Formatage Markdown des alertes Telegram pour ImmoScan.

Fournit des methodes de mise en forme pour les differents types d'alertes :
TOP opportunite, BON plan, baisse de prix, digest quotidien et alertes systeme.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any


class AlertFormatter:
    """Formate les messages d'alerte en Markdown pour Telegram.

    Gere la mise en forme de tous les types de messages envoyes via Telegram :
    alertes immediates (TOP/BON), baisses de prix, digest quotidien et alertes
    techniques de monitoring.
    """

    def format_top_alert(
        self,
        annonce: dict[str, Any],
        score: dict[str, Any],
        renta: dict[str, Any],
        enrichment: dict[str, Any] | None = None,
    ) -> str:
        """Formate une alerte TOP OPPORTUNITE en Markdown Telegram.

        Args:
            annonce: Donnees de l'annonce (prix, surface_m2, nb_pieces, dpe,
                adresse_brute, quartier, url_source).
            score: Donnees de scoring (score_global).
            renta: Donnees de rentabilite (renta_brute, renta_brute_nego_5,
                renta_brute_nego_10, renta_brute_nego_15, loyer_estime).
            enrichment: Donnees d'enrichissement IA optionnelles (resume_ia).

        Returns:
            Message formate en Markdown.
        """
        prix = annonce.get("prix", 0)
        surface = annonce.get("surface_m2", 0)
        nb_pieces = annonce.get("nb_pieces", 0)
        dpe = annonce.get("dpe", "N/A")
        adresse = annonce.get("adresse_brute", "Adresse inconnue")
        quartier = annonce.get("quartier", "Quartier inconnu")
        url = annonce.get("url_source", "")
        score_global = score.get("score_global", 0)
        renta_brute = renta.get("renta_brute", 0)

        prix_m2 = round(prix / surface) if surface > 0 else 0

        # Scenarios de negociation
        prix_5 = round(prix * 0.95)
        prix_10 = round(prix * 0.90)
        prix_15 = round(prix * 0.85)
        renta_5 = renta.get("renta_brute_nego_5", 0)
        renta_10 = renta.get("renta_brute_nego_10", 0)
        renta_15 = renta.get("renta_brute_nego_15", 0)

        lines = [
            "\U0001f7e2 *TOP OPPORTUNITE*",
            "",
            f"\U0001f4cd {adresse} \\- {quartier}",
            f"\U0001f4b0 {self._format_prix(prix)} \\({self._format_prix(prix_m2)}/m\\u00b2\\)",
            f"\U0001f4d0 {surface}m\\u00b2 \\- T{nb_pieces}",
            f"\U0001f3f7\\ufe0f DPE: {dpe}",
            f"\U0001f4ca Score: {score_global}/100",
            f"\U0001f4b8 Renta brute: {self._format_renta(renta_brute)}",
            "",
            "Scenarios nego:",
            f"\\u2022 \\-5%: {self._format_renta(renta_5)} \\({self._format_prix(prix_5)}\\)",
            f"\\u2022 \\-10%: {self._format_renta(renta_10)} \\({self._format_prix(prix_10)}\\)",
            f"\\u2022 \\-15%: {self._format_renta(renta_15)} \\({self._format_prix(prix_15)}\\)",
        ]

        # Resume IA si disponible
        if enrichment and enrichment.get("resume_ia"):
            lines.append("")
            lines.append(f"\U0001f916 {enrichment['resume_ia']}")

        lines.append(f"\U0001f517 [Voir l'annonce]({url})")

        return "\n".join(lines)

    def format_bon_alert(
        self,
        annonce: dict[str, Any],
        score: dict[str, Any],
        renta: dict[str, Any],
    ) -> str:
        """Formate une alerte BON PLAN en Markdown Telegram.

        Args:
            annonce: Donnees de l'annonce (prix, surface_m2, nb_pieces, dpe,
                adresse_brute, quartier, url_source).
            score: Donnees de scoring (score_global).
            renta: Donnees de rentabilite (renta_brute, renta_brute_nego_5,
                renta_brute_nego_10, renta_brute_nego_15).

        Returns:
            Message formate en Markdown.
        """
        prix = annonce.get("prix", 0)
        surface = annonce.get("surface_m2", 0)
        nb_pieces = annonce.get("nb_pieces", 0)
        dpe = annonce.get("dpe", "N/A")
        adresse = annonce.get("adresse_brute", "Adresse inconnue")
        quartier = annonce.get("quartier", "Quartier inconnu")
        url = annonce.get("url_source", "")
        score_global = score.get("score_global", 0)
        renta_brute = renta.get("renta_brute", 0)

        prix_m2 = round(prix / surface) if surface > 0 else 0

        # Scenarios de negociation
        prix_5 = round(prix * 0.95)
        prix_10 = round(prix * 0.90)
        prix_15 = round(prix * 0.85)
        renta_5 = renta.get("renta_brute_nego_5", 0)
        renta_10 = renta.get("renta_brute_nego_10", 0)
        renta_15 = renta.get("renta_brute_nego_15", 0)

        lines = [
            "\U0001f7e1 *BON PLAN*",
            "",
            f"\U0001f4cd {adresse} \\- {quartier}",
            f"\U0001f4b0 {self._format_prix(prix)} \\({self._format_prix(prix_m2)}/m\\u00b2\\)",
            f"\U0001f4d0 {surface}m\\u00b2 \\- T{nb_pieces}",
            f"\U0001f3f7\\ufe0f DPE: {dpe}",
            f"\U0001f4ca Score: {score_global}/100",
            f"\U0001f4b8 Renta brute: {self._format_renta(renta_brute)}",
            "",
            "Scenarios nego:",
            f"\\u2022 \\-5%: {self._format_renta(renta_5)} \\({self._format_prix(prix_5)}\\)",
            f"\\u2022 \\-10%: {self._format_renta(renta_10)} \\({self._format_prix(prix_10)}\\)",
            f"\\u2022 \\-15%: {self._format_renta(renta_15)} \\({self._format_prix(prix_15)}\\)",
            f"\U0001f517 [Voir l'annonce]({url})",
        ]

        return "\n".join(lines)

    def format_baisse_prix(
        self,
        annonce: dict[str, Any],
        ancien_prix: int,
        nouveau_prix: int,
    ) -> str:
        """Formate une alerte de baisse de prix en Markdown Telegram.

        Args:
            annonce: Donnees de l'annonce (adresse_brute, quartier, surface_m2,
                nb_pieces, url_source).
            ancien_prix: Prix avant la baisse en euros.
            nouveau_prix: Nouveau prix apres la baisse en euros.

        Returns:
            Message formate en Markdown.
        """
        adresse = annonce.get("adresse_brute", "Adresse inconnue")
        quartier = annonce.get("quartier", "Quartier inconnu")
        surface = annonce.get("surface_m2", 0)
        nb_pieces = annonce.get("nb_pieces", 0)
        url = annonce.get("url_source", "")

        diff = ancien_prix - nouveau_prix
        pct = round((diff / ancien_prix) * 100, 1) if ancien_prix > 0 else 0

        lines = [
            "\U0001f4c9 *BAISSE DE PRIX*",
            "",
            f"\U0001f4cd {adresse} \\- {quartier}",
            f"\U0001f4d0 {surface}m\\u00b2 \\- T{nb_pieces}",
            "",
            f"Ancien prix: {self._format_prix(ancien_prix)}",
            f"Nouveau prix: {self._format_prix(nouveau_prix)}",
            f"Baisse: \\-{self._format_prix(diff)} \\(\\-{pct}%\\)",
            "",
            f"\U0001f517 [Voir l'annonce]({url})",
        ]

        return "\n".join(lines)

    def format_digest(
        self,
        top_annonces: list[dict[str, Any]],
        baisses: list[dict[str, Any]],
        stats: dict[str, Any],
        obs_stats: dict[str, Any],
    ) -> str:
        """Formate le digest quotidien (21h) en Markdown Telegram.

        Args:
            top_annonces: Liste des top 3 annonces du jour (chacune avec
                annonce, score, renta).
            baisses: Liste des baisses de prix detectees (chacune avec
                annonce, ancien_prix, nouveau_prix).
            stats: Statistiques du pipeline (nb_scrapees, nb_nouvelles,
                nb_erreurs, sources).
            obs_stats: Statistiques de l'observatoire des loyers
                (nb_locations, segments_couverts, fiabilite).

        Returns:
            Message formate en Markdown.
        """
        date_str = datetime.now().strftime("%d/%m/%Y")

        lines = [
            f"\U0001f4cb *DIGEST QUOTIDIEN \\- {date_str}*",
            "",
        ]

        # Top 3 du jour
        lines.append("*Top 3 du jour:*")
        if top_annonces:
            for i, item in enumerate(top_annonces[:3], 1):
                annonce = item.get("annonce", {})
                score = item.get("score", {})
                renta = item.get("renta", {})
                quartier = annonce.get("quartier", "?")
                prix = annonce.get("prix", 0)
                score_val = score.get("score_global", 0)
                renta_val = renta.get("renta_brute", 0)
                url = annonce.get("url_source", "")
                lines.append(
                    f"{i}\\. {quartier} \\- {self._format_prix(prix)} "
                    f"\\- Score {score_val} \\- Renta {self._format_renta(renta_val)} "
                    f"[lien]({url})"
                )
        else:
            lines.append("Aucune opportunite aujourd'hui\\.")
        lines.append("")

        # Baisses de prix
        lines.append("*Baisses de prix:*")
        if baisses:
            for item in baisses:
                annonce = item.get("annonce", {})
                ancien = item.get("ancien_prix", 0)
                nouveau = item.get("nouveau_prix", 0)
                quartier = annonce.get("quartier", "?")
                diff = ancien - nouveau
                lines.append(
                    f"\\u2022 {quartier}: {self._format_prix(ancien)} "
                    f"\\u2192 {self._format_prix(nouveau)} "
                    f"\\(\\-{self._format_prix(diff)}\\)"
                )
        else:
            lines.append("Aucune baisse detectee\\.")
        lines.append("")

        # Stats pipeline
        nb_scrapees = stats.get("nb_scrapees", 0)
        nb_nouvelles = stats.get("nb_nouvelles", 0)
        nb_erreurs = stats.get("nb_erreurs", 0)
        sources = stats.get("sources", [])

        lines.append("*Stats pipeline:*")
        lines.append(f"\\u2022 Annonces scrapees: {nb_scrapees}")
        lines.append(f"\\u2022 Nouvelles: {nb_nouvelles}")
        lines.append(f"\\u2022 Erreurs: {nb_erreurs}")
        if sources:
            lines.append(f"\\u2022 Sources actives: {', '.join(sources)}")
        lines.append("")

        # Stats Observatoire
        nb_locations = obs_stats.get("nb_locations", 0)
        segments = obs_stats.get("segments_couverts", 0)
        fiabilite = obs_stats.get("fiabilite", "N/A")

        lines.append("*Observatoire loyers:*")
        lines.append(f"\\u2022 Annonces location: {nb_locations}")
        lines.append(f"\\u2022 Segments couverts: {segments}")
        lines.append(f"\\u2022 Fiabilite: {fiabilite}")

        return "\n".join(lines)

    def format_system_alert(self, event: str, detail: str) -> str:
        """Formate une alerte technique systeme en Markdown Telegram.

        Args:
            event: Type d'evenement technique (ex: 'source_indisponible',
                'parsing_rate_low', 'zero_annonces', 'disk_space', etc.).
            detail: Detail textuel de l'evenement.

        Returns:
            Message formate en Markdown.
        """
        timestamp = datetime.now().strftime("%d/%m/%Y %H:%M")

        lines = [
            "\\u26a0\\ufe0f *ALERTE SYSTEME*",
            "",
            f"*Evenement:* {event}",
            f"*Detail:* {detail}",
            f"*Date:* {timestamp}",
        ]

        return "\n".join(lines)

    @staticmethod
    def _format_prix(prix: int | float) -> str:
        """Formate un prix avec des espaces comme separateurs de milliers.

        Args:
            prix: Montant en euros.

        Returns:
            Prix formate (ex: 145000 -> '145 000\\u20ac').
        """
        prix_int = int(round(prix))
        formatted = f"{prix_int:,}".replace(",", " ")
        return f"{formatted}\u20ac"

    @staticmethod
    def _format_renta(renta: float) -> str:
        """Formate une rentabilite brute a une decimale.

        Args:
            renta: Rentabilite brute en pourcentage.

        Returns:
            Rentabilite formatee (ex: 8.234 -> '8.2%').
        """
        return f"{renta:.1f}%"
