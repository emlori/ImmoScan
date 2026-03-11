"""Bot Telegram pour l'envoi d'alertes ImmoScan.

Gere l'envoi des differents types d'alertes (TOP, BON, baisse de prix,
digest quotidien, alertes systeme) via l'API Telegram Bot.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import TelegramError

from src.alerts.formatter import AlertFormatter
from src.config import TelegramSettings, get_settings

logger = logging.getLogger(__name__)


class TelegramBot:
    """Client Telegram pour l'envoi d'alertes ImmoScan.

    Envoie des messages formates en MarkdownV2 sur un chat Telegram configure.
    Supporte les alertes immediates, les digests et les alertes techniques.

    Attributes:
        bot_token: Token d'authentification du bot Telegram.
        chat_id: Identifiant du chat/groupe Telegram cible.
        formatter: Instance d'AlertFormatter pour le formatage des messages.
        _bot: Instance interne de telegram.Bot.
    """

    def __init__(
        self,
        bot_token: str | None = None,
        chat_id: str | None = None,
        formatter: AlertFormatter | None = None,
    ) -> None:
        """Initialise le bot Telegram.

        Args:
            bot_token: Token du bot Telegram. Si None, charge depuis les settings.
            chat_id: ID du chat Telegram. Si None, charge depuis les settings.
            formatter: Instance d'AlertFormatter. Si None, en cree une nouvelle.
        """
        if bot_token is not None and chat_id is not None:
            self.bot_token = bot_token
            self.chat_id = chat_id
        else:
            settings = get_settings()
            self.bot_token = bot_token or settings.telegram.bot_token
            self.chat_id = chat_id or settings.telegram.chat_id

        self.formatter = formatter or AlertFormatter()
        self._bot = Bot(token=self.bot_token)

    async def send_alert(self, message: str, niveau: str) -> bool:
        """Envoie un message d'alerte formate sur Telegram.

        Args:
            message: Texte du message en format MarkdownV2.
            niveau: Niveau d'alerte ('top', 'bon', 'baisse_prix', 'system').

        Returns:
            True si le message a ete envoye avec succes, False sinon.
        """
        try:
            await self._bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode=ParseMode.MARKDOWN_V2,
            )
            logger.info(
                "Alerte Telegram envoyee (niveau=%s, longueur=%d)",
                niveau,
                len(message),
            )
            return True
        except TelegramError as e:
            logger.error(
                "Erreur envoi Telegram (niveau=%s): %s", niveau, str(e)
            )
            return False
        except Exception as e:
            logger.error(
                "Erreur inattendue envoi Telegram (niveau=%s): %s",
                niveau,
                str(e),
            )
            return False

    def send_alert_sync(self, message: str, niveau: str) -> bool:
        """Version synchrone de send_alert.

        Cree ou reutilise une boucle asyncio pour executer l'envoi.

        Args:
            message: Texte du message en format MarkdownV2.
            niveau: Niveau d'alerte ('top', 'bon', 'baisse_prix', 'system').

        Returns:
            True si le message a ete envoye avec succes, False sinon.
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            # Si une boucle est deja en cours, creer une tache
            import concurrent.futures

            with concurrent.futures.ThreadPoolExecutor() as pool:
                result = pool.submit(
                    asyncio.run, self.send_alert(message, niveau)
                ).result()
            return result
        else:
            return asyncio.run(self.send_alert(message, niveau))

    async def send_immediate(
        self,
        annonce_data: dict[str, Any],
        score: dict[str, Any],
        renta: dict[str, Any],
        enrichment: dict[str, Any] | None = None,
    ) -> bool:
        """Envoie une alerte TOP immediate.

        Formate et envoie une alerte TOP OPPORTUNITE pour une annonce
        qui depasse les seuils d'alerte.

        Args:
            annonce_data: Donnees de l'annonce.
            score: Donnees de scoring.
            renta: Donnees de rentabilite.
            enrichment: Donnees d'enrichissement IA (optionnel).

        Returns:
            True si envoye avec succes, False sinon.
        """
        message = self.formatter.format_top_alert(
            annonce_data, score, renta, enrichment
        )
        escaped_message = self._escape_markdown(message)
        return await self.send_alert(escaped_message, "top")

    async def send_bon_alert(
        self,
        annonce_data: dict[str, Any],
        score: dict[str, Any],
        renta: dict[str, Any],
    ) -> bool:
        """Envoie une alerte BON PLAN.

        Formate et envoie une alerte pour un bon plan detecte.

        Args:
            annonce_data: Donnees de l'annonce.
            score: Donnees de scoring.
            renta: Donnees de rentabilite.

        Returns:
            True si envoye avec succes, False sinon.
        """
        message = self.formatter.format_bon_alert(annonce_data, score, renta)
        escaped_message = self._escape_markdown(message)
        return await self.send_alert(escaped_message, "bon")

    async def send_baisse_prix(
        self,
        annonce_data: dict[str, Any],
        ancien_prix: int,
        nouveau_prix: int,
    ) -> bool:
        """Envoie une alerte de baisse de prix.

        Args:
            annonce_data: Donnees de l'annonce.
            ancien_prix: Ancien prix en euros.
            nouveau_prix: Nouveau prix en euros.

        Returns:
            True si envoye avec succes, False sinon.
        """
        message = self.formatter.format_baisse_prix(
            annonce_data, ancien_prix, nouveau_prix
        )
        escaped_message = self._escape_markdown(message)
        return await self.send_alert(escaped_message, "baisse_prix")

    async def send_digest(self, digest_text: str) -> bool:
        """Envoie le digest quotidien.

        Args:
            digest_text: Texte du digest deja formate.

        Returns:
            True si envoye avec succes, False sinon.
        """
        escaped_text = self._escape_markdown(digest_text)
        return await self.send_alert(escaped_text, "digest")

    async def send_system_alert(self, event: str, detail: str) -> bool:
        """Envoie une alerte technique systeme.

        Args:
            event: Type d'evenement (ex: 'source_indisponible').
            detail: Description detaillee de l'evenement.

        Returns:
            True si envoye avec succes, False sinon.
        """
        message = self.formatter.format_system_alert(event, detail)
        escaped_message = self._escape_markdown(message)
        return await self.send_alert(escaped_message, "system")

    @staticmethod
    def _escape_markdown(text: str) -> str:
        """Echappe les caracteres speciaux MarkdownV2 de Telegram.

        Les caracteres suivants doivent etre echappes avec un antislash
        dans le format MarkdownV2 de Telegram, sauf ceux qui font partie
        de la syntaxe Markdown intentionnelle (bold, links, etc.).

        Note: Cette methode echappe de maniere selective pour eviter
        de casser le formatage existant. Les caracteres deja echappes
        ou faisant partie de la syntaxe Markdown ne sont pas re-echappes.

        Args:
            text: Texte brut ou partiellement formate.

        Returns:
            Texte avec les caracteres speciaux echappes pour MarkdownV2.
        """
        # Caracteres speciaux MarkdownV2 qui doivent etre echappes
        # On ne re-echappe pas les caracteres deja echappes
        special_chars = [
            "_", "~", "`", ">", "#", "+", "=", "|", "{", "}", "!", ".",
        ]
        result = text
        for char in special_chars:
            # Ne pas re-echapper ce qui l'est deja
            result = result.replace(f"\\{char}", f"\x00{char}")
            result = result.replace(char, f"\\{char}")
            result = result.replace(f"\x00{char}", f"\\{char}")
        return result
