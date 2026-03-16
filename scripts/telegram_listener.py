#!/usr/bin/env python3
"""Listener Telegram ImmoScan - analyse d'annonces a la demande.

Service permanent qui ecoute les messages Telegram et analyse
les URLs LeBonCoin soumises par l'utilisateur.

Planification : tourne en continu via systemd (immoscan-listener.service).
"""

from __future__ import annotations

import logging
import os
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("listener")

AUTHORIZED_CHAT_ID = int(os.environ.get("TELEGRAM_CHAT_ID", "0"))
LBC_URL_RE = re.compile(r"https?://(?:www\.)?leboncoin\.fr/\S+")


async def handle_message(update, context) -> None:
    """Traite les messages entrants : detecte les URLs LeBonCoin et lance l'analyse."""
    message = update.message
    if not message or not message.text:
        return

    # Securite : uniquement le chat autorise
    if message.chat_id != AUTHORIZED_CHAT_ID:
        logger.warning("Message ignore (chat_id=%s)", message.chat_id)
        return

    # Detecter une URL LeBonCoin
    match = LBC_URL_RE.search(message.text)
    if not match:
        return

    url = match.group(0)
    # Nettoyer les caracteres de fin (ponctuation, etc.)
    url = url.rstrip(".,;:!?)")
    logger.info("Analyse demandee: %s", url)

    # Message de statut
    status_msg = await message.reply_text("\U0001f50d Analyse en cours...")

    try:
        from scripts.analyze_url import analyze

        result = analyze(url)

        if result:
            from src.alerts.telegram_bot import TelegramBot

            msg = result["formatted_message"]
            esc = TelegramBot._escape_markdown(msg)

            await context.bot.send_message(
                chat_id=message.chat_id,
                text=esc,
                parse_mode="MarkdownV2",
            )
            await status_msg.delete()

            sd = result.get("score_data", {})
            logger.info(
                "Analyse envoyee: score=%.0f, niveau=%s",
                sd.get("score_global", 0),
                result.get("niveau", "?"),
            )
        else:
            await status_msg.edit_text(
                "\u274c Impossible d'analyser cette annonce."
            )

    except Exception as e:
        logger.error("Erreur analyse: %s", e, exc_info=True)
        try:
            await status_msg.edit_text(
                f"\u274c Erreur: {str(e)[:200]}"
            )
        except Exception:
            pass


async def handle_error(update, context) -> None:
    """Log les erreurs sans crasher le listener."""
    logger.error("Erreur Telegram: %s", context.error, exc_info=context.error)


def main() -> None:
    from telegram.ext import Application, MessageHandler, filters

    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    if not bot_token:
        logger.error("TELEGRAM_BOT_TOKEN non configure")
        sys.exit(1)

    if not AUTHORIZED_CHAT_ID:
        logger.error("TELEGRAM_CHAT_ID non configure")
        sys.exit(1)

    app = Application.builder().token(bot_token).build()
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message)
    )
    app.add_error_handler(handle_error)

    logger.info(
        "Listener demarre (chat_id autorise: %s)", AUTHORIZED_CHAT_ID
    )
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
