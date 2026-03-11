"""Base de donnees PostgreSQL/PostGIS - modeles et connexion.

Exporte les composants principaux du module de base de donnees :
- Base : classe declarative SQLAlchemy
- Tous les modeles ORM
- Fonctions de connexion et de session
"""

from src.db.connection import get_engine, get_session, get_session_factory
from src.db.models import (
    AlerteLog,
    Annonce,
    Base,
    EnrichissementIA,
    LoyerMarche,
    LoyerReference,
    Quartier,
    Score,
    ScrapingLog,
    ValidationLog,
)

__all__ = [
    "Base",
    "Annonce",
    "Score",
    "EnrichissementIA",
    "Quartier",
    "LoyerMarche",
    "LoyerReference",
    "AlerteLog",
    "ScrapingLog",
    "ValidationLog",
    "get_engine",
    "get_session",
    "get_session_factory",
]
