"""Connexion PostgreSQL et gestion des sessions SQLAlchemy.

Fournit le moteur SQLAlchemy 2.0, la fabrique de sessions, et un
context manager generateur pour obtenir des sessions transactionnelles.
"""

from __future__ import annotations

import logging
from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.config import get_settings

logger = logging.getLogger(__name__)


def get_engine() -> Engine:
    """Cree et retourne le moteur SQLAlchemy connecte a PostgreSQL.

    Configure le pool de connexions avec des parametres adaptes
    a l'usage d'ImmoScan (scraping periodique, pas de charge elevee).

    Returns:
        Instance Engine configuree.
    """
    settings = get_settings()
    database_url = settings.database.database_url

    engine = create_engine(
        database_url,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        pool_recycle=1800,
        pool_pre_ping=True,
        echo=False,
    )

    logger.info("Moteur SQLAlchemy cree pour %s", _mask_url(database_url))
    return engine


def get_session_factory(engine: Engine | None = None) -> sessionmaker[Session]:
    """Cree une fabrique de sessions liee au moteur.

    Args:
        engine: Moteur SQLAlchemy. Si None, un nouveau moteur est cree
            via get_engine().

    Returns:
        Fabrique sessionmaker configuree.
    """
    if engine is None:
        engine = get_engine()

    factory: sessionmaker[Session] = sessionmaker(
        bind=engine,
        autocommit=False,
        autoflush=False,
        expire_on_commit=False,
    )
    return factory


@contextmanager
def get_session(engine: Engine | None = None) -> Generator[Session, None, None]:
    """Context manager fournissant une session transactionnelle.

    Ouvre une session, la fournit au bloc appelant, et gere le
    commit ou rollback automatiquement.

    Args:
        engine: Moteur SQLAlchemy optionnel. Si None, utilise get_engine().

    Yields:
        Session SQLAlchemy active.

    Raises:
        Exception: Re-leve toute exception apres rollback.

    Example:
        >>> with get_session() as session:
        ...     annonce = session.get(Annonce, 1)
        ...     annonce.statut = "vu"
    """
    factory = get_session_factory(engine)
    session = factory()

    try:
        yield session
        session.commit()
        logger.debug("Session committee avec succes.")
    except Exception:
        session.rollback()
        logger.exception("Erreur dans la session, rollback effectue.")
        raise
    finally:
        session.close()


def _mask_url(url: str) -> str:
    """Masque le mot de passe dans une URL de connexion pour les logs.

    Args:
        url: URL de connexion PostgreSQL.

    Returns:
        URL avec le mot de passe remplace par '***'.
    """
    try:
        # Format: postgresql://user:password@host:port/db
        if "@" in url and ":" in url.split("@")[0]:
            prefix, rest = url.split("@", 1)
            scheme_user = prefix.rsplit(":", 1)[0]
            return f"{scheme_user}:***@{rest}"
    except (ValueError, IndexError):
        pass
    return url
