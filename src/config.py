"""Configuration centralisee pour ImmoScan.

Utilise pydantic-settings pour charger et valider les variables d'environnement,
et fournit des methodes pour charger les fichiers YAML de configuration.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Racine du projet (parent du dossier src/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"


class DatabaseSettings(BaseSettings):
    """Configuration de la connexion PostgreSQL."""

    model_config = SettingsConfigDict(env_prefix="", extra="ignore")

    database_url: str = Field(
        default="postgresql://immoscan:password@localhost:5432/immoscan",
        description="URL de connexion PostgreSQL",
    )

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, v: str) -> str:
        """Verifie que l'URL commence par postgresql://."""
        if not v.startswith(("postgresql://", "postgresql+asyncpg://")):
            raise ValueError("DATABASE_URL doit commencer par 'postgresql://'")
        return v


class AnthropicSettings(BaseSettings):
    """Configuration de l'API Claude."""

    model_config = SettingsConfigDict(env_prefix="ANTHROPIC_", extra="ignore")

    api_key: str = Field(
        default="",
        description="Cle API Anthropic",
    )
    model: str = Field(
        default="claude-haiku-4-5-20251001",
        description="Modele Claude a utiliser",
    )
    max_daily_calls: int = Field(
        default=300,
        description="Nombre maximum d'appels API par jour",
        ge=0,
    )


class TelegramSettings(BaseSettings):
    """Configuration du bot Telegram."""

    model_config = SettingsConfigDict(env_prefix="TELEGRAM_", extra="ignore")

    bot_token: str = Field(
        default="",
        description="Token du bot Telegram",
    )
    chat_id: str = Field(
        default="",
        description="ID du chat Telegram pour les alertes",
    )


class ProxySettings(BaseSettings):
    """Configuration des proxies."""

    model_config = SettingsConfigDict(env_prefix="PROXY_", extra="ignore")

    pool_url: str = Field(
        default="",
        description="URL du pool de proxies",
    )
    enabled: bool = Field(
        default=True,
        description="Activer l'utilisation des proxies",
    )


class ScrapingSettings(BaseSettings):
    """Configuration du scraping."""

    model_config = SettingsConfigDict(env_prefix="SCRAPING_", extra="ignore")

    delay_min: int = Field(
        default=2,
        description="Delai minimum entre requetes (secondes)",
        ge=1,
    )
    delay_max: int = Field(
        default=5,
        description="Delai maximum entre requetes (secondes)",
        ge=1,
    )

    @field_validator("delay_max")
    @classmethod
    def validate_delay_max(cls, v: int, info: Any) -> int:
        """Verifie que delay_max >= delay_min."""
        delay_min = info.data.get("delay_min", 2)
        if v < delay_min:
            raise ValueError("SCRAPING_DELAY_MAX doit etre >= SCRAPING_DELAY_MIN")
        return v


class ScraplingSettings(BaseSettings):
    """Configuration specifique a Scrapling."""

    model_config = SettingsConfigDict(env_prefix="SCRAPLING_", extra="ignore")

    adaptive: bool = Field(
        default=True,
        description="Activer le mode adaptatif de Scrapling",
    )


class Settings(BaseSettings):
    """Configuration principale de l'application ImmoScan.

    Charge les variables d'environnement depuis un fichier .env
    et valide toutes les valeurs.
    """

    model_config = SettingsConfigDict(
        env_file=str(PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Sous-configurations
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    anthropic: AnthropicSettings = Field(default_factory=AnthropicSettings)
    telegram: TelegramSettings = Field(default_factory=TelegramSettings)
    proxy: ProxySettings = Field(default_factory=ProxySettings)
    scraping: ScrapingSettings = Field(default_factory=ScrapingSettings)
    scrapling: ScraplingSettings = Field(default_factory=ScraplingSettings)

    @staticmethod
    def load_yaml(filename: str) -> dict[str, Any]:
        """Charge un fichier YAML depuis le dossier config/.

        Args:
            filename: Nom du fichier YAML (ex: 'sources.yaml').

        Returns:
            Dictionnaire avec le contenu du fichier YAML.

        Raises:
            FileNotFoundError: Si le fichier n'existe pas.
            yaml.YAMLError: Si le fichier YAML est malforma.
        """
        filepath = CONFIG_DIR / filename
        if not filepath.exists():
            raise FileNotFoundError(f"Fichier de configuration introuvable : {filepath}")
        with open(filepath, encoding="utf-8") as f:
            return yaml.safe_load(f)

    def load_sources(self) -> dict[str, Any]:
        """Charge la configuration des sources de scraping.

        Returns:
            Dictionnaire avec la configuration des sources.
        """
        return self.load_yaml("sources.yaml")

    def load_scoring(self) -> dict[str, Any]:
        """Charge la configuration du scoring.

        Returns:
            Dictionnaire avec les poids et seuils de scoring.
        """
        return self.load_yaml("scoring.yaml")

    def load_quartiers(self) -> dict[str, Any]:
        """Charge la configuration des quartiers.

        Returns:
            Dictionnaire avec les zones geographiques et leurs parametres.
        """
        return self.load_yaml("quartiers.yaml")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Retourne l'instance singleton des parametres de l'application.

    Utilise un cache LRU pour eviter de recharger la configuration
    a chaque appel.

    Returns:
        Instance de Settings configuree.
    """
    return Settings()
