"""Scrapers pour les differentes sources d'annonces immobilieres.

Ce module fournit les scrapers pour chaque source d'annonces :
- LeBonCoin (vente + location)
- PAP (vente + location)
- SeLoger (location uniquement en v1)

Tous les scrapers heritent de BaseScraper et implementent les methodes
abstraites pour l'extraction des donnees.
"""

from src.scrapers.base import BaseScraper
from src.scrapers.leboncoin import LeBonCoinScraper
from src.scrapers.pap import PAPScraper
from src.scrapers.seloger import SeLogerScraper

__all__ = [
    "BaseScraper",
    "LeBonCoinScraper",
    "PAPScraper",
    "SeLogerScraper",
]
