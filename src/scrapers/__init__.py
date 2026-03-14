"""Scrapers pour les annonces immobilieres LeBonCoin.

Ce module fournit le scraper LeBonCoin (vente + location)
qui herite de BaseScraper et implemente les methodes abstraites
pour l'extraction des donnees.
"""

from src.scrapers.base import BaseScraper
from src.scrapers.leboncoin import LeBonCoinScraper

__all__ = [
    "BaseScraper",
    "LeBonCoinScraper",
]
