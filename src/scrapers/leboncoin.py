"""Scraper LeBonCoin pour les annonces immobilieres a Besancon.

Implemente le scraping des annonces de vente et de location
depuis LeBonCoin, avec gestion des specificites du site :
structure HTML, pagination, parametres de recherche.
"""

from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from src.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)


class LeBonCoinScraper(BaseScraper):
    """Scraper pour les annonces LeBonCoin.

    Supporte le scraping des annonces de vente (appartements T2/T3
    a Besancon, 120k-160k) et de location (observatoire des loyers).

    Le scraper utilise les selecteurs CSS definis dans sources.yaml
    et gere la pagination specifique de LeBonCoin.

    Attributes:
        source_name: Toujours 'leboncoin'.
    """

    def __init__(self, **kwargs: Any) -> None:
        """Initialise le scraper LeBonCoin.

        Args:
            **kwargs: Arguments transmis a BaseScraper (settings, fetcher).
        """
        super().__init__(source_name="leboncoin", **kwargs)

    def _get_search_url(self, scrape_type: str) -> str:
        """Construit l'URL de recherche LeBonCoin.

        Utilise l'URL de base configuree dans sources.yaml pour le type
        de scraping demande (vente ou location).

        Args:
            scrape_type: 'vente' ou 'location'.

        Returns:
            URL de recherche LeBonCoin complete.
        """
        base_urls = self.source_config.get("base_urls", {})
        url = base_urls.get(scrape_type, "")
        if not url:
            logger.error(
                "Aucune URL configuree pour LeBonCoin type '%s'", scrape_type
            )
        return url

    def _parse_listing_page(
        self, response: Any, scrape_type: str
    ) -> list[dict[str, Any]]:
        """Parse une page de resultats LeBonCoin.

        Extrait les informations de base de chaque carte d'annonce :
        titre, prix, surface, nombre de pieces, localisation et lien.

        Args:
            response: Reponse Scrapling de la page de listing.
            scrape_type: 'vente' ou 'location'.

        Returns:
            Liste de dictionnaires avec les donnees de base par annonce.
        """
        listings: list[dict[str, Any]] = []
        container_selector = self.selectors.get("listing_container", "")

        if not container_selector:
            logger.warning("Selecteur listing_container manquant pour LeBonCoin")
            return listings

        try:
            cards = response.css(container_selector)
        except Exception:
            logger.error("Erreur CSS sur le listing LeBonCoin", exc_info=True)
            return listings

        logger.debug("LeBonCoin: %d cartes trouvees sur la page", len(cards))

        for card in cards:
            try:
                listing = self._parse_card(card, scrape_type)
                if listing:
                    listings.append(listing)
            except Exception:
                logger.debug(
                    "Erreur parsing d'une carte LeBonCoin", exc_info=True
                )

        return listings

    def _parse_card(
        self, card: Any, scrape_type: str
    ) -> dict[str, Any] | None:
        """Parse une carte d'annonce individuelle depuis la page de listing.

        Args:
            card: Element HTML de la carte d'annonce.
            scrape_type: 'vente' ou 'location'.

        Returns:
            Dictionnaire avec les donnees extraites, ou None si echec.
        """
        data: dict[str, Any] = {}

        # Titre
        title_el = card.css_first(self.selectors.get("title", ""))
        if title_el:
            data["titre"] = title_el.text.strip()

        # Lien vers la page de detail
        link_el = card.css_first(self.selectors.get("link", ""))
        if link_el:
            href = link_el.attrib.get("href", "")
            if href and not href.startswith("http"):
                href = f"https://www.leboncoin.fr{href}"
            data["url_source"] = href
        else:
            # Sans lien, l'annonce est inutilisable
            return None

        # Prix
        price_el = card.css_first(self.selectors.get("price", ""))
        if price_el:
            data["prix"] = self._parse_price(price_el.text)

        # Surface
        surface_el = card.css_first(self.selectors.get("surface", ""))
        if surface_el:
            data["surface_m2"] = self._parse_surface(surface_el.text)

        # Nombre de pieces
        rooms_el = card.css_first(self.selectors.get("rooms", ""))
        if rooms_el:
            data["nb_pieces"] = self._parse_rooms(rooms_el.text)

        # Localisation
        location_el = card.css_first(self.selectors.get("location", ""))
        if location_el:
            data["adresse_brute"] = location_el.text.strip()

        return data

    def _parse_detail_page(
        self, response: Any, scrape_type: str
    ) -> dict[str, Any]:
        """Parse une page de detail d'annonce LeBonCoin.

        Extrait les informations detaillees : description complete,
        DPE, etage, charges de copropriete, photos, date de publication.

        Args:
            response: Reponse Scrapling de la page de detail.
            scrape_type: 'vente' ou 'location'.

        Returns:
            Dictionnaire avec les donnees detaillees de l'annonce.
        """
        data: dict[str, Any] = {}

        # Description
        desc_el = response.css_first(self.selectors.get("description", ""))
        if desc_el:
            data["description_texte"] = desc_el.text.strip()

        # DPE
        dpe_el = response.css_first(self.selectors.get("dpe", ""))
        if dpe_el:
            data["dpe"] = self._parse_dpe(dpe_el.text)

        # Photos
        photo_els = response.css(self.selectors.get("photos", ""))
        if photo_els:
            data["photos_urls"] = [
                img.attrib.get("src", "")
                for img in photo_els
                if img.attrib.get("src")
            ]

        # Criteres specifiques de la page de detail
        # LeBonCoin utilise des blocs de criteres avec des labels
        criteria_items = response.css("div[data-qa-id='criteria_item']")
        for item in criteria_items:
            label_el = item.css_first("div[data-qa-id='criteria_item_label']")
            value_el = item.css_first("div[data-qa-id='criteria_item_value']")
            if label_el and value_el:
                label = label_el.text.strip().lower()
                value = value_el.text.strip()
                self._extract_criterion(data, label, value, scrape_type)

        # Date de publication (balise time ou meta)
        date_el = response.css_first("time[datetime]")
        if date_el:
            data["date_publication"] = date_el.attrib.get("datetime", "")

        return data

    def _extract_criterion(
        self,
        data: dict[str, Any],
        label: str,
        value: str,
        scrape_type: str,
    ) -> None:
        """Extrait un critere specifique de la page de detail LeBonCoin.

        Args:
            data: Dictionnaire de donnees a enrichir.
            label: Label du critere (en minuscules).
            value: Valeur du critere.
            scrape_type: 'vente' ou 'location'.
        """
        if "surface" in label:
            data.setdefault("surface_m2", self._parse_surface(value))
        elif "pièce" in label or "piece" in label:
            data.setdefault("nb_pieces", self._parse_rooms(value))
        elif "étage" in label or "etage" in label:
            data["etage"] = self._parse_integer(value)
        elif "charges" in label:
            data["charges_copro"] = self._parse_float(value)
        elif "ges" in label or "énergie" in label or "energie" in label:
            # Le DPE est parfois dans les criteres
            parsed_dpe = self._parse_dpe(value)
            if parsed_dpe:
                data.setdefault("dpe", parsed_dpe)
        elif "meublé" in label or "meuble" in label:
            if scrape_type == "location":
                data["meuble"] = value.lower() in ("oui", "meublé", "meuble")
        elif "loyer" in label and scrape_type == "location":
            data.setdefault("loyer_cc", self._parse_float(value))

    def _get_next_page_url(self, response: Any, current_page: int) -> str | None:
        """Determine l'URL de la page suivante LeBonCoin.

        LeBonCoin utilise un parametre 'page' dans l'URL pour la pagination.

        Args:
            response: Reponse Scrapling de la page courante.
            current_page: Numero de la page courante.

        Returns:
            URL de la page suivante, ou None si derniere page.
        """
        next_page = current_page + 1
        if next_page > self._max_pages:
            return None

        # Construire l'URL avec le parametre de page
        current_url = str(response.url) if hasattr(response, "url") else ""
        if not current_url:
            return None

        return self._add_page_param(current_url, next_page)

    def _add_page_param(self, url: str, page: int) -> str:
        """Ajoute ou met a jour le parametre de page dans une URL.

        Args:
            url: URL a modifier.
            page: Numero de page a inserer.

        Returns:
            URL modifiee avec le parametre de page.
        """
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        params[self._pagination_param] = [str(page)]
        new_query = urlencode(params, doseq=True)
        return urlunparse(parsed._replace(query=new_query))

    # ------------------------------------------------------------------
    # Methodes utilitaires de parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_price(text: str) -> int | None:
        """Extrait un prix entier depuis un texte.

        Gere les formats courants : '140 000 €', '140000€', '140.000 €'.

        Args:
            text: Texte contenant le prix.

        Returns:
            Prix en euros (entier), ou None si non parsable.
        """
        if not text:
            return None
        cleaned = re.sub(r"[^\d]", "", text)
        if cleaned:
            try:
                return int(cleaned)
            except ValueError:
                return None
        return None

    @staticmethod
    def _parse_surface(text: str) -> float | None:
        """Extrait une surface en m2 depuis un texte.

        Gere les formats : '55 m²', '55m2', '55,5 m²'.

        Args:
            text: Texte contenant la surface.

        Returns:
            Surface en metres carres (float), ou None si non parsable.
        """
        if not text:
            return None
        match = re.search(r"(\d+[.,]?\d*)\s*m", text)
        if match:
            value = match.group(1).replace(",", ".")
            try:
                return float(value)
            except ValueError:
                return None
        return None

    @staticmethod
    def _parse_rooms(text: str) -> int | None:
        """Extrait un nombre de pieces depuis un texte.

        Gere les formats : '3 pièces', 'T3', '3p'.

        Args:
            text: Texte contenant le nombre de pieces.

        Returns:
            Nombre de pieces (entier), ou None si non parsable.
        """
        if not text:
            return None
        match = re.search(r"(\d+)", text)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return None
        return None

    @staticmethod
    def _parse_dpe(text: str) -> str | None:
        """Extrait la classe DPE depuis un texte.

        Cherche une lettre A-G isolee en fin de chaine, precedee d'un espace,
        d'un deux-points ou en debut de chaine. Gere les formats :
        'C', 'Classe A', 'DPE : B'.

        Args:
            text: Texte contenant le DPE.

        Returns:
            Lettre DPE (A-G) en majuscule, ou None si non parsable.
        """
        if not text:
            return None
        text = text.strip()
        match = re.search(r"(?:^|\s|:)\s*([A-Ga-g])\s*$", text)
        if match:
            return match.group(1).upper()
        return None

    @staticmethod
    def _parse_integer(text: str) -> int | None:
        """Extrait un entier depuis un texte.

        Args:
            text: Texte contenant un nombre entier.

        Returns:
            Entier extrait, ou None si non parsable.
        """
        if not text:
            return None
        match = re.search(r"(\d+)", text)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return None
        return None

    @staticmethod
    def _parse_float(text: str) -> float | None:
        """Extrait un nombre decimal depuis un texte.

        Args:
            text: Texte contenant un nombre.

        Returns:
            Nombre extrait (float), ou None si non parsable.
        """
        if not text:
            return None
        cleaned = re.sub(r"[^\d.,]", "", text)
        cleaned = cleaned.replace(",", ".")
        # Ne garder que le dernier point (separateur decimal)
        parts = cleaned.rsplit(".", 1)
        if len(parts) == 2:
            cleaned = parts[0].replace(".", "") + "." + parts[1]
        else:
            cleaned = cleaned.replace(".", "")
        if cleaned:
            try:
                return float(cleaned)
            except ValueError:
                return None
        return None
