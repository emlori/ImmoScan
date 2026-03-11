"""Scraper SeLoger pour les annonces de location a Besancon.

Implemente le scraping des annonces de location uniquement (v1)
depuis SeLoger.com, pour alimenter l'observatoire des loyers.

SeLoger est une source complementaire importante pour les locations,
car elle presente un volume significatif d'annonces professionnelles.
"""

from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from src.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)


class SeLogerScraper(BaseScraper):
    """Scraper pour les annonces de location SeLoger.com.

    En v1, ce scraper ne gere que les annonces de location
    pour alimenter l'observatoire des loyers a Besancon.
    Le scraping des ventes pourra etre ajoute dans une version future.

    SeLoger utilise des attributs data-testid pour ses selecteurs CSS,
    ce qui facilite le ciblage des elements.

    Attributes:
        source_name: Toujours 'seloger'.
    """

    def __init__(self, **kwargs: Any) -> None:
        """Initialise le scraper SeLoger.

        Args:
            **kwargs: Arguments transmis a BaseScraper (settings, fetcher).
        """
        super().__init__(source_name="seloger", **kwargs)

    def _get_search_url(self, scrape_type: str) -> str:
        """Construit l'URL de recherche SeLoger.

        En v1, seule la location est supportee.
        SeLoger utilise des parametres specifiques dans l'URL :
        projects=1 (location), types=1 (appartement), places (code commune).

        Args:
            scrape_type: Doit etre 'location' en v1.

        Returns:
            URL de recherche SeLoger complete.

        Raises:
            ValueError: Si scrape_type n'est pas 'location' en v1.
        """
        if scrape_type != "location":
            logger.error(
                "SeLoger v1 ne supporte que la location, type demande: '%s'",
                scrape_type,
            )
            return ""

        base_urls = self.source_config.get("base_urls", {})
        url = base_urls.get(scrape_type, "")
        if not url:
            logger.error(
                "Aucune URL configuree pour SeLoger type '%s'", scrape_type
            )
        return url

    def _parse_listing_page(
        self, response: Any, scrape_type: str
    ) -> list[dict[str, Any]]:
        """Parse une page de resultats SeLoger.

        Extrait les informations de base de chaque carte d'annonce.
        SeLoger utilise des attributs data-testid pour structurer le DOM.

        Args:
            response: Reponse Scrapling de la page de listing.
            scrape_type: 'location' (seul type supporte en v1).

        Returns:
            Liste de dictionnaires avec les donnees de base par annonce.
        """
        listings: list[dict[str, Any]] = []
        container_selector = self.selectors.get("listing_container", "")

        if not container_selector:
            logger.warning("Selecteur listing_container manquant pour SeLoger")
            return listings

        try:
            cards = response.css(container_selector)
        except Exception:
            logger.error("Erreur CSS sur le listing SeLoger", exc_info=True)
            return listings

        logger.debug("SeLoger: %d cartes trouvees sur la page", len(cards))

        for card in cards:
            try:
                listing = self._parse_card(card)
                if listing:
                    listings.append(listing)
            except Exception:
                logger.debug(
                    "Erreur parsing d'une carte SeLoger", exc_info=True
                )

        return listings

    def _parse_card(self, card: Any) -> dict[str, Any] | None:
        """Parse une carte d'annonce SeLoger depuis la page de listing.

        Args:
            card: Element HTML de la carte d'annonce.

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
                href = f"https://www.seloger.com{href}"
            data["url_source"] = href
        else:
            return None

        # Prix (loyer)
        price_el = card.css_first(self.selectors.get("price", ""))
        if price_el:
            data["loyer_cc"] = self._parse_float(price_el.text)

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
        """Parse une page de detail d'annonce SeLoger.

        Extrait les informations detaillees pour une annonce de location :
        description, DPE, surface, photos, meuble/nu, loyer HC.

        SeLoger structure ses pages de detail avec des sections thematiques
        et des attributs data-testid.

        Args:
            response: Reponse Scrapling de la page de detail.
            scrape_type: 'location' (seul type supporte en v1).

        Returns:
            Dictionnaire avec les donnees detaillees de l'annonce.
        """
        data: dict[str, Any] = {}

        # Description
        desc_el = response.css_first(
            self.selectors.get("description", "")
        )
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

        # Criteres de detail SeLoger
        # SeLoger utilise des blocs de criteres structures
        criteria_items = response.css(
            "div[data-testid='sl.detail.criteria'] li"
        )
        for item in criteria_items:
            text = item.text.strip().lower()
            self._extract_criterion(data, text)

        # Informations financieres (loyer HC, charges, depot de garantie)
        finance_items = response.css(
            "div[data-testid='sl.detail.financial'] li"
        )
        for item in finance_items:
            label_el = item.css_first("span.label")
            value_el = item.css_first("span.value")
            if label_el and value_el:
                label = label_el.text.strip().lower()
                value = value_el.text.strip()
                self._extract_financial(data, label, value)

        # Date de publication
        date_el = response.css_first(
            "div[data-testid='sl.detail.publication-date']"
        )
        if date_el:
            data["date_publication"] = date_el.text.strip()

        return data

    def _extract_criterion(self, data: dict[str, Any], text: str) -> None:
        """Extrait un critere depuis le texte d'un element de detail.

        Args:
            data: Dictionnaire de donnees a enrichir.
            text: Texte du critere en minuscules.
        """
        if "m²" in text or "m2" in text:
            data.setdefault("surface_m2", self._parse_surface(text))
        elif "pièce" in text or "piece" in text:
            data.setdefault("nb_pieces", self._parse_rooms(text))
        elif "étage" in text or "etage" in text:
            data.setdefault("etage", self._parse_integer(text))
        elif "meublé" in text or "meuble" in text:
            data["meuble"] = True
        elif "non meublé" in text or "non meuble" in text:
            data["meuble"] = False

    def _extract_financial(
        self, data: dict[str, Any], label: str, value: str
    ) -> None:
        """Extrait une information financiere depuis un couple label/valeur.

        Args:
            data: Dictionnaire de donnees a enrichir.
            label: Label de l'information en minuscules.
            value: Valeur de l'information.
        """
        if "loyer" in label and "charge" not in label:
            data.setdefault("loyer_hc", self._parse_float(value))
        elif "charge" in label:
            data.setdefault("charges", self._parse_float(value))

    def _get_next_page_url(self, response: Any, current_page: int) -> str | None:
        """Determine l'URL de la page suivante SeLoger.

        SeLoger utilise un parametre 'LISTING-LISTpg' pour la pagination.

        Args:
            response: Reponse Scrapling de la page courante.
            current_page: Numero de la page courante.

        Returns:
            URL de la page suivante, ou None si derniere page.
        """
        next_page = current_page + 1
        if next_page > self._max_pages:
            return None

        # Chercher un lien "page suivante" explicite
        next_link = response.css_first(
            "a[data-testid='sl.explore.pagination-next']"
        )
        if next_link:
            href = next_link.attrib.get("href", "")
            if href:
                if not href.startswith("http"):
                    href = f"https://www.seloger.com{href}"
                return href

        # Fallback : construire l'URL avec le parametre de pagination
        current_url = str(response.url) if hasattr(response, "url") else ""
        if not current_url:
            return None

        return self._add_page_param(current_url, next_page)

    def _add_page_param(self, url: str, page: int) -> str:
        """Ajoute ou met a jour le parametre de pagination SeLoger.

        Args:
            url: URL a modifier.
            page: Numero de page a inserer.

        Returns:
            URL modifiee avec le parametre de pagination.
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
    def _parse_surface(text: str) -> float | None:
        """Extrait une surface en m2 depuis un texte.

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
        d'un deux-points ou en debut de chaine.

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
