"""Scraper PAP (Particulier a Particulier) pour les annonces immobilieres a Besancon.

Implemente le scraping des annonces de vente et de location
depuis PAP.fr, avec gestion des specificites du site :
structure HTML, pagination, parametres de recherche.
"""

from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import urljoin, urlparse

from src.scrapers.base import BaseScraper

logger = logging.getLogger(__name__)


class PAPScraper(BaseScraper):
    """Scraper pour les annonces PAP.fr.

    Supporte le scraping des annonces de vente (appartements T2/T3
    a Besancon, jusqu'a 160k) et de location (observatoire des loyers).

    PAP est un site de particuliers a particuliers, ce qui offre
    souvent de meilleures opportunites de negociation.

    Attributes:
        source_name: Toujours 'pap'.
    """

    def __init__(self, **kwargs: Any) -> None:
        """Initialise le scraper PAP.

        Args:
            **kwargs: Arguments transmis a BaseScraper (settings, fetcher).
        """
        super().__init__(source_name="pap", **kwargs)

    def _get_search_url(self, scrape_type: str) -> str:
        """Construit l'URL de recherche PAP.

        PAP utilise des URLs structurees avec les criteres directement
        dans le chemin : /annonce/vente-appartement-besancon-25000-...

        Args:
            scrape_type: 'vente' ou 'location'.

        Returns:
            URL de recherche PAP complete.
        """
        base_urls = self.source_config.get("base_urls", {})
        url = base_urls.get(scrape_type, "")
        if not url:
            logger.error(
                "Aucune URL configuree pour PAP type '%s'", scrape_type
            )
        return url

    def _parse_listing_page(
        self, response: Any, scrape_type: str
    ) -> list[dict[str, Any]]:
        """Parse une page de resultats PAP.

        Extrait les informations de base de chaque item de recherche :
        titre, prix/loyer, surface, nombre de pieces, localisation et lien.

        PAP utilise une structure avec des classes CSS specifiques :
        - .search-list-item pour chaque carte
        - .item-title pour le titre
        - .item-price pour le prix

        Args:
            response: Reponse Scrapling de la page de listing.
            scrape_type: 'vente' ou 'location'.

        Returns:
            Liste de dictionnaires avec les donnees de base par annonce.
        """
        listings: list[dict[str, Any]] = []
        container_selector = self.selectors.get("listing_container", "")

        if not container_selector:
            logger.warning("Selecteur listing_container manquant pour PAP")
            return listings

        try:
            cards = response.css(container_selector)
        except Exception:
            logger.error("Erreur CSS sur le listing PAP", exc_info=True)
            return listings

        logger.debug("PAP: %d items trouves sur la page", len(cards))

        for card in cards:
            try:
                listing = self._parse_card(card, scrape_type)
                if listing:
                    listings.append(listing)
            except Exception:
                logger.debug(
                    "Erreur parsing d'un item PAP", exc_info=True
                )

        return listings

    def _parse_card(
        self, card: Any, scrape_type: str
    ) -> dict[str, Any] | None:
        """Parse un item de recherche PAP depuis la page de listing.

        Args:
            card: Element HTML de l'item de recherche.
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
                href = f"https://www.pap.fr{href}"
            data["url_source"] = href
        else:
            return None

        # Prix / Loyer
        price_el = card.css_first(self.selectors.get("price", ""))
        if price_el:
            price_text = price_el.text.strip()
            if scrape_type == "location":
                data["loyer_cc"] = self._parse_float(price_text)
            else:
                data["prix"] = self._parse_price(price_text)

        # Surface (dans les tags de l'annonce)
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
        """Parse une page de detail d'annonce PAP.

        Extrait les informations detaillees : description complete,
        DPE, etage, charges, photos, date de publication.

        PAP affiche les details dans des blocs specifiques :
        - .item-description pour la description
        - .item-dpe pour le DPE
        - .item-tags pour les criteres (surface, pieces, etc.)

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

        # Criteres de detail PAP (tableau ou liste de features)
        feature_items = response.css("ul.item-features li")
        for item in feature_items:
            text = item.text.strip().lower()
            self._extract_feature(data, text, scrape_type)

        # Informations complementaires dans le bloc de specs
        spec_items = response.css("ul.item-specs li")
        for item in spec_items:
            label_el = item.css_first("span.label")
            value_el = item.css_first("span.value")
            if label_el and value_el:
                label = label_el.text.strip().lower()
                value = value_el.text.strip()
                self._extract_spec(data, label, value, scrape_type)

        # Date de publication
        date_el = response.css_first("p.item-date")
        if date_el:
            data["date_publication"] = date_el.text.strip()

        return data

    def _extract_feature(
        self, data: dict[str, Any], text: str, scrape_type: str
    ) -> None:
        """Extrait une feature depuis le texte d'un element de liste.

        Args:
            data: Dictionnaire de donnees a enrichir.
            text: Texte de la feature en minuscules.
            scrape_type: 'vente' ou 'location'.
        """
        if "m²" in text or "m2" in text:
            data.setdefault("surface_m2", self._parse_surface(text))
        elif "pièce" in text or "piece" in text:
            data.setdefault("nb_pieces", self._parse_rooms(text))
        elif "étage" in text or "etage" in text:
            data.setdefault("etage", self._parse_integer(text))
        elif "meublé" in text or "meuble" in text:
            if scrape_type == "location":
                data["meuble"] = True
        elif "cave" in text or "parking" in text or "garage" in text:
            equipements = data.get("equipements", [])
            equipements.append(text.strip())
            data["equipements"] = equipements

    def _extract_spec(
        self,
        data: dict[str, Any],
        label: str,
        value: str,
        scrape_type: str,
    ) -> None:
        """Extrait une specification depuis un couple label/valeur.

        Args:
            data: Dictionnaire de donnees a enrichir.
            label: Label de la specification en minuscules.
            value: Valeur de la specification.
            scrape_type: 'vente' ou 'location'.
        """
        if "surface" in label:
            data.setdefault("surface_m2", self._parse_surface(value))
        elif "pièce" in label or "piece" in label:
            data.setdefault("nb_pieces", self._parse_rooms(value))
        elif "étage" in label or "etage" in label:
            data.setdefault("etage", self._parse_integer(value))
        elif "charge" in label:
            data.setdefault("charges_copro", self._parse_float(value))
        elif "énergie" in label or "energie" in label or "dpe" in label:
            dpe = self._parse_dpe(value)
            if dpe:
                data.setdefault("dpe", dpe)

    def _get_next_page_url(self, response: Any, current_page: int) -> str | None:
        """Determine l'URL de la page suivante PAP.

        PAP utilise un suffixe '-pageN' dans l'URL ou un parametre 'page'.
        On verifie aussi la presence d'un lien 'Page suivante'.

        Args:
            response: Reponse Scrapling de la page courante.
            current_page: Numero de la page courante.

        Returns:
            URL de la page suivante, ou None si derniere page.
        """
        next_page = current_page + 1
        if next_page > self._max_pages:
            return None

        # Chercher un lien "page suivante" dans le HTML
        next_link = response.css_first("a.next, a[rel='next'], a.pagination-next")
        if next_link:
            href = next_link.attrib.get("href", "")
            if href:
                if not href.startswith("http"):
                    href = f"https://www.pap.fr{href}"
                return href

        # Fallback : construire l'URL avec le suffixe de page PAP
        current_url = str(response.url) if hasattr(response, "url") else ""
        if not current_url:
            return None

        # PAP utilise souvent le format /annonce/...-page-N
        # Retirer un eventuel suffixe de page existant
        clean_url = re.sub(r"-page-\d+", "", current_url)
        return f"{clean_url}-page-{next_page}"

    # ------------------------------------------------------------------
    # Methodes utilitaires de parsing (heritees de la logique commune)
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_price(text: str) -> int | None:
        """Extrait un prix entier depuis un texte.

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
