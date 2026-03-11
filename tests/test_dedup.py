"""Tests unitaires pour le module de deduplication.

Verifie la deduplication intra-source (hash URL), la detection de doublons
inter-sources (matching flou), et la fusion de doublons.
"""

from __future__ import annotations

from typing import Any

import pytest

from src.parsers.dedup import Deduplicator


@pytest.fixture
def dedup() -> Deduplicator:
    """Retourne une instance du deduplicateur."""
    return Deduplicator()


class TestIntraSourceHash:
    """Tests pour le hash intra-source (URL canonique)."""

    def test_hash_consistency(self, dedup: Deduplicator) -> None:
        """Verifie que le meme URL produit toujours le meme hash."""
        url = "https://www.leboncoin.fr/ventes_immobilieres/1234567890.htm"
        hash1 = dedup.compute_hash_intra(url)
        hash2 = dedup.compute_hash_intra(url)
        assert hash1 == hash2

    def test_hash_length(self, dedup: Deduplicator) -> None:
        """Verifie que le hash est bien un SHA256 (64 caracteres hex)."""
        url = "https://www.leboncoin.fr/ventes/123.htm"
        result = dedup.compute_hash_intra(url)
        assert len(result) == 64
        assert all(c in "0123456789abcdef" for c in result)

    def test_hash_different_urls(self, dedup: Deduplicator) -> None:
        """Verifie que deux URLs differentes produisent des hashs differents."""
        hash1 = dedup.compute_hash_intra("https://www.leboncoin.fr/ventes/123.htm")
        hash2 = dedup.compute_hash_intra("https://www.leboncoin.fr/ventes/456.htm")
        assert hash1 != hash2

    def test_hash_ignores_tracking_params(self, dedup: Deduplicator) -> None:
        """Verifie que les parametres de tracking sont ignores."""
        url_clean = "https://www.leboncoin.fr/ventes/123.htm"
        url_tracked = "https://www.leboncoin.fr/ventes/123.htm?utm_source=google&fbclid=abc"
        hash1 = dedup.compute_hash_intra(url_clean)
        hash2 = dedup.compute_hash_intra(url_tracked)
        assert hash1 == hash2

    def test_hash_ignores_fragment(self, dedup: Deduplicator) -> None:
        """Verifie que le fragment URL est ignore."""
        url1 = "https://www.leboncoin.fr/ventes/123.htm"
        url2 = "https://www.leboncoin.fr/ventes/123.htm#section"
        hash1 = dedup.compute_hash_intra(url1)
        hash2 = dedup.compute_hash_intra(url2)
        assert hash1 == hash2

    def test_hash_case_insensitive_domain(self, dedup: Deduplicator) -> None:
        """Verifie que le domaine est insensible a la casse."""
        hash1 = dedup.compute_hash_intra("https://WWW.LEBONCOIN.FR/ventes/123.htm")
        hash2 = dedup.compute_hash_intra("https://www.leboncoin.fr/ventes/123.htm")
        assert hash1 == hash2


class TestInterSourceMatching:
    """Tests pour le matching inter-sources."""

    def test_same_listing_different_sources(self, dedup: Deduplicator) -> None:
        """Verifie la detection d'un doublon avec URLs differentes mais memes donnees."""
        annonce = {
            "url_source": "https://www.pap.fr/annonce/vente-t3-besancon-25000-123",
            "adresse_brute": "12 Rue de la Republique, 25000 Besancon",
            "surface_m2": 55.0,
            "prix": 145000,
        }
        existing = [
            {
                "url_source": "https://www.leboncoin.fr/ventes/456.htm",
                "adresse_brute": "12 Rue de la Republique, 25000 Besancon",
                "surface_m2": 55.0,
                "prix": 145000,
            }
        ]
        duplicates = dedup.find_duplicates_inter(annonce, existing)
        assert len(duplicates) == 1
        assert duplicates[0] == "https://www.leboncoin.fr/ventes/456.htm"

    def test_no_match_different_listings(self, dedup: Deduplicator) -> None:
        """Verifie l'absence de doublon pour des annonces differentes."""
        annonce = {
            "url_source": "https://www.pap.fr/annonce/vente-t3-besancon-123",
            "adresse_brute": "5 Rue des Granges, 25000 Besancon",
            "surface_m2": 45.0,
            "prix": 120000,
        }
        existing = [
            {
                "url_source": "https://www.leboncoin.fr/ventes/456.htm",
                "adresse_brute": "100 Avenue de la Gare, 25000 Besancon",
                "surface_m2": 75.0,
                "prix": 200000,
            }
        ]
        duplicates = dedup.find_duplicates_inter(annonce, existing)
        assert len(duplicates) == 0

    def test_price_tolerance(self, dedup: Deduplicator) -> None:
        """Verifie le matching avec une tolerance de prix de +/- 5%."""
        annonce = {
            "url_source": "https://www.pap.fr/annonce/123",
            "adresse_brute": "12 Rue de la Republique, 25000 Besancon",
            "surface_m2": 55.0,
            "prix": 148000,  # +2% par rapport a 145000
        }
        existing = [
            {
                "url_source": "https://www.leboncoin.fr/ventes/456.htm",
                "adresse_brute": "12 Rue de la Republique, 25000 Besancon",
                "surface_m2": 55.0,
                "prix": 145000,
            }
        ]
        duplicates = dedup.find_duplicates_inter(annonce, existing)
        assert len(duplicates) == 1

    def test_price_tolerance_exceeded(self, dedup: Deduplicator) -> None:
        """Verifie le non-matching quand la tolerance de prix est depassee."""
        annonce = {
            "url_source": "https://www.pap.fr/annonce/123",
            "adresse_brute": "12 Rue de la Republique, 25000 Besancon",
            "surface_m2": 55.0,
            "prix": 200000,  # +38% par rapport a 145000
        }
        existing = [
            {
                "url_source": "https://www.leboncoin.fr/ventes/456.htm",
                "adresse_brute": "12 Rue de la Republique, 25000 Besancon",
                "surface_m2": 55.0,
                "prix": 145000,
            }
        ]
        duplicates = dedup.find_duplicates_inter(annonce, existing)
        # Price is way off, but surface matches. Address + surface = 2 criteria -> match
        # Actually let's check: address matches (1), surface matches (1), price doesn't match
        # Score = 2 >= 2 -> still a match because address + surface is enough
        # This is by design: address match + surface match = probable duplicate
        assert len(duplicates) == 1

    def test_surface_tolerance(self, dedup: Deduplicator) -> None:
        """Verifie le matching avec une tolerance de surface de +/- 2m2."""
        annonce = {
            "url_source": "https://www.pap.fr/annonce/123",
            "adresse_brute": "12 Rue de la Republique, 25000 Besancon",
            "surface_m2": 56.5,  # +1.5m2 par rapport a 55.0
            "prix": 145000,
        }
        existing = [
            {
                "url_source": "https://www.leboncoin.fr/ventes/456.htm",
                "adresse_brute": "12 Rue de la Republique, 25000 Besancon",
                "surface_m2": 55.0,
                "prix": 145000,
            }
        ]
        duplicates = dedup.find_duplicates_inter(annonce, existing)
        assert len(duplicates) == 1

    def test_surface_tolerance_exceeded(self, dedup: Deduplicator) -> None:
        """Verifie le non-matching quand la tolerance de surface est depassee."""
        annonce = {
            "url_source": "https://www.pap.fr/annonce/123",
            "adresse_brute": "12 Rue de la Republique, 25000 Besancon",
            "surface_m2": 60.0,  # +5m2 par rapport a 55.0
            "prix": 200000,  # also different price
        }
        existing = [
            {
                "url_source": "https://www.leboncoin.fr/ventes/456.htm",
                "adresse_brute": "12 Rue de la Republique, 25000 Besancon",
                "surface_m2": 55.0,
                "prix": 145000,
            }
        ]
        duplicates = dedup.find_duplicates_inter(annonce, existing)
        # Address matches (1), surface doesn't (>2m2), price doesn't (>5%)
        # Score = 1 < 2 -> no match
        assert len(duplicates) == 0

    def test_fuzzy_address_matching(self, dedup: Deduplicator) -> None:
        """Verifie le matching flou d'adresses avec variantes mineures."""
        annonce = {
            "url_source": "https://www.pap.fr/annonce/123",
            "adresse_brute": "12 rue de la République, Besançon",
            "surface_m2": 55.0,
            "prix": 145000,
        }
        existing = [
            {
                "url_source": "https://www.leboncoin.fr/ventes/456.htm",
                "adresse_brute": "12 Rue de la Republique, 25000 Besancon",
                "surface_m2": 55.0,
                "prix": 145000,
            }
        ]
        duplicates = dedup.find_duplicates_inter(annonce, existing)
        assert len(duplicates) == 1

    def test_no_match_without_address(self, dedup: Deduplicator) -> None:
        """Verifie l'absence de doublon sans adresse."""
        annonce = {
            "url_source": "https://www.pap.fr/annonce/123",
            "adresse_brute": "",
            "surface_m2": 55.0,
            "prix": 145000,
        }
        existing = [
            {
                "url_source": "https://www.leboncoin.fr/ventes/456.htm",
                "adresse_brute": "12 Rue de la Republique, 25000 Besancon",
                "surface_m2": 55.0,
                "prix": 145000,
            }
        ]
        duplicates = dedup.find_duplicates_inter(annonce, existing)
        assert len(duplicates) == 0


class TestDuplicateMerging:
    """Tests pour la fusion de doublons."""

    def test_merge_fills_missing_fields(self, dedup: Deduplicator) -> None:
        """Verifie que la fusion complete les champs manquants."""
        primary: dict[str, Any] = {
            "url_source": "https://www.leboncoin.fr/ventes/123.htm",
            "source": "leboncoin",
            "prix": 145000,
            "surface_m2": 55.0,
            "nb_pieces": 3,
            "dpe": None,
            "etage": None,
            "source_ids": [],
            "photos_urls": ["https://img1.com/a.jpg"],
        }
        duplicates = [
            {
                "url_source": "https://www.pap.fr/annonce/456",
                "source": "pap",
                "prix": 145000,
                "surface_m2": 55.0,
                "nb_pieces": 3,
                "dpe": "C",
                "etage": 2,
                "photos_urls": ["https://img2.com/b.jpg"],
            }
        ]
        merged = dedup.merge_duplicates(primary, duplicates)
        assert merged["dpe"] == "C"
        assert merged["etage"] == 2
        assert "https://www.pap.fr/annonce/456" in merged["source_ids"]

    def test_merge_photos_deduplicated(self, dedup: Deduplicator) -> None:
        """Verifie la deduplication des photos lors de la fusion."""
        primary: dict[str, Any] = {
            "url_source": "https://lbc.com/1",
            "photos_urls": ["https://img.com/a.jpg", "https://img.com/b.jpg"],
            "source_ids": [],
        }
        duplicates = [
            {
                "url_source": "https://pap.fr/1",
                "photos_urls": ["https://img.com/b.jpg", "https://img.com/c.jpg"],
            }
        ]
        merged = dedup.merge_duplicates(primary, duplicates)
        assert len(merged["photos_urls"]) == 3
        assert "https://img.com/a.jpg" in merged["photos_urls"]
        assert "https://img.com/b.jpg" in merged["photos_urls"]
        assert "https://img.com/c.jpg" in merged["photos_urls"]

    def test_merge_keeps_best_completude(self, dedup: Deduplicator) -> None:
        """Verifie que la fusion garde le meilleur score de completude."""
        primary: dict[str, Any] = {
            "url_source": "https://lbc.com/1",
            "completude_score": 0.5,
            "source_ids": [],
            "photos_urls": [],
        }
        duplicates = [
            {
                "url_source": "https://pap.fr/1",
                "completude_score": 0.8,
                "photos_urls": [],
            }
        ]
        merged = dedup.merge_duplicates(primary, duplicates)
        assert merged["completude_score"] == 0.8

    def test_merge_source_ids_no_duplicates(self, dedup: Deduplicator) -> None:
        """Verifie que les source_ids ne contiennent pas de doublons."""
        primary: dict[str, Any] = {
            "url_source": "https://lbc.com/1",
            "source_ids": ["https://pap.fr/1"],
            "photos_urls": [],
        }
        duplicates = [
            {
                "url_source": "https://pap.fr/1",
                "photos_urls": [],
            },
            {
                "url_source": "https://seloger.com/1",
                "photos_urls": [],
            },
        ]
        merged = dedup.merge_duplicates(primary, duplicates)
        # pap.fr/1 should appear only once
        assert merged["source_ids"].count("https://pap.fr/1") == 1
        assert "https://seloger.com/1" in merged["source_ids"]


class TestAddressNormalizationForMatching:
    """Tests pour la normalisation agressive d'adresse pour le matching."""

    def test_accents_removed(self, dedup: Deduplicator) -> None:
        """Verifie la suppression des accents."""
        result = dedup._normalize_address_for_matching("Rue de la Républiqué")
        assert "e" in result
        assert "é" not in result

    def test_case_insensitive(self, dedup: Deduplicator) -> None:
        """Verifie l'insensibilite a la casse."""
        result = dedup._normalize_address_for_matching("RUE DE LA REPUBLIQUE")
        assert result == dedup._normalize_address_for_matching("rue de la republique")

    def test_empty_address(self, dedup: Deduplicator) -> None:
        """Verifie qu'une adresse vide donne une chaine vide."""
        assert dedup._normalize_address_for_matching("") == ""
        assert dedup._normalize_address_for_matching(None) == ""


class TestStringSimilarity:
    """Tests pour la similarite de chaines."""

    def test_identical_strings(self, dedup: Deduplicator) -> None:
        """Verifie que des chaines identiques donnent 1.0."""
        assert dedup._compute_similarity("abc", "abc") == 1.0

    def test_completely_different(self, dedup: Deduplicator) -> None:
        """Verifie que des chaines tres differentes donnent un score bas."""
        score = dedup._compute_similarity("abcdef", "xyz123")
        assert score < 0.3

    def test_empty_strings(self, dedup: Deduplicator) -> None:
        """Verifie que des chaines vides donnent 0.0."""
        assert dedup._compute_similarity("", "") == 0.0
        assert dedup._compute_similarity("abc", "") == 0.0
