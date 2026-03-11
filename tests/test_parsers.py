"""Tests unitaires pour le module de normalisation des donnees.

Verifie la normalisation des prix, surfaces, DPE, etages, adresses,
dates, descriptions et le calcul du score de completude.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from src.parsers.normalizer import AnnonceNormalizer


@pytest.fixture
def normalizer() -> AnnonceNormalizer:
    """Retourne une instance du normaliseur."""
    return AnnonceNormalizer()


class TestPriceNormalization:
    """Tests pour la normalisation des prix."""

    def test_price_with_euro_and_spaces(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie le nettoyage d'un prix avec symbole euro et espaces."""
        raw = {"prix": "145 000 €", "surface_m2": 55.0, "nb_pieces": 3,
               "url_source": "https://example.com/1", "source": "leboncoin"}
        result = normalizer.normalize_vente(raw)
        assert result["prix"] == 145000

    def test_price_with_euro_no_space(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie le nettoyage d'un prix colle au symbole euro."""
        raw = {"prix": "145000€", "surface_m2": 55.0, "nb_pieces": 3,
               "url_source": "https://example.com/2", "source": "pap"}
        result = normalizer.normalize_vente(raw)
        assert result["prix"] == 145000

    def test_price_with_comma_separator(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie le nettoyage d'un prix avec separateur virgule."""
        raw = {"prix": "145,000", "surface_m2": 55.0, "nb_pieces": 3,
               "url_source": "https://example.com/3", "source": "leboncoin"}
        result = normalizer.normalize_vente(raw)
        assert result["prix"] == 145000

    def test_price_integer(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie qu'un prix deja entier est conserve."""
        raw = {"prix": 145000, "surface_m2": 55.0, "nb_pieces": 3,
               "url_source": "https://example.com/4", "source": "leboncoin"}
        result = normalizer.normalize_vente(raw)
        assert result["prix"] == 145000

    def test_price_float(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie qu'un prix float est converti en entier."""
        raw = {"prix": 145000.99, "surface_m2": 55.0, "nb_pieces": 3,
               "url_source": "https://example.com/5", "source": "leboncoin"}
        result = normalizer.normalize_vente(raw)
        assert result["prix"] == 145000

    def test_price_none(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie qu'un prix None reste None."""
        raw = {"prix": None, "surface_m2": 55.0, "nb_pieces": 3,
               "url_source": "https://example.com/6", "source": "leboncoin"}
        result = normalizer.normalize_vente(raw)
        assert result["prix"] is None


class TestSurfaceNormalization:
    """Tests pour la normalisation des surfaces."""

    def test_surface_with_m2_unicode(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie le nettoyage d'une surface avec m2 unicode."""
        raw = {"prix": 145000, "surface_m2": "42 m²", "nb_pieces": 2,
               "url_source": "https://example.com/s1", "source": "leboncoin"}
        result = normalizer.normalize_vente(raw)
        assert result["surface_m2"] == 42.0

    def test_surface_decimal_with_m2(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie le nettoyage d'une surface decimale avec m2."""
        raw = {"prix": 145000, "surface_m2": "42.5m²", "nb_pieces": 2,
               "url_source": "https://example.com/s2", "source": "leboncoin"}
        result = normalizer.normalize_vente(raw)
        assert result["surface_m2"] == 42.5

    def test_surface_french_decimal(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie le nettoyage d'une surface avec virgule decimale."""
        raw = {"prix": 145000, "surface_m2": "42,5 m2", "nb_pieces": 2,
               "url_source": "https://example.com/s3", "source": "leboncoin"}
        result = normalizer.normalize_vente(raw)
        assert result["surface_m2"] == 42.5

    def test_surface_float_passthrough(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie qu'une surface deja float est conservee."""
        raw = {"prix": 145000, "surface_m2": 42.5, "nb_pieces": 2,
               "url_source": "https://example.com/s4", "source": "leboncoin"}
        result = normalizer.normalize_vente(raw)
        assert result["surface_m2"] == 42.5

    def test_surface_none(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie qu'une surface None reste None."""
        raw = {"prix": 145000, "surface_m2": None, "nb_pieces": 2,
               "url_source": "https://example.com/s5", "source": "leboncoin"}
        result = normalizer.normalize_vente(raw)
        assert result["surface_m2"] is None


class TestDPENormalization:
    """Tests pour la normalisation du DPE."""

    def test_dpe_lowercase(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie que le DPE minuscule est converti en majuscule."""
        raw = {"prix": 145000, "surface_m2": 55.0, "nb_pieces": 3,
               "url_source": "https://example.com/d1", "source": "leboncoin",
               "dpe": "c"}
        result = normalizer.normalize_vente(raw)
        assert result["dpe"] == "C"

    def test_dpe_uppercase(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie que le DPE majuscule est conserve."""
        raw = {"prix": 145000, "surface_m2": 55.0, "nb_pieces": 3,
               "url_source": "https://example.com/d2", "source": "leboncoin",
               "dpe": "A"}
        result = normalizer.normalize_vente(raw)
        assert result["dpe"] == "A"

    def test_dpe_empty_string(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie que le DPE chaine vide devient None."""
        raw = {"prix": 145000, "surface_m2": 55.0, "nb_pieces": 3,
               "url_source": "https://example.com/d3", "source": "leboncoin",
               "dpe": ""}
        result = normalizer.normalize_vente(raw)
        assert result["dpe"] is None

    def test_dpe_invalid_letter(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie que le DPE avec lettre invalide devient None."""
        raw = {"prix": 145000, "surface_m2": 55.0, "nb_pieces": 3,
               "url_source": "https://example.com/d4", "source": "leboncoin",
               "dpe": "X"}
        result = normalizer.normalize_vente(raw)
        assert result["dpe"] is None

    def test_dpe_none(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie que le DPE None reste None."""
        raw = {"prix": 145000, "surface_m2": 55.0, "nb_pieces": 3,
               "url_source": "https://example.com/d5", "source": "leboncoin",
               "dpe": None}
        result = normalizer.normalize_vente(raw)
        assert result["dpe"] is None

    def test_dpe_all_valid_letters(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie que toutes les lettres A-G sont acceptees."""
        for letter in "ABCDEFG":
            result = AnnonceNormalizer()._clean_dpe(letter)
            assert result == letter


class TestFloorNormalization:
    """Tests pour la normalisation des etages."""

    def test_etage_with_suffix(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie l'extraction du numero d'etage avec suffixe."""
        raw = {"prix": 145000, "surface_m2": 55.0, "nb_pieces": 3,
               "url_source": "https://example.com/e1", "source": "leboncoin",
               "etage": "3ème étage"}
        result = normalizer.normalize_vente(raw)
        assert result["etage"] == 3

    def test_etage_rdc(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie que RDC est converti en 0."""
        raw = {"prix": 145000, "surface_m2": 55.0, "nb_pieces": 3,
               "url_source": "https://example.com/e2", "source": "leboncoin",
               "etage": "RDC"}
        result = normalizer.normalize_vente(raw)
        assert result["etage"] == 0

    def test_etage_premier(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie l'extraction du 1er etage."""
        raw = {"prix": 145000, "surface_m2": 55.0, "nb_pieces": 3,
               "url_source": "https://example.com/e3", "source": "leboncoin",
               "etage": "1er"}
        result = normalizer.normalize_vente(raw)
        assert result["etage"] == 1

    def test_etage_integer(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie qu'un etage deja entier est conserve."""
        raw = {"prix": 145000, "surface_m2": 55.0, "nb_pieces": 3,
               "url_source": "https://example.com/e4", "source": "leboncoin",
               "etage": 5}
        result = normalizer.normalize_vente(raw)
        assert result["etage"] == 5

    def test_etage_rez_de_chaussee(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie que rez-de-chaussee est converti en 0."""
        raw = {"prix": 145000, "surface_m2": 55.0, "nb_pieces": 3,
               "url_source": "https://example.com/e5", "source": "leboncoin",
               "etage": "rez-de-chaussée"}
        result = normalizer.normalize_vente(raw)
        assert result["etage"] == 0

    def test_etage_none(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie qu'un etage None reste None."""
        raw = {"prix": 145000, "surface_m2": 55.0, "nb_pieces": 3,
               "url_source": "https://example.com/e6", "source": "leboncoin",
               "etage": None}
        result = normalizer.normalize_vente(raw)
        assert result["etage"] is None


class TestAddressNormalization:
    """Tests pour la normalisation des adresses."""

    def test_address_capitalization(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie la capitalisation des mots dans l'adresse."""
        result = normalizer._normalize_adresse("12 rue de la republique, besancon")
        assert result is not None
        assert "Republique" in result

    def test_address_abbreviation_expansion(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie l'expansion des abreviations de voie."""
        result = normalizer._normalize_adresse("12 av. de la gare")
        assert result is not None
        # L'abreviation "av." devrait etre expandee en "Avenue"
        assert "Avenue" in result or "avenue" in result.lower()

    def test_address_none(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie qu'une adresse None reste None."""
        result = normalizer._normalize_adresse(None)
        assert result is None

    def test_address_empty(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie qu'une adresse vide reste None."""
        result = normalizer._normalize_adresse("")
        assert result is None

    def test_address_whitespace_normalization(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie la normalisation des espaces dans l'adresse."""
        result = normalizer._normalize_adresse("12   rue  de   la  paix")
        assert result is not None
        assert "  " not in result


class TestDateParsing:
    """Tests pour le parsing des dates."""

    def test_date_french_format(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie le parsing d'une date au format francais."""
        result = normalizer._parse_date("10 mars 2026")
        assert result is not None
        assert result.year == 2026
        assert result.month == 3
        assert result.day == 10

    def test_date_slash_format(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie le parsing d'une date au format DD/MM/YYYY."""
        result = normalizer._parse_date("10/03/2026")
        assert result is not None
        assert result.year == 2026
        assert result.month == 3
        assert result.day == 10

    def test_date_iso_format(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie le parsing d'une date au format ISO."""
        result = normalizer._parse_date("2026-03-10")
        assert result is not None
        assert result.year == 2026
        assert result.month == 3
        assert result.day == 10

    def test_date_iso_with_time(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie le parsing d'une date ISO avec heure."""
        result = normalizer._parse_date("2026-03-10T14:30:00")
        assert result is not None
        assert result.year == 2026
        assert result.hour == 14
        assert result.minute == 30

    def test_date_aujourdhui(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie le parsing de 'Aujourd'hui'."""
        result = normalizer._parse_date("Aujourd'hui")
        assert result is not None
        today = datetime.now().date()
        assert result.date() == today

    def test_date_hier(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie le parsing de 'Hier'."""
        result = normalizer._parse_date("hier")
        assert result is not None
        yesterday = (datetime.now() - timedelta(days=1)).date()
        assert result.date() == yesterday

    def test_date_none(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie qu'une date None reste None."""
        result = normalizer._parse_date(None)
        assert result is None

    def test_date_datetime_passthrough(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie qu'un objet datetime est conserve tel quel."""
        dt = datetime(2026, 3, 10, 14, 0, 0)
        result = normalizer._parse_date(dt)
        assert result == dt

    def test_date_dash_format(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie le parsing d'une date DD-MM-YYYY."""
        result = normalizer._parse_date("10-03-2026")
        assert result is not None
        assert result.year == 2026
        assert result.month == 3
        assert result.day == 10


class TestCompletudeScore:
    """Tests pour le calcul du score de completude."""

    def test_completude_all_fields(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie que tous les champs remplis donnent un score de 1.0."""
        raw = {
            "prix": 145000,
            "surface_m2": 55.0,
            "nb_pieces": 3,
            "url_source": "https://example.com/comp1",
            "source": "leboncoin",
            "dpe": "C",
            "etage": 2,
            "adresse_brute": "12 Rue de la Republique, 25000 Besancon",
            "quartier": "Centre-Ville",
            "charges_copro": 120.0,
            "description_texte": "Bel appartement lumineux.",
            "photos_urls": ["https://example.com/photo1.jpg"],
            "date_publication": "2026-03-10",
        }
        result = normalizer.normalize_vente(raw)
        assert result["completude_score"] == 1.0

    def test_completude_no_optional_fields(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie que aucun champ optionnel donne un score de 0.0."""
        raw = {
            "prix": 145000,
            "surface_m2": 55.0,
            "nb_pieces": 3,
            "url_source": "https://example.com/comp2",
            "source": "leboncoin",
        }
        result = normalizer.normalize_vente(raw)
        assert result["completude_score"] == 0.0

    def test_completude_partial(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie que quelques champs optionnels donnent un score partiel."""
        raw = {
            "prix": 145000,
            "surface_m2": 55.0,
            "nb_pieces": 3,
            "url_source": "https://example.com/comp3",
            "source": "leboncoin",
            "dpe": "C",
            "etage": 2,
            "adresse_brute": "12 Rue de la Republique, Besancon",
            "quartier": "Centre-Ville",
        }
        result = normalizer.normalize_vente(raw)
        # 4 sur 8 champs optionnels remplis = 0.5
        assert result["completude_score"] == 0.5


class TestMeubleDetection:
    """Tests pour la detection du meuble."""

    def test_meuble_explicit_true(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie la detection meuble avec champ explicite True."""
        raw = {
            "loyer_cc": 550.0,
            "surface_m2": 45.0,
            "nb_pieces": 2,
            "url_source": "https://example.com/m1",
            "source": "leboncoin",
            "meuble": True,
        }
        result = normalizer.normalize_location(raw)
        assert result["meuble"] is True

    def test_meuble_from_description(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie la detection meuble dans la description."""
        raw = {
            "loyer_cc": 550.0,
            "surface_m2": 45.0,
            "nb_pieces": 2,
            "url_source": "https://example.com/m2",
            "source": "leboncoin",
            "description_texte": "Appartement entièrement meublé, tout équipé.",
        }
        result = normalizer.normalize_location(raw)
        assert result["meuble"] is True

    def test_meuble_none_no_signal(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie que sans signal le meuble est None."""
        raw = {
            "loyer_cc": 550.0,
            "surface_m2": 45.0,
            "nb_pieces": 2,
            "url_source": "https://example.com/m3",
            "source": "leboncoin",
            "description_texte": "Bel appartement lumineux, 2 pieces.",
        }
        result = normalizer.normalize_location(raw)
        assert result["meuble"] is None

    def test_meuble_explicit_string_oui(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie la detection meuble avec champ string 'oui'."""
        raw = {
            "loyer_cc": 550.0,
            "surface_m2": 45.0,
            "nb_pieces": 2,
            "url_source": "https://example.com/m4",
            "source": "leboncoin",
            "meuble": "oui",
        }
        result = normalizer.normalize_location(raw)
        assert result["meuble"] is True


class TestHTMLCleaning:
    """Tests pour le nettoyage HTML dans les descriptions."""

    def test_strip_html_tags(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie la suppression des balises HTML."""
        result = normalizer._clean_text("<p>Bel <b>appartement</b> T3.</p>")
        assert result is not None
        assert "<p>" not in result
        assert "<b>" not in result
        assert "Bel" in result
        assert "appartement" in result

    def test_br_to_newline(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie la conversion des <br> en sauts de ligne."""
        result = normalizer._clean_text("Ligne 1<br/>Ligne 2")
        assert result is not None
        assert "\n" in result

    def test_html_entities(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie le decodage des entites HTML."""
        result = normalizer._clean_text("Prix &amp; qualit&eacute;")
        assert result is not None
        assert "&amp;" not in result
        assert "&" in result

    def test_normalize_whitespace(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie la normalisation des espaces multiples."""
        result = normalizer._clean_text("Texte   avec    trop   d'espaces")
        assert result is not None
        assert "   " not in result

    def test_clean_text_none(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie que None retourne None."""
        assert normalizer._clean_text(None) is None

    def test_clean_text_empty(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie qu'une chaine vide retourne None."""
        assert normalizer._clean_text("") is None


class TestNbPiecesNormalization:
    """Tests pour la normalisation du nombre de pieces."""

    def test_nb_pieces_from_t_format(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie l'extraction du nombre de pieces format T3."""
        assert normalizer._clean_nb_pieces("T3") == 3

    def test_nb_pieces_from_string_pieces(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie l'extraction depuis '3 pieces'."""
        assert normalizer._clean_nb_pieces("3 pièces") == 3

    def test_nb_pieces_integer(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie qu'un entier est conserve."""
        assert normalizer._clean_nb_pieces(3) == 3


class TestLocationNormalization:
    """Tests pour la normalisation des locations."""

    def test_loyer_normalization(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie la normalisation du loyer."""
        raw = {
            "loyer_cc": "550 €",
            "loyer_hc": "480€",
            "surface_m2": 45.0,
            "nb_pieces": 2,
            "url_source": "https://example.com/loc1",
            "source": "leboncoin",
        }
        result = normalizer.normalize_location(raw)
        assert result["loyer_cc"] == 550.0
        assert result["loyer_hc"] == 480.0

    def test_location_has_completude(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie que la normalisation location calcule la completude."""
        raw = {
            "loyer_cc": 550.0,
            "surface_m2": 45.0,
            "nb_pieces": 2,
            "url_source": "https://example.com/loc2",
            "source": "leboncoin",
        }
        result = normalizer.normalize_location(raw)
        assert "completude_score" in result
        assert isinstance(result["completude_score"], float)


class TestPhotoNormalization:
    """Tests pour la normalisation des URLs de photos."""

    def test_absolute_urls_preserved(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie que les URLs absolues sont conservees."""
        photos = ["https://img.example.com/photo1.jpg", "https://img.example.com/photo2.jpg"]
        result = normalizer._normalize_photos(photos, "https://example.com/annonce/1")
        assert len(result) == 2
        assert result[0] == "https://img.example.com/photo1.jpg"

    def test_relative_urls_resolved(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie que les URLs relatives sont resolues."""
        photos = ["/images/photo1.jpg"]
        result = normalizer._normalize_photos(photos, "https://example.com/annonce/1")
        assert len(result) == 1
        assert result[0].startswith("https://example.com")

    def test_empty_photos(self, normalizer: AnnonceNormalizer) -> None:
        """Verifie que None retourne une liste vide."""
        result = normalizer._normalize_photos(None, "")
        assert result == []
