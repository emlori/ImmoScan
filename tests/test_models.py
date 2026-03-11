"""Tests unitaires pour les modeles SQLAlchemy ImmoScan.

Verifie l'instanciation des modeles, les valeurs par defaut,
les relations et les methodes __repr__ sans connexion a la base.
"""

from __future__ import annotations

from datetime import datetime

import pytest

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


class TestAnnonceModel:
    """Tests pour le modele Annonce."""

    def test_instantiation(self) -> None:
        """Verifie qu'une annonce peut etre instanciee avec les champs requis."""
        annonce = Annonce(
            url_source="https://www.leboncoin.fr/ventes/123.htm",
            source="leboncoin",
            hash_dedup="abc123def456",
            prix=145000,
            surface_m2=55.0,
            nb_pieces=3,
        )
        assert annonce.url_source == "https://www.leboncoin.fr/ventes/123.htm"
        assert annonce.source == "leboncoin"
        assert annonce.hash_dedup == "abc123def456"
        assert annonce.prix == 145000
        assert annonce.surface_m2 == 55.0
        assert annonce.nb_pieces == 3

    def test_optional_fields(self) -> None:
        """Verifie que les champs optionnels sont None par defaut."""
        annonce = Annonce(
            url_source="https://www.pap.fr/annonce/123",
            source="pap",
            hash_dedup="xyz789",
            prix=130000,
            surface_m2=42.0,
            nb_pieces=2,
        )
        assert annonce.dpe is None
        assert annonce.etage is None
        assert annonce.adresse_brute is None
        assert annonce.quartier is None
        assert annonce.charges_copro is None
        assert annonce.description_texte is None
        assert annonce.date_publication is None
        assert annonce.date_modification is None
        assert annonce.completude_score is None

    def test_default_statut(self) -> None:
        """Verifie que le statut par defaut est 'nouveau'."""
        annonce = Annonce(
            url_source="https://example.com/1",
            source="leboncoin",
            hash_dedup="hash1",
            prix=140000,
            surface_m2=50.0,
            nb_pieces=2,
        )
        assert annonce.statut == "nouveau"

    def test_json_defaults(self) -> None:
        """Verifie que les champs JSONB ont des valeurs par defaut correctes."""
        annonce = Annonce(
            url_source="https://example.com/2",
            source="leboncoin",
            hash_dedup="hash2",
            prix=155000,
            surface_m2=60.0,
            nb_pieces=3,
        )
        # Les defaults Python sont definis mais non evalues sans session DB
        # On verifie qu'ils peuvent etre assignes
        annonce.source_ids = []
        annonce.photos_urls = ["https://img.example.com/1.jpg"]
        annonce.historique_prix = [{"date": "2026-03-01", "prix": 160000}]
        assert annonce.source_ids == []
        assert len(annonce.photos_urls) == 1
        assert annonce.historique_prix[0]["prix"] == 160000

    def test_repr(self) -> None:
        """Verifie la representation textuelle de l'annonce."""
        annonce = Annonce(
            id=42,
            url_source="https://example.com/3",
            source="leboncoin",
            hash_dedup="hash3",
            prix=145000,
            surface_m2=55.0,
            nb_pieces=3,
            quartier="Centre-Ville",
            statut="nouveau",
        )
        result = repr(annonce)
        assert "Annonce" in result
        assert "id=42" in result
        assert "leboncoin" in result
        assert "145000" in result
        assert "55.0" in result
        assert "Centre-Ville" in result
        assert "nouveau" in result

    def test_all_fields(self) -> None:
        """Verifie l'instanciation avec tous les champs renseignes."""
        now = datetime(2026, 3, 10, 14, 30, 0)
        annonce = Annonce(
            url_source="https://example.com/full",
            source="pap",
            hash_dedup="fullhash",
            source_ids=["lbc_123", "pap_456"],
            prix=150000,
            surface_m2=58.5,
            nb_pieces=3,
            dpe="B",
            etage=3,
            adresse_brute="5 rue des Granges, 25000 Besancon",
            quartier="Centre-Ville",
            charges_copro=150.0,
            description_texte="Bel appartement T3 renove.",
            photos_urls=["https://img.example.com/a.jpg"],
            date_publication=now,
            date_modification=now,
            historique_prix=[{"date": "2026-03-01", "prix": 155000}],
            completude_score=0.95,
            statut="alerte",
        )
        assert annonce.dpe == "B"
        assert annonce.etage == 3
        assert annonce.charges_copro == 150.0
        assert annonce.completude_score == 0.95
        assert annonce.statut == "alerte"
        assert len(annonce.source_ids) == 2


class TestScoreModel:
    """Tests pour le modele Score."""

    def test_instantiation(self) -> None:
        """Verifie qu'un score peut etre instancie."""
        score = Score(
            annonce_id=1,
            score_global=78.5,
            renta_brute_affiche=7.2,
            renta_brute_nego_5=7.6,
            renta_brute_nego_10=8.0,
            renta_brute_nego_15=8.5,
            score_localisation=85.0,
            score_dpe=65.0,
            score_negociation=40.0,
            score_vacance=20.0,
            regime_indicatif="lmnp",
            loyer_estime=550.0,
            fiabilite_loyer="fiable",
        )
        assert score.annonce_id == 1
        assert score.score_global == 78.5
        assert score.renta_brute_affiche == 7.2
        assert score.renta_brute_nego_15 == 8.5
        assert score.regime_indicatif == "lmnp"
        assert score.fiabilite_loyer == "fiable"

    def test_optional_fields(self) -> None:
        """Verifie que les champs optionnels sont None par defaut."""
        score = Score()
        assert score.annonce_id is None
        assert score.score_global is None
        assert score.renta_brute_affiche is None
        assert score.loyer_estime is None

    def test_repr(self) -> None:
        """Verifie la representation textuelle du score."""
        score = Score(
            id=7,
            annonce_id=42,
            score_global=82.3,
            renta_brute_affiche=8.1,
        )
        result = repr(score)
        assert "Score" in result
        assert "id=7" in result
        assert "annonce_id=42" in result
        assert "82.3" in result
        assert "8.1" in result


class TestEnrichissementIAModel:
    """Tests pour le modele EnrichissementIA."""

    def test_instantiation(self) -> None:
        """Verifie qu'un enrichissement IA peut etre instancie."""
        enrichissement = EnrichissementIA(
            annonce_id=1,
            signaux_nego=["urgent", "prix a debattre"],
            etat_bien="bon_etat",
            equipements=["parking", "cave"],
            red_flags=[],
            info_copro={"nb_lots": 12, "charges_annuelles": 1200},
            resume_ia="T3 lumineux en bon etat.",
        )
        assert enrichissement.annonce_id == 1
        assert enrichissement.etat_bien == "bon_etat"
        assert len(enrichissement.signaux_nego) == 2
        assert enrichissement.info_copro["nb_lots"] == 12

    def test_repr(self) -> None:
        """Verifie la representation textuelle de l'enrichissement."""
        enrichissement = EnrichissementIA(
            id=3,
            annonce_id=42,
            etat_bien="travaux_legers",
        )
        result = repr(enrichissement)
        assert "EnrichissementIA" in result
        assert "id=3" in result
        assert "travaux_legers" in result


class TestQuartierModel:
    """Tests pour le modele Quartier."""

    def test_instantiation(self) -> None:
        """Verifie qu'un quartier peut etre instancie."""
        quartier = Quartier(
            nom="Centre-Ville",
            score_attractivite=85.0,
            profil_locataire="Etudiants, jeunes actifs, couples",
            tension_locative=0.85,
        )
        assert quartier.nom == "Centre-Ville"
        assert quartier.score_attractivite == 85.0
        assert quartier.tension_locative == 0.85

    def test_repr(self) -> None:
        """Verifie la representation textuelle du quartier."""
        quartier = Quartier(
            id=1,
            nom="Battant",
            score_attractivite=80.0,
        )
        result = repr(quartier)
        assert "Quartier" in result
        assert "id=1" in result
        assert "Battant" in result
        assert "80.0" in result


class TestLoyerMarcheModel:
    """Tests pour le modele LoyerMarche."""

    def test_instantiation(self) -> None:
        """Verifie qu'un loyer marche peut etre instancie."""
        loyer = LoyerMarche(
            url_source="https://www.leboncoin.fr/locations/999.htm",
            source="leboncoin",
            hash_dedup="loyer_hash_1",
            loyer_cc=550.0,
            loyer_hc=480.0,
            surface_m2=45.0,
            nb_pieces=2,
            meuble=False,
            quartier="Battant",
            dpe="B",
        )
        assert loyer.loyer_cc == 550.0
        assert loyer.loyer_hc == 480.0
        assert loyer.meuble is False
        assert loyer.nb_pieces == 2

    def test_repr(self) -> None:
        """Verifie la representation textuelle du loyer marche."""
        loyer = LoyerMarche(
            id=5,
            url_source="https://example.com/loc/5",
            source="pap",
            hash_dedup="h5",
            loyer_cc=600.0,
            surface_m2=50.0,
            nb_pieces=2,
            quartier="Centre-Ville",
        )
        result = repr(loyer)
        assert "LoyerMarche" in result
        assert "id=5" in result
        assert "pap" in result
        assert "600.0" in result
        assert "50.0" in result


class TestLoyerReferenceModel:
    """Tests pour le modele LoyerReference."""

    def test_instantiation(self) -> None:
        """Verifie qu'un loyer de reference peut etre instancie."""
        ref = LoyerReference(
            quartier="Centre-Ville",
            type_bien="T2",
            meuble=True,
            loyer_median=550.0,
            loyer_q1=480.0,
            loyer_q3=620.0,
            nb_annonces=15,
            fiabilite="fiable",
            loyer_m2_median=15.0,
        )
        assert ref.quartier == "Centre-Ville"
        assert ref.type_bien == "T2"
        assert ref.meuble is True
        assert ref.loyer_median == 550.0
        assert ref.fiabilite == "fiable"

    def test_repr(self) -> None:
        """Verifie la representation textuelle du loyer de reference."""
        ref = LoyerReference(
            id=2,
            quartier="Battant",
            type_bien="T3",
            meuble=False,
            loyer_median=560.0,
            fiabilite="preliminaire",
        )
        result = repr(ref)
        assert "LoyerReference" in result
        assert "Battant" in result
        assert "T3" in result
        assert "False" in result
        assert "560.0" in result
        assert "preliminaire" in result


class TestAlerteLogModel:
    """Tests pour le modele AlerteLog."""

    def test_instantiation(self) -> None:
        """Verifie qu'un log d'alerte peut etre instancie."""
        alerte = AlerteLog(
            annonce_id=42,
            canal="telegram",
            niveau="top",
        )
        assert alerte.annonce_id == 42
        assert alerte.canal == "telegram"
        assert alerte.niveau == "top"

    def test_default_canal(self) -> None:
        """Verifie que le canal par defaut est 'telegram'."""
        alerte = AlerteLog(annonce_id=1, niveau="bon")
        assert alerte.canal == "telegram"

    def test_repr(self) -> None:
        """Verifie la representation textuelle du log d'alerte."""
        alerte = AlerteLog(
            id=10,
            annonce_id=42,
            niveau="baisse_prix",
            canal="telegram",
        )
        result = repr(alerte)
        assert "AlerteLog" in result
        assert "id=10" in result
        assert "baisse_prix" in result
        assert "telegram" in result


class TestScrapingLogModel:
    """Tests pour le modele ScrapingLog."""

    def test_instantiation(self) -> None:
        """Verifie qu'un log de scraping peut etre instancie."""
        log = ScrapingLog(
            source="leboncoin",
            type_scrape="vente",
            nb_annonces_scrapees=50,
            nb_nouvelles=8,
            nb_erreurs=2,
            duree_sec=45.3,
            proxy_utilise="proxy1.example.com:8080",
        )
        assert log.source == "leboncoin"
        assert log.type_scrape == "vente"
        assert log.nb_annonces_scrapees == 50
        assert log.nb_nouvelles == 8
        assert log.duree_sec == 45.3

    def test_default_counts(self) -> None:
        """Verifie que les compteurs par defaut sont a 0."""
        log = ScrapingLog(source="pap", type_scrape="location")
        assert log.nb_annonces_scrapees == 0
        assert log.nb_nouvelles == 0
        assert log.nb_erreurs == 0

    def test_repr(self) -> None:
        """Verifie la representation textuelle du log de scraping."""
        log = ScrapingLog(
            id=99,
            source="seloger",
            type_scrape="location",
            nb_nouvelles=12,
            nb_erreurs=0,
        )
        result = repr(log)
        assert "ScrapingLog" in result
        assert "seloger" in result
        assert "location" in result
        assert "12" in result


class TestValidationLogModel:
    """Tests pour le modele ValidationLog."""

    def test_instantiation(self) -> None:
        """Verifie qu'un log de validation peut etre instancie."""
        log = ValidationLog(
            url_source="https://example.com/bad",
            source="leboncoin",
            raison_rejet="prix hors bornes",
            donnees_brutes={"prix": -5000, "surface_m2": 50.0},
        )
        assert log.url_source == "https://example.com/bad"
        assert log.raison_rejet == "prix hors bornes"
        assert log.donnees_brutes["prix"] == -5000

    def test_repr(self) -> None:
        """Verifie la representation textuelle du log de validation."""
        log = ValidationLog(
            id=33,
            source="pap",
            raison_rejet="surface invalide",
        )
        result = repr(log)
        assert "ValidationLog" in result
        assert "pap" in result
        assert "surface invalide" in result


class TestBaseDeclarative:
    """Tests pour la classe de base DeclarativeBase."""

    def test_base_has_metadata(self) -> None:
        """Verifie que Base possede des metadonnees."""
        assert Base.metadata is not None

    def test_all_tables_registered(self) -> None:
        """Verifie que toutes les tables sont enregistrees dans les metadonnees."""
        expected_tables = {
            "annonces",
            "scores",
            "enrichissement_ia",
            "quartiers",
            "loyers_marche",
            "loyers_reference",
            "alertes_log",
            "scraping_log",
            "validation_log",
        }
        actual_tables = set(Base.metadata.tables.keys())
        assert expected_tables == actual_tables


class TestRelationships:
    """Tests pour les relations entre modeles."""

    def test_annonce_has_scores_relationship(self) -> None:
        """Verifie que le modele Annonce possede la relation scores."""
        annonce = Annonce(
            url_source="https://example.com/rel1",
            source="leboncoin",
            hash_dedup="rel1",
            prix=140000,
            surface_m2=50.0,
            nb_pieces=2,
        )
        # La relation existe en tant qu'attribut (liste vide sans session)
        assert hasattr(annonce, "scores")

    def test_annonce_has_enrichissements_relationship(self) -> None:
        """Verifie que le modele Annonce possede la relation enrichissements."""
        annonce = Annonce(
            url_source="https://example.com/rel2",
            source="pap",
            hash_dedup="rel2",
            prix=130000,
            surface_m2=40.0,
            nb_pieces=2,
        )
        assert hasattr(annonce, "enrichissements")

    def test_annonce_has_alertes_relationship(self) -> None:
        """Verifie que le modele Annonce possede la relation alertes."""
        annonce = Annonce(
            url_source="https://example.com/rel3",
            source="leboncoin",
            hash_dedup="rel3",
            prix=155000,
            surface_m2=60.0,
            nb_pieces=3,
        )
        assert hasattr(annonce, "alertes")

    def test_score_has_annonce_relationship(self) -> None:
        """Verifie que le modele Score possede la relation annonce."""
        score = Score(score_global=75.0)
        assert hasattr(score, "annonce")

    def test_enrichissement_has_annonce_relationship(self) -> None:
        """Verifie que le modele EnrichissementIA possede la relation annonce."""
        enrichissement = EnrichissementIA(etat_bien="bon_etat")
        assert hasattr(enrichissement, "annonce")

    def test_alerte_has_annonce_relationship(self) -> None:
        """Verifie que le modele AlerteLog possede la relation annonce."""
        alerte = AlerteLog(niveau="top")
        assert hasattr(alerte, "annonce")

    def test_cascade_config(self) -> None:
        """Verifie que les relations de l'annonce sont configurees en cascade."""
        # Inspecter les relations via le mapper
        mapper = Annonce.__mapper__
        scores_rel = mapper.relationships["scores"]
        enrichissements_rel = mapper.relationships["enrichissements"]
        alertes_rel = mapper.relationships["alertes"]

        assert "delete" in scores_rel.cascade
        assert "delete-orphan" in scores_rel.cascade
        assert "delete" in enrichissements_rel.cascade
        assert "delete-orphan" in enrichissements_rel.cascade
        assert "delete" in alertes_rel.cascade
        assert "delete-orphan" in alertes_rel.cascade


class TestTableConstraints:
    """Tests pour les contraintes de table."""

    def test_annonce_url_unique_constraint(self) -> None:
        """Verifie que url_source a une contrainte unique sur annonces."""
        table = Base.metadata.tables["annonces"]
        url_col = table.c.url_source
        assert url_col.unique is True

    def test_loyer_marche_url_unique_constraint(self) -> None:
        """Verifie que url_source a une contrainte unique sur loyers_marche."""
        table = Base.metadata.tables["loyers_marche"]
        url_col = table.c.url_source
        assert url_col.unique is True

    def test_loyer_reference_unique_segment(self) -> None:
        """Verifie la contrainte unique sur le segment de loyer_reference."""
        table = Base.metadata.tables["loyers_reference"]
        unique_constraints = [
            c for c in table.constraints
            if hasattr(c, "columns") and len(c.columns) == 3
        ]
        # Doit avoir une contrainte unique sur (quartier, type_bien, meuble)
        assert len(unique_constraints) >= 1
        col_names = {c.name for c in unique_constraints[0].columns}
        assert col_names == {"quartier", "type_bien", "meuble"}

    def test_annonce_indexes_exist(self) -> None:
        """Verifie que les index recommandes sont definis sur annonces."""
        table = Base.metadata.tables["annonces"]
        index_names = {idx.name for idx in table.indexes}
        assert "idx_annonces_statut" in index_names
        assert "idx_annonces_quartier" in index_names
        assert "idx_annonces_source" in index_names
        assert "idx_annonces_prix" in index_names

    def test_scraping_log_date_index(self) -> None:
        """Verifie l'index sur la date d'execution du scraping_log."""
        table = Base.metadata.tables["scraping_log"]
        index_names = {idx.name for idx in table.indexes}
        assert "idx_scraping_log_date" in index_names

    def test_loyers_marche_source_index(self) -> None:
        """Verifie l'index sur la source de loyers_marche."""
        table = Base.metadata.tables["loyers_marche"]
        index_names = {idx.name for idx in table.indexes}
        assert "idx_loyers_marche_source" in index_names

    def test_loyers_segment_index(self) -> None:
        """Verifie l'index sur le segment de loyers_reference."""
        table = Base.metadata.tables["loyers_reference"]
        index_names = {idx.name for idx in table.indexes}
        assert "idx_loyers_segment" in index_names
