"""Modeles SQLAlchemy 2.0 pour ImmoScan.

Definit toutes les tables PostgreSQL/PostGIS utilisees par l'application :
annonces de vente, scores, enrichissement IA, quartiers, observatoire loyers,
alertes, et tables de monitoring.

Utilise le style DeclarativeBase avec Mapped et mapped_column.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from geoalchemy2 import Geometry
from sqlalchemy import (
    Boolean,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy import func as sa_func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Classe de base pour tous les modeles SQLAlchemy ImmoScan."""

    pass


class Annonce(Base):
    """Annonce de vente immobiliere.

    Stocke les donnees brutes et normalisees des annonces de vente
    scrapees depuis les differentes sources (LeBonCoin, PAP, etc.).

    Attributes:
        id: Identifiant unique auto-incremente.
        url_source: URL d'origine de l'annonce (unique).
        source: Source de l'annonce ('leboncoin', 'pap').
        hash_dedup: Hash pour la deduplication intra-source.
        source_ids: IDs des doublons inter-sources detectes.
        prix: Prix de vente en euros.
        surface_m2: Surface en metres carres.
        nb_pieces: Nombre de pieces.
        dpe: Diagnostic de performance energetique (A-G ou null).
        etage: Numero d'etage (nullable).
        adresse_brute: Adresse telle que scrapee.
        quartier: Quartier de Besancon identifie.
        coordonnees: Point geographique PostGIS (SRID 4326).
        charges_copro: Charges de copropriete mensuelles.
        description_texte: Description textuelle de l'annonce.
        photos_urls: Liste d'URLs des photos.
        date_publication: Date de publication de l'annonce.
        date_scrape: Date de scraping.
        date_modification: Date de derniere modification detectee.
        historique_prix: Historique des changements de prix.
        completude_score: Score de completude des donnees (0-1).
        statut: Statut de traitement de l'annonce.
        created_at: Date de creation en base.
    """

    __tablename__ = "annonces"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    url_source: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    source: Mapped[str] = mapped_column(String(20), nullable=False)
    hash_dedup: Mapped[str] = mapped_column(String(64), nullable=False)
    source_ids: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    prix: Mapped[int] = mapped_column(Integer, nullable=False)
    surface_m2: Mapped[float] = mapped_column(Float, nullable=False)
    nb_pieces: Mapped[int] = mapped_column(Integer, nullable=False)
    dpe: Mapped[Optional[str]] = mapped_column(String(1), nullable=True)
    etage: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    adresse_brute: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    quartier: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    coordonnees = mapped_column(
        Geometry(geometry_type="POINT", srid=4326), nullable=True
    )
    charges_copro: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    description_texte: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    photos_urls: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    date_publication: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    date_scrape: Mapped[datetime] = mapped_column(
        nullable=False, server_default=sa_func.now()
    )
    date_modification: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    historique_prix: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    completude_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    statut: Mapped[Optional[str]] = mapped_column(
        String(20), default="nouveau", server_default="nouveau"
    )
    created_at: Mapped[Optional[datetime]] = mapped_column(
        server_default=sa_func.now()
    )

    # Relations
    scores: Mapped[list[Score]] = relationship(
        "Score", back_populates="annonce", cascade="all, delete-orphan"
    )
    enrichissements: Mapped[list[EnrichissementIA]] = relationship(
        "EnrichissementIA", back_populates="annonce", cascade="all, delete-orphan"
    )
    alertes: Mapped[list[AlerteLog]] = relationship(
        "AlerteLog", back_populates="annonce", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("idx_annonces_statut", "statut"),
        Index("idx_annonces_quartier", "quartier"),
        Index("idx_annonces_source", "source"),
        Index("idx_annonces_prix", "prix"),
    )

    def __init__(self, **kwargs: object) -> None:
        kwargs.setdefault("statut", "nouveau")
        kwargs.setdefault("source_ids", [])
        kwargs.setdefault("photos_urls", [])
        kwargs.setdefault("historique_prix", [])
        super().__init__(**kwargs)

    def __repr__(self) -> str:
        return (
            f"<Annonce(id={self.id}, source='{self.source}', "
            f"prix={self.prix}, surface={self.surface_m2}m2, "
            f"quartier='{self.quartier}', statut='{self.statut}')>"
        )


class Score(Base):
    """Score calcule pour une annonce de vente.

    Contient le score composite et ses composantes, ainsi que
    les 4 scenarios de rentabilite brute (0%, -5%, -10%, -15%).

    Attributes:
        id: Identifiant unique.
        annonce_id: Reference vers l'annonce scoree.
        score_global: Score composite 0-100.
        renta_brute_affiche: Rentabilite brute au prix affiche.
        renta_brute_nego_5: Rentabilite brute avec -5% de nego.
        renta_brute_nego_10: Rentabilite brute avec -10% de nego.
        renta_brute_nego_15: Rentabilite brute avec -15% de nego.
        score_localisation: Score composante localisation.
        score_dpe: Score composante DPE.
        score_negociation: Score composante potentiel de negociation.
        score_vacance: Score composante risque vacance.
        regime_indicatif: Regime fiscal indicatif ('lmnp' ou 'nu').
        loyer_estime: Loyer estime en euros.
        fiabilite_loyer: Fiabilite de l'estimation ('preliminaire' ou 'fiable').
        date_calcul: Date du calcul.
    """

    __tablename__ = "scores"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    annonce_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("annonces.id", ondelete="CASCADE"), nullable=True
    )
    score_global: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    renta_brute_affiche: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    renta_brute_nego_5: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    renta_brute_nego_10: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    renta_brute_nego_15: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    score_localisation: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    score_dpe: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    score_negociation: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    score_vacance: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    regime_indicatif: Mapped[Optional[str]] = mapped_column(
        String(10), nullable=True
    )
    loyer_estime: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    fiabilite_loyer: Mapped[Optional[str]] = mapped_column(
        String(20), nullable=True
    )
    date_calcul: Mapped[Optional[datetime]] = mapped_column(
        server_default=sa_func.now()
    )

    # Relation
    annonce: Mapped[Optional[Annonce]] = relationship(
        "Annonce", back_populates="scores"
    )

    def __repr__(self) -> str:
        return (
            f"<Score(id={self.id}, annonce_id={self.annonce_id}, "
            f"score_global={self.score_global}, "
            f"renta_affiche={self.renta_brute_affiche})>"
        )


class EnrichissementIA(Base):
    """Enrichissement par Claude Haiku d'une annonce.

    Contient les signaux de negociation, l'etat du bien, les equipements,
    les red flags et le resume genere par l'IA.

    Attributes:
        id: Identifiant unique.
        annonce_id: Reference vers l'annonce enrichie.
        signaux_nego: Signaux de negociation detectes.
        etat_bien: Etat general du bien.
        equipements: Liste des equipements.
        red_flags: Points de vigilance detectes.
        info_copro: Informations sur la copropriete.
        resume_ia: Resume genere par l'IA.
        date_analyse: Date de l'analyse.
    """

    __tablename__ = "enrichissement_ia"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    annonce_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("annonces.id", ondelete="CASCADE"), nullable=True
    )
    signaux_nego: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    etat_bien: Mapped[Optional[str]] = mapped_column(String(30), nullable=True)
    equipements: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    red_flags: Mapped[Optional[list]] = mapped_column(JSONB, default=list)
    info_copro: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    resume_ia: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    date_analyse: Mapped[Optional[datetime]] = mapped_column(
        server_default=sa_func.now()
    )

    # Relation
    annonce: Mapped[Optional[Annonce]] = relationship(
        "Annonce", back_populates="enrichissements"
    )

    def __init__(self, **kwargs: object) -> None:
        kwargs.setdefault("signaux_nego", [])
        kwargs.setdefault("equipements", [])
        kwargs.setdefault("red_flags", [])
        super().__init__(**kwargs)

    def __repr__(self) -> str:
        return (
            f"<EnrichissementIA(id={self.id}, annonce_id={self.annonce_id}, "
            f"etat_bien='{self.etat_bien}')>"
        )


class Quartier(Base):
    """Zone geographique de reference pour Besancon.

    Definit les polygones des quartiers cibles, leurs scores d'attractivite
    et les profils locataires associes.

    Attributes:
        id: Identifiant unique.
        nom: Nom du quartier.
        polygone: Polygone PostGIS (SRID 4326).
        score_attractivite: Score d'attractivite (0-100).
        profil_locataire: Description du profil locataire type.
        tension_locative: Tension locative (0-1).
    """

    __tablename__ = "quartiers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    nom: Mapped[str] = mapped_column(String(50), nullable=False)
    polygone = mapped_column(
        Geometry(geometry_type="POLYGON", srid=4326), nullable=True
    )
    score_attractivite: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    profil_locataire: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    tension_locative: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    def __repr__(self) -> str:
        return (
            f"<Quartier(id={self.id}, nom='{self.nom}', "
            f"score_attractivite={self.score_attractivite})>"
        )


class LoyerMarche(Base):
    """Annonce de location observee sur le marche.

    Alimente l'observatoire des loyers pour calculer les medianes
    par segment (quartier x type x meuble).

    Attributes:
        id: Identifiant unique.
        url_source: URL d'origine (unique).
        source: Source de l'annonce ('leboncoin', 'pap', 'seloger').
        hash_dedup: Hash pour la deduplication.
        loyer_cc: Loyer charges comprises.
        loyer_hc: Loyer hors charges.
        surface_m2: Surface en metres carres.
        nb_pieces: Nombre de pieces.
        meuble: Location meublee ou non.
        quartier: Quartier identifie.
        coordonnees: Point geographique PostGIS.
        dpe: Diagnostic de performance energetique.
        date_publication: Date de publication.
        date_scrape: Date de scraping.
    """

    __tablename__ = "loyers_marche"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    url_source: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    source: Mapped[str] = mapped_column(String(20), nullable=False)
    hash_dedup: Mapped[str] = mapped_column(String(64), nullable=False)
    loyer_cc: Mapped[float] = mapped_column(Float, nullable=False)
    loyer_hc: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    surface_m2: Mapped[float] = mapped_column(Float, nullable=False)
    nb_pieces: Mapped[int] = mapped_column(Integer, nullable=False)
    meuble: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    quartier: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    coordonnees = mapped_column(
        Geometry(geometry_type="POINT", srid=4326), nullable=True
    )
    dpe: Mapped[Optional[str]] = mapped_column(String(1), nullable=True)
    date_publication: Mapped[Optional[datetime]] = mapped_column(nullable=True)
    date_scrape: Mapped[datetime] = mapped_column(
        nullable=False, server_default=sa_func.now()
    )

    __table_args__ = (
        Index("idx_loyers_marche_source", "source"),
    )

    def __repr__(self) -> str:
        return (
            f"<LoyerMarche(id={self.id}, source='{self.source}', "
            f"loyer_cc={self.loyer_cc}, surface={self.surface_m2}m2, "
            f"quartier='{self.quartier}')>"
        )


class LoyerReference(Base):
    """Mediane de loyer par segment de l'observatoire.

    Stocke les statistiques calculees de loyers par combinaison
    quartier x type de bien x meuble/nu.

    Attributes:
        id: Identifiant unique.
        quartier: Nom du quartier.
        type_bien: Type de bien ('T2', 'T3').
        meuble: Meuble ou non.
        loyer_median: Loyer median calcule.
        loyer_q1: Premier quartile.
        loyer_q3: Troisieme quartile.
        nb_annonces: Nombre d'annonces utilisees pour le calcul.
        fiabilite: 'preliminaire' (<5 annonces) ou 'fiable' (>=5).
        loyer_m2_median: Loyer median au metre carre.
        date_calcul: Date du dernier calcul.
    """

    __tablename__ = "loyers_reference"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    quartier: Mapped[str] = mapped_column(String(50), nullable=False)
    type_bien: Mapped[str] = mapped_column(String(5), nullable=False)
    meuble: Mapped[bool] = mapped_column(Boolean, nullable=False)
    loyer_median: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    loyer_q1: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    loyer_q3: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    nb_annonces: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    fiabilite: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    loyer_m2_median: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    date_calcul: Mapped[Optional[datetime]] = mapped_column(
        server_default=sa_func.now()
    )

    __table_args__ = (
        UniqueConstraint("quartier", "type_bien", "meuble", name="uq_loyer_segment"),
        Index("idx_loyers_segment", "quartier", "type_bien", "meuble"),
    )

    def __repr__(self) -> str:
        return (
            f"<LoyerReference(id={self.id}, quartier='{self.quartier}', "
            f"type_bien='{self.type_bien}', meuble={self.meuble}, "
            f"loyer_median={self.loyer_median}, fiabilite='{self.fiabilite}')>"
        )


class AlerteLog(Base):
    """Journal des alertes envoyees via Telegram.

    Attributes:
        id: Identifiant unique.
        annonce_id: Reference vers l'annonce alertee.
        canal: Canal d'envoi ('telegram').
        niveau: Niveau d'alerte ('top', 'bon', 'baisse_prix').
        date_envoi: Date et heure d'envoi.
    """

    __tablename__ = "alertes_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    annonce_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("annonces.id"), nullable=True
    )
    canal: Mapped[str] = mapped_column(
        String(20), default="telegram", server_default="telegram"
    )
    niveau: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    date_envoi: Mapped[Optional[datetime]] = mapped_column(
        server_default=sa_func.now()
    )

    # Relation
    annonce: Mapped[Optional[Annonce]] = relationship(
        "Annonce", back_populates="alertes"
    )

    def __init__(self, **kwargs: object) -> None:
        kwargs.setdefault("canal", "telegram")
        super().__init__(**kwargs)

    def __repr__(self) -> str:
        return (
            f"<AlerteLog(id={self.id}, annonce_id={self.annonce_id}, "
            f"niveau='{self.niveau}', canal='{self.canal}')>"
        )


class ScrapingLog(Base):
    """Journal d'execution des sessions de scraping.

    Permet le monitoring des performances et la detection
    d'anomalies dans le pipeline de scraping.

    Attributes:
        id: Identifiant unique.
        source: Source scrapee ('leboncoin', 'pap', 'seloger').
        type_scrape: Type de scraping ('vente' ou 'location').
        date_exec: Date d'execution.
        nb_annonces_scrapees: Nombre total d'annonces scrapees.
        nb_nouvelles: Nombre de nouvelles annonces.
        nb_erreurs: Nombre d'erreurs rencontrees.
        duree_sec: Duree d'execution en secondes.
        proxy_utilise: Proxy utilise pour cette session.
        erreur_detail: Detail de l'erreur eventuelle.
    """

    __tablename__ = "scraping_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(20), nullable=False)
    type_scrape: Mapped[str] = mapped_column(String(10), nullable=False)
    date_exec: Mapped[Optional[datetime]] = mapped_column(
        server_default=sa_func.now()
    )
    nb_annonces_scrapees: Mapped[int] = mapped_column(
        Integer, default=0, server_default="0"
    )
    nb_nouvelles: Mapped[int] = mapped_column(
        Integer, default=0, server_default="0"
    )
    nb_erreurs: Mapped[int] = mapped_column(
        Integer, default=0, server_default="0"
    )
    duree_sec: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    proxy_utilise: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    erreur_detail: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    __table_args__ = (
        Index("idx_scraping_log_date", "date_exec"),
    )

    def __init__(self, **kwargs: object) -> None:
        kwargs.setdefault("nb_annonces_scrapees", 0)
        kwargs.setdefault("nb_nouvelles", 0)
        kwargs.setdefault("nb_erreurs", 0)
        super().__init__(**kwargs)

    def __repr__(self) -> str:
        return (
            f"<ScrapingLog(id={self.id}, source='{self.source}', "
            f"type='{self.type_scrape}', nouvelles={self.nb_nouvelles}, "
            f"erreurs={self.nb_erreurs})>"
        )


class ValidationLog(Base):
    """Journal des annonces rejetees par la validation.

    Attributes:
        id: Identifiant unique.
        url_source: URL de l'annonce rejetee.
        source: Source de l'annonce.
        raison_rejet: Raison du rejet.
        donnees_brutes: Donnees brutes de l'annonce rejetee.
        date_rejet: Date du rejet.
    """

    __tablename__ = "validation_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    url_source: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    raison_rejet: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    donnees_brutes: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    date_rejet: Mapped[Optional[datetime]] = mapped_column(
        server_default=sa_func.now()
    )

    def __repr__(self) -> str:
        return (
            f"<ValidationLog(id={self.id}, source='{self.source}', "
            f"raison='{self.raison_rejet}')>"
        )
