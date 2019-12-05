# -*- coding=utf-8 -*-

import re
import numpy as np
from sqlalchemy import Column, BigInteger, Float, String, \
    Integer, func
from geoalchemy2 import Geometry
from bulk_geocoding import geocode as bulk_geocode

from datasets import Dataset
from utils import merge_dtype, row2dict
import precisions
from constants import LAMBERT2, WGS84
from config import DEPARTEMENTS
import transformers.basias_transformers as transformers


def filter_departements():
    """
    Garde uniquement les enregistrements référencés dans les
    départements configurés. Le numéro de département est
    extrait du champ "indice_departemental"
    Ex PAC8300506 => 83 (Var)

    Le processus est répété pour les trois tables
    - sites
    - cadastre
    - localisation
    """

    # input datasets
    basias_sites_source = Dataset("etl", "basias_sites_source")
    basias_localisation_source = Dataset("etl", "basias_localisation_source")
    basias_cadastre_source = Dataset("etl", "basias_cadastre_source")

    # output datasets
    basias_sites_filtered = Dataset("etl", "basias_sites_filtered")
    basias_localisation_filtered = Dataset(
        "etl", "basias_localisation_filtered")
    basias_cadastre_filtered = Dataset("etl", "basias_cadastre_filtered")

    datasets = [
        (basias_sites_source, basias_sites_filtered),
        (basias_localisation_source, basias_localisation_filtered),
        (basias_cadastre_source, basias_cadastre_filtered)]

    # Regex pour extraire le numéro de dép PAC8300506 => 83 (Var)
    r = re.compile("[A-Z]{3}(\d{2})\d{4}")

    def keep_row(row):
        m = r.match(row["indice_departemental"])
        if m:
            groups = m.groups()
            if groups:
                departement = groups[0]
                if departement in DEPARTEMENTS:
                    return True
        return False

    for (source, filtered) in datasets:

        # écrit le schéma de la table de sortie
        filtered.write_dtype([
            Column("id", BigInteger(), primary_key=True, autoincrement=True),
            *source.read_dtype(
                primary_key="indice_departemental")])

        with filtered.get_writer() as writer:
            for row in source.iter_rows():
                if keep_row(row):
                    writer.write_row_dict(row)


def prepare_sites():
    """
    Cette recette ajoute une clé primaire
    et garde uniquement certaines colonnes
    """

    # input dataset
    basias_sites_filtered = Dataset("etl", "basias_sites_filtered")

    # output dataset
    basias_sites_prepared = Dataset("etl", "basias_sites_prepared")

    # columns to keep
    keep = [
        "indice_departemental",
        "nom_usuel",
        "raison_sociale"]

    dtype = basias_sites_filtered.read_dtype()

    # transform schema
    output_dtype = [
        column
        for column in dtype
        if column.name in keep]

    id_column = Column(
        "id",
        BigInteger,
        primary_key=True,
        autoincrement=True)

    output_dtype = [id_column, *output_dtype]

    basias_sites_prepared.write_dtype(output_dtype)

    # transform data
    with basias_sites_prepared.get_writer() as writer:
        for row in basias_sites_filtered.iter_rows():
            output_row = dict(
                (key, row[key])
                for key in row
                if key in keep)
            writer.write_row_dict(output_row)


def merge_geog():
    """
    Sélectionne la meilleure information géographique
    entre
    - (x_lambert2_etendue, y_lambert2_etendue)
    - (xl2_adresse, yl2_adresse)
    - point géocodé

    Ajoute la précision associée et la source de l'info
    """

    # Input dataset
    basias_localisation_geocoded = Dataset(
        "etl", "basias_localisation_geocoded")

    # Output dataset
    basias_localisation_geog_merged = Dataset(
        "etl", "basias_localisation_geog_merged")

    basias_localisation_geog_merged.write_dtype([
        *basias_localisation_geocoded.read_dtype(),
        Column("geog", Geometry(srid=4326)),
        Column("geog_precision", String),
        Column("geog_source", String)])

    BasiasLocalisationGeocoded = basias_localisation_geocoded.reflect()

    session = basias_localisation_geocoded.get_session()

    point_lambert2_etendue = func.ST_Transform(
        func.ST_setSRID(
            func.ST_MakePoint(
                BasiasLocalisationGeocoded.x_lambert2_etendue,
                BasiasLocalisationGeocoded.y_lambert2_etendue
            ), LAMBERT2), WGS84)

    point_adresse = func.ST_Transform(
        func.ST_setSRID(
            func.ST_MakePoint(
                BasiasLocalisationGeocoded.xl2_adresse,
                BasiasLocalisationGeocoded.yl2_adresse
            ), LAMBERT2), WGS84)

    point_geocoded = func.ST_setSRID(
        func.ST_MakePoint(
            BasiasLocalisationGeocoded.geocoded_longitude,
            BasiasLocalisationGeocoded.geocoded_latitude), WGS84)

    q = session.query(
        BasiasLocalisationGeocoded,
        point_lambert2_etendue,
        point_adresse,
        point_geocoded).all()

    with basias_localisation_geog_merged.get_writer() as writer:
        for (row, p_lambert2_etendue, p_adresse, p_geocoded) in q:
            output_row = {
                **row2dict(row),
                "geog": None,
                "geog_precision": None,
                "geog_source": None}
            if p_lambert2_etendue is not None:
                # assert this data is accurate
                output_row["geog"] = p_lambert2_etendue
                output_row["geog_precision"] = precisions.PARCEL
                output_row["geog_source"] = "lambert2_etendue"
            elif p_adresse is not None and row.precision_adresse == "numéro":
                output_row["geog"] = p_adresse
                output_row["geog_precision"] = precisions.HOUSENUMBER
                output_row["geog_source"] = "adresse"
            elif p_geocoded is not None and row.geocoded_result_score > 0.6 \
                    and row.geocoded_result_type == precisions.HOUSENUMBER:
                output_row["geog"] = p_geocoded
                output_row["geog_precision"] = precisions.HOUSENUMBER
                output_row["geog_source"] = "geocodage"

            writer.write_row_dict(output_row)

    session.close()


def intersect():
    """
    Projette les points géométriques sur la parcelle la plus proche.
    On se limite à un voisinage d'environ 10m = 0.0001 degré.

    Si aucune parcelle n'a été trouvée dans ce voisinage on conserve
    le point d'origine.

    La requête SQL Alchemy est équivalente à

    SELECT *,
        (SELECT geog
            FROM kelrisks.cadastre AS c
            WHERE st_dwithin(basias.geog, c.geog, 0.0001)
            ORDER BY st_distance(basias.geog, c.geog)
            LIMIT 1) nearest
    FROM etl.basias_localisation_geog_merged basias

    On utilise la table cadastre du schéma kelrisks
    et non la table du schéma etl car il n'y a pas assez
    de stockage sur le serveur pour avoir 4 tables cadastre
    (preprod `etl`, preprod `kelrisks`, prod `etl`, prod `kelrisks`).
    On est donc obligé de supprimer les tables cadastres
    du schéma `etl` après les avoir copié dans le schéma `kelrisks`.
    La table `etl.cadastre` n'existe donc pas forcément au moment
    où ce DAG est executé.
    """

    # Input dataset
    basias_localisation_geog_merged = Dataset(
        "etl", "basias_localisation_geog_merged")
    cadastre = Dataset("kelrisks", "cadastre")

    # Output dataset
    basias_localisation_intersected = Dataset(
        "etl", "basias_localisation_intersected")

    dtype = basias_localisation_geog_merged.read_dtype()
    basias_localisation_intersected.write_dtype(dtype)

    Cadastre = cadastre.reflect()

    BasiasLocalisationGeogMerged = basias_localisation_geog_merged.reflect()
    session = basias_localisation_geog_merged.get_session()

    stmt = session.query(Cadastre.geog) \
                  .filter(func.st_dwithin(
                      Cadastre.geog,
                      BasiasLocalisationGeogMerged.geog,
                      0.0001)) \
                  .order_by(func.st_distance(
                      Cadastre.geog,
                      BasiasLocalisationGeogMerged.geog)) \
                  .limit(1) \
                  .label("nearest")

    q = session.query(BasiasLocalisationGeogMerged, stmt).yield_per(500)

    with basias_localisation_intersected.get_writer() as writer:
        for (row, cadastre_geog) in q:
            if cadastre_geog is not None:
                row.geog = cadastre_geog
            writer.write_row_dict(row2dict(row))

    session.close()


def join_localisation_cadastre():
    """
    Réalise une jointure entre la table localisation et la table cadastre
    """

    # Input datasets
    basias_localisation_intersected = Dataset(
        "etl", "basias_localisation_intersected")
    basias_cadastre_merged = Dataset(
        "etl", "basias_cadastre_merged")

    # Output dataset
    basias_localisation_with_cadastre = Dataset(
        "etl", "basias_localisation_with_cadastre")

    basias_localisation_with_cadastre.write_dtype(
        basias_localisation_intersected.read_dtype())

    BasiasLocalisationIntersected = basias_localisation_intersected.reflect()
    BasiasCadastreMerged = basias_cadastre_merged.reflect()

    session = basias_localisation_intersected.get_session()

    cond = BasiasCadastreMerged.indice_departemental \
        == BasiasLocalisationIntersected.indice_departemental

    q = session.query(
        BasiasLocalisationIntersected,
        BasiasCadastreMerged.geog) \
        .join(BasiasCadastreMerged, cond, isouter=True) \
        .all()

    with basias_localisation_with_cadastre.get_writer() as writer:
        for (row, cadastre_geog) in q:
            if cadastre_geog is not None:
                # replace geog with cadastre geog
                row.geog = cadastre_geog
                row.geog_precision = precisions.PARCEL
                row.geog_source = "cadastre"
            writer.write_row_dict(row2dict(row))


def parse_cadastre():
    """
    Cette recette permet d'extraire des parcelles
    du champ "numero_de_parcelle" qui n'est pas
    formaté correctement

    Exemple de valeurs:
    ZA 128 et 146
    ? 131
    AH 575-574-43-224 etc
    """

    # input dataset
    basias_cadastre_filtered = Dataset(
        "etl", "basias_cadastre_filtered")
    basias_localisation_filtered = Dataset(
        "etl", "basias_localisation_filtered")

    # output dataset
    basias_cadastre_parsed = Dataset("etl", "basias_cadastre_parsed")

    BasiasCadastreFiltered = basias_cadastre_filtered.reflect()
    BasiasLocalisationFiltered = basias_localisation_filtered.reflect()

    session = basias_cadastre_filtered.get_session()

    # write output schema
    output_dtype = [
        Column("id", BigInteger, primary_key=True, autoincrement=True),
        Column("indice_departemental", String),
        Column("commune", String),
        Column("section", String),
        Column("numero", String)]

    basias_cadastre_parsed.write_dtype(output_dtype)

    # Fait une jointure avec la table basias_localiation_source pour
    # récuperer le code insee
    q = session \
        .query(BasiasCadastreFiltered, BasiasLocalisationFiltered.numero_insee) \
        .join(
            BasiasLocalisationFiltered,
            BasiasCadastreFiltered.indice_departemental ==
            BasiasLocalisationFiltered.indice_departemental,
            isouter=True) \
        .all()

    with basias_cadastre_parsed.get_writer() as writer:
        for (cadastre, numero_insee) in q:
            row = {**row2dict(cadastre), "numero_insee": numero_insee}
            parcelles = transformers.extract_basias_parcelles_from_row(row)
            for parcelle in parcelles:
                output_row = {
                    "indice_departemental": row["indice_departemental"],
                    "commune": parcelle.commune,
                    "section": parcelle.section,
                    "numero": parcelle.numero}
                writer.write_row_dict(output_row)

    session.close()


def add_geog():
    """
    Cette recette réalise une jointure avec la table cadastre
    pour nettoyer les parcelles invalide.

    On utilise la table cadastre du schéma kelrisks
    et non la table du schéma etl car il n'y a pas assez
    de stockage sur le serveur pour avoir 4 tables cadastre
    (preprod `etl`, preprod `kelrisks`, prod `etl`, prod `kelrisks`).
    On est donc obligé de supprimer les tables cadastres
    du schéma `etl` après les avoir copié dans le schéma `kelrisks`.
    La table `etl.cadastre` n'existe donc pas forcément au moment
    où ce DAG est executé.
    """

    # Input dataset
    basias_cadastre_parsed = Dataset("etl", "basias_cadastre_parsed")
    cadastre = Dataset("kelrisks", "cadastre")

    # Output dataset
    basias_cadastre_with_geog = Dataset("etl", "basias_cadastre_with_geog")

    BasiasCadastreParsed = basias_cadastre_parsed.reflect()
    Cadastre = cadastre.reflect()

    basias_cadastre_with_geog.write_dtype([
        *basias_cadastre_parsed.read_dtype(),
        Column("geog", Geometry(srid=4326))
    ])

    session = basias_cadastre_parsed.get_session()

    cond = (BasiasCadastreParsed.commune == Cadastre.commune) & \
           (BasiasCadastreParsed.section == Cadastre.section) & \
           (BasiasCadastreParsed.numero == Cadastre.numero)

    q = session.query(BasiasCadastreParsed, Cadastre.geog) \
        .join(Cadastre, cond) \
        .yield_per(500)

    with basias_cadastre_with_geog.get_writer() as writer:

        for (row, geog) in q:
            output_row = {
                **row2dict(row),
                "geog": None}
            del output_row["id"]
            if geog is not None:
                output_row["geog"] = geog

            writer.write_row_dict(output_row)

    session.close()


def merge_cadastre_geog():
    """
    Aggrège les différentes parcelles d'un même
    site dans un Multi Polygon """

    # Input dataset
    basias_cadastre_with_geog = Dataset("etl", "basias_cadastre_with_geog")

    # Output dataset
    basias_cadastre_merged = Dataset("etl", "basias_cadastre_merged")

    dtype = [
        Column("id", BigInteger, primary_key=True, autoincrement=True),
        Column("indice_departemental", String),
        Column("geog", Geometry(srid=4326))
    ]
    basias_cadastre_merged.write_dtype(dtype)
    basias_cadastre_merged.write_dtype(dtype)

    BasiasCadastreWithGeog = basias_cadastre_with_geog.reflect()

    session = basias_cadastre_with_geog.get_session()

    select = [
        BasiasCadastreWithGeog.indice_departemental,
        func.st_multi(func.st_union(BasiasCadastreWithGeog.geog))
    ]

    q = session.query(*select) \
               .group_by(BasiasCadastreWithGeog.indice_departemental) \
               .all()

    with basias_cadastre_merged.get_writer() as writer:
        for (indice_departemental, geog) in q:
            row = {
                "indice_departemental": indice_departemental,
                "geog": geog}
            writer.write_row_dict(row)

    session.close()


def join_sites_localisation():
    """
    Réalise une jointure entre la table sites et la table localisation
    """

    # datasets to join
    basias_sites_prepared = Dataset("etl", "basias_sites_prepared")
    basias_localisation_with_cadastre = Dataset(
        "etl", "basias_localisation_with_cadastre")

    # output dataset
    basias_sites_localisation_joined = Dataset(
        "etl", "basias_sites_localisation_joined")

    # transform types
    output_dtype = [
        *basias_sites_prepared.read_dtype(),
        *basias_localisation_with_cadastre.read_dtype()]

    output_dtype = merge_dtype(output_dtype)

    basias_sites_localisation_joined.write_dtype(output_dtype)

    # transform data
    BasiasSitesPrepared = basias_sites_prepared.reflect()
    BasiasLocalisation = basias_localisation_with_cadastre.reflect()

    session = basias_sites_prepared.get_session()

    join_query = session \
        .query(BasiasSitesPrepared, BasiasLocalisation) \
        .join(BasiasLocalisation,
              BasiasSitesPrepared.indice_departemental ==
              BasiasLocalisation.indice_departemental,
              isouter=True) \
        .all()

    with basias_sites_localisation_joined.get_writer() as writer:
        for (site, localisation) in join_query:
            output_row = {c.name: None for c in output_dtype}
            output_row = {**output_row, **row2dict(site)}
            if localisation:
                output_row = {**output_row, **row2dict(localisation)}
            del output_row["id"]
            writer.write_row_dict(output_row)

    session.close()


def add_commune():
    """
    Ajoute le contour des communes comme nouvelle valeur pour
    geog dans le cas où la précision est MUNICIPALITY

    La reqête SqlAlchemy est équivalente à

    SELECT *
    FROM etl.basias_sites_localisation_joined A
    LEFT JOIN etl.commune B
    ON A.numero_insee == B.insee
    """

    # input dataset
    basias_sites_localisation_joined = Dataset(
        "etl", "basias_sites_localisation_joined")
    communes = Dataset("etl", "commune")

    # output dataset
    basias_sites_with_commune = Dataset("etl", "basias_sites_with_commune")

    dtype = basias_sites_localisation_joined.read_dtype()
    basias_sites_with_commune.write_dtype(dtype)

    BasiasSites = basias_sites_localisation_joined.reflect()
    Commune = communes.reflect()

    session = basias_sites_localisation_joined.get_session()

    q = session.query(BasiasSites, Commune.geog) \
               .join(Commune,
                     BasiasSites.numero_insee == Commune.insee,
                     isouter=True) \
               .all()

    with basias_sites_with_commune.get_writer() as writer:

        for (row, commune) in q:

            if row.geog is None:
                row.geog = commune
                row.geog_precision = precisions.MUNICIPALITY
                row.geog_source = "numero_insee"

            writer.write_row_dict(row2dict(row))

    session.close()


def check():
    """
    Cette recette permet de faire des tests afin de vérifier que
    la table générée est bien conforme. Dans un premier temps
    on vérifie uniquement que le nombre d'enregistrements est
    supérieur à 0
    """
    basias = Dataset("etl", "basias")
    Basias = basias.reflect()
    session = basias.get_session()
    basias_count = session.query(Basias).count()
    session.close()

    assert basias_count > 0
