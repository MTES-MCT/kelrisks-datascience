# -*- coding=utf-8 -*-

"""
Regroupe les recettes Python utilisées pour la préparation
des données s3ic
"""

from sqlalchemy import Column, String, func
from geoalchemy2 import Geometry

from datasets import Dataset
from constants import WGS84
from utils import row2dict
from scrapers import IcpeScraper, fetch_parallel
import precisions


def scrap_adresses():
    """
    Scrappe les adresses présentes sur les fiches détails Géorisques

    Exemple:

    À partir de l'url
    http://www.installationsclassees.developpement-durable.gouv.fr
    /ficheEtablissement.php?champEtablBase=61&champEtablNumero=14605

    On extraie => Lieu dit 'Les Murettes' 26300 BEAUREGARD BARET

    Pour des raisons de performance, on scrappe uniquement les adresses
    pour les enregistrements dont la précision est "Centroïde Commune"
    """

    # input dataset
    s3ic_filtered = Dataset("etl", "s3ic_source")

    # output dataset
    s3ic_scraped = Dataset("etl", "s3ic_scraped")

    dtype = s3ic_filtered.read_dtype()

    output_dtype = [
        *dtype,
        Column("adresse", String)]

    s3ic_scraped.write_dtype(output_dtype)

    with s3ic_scraped.get_writer() as writer:

        for df in s3ic_filtered.get_dataframes(chunksize=100):

            filtered = df.loc[
                (df["lib_precis"] == "Centroïde Commune")
                & (df["url_fiche"].notnull())
            ].copy()

            urls = filtered["url_fiche"].tolist()
            scrapers = [IcpeScraper(url) for url in urls]
            fetch_parallel(scrapers)

            for scraper in scrapers:
                scraper.parse()
                scraper.find_adresse()

            filtered["adresse"] = [s.adresse for s in scrapers]

            def f(row):
                try:
                    return filtered["adresse"].loc[row.name]
                except KeyError:
                    return None

            df["adresse"] = df.apply(lambda row: f(row), axis=1)

            writer.write_dataframe(df)


def normalize_precision():
    """
    Cette recette permet de normaliser les valeurs
    de la colonne lib_precis dans la nomenclature
    PARCEL, HOUSENUMBER, MUNICIPALITY
    """

    # input dataset
    s3ic_geocoded = Dataset("etl", "s3ic_geocoded")

    # output dataset
    s3ic_normalized = Dataset("etl", "s3ic_normalized")

    dtype = s3ic_geocoded.read_dtype()
    s3ic_normalized.write_dtype(dtype)

    with s3ic_normalized.get_writer() as writer:

        for row in s3ic_geocoded.iter_rows():

            mapping = {
                "Coordonnées précises": precisions.PARCEL,
                "Coordonnée précise": precisions.PARCEL,
                "Valeur Initiale": precisions.PARCEL,
                "Adresse postale": precisions.HOUSENUMBER,
                "Centroïde Commune": precisions.MUNICIPALITY,
                "Inconnu": precisions.MUNICIPALITY
            }
            precision = row.get("lib_precis")
            if precision:
                row["lib_precis"] = mapping.get(precision)
            else:
                row["lib_precis"] = precisions.MUNICIPALITY

            writer.write_row_dict(row)


def merge_geog():
    """
    Fusionne les infomations géographiques et produit
    les champs normalisés geog, geog_precision, geog_source

    On choisit de privilégier les informations de géocodage
    lorsque la précision initiale est MUNICIPALITY et que
    le score de géocodage est supérieur à 0.6
    """

    # input dataset
    s3ic_normalized = Dataset("etl", "s3ic_normalized")

    # output dataset
    s3ic_merged = Dataset("etl", "s3ic_geog_merged")

    dtype = s3ic_normalized.read_dtype()

    # geom => geog
    # lib_precis => precision
    dtype = [c for c in dtype if c.name not in ("geom", "lib_precis")]

    output_dtype = [
        *dtype,
        Column("geog", Geometry(srid=4326)),
        Column("geog_precision", String),
        Column("geog_source", String)]

    s3ic_merged.write_dtype(output_dtype)

    S3icNormalized = s3ic_normalized.reflect()

    session = s3ic_normalized.get_session()

    point_geocoded = func.ST_setSRID(
        func.ST_MakePoint(
            S3icNormalized.geocoded_longitude,
            S3icNormalized.geocoded_latitude), WGS84)

    q = session.query(S3icNormalized, point_geocoded).all()

    with s3ic_merged.get_writer() as writer:

        for (row, point) in q:

            output_row = {
                **row2dict(row),
                "geog": row.geom,
                "geog_precision": row.lib_precis,
                "geog_source": "initial_data"
            }

            c1 = row.lib_precis not in \
                (precisions.HOUSENUMBER, precisions.PARCEL)
            c2 = row.geocoded_latitude and row.geocoded_longitude
            c3 = row.geocoded_result_score and row.geocoded_result_score > 0.6
            c4 = row.geocoded_result_type == precisions.HOUSENUMBER

            if c1 and c2 and c3 and c4:
                output_row["geog"] = point
                output_row["geog_precision"] = row.geocoded_result_type
                output_row["geog_source"] = "geocodage"

            writer.write_row_dict(output_row)

    session.close()


def add_parcelle():
    """
    Projette les points géométriques sur la parcelle la plus proche.
    On se limite à un voisinage d'environ 10m = 0.0001 degré.

    Si aucune parcelle n'a été trouvée dans ce voisinage on conserve
    le point d'origine.

    La requête SQL Alchemy est équivalente à

    SELECT *,
        (SELECT geog
            FROM kelrisks.cadastre AS c
            WHERE st_dwithin(ic.geog, c.geog, 0.0001)
            ORDER BY st_distance(ic.geog, c.geog)
            LIMIT 1) nearest
    FROM etl.s3ic_geog_merged ic

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
    s3ic_geog_merged = Dataset("etl", "s3ic_geog_merged")
    cadastre = Dataset("kelrisks", "cadastre")

    # Output dataset
    s3ic_with_parcelle = Dataset("etl", "s3ic_with_parcelle")

    dtype = s3ic_geog_merged.read_dtype()
    s3ic_with_parcelle.write_dtype(dtype)

    Cadastre = cadastre.reflect()

    S3icGeogMerged = s3ic_geog_merged.reflect()
    session = s3ic_geog_merged.get_session()

    stmt = session.query(Cadastre.geog) \
                  .filter(func.st_dwithin(
                      Cadastre.geog,
                      S3icGeogMerged.geog,
                      0.0001)) \
                  .order_by(func.st_distance(
                      Cadastre.geog,
                      S3icGeogMerged.geog)) \
                  .limit(1) \
                  .label("nearest")

    q = session.query(S3icGeogMerged, stmt).all()

    with s3ic_with_parcelle.get_writer() as writer:
        for (row, cadastre_geog) in q:
            if cadastre_geog is not None \
               and row.geog_precision != precisions.MUNICIPALITY:
                row.geog = cadastre_geog
            writer.write_row_dict(row2dict(row))

    session.close()


def add_commune():
    """
    Ajoute le contour des communes comme nouvelle valeur pour
    geog dans le cas où la précision est MUNICIPALITY

    La reqête SqlAlchemy est équivalente à

    SELECT *
    FROM etl.s3ic_with_parcelle A
    LEFT JOIN etl.commune B
    ON A.cd_insee == B.insee
    """

    # input dataset
    s3ic_with_parcelle = Dataset("etl", "s3ic_with_parcelle")
    communes = Dataset("etl", "commune")

    # output dataset
    s3ic_with_commune = Dataset("etl", "s3ic_with_commune")

    dtype = s3ic_with_parcelle.read_dtype()
    s3ic_with_commune.write_dtype(dtype)

    S3icWithParcelle = s3ic_with_parcelle.reflect()
    Commune = communes.reflect()

    session = s3ic_with_parcelle.get_session()

    q = session.query(S3icWithParcelle, Commune.geog) \
               .join(Commune,
                     S3icWithParcelle.cd_insee == Commune.insee,
                     isouter=True) \
               .all()

    with s3ic_with_commune.get_writer() as writer:

        for (row, commune) in q:

            if row.geog_precision == precisions.MUNICIPALITY:
                row.geog = commune
                row.precision = precisions.MUNICIPALITY
                row.geog_source = "code_insee"

            writer.write_row_dict(row2dict(row))

    session.close()


def check():
    """
    Cette recette permet de faire des tests afin de vérifier que
    la table générée est bien conforme. Dans un premier temps
    on vérifie uniquement que le nombre d'enregistrements est
    supérieur à 0
    """

    s3ic = Dataset("etl", "s3ic")
    S3ic = s3ic.reflect()
    session = s3ic.get_session()
    s3ic_count = session.query(S3ic).count()
    session.close()

    assert s3ic_count > 0
