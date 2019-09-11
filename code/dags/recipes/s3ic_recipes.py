# -*- coding=utf-8 -*-


from sqlalchemy import Column, BigInteger, Float, String, Integer, func
from geoalchemy2 import Geometry
from geoalchemy2.shape import to_shape
from shapely import wkb
import numpy as np
from bulk_geocoding import geocode as bulk_geocode

from datasets import Dataset
from constants import WGS84, LAMBRT93
from utils import row2dict
from scrapers import IcpeScraper, fetch_parallel
import precisions
from config import DEPARTEMENTS
import precisions


def filter_idf():
    """
    Keep only records outside of IDF
    """

    # input dataset
    s3ic_source = Dataset("etl", "s3ic_source")

    # s3ic filtered
    s3ic_without_idf = Dataset("etl", "s3ic_without_idf")

    dtype = s3ic_source.read_dtype(primary_key="code_s3ic")
    dtype = [c for c in dtype if c.name != "gid"]

    s3ic_without_idf.write_dtype([
        Column("id", BigInteger(), primary_key=True, autoincrement=True),
        *dtype])

    idf = ["75", "77", "78", "91", "92", "93", "94", "95"]

    with s3ic_without_idf.get_writer() as writer:
        for row in s3ic_source.iter_rows():
            if row["num_dep"] not in idf:
                writer.write_row_dict(row)


def create_geog_idf():
    """ Create geometry field from x, y info on s3ic_idf_source """

    # Input dataset
    s3ic_idf_source = Dataset("etl", "s3ic_idf_source")

    # Output dataset
    s3ic_idf_with_geog = Dataset("etl", "s3ic_idf_with_geog")

    dtype = s3ic_idf_source.read_dtype(primary_key="code")

    s3ic_idf_with_geog.write_dtype([
        Column("id", BigInteger(), primary_key=True, autoincrement=True),
        *dtype,
        Column("geog", Geometry(srid=WGS84))])

    S3IC_IDF_Source = s3ic_idf_source.reflect(primary_key="code")

    session = s3ic_idf_source.get_session()

    geog = func.ST_Transform(
                func.ST_SetSrid(
                    func.ST_MakePoint(
                        S3IC_IDF_Source.x,
                        S3IC_IDF_Source.y),
                    LAMBRT93),
                WGS84)

    q = session.query(S3IC_IDF_Source, geog)

    with s3ic_idf_with_geog.get_writer() as writer:
        for (row, geog) in q:
            output_row = {
                **row2dict(row),
                "geog": geog}
            writer.write_row_dict(output_row)

    session.close()


def stack():
    """ Stack data from France Entière and IDF """

    # input dataset
    s3ic_without_idf = Dataset("etl", "s3ic_without_idf")
    s3ic_idf_with_geog = Dataset("etl", "s3ic_idf_with_geog")

    # outpt dataset
    s3ic_stacked = Dataset("etl", "s3ic_stacked")

    output_dtype = [
        Column("id", BigInteger(), primary_key=True, autoincrement=True),
        Column("code", String),
        Column("nom", String),
        Column("adresse", String),
        Column("complement_adresse", String),
        Column("code_insee", String),
        Column("code_postal", String),
        Column("commune", String),
        Column("code_naf", String),
        Column("lib_naf", String),
        Column("num_siret", String),
        Column("regime", String),
        Column("lib_regime", String),
        Column("ipcc", String),
        Column("seveso", String),
        Column("lib_seveso", String),
        Column("famille_ic", String),
        Column("url_fiche", String),
        Column("x", BigInteger),
        Column("y", BigInteger),
        Column("epsg", String),
        Column("precision", String),
        Column("geog", Geometry(srid=WGS84))]

    s3ic_stacked.write_dtype(output_dtype)

    with s3ic_stacked.get_writer() as writer:

        for row in s3ic_without_idf.iter_rows():
            del row["id"]
            row["code"] = row.pop("code_s3ic")
            row["nom"] = row.pop("nom_ets")
            row["code_insee"] = row.pop("cd_insee")
            row["code_postal"] = row.pop("cd_postal")
            row["commune"] = row.pop("nomcommune")
            row["precision"] = row.pop("lib_precis")

            s = to_shape(row.pop("geom"))
            row["geog"] = wkb.dumps(s, hex=True, srid=WGS84)

            writer.write_row_dict(row)

    with s3ic_stacked.get_writer() as writer:
        for row in s3ic_idf_with_geog.iter_rows():
            del row["id"]
            row["epsg"] = LAMBRT93
            writer.write_row_dict(row)


def filter_departements():
    """
    Keep only departements specified in config
    """

    # input dataset
    s3ic_stacked = Dataset("etl", "s3ic_stacked")

    # output dataset
    s3ic_filtered = Dataset("etl", "s3ic_filtered")

    s3ic_filtered.write_dtype(
        s3ic_stacked.read_dtype())

    def keep_row(row):
        for departement in DEPARTEMENTS:
            code_insee = row.get("code_insee")
            if code_insee and code_insee.startswith(departement):
                return True
        return False

    with s3ic_filtered.get_writer() as writer:
        for row in s3ic_stacked.iter_rows():
            if keep_row(row):
                writer.write_row_dict(row)


def scrap_adresses():
    """
    Scrap adresses from urlfiche for the records with no adress
    and precision = Centroïde Commune
    """

    # input dataset
    s3ic_filtered = Dataset("etl", "s3ic_filtered")

    # output dataset
    s3ic_scraped = Dataset("etl", "s3ic_scraped")

    s3ic_scraped.write_dtype(s3ic_filtered.read_dtype())

    with s3ic_scraped.get_writer() as writer:

        for df in s3ic_filtered.get_dataframes(chunksize=100):

            filtered = df.loc[
                (df["precision"] == "Centroïde Commune")
                & (df["adresse"].isnull())
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
                if row["adresse"]:
                    return row["adresse"]
                try:
                    return filtered["adresse"].loc[row.name]
                except KeyError:
                    return None

            df["adresse"] = df.apply(lambda row: f(row), axis=1)

            writer.write_dataframe(df)


def geocode():
    """ Geocode S3IC adresses """

    # input dataset
    s3ic_scraped = Dataset("etl", "s3ic_scraped")

    # output dataset
    s3ic_geocoded = Dataset("etl", "s3ic_geocoded")

    # write output schema
    dtype = s3ic_scraped.read_dtype()

    output_dtype = [
        Column("id", BigInteger(), primary_key=True, autoincrement=True),
        *dtype,
        Column("geocoded_latitude", Float(precision=10)),
        Column("geocoded_longitude", Float(precision=10)),
        Column("geocoded_result_score", Float()),
        Column("geocoded_result_type", String()),
        Column("adresse_id", String())
    ]

    s3ic_geocoded.write_dtype(output_dtype)

    with s3ic_geocoded.get_writer() as writer:

        for df in s3ic_scraped.get_dataframes(chunksize=50):

            df = df.replace({np.nan: None})

            rows = df.to_dict(orient="records")

            payload = [{
                "adresse": row["adresse"],
                "complement_adresse": row["complement_adresse"],
                "code_insee": row["code_insee"]
            } for row in rows]

            try:
                geocoded = bulk_geocode(
                    payload,
                    columns=["adresse", "complement_adresse"],
                    citycode="code_insee")

                zipped = list(zip(rows, geocoded))

                for (row, geocodage) in zipped:
                    latitude = geocodage["latitude"]
                    row["geocoded_latitude"] = float(latitude) \
                        if latitude else None
                    longitude = geocodage["longitude"]
                    row["geocoded_longitude"] = float(longitude) \
                        if longitude else None
                    result_score = geocodage["result_score"]
                    row["geocoded_result_score"] = float(result_score) \
                        if result_score else None
                    row["geocoded_result_type"] = geocodage["result_type"]
                    if geocodage["result_type"] == precisions.HOUSENUMBER \
                            and float(geocodage["result_score"]) > 0.6:
                        row["adresse_id"] = geocodage["result_id"]
                    else:
                        row["adresse_id"] = None
                    writer.write_row_dict(row)
            except Exception as e:
                print(e)
                # write data without geocoding
                for row in rows:
                    output_row = {
                        **row,
                        "geocoded_latitude": None,
                        "geocoded_longitude": None,
                        "geocoded_result_score": None,
                        "geocoded_result_type": None,
                        "adresse_id": None}
                    writer.write_row_dict(output_row)


def normalize_precision():
    """ normalize precision fields """

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
            precision = row.get("precision")
            if precision:
                row["precision"] = mapping.get(precision)
            else:
                row["precision"] = precisions.MUNICIPALITY

            writer.write_row_dict(row)


def merge_geog():
    """ choose best geography field """

    # input dataset
    s3ic_normalized = Dataset("etl", "s3ic_normalized")

    # output dataset
    s3ic_merged = Dataset("etl", "s3ic_geog_merged")

    dtype = s3ic_normalized.read_dtype()

    dtype = [c for c in dtype if c.name not in ("geog", "precision")]

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
                "geog_precision": row.precision,
                "geog_source": "initial_data"
            }

            del output_row["precision"]

            c1 = row.precision not in \
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


def intersect():
    """
    Find the closest parcelle to the point and set it as
    new geography
    """

    # Input dataset
    s3ic_geog_merged = Dataset("etl", "s3ic_geog_merged")
    cadastre = Dataset("etl", "cadastre")

    # Output dataset
    s3ic_intersected = Dataset("etl", "s3ic_intersected")

    dtype = s3ic_geog_merged.read_dtype()
    s3ic_intersected.write_dtype(dtype)

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

    with s3ic_intersected.get_writer() as writer:
        for (row, cadastre_geog) in q:
            if cadastre_geog is not None \
               and row.geog_precision != precisions.MUNICIPALITY:
                row.geog = cadastre_geog
            writer.write_row_dict(row2dict(row))

    session.close()


def add_communes():
    """
    set commune geog for all records where geog is not set with
    a better precision
    """

    # input dataset
    s3ic_intersected = Dataset("etl", "s3ic_intersected")
    communes = Dataset("etl", "commune")

    # output dataset
    s3ic_with_commune = Dataset("etl", "s3ic_with_commune")

    dtype = s3ic_intersected.read_dtype()
    s3ic_with_commune.write_dtype(dtype)

    S3icIntersected = s3ic_intersected.reflect()
    Commune = communes.reflect()

    session = s3ic_intersected.get_session()

    q = session.query(S3icIntersected, Commune.geog) \
               .join(Commune,
                     S3icIntersected.code_insee == Commune.insee,
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


def add_version():
    """ Add a version column for compatibility with Spring """

    # Input dataset
    s3ic_with_commune = Dataset("etl", "s3ic_with_commune")

    # Output dataset
    s3ic_with_version = Dataset("etl", "s3ic_with_version")

    s3ic_with_version.write_dtype([
        *s3ic_with_commune.read_dtype(),
        Column("version", Integer)
    ])

    with s3ic_with_version.get_writer() as writer:
        for row in s3ic_with_commune.iter_rows():
            writer.write_row_dict(row)


def check():

    # check we have same number of records
    # than data filtered

    s3ic = Dataset("etl", "s3ic")
    S3ic = s3ic.reflect()
    session = s3ic.get_session()
    s3ic_count = session.query(S3ic).count()
    session.close()

    s3ic_filtered = Dataset("etl", "s3ic_filtered")
    S3icFiltered = s3ic_filtered.reflect()
    session = s3ic_filtered.get_session()
    s3ic_filtered_count = session.query(S3icFiltered).count()
    session.close()

    assert s3ic_count > 0
    assert s3ic_count == s3ic_filtered_count
