# -*- coding=utf-8 -*-


from sqlalchemy import Column, BigInteger, Float, String, func
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

        for row in s3ic_idf_with_geog.iter_rows():
            del row["id"]
            row["epsg"] = LAMBRT93
            writer.write_row_dict(row)


def scrap_adresses():
    """
    Scrap adresses from urlfiche for the records with no adress
    and precision = Centroïde Commune
    """

    # input dataset
    s3ic_stacked = Dataset("etl", "s3ic_stacked")

    # output dataset
    s3ic_scraped = Dataset("etl", "s3ic_scraped")

    s3ic_scraped.write_dtype(s3ic_stacked.read_dtype())

    with s3ic_scraped.get_writer() as writer:

        for df in s3ic_stacked.get_dataframes(chunksize=100):

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

        for df in s3ic_scraped.get_dataframes(chunksize=100):

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