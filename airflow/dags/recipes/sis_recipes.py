# -*- coding=utf-8 -*-

import tempfile
from urllib.request import urlretrieve
import csv
import zipfile
import io
import geojson
import numpy as np

from sqlalchemy import Column, String, BigInteger, Float, Text, Integer
from geoalchemy2 import Geometry
from shapely.geometry import shape
from shapely import wkb
from bulk_geocoding import geocode as bulk_geocode

from constants import WGS84
from datasets import Dataset
import precisions


def load_sis():
    """
    Load SIS data
    We do not use embulk here because it does not handle geojson
    """

    sis_source = Dataset("etl", "sis_source")

    dtype = [
        Column("id", BigInteger(), primary_key=True, autoincrement=True),
        Column("id_sis", String),
        Column("numero_affichage", String),
        Column("numero_basol", String),
        Column("adresse", Text),
        Column("lieu_dit", String),
        Column("code_insee", String),
        Column("nom_commune", String),
        Column("nom_departement", String),
        Column("x", Float(8)),
        Column("y", Float(8)),
        Column("surface_m2", Float),
        Column("geog", Geometry(srid=WGS84))
    ]

    sis_source.write_dtype(dtype)

    with sis_source.get_writer() as writer:

        with tempfile.NamedTemporaryFile() as temp:

            url = "https://kelrisks.fra1.digitaloceanspaces.com/sis.zip"
            urlretrieve(url, temp.name)

            with zipfile.ZipFile(temp.name) as zfile:

                with zfile.open("sis/sis.csv") as csvfile:

                    reader = csv.DictReader(
                        io.TextIOWrapper(csvfile),
                        delimiter=",",
                        quotechar="\"")

                    for row in reader:

                        g = geojson.loads(row["geom"])
                        s = shape(g)
                        row["geog"] = wkb.dumps(s, hex=True, srid=4326)

                        writer.write_row_dict(row)


def geocode():
    """ Geocode adresses """

    # input dataset
    sis_source = Dataset("etl", "sis_source")

    # output dataset
    sis_geocoded = Dataset("etl", "sis_geocoded")

    # write output schema
    dtype = sis_source.read_dtype()

    output_dtype = [
        *dtype,
        Column("geocoded_latitude", Float(precision=10)),
        Column("geocoded_longitude", Float(precision=10)),
        Column("geocoded_result_score", Float()),
        Column("geocoded_result_type", String()),
        Column("adresse_id", String())
    ]

    sis_geocoded.write_dtype(output_dtype)

    with sis_geocoded.get_writer() as writer:

        for df in sis_source.get_dataframes(chunksize=100):

            df = df.replace({np.nan: None})
            rows = df.to_dict(orient="records")
            payload = [{
                "adresse": row["adresse"],
                "code_insee": row["code_insee"]
            } for row in rows]

            geocoded = bulk_geocode(
                payload,
                columns=["adresse"],
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

                if row["geocoded_result_type"] == precisions.HOUSENUMBER and \
                   row["geocoded_result_score"] > 0.6:
                    row["adresse_id"] = geocodage["result_id"]
                else:
                    row["adresse_id"] = None

                writer.write_row_dict(row)


def set_precision():
    """ add fields geog_precision and geog_source """

    # Input dataset
    sis_source = Dataset("etl", "sis_geocoded")

    # Output dataset
    sis_with_precision = Dataset("etl", "sis_with_precision")

    dtype = sis_source.read_dtype()
    sis_with_precision.write_dtype([
        *dtype,
        Column("geog_precision", String),
        Column("geog_source", String)])

    with sis_with_precision.get_writer() as writer:

        for row in sis_source.iter_rows():

            output_row = {
                **row,
                "geog_precision": precisions.PARCEL,
                "geog_source": "geog"
            }
            writer.write_row_dict(output_row)


def add_version():
    """ Add a version column for compatibility with Spring """

    # Input dataset
    sis_with_precision = Dataset("etl", "sis_with_precision")

    # Output dataset
    sis_with_version = Dataset("etl", "sis_with_version")

    sis_with_version.write_dtype([
        *sis_with_precision.read_dtype(),
        Column("version", Integer)
    ])

    with sis_with_version.get_writer() as writer:
        for row in sis_with_precision.iter_rows():
            writer.write_row_dict(row)


def check():
    """ perform sanity checks on staged table """

    sis = Dataset("etl", "sis")

    SIS = sis.reflect()

    session = sis.get_session()

    count = session.query(SIS).count()

    assert count == 656

    session.close()