# -*- coding=utf-8 -*-

import geojson
from shapely.geometry import shape
from shapely import wkb
from sqlalchemy import Column, BigInteger, String, Integer
from geoalchemy2 import Geometry

from config import DATA_DIR
from constants import WGS84
from datasets import Dataset


def load_communes():

    communes_filepath = "{data_dir}/communes/communes-20190101.json" \
        .format(data_dir=DATA_DIR)

    communes = Dataset("etl", "commune")

    dtype = [
        Column("id", BigInteger(), primary_key=True, autoincrement=True),
        Column("insee", String),
        Column("nom", String),
        Column("geog", Geometry(srid=4326))
    ]

    communes.write_dtype(dtype)

    with open(communes_filepath, 'r') as f:

        data = geojson.loads(f.read())

        features = data["features"]

        with communes.get_writer() as writer:

            for feature in features:

                s = shape(feature["geometry"])

                row = {
                    "insee": feature["properties"]["insee"],
                    "nom": feature["properties"]["nom"],
                    "type_geom": feature["geometry"]["type"],
                    "geog": wkb.dumps(s, hex=True, srid=WGS84)
                }

                writer.write_row_dict(row)


def prepare_code_postal():

    code_postal_source = Dataset("etl", "code_postal_source")
    code_postal = Dataset("etl", "code_postal")

    dtype = [
        Column("id", BigInteger(), primary_key=True, autoincrement=True),
        Column("code_insee", String),
        Column("code_postal", String),
        Column("nom_commune", String),
        Column("version", Integer)]

    code_postal.write_dtype(dtype)

    with code_postal.get_writer() as writer:

        for row in code_postal_source.iter_rows(
                primary_key="Code_commune_INSEE"):

            output_row = {
                "code_insee": row["Code_commune_INSEE"],
                "code_postal": row["Code_postal"],
                "nom_commune": row["Nom_commune"]
            }

            writer.write_row_dict(output_row)