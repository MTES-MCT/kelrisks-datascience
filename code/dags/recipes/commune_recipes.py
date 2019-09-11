# -*- coding=utf-8 -*-

import geojson
from shapely.geometry import shape
from shapely import wkb
from sqlalchemy import Column, BigInteger, String
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