# -*- coding=utf-8 -*-

import tempfile
from urllib.request import urlretrieve
import gzip
import geojson

from sqlalchemy import Column, BigInteger, String, Integer
from geoalchemy2 import Geometry
from shapely.geometry import shape
from shapely import wkb

from datasets import Dataset
from constants import WGS84


def load_cadastre_for_department(department):

    cadastre = Dataset("etl", "cadastre")

    with cadastre.get_writer() as writer:

        with tempfile.NamedTemporaryFile() as temp:

            url = "https://cadastre.data.gouv.fr" + \
                "/data/etalab-cadastre/latest/geojson/departements" + \
                "/{dep}/cadastre-{dep}-parcelles.json.gz" \
                .format(dep=department)

            urlretrieve(url, temp.name)

            with gzip.open(temp.name) as f:

                data = geojson.loads(f.read())

                features = data["features"]

                for feature in features:

                    s = shape(feature["geometry"])

                    row = {
                        "code": feature["id"],
                        "commune": feature["properties"]["commune"],
                        "prefixe": feature["properties"]["prefixe"],
                        "section": feature["properties"]["section"],
                        "numero": feature["properties"]["numero"],
                        "type": feature["type"],
                        "type_geom": feature["geometry"]["type"],
                        "geog": wkb.dumps(s, hex=True, srid=WGS84)
                    }

                    writer.write_row_dict(row)


def create_cadastre_table():

    cadastre = Dataset("etl", "cadastre")

    dtype = [
        Column("id", BigInteger(), primary_key=True, autoincrement=True),
        Column("version", Integer),
        Column("code", String),
        Column("commune", String),
        Column("prefixe", String),
        Column("section", String),
        Column("numero", String),
        Column("type", String),
        Column("type_geom", String),
        Column("geog", Geometry(srid=4326))
    ]

    cadastre.write_dtype(dtype)