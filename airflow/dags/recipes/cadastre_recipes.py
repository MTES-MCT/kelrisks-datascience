# -*- coding=utf-8 -*-

import tempfile
from urllib.request import urlretrieve
import gzip
import geojson
import asyncio
from concurrent.futures import ThreadPoolExecutor

from sqlalchemy import Column, BigInteger, String, Index
from geoalchemy2 import Geometry
from shapely.geometry import shape
from shapely import wkb

from datasets import Dataset
from constants import DEPARTMENTS, WGS84


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


def load_cadastre_parallel(max_workers=5):

    async def inner():

        with ThreadPoolExecutor(max_workers=max_workers) as executor:

            tasks = [
                loop.run_in_executor(
                    executor,
                    load_cadastre_for_department,
                    departement
                )
                for departement in DEPARTMENTS
            ]

            return await asyncio.gather(*tasks)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(inner())


def load_cadastre():

    cadastre = Dataset("etl", "cadastre")

    dtype = [
        Column("id", BigInteger(), primary_key=True, autoincrement=True),
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

    load_cadastre_parallel()