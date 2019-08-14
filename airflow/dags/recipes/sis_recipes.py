# -*- coding=utf-8 -*-

import tempfile
from urllib.request import urlretrieve
import csv
import zipfile
import io
import geojson

from sqlalchemy import Column, String, BigInteger, Float, Text
from geoalchemy2 import Geometry
from shapely.geometry import shape
from shapely import wkb

from constants import WGS84
from datasets import Dataset


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
        Column("geom", Geometry(srid=WGS84))
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
                        row["geom"] = wkb.dumps(s, hex=True, srid=4326)

                        writer.write_row_dict(row)