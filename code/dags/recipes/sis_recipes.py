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
from config import DEPARTEMENTS


def load_sis():
    """
    Charge les données SIS à partir d'un fichier csv contenant
    un champ geom au format geojson
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


def filter_departements():
    """
    Filtre les données pour conserver uniquement
    les enregistrements localisés dans les
    départements sélectionnés dans la config
    """

    # Input dataset
    sis_source = Dataset("etl", "sis_source")

    # output dataset
    sis_filtered = Dataset("etl", "sis_filtered")

    sis_filtered.write_dtype(
        sis_source.read_dtype())

    with sis_filtered.get_writer() as writer:
        for row in sis_source.iter_rows():
            code_insee = row["code_insee"]
            keep_row = False
            for departement in DEPARTEMENTS:
                if code_insee.startswith(departement):
                    keep_row = True
                    break
            if keep_row:
                writer.write_row_dict(row)


def check():
    """
    Cette recette permet de faire des tests afin de vérifier que
    la table générée est bien conforme. Dans un premier temps
    on vérifie uniquement que le nombre d'enregistrements est
    supérieur à 0
    """

    sis = Dataset("etl", "sis_with_precision")
    SIS = sis.reflect()
    session = sis.get_session()
    sis_count = session.query(SIS).count()
    session.close()

    assert sis_count > 0
