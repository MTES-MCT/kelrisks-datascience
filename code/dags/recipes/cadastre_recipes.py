# -*- coding=utf-8 -*-

import tempfile
from urllib.request import urlretrieve
import gzip
import geojson

from shapely.geometry import shape
from shapely import wkb
import requests
from airflow.hooks.data_preparation import PostgresDataset

from constants import WGS84
from scrapers import CadastreCommuneScraper
from config import CONN_ID


def load_cadastre_for_department(department):

    cadastre_temp = PostgresDataset(
        name="cadastre_{}_temp".format(department),
        schema="etl",
        postgres_conn_id=CONN_ID)

    dep_url = "https://cadastre.data.gouv.fr/data/etalab-cadastre" + \
        "/latest/geojson/communes/{dep}/".format(dep=department)

    # Recherche la liste des communes dans la page web
    scraper = CadastreCommuneScraper(dep_url)
    with requests.Session() as session:
        scraper.fetch_url(session)
    scraper.parse()
    scraper.find_communes()
    communes = scraper.communes

    # Iterate over each commune and load data into temporary table
    with cadastre_temp.get_writer() as writer:

        for commune in communes:

            with tempfile.NamedTemporaryFile() as temp:

                commune_url = "https://cadastre.data.gouv.fr/" + \
                    "data/etalab-cadastre/latest/geojson/communes/" + \
                    "{dep}/{commune}/cadastre-{commune}-parcelles.json.gz" \
                    .format(dep=department, commune=commune)

                urlretrieve(commune_url, temp.name)

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
