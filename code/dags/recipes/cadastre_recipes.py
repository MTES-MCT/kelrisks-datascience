# -*- coding=utf-8 -*-

import tempfile
from urllib.request import urlretrieve
import gzip
import geojson

from shapely.geometry import shape
from shapely import wkb
import requests

from constants import WGS84
from scrapers import CadastreCommuneScraper
from datasets import Dataset


def load_cadastre_for_department(department):

    cadastre_temp = Dataset(
        "etl", "cadastre_{dep}_temp".format(dep=department))

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
                            "geog": wkb.dumps(s, hex=True, srid=WGS84),
                            "version": 1
                        }

                        writer.write_row_dict(row)
