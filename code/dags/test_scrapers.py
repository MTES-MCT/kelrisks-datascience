# -*- coding=utf-8 -*-

from unittest import TestCase

import requests

from scrapers import IcpeScraper, CadastreCommuneScraper


class ICPEScraperTestCase(TestCase):

    def test_find_adresse(self):

        url = "http://www.installationsclassees.developpement-durable" + \
              ".gouv.fr/ficheEtablissement.php?champEtablBase=30&" + \
              "champEtablNumero=12015"

        scraper = IcpeScraper(url)

        with requests.Session() as session:

            scraper.fetch_url(session)

        scraper.parse()
        scraper.find_adresse()

        expected = "2, chemin de la sablière"
        self.assertEqual(scraper.adresse, expected)


class CadastreCommuneScraperTestCase(TestCase):

    def test_find_communes(self):

        url = "https://cadastre.data.gouv.fr/data/" + \
            "etalab-cadastre/2019-07-01/geojson/communes/01/"

        scraper = CadastreCommuneScraper(url)

        with requests.Session() as session:
            scraper.fetch_url(session)

        scraper.parse()
        scraper.find_communes()

        self.assertEqual(len(scraper.communes), 407)
