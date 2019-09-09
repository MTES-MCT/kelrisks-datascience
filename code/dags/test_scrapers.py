# -*- coding=utf-8 -*-

from unittest import TestCase

import requests

from scrapers import IcpeScraper


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
