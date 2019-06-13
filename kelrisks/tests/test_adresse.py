
from unittest import TestCase
import requests

from ..adresse import geocode, geocode_parallel


class AdresseTestCase(TestCase):

    def test_geocode(self):
        adresse = '4 boulevard longchamp'
        citycode = '13201'
        with requests.Session() as session:
            r = geocode(session, adresse, citycode)
