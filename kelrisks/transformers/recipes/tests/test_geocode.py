
from unittest import TestCase

from ..geocode import geocode_bulk, geocode_csv


class GeocodeTestCase(TestCase):

    def test_geocode_csv(self):
        """ it should geocoded a csv file """

        filepath = './kelrisks/transformers/recipes/tests/files/adresses.csv'

        with open(filepath, 'rb') as f:
            columns = ['adresse', 'city']
            postcode = 'postcode'
            response = geocode_csv(f, columns, postcode=postcode)
            self.assertEqual(response.status_code, 200)

    def test_geocode_bulk(self):
        """ it should geocode data in bulk """

        data = [
            {
                'adresse': '6 Rue Albert 1er',
                'city': 'Villers-lès-Nancy',
                'postcode': '54600'
            },
            {
                'adresse': '6 Rue d\'Aquitaine',
                'city': 'Vandœuvre-lès-Nancy',
                'postcode': '54500'
            }
        ]
        geocoded = geocode_bulk(
            data,
            columns=['adresse', 'city'],
            postcode='postcode')

        self.assertEqual(len(geocoded), 2)

