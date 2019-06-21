

from unittest import TestCase, mock


from ..transformers import Geocode, AddGeography, Prepare
from .helpers import get_test_db


test_db = get_test_db()


class BaseTestCase(TestCase):

    def __init__(self, transformer, *args, **kwargs):
        self.transformer = transformer
        super(BaseTestCase, self).__init__(*args, **kwargs)

    def setUp(self):
        self.transformer.in_model.create_table()

    def tearDown(self):
        test_db.drop_tables(self.transformer.models, cascade=True)
        test_db.close()


class GeocodeTestCase(BaseTestCase):

    def __init__(self, *args, **kwargs):
        transformer = Geocode(test_db)
        super(GeocodeTestCase, self).__init__(
            transformer, *args, **kwargs)

    @mock.patch('kelrisks.transformers.geocode')
    def test_transform_load(self, mock_geocode):
        mock_geocodage = mock.Mock()
        mock_geocodage.x = 639260.0
        mock_geocodage.y = 6848190.0
        mock_geocodage.score = 0.821673
        mock_geocode.return_value = mock_geocodage
        data = {
            'code': '0065.04881',
            'nom': 'EIFFAGE ROUTE IDF (ex GERLAND,INFRA)',
            'raison_sociale': 'EIFFAGE ROUTE IDF',
            'etat_activite': 'Récolement fait',
            'regime': 'A',
            'commune': 'SACLAY',
            'code_insee': '91534',
            'code_postal': '91400',
            'adresse': '1 rue de Paris',
            'complement_adresse': '',
            'departement': 'ESSONNE',
            'x': 638428.0,
            'y': 6848180.0,
            'precision': 'Centroïde Commune'
        }
        self.transformer.in_model.create(**data)
        self.transformer.transform_load()
        transformed = list(self.transformer.out_model.select().dicts())
        self.assertEqual(len(transformed), 1)

        expected = {
            'code': '0065.04881',
            'nom': 'EIFFAGE ROUTE IDF (ex GERLAND,INFRA)',
            'raison_sociale': 'EIFFAGE ROUTE IDF',
            'etat_activite': 'Récolement fait',
            'regime': 'A',
            'commune': 'SACLAY',
            'code_insee': '91534',
            'code_postal': '91400',
            'adresse': '1 rue de Paris',
            'complement_adresse': '',
            'departement': 'ESSONNE',
            'x': 638428.0,
            'y': 6848180.0,
            'precision': 'Centroïde Commune',
            'geocoded_x': 639260.0,
            'geocoded_y': 6848190.0,
            'geocoded_score': 0.821673}

        self.assertEqual(transformed[0], expected)


class AddGeographyTestCase(BaseTestCase):

    def __init__(self, *args, **kwargs):
        transformer = AddGeography(test_db)
        super(AddGeographyTestCase, self).__init__(
            transformer, *args, **kwargs)

    def test_transform_load(self):
        data = {
            'code': '0065.04881',
            'nom': 'EIFFAGE ROUTE IDF (ex GERLAND,INFRA)',
            'raison_sociale': 'EIFFAGE ROUTE IDF',
            'etat_activite': 'Récolement fait',
            'regime': 'A',
            'commune': 'SACLAY',
            'code_insee': '91534',
            'code_postal': '91400',
            'adresse': '1 rue de Paris',
            'complement_adresse': '',
            'departement': 'ESSONNE',
            'x': 638428.0,
            'y': 6848180.0,
            'precision': 'Centroïde Commune',
            'geocoded_x': 639260.0,
            'geocoded_y': 6848190.0,
            'geocoded_score': 0.821673}

        self.transformer.in_model.create(**data)
        self.transformer.transform_load()
        transformed = list(self.transformer.out_model.select())
        self.assertEqual(len(transformed), 1)
        record = transformed[0]
        self.assertIsNotNone(record.geog)
        self.assertIsNotNone(record.geocoded_geog)


class PrepareTestCase(BaseTestCase):

    def __init__(self, *args, **kwargs):
        transformer = Prepare(test_db)
        super(PrepareTestCase, self).__init__(
            transformer, *args, **kwargs)

    def test_transform_load(self):

        data = {
            'code': '0065.04881',
            'nom': 'EIFFAGE ROUTE IDF (ex GERLAND,INFRA)',
            'raison_sociale': 'EIFFAGE ROUTE IDF',
            'etat_activite': 'Récolement fait',
            'regime': 'A',
            'commune': 'SACLAY',
            'code_insee': '91534',
            'code_postal': '91400',
            'adresse': '1 rue de Paris',
            'complement_adresse': '',
            'departement': 'ESSONNE',
            'x': 638428.0,
            'y': 6848180.0,
            'precision': 'Centroïde Commune',
            'geocoded_x': 639260.0,
            'geocoded_y': 6848190.0,
            'geocoded_score': 0.821673,
            'geog': [638428.0, 6848180.0],
            'geocoded_geog': [639260.0, 6848180.0]
        }

        query = self.transformer.in_model.create(**data)
        data = self.transformer.in_model.select().dicts()
        print(data[0])
        # self.transformer.transform_load()
        # transformed = list(self.transformer.out_model.select().dicts())
        # self.assertEqual(len(transformed), 1)
        # record = transformed[0]
        # print(record['geog'].x)