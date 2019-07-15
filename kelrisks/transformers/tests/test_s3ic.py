
from unittest import mock
from shapely.geometry import Point

from ...tests.helpers import BaseTestCase
from ..s3ic import GeocodeTransformer, CreateGeographyTransformer, \
    CreateCentroideCommuneTransformer
from ...models.base import Geometry


class GeocodeTransformerTestCase(BaseTestCase):

    @mock.patch('kelrisks.transformers.s3ic.geocode_bulk')
    def test_transform_load(self, mock_geocode_bulk):

        transformer = GeocodeTransformer()
        data = {
            'code': '0065.04881',
            'nom': ' EIFFAGE ROUTE IDF (ex GERLAND,INFRA)',
            'raison_sociale': 'EIFFAGE ROUTE IDF',
            'etat_activite': 'Récolement fait',
            'regime': None,
            'commune': 'SACLAY',
            'code_insee': '91534',
            'code_postal': '91400',
            'adresse': 'CARREFOUR DU CHRIST',
            'complement_adresse': None,
            'departement': 'ESSONNE',
            'x': 638428.0,
            'y': 6848180.0,
            'precision': 'Valeur Initiale'}

        geocoded = {
            'latitude': '48.670355',
            'longitude': '6.146862',
            'result_label': '6 Rue Albert 1er 54600 Villers-lès-Nancy',
            'result_score': '0.91',
            'result_type': 'housenumber'
        }

        mock_geocode_bulk.return_value = [{**data, **geocoded}]

        transformer.input_model.create(**data)

        transformer.transform_load()

        transformed = transformer.output_model.select().dicts()[0]

        expected = {
            **data,
            'id': 1,
            'geocoded_latitude': 48.6704,
            'geocoded_longitude': 6.14686,
            'geocoded_score': 0.91,
            'geocoded_precision': 'housenumber',
            'geocoded_label': '6 Rue Albert 1er 54600 Villers-lès-Nancy'}

        self.assertEqual(transformed, expected)


class CreateGeographyTransformerTestCase(BaseTestCase):

    def test_transform_load(self):

        transformer = CreateGeographyTransformer()

        data = {
            'id': 1,
            'code': '0065.04881',
            'nom': ' EIFFAGE ROUTE IDF (ex GERLAND,INFRA)',
            'raison_sociale': 'EIFFAGE ROUTE IDF',
            'etat_activite': 'Récolement fait',
            'regime': None,
            'commune': 'SACLAY',
            'code_insee': '91534',
            'code_postal': '91400',
            'adresse': 'CARREFOUR DU CHRIST',
            'complement_adresse': None,
            'departement': 'ESSONNE',
            'x': 638428.0,
            'y': 6848180.0,
            'precision': 'Valeur Initiale',
            'geocoded_latitude': 48.6704,
            'geocoded_longitude': 6.14686,
            'geocoded_score': 0.91,
            'geocoded_precision': 'housenumber',
            'geocoded_label': '6 Rue Albert 1er 54600 Villers-lès-Nancy'}

        transformer.input_model.create(**data)

        transformer.transform_load()

        transformed = transformer.output_model.select().dicts()[0]

        expected = {
            **data,
            'geog': Geometry(
                Point(2.16284945854741, 48.7308078326469),
                4326),
            'geocoded_geog': Geometry(Point(6.14686, 48.6704), 4326)
        }


        expected_geog = Geometry(
            Point(
                2.1628494585474103,
                48.730807832646896),
            4326)
        self.assertEqual(transformed['geog'], expected_geog)


        #self.assertEqual(transformed, expected)


class CreateCentroideCommuneTransformerTestCase(BaseTestCase):

    def test_transform_load(self):

        transformer = CreateCentroideCommuneTransformer()

        data = {
            'id': 1,
            'code': '0065.04881',
            'nom': ' EIFFAGE ROUTE IDF (ex GERLAND,INFRA)',
            'raison_sociale': 'EIFFAGE ROUTE IDF',
            'etat_activite': 'Récolement fait',
            'regime': None,
            'commune': 'SACLAY',
            'code_insee': '91534',
            'code_postal': '91400',
            'adresse': 'CARREFOUR DU CHRIST',
            'complement_adresse': None,
            'departement': 'ESSONNE',
            'x': 638428.0,
            'y': 6848180.0,
            'precision': 'Valeur Initiale',
            'geocoded_latitude': 48.6704,
            'geocoded_longitude': 6.14686,
            'geocoded_score': 0.91,
            'geocoded_precision': 'housenumber',
            'geocoded_label': '6 Rue Albert 1er 54600 Villers-lès-Nancy',
            'geog': Geometry(
                Point(2.16284945854741, 48.7308078326469), 4326),
            'geocoded_geog': Geometry(Point(6.14686, 48.6704), 4326)}

        transformer.input_model.create(**data)

        transformer.transform_load()

        transformed = transformer.output_model.select().dicts()[0]

        expected = {**data, 'centroide_commune': False}

        self.assertEqual(transformed, expected)