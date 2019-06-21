
from unittest import mock
from ...tests.helpers import BaseTestCase

from ..s3ic import GeocodeTransformer


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

        self.assertEqual(transformed, expected)
