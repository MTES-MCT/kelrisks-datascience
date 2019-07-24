
import textwrap
from shapely.geometry import Point


from ..models.base import Geometry
from .base import PostgresPythonTransformer
from .recipes.geocode import geocode_bulk
from .recipes.parcelle import parse_parcelle


class GeocodeTransformer(PostgresPythonTransformer):

    input_table = 'basol_source'
    output_table = 'basol_geocoded'

    def transform(self, data):
        """ apply geocoding to adress information """

        # Input fields
        ADRESSE = self.input_model.adresse.column_name
        CODE_INSEE = self.output_model.code_insee.column_name

        # Output fields
        GEOCODED_LATITUDE = self.output_model.geocoded_latitude.column_name
        GEOCODED_LONGITUDE = self.output_model.geocoded_longitude.column_name
        GEOCODED_SCORE = self.output_model.geocoded_score.column_name
        GEOCODED_PRECISION = self.output_model.geocoded_precision.column_name
        GEOCODED_LABEL = self.output_model.geocoded_label.column_name

        keys = [ADRESSE, CODE_INSEE]
        # trim adresses
        adresses = [
            {
                ADRESSE: record[ADRESSE][:180] if record[ADRESSE] else None,
                CODE_INSEE: record[CODE_INSEE]
            }
            for record in data]

        result = geocode_bulk(
            adresses,
            columns=[ADRESSE, CODE_INSEE],
            citycode=CODE_INSEE)

        zipped = list(zip(data, result))

        geocoded = []

        for (record, geocodage) in zipped:

            r = record
            try:
                r[GEOCODED_LATITUDE] = float(geocodage['latitude'])
                r[GEOCODED_LONGITUDE] = float(geocodage['longitude'])
                r[GEOCODED_SCORE] = float(geocodage['result_score'])
                r[GEOCODED_PRECISION] = geocodage['result_type']
                r[GEOCODED_LABEL] = geocodage['result_label']
            except ValueError:
                # cannot convert string to float
                pass

            geocoded.append(r)

        return geocoded


class CreateGeometryTransformer(PostgresPythonTransformer):

    input_table = 'basol_geocoded'
    output_table = 'basol_with_geog'

    def transform(self, data):
        """ Create geometry field """

        COORDXLAMBERTII = self.input_model.coordxlambertii.column_name
        COORDYLAMBERTII = self.input_model.coordylambertii.column_name
        COORDXLAMBERT93 = self.input_model.coordxlambert93.column_name
        COORDYLAMBERT93 = self.input_model.coordylambert93.column_name
        L2E_PRECISION = self.input_model.l2e_precision.column_name
        L93_PRECISION = self.input_model.l93_precision.column_name
        GEOCODED_LATITUDE = self.input_model.geocoded_latitude.column_name
        GEOCODED_LONGITUDE = self.input_model.geocoded_longitude.column_name

        GEOG = self.output_model.geog.column_name
        PRECISION = self.output_model.precision.column_name
        GEOCODED_GEOG = self.output_model.geocoded_geog.column_name

        for record in data:
            coordxlambertii = record[COORDXLAMBERTII]
            coordylambertii = record[COORDYLAMBERTII]
            coordxlambert93 = record[COORDXLAMBERT93]
            coordylambert93 = record[COORDYLAMBERT93]
            l2e_precision = record[L2E_PRECISION]
            l93_precision = record[L93_PRECISION]
            geocoded_latitude = record[GEOCODED_LATITUDE]
            geocoded_longitude = record[GEOCODED_LONGITUDE]

            if coordxlambertii and coordylambertii:
                point = Point(coordxlambertii, coordylambertii)
                geog = Geometry(point, 27572)
                record[GEOG] = geog
                record[PRECISION] = l2e_precision

            elif coordxlambert93 and coordylambert93:
                point = Point(coordylambert93, coordylambert93)
                geog = Geometry(point, 2154)
                record[GEOG] = geog
                record[PRECISION] = l93_precision

            if geocoded_latitude and geocoded_longitude:
                point = Point(geocoded_longitude, geocoded_latitude)
                geog = Geometry(point, 4326)
                record[GEOCODED_GEOG] = geog

        return data


class NormalizePrecisionTransformer(PostgresPythonTransformer):
    """ Normalize the field precision """

    input_table = 'basol_with_geog'
    output_table = 'basol_precision_normalized'

    def transform(self, data):

        PRECISION = self.input_model.precision.column_name

        # TODO get the meaning of 'autre' from the field 'georeferencement'
        mapping = {
            'Adresse (numéro)': 'housenumber',
            'Adresse (rue)': 'street',
            'Commune (centre)': 'municipality',
            'Autre': 'housenumber'
        }

        for record in data:
            precision = record[PRECISION]
            if precision:
                record[PRECISION] = mapping.get(precision) or precision
        return data


class StagingTransformer(PostgresPythonTransformer):

    input_table = 'basol_precision_normalized'
    output_table = 'basol_prepared'

    def transform(self, data):
        return data


class DeployTransformer(PostgresPythonTransformer):

    input_table = 'basol_prepared'
    output_table = 'basol'

    def transform(self, data):
        return data


class ParcelleTransformer(PostgresPythonTransformer):
    """ Parse the field `cadastre_multi` """

    input_table = 'basol_source'
    output_table = 'basol_parcelle_prepared'

    def transform(self, data):

        CADASTRE_MULTI = self.input_model.cadastre_multi.column_name
        NUMEROBASOL = self.input_model.numerobasol.column_name

        parcelles = []

        for record in data:
            parcelle_multi = record[CADASTRE_MULTI]
            numerobasol = record[NUMEROBASOL]
            if parcelle_multi:
                parsed = parse_parcelle(parcelle_multi)
                partial = {NUMEROBASOL: numerobasol}
                parsed = [{**p._asdict(), **partial} for p in parsed]
                parcelles = [*parcelles, *parsed]

        return parcelles


class DeployParcelleTransformer(PostgresPythonTransformer):

    input_table = 'basol_parcelle_prepared'
    output_table = 'basol_parcelle'

    def transform(self, data):
        return data