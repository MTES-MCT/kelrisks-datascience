
from .recipes.geocode import geocode_bulk
from .base import PostgresPythonTransformer, PostgresSQLTransformer
from ..models.base import Point


class GeocodeTransformer(PostgresPythonTransformer):

    input_table = 's3ic_source'
    output_table = 's3ic_geocoded'

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


class CreateGeographyTransformer(PostgresPythonTransformer):

    input_table = 's3ic_geocoded'
    output_table = 's3ic_with_geog'

    def transform(self, data):
        """ create geography field """

        # Input fields
        X = self.input_model.x.column_name
        Y = self.input_model.y.column_name
        GEOCODED_LATITUDE = self.input_model.geocoded_latitude.column_name
        GEOCODED_LONGITUDE = self.input_model.geocoded_longitude.column_name

        # Output fields
        GEOG = self.output_model.geog.column_name
        GEOCODED_GEOG = self.output_model.geocoded_geog.column_name

        for record in data:

            # Cf https://epsg.io
            lambert_93 = 2154
            wgs_84 = 4326

            record[GEOG] = Point(record[X], record[Y], lambert_93)
            record[GEOCODED_GEOG] = Point(record[GEOCODED_LONGITUDE],
                                          record[GEOCODED_LATITUDE],
                                          wgs_84)

        return data


class CreateCentroideCommuneTransformer(PostgresPythonTransformer):
    """
    create a column named centroide_commune
    that equals true if precision == 'Centroïde Commune'
    """

    input_table = 's3ic_with_geog'
    output_table = 's3ic_with_centroide_commune'

    def transform(self, data):

        # INPUT FIELD
        PRECISION = self.input_model.precision.column_name

        # OUTPUT FIELD
        CENTROIDE_COMMUNE = self.output_model.centroide_commune.column_name

        for record in data:
            precision = record[PRECISION]
            if precision == 'Centroïde Commune':
                record[CENTROIDE_COMMUNE] = True
            else:
                record[CENTROIDE_COMMUNE] = False

        return data


class StagingTransformer(PostgresPythonTransformer):
    """
    Dummy transformer that copy data to the staging table
    TODO run some tests on the table
    """

    input_table = 's3ic_with_centroide_commune'
    output_table = 's3ic_prepared'

    def transform(self, data):
        return data


class DeployTransformer(PostgresPythonTransformer):
    """
    Dummy transformer that copy data from the staging table
    to the prod table in schema kelrisks
    """

    input_table = 's3ic_prepared'
    output_table = 's3ic'

    def transform(self, data):
        return data
