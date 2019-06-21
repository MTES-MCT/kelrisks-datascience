
from .recipes.geocode import geocode_bulk
from .base import PostgresPythonTransformer


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

        geocoded = geocode_bulk(
            data,
            columns=[ADRESSE, CODE_INSEE],
            citycode=CODE_INSEE)

        for record in geocoded:
            record[GEOCODED_LATITUDE] = record['latitude']
            record[GEOCODED_LONGITUDE] = record['longitude']
            record[GEOCODED_SCORE] = record['result_score']
            record[GEOCODED_PRECISION] = record['result_type']
            record[GEOCODED_LABEL] = record['result_label']

        return geocoded
