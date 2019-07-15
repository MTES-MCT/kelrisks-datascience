
import json
import geojson
from shapely.geometry import Point, shape

from .base import PostgresPythonTransformer, PostgresSQLTransformer
from ..models.base import Geometry


class CreateGeographyTransformer(PostgresPythonTransformer):

    input_table = 'sis_source'
    output_table = 'sis_with_geog'

    def transform(self, data):
        """ create geometry fields """

        X = self.input_model.x.column_name
        Y = self.input_model.y.column_name
        GEOM = self.input_model.geom.column_name

        GEOG = self.output_model.geog.column_name
        GEOG_CENTROID = self.output_model.geog_centroid.column_name

        for record in data:
            x = record[X]
            y = record[Y]

            record[GEOG_CENTROID] = Geometry(Point(x, y), 4326)

            # convert from geojson to shapely.Geometry
            g = eval(record[GEOM])
            g = json.dumps(g)
            g = geojson.loads(g)
            g = shape(g)
            record[GEOG] = Geometry(g, 4326)

        return data


class StagingTransformer(PostgresPythonTransformer):
    """
    Dummy transformer that copy data to the staging table
    TODO run some tests on the table
    """

    input_table = 'sis_with_geog'
    output_table = 'sis_prepared'

    def transform(self, data):
        return data


class DeployTransformer(PostgresPythonTransformer):
    """
    Dummy transformer that copy data from the staging table
    to the prod table in schema kelrisks
    """

    input_table = 'sis_prepared'
    output_table = 'sis'

    def transform(self, data):
        return data
