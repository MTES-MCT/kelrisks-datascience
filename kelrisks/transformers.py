"""
Transformers are used to move data from one
or several input sources to one output source
performing some work in between
"""

import requests

from .adresse import geocode

from .models import S3IC_source, S3IC_geocoded, S3IC_with_geog, \
    S3IC_prepared
from .utils import is_float


class PeeweeTransformer(object):
    """
    Base class for applying a transformation
    between two SQL tables using Peewee
    """

    def __init__(self, db, in_model, out_model):
        self.db = db
        self.in_model = in_model
        self.out_model = out_model
        self.models = [self.in_model, self.out_model]
        self.db.bind(self.models)

    def select(self):
        return list(self.in_model.select().dicts())

    def load(self, data, append=False):
        if not append:
            self.out_model.drop_table()
            self.out_model.create_table()
        instances = [self.out_model(**record) for record in data]
        with self.db.atomic():
            self.out_model.bulk_create(instances, batch_size=100)

    def transform(self, data):
        raise NotImplementedError()

    def transform_load(self, append=False):
        data = self.select()
        transformed = self.transform(data)
        self.load(transformed, append=append)


class SQLTransformer(object):

    def __init__(self, db, in_model, out_model):
        self.db = db
        self.in_model = in_model
        self.out_model = out_model
        self.models = [self.in_model, self.out_model]
        self.db.bind(self.models)

    def sql(self):
        raise NotImplementedError()

    def transform_load(self):
        self.out_model.drop_table()
        self.db.execute_sql(self.sql())


class Geocode(PeeweeTransformer):

    def __init__(self, db):
        in_model = S3IC_source
        out_model = S3IC_geocoded
        super(Geocode, self).__init__(db, in_model, out_model)

    def transform(self, data):
        """ apply geocoding to adress information """

        # Input fields
        ADRESSE = self.in_model.adresse.column_name
        CODE_INSEE = self.out_model.code_insee.column_name

        # Output fields
        GEOCODED_X = self.out_model.geocoded_x.column_name
        GEOCODED_Y = self.out_model.geocoded_y.column_name
        GEOCODED_SCORE = self.out_model.geocoded_score.column_name

        with requests.Session() as session:
            for et in data:
                geocodage = geocode(session, et[ADRESSE], et[CODE_INSEE])
                if geocodage:
                    et[GEOCODED_X] = geocodage.x
                    et[GEOCODED_Y] = geocodage.y
                    et[GEOCODED_SCORE] = geocodage.score
            return data


# class AddGeography(PeeweeTransformer):

#     def __init__(self, db):
#         in_model = S3IC_geocoded
#         out_model = S3IC_with_geog
#         super(AddGeography, self).__init__(
#             db, in_model, out_model)

#     def transform(self, data):
#         """ create geography fields from x, y data """

#         # Input fields
#         X = self.in_model.x.column_name
#         Y = self.in_model.y.column_name
#         GEOCODED_X = self.in_model.geocoded_x.column_name
#         GEOCODED_Y = self.in_model.geocoded_y.column_name

#         # Output fields
#         GEOG = self.out_model.geog.column_name
#         GEOCODED_GEOG = self.out_model.geocoded_geog.column_name

#         for record in data:
#             x = record[X]
#             y = record[Y]
#             geocoded_x = record[GEOCODED_X]
#             geocoded_y = record[GEOCODED_Y]
#             if is_float(x) and is_float(y):
#                 record[GEOG] = (x, y)
#             if is_float(geocoded_x) and is_float(geocoded_y):
#                 record[GEOCODED_GEOG] = (geocoded_x, geocoded_y)

#         return data


class AddGeography(SQLTransformer):

    def __init__(self, db):
        in_model = S3IC_geocoded
        out_model = S3IC_with_geog
        super(AddGeography, self).__init__(
            db, in_model, out_model)

    def sql(self):
        query = """
            INSERT INTO %s
                SELECT
                    *,
                    ST_Transform(ST_SetSRID(fn.ST_MakePoint(x, y), 2154), 4326),
                    ST_Transform(ST_SetSRID(fn.ST_MakePoint(geocoded_x, geocoded_y), 2154), 4326)
                FROM %s
            """ % (self.out_model.table_name, self.in_model.table_name)
        return query


class Prepare(PeeweeTransformer):

    def __init__(self, db):
        in_model = S3IC_with_geog
        out_model = S3IC_prepared
        super(Prepare, self).__init__(
            db, in_model, out_model)

    def transform(self, data):

        # INPUT FIELD
        PRECISION = self.in_model.precision.column_name

        # OUTPUT FIELD
        CENTROIDE_COMMUNE = self.out_model.centroide_commune.column_name

        for record in data:
            precision = record[PRECISION]
            if precision == 'Centroïde Commune':
                record[CENTROIDE_COMMUNE] = True
            else:
                record[CENTROIDE_COMMUNE] = False

        return data
