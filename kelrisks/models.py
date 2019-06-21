
from collections import namedtuple
from peewee import *

from shapely import wkb

from .connections import get_database


db = get_database()


class BaseModel(Model):

    class Meta:
        database = db

Point = namedtuple('Point', ['x', 'y', 'srid'])


class PointField(Field):

    def __init__(self, srid, *args, **kwargs):
        self.srid = srid
        super(PointField, self).__init__(*args, **kwargs)

    field_type = 'geometry'

    def db_value(self, value):
        if not value:
            return None
        x = value.x
        y = value.y
        srid = value.srid
        point = fn.ST_SetSRID(fn.ST_MakePoint(x, y), srid)
        if srid != self.srid:
            point = fn.ST_Transform(point, self.srid)
        return point

    def python_value(self, value):
        point = wkb.loads(value, hex=True)
        return Point(point.x, point.y, self.srid)


class S3IC_source(BaseModel):
    """ s3ic data from source files """

    code = CharField(null=True)
    nom = CharField(null=True)
    raison_sociale = CharField(null=True)
    etat_activite = CharField(null=True)
    regime = CharField(null=True)
    commune = CharField(null=True)
    code_insee = CharField(null=True)
    code_postal = CharField(null=True)
    adresse = CharField(null=True)
    complement_adresse = CharField(null=True)
    departement = CharField(null=True)
    x = FloatField(null=True)
    y = FloatField(null=True)
    precision = CharField(null=True)

    class Meta:
        primary_key = False
        schema = 'etl'


class S3IC_geocoded(BaseModel):
    """ s3ic with geoodage of addresses when possible """

    code = CharField(null=True)
    nom = CharField(null=True)
    raison_sociale = CharField(null=True)
    etat_activite = CharField(null=True)
    regime = CharField(null=True)
    commune = CharField(null=True)
    code_insee = CharField(null=True)
    code_postal = CharField(null=True)
    adresse = CharField(null=True)
    complement_adresse = CharField(null=True)
    departement = CharField(null=True)
    x = FloatField(null=True)
    y = FloatField(null=True)
    precision = CharField(null=True)
    geocoded_latitude = FloatField(null=True)
    geocoded_longitude = FloatField(null=True)
    geocoded_score = FloatField(null=True)
    geocoded_precision = CharField(null=True)
    geocoded_label = TextField(null=True)

    class Meta:
        schema = 'etl'


class S3IC_with_geog(S3IC_geocoded):
    """ s3ic with geometry fields """
    geog = PointField(4326, null=True)
    geocoded_geog = PointField(4326, null=True)

    class Meta:
        schema = 'etl'


class S3IC_prepared(S3IC_with_geog):
    """ s3ic with some data preparation """
    centroide_commune = BooleanField(null=True)

    class Meta:
        schema = 'etl'


def get_models():
    return {
        's3ic_source': S3IC_source,
        's3ic_geocoded': S3IC_geocoded,
        's3ic_with_geog': S3IC_with_geog,
        's3ic_prepared': S3IC_prepared
    }