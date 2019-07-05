
from collections import namedtuple
from peewee import *

from shapely import wkb

from .connections import get_database


db = get_database()


class BaseModel(Model):
    """
    Base model for table in etl schema
    """
    class Meta:
        database = db
        schema = 'etl'


class BaseProdModel(Model):
    """
    Base model for table in kelrisks schema
    """
    class Meta:
        database = db
        schema = 'kelrisks'


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
        if not value:
            return None
        point = wkb.loads(value, hex=True)
        return Point(point.x, point.y, self.srid)


class BaseFieldsMixin(Model):
    """ Basic s3ic fields """

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


class S3IC_source(BaseModel, BaseFieldsMixin):
    """ s3ic data from source files """

    class Meta:
        primary_key = False


class GeocodedFieldsMixin(Model):

    geocoded_latitude = FloatField(null=True)
    geocoded_longitude = FloatField(null=True)
    geocoded_score = FloatField(null=True)
    geocoded_precision = CharField(null=True)
    geocoded_label = TextField(null=True)


class S3IC_geocoded(BaseModel, GeocodedFieldsMixin, BaseFieldsMixin):
    """ s3ic with geoodage of addresses when possible """
    pass


class GeographyFieldsMixin(Model):

    geog = PointField(4326, null=True)
    geocoded_geog = PointField(4326, null=True)


class S3IC_with_geog(BaseModel, GeographyFieldsMixin, GeocodedFieldsMixin,
                     BaseFieldsMixin):
    """ s3ic with geometry fields """
    pass


class CentroideCommuneFieldsMixin(Model):

    centroide_commune = BooleanField(null=True)


class S3IC_with_centroide_commune(BaseModel, CentroideCommuneFieldsMixin,
                                  GeocodedFieldsMixin, GeographyFieldsMixin,
                                  BaseFieldsMixin):
    """ s3ic with centroide_commune fields """
    pass


class S3IC_prepared(BaseModel, CentroideCommuneFieldsMixin,
                    GeocodedFieldsMixin, GeographyFieldsMixin,
                    BaseFieldsMixin):
    """ staging table """
    pass


class S3IC(BaseProdModel, CentroideCommuneFieldsMixin, GeocodedFieldsMixin,
           GeographyFieldsMixin, BaseFieldsMixin):
    """ prod table """
    pass


def get_models():
    return {
        's3ic_source': S3IC_source,
        's3ic_geocoded': S3IC_geocoded,
        's3ic_with_geog': S3IC_with_geog,
        's3ic_with_centroide_commune': S3IC_with_centroide_commune,
        's3ic_prepared': S3IC_prepared,
        's3ic': S3IC
    }
