from collections import namedtuple
import json

from peewee import *

from shapely import wkb


from ..connections import get_database


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


Geometry = namedtuple('Geometry', ['geom', 'srid'])


class GeometryField(Field):
    """
    Generic Geometry field that maps to
    shapely geometry types
    """

    def __init__(self, srid, *args, **kwargs):
        self.srid = srid
        super(GeometryField, self).__init__(*args, **kwargs)

    field_type = 'geometry'

    def db_value(self, value):
        if not value:
            return None
        srid = value.srid
        geom = fn.ST_GeomFromEWKB(wkb.dumps(value.geom))
        geom = fn.ST_SetSRID(geom, srid)
        if srid != self.srid:
            geom = fn.ST_Transform(geom, self.srid)
        return geom

    def python_value(self, value):
        if not value:
            return None
        geom = wkb.loads(value, hex=True)
        return Geometry(geom, self.srid)