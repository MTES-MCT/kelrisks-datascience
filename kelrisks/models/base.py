from collections import namedtuple
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


class GeometryField(Field):

    def __init__(self, *args, **kwargs):
        super(GeometryField, self).__init__(*args, **kwargs)

    field_type = 'geometry'