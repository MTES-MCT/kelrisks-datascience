
from peewee import *


class PointField(Field):

    def __init__(self, epsg, *args, **kwargs):
        self.epsg = epsg
        super(PointField, self).__init__(*args, **kwargs)

    field_type = 'geometry'

    def db_value(self, value):
        try:
            x = value[0]
            y = value[1]
            return fn.ST_SetSRID(fn.ST_MakePoint(x, y), self.epsg)
        except:
            return None

    def python_value(self, value):
        return (fn.ST_X(value), fn.ST_Y(value))


class S3IC_source(Model):
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


class S3IC_geocoded(S3IC_source):
    """ s3ic with geoodage of addresses when possible """

    geocoded_x = FloatField(null=True)
    geocoded_y = FloatField(null=True)
    geocoded_score = FloatField(null=True)

    class Meta:
        schema = 'etl'


class S3IC_with_geog(S3IC_geocoded):
    """ s3ic with geometry fields """
    geog = PointField(2154, null=True)
    geocoded_geog = PointField(2154, null=True)


class S3IC_prepared(S3IC_with_geog):
    """ s3ic with some data preparation """
    centroide_commune = BooleanField(null=True)
