from peewee import *

from .base import BaseModel, BaseProdModel, GeometryField, PointField


class BaseFieldMixin(Model):

    id_sis = CharField(max_length=255, null=True)
    numero_affichage = CharField(max_length=255, null=True)
    numero_basol = CharField(max_length=255, null=True)
    adresse = TextField(null=True)
    lieu_dit = CharField(max_length=255, null=True)
    code_insee = CharField(max_length=255, null=True)
    nom_commune = CharField(max_length=255, null=True)
    code_departement = CharField(max_length=255, null=True)
    nom_departement = CharField(max_length=255, null=True)
    surface_m2 = FloatField(null=True)
    x = DoubleField(null=True)
    y = DoubleField(null=True)


class SIS_source(BaseModel, BaseFieldMixin):

    geom = TextField()


class GeographyMixin(Model):

    geog = GeometryField()
    geog_centroid = PointField(4326)


class SIS_with_geog(BaseModel, GeographyMixin, BaseFieldMixin):
    pass


class SIS_prepared(BaseModel, GeographyMixin, BaseFieldMixin):
    pass


class SIS(BaseProdModel, GeographyMixin, BaseFieldMixin):
    pass
