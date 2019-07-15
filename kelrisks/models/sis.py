from peewee import *

from .base import BaseModel, BaseProdModel, GeometryField


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


class VersionMixin(Model):
    """ Add version for compatibility with Hibernate"""

    version = IntegerField(null=True)


class SIS_source(BaseModel, BaseFieldMixin):

    geom = TextField()

    class Meta:
        primary_key = False


class GeographyMixin(Model):

    geog = GeometryField(4326, null=True)
    geog_centroid = GeometryField(4326, null=True)


class SIS_with_geog(BaseModel, GeographyMixin,
                    BaseFieldMixin):
    pass


class SIS_prepared(BaseModel, VersionMixin, GeographyMixin,
                   BaseFieldMixin):

    id = BigAutoField(primary_key=True)


class SIS(BaseProdModel, SIS_prepared):

    id = BigAutoField(primary_key=True)
