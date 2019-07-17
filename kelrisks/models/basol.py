from peewee import *

from .base import BaseModel, BaseProdModel, GeometryField


class BaseFieldsMixin(Model):
    """ Basic basol fields """
    region = CharField(null=True)
    departement = CharField(null=True)
    num = IntegerField(null=True)
    numerobasol = CharField(null=True)
    numerogidic = CharField(null=True)
    identifiantbasias = CharField(null=True)
    date_publication = DateField(null=True)
    sis = CharField(null=True)
    etat = TextField(null=True)
    georeferencement = TextField(null=True)
    coordxlambertii = DoubleField(null=True)
    coordylambertii = DoubleField(null=True)
    l2e_precision = TextField(null=True)
    coordxlambert93 = DoubleField(null=True)
    coordylambert93 = DoubleField(null=True)
    l93_precision = TextField(null=True)
    cadastre_multi = TextField(null=True)
    adresse = TextField(null=True)
    lieu_dit = TextField(null=True)
    commune = CharField(null=True)
    code_insee = CharField(null=True)
    arrondissement = CharField(null=True)
    proprietaire = TextField(null=True)


class VersionMixin(Model):
    """ Add version for compatibility with Hibernate"""

    version = IntegerField(null=True)


class Basol_source(BaseModel, BaseFieldsMixin):

    prec_coord = IntegerField()

    class Meta:
        primary_key = False


class GeocodedFieldsMixin(Model):

    geocoded_latitude = FloatField(null=True)
    geocoded_longitude = FloatField(null=True)
    geocoded_score = FloatField(null=True)
    geocoded_precision = CharField(null=True)
    geocoded_label = TextField(null=True)


class Basol_geocoded(BaseModel, GeocodedFieldsMixin, BaseFieldsMixin):
    pass


class GeographyFieldsMixin(Model):

    geocoded_geog = GeometryField(4326, null=True)
    geog = GeometryField(4326, null=True)
    precision = TextField(null=True)


class Basol_with_geog(BaseModel, GeographyFieldsMixin,
                      GeocodedFieldsMixin, BaseFieldsMixin):
    pass


class Basol_precision_normalized(BaseModel, GeographyFieldsMixin,
                      GeocodedFieldsMixin, BaseFieldsMixin):
    pass


class Basol_prepared(BaseModel, VersionMixin, GeographyFieldsMixin,
                     GeocodedFieldsMixin, BaseFieldsMixin):

    id = BigAutoField(primary_key=True)


class Basol(BaseProdModel, Basol_prepared):

    id = BigAutoField(primary_key=True)


class Basol_parcelle_prepared(BaseModel, VersionMixin):

    id = BigAutoField(primary_key=True)
    numerobasol = CharField(null=True, index=True)
    commune = CharField(null=True)
    section = CharField(null=True)
    numero = CharField(null=True)


class Basol_parcelle(BaseProdModel, Basol_parcelle_prepared):

    id = BigAutoField(primary_key=True)