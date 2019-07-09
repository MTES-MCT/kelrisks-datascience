from peewee import *

from .base import BaseModel, BaseProdModel, PointField


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


class VersionMixin(Model):
    """ Add version for compatibility with Hibernate"""

    version = IntegerField(null=True)


class BigIntegerSerialFieldMixin(Model):
    """ Use BigInteger for ids """
    id = BigAutoField(primary_key=True)


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


class S3IC_geocoded(BaseModel, GeocodedFieldsMixin,
                    BaseFieldsMixin):
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


class S3IC_prepared(BaseModel, VersionMixin, CentroideCommuneFieldsMixin,
                    GeocodedFieldsMixin, GeographyFieldsMixin,
                    BaseFieldsMixin):
    """ staging table """

    id = BigAutoField(primary_key=True)


class S3IC(BaseProdModel, S3IC_prepared):
    """ prod table """

    id = BigAutoField(primary_key=True)
