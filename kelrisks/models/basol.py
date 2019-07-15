from peewee import *

from .base import BaseModel, BaseProdModel, GeometryField


class BaseFieldsMixin(Model):
    """ Basic basol fields """
    "gid" int4 NOT NULL DEFAULT nextval('basol.basol_public_gid_seq'::regclass),
    "sp1_region" varchar(4),
    "sp1_dept" varchar(3),
    "sp1_num" int4,
    "numero_basol" varchar(8),
    "numero_gidic" varchar(20),
    "sp1_idbasias" varchar(12),
    "date_publication" date,
    "sp1_sis" varchar(3),
    "sp2_etat" text,
    "georeferencement" text,
    "sp1_x" numeric,
    "sp1_y" numeric,
    "l2e_precision" text,
    "sp1_x93" numeric,
    "sp1_y93" numeric,
    "second_georef_precision" text,
    "sp1_cadastre_multi" text,
    "sp1_adresse" text,
    "sp1_lieudit" text,
    "nom_commune" varchar(70),
    "sp1_insee" varchar(5),
    "nom_arrondissement" varchar(70),
    "sp1_site" text,
    "prec_coord" int4,
    "geom_wgs84" geometry,
    "geom2_wgs84" geometry
