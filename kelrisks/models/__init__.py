
from .s3ic import S3IC_source, S3IC_geocoded, S3IC_with_geog, \
    S3IC_with_centroide_commune, S3IC_prepared, S3IC

from .sis import SIS_source, SIS_with_geog, SIS_prepared, SIS

from .basol import Basol_source, Basol_geocoded, Basol_with_geog, \
    Basol_precision_normalized, Basol_prepared, Basol, \
    Basol_parcelle_prepared, Basol_parcelle


def get_models():
    return {
        's3ic_source': S3IC_source,
        's3ic_geocoded': S3IC_geocoded,
        's3ic_with_geog': S3IC_with_geog,
        's3ic_with_centroide_commune': S3IC_with_centroide_commune,
        's3ic_prepared': S3IC_prepared,
        's3ic': S3IC,
        'sis_source': SIS_source,
        'sis_with_geog': SIS_with_geog,
        'sis_prepared': SIS_prepared,
        'sis': SIS,
        'basol_source': Basol_source,
        'basol_geocoded': Basol_geocoded,
        'basol_with_geog': Basol_with_geog,
        'basol_precision_normalized': Basol_precision_normalized,
        'basol_prepared': Basol_prepared,
        'basol': Basol,
        'basol_parcelle_prepared': Basol_parcelle_prepared,
        'basol_parcelle': Basol_parcelle
    }
