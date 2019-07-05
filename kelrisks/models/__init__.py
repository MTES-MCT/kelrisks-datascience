
from .s3ic import S3IC_source, S3IC_geocoded, S3IC_with_geog, \
    S3IC_with_centroide_commune, S3IC_prepared, S3IC

from .sis import SIS_source, SIS_with_geog, SIS_prepared, SIS


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
        'sis': SIS
    }
