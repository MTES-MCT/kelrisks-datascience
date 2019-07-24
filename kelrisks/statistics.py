import os
from jinja2 import Environment, PackageLoader, select_autoescape

from .models.s3ic import S3IC
from .models.basol import Basol, Basol_parcelle
from .models.sis import SIS
from .config import ROOT_DIR

env = Environment(
    loader=PackageLoader('kelrisks', 'templates'))


geocoded_score_threshold = 0.6


def quality_report():
    """ Generate a report with some statistics about data enhancement """
    template = env.get_template('qualite.md')

    # SIIIC
    s3ic_count = S3IC.select().count()

    s3ic_accurate_coordinates_count = \
        S3IC \
        .select() \
        .where(
            (S3IC.geog.is_null(False)) &
            (S3IC.centroide_commune == False)) \
        .count()

    s3ic_centroide_commune_count = \
        S3IC \
        .select() \
        .where(
            (S3IC.geog.is_null()) |
            (S3IC.centroide_commune)) \
        .count()

    s3ic_geocoded_housenumber_count = S3IC.select() \
        .where(
            (S3IC.geocoded_precision == 'housenumber') &
            (S3IC.geocoded_score > geocoded_score_threshold)) \
        .count()

    s3ic_geocoded_street_count = S3IC.select() \
        .where(
            (S3IC.geocoded_precision == 'street') &
            (S3IC.geocoded_score > geocoded_score_threshold)) \
        .count()

    s3ic_geocoded_locality_count = S3IC.select() \
        .where(
            (S3IC.geocoded_precision == 'locality') &
            (S3IC.geocoded_score > geocoded_score_threshold)) \
        .count()

    s3ic_geocoded_municipality_count = S3IC.select() \
        .where(
            (S3IC.geocoded_precision == 'municipality') |
            (S3IC.geocoded_precision.is_null()) |
            (S3IC.geocoded_score <= geocoded_score_threshold)) \
        .count()

    cond1 = (S3IC.geog.is_null(False)) & (S3IC.centroide_commune == False)
    cond2 = (S3IC.geocoded_score > 0.6) & \
        (S3IC.geocoded_precision == 'housenumber')

    s3ic_initial_precision = (s3ic_accurate_coordinates_count / s3ic_count) * 100

    s3ic_precision_after_geocodage = (S3IC.select() \
        .where(cond1 | cond2) \
        .count() / s3ic_count) * 100

    # Basol

    basol_count = Basol.select().count()

    basol_parcelle = Basol_parcelle \
        .select(Basol_parcelle.numerobasol) \
        .distinct() \

    basol_parcelle_count = basol_parcelle.count()

    basol_not_parcelle = Basol.select() \
        .where(Basol.numerobasol.not_in(basol_parcelle))

    basol_housenumber_count = basol_not_parcelle \
        .where(Basol.precision == 'housenumber') \
        .count()

    basol_street_count = basol_not_parcelle \
        .where(Basol.precision == 'street') \
        .count()

    basol_municipality_count = basol_not_parcelle \
        .where(
            (Basol.precision == 'municipality') |
            (Basol.precision.is_null())) \
        .count()

    basol_geocoded_housenumber_count = Basol.select() \
        .where(
            (Basol.geocoded_precision == 'housenumber') &
            (Basol.geocoded_score > geocoded_score_threshold)) \
        .count()

    basol_geocoded_street_count = Basol.select() \
        .where(
            (Basol.geocoded_precision == 'street') &
            (Basol.geocoded_score > geocoded_score_threshold)) \
        .count()

    basol_geocoded_locality_count = Basol.select() \
        .where(
            (Basol.geocoded_precision == 'locality') &
            (Basol.geocoded_score > geocoded_score_threshold)) \
        .count()

    basol_geocoded_municipality_count = Basol.select() \
        .where(
            (Basol.geocoded_precision == 'municipality') |
            (Basol.geocoded_precision.is_null()) |
            (Basol.geocoded_score <= geocoded_score_threshold)) \
        .count()

    basol_initial_precision = (Basol.select() \
        .where(Basol.precision == 'housenumber') \
        .count() / basol_count) * 100

    basol_precision_after_parcelle = (Basol.select() \
        .where(
            (Basol.precision == 'housenumber') |
            (Basol.numerobasol.in_(basol_parcelle))) \
        .count() / basol_count) * 100

    cond = (Basol.geocoded_score > 0.6) & \
        (Basol.geocoded_precision == 'housenumber')

    basol_precision_after_geocodage = (Basol.select() \
        .where(
            (Basol.precision == 'housenumber') |
            (Basol.numerobasol.in_(basol_parcelle)) |
            cond) \
        .count() / basol_count) * 100

    # SIS

    sis_count = SIS.select().count()

    variables = {
        's3ic_count': s3ic_count,
        's3ic_accurate_coordinates_count': s3ic_accurate_coordinates_count,
        's3ic_centroide_commune_count': s3ic_centroide_commune_count,
        's3ic_geocoded_housenumber_count': s3ic_geocoded_housenumber_count,
        's3ic_geocoded_street_count': s3ic_geocoded_street_count,
        's3ic_geocoded_locality_count': s3ic_geocoded_locality_count,
        's3ic_geocoded_municipality_count': s3ic_geocoded_municipality_count,
        's3ic_initial_precision': s3ic_initial_precision,
        's3ic_precision_after_geocodage': s3ic_precision_after_geocodage,
        'basol_count': basol_count,
        'basol_parcelle_count': basol_parcelle_count,
        'basol_housenumber_count': basol_housenumber_count,
        'basol_street_count': basol_street_count,
        'basol_municipality_count': basol_municipality_count,
        'basol_geocoded_housenumber_count': basol_geocoded_housenumber_count,
        'basol_geocoded_street_count': basol_geocoded_street_count,
        'basol_geocoded_locality_count': basol_geocoded_locality_count,
        'basol_geocoded_municipality_count': basol_geocoded_municipality_count,
        'basol_initial_precision': basol_initial_precision,
        'basol_precision_after_parcelle': basol_precision_after_parcelle,
        'basol_precision_after_geocodage': basol_precision_after_geocodage,
        'sis_count': sis_count
    }

    report_path = os.path.join(ROOT_DIR, 'qualite.md')
    with open(report_path, 'w') as f:
        rendered = template.render(**variables)
        f.write(rendered)