# -*- coding=utf-8 -*-

import os
from datetime import datetime

from jinja2 import Environment, FileSystemLoader
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import func, or_, and_, not_

from datasets import Dataset
import helpers


def run_report():

    env = Environment(
        loader=FileSystemLoader('templates'))

    template = env.get_template('qualite.md')

    sis = Dataset("etl", "sis")
    SIS = sis.reflect()
    session = sis.get_session()
    sis_count = session.query(SIS).count()
    session.close()

    basol = Dataset("etl", "basol")
    Basol = basol.reflect()
    session = basol.get_session()
    basol_count = session.query(Basol).count()
    session.close()

    basias = Dataset("etl", "basias")
    Basias = basias.reflect()
    session = basias.get_session()
    basias_count = session.query(Basias).count()
    session.close()

    s3ic = Dataset("etl", "s3ic")
    S3ic = s3ic.reflect()
    session = s3ic.get_session()
    s3ic_count = session.query(S3ic).count()
    session.close()

    s3ic_idf = Dataset("etl", "s3ic_idf_source")
    S3icIdf = s3ic_idf.reflect(primary_key="code")
    session = s3ic_idf.get_session()
    s3ic_idf_count = session.query(S3icIdf).count()
    s3ic_idf_precision_parcel_count = session \
        .query(S3icIdf) \
        .filter(
            S3icIdf.x.isnot(None),
            S3icIdf.y.isnot(None),
            S3icIdf.precision == "Centroïde Commune") \
        .count()
    s3ic_idf_precision_commune_count = s3ic_idf_count - \
        s3ic_idf_precision_parcel_count

    session.close()

    s3ic_france = Dataset("etl", "s3ic_without_idf")
    S3icFrance = s3ic_france.reflect()
    session = s3ic_france.get_session()
    s3ic_france_count = session.query(S3icFrance).count()
    s3ic_france_by_precision = dict(session.query(
            S3icFrance.lib_precis,
            func.count(S3icFrance.lib_precis))
        .group_by(S3icFrance.lib_precis)
        .all())

    s3ic_france_parcel_count = s3ic_france_by_precision['Coordonnées précises'] \
        + s3ic_france_by_precision['Valeur Initiale']
    s3ic_france_housenumber_count = s3ic_france_by_precision['Adresse postale']
    s3ic_france_municipality_count = s3ic_france_by_precision['Centroïde Commune'] \
        + s3ic_france_by_precision['Inconnu']

    session.close()

    s3ic_geocoded = Dataset("etl", "s3ic_geocoded")
    S3icGeocoded = s3ic_geocoded.reflect()
    session = s3ic_geocoded.get_session()

    s3ic_geocoded_idf_by_precision = session.query(
            S3icGeocoded.geocoded_result_type,
            func.count(S3icGeocoded.geocoded_result_type)) \
        .filter(or_(
            S3icGeocoded.code_insee.like("75%"),
            S3icGeocoded.code_insee.like("77%"),
            S3icGeocoded.code_insee.like("78%"),
            S3icGeocoded.code_insee.like("91%"),
            S3icGeocoded.code_insee.like("92%"),
            S3icGeocoded.code_insee.like("93%"),
            S3icGeocoded.code_insee.like("94%"),
            S3icGeocoded.code_insee.like("95%"))) \
        .filter(S3icGeocoded.geocoded_result_score > 0.6) \
        .group_by(S3icGeocoded.geocoded_result_type) \
        .all()

    s3ic_geocoded_idf_by_precision = dict(s3ic_geocoded_idf_by_precision)

    s3ic_geocoded_idf_housenumber_count = s3ic_geocoded_idf_by_precision.get("housenumber") or 0
    s3ic_geocoded_idf_street_count = s3ic_geocoded_idf_by_precision.get("street") or 0
    s3ic_geocoded_idf_municipality_count = s3ic_idf_count - \
        (s3ic_geocoded_idf_housenumber_count + s3ic_geocoded_idf_street_count)

    s3ic_geocoded_hors_idf_by_precision = session.query(
            S3icGeocoded.geocoded_result_type,
            func.count(S3icGeocoded.geocoded_result_type)) \
        .filter(and_(
            not_(S3icGeocoded.code_insee.like("75%")),
            not_(S3icGeocoded.code_insee.like("77%")),
            not_(S3icGeocoded.code_insee.like("78%")),
            not_(S3icGeocoded.code_insee.like("91%")),
            not_(S3icGeocoded.code_insee.like("92%")),
            not_(S3icGeocoded.code_insee.like("93%")),
            not_(S3icGeocoded.code_insee.like("94%")),
            not_(S3icGeocoded.code_insee.like("95%")),
        )) \
        .filter(S3icGeocoded.geocoded_result_score > 0.6) \
        .group_by(S3icGeocoded.geocoded_result_type) \
        .all()

    s3ic_geocoded_hors_idf_by_precision = dict(s3ic_geocoded_hors_idf_by_precision)

    s3ic_geocoded_hors_idf_housenumber_count = s3ic_geocoded_hors_idf_by_precision.get("housenumber") or 0
    s3ic_geocoded_hors_idf_street_count = s3ic_geocoded_hors_idf_by_precision.get("street") or 0
    s3ic_geocoded_hors_idf_municipality_count = s3ic_france_count - \
        (s3ic_geocoded_hors_idf_housenumber_count + s3ic_geocoded_hors_idf_street_count)
    session.close()

    s3ic_initial_precision = (s3ic_idf_precision_parcel_count
                              + s3ic_france_housenumber_count
                              + s3ic_france_parcel_count) * 100.0 / s3ic_count

    s3ic = Dataset("etl", "s3ic")
    S3ic = s3ic.reflect()
    session = s3ic.get_session()

    s3ic_by_geog_precision = dict(session.query(
            S3ic.geog_precision,
            func.count(S3ic.geog_precision))
        .group_by(S3ic.geog_precision)
        .all())

    session.close()

    s3ic_final_precision = (s3ic_by_geog_precision["parcel"]
                            + s3ic_by_geog_precision["housenumber"]) \
        * 100.0 / s3ic_count

    # s3ic_centroide_commune_count = \
    #     S3IC \
    #     .select() \
    #     .where(
    #         (S3IC.geog.is_null()) |
    #         (S3IC.centroide_commune)) \
    #     .count()

    session.close()

    variables = {
        's3ic_count': s3ic_count,
        's3ic_idf_count': s3ic_idf_count,
        's3ic_france_count': s3ic_france_count,
        's3ic_idf_precision_parcel_count':
            s3ic_idf_precision_parcel_count,
        's3ic_idf_precision_commune_count': s3ic_idf_precision_commune_count,
        's3ic_france_parcel_count': s3ic_france_parcel_count,
        's3ic_france_housenumber_count': s3ic_france_housenumber_count,
        's3ic_france_municipality_count': s3ic_france_municipality_count,
        's3ic_geocoded_idf_housenumber_count': s3ic_geocoded_idf_housenumber_count,
        's3ic_geocoded_idf_street_count': s3ic_geocoded_idf_street_count,
        's3ic_geocoded_idf_municipality_count': s3ic_geocoded_idf_municipality_count,
        's3ic_geocoded_hors_idf_housenumber_count': s3ic_geocoded_hors_idf_housenumber_count,
        's3ic_geocoded_hors_idf_street_count': s3ic_geocoded_hors_idf_street_count,
        's3ic_geocoded_hors_idf_municipality_count': s3ic_geocoded_hors_idf_municipality_count,
        's3ic_initial_precision': s3ic_initial_precision,
        's3ic_final_precision': s3ic_final_precision,
        's3ic_precision_after_geocodage': 100,
        'basol_count': basol_count,
        'basol_parcelle_count': 100,
        'basol_housenumber_count': 100,
        'basol_street_count': 100,
        'basol_municipality_count': 100,
        'basol_geocoded_housenumber_count': 100,
        'basol_geocoded_street_count': 100,
        'basol_geocoded_locality_count': 100,
        'basol_geocoded_municipality_count': 100,
        'basol_initial_precision': 100,
        'basol_precision_after_parcelle': 100,
        'basol_precision_after_geocodage': 100,
        'sis_count': sis_count,
        'basias_count': basias_count
    }

    dir_path = os.path.dirname(os.path.realpath(__file__))
    report_path = os.path.join(dir_path, "qualite.md")

    with open(report_path, 'w') as f:
        rendered = template.render(**variables)
        f.write(rendered)


default_args = helpers.default_args({"start_date": datetime(2019, 6, 11, 5)})


with DAG("qualite",
         default_args=default_args,
         schedule_interval=None) as dag:

    quality = PythonOperator(
        task_id="run_report",
        python_callable=run_report)