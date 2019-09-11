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
    session.close()

    basias_localisation_source = Dataset("etl", "basias_localisation_source")
    BasiasLocalisationSource = basias_localisation_source.reflect(
        primary_key="indice_departemental")
    session = basias_localisation_source.get_session()
    basias_by_precision = dict(session.query(
            BasiasLocalisationSource.precision_adresse,
            func.count(BasiasLocalisationSource.precision_adresse))
        .group_by(BasiasLocalisationSource.precision_adresse)
        .all())

    basias_parcel_count = 0
    basias_housenumber_count = basias_by_precision.get("numéro") or 0
    basias_street_count = basias_by_precision.get("rue") or 0
    basias_municipality_count = basias_count - basias_housenumber_count - basias_street_count


    basias_geocoded = Dataset("etl", "basias_localisation_geocoded")
    BasiasGeocoded = basias_geocoded.reflect()
    session = basias_geocoded.get_session()

    basias_geocoded_by_precision = dict(session.query(
            BasiasGeocoded.geocoded_result_type,
            func.count(BasiasGeocoded.geocoded_result_type))
        .filter(BasiasGeocoded.geocoded_result_score > 0.6)
        .group_by(BasiasGeocoded.geocoded_result_type)
        .all())
    session.close()

    basias_geocoded_housenumber_count = basias_geocoded_by_precision.get("housenumber") or 0
    basias_geocoded_street_count = basias_geocoded_by_precision.get("street") or 0
    basias_geocoded_municipality_count = basias_count \
        - basias_geocoded_housenumber_count \
        - basias_geocoded_street_count

    basias_cadastre_merged = Dataset("etl", "basias_cadastre_merged")
    BasiasCadastreMerged = basias_cadastre_merged.reflect()
    session = basias_cadastre_merged.get_session()
    basias_extracted_parcelles_count = session.query(BasiasCadastreMerged).count()
    session.close()

    basias_initial_precision = (basias_parcel_count + basias_housenumber_count) * 100 / basias_count

    basias = Dataset("etl", "basias")
    Basias = basias.reflect()
    session = basias.get_session()
    basias_by_precision = dict(session.query(
            Basias.geog_precision,
            func.count(Basias.geog_precision))
        .group_by(Basias.geog_precision)
        .all())
    session.close()

    basias_final_precision = (
        basias_by_precision["parcel"]
        + basias_by_precision["housenumber"]) * 100 / basias_count

    basol_normalized = Dataset("etl", "basol_normalized")
    BasolNormalized = basol_normalized.reflect()
    session = basol_normalized.get_session()
    basol_by_precision = dict(session.query(
            BasolNormalized.l2e_precision,
            func.count(BasolNormalized.l2e_precision))
        .group_by(BasolNormalized.l2e_precision)
        .all())
    session.close()

    basol_housenumber_count = basol_by_precision.get("housenumber") or 0
    basol_street_count = basol_by_precision.get("street") or 0
    basol_municipality_count = basol_count \
        - basol_housenumber_count \
        - basol_street_count

    basol_geocoded = Dataset("etl", "basol_geocoded")
    BasolGeocoded = basol_geocoded.reflect()
    session = basol_geocoded.get_session()

    basol_geocoded_by_precision = dict(session.query(
            BasolGeocoded.geocoded_result_type,
            func.count(BasolGeocoded.geocoded_result_type))
        .filter(BasolGeocoded.geocoded_result_score > 0.6)
        .group_by(BasolGeocoded.geocoded_result_type)
        .all())
    session.close()

    basol_geocoded_housenumber_count = basol_geocoded_by_precision.get("housenumber") or 0
    basol_geocoded_street_count = basol_geocoded_by_precision.get("street") or 0
    basol_geocoded_municipality_count = basol_count \
        - basol_geocoded_housenumber_count \
        - basol_geocoded_street_count

    basol_cadastre_merged = Dataset("etl", "basol_cadastre_merged")
    BasolCadastreMerged = basol_cadastre_merged.reflect()
    session = basol_cadastre_merged.get_session()
    basol_extracted_parcelles_count = session.query(BasolCadastreMerged).count()
    session.close()

    basol_initial_precision = basol_housenumber_count * 100 / basol_count

    basol = Dataset("etl", "basol")
    Basol = basol.reflect()
    session = basol.get_session()
    basol_by_precision = dict(session.query(
            Basol.geog_precision,
            func.count(Basol.geog_precision))
        .group_by(Basol.geog_precision)
        .all())
    session.close()

    basol_final_precision = (
        basol_by_precision["parcel"]
        + basol_by_precision["housenumber"]) * 100 / basol_count

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
        'basias_count': basias_count,
        'basias_parcel_count': basias_parcel_count,
        'basias_housenumber_count': basias_housenumber_count,
        'basias_street_count': basias_street_count,
        'basias_municipality_count': basias_municipality_count,
        'basias_geocoded_housenumber_count': basias_geocoded_housenumber_count,
        'basias_geocoded_street_count': basias_geocoded_street_count,
        'basias_geocoded_municipality_count': basias_geocoded_municipality_count,
        'basias_extracted_parcelles_count': basias_extracted_parcelles_count,
        'basias_initial_precision': basias_initial_precision,
        'basias_final_precision': basias_final_precision,
        'basol_count': basol_count,
        'basol_housenumber_count': basol_housenumber_count,
        'basol_street_count': basol_street_count,
        'basol_municipality_count': basol_municipality_count,
        'basol_geocoded_housenumber_count': basol_geocoded_housenumber_count,
        'basol_geocoded_street_count': basol_geocoded_street_count,
        'basol_geocoded_municipality_count': basol_geocoded_municipality_count,
        'basol_extracted_parcelles_count': basol_extracted_parcelles_count,
        'basol_initial_precision': basol_initial_precision,
        'basol_final_precision': basol_final_precision,
        'sis_count': sis_count
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