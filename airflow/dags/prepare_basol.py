"""
Extract, transform, and load basol data
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import helpers

from kelrisks.embulk import load_basol
from kelrisks.transformers.basol import CreateGeometryTransformer, \
    GeocodeTransformer, ParcelleTransformer, StagingTransformer, \
    DeployTransformer, DeployParcelleTransformer, NormalizePrecisionTransformer


default_args = helpers.default_args({"start_date": datetime(2019, 6, 11, 5)})


dag = DAG(
    "prepare_basol",
    default_args=default_args,
    schedule_interval=None)


load = PythonOperator(
    task_id='load_basol',
    python_callable=load_basol,
    dag=dag)


geocode_transformer = GeocodeTransformer()

geocode = PythonOperator(
    task_id='geocode',
    python_callable=geocode_transformer.transform_load,
    dag=dag)

create_geometry_transformer = CreateGeometryTransformer()

create_geometry = PythonOperator(
    task_id='create_geometry',
    python_callable=create_geometry_transformer.transform_load,
    dag=dag)

normalize_precision_transformer = NormalizePrecisionTransformer()

normalize_precision = PythonOperator(
    task_id='normalize_precision',
    python_callable=normalize_precision_transformer.transform_load,
    dag=dag)

staging_transformer = StagingTransformer()

stage = PythonOperator(
    task_id='stage',
    python_callable=staging_transformer.transform_load,
    dag=dag)

deploy_transformer = DeployTransformer()

deploy = PythonOperator(
    task_id='deploy',
    python_callable=deploy_transformer.transform_load,
    dag=dag)

parcelle_transformer = ParcelleTransformer()

parse_parcelle = PythonOperator(
    task_id='parse_parcelle',
    python_callable=parcelle_transformer.transform_load,
    dag=dag)

deploy_parcelle_transformer = DeployParcelleTransformer()

deploy_parcelle = PythonOperator(
    task_id='deploy_parcelle',
    python_callable=deploy_parcelle_transformer.transform_load,
    dag=dag)


load >> geocode >> create_geometry >> normalize_precision >> stage >> deploy

load >> parse_parcelle >> deploy_parcelle
