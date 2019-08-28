"""
Extract, transform, and load s3ic data
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import helpers

from kelrisks.transformers.s3ic import GeocodeTransformer, \
    CreateGeographyTransformer, CreateCentroideCommuneTransformer, \
    StagingTransformer, DeployTransformer
from kelrisks.embulk import load_s3ic


default_args = helpers.default_args({"start_date": datetime(2019, 6, 11, 5)})


dag = DAG(
    "prepare_s3ic_copy",
    default_args=default_args,
    schedule_interval=None)

load_s3ic = PythonOperator(
    task_id='load_s3ic',
    python_callable=load_s3ic,
    dag=dag)

geocode_transformer = GeocodeTransformer()

geocode = PythonOperator(
    task_id='geocode',
    python_callable=geocode_transformer.transform_load,
    dag=dag)

create_geo_transformer = CreateGeographyTransformer()

create_geo = PythonOperator(
    task_id='create_geo',
    python_callable=create_geo_transformer.transform_load,
    dag=dag)

create_centroide_commune_transformer = CreateCentroideCommuneTransformer()

create_centroide_commune = PythonOperator(
    task_id='create_centroide_commune',
    python_callable=create_centroide_commune_transformer.transform_load,
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


load_s3ic >> geocode >> create_geo >> \
    create_centroide_commune >> stage >> deploy
