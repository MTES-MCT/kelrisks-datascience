"""
Extract, transform, and load sis data
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import helpers

from kelrisks.embulk import load_sis
from kelrisks.transformers.sis import CreateGeographyTransformer, \
    StagingTransformer, DeployTransformer


default_args = helpers.default_args({"start_date": datetime(2019, 6, 11, 5)})


dag = DAG(
    "prepare_sis",
    default_args=default_args,
    schedule_interval=None)


load_sis = PythonOperator(
    task_id='load_sis',
    python_callable=load_sis,
    dag=dag)


create_geography_transformer = CreateGeographyTransformer()


create_geography = PythonOperator(
    task_id='create_geography',
    python_callable=create_geography_transformer.transform_load,
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


load_sis >> create_geography >> stage >> deploy
