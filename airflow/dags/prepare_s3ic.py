"""
Extract, transform, and load s3ic data
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import helpers

from kelrisks.transformers.s3ic import GeocodeTransformer
from kelrisks.embulk import load_s3ic


default_args = helpers.default_args({"start_date": datetime(2019, 6, 11, 5)})


dag = DAG(
    "prepare_s3ic",
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

# add_geography_transformer = AddGeography(peewee_database)

# add_geography = PythonOperator(
#     task_id='add_geography',
#     python_callable=add_geography_transformer.transform_load,
#     dag=dag)

# prepare_transformer = Prepare(peewee_database)

# prepare = PythonOperator(
#     task_id='prepare',
#     python_callable=prepare_transformer.transform_load,
#     dag=dag)

# load >> geocode >>
#add_geography >> prepare

load_s3ic >> geocode