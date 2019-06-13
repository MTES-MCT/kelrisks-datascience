"""
Extract, transform, and load s3ic data
"""
from datetime import datetime

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

import helpers
import hooks

from kelrisks.transformers import Geocode, AddGeography, Prepare


default_args = helpers.default_args({"start_date": datetime(2019, 6, 11, 5)})

peewee_database = hooks.get_peewee_database('postgres_etl')


dag = DAG(
    "prepare_s3ic",
    default_args=default_args,
    schedule_interval=None)


def embulk_import(dag, table):
    return helpers.embulk_run(
        dag,
        table,
        {'EMBULK_FILEPREFIX': helpers.data_path(in_fileprefix)})

in_fileprefix = 's3ic/csv'

load = embulk_import(dag, 's3ic')

geocode_transformer = Geocode(peewee_database)

geocode = PythonOperator(
    task_id='geocode',
    python_callable=geocode_transformer.transform_load,
    dag=dag)

add_geography_transformer = AddGeography(peewee_database)

add_geography = PythonOperator(
    task_id='add_geography',
    python_callable=add_geography_transformer.transform_load,
    dag=dag)

prepare_transformer = Prepare(peewee_database)

prepare = PythonOperator(
    task_id='prepare',
    python_callable=prepare_transformer.transform_load,
    dag=dag)

load >> geocode >> add_geography >> prepare
