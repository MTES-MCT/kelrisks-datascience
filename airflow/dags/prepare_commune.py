# -*- coding: utf-8 -*-

from datetime import datetime
import textwrap

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.data_preparation import DownloadUnzipOperator

import helpers
import recipes.commune_recipes as recipes
from config import CONN_ID, DATA_DIR


default_args = helpers.default_args({"start_date": datetime(2019, 6, 11, 5)})


with DAG("prepare_commune",
         default_args=default_args,
         schedule_interval=None) as dag:

    download = DownloadUnzipOperator(
        task_id="download",
        url="https://www.data.gouv.fr/fr/datasets/r/07b7c9a2-d1e2-4da6-9f20-01a7b72d4b12",
        dir_path="{data_dir}/communes".format(data_dir=DATA_DIR))

    load = PythonOperator(
        task_id="load",
        python_callable=recipes.load_communes)

    create_index = PostgresOperator(
        task_id="create_index",
        sql=textwrap.dedent("""
            CREATE INDEX communes_insee_idx
            ON etl.communes (insee)"""),
        postgres_conn_id=CONN_ID)