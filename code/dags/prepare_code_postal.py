# -*- coding: utf-8 -*-

"""
Ce DAG permet de charger les données de correspondance
Code Insee / Code postal utilisées par le backend
"""

import textwrap
from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.data_preparation import EmbulkOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

import helpers
import recipes.commune_recipes as recipes
from config import CONN_ID, DATA_DIR

default_args = helpers.default_args({"start_date": datetime(2019, 6, 11, 5)})


with DAG("prepare_code_postal",
         default_args=default_args,
         schedule_interval=None) as dag:

    cmd = "mkdir -p $DIR_PATH & wget \"$URL\" -O $DIR_PATH/code_postal.csv"

    download = BashOperator(
        env={
            "URL": "https://www.data.gouv.fr/fr/datasets/r/554590ab-ae62-40ac-8353-ee75162c05ee",
            "DIR_PATH": "{data_dir}/communes".format(data_dir=DATA_DIR)},
        task_id="download",
        bash_command=cmd)

    load = EmbulkOperator(
        task_id="load",
        embulk_config="code_postal.yml.liquid")

    prepare = PythonOperator(
        task_id="prepare",
        python_callable=recipes.prepare_code_postal)

    create_index = PostgresOperator(
        task_id="create_index",
        sql=textwrap.dedent("""
            CREATE INDEX code_postal_code_insee_idx
            ON etl.code_postal (code_insee)"""),
        postgres_conn_id=CONN_ID)

    download >> load >> prepare >> create_index
