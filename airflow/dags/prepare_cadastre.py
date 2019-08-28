# -*- coding: utf-8 -*-

from datetime import datetime
import textwrap

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator

import helpers
import recipes.cadastre_recipes as recipes
from config import CONN_ID


default_args = helpers.default_args({"start_date": datetime(2019, 6, 11, 5)})


with DAG("prepare_cadastre",
         default_args=default_args,
         schedule_interval=None) as dag:

    load_cadastre = PythonOperator(
        task_id="load",
        python_callable=recipes.load_cadastre)

    create_index = PostgresOperator(
        task_id="create_index",
        sql=textwrap.dedent("""
            CREATE INDEX cadastre_commune_prefixe_section_numero_idx
            ON etl.cadastre USING brin (commune, prefixe, section, numero)"""),
        postgres_conn_id=CONN_ID)