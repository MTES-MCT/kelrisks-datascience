# -*- coding: utf-8 -*-

from datetime import datetime
import textwrap

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.data_preparation import CopyTableOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

import helpers
import recipes.sis_recipes as recipes
from config import CONN_ID


default_args = helpers.default_args({"start_date": datetime(2019, 6, 11, 5)})


with DAG("prepare_sis",
         default_args=default_args,
         schedule_interval=None) as dag:

    start = DummyOperator(task_id="start")

    load = PythonOperator(
        task_id="load",
        python_callable=recipes.load_sis)

    filter_departements = PythonOperator(
        task_id="filter_departements",
        python_callable=recipes.filter_departements)

    geocode = PythonOperator(
        task_id="geocode",
        python_callable=recipes.geocode)

    set_precision = PythonOperator(
        task_id="set_precision",
        python_callable=recipes.set_precision)

    add_version = PythonOperator(
        task_id="add_version",
        python_callable=recipes.add_version)

    create_address_id_index = PostgresOperator(
        task_id="create_address_id_index",
        postgres_conn_id=CONN_ID,
        sql=textwrap.dedent("""
            CREATE INDEX sis_adresse_id_idx
            ON etl.sis_with_version (adresse_id)"""))

    stage = CopyTableOperator(
        task_id="stage",
        source="etl.sis_with_version",
        destination="etl.sis",
        postgres_conn_id=CONN_ID)

    check = PythonOperator(
        task_id="check",
        python_callable=recipes.check)

    start >> load >> filter_departements >> \
        geocode >> set_precision >> add_version >> create_address_id_index \
        >> stage >> check
