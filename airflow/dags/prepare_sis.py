# -*- coding: utf-8 -*-

from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.data_preparation import CopyTableOperator
from airflow.operators.dummy_operator import DummyOperator

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

    geocode = PythonOperator(
        task_id="geocode",
        python_callable=recipes.geocode)

    set_precision = PythonOperator(
        task_id="set_precision",
        python_callable=recipes.set_precision)

    stage = CopyTableOperator(
        task_id="stage",
        source="etl.sis_with_precision",
        destination="etl.sis",
        postgres_conn_id=CONN_ID)

    check = PythonOperator(
        task_id="check",
        python_callable=recipes.check)

    start >> load >> geocode >> set_precision >> stage >> check
