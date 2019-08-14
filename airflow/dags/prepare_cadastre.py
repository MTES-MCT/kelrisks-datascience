# -*- coding: utf-8 -*-

from airflow import DAG
from datetime import datetime

from airflow.operators.python_operator import PythonOperator

import helpers
import recipes.cadastre_recipes as recipes


default_args = helpers.default_args({"start_date": datetime(2019, 6, 11, 5)})


with DAG("prepare_cadastre",
         default_args=default_args,
         schedule_interval=None) as dag:

    load_cadastre = PythonOperator(
        task_id="load_cadastre",
        python_callable=recipes.load_cadastre)
