# -*- coding: utf-8 -*-

from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import helpers
import recipes.sis_recipes as recipes


default_args = helpers.default_args({"start_date": datetime(2019, 6, 11, 5)})


with DAG("prepare_sis",
         default_args=default_args,
         schedule_interval=None) as dag:

    load_sis = PythonOperator(
        task_id="load_sis",
        python_callable=recipes.load_sis)
