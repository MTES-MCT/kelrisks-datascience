# -*- coding: utf-8 -*-

from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.data_preparation import DownloadUnzipOperator, \
    EmbulkOperator
from airflow.operators.dummy_operator import DummyOperator

import helpers
import recipes.basol_recipes as recipes
from config import DATA_DIR

default_args = helpers.default_args({"start_date": datetime(2019, 6, 11, 5)})


with DAG("prepare_basol",
         default_args=default_args,
         schedule_interval=None) as dag:

    start = DummyOperator(task_id="start")

    download = DownloadUnzipOperator(
        task_id="download",
        url="https://kelrisks.fra1.digitaloceanspaces.com/basol.zip",
        dir_path=DATA_DIR)

    load = EmbulkOperator(
        task_id="load",
        embulk_config="basol.yml.liquid")

    parse_cadastre = PythonOperator(
        task_id="parse_cadastre",
        python_callable=recipes.parse_cadastre)

    join_cadastre = PythonOperator(
        task_id="join_cadastre",
        python_callable=recipes.join_cadastre)

    merge_cadastre = PythonOperator(
        task_id="merge_cadastre",
        python_callable=recipes.merge_cadastre)

    geocode = PythonOperator(
        task_id="geocode",
        python_callable=recipes.geocode)

    normalize_precision = PythonOperator(
        task_id="normalize_precision",
        python_callable=recipes.normalize_precision)

    add_parcels = PythonOperator(
        task_id="add_parcels",
        python_callable=recipes.add_parcels)

    merge_geog = PythonOperator(
        task_id="merge_geog",
        python_callable=recipes.merge_geog)
