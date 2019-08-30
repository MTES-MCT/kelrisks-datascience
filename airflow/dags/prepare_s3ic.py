# -*- coding: utf-8 -*-

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.data_preparation import DownloadUnzipOperator, \
    Shp2pgsqlOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.data_preparation import EmbulkOperator

import helpers
import recipes.s3ic_recipes as recipes
from config import CONN_ID, DATA_DIR


default_args = helpers.default_args({"start_date": datetime(2019, 6, 11, 5)})


with DAG("prepare_s3ic",
         default_args=default_args,
         schedule_interval=None) as dag:

    start = DummyOperator(task_id="start")

    # Download s3ic shapefile
    base_url = "https://kelrisks.fra1.digitaloceanspaces.com/s3ic.zip"

    download = DownloadUnzipOperator(
        task_id="download",
        url="https://kelrisks.fra1.digitaloceanspaces.com/s3ic.zip",
        dir_path=DATA_DIR)

    # Load s3ic data from shapefile (France Entière)
    s3ic_shapefile = os.path.join(DATA_DIR, 's3ic', 'ICPE_4326.shp')
    load_shp = Shp2pgsqlOperator(
        task_id="load_shp",
        shapefile=s3ic_shapefile,
        table="etl.s3ic_source",
        connection=CONN_ID)

    # Load s3ic for Ile-de-France
    load_idf = EmbulkOperator(
        task_id="load_idf",
        embulk_config="s3ic.yml.liquid")

    # Create geography field from x, y
    create_geog_idf = PythonOperator(
        task_id="create_geog_idf",
        python_callable=recipes.create_geog_idf)

    # Filter out IDF from s3ic_source
    filter_idf = PythonOperator(
        task_id="filter_idf",
        python_callable=recipes.filter_idf)

    # Stack data France Entière and IDF
    stack = PythonOperator(
        task_id="stack",
        python_callable=recipes.stack)

    # Scrap adresses
    scrap_adresses = PythonOperator(
        task_id="scrap_adresses",
        python_callable=recipes.scrap_adresses)

    # Geocode adresses
    geocode = PythonOperator(
        task_id="geocode",
        python_callable=recipes.geocode)
