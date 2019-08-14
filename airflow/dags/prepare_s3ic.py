# -*- coding: utf-8 -*-

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.data_preparation import DownloadUnzipOperator, \
    Shp2pgsqlOperator
from airflow.operators.dummy_operator import DummyOperator

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

    download_shp = DownloadUnzipOperator(
        task_id="download_shp",
        url="https://kelrisks.fra1.digitaloceanspaces.com/s3ic.zip",
        dir_path=DATA_DIR)

    # Load s3ic data
    s3ic_shapefile = os.path.join(DATA_DIR, 's3ic', 'ICPE_4326.shp')
    load_shp = Shp2pgsqlOperator(
        task_id="load_shp",
        shapefile=s3ic_shapefile,
        table="etl.s3ic_source",
        connection=CONN_ID)

    # Geocode adresses
    geocode = PythonOperator(
        task_id="geocode",
        python_callable=recipes.geocode)
