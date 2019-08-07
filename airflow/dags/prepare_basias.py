# -*- coding: utf-8 -*-

from datetime import datetime

from sqlalchemy import BigInteger, Column
from geoalchemy2 import Geometry

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.data_preparation import EmbulkOperator, \
    DownloadUnzipOperator

import helpers
from python_recipes import prepare_basias_sites_recipe, \
    join_basias_sites_localisation_recipe, create_basias_geopoint
from config import CONN_ID, SQL_DIR
from datasets import Dataset


default_args = helpers.default_args({"start_date": datetime(2019, 6, 11, 5)})


with DAG("prepare_basias",
         default_args=default_args,
         schedule_interval=None,
         template_searchpath=SQL_DIR) as dag:

        # Download basias csv files to the data directory
        download_basias = DownloadUnzipOperator(
            task_id="download_basias",
            url="https://kelrisks.fra1.digitaloceanspaces.com/basias.zip")

        # Load basias sites into PostgreSQL
        load_basias_sites = EmbulkOperator(
            task_id="load_basias_sites",
            embulk_config="basias_sites.yml.liquid")

        # Load basias geolocalisation into PostgreSQL
        load_basias_localisation = EmbulkOperator(
            task_id="load_basias_localisation",
            embulk_config="basias_localisation.yml.liquid")

        # Load basias cadastre into PostgreSQL
        load_basias_cadastre = EmbulkOperator(
            task_id="load_basias_cadastre",
            embulk_config="basias_cadastre.yml.liquid")

        basias_sites_source = Dataset("etl", "basias_sites_source")
        basias_sites_prepared = Dataset("etl", "basias_sites_prepared")

        prepare_basias_sites = PythonOperator(
            task_id="prepare_basias_sites",
            python_callable=prepare_basias_sites_recipe)

        join_basias_sites_localisation = PythonOperator(
            task_id="join_basias_sites_localisation",
            python_callable=join_basias_sites_localisation_recipe)

        create_basias_geopoint = PythonOperator(
            task_id="create_basias_geopoint",
            python_callable=create_basias_geopoint)

        # basias_with_geom_prepared = Dataset("etl", "basias_with_geom_prepared")

        # copy_geog = PrepareDatasetOperator(
        #     task_id="copy_geog",
        #     input_dataset=basias_with_geom,
        #     output_dataset=basias_with_geom_prepared,
        #     processors=[(identity, {})])