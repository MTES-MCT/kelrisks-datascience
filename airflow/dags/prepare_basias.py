# -*- coding: utf-8 -*-

from datetime import datetime
import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.data_preparation import EmbulkOperator, \
    DownloadUnzipOperator, CopyTableOperator

import helpers
import recipes.basias_recipes as recipes
from config import SQL_DIR, ROOT_DIR, CONN_ID


default_args = helpers.default_args({"start_date": datetime(2019, 6, 11, 5)})


with DAG("prepare_basias",
         default_args=default_args,
         schedule_interval=None,
         template_searchpath=SQL_DIR) as dag:

    start = DummyOperator(task_id="start")

    # Download basias csv files to the data directory
    download = DownloadUnzipOperator(
        task_id="download",
        url="https://kelrisks.fra1.digitaloceanspaces.com/basias.zip",
        dir_path=os.path.join(ROOT_DIR, "data"))

    # Load basias sites into PostgreSQL
    load_sites = EmbulkOperator(
        task_id="load_sites",
        embulk_config="basias_sites.yml.liquid")

    # Load basias geolocalisation into PostgreSQL
    load_localisation = EmbulkOperator(
        task_id="load_localisation",
        embulk_config="basias_localisation.yml.liquid")

    # Load basias cadastre into PostgreSQL
    load_cadastre = EmbulkOperator(
        task_id="load_cadastre",
        embulk_config="basias_cadastre.yml.liquid")

    parse_cadastre = PythonOperator(
        task_id="parse_cadastre",
        python_callable=recipes.parse_cadastre)

    add_geog = PythonOperator(
        task_id="add_geog",
        python_callable=recipes.add_geog)

    merge_cadastre_geog = PythonOperator(
        task_id="merge_cadastre_geog",
        python_callable=recipes.merge_cadastre_geog)

    prepare_sites = PythonOperator(
        task_id="prepare_sites",
        python_callable=recipes.prepare_sites)

    geocode = PythonOperator(
        task_id="geocode",
        python_callable=recipes.geocode)

    merge_geog = PythonOperator(
        task_id="merge_geog",
        python_callable=recipes.merge_geog)

    intersect = PythonOperator(
        task_id="intersect",
        python_callable=recipes.intersect)

    join_localisation_cadastre = PythonOperator(
        task_id="join_localisation_cadastre",
        python_callable=recipes.join_localisation_cadastre)

    join_sites_localisation = PythonOperator(
        task_id="join_sites_localisation",
        python_callable=recipes.join_sites_localisation)

    add_commune = PythonOperator(
        task_id="add_commune",
        python_callable=recipes.add_commune)

    add_version = PythonOperator(
        task_id="add_version",
        python_callable=recipes.add_version)

    stage = CopyTableOperator(
        task_id="stage",
        postgres_conn_id=CONN_ID,
        source="etl.basias_sites_with_version",
        destination="etl.basias")

    check = PythonOperator(
        task_id="check",
        python_callable=recipes.check)

    start >> download >> [
        load_sites,
        load_localisation,
        load_cadastre]

    load_sites >> prepare_sites

    load_localisation >> geocode >> merge_geog >> intersect

    load_cadastre >> parse_cadastre >> add_geog >> merge_cadastre_geog

    [intersect, merge_cadastre_geog] >> join_localisation_cadastre

    [join_localisation_cadastre, prepare_sites] >> join_sites_localisation \
        >> add_commune >> add_version >> stage >> check