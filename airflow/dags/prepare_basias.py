# -*- coding: utf-8 -*-

from datetime import datetime
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.data_preparation import EmbulkOperator, \
    DownloadUnzipOperator

import helpers
import recipes.basias_recipes as recipes
from config import SQL_DIR, ROOT_DIR


default_args = helpers.default_args({"start_date": datetime(2019, 6, 11, 5)})


with DAG("prepare_basias",
         default_args=default_args,
         schedule_interval=None,
         template_searchpath=SQL_DIR) as dag:

    # Download basias csv files to the data directory
    download_basias = DownloadUnzipOperator(
        task_id="download_basias",
        url="https://kelrisks.fra1.digitaloceanspaces.com/basias.zip",
        dir_path=os.path.join(ROOT_DIR, "data"))

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

    prepare_basias_sites = PythonOperator(
        task_id="prepare_basias_sites",
        python_callable=recipes.prepare_basias_sites)

    geocode_basias_adresses = PythonOperator(
        task_id="geocode_basias_adresses",
        python_callable=recipes.geocode_basias_adresses)

    # join_basias_sites_localisation = PythonOperator(
    #     task_id="join_basias_sites_localisation",
    #     python_callable=recipes.join_basias_sites_localisation)

    extract_basias_parcelles = PythonOperator(
        task_id="extract_basias_parcelles",
        python_callable=recipes.extract_basias_parcelles)

    # create_basias_geopoint = PythonOperator(
    #     task_id="create_basias_geopoint",
    #     python_callable=recipes.create_basias_geopoint)

    download_basias >> [
        load_basias_sites,
        load_basias_localisation,
        load_basias_cadastre]

    load_basias_sites >> prepare_basias_sites

    load_basias_localisation >> [
        extract_basias_parcelles,
        geocode_basias_adresses]