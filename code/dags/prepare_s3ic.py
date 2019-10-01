# -*- coding: utf-8 -*-

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.data_preparation import (CopyTableOperator,
                                                DownloadUnzipOperator,
                                                Shp2pgsqlOperator)
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

import helpers
import recipes.s3ic_recipes as recipes
from config import CONN_ID, DATA_DIR, DEPARTEMENTS, SQL_DIR
from georisques import get_s3ic_shp_file_url
from constants import DOM_TOM
from operators.geocode_operator import GeocodeOperator
from datasets import Dataset

default_args = helpers.default_args({"start_date": datetime(2019, 6, 11, 5)})


with DAG("prepare_s3ic",
         default_args=default_args,
         template_searchpath=os.path.join(SQL_DIR, "s3ic"),
         schedule_interval=None) as dag:

    start = DummyOperator(task_id="start")

    create_s3ic_source = PostgresOperator(
        task_id="create_s3ic",
        sql="s3ic.sql",
        postgres_conn_id=CONN_ID)

    start_load = []
    end_load = []

    for departement in DEPARTEMENTS:

        if departement not in DOM_TOM:

            url = get_s3ic_shp_file_url(departement)

            dir_path = os.path.join(DATA_DIR, "s3ic", departement)

            temp_table = "\"etl\".\"s3ic_{dep}_temp\"".format(
                dep=departement.lower())

            # dummy task pour exprimer la dépendance
            start_departement = DummyOperator(
                task_id="start_{dep}".format(dep=departement))

            start_load.append(start_departement)

            # Télécharge le fichier shp pour le département
            download = DownloadUnzipOperator(
                task_id="download_{dep}".format(dep=departement),
                url=url,
                dir_path=dir_path)

            shapefile = os.path.join(
                dir_path,
                "InstallationsClassees_France.shp")

            # Charge les données du département dans une table temporaire
            load_temp = Shp2pgsqlOperator(
                task_id="load_{dep}".format(dep=departement),
                shapefile=shapefile,
                table=temp_table,
                connection=CONN_ID)

            # Copie les données du département dans la table principale
            copy_temp = PostgresOperator(
                task_id="copy_{dep}".format(dep=departement),
                sql="copy_temp.sql",
                params={"source": temp_table},
                postgres_conn_id=CONN_ID)

            # Supprime la table temporaire
            delete_temp = PostgresOperator(
                task_id="delete_{dep}".format(dep=departement),
                sql="DROP TABLE IF EXISTS {table}".format(table=temp_table),
                postgres_conn_id=CONN_ID)

            # dummy task pour exprimer la dépendance
            end_departement = DummyOperator(
                task_id="end_{dep}".format(dep=departement))

            end_load.append(end_departement)

            start_departement >> download >> load_temp >> copy_temp >> \
                delete_temp >> end_departement

    start >> create_s3ic_source >> start_load

    # Scrap adresses
    scrap_adresses = PythonOperator(
        task_id="scrap_adresses",
        python_callable=recipes.scrap_adresses)

    end_load >> scrap_adresses

    # Géocode les adresses scrapées
    geocode = GeocodeOperator(
        input_dataset=Dataset("etl", "s3ic_scraped"),
        output_dataset=Dataset("etl", "s3ic_geocoded"),
        columns=["adresse"],
        citycode="cd_insee",
        task_id="geocode")

    scrap_adresses >> geocode

    # Normalisation de la colonne lib_precis
    normalize_precision = PythonOperator(
        task_id="normalize_precision",
        python_callable=recipes.normalize_precision)

    geocode >> normalize_precision

    # Fusionne les informations géographiques
    merge_geog = PythonOperator(
        task_id="merge_geog",
        python_callable=recipes.merge_geog)

    normalize_precision >> merge_geog

    # Remplace les points non "Centroïde commune"
    # par le contour de la parcelle la plus proche
    add_parcelle = PythonOperator(
        task_id="add_parcelle",
        python_callable=recipes.add_parcelle)

    merge_geog >> add_parcelle

    # Remplace les points "Centroïde commune"
    # par un polygone représentant le contour
    # de la commune
    add_commune = PythonOperator(
        task_id="add_commune",
        python_callable=recipes.add_commune)

    add_parcelle >> add_commune

    # Ajoute une colonne version pour des raisons
    # de compatibilité avec le framework Spring
    add_version = PostgresOperator(
        task_id="add_version",
        sql="add_version.sql",
        postgres_conn_id=CONN_ID)

    add_commune >> add_version

    # Copie les données dans la table finale
    stage = CopyTableOperator(
        task_id="stage",
        postgres_conn_id=CONN_ID,
        source="etl.s3ic_with_version",
        destination="etl.s3ic")

    add_version >> stage

    # Crée un index sur la colonne adresse_id
    create_index = PostgresOperator(
        task_id="create_index",
        sql="create_index.sql",
        postgres_conn_id=CONN_ID)

    stage >> create_index

    # Vérifie que la table finale est conforme
    check = PythonOperator(
        task_id="check",
        python_callable=recipes.check)

    create_index >> check
