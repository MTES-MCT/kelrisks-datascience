# -*- coding: utf-8 -*-

from datetime import datetime
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.data_preparation import CopyTableOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

import helpers
from operators.geocode_operator import GeocodeOperator
from datasets import Dataset
import recipes.sis_recipes as recipes
from config import CONN_ID, SQL_DIR


default_args = helpers.default_args({"start_date": datetime(2019, 6, 11, 5)})


"""
Ce DAG permet de préparer les données SIS
On utilise un dump SQL Géorisques converti en csv
car les données complètes ne sont pas disponibles
en téléchargement sur le site
"""

with DAG("prepare_sis",
         default_args=default_args,
         template_searchpath=os.path.join(SQL_DIR, "sis"),
         schedule_interval=None) as dag:

    start = DummyOperator(task_id="start")

    # Charge les données dans la table sis_source
    load = PythonOperator(
        task_id="load",
        python_callable=recipes.load_sis)

    start >> load

    # filtre les données sur les départements configurés
    filter_departements = PythonOperator(
        task_id="filter_departements",
        python_callable=recipes.filter_departements)

    load >> filter_departements

    # Géocode les adresses
    # La géolocalisation des données source est déjà
    # assez précise mais on applique quand même le géocodage
    # pour autoriser des recherches par adresse_id
    # Cf concordance adresse / parcelle
    geocode = GeocodeOperator(
        input_dataset=Dataset("etl", "sis_filtered"),
        output_dataset=Dataset("etl", "sis_geocoded"),
        columns=["adresse"],
        citycode="code_insee",
        task_id="geocode")

    filter_departements >> geocode

    # Ajout des colonnes normalisées geog_precision
    # et geog_source
    set_precision = PostgresOperator(
        task_id="set_precision",
        postgres_conn_id=CONN_ID,
        sql="set_precision.sql")

    geocode >> set_precision

    # Ajout d'un numéro de version
    add_version = PostgresOperator(
        task_id="add_version",
        postgres_conn_id=CONN_ID,
        sql="add_version.sql")

    set_precision >> add_version

    # Copie les données dans la table finale
    stage = CopyTableOperator(
        task_id="stage",
        source="etl.sis_with_version",
        destination="etl.sis",
        postgres_conn_id=CONN_ID)

    add_version >> stage

    # Crée un index sur la colonne adresse_id
    create_index = PostgresOperator(
        task_id="create_index",
        postgres_conn_id=CONN_ID,
        sql="create_index.sql")

    stage >> create_index

    # Vérifie que la table finale est conforme
    check = PythonOperator(
        task_id="check",
        python_callable=recipes.check)

    create_index >> check
