# -*- coding: utf-8 -*-
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.data_preparation import EmbulkOperator, \
    DownloadUnzipOperator, CopyTableOperator

import helpers
from operators.geocode_operator import GeocodeOperator
import recipes.basias_recipes as recipes
from datasets import Dataset
from config import DATA_DIR, CONN_ID, SQL_DIR


default_args = helpers.default_args({"start_date": datetime(2019, 6, 11, 5)})

"""
Ce DAG permet de préparer les données BASIAS
On utilise les dumps de trois tables géorisques
"sites", "localisation" et "cadastre" convertis
au format csv.

Les infomations de géolocalisation provenant de
différentes sources sont fusionnées
- champs x, y, xl2_adresse, yl2_adresse, etc de la table localisation
- géocodage du champ adresse de la table localisation
- extraction des numéro de parcelle de la table parcelle
"""

with DAG("prepare_basias",
         default_args=default_args,
         template_searchpath=os.path.join(SQL_DIR, "basias"),
         schedule_interval=None) as dag:

    start = DummyOperator(task_id="start")

    # Télécharge les fichiers sites.csv, localisation.csv et cadastre.csv
    download = DownloadUnzipOperator(
        task_id="download",
        url="https://kelrisks.fra1.digitaloceanspaces.com/basias.zip",
        dir_path=DATA_DIR)

    # Charge le fichier sites.csv en base
    load_sites = EmbulkOperator(
        task_id="load_sites",
        embulk_config="basias_sites.yml.liquid")

    # Charge le fichier localisation.csv en base
    load_localisation = EmbulkOperator(
        task_id="load_localisation",
        embulk_config="basias_localisation.yml.liquid")

    # Charge le fichier cadastre.csv en base
    load_cadastre = EmbulkOperator(
        task_id="load_cadastre",
        embulk_config="basias_cadastre.yml.liquid")

    # Filtre les données sur les départements configurés
    filter_departements = PythonOperator(
        task_id="filter_departements",
        python_callable=recipes.filter_departements)

    # Parse les données cadastrales
    parse_cadastre = PythonOperator(
        task_id="parse_cadastre",
        python_callable=recipes.parse_cadastre)

    # Associe le numéro de parcelle avec son contour
    # géométrique en faisait une jointure avec les
    # données cadastrales
    add_geog = PythonOperator(
        task_id="add_geog",
        python_callable=recipes.add_geog)

    # Fusionne les différentes parcelles d'un même
    # site en un multi polygon
    merge_cadastre_geog = PythonOperator(
        task_id="merge_cadastre_geog",
        python_callable=recipes.merge_cadastre_geog)

    # Quelques traitements sur la table sites
    prepare_sites = PythonOperator(
        task_id="prepare_sites",
        python_callable=recipes.prepare_sites)

    # géocode les adresses de la table localisation
    geocode = GeocodeOperator(
        input_dataset=Dataset(
            "etl", "basias_localisation_filtered"),
        output_dataset=Dataset(
            "etl", "basias_localisation_geocoded"),
        columns=["numero", "bister", "type_voie", "nom_voie"],
        citycode="numero_insee",
        task_id="geocode")

    # Sélectionne la meilleure information
    merge_geog = PythonOperator(
        task_id="merge_geog",
        python_callable=recipes.merge_geog)

    # Projette les points sur la parcelle la plus proche
    find_nearest_parcelle = PythonOperator(
        task_id="find_nearest_parcelle",
        python_callable=recipes.intersect)

    # Jointure entre la table localisation et la table cadastre
    join_localisation_cadastre = PythonOperator(
        task_id="join_localisation_cadastre",
        python_callable=recipes.join_localisation_cadastre)

    # Jointure entre la table sites et la table
    # localisation_join_cadastre obtenu à l'étape
    # précédente
    join_sites_localisation = PythonOperator(
        task_id="join_sites_localisation",
        python_callable=recipes.join_sites_localisation)

    # Remplace les points dont la précision est
    # de type MUNICIPALITY par un polygone
    # représentant le contour de la commune
    add_commune = PythonOperator(
        task_id="add_commune",
        python_callable=recipes.add_commune)

    # Ajoute une colonne version pour des raisons
    # de compatibilité avec le framework Spring
    add_version = PostgresOperator(
        task_id="add_version",
        postgres_conn_id=CONN_ID,
        sql="add_version.sql")

    # Copie les données dans la table finale
    stage = CopyTableOperator(
        task_id="stage",
        postgres_conn_id=CONN_ID,
        source="etl.basias_sites_with_version",
        destination="etl.basias")

    # Crée un index sur la colonne adresse_id
    create_index = PostgresOperator(
        task_id="create_index",
        postgres_conn_id=CONN_ID,
        sql="create_index.sql")

    # Vérifie que la table finale est conforme
    check = PythonOperator(
        task_id="check",
        python_callable=recipes.check)

    start >> download >> [
        load_sites,
        load_localisation,
        load_cadastre] >> filter_departements

    filter_departements >> prepare_sites

    filter_departements >> geocode >> merge_geog >> find_nearest_parcelle

    filter_departements >> parse_cadastre >> add_geog >> merge_cadastre_geog

    [find_nearest_parcelle, merge_cadastre_geog] >> join_localisation_cadastre

    [join_localisation_cadastre, prepare_sites] >> join_sites_localisation \
        >> add_commune >> add_version >> stage >> create_index >> check
