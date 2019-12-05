# -*- coding: utf-8 -*-

"""
Ce DAG permet de charger l'intégralité des parcelles cadastrales
pour tous les départements français, y compris outre-mer.
Le dernier millésime du cadastre Etalab est utilisé ce qui
permet de faire des mises à jour. Voir
https://cadastre.data.gouv.fr/

Il est également possible de charger les données
pour certains départements uniquement en utilisant la variable
d'environnement DEPARTEMENTS

Ex:
DEPARTEMENTS=07,26 => Charge les parcelles de l'Ardèche et de la Drôme
DEPARTEMENTS=all => Charges les parcelles France entière

On utilise les données au format geojson par commune plutôt
que par département pour éviter de consommer trop de mémoire
d'un coup. Il est possible de paralléliser le chargement
de plusieurs départements en simultané en ajustant la variable
d'environnement AIRFLOW__CORE__DAG_CONCURRENCY

On ne charge pas les données directement dans la table cadastre.
On utilise à la place une table temporaire dont le contenu est
ensuite reversé dans la table principale. Cela permet d'ajouter
les données pour un département de manière atomique. Si
pour une raison ou pour une autre la chargement d'un département
échoue, on peut le relancer sans risquer de se retrouver avec
des doublons.
"""

from datetime import datetime
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

import helpers
import recipes.cadastre_recipes as recipes
from config import CONN_ID, DEPARTEMENTS, SQL_DIR

default_args = helpers.default_args({"start_date": datetime(2019, 6, 11, 5)})


with DAG("prepare_cadastre",
         default_args=default_args,
         schedule_interval=None,
         template_searchpath=os.path.join(SQL_DIR, "cadastre")) as dag:

    start = DummyOperator(task_id="start")

    create_cadastre_table = PostgresOperator(
        task_id="create_cadastre_table",
        sql="cadastre.sql",
        postgres_conn_id=CONN_ID,
        params={"schema": "etl", "table": "cadastre"})

    load_tasks = []

    for departement in DEPARTEMENTS:

        temp_table = "cadastre_{dep}_temp".format(
            dep=departement)

        start_dep = DummyOperator(
            task_id="start_{dep}".format(dep=departement))

        load_tasks.append(start_dep)

        create_temp = PostgresOperator(
            task_id="create_temp_{dep}".format(dep=departement),
            sql="cadastre.sql",
            postgres_conn_id=CONN_ID,
            params={"schema": "etl", "table": temp_table})

        load = PythonOperator(
            task_id="load_{dep}".format(dep=departement),
            python_callable=recipes.load_cadastre_for_department,
            op_args=[departement])

        copy = PostgresOperator(
            task_id="copy_{dep}".format(dep=departement),
            sql="copy_temp.sql",
            postgres_conn_id=CONN_ID,
            params={
                "source": "etl.{table}".format(table=temp_table),
                "destination": "etl.cadastre"})

        delete_temp = PostgresOperator(
            task_id="delete_temp_{dep}".format(dep=departement),
            sql="DROP TABLE IF EXISTS etl.{table_name}".format(
                table_name=temp_table),
            postgres_conn_id=CONN_ID)

        start_dep >> create_temp >> load >> copy >> delete_temp

    # create_index = PostgresOperator(
    #     task_id="create_index",
    #     sql=textwrap.dedent("""
    #         CREATE INDEX cadastre_commune_prefixe_section_numero_idx
    #         ON etl.cadastre (commune, prefixe, section, numero)"""),
    #     postgres_conn_id=CONN_ID)

    # create_index = PostgresOperator(
    #     task_id="create_index_code",
    #     sql=textwrap.dedent("""
    #         CREATE INDEX cadastre_code_idx
    #         ON etl.cadastre (code)"""),
    #     postgres_conn_id=CONN_ID)

    start >> create_cadastre_table >> load_tasks
