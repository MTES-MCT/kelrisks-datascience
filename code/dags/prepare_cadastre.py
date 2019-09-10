# -*- coding: utf-8 -*-

from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import \
    PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

import helpers
import recipes.cadastre_recipes as recipes
from config import CONN_ID, DEPARTEMENTS, CADASTRE_CONCURRENCY


default_args = helpers.default_args({"start_date": datetime(2019, 6, 11, 5)})


with DAG("prepare_cadastre",
         default_args=default_args,
         schedule_interval=None,
         concurrency=CADASTRE_CONCURRENCY) as dag:

    start = DummyOperator(task_id="start")

    create_cadastre_table = PythonOperator(
        task_id="create_cadastre_table",
        python_callable=recipes.create_cadastre_table)

    load_departements = []

    for departement in DEPARTEMENTS:

        load_departements.append(
            PythonOperator(
                task_id="load_{dep}".format(dep=departement),
                python_callable=recipes.load_cadastre_for_department,
                op_args=[departement]))

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

    start >> create_cadastre_table >> load_departements
