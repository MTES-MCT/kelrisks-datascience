# -*- coding=utf-8 -*-

import textwrap
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.data_preparation import CopyTableOperator
from airflow.operators.postgres_operator import PostgresOperator

import helpers
from config import CONN_ID, KELRISKS_POSTGRES_USER


default_args = helpers.default_args({"start_date": datetime(2019, 6, 11, 5)})


"""
Ce DAG permet de copier la table cadastre du schéma `etl` vers
le schéma `kelrisks`. À noter que la copie des autres tables se
fait dans le DAG `deploy`
"""

with DAG("deploy_cadastre",
         default_args=default_args,
         schedule_interval=None) as dag:

    start = DummyOperator(
        task_id="start")

    deploy_cadastre = CopyTableOperator(
        task_id="deploy_cadastre",
        postgres_conn_id=CONN_ID,
        source="etl.cadastre",
        destination="kelrisks.cadastre")

    create_cadastre_id_seq = PostgresOperator(
        task_id="create_cadastre_id_seq",
        postgres_conn_id=CONN_ID,
        sql=textwrap.dedent("""
            DROP SEQUENCE IF EXISTS cadastre_id_seq;
            CREATE SEQUENCE cadastre_id_seq;
            ALTER SEQUENCE cadastre_id_seq owner to {user};"""
                            .format(user=KELRISKS_POSTGRES_USER)))

    start >> deploy_cadastre >> create_cadastre_id_seq
