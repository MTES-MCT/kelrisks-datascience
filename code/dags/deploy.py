# -*- coding=utf-8 -*-

import textwrap
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.data_preparation import CopyTableOperator
from airflow.operators.postgres_operator import PostgresOperator

import helpers
from config import CONN_ID


default_args = helpers.default_args({"start_date": datetime(2019, 6, 11, 5)})


"""
This DAG move prepared tables from schema etl to schema kelrisks
"""

with DAG("deploy",
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
            ALTER SEQUENCE cadastre_id_seq owner to postgres;"""))

    deploy_sis = CopyTableOperator(
        task_id="deploy_sis",
        postgres_conn_id=CONN_ID,
        source="etl.sis",
        destination="kelrisks.sis")

    create_sis_id_seq = PostgresOperator(
        task_id="create_sis_id_seq",
        postgres_conn_id=CONN_ID,
        sql=textwrap.dedent("""
            DROP SEQUENCE IF EXISTS sis_id_seq;
            CREATE SEQUENCE sis_id_seq;
            ALTER SEQUENCE sis_id_seq owner to postgres;"""))

    deploy_basol = CopyTableOperator(
        task_id="deploy_basol",
        postgres_conn_id=CONN_ID,
        source="etl.basol",
        destination="kelrisks.basol")

    create_basol_id_seq = PostgresOperator(
        task_id="create_basol_id_seq",
        postgres_conn_id=CONN_ID,
        sql=textwrap.dedent("""
            DROP SEQUENCE IF EXISTS basol_id_seq;
            CREATE SEQUENCE basol_id_seq;
            ALTER SEQUENCE basol_id_seq owner to postgres;"""))

    deploy_basias = CopyTableOperator(
        task_id="deploy_basias",
        postgres_conn_id=CONN_ID,
        source="etl.basias",
        destination="kelrisks.basias")

    create_basias_id_seq = PostgresOperator(
        task_id="create_basias_id_seq",
        postgres_conn_id=CONN_ID,
        sql=textwrap.dedent("""
            DROP SEQUENCE IF EXISTS basias_id_seq;
            CREATE SEQUENCE basias_id_seq;
            ALTER SEQUENCE basias_id_seq owner to postgres;"""))

    deploy_s3ic = CopyTableOperator(
        task_id="deploy_s3ic",
        postgres_conn_id=CONN_ID,
        source="etl.s3ic",
        destination="kelrisks.s3ic")

    create_s3ic_id_seq = PostgresOperator(
        task_id="create_s3ic_id_seq",
        postgres_conn_id=CONN_ID,
        sql=textwrap.dedent("""
            DROP SEQUENCE IF EXISTS s3ic_id_seq;
            CREATE SEQUENCE s3ic_id_seq;
            ALTER SEQUENCE s3ic_id_seq owner to postgres;"""))

    deploy_code_postal = CopyTableOperator(
        task_id="deploy_code_postal",
        postgres_conn_id=CONN_ID,
        source="etl.code_postal",
        destination="kelrisks.adresse_commune")

    create_code_postal_id_seq = PostgresOperator(
        task_id="create_code_postal_id_seq",
        postgres_conn_id=CONN_ID,
        sql=textwrap.dedent("""
            DROP SEQUENCE IF EXISTS adresse_commune_id_seq;
            CREATE SEQUENCE adresse_commune_id_seq;
            ALTER SEQUENCE adresse_commune_id_seq owner to postgres;"""))

    start >> [
        deploy_cadastre,
        deploy_sis,
        deploy_basol,
        deploy_basias,
        deploy_s3ic,
        deploy_code_postal]

    deploy_cadastre >> create_cadastre_id_seq

    deploy_sis >> create_sis_id_seq

    deploy_basol >> create_basol_id_seq

    deploy_basias >> create_basias_id_seq

    deploy_s3ic >> create_s3ic_id_seq

    deploy_code_postal >> create_code_postal_id_seq

