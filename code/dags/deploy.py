# -*- coding=utf-8 -*-

from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.data_preparation import CopyTableOperator

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

    deploy_sis = CopyTableOperator(
        task_id="deploy_sis",
        postgres_conn_id=CONN_ID,
        source="etl.sis",
        destination="kelrisks.sis")

    deploy_basol = CopyTableOperator(
        task_id="deploy_basol",
        postgres_conn_id=CONN_ID,
        source="etl.basol",
        destination="kelrisks.basol")

    deploy_basias = CopyTableOperator(
        task_id="deploy_basias",
        postgres_conn_id=CONN_ID,
        source="etl.basias",
        destination="kelrisks.basias")

    deploy_s3ic = CopyTableOperator(
        task_id="deploy_s3ic",
        postgres_conn_id=CONN_ID,
        source="etl.s3ic",
        destination="kelrisks.s3ic")

    deploy_code_postal = CopyTableOperator(
        task_id="deploy_code_postal",
        postgres_conn_id=CONN_ID,
        source="etl.code_postal",
        destination="kelrisks.adresse_commune")

    start >> [
        deploy_cadastre,
        deploy_sis,
        deploy_basol,
        deploy_basias,
        deploy_s3ic]
