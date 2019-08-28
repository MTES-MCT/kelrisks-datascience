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
