# -*- coding: utf-8 -*-

from airflow.hooks.data_preparation import PostgresDataset

from config import CONN_ID


def Dataset(schema, name):
    """
    Petit wrapper autour de PostgresDataset pour ne pas avoir à
    spécifier l'argument postgres_conn_id à chaque fois

    Voir le projet https://github.com/MTES-MCT/data-preparation-plugin
    pour plus d'information sur la classe PostgresDataset
    """
    return PostgresDataset(
        schema=schema, name=name, postgres_conn_id=CONN_ID)
