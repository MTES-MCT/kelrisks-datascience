# -*- coding: utf-8 -*-

from datetime import timedelta

from config import SQL_DIR


def resolve_env(env):
    if type(env) == dict:
        return {**(default_env()), **env}
    return default_env()


def default_args(conf):
    default = {
        "owner": "airflow",
        "depends_on_past": False,
        "email": ["benoit.guigal@protonmail.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=1)
    }
    return {**(default), **conf}
