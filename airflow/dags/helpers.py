# -*- coding: utf-8 -*-

from datetime import timedelta


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
