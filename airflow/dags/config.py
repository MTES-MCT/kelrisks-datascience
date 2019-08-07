# -*- coding: utf-8 -*-
import os


class ImproperlyConfigured(Exception):
    pass


def get_env_setting(setting):
    """ Get the environment setting or return exception """
    try:
        return os.environ[setting]
    except KeyError:
        error_msg = "Set the %s env variable" % setting
        raise ImproperlyConfigured(error_msg)


# Airflow connection defined as environement variables
# See https://airflow.apache.org/howto/connection/index.html
CONN_ID = 'postgres_kelrisks'

# SQL scripts
SQL_DIR = get_env_setting('SQL_DIR')