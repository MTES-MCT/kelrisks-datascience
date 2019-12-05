# -*- coding: utf-8 -*-
import os
import constants


class ImproperlyConfigured(Exception):
    pass


def get_env_setting(setting):
    """ Get the environment setting or return exception """
    try:
        return os.environ[setting]
    except KeyError:
        error_msg = "Set the %s env variable" % setting
        raise ImproperlyConfigured(error_msg)


# List of departements to load
departements = get_env_setting("DEPARTEMENTS")
if departements == "all":
    DEPARTEMENTS = constants.DEPARTEMENTS
else:
    departements = departements.split(",")
    DEPARTEMENTS = {k: constants.DEPARTEMENTS[k] for k in departements}

KELRISKS_POSTGRES_USER = get_env_setting("KELRISKS_POSTGRES_USER")

# Airflow connection defined as environement variables
# See https://airflow.apache.org/howto/connection/index.html
CONN_ID = "postgres_kelrisks"

# directories structure
DATA_DIR = get_env_setting("DATA_DIR")

SQL_DIR = get_env_setting("SQL_DIR")
