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


# FOLDER CONFIGURATION

ROOT_DIR = get_env_setting('ROOT_DIR')


# POSTGRES CONFIGURATION

POSTGRES_HOST = get_env_setting('POSTGRES_HOST')

POSTGRES_DB = get_env_setting('POSTGRES_DB')

POSTGRES_SCHEMA = get_env_setting('POSTGRES_SCHEMA')

POSTGRES_USER = get_env_setting('POSTGRES_USER')

POSTGRES_PASSWORD = get_env_setting('POSTGRES_PASSWORD')

POSTGRES_PORT = get_env_setting('POSTGRES_PORT')

POSTGRES_SSL_ON = get_env_setting('POSTGRES_SSL_ON') == 'True'


# EMBULK CONFIGURATION

EMBULK_BIN = get_env_setting('EMBULK_BIN')
