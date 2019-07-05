
import subprocess
import os

from .config import EMBULK_BIN, ROOT_DIR, POSTGRES_DB, POSTGRES_HOST, \
    POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_PORT, POSTGRES_SSL_ON, \
    POSTGRES_SCHEMA


def embulk_config_path(filename):
    return os.path.join(
        ROOT_DIR,
        'embulk',
        '{filename}.yml.liquid'.format(filename=filename)
    )


def embulk_input_path(prefix):
    return os.path.join(ROOT_DIR, 'embulk', 'data', prefix)


def embulk_run(filename, input, output):
    config = embulk_config_path(filename)
    env = {
        'INPUT': embulk_input_path(input),
        'OUTPUT': output,
        'POSTGRES_HOST': POSTGRES_HOST,
        'POSTGRES_DB': POSTGRES_DB,
        'POSTGRES_SCHEMA': POSTGRES_SCHEMA,
        'POSTGRES_DB': POSTGRES_DB,
        'POSTGRES_USER': POSTGRES_USER,
        'POSTGRES_PASSWORD': POSTGRES_PASSWORD,
        'POSTGRES_PORT': POSTGRES_PORT,
        'POSTGRES_SSL_ON': 'true' if POSTGRES_SSL_ON else 'false'
    }

    cmd = '{embulk_bin} run {config}'.format(
        embulk_bin=EMBULK_BIN, config=config)
    subprocess.run(cmd, shell=True, env=env)


def load_s3ic():
    embulk_run('s3ic', 's3ic', 's3ic_source')


def load_sis():
    embulk_run('sis', 'sis', 'sis_source')
