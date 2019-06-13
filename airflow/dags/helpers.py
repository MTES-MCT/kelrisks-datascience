
from datetime import timedelta


from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator

import hooks


def default_env():
    return hooks.get_conn_env('postgres_etl')


def resolve_env(env):
    if type(env) == dict:
        return {**(default_env()), **env}
    return default_env()


def base_path():
    return Variable.get("BASE_PATH")


def data_path(filename):
    return "{base}/data/{filename}".format(base=base_path(), filename=filename)


def embulk_filepath(filename):
    return "{base}/embulk/{filename}.yml.liquid".format(
        base=base_path(), filename=filename)


def embulk_run(dag, script, env=None, task_id=None):
    return BashOperator(
        task_id=task_id or "embulk_run_" + script,
        bash_command="%s run %s"
        % (Variable.get("EMBULK_BIN"), embulk_filepath(script)),
        dag=dag,
        env=resolve_env(env))


def default_args(conf):
    default = {
        "owner": "airflow",
        "depends_on_past": False,
        "email": ["benoit.guigal@protonmail.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=1),
    }
    return {**(default), **conf}
