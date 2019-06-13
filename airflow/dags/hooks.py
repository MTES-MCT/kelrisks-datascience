

from airflow.hooks.postgres_hook import PostgresHook

from peewee import PostgresqlDatabase


def get_connection(conn_name):
    return PostgresHook.get_connection(conn_name)


def get_peewee_database(conn_name):
    conn = get_connection(conn_name)
    return PostgresqlDatabase(
        conn.schema,
        host=conn.host,
        user=conn.login,
        password=conn.get_password(),
        port=conn.port)


def get_conn_env(conn_name):
    conn = get_connection(conn_name)
    return {
        # PostgreSQL
        "POSTGRES_HOST": conn.host,
        "POSTGRES_USER": conn.login,
        "POSTGRES_PWD": conn.get_password(),
        "POSTGRES_PORT": str(conn.port),
        "POSTGRES_DATABASE": conn.schema
    }

