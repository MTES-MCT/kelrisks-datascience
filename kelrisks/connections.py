
from peewee import PostgresqlDatabase

from .config import POSTGRES_DB, POSTGRES_HOST, POSTGRES_USER, \
    POSTGRES_PASSWORD, POSTGRES_PORT


def prepare_db(function):
    """
    decorator thats adds postgis extension
    and etl schema if not exists """

    def wrapper():
        db = function()
        create_schema_stmt = 'CREATE SCHEMA IF NOT EXISTS kelrisks'
        db.execute_sql(create_schema_stmt)
        create_schema_stmt = 'CREATE SCHEMA IF NOT EXISTS etl'
        db.execute_sql(create_schema_stmt)
        db.execute_sql('CREATE EXTENSION IF NOT EXISTS postgis')
        return db

    return wrapper


@prepare_db
def get_database():
    """ return main database """

    return PostgresqlDatabase(
            POSTGRES_DB,
            host=POSTGRES_HOST,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            port=POSTGRES_PORT)


test_db_name = 'test_%s' % POSTGRES_DB


@prepare_db
def get_test_database():
    """ return test database """

    return PostgresqlDatabase(
            test_db_name,
            host=POSTGRES_HOST,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            port=POSTGRES_PORT)


def get_default_database():
    """ get default postgres database """

    return PostgresqlDatabase(
        'postgres',
        host=POSTGRES_HOST,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        port=POSTGRES_PORT,
        autocommit=True)
