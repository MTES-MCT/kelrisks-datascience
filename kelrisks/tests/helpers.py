
import os
from peewee import PostgresqlDatabase


class TestDatabaseNotConfigured(Exception):

    def __init__(self):
        message = "You should set env variables TEST_POSTGRES_DB, " + \
            "TEST_POSTGRES_HOST, TEST_POSTGRES_USER, TEST_POSTGRES_PWD " + \
            "and TEST_POSTGRES_PORT"
        Exception.__init__(message)

try:
    POSTGRES_DB = os.environ['TEST_POSTGRES_DB']
    POSTGRES_HOST = os.environ['TEST_POSTGRES_HOST']
    POSTGRES_USER = os.environ['TEST_POSTGRES_USER']
    POSTGRES_PWD = os.environ['TEST_POSTGRES_PWD']
    POSTGRES_PORT = os.environ['TEST_POSTGRES_PORT']
except KeyError:
    raise TestDatabaseNotConfigured()


def get_test_db():
    return PostgresqlDatabase(
        POSTGRES_DB,
        host=POSTGRES_HOST,
        user=POSTGRES_USER,
        password=POSTGRES_PWD,
        port=POSTGRES_PORT)