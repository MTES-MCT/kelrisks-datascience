
from unittest import TestCase
from ..connections import get_default_database, get_test_database, test_db_name
from ..models import get_models


default_db = get_default_database()


def setup_database():
    """ Create the test database """
    conn = default_db._connect()
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute('DROP DATABASE IF EXISTS %s' % test_db_name)
    cursor.execute('CREATE DATABASE %s' % test_db_name)


def teardown_database():
    """ Destroy the test database """
    conn = default_db._connect()
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute('DROP DATABASE IF EXISTS %s' % test_db_name)


class BaseTestCase(TestCase):

    def setUp(self):
        self.models = get_models()
        self.db = get_test_database()
        self.db.bind(self.models.values())
        self.db.create_tables(self.models.values())

    def tearDown(self):
        self.db.drop_tables(self.models.values())