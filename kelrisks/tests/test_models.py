

from unittest import TestCase

from ..models import PointField
from .helpers import get_test_db

from peewee import *


class PointFieldTestCase(TestCase):

    def setUp(self):

        # define a test model with a PointField
        class TestModel(Model):
            point = PointField(2154, null=True)

            class Meta:
                database = get_test_db()

        self.model = TestModel
        self.model.create_table()

    def tearDown(self):
        self.model.drop_table()

    def test_to_db_value_to_python_value(self):
        point = (639260.0, 6848180.0)
        instance = self.model(point=point)
        instance.save()
        self.assertEqual(instance.point, (639260.0, 6848180.0))

    def test_to_db_value_to_python_value_is_none(self):
        point = None
        instance = self.model(point=point)
        instance.save()
        self.assertEqual(instance.point, None)
