

from unittest import TestCase
from peewee import *

from ..models import Point, PointField
from ..connections import get_test_database


class PointFieldTestCase(TestCase):

    def setUp(self):

        # define a test model with a PointField in srid 4326
        class TestModel(Model):
            point = PointField(4326, null=True)

            class Meta:
                database = get_test_database()

        self.model = TestModel
        self.model.create_table()

    def tearDown(self):
        self.model.drop_table()

    def test_to_db_value_to_python_value(self):
        """ it should serialize and deserialize from/to db """
        point = Point(2.1, 48.7, 4326)
        instance = self.model(point=point)
        instance.save()
        data = self.model.select().dicts()
        record = data[0]['point']
        expected = Point(x=2.1, y=48.7, srid=4326)
        self.assertEqual(record, expected)

    def test_to_db_value_to_python_value_different_srid(self):
        """ it should convert the point in the field srid """
        point = Point(639260.0, 6848180.0, 2154)
        instance = self.model(point=point)
        instance.save()
        data = self.model.select().dicts()
        record = data[0]['point']
        expected = Point(x=2.1741607382356576, y=48.73088663096782, srid=4326)
        self.assertEqual(record, expected)

    def test_to_db_value_to_python_value_is_none(self):
        point = None
        instance = self.model(point=point)
        instance.save()
        self.assertEqual(instance.point, None)
        data = self.model.select().dicts()
        record = data[0]['point']
        self.assertIsNone(record)
