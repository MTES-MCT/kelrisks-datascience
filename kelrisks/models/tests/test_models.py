

from unittest import TestCase
import json

from peewee import *
from shapely.geometry import Point, MultiPolygon

from ..base import Geometry, GeometryField

from ...connections import get_test_database


class GeometryFieldTestCase(TestCase):

    def setUp(self):

        # define a test model with a PointField in srid 4326
        class TestModel(Model):
            geom = GeometryField(4326, null=True)

            class Meta:
                database = get_test_database()

        self.model = TestModel
        self.model.create_table()

    def tearDown(self):
        self.model.drop_table()

    def test_to_db_value_to_python_value_point(self):
        """ it should serialize and deserialize from/to db """
        point = Point(1, 2)
        geom = Geometry(point, 4326)
        instance = self.model(geom=geom)
        instance.save()
        data = self.model.select().dicts()
        result = data[0]['geom']
        self.assertEqual(result.geom, point)

    def test_to_db_value_to_python_value_point_different_srid(self):
        point = Point(
            7.82778710302907,
            48.9433371177302)
        geom = Geometry(point, 2154)
        instance = self.model(geom=geom)
        instance.save()
        data = self.model.select().dicts()
        result = data[0]['geom']
        expected = Point(
            -1.3630493579752925,
            -5.983548810597482)
        self.assertEqual(result.geom, expected)

    def test_to_db_value_to_python_value_multi_polygon(self):
        multipolygon = MultiPolygon(
            [
                (
                    ((0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0)),
                    [((0.1, 0.1), (0.1, 0.2), (0.2, 0.2), (0.2, 0.1))]
                )
            ]
        )
        geom = Geometry(multipolygon, 4326)
        instance = self.model(geom=geom)
        instance.save()
        data = self.model.select().dicts()
        result = data[0]['geom']
        self.assertEqual(result.geom, multipolygon)

    def test_to_db_value_to_python_value_is_none(self):
        instance = self.model(geom=None)
        instance.save()
        self.assertEqual(instance.geom, None)
        data = self.model.select().dicts()
        record = data[0]['geom']
        self.assertIsNone(record)





