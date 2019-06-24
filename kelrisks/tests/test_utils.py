
import io
from unittest import TestCase

from ..utils import csv2dicts, dicts2csv, chunks


class UtilsTestCase(TestCase):

    def test_csv2dicts(self):

        csv = """food,calorie,score
apple,10,good
hamburger,100,mediocre"""
        dicts = csv2dicts(io.StringIO(csv), delimiter=',', dialect='unix')
        expected = [
            {'food': 'apple', 'calorie': '10', 'score': 'good'},
            {'food': 'hamburger', 'calorie': '100', 'score': 'mediocre'}
        ]
        self.assertEqual(dicts, expected)

    def test_dicts2csv(self):

        dicts = [
            {'food': 'apple', 'calorie': '10', 'score': 'good'},
            {'food': 'hamburger', 'calorie': '100', 'score': 'mediocre'}
        ]
        csv = dicts2csv(dicts, dialect='unix')
        expected = '"food","calorie","score"\n' \
                   '"apple","10","good"\n' \
                   '"hamburger","100","mediocre"\n'
        self.assertEqual(csv.read(), expected)


    def test_chunks(self):
        array = [1, 2, 3, 4, 5, 6, 7]
        chunked = list(chunks(array, 2))
        expected = [[1, 2], [3, 4], [5, 6], [7]]
        self.assertEqual(chunked, expected)







