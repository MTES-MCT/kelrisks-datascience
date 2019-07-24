from unittest import TestCase

from ..statistics import quality_report


class StatisticsTestCase(TestCase):

    def test_quality_report(self):
        """ it should render quality report """
        quality_report()