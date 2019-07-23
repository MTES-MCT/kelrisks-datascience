from unittest import TestCase
import textwrap

from ..parcelle import parse_parcelle, Parcelle


class ParcelleTestCase(TestCase):

    def test_parcelle(self):
        """ it should return an array of Parcelles """
        parcelles_str = textwrap.dedent("""
            08|08105|05/02/2014|DO |308|#
            08|08105|05/02/2014|DO |225|""")
        parcelles = parse_parcelle(parcelles_str)
        expected = [
            Parcelle('08105', 'DO', '308'),
            Parcelle('08105', 'DO', '225')]
        self.assertEqual(parcelles, expected)

    def test_parse_parcelle_invalid_string(self):
        """ it should return an empty array """
        invalid = textwrap.dedent("""
            27||00/00/0000|AN|110|#
            27||00/00/0000|AN|111|""")
        parcelles = parse_parcelle(invalid)
        self.assertEqual(parcelles, [])

    def test_parse_parcelle_section_numeric(self):
        """ it should return an empty array """
        invalid = "08|08105|05/02/2014|12|308|"
        parcelles = parse_parcelle(invalid)
        self.assertEqual(parcelles, [])

    def test_parse_parcelle_section_too_long(self):
        """ it should return an empty array """
        invalid = "08|08105|05/02/2014|ADT|308|"
        parcelles = parse_parcelle(invalid)
        self.assertEqual(parcelles, [])

    def test_parse_parcelle_numero_too_long(self):
        """ it should return an empty array"""
        invalid = "08|08105|05/02/2014|ADT|30887|"
        parcelles = parse_parcelle(invalid)
        self.assertEqual(parcelles, [])

