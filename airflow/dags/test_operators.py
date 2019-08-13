# -*- coding=utf-8 -*-

from unittest import TestCase

from operators import extract_basias_cadastre_section, \
    extract_basias_cadastre_numeros


class OperatorsTestCase(TestCase):

    def test_extract_basias_cadastre_section(self):

        section = "AI"
        extracted = extract_basias_cadastre_section(section)
        self.assertEqual(extracted, section)

        section = "H"
        extracted = extract_basias_cadastre_section(section)
        self.assertEqual(extracted, section)

        section = "000BL"
        extracted = extract_basias_cadastre_section(section)
        self.assertEqual(extracted, "BL")

        section = "a"
        extracted = extract_basias_cadastre_section(section)
        self.assertEqual(extracted, "A")

        section = "?"
        extracted = extract_basias_cadastre_section(section)
        self.assertEqual(extracted, None)

        section = "B n° 1"
        extracted = extract_basias_cadastre_section(section)
        self.assertEqual(extracted, "B")

        section = "4"
        extracted = extract_basias_cadastre_section(section)
        self.assertEqual(extracted, None)

        section = "C et D"
        extracted = extract_basias_cadastre_section(section)
        self.assertEqual(extracted, "C")

    def test_extract_basias_numeros(self):

        numero = "322"
        extracted = extract_basias_cadastre_numeros(numero)
        self.assertEqual(extracted, ["322"])

        numero = "34/35"
        extracted = extract_basias_cadastre_numeros(numero)
        self.assertEqual(extracted, ["34", "35"])

        numero = "89-90-91-92-93-280a"
        extracted = extract_basias_cadastre_numeros(numero)
        self.assertEqual(extracted, ["89", "90", "91", "92", "93", "280"])

        numero = "?"
        extracted = extract_basias_cadastre_numeros(numero)
        self.assertEqual(extracted, [])
