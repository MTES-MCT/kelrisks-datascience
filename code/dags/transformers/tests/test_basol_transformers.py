# -*- coding=utf-8 -*-

from unittest import TestCase

from transformers.basol_transformers import parse_cadastre


class BasolTransformersTestCase(TestCase):

    def test_parse_cadastre(self):

        cadastre_multi = "08|08105|05/02/2014|DO |308|#08|08105|05/02/2014|DO |225|"
        parcelles = parse_cadastre(cadastre_multi)
        expected = [
            {
                'commune': '08105',
                'section': 'DO',
                'numero': '308'
            },
            {
                'commune': '08105',
                'section': 'DO',
                'numero': '225'
            }
        ]
        self.assertEqual(parcelles, expected)

        cadastre_multi = ""
        parcelles = parse_cadastre(cadastre_multi)
        self.assertEqual(parcelles, [])
