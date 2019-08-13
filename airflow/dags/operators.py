# -*- coding=utf-8 -*-

import re
from collections import namedtuple

"""
Operators are building blocks for Python recipes. They operate
on individudal columns or cells
"""


Parcelle = namedtuple("Parcelle", ["commune", "section", "numero"])


section_regex = re.compile("(?:000)?([a-zA-Z]{1,2})")


def extract_basias_cadastre_section(section):
    search = section_regex.findall(section)
    return search[0].upper() if search else None


numero_regex = re.compile("[0-9]{1,4}")


def extract_basias_cadastre_numeros(numero):
    search = numero_regex.findall(numero)
    return search


def extract_basias_parcelles_from_row(row):
    """
    parse rows from table basias_cadastre_source
    returning a parcelle
    """
    s = row["section_du_cadastre"]
    section = extract_basias_cadastre_section(s)
    if section:
        n = row["numero_de_parcelle"]
        numeros = extract_basias_cadastre_numeros(n)
        if numeros:
            commune = row["numero_insee"]
            return [Parcelle(commune, section, numero) for numero in numeros]
    return []
