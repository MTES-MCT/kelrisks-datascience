# -*- coding=utf-8 -*-

"""
Transformers are building blocks for Python recipes. They operate
on individudal rows, cells or dataframes
"""

import re

import precisions


regex = re.compile(
    '\d{2}\|(\d{5})\|\d{2}/\d{2}/\d{4}\|\d*([A-Z]{1,2})\d*\|(\d{1,4})\|',
    re.IGNORECASE)


def parse_cadastre(cadastre_multi):
    """
    Parse a string representation of several parcelles.
    Ex: 08|08105|05/02/2014|DO |308|#08|08105|05/02/2014|DO |225|
    => Parcelle('08105', 'DO', '308'), Parcelle('08105', 'DO', '225')
    """

    # remove white spaces
    cadastre_multi = cadastre_multi.replace(' ', '')

    # split on #
    delimiter = '#'
    cadastre = cadastre_multi.split(delimiter)

    parcelles = []
    for parcelle_str in cadastre:
        search = regex.search(parcelle_str)
        if search:
            groups = search.groups()
            parcelle = {
                "commune": groups[0],
                "section": groups[1],
                "numero": groups[2]
            }
            parcelles.append(parcelle)
    return parcelles


def normalize_precision(row):
    # TODO get the meaning of 'autre' from the field 'georeferencement'
    mapping = {
        'Adresse (numéro)': precisions.HOUSENUMBER,
        'Adresse (rue)': precisions.STREET,
        'Commune (centre)': precisions.MUNICIPALITY,
        'Autre': precisions.HOUSENUMBER
    }
    row["l2e_precision"] = mapping.get(row["l2e_precision"], None)
    row["l93_precision"] = mapping.get(row["l93_precision"], None)
    return row



