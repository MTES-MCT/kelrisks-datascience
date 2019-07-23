import re
from collections import namedtuple

regex = re.compile(
    '\d{2}\|(\d{5})\|\d{2}/\d{2}/\d{4}\|([A-Z]{1,2})\|(\d{1,4})\|',
    re.IGNORECASE)


Parcelle = namedtuple('Parcelle', ['commune', 'section', 'numero'])

def parse_parcelle(parcelles_str):
    """
    Parse a string representation of several parcelles.
    Ex: 08|08105|05/02/2014|DO |308|#08|08105|05/02/2014|DO |225|
    => Parcelle('08105', 'DO', '308'), Parcelle('08105', 'DO', '225')
    """

    # remove white spaces
    parcelles_str = parcelles_str.replace(' ', '')

    # split on #
    delimiter = '#'
    parcelles_str = parcelles_str.split(delimiter)

    parcelles = []
    for parcelle_str in parcelles_str:
        search = regex.search(parcelle_str)
        if search:
            parcelle = Parcelle(*search.groups())
            parcelles.append(parcelle)
    return parcelles
