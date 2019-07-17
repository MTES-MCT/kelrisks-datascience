import asyncio
from io import StringIO

import requests

from ...utils import csv2dicts, dicts2csv, chunks


min_score = 0.7
addok_bano_search = 'https://api-adresse.data.gouv.fr/search/'
addok_bano_search_csv = addok_bano_search + 'csv/'


def geocode_bulk(data, columns, citycode=None, postcode=None):
    """
    geocode a in bulk using the /search/csv endpoint
    """

    def inner(chunk):
        """ Perform the actual geocodage """
        csvfile = dicts2csv(chunk, dialect='unix')
        response = geocode_csv(csvfile,
                               columns,
                               citycode=citycode,
                               postcode=postcode)
        return csv2dicts(StringIO(response.text), dialect='unix')

    geocoded = []
    # split data in chunks
    chunk_size = 100
    for chunk in chunks(data, chunk_size):
        r = inner(chunk)
        geocoded += r
    return geocoded


def geocode_csv(csvlike, columns, citycode=None, postcode=None):

    payload = {'columns': columns}

    if citycode:
        payload['citycode'] = citycode
    if postcode:
        payload['postcode'] = postcode

    files = {'data': csvlike}

    response = requests.post(addok_bano_search_csv, data=payload, files=files)

    if response.status_code != 200:
        csvlike.seek(0)
        print(csvlike.read())

    response.raise_for_status()

    return response
