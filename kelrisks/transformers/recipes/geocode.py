import asyncio
from io import StringIO

import requests

from ...utils import csv2dicts, dicts2csv


min_score = 0.7
addok_bano_search = 'https://api-adresse.data.gouv.fr/search/'
addok_bano_search_csv = addok_bano_search + 'csv/'


class Geocodage():

    def __init__(self, x, y, score):
        self.x = x
        self.y = y
        self.score = score


def geocode(session, adresse, citycode):

    if adresse:
        try:
            params = {'q': adresse, 'citycode': citycode}
            r = session.get(addok_bano, params=params)
            data = r.json()
            features = data['features']
            if len(features) >= 1:
                feature = features[0]
                properties = feature['properties']
                feature_type = properties['type']
                score = float(properties['score'])
                if feature_type == 'housenumber' and score > min_score:
                    try:
                        x = float(properties['x'])
                        y = float(properties['y'])
                        return Geocodage(x, y, score)
                    except:
                        return None
            return None
        except Exception as e:
            print(r.status_code)
            return None

    return None


def geocode_bulk(data, columns, citycode=None, postcode=None):
    """
    geocode a in bulk using the /search/csv endpoint
    """

    def chunks(l, n):
        """ Yield successive n-sized chunks from l. """
        for i in range(0, len(l), n):
            yield l[i:i + n]

    def inner(chunk):
        """ Perform the actual geocodage """
        csvfile = dicts2csv(chunk, dialect='unix')
        response = geocode_csv(csvfile,
                               columns,
                               citycode=citycode,
                               postcode=postcode)
        return csv2dicts(StringIO(response.text), dialect='unix')

    geocoded = []
    # split data in chunks of 1000
    for chunk in chunks(data, 1000):
        geocoded += inner(chunk)

    return geocoded


def geocode_csv(csvlike, columns, citycode=None, postcode=None):

    payload = {'columns': columns}

    if citycode:
        payload['citycode'] = citycode
    if postcode:
        payload['postcode'] = postcode

    files = {'data': csvlike}

    response = requests.post(addok_bano_search_csv, data=payload, files=files)

    return response

