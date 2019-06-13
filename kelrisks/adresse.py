import asyncio
from concurrent.futures import ThreadPoolExecutor

import requests

min_score = 0.7
addok_bano = 'https://api-adresse.data.gouv.fr/search'


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
