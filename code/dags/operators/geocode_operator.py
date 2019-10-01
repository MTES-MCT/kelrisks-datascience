# -*- coding=utf-8 -*-


import numpy as np
from airflow.operators.python_operator import PythonOperator
from bulk_geocoding import geocode as bulk_geocode
from sqlalchemy import Column, Float, String

import precisions


# Seuil du champ geocoded_result_score en dessous
# duquel on considère que le géocodage n'est pas bon
SCORE_THRESHOLD = 0.6

# Définition des colonnes qui sont ajoutées lors
# du géocodage
GEOCODED_LATITUDE = "geocoded_latitude"
GEOCODED_LONGITUDE = "geocoded_longitude"
GEOCODED_RESULT_SCORE = "geocoded_result_score"
GEOCODED_RESULT_TYPE = "geocoded_result_type"
ADRESSE_ID = "adresse_id"


class GeocodeOperator(PythonOperator):
    """
    Operator custom permettant de géocoder un dataset d'entrée
    et d'écrire le résultat dans un dataset de sortie en utilisant
    le service adresse.data.gouv

    Le code permettant de faire des requêtes de géocodage en masse
    sur https://api-adresse.data.gouv.fr/search/csv/
    a été réfactoré dans une librairie externe publiée sur le Github
    de la Fabrique
    https://github.com/MTES-MCT/bulk-geocoding-python-client

    :param input_dataset: Le dataset d'entré
    :param output_dataset: Le dataset de sortie
    :param columns liste des colonnes servant au géocodage. Cette liste
        permet de concaténer un numéro de rue, nom de rue, libellé
        de voie, etc présents dans différentes colonnes
    :param citycode: Nom de la colonne portant l'information du code insee

    Les colonnes suivantes sont ajoutées au dataset de sortie

    geocoded_latitude: Latitde du point géocodé dans le référentiel WGS84
    geocoded_longitude: Longitde du point géocodé dans le référntiel WGS84
    geocoded_result_score: Score de géocodage
    geocoded_result_type: Précision du géocodage: municipality, locality,
        street, housenumber
    adresse_id: Identifiant unique associé à l'adresse
    """

    def __init__(
            self,
            input_dataset,
            output_dataset,
            columns,
            citycode,
            **kwargs):

        super().__init__(
            python_callable=geocode,
            op_args=[input_dataset, output_dataset, columns, citycode],
            **kwargs)


def geocode(input_dataset, output_dataset, columns, citycode):

    # écrit le schéma de sortie
    dtype = input_dataset.read_dtype()

    output_dtype = [
        *dtype,
        Column(GEOCODED_LATITUDE, Float(precision=10)),
        Column(GEOCODED_LONGITUDE, Float(precision=10)),
        Column(GEOCODED_RESULT_SCORE, Float()),
        Column(GEOCODED_RESULT_TYPE, String()),
        Column(ADRESSE_ID, String())
    ]

    output_dataset.write_dtype(output_dtype)

    with output_dataset.get_writer() as writer:

        for df in input_dataset.get_dataframes(chunksize=50):

            df = df.replace({np.nan: None})

            rows = df.to_dict(orient="records")

            payload = []

            for row in rows:
                light_row = {
                    k: row[k]
                    for k in row
                    if k in [*columns, citycode]}
                payload.append(light_row)

            try:
                geocoded = bulk_geocode(
                    payload,
                    columns=columns,
                    citycode=citycode)

                zipped = list(zip(rows, geocoded))

                for (row, geocodage) in zipped:
                    output_row = {**row, **parse_geocodage(geocodage)}
                    writer.write_row_dict(output_row)

            except Exception as e:
                # Un problème est survenu pendant le géocodage en masse
                # On print l'exception et on écrit les données initiales
                # sans géocodage
                print(e)
                for row in rows:
                    output_row = {**row, **empty_geocodage()}
                    writer.write_row_dict(output_row)


def to_float(v):
    try:
        return float(v)
    except ValueError:
        return None


def empty_geocodage():
    return {
        GEOCODED_LATITUDE: None,
        GEOCODED_LONGITUDE: None,
        GEOCODED_RESULT_SCORE: None,
        GEOCODED_RESULT_TYPE: None,
        ADRESSE_ID: None
    }


def parse_geocodage(geocodage):
    """
    Parse le résultat renvoyé par adresse.data.gouv
    Convertit les champs latitude, longitude, et score
    en float
    """
    r = empty_geocodage()
    latitude = geocodage.get("latitude")
    if latitude:
        r[GEOCODED_LATITUDE] = to_float(latitude)
    longitude = geocodage.get("longitude")
    if longitude:
        r[GEOCODED_LONGITUDE] = to_float(longitude)
    result_score = geocodage.get("result_score")
    if result_score:
        r[GEOCODED_RESULT_SCORE] = to_float(result_score)
        if (geocodage.get("result_type") == precisions.HOUSENUMBER) and \
                (r[GEOCODED_RESULT_SCORE] > SCORE_THRESHOLD):
            r[ADRESSE_ID] = geocodage.get("result_id")
    r[GEOCODED_RESULT_TYPE] = geocodage.get("result_type")
    return r
