# -*- coding=utf-8 -*-

"""
Transformers are building blocks for Python recipes. They operate
on individudal rows, cells or dataframes
"""

import numpy as np
from bulk_geocoding import geocode as bulk_geocode


def geocode(df, address_attrs, code_insee_attr):

    df = df.replace({np.nan: None})
    rows = df.to_dict(orient="records")

    payload = [
        dict((address_attr, row[address_attr])
             for address_attr
             in address_attrs)
        for row in rows]

    geocoded = bulk_geocode(
        payload,
        columns=address_attrs,
        citycode=code_insee_attr)

    zipped = list(zip(rows, geocoded))

    result = []

    for (row, geocodage) in zipped:
        latitude = geocodage["latitude"]
        row["geocoded_latitude"] = float(latitude) \
            if latitude else None
        longitude = geocodage["longitude"]
        row["geocoded_longitude"] = float(longitude) \
            if longitude else None
        result_score = geocodage["result_score"]
        row["geocoded_result_score"] = float(result_score) \
            if result_score else None
        row["geocoded_result_type"] = geocodage["result_type"]
        row["geocoded_result_id"] = geocodage["result_id"]
        result.append(row)

    return result
