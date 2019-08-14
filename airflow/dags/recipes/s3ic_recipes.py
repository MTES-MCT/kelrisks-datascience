# -*- coding=utf-8 -*-

import numpy as np
from sqlalchemy import Column, BigInteger, Float, String

from datasets import Dataset
from transformers.common_transformers import geocode as geocode_transformer


def geocode():
    """ Geocode S3IC adresses """

    # input dataset
    s3ic_source = Dataset("etl", "s3ic_source")

    # output dataset
    s3ic_geocoded = Dataset("etl", "s3ic_geocoded")

    # write output schema
    dtype = s3ic_source.read_dtype(
        primary_key="code")

    output_dtype = [
        Column("id", BigInteger(), primary_key=True, autoincrement=True),
        *dtype,
        Column("geocoded_latitude", Float(precision=10)),
        Column("geocoded_longitude", Float(precision=10)),
        Column("geocoded_result_score", Float()),
        Column("geocoded_result_type", String()),
        Column("geocoded_result_id", String())
    ]

    s3ic_geocoded.write_dtype(output_dtype)

    with s3ic_geocoded.get_writer() as writer:

        for df in s3ic_source.get_dataframes(chunksize=100):

            rows = geocode_transformer(
                df,
                ["adresse", "complement_adresse"],
                "code_insee")

            for row in rows:
                writer.write_row_dict(row)