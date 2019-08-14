# -*- coding=utf-8 -*-

import numpy as np
from sqlalchemy import Column, String, BigInteger, Float, func
from geoalchemy2 import Geometry
from datasets import Dataset
from bulk_geocoding import geocode as bulk_geocode

import transformers.basol_transformers as transformers
from utils import row2dict
import precisions
from constants import LAMBERT2, WGS84


def parse_cadastre():

    basol_source = Dataset("etl", "basol_source")
    basol_cadastre = Dataset("etl", "basol_cadastre")

    dtype = [
        Column("id", BigInteger, primary_key=True, autoincrement=True),
        Column("numerobasol", String),
        Column("commune", String),
        Column("section", String),
        Column("numero", String)]

    basol_cadastre.write_dtype(dtype)

    with basol_cadastre.get_writer() as writer:
        for row in basol_source.iter_rows(primary_key="numerobasol"):
            cadastre_multi = row["cadastre_multi"]
            if cadastre_multi:
                parcelles = transformers.parse_cadastre(cadastre_multi)
                for parcelle in parcelles:
                    output_row = {
                        "numerobasol": row["numerobasol"],
                        **parcelle}
                    writer.write_row_dict(output_row)


def join_cadastre():
    """ Join the table basol_cadastre with cadastre table """

    # Input datasets
    basol_cadastre = Dataset("etl", "basol_cadastre")
    cadastre = Dataset("etl", "cadastre")

    # Output datasets
    basol_cadastre_joined = Dataset("etl", "basol_cadastre_joined")

    dtype = [
        *basol_cadastre.read_dtype(),
        Column("geog", Geometry(srid=4326))
    ]
    basol_cadastre_joined.write_dtype(dtype)

    BasolCadastre = basol_cadastre.reflect()
    Cadastre = cadastre.reflect()

    session = basol_cadastre.get_session()

    cond = (BasolCadastre.commune == Cadastre.commune) and \
           (BasolCadastre.section == Cadastre.section) and \
           (BasolCadastre.numero == Cadastre.numero)

    q = session.query(BasolCadastre, Cadastre.geog) \
               .join(Cadastre, cond) \
               .filter(Cadastre.geog is not None) \
               .limit(100) \
               .all()

    with basol_cadastre_joined.get_writer() as writer:

        for (basol, geog) in q:
            output_row = {
                **row2dict(basol),
                "geog": geog}
            output_row.pop("id")
            writer.write_row_dict(output_row)
    session.close()


def merge_cadastre():
    """ Merge the different parcelles into a MultiPolygon """

    # Input dataset
    basol_cadastre_joined = Dataset("etl", "basol_cadastre_joined")

    # Output dataset
    basol_cadastre_merged = Dataset("etl", "basol_cadastre_merged")

    dtype = [
        Column("id", BigInteger, primary_key=True, autoincrement=True),
        Column("numerobasol", String),
        Column("geog", Geometry(srid=4326))
    ]
    basol_cadastre_merged.write_dtype(dtype)

    BasolCadastreJoined = basol_cadastre_joined.reflect()

    session = basol_cadastre_joined.get_session()

    select = [
        BasolCadastreJoined.numerobasol,
        func.st_multi(func.st_union(BasolCadastreJoined.geog))
    ]

    q = session.query(*select) \
               .group_by(BasolCadastreJoined.numerobasol) \
               .all()

    with basol_cadastre_merged.get_writer() as writer:
        for (numerobasol, geog) in q:
            row = {
                "numerobasol": numerobasol,
                "geog": geog}
            writer.write_row_dict(row)


def geocode():
    """ Geocode Basol adresses """

    # input dataset
    basol_source = Dataset("etl", "basol_source")

    # output dataset
    basol_geocoded = Dataset("etl", "basol_geocoded")

    # write output schema
    dtype = basol_source.read_dtype(
        primary_key="numerobasol")

    output_dtype = [
        Column("id", BigInteger(), primary_key=True, autoincrement=True),
        *dtype,
        Column("geocoded_latitude", Float(precision=10)),
        Column("geocoded_longitude", Float(precision=10)),
        Column("geocoded_result_score", Float()),
        Column("geocoded_result_type", String()),
        Column("geocoded_result_id", String())
    ]

    basol_geocoded.write_dtype(output_dtype)

    with basol_geocoded.get_writer() as writer:

        for df in basol_source.get_dataframes(chunksize=100):

            df = df.replace({np.nan: None})
            rows = df.to_dict(orient="records")
            payload = [{
                "adresse": row["adresse"],
                "code_insee": row["code_insee"]
            } for row in rows]

            geocoded = bulk_geocode(
                payload,
                columns=["adresse"],
                citycode="code_insee")

            zipped = list(zip(rows, geocoded))

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
                writer.write_row_dict(row)


def normalize_precision():
    """ normalize precision fields """

    # input dataset
    basol_geocoded = Dataset("etl", "basol_geocoded")

    # output dataset
    basol_normalized = Dataset("etl", "basol_normalized")

    dtype = basol_geocoded.read_dtype()
    basol_normalized.write_dtype(dtype)

    with basol_normalized.get_writer() as writer:
        for row in basol_geocoded.iter_rows():
            normalized = transformers.normalize_precision(row)
            writer.write_row_dict(normalized)


def add_parcels():
    """ join table basol_normalized with basol_cadastre_merged """

    # input datasets
    basol_normalized = Dataset("etl", "basol_normalized")
    basol_cadastre_merged = Dataset("etl", "basol_cadastre_merged")

    # output datasets
    basol_with_parcels = Dataset("etl", "basol_with_parcels")

    BasolNormalized = basol_normalized.reflect()
    BasolCadastreMerged = basol_cadastre_merged.reflect()

    basol_with_parcels.write_dtype([
        *basol_normalized.read_dtype(),
        Column("geog", Geometry(srid=4326)),
        Column("precision", String),
        Column("geog_source", String)])

    session = basol_normalized.get_session()

    cond = (BasolNormalized.numerobasol == BasolCadastreMerged.numerobasol)
    q = session.query(BasolNormalized, BasolCadastreMerged.geog) \
               .join(BasolCadastreMerged, cond, isouter=True) \
               .all()

    with basol_with_parcels.get_writer() as writer:
        for (row, geog) in q:
            output_row = {
                **row2dict(row),
                "geog": geog,
                "precision": None,
                "geog_source": None}
            if geog is not None:
                output_row["precision"] = precisions.PARCEL
                output_row["geog_source"] = "cadastre"
            writer.write_row_dict(output_row)


def merge_geog():
    """
    Choose best precision between initial coordinates
    or geocoded coordinates if geog is not set from
    cadastre information
    """

    # Input dataset
    basol_with_parcels = Dataset("etl", "basol_with_parcels")

    # Output dataset
    basol_geog_merged = Dataset("etl", "basol_geog_merged")

    dtype = basol_with_parcels.read_dtype()
    basol_geog_merged.write_dtype(dtype)

    BasolWithParcels = basol_with_parcels.reflect()

    session = basol_with_parcels.get_session()

    point_lambert2 = func.ST_Transform(
        func.ST_setSRID(
            func.ST_MakePoint(
                BasolWithParcels.coordxlambertii,
                BasolWithParcels.coordylambertii
            ), LAMBERT2), WGS84)

    point_geocoded = func.ST_setSRID(
        func.ST_MakePoint(
            BasolWithParcels.geocoded_longitude,
            BasolWithParcels.geocoded_latitude), WGS84)

    q = session.query(BasolWithParcels, point_lambert2, point_geocoded).all()

    with basol_geog_merged.get_writer() as writer:

        for (row, point_lambert2, point_geocoded) in q:

            if row.geog is not None:
                # if geog is already set, do nothing
                pass

            elif row.l2e_precision == precisions.HOUSENUMBER:
                row.geog = point_lambert2
                row.precision = row.l2e_precision
                row.geog_source = "lambert2"

            elif (row.geocoded_result_type == precisions.HOUSENUMBER) and \
                 (row.geocoded_result_score >= 0.6):
                row.geog = point_geocoded
                row.precision = row.geocoded_result_type
                row.geog_source = "geocodage"

            writer.write_row_dict(row2dict(row))