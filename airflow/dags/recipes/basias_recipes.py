# -*- coding=utf-8 -*-

import numpy as np
from sqlalchemy import Column, BigInteger, Float, String, func
from geoalchemy2 import Geometry
from bulk_geocoding import geocode

from datasets import Dataset
from utils import merge_dtype, row2dict
import transformers.basias_transformers as transformers


def prepare_basias_sites():
    """
    This recipe add an BigInteger field id and
    keep only particular columns
    """

    # input dataset
    basias_sites_source = Dataset("etl", "basias_sites_source")

    # output dataset
    basias_sites_prepared = Dataset("etl", "basias_sites_prepared")

    # columns to keep
    keep = [
        "indice_departemental",
        "nom_usuel",
        "raison_sociale"]

    dtype = basias_sites_source.read_dtype(
        primary_key="indice_departemental")

    # transform schema
    output_dtype = [
        column
        for column in dtype
        if column.name in keep]

    id_column = Column(
        "id",
        BigInteger,
        primary_key=True,
        autoincrement=True)

    output_dtype = [id_column, *output_dtype]

    basias_sites_prepared.write_dtype(output_dtype)

    # transform data
    with basias_sites_prepared.get_writer() as writer:
        for row in basias_sites_source.iter_rows():
            output_row = dict(
                (key, row[key])
                for key in row
                if key in keep)
            writer.write_row_dict(output_row)


def geocode_basias_adresses():

    # input dataset
    basias_localisation_source = Dataset("etl", "basias_localisation_source")

    # output dataset
    basias_geocoded = Dataset("etl", "basias_geocoded")

    # write output schema
    dtype = basias_localisation_source.read_dtype(
        primary_key="indice_departemental")

    output_dtype = [
        Column("id", BigInteger(), primary_key=True, autoincrement=True),
        *dtype,
        Column("geocoded_latitude", Float(precision=10)),
        Column("geocoded_longitude", Float(precision=10)),
        Column("geocoded_result_score", Float()),
        Column("geocoded_result_type", String()),
        Column("geocoded_result_id", String())
    ]

    basias_geocoded.write_dtype(output_dtype)

    with basias_geocoded.get_writer() as writer:

        for df in basias_localisation_source.get_dataframes(chunksize=100):

            df = df.replace({np.nan: None})
            rows = df.to_dict(orient="records")
            payload = [{
                "numero": row["numero"],
                "bister": row["bister"],
                "nom_voie": row["nom_voie"],
                "numero_insee": row["numero_insee"]
            } for row in rows]

            geocoded = geocode(
                payload,
                columns=["numero", "bister", "nom_voie"],
                citycode="numero_insee")

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


def join_basias_sites_localisation():

    # datasets to join
    basias_sites_prepared = Dataset("etl", "basias_sites_prepared")
    basias_localisation_source = Dataset("etl", "basias_localisation_source")

    # output dataset
    basias_joined = Dataset("etl", "basias_joined")

    # transform types
    output_dtype = [
        *basias_sites_prepared.read_dtype(),
        *basias_localisation_source.read_dtype(
            primary_key="indice_departemental")]

    output_dtype = merge_dtype(output_dtype)

    basias_joined.write_dtype(output_dtype)

    # transform data
    BasiasSitesPrepared = basias_sites_prepared.reflect()
    BasiasLocalisationSource = basias_localisation_source.reflect()

    session = basias_sites_prepared.get_session()

    join_query = session \
        .query(BasiasSitesPrepared, BasiasLocalisationSource) \
        .join(BasiasLocalisationSource,
              BasiasSitesPrepared.indice_departemental ==
              BasiasLocalisationSource.indice_departemental) \
        .all()

    with basias_joined.get_writer() as writer:
        for (site, localisation) in join_query:
            output_row = {**row2dict(site), **row2dict(localisation)}
            writer.write_row_dict(output_row)

    session.close()


def create_basias_geopoint():

    # input dataset
    basias_joined = Dataset("etl", "basias_joined")
    BasiasJoined = basias_joined.reflect()

    # output dataset
    basias_with_geog = Dataset("etl", "basias_with_geog")

    # transform type
    dtype = basias_joined.read_dtype()
    geopoint = Column("geog", Geometry(geometry_type="POINT", srid=4326))
    dtype.append(geopoint)

    basias_with_geog.write_dtype(dtype)

    def makepoint_with_srid(x, y, srid):
        return func.st_setsird(
            func.st_makepoint(x, y),
            srid)

    # transform data
    session = basias_joined.get_session()
    q = session.query(
        BasiasJoined,
        func.st_transform(
            func.st_setsrid(
                func.st_makepoint(
                    BasiasJoined.xl2_adresse,
                    BasiasJoined.yl2_adresse), 27572), 4326)) \
        .all()

    with basias_with_geog.get_writer() as writer:
        for (site, geopoint) in q:
            output_row = {
                **row2dict(site),
                "geog": geopoint}
            writer.write_row_dict(output_row)


def extract_basias_parcelles():

    # input dataset
    basias_cadastre_source = Dataset("etl", "basias_cadastre_source")
    basias_joined = Dataset("etl", "basias_joined")

    # output dataset
    basias_parcelles = Dataset("etl", "basias_parcelles")

    # join basias_cadastre with basias_sites to get numero_insee
    BasiasCadastreSource = basias_cadastre_source.reflect(
        primary_key="indice_departemental")
    BasiasJoined = basias_joined.reflect()

    session = basias_cadastre_source.get_session()

    # write output schema
    output_dtype = [
        Column("id", BigInteger, primary_key=True, autoincrement=True),
        Column("indice_departemental", String),
        Column("commune", String),
        Column("section", String),
        Column("numero", String)]

    basias_parcelles.write_dtype(output_dtype)

    q = session \
        .query(BasiasCadastreSource, BasiasJoined.numero_insee) \
        .join(
            BasiasJoined,
            BasiasCadastreSource.indice_departemental ==
            BasiasJoined.indice_departemental) \
        .all()

    with basias_parcelles.get_writer() as writer:
        for (cadastre, numero_insee) in q:
            row = {**row2dict(cadastre), "numero_insee": numero_insee}
            parcelles = transformers.extract_basias_parcelles_from_row(row)
            for parcelle in parcelles:
                output_row = {
                    "indice_departemental": row["indice_departemental"],
                    "commune": parcelle.commune,
                    "section": parcelle.section,
                    "numero": parcelle.numero}
                writer.write_row_dict(output_row)

    session.close()
