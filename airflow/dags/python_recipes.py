# -*- coding=utf-8 -*-

from sqlalchemy import Column, BigInteger, func
from geoalchemy2 import Geometry

from datasets import Dataset
from utils import row2dict, merge_dtype

###################
# BASIAS recipes
###################


def prepare_basias_sites_recipe():
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


def join_basias_sites_localisation_recipe():

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
    BasiasJoined = basias_joined.reflect()

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
        .limit(5) \
        .all()

    with basias_with_geog.get_writer() as writer:
        for (site, geopoint) in q:
            output_row = {
                **row2dict(site),
                "geog": geopoint}
            writer.write_row_dict(output_row)
