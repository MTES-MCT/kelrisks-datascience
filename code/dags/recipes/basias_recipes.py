# -*- coding=utf-8 -*-

import re
import numpy as np
from sqlalchemy import Column, BigInteger, Float, String, \
    Integer, func
from geoalchemy2 import Geometry
from bulk_geocoding import geocode as bulk_geocode

from datasets import Dataset
from utils import merge_dtype, row2dict
import precisions
from constants import LAMBERT2, WGS84
from config import DEPARTEMENTS
import transformers.basias_transformers as transformers


def filter_departements():
    """
    Keep only departements specified in config
    """

    # input datasets
    basias_sites_source = Dataset("etl", "basias_sites_source")
    basias_localisation_source = Dataset("etl", "basias_localisation_source")
    basias_cadastre_source = Dataset("etl", "basias_cadastre_source")

    # output datasets
    basias_sites_filtered = Dataset("etl", "basias_sites_filtered")
    basias_localisation_filtered = Dataset(
        "etl", "basias_localisation_filtered")
    basias_cadastre_filtered = Dataset("etl", "basias_cadastre_filtered")

    datasets = [
        (basias_sites_source, basias_sites_filtered),
        (basias_localisation_source, basias_localisation_filtered),
        (basias_cadastre_source, basias_cadastre_filtered)]

    r = re.compile("[A-Z]{3}(\d{2})\d{4}")

    def keep_row(row):
        m = r.match(row["indice_departemental"])
        if m:
            groups = m.groups()
            if groups:
                departement = groups[0]
                if departement in DEPARTEMENTS:
                    return True
        return False

    for (source, filtered) in datasets:

        # write output schemas
        filtered.write_dtype([
            Column("id", BigInteger(), primary_key=True, autoincrement=True),
            *source.read_dtype(
                primary_key="indice_departemental")])

        with filtered.get_writer() as writer:
            for row in source.iter_rows():
                if keep_row(row):
                    writer.write_row_dict(row)


def prepare_sites():
    """
    This recipe add a BigInteger field id and
    keep only particular columns
    """

    # input dataset
    basias_sites_filtered = Dataset("etl", "basias_sites_filtered")

    # output dataset
    basias_sites_prepared = Dataset("etl", "basias_sites_prepared")

    # columns to keep
    keep = [
        "indice_departemental",
        "nom_usuel",
        "raison_sociale"]

    dtype = basias_sites_filtered.read_dtype()

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
        for row in basias_sites_filtered.iter_rows():
            output_row = dict(
                (key, row[key])
                for key in row
                if key in keep)
            writer.write_row_dict(output_row)


def geocode():

    # input dataset
    basias_localisation_filtered = Dataset(
        "etl", "basias_localisation_filtered")

    # output dataset
    basias_localisation_geocoded = Dataset(
        "etl", "basias_localisation_geocoded")

    # write output schema
    dtype = basias_localisation_filtered.read_dtype()

    output_dtype = [
        *dtype,
        Column("geocoded_latitude", Float(precision=10)),
        Column("geocoded_longitude", Float(precision=10)),
        Column("geocoded_result_score", Float()),
        Column("geocoded_result_type", String()),
        Column("adresse_id", String())
    ]

    basias_localisation_geocoded.write_dtype(output_dtype)

    with basias_localisation_geocoded.get_writer() as writer:

        for df in basias_localisation_filtered.get_dataframes(chunksize=50):

            df = df.replace({np.nan: None})

            rows = df.to_dict(orient="records")

            # numero is converted to float by pandas because it
            # contains null values. Convert it back to integer
            # Cf https://stackoverflow.com/questions/21287624/
            # convert-pandas-column-containing-nans-to-dtype-int/21290084
            for row in rows:
                row["numero"] = int(row["numero"]) if row["numero"] else None
                row["nom_voie"] = row["nom_voie"][:150] \
                    if row["nom_voie"] \
                    else None

            payload = [{
                "numero": row["numero"],
                "bister": row["bister"],
                "type_voie": row["type_voie"],
                "nom_voie": row["nom_voie"],
                "numero_insee": row["numero_insee"]
            } for row in rows]

            try:
                geocoded = bulk_geocode(
                    payload,
                    columns=["numero", "bister", "type_voie", "nom_voie"],
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
                    if geocodage["result_type"] == precisions.HOUSENUMBER \
                            and float(geocodage["result_score"]) > 0.6:
                        row["adresse_id"] = geocodage["result_id"]
                    else:
                        row["adresse_id"] = None
                    writer.write_row_dict(row)
            except Exception as e:
                print(e)


def merge_geog():
    """
    Select best geography information
    """
    pass

    # Input dataset
    basias_localisation_geocoded = Dataset(
        "etl", "basias_localisation_geocoded")

    # Output dataset
    basias_localisation_geog_merged = Dataset(
        "etl", "basias_localisation_geog_merged")

    basias_localisation_geog_merged.write_dtype([
        *basias_localisation_geocoded.read_dtype(),
        Column("geog", Geometry(srid=4326)),
        Column("geog_precision", String),
        Column("geog_source", String)])

    BasiasLocalisationGeocoded = basias_localisation_geocoded.reflect()

    session = basias_localisation_geocoded.get_session()

    point_lambert2_etendue = func.ST_Transform(
        func.ST_setSRID(
            func.ST_MakePoint(
                BasiasLocalisationGeocoded.x_lambert2_etendue,
                BasiasLocalisationGeocoded.y_lambert2_etendue
            ), LAMBERT2), WGS84)

    point_adresse = func.ST_Transform(
        func.ST_setSRID(
            func.ST_MakePoint(
                BasiasLocalisationGeocoded.xl2_adresse,
                BasiasLocalisationGeocoded.yl2_adresse
            ), LAMBERT2), WGS84)

    point_geocoded = func.ST_setSRID(
        func.ST_MakePoint(
            BasiasLocalisationGeocoded.geocoded_longitude,
            BasiasLocalisationGeocoded.geocoded_latitude), WGS84)

    q = session.query(
        BasiasLocalisationGeocoded,
        point_lambert2_etendue,
        point_adresse,
        point_geocoded).all()

    with basias_localisation_geog_merged.get_writer() as writer:
        for (row, p_lambert2_etendue, p_adresse, p_geocoded) in q:
            output_row = {
                **row2dict(row),
                "geog": None,
                "geog_precision": None,
                "geog_source": None}
            if p_lambert2_etendue is not None:
                # assert this data is accurate
                output_row["geog"] = p_lambert2_etendue
                output_row["geog_precision"] = precisions.PARCEL
                output_row["geog_source"] = "lambert2_etendue"
            elif p_adresse is not None and row.precision_adresse == "numéro":
                output_row["geog"] = p_adresse
                output_row["geog_precision"] = precisions.HOUSENUMBER
                output_row["geog_source"] = "adresse"
            elif p_geocoded is not None and row.geocoded_result_score > 0.6 \
                    and row.geocoded_result_type == precisions.HOUSENUMBER:
                output_row["geog"] = p_geocoded
                output_row["geog_precision"] = precisions.HOUSENUMBER
                output_row["geog_source"] = "geocodage"

            writer.write_row_dict(output_row)

    session.close()


def intersect():
    """
    Find the closest parcelle to the point and set it as
    new geography
    """

    # Input dataset
    basias_localisation_geog_merged = Dataset(
        "etl", "basias_localisation_geog_merged")
    cadastre = Dataset("etl", "cadastre")

    # Output dataset
    basias_localisation_intersected = Dataset(
        "etl", "basias_localisation_intersected")

    dtype = basias_localisation_geog_merged.read_dtype()
    basias_localisation_intersected.write_dtype(dtype)

    Cadastre = cadastre.reflect()

    BasiasLocalisationGeogMerged = basias_localisation_geog_merged.reflect()
    session = basias_localisation_geog_merged.get_session()

    stmt = session.query(Cadastre.geog) \
                  .filter(func.st_dwithin(
                      Cadastre.geog,
                      BasiasLocalisationGeogMerged.geog,
                      0.0001)) \
                  .order_by(func.st_distance(
                      Cadastre.geog,
                      BasiasLocalisationGeogMerged.geog)) \
                  .limit(1) \
                  .label("nearest")

    q = session.query(BasiasLocalisationGeogMerged, stmt).yield_per(500)

    with basias_localisation_intersected.get_writer() as writer:
        for (row, cadastre_geog) in q:
            if cadastre_geog is not None:
                row.geog = cadastre_geog
            writer.write_row_dict(row2dict(row))

    session.close()


def join_localisation_cadastre():

    # Input datasets
    basias_localisation_intersected = Dataset(
        "etl", "basias_localisation_intersected")
    basias_cadastre_merged = Dataset(
        "etl", "basias_cadastre_merged")

    # Output dataset
    basias_localisation_with_cadastre = Dataset(
        "etl", "basias_localisation_with_cadastre")

    basias_localisation_with_cadastre.write_dtype(
        basias_localisation_intersected.read_dtype())

    BasiasLocalisationIntersected = basias_localisation_intersected.reflect()
    BasiasCadastreMerged = basias_cadastre_merged.reflect()

    session = basias_localisation_intersected.get_session()

    cond = BasiasCadastreMerged.indice_departemental \
        == BasiasLocalisationIntersected.indice_departemental

    q = session.query(
            BasiasLocalisationIntersected,
            BasiasCadastreMerged.geog) \
        .join(BasiasCadastreMerged, cond, isouter=True) \
        .all()

    with basias_localisation_with_cadastre.get_writer() as writer:
        for (row, cadastre_geog) in q:
            if cadastre_geog is not None:
                # replace geog with cadastre geog
                row.geog = cadastre_geog
                row.geog_precision = precisions.PARCEL
                row.geog_source = "cadastre"
            writer.write_row_dict(row2dict(row))


def parse_cadastre():
    """
    This recipe parse cadastre information which is dirty
    Exemple:
    ZA 128 et 146
    ? 131
    AH 575-574-43-224 etc
    """

    # input dataset
    basias_cadastre_filtered = Dataset(
        "etl", "basias_cadastre_filtered")
    basias_localisation_filtered = Dataset(
        "etl", "basias_localisation_filtered")

    # output dataset
    basias_cadastre_parsed = Dataset("etl", "basias_cadastre_parsed")

    # join basias_cadastre with basias_sites to get numero_insee
    BasiasCadastreFiltered = basias_cadastre_filtered.reflect()
    BasiasLocalisationFiltered = basias_localisation_filtered.reflect()

    session = basias_cadastre_filtered.get_session()

    # write output schema
    output_dtype = [
        Column("id", BigInteger, primary_key=True, autoincrement=True),
        Column("indice_departemental", String),
        Column("commune", String),
        Column("section", String),
        Column("numero", String)]

    basias_cadastre_parsed.write_dtype(output_dtype)

    # We need to join with basias_localiation_source because the table
    # basias_cadastre_source does not contains insee code
    q = session \
        .query(BasiasCadastreFiltered, BasiasLocalisationFiltered.numero_insee) \
        .join(
            BasiasLocalisationFiltered,
            BasiasCadastreFiltered.indice_departemental ==
            BasiasLocalisationFiltered.indice_departemental,
            isouter=True) \
        .all()

    with basias_cadastre_parsed.get_writer() as writer:
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


def add_geog():
    """
    join table with basol_cadastre_parsed with table cadastre
    to retrive geog field and discard invalid parcelles
    """

    # Input dataset
    basias_cadastre_parsed = Dataset("etl", "basias_cadastre_parsed")
    cadastre = Dataset("etl", "cadastre")

    # Output dataset
    basias_cadastre_with_geog = Dataset("etl", "basias_cadastre_with_geog")

    BasiasCadastreParsed = basias_cadastre_parsed.reflect()
    Cadastre = cadastre.reflect()

    basias_cadastre_with_geog.write_dtype([
        *basias_cadastre_parsed.read_dtype(),
        Column("geog", Geometry(srid=4326))
    ])

    session = basias_cadastre_parsed.get_session()

    cond = (BasiasCadastreParsed.commune == Cadastre.commune) & \
           (BasiasCadastreParsed.section == Cadastre.section) & \
           (BasiasCadastreParsed.numero == Cadastre.numero)

    q = session.query(BasiasCadastreParsed, Cadastre.geog) \
        .join(Cadastre, cond) \
        .yield_per(500)

    with basias_cadastre_with_geog.get_writer() as writer:

        for (row, geog) in q:
            output_row = {
                **row2dict(row),
                "geog": None}
            del output_row["id"]
            if geog is not None:
                output_row["geog"] = geog

            writer.write_row_dict(output_row)

    session.close()


def merge_cadastre_geog():
    """ Merge parcelle for a same Basias site into a MULTIPOLYGON """

    # Input dataset
    basias_cadastre_with_geog = Dataset("etl", "basias_cadastre_with_geog")

    # Output dataset
    basias_cadastre_merged = Dataset("etl", "basias_cadastre_merged")

    dtype = [
        Column("id", BigInteger, primary_key=True, autoincrement=True),
        Column("indice_departemental", String),
        Column("geog", Geometry(srid=4326))
    ]
    basias_cadastre_merged.write_dtype(dtype)
    basias_cadastre_merged.write_dtype(dtype)

    BasiasCadastreWithGeog = basias_cadastre_with_geog.reflect()

    session = basias_cadastre_with_geog.get_session()

    select = [
        BasiasCadastreWithGeog.indice_departemental,
        func.st_multi(func.st_union(BasiasCadastreWithGeog.geog))
    ]

    q = session.query(*select) \
               .group_by(BasiasCadastreWithGeog.indice_departemental) \
               .all()

    with basias_cadastre_merged.get_writer() as writer:
        for (indice_departemental, geog) in q:
            row = {
                "indice_departemental": indice_departemental,
                "geog": geog}
            writer.write_row_dict(row)

    session.close()


def join_sites_localisation():

    # datasets to join
    basias_sites_prepared = Dataset("etl", "basias_sites_prepared")
    basias_localisation_with_cadastre = Dataset(
        "etl", "basias_localisation_with_cadastre")

    # output dataset
    basias_sites_localisation_joined = Dataset(
        "etl", "basias_sites_localisation_joined")

    # transform types
    output_dtype = [
        *basias_sites_prepared.read_dtype(),
        *basias_localisation_with_cadastre.read_dtype()]

    output_dtype = merge_dtype(output_dtype)

    basias_sites_localisation_joined.write_dtype(output_dtype)

    # transform data
    BasiasSitesPrepared = basias_sites_prepared.reflect()
    BasiasLocalisation = basias_localisation_with_cadastre.reflect()

    session = basias_sites_prepared.get_session()

    join_query = session \
        .query(BasiasSitesPrepared, BasiasLocalisation) \
        .join(BasiasLocalisation,
              BasiasSitesPrepared.indice_departemental ==
              BasiasLocalisation.indice_departemental,
              isouter=True) \
        .all()

    with basias_sites_localisation_joined.get_writer() as writer:
        for (site, localisation) in join_query:
            output_row = {c.name: None for c in output_dtype}
            output_row = {**output_row, **row2dict(site)}
            if localisation:
                output_row = {**output_row, **row2dict(localisation)}
            writer.write_row_dict(output_row)

    session.close()


def add_commune():
    """
    set commune geog for all records where geog is not set with
    a better precision
    """

    # input dataset
    basias_sites_localisation_joined = Dataset(
        "etl", "basias_sites_localisation_joined")
    communes = Dataset("etl", "commune")

    # output dataset
    basias_sites_with_commune = Dataset("etl", "basias_sites_with_commune")

    dtype = basias_sites_localisation_joined.read_dtype()
    basias_sites_with_commune.write_dtype(dtype)

    BasiasSites = basias_sites_localisation_joined.reflect()
    Commune = communes.reflect()

    session = basias_sites_localisation_joined.get_session()

    q = session.query(BasiasSites, Commune.geog) \
               .join(Commune,
                     BasiasSites.numero_insee == Commune.insee,
                     isouter=True) \
               .all()

    with basias_sites_with_commune.get_writer() as writer:

        for (row, commune) in q:

            if row.geog is None:
                row.geog = commune
                row.geog_precision = precisions.MUNICIPALITY
                row.geog_source = "numero_insee"

            writer.write_row_dict(row2dict(row))

    session.close()


def add_version():
    """ Add a version column for compatibility with Spring """

    # Input dataset
    basias_sites_localisation_joined = Dataset(
        "etl", "basias_sites_with_commune")

    # Output dataset
    basias_sites_with_version = Dataset(
        "etl", "basias_sites_with_version")

    basias_sites_with_version.write_dtype([
        *basias_sites_localisation_joined.read_dtype(),
        Column("version", Integer)
    ])

    with basias_sites_with_version.get_writer() as writer:
        for row in basias_sites_localisation_joined.iter_rows():
            writer.write_row_dict(row)


def check():
    """ Perform sanity checks on table """

    # check we have same number of records
    # than data filtered

    basias = Dataset("etl", "basias")
    Basias = basias.reflect()
    session = basias.get_session()
    basias_count = session.query(Basias).count()
    session.close()

    basias_filtered = Dataset("etl", "basias_sites_filtered")
    BasiasFiltered = basias_filtered.reflect()
    session = basias_filtered.get_session()
    basias_filtered_count = session.query(BasiasFiltered).count()
    session.close()

    assert basias_count > 0
    assert basias_count == basias_filtered_count