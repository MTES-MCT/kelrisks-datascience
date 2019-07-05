
from .base import PostgresPythonTransformer, PostgresSQLTransformer


class CreateGeographyTransformer(PostgresSQLTransformer):

    input_table = 'sis_source'
    output_table = 'sis'

    sql = """
INSERT INTO kelrisks.sis (
    id_sis,
    numero_affichage,
    numero_basol,
    adresse,
    lieu_dit,
    code_insee,
    nom_commune,
    code_departement,
    nom_departement,
    surface_m2,
    x,
    y,
    geog,
    geog_centroid
)
SELECT
    id_sis,
    numero_affichage,
    numero_basol,
    adresse,
    lieu_dit,
    code_insee,
    nom_commune,
    code_departement,
    nom_departement,
    surface_m2,
    x,
    y,
    ST_GeomFromGeoJSON(geom) as geog,
    ST_SetSRID(ST_Point(x, y), 4326) as geog_centroid
from etl.sis_source
"""


class StagingTransformer(PostgresPythonTransformer):
    """
    Dummy transformer that copy data to the staging table
    TODO run some tests on the table
    """

    input_table = 'sis_with_geography'
    output_table = 'sis_prepared'

    def transform(self, data):
        return data


class DeployTransformer(PostgresPythonTransformer):
    """
    Dummy transformer that copy data from the staging table
    to the prod table in schema kelrisks
    """

    input_table = 'sis_prepared'
    output_table = 'sis'

    def transform(self, data):
        return data
