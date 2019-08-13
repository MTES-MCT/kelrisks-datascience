# -*- coding: utf-8 -*-

from airflow.hooks.data_preparation import PostgresDataset

from config import CONN_ID


# Default arguments for datasets in schema etl
etl_default_args = {
    "postgres_conn_id": CONN_ID,
    "schema": "etl"
}

# Default arguments for datasets in schema kelrisks
kelrisks_default_args = {
    "postgres_conn_id": CONN_ID,
    "schema": "kelrisks"
}

# List of datasets in schema etl
etl_datasets = [
    PostgresDataset(name="cadastre", **etl_default_args),
    PostgresDataset(name="sis_source", **etl_default_args),
    PostgresDataset(name="basias_sites_source", **etl_default_args),
    PostgresDataset(name="basias_localisation_source", **etl_default_args),
    PostgresDataset(name="basias_geocoded", **etl_default_args),
    PostgresDataset(name="basias_cadastre_source", **etl_default_args),
    PostgresDataset(name="basias_sites_prepared", **etl_default_args),
    PostgresDataset(name="basias_joined", **etl_default_args),
    PostgresDataset(name="basias_with_geog", **etl_default_args),
    PostgresDataset(name="basias_parcelles", **etl_default_args),
    PostgresDataset(name="basias_with_geom_prepared", **etl_default_args),
]

# List of datasets in schema kelrisks
kelrisks_datasets = []

datasets = {
    "etl": dict((dataset.name, dataset) for dataset in etl_datasets),
    "kelrisks": dict((dataset.name, dataset) for dataset in kelrisks_datasets)
}


class DatasetDoesNotExist(Exception):

    def __init__(self, schema, name):
        msg = "Dataset {schema}.{name} does not exist" \
            .format(schema=schema, name=name)
        super().__init__(msg)


def Dataset(schema, name):
    """
    Return the dataset with the given name in a specific schema
    """
    try:
        return datasets[schema][name]
    except KeyError:
        raise DatasetDoesNotExist(schema, name)