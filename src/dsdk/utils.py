import pickle

import configargparse


def get_base_config():
    """Helper function to get the configuration.

    :returns a configargparse.ArgParser
    """
    config_parser = configargparse.ArgParser(
        default_config_files=[
            "/local/config.yaml",
            "/secrets/config.yaml",
            "secrets.yaml",
            "local.yaml",
        ],
        ignore_unknown_config_file_keys=True,
    )
    config_parser.add(
        "-c", "--config", is_config_file=True, help="config file path", env_var="CONFIG_PATH"
    )
    return config_parser


def get_mongo_connection(uri):
    """Helper function to connect to mongodb

    uri (str): e.g.
    mongodb://user:pass@host1,host2,host3/database?replicaSet=replica&authSource=admin

    :returns a MongoClient
    """
    # Since not everyone will use mongo
    from pymongo import MongoClient

    return MongoClient(uri)


def get_mssql_connection(uri):
    """Helper function to connect to mssql

    uri (str): e.g. mssql+pymssql://domain\\user:pass@host:port/database?timeout=timeout
               domain and timeout are optional. See sqlalchemy docs for additional options.

    :returns a sqlalchemy engine
    """
    # Since not everyone will use mssql
    from sqlalchemy import create_engine

    return create_engine(uri)


def get_model(model_path):
    with open(model_path, "rb") as f:
        return pickle.load(f)
