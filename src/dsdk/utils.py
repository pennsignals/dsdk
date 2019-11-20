import pickle
from collections import OrderedDict
from datetime import datetime

import configargparse
from pandas import DataFrame
from pandas import concat as pd_concat


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


def create_new_batch(mongo, date=None):
    if date is None:
        date = datetime.now()
        date = datetime(date.year, date.month, date.day)

    oid = mongo.batch.insert_one({"date": date}).inserted_id
    return oid


def get_res_with_values(q, values, conn):
    res = conn.execute(q, values)
    data = res.fetchall()
    data_d = [dict(r.items()) for r in data]
    return data_d


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i : i + n]  # noqa: E203


def chunk_res_with_values(query, ids, conn, chunk_size=10000, params=None):
    if params is None:
        params = {}
    res = []
    for sub_ids in chunks(ids, chunk_size):
        params.update({"ids": sub_ids})
        res.append(DataFrame(get_res_with_values(query, params, conn)))
    return pd_concat(res, ignore_index=True)


class WriteOnceDict(OrderedDict):
    def __setitem__(self, key, value):
        if key in self:
            raise KeyError("{} has already been set".format(key))
        super(WriteOnceDict, self).__setitem__(key, value)
