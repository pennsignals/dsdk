# -*- coding: utf-8 -*-
"""Utils."""

from __future__ import annotations

import pickle
from collections import OrderedDict
from datetime import datetime

from configargparse import ArgParser
from pandas import DataFrame
from pandas import concat as pd_concat

try:
    # Since not everyone will use mssql
    from sqlalchemy import create_engine
    from sqlalchemy.engine.base import Engine
except ImportError:
    create_engine = None
    Engine = None

try:
    # Since not everyone will use mongo
    from bson.objectid import ObjectId
    from pymongo import MongoClient
except ImportError:
    ObjectId = None
    MongoClient = None


def get_base_config() -> ArgParser:
    """Get the base configuration parser."""
    config_parser = ArgParser(
        default_config_files=[
            "/local/config.yaml",
            "/secrets/config.yaml",
            "secrets.yaml",
            "local.yaml",
        ],
        ignore_unknown_config_file_keys=True,
    )
    config_parser.add(
        "-c",
        "--config",
        is_config_file=True,
        help="config file path",
        env_var="CONFIG_PATH",
    )
    return config_parser


def get_mongo_connection(uri: str) -> MongoClient:
    """Get mongo connection.

    uri (str): e.g.
        mongodb://user:pass@host1,host2,host3/database?replicaSet=replica&authSource=admin
    """
    return MongoClient(uri)


def get_mssql_connection(uri: str) -> Engine:
    r"""Get mssql connection.

    uri (str): e.g.
        mssql+pymssql://domain\\user:pass@host:port/database?timeout=timeout
        Domain and timeout are optional. See sqlalchemy docs for
        additional options.
    """
    return create_engine(uri)


def get_model(model_path: str) -> object:
    """Get model from path."""
    with open(model_path, "rb") as fin:
        return pickle.load(fin)


def create_new_batch(mongo, *, time=None, **kwargs) -> ObjectId:
    """Create new batch."""
    if time is None:
        time = datetime.utcnow()

    document = {"time": time}
    document.update(kwargs)

    oid = mongo.batch.insert_one(document).inserted_id
    return oid


def get_res_with_values(query, values, conn) -> list:
    """Get result from query with values."""
    res = conn.execute(query, values)
    data = res.fetchall()
    data_d = [dict(r.items()) for r in data]
    return data_d


def chunks(lst, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]  # noqa: E203


def chunk_res_with_values(
    query, ids, conn, chunk_size=10000, params=None
) -> DataFrame:
    """Chunk query result values."""
    if params is None:
        params = {}
    res = []
    for sub_ids in chunks(ids, chunk_size):
        params.update({"ids": sub_ids})
        res.append(DataFrame(get_res_with_values(query, params, conn)))
    return pd_concat(res, ignore_index=True)


class WriteOnceDict(OrderedDict):
    """Write Once Dict."""

    def __setitem__(self, key, value):
        """__setitem__."""
        if key in self:
            raise KeyError("{} has already been set".format(key))
        super(WriteOnceDict, self).__setitem__(key, value)
