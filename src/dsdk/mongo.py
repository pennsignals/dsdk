# -*- coding: utf-8 -*-
"""Mongo."""

from contextlib import contextmanager
from functools import partial, wraps
from logging import INFO, basicConfig, getLogger
from sys import stdout

from pandas import DataFrame

from dsdk.utils import create_new_batch

try:
    # Since not everyone will use mongo
    from pymongo import MongoClient
    from pymongo.database import Database
except ImportError:
    MongoClient = None
    Database = None

basicConfig(
    level=INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=stdout,
)
logger = getLogger(__name__)


@contextmanager
def open_database(
    uri: str, document_class=dict, tz_aware=True, connect=True, **kwargs
) -> Database:
    """Contextmanager for database.

    uri:
        mongodb://user:pass@host1,host2,host3/database?replicaSet=replica&authSource=admin

    Ensure that the mongo client connection is closed.
    """
    with MongoClient(
        uri,
        document_class=document_class,
        tz_aware=tz_aware,
        connect=connect,
        **kwargs,
    ) as client:
        database = client.get_database()
        # is_master to force lazy connection open
        is_master = client.admin.command("ismaster")
        logger.debug(
            '{"open_database: {"name": "%s", "is_master": "%s"}}',
            database.name,
            is_master,
        )
        try:
            yield database
        finally:
            logger.debug('{"close_database: {"name": "%s"}}', database.name)


# TODO: Make these wrappers classes to make them easier to customize?
def needs_batch_id(func):
    """Wrapper used to create a batch if it doesn't already exist."""

    def wrapper(self, batch, *args, **kwargs):
        if not hasattr(batch, "batch_id"):
            batch.batch_id = create_new_batch(
                batch.mongo, time=batch.start_time, **batch.extra_batch_info
            )
        return func(self, batch, *args, **kwargs)

    return wrapper


# TODO: optional parameter to specify fields that aren't retained
def store_evidence(func=None, *, exclude_cols=None):
    """Store evidence."""
    if exclude_cols is None:
        exclude_cols = []
    exclude_cols = frozenset(exclude_cols)
    if func is None:
        return partial(store_evidence, exclude_cols=exclude_cols)

    @wraps(func)
    @needs_batch_id
    def wrapper(self, batch, *args, **kwargs):
        evidence = func(self, batch, *args, **kwargs)
        if isinstance(evidence, DataFrame):
            # TODO: We need to check column types and convert as needed
            evidence["batch_id"] = batch.batch_id
            evidence_keep = evidence[
                [c for c in evidence.columns if c not in exclude_cols]
            ]
            res = batch.mongo[self.name].insert_many(
                evidence_keep.to_dict(orient="records")
            )
            assert evidence_keep.shape[0] == len(
                res.inserted_ids
            )  # TODO: Better exception
            evidence.drop(columns=["batch_id"], inplace=True)
        else:
            raise NotImplementedError(
                "Serialization is not implemented for type {}".format(
                    type(evidence)
                )
            )  # TODO: Is there a better way to handle this?
        return evidence

    return wrapper
