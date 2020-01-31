# -*- coding: utf-8 -*-
"""Mongo."""

from __future__ import annotations

from argparse import Namespace
from contextlib import contextmanager
from datetime import datetime
from functools import partial, wraps
from logging import NullHandler, getLogger
from typing import Any, Sequence
from warnings import warn

from configargparse import ArgParser as ArgumentParser
from pandas import DataFrame

from .service import Batch, Service

try:
    # Since not everyone will use mongo
    from bson.objectid import ObjectId
    from pymongo import MongoClient
    from pymongo.database import Database
except ImportError:
    MongoClient = None
    ObjectId = None
    Database = None


logger = getLogger(__name__)
logger.addHandler(NullHandler())


def create_new_batch(mongo, *, time=None, **kwargs) -> ObjectId:
    """Create new batch."""
    warn("Use dsdk.mongo.EvidenceMixin or a component.", DeprecationWarning)
    # TODO batch must be stores *after* work is done.
    # TODO why allow unvalidated time to be passed in?
    #    Start (and missing end) of the running interval shouldn't fudgable.
    # TODO interval suggests a contextmanager.
    if time is None:
        # TODO is non-naive time serializable?
        # TODO does non-naive time work with Dataframe times, NaT?
        # TODO remove naive time
        time = datetime.utcnow()
        # time = datetime.now(timezone.utc)

    document = {"time": time}
    document.update(kwargs)

    oid = mongo.batch.insert_one(document).inserted_id
    return oid


class Mixin(Service):  # pylint: disable=abstract-method
    """Mixin."""

    @classmethod
    def add_arguments(cls, parser: ArgumentParser) -> None:
        """Add arguments."""
        super().add_arguments(parser)
        parser.add(
            "--mongouri",
            required=True,
            help="Mongo URI used to connect to MongoDB",
            env_var="MONGO_URI",
        )

    def setup(self, args: Namespace) -> None:
        """Setup."""
        warn("Use dsdk.mongo:open_database.", DeprecationWarning)
        super().setup(args)
        self.mongo = MongoClient(args.mongouri).get_database()


class EvidenceMixin(Mixin):
    """Evidence Mixin."""

    def new_batch(self):
        """New batch."""
        return Batch(ObjectId(), self.info)

    def store_batch(self, batch: Batch):
        """Store batch."""
        raise NotImplementedError()

    def store_evidence(
        self,
        name: str,
        batch: Batch,
        evidence: Any,
        exclude: Sequence[str] = (),
    ):
        """Store Evidence."""
        if isinstance(evidence, DataFrame):
            # TODO We need to check column types and convert as needed
            # TODO Find a way to add batch__id without mutating,
            #      unmutating evidence
            evidence["batch_id"] = batch.batch_id
            columns = evidence[
                [c for c in evidence.columns if c not in exclude]
            ]
            res = self.mongo[name].insert_many(
                columns.to_dict(orient="records")
            )
            assert columns.shape[0] == len(
                res.inserted_ids
            )  # TODO: Better exception
            evidence.drop(columns=["batch_id"], inplace=True)
        else:
            raise NotImplementedError(
                "Serialization is not implemented for type {}".format(
                    type(evidence)
                )
            )  # TODO: Is there a better way to handle this?
        batch.evidence[name] = evidence


@contextmanager
def open_database(
    uri: str, document_class=dict, tz_aware=True, connect=True, **kwargs
) -> Database:
    """Contextmanager for database.

    Ensures that the mongo connection is opened and closed.

    uri:
        mongodb://user:pass@host1,host2,host3/database?replicaSet=replica&authSource=admin

    Like any uri, components (like user and pass) must be urlencoded to
        prevent special characters (like the slash following user's
        domain or an '@' in a password') from creating an invalid uri.
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


def needs_batch_id(func):
    """Wrapper used to create a batch if it doesn't already exist."""
    warn("Use dsdk.Service.run to create the batch.", DeprecationWarning)

    def wrapper(self, batch, *args, **kwargs):
        if not hasattr(batch, "batch_id"):
            batch.batch_id = create_new_batch(
                batch.mongo, time=batch.start_time, **batch.extra_batch_info
            )
        return func(self, batch, *args, **kwargs)

    return wrapper


def store_evidence(func=None, *, exclude_cols=None):
    """Store evidence."""
    warn("Use dsdk.mongo.EvidenceMixin.store_evidence.", DeprecationWarning)

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
