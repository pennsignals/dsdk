# -*- coding: utf-8 -*-
"""Mongo."""

from __future__ import annotations

from abc import ABC
from contextlib import contextmanager
from logging import INFO, LoggerAdapter, NullHandler, basicConfig, getLogger
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generator,
    Optional,
    Sequence,
    cast,
)

from configargparse import ArgParser as ArgumentParser

from .service import Batch, Model, Service
from .utils import retry

try:
    # Since not everyone will use mongo
    from bson.objectid import ObjectId
    from pymongo import MongoClient
    from pymongo.collection import Collection
    from pymongo.database import Database
    from pymongo.errors import AutoReconnect
except ImportError:
    MongoClient = None
    ObjectId = None
    Database = None
    AutoReconnect = None

# TODO Add import calling function from parent application
extra = {"callingfunc": ""}
logger = getLogger(__name__)
FORMAT = '%(asctime)-15s - %(name)s - %(levelname)s - {"callingfunc": "%(callingfunc)s", "module": "%(module)s", "function": "%(funcName)s", %(message)s}'
basicConfig(format=FORMAT)
logger.setLevel(INFO)
# Add extra kwargs to message format
logger.addHandler(NullHandler())
logger = LoggerAdapter(logger, extra)


if TYPE_CHECKING:
    BaseMixin = Service
else:
    BaseMixin = ABC


class Mixin(BaseMixin):
    """Mixin."""

    def __init__(self, *, mongo_uri: Optional[str] = None, **kwargs):
        """__init__."""
        # inferred type of self._mongo_uri must not be optional...
        self._mongo_uri = cast(str, mongo_uri)
        super().__init__(**kwargs)

        # ... because self._mongo_uri is not optional
        assert self._mongo_uri is not None

    def inject_arguments(self, parser: ArgumentParser) -> None:
        """Inject arguments."""
        super().inject_arguments(parser)

        def _inject_mongo_uri(mongo_uri: str) -> str:
            self._mongo_uri = mongo_uri
            return mongo_uri

        parser.add(
            "--mongo-uri",
            required=True,
            help=(
                "Mongo URI used to connect to a Mongo database: "
                "mongodb://USER:PASS@HOST1,HOST2,.../DATABASE?"
                "replicaset=REPLICASET&authsource=admin "
                "Url encode all parts: PASS in particular"
            ),
            env_var="MONGO_URI",
            type=_inject_mongo_uri,
        )

    @contextmanager
    def open_mongo(self) -> Generator:
        """Open mongo."""
        with open_database(self._mongo_uri) as database:
            yield database


class EvidenceMixin(Mixin):
    """Evidence Mixin."""

    def __init__(self, **kwargs):
        """__init__."""
        super().__init__(**kwargs)

    @contextmanager
    def open_batch(
        self, key: Any = None, model: Optional[Model] = None
    ) -> Generator[Batch, None, None]:
        """Open batch."""
        if key is None:
            key = ObjectId()
        with super().open_batch(key) as batch:
            doc = batch.as_insert_doc(model)  # <- model dependency
            with self.open_mongo() as database:
                key = insert_one(database.batches, doc)
                logger.info(
                    '"action": "insert": "database": "%s", "collection": "%s"',
                    database.name,
                    database.collection.name,
                )
            yield batch

            key, doc = batch.as_update_doc()
            with self.open_mongo() as database:
                update_one(database.batches, key, doc)
                logger.info(
                    '"action": "update", "database": "%s", "collection": "%s"',
                    database.name,
                    database.collection.name,
                )

    def store_evidence(self, batch: Batch, *args, **kwargs) -> None:
        """Store Evidence."""
        super().store_evidence(batch, *args, **kwargs)
        exclude = kwargs.get("exclude", ())
        while args:
            key, df, *args = args  # type: ignore
            # TODO We need to check column types and convert as needed
            # TODO Find a way to add batch_id without mutating df
            df["batch_id"] = batch.key
            columns = df[[c for c in df.columns if c not in exclude]]
            docs = columns.to_dict(orient="records")
            with self.open_mongo() as database:
                result = insert_many(database[key], docs)
                assert columns.shape[0] == len(
                    result.inserted_ids
                ), logger.error(
                    '"action" "insert_many", "database": "%s", "collection": "%s", \
                        "message": "columns.shape[0] != len(results.inserted_ids)"',
                    database.name,
                    database.collection.name,
                )

                # TODO: Better exception
            df.drop(columns=["batch_id"], inplace=True)
            logger.info(
                '"action": "insert_many", "database": "%s", "collection": "%s", "count": %s"',
                database.name,
                database.collection.name,
                len(df.index),
            )


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
    Do not urlencode the entire uri.
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
            '{"opened_mongo_database: {"name": "%s", "is_master": "%s"}}',
            database.name,
            is_master,
        )
        try:
            yield database
        finally:
            logger.debug(
                '{"close_mongo_database: {"name": "%s"}}', database.name
            )


@retry(AutoReconnect)
def insert_one(collection: Collection, doc: Dict[str, Any]):
    """Insert one with retry."""
    return collection.insert_one(doc)


@retry(AutoReconnect)
def insert_many(collection: Collection, docs: Sequence[Dict[str, Any]]):
    """Insert many with retry."""
    return collection.insert_many(docs)


@retry(AutoReconnect)
def update_one(
    collection: Collection, key: Dict[str, Any], doc: Dict[str, Any]
):
    """Update one with retry."""
    return collection.update_one(key, doc)
