# -*- coding: utf-8 -*-
"""Mongo."""

from __future__ import annotations

from abc import ABC
from contextlib import contextmanager
from logging import INFO
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
from .utils import get_logger, retry

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

logger = get_logger(__name__, INFO)


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
            help=" ".join(
                (
                    "Mongo URI used to connect to a Mongo database:",
                    (
                        "mongodb://USER:PASSWORD@HOST1,HOST2,.../DATABASE?"
                        "replicaset=REPLICASET&authsource=admin"
                    ),
                    "Use a valid uri."
                    "Url encode all parts, but do not encode the entire uri.",
                    "No unencoded colons, ampersands, slashes,",
                    "question-marks, etc. in parts.",
                    "Specifically, check url encoding of PASSWORD.",
                )
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

    RESULTSET_ERROR = "".join(
        (
            "{",
            ", ".join(
                (
                    '"key": "mongo.resultset.error"',
                    '"collection": "%s.%s"',
                    '"actual": %s',
                    '"expected": %s',
                )
            ),
            "}",
        )
    )

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
            yield batch

        key, doc = batch.as_update_doc()
        with self.open_mongo() as database:
            update_one(database.batches, key, doc)

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
                collection = database[key]
                result = insert_many(collection, docs)
                actual = len(result.inserted_ids)
                expected = columns.shape[0]
                assert actual == expected, self.RESULTSET_ERROR % (
                    database.name,
                    collection.name,
                    actual,
                    expected,
                )

                # TODO: Better exception
            df.drop(columns=["batch_id"], inplace=True)


OPEN = '{"key": "mongo.open", "database": "%s",  "is_master": "%s" }'
CLOSE = '{"key": "mongo.close", "database": "%s"}'


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
        # force lazy connection open
        is_master = client.admin.command("ismaster")
        logger.info(OPEN, database.name, is_master)
        try:
            yield database
        finally:
            logger.info(CLOSE, database.name)


INSERT_ONE = "".join(
    (
        "{",
        ", ".join(
            (
                '"key": "mongo.insert_one"',
                '"collection": "%s.%s"',
                '"id": "%s"',
            )
        ),
        "}",
    )
)


@retry(AutoReconnect)
def insert_one(collection: Collection, doc: Dict[str, Any]):
    """Insert one with retry."""
    result = collection.insert_one(doc)
    logger.info(
        INSERT_ONE,
        collection.database.name,
        collection.name,
        result.inserted_id,
    )
    return result


INSERT_MANY = "".join(
    (
        "{",
        ", ".join(
            (
                '"key": "mongo.insert_many"',
                '"collection": "%s.%s"',
                '"value": %s',
            )
        ),
        "}",
    )
)


@retry(AutoReconnect)
def insert_many(collection: Collection, docs: Sequence[Dict[str, Any]]):
    """Insert many with retry."""
    result = collection.insert_many(docs)
    logger.info(
        INSERT_MANY,
        collection.database.name,
        collection.name,
        len(result.inserted_ids),
    )
    return result


UPDATE_ONE = "".join(
    (
        "{",
        ", ".join(
            (
                '"key": "mongo.update_one"',
                '"collection": "%s.%s"',
                '"id": "%s"',
            )
        ),
        "}",
    )
)


@retry(AutoReconnect)
def update_one(
    collection: Collection, key: Dict[str, Any], doc: Dict[str, Any]
):
    """Update one with retry."""
    result = collection.update_one(key, doc)
    logger.info(
        UPDATE_ONE,
        collection.database.name,
        collection.name,
        result.inserted_id,
    )
    return result
