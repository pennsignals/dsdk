# -*- coding: utf-8 -*-
"""Mongo."""

from __future__ import annotations

from abc import ABC
from contextlib import contextmanager
from logging import getLogger
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

from .model import Model
from .persistor import _inject_str
from .service import Batch, Service
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

logger = getLogger(__name__)


if TYPE_CHECKING:
    BaseMixin = Service
else:
    BaseMixin = ABC


class Messages:  # pylint: disable=too-few-public-methods
    """Messages."""

    KEY = "mongo"

    CLOSE = "".join(
        ("{", ", ".join((f'"key": "{KEY}.close"', '"database": "%s"',)), "}",)
    )

    OPEN = "".join(
        ("{", ", ".join((f'"key": "{KEY}.open"', '"database": "%s"',)), "}",)
    )

    RESULTSET_ERROR = "".join(
        (
            "{",
            ", ".join(
                (
                    f'"key": "{KEY}.resultset.error"',
                    '"collection": "%s.%s"',
                    '"actual": %s',
                    '"expected": %s',
                )
            ),
            "}",
        )
    )

    INSERT_ONE = "".join(
        (
            "{",
            ", ".join(
                (
                    f'"key": "{KEY}.insert_one"',
                    '"collection": "%s.%s"',
                    '"id": "%s"',
                )
            ),
            "}",
        )
    )

    INSERT_MANY = "".join(
        (
            "{",
            ", ".join(
                (
                    f'"key": "{KEY}.insert_many"',
                    '"collection": "%s.%s"',
                    '"value": %s',
                )
            ),
            "}",
        )
    )

    UPDATE_ONE = "".join(
        (
            "{",
            ", ".join(
                (f'"key": "{KEY}.update_one"', '"collection": "%s.%s"',)
            ),
            "}",
        )
    )


class Persistor(Messages):
    """Persistor."""

    @classmethod
    @contextmanager
    def dependencies(
        cls, service: Service, parser
    ) -> Generator[None, None, None]:
        """Dependencies."""
        kwargs: Dict[str, Any] = {}

        for key, help_, inject in (
            (
                "uri",
                " ".join(
                    (
                        "Mongo URI used to connect to a Mongo database:",
                        (
                            "mongodb://USER:PASSWORD@HOST1,HOST2,.../DATABASE?"
                            "replicaset=REPLICASET&authsource=admin"
                        ),
                        "Use a valid uri."
                        "Url encode parts, do not encode the entire uri.",
                        "No unencoded colons, ampersands, slashes,",
                        "question-marks, etc. in parts.",
                        "Specifically, check url encoding of PASSWORD.",
                    )
                ),
                _inject_str,
            ),
        ):
            parser.add(
                f"--{cls.KEY}-{key}",
                env_var=f"{cls.KEY.upper()}_{key.upper()}",
                help=help_,
                required=True,
                type=inject(key, kwargs),
            )
        yield

        service.dependency(cls.KEY, cls, kwargs)

    def __init__(self, uri: str):
        """__init__."""
        self.uri = uri
        self.document_class = dict
        self.tz_aware = True
        self.connect_ = True

    @contextmanager
    def connect(self, **kwargs) -> Generator[Database, None, None]:
        """Contextmanager for database.

        Ensures that the mongo connection is opened and closed.
        """
        with MongoClient(
            self.uri,
            document_class=self.document_class,
            tz_aware=self.tz_aware,
            connect=self.connect_,
            **kwargs,
        ) as client:
            database = client.get_database()
            # force lazy connection open
            client.admin.command("ismaster")
            logger.info(self.OPEN, database.name)
            try:
                yield database
            finally:
                logger.info(self.CLOSE, database.name)

    @retry(AutoReconnect)
    def insert_one(self, collection: Collection, doc: Dict[str, Any]):
        """Insert one with retry."""
        result = collection.insert_one(doc)
        logger.info(
            self.INSERT_ONE,
            collection.database.name,
            collection.name,
            result.inserted_id,
        )
        return result

    @retry(AutoReconnect)
    def insert_many(
        self, collection: Collection, docs: Sequence[Dict[str, Any]]
    ):
        """Insert many with retry."""
        result = collection.insert_many(docs)
        logger.info(
            self.INSERT_MANY,
            collection.database.name,
            collection.name,
            len(result.inserted_ids),
        )
        return result

    @retry(AutoReconnect)
    def update_one(
        self, collection: Collection, key: Dict[str, Any], doc: Dict[str, Any]
    ):
        """Update one with retry."""
        result = collection.update_one(key, doc)
        logger.info(
            self.UPDATE_ONE, collection.database.name, collection.name,
        )
        return result


class EvidencePersistor(Persistor):
    """Evidence Persistor."""


class Mixin(BaseMixin):
    """Mixin."""

    def __init__(
        self,
        *,
        mongo=None,
        mongo_uri=None,  # pylint: disable=unused-argument
        mongo_cls=Persistor,
        **kwargs,
    ):
        """__init__."""
        self.mongo = cast(Persistor, mongo)
        self.mongo_cls = mongo_cls
        super().__init__(**kwargs)

    @contextmanager
    def inject_arguments(
        self, parser: ArgumentParser,
    ) -> Generator[None, None, None]:
        """Inject arguments."""
        with self.mongo_cls.dependencies(self, parser):
            with super().inject_arguments(parser):
                yield


class EvidenceMixin(Mixin):
    """Evidence Mixin."""

    def __init__(self, *, mongo_cls=EvidencePersistor, **kwargs):
        """__init__."""
        super().__init__(mongo_cls=mongo_cls, **kwargs)

    @contextmanager
    def open_batch(
        self, key: Any = None, model: Optional[Model] = None
    ) -> Generator[Batch, None, None]:
        """Open batch."""
        if key is None:
            key = ObjectId()
        mongo = self.mongo
        with super().open_batch(key) as batch:
            doc = batch.as_insert_doc(model)  # <- model dependency
            with mongo.connect() as database:
                key = mongo.insert_one(database.batches, doc)
            yield batch

        key, doc = batch.as_update_doc()
        with mongo.connect() as database:
            mongo.update_one(database.batches, key, doc)

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
            mongo = self.mongo
            with mongo.connect() as database:
                collection = database[key]
                result = mongo.insert_many(collection, docs)
                actual = len(result.inserted_ids)
                expected = columns.shape[0]
                assert actual == expected, mongo.RESULTSET_ERROR % (
                    database.name,
                    collection.name,
                    actual,
                    expected,
                )

                # TODO: Better exception
            df.drop(columns=["batch_id"], inplace=True)
