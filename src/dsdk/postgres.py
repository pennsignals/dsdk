# -*- coding: utf-8 -*-
"""Postgres."""

from __future__ import annotations

from abc import ABC
from contextlib import contextmanager
from logging import getLogger
from typing import TYPE_CHECKING, Any, Generator, Type, cast

from configargparse import ArgParser as ArgumentParser

from .dependency import StubException
from .persistor import Persistor as BasePersistor
from .service import Service, Task
from .utils import retry

logger = getLogger(__name__)

try:
    # Not everyone will be using postgres
    from psycopg2 import (
        DatabaseError,
        InterfaceError,
        OperationalError,
        connect,
    )
except ImportError as import_error:
    logger.warning(import_error)

    DatabaseError = InterfaceError = OperationalError = StubException

    def connect(*args, **kwargs):
        """Connect stub."""
        raise NotImplementedError()


if TYPE_CHECKING:
    BaseMixin = Service
else:
    BaseMixin = ABC


class Messages:  # pylint: disable=too-few-public-methods
    """Messages."""

    KEY = "postgres"

    CLOSE = "".join(("{", ", ".join((f'"key": "{KEY}.close"',)), "}"))
    COMMIT = "".join(("{", ", ".join((f'"key": "{KEY}.commit"',)), "}"))
    END = "".join(("{", f'"key": "{KEY}.end"', "}"))
    ERROR = "".join(
        ("{", ", ".join((f'"key": "{KEY}.table.error"', '"table": "%s"')), "}")
    )
    ERRORS = "".join(
        (
            "{",
            ", ".join((f'"key": "{KEY}.tables.error"', '"tables": "%s"')),
            "}",
        )
    )
    EXTANT = "".join(
        ("{", ", ".join((f'"key": "{KEY}.sql.extant"', '"value": "%s"')), "}",)
    )
    ON = "".join(("{", ", ".join((f'"key": "{KEY}.on"',)), "}"))
    OPEN = "".join(("{", ", ".join((f'"key": "{KEY}.open"',)), "}"))
    ROLLBACK = "".join(("{", ", ".join((f'"key": "{KEY}.rollback"',)), "}"))


class Persistor(Messages, BasePersistor):
    """Persistor."""

    @retry((OperationalError,))
    def retry_connect(self):
        """Retry connect."""
        return connect(
            user=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            dbname=self.database,
        )

    @contextmanager
    def connect(self) -> Generator[Any, None, None]:
        """Connect."""
        # The `with ... as con:` formulation does not close the connection:
        # https://www.psycopg.org/docs/usage.html#with-statement
        con = self.retry_connect()
        logger.info(self.OPEN)
        try:
            yield con
        finally:
            con.close()
            logger.info(self.CLOSE)

    def check(self, cur, exceptions=(DatabaseError, InterfaceError)):
        """Check."""
        super().check(cur, exceptions)


class Mixin(BaseMixin):
    """Mixin."""

    def __init__(
        self, *, postgres=None, postgres_cls: Type = Persistor, **kwargs,
    ):
        """__init__."""
        self.postgres = cast(Persistor, postgres)
        self.postgres_cls = postgres_cls
        super().__init__(**kwargs)

    @contextmanager
    def inject_arguments(
        self, parser: ArgumentParser
    ) -> Generator[None, None, None]:
        """Inject arguments."""
        with self.postgres_cls.configure(self, parser):
            with super().inject_arguments(parser):
                yield


class Run:  # pylint: disable=too-few-public-methods
    """Run."""

    def __init__(self, id_, microservice_id, model_id, duration):
        """__init__."""
        self.id = id_
        self.microservice_id = microservice_id
        self.model_id = model_id
        self.duration = duration
        self.predictions = []


class PredictionPersistor(Persistor):
    """PredictionPersistor."""

    @contextmanager
    def open_run(
        self, microservice_version: str, model_version: str
    ) -> Generator[Run, None, None]:
        """Open run."""
        sql = self.sql
        with self.commit() as cur:
            cur.execute(sql.schema)
            cur.execute(sql.runs.open, (microservice_version, model_version,))
            run_id, microservice_id, model_id, duration = cur.fetchone()
            run = Run(run_id, microservice_id, model_id, duration)
        yield run
        with self.commit() as cur:
            cur.execute(sql.schema)
            cur.executemany(
                sql.predictions.insert,
                (
                    (run_id, patient_id, score)
                    for patient_id, score in run.predictions
                ),
            )
            cur.execute(sql.runs.close, (run.id,))
            _, _, _, _, duration = cur.fetchone()
            run.duration = duration


class PredictionMixin(Mixin):  # pylint: disable=too-few-public-methods.
    """Prediction Mixin."""

    def __init__(  # pylint: disable=useless-super-delegation
        self, *, postgres_cls=PredictionPersistor, **kwargs
    ):
        """__init__."""
        super().__init__(postgres_cls=postgres_cls, **kwargs)


class CheckTablePrivileges(Task):  # pylint: disable=too-few-public-methods
    """CheckTablePrivileges."""

    def __call__(self, batch, service):
        """__call__."""
        postgres = service.postgres
        with postgres.rollback() as cur:
            postgres.check(cur)
