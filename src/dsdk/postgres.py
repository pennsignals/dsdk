# -*- coding: utf-8 -*-
"""Postgres."""

from __future__ import annotations

from abc import ABC
from argparse import Namespace
from contextlib import contextmanager
from logging import getLogger
from os import listdir
from os.path import isdir, join, splitext
from typing import TYPE_CHECKING, Generator, Optional, Type

from configargparse import ArgParser as ArgumentParser

from .service import Service, Task

logger = getLogger(__name__)

try:
    # Not everyone will be using postgres
    from psycopg2 import DatabaseError, InterfaceError, connect
except ImportError:
    DatabaseError = InterfaceError = Exception

    def connect(uri: str):
        """Connect stub."""
        raise NotImplementedError()


if TYPE_CHECKING:
    BaseMixin = Service
else:
    BaseMixin = ABC


def namespace_directory(root: str = "./", ext: str = ".sql") -> Namespace:
    """Return namespace from code directory."""
    result = Namespace()
    for name in listdir(root):
        if name[0] == ".":
            continue
        path = join(root, name)
        if isdir(path):
            setattr(result, name, namespace_directory(path, ext))
            continue
        s_name, s_ext = splitext(name)
        if s_ext != ext:
            continue
        with open(path) as fin:
            setattr(result, s_name, fin.read())
    return result


class Persistor:
    """Persistor."""

    OPEN = "".join(("{", ", ".join(('"key": "postgres.open"',)), "}"))

    CLOSE = "".join(("{", ", ".join(('"key": "postgres.close"',)), "}"))

    COMMIT = "".join(("{", ", ".join(('"key": "postgres.commit"')), "}"))

    ROLLBACK = "".join(("{", ", ".join(('"key": "postgres.rollback"')), "}"))

    def __init__(self, sql: Namespace, uri: str):
        """__init__."""
        self.sql = sql
        self.uri = uri

    @contextmanager
    def commit(self) -> Generator:
        """Commit."""
        con = connect(self.uri)
        try:
            logger.info(self.OPEN)
            with con.cursor() as cur:
                yield cur
            con.commit()
            logger.info(self.COMMIT)
        finally:
            logger.info(self.CLOSE)

    @contextmanager
    def rollback(self) -> Generator:
        """Rollback."""
        con = connect(self.uri)
        try:
            logger.info(self.OPEN)
            with con.cursor() as cur:
                yield cur
            con.rollback()
        finally:
            logger.info(self.ROLLBACK)
            logger.info(self.CLOSE)


class Mixin(BaseMixin):
    """Mixin."""

    def __init__(
        self,
        *,
        postgres_persistor: Type = Persistor,
        postgres_code: Optional[Namespace] = None,
        postgres_uri: Optional[str] = None,
        **kwargs,
    ):
        """__init__."""
        assert postgres_code is not None
        assert postgres_uri is not None
        self.postgres = postgres_persistor(postgres_code, postgres_uri)
        super().__init__(**kwargs)

    def inject_arguments(self, parser: ArgumentParser) -> None:
        """Inject arguments."""
        super().inject_arguments(parser)

        def _inject_uri(uri: str) -> str:
            return uri

        def _inject_code(directory: str) -> Namespace:
            return namespace_directory(directory)

        parser.add(
            "--postgres-uri",
            required=True,
            help=" ".join(
                (
                    "Postgres URI to connect to a postgres database:",
                    (
                        "postgresql+psycopg2://USER:PASSWORD@"
                        "HOST:PORT/DATABASE?timeout=TIMEOUT"
                    ),
                    "Use a valid uri."
                    "Url encode all parts, but do not encode the entire uri.",
                    "No unencoded colons, ampersands, slashes,",
                    "question-marks, etc. in parts.",
                    "Specifically, check url encoding of PASSWORD.",
                )
            ),
            env_var="POSTGRES_URI",
            type=_inject_uri,
        )
        parser.add(
            "--postgres-code",
            required=True,
            help=" ".join("Directory of nested postgres code fragments."),
            env_var="POSTGRES_CODE",
            type=_inject_code,
        )


class Run:  # pylint: disable=too-few-public-methods
    """Run."""

    def __init__(  # pylint: disable=redefined-builtin
        self, id, microservice_id, model_id, duration
    ):
        """__init__."""
        self.id = id
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
        with self.commit() as cur:
            cur.execute(self.sql.create)
        with self.commit() as cur:
            cur.execute(
                self.sql.runs.open, microservice_version, model_version
            )
            run_id, microservice_id, model_id, duration = cur.fetchone()
            run = Run(run_id, microservice_id, model_id, duration)
            yield run
            cur.execute_many(
                self.sql.predictions.insert,
                (
                    (run_id, csn, pat_id, score, *features)
                    for csn, pat_id, score, features in run.predictions
                ),
            )
            cur.execute(self.sql.runs.close, run.id)
            _, _, _, _, duration = cur.fetchone()
            run.duration = duration


class PredictionMixin(Mixin):  # pylint: disable=too-few-public-methods.
    """Prediction Mixin."""

    def __init__(self, **kwargs):
        """__init__."""
        super().__init__(postgres_persistor=PredictionPersistor, **kwargs)


class CheckTablePrivileges(Task):  # pylint: disable=too-few-public-methods
    """Check table privileges."""

    KEY = "postgres.table_privilege_check"

    ON = "".join(("{", f'"key": "{KEY}.on"', "}"))

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

    def __init__(self, tables):
        """__init__."""
        self.tables = tables

    def __call__(self, batch, service):
        """__call__."""
        logger.info(self.ON)
        with service.postgres.rollback() as cur:
            errors = []
            for table in self.tables:
                try:
                    cur.execute(service.postgres.sql.extant, table)
                    (n,) = cur.fetchone()
                    assert n == 1
                except (DatabaseError, InterfaceError):
                    logger.warning(self.ERROR, table)
                    errors.append(table)
            if bool(errors):
                raise RuntimeError(self.ERRORS, errors)
        logger.info(self.END)
