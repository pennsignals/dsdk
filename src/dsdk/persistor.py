# -*- coding: utf-8 -*-
"""Persistor."""

from __future__ import annotations

from argparse import Namespace
from contextlib import contextmanager
from logging import getLogger
from typing import Any, Dict, Generator, Tuple

from .dependency import (
    inject_int,
    inject_namespace,
    inject_str,
    inject_str_tuple,
)
from .service import Service

logger = getLogger(__name__)


class AbstractPersistor:
    """AbstractPersistor."""

    KEY = "abstract_persistor"

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
    ON = "".join(("{", ", ".join((f'"key": "{KEY}.on"',)), "}"))
    OPEN = "".join(("{", ", ".join(('"key": "{KEY}.open"',)), "}"))
    ROLLBACK = "".join(("{", ", ".join(('"key": "{KEY}.rollback"',)), "}"))

    @classmethod
    @contextmanager
    def configure(cls, service: Service, parser):
        """Configure."""
        raise NotImplementedError()

    def __init__(self, sql: Namespace, tables: Tuple[str, ...]):
        """__init__."""
        self.sql = sql
        self.tables = tables

    def check(self, cur, exceptions):
        """Check."""
        logger.info(self.ON)
        errors = []
        for table in self.tables:
            try:
                cur.execute(self.sql.extant.format(self.identifier(table)))
                (n,) = cur.fetchone()
                assert n == 1
            except exceptions:
                logger.warning(self.ERROR, table)
                errors.append(table)
        if bool(errors):
            raise RuntimeError(self.ERRORS, errors)
        logger.info(self.END)

    @contextmanager
    def connect(self) -> Generator[Any, None, None]:
        """Connect."""
        raise NotImplementedError()

    @contextmanager
    def commit(self) -> Generator[Any, None, None]:
        """Commit."""
        with self.connect() as con:
            try:
                with con.cursor() as cur:
                    yield cur
                con.commit()
                logger.info(self.COMMIT)
            except BaseException:
                con.rollback()
                logger.info(self.ROLLBACK)
                raise

    def identifier(self, name: str):
        """Safe quoting for sql identifier."""
        raise NotImplementedError()

    @contextmanager
    def rollback(self) -> Generator[Any, None, None]:
        """Rollback."""
        with self.connect() as con:
            try:
                with con.cursor() as cur:
                    yield cur
            finally:
                con.rollback()
                logger.info(self.ROLLBACK)


class Persistor(AbstractPersistor):
    """Persistor."""

    @classmethod
    @contextmanager
    def configure(
        cls, service: Service, parser
    ) -> Generator[None, None, None]:
        """Configure."""
        kwargs: Dict[str, Any] = {}

        for key, help_, inject in (
            ("database", "The database name", inject_str),
            ("host", "The database host name or ip address", inject_str),
            ("password", "The database password", inject_str),
            ("port", "The database port", inject_int),
            ("sql", "A nested directory of sql fragments.", inject_namespace),
            (
                "tables",
                "A comma delimited list of tables to check",
                inject_str_tuple,
            ),
            ("username", "The database username", inject_str),
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

    def __init__(  # pylint: disable=too-many-arguments
        self,
        username: str,
        password: str,
        host: str,
        port: int,
        database: str,
        sql: Namespace,
        tables: Tuple[str, ...],
    ):
        """__init__."""
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        super().__init__(sql, tables)

    @contextmanager
    def connect(self) -> Generator[Any, None, None]:
        """Connect."""
        raise NotImplementedError()
