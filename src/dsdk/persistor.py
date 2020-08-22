# -*- coding: utf-8 -*-
"""Persistor."""

from __future__ import annotations

from argparse import Namespace
from contextlib import contextmanager
from json import dumps
from logging import getLogger
from re import compile as re_compile
from typing import Any, Dict, Generator, Optional, Sequence, Tuple

from pandas import DataFrame, concat

from .dependency import (
    inject_int,
    inject_namespace,
    inject_str,
    inject_str_tuple,
)
from .service import Service
from .utils import chunks

logger = getLogger(__name__)


ALPHA_NUMERIC_DOT = re_compile("^[a-zA-Z_][A-Za-z0-9_.]*$")


class AbstractPersistor:
    """AbstractPersistor."""

    KEY = "abstract_persistor"

    CLOSE = dumps({"key": f"{KEY}.close"})
    COMMIT = dumps({"key": f"{KEY}.commit"})
    END = dumps({"key": f"{KEY}.end"})
    ERROR = dumps({"key": f"{KEY}.table.error", "table": "%s"})
    ERRORS = dumps({"key": f"{KEY}.tables.error", "tables": "%s"})
    EXTANT = dumps({"key": f"{KEY}.sql.extant", "value": "%s"})
    ON = dumps({"key": f"{KEY}.on"})
    OPEN = dumps({"key": f"{KEY}.open"})
    ROLLBACK = dumps({"key": f"{KEY}.rollback"})

    @classmethod
    @contextmanager
    def configure(cls, service: Service, parser):
        """Configure."""
        raise NotImplementedError()

    @classmethod
    def df_from_query(
        cls, cur, query: str, parameters: Optional[Dict[str, Any]],
    ) -> DataFrame:
        """Return DataFrame from query."""
        if parameters is None:
            parameters = {}
        cur.execute(query, parameters)
        rows = cur.fetchall()
        return DataFrame(rows)

    @classmethod
    def df_from_query_by_ids(  # pylint: disable=too-many-arguments
        cls,
        cur,
        query: str,
        ids: Sequence[Any],
        parameters: Optional[Dict[str, Any]] = None,
        size: int = 10000,
    ) -> DataFrame:
        """Return DataFrame from query by ids."""
        if parameters is None:
            parameters = {}
        dfs = []
        for chunk in chunks(ids, size):
            cur.execute(query, {"ids": chunk, **parameters})
            rows = cur.fetchall()
            dfs.append(DataFrame(rows))
        return concat(dfs, ignore_index=True)

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
                statement = self.extant(table)
                logger.info(self.EXTANT, statement)
                cur.execute(statement)
                for row in cur:
                    n = row["n"]
                    assert n == 1
                    continue
            except exceptions:
                logger.warning(self.ERROR, table)
                errors.append(table)
        if bool(errors):
            raise RuntimeError(self.ERRORS, errors)
        logger.info(self.END)

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

    @contextmanager
    def connect(self) -> Generator[Any, None, None]:
        """Connect."""
        raise NotImplementedError()

    def extant(self, table: str) -> str:
        """Return extant table sql."""
        if not ALPHA_NUMERIC_DOT.match(table):
            raise ValueError(f"Not a sql identifier: {table}.")
        return self.sql.extant.format(table=table)

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
