# -*- coding: utf-8 -*-
"""Persistor."""

from __future__ import annotations

from argparse import Namespace
from contextlib import contextmanager
from json import dumps
from logging import getLogger
from re import compile as re_compile
from tempfile import NamedTemporaryFile
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
        columns = (each[0] for each in cur.description)
        rows = cur.fetchall()
        if rows:
            df = DataFrame(rows)
            df.columns = columns
        else:
            df = DataFrame(columns=columns)
        return df

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
        columns = None
        chunk = None
        # The sql 'in (<item, ...>)' used for ids is problematic
        # - limit on the number of items
        # - hard query plan
        # - renders as multiple 'or' after query planning
        # - poor performance
        for chunk in chunks(ids, size):
            cur.execute(query, {"ids": chunk, **parameters})
            rows = cur.fetchall()
            if rows:
                dfs.append(DataFrame(rows))
        if chunk is None:
            raise ValueError("Parameter ids must not be empty")
        columns = (each[0] for each in cur.description)
        df = concat(dfs, ignore_index=True)
        df.columns = columns
        return df

    @classmethod
    def df_from_query_by_keys(
        cls,
        cur,
        query: str,
        keys: Dict[str, Sequence[Any]] = None,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> DataFrame:
        """Return df from query by key sequences and parameters.

        Query is expected to use {name} for keys and %(name)s for parameters.
        """
        if keys is None:
            keys = {}
        if parameters is None:
            parameters = {}
        keys = {
            key: cls.union_all(cur, sequence) for key, sequence in keys.items()
        }
        query = query.format(**keys)
        rendered = cls.mogrify(cur, query, parameters).decode("utf-8")
        with NamedTemporaryFile("w", delete=False, suffix=".sql") as fout:
            fout.write(rendered)
        cur.execute(rendered)
        rows = cur.fetchall()
        df = DataFrame(rows)
        columns = (each[0] for each in cur.description)
        df.columns = columns
        return df

    @classmethod
    def mogrify(cls, cur, query: str, parameters: Any,) -> bytes:
        """Safely mogrify parameters into query or fragment."""
        raise NotImplementedError()

    @classmethod
    def union_all(cls, cur, keys: Sequence[Any],) -> str:
        """Return 'union all select %s...' clause."""
        parameters = tuple(keys)
        union = "\n    ".join("union all select %s" for _ in parameters)
        union = cls.mogrify(cur, union, parameters).decode("utf-8")
        return union

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
                logger.info(self.EXTANT, table)
                logger.debug(self.EXTANT, statement)
                cur.execute(statement)
                for row in cur:
                    n, *_ = row
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
        # Replace return type with ContextManager[Any] when mypy is fixed.
        with self.connect() as con:
            try:
                with self.cursor(con) as cur:
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
        # Replace return type with ContextManager[Any] when mypy is fixed.
        raise NotImplementedError()

    @contextmanager
    def cursor(self, con):  # pylint: disable=no-self-use
        """Yield a cursor that provides dicts."""
        # Replace return type with ContextManager[Any] when mypy is fixed.
        with con.cursor() as cursor:
            yield cursor

    def extant(self, table: str) -> str:
        """Return extant table sql."""
        if not ALPHA_NUMERIC_DOT.match(table):
            raise ValueError(f"Not a sql identifier: {table}.")
        return self.sql.extant.format(table=table)

    @contextmanager
    def rollback(self) -> Generator[Any, None, None]:
        """Rollback."""
        # Replace return type with ContextManager[Any] when mypy is fixed.
        with self.connect() as con:
            try:
                with self.cursor(con) as cur:
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
        # Replace return type with ContextManager[None] when mypy is fixed.
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
        # Replace return type with ContextManager[Any] when mypy is fixed.
        raise NotImplementedError()
