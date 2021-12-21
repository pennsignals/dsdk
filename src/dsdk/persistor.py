# -*- coding: utf-8 -*-
"""Persistor."""

from __future__ import annotations

from contextlib import contextmanager
from json import dumps
from logging import getLogger
from re import compile as re_compile
from string import Formatter
from tempfile import NamedTemporaryFile
from typing import Any, Dict, Generator, List, Optional, Sequence, Tuple

from cfgenvy import yaml_type
from pandas import DataFrame, concat

from .asset import Asset
from .utils import chunks

logger = getLogger(__name__)


ALPHA_NUMERIC_DOT = re_compile("^[a-zA-Z_][A-Za-z0-9_.]*$")


class AbstractPersistor:
    """AbstractPersistor."""

    KEY = "abstract_persistor"

    CLOSE = dumps({"key": f"{KEY}.close"})
    COMMIT = dumps({"key": f"{KEY}.commit"})
    END = dumps({"key": f"{KEY}.end"})
    ERROR = dumps({"key": f"{KEY}.table.error", "query": "%s"})
    ERRORS = dumps({"key": f"{KEY}.dry_run.error", "query": "%s"})
    ON = dumps({"key": f"{KEY}.on"})
    OPEN = dumps({"key": f"{KEY}.open"})
    ROLLBACK = dumps({"key": f"{KEY}.rollback"})

    @classmethod
    def df_from_query(
        cls,
        cur,
        query: str,
        parameters: Optional[Dict[str, Any]],
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
        # - different implementation than set-based join
        #   - arbitrary limit on the number of items
        # - terrible performance
        #   - renders as multiple 'or' after query planning
        #   - cpu branch prediction failure?
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

        Query is expected to use {name} for key sequences and %(name)s
        for parameters.
        The mogrified fragments produced by union_all are mogrified again.
        There is a chance that python placeholders could be injected by the
        first pass from sequence data.
        However, it seems that percent in `'...%s...'` or `'...'%(name)s...'`
        inside string literals produced from the first mogrification pass are
        not interpreted as parameter placeholders in the second pass by
        the pymssql driver.
        Actual placeholders to by interpolated by the driver are not
        inside quotes.
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
    def render_without_keys(cls, cur, query, parameters):
        """Render query without keys."""
        if parameters is None:
            parameters = {}
        formatter = Formatter()
        query = "".join((each[0] for each, in formatter.parse(query)))
        return cls.mogrify(cur, query, parameters).decode("utf-8")

    @classmethod
    def mogrify(
        cls,
        cur,
        query: str,
        parameters: Any,
    ) -> bytes:
        """Safely mogrify parameters into query or fragment."""
        raise NotImplementedError()

    @classmethod
    def union_all(
        cls,
        cur,
        keys: Sequence[Any],
    ) -> str:
        """Return 'union all select %s...' clause."""
        parameters = tuple(keys)
        union = "\n    ".join("union all select %s" for _ in parameters)
        union = cls.mogrify(cur, union, parameters).decode("utf-8")
        return union

    def __init__(self, sql: Asset):
        """__init__."""
        self.sql = sql

    def on_dry_run(
        self,
        sql: Asset,
        query_parameters: Dict[str, Any],
        skip: Tuple,
        exceptions: Tuple,
    ):
        """On dry run."""
        errors: List[Exception] = []
        for key, value in vars(sql).items():
            if value.__class__ == Asset:
                errors += self.on_dry_run(
                    value, query_parameters, skip, exceptions
                )
                continue
            if key in skip:
                continue
            with self.rollback() as cur:
                rendered = self.render_without_keys(
                    cur,
                    value,
                    query_parameters,
                )
                with NamedTemporaryFile(
                    "w", delete=False, suffix=".sql"
                ) as fout:
                    fout.write(rendered)
                try:
                    cur.execute(rendered)
                except exceptions as e:
                    logger.warning(self.ERROR, key)
                    errors.append(e)
        return errors

    def dry_run(
        self,
        query_parameters: Dict[str, Any],
        skip: Tuple = (),
        exceptions: Tuple = (),
    ):
        """Execute sql found in asse with dry_run parameter set to 1."""
        logger.info(self.ON)
        query_parameters = query_parameters.copy()
        query_parameters["dry_run"] = 1
        errors = self.on_dry_run(self.sql, query_parameters, skip, exceptions)
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

    YAML = "!basepersistor"

    @classmethod
    def as_yaml_type(cls, tag: Optional[str] = None) -> None:
        """As yaml type."""
        Asset.as_yaml_type()
        yaml_type(
            cls,
            tag or cls.YAML,
            init=cls._yaml_init,
            repr=cls._yaml_repr,
        )

    @classmethod
    def _yaml_init(cls, loader, node):
        """Yaml init."""
        return cls(**loader.construct_mapping(node, deep=True))

    @classmethod
    def _yaml_repr(cls, dumper, self, *, tag: str):
        """Yaml repr."""
        return dumper.represent_mapping(tag, self.as_yaml())

    def __init__(  # pylint: disable=too-many-arguments
        self,
        *,
        database: str,
        host: str,
        password: str,
        port: int,
        sql: Asset,
        username: str,
    ):
        """__init__."""
        self.database = database
        self.host = host
        self.password = password
        self.port = port
        self.username = username
        super().__init__(sql)

    def as_yaml(self) -> Dict[str, Any]:
        """As yaml."""
        return {
            "database": self.database,
            "host": self.host,
            "password": self.password,
            "port": self.port,
            "sql": self.sql,
            "username": self.username,
        }

    @contextmanager
    def connect(self) -> Generator[Any, None, None]:
        """Connect."""
        # Replace return type with ContextManager[Any] when mypy is fixed.
        raise NotImplementedError()
