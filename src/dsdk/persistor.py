"""Persistor."""

from __future__ import annotations

from contextlib import contextmanager
from json import dumps
from logging import getLogger
from re import compile as re_compile
from string import Formatter
from tempfile import NamedTemporaryFile
from typing import Any, Generator, Sequence

from cfgenvy import yaml_type
from pandas import DataFrame

from .asset import Asset

logger = getLogger(__name__)


ALPHA_NUMERIC_DOT = re_compile("^[a-zA-Z_][A-Za-z0-9_.]*$")


class AbstractPersistor:
    """AbstractPersistor."""

    KEY = "abstract_persistor"

    CLOSE = dumps({"key": f"{KEY}.close"})
    COMMIT = dumps({"key": f"{KEY}.commit"})
    END = dumps({"key": f"{KEY}.end"})
    DRY_RUN = dumps({"key": f"{KEY}.dry_run.try", "path": "%s"})
    ERROR = dumps({"key": f"{KEY}.dry_run.error", "path": "%s"})
    ERRORS = dumps({"key": f"{KEY}.dry_run.errors", "path": "%s"})
    ON = dumps({"key": f"{KEY}.on"})
    OPEN = dumps({"key": f"{KEY}.open"})
    ROLLBACK = dumps({"key": f"{KEY}.rollback"})

    @classmethod
    def query(
        cls,
        cur,
        query: str,
        parameters: dict[str, Any] | None,
    ) -> None:
        """Query by parameters."""
        if parameters is None:
            parameters = {}
        cur.execute(query, parameters)

    @classmethod
    def df_from_query(
        cls,
        cur,
        query: str,
        parameters: dict[str, Any] | None,
    ) -> DataFrame:
        """Return DataFrame from query."""
        if parameters is None:
            parameters = {}
        cur.execute(query, parameters)
        columns = tuple(each[0] for each in cur.description)
        rows = cur.fetchall()
        if rows:
            df = DataFrame(rows)
            df.columns = columns
        else:
            df = DataFrame(columns=columns)
        return df

    @classmethod
    def query_by_keys(
        cls,
        cur,
        query: str,
        keys: dict[str, Sequence[Any]] = None,
        parameters: dict[str, Any] | None = None,
    ) -> None:
        """Query by key sequences and parameters.

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

    @classmethod
    def df_from_query_by_keys(
        cls,
        cur,
        query: str,
        keys: dict[str, Sequence[Any]] = None,
        parameters: dict[str, Any] | None = None,
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
        columns = tuple(each[0] for each in cur.description)
        rows = cur.fetchall()
        if rows:
            df = DataFrame(rows)
            df.columns = columns
        else:
            df = DataFrame(columns=columns)
        return df

    @classmethod
    def render_without_keys(cls, cur, query, parameters):
        """Render query without keys."""
        if parameters is None:
            parameters = {}
        formatter = Formatter()
        query = "".join(each[0] for each in formatter.parse(query))
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

    def dry_run(
        self,
        parameters: dict[str, Any],
        exceptions: tuple = (),
    ):
        """Execute sql found in asset with dry_run."""
        logger.info(self.ON)
        errors = []
        for path, query in self.sql():
            logger.info(self.DRY_RUN, path)
            try:
                self.dry_run_query(query, parameters)
            except exceptions as error:
                errors.append(error)
                logger.warning(self.ERROR, path)
        if bool(errors):
            raise RuntimeError(self.ERRORS, errors)
        logger.info(self.END)

    def dry_run_query(
        self,
        query,
        parameters,
    ) -> None:
        """Dry run query with dry_run parameter set to 1."""
        with self.rollback() as cur:
            rendered = self.render_without_keys(
                cur,
                query,
                {**parameters, "dry_run": 1},
            )
            with NamedTemporaryFile("w", delete=False, suffix=".sql") as fout:
                fout.write(rendered)
                cur.execute(rendered)

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
    def cursor(self, con):
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
    def as_yaml_type(cls, tag: str | None = None) -> None:
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
        schema: str,
        sql: Asset,
        username: str,
    ):
        """__init__."""
        self.database = database
        self.host = host
        self.password = password
        self.port = port
        self.schema = schema
        self.username = username
        super().__init__(sql)

    def as_yaml(self) -> dict[str, Any]:
        """As yaml."""
        return {
            "database": self.database,
            "host": self.host,
            "password": self.password,
            "port": self.port,
            "schema": self.schema,
            "sql": self.sql,
            "username": self.username,
        }

    @contextmanager
    def connect(self) -> Generator[Any, None, None]:
        """Connect."""
        # Replace return type with ContextManager[Any] when mypy is fixed.
        raise NotImplementedError()
