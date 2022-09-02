"""Persistor."""

from __future__ import annotations

from contextlib import contextmanager
from hashlib import blake2b
from itertools import chain
from json import dumps
from logging import getLogger
from os.path import join as path_join
from pickle import HIGHEST_PROTOCOL
from pickle import dumps as pickle_dumps
from pickle import loads as pickle_loads
from re import compile as re_compile
from string import Formatter
from typing import Any, Generator, Sequence

from blosc import compress, decompress
from cfgenvy import YamlMapping, yaml_type
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
        *,
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
        Actual placeholders to be interpolated by the driver are not
        inside quotes.
        """
        cur.execute(cls.render(cur, query, keys=keys, parameters=parameters))

    @classmethod
    def df_from_query(
        cls,
        cur,
        query: str,
        *,
        cache: str | None = None,
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
        rendered = cls.render(cur, query, keys=keys, parameters=parameters)
        if cache is None:
            return cls.df_from_rendered(cur, rendered)
        name = path_join(
            cache,
            blake2b(rendered.encode("utf-8")).hexdigest()
            + ".pkl.blosc.sql.cache",
        )
        table: dict[str, DataFrame] = {}
        try:
            with open(name, "rb") as fin:
                compressed = fin.read()
        except FileNotFoundError:
            pass
        else:
            table = pickle_loads(decompress(compressed))
            df = table.get(rendered)
            if df is not None:
                return df
        table[rendered] = df = cls.df_from_rendered(cur, rendered)
        compressed = compress(pickle_dumps(table, protocol=HIGHEST_PROTOCOL))
        with open(name, "wb") as fout:
            fout.write(compressed)
        return df

    @classmethod
    def df_from_rendered(cls, cur, rendered):
        """Return df from rendered query."""
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
    def render(
        cls,
        cur,
        query: str,
        *,
        keys: dict[str, DataFrame | Sequence] | None = None,
        parameters: dict[str, Any] | None = None,
    ) -> str:
        """Render query with keys and parameters."""
        if keys is None:
            keys = {}
        if parameters is None:
            parameters = {}
        resolve = {
            DataFrame: cls.union_all_df,
        }
        union_alls = {
            key: resolve.get(value.__class__, cls.union_all)(cur, value)
            for key, value in keys.items()
        }
        logger.debug("Union_alls: %s", union_alls)
        logger.debug("Query: %s", query)
        query = query.format(**union_alls)
        return cls.mogrify(cur, query, parameters).decode("utf-8")

    @classmethod
    def render_without_keys(cls, cur, query, parameters):
        """Render query with parameters without keys."""
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
        sequence: Sequence[Any],
    ) -> str:
        """Return 'union all select %s...' clause."""
        parameters = tuple(sequence)
        union = "\n    ".join("union all select %s" for _ in parameters)
        return cls.mogrify(cur, union, parameters).decode("utf-8")

    @classmethod
    def union_all_df(
        cls,
        cur,
        df: DataFrame,
    ) -> str:
        """Return 'union all select %s, %s...' clause."""
        row = ", ".join("%s" for _ in range(len(df.columns)))
        union = "\n    ".join(
            f"union all select {row}" for _ in range(len(df))
        )
        parameters = tuple(chain(*df.itertuples(index=False, name=None)))
        return cls.mogrify(cur, union, parameters).decode("utf-8")

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
        query: str,
        parameters,
    ) -> None:
        """Dry run query with dry_run parameter set to 1."""
        with self.rollback() as cur:
            rendered = self.render_without_keys(
                cur,
                query,
                {**parameters, "dry_run": 1},
            )
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


class Persistor(YamlMapping, AbstractPersistor):
    """Persistor."""

    YAML = "!persistor"

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
