# -*- coding: utf-8 -*-
"""Persistor."""

from __future__ import annotations

from argparse import Namespace
from contextlib import contextmanager
from logging import getLogger
from os import listdir
from os.path import isdir, join, splitext
from typing import Any, Callable, Dict, Generator, Tuple

from .service import Service

logger = getLogger(__name__)


class StubException(Exception):
    """StubException."""


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


class AbstractPersistor:
    """AbstractPersistor."""

    KEY = "abstract_persistor"

    CLOSE = "".join(("{", ", ".join((f'"key": "{KEY}.close"',)), "}"))
    COMMIT = "".join(("{", ", ".join((f'"key": "{KEY}.commit"')), "}"))
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
    ROLLBACK = "".join(("{", ", ".join(('"key": "{KEY}.rollback"')), "}"))

    @classmethod
    @contextmanager
    def dependencies(cls, service: Service, parser):
        """Dependencies."""
        raise NotImplementedError()

    def __init__(self, sql: Namespace, tables: Tuple[str, ...]):
        """__init__."""
        self.sql = sql
        self.tables = tables

    def check(self, cur, exceptions):
        """check."""
        logger.info(self.ON)
        errors = []
        for table in self.tables:
            try:
                cur.execute(self.sql.extant, table)
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


def _inject_int(key, kwargs: Dict[str, Any]) -> Callable:
    def _inject(value) -> int:
        kwargs[key] = result = int(value)
        return result

    return _inject


def _inject_str(key, kwargs: Dict[str, Any]) -> Callable:
    def _inject(value: str):
        assert value.__class__ is str
        kwargs[key] = result = value
        return result

    return _inject


def _inject_str_tuple(key, kwargs: Dict[str, Any]) -> Callable:
    def _inject(value: str):
        assert value.__class__ is str
        kwargs[key] = result = tuple(",".split(value))
        return result

    return _inject


def _inject_namespace(key, kwargs: Dict[str, Any]) -> Callable:
    def _inject(value: str):
        result = namespace_directory(value)
        kwargs[key] = result

    return _inject


class Persistor(AbstractPersistor):
    """Persistor."""

    @classmethod
    @contextmanager
    def dependencies(
        cls, service: Service, parser
    ) -> Generator[None, None, None]:
        """Dependencies."""
        kwargs: Dict[str, Any] = {}

        for key, help_, inject in (
            ("database", "The database name", _inject_str),
            ("host", "The database host name or ip address", _inject_str),
            ("password", "The database password", _inject_str),
            ("port", "The database port", _inject_int),
            ("sql", "A nested directory of sql fragments.", _inject_namespace),
            (
                "tables",
                "A comma delimited list of tables to check",
                _inject_str_tuple,
            ),
            ("username", "The database username", _inject_str),
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
