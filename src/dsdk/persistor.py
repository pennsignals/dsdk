# -*- coding: utf-8 -*-
"""Persistor."""

from __future__ import annotations

from argparse import Namespace
from contextlib import contextmanager
from logging import getLogger
from os import listdir
from os.path import isdir, join, splitext
from typing import Any, Dict, Generator, Tuple, Type, cast

from yaml import safe_load as yaml_loads

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
    def inject_arguments(cls, mixin, parser):
        """Inject arguments."""
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


class Persistor(AbstractPersistor):
    """Persistor."""

    @classmethod
    def inject_arguments(cls, mixin, parser):
        """Inject arguments."""
        kwargs: Dict[str, Any] = {}

        def _inject_cfg(name: str) -> Dict:
            nonlocal kwargs
            with open(name) as fin:
                cfg = yaml_loads(fin.read())
            for key in ("username", "password", "host", "database"):
                kwargs[key] = value = cfg[key]
                assert value.__class__ is str
                del cfg[key]
            for key in ("port",):
                kwargs[key] = value = cfg[key]
                assert value.__class__ is int
                del cfg[key]
            for key in ("tables",):
                value = cfg[key]
                assert value.__class__ is list
                strings = tuple(value)
                for string in strings:
                    assert string.__class__ is str
                kwargs[key] = strings
                del cfg[key]
            for key in ("sql",):
                value = cfg[key]
                assert value.__class__ is str
                namespace = namespace_directory(value)
                kwargs[key] = namespace
                del cfg[key]
            for key in cfg:
                logger.error("Unexpected key in configuration: %s", key)
            return kwargs

        def _inject_persistor(cls: Type) -> Persistor:
            persistor = cast(Persistor, cls(**kwargs))
            setattr(mixin, cls.KEY, persistor)
            return persistor

        parser.add(
            f"--{cls.KEY}-cfg",
            required=True,
            help=f"{cls.KEY} cfg file",
            env_var=f"{cls.KEY.upper()}_CFG",
            type=_inject_cfg,
        )
        parser.add(
            f"--{cls.KEY}", default=cls, required=True, type=_inject_persistor
        )

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
