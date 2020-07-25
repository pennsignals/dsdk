# -*- coding: utf-8 -*-
"""Mssql."""

from __future__ import annotations

from abc import ABC
from argparse import Namespace
from contextlib import contextmanager
from logging import getLogger
from typing import TYPE_CHECKING, Any, Dict, Generator, Tuple, Type, cast

from configargparse import ArgParser as ArgumentParser

from .persistor import AbstractPersistor as BaseAbstractPersistor
from .persistor import Persistor as BasePersistor
from .persistor import StubException, namespace_directory
from .service import Service, Task

logger = getLogger(__name__)


try:
    from pymssql import connect, DatabaseError, InterfaceError
except ImportError as import_error:
    logger.warning(import_error)

    DatabaseError = InterfaceError = StubException

    def connect(*args, **kwargs):
        """Connect stub."""
        raise NotImplementedError()


try:
    from sqlalchemy import create_engine
    from sqlalchemy.exc import (
        DatabaseError as AlchemyDatabaseError,
        InterfaceError as AlchemyInterfaceError,
    )
except ImportError as import_error:
    logger.warning(import_error)

    AchemyDatabaseError = AlchemyInterfaceError = StubException


if TYPE_CHECKING:
    BaseMixin = Service
else:
    BaseMixin = ABC


class Messages:  # pylint: disable=too-few-public-methods
    """Messages."""

    KEY = "mssql"

    CLOSE = "".join(("{", ", ".join((f'"key": "{KEY}.close"',)), "}"))
    COLUMN_PRIVILEGE = "".join(
        ("{", ", ".join((f'"key": "{KEY}.warn"', '"value": "%s"')), "}",)
    )
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

    def check(self, cur, exceptions):
        """check."""
        logger.info(self.ON)
        errors = []
        for table in self.tables:  # pylint: disable=no-member; type: ignore
            try:
                cur.execute(
                    # pylint: disable=no-member; type: ignore
                    self.sql.extant.format(table=table)
                )
                (n,) = cur.fetchone()
                assert n == 1
            except exceptions as error:
                number, *_ = error.orig.args
                # column privileges are a standards-breaking mssql mis-feature
                if number == 230:
                    logger.info(self.COLUMN_PRIVILEGE, table)
                    continue
                logger.warning(self.ERROR, table)
                errors.append(table)
        if bool(errors):
            raise RuntimeError(self.ERRORS, errors)
        logger.info(self.END)


class Persistor(Messages, BasePersistor):
    """Persistor."""

    @contextmanager
    def connect(self) -> Generator[Any, None, None]:
        """Connect."""
        con = connect(
            server=self.host,
            username=self.username,
            password=self.password,
            database=self.database,
            port=self.port,
        )
        logger.info(self.OPEN)
        # TODO check semantics on pymssql connection and `with ... as`
        try:
            yield con
        finally:
            con.close()
            logger.info(self.CLOSE)

    def check(self, cur, exceptions=(DatabaseError, InterfaceError)):
        """Check."""
        super().check(cur, exceptions)


class AlchemyPersistor(Messages, BaseAbstractPersistor):
    """AlchemyPersistor."""

    @classmethod
    def inject_arguments(cls, mixin, parser):
        """Inject arguments."""
        kwargs: Dict[str, Any] = {}

        def _inject_sql(sql: str) -> Namespace:
            nonlocal kwargs
            kwargs["sql"] = value = namespace_directory(sql)
            return value

        def _inject_tables(strings: str) -> Tuple[str, ...]:
            nonlocal kwargs
            value = tuple(",".split(strings))
            for string in value:
                assert string.__class__ is str
            kwargs["tables"] = value
            return value

        def _inject_uri(uri: str) -> str:
            nonlocal kwargs
            kwargs["uri"] = uri
            return uri

        def _inject_persistor(cls: Type) -> AlchemyPersistor:
            mixin.mssql = persistor = cast(AlchemyPersistor, cls(**kwargs))
            return persistor

        parser.add(
            "--mssql-uri",
            env_var="MSSQL_URI",
            help=" ".join(
                (
                    "MSSQL URI used to connect to a MSSQL database:",
                    (
                        "mssql+pymssql://USER:PASS@HOST:PORT/DATABASE?"
                        "timeout=TIMEOUT"
                    ),
                    "Use a valid uri."
                    "Url encode all parts, but do not encode the entire uri.",
                    "No unencoded colons, ampersands, slashes,",
                    "question-marks, etc. in parts.",
                    "Specifically, check url encoding of USER (domain slash),"
                    "and PASSWORD.",
                )
            ),
            required=True,
            type=_inject_uri,
        )
        parser.add(
            "--mssql-tables",
            env_var="MSSQL_TABLES",
            help="Comma delimited tables to check.",
            required=True,
            type=_inject_tables,
        )
        parser.add(
            "--mssql-sql",
            help="Nested directory of sql fragments.",
            required=True,
            type=_inject_sql,
        )
        parser.add(
            "--mssql", default=cls, required=False, type=_inject_persistor,
        )

    def __init__(self, sql: Namespace, tables: Tuple[str, ...], uri: str):
        """__init__."""
        self.engine = create_engine(uri)
        self.uri = uri
        super().__init__(sql, tables)

    def check(
        self, cur, exceptions=(AlchemyDatabaseError, AlchemyInterfaceError),
    ):
        """Check."""
        super().check(cur, exceptions)

    @contextmanager
    def connect(self) -> Generator[Any, None, None]:
        """Connect."""
        con = self.engine.connect()
        logger.info(self.OPEN)
        try:
            yield con
        finally:
            con.close()
            logger.info(self.CLOSE)


class Mixin(BaseMixin):
    """Mixin."""

    def __init__(self, *, mssql_cls: Type = Persistor, **kwargs):
        """__init__."""
        self.mssql = cast(Persistor, None)
        self.mssql_cls = mssql_cls
        super().__init__(**kwargs)

    def inject_arguments(self, parser: ArgumentParser) -> None:
        """Inject arguments."""
        self.mssql_cls.inject_arguments(self, parser)
        super().inject_arguments(parser)


class AlchemyMixin(BaseMixin):
    """AlchemyMixin."""

    def __init__(
        self,
        *,
        mssql_tables=None,  # pylint: disable=unused-argument
        mssql_sql=None,  # pylint: disable=unused-argument
        mssql_uri=None,  # pylint: disable=unused-argument
        mssql_cls: Type = AlchemyPersistor,
        **kwargs,
    ):
        """__init__."""
        self.mssql = cast(AlchemyPersistor, None)
        self.mssql_cls = mssql_cls
        super().__init__(**kwargs)

    def inject_arguments(self, parser: ArgumentParser) -> None:
        """Inject arguments."""
        self.mssql_cls.inject_arguments(self, parser)
        super().inject_arguments(parser)


class CheckTablePrivileges(Task):  # pylint: disable=too-few-public-methods
    """CheckTablePrivileges."""

    def __call__(self, batch, service):
        """__call__."""
        mssql = service.mssql
        with mssql.rollback() as cur:
            mssql.check(cur)
