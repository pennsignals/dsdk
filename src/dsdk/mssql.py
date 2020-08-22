# -*- coding: utf-8 -*-
"""Mssql."""

from __future__ import annotations

from abc import ABC
from argparse import Namespace
from contextlib import contextmanager
from json import dumps
from logging import getLogger
from typing import TYPE_CHECKING, Any, Dict, Generator, Tuple, Type, cast

from configargparse import ArgParser as ArgumentParser

from .dependency import (
    StubException,
    inject_namespace,
    inject_str,
    inject_str_tuple,
)
from .persistor import AbstractPersistor as BaseAbstractPersistor
from .persistor import Persistor as BasePersistor
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

    AlchemyDatabaseError = AlchemyInterfaceError = StubException


if TYPE_CHECKING:
    BaseMixin = Service
else:
    BaseMixin = ABC


class Messages:  # pylint: disable=too-few-public-methods
    """Messages."""

    KEY = "mssql"

    CLOSE = dumps({"key": f"{KEY}.close"})
    COLUMN_PRIVILEGE = dumps({"key": f"{KEY}.warn", "value": "%s"})
    COMMIT = dumps({"key": f"{KEY}.commit"})
    END = dumps({"key": f"{KEY}.end"})
    ERROR = dumps({"key": f"{KEY}.table.error", "table": "%s"})
    ERRORS = dumps({"key": f"{KEY}.tables.error", "tables": "%s"})
    EXTANT = dumps({"key": f"{KEY}.sql.extant", "value": "%s"})
    ON = dumps({"key": f"{KEY}.on"})
    OPEN = dumps({"key": f"{KEY}.open"})
    ROLLBACK = dumps({"key": f"{KEY}.rollback"})


class Persistor(Messages, BasePersistor):
    """Persistor."""

    def check(self, cur, exceptions=(DatabaseError, InterfaceError)):
        """check."""
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
            # pylint: disable=catching-non-exception
            except exceptions as error:
                number, *_ = error.args  # args are not wrapped
                # column privileges are a standards-breaking mssql mis-feature
                if number == 230:
                    logger.info(self.COLUMN_PRIVILEGE, table)
                    continue
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
                with con.cursor(as_dict=True) as cur:
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
        con = connect(
            server=self.host,
            user=self.username,
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

    @contextmanager
    def rollback(self) -> Generator[Any, None, None]:
        """Rollback."""
        with self.connect() as con:
            try:
                with con.cursor(as_dict=True) as cur:
                    yield cur
            finally:
                con.rollback()
                logger.info(self.ROLLBACK)


class AlchemyPersistor(Messages, BaseAbstractPersistor):
    """AlchemyPersistor."""

    @classmethod
    @contextmanager
    def configure(
        cls, service: Service, parser
    ) -> Generator[None, None, None]:
        """Dependencies."""
        kwargs: Dict[str, Any] = {}

        for key, help_, inject in (
            (
                "tables",
                "A comma delimited list of tables to check.",
                inject_str_tuple,
            ),
            ("sql", "A nested directory of sql fragments.", inject_namespace),
            (
                "uri",
                " ".join(
                    (
                        "MSSQL URI used to connect to a MSSQL database:",
                        (
                            "mssql+pymssql://USER:PASS@HOST:PORT/DATABASE?"
                            "timeout=TIMEOUT"
                        ),
                        "Use a valid uri.",
                        "Url encode parts, do not encode the entire uri.",
                        "No unencoded colons, ampersands, slashes,",
                        "question-marks, etc. in parts.",
                        "Specifically, check url encoding of USER",
                        "(domain slash)",
                        "and PASSWORD.",
                    )
                ),
                inject_str,
            ),
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

    def __init__(self, sql: Namespace, tables: Tuple[str, ...], uri: str):
        """__init__."""
        self.engine = create_engine(uri)
        self.uri = uri
        super().__init__(sql, tables)

    def check(
        self, cur, exceptions=(AlchemyDatabaseError, AlchemyInterfaceError),
    ):
        """check."""
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
            except exceptions as error:
                number, *_ = error.orig.args  # args are wrapped
                # column privileges are a standards-breaking mssql mis-feature
                if number == 230:
                    logger.info(self.COLUMN_PRIVILEGE, table)
                    continue
                logger.warning(self.ERROR, table)
                errors.append(table)
        if bool(errors):
            raise RuntimeError(self.ERRORS, errors)
        logger.info(self.END)

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

    def __init__(self, *, mssql=None, mssql_cls: Type = Persistor, **kwargs):
        """__init__."""
        self.mssql = cast(Persistor, mssql)
        self.mssql_cls = mssql_cls
        super().__init__(**kwargs)

    @contextmanager
    def inject_arguments(
        self, parser: ArgumentParser
    ) -> Generator[None, None, None]:
        """Inject arguments."""
        with self.mssql_cls.configure(self, parser):
            with super().inject_arguments(parser):
                yield


class AlchemyMixin(BaseMixin):
    """AlchemyMixin."""

    def __init__(
        self, *, mssql=None, mssql_cls: Type = AlchemyPersistor, **kwargs,
    ):
        """__init__."""
        self.mssql = cast(AlchemyPersistor, mssql)
        self.mssql_cls = mssql_cls
        super().__init__(**kwargs)

    @contextmanager
    def inject_arguments(
        self, parser: ArgumentParser
    ) -> Generator[None, None, None]:
        """Inject arguments."""
        with self.mssql_cls.configure(self, parser):
            with super().inject_arguments(parser):
                yield


class CheckTablePrivileges(Task):  # pylint: disable=too-few-public-methods
    """CheckTablePrivileges."""

    def __call__(self, batch, service):
        """__call__."""
        mssql = service.mssql
        with mssql.rollback() as cur:
            mssql.check(cur)
