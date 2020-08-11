# -*- coding: utf-8 -*-
"""Mssql."""

from __future__ import annotations

from abc import ABC
from argparse import Namespace
from contextlib import contextmanager
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
    EXTANT = "".join(
        ("{", ", ".join((f'"key": "{KEY}.sql.extant"', '"value": "%s"')), "}",)
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

    def check(self, cur, exceptions=(DatabaseError, InterfaceError)):
        """Check."""
        super().check(cur, exceptions)


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
