# -*- coding: utf-8 -*-
"""Mssql."""

from __future__ import annotations

from abc import ABC
from contextlib import contextmanager
from json import dumps
from logging import getLogger
from typing import TYPE_CHECKING, Any, Generator, Sequence, Type, cast

from configargparse import ArgParser as ArgumentParser

from .dependency import StubException
from .persistor import Persistor as BasePersistor
from .service import Service, Task

logger = getLogger(__name__)


try:
    from pymssql import connect, _mssql, DatabaseError, InterfaceError
except ImportError as import_error:
    logger.warning(import_error)

    DatabaseError = InterfaceError = StubException

    def connect(*args, **kwargs):
        """Connect stub."""
        raise NotImplementedError()


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

    @classmethod
    def union_all(cls, cur, keys: Sequence[Any],) -> str:
        """Return 'union all select %s...' clause."""
        parameters = tuple(keys)
        union = "".join("    union all select %s\n" for _ in parameters)
        union = _mssql.substitute_params(union, parameters).decode("utf-8")
        # in case the mogrified strings have %
        # mistaken for python placeholders
        # when mogrified twice.
        union = union.replace("%", "%%")
        return union

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
                    n, *_ = row
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
    def connect(self) -> Generator[Any, None, None]:
        """Connect."""
        # Replace return type with ContextManager[Any] when mypy is fixed.
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
        # Replace return type with ContextManager[Any] when mypy is fixed.
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
