# -*- coding: utf-8 -*-
"""Mssql support."""

from __future__ import annotations

from abc import ABC
from contextlib import contextmanager
from logging import INFO
from typing import TYPE_CHECKING, Generator, Optional, cast

from configargparse import ArgParser as ArgumentParser

from .service import Service, Task
from .utils import get_logger

logger = get_logger(__name__, INFO)

try:
    # Since not everyone will use mssql
    from sqlalchemy import create_engine
    from sqlalchemy.exc import DatabaseError, InterfaceError
except ImportError:
    create_engine = None
    DatabaseError = InterfaceError = Exception


if TYPE_CHECKING:
    BaseMixin = Service
else:
    BaseMixin = ABC


class Mixin(BaseMixin):
    """Mixin."""

    def __init__(self, *, mssql_uri: Optional[str] = None, **kwargs):
        """__init__."""
        # inferred type of self._mssql_uri must not be optional...
        self._mssql_uri = cast(str, mssql_uri)
        super().__init__(**kwargs)

        # ... because self._mssql_uri is not optional
        assert self._mssql_uri is not None
        self._mssql = create_engine(self._mssql_uri)

    def inject_arguments(self, parser: ArgumentParser) -> None:
        """Inject arguments."""
        super().inject_arguments(parser)

        def _inject_mssql_uri(mssql_uri: str) -> str:
            self._mssql_uri = mssql_uri
            return mssql_uri

        parser.add(
            "--mssql-uri",
            required=True,
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
            env_var="MSSQL_URI",
            type=_inject_mssql_uri,
        )

    OPEN = "".join(("{", ", ".join(('"key": "mssql.open"')), "}"))

    CLOSE = "".join(("{", ", ".join(('"key": "mssql.close"')), "}"))

    CONNECT = """
select 1 as n
"""

    @contextmanager
    def open_mssql(self) -> Generator:
        """Open mssql."""
        with self._mssql.connect() as con:
            # force lazy connection open.
            cur = con.execute(self.CONNECT)
            for _ in cur.fetchall():
                pass
            logger.info(self.OPEN)
            try:
                yield con
            finally:
                logger.info(self.CLOSE)


class CheckTablePrivileges(Task):  # pylint: disable=too-few-public-methods
    """Check table privileges."""

    EXTANT = """
select 1 as n where exists (select 1 as n from {table})
"""

    KEY = "mssql.table_privilege_check"

    ON = "".join(("{", f'"key": "{KEY}.on"', "}"))

    END = "".join(("{", f'"key": "{KEY}.end"', "}"))

    COLUMN_PRIVILEGE = "".join(
        ("{", ", ".join((f'"key": "{KEY}.warn"', '"value": "%s"')), "}",)
    )

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

    def __init__(self, tables):
        """__init__."""
        self.tables = tables

    def __call__(self, batch, service):
        """__call__."""
        logger.info(self.ON)
        with service.open_mssql() as con:
            errors = []
            for table in self.tables:
                sql = self.EXTANT.format(table=table)
                try:
                    cur = con.execute(sql)
                    for _ in cur.fetchall():
                        pass
                except (DatabaseError, InterfaceError) as error:
                    number, *_ = error.orig.args
                    # column privileges is a standard-breaking mssql "feature"
                    if number == 230:
                        logger.info(self.COLUMN_PRIVILEGE, table)
                        continue
                    logger.warning(self.ERROR, table)
                    errors.append(table)
            if bool(errors):
                raise RuntimeError(self.ERRORS, errors)
        logger.info(self.END)
