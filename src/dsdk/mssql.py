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
            help=(
                "MSSQL URI used to connect to a MSSQL database: "
                "mssql+pymssql://USER:PASS@HOST:PORT/DATABASE?timeout=TIMEOUT "
                "Url encode all parts: USER (domain slash), PASS in particular"
            ),
            env_var="MSSQL_URI",
            type=_inject_mssql_uri,
        )

    @contextmanager
    def open_mssql(self) -> Generator:
        """Open mssql."""
        with self._mssql.connect() as con:
            yield con
            logger.info('"action": "connect"')


class CheckTablePrivileges(Task):  # pylint: disable=too-few-public-methods
    """Check table privileges."""

    CONNECT = """
select 1 as n
"""

    EXTANT = """
select 1 as n where exists (select 1 as n from {table})
"""

    KEY = "table_privilege_check"

    ON = "".join(("{", f'"key": "{KEY}.on"', "}"))

    END = "".join(("{", f'"key": "{KEY}.end"', "}"))

    COLUMN_PRIVILEGE = "".join(
        (
            "{",
            ", ".join(
                (f'"key": "{KEY}.column_privilege_warning"', '"value": "%s"')
            ),
            "}",
        )
    )

    FAILED = "".join(
        ("{", ", ".join((f'"key": "{KEY}.failed"', '"value": "%s"')), "}")
    )

    FAILURES = "".join(
        ("{", ", ".join((f'"key": "{KEY}.failures"', '"value": "%s"')), "}")
    )

    def __init__(self, tables):
        """__init__."""
        self.tables = tables

    def __call__(self, batch, service):
        """__call__."""
        logger.info(self.ON)
        with service.open_mssql() as con:
            # force lazy connection open.
            cur = con.execute(self.CONNECT)
            for _ in cur.fetchall():
                pass
            failures = []
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
                    logger.warning(self.FAILED, table)
                    failures.append(table)
            if bool(failures):
                raise RuntimeError(self.FAILURES, failures)
        logger.info(self.END)
