# -*- coding: utf-8 -*-
"""Mssql support."""

from __future__ import annotations

from abc import ABC
from contextlib import contextmanager
from logging import NullHandler, getLogger, basicConfig, LoggerAdapter, INFO
from typing import TYPE_CHECKING, Generator, Optional, cast

from configargparse import ArgParser as ArgumentParser

from .service import Service

try:
    # Since not everyone will use mssql
    from sqlalchemy import create_engine
except ImportError:
    create_engine = None

# TODO Add import calling function from parent application
extra = {'callingfunc':''}
logger = getLogger(__name__)
FORMAT = '%(asctime)-15s - %(name)s - %(levelname)s {"callingfunc": "%(callingfunc)s", "module": "%(module)s", "function": "%(funcName)s", %(message)s}' 
basicConfig(format=FORMAT)
logger.setLevel(INFO)
# Add extra kwargs to message format
logger.addHandler(NullHandler())
logger = LoggerAdapter(logger, extra)


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