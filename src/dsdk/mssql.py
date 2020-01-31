# -*- coding: utf-8 -*-
"""Mssql support."""

from __future__ import annotations

from logging import NullHandler, getLogger
from urllib.parse import unquote

from configargparse import ArgParser as ArgumentParser
from configargparse import Namespace

from .service import Service

try:
    # Since not everyone will use mssql
    from sqlalchemy import create_engine
    from sqlalchemy.engine.base import Engine
except ImportError:
    create_engine = None
    Engine = None


logger = getLogger(__name__)
logger.addHandler(NullHandler())


def get_mssql_connection(uri: str) -> Engine:
    r"""Get mssql connection.

    uri: mssql+pymssql://domain\\user:pass@host:port/database?timeout=timeout

        Domain and timeout are optional. See sqlalchemy docs for
        additional options.
    """
    return create_engine(uri)


class Mixin(Service):
    """Mixin."""

    @classmethod
    def add_arguments(cls, parser: ArgumentParser) -> None:
        """Add arguments."""
        super().add_arguments(parser)
        parser.add(
            "--mssqluri",
            required=True,
            help="MS SQL URI used to connect to MS SQL Server",
            env_var="MSSQL_URI",
        )

    def setup(self, args: Namespace):
        """Setup."""
        super().setup(args)
        self.mssql = get_mssql_connection(unquote(args.mssqluri))
