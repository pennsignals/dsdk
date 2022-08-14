"""Mssql."""

from __future__ import annotations

from abc import ABC
from contextlib import contextmanager
from json import dumps
from logging import getLogger
from typing import TYPE_CHECKING, Any, Generator

from .persistor import Persistor as BasePersistor
from .service import Service
from .utils import StubError

logger = getLogger(__name__)


try:
    from pymssql import DatabaseError, InterfaceError, _mssql, connect
except ImportError as import_error:
    logger.warning(import_error)

    DatabaseError = InterfaceError = StubError

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
    EXTANT = dumps({"key": f"{KEY}.sql.extant", "value": "%s"})
    ON = dumps({"key": f"{KEY}.on"})
    OPEN = dumps({"key": f"{KEY}.open"})
    ROLLBACK = dumps({"key": f"{KEY}.rollback"})


class Persistor(Messages, BasePersistor):
    """Persistor."""

    YAML = "!mssql"

    @classmethod
    def mogrify(
        cls,
        cur,
        query: str,
        parameters: Any,
    ) -> bytes:
        """Safely mogrify parameters into query or fragment."""
        return _mssql.substitute_params(query, parameters)

    def __init__(
        self,
        *,
        port: int = 1433,
        schema: str = "dbo",
        **kwargs,
    ):
        """__init__."""
        super().__init__(port=port, schema=schema, **kwargs)

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

    def dry_run(
        self,
        parameters: dict[str, Any],
        exceptions=(DatabaseError, InterfaceError),
    ):
        """Dry run."""
        super().dry_run(parameters, exceptions)


class Mixin(BaseMixin):
    """Mixin."""

    @classmethod
    def yaml_types(cls) -> None:
        """Yaml types."""
        logger.debug("dsdk.mssql.Mixin.yaml_types()")
        Persistor.as_yaml_type()
        super().yaml_types()

    def __init__(self, *, mssql: Persistor = None, **kwargs):
        """__init__."""
        self.mssql = mssql
        super().__init__(**kwargs)

    def as_yaml(self) -> dict[str, Any]:
        """As yaml."""
        return {
            "mssql": self.mssql,
            **super().as_yaml(),
        }
