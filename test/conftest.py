"""Conftest."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Generator, Sequence

from pandas import DataFrame
from pytest import fixture
from requests import Session

from dsdk import FlowsheetMixin, Postgres, PostgresMixin, Service, FlowsheetResult


class StubPostgres(Postgres):
    """Stub Postgres."""

    return_value = DataFrame()

    @contextmanager
    def commit(self) -> Generator[Any, None, None]:
        """Commit."""
        yield None

    @contextmanager
    def rollback(self) -> Generator[Any, None, None]:
        """Rollback."""
        yield None

    @classmethod
    def df_from_query(
        cls,
        cur,
        query: str,
        *,
        cache: str | None = None,
        keys: dict[str, Sequence[Any]] | None = None,
        parameters: dict[str, Any] | None = None,
    ) -> DataFrame:
        return cls.return_value


class StubFlowsheetsService(  # pylint: disable=too-many-ancestors
    PostgresMixin,
    FlowsheetMixin,
    Service,
):
    """Stub Flowsheets Service."""

    YAML = "!example"

    def rest(
        self,
        missing,
        session: Session,
    ) -> FlowsheetResult:
        """Rest."""
        print("rest")
        return super().rest(missing, session)

    @classmethod
    def yaml_types(cls):
        """Yaml types."""
        super().yaml_types()
        StubPostgres.as_yaml_type()

    def __init__(self, **kwargs):
        """__init__."""
        super().__init__(pipeline=None, **kwargs)

    def publish(self) -> Generator[Any, None, None]:
        """Publish."""
        print("publish")
        yield from self.flowsheets.publish(self.postgres)


@fixture
def stub_flowsheets_service():
    """Stub flowsheet service."""
    return StubFlowsheetsService.parse(
        argv=[
            "-c",
            "./local/test.yaml",
            "-e",
            "./secrets/example.env",
        ]
    )
