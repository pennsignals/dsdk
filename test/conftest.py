"""Conftest."""

from __future__ import annotations

from collections.abc import Generator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from pandas import DataFrame
from pytest import fixture

from dsdk import (
    FlowsheetMixin,
    Model,
    ModelMixin,
    Postgres,
    PostgresMixin,
    Service,
)


class StubPostgres(Postgres):
    """Stub Postgres."""

    return_value = DataFrame()

    @contextmanager
    def commit(self) -> Generator[Any]:
        """Commit."""
        yield None

    @contextmanager
    def rollback(self) -> Generator[Any]:
        """Rollback."""
        yield None

    @classmethod
    def df_from_query(  # pylint: disable=too-many-arguments
        cls,
        cur,
        query: str,
        *,
        by: str | None = None,
        cache: str | None = None,
        keys: dict[str, Sequence[Any]] | None = None,
        parameters: dict[str, Any] | None = None,
        size: int = 1000,
    ) -> DataFrame:
        """Dataframe From Query."""
        return cls.return_value

    @classmethod
    def query(  # pylint: disable=too-many-arguments
        cls,
        cur,
        query: str,
        *,
        by: str | None = None,
        cache: str | None = None,
        keys: dict[str, Sequence[Any]] | None = None,
        parameters: dict[str, Any] | None = None,
        size: int = 1000,
    ) -> None:
        """Query."""


class StubFlowsheetsService(  # pylint: disable=too-many-ancestors
    PostgresMixin,
    FlowsheetMixin,
    Service,
):
    """Stub Flowsheets Service."""

    YAML = "!example"

    @classmethod
    def yaml_types(cls):
        """Yaml types."""
        super().yaml_types()
        StubPostgres.as_yaml_type()

    def __init__(self, **kwargs):
        """__init__."""
        super().__init__(pipeline=None, **kwargs)

    def publish(self) -> Generator[Any]:
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


class StubModelService(  # pylint: disable=too-many-ancestors
    ModelMixin,
    Service,
):
    """Stub Model Service."""

    YAML = "!example"

    @classmethod
    def yaml_types(cls):
        """Yaml types."""
        super().yaml_types()
        Model.as_yaml_type()

    def __init__(self, **kwargs):
        """__init__."""
        super().__init__(pipeline=None, **kwargs)


@fixture
def stub_model_service():
    """Stub model service."""
    return StubModelService.parse(
        argv=[
            "-c",
            "./local/test.model.yaml",
            "-e",
            "./secrets/example.env",
        ]
    )


@fixture(autouse=True, scope="session")
def cleanup_cache_test():
    """Cleanup cache test."""
    path = Path("cache/test")
    yield
    for child in path.iterdir():
        child.unlink()
