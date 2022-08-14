"""Conftest."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Generator
from unittest.mock import Mock

from pytest import fixture

from dsdk import FlowsheetMixin, PostgresMixin, Service


@contextmanager
def rollback():
    """Rollback contextmanager stub."""
    yield Mock()


class MockFlowsheetsService(  # pylint: disable=too-many-ancestors
    PostgresMixin,
    FlowsheetMixin,
    Service,
):
    """Mock Flowsheet Service."""

    YAML = "!example"

    def __init__(self, postgres, **kwargs):
        """__init__."""
        postgres = Mock()
        postgres.rollback = rollback
        postgres.commit = rollback
        super().__init__(pipeline=None, postgres=postgres, **kwargs)

    def publish(self) -> Generator[Any, None, None]:
        """Publish."""
        yield from self.flowsheets.publish(self.postgres)


@fixture
def mock_flowsheets_service():
    """Mock flowsheet service."""
    return MockFlowsheetsService.parse(
        argv=[
            "-c",
            "./local/test.yaml",
            "-e",
            "./secrets/example.env",
        ]
    )
