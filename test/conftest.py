"""Conftest."""

from contextlib import contextmanager
from pytest import fixture
from typing import Any, Generator

from unittest.mock import Mock

from dsdk import Service, FlowsheetMixin, PostgresMixin


@contextmanager
def rollback():
    """Rollback contextmanager stub."""
    yield Mock()


class _MockFlowsheetsService(  # pylint: disable=too-many-ancestors
    PostgresMixin, FlowsheetMixin, Service,
):
    """Mock Flowsheet Service."""

    YAML = "!example"

    def __init__(self, postgres, **kwargs):
        """__init__."""
        postgres = Mock()
        postgres.rollback = rollback
        postgres.commit = rollback
        super().__init__(postgres=postgres, **kwargs)

    def publish(self) -> Generator[Any, None, None]:
        """Publish."""
        yield from self.flowsheets.publish(self.postgres)


@fixture
def mock_flowsheets_service():
    """Mock flowsheet service."""
    return _MockFlowsheetsService.parse()
