"""Interval."""

from __future__ import annotations

from contextlib import contextmanager
from logging import getLogger
from time import perf_counter_ns
from typing import Any, Generator

from cfgenvy import YamlMapping

logger = getLogger(__name__)


class Interval(YamlMapping):
    """Interval."""

    YAML = "!interval"

    @classmethod
    def _yaml_init(cls, loader, node) -> Any:
        """Yaml init.

        Yaml 1.1 impicit boolean y|n|on|off|yes|no|true|false
        """
        a = loader.construct_mapping(node, deep=True)
        b: dict[str, Any] = {}
        for key, value in a.items():
            if key == "'on'" or (key.__class__ == bool and key is True):
                b["on"] = value
                continue
            b[key] = value
        return cls(**b)  # pylint: disable=missing-kwoa

    def __init__(self, *, on: Any, end: Any = None):
        """__init__."""
        self.end = end
        self.on = on

    def __repr__(self):
        """__repr__."""
        return f"Interval(end={self.end}, on={self.on})"

    def as_yaml(self) -> dict[str, Any]:
        """As yaml."""
        return {"end": self.end, "on": self.on}


@contextmanager
def profile(key: str) -> Generator[Interval, None, None]:
    """Profile."""
    # Replace return type with ContextManager[Interval] when mypy is fixed.
    i = Interval(on=perf_counter_ns())
    logger.info('{"key": "%s.on", "ns": "%s"}', key, i.on)
    try:
        yield i
    finally:
        i.end = perf_counter_ns()
        logger.info(
            '{"key": "%s.end", "ns": "%s", "elapsed": "%s"}',
            key,
            i.end,
            i.end - i.on,
        )
