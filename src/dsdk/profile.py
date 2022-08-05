"""Profile."""

from contextlib import contextmanager
from logging import getLogger
from time import perf_counter_ns
from typing import Any, Dict, Generator, Optional

from cfgenvy import yaml_type

logger = getLogger(__name__)


class Profile:
    """Profile."""

    YAML = "!profile"

    @classmethod
    def as_yaml_type(cls, tag: Optional[str] = None):
        """As yaml type."""
        yaml_type(
            cls,
            tag or cls.YAML,
            init=cls._yaml_init,
            repr=cls._yaml_repr,
        )

    @classmethod
    def _yaml_init(cls, loader, node):
        """Yaml init."""
        return cls(**loader.construct_mapping(node, deep=True))

    @classmethod
    def _yaml_repr(cls, dumper, self, *, tag: str):
        """Yaml repr."""
        return dumper.represent_mapping(tag, self.as_yaml())

    def __init__(self, on: int, end: Optional[int] = None):
        """__init__."""
        self.end = end
        self.on = on

    def as_yaml(self) -> Dict[str, Any]:
        """As yaml."""
        return {"end": self.end, "on": self.on}

    def __repr__(self):
        """__repr__."""
        return f"Profile(end={self.end}, on={self.on})"

    def __str__(self):
        """__str__."""
        return str(
            {
                "end": self.end,
                "on": self.on,
            }
        )


@contextmanager
def profile(key: str) -> Generator[Profile, None, None]:
    """Profile."""
    # Replace return type with ContextManager[Profile] when mypy is fixed.
    i = Profile(perf_counter_ns())
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
