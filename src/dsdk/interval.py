"""Interval."""

from datetime import datetime
from typing import Any, Dict, Optional

from cfgenvy import yaml_type


class Interval:
    """Interval."""

    YAML = "!interval"

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

    def __init__(self, on: datetime, end: Optional[datetime] = None):
        """__init__."""
        self.on = on
        self.end = end

    def as_yaml(self) -> Dict[str, Any]:
        """As yaml."""
        return {"end": self.end, "on": self.on}
