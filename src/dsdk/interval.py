# -*- coding: utf-8 -*-
"""Interval."""

from datetime import datetime
from typing import Any, Dict, Optional

try:
    from yaml import CSafeDumper as Dumper  # type: ignore[misc]
    from yaml import CSafeLoader as Loader  # type: ignore[misc]
except ImportError:
    from yaml import SafeDumper as Dumper  # type: ignore[misc]
    from yaml import SafeLoader as Loader  # type: ignore[misc]


class Interval:
    """Interval."""

    YAML = "!interval"

    @classmethod
    def as_yaml_type(cls):
        """As yaml type."""
        Loader.add_constructor(cls.YAML, cls._yaml_init)
        Dumper.add_representer(cls, cls._yaml_repr)

    @classmethod
    def _yaml_init(cls, loader, node):
        """Yaml init."""
        return cls(**loader.construct_mapping(node, deep=True))

    @classmethod
    def _yaml_repr(cls, dumper, self):
        """Yaml repr."""
        return dumper.represent_mapping(cls.YAML, self.as_yaml())

    def __init__(self, on: datetime, end: Optional[datetime] = None):
        """__init__."""
        self.on = on
        self.end = end

    def as_yaml(self) -> Dict[str, Any]:
        """As yaml."""
        return {"end": self.end, "on": self.on}
