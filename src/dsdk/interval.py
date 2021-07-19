# -*- coding: utf-8 -*-
"""Interval."""

from datetime import datetime
from typing import Any, Dict, Optional

from .utils import YamlDumper, YamlLoader


class Interval:
    """Interval."""

    YAML = "!interval"

    @classmethod
    def as_yaml_type(cls):
        """As yaml type."""
        YamlLoader.add_constructor(cls.YAML, cls._yaml_init)
        YamlDumper.add_representer(cls, cls._yaml_repr)

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
