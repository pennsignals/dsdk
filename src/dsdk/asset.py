# -*- coding: utf-8 -*-
"""Asset."""

from __future__ import annotations

from argparse import Namespace
from logging import getLogger
from os import listdir
from os.path import isdir
from os.path import join as joinpath
from os.path import splitext
from typing import Any, Dict

from .utils import YamlDumper, YamlLoader

logger = getLogger(__name__)


class Asset(Namespace):
    """Asset."""

    YAML = "!asset"

    @classmethod
    def as_yaml_type(cls):
        """As yaml type."""
        YamlLoader.add_constructor(cls.YAML, cls._yaml_init)
        YamlDumper.add_representer(cls, cls._yaml_repr)

    @classmethod
    def build(cls, *, path: str, ext: str):
        """Build."""
        kwargs = {}
        for name in listdir(path):
            if name[0] == ".":
                continue
            child = joinpath(path, name)
            if isdir(child):
                kwargs[name] = cls.build(path=child, ext=ext)
                continue
            s_name, s_ext = splitext(name)
            if s_ext != ext:
                continue
            with open(child) as fin:
                kwargs[s_name] = fin.read()
        return cls(path=path, ext=ext, **kwargs)

    @classmethod
    def _yaml_init(cls, loader, node):
        """Yaml init."""
        return cls.build(**loader.construct_mapping(node, deep=True))

    @classmethod
    def _yaml_repr(cls, dumper, self):
        """Yaml repr."""
        return dumper.represent_mapping(cls.YAML, self.as_yaml())

    def __init__(
        self,
        *,
        path: str,
        ext: str,
        **kwargs: Asset,
    ):
        """__init__."""
        self.path = path
        self.ext = ext
        super().__init__(**kwargs)

    def as_yaml(self) -> Dict[str, Any]:
        """As yaml."""
        return {
            "ext": self.ext,
            "path": self.path,
        }
