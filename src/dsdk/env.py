# -*- coding: utf-8 -*-
"""Env."""

from __future__ import annotations

from os import environ as os_env
from re import compile as re_compile
from typing import Mapping, Optional

try:
    from yaml import CSafeLoader as Loader  # type: ignore[misc]
except ImportError:
    from yaml import SafeLoader as Loader  # type: ignore[misc]


class Env:
    """Env."""

    YAML = "!env"
    PATTERN = re_compile(r".*?\$\{(\w+)\}.*?")

    @classmethod
    def as_yaml_type(cls, *, env: Optional[Mapping[str, str]] = None):
        """As yaml type."""
        _env = env or os_env

        def _yaml_init(loader, node) -> str:
            """This closure passed env."""
            return cls._yaml_init(loader, node, _env)

        Loader.add_implicit_resolver(cls.YAML, cls.PATTERN, None)
        Loader.add_constructor(cls.YAML, _yaml_init)

    @classmethod
    def _yaml_init(cls, loader, node, env: Mapping[str, str]):
        """From yaml."""
        value = loader.construct_scalar(node)
        match = cls.PATTERN.findall(value)
        if not match:
            return value
        for group in match:
            variable = env.get(group, None)
            if not variable:
                raise ValueError(f"No value for ${{{group}}}.")
            value = value.replace(f"${{{group}}}", variable)
        return value

    @classmethod
    def load(cls, path: str) -> Mapping[str, str]:
        """Env load."""
        with open(path) as fin:
            return cls.loads(fin.read())

    @classmethod
    def loads(cls, envs: str) -> Mapping[str, str]:
        """Env loads."""
        result = {}
        for line in envs.split("\n"):
            line = line.strip()
            if not line:
                continue
            if line.startswith("#"):
                continue
            key, value = line.split("=", 1)
            result[key] = value
        return result
