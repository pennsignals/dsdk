# -*- coding: utf-8 -*-
"""Env."""

from __future__ import annotations

from os import environ as os_env
from re import compile as re_compile
from typing import Mapping, Optional, Pattern

from .utils import yaml_implicit_type


class Env:
    """Env."""

    YAML = "!env"
    PATTERN = re_compile(r".?\$\{([^\}^\{]+)\}.?")

    @classmethod
    def as_yaml_type(
        cls,
        tag: Optional[str] = None,
        *,
        env: Optional[Mapping[str, str]] = None,
        pattern: Optional[Pattern] = None,
    ):
        """As yaml type."""
        yaml_implicit_type(
            cls,
            tag or cls.YAML,
            pattern=pattern or cls.PATTERN,
            init=cls._yaml_init,
            env=env or os_env,
        )

    @classmethod
    def _yaml_init(
        cls,
        loader,
        node,
        *,
        env: Mapping[str, str],
        pattern: Pattern,
    ):
        """From yaml."""
        value = loader.construct_scalar(node)
        match = pattern.findall(value)
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
            return cls.loads(fin)

    @classmethod
    def loads(cls, stream) -> Mapping[str, str]:
        """Env loads."""
        result = {}
        for line in stream:
            line = line.strip()
            if not line:
                continue
            if line.startswith("#"):
                continue
            key, value = line.split("=", 1)
            result[key] = value
        return result
