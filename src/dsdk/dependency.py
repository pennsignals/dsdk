# -*- coding: utf-8 -*-
"""Dependency injection."""

from argparse import Namespace
from os import listdir
from os.path import isdir, join, splitext
from typing import Any, Callable, Dict, Tuple


class StubException(Exception):
    """StubException."""


def namespace_directory(root: str = "./", ext: str = ".sql") -> Namespace:
    """Return namespace from code directory."""
    result = Namespace()
    for name in listdir(root):
        if name[0] == ".":
            continue
        path = join(root, name)
        if isdir(path):
            setattr(result, name, namespace_directory(path, ext))
            continue
        s_name, s_ext = splitext(name)
        if s_ext != ext:
            continue
        with open(path) as fin:
            setattr(result, s_name, fin.read())
    return result


def inject_int(key, kwargs: Dict[str, Any]) -> Callable:
    """Inject int."""

    def _inject(value) -> int:
        kwargs[key] = result = int(value)
        return result

    return _inject


def inject_str(key, kwargs: Dict[str, Any]) -> Callable:
    """Inject str."""

    def _inject(value: str) -> str:
        assert value.__class__ is str
        kwargs[key] = result = value
        return result

    return _inject


def inject_str_tuple(key, kwargs: Dict[str, Any]) -> Callable:
    """Inject str tuple."""

    def _inject(value: str) -> Tuple[str, ...]:
        assert value.__class__ is str
        kwargs[key] = result = tuple(",".split(value))
        return result

    return _inject


def inject_namespace(key, kwargs: Dict[str, Any]) -> Callable:
    """Inject namespace."""

    def _inject(value: str) -> Namespace:
        result = namespace_directory(value)
        kwargs[key] = result
        return result

    return _inject
