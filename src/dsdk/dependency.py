# -*- coding: utf-8 -*-
"""Dependency injection."""

from argparse import Namespace
from datetime import datetime, timezone, tzinfo
from os import listdir
from os.path import isdir, join, splitext
from typing import Any, Callable, Dict, Tuple

from dateutil import tz


class StubException(Exception):
    """StubException."""


def epoch_ms_from_utc_datetime(utc: datetime) -> float:
    """Epoch ms from non-naive UTC datetime."""
    return utc.timestamp() * 1000


def utc_datetime_from_epoch_ms(epoch_ms: float) -> datetime:
    """Non-naive UTC datetime from UTC epoch ms."""
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)


def now_utc_datetime() -> datetime:
    """Non-naive now UTC datetime."""
    return datetime.now(tz=timezone.utc)


def local_timezone() -> tzinfo:
    """Return local timezone."""
    result = datetime.now().astimezone().tzinfo
    assert result is not None
    return result


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


def inject_float(key: str, kwargs: Dict[str, Any]) -> Callable:
    """Inject float."""

    def _inject(value) -> float:
        kwargs[key] = result = float(value)
        return result

    return _inject


def inject_int(key: str, kwargs: Dict[str, Any]) -> Callable:
    """Inject int."""

    def _inject(value) -> int:
        kwargs[key] = result = int(value)
        return result

    return _inject


def inject_str(key: str, kwargs: Dict[str, Any]) -> Callable:
    """Inject str."""

    def _inject(value: str) -> str:
        assert value.__class__ is str
        kwargs[key] = result = value
        return result

    return _inject


def inject_str_tuple(key: str, kwargs: Dict[str, Any]) -> Callable:
    """Inject str tuple."""

    def _inject(value: str) -> Tuple[str, ...]:
        assert value.__class__ is str
        kwargs[key] = result = tuple(value.split(","))
        return result

    return _inject


def inject_timezone(key: str, kwargs: Dict[str, Any]) -> Callable:
    """Inject timezone."""

    def _inject(value: str) -> tzinfo:
        assert value.__class__ is str
        kwargs[key] = result = tz.gettz(value)
        assert result is not None
        return result

    return _inject


def inject_namespace(key: str, kwargs: Dict[str, Any]) -> Callable:
    """Inject namespace."""

    def _inject(value: str) -> Namespace:
        result = namespace_directory(value)
        kwargs[key] = result
        return result

    return _inject
