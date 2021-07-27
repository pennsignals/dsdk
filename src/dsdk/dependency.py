# -*- coding: utf-8 -*-
"""Dependency injection."""

from argparse import Namespace
from datetime import datetime, timezone, tzinfo
from os import listdir
from os.path import isdir, join, splitext
from typing import Any, Callable, Dict, Optional, Tuple

from dateutil import parser, tz


class StubError(Exception):
    """StubError."""


class Interval:  # pylint: disable=too-few-public-methods
    """Interval."""

    def __init__(self, on: datetime, end: Optional[datetime] = None):
        """__init__."""
        self.on = on
        self.end = end

    def as_doc(self) -> Dict[str, Any]:
        """As doc."""
        return {"end": self.end, "on": self.on}


def epoch_ms_from_utc_datetime(utc: datetime) -> float:
    """Epoch ms from non-naive UTC datetime."""
    return utc.timestamp() * 1000


def utc_datetime_from_epoch_ms(epoch_ms: float) -> datetime:
    """Non-naive UTC datetime from UTC epoch ms."""
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)


def now_utc_datetime() -> datetime:
    """Non-naive now UTC datetime."""
    return datetime.now(tz=timezone.utc)


def get_tzinfo(key: str) -> tzinfo:
    """Get tzinfo."""
    result = tz.gettz(key)
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


def inject_utc_non_naive_datetime(
    key: str, kwargs: Dict[str, Any]
) -> Callable:
    """Inject utc non-naive datetime."""

    def _inject(value: str) -> datetime:
        assert value.__class__ is str
        # dateutil.parser can handle timestamptz output copied
        # from psql directly
        result = parser.parse(value)
        assert result.tzinfo == tz.tzutc()
        result.replace(tzinfo=timezone.utc)
        kwargs[key] = result
        return result

    return _inject


def inject_namespace(key: str, kwargs: Dict[str, Any]) -> Callable:
    """Inject namespace."""

    def _inject(value: str) -> Namespace:
        result = namespace_directory(value)
        kwargs[key] = result
        return result

    return _inject
