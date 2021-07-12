# -*- coding: utf-8 -*-
"""Dependency injection as_type.

The type parameter for parser.add_arugment(...) is a function/ctor.

The xxx_as_type functions return the function/ctor used as the type parameter,
creating a closure for the other parameters passed to
xxx_as_type(parameters...).

This is an alternative to the typical temporal coupling in code where
resolution of what to do with an command line or env argument is separate
code from the declaration of that parameter.
"""

from argparse import Namespace
from datetime import datetime, timezone, tzinfo
from json import load as json_load
from os import listdir
from os.path import isdir, join, splitext
from pickle import load as pickle_load
from typing import Any, Callable, Dict, Optional, Tuple, cast

from dateutil import parser, tz
from yaml import safe_load as yaml_load


class StubException(Exception):
    """StubException."""


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


def as_type_pickle_file(key: str, kwargs: Dict[str, Any]) -> Callable:
    """As type pickle."""

    def _as_type(path: str) -> object:
        with open(path) as fin:
            result = pickle_load(fin)
        if key is None:
            kwargs.update(cast(Dict[str:Any], result))
            return result
        kwargs[key] = result
        return result


def as_type_yaml_file(key: str, kwargs: Dict[str, Any]) -> Callable:
    """As type yaml."""

    def _as_type(path: str) -> Dict[str, Any]:
        with open(path) as fin:
            result = yaml_load(fin)
        if key is None:
            kwargs.update(result)
            return result
        kwargs[key] = result
        return result

    return _as_type


def as_type_json_file(key: str, kwargs: Dict[str, Any]) -> Callable:
    """As type json."""

    def _as_type(path: str) -> Dict[str, Any]:
        with open(path) as fin:
            result = json_load(fin)
        if key is None:
            kwargs.update(result)
            return result
        kwargs[key] = result
        return result

    return _as_type


def as_type_float(key: str, kwargs: Dict[str, Any]) -> Callable:
    """As type float."""

    def _as_type(value) -> float:
        kwargs[key] = result = float(value)
        return result

    return _as_type


def as_type_int(key: str, kwargs: Dict[str, Any]) -> Callable:
    """As type int."""

    def _as_type(value) -> int:
        kwargs[key] = result = int(value)
        return result

    return _as_type


def as_type_str(key: str, kwargs: Dict[str, Any]) -> Callable:
    """As type str."""

    def _as_type(value: str) -> str:
        assert value.__class__ is str
        kwargs[key] = result = value
        return result

    return _as_type


def as_type_str_tuple(key: str, kwargs: Dict[str, Any]) -> Callable:
    """As type str tuple."""

    def _as_type(value: str) -> Tuple[str, ...]:
        assert value.__class__ is str
        kwargs[key] = result = tuple(value.split(","))
        return result

    return _as_type


def as_type_yaml_tuple(key: str, kwargs: Dict[str, Any]) -> Callable:
    """As type yaml tuple."""

    def _as_type(value: str) -> Tuple[Any, ...]:
        kwargs[key] = result = tuple(yaml_load(value))
        return result

    return _as_type


def as_type_yaml_list(key: str, kwargs: Dict[str, Any]) -> Callable:
    """As type yaml list."""

    def _as_type(value: str) -> Dict[str, Any]:
        kwargs[key] = result = yaml_load(value)
        assert result.__class__ is list

    return _as_type


def as_type_yaml_dict(key: str, kwargs: Dict[str, Any]) -> Callable:
    """As type yaml dict."""

    def _as_type(value: str) -> Dict[str, Any]:
        kwargs[key] = result = yaml_load(value)
        assert result.__class__ is dict
        return result

    return _as_type


def as_type_utc_non_naive_datetime(
    key: str, kwargs: Dict[str, Any]
) -> Callable:
    """As type utc non-naive datetime."""

    def _as_type(value: str) -> datetime:
        assert value.__class__ is str
        # dateutil.parser can handle timestamptz output copied
        # from psql directly
        result = parser.parse(value)
        assert result.tzinfo == tz.tzutc()
        result.replace(tzinfo=timezone.utc)
        kwargs[key] = result
        return result

    return _as_type


def as_type_namespace(key: str, kwargs: Dict[str, Any]) -> Callable:
    """As type Namespace."""

    def _as_type(value: str) -> Namespace:
        result = namespace_directory(value)
        kwargs[key] = result
        return result

    return _as_type
