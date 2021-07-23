# -*- coding: utf-8 -*-
"""Utils."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone, tzinfo
from functools import wraps
from json import dump as json_dump
from json import load as json_load
from logging import INFO, Formatter, StreamHandler, getLogger
from pickle import dump as pickle_dump
from pickle import load as pickle_load
from sys import stdout
from time import perf_counter_ns
from time import sleep as default_sleep
from typing import Any, Callable, Generator, Optional, Pattern, Sequence, Type

from yaml import dump as _yaml_dumps
from yaml import load as _yaml_loads

try:
    from yaml import CSafeDumper as Dumper  # type: ignore[misc]
    from yaml import CSafeLoader as Loader  # type: ignore[misc]
except ImportError:
    from yaml import SafeDumper as Dumper  # type: ignore[misc]
    from yaml import SafeLoader as Loader  # type: ignore[misc]

from dateutil import parser, tz

logger = getLogger(__name__)


def as_utc_non_naive_datetime(value: str) -> datetime:
    """As utc non-naive datetime."""
    assert value.__class__ is str
    # dateutil.parser can handle timestamptz output copied
    # from psql directly
    result = parser.parse(value)
    assert result.tzinfo == tz.tzutc()
    result.replace(tzinfo=timezone.utc)
    return result


def configure_logger(name, level=INFO):
    """Configure logger.

    This function should be done by the application.
    Libraries (like DSDK) should not configure their own loggers.
    """
    result = getLogger(name)
    result.setLevel(level)
    formatter_string = " - ".join(
        (
            "%(asctime)-15s",
            "%(levelname)s",
            "%(name)s.%(funcName)s",
            "%(message)s",
        )
    )
    handler = StreamHandler(stdout)
    handler.setLevel(level)
    handler.setFormatter(Formatter(formatter_string))
    result.addHandler(handler)
    return result


def chunks(sequence: Sequence[Any], n: int):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(sequence), n):
        yield sequence[i : i + n]


def dump_json_file(obj: Any, path: str) -> None:
    """Dump json to file."""
    with open(path, "w") as fout:
        json_dump(obj, fout)


def dump_pickle_file(obj: Any, path: str) -> None:
    """Dump pickle to file."""
    with open(path, "wb") as fout:
        pickle_dump(obj, fout)


def dump_yaml_file(data: Any, path: str, **kwargs) -> None:
    """Dump yaml file."""
    with open(path, "w") as fout:
        yaml_dumps(data=data, stream=fout, **kwargs)


def epoch_ms_from_utc_datetime(utc: datetime) -> float:
    """Epoch ms from non-naive UTC datetime."""
    return utc.timestamp() * 1000


def get_tzinfo(key: str) -> tzinfo:
    """Get tzinfo."""
    result = tz.gettz(key)
    assert result is not None
    return result


def load_json_file(path: str) -> object:
    """Load json from file."""
    with open(path, "r") as fin:
        return json_load(fin)


def load_pickle_file(path: str) -> object:
    """Load pickle from file."""
    with open(path, "rb") as fin:
        return pickle_load(fin)


def load_yaml_file(path: str):
    """Load yaml file."""
    with open(path) as fin:
        return yaml_loads(fin)


def now_utc_datetime() -> datetime:
    """Non-naive now UTC datetime."""
    return datetime.now(tz=timezone.utc)


@contextmanager
def profile(key: str) -> Generator[Any, None, None]:
    """Profile."""
    # Replace return type with ContextManager[Any] when mypy is fixed.
    begin = perf_counter_ns()
    logger.info('{"key": "%s.begin", "ns": "%s"}', key, begin)
    yield
    end = perf_counter_ns()
    logger.info(
        '{"key": "%s.end", "ns": "%s", "elapsed": "%s"}', key, end, end - begin
    )


def retry(
    exceptions: Sequence[Exception],
    retries: int = 60,
    delay: float = 1.0,
    backoff: float = 1.05,
    sleep: Callable = default_sleep,
):
    """
    Retry calling the decorated function using an exponential backoff.

    Args:
        exceptions: The exception to check. may be a tuple of
            exceptions to check.
        retries: Number of times to retry before giving up.
        delay: Initial delay between retries in seconds.
        backoff: Backoff multiplier (e.g. value of 2 will double the delay
            each retry).
    """
    delay = float(delay)
    backoff = float(backoff)

    def wrapper(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except exceptions as exception:
                logger.exception(exception)
                wait = delay
                for _ in range(retries):
                    message = f"Retrying in {wait:.2f} seconds..."
                    logger.warning(message)
                    sleep(wait)
                    wait *= backoff
                    try:
                        return func(*args, **kwargs)
                    except exceptions as exception:
                        logger.exception(exception)
                raise

        return wrapped

    return wrapper


def utc_datetime_from_epoch_ms(epoch_ms: float) -> datetime:
    """Non-naive UTC datetime from UTC epoch ms."""
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)


def yaml_dumps(data, **kwargs):
    """Yaml dumps."""
    return _yaml_dumps(data=data, Dumper=Dumper, **kwargs)


def yaml_loads(stream, **kwargs):
    """Yaml loads."""
    return _yaml_loads(stream=stream, Loader=Loader, **kwargs)


def yaml_type(
    cls: type,
    tag: str,
    *,
    init: Optional[Callable] = None,
    repr: Optional[Callable] = None,  # pylint: disable=redefined-builtin
    loader: Optional[Type[Loader]] = None,
    dumper: Optional[Type[Dumper]] = None,
    **kwargs,
):
    """Yaml type."""
    _loader = loader or Loader
    _dumper = dumper or Dumper
    if init is not None:

        def _init_closure(loader, node):
            return init(loader, node, **kwargs)

        _loader.add_constructor(tag, _init_closure)

    if repr is not None:

        def _repr_closure(dumper, self):
            return repr(dumper, self, tag=tag, **kwargs)

        _dumper.add_representer(cls, _repr_closure)


def yaml_implicit_type(
    cls: type,
    tag: str,
    *,
    init: Callable,
    pattern: Pattern,
    repr: Optional[Callable] = None,  # pylint: disable=redefined-builtin
    loader: Optional[Type[Loader]] = None,
    dumper: Optional[Type[Dumper]] = None,
    **kwargs,
):
    """Yaml implicit type."""
    _loader = loader or Loader
    _dumper = dumper or Dumper

    def _init_closure(loader, node):
        return init(loader, node, pattern=pattern, **kwargs)

    _loader.add_constructor(tag, _init_closure)
    _loader.add_implicit_resolver(tag, pattern, None)

    if repr is not None:

        def _repr_closure(dumper, self):
            return repr(dumper, self, tag=tag, pattern=pattern, **kwargs)

        _dumper.add_representer(cls, _repr_closure)


class StubError(Exception):
    """StubError."""
