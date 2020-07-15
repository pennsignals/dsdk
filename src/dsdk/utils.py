# -*- coding: utf-8 -*-
"""Utils."""

from __future__ import annotations

from functools import wraps
from json import dump as json_dump
from json import load as json_load
from logging import INFO, Formatter, StreamHandler, getLogger
from pickle import dump as pickle_dump
from pickle import load as pickle_load
from sys import stdout
from time import sleep as default_sleep
from typing import Any, Callable, Dict, List, Optional, Sequence

from pandas import DataFrame
from pandas import concat as pd_concat

logger = getLogger(__name__)


def get_logger(name="", level=INFO):
    """Get logger.

    Actual handlers are typically set by the application.
    Libraries (like DSDK) typically use a NullHandler, so that the application
        logger configuration is used.

    Use this function to hide the logger implementation/config for now.
    Show that the conventions demonstrated here work for the applications.
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


def load_json_file(path: str) -> object:
    """Load json from file."""
    with open(path, "r") as fin:
        return json_load(fin)


def load_pickle_file(path: str) -> object:
    """Load pickle from file."""
    with open(path, "rb") as fin:
        return pickle_load(fin)


def df_from_query_by_ids(
    con,  # TODO type annotation for con
    query: str,
    ids: Sequence[Any],
    parameters: Optional[Dict[str, Any]] = None,
    size: int = 10000,
) -> DataFrame:
    """Return DataFrame from query by ids."""
    if parameters is None:
        parameters = {}
    return pd_concat(
        [
            DataFrame(
                [
                    dict(each.items())
                    for each in con.execute(
                        query, {"ids": chunk, **parameters}
                    ).fetchall()
                ]
            )
            for chunk in chunks(ids, size)
        ],
        ignore_index=True,
    )


def get_res_with_values(query, values, con) -> List[Dict[str, Any]]:
    """Get result from query with values."""
    result = con.execute(query, values)
    return [dict(each.items()) for each in result.fetchall()]


def chunk_res_with_values(
    query: str,
    ids: Sequence[Any],
    con,
    size: int = 10000,
    params: Optional[Dict[str, Any]] = None,
) -> DataFrame:
    """Chunk query result values."""
    if params is None:
        params = {}
    result = []
    for chunk in chunks(ids, size):
        params.update({"ids": chunk})
        result.append(DataFrame(get_res_with_values(query, params, con)))
    del params["ids"]
    return pd_concat(result, ignore_index=True)


def retry(
    exceptions: Sequence[Exception],
    retries: int = 5,
    delay: float = 1.0,
    backoff: float = 1.5,
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
