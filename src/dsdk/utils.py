# -*- coding: utf-8 -*-
"""Utils."""

from __future__ import annotations

from collections import OrderedDict
from functools import wraps
from logging import NullHandler, getLogger
from time import sleep as default_sleep
from typing import Callable, Sequence

from pandas import DataFrame
from pandas import concat as pd_concat

logger = getLogger(__name__)
logger.addHandler(NullHandler())


def get_res_with_values(query, values, conn) -> list:
    """Get result from query with values."""
    res = conn.execute(query, values)
    data = res.fetchall()
    data_d = [dict(r.items()) for r in data]
    return data_d


def chunks(lst, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]  # noqa: E203


def chunk_res_with_values(
    query, ids, conn, chunk_size=10000, params=None
) -> DataFrame:
    """Chunk query result values."""
    if params is None:
        params = {}
    res = []
    for sub_ids in chunks(ids, chunk_size):
        params.update({"ids": sub_ids})
        res.append(DataFrame(get_res_with_values(query, params, conn)))
    return pd_concat(res, ignore_index=True)


class WriteOnceDict(OrderedDict):
    """Write Once Dict."""

    def __setitem__(self, key, value):
        """__setitem__."""
        if key in self:
            raise KeyError("{} has already been set".format(key))
        super(WriteOnceDict, self).__setitem__(key, value)


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
