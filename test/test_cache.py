"""Test cache."""

from contextlib import contextmanager
from typing import Any, Generator

import numpy as np
from pandas import DataFrame
from pandas.testing import assert_frame_equal

from dsdk.persistor import Persistor as BasePersistor


class Persistor(BasePersistor):
    """Mock Persistor."""

    @classmethod
    def df_from_rendered(cls, cur, rendered):
        """Dataframer from rendered query."""
        return DataFrame(
            np.random.randint(0, 100, size=(100, 4)), columns=list("test")
        )

    @classmethod
    def mogrify(cls, cur, query: str, parameters: Any):
        """Mogrify query."""
        return ((query % (parameters)) + "_rendered").encode("utf-8")

    @contextmanager
    def connect(self) -> Generator[Any, None, None]:
        """Connect."""
        # Replace return type with ContextManager[Any] when mypy is fixed.
        raise NotImplementedError()


def test_no_cache(cls=Persistor):
    """Test no cache."""
    query = "select 1 as extant"
    a = cls.df_from_query(None, query)
    b = cls.df_from_query(None, query)
    try:
        assert_frame_equal(a, b)
    except AssertionError:
        pass
    else:
        raise AssertionError("DataFrames should not be equal")


def test_cache_equal(cls=Persistor):
    """Test cache equal."""
    a = cls.df_from_query(None, "select 1 as a", cache="cache")
    b = cls.df_from_query(None, "select 1 as a", cache="cache")
    assert_frame_equal(a, b)


def test_cache_not_equal(cls=Persistor):
    """Test cache not equal."""
    a = cls.df_from_query(None, "select 1 as a", cache="cache")
    b = cls.df_from_query(None, "select 1 as b", cache="cache")
    try:
        assert_frame_equal(a, b)
    except AssertionError:
        pass
    else:
        raise AssertionError("DataFrames should not be equal")
