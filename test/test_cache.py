"""Test cache.

Refactor so lookups are part of each test function a mark parametrized.
"""

from contextlib import contextmanager
from typing import Any, Generator

from pandas.testing import assert_frame_equal
from pytest import raises

from dsdk.persistor import Persistor as BasePersistor


class Cursor:  # pylint: disable=too-few-public-methods
    """Stub cursor."""

    def __init__(self):
        """__init__."""
        self.description: tuple[tuple[str]] | None = None
        self.result: str | None = None
        self.executes = 0
        self.fetches = 0

    def fetchall(self):
        """Fetchall."""
        self.fetches += 1
        return self.result


def lookup():
    """Lookup sql."""
    d = {}
    d["""select 1 as a"""] = (
        (("a",),),
        ((1,)),
    )
    d["""select 1 as b"""] = (
        (("b",),),
        ((1,)),
    )
    d["""select 1 as c"""] = (
        (("c",),),
        ((1,)),
    )
    d["""select 1 as d"""] = (
        (("d",),),
        ((1,)),
    )
    d["""select 1 as extant"""] = (
        (("extant",),),
        ((1,)),
    )
    d[
        """
with cohort as (
    select cast(null as int) as id
    union all select 0
    union all select 1
    union all select 2
)
select id from cohort where id is not null"""
    ] = (
        (("id",),),
        ((0, 1, 2)),
    )
    d[
        """
with cohort as (
    select cast(null as int) as id
    union all select 0
)
select id from cohort where id is not null"""
    ] = (
        (("id",),),
        ((0,)),
    )
    d[
        """
with cohort as (
    select cast(null as int) as id
    union all select 1
)
select id from cohort where id is not null"""
    ] = (
        (("id",),),
        ((1,)),
    )
    d[
        """
with cohort as (
    select cast(null as int) as id
    union all select 2
)
select id from cohort where id is not null"""
    ] = (
        (("id",),),
        ((2,)),
    )
    return d


class Persistor(BasePersistor):
    """Stub Persistor."""

    LOOKUP = lookup()

    @classmethod
    def execute(cls, cur: Any, rendered: str):
        """Execute rendered sql with cur."""
        hit = cls.LOOKUP[rendered]
        cur.description, cur.result = hit
        cur.executes += 1

    @classmethod
    def mogrify(cls, cur, query: str, parameters: dict[str, Any]):
        """Mogrify query.

        This mogrify is NOT sql-injection safe, and should not be used.
        """
        return (query % (parameters)).encode("utf-8")

    @contextmanager
    def connect(self) -> Generator[Any, None, None]:
        """Connect."""
        # Replace return type with ContextManager[Any] when mypy is fixed.
        raise NotImplementedError()


def test_no_cache(cls=Persistor):
    """Test no cache."""
    query = "select 1 as extant"
    cur = Cursor()
    a = cls.df_from_query(cur, query)
    b = cls.df_from_query(cur, query)
    assert cur.executes == 2
    assert_frame_equal(a, b)


def test_cache_equal(cls=Persistor):
    """Test cache equal."""
    cur = Cursor()
    a = cls.df_from_query(cur, "select 1 as a", cache="cache/test")
    b = cls.df_from_query(cur, "select 1 as a", cache="cache/test")
    assert cur.executes == 1
    assert_frame_equal(a, b)


def test_cache_not_equal(cls=Persistor):
    """Test cache not equal."""
    cur = Cursor()
    c = cls.df_from_query(cur, "select 1 as c", cache="cache/test")
    d = cls.df_from_query(cur, "select 1 as d", cache="cache/test")
    assert cur.executes == 2
    with raises(AssertionError):
        assert_frame_equal(c, d)


def test_chunk_query(cls=Persistor):
    """Test chunk query."""
    sql = """
with cohort as (
    select cast(null as int) as id
    {cohort}
)
select id from cohort where id is not null"""
    cur = Cursor()
    cohort = (0, 1, 2)
    _ = cls.query(cur, sql, by="cohort", keys={"cohort": cohort}, size=1)
    assert cur.executes == 3


def test_chunk_df_from_query(cls=Persistor):
    """Test chunk df_from_query."""
    sql = """
with cohort as (
    select cast(null as int) as id
    {cohort}
)
select id from cohort where id is not null"""
    cohort = (0, 1, 2)
    cur = Cursor()
    a = cls.df_from_query(
        cur,
        sql,
        by="cohort",
        keys={"cohort": cohort},
        cache="cache/test",
        size=1,
    )
    b = cls.df_from_query(
        cur,
        sql,
        by="cohort",
        keys={"cohort": cohort},
        cache="cache/test",
    )
    assert cur.executes == 4
    assert_frame_equal(a, b)
