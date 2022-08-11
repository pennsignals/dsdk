"""Test union all."""

from contextlib import contextmanager
from typing import Any, Generator

from pandas import DataFrame

from dsdk.persistor import Persistor as BasePersistor


class Persistor(BasePersistor):
    """Mock Persistor."""

    @classmethod
    def mogrify(cls, cur, query: str, parameters: Any) -> bytes:
        """Mogrify query.

        This mogrify is NOT sql-injection safe and should not be used.
        """
        return (query % (parameters)).encode("utf-8")

    @contextmanager
    def connect(self) -> Generator[Any, None, None]:
        """Connect."""
        # Replace return type with ContextManager[Any] when mypy is fixed.
        raise NotImplementedError()


def test_union_all(cls=Persistor):
    """Test union all."""
    sql = """
with cohort as (
    select cast(null as int) as id
    {cohort}
)
select id from cohort"""
    expected = """
with cohort as (
    select cast(null as int) as id
    union all select 1
    union all select 2
    union all select 3
)
select id from cohort"""
    actual = cls.render(None, sql, keys={"cohort": (1, 2, 3)})
    assert actual == expected


def test_union_all_many(cls=Persistor):
    """Test union all many."""
    sql = """
with cohort as (
    select cast(null as int) as id, cast(null as int) as pcp_id
    {cohort}
)
select id from cohort"""
    expected = """
with cohort as (
    select cast(null as int) as id, cast(null as int) as pcp_id
    union all select 11, 12
    union all select 21, 22
    union all select 31, 32
)
select id from cohort"""
    actual = cls.render(
        None,
        sql,
        keys={"cohort": DataFrame({"a": (11, 21, 31), "b": (12, 22, 32)})},
    )
    assert actual == expected
