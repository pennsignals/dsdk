"""Test mssql."""

from dsdk import Mssql


def test_mogrify():
    """Test mogrify."""
    expected = b"select cast(N'a' as varchar) as column_name"
    actual = Mssql.mogrify(
        None,
        "select cast(%(a)s as varchar) as column_name",
        parameters={"a": "a"},
    )
    assert actual == expected
