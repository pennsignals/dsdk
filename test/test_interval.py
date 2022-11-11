"""Test interval."""

from io import StringIO

from cfgenvy import yaml_dumps, yaml_loads
from pytest import mark

from dsdk import Interval

CONFIGS = """
!interval
end: 1
'on': 0
""".strip()

ALT_CONFIGS = """
!interval
end: 1
on: 0
""".strip()


@mark.parametrize(
    "cls,configs,expected",
    ((Interval, CONFIGS, CONFIGS), (Interval, ALT_CONFIGS, CONFIGS)),
)
def test_round_trip(cls, configs, expected):
    """Test round trip."""
    cls.as_yaml_type()
    interval = yaml_loads(StringIO(configs))
    actual = yaml_dumps(interval).strip()
    assert actual == expected
