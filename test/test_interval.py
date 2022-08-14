"""Test interval."""

from io import StringIO

from cfgenvy import yaml_dumps, yaml_loads

from dsdk import Interval

CONFIGS = """
!interval
end: 1
'on': 0
""".strip()


def test_round_trip(cls=Interval, configs=CONFIGS):
    """Test round trip."""
    cls.as_yaml_type()

    interval = yaml_loads(StringIO(configs))
    actual = yaml_dumps(interval).strip()
    # raise ValueError("%s, %s" % (actual, configs))
    assert actual == configs
