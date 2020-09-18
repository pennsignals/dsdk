# -*- coding: utf-8 -*-
"""Test epoch_ms and utc datetime conversions."""

from dsdk.dependency import (
    epoch_ms_from_utc_datetime,
    now_utc_datetime,
    utc_datetime_from_epoch_ms,
)


def test_conversions():
    """Test conversions."""
    expected = now_utc_datetime()
    epoch_ms = epoch_ms_from_utc_datetime(expected)
    actual = utc_datetime_from_epoch_ms(epoch_ms)
    assert expected == actual
